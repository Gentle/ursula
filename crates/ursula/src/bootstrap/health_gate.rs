//! Shared core of the leadership health gates (the snapshot-store gate and
//! the cold-health gate): the shed/heal hysteresis, and the drain that hands
//! a shedding node's leaderships to its peers a few per tick.

use std::collections::BTreeSet;

use ursula_raft::LeadershipShedReason;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_raft::RaftGroupMetricsSnapshot;
use ursula_shard::RaftGroupId;

use crate::bootstrap::util::leader_counts;

/// Most leaderships a health gate hands off per tick. A node that sheds moves
/// its leaderships over several ticks, so its peers take them over a few at
/// a time rather than all at once (#508).
const SHED_HANDOFFS_PER_TICK: usize = 4;

/// What one tick of a health gate observed. Ordered from healthy to failing,
/// so the verdict of several signals is the maximum of theirs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum TickHealth {
    /// Every signal is healthy.
    Good,
    /// No signal fails, but not every signal is healthy. The tick continues
    /// a bad streak without lengthening it, and restarts a good streak.
    Inconclusive,
    /// A signal fails.
    Bad,
}

impl TickHealth {
    /// Classifies the errors one tick added to a counter: none is good, up
    /// to `threshold` is inconclusive, and more is bad.
    pub(crate) fn of_new_errors(errors: u64, threshold: u64) -> Self {
        if errors == 0 {
            Self::Good
        } else if errors <= threshold {
            Self::Inconclusive
        } else {
            Self::Bad
        }
    }
}

/// How a tick changed a health gate's stance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HealthTransition {
    /// The stance did not change.
    Hold,
    /// The node starts shedding its leadership.
    Shed,
    /// The node stops shedding.
    Heal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stance {
    /// The node may lead. Counts the bad ticks since the last good tick.
    Serving { bad_ticks: usize },
    /// The node sheds. Counts the good ticks in a row.
    Shedding { good_ticks: usize },
}

/// The hysteresis of every leadership health gate: shed after
/// `unhealthy_ticks` bad ticks with no good tick between them, and heal
/// after `heal_ticks` good ticks in a row.
#[derive(Debug, Clone)]
pub(crate) struct HealthHysteresis {
    unhealthy_ticks: usize,
    heal_ticks: usize,
    stance: Stance,
}

impl HealthHysteresis {
    pub(crate) fn new(unhealthy_ticks: usize, heal_ticks: usize) -> Self {
        Self {
            unhealthy_ticks: unhealthy_ticks.max(1),
            heal_ticks: heal_ticks.max(1),
            stance: Stance::Serving { bad_ticks: 0 },
        }
    }

    pub(crate) fn observe(&mut self, tick: TickHealth) -> HealthTransition {
        let (stance, transition) = match (self.stance, tick) {
            (Stance::Serving { bad_ticks }, TickHealth::Bad) => {
                let bad_ticks = bad_ticks.saturating_add(1);
                if bad_ticks >= self.unhealthy_ticks {
                    (Stance::Shedding { good_ticks: 0 }, HealthTransition::Shed)
                } else {
                    (Stance::Serving { bad_ticks }, HealthTransition::Hold)
                }
            }
            (Stance::Serving { bad_ticks }, TickHealth::Inconclusive) => {
                (Stance::Serving { bad_ticks }, HealthTransition::Hold)
            }
            (Stance::Serving { .. }, TickHealth::Good) => {
                (Stance::Serving { bad_ticks: 0 }, HealthTransition::Hold)
            }
            (Stance::Shedding { good_ticks }, TickHealth::Good) => {
                let good_ticks = good_ticks.saturating_add(1);
                if good_ticks >= self.heal_ticks {
                    (Stance::Serving { bad_ticks: 0 }, HealthTransition::Heal)
                } else {
                    (Stance::Shedding { good_ticks }, HealthTransition::Hold)
                }
            }
            (Stance::Shedding { .. }, TickHealth::Inconclusive | TickHealth::Bad) => {
                (Stance::Shedding { good_ticks: 0 }, HealthTransition::Hold)
            }
        };
        self.stance = stance;
        transition
    }

    #[cfg(test)]
    pub(crate) fn is_shedding(&self) -> bool {
        matches!(self.stance, Stance::Shedding { .. })
    }
}

/// What a cumulative counter gained between ticks. The first reading is the
/// baseline, so counts from before the gate started are not new.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct CounterGrowth {
    last: Option<u64>,
}

impl CounterGrowth {
    pub(crate) fn advance(&mut self, total: u64) -> u64 {
        let growth = self.last.map_or(0, |last| total.saturating_sub(last));
        self.last = Some(total);
        growth
    }
}

/// One planned handoff of a leadership the node sheds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ShedHandoff {
    pub group_id: u32,
    pub target: u64,
}

/// Plans at most `limit` handoffs of the groups `node_id` leads and
/// `selected` accepts, smallest group id first. Each goes to the `eligible` peer voter
/// that leads the fewest groups once the handoffs planned before it are
/// counted, so the handoffs spread over the peers.
pub(crate) fn plan_shed_handoffs(
    snaps: &[RaftGroupMetricsSnapshot],
    node_id: u64,
    limit: usize,
    selected: impl Fn(u32) -> bool,
    eligible: impl Fn(RaftGroupId, u64) -> bool,
) -> Vec<ShedHandoff> {
    let mut planned_load = leader_counts(snaps);
    let mut groups_we_lead: Vec<&RaftGroupMetricsSnapshot> = snaps
        .iter()
        .filter(|snap| snap.current_leader == Some(node_id) && selected(snap.raft_group_id))
        .collect();
    groups_we_lead.sort_by_key(|snap| snap.raft_group_id);

    let mut handoffs = Vec::new();
    for snap in groups_we_lead {
        if handoffs.len() >= limit {
            break;
        }
        let Some(target) = snap
            .voter_ids
            .iter()
            .copied()
            .filter(|voter| *voter != node_id && eligible(RaftGroupId(snap.raft_group_id), *voter))
            .min_by_key(|target| (planned_load.get(target).copied().unwrap_or(0), *target))
        else {
            continue;
        };
        handoffs.push(ShedHandoff {
            group_id: snap.raft_group_id,
            target,
        });
        let target_load = planned_load.entry(target).or_insert(0);
        *target_load = target_load.saturating_add(1);
        if let Some(load) = planned_load.get_mut(&node_id) {
            *load = load.saturating_sub(1);
        }
    }
    handoffs
}

/// One health gate's shed of this node: the leadership-shed mark of the
/// gate's reason, and the leaderships the node still has to hand off.
///
/// Dropping it clears the mark, so a gate that stops does not keep the node
/// out of elections.
#[derive(Debug)]
pub(crate) struct LeadershipShed {
    registry: RaftGroupHandleRegistry,
    reason: LeadershipShedReason,
    node_id: u64,
    /// Groups the node led when the gate shed and has not handed off yet.
    /// Leaderships the node gains later are left alone, so a shed does not
    /// fight the leadership balancer over a node that may still campaign.
    pending: BTreeSet<u32>,
}

impl LeadershipShed {
    pub(crate) fn new(
        registry: &RaftGroupHandleRegistry,
        reason: LeadershipShedReason,
        node_id: u64,
    ) -> Self {
        Self {
            registry: registry.clone(),
            reason,
            node_id,
            pending: BTreeSet::new(),
        }
    }

    /// Marks the shed and starts handing off the leaderships the node holds.
    pub(crate) fn begin(&mut self) {
        self.registry.mark_leadership_shed(self.reason);
        self.start(&self.registry.metrics_snapshot());
    }

    /// Clears the mark and stops handing off leaderships.
    pub(crate) fn end(&mut self) {
        self.registry.clear_leadership_shed(self.reason);
        self.pending.clear();
    }

    /// Hands off this tick's share of the pending leaderships.
    pub(crate) async fn step(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let snaps = self.registry.metrics_snapshot();
        let registry = self.registry.clone();
        let handoffs = self.plan(&snaps, |group, target| {
            registry.check_handoff(group, target).is_ok()
        });
        for handoff in handoffs {
            match registry
                .transfer_leader(RaftGroupId(handoff.group_id), handoff.target)
                .await
            {
                Ok(()) => {
                    self.pending.remove(&handoff.group_id);
                    tracing::warn!(
                        reason = %self.reason,
                        node_id = self.node_id,
                        raft_group_id = handoff.group_id,
                        target = handoff.target,
                        pending = self.pending.len(),
                        "leadership-shed: handed off a leadership"
                    );
                }
                Err(err) => tracing::error!(
                    reason = %self.reason,
                    raft_group_id = handoff.group_id,
                    target = handoff.target,
                    %err,
                    "leadership-shed: handoff failed; retrying next tick"
                ),
            }
        }
        if self.pending.is_empty() {
            tracing::info!(
                reason = %self.reason,
                node_id = self.node_id,
                "leadership-shed: no leadership left to hand off"
            );
        }
    }

    fn start(&mut self, snaps: &[RaftGroupMetricsSnapshot]) {
        self.pending = groups_led_by(snaps, self.node_id);
    }

    /// This tick's handoffs: at most [`SHED_HANDOFFS_PER_TICK`] of the
    /// pending groups the node still leads. Forgets the pending groups it no
    /// longer leads. A group with no eligible target stays pending.
    fn plan(
        &mut self,
        snaps: &[RaftGroupMetricsSnapshot],
        eligible: impl Fn(RaftGroupId, u64) -> bool,
    ) -> Vec<ShedHandoff> {
        let led = groups_led_by(snaps, self.node_id);
        self.pending.retain(|group| led.contains(group));
        plan_shed_handoffs(
            snaps,
            self.node_id,
            SHED_HANDOFFS_PER_TICK,
            |group| self.pending.contains(&group),
            eligible,
        )
    }
}

impl Drop for LeadershipShed {
    fn drop(&mut self) {
        self.registry.clear_leadership_shed(self.reason);
    }
}

fn groups_led_by(snaps: &[RaftGroupMetricsSnapshot], node_id: u64) -> BTreeSet<u32> {
    snaps
        .iter()
        .filter(|snap| snap.current_leader == Some(node_id))
        .map(|snap| snap.raft_group_id)
        .collect()
}

#[cfg(test)]
mod tests {
    use proptest::prelude::ProptestConfig;
    use proptest::prelude::Strategy;
    use proptest::prelude::prop;
    use proptest::prop_assert_eq;
    use proptest::prop_oneof;
    use proptest::proptest;
    use proptest::strategy::Just;
    use ursula_raft::LeadershipShedReason;
    use ursula_raft::RaftGroupHandleRegistry;
    use ursula_raft::RaftGroupMetricsSnapshot;

    use super::CounterGrowth;
    use super::HealthHysteresis;
    use super::HealthTransition;
    use super::LeadershipShed;
    use super::SHED_HANDOFFS_PER_TICK;
    use super::ShedHandoff;
    use super::TickHealth;
    use super::plan_shed_handoffs;

    const ME: u64 = 1;

    fn snap(group_id: u32, leader: Option<u64>) -> RaftGroupMetricsSnapshot {
        RaftGroupMetricsSnapshot {
            apply_failure: None,
            raft_group_id: group_id,
            node_id: ME,
            current_term: 1,
            current_leader: leader,
            last_log: None,
            committed: None,
            last_applied: None,
            snapshot: None,
            purged: None,
            voter_ids: vec![1, 2, 3],
            voter_configurations: vec![[1, 2, 3].into()],
            learner_ids: Vec::new(),
            maintenance: ursula_raft::RaftGroupMaintenanceState::default(),
            log: Default::default(),
        }
    }

    fn transitions(
        hysteresis: &mut HealthHysteresis,
        ticks: &[TickHealth],
    ) -> Vec<HealthTransition> {
        ticks.iter().map(|tick| hysteresis.observe(*tick)).collect()
    }

    /// The hysteresis spelled out over the tick history: shed once the ticks
    /// since the last good tick (and since the last heal) hold
    /// `unhealthy_ticks` bad ones, heal once the ticks since the shed end in
    /// `heal_ticks` good ones.
    fn reference(
        ticks: &[TickHealth],
        unhealthy_ticks: usize,
        heal_ticks: usize,
    ) -> Vec<HealthTransition> {
        let (unhealthy_ticks, heal_ticks) = (unhealthy_ticks.max(1), heal_ticks.max(1));
        let mut shedding = false;
        let mut since = 0;
        let mut out = Vec::new();
        for now in 0..ticks.len() {
            let window = &ticks[since..=now];
            let transition = if shedding {
                let good_run = window
                    .iter()
                    .rev()
                    .take_while(|tick| **tick == TickHealth::Good)
                    .count();
                if good_run >= heal_ticks {
                    HealthTransition::Heal
                } else {
                    HealthTransition::Hold
                }
            } else {
                let streak_start = window
                    .iter()
                    .rposition(|tick| *tick == TickHealth::Good)
                    .map_or(0, |good| good.saturating_add(1));
                let bad = window[streak_start..]
                    .iter()
                    .filter(|tick| **tick == TickHealth::Bad)
                    .count();
                if bad >= unhealthy_ticks {
                    HealthTransition::Shed
                } else {
                    HealthTransition::Hold
                }
            };
            if transition != HealthTransition::Hold {
                shedding = !shedding;
                since = now.saturating_add(1);
            }
            out.push(transition);
        }
        out
    }

    fn tick_health() -> impl Strategy<Value = TickHealth> {
        prop_oneof![
            Just(TickHealth::Good),
            Just(TickHealth::Inconclusive),
            Just(TickHealth::Bad),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        #[test]
        fn hysteresis_follows_its_definition(
            ticks in prop::collection::vec(tick_health(), 0..64),
            unhealthy_ticks in 0usize..5,
            heal_ticks in 0usize..5,
        ) {
            let mut hysteresis = HealthHysteresis::new(unhealthy_ticks, heal_ticks);
            prop_assert_eq!(
                transitions(&mut hysteresis, &ticks),
                reference(&ticks, unhealthy_ticks, heal_ticks)
            );
        }
    }

    #[test]
    fn sheds_after_unhealthy_ticks_bad_ticks_and_heals_after_heal_ticks() {
        use HealthTransition::Heal;
        use HealthTransition::Hold;
        use HealthTransition::Shed;
        use TickHealth::Bad;
        use TickHealth::Good;

        let mut hysteresis = HealthHysteresis::new(3, 2);
        assert_eq!(
            transitions(&mut hysteresis, &[Bad, Bad, Good, Bad, Bad]),
            vec![Hold, Hold, Hold, Hold, Hold],
            "a good tick restarts the bad streak"
        );
        assert_eq!(transitions(&mut hysteresis, &[Bad]), vec![Shed]);
        assert!(hysteresis.is_shedding());
        assert_eq!(
            transitions(&mut hysteresis, &[Bad, Good, Bad, Good, Good, Good]),
            vec![Hold, Hold, Hold, Hold, Heal, Hold],
        );
        assert!(!hysteresis.is_shedding());
    }

    #[test]
    fn an_inconclusive_tick_neither_breaks_nor_lengthens_a_bad_streak() {
        use HealthTransition::Hold;
        use HealthTransition::Shed;
        use TickHealth::Bad;
        use TickHealth::Good;
        use TickHealth::Inconclusive;

        let mut hysteresis = HealthHysteresis::new(2, 1);
        assert_eq!(
            transitions(&mut hysteresis, &[Inconclusive, Inconclusive, Bad]),
            vec![Hold, Hold, Hold],
        );
        assert_eq!(transitions(&mut hysteresis, &[Inconclusive, Bad]), vec![
            Hold, Shed
        ]);
        assert_eq!(
            transitions(&mut hysteresis, &[Inconclusive, Inconclusive]),
            vec![Hold, Hold],
            "an inconclusive tick never heals"
        );
        assert!(hysteresis.is_shedding());
        assert_eq!(transitions(&mut hysteresis, &[Good]), vec![
            HealthTransition::Heal
        ]);
    }

    #[test]
    fn new_errors_up_to_the_threshold_are_inconclusive() {
        assert_eq!(TickHealth::of_new_errors(0, 1), TickHealth::Good);
        assert_eq!(TickHealth::of_new_errors(1, 1), TickHealth::Inconclusive);
        assert_eq!(TickHealth::of_new_errors(2, 1), TickHealth::Bad);
        assert_eq!(TickHealth::of_new_errors(1, 0), TickHealth::Bad);
        assert_eq!(
            TickHealth::Good
                .max(TickHealth::Inconclusive)
                .max(TickHealth::Bad),
            TickHealth::Bad
        );
    }

    #[test]
    fn counter_growth_starts_from_its_first_reading() {
        let mut growth = CounterGrowth::default();
        assert_eq!(growth.advance(1_000), 0, "errors from before the gate");
        assert_eq!(growth.advance(1_000), 0);
        assert_eq!(growth.advance(1_003), 3);
        assert_eq!(growth.advance(5), 0, "a reset counter is no growth");
        assert_eq!(growth.advance(6), 1);
    }

    #[test]
    fn handoff_plan_honors_its_limit_and_selection() {
        let snaps: Vec<_> = (0..6).map(|group| snap(group, Some(ME))).collect();
        let handoffs = plan_shed_handoffs(&snaps, ME, 3, |group| group != 1, |_, _| true);
        assert_eq!(handoffs, vec![
            ShedHandoff {
                group_id: 0,
                target: 2
            },
            ShedHandoff {
                group_id: 2,
                target: 3
            },
            ShedHandoff {
                group_id: 3,
                target: 2
            },
        ]);
    }

    #[test]
    fn drain_hands_off_a_few_leaderships_per_tick_until_none_is_left() {
        let groups = 10_u32;
        let mut snaps: Vec<_> = (0..groups).map(|group| snap(group, Some(ME))).collect();
        snaps.push(snap(groups, Some(2)));
        let mut shed = LeadershipShed::new(
            &RaftGroupHandleRegistry::default(),
            LeadershipShedReason::SnapshotDriverS3,
            ME,
        );
        shed.start(&snaps);

        let mut ticks = 0_usize;
        let mut handed_off = Vec::new();
        loop {
            let handoffs = shed.plan(&snaps, |_, _| true);
            if handoffs.is_empty() {
                break;
            }
            ticks = ticks.saturating_add(1);
            assert!(
                handoffs.len() <= SHED_HANDOFFS_PER_TICK,
                "tick {ticks} planned {handoffs:?}"
            );
            for handoff in handoffs {
                // The transfer was submitted and has completed by the next tick.
                shed.pending.remove(&handoff.group_id);
                snaps[handoff.group_id as usize].current_leader = Some(handoff.target);
                handed_off.push(handoff.group_id);
            }
        }
        handed_off.sort_unstable();
        assert_eq!(handed_off, (0..groups).collect::<Vec<_>>());
        assert_eq!(ticks, 3, "10 leaderships at 4 per tick");
    }

    #[test]
    fn drain_retries_ineligible_groups_and_ignores_leaderships_gained_later() {
        let mut snaps = vec![snap(0, Some(ME)), snap(1, Some(ME)), snap(2, Some(2))];
        let mut shed = LeadershipShed::new(
            &RaftGroupHandleRegistry::default(),
            LeadershipShedReason::ColdHealth,
            ME,
        );
        shed.start(&snaps);

        // No eligible target for group 1 yet: it stays pending. Group 0 goes
        // to node 3, which leads nothing.
        let handoffs = shed.plan(&snaps, |group, _| group.0 != 1);
        assert_eq!(handoffs, vec![ShedHandoff {
            group_id: 0,
            target: 3
        }]);
        shed.pending.remove(&0);

        // The node gains group 2 and the balancer moves group 0 back to it:
        // neither was pending, so neither is handed off again.
        snaps[0].current_leader = Some(ME);
        snaps[2].current_leader = Some(ME);
        let handoffs = shed.plan(&snaps, |_, _| true);
        assert_eq!(handoffs, vec![ShedHandoff {
            group_id: 1,
            target: 2
        }]);

        // A pending group the node lost is forgotten.
        snaps[1].current_leader = Some(3);
        assert!(shed.plan(&snaps, |_, _| true).is_empty());
        assert!(shed.pending.is_empty());
    }
}
