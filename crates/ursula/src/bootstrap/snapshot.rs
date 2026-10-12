use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use tokio::time::Instant;
use ursula_raft::Handoff;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_raft::RaftGroupMetricsSnapshot;
use ursula_raft::SNAPSHOT_HANDOFF_WAIT;
use ursula_raft::SnapshotBuildCoordinator;
use ursula_raft::snapshot_cadence::DriveLimits;
use ursula_raft::snapshot_cadence::GroupDriveState;
use ursula_raft::snapshot_cadence::GroupLogProgress;
use ursula_raft::snapshot_cadence::LogPressure;
use ursula_raft::snapshot_cadence::PassStep;
use ursula_raft::snapshot_cadence::SnapshotCadence;
use ursula_raft::snapshot_cadence::SnapshotPlan;
use ursula_runtime::RaftLogPressureSample;
use ursula_runtime::RuntimeMetrics;
use ursula_runtime::ShardRuntime;
use ursula_runtime::SharedSnapshotStore;
use ursula_shard::RaftGroupId;

use crate::bootstrap::participation::PeerParticipation;
use crate::bootstrap::store_health::StoreGateConfig;
use crate::bootstrap::store_health::spawn_store_health_gate;

/// Longest the driver waits for a build slot (a build in flight to finish)
/// before it ends the tick.
const SNAPSHOT_PERMIT_WAIT: Duration = Duration::from_secs(60);
/// Pause between ticks while the node is over its log watermark, so a
/// pressure pass is not capped at its group count per interval.
const SNAPSHOT_PRESSURE_RETICK: Duration = Duration::from_millis(250);
/// How often the node's unsnapshotted log is checked against its hard limit,
/// and weighed again by a pass that waits for a build slot.
const LOG_PRESSURE_POLL: Duration = Duration::from_millis(100);
/// Passes a group is left out of after a trigger of it went unclaimed
/// ([`HeldGroups`]), doubled for each such trigger in a row.
const FIRST_HOLD_PASSES: u32 = 2;
/// The most passes a group is left out of after triggers of it went
/// unclaimed ([`HeldGroups`]).
const MAX_HOLD_PASSES: u32 = 32;

/// What became of one snapshot trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Trigger {
    /// The group claimed the handed-off permit, so its build runs.
    Started,
    /// The group is gone, the trigger failed, or a permit handed to the
    /// group earlier is still pending (an operator's snapshot of it starts).
    NotStarted,
    /// The group did not claim the handed-off permit in time: OpenRaft
    /// dropped the trigger, or the build waits behind the group's applies.
    /// The pass goes on, and leaves the group out of the next passes
    /// ([`HeldGroups`]).
    Unclaimed,
    /// No build slot came free in time: the pass ends.
    NoSlot,
    /// The log climbed past the pressure the pass was planned for: the pass
    /// ends, so the next one plans for the pressure now.
    Outgrown,
}

/// Trigger one group's snapshot and see the build through to its start.
///
/// Builds share the node-wide build permits, and a triggered build that
/// finds none free is refused rather than queued. Triggering a whole plan at
/// once therefore built about one group per tick while the rest were
/// refused, and under sustained writes the Raft log outgrew the node. The
/// driver takes a permit first and hands it to the group it triggers. It
/// takes one once fewer builds run than `pace` asks for, and asks again
/// whenever a build starts or ends and every [`LOG_PRESSURE_POLL`], so the
/// pass follows the node's log while it waits (#519). `pace` returns `None`
/// once the log outgrew the pass.
async fn trigger_snapshot_build(
    registry: &RaftGroupHandleRegistry,
    coordinator: &SnapshotBuildCoordinator,
    raft_group_id: u32,
    pace: impl FnMut() -> Option<usize>,
) -> Trigger {
    let Some(raft) = registry.get(RaftGroupId(raft_group_id)) else {
        return Trigger::NotStarted;
    };
    let slot = coordinator.acquire_below(LOG_PRESSURE_POLL, pace);
    let permit = match tokio::time::timeout(SNAPSHOT_PERMIT_WAIT, slot).await {
        Ok(Ok(Some(permit))) => permit,
        Ok(Ok(None)) => return Trigger::Outgrown,
        Ok(Err(err)) => {
            tracing::error!(
                raft_group_id,
                %err,
                "snapshot driver: no build permit; ending this tick"
            );
            return Trigger::NoSlot;
        }
        Err(_elapsed) => {
            tracing::warn!(
                raft_group_id,
                "snapshot driver: no build slot within {SNAPSHOT_PERMIT_WAIT:?}; ending this tick"
            );
            return Trigger::NoSlot;
        }
    };
    match coordinator
        .hand_off_and_trigger(
            raft_group_id,
            permit,
            SNAPSHOT_HANDOFF_WAIT,
            raft.trigger().snapshot(),
        )
        .await
    {
        Handoff::Claimed => Trigger::Started,
        Handoff::Unclaimed => {
            // Each such trigger holds the pass up for the whole wait.
            tracing::warn!(
                raft_group_id,
                "snapshot driver: the group did not claim its build permit within \
                 {SNAPSHOT_HANDOFF_WAIT:?}; taking it back"
            );
            Trigger::Unclaimed
        }
        Handoff::TriggerFailed(err) => {
            tracing::error!("snapshot driver trigger group {raft_group_id} error: {err}");
            Trigger::NotStarted
        }
        Handoff::Busy => Trigger::NotStarted,
    }
}

/// The groups the snapshot driver leaves out of its next passes because a
/// trigger of theirs went unclaimed.
///
/// OpenRaft drops a trigger while it counts a build of the group as running,
/// which it does until RaftCore handles the build's end, and that can come
/// after the build released its permit. A pass planned in between can draw
/// such a trigger, and each holds the pass up for [`SNAPSHOT_HANDOFF_WAIT`]
/// with its build slot idle. So a group whose trigger went unclaimed is left out of the next
/// [`FIRST_HOLD_PASSES`] passes, and of twice as many after each such trigger
/// in a row, up to [`MAX_HOLD_PASSES`], so that a group whose triggers keep
/// going unclaimed does not head every pass. A claimed trigger clears it.
#[derive(Debug, Default)]
struct HeldGroups {
    groups: BTreeMap<u32, Hold>,
}

#[derive(Debug, Clone, Copy)]
struct Hold {
    /// Passes the group is still left out of.
    passes: u32,
    /// Passes its next unclaimed trigger leaves it out of.
    next: u32,
}

impl HeldGroups {
    /// The groups this pass leaves out, which it counts.
    fn pass(&mut self) -> BTreeSet<u32> {
        let held = self
            .groups
            .iter()
            .filter(|(_, hold)| hold.passes > 0)
            .map(|(group, _)| *group)
            .collect();
        for hold in self.groups.values_mut() {
            hold.passes = hold.passes.saturating_sub(1);
        }
        held
    }

    /// Records what became of a trigger of `raft_group_id`.
    fn triggered(&mut self, raft_group_id: u32, trigger: Trigger) {
        match trigger {
            Trigger::Started => {
                self.groups.remove(&raft_group_id);
            }
            Trigger::Unclaimed => {
                let hold = self.groups.entry(raft_group_id).or_insert(Hold {
                    passes: 0,
                    next: FIRST_HOLD_PASSES,
                });
                hold.passes = hold.next;
                hold.next = hold.next.saturating_mul(2).min(MAX_HOLD_PASSES);
            }
            Trigger::NotStarted | Trigger::NoSlot | Trigger::Outgrown => {}
        }
    }
}

/// Whether this tick triggers snapshot builds.
///
/// While the store health gate's latest tick was bad (the probe failed, or a
/// cold-flush write failed within the gate's flush window and none succeeded
/// since), routine snapshots are held back so the driver does not push
/// uploads into a failing store. Not once the log is over the pressure
/// watermark: a build whose upload, verification or reference publication
/// fails falls back to an inline snapshot, which still truncates the log, so
/// nothing a snapshot needs depends on the store. Holding builds back there
/// would grow the log into the node-wide 503 write gate while the store is
/// merely throttling.
fn should_drive_snapshots(store_trouble: bool, log_pressure: bool) -> bool {
    !store_trouble || log_pressure
}

/// The pause before the snapshot driver's next pass. While the node's log is
/// over its pressure watermark, the next pass comes after
/// [`SNAPSHOT_PRESSURE_RETICK`] when this one started a build, builds are in
/// flight or groups are held ([`HeldGroups`]), so neither a pass's group
/// count, a group that still builds nor a held group, whose hold counts
/// passes, holds the node to one pass per interval. So does a pass that
/// ended because the log climbed to a higher pressure than it was planned
/// for.
fn next_pause(
    interval: Duration,
    pressure: LogPressure,
    triggered: u64,
    in_flight: usize,
    held: usize,
    outgrown: bool,
) -> Duration {
    if outgrown || (pressure.is_pressure() && (triggered > 0 || in_flight > 0 || held > 0)) {
        interval.min(SNAPSHOT_PRESSURE_RETICK)
    } else {
        interval
    }
}

/// Keeps the coordinator's log-pressure flag (read by HTTP admission) in step
/// with the node's unsnapshotted Raft log bytes, and reports the pressure
/// the snapshot driver works under.
fn spawn_log_pressure_monitor(
    coordinator: SnapshotBuildCoordinator,
    cadence: SnapshotCadence,
    max_builds: usize,
    metrics: RuntimeMetrics,
) {
    let limit = cadence.hard_limit_bytes();
    let resume = cadence.resume_bytes();
    tokio::spawn(async move {
        loop {
            let log_bytes = coordinator.node_log_bytes();
            let pressure = cadence.pressure(log_bytes, max_builds);
            metrics.record_raft_log_pressure(RaftLogPressureSample {
                log_bytes,
                level: pressure.level(),
                builds: u64::try_from(pressure.builds()).unwrap_or(u64::MAX),
                builds_in_flight: u64::try_from(coordinator.running_builds()).unwrap_or(u64::MAX),
            });
            match coordinator.observe_log_bytes(log_bytes, limit, resume) {
                Some(true) => tracing::warn!(
                    log_bytes,
                    limit,
                    "raft log over its hard limit: refusing client writes until snapshots catch up"
                ),
                Some(false) => tracing::info!(log_bytes, resume, "raft log pressure cleared"),
                None => {}
            }
            tokio::time::sleep(LOG_PRESSURE_POLL).await;
        }
    });
}

/// Default driver interval: 5 s with an external snapshot store, whose
/// health gate ticks on the same interval, and 1 s inline. `Some(0)`
/// disables the driver and the store health gate, and OpenRaft's entry-count
/// policy runs instead.
pub(crate) fn resolve_snapshot_drive_interval_ms(
    configured: Option<usize>,
    snapshot_store_configured: bool,
) -> usize {
    configured.unwrap_or(if snapshot_store_configured {
        5_000
    } else {
        1_000
    })
}

/// One group's log progress as the driver sees it (F12e): the state
/// machine's log gauge, plus OpenRaft's view of whether a snapshot exists.
/// A group with no applied state reports no log, so it is never due: an
/// empty in-memory node must not publish a `group-X-empty` snapshot.
pub(crate) fn group_log_progress(snapshot: &RaftGroupMetricsSnapshot) -> GroupLogProgress {
    if snapshot.last_applied.is_none() {
        return GroupLogProgress::default();
    }
    let mut progress = snapshot.log;
    progress.has_snapshot |= snapshot.snapshot.is_some();
    progress
}

/// The groups to snapshot this pass under the byte-based cadence (F12e) and
/// the node's log pressure (#519): the groups the Raft WAL reports
/// `lagging` first, never a group with a build in `building` (the log each
/// such build will still free) or one the driver `held` out of the pass
/// ([`HeldGroups`], counted as building with nothing to free), within
/// `limits`.
pub(crate) fn plan_snapshot_drive<'a>(
    snapshots: &'a [RaftGroupMetricsSnapshot],
    cadence: &SnapshotCadence,
    limits: DriveLimits,
    lagging: &BTreeSet<RaftGroupId>,
    building: &BTreeMap<u32, u64>,
    held: &BTreeSet<u32>,
) -> (SnapshotPlan, Vec<&'a RaftGroupMetricsSnapshot>) {
    let groups = snapshots
        .iter()
        .map(|snapshot| GroupDriveState {
            progress: if snapshot.apply_failure.is_some() {
                GroupLogProgress::default()
            } else {
                group_log_progress(snapshot)
            },
            lagging: lagging.contains(&RaftGroupId(snapshot.raft_group_id)),
            building: building
                .get(&snapshot.raft_group_id)
                .copied()
                .or_else(|| held.contains(&snapshot.raft_group_id).then_some(0)),
        })
        .collect::<Vec<_>>();
    let plan = cadence.plan_pass(&groups, limits);
    let selected = plan
        .groups
        .iter()
        .filter_map(|index| snapshots.get(*index))
        .collect();
    (plan, selected)
}

/// What the snapshot driver hands its snapshot-store health gate.
#[derive(Debug, Clone, Copy)]
pub(crate) struct StoreGateInputs<'a> {
    /// `[storage.cold.s3]`, or `None` for its defaults.
    pub s3: Option<&'a ursula_config::S3Config>,
    /// `storage.cold.flush_interval`, the flush worker's pause between
    /// passes, which the gate's flush window spans.
    pub cold_flush_interval: Duration,
    /// The node's peers, which take over the leaderships the gate sheds.
    pub participation: &'a PeerParticipation,
}

/// Config-driven snapshot driver. Reads parameters from typed config.
///
/// With an external snapshot store it also spawns the store health gate
/// ([`spawn_store_health_gate`]) on the same interval, and owns it: the gate
/// stops when the driver does. Only an external store is probed: inline
/// snapshots depend on nothing outside the node, so the inline driver never
/// sheds leadership. A drive interval of 0 turns both off.
///
/// Each tick plans one pass ([`plan_snapshot_drive`]) and starts its builds
/// in order, at most as many at once as the node's log pressure calls for
/// now (#519, [`SnapshotCadence::pass_step`]): fewer as the pass brings the
/// log down, one below the budget. A pass ends early once the log has
/// climbed to a higher pressure than when the pass was planned, so the next
/// pass plans for that.
pub fn spawn_snapshot_driver(
    runtime: &ShardRuntime,
    registry: &RaftGroupHandleRegistry,
    snapshot_store: Option<SharedSnapshotStore>,
    store_gate: StoreGateInputs<'_>,
    interval_ms: usize,
    cadence: SnapshotCadence,
    wal_lagging: Option<std::sync::Arc<ursula_raft::LaggingGroups>>,
    max_groups_per_tick: usize,
) {
    if interval_ms == 0 {
        return;
    }
    let interval = Duration::from_millis(u64::try_from(interval_ms).unwrap_or(u64::MAX));
    let mut gate = snapshot_store.map(|store| {
        let default_s3 = ursula_config::S3Config::default();
        spawn_store_health_gate(
            runtime,
            registry,
            store,
            store_gate.participation.clone(),
            StoreGateConfig::new(
                store_gate.s3.unwrap_or(&default_s3),
                store_gate.cold_flush_interval,
                interval,
            ),
        )
    });
    let runtime = runtime.clone();
    let registry = registry.clone();
    let coordinator = registry.snapshot_build_coordinator();
    let limits = DriveLimits {
        max_groups: max_groups_per_tick,
        max_builds: coordinator.max_concurrency(),
    };
    spawn_log_pressure_monitor(
        coordinator.clone(),
        cadence,
        limits.max_builds,
        runtime.metrics(),
    );
    tokio::spawn(async move {
        let mut held_groups = HeldGroups::default();
        loop {
            // The builds first: a group whose build ends after this read
            // still counts as in flight, and one whose build ended before it
            // shows, in the metrics read next, the log after the build freed
            // it, never the log before. OpenRaft keeps counting a build until
            // RaftCore handles its end, which can come after the build
            // released its permit, so a trigger in between still goes
            // unclaimed: its group then sits out the next passes
            // (`HeldGroups`).
            let building = coordinator.builds_in_flight();
            let held = held_groups.pass();
            let snaps = registry.metrics_snapshot();
            let store_trouble = match gate.as_mut().map(|gate| gate.latest_bad()) {
                None => false,
                Some(Ok(latest_bad)) => latest_bad,
                Some(Err(err)) => {
                    tracing::error!(
                        %err,
                        "snapshot driver: the store is no longer judged; driving routine \
                         snapshots as if it were healthy"
                    );
                    gate = None;
                    false
                }
            };
            let mut pause = interval;
            let lagging = wal_lagging
                .as_ref()
                .map(|lagging| lagging.groups())
                .unwrap_or_default();
            if !lagging.is_empty() {
                tracing::info!(
                    groups = ?lagging,
                    "snapshot driver: the Raft WAL reports groups keeping old segments"
                );
            }
            let (plan, selected) =
                plan_snapshot_drive(&snaps, &cadence, limits, &lagging, &building, &held);
            // Measured the way the pass measures it below, so only a rise
            // during the pass ends it.
            let planned_pressure =
                cadence.pressure(coordinator.node_log_bytes(), limits.max_builds);
            if should_drive_snapshots(store_trouble, plan.pressure.is_pressure()) {
                let started_at = Instant::now();
                let builds = plan.pressure.builds();
                let planned = selected.len();
                let mut triggered = 0u64;
                let mut not_started = 0u64;
                let mut unclaimed = 0u64;
                let mut outgrown = false;
                // The builds the pass keeps in flight as the log moves, or
                // none once the log outgrew the pass.
                let pace = || match cadence.pass_step(
                    planned_pressure,
                    coordinator.node_log_bytes(),
                    limits.max_builds,
                ) {
                    PassStep::Outgrown => None,
                    PassStep::Start { builds: now } => Some(now),
                };
                for snapshot in selected {
                    let trigger = trigger_snapshot_build(
                        &registry,
                        &coordinator,
                        snapshot.raft_group_id,
                        pace,
                    )
                    .await;
                    held_groups.triggered(snapshot.raft_group_id, trigger);
                    match trigger {
                        Trigger::Started => triggered = triggered.saturating_add(1),
                        Trigger::NotStarted => not_started = not_started.saturating_add(1),
                        Trigger::Unclaimed => unclaimed = unclaimed.saturating_add(1),
                        Trigger::NoSlot => break,
                        Trigger::Outgrown => {
                            outgrown = true;
                            break;
                        }
                    }
                }
                pause = next_pause(
                    interval,
                    plan.pressure,
                    triggered,
                    building.len(),
                    held.len(),
                    outgrown,
                );
                // Counts and logs the passes that planned a build or held
                // groups out: one that found nothing to start while builds
                // run only waits for them.
                if plan.pressure.is_pressure() && (planned > 0 || outgrown || !held.is_empty()) {
                    let elapsed = started_at.elapsed();
                    runtime.metrics().record_raft_snapshot_pressure(
                        triggered,
                        u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
                    );
                    tracing::info!(
                        node_log_bytes = plan.node_log_bytes,
                        node_log_budget = cadence.node_budget_bytes,
                        level = plan.pressure.level(),
                        builds,
                        building = building.len(),
                        held = held.len(),
                        planned,
                        triggered,
                        not_started,
                        unclaimed,
                        outgrown,
                        pass_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
                        "raft snapshot pressure pass completed"
                    );
                }
            }

            // A change in the WAL's lagging groups starts the next tick
            // early, so they are snapshotted and purged sooner.
            match &wal_lagging {
                Some(lagging) => {
                    tokio::select! {
                        biased;
                        () = tokio::time::sleep(pause) => {}
                        () = lagging.changed() => {}
                    }
                }
                None => tokio::time::sleep(pause).await,
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use ursula_raft::snapshot_cadence::LogPressure;

    use super::HeldGroups;
    use super::SNAPSHOT_PRESSURE_RETICK;
    use super::Trigger;
    use super::next_pause;
    use super::should_drive_snapshots;

    /// #519: under pressure the driver plans again soon while its passes
    /// start builds, builds run or groups are held, so a group whose build
    /// is in flight, and skipped for it, is triggered again soon after the
    /// build ends, and a held group's hold, which counts passes, ends soon.
    #[test]
    fn a_pressure_pass_plans_again_soon_while_builds_start_or_run() {
        let interval = Duration::from_secs(5);
        let escalated = LogPressure::Escalated { builds: 2 };
        assert_eq!(
            next_pause(interval, LogPressure::Calm, 3, 2, 1, false),
            interval
        );
        assert_eq!(
            next_pause(interval, LogPressure::Watermark, 1, 0, 0, false),
            SNAPSHOT_PRESSURE_RETICK
        );
        assert_eq!(
            next_pause(interval, escalated, 0, 1, 0, false),
            SNAPSHOT_PRESSURE_RETICK
        );
        // Nothing started and nothing runs: the log is at its target.
        assert_eq!(next_pause(interval, escalated, 0, 0, 0, false), interval);
        // A held group's hold counts passes, so they come soon under pressure.
        assert_eq!(
            next_pause(interval, escalated, 0, 0, 1, false),
            SNAPSHOT_PRESSURE_RETICK
        );
        // A pass the log outgrew goes again soon.
        assert_eq!(
            next_pause(interval, LogPressure::Calm, 0, 0, 0, true),
            SNAPSHOT_PRESSURE_RETICK
        );
        // A shorter interval stays.
        let short = Duration::from_millis(100);
        assert_eq!(next_pause(short, escalated, 1, 0, 0, false), short);
    }

    /// A group whose trigger went unclaimed sits out the next two passes,
    /// twice as many after each such trigger in a row, at most 32, until a
    /// trigger of it is claimed.
    #[test]
    fn a_group_whose_trigger_went_unclaimed_sits_out_the_next_passes() {
        let mut held = HeldGroups::default();
        let sits_out =
            |held: &mut HeldGroups| (0..64).take_while(|_| held.pass().contains(&7)).count();
        assert!(held.pass().is_empty());
        held.triggered(7, Trigger::Unclaimed);
        assert_eq!(sits_out(&mut held), 2);
        held.triggered(7, Trigger::Unclaimed);
        assert_eq!(sits_out(&mut held), 4);
        for _ in 0..5 {
            held.triggered(7, Trigger::Unclaimed);
        }
        assert_eq!(sits_out(&mut held), 32);
        // A claimed trigger clears it.
        held.triggered(7, Trigger::Started);
        held.triggered(7, Trigger::Unclaimed);
        assert_eq!(sits_out(&mut held), 2);
        // Other outcomes leave it as it was.
        held.triggered(7, Trigger::NotStarted);
        held.triggered(7, Trigger::NoSlot);
        held.triggered(7, Trigger::Outgrown);
        held.triggered(7, Trigger::Unclaimed);
        assert_eq!(sits_out(&mut held), 4);
        // Each group is held on its own.
        held.triggered(8, Trigger::Unclaimed);
        assert_eq!(held.pass(), BTreeSet::from([8]));
    }

    #[test]
    fn store_trouble_holds_back_routine_snapshots_but_not_a_pressure_pass() {
        assert!(should_drive_snapshots(false, false));
        assert!(!should_drive_snapshots(true, false));
        assert!(should_drive_snapshots(true, true));
        assert!(should_drive_snapshots(false, true));
    }
}
