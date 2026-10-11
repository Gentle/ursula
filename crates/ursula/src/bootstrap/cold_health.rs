use std::time::Duration;

use ursula_raft::LeadershipShedReason;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_runtime::ShardRuntime;

use crate::bootstrap::health_gate::CounterGrowth;
use crate::bootstrap::health_gate::HealthHysteresis;
use crate::bootstrap::health_gate::HealthTransition;
use crate::bootstrap::health_gate::LeadershipShed;
use crate::bootstrap::health_gate::TickHealth;

/// M4 — cold-health leadership gate. Two signals declare this node
/// "cold-impaired":
///
/// 1. `cold_flush_write_errors` climbed since last tick — S3 PUT or its
///    network path is failing under load.
/// 2. `cold_hot_group_bytes_max` stayed ≥ HOT_HIGH bytes — the per-group
///    hot tier isn't draining back, so we're queued up against the cap
///    and writes will reactively backpressure at the cliff.
///
/// Either condition sustained for `unhealthy_ticks` ticks triggers a soft
/// shed: the node hands off the groups it leads, a few per tick and the
/// hottest first, but it stays campaign-eligible. Cold-health is often cluster-wide during backlog;
/// disabling elections on every hot peer can freeze leadership movement. The
/// node heals when error growth stops AND hot_max drops below HOT_LOW for
/// `heal_ticks` ticks.
///
/// Why this matters: ColdBackpressure 503 is a per-write reactive gate —
/// once hot pegs at cap, the leader keeps accepting writes that immediately
/// 503, and the cold queue can only drain at the per-publish raft RTT.
/// Shedding leadership routes new client writes to a peer instead, giving
/// this node's cold queue an actual catch-up window. That's the gap that
/// dominated the long-running chaos test — many `s3_unavailable` injections
/// on the same node drove cumulative deficit because writes kept arriving.
/// Per-tick input for the M4 tracker. Both fields are cumulative counters
/// straight from `RuntimeMetricsSnapshot`; the tracker handles the delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ColdHealthSample {
    pub cold_flush_write_errors: u64,
    pub cold_hot_group_bytes_max: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ColdHealthDecision {
    NoChange,
    Shed { reason: String },
    Heal,
}

/// Pure decision core for the M4 cold-health gate, split from the spawn so
/// tests drive it with synthetic samples instead of a live runtime.
pub(crate) struct ColdHealthTracker {
    hysteresis: HealthHysteresis,
    flush_errors: CounterGrowth,
    hot_high_bytes: u64,
    hot_low_bytes: u64,
    errors_per_tick_high: u64,
}

impl ColdHealthTracker {
    pub fn new(
        unhealthy_ticks: usize,
        heal_ticks: usize,
        hot_high_bytes: u64,
        hot_low_bytes: u64,
        errors_per_tick_high: u64,
    ) -> Self {
        Self {
            hysteresis: HealthHysteresis::new(unhealthy_ticks, heal_ticks),
            flush_errors: CounterGrowth::default(),
            hot_high_bytes,
            hot_low_bytes,
            errors_per_tick_high,
        }
    }

    pub fn evaluate(&mut self, sample: ColdHealthSample) -> ColdHealthDecision {
        let delta_errors = self.flush_errors.advance(sample.cold_flush_write_errors);
        let errors = TickHealth::of_new_errors(delta_errors, self.errors_per_tick_high);
        let hot = if sample.cold_hot_group_bytes_max >= self.hot_high_bytes {
            TickHealth::Bad
        } else if sample.cold_hot_group_bytes_max <= self.hot_low_bytes {
            TickHealth::Good
        } else {
            TickHealth::Inconclusive
        };

        match self.hysteresis.observe(errors.max(hot)) {
            HealthTransition::Shed => {
                let reason = if errors == TickHealth::Bad {
                    format!(
                        "cold_flush_write_errors +{delta_errors}/tick > {}",
                        self.errors_per_tick_high
                    )
                } else {
                    format!(
                        "cold_hot_max {} ≥ HIGH {}",
                        sample.cold_hot_group_bytes_max, self.hot_high_bytes
                    )
                };
                ColdHealthDecision::Shed { reason }
            }
            HealthTransition::Heal => ColdHealthDecision::Heal,
            HealthTransition::Hold => ColdHealthDecision::NoChange,
        }
    }

    #[cfg(test)]
    pub fn yielded(&self) -> bool {
        self.hysteresis.is_shedding()
    }
}

/// Config-driven cold health gate.
pub fn spawn_cold_health_gate(
    runtime: &ShardRuntime,
    registry: &RaftGroupHandleRegistry,
    node_id: u64,
    ch_cfg: &ursula_config::ColdHealthConfig,
) {
    let interval_ms = ch_cfg.interval.as_duration().as_millis() as usize;
    if interval_ms == 0 {
        return;
    }
    let unhealthy_ticks = ch_cfg.unhealthy_ticks;
    let heal_ticks = ch_cfg.heal_ticks;
    let hot_high_bytes = ch_cfg.hot_size_high.as_bytes();
    let hot_low_bytes = ch_cfg.hot_size_low.as_bytes();
    let errors_per_tick_high = u64::try_from(ch_cfg.errors_per_tick_high).unwrap_or(1);
    let metrics = runtime.metrics();
    let registry = registry.clone();
    tokio::spawn(async move {
        let interval = Duration::from_millis(u64::try_from(interval_ms).unwrap_or(2_000));
        let mut tracker = ColdHealthTracker::new(
            unhealthy_ticks,
            heal_ticks,
            hot_high_bytes,
            hot_low_bytes,
            errors_per_tick_high,
        );
        let mut shed = LeadershipShed::new(&registry, LeadershipShedReason::ColdHealth, node_id);
        loop {
            tokio::time::sleep(interval).await;
            let snap = metrics.snapshot();
            let sample = ColdHealthSample {
                cold_flush_write_errors: snap.cold_flush_write_errors,
                cold_hot_group_bytes_max: snap.cold_hot_group_bytes_max,
            };
            match tracker.evaluate(sample) {
                ColdHealthDecision::Shed { reason } => {
                    tracing::warn!(
                        "cold-health: node {node_id} cold-impaired ({reason}); yielding leadership"
                    );
                    shed.begin();
                }
                ColdHealthDecision::Heal => shed.end(),
                ColdHealthDecision::NoChange => {}
            }
            if shed.is_draining() {
                shed.step(&snap.per_group_cold_hot_bytes).await;
            }
        }
    });
}
