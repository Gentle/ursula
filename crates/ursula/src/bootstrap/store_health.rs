//! Snapshot-store health gate: probes the external snapshot store every tick
//! and sheds the node's leadership while the store stays unreachable.

use std::time::Duration;

use tokio::sync::watch;
use ursula_raft::LeadershipShedReason;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_runtime::ShardRuntime;
use ursula_runtime::SharedSnapshotStore;

use crate::bootstrap::health_gate::CounterGrowth;
use crate::bootstrap::health_gate::HealthHysteresis;
use crate::bootstrap::health_gate::HealthTransition;
use crate::bootstrap::health_gate::LeadershipShed;
use crate::bootstrap::health_gate::TickHealth;

/// More failed cold-flush writes than this in one tick make the tick bad. A
/// single failed write is not enough on its own (#508).
const FLUSH_ERRORS_PER_TICK_HIGH: u64 = 1;

/// What one tick of the snapshot-store gate read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StoreHealthSample {
    /// Whether the store answered the health probe before its deadline.
    pub probe_answered: bool,
    /// The node's failed cold-flush writes so far. Cold flushes write to the
    /// same object store, and an idle pooled connection can answer the probe
    /// while writes on new connections fail.
    pub cold_flush_write_errors: u64,
}

/// The snapshot-store gate's verdict on one tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StoreHealthTick {
    pub health: TickHealth,
    /// Failed cold-flush writes since the previous tick.
    pub flush_errors: u64,
    pub transition: HealthTransition,
}

/// Decision core of the snapshot-store gate, free of I/O so tests drive it
/// with synthetic samples.
#[derive(Debug, Clone)]
pub(crate) struct StoreHealthTracker {
    hysteresis: HealthHysteresis,
    flush_errors: CounterGrowth,
}

impl StoreHealthTracker {
    pub(crate) fn new(unhealthy_ticks: usize, heal_ticks: usize) -> Self {
        Self {
            hysteresis: HealthHysteresis::new(unhealthy_ticks, heal_ticks),
            flush_errors: CounterGrowth::default(),
        }
    }

    pub(crate) fn evaluate(&mut self, sample: StoreHealthSample) -> StoreHealthTick {
        let flush_errors = self.flush_errors.advance(sample.cold_flush_write_errors);
        let probe = if sample.probe_answered {
            TickHealth::Good
        } else {
            TickHealth::Bad
        };
        let health = probe.max(TickHealth::of_new_errors(
            flush_errors,
            FLUSH_ERRORS_PER_TICK_HIGH,
        ));
        StoreHealthTick {
            health,
            flush_errors,
            transition: self.hysteresis.observe(health),
        }
    }
}

/// Spawns the snapshot-store gate. Every `interval` it probes the store
/// within `probe_timeout`, which by default leaves the S3 client time to
/// retry once, and counts the node's failed cold-flush writes. After
/// `unhealthy_ticks` bad ticks the node stops campaigning and hands its
/// leaderships off a few per tick. After `heal_ticks` good ticks in a row it
/// may lead again.
///
/// Returns whether the latest tick was bad. The snapshot driver holds
/// routine snapshots back while it is.
pub(crate) fn spawn_store_health_gate(
    runtime: &ShardRuntime,
    registry: &RaftGroupHandleRegistry,
    snapshot_store: SharedSnapshotStore,
    node_id: u64,
    s3_cfg: &ursula_config::S3Config,
    interval: Duration,
) -> watch::Receiver<bool> {
    let probe_timeout = s3_cfg.probe_timeout.as_duration();
    let unhealthy_ticks = s3_cfg.unhealthy_ticks;
    let mut tracker = StoreHealthTracker::new(unhealthy_ticks, s3_cfg.heal_ticks);
    let metrics = runtime.metrics();
    let registry = registry.clone();
    let (latest_bad, store_trouble) = watch::channel(false);
    tokio::spawn(async move {
        let mut shed = LeadershipShed::new(LeadershipShedReason::SnapshotDriverS3, node_id);
        loop {
            let tick_started = tokio::time::Instant::now();
            let probe = tokio::time::timeout(probe_timeout, snapshot_store.health_check()).await;
            let probe_answered = match probe {
                Ok(Ok(())) => true,
                Ok(Err(err)) => {
                    tracing::warn!(node_id, %err, "snapshot store health probe failed");
                    false
                }
                Err(_elapsed) => {
                    tracing::warn!(
                        node_id,
                        ?probe_timeout,
                        "snapshot store health probe timed out"
                    );
                    false
                }
            };
            let tick = tracker.evaluate(StoreHealthSample {
                probe_answered,
                cold_flush_write_errors: metrics.snapshot().cold_flush_write_errors,
            });
            latest_bad.send_replace(tick.health == TickHealth::Bad);
            match tick.transition {
                HealthTransition::Shed => {
                    tracing::warn!(
                        node_id,
                        unhealthy_ticks,
                        probe_answered,
                        flush_errors = tick.flush_errors,
                        "snapshot store unhealthy; shedding leadership"
                    );
                    shed.begin(&registry);
                }
                HealthTransition::Heal => {
                    tracing::info!(node_id, "snapshot store healthy again");
                    shed.end(&registry);
                }
                HealthTransition::Hold => {}
            }
            shed.step(&registry).await;
            tokio::time::sleep(interval.saturating_sub(tick_started.elapsed())).await;
        }
    });
    store_trouble
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use axum::body::Bytes;
    use proptest::prelude::ProptestConfig;
    use proptest::prelude::prop;
    use proptest::prop_assert_eq;
    use proptest::proptest;
    use ursula_raft::LeadershipShedReason;
    use ursula_raft::RaftGroupHandleRegistry;
    use ursula_raft::snapshot_cadence::SnapshotCadence;
    use ursula_runtime::RuntimeConfig;
    use ursula_runtime::ShardRuntime;
    use ursula_runtime::SnapshotKey;
    use ursula_runtime::SnapshotLocation;
    use ursula_runtime::SnapshotStore;
    use ursula_runtime::SnapshotStoreError;
    use ursula_runtime::SnapshotStoreFuture;

    use super::StoreHealthSample;
    use super::StoreHealthTracker;
    use crate::bootstrap::health_gate::HealthTransition;
    use crate::bootstrap::health_gate::TickHealth;

    /// `S3Config` defaults: three bad ticks shed, two good ticks heal.
    fn default_tracker() -> StoreHealthTracker {
        let defaults = ursula_config::S3Config::default();
        StoreHealthTracker::new(defaults.unhealthy_ticks, defaults.heal_ticks)
    }

    fn probe(answered: bool, cold_flush_write_errors: u64) -> StoreHealthSample {
        StoreHealthSample {
            probe_answered: answered,
            cold_flush_write_errors,
        }
    }

    fn run(
        tracker: &mut StoreHealthTracker,
        samples: &[StoreHealthSample],
    ) -> Vec<HealthTransition> {
        samples
            .iter()
            .map(|sample| tracker.evaluate(*sample).transition)
            .collect()
    }

    #[test]
    fn one_failed_probe_does_not_shed() {
        let mut tracker = default_tracker();
        let transitions = run(&mut tracker, &[
            probe(true, 0),
            probe(false, 0),
            probe(true, 0),
            probe(false, 0),
            probe(false, 0),
            probe(true, 0),
        ]);
        assert!(
            transitions.iter().all(|t| *t == HealthTransition::Hold),
            "{transitions:?}"
        );
    }

    #[test]
    fn one_failed_flush_write_per_tick_is_not_a_bad_tick() {
        let mut tracker = default_tracker();
        let baseline = tracker.evaluate(probe(true, 40));
        assert_eq!(baseline.health, TickHealth::Good, "errors before the gate");
        for errors in 41..60 {
            let tick = tracker.evaluate(probe(true, errors));
            assert_eq!(tick.flush_errors, 1);
            assert_eq!(tick.health, TickHealth::Inconclusive);
            assert_eq!(tick.transition, HealthTransition::Hold);
        }
    }

    #[test]
    fn sustained_probe_failures_shed_and_good_probes_heal() {
        let mut tracker = default_tracker();
        assert_eq!(
            run(&mut tracker, &[
                probe(false, 0),
                probe(false, 0),
                probe(false, 0),
            ]),
            vec![
                HealthTransition::Hold,
                HealthTransition::Hold,
                HealthTransition::Shed
            ]
        );
        assert_eq!(
            run(&mut tracker, &[
                probe(false, 0),
                probe(true, 0),
                probe(true, 0),
                probe(true, 0),
            ]),
            vec![
                HealthTransition::Hold,
                HealthTransition::Hold,
                HealthTransition::Heal,
                HealthTransition::Hold
            ]
        );
    }

    #[test]
    fn sustained_flush_write_failures_shed_although_the_probe_answers() {
        let mut tracker = default_tracker();
        assert_eq!(
            run(&mut tracker, &[
                probe(true, 0),
                probe(true, 2),
                probe(true, 3),
                probe(true, 7),
                probe(true, 10),
            ]),
            vec![
                HealthTransition::Hold,
                HealthTransition::Hold,
                HealthTransition::Hold,
                HealthTransition::Hold,
                HealthTransition::Shed
            ],
            "+2 and +4 are bad, +1 neither ends nor extends the streak, +3 is the third bad tick"
        );
        // While leaderships are still being handed off, their flushes can
        // keep failing: a tick with a failed write is no good tick, so the
        // node does not heal until two clean ticks in a row.
        assert_eq!(
            run(&mut tracker, &[
                probe(true, 11),
                probe(true, 11),
                probe(true, 12),
                probe(true, 12),
                probe(true, 12),
            ]),
            vec![
                HealthTransition::Hold,
                HealthTransition::Hold,
                HealthTransition::Hold,
                HealthTransition::Hold,
                HealthTransition::Heal
            ]
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// Against the gate's definition: a tick is bad when the probe went
        /// unanswered or more than one cold-flush write failed, and the node
        /// sheds once the ticks since the last good one hold
        /// `unhealthy_ticks` bad ones.
        #[test]
        fn sheds_only_after_unhealthy_ticks_bad_ticks(
            base in 0u64..1_000,
            ticks in prop::collection::vec((prop::bool::weighted(0.7), 0u64..4), 1..48),
            unhealthy_ticks in 1usize..5,
        ) {
            let mut tracker = StoreHealthTracker::new(unhealthy_ticks, 2);
            let mut total = base;
            let mut bad_since_good = 0_usize;
            let mut shedding = false;
            for (index, (answered, new_errors)) in ticks.into_iter().enumerate() {
                total = total.saturating_add(new_errors);
                let tick = tracker.evaluate(probe(answered, total));
                let counted = if index == 0 { 0 } else { new_errors };
                prop_assert_eq!(tick.flush_errors, counted);
                let bad = !answered || counted > 1;
                prop_assert_eq!(tick.health == TickHealth::Bad, bad);
                if shedding {
                    if tick.transition == HealthTransition::Heal {
                        shedding = false;
                        bad_since_good = 0;
                    }
                    continue;
                }
                if bad {
                    bad_since_good = bad_since_good.saturating_add(1);
                } else if counted == 0 {
                    bad_since_good = 0;
                }
                let sheds = bad_since_good >= unhealthy_ticks;
                prop_assert_eq!(tick.transition == HealthTransition::Shed, sheds);
                shedding = sheds;
            }
        }
    }

    fn shedding_for_the_store(registry: &RaftGroupHandleRegistry) -> bool {
        registry
            .leadership_shed_state()
            .contains(LeadershipShedReason::SnapshotDriverS3.into())
    }

    /// A snapshot store whose health probes follow a script. Each probe
    /// records whether the node was shedding for the snapshot store when the
    /// probe ran.
    #[derive(Debug)]
    struct ScriptedProbeStore {
        registry: RaftGroupHandleRegistry,
        failing: BTreeSet<usize>,
        latency: Duration,
        shedding_at_probe: Mutex<Vec<bool>>,
    }

    impl ScriptedProbeStore {
        fn new(
            registry: &RaftGroupHandleRegistry,
            failing: impl IntoIterator<Item = usize>,
        ) -> Self {
            Self {
                registry: registry.clone(),
                failing: failing.into_iter().collect(),
                latency: Duration::ZERO,
                shedding_at_probe: Mutex::new(Vec::new()),
            }
        }

        fn shedding_at_probe(&self) -> Vec<bool> {
            self.shedding_at_probe.lock().unwrap().clone()
        }
    }

    impl SnapshotStore for ScriptedProbeStore {
        fn upload<'a>(
            &'a self,
            _key: SnapshotKey,
            _bytes: Bytes,
        ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
            Box::pin(async { Err(SnapshotStoreError::Backend("probe-only store".into())) })
        }

        fn download<'a>(
            &'a self,
            _location: &'a SnapshotLocation,
        ) -> SnapshotStoreFuture<'a, Vec<u8>> {
            Box::pin(async { Err(SnapshotStoreError::Backend("probe-only store".into())) })
        }

        fn delete<'a>(&'a self, _location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }

        fn health_check(&self) -> SnapshotStoreFuture<'_, ()> {
            let mut seen = self.shedding_at_probe.lock().unwrap();
            let index = seen.len();
            seen.push(shedding_for_the_store(&self.registry));
            let fails = self.failing.contains(&index);
            let latency = self.latency;
            Box::pin(async move {
                tokio::time::sleep(latency).await;
                if fails {
                    Err(SnapshotStoreError::Backend("scripted probe failure".into()))
                } else {
                    Ok(())
                }
            })
        }
    }

    /// Runs the snapshot driver, with the default `[storage.cold.s3]` health
    /// settings, against `store` until it has been probed `probes` times.
    async fn drive(
        store: Arc<ScriptedProbeStore>,
        registry: &RaftGroupHandleRegistry,
        probes: usize,
    ) -> Vec<bool> {
        let runtime = ShardRuntime::spawn(RuntimeConfig::new(1, 1)).expect("runtime");
        crate::bootstrap::snapshot::spawn_snapshot_driver(
            &runtime,
            registry,
            1,
            Some(store.clone()),
            None,
            5_000,
            SnapshotCadence::new(1 << 30, 1, 100_000),
            None,
            16,
        );
        while store.shedding_at_probe().len() < probes {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        store.shedding_at_probe()
    }

    #[tokio::test(start_paused = true)]
    async fn the_driver_rides_out_a_single_failed_probe() {
        let registry = RaftGroupHandleRegistry::default();
        let store = Arc::new(ScriptedProbeStore::new(&registry, [1]));
        assert_eq!(drive(store, &registry, 8).await, vec![false; 8]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_probe_answered_after_a_client_retry_is_healthy() {
        // The S3 client's first retry comes 1 to 2 seconds after a failed
        // attempt: a probe that answers 3 seconds in is the store answering.
        let registry = RaftGroupHandleRegistry::default();
        let mut store = ScriptedProbeStore::new(&registry, []);
        store.latency = Duration::from_secs(3);
        assert_eq!(drive(Arc::new(store), &registry, 6).await, vec![false; 6]);
    }

    #[tokio::test(start_paused = true)]
    async fn the_driver_sheds_on_sustained_probe_failures_and_heals() {
        let registry = RaftGroupHandleRegistry::default();
        let store = Arc::new(ScriptedProbeStore::new(&registry, [1, 2, 3]));
        assert_eq!(drive(store, &registry, 8).await, vec![
            false, false, false, false, true, true, false, false
        ]);
        assert!(!shedding_for_the_store(&registry));
    }
}
