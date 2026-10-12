use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use tokio::time::Instant;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_raft::RaftGroupMetricsSnapshot;
use ursula_raft::SnapshotBuildCoordinator;
use ursula_raft::snapshot_cadence::DriveLimits;
use ursula_raft::snapshot_cadence::GroupDriveState;
use ursula_raft::snapshot_cadence::GroupLogProgress;
use ursula_raft::snapshot_cadence::LogPressure;
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
/// Longest a handed-off permit waits for the triggered group to claim it.
const SNAPSHOT_HANDOFF_WAIT: Duration = Duration::from_secs(5);
/// Pause between ticks while the node is over its log watermark, so a
/// pressure pass is not capped at its group count per interval.
const SNAPSHOT_PRESSURE_RETICK: Duration = Duration::from_millis(250);
/// How often the node's unsnapshotted log is checked against its hard limit.
const LOG_PRESSURE_POLL: Duration = Duration::from_millis(100);

/// What became of one snapshot trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Trigger {
    /// The group claimed the handed-off permit, so its build runs.
    Started,
    /// The group is gone, the trigger failed, or OpenRaft dropped it.
    NotStarted,
    /// No build slot came free in time: the pass ends.
    NoSlot,
}

/// Trigger one group's snapshot and see the build through to its start.
///
/// Builds share the node-wide build permits, and a triggered build that
/// finds none free is refused rather than queued. Triggering a whole plan at
/// once therefore built about one group per tick while the rest were
/// refused, and under sustained writes the Raft log outgrew the node. The
/// driver takes a permit first, once fewer than `builds` builds run (which
/// waits for one to finish), and hands it to the group it triggers.
async fn trigger_snapshot_build(
    registry: &RaftGroupHandleRegistry,
    coordinator: &SnapshotBuildCoordinator,
    raft_group_id: u32,
    builds: usize,
) -> Trigger {
    let Some(raft) = registry.get(RaftGroupId(raft_group_id)) else {
        return Trigger::NotStarted;
    };
    let Ok(Ok(permit)) =
        tokio::time::timeout(SNAPSHOT_PERMIT_WAIT, coordinator.acquire_below(builds)).await
    else {
        tracing::warn!(
            raft_group_id,
            builds,
            "snapshot driver: no build slot within {SNAPSHOT_PERMIT_WAIT:?}; ending this tick"
        );
        return Trigger::NoSlot;
    };
    coordinator.hand_off(raft_group_id, permit);
    if let Err(err) = raft.trigger().snapshot().await {
        coordinator.reclaim_handoff(raft_group_id);
        tracing::error!("snapshot driver trigger group {raft_group_id} error: {err}");
        return Trigger::NotStarted;
    }
    let claimed = async {
        while coordinator.handoff_pending(raft_group_id) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    if tokio::time::timeout(SNAPSHOT_HANDOFF_WAIT, claimed)
        .await
        .is_err()
    {
        tracing::debug!(
            raft_group_id,
            "snapshot driver: handoff not claimed in time"
        );
    }
    // Unclaimed: OpenRaft dropped the trigger (e.g. a build was already running).
    if coordinator.reclaim_handoff(raft_group_id) {
        Trigger::NotStarted
    } else {
        Trigger::Started
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
/// [`SNAPSHOT_PRESSURE_RETICK`] when this one started a build or builds are in
/// flight, so neither a pass's group count nor a group that still builds holds
/// the node to one pass per interval. So does a pass that ended because the
/// log climbed to a higher pressure than it was planned for.
fn next_pause(
    interval: Duration,
    pressure: LogPressure,
    triggered: u64,
    in_flight: usize,
    outgrown: bool,
) -> Duration {
    if outgrown || (pressure.is_pressure() && (triggered > 0 || in_flight > 0)) {
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
/// such build will still free), within `limits`.
pub(crate) fn plan_snapshot_drive<'a>(
    snapshots: &'a [RaftGroupMetricsSnapshot],
    cadence: &SnapshotCadence,
    limits: DriveLimits,
    lagging: &BTreeSet<RaftGroupId>,
    building: &BTreeMap<u32, u64>,
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
            building: building.get(&snapshot.raft_group_id).copied(),
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
/// (#519). A pass ends early once the log has climbed to a higher pressure
/// than when the pass was planned, so the next pass plans for that.
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
        loop {
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
            let building = coordinator.builds_in_flight();
            let (plan, selected) =
                plan_snapshot_drive(&snaps, &cadence, limits, &lagging, &building);
            // Measured the way the pass measures it below, so only a rise
            // during the pass ends it.
            let planned_pressure =
                cadence.pressure(coordinator.node_log_bytes(), limits.max_builds);
            if should_drive_snapshots(store_trouble, plan.pressure.is_pressure()) {
                let started_at = Instant::now();
                let builds = plan.pressure.builds();
                let planned = selected.len();
                let mut triggered = 0u64;
                let mut outgrown = false;
                for snapshot in selected {
                    if cadence.pressure(coordinator.node_log_bytes(), limits.max_builds)
                        > planned_pressure
                    {
                        outgrown = true;
                        break;
                    }
                    match trigger_snapshot_build(
                        &registry,
                        &coordinator,
                        snapshot.raft_group_id,
                        builds,
                    )
                    .await
                    {
                        Trigger::Started => triggered = triggered.saturating_add(1),
                        Trigger::NotStarted => {}
                        Trigger::NoSlot => break,
                    }
                }
                pause = next_pause(interval, plan.pressure, triggered, building.len(), outgrown);
                // Counts and logs the passes that planned a build: one that
                // found nothing to start while builds run only waits for them.
                if plan.pressure.is_pressure() && (planned > 0 || outgrown) {
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
                        triggered,
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
    use std::time::Duration;

    use ursula_raft::snapshot_cadence::LogPressure;

    use super::SNAPSHOT_PRESSURE_RETICK;
    use super::next_pause;
    use super::should_drive_snapshots;

    /// #519: under pressure the driver plans again soon while its passes
    /// start builds or builds run, so a group whose build is in flight, and
    /// skipped for it, is triggered again soon after the build ends.
    #[test]
    fn a_pressure_pass_plans_again_soon_while_builds_start_or_run() {
        let interval = Duration::from_secs(5);
        let escalated = LogPressure::Escalated { builds: 2 };
        assert_eq!(
            next_pause(interval, LogPressure::Calm, 3, 2, false),
            interval
        );
        assert_eq!(
            next_pause(interval, LogPressure::Watermark, 1, 0, false),
            SNAPSHOT_PRESSURE_RETICK
        );
        assert_eq!(
            next_pause(interval, escalated, 0, 1, false),
            SNAPSHOT_PRESSURE_RETICK
        );
        // Nothing started and nothing runs: the log is at its target.
        assert_eq!(next_pause(interval, escalated, 0, 0, false), interval);
        // A pass the log outgrew goes again soon.
        assert_eq!(
            next_pause(interval, LogPressure::Calm, 0, 0, true),
            SNAPSHOT_PRESSURE_RETICK
        );
        // A shorter interval stays.
        let short = Duration::from_millis(100);
        assert_eq!(next_pause(short, escalated, 1, 0, false), short);
    }

    #[test]
    fn store_trouble_holds_back_routine_snapshots_but_not_a_pressure_pass() {
        assert!(should_drive_snapshots(false, false));
        assert!(!should_drive_snapshots(true, false));
        assert!(should_drive_snapshots(true, true));
        assert!(should_drive_snapshots(false, true));
    }
}
