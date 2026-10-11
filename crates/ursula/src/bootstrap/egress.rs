use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use ursula_raft::LeadershipShedReason;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_raft::RaftGroupMetricsSnapshot;
use ursula_shard::RaftGroupId;

use crate::bootstrap::health_gate::HealthHysteresis;
use crate::bootstrap::health_gate::HealthTransition;
use crate::bootstrap::health_gate::ShedCandidate;
use crate::bootstrap::health_gate::TickHealth;
use crate::bootstrap::health_gate::plan_shed_handoffs;
use crate::bootstrap::participation::PeerParticipation;

const DEFAULT_CLUSTER_PROBE_INTERVAL_MS: usize = 500;
const DEFAULT_CLUSTER_PROBE_TIMEOUT_MS: usize = 200;

/// M2 — egress-health leadership gate. Each node actively measures its own
/// egress to peers over the cluster plane (a payload POST with a tight
/// deadline, sized so 15% loss / added delay fails it while a healthy link
/// passes trivially — a heartbeat-sized probe would mask loss). A node
/// that cannot push to a quorum of peers cannot replicate, so it sheds
/// leadership and disables elections; it clears this reason once its egress is
/// clean and re-enables elections only if no other shed reason remains.
///
/// Unlike inferring health from commit progress (load-dependent, and gone once
/// the client gives up), this probe is role-independent: a yielded follower
/// keeps measuring its own egress, so recovery is self-evident even though
/// egress-only loss leaves its inbound replication looking healthy. That is
/// what prevents the flap that a commit-stall step-down suffers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ClusterEgressProbeScope {
    Global,
    Group(RaftGroupId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClusterEgressProbeGroup {
    pub scope: ClusterEgressProbeScope,
    pub peer_urls: Vec<String>,
    pub needed_peers: usize,
}

fn remote_peers_needed_for_quorum(total_voters: usize) -> usize {
    // A quorum is `total_voters / 2 + 1` voters and this node is one of them.
    total_voters / 2
}

pub(crate) fn cluster_egress_probe_groups(
    node_id: u64,
    peers: &[(u64, String)],
    per_group_voters: &BTreeMap<RaftGroupId, BTreeSet<u64>>,
    snapshots: &[RaftGroupMetricsSnapshot],
) -> Vec<ClusterEgressProbeGroup> {
    let peer_urls: BTreeMap<u64, String> = peers
        .iter()
        .map(|(node_id, url)| (*node_id, url.clone()))
        .collect();

    if per_group_voters.is_empty() {
        let peer_urls: Vec<String> = peers
            .iter()
            .filter(|(id, _)| *id != node_id)
            .map(|(_, url)| url.clone())
            .collect();
        if peer_urls.is_empty() {
            return Vec::new();
        }
        return vec![ClusterEgressProbeGroup {
            scope: ClusterEgressProbeScope::Global,
            peer_urls,
            needed_peers: remote_peers_needed_for_quorum(peers.len()),
        }];
    }

    let registered_groups: BTreeSet<RaftGroupId> = snapshots
        .iter()
        .map(|snapshot| RaftGroupId(snapshot.raft_group_id))
        .collect();

    per_group_voters
        .iter()
        .filter(|(raft_group_id, voters)| {
            voters.contains(&node_id)
                && (registered_groups.is_empty() || registered_groups.contains(raft_group_id))
        })
        .filter_map(|(raft_group_id, voters)| {
            let peer_urls: Vec<String> = voters
                .iter()
                .filter(|id| **id != node_id)
                .filter_map(|id| peer_urls.get(id).cloned())
                .collect();
            if peer_urls.is_empty() {
                return None;
            }
            Some(ClusterEgressProbeGroup {
                scope: ClusterEgressProbeScope::Group(*raft_group_id),
                peer_urls,
                needed_peers: remote_peers_needed_for_quorum(voters.len()),
            })
        })
        .collect()
}

/// Config-driven cluster egress gate. It probes `probe_peers` and, when it
/// sheds, hands every leadership off at once to the peers in `participation`
/// that may campaign.
pub fn spawn_egress_gate(
    registry: &RaftGroupHandleRegistry,
    participation: &PeerParticipation,
    probe_peers: &[(u64, String)],
    per_group_voters: BTreeMap<RaftGroupId, BTreeSet<u64>>,
    cp_cfg: &ursula_config::ClusterProbeConfig,
) {
    let node_id = participation.node_id();
    let interval_ms = cp_cfg.interval.as_duration().as_millis() as usize;
    if interval_ms == 0 {
        return;
    }
    let initial_probe_groups =
        cluster_egress_probe_groups(node_id, probe_peers, &per_group_voters, &[]);
    if initial_probe_groups.is_empty() {
        return;
    }
    let probe_bytes = cp_cfg.probe_size.as_bytes() as usize;
    let probe_timeout_ms = cp_cfg.timeout.as_duration().as_millis() as usize;
    let unhealthy_ticks = cp_cfg.unhealthy_ticks;
    let heal_ticks = cp_cfg.heal_ticks;
    let registry = registry.clone();
    let peers = probe_peers.to_vec();
    let participation = participation.clone();
    tokio::spawn(async move {
        let interval = Duration::from_millis(
            u64::try_from(interval_ms)
                .unwrap_or(u64::try_from(DEFAULT_CLUSTER_PROBE_INTERVAL_MS).unwrap_or(500)),
        );
        let client = match reqwest::Client::builder()
            .timeout(Duration::from_millis(
                u64::try_from(probe_timeout_ms)
                    .unwrap_or(u64::try_from(DEFAULT_CLUSTER_PROBE_TIMEOUT_MS).unwrap_or(200)),
            ))
            .build()
        {
            Ok(client) => client,
            Err(err) => {
                tracing::error!("cluster-egress: failed to build probe client: {err}");
                return;
            }
        };
        let payload = vec![0u8; probe_bytes];
        let mut hysteresis = HealthHysteresis::new(unhealthy_ticks, heal_ticks);
        loop {
            tokio::time::sleep(interval).await;
            let snaps = registry.metrics_snapshot();
            let probe_groups =
                cluster_egress_probe_groups(node_id, &peers, &per_group_voters, &snaps);
            if probe_groups.is_empty() {
                continue;
            }
            let mut degraded_probe = None;
            for group in &probe_groups {
                let mut healthy_peers = 0usize;
                for url in &group.peer_urls {
                    let probe_url = format!("{url}{}", crate::CLUSTER_PROBE_PATH);
                    if let Ok(resp) = client.post(&probe_url).body(payload.clone()).send().await
                        && resp.status().is_success()
                    {
                        healthy_peers = healthy_peers.saturating_add(1);
                    }
                }
                if healthy_peers < group.needed_peers {
                    degraded_probe = Some((
                        group.scope.clone(),
                        healthy_peers,
                        group.peer_urls.len(),
                        group.needed_peers,
                    ));
                    break;
                }
            }
            let tick = if degraded_probe.is_none() {
                TickHealth::Good
            } else {
                TickHealth::Bad
            };
            match hysteresis.observe(tick) {
                HealthTransition::Shed => {
                    registry.mark_leadership_shed(LeadershipShedReason::ClusterEgress);
                    // A node that cannot reach a quorum cannot replicate, so its
                    // leaderships stall where they are: hand them all off now.
                    let may_campaign = participation.campaign_eligible_nodes(&registry).await;
                    let plan = plan_shed_handoffs(
                        &snaps,
                        node_id,
                        ShedCandidate::every_group_led_by(&snaps, node_id),
                        &[],
                        usize::MAX,
                        &may_campaign,
                        |group, target| registry.check_handoff(group, target).is_ok(),
                    );
                    if !plan.stranded.is_empty() {
                        tracing::warn!(
                            node_id,
                            groups = ?plan.stranded,
                            ?may_campaign,
                            "cluster-egress: no peer that may campaign can take over these \
                             leaderships"
                        );
                    }
                    for handoff in plan.handoffs {
                        if let Err(err) = registry
                            .transfer_leader(RaftGroupId(handoff.group_id), handoff.target)
                            .await
                        {
                            tracing::error!(
                                "cluster-egress: transfer_leader group {} -> {} failed while yielding: {err}",
                                handoff.group_id,
                                handoff.target,
                            );
                        }
                    }
                    if let Some((scope, healthy_peers, peer_count, needed_peers)) = degraded_probe {
                        tracing::warn!(
                            "cluster-egress: node {node_id} {scope:?} egress degraded (reached {healthy_peers}/{peer_count} peers, need {needed_peers}); yielding leadership",
                        );
                    }
                }
                HealthTransition::Heal => {
                    registry.clear_leadership_shed(LeadershipShedReason::ClusterEgress);
                }
                HealthTransition::Hold => {}
            }
        }
    });
}
