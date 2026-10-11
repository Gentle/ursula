//! Which nodes may take over a leadership: the ones whose leadership-shed
//! policy lets them campaign. This node reads its own policy and asks each
//! peer for its state (`GET /__ursula/leadership-shed`). The leadership
//! balancer, the shutdown handoff and the snapshot-store and cold-health
//! gates hand leaderships only to the nodes known to be allowed to campaign.
//! The cluster-egress gate, whose questions travel the path it judges
//! impaired, passes over only the nodes that answered that they may not. A
//! node that may not campaign (under drain, behind its recovery gate, or
//! shedding for its snapshot store, cluster egress or WAL disk) refuses a
//! transfer without telling the leader, which then forwards writes to it
//! until Raft abandons the transfer.

use std::collections::BTreeMap;
use std::collections::HashSet;

use futures_util::future::join_all;
use ursula_proto::admin::LeadershipShedStatus;
use ursula_raft::RaftGroupHandleRegistry;

/// What a node's leadership-shed policy lets it do, as far as this node
/// knows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CampaignStance {
    /// Its policy lets it campaign.
    MayCampaign,
    /// It answered that its policy does not let it campaign.
    MayNotCampaign,
    /// It did not answer in time, or its answer could not be read.
    Unknown,
}

/// The campaign stance of each node: this node's from its own policy, and
/// each peer's from its answer.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CampaignStances {
    by_node: BTreeMap<u64, CampaignStance>,
}

impl CampaignStances {
    /// The nodes known to be allowed to campaign.
    pub(crate) fn may_campaign(&self) -> HashSet<u64> {
        self.nodes(|stance| stance == CampaignStance::MayCampaign)
    }

    /// The nodes not known to refuse: the ones allowed to campaign and the
    /// ones that did not answer. A planner that uses this set leaves a node
    /// that did not answer to the registry's handoff check, whose Raft acks
    /// are fresher evidence than a lost request.
    pub(crate) fn not_known_to_refuse(&self) -> HashSet<u64> {
        self.nodes(|stance| stance != CampaignStance::MayNotCampaign)
    }

    fn nodes(&self, keep: impl Fn(CampaignStance) -> bool) -> HashSet<u64> {
        self.by_node
            .iter()
            .filter(|(_, stance)| keep(**stance))
            .map(|(node_id, _)| *node_id)
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn of(&self, node_id: u64) -> Option<CampaignStance> {
        self.by_node.get(&node_id).copied()
    }
}

/// A node's cluster peers, and the client that asks them for their
/// leadership-shed state.
#[derive(Debug, Clone)]
pub(crate) struct PeerParticipation {
    node_id: u64,
    peers: Vec<(u64, String)>,
    client: reqwest::Client,
}

impl PeerParticipation {
    /// `peers` may list this node too. `client` bounds each peer's answer.
    pub(crate) fn new(node_id: u64, peers: &[(u64, String)], client: reqwest::Client) -> Self {
        Self {
            node_id,
            peers: peers
                .iter()
                .filter(|(peer_id, _)| *peer_id != node_id)
                .cloned()
                .collect(),
            client,
        }
    }

    pub(crate) fn node_id(&self) -> u64 {
        self.node_id
    }

    /// The campaign stance of this node and of each peer. A node shedding
    /// for cold health may campaign, because that reason does not stop it
    /// from campaigning: cold-health backlogs are often cluster-wide, and
    /// leaving every hot node out could leave no node to lead.
    pub(crate) async fn campaign_stances(
        &self,
        registry: &RaftGroupHandleRegistry,
    ) -> CampaignStances {
        let answers = join_all(self.peers.iter().map(|(peer_id, peer_url)| async move {
            (*peer_id, self.peer_stance(*peer_id, peer_url).await)
        }))
        .await;
        let mut by_node: BTreeMap<u64, CampaignStance> = answers.into_iter().collect();
        let own = if registry.participation_status().should_campaign {
            CampaignStance::MayCampaign
        } else {
            CampaignStance::MayNotCampaign
        };
        by_node.insert(self.node_id, own);
        CampaignStances { by_node }
    }

    /// The nodes known to be allowed to campaign
    /// ([`CampaignStances::may_campaign`]). A peer that does not answer in
    /// time is left out.
    pub(crate) async fn campaign_eligible_nodes(
        &self,
        registry: &RaftGroupHandleRegistry,
    ) -> HashSet<u64> {
        self.campaign_stances(registry).await.may_campaign()
    }

    async fn peer_stance(&self, peer_id: u64, peer_url: &str) -> CampaignStance {
        let url = format!("{peer_url}{}", crate::LEADERSHIP_SHED_PATH);
        let response = match self.client.get(url).send().await {
            Ok(response) if response.status().is_success() => response,
            Ok(response) => {
                tracing::debug!(
                    peer_id,
                    status = %response.status(),
                    "leadership-shed state request refused"
                );
                return CampaignStance::Unknown;
            }
            Err(err) => {
                tracing::debug!(peer_id, %err, "leadership-shed state request failed");
                return CampaignStance::Unknown;
            }
        };
        let body = match response.text().await {
            Ok(body) => body,
            Err(err) => {
                tracing::debug!(peer_id, %err, "leadership-shed state body failed");
                return CampaignStance::Unknown;
            }
        };
        match serde_json::from_str::<LeadershipShedStatus>(&body) {
            Ok(status) if status.should_campaign => CampaignStance::MayCampaign,
            Ok(_) => CampaignStance::MayNotCampaign,
            Err(err) => {
                tracing::debug!(peer_id, %err, "unreadable leadership-shed state");
                CampaignStance::Unknown
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use axum::Json;
    use axum::Router;
    use axum::routing::get;
    use ursula_proto::admin::LeadershipShedStatus;
    use ursula_raft::LeadershipShedReason;
    use ursula_raft::RaftGroupHandleRegistry;

    use super::CampaignStance;
    use super::PeerParticipation;

    /// A peer that answers its leadership-shed state with `should_campaign`.
    async fn answering_peer(should_campaign: bool) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind peer");
        let address = listener.local_addr().expect("peer address");
        let status = LeadershipShedStatus {
            bits: 0,
            state: String::new(),
            should_accept_transfer: should_campaign,
            should_campaign,
            recovery_barriers_ready: true,
            should_shed_current_leaders: false,
        };
        let app = Router::new().route(
            crate::LEADERSHIP_SHED_PATH,
            get(move || {
                let status = status.clone();
                async move { Json(status) }
            }),
        );
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve peer");
        });
        (format!("http://{address}"), task)
    }

    /// A peer that accepts connections and never answers. The listener must
    /// outlive the request.
    async fn silent_peer() -> (String, tokio::net::TcpListener) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind peer");
        let address = listener.local_addr().expect("peer address");
        (format!("http://{address}"), listener)
    }

    #[tokio::test]
    async fn only_a_peer_that_answers_that_it_may_not_campaign_is_known_to_refuse() {
        let (willing, willing_task) = answering_peer(true).await;
        let (refusing, refusing_task) = answering_peer(false).await;
        let (silent, _listener) = silent_peer().await;
        let participation = PeerParticipation::new(
            1,
            &[
                (1, "http://unused".to_owned()),
                (2, willing),
                (3, refusing),
                (4, silent),
            ],
            reqwest::Client::builder()
                .timeout(Duration::from_millis(300))
                .build()
                .expect("client"),
        );
        let registry = RaftGroupHandleRegistry::default();

        let stances = participation.campaign_stances(&registry).await;
        assert_eq!(stances.of(1), Some(CampaignStance::MayCampaign));
        assert_eq!(stances.of(2), Some(CampaignStance::MayCampaign));
        assert_eq!(stances.of(3), Some(CampaignStance::MayNotCampaign));
        assert_eq!(stances.of(4), Some(CampaignStance::Unknown));
        assert_eq!(stances.may_campaign(), HashSet::from([1, 2]));
        assert_eq!(stances.not_known_to_refuse(), HashSet::from([1, 2, 4]));
        assert_eq!(
            participation.campaign_eligible_nodes(&registry).await,
            HashSet::from([1, 2])
        );

        // This node reads its own policy.
        registry.mark_leadership_shed(LeadershipShedReason::MaintenanceDrain);
        let stances = participation.campaign_stances(&registry).await;
        assert_eq!(stances.of(1), Some(CampaignStance::MayNotCampaign));
        assert_eq!(stances.not_known_to_refuse(), HashSet::from([2, 4]));

        willing_task.abort();
        refusing_task.abort();
    }
}
