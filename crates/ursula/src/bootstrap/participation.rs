//! Which nodes may take over a leadership: the ones whose leadership-shed
//! policy lets them campaign. This node reads its own policy and asks each
//! peer for its state (`GET /__ursula/leadership-shed`). The leadership
//! balancer, the shutdown handoff and the health gates' handoff planner hand
//! leaderships only to these nodes. A node that may not campaign (under
//! drain, behind its recovery gate, or shedding for its snapshot store,
//! cluster egress or WAL disk) refuses a transfer without telling the
//! leader, which then forwards writes to it until Raft abandons the
//! transfer.

use std::collections::HashSet;

use futures_util::future::join_all;
use ursula_proto::admin::LeadershipShedStatus;
use ursula_raft::RaftGroupHandleRegistry;

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

    /// The nodes that may campaign: this node when its own policy lets it,
    /// and each peer that answered its leadership-shed state with
    /// `should_campaign`. A peer that does not answer in time is left out. A
    /// node shedding for cold health stays in, because that reason does not
    /// stop it from campaigning: cold-health backlogs are often cluster-wide,
    /// and leaving every hot node out could leave no node to lead.
    pub(crate) async fn campaign_eligible_nodes(
        &self,
        registry: &RaftGroupHandleRegistry,
    ) -> HashSet<u64> {
        let answers = join_all(self.peers.iter().map(|(peer_id, peer_url)| async move {
            (*peer_id, self.peer_may_campaign(*peer_id, peer_url).await)
        }))
        .await;
        let mut eligible: HashSet<u64> = answers
            .into_iter()
            .filter_map(|(peer_id, may_campaign)| may_campaign.then_some(peer_id))
            .collect();
        if registry.participation_status().should_campaign {
            eligible.insert(self.node_id);
        }
        eligible
    }

    async fn peer_may_campaign(&self, peer_id: u64, peer_url: &str) -> bool {
        let url = format!("{peer_url}{}", crate::LEADERSHIP_SHED_PATH);
        let response = match self.client.get(url).send().await {
            Ok(response) if response.status().is_success() => response,
            Ok(response) => {
                tracing::debug!(
                    peer_id,
                    status = %response.status(),
                    "leadership-shed state request refused"
                );
                return false;
            }
            Err(err) => {
                tracing::debug!(peer_id, %err, "leadership-shed state request failed");
                return false;
            }
        };
        let body = match response.text().await {
            Ok(body) => body,
            Err(err) => {
                tracing::debug!(peer_id, %err, "leadership-shed state body failed");
                return false;
            }
        };
        match serde_json::from_str::<LeadershipShedStatus>(&body) {
            Ok(status) => status.should_campaign,
            Err(err) => {
                tracing::debug!(peer_id, %err, "unreadable leadership-shed state");
                false
            }
        }
    }
}
