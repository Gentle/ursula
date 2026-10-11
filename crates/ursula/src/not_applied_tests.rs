//! `x-ursula-not-applied`: ingress admission marks the refusals it makes
//! before a request reaches a handler, and each marked refusal applied
//! nothing. The refusals left unmarked on purpose are pinned here, and so is
//! the rule that a node never sets the gateway's vouch,
//! `x-ursula-not-applied-checked`, which [`send`] checks on every response.
//! The pressure refusals are pinned with their tests in `tests.rs`, which
//! check the vouch too.

use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::body::to_bytes;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::header::CONTENT_LENGTH;
use axum::http::header::LOCATION;
use axum::response::Response;
use tower::ServiceExt;
use ursula_proto::admin::ApplyFailureKind;
use ursula_proto::headers::NOT_APPLIED_CHECKED_HEADER;
use ursula_proto::headers::NOT_APPLIED_HEADER;
use ursula_raft::RaftGroupHandleRegistry;
use ursula_runtime::AppendRequest;
use ursula_runtime::CloseStreamRequest;
use ursula_runtime::ColdWriteAdmission;
use ursula_runtime::CreateStreamRequest;
use ursula_runtime::DeleteStreamRequest;
use ursula_runtime::GroupAppendFuture;
use ursula_runtime::GroupBucketUsageFuture;
use ursula_runtime::GroupCloseStreamFuture;
use ursula_runtime::GroupCreateStreamFuture;
use ursula_runtime::GroupDeleteStreamFuture;
use ursula_runtime::GroupEngine;
use ursula_runtime::GroupEngineCreateFuture;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupEngineFactory;
use ursula_runtime::GroupEngineMetrics;
use ursula_runtime::GroupHeadStreamFuture;
use ursula_runtime::GroupInfraError;
use ursula_runtime::GroupInstallSnapshotFuture;
use ursula_runtime::GroupReadStreamFuture;
use ursula_runtime::GroupSnapshot;
use ursula_runtime::GroupSnapshotFuture;
use ursula_runtime::GroupTouchStreamAccessFuture;
use ursula_runtime::HeadStreamRequest;
use ursula_runtime::InMemoryGroupEngine;
use ursula_runtime::ReadStreamRequest;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::ShardRuntime;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardPlacement;

use crate::HttpState;
use crate::IngressAdmission;
use crate::MAX_HTTP_BODY_BYTES;
use crate::client_router_with_admission;
use crate::router;
use crate::router_with_static_raft_cluster;

const STREAM: &str = "/benchcmp/committed";
const PEER: &str = "http://127.0.0.1:4437";
const TEXT: (&str, &str) = ("content-type", "text/plain");

/// How the test engine answers an append.
#[derive(Clone)]
enum AppendFailure {
    /// Commits the append, then answers with the error, as a node does when
    /// the response to a committed proposal is lost or overtaken by a leader
    /// change.
    AfterCommit(GroupEngineError),
    /// Answers with the error and proposes nothing.
    BeforeProposal(GroupEngineError),
}

/// An in-memory engine whose appends fail as `failure` says. Everything else
/// passes through.
struct FailingAppendEngine {
    inner: InMemoryGroupEngine,
    failure: AppendFailure,
}

impl GroupEngine for FailingAppendEngine {
    fn create_stream<'a>(
        &'a mut self,
        request: CreateStreamRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupCreateStreamFuture<'a> {
        self.inner.create_stream(request, placement, admission)
    }

    fn head_stream<'a>(
        &'a mut self,
        request: HeadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupHeadStreamFuture<'a> {
        self.inner.head_stream(request, placement)
    }

    fn bucket_usage<'a>(&'a mut self, placement: ShardPlacement) -> GroupBucketUsageFuture<'a> {
        self.inner.bucket_usage(placement)
    }

    fn read_stream<'a>(
        &'a mut self,
        request: ReadStreamRequest,
        placement: ShardPlacement,
    ) -> GroupReadStreamFuture<'a> {
        self.inner.read_stream(request, placement)
    }

    fn touch_stream_access<'a>(
        &'a mut self,
        stream_id: BucketStreamId,
        now_ms: u64,
        renew_ttl: bool,
        placement: ShardPlacement,
    ) -> GroupTouchStreamAccessFuture<'a> {
        self.inner
            .touch_stream_access(stream_id, now_ms, renew_ttl, placement)
    }

    fn close_stream<'a>(
        &'a mut self,
        request: CloseStreamRequest,
        placement: ShardPlacement,
    ) -> GroupCloseStreamFuture<'a> {
        self.inner.close_stream(request, placement)
    }

    fn delete_stream<'a>(
        &'a mut self,
        request: DeleteStreamRequest,
        placement: ShardPlacement,
    ) -> GroupDeleteStreamFuture<'a> {
        self.inner.delete_stream(request, placement)
    }

    fn append<'a>(
        &'a mut self,
        request: AppendRequest,
        placement: ShardPlacement,
        admission: ColdWriteAdmission,
    ) -> GroupAppendFuture<'a> {
        let failure = self.failure.clone();
        Box::pin(async move {
            match failure {
                AppendFailure::AfterCommit(error) => {
                    self.inner.append(request, placement, admission).await?;
                    Err(error)
                }
                AppendFailure::BeforeProposal(error) => Err(error),
            }
        })
    }

    fn snapshot<'a>(&'a mut self, placement: ShardPlacement) -> GroupSnapshotFuture<'a> {
        self.inner.snapshot(placement)
    }

    fn install_snapshot<'a>(
        &'a mut self,
        snapshot: GroupSnapshot,
    ) -> GroupInstallSnapshotFuture<'a> {
        self.inner.install_snapshot(snapshot)
    }
}

struct FailingAppendFactory {
    failure: AppendFailure,
}

impl GroupEngineFactory for FailingAppendFactory {
    fn create<'a>(
        &'a self,
        _placement: ShardPlacement,
        _metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        let engine: Box<dyn GroupEngine> = Box::new(FailingAppendEngine {
            inner: InMemoryGroupEngine::default(),
            failure: self.failure.clone(),
        });
        Box::pin(async move { Ok(engine) })
    }
}

/// A node whose appends fail as `failure` says. A clustered node knows one
/// peer, node 1 at [`PEER`], so a forward to node 1 answers a redirect there.
fn node_failing_appends(failure: AppendFailure, clustered: bool) -> Router {
    let runtime =
        ShardRuntime::spawn_with_engine_factory(RuntimeConfig::new(1, 1), FailingAppendFactory {
            failure,
        })
        .expect("runtime");
    if clustered {
        router_with_static_raft_cluster(runtime, RaftGroupHandleRegistry::default(), [(
            1,
            PEER.to_owned(),
        )])
    } else {
        router(runtime)
    }
}

/// A node with the production admission and an in-flight write-body budget
/// of `bytes`.
fn node_with_body_budget(bytes: u64) -> Router {
    let runtime = ShardRuntime::spawn(RuntimeConfig::new(1, 1)).expect("runtime");
    client_router_with_admission(
        HttpState::new(runtime),
        IngressAdmission::new(&ursula_config::ServerConfig {
            http_inflight_body_size: ursula_config::HumanSize::bytes(bytes),
            ..ursula_config::ServerConfig::default()
        }),
    )
}

/// Sends one request to `app`. Every response a test here gets passes
/// through this helper, and none may carry the gateway's vouch.
async fn send(
    app: &Router,
    method: &str,
    uri: &str,
    headers: &[(&str, &str)],
    body: &'static str,
) -> Response {
    let mut request = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::from(body)).expect("request"))
        .await
        .expect("response");
    assert!(
        !response.headers().contains_key(NOT_APPLIED_CHECKED_HEADER),
        "{method} {uri} answered {}: a node never sets the vouch",
        response.status()
    );
    response
}

async fn create(app: &Router) {
    let created = send(app, "PUT", STREAM, &[TEXT], "").await;
    assert_eq!(created.status(), StatusCode::CREATED);
}

/// Everything the stream holds, read from its start.
async fn stream_content(app: &Router) -> Bytes {
    let read = send(app, "GET", &format!("{STREAM}?offset=-1"), &[], "").await;
    assert_eq!(read.status(), StatusCode::OK);
    to_bytes(read.into_body(), usize::MAX).await.expect("body")
}

fn not_applied(response: &Response) -> Option<&str> {
    response
        .headers()
        .get(NOT_APPLIED_HEADER)
        .map(|value| value.to_str().expect("an ASCII token"))
}

fn location(response: &Response) -> Option<&str> {
    response
        .headers()
        .get(LOCATION)
        .map(|value| value.to_str().expect("an ASCII URL"))
}

/// Admission refuses a body over the route's limit from its `Content-Length`,
/// before reading it, so the `413` applied nothing.
#[tokio::test]
async fn admission_marks_an_oversized_body_as_not_applied() {
    let app = router(ShardRuntime::spawn(RuntimeConfig::new(1, 1)).expect("runtime"));
    create(&app).await;
    let oversized = MAX_HTTP_BODY_BYTES.saturating_add(1).to_string();

    let response = send(
        &app,
        "POST",
        STREAM,
        &[TEXT, (CONTENT_LENGTH.as_str(), &oversized)],
        "abc",
    )
    .await;

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(not_applied(&response), Some("RequestBodyTooLarge"));
    assert_eq!(stream_content(&app).await, "");
}

/// Admission refuses a body that the in-flight write-body budget has no room
/// for, before the handler reads it, so the `503` applied nothing.
#[tokio::test]
async fn admission_marks_an_exhausted_body_budget_as_not_applied() {
    let app = node_with_body_budget(4);
    create(&app).await;

    let refused = send(&app, "POST", STREAM, &[TEXT], "abcde").await;

    assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(not_applied(&refused), Some("IngressBodyBytesLimitReached"));
    assert_eq!(stream_content(&app).await, "");
    // A body that fits is appended, without the marker.
    let appended = send(&app, "POST", STREAM, &[TEXT], "abcd").await;
    assert_eq!(appended.status(), StatusCode::NO_CONTENT);
    assert_eq!(not_applied(&appended), None);
    assert_eq!(stream_content(&app).await, "abcd");
}

/// An answer that can follow a committed proposal never carries the marker:
/// a client that took it as "applied nothing" and reused the producer
/// sequence for other data would have that data acknowledged as a duplicate
/// of the committed append. That includes the redirect to a known leader:
/// the gateway keeps a leader's marker after a redirect only when the
/// redirect carried one too.
#[tokio::test]
async fn an_answer_that_can_follow_a_proposal_carries_no_marker() {
    let cases = [
        (
            "outcome unknown",
            GroupEngineError::Infra(GroupInfraError::OutcomeUnknown),
            false,
            StatusCode::SERVICE_UNAVAILABLE,
            None,
        ),
        (
            "apply stopped",
            GroupEngineError::Infra(GroupInfraError::ApplyStopped {
                raft_group_id: RaftGroupId(0),
                term: 1,
                index: 1,
                kind: ApplyFailureKind::Panic,
            }),
            false,
            StatusCode::SERVICE_UNAVAILABLE,
            None,
        ),
        // OpenRaft reports a forward after `client_write` (RT1). With no
        // known leader the node answers the retryable 503.
        (
            "forward after client_write, leader unknown",
            GroupEngineError::forward_to_leader("client_write responder", None, None),
            true,
            StatusCode::SERVICE_UNAVAILABLE,
            None,
        ),
        // With a known leader it redirects there.
        (
            "forward after client_write, leader known",
            GroupEngineError::forward_to_leader("client_write responder", Some(1), None),
            true,
            StatusCode::TEMPORARY_REDIRECT,
            Some(format!("{PEER}{STREAM}")),
        ),
    ];
    for (case, error, clustered, status, redirect) in cases {
        let app = node_failing_appends(AppendFailure::AfterCommit(error), clustered);
        create(&app).await;

        let appended = send(&app, "POST", STREAM, &[TEXT], "abc").await;

        assert_eq!(appended.status(), status, "{case}");
        assert_eq!(location(&appended), redirect.as_deref(), "{case}");
        assert_eq!(not_applied(&appended), None, "{case}");
        // The append did commit.
        assert_eq!(stream_content(&app).await, "abc", "{case}");
    }
}

/// Refusals the runtime makes before it proposes anything stay unmarked as
/// well. The shared runtime-error path renders them, and it also answers
/// handlers that issue several commands: a bucket purge proposes once per
/// group, so a later group's refusal there can follow earlier groups'
/// purges. Marking them needs a decision per handler.
#[tokio::test]
async fn runtime_refusals_before_proposal_stay_unmarked() {
    let cases = [
        (
            "uncommitted backpressure",
            GroupEngineError::raft_uncommitted_backpressure(8, 4, 10),
            false,
            StatusCode::SERVICE_UNAVAILABLE,
            None,
        ),
        (
            "cold backpressure",
            GroupEngineError::cold_backpressure(
                BucketStreamId::new("benchcmp", "committed"),
                8,
                11,
                10,
            ),
            false,
            StatusCode::SERVICE_UNAVAILABLE,
            None,
        ),
        (
            "forward before proposal, leader unknown",
            GroupEngineError::forward_to_leader_before_proposal("not the leader", None, None),
            true,
            StatusCode::SERVICE_UNAVAILABLE,
            None,
        ),
        (
            "forward before proposal, leader known",
            GroupEngineError::forward_to_leader_before_proposal("not the leader", Some(1), None),
            true,
            StatusCode::TEMPORARY_REDIRECT,
            Some(format!("{PEER}{STREAM}")),
        ),
    ];
    for (case, error, clustered, status, redirect) in cases {
        let app = node_failing_appends(AppendFailure::BeforeProposal(error), clustered);
        create(&app).await;

        let refused = send(&app, "POST", STREAM, &[TEXT], "abc").await;

        assert_eq!(refused.status(), status, "{case}");
        assert_eq!(location(&refused), redirect.as_deref(), "{case}");
        assert_eq!(not_applied(&refused), None, "{case}");
        assert_eq!(stream_content(&app).await, "", "{case}");
    }
}

/// Only a gateway vouches for the marker. A node that set
/// `x-ursula-not-applied-checked` itself would have its marker trusted after
/// redirects that a gateway older than 0.7.3, or a client's HTTP library,
/// follows without checking the redirect.
///
/// [`send`] checks every response in this module for the vouch, among them
/// the runtime refusals and the answers that can follow a proposal above.
/// This test adds a plain create, append and `404`, and the two admission
/// refusals a node with a small body budget makes. The `WalDiskPressure` and
/// `RaftLogPressure` tests in `tests.rs` check their refusals.
#[tokio::test]
async fn a_node_never_sets_the_vouch() {
    let app = node_with_body_budget(4);
    let oversized = MAX_HTTP_BODY_BYTES.saturating_add(1).to_string();
    let cases = [
        (
            "create",
            send(&app, "PUT", STREAM, &[TEXT], "").await,
            StatusCode::CREATED,
            None,
        ),
        (
            "append",
            send(&app, "POST", STREAM, &[TEXT], "abc").await,
            StatusCode::NO_CONTENT,
            None,
        ),
        (
            "budget refusal",
            send(&app, "POST", STREAM, &[TEXT], "abcde").await,
            StatusCode::SERVICE_UNAVAILABLE,
            Some("IngressBodyBytesLimitReached"),
        ),
        (
            "size refusal",
            send(
                &app,
                "POST",
                STREAM,
                &[TEXT, (CONTENT_LENGTH.as_str(), &oversized)],
                "abc",
            )
            .await,
            StatusCode::PAYLOAD_TOO_LARGE,
            Some("RequestBodyTooLarge"),
        ),
        (
            "missing stream",
            send(&app, "GET", "/benchcmp/missing?offset=-1", &[], "").await,
            StatusCode::NOT_FOUND,
            None,
        ),
    ];

    // Each request got the answer it stands for.
    for (case, response, status, marker) in &cases {
        assert_eq!(response.status(), *status, "{case}");
        assert_eq!(not_applied(response), *marker, "{case}");
    }
}
