//! `x-ursula-not-applied`: ingress admission marks the refusals it makes
//! before a request reaches a handler, and an answer that can follow a
//! proposal never carries the marker. The pressure refusals are pinned with
//! their tests in `tests.rs`.

use axum::Router;
use axum::body::Body;
use axum::body::to_bytes;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::header::CONTENT_LENGTH;
use axum::http::header::CONTENT_TYPE;
use axum::http::header::LOCATION;
use axum::response::Response;
use tower::ServiceExt;
use ursula_proto::http::NOT_APPLIED_HEADER;
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
use ursula_shard::ShardPlacement;

use crate::MAX_HTTP_BODY_BYTES;
use crate::router;
use crate::router_with_static_raft_cluster;

/// An in-memory engine that commits every append and then answers it with
/// `error`, as a node does when the response to a committed proposal is
/// lost or overtaken by a leader change.
struct CommitThenFailEngine {
    inner: InMemoryGroupEngine,
    error: GroupEngineError,
}

impl GroupEngine for CommitThenFailEngine {
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
        Box::pin(async move {
            self.inner.append(request, placement, admission).await?;
            Err(self.error.clone())
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

struct CommitThenFailFactory {
    error: GroupEngineError,
}

impl GroupEngineFactory for CommitThenFailFactory {
    fn create<'a>(
        &'a self,
        _placement: ShardPlacement,
        _metrics: GroupEngineMetrics,
    ) -> GroupEngineCreateFuture<'a> {
        let engine: Box<dyn GroupEngine> = Box::new(CommitThenFailEngine {
            inner: InMemoryGroupEngine::default(),
            error: self.error.clone(),
        });
        Box::pin(async move { Ok(engine) })
    }
}

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
    app.clone()
        .oneshot(request.body(Body::from(body)).expect("request"))
        .await
        .expect("response")
}

fn not_applied(response: &Response) -> Option<&str> {
    response
        .headers()
        .get(NOT_APPLIED_HEADER)
        .map(|value| value.to_str().expect("an ASCII token"))
}

/// Admission refuses a body over the route's limit from its `Content-Length`,
/// before reading it, so the `413` applied nothing.
#[tokio::test]
async fn admission_marks_an_oversized_body_as_not_applied() {
    let app = router(ShardRuntime::spawn(RuntimeConfig::new(1, 1)).expect("runtime"));
    let oversized = MAX_HTTP_BODY_BYTES.saturating_add(1).to_string();

    let response = send(
        &app,
        "POST",
        "/benchcmp/oversized",
        &[
            (CONTENT_TYPE.as_str(), "text/plain"),
            (CONTENT_LENGTH.as_str(), &oversized),
        ],
        "",
    )
    .await;

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(not_applied(&response), Some("RequestBodyTooLarge"));
}

/// An answer that can follow a committed proposal never carries the marker:
/// a client that took it as "applied nothing" and reused the producer
/// sequence for other data would have that data acknowledged as a duplicate
/// of the committed append. That includes the redirect to a known leader:
/// the gateway keeps a leader's marker after a redirect only when the
/// redirect carried one too.
#[tokio::test]
async fn an_answer_that_can_follow_a_proposal_carries_no_marker() {
    const PEER: &str = "http://127.0.0.1:4437";
    let uri = "/benchcmp/committed";
    let cases = [
        (
            "outcome unknown",
            GroupEngineError::Infra(GroupInfraError::OutcomeUnknown),
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
            Some(format!("{PEER}{uri}")),
        ),
    ];
    for (case, error, clustered, status, location) in cases {
        let runtime = ShardRuntime::spawn_with_engine_factory(
            RuntimeConfig::new(1, 1),
            CommitThenFailFactory { error },
        )
        .expect("runtime");
        let app = if clustered {
            router_with_static_raft_cluster(runtime, RaftGroupHandleRegistry::default(), [(
                1,
                PEER.to_owned(),
            )])
        } else {
            router(runtime)
        };
        let created = send(
            &app,
            "PUT",
            uri,
            &[(CONTENT_TYPE.as_str(), "text/plain")],
            "",
        )
        .await;
        assert_eq!(created.status(), StatusCode::CREATED, "{case}");

        let appended = send(
            &app,
            "POST",
            uri,
            &[(CONTENT_TYPE.as_str(), "text/plain")],
            "abc",
        )
        .await;

        assert_eq!(appended.status(), status, "{case}");
        assert_eq!(
            appended
                .headers()
                .get(LOCATION)
                .map(|value| value.to_str().expect("an ASCII URL")),
            location.as_deref(),
            "{case}"
        );
        assert_eq!(not_applied(&appended), None, "{case}");
        // The append did commit.
        let read = send(&app, "GET", &format!("{uri}?offset=-1"), &[], "").await;
        assert_eq!(read.status(), StatusCode::OK, "{case}");
        let body = to_bytes(read.into_body(), usize::MAX).await.expect("body");
        assert_eq!(&body[..], b"abc", "{case}");
    }
}
