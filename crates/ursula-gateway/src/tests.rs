use std::convert::Infallible;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::http::header::AUTHORIZATION;
use axum::http::header::LOCATION;
use axum::routing::any;
use axum::routing::get;
use axum::routing::post;
use axum::routing::put;
use http_body_util::BodyExt;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

use super::*;
use crate::auth::AccessControl;
use crate::auth::AuthorizationError;
use crate::auth::AuthorizationFuture;
use crate::auth::Authorizer;
use crate::auth::PrincipalResolver;
use crate::auth::PrincipalResolverFuture;
use crate::auth::VerifiedPrincipal;
use crate::ejection::EJECTION_BASE;

#[test]
fn header_forwarding_applies_proxy_rules() {
    let mut request_headers = HeaderMap::new();
    request_headers.insert("content-type", "text/plain".parse().unwrap());
    request_headers.insert("host", "example.com".parse().unwrap());
    request_headers.insert("connection", "x-remove".parse().unwrap());
    request_headers.insert("x-remove", "drop-me".parse().unwrap());
    request_headers.insert(NOT_APPLIED_CHECKED_HEADER, "1".parse().unwrap());

    let copied_request = copy_forwarded_headers(&request_headers, true);
    assert_eq!(copied_request.get("content-type").unwrap(), "text/plain");
    assert!(copied_request.get("host").is_none());
    assert!(copied_request.get("connection").is_none());
    assert!(copied_request.get("x-remove").is_none());
    assert!(copied_request.get(NOT_APPLIED_CHECKED_HEADER).is_none());

    let mut response_headers = HeaderMap::new();
    response_headers.insert("content-type", "text/plain".parse().unwrap());
    response_headers.insert("transfer-encoding", "chunked".parse().unwrap());
    response_headers.insert("x-custom", "value".parse().unwrap());
    response_headers.append("set-cookie", "a=1".parse().unwrap());
    response_headers.append("set-cookie", "b=2".parse().unwrap());
    response_headers.insert(NOT_APPLIED_HEADER, "RaftLogPressure".parse().unwrap());
    response_headers.append(NOT_APPLIED_CHECKED_HEADER, "1".parse().unwrap());
    response_headers.append(NOT_APPLIED_CHECKED_HEADER, "0".parse().unwrap());

    let copied_response = copy_forwarded_headers(&response_headers, false);
    assert_eq!(copied_response.get("content-type").unwrap(), "text/plain");
    assert!(copied_response.get("transfer-encoding").is_none());
    assert_eq!(copied_response.get("x-custom").unwrap(), "value");
    // A node's marker passes. The vouch is the gateway's own and never does.
    assert_eq!(
        copied_response.get(NOT_APPLIED_HEADER).unwrap(),
        "RaftLogPressure"
    );
    assert!(copied_response.get(NOT_APPLIED_CHECKED_HEADER).is_none());

    let cookies = copied_response
        .get_all("set-cookie")
        .iter()
        .map(|value| value.to_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(cookies, vec!["a=1", "b=2"]);
}

// Owns a mock upstream server for one test. Dropping it aborts the server
// task so tests do not need repeated cleanup code.
struct TestUpstream {
    url: String,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for TestUpstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

// Some tests need the bound URL while constructing the app.
async fn spawn_upstream_with_url(app_for_url: impl FnOnce(String) -> Router) -> TestUpstream {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind upstream");
    let addr = listener.local_addr().expect("upstream local addr");
    let url = format!("http://{addr}");
    let app = app_for_url(url.clone());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve upstream");
    });

    TestUpstream { url, task }
}

async fn spawn_upstream(app: Router) -> TestUpstream {
    spawn_upstream_with_url(|_| app).await
}

// Start a leader plus a follower. The follower returns Ursula's internal
// Raft 307 redirect marker pointing at the leader.
async fn spawn_raft_redirect_upstreams(leader_app: Router) -> (TestUpstream, TestUpstream) {
    let leader = spawn_upstream(leader_app).await;
    let leader_url = format!("{}/bucket/stream", leader.url);
    let follower_app = Router::new().route(
        "/bucket/stream",
        any(move || {
            let leader_url = leader_url.clone();
            async move {
                (
                    StatusCode::TEMPORARY_REDIRECT,
                    [
                        ("location", leader_url),
                        ("x-ursula-raft-leader-id", "1".to_owned()),
                    ],
                    "redirecting",
                )
            }
        }),
    );
    let follower = spawn_upstream(follower_app).await;

    (leader, follower)
}

fn test_config(upstreams: Vec<String>) -> GatewayConfig {
    GatewayConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        upstreams,
        response_header_timeout: Duration::from_secs(5),
        connect_timeout: Duration::from_secs(1),
        upstream_tcp_user_timeout_secs: NonZeroU64::new(5).unwrap(),
        max_request_body_bytes: DEFAULT_MAX_REQUEST_BODY_BYTES,
        raft_group_count: None,
        cors_allowed_origins: Vec::new(),
        usage_chunk_bytes: None,
    }
}

fn gateway_for_url(upstream_url: impl Into<String>) -> Arc<Gateway> {
    Arc::new(Gateway::new(test_config(vec![upstream_url.into()])))
}

/// The response's `x-ursula-not-applied` token, if it has one.
fn not_applied(response: &AxumResponse) -> Option<&str> {
    response
        .headers()
        .get(NOT_APPLIED_HEADER)
        .map(|value| value.to_str().expect("an ASCII token"))
}

/// Every `x-ursula-not-applied` value of the response, in order.
fn markers(response: &AxumResponse) -> Vec<&str> {
    response
        .headers()
        .get_all(NOT_APPLIED_HEADER)
        .iter()
        .map(|value| value.to_str().expect("an ASCII token"))
        .collect()
}

/// The response's vouch, `x-ursula-not-applied-checked`. The gateway sets
/// one value at most.
fn vouch(response: &AxumResponse) -> Option<&str> {
    let mut values = response
        .headers()
        .get_all(NOT_APPLIED_CHECKED_HEADER)
        .iter();
    let first = values
        .next()
        .map(|value| value.to_str().expect("an ASCII value"));
    assert!(values.next().is_none(), "the gateway vouches once");
    first
}

#[test]
fn gateway_reuses_learned_stream_leader() {
    let gateway = Gateway::new(test_config(vec![
        "http://follower.test".to_owned(),
        "http://leader.test".to_owned(),
    ]));
    gateway.remember_leader("/bucket/stream".to_owned(), "http://leader.test".to_owned());

    let uri: Uri = "/bucket/stream?live=long-poll".parse().expect("uri");
    assert_eq!(
        gateway.pick_upstream(&uri).as_deref(),
        Some("http://leader.test")
    );
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.leader_cache_hits, 1);
    assert_eq!(metrics.leader_cache_entries, 1);
}

#[tokio::test]
async fn gateway_evicts_cached_leader_on_retryable_leader_unknown_response() {
    let stale = spawn_upstream(Router::new().route(
        "/bucket/stream",
        get(|| async {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                [(RETRY_AFTER.as_str(), "1")],
                "leader unknown",
            )
        }),
    ))
    .await;
    let gateway = Gateway::new(test_config(vec![stale.url.clone()]));
    gateway.remember_leader("/bucket/stream".to_owned(), stale.url.clone());

    let req = Request::builder()
        .method("GET")
        .uri("/bucket/stream")
        .body(Body::empty())
        .expect("request");
    let (parts, body) = req.into_parts();
    let body_bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .expect("request body");
    let response = gateway
        .forward(&stale.url, &parts, body_bytes, ResponseTail::default())
        .await
        .expect("gateway response");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.leader_cache_evictions, 1);
    assert_eq!(metrics.leader_cache_entries, 0);
}

fn get_stream() -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri("/bucket/stream")
        .body(Body::empty())
        .expect("request")
}

/// A cached leader whose node is gone (a deleted pod, a host that lost
/// power) is dropped from the cache on the transport failure, and the client
/// gets a retryable 503 instead of an empty 502, so its retry reaches a live
/// node.
#[tokio::test]
async fn gateway_drops_an_unreachable_cached_leader_and_answers_retryable_503() {
    let gone = refusing_upstream().await;
    let gateway = Gateway::new(test_config(vec![gone.clone()]));
    gateway.remember_leader("/bucket/stream".to_owned(), gone.clone());

    let response = gateway.handle(get_stream()).await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response
            .headers()
            .get(RETRY_AFTER)
            .map(HeaderValue::as_bytes),
        Some(&b"1"[..])
    );
    // The first hop failed to connect, so no node received the request.
    assert_eq!(not_applied(&response), Some("UpstreamUnreachable"));
    assert_eq!(vouch(&response), Some("1"));
    let body = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();
    assert!(!body.is_empty());
    assert!(!String::from_utf8_lossy(&body).contains(&gone));
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.leader_cache_evictions, 1);
    assert_eq!(metrics.leader_cache_entries, 0);
}

/// A port nothing listens on: connects are refused.
async fn refusing_upstream() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    format!("http://{}", listener.local_addr().expect("local addr"))
}

fn get_path(path: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(path)
        .body(Body::empty())
        .expect("request")
}

fn answering_router() -> Router {
    Router::new().route("/{bucket}/{stream}", any(|| async { "ok" }))
}

async fn answering_upstream() -> TestUpstream {
    spawn_upstream(answering_router()).await
}

/// Accepts requests and never answers them. Each request's arrival is sent
/// on the returned channel.
async fn silent_upstream() -> (TestUpstream, tokio::sync::mpsc::UnboundedReceiver<()>) {
    let (arrived, arrivals) = tokio::sync::mpsc::unbounded_channel();
    let upstream = spawn_upstream(Router::new().route(
        "/{bucket}/{stream}",
        any(move || {
            arrived.send(()).expect("the test awaits arrivals");
            std::future::pending::<&'static str>()
        }),
    ))
    .await;
    (upstream, arrivals)
}

/// Holds a paused clock still while requests wait on real sockets, so time
/// moves only by `tokio::time::advance`. Tokio otherwise advances a paused
/// clock to the next timer whenever the runtime idles, as it does while a
/// request waits on the loopback, but not while a blocking task runs. Needs
/// `start_paused = true`.
struct FrozenClock {
    _release: std::sync::mpsc::Sender<()>,
    _holder: tokio::task::JoinHandle<()>,
}

impl FrozenClock {
    fn hold() -> Self {
        let (release, released) = std::sync::mpsc::channel::<()>();
        let holder = tokio::task::spawn_blocking(move || {
            // Returns when `release` is dropped.
            let _released = released.recv();
        });
        Self {
            _release: release,
            _holder: holder,
        }
    }
}

/// Makes `gone` fail one request through `gateway`, along a cached route to
/// it, as a dead node's first failure does.
async fn fail_once(gateway: &Gateway, gone: &str) {
    gateway.remember_leader("/bucket/stream".to_owned(), gone.to_owned());
    let response = gateway.handle(get_stream()).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

/// `count` requests to distinct streams, each answered 200.
async fn all_answered(gateway: &Gateway, prefix: &str, count: usize) {
    for i in 0..count {
        let response = gateway
            .handle(get_path(&format!("/bucket/{prefix}{i}")))
            .await;
        assert_eq!(response.status(), StatusCode::OK, "{prefix}{i}");
    }
}

/// After an upstream fails at the transport, requests stop picking it at
/// random (#454): before, each one that did paid a connect timeout or a
/// dead pooled connection.
#[tokio::test(start_paused = true)]
async fn gateway_leaves_an_upstream_that_failed_out_of_selection() {
    let _frozen = FrozenClock::hold();
    let gone = refusing_upstream().await;
    let live = [answering_upstream().await, answering_upstream().await];
    let gateway = Gateway::new(test_config(vec![
        gone.clone(),
        live[0].url.clone(),
        live[1].url.clone(),
    ]));
    fail_once(&gateway, &gone).await;

    // 60 random picks among three would miss `gone` with probability (2/3)^60.
    all_answered(&gateway, "s", 60).await;
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.upstream_ejections, 1);
    assert_eq!(metrics.upstream_ejected, 1);
}

/// When its window expires, the next request that selects the upstream
/// probes it. Another failure ejects it for a doubled window.
#[tokio::test(start_paused = true)]
async fn gateway_probes_an_ejected_upstream_after_its_window() {
    let _frozen = FrozenClock::hold();
    let gone = refusing_upstream().await;
    let live = answering_upstream().await;
    let gateway = Gateway::new(test_config(vec![gone.clone(), live.url.clone()]));
    fail_once(&gateway, &gone).await;
    all_answered(&gateway, "s", 20).await;

    tokio::time::advance(EJECTION_BASE).await;
    assert_eq!(gateway.metrics_snapshot().upstream_ejected, 0);
    // The probe: a request along a cached route to it.
    fail_once(&gateway, &gone).await;
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.upstream_ejections, 2);
    assert_eq!(metrics.upstream_ejected, 1);

    // The second window is twice the first.
    let second_window = EJECTION_BASE.saturating_mul(2);
    tokio::time::advance(second_window.saturating_sub(Duration::from_secs(1))).await;
    all_answered(&gateway, "t", 20).await;
    assert_eq!(gateway.metrics_snapshot().upstream_ejected, 1);
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(gateway.metrics_snapshot().upstream_ejected, 0);
}

/// While one request probes an expired upstream, every other request skips
/// it, through a cached route or at random, until the probe deadline.
#[tokio::test(start_paused = true)]
async fn gateway_sends_one_probe_at_a_time() {
    let _frozen = FrozenClock::hold();
    let (silent, mut arrivals) = silent_upstream().await;
    let live = answering_upstream().await;
    let gateway = Arc::new(Gateway::new(test_config(vec![
        silent.url.clone(),
        live.url.clone(),
    ])));
    gateway.ejections.failed(&silent.url, Instant::now());
    tokio::time::advance(EJECTION_BASE).await;
    assert_eq!(gateway.metrics_snapshot().upstream_ejected, 0);

    // The probe: a request along a cached route to the silent upstream.
    gateway.remember_leader("/bucket/stream".to_owned(), silent.url.clone());
    let probe = tokio::spawn({
        let gateway = Arc::clone(&gateway);
        async move { gateway.handle(get_stream()).await }
    });
    arrivals
        .recv()
        .await
        .expect("the probe reaches the upstream");
    assert_eq!(gateway.metrics_snapshot().upstream_ejected, 1);

    all_answered(&gateway, "s", 40).await;
    gateway.remember_leader("/bucket/other".to_owned(), silent.url.clone());
    let response = gateway.handle(get_path("/bucket/other")).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(gateway.metrics_snapshot().upstream_ejected, 1);

    // The probe deadline is the 1 s connect timeout plus the 5 s user
    // timeout. Past it, the probe no longer holds the others back. The probe
    // itself gets a 504 at the 5 s header timeout, which ejects nothing.
    tokio::time::advance(Duration::from_secs(6)).await;
    assert_eq!(gateway.metrics_snapshot().upstream_ejected, 0);
    let probed = probe.await.expect("probe task");
    assert_eq!(probed.status(), StatusCode::GATEWAY_TIMEOUT);
    // The request reached the upstream, which may have applied it.
    assert_eq!(not_applied(&probed), None);
    assert_eq!(gateway.metrics_snapshot().upstream_ejections, 0);
    assert!(arrivals.try_recv().is_err(), "one probe only");
}

/// A response clears an ejection. Here the leader answers the request that
/// probes it after its window, along its cached route, which stays.
#[tokio::test(start_paused = true)]
async fn gateway_restores_an_upstream_that_answers_its_probe() {
    let _frozen = FrozenClock::hold();
    let (leader, follower) = spawn_raft_redirect_upstreams(answering_router()).await;
    let gateway = Gateway::new(test_config(vec![follower.url.clone(), leader.url.clone()]));
    gateway.ejections.failed(&leader.url, Instant::now());
    tokio::time::advance(EJECTION_BASE).await;
    gateway.remember_leader("/bucket/stream".to_owned(), leader.url.clone());

    let response = gateway.handle(get_stream()).await;

    assert_eq!(response.status(), StatusCode::OK);
    let metrics = gateway.metrics_snapshot();
    // Unanswered, the probe would still count.
    assert_eq!(metrics.upstream_ejected, 0);
    assert_eq!(metrics.leader_cache_entries, 1);
    assert_eq!(metrics.leader_cache_hits, 1);
    assert_eq!(metrics.leader_redirects, 0);
}

/// Inside its window an ejected leader is skipped, so a request lands on a
/// follower. The follower's 307 re-learns the route to the leader, and the
/// request that follows it acts as the leader's probe: its response clears
/// the ejection.
#[tokio::test(start_paused = true)]
async fn gateway_follows_a_redirect_to_an_ejected_leader_as_its_probe() {
    let _frozen = FrozenClock::hold();
    let (leader, follower) = spawn_raft_redirect_upstreams(answering_router()).await;
    let gateway = Gateway::new(test_config(vec![follower.url.clone(), leader.url.clone()]));
    gateway.ejections.failed(&leader.url, Instant::now());

    let response = gateway.handle(get_stream()).await;

    assert_eq!(response.status(), StatusCode::OK);
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.leader_redirects, 1);
    assert_eq!(metrics.leader_cache_entries, 1);
    assert_eq!(metrics.upstream_ejected, 0);

    // The next request goes along the re-learned route.
    let response = gateway.handle(get_stream()).await;
    assert_eq!(response.status(), StatusCode::OK);
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.leader_cache_hits, 1);
    assert_eq!(metrics.leader_redirects, 1);
}

/// The leader a follower redirects to is ejected when it fails, and the
/// follower, which answered, is not.
#[tokio::test(start_paused = true)]
async fn gateway_ejects_an_unreachable_redirect_leader() {
    let _frozen = FrozenClock::hold();
    let gone = refusing_upstream().await;
    let leader_url = format!("{gone}/bucket/stream");
    let follower = spawn_upstream(Router::new().route(
        "/bucket/stream",
        any(move || {
            let leader_url = leader_url.clone();
            async move {
                (
                    StatusCode::TEMPORARY_REDIRECT,
                    [
                        ("location", leader_url),
                        ("x-ursula-raft-leader-id", "2".to_owned()),
                    ],
                    "redirecting",
                )
            }
        }),
    ))
    .await;
    let gateway = Gateway::new(test_config(vec![follower.url.clone(), gone.clone()]));
    gateway.remember_leader("/bucket/stream".to_owned(), follower.url.clone());

    let response = gateway.handle(get_stream()).await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    // The follower may have proposed the request before it redirected.
    assert_eq!(not_applied(&response), None);
    assert_eq!(vouch(&response), Some("1"));
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.upstream_ejections, 1);
    assert_eq!(metrics.upstream_ejected, 1);
    // Every request now goes to the follower, which redirects again.
    for i in 0..20 {
        let response = gateway.handle(get_path(&format!("/bucket/r{i}"))).await;
        assert_ne!(response.status(), StatusCode::SERVICE_UNAVAILABLE, "r{i}");
    }
}

/// With every upstream ejected, requests still go out rather than nowhere.
#[tokio::test(start_paused = true)]
async fn gateway_still_sends_when_every_upstream_is_ejected() {
    let _frozen = FrozenClock::hold();
    let gone = refusing_upstream().await;
    let gateway = Gateway::new(test_config(vec![gone.clone()]));
    fail_once(&gateway, &gone).await;

    let response = gateway.handle(get_path("/bucket/again")).await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(gateway.metrics_snapshot().upstream_ejections, 1);
}

/// With every upstream ejected, a request keeps its cached route rather
/// than dropping it for a random pick.
#[test]
fn gateway_keeps_the_cached_route_when_every_upstream_is_ejected() {
    let gateway = Gateway::new(test_config(vec![
        "http://follower.test".to_owned(),
        "http://leader.test".to_owned(),
    ]));
    let now = Instant::now();
    gateway.ejections.failed("http://follower.test", now);
    gateway.ejections.failed("http://leader.test", now);
    gateway.remember_leader("/bucket/stream".to_owned(), "http://leader.test".to_owned());

    let uri: Uri = "/bucket/stream".parse().expect("uri");
    assert_eq!(
        gateway.pick_upstream(&uri).as_deref(),
        Some("http://leader.test")
    );
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.leader_cache_hits, 1);
    assert_eq!(metrics.leader_cache_evictions, 0);
    assert_eq!(metrics.leader_cache_entries, 1);
}

/// A cached route to an ejected upstream is not followed: the request picks
/// a live one.
#[tokio::test(start_paused = true)]
async fn gateway_drops_a_cached_route_to_an_ejected_upstream() {
    let _frozen = FrozenClock::hold();
    let gone = refusing_upstream().await;
    let live = answering_upstream().await;
    let gateway = Gateway::new(test_config(vec![gone.clone(), live.url.clone()]));
    fail_once(&gateway, &gone).await;
    gateway.remember_leader("/bucket/stream".to_owned(), gone.clone());

    let response = gateway.handle(get_stream()).await;

    assert_eq!(response.status(), StatusCode::OK);
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.leader_cache_entries, 0);
    assert_eq!(metrics.upstream_ejections, 1);
}

/// A request that could not be built never left the gateway, so its
/// upstream stays in selection.
#[tokio::test]
async fn gateway_does_not_eject_an_upstream_for_a_request_it_could_not_build() {
    let gateway = Gateway::new(test_config(vec!["not a url".to_owned()]));

    let response = gateway.handle(get_stream()).await;

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    // Upstream failures carry no marker. The same `502` answers an exchange
    // that broke after the request reached the upstream.
    assert_eq!(not_applied(&response), None);
    assert_eq!(vouch(&response), Some("1"));
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.upstream_ejections, 0);
    assert_eq!(metrics.upstream_ejected, 0);
}

/// A cached leader that accepts the request and never answers is dropped
/// too. The request may have reached it, so the answer is a 504, not a
/// retry invitation. The upstream stays in selection: one slow group on a
/// live node looks the same.
#[tokio::test]
async fn gateway_drops_a_silent_cached_leader_and_answers_gateway_timeout() {
    let silent = spawn_upstream(
        Router::new().route("/bucket/stream", get(std::future::pending::<&'static str>)),
    )
    .await;
    let gateway = gateway_with_response_header_timeout(&silent.url, Duration::from_millis(200));
    gateway.remember_leader("/bucket/stream".to_owned(), silent.url.clone());

    let response = gateway.handle(get_stream()).await;

    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(not_applied(&response), None);
    assert_eq!(vouch(&response), Some("1"));
    let metrics = gateway.metrics_snapshot();
    assert_eq!(metrics.leader_cache_evictions, 1);
    assert_eq!(metrics.leader_cache_entries, 0);
    assert_eq!(metrics.upstream_ejections, 0);
    assert_eq!(metrics.upstream_ejected, 0);
}

#[test]
fn upstream_pin_key_ignores_subresource_and_internal_routes() {
    let bootstrap: Uri = "/bucket/stream/bootstrap".parse().expect("uri");
    let metrics: Uri = "/__ursula/gateway/metrics".parse().expect("uri");

    assert_eq!(
        upstream_pin_key(&bootstrap, None).as_deref(),
        Some("/bucket/stream")
    );
    assert_eq!(upstream_pin_key(&metrics, None), None);
}

#[test]
fn group_pin_key_is_shared_by_streams_in_the_same_group() {
    let shard_map = StaticShardMap::new(1, 16).expect("valid shard map");
    let first: Uri = "/bucket/stream-1".parse().expect("uri");
    let first_key = upstream_pin_key(&first, Some(&shard_map)).expect("first key");
    let second = (2..10_000)
        .map(|index| {
            format!("/bucket/stream-{index}")
                .parse::<Uri>()
                .expect("uri")
        })
        .find(|uri| upstream_pin_key(uri, Some(&shard_map)).as_ref() == Some(&first_key))
        .expect("stream in same group");

    assert_ne!(first.path(), second.path());
    assert_eq!(
        upstream_pin_key(&second, Some(&shard_map)).as_deref(),
        Some(first_key.as_str())
    );

    let mut config = test_config(vec![
        "http://follower.test".to_owned(),
        "http://leader.test".to_owned(),
    ]);
    config.raft_group_count = Some(16);
    let gateway = Gateway::new(config);
    gateway.remember_leader(first_key, "http://leader.test".to_owned());
    assert_eq!(
        gateway.pick_upstream(&second).as_deref(),
        Some("http://leader.test")
    );
    assert_eq!(gateway.metrics_snapshot().leader_cache_hits, 1);
}

fn gateway_with_response_header_timeout(
    upstream_url: impl Into<String>,
    response_header_timeout: Duration,
) -> Arc<Gateway> {
    let mut config = test_config(vec![upstream_url.into()]);
    config.response_header_timeout = response_header_timeout;
    Arc::new(Gateway::new(config))
}

#[derive(Debug)]
struct FixedPrincipalResolver {
    calls: AtomicUsize,
    result: Result<VerifiedPrincipal, AuthenticationError>,
}

impl FixedPrincipalResolver {
    fn valid() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            result: Ok(VerifiedPrincipal {
                issuer: "https://issuer.example".to_owned(),
                subject: "user-1".to_owned(),
                client_id: "client-1".to_owned(),
                scopes: auth::parse_scope("streams:read streams:write"),
                issued_at: 1,
                expires_at: u64::MAX,
                token_id: "token-1".to_owned(),
            }),
        }
    }
}

impl PrincipalResolver for FixedPrincipalResolver {
    fn resolve<'a>(&'a self, bearer_token: &'a str) -> PrincipalResolverFuture<'a> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let result = if bearer_token == "valid-token" {
            self.result.clone()
        } else {
            Err(AuthenticationError::InvalidCredential)
        };
        Box::pin(async move { result })
    }
}

#[derive(Debug)]
struct RecordingAuthorizer {
    decision: Result<AuthorizationDecision, AuthorizationError>,
    requests: Mutex<Vec<AuthorizationRequest>>,
}

impl RecordingAuthorizer {
    fn new(decision: AuthorizationDecision) -> Self {
        Self {
            decision: Ok(decision),
            requests: Mutex::new(Vec::new()),
        }
    }
}

impl Authorizer for RecordingAuthorizer {
    fn authorize<'a>(&'a self, request: AuthorizationRequest) -> AuthorizationFuture<'a> {
        self.requests
            .lock()
            .expect("authorization request lock")
            .push(request);
        let decision = self.decision.clone();
        Box::pin(async move { decision })
    }
}

fn gateway_with_access_control(
    upstream_url: impl Into<String>,
    resolver: Arc<FixedPrincipalResolver>,
    authorizer: Arc<RecordingAuthorizer>,
) -> Gateway {
    Gateway::with_access_control(
        test_config(vec![upstream_url.into()]),
        AccessControl::new(resolver, authorizer),
    )
}

#[test]
fn request_classifier_maps_durable_stream_routes_to_bucket_resources() {
    let cases = [
        ("PUT", "/owner-a", Action::AdministerBucket, None),
        ("PUT", "/owner-a/orders", Action::Create, Some("orders")),
        ("POST", "/owner-a/orders", Action::Append, Some("orders")),
        ("GET", "/owner-a/orders", Action::Read, Some("orders")),
        (
            "GET",
            "/owner-a/orders?offset=now&live=sse",
            Action::Tail,
            Some("orders"),
        ),
        ("HEAD", "/owner-a/orders", Action::Head, Some("orders")),
        ("DELETE", "/owner-a/orders", Action::Delete, Some("orders")),
        (
            "GET",
            "/owner-a/orders/snapshot",
            Action::ReadSnapshot,
            Some("orders"),
        ),
        (
            "PUT",
            "/owner-a/orders/snapshot/42",
            Action::PublishSnapshot,
            Some("orders"),
        ),
    ];

    for (method, uri, expected_action, expected_stream) in cases {
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::empty())
            .expect("request");
        let classified = classify_request(request.method(), request.uri(), request.headers())
            .expect("classified Durable Streams request");
        assert_eq!(classified.resource.bucket_id, "owner-a");
        assert_eq!(
            classified.resource.stream_id.as_deref(),
            expected_stream,
            "{method} {uri}"
        );
        assert_eq!(classified.action, expected_action, "{method} {uri}");
    }
}

#[test]
fn request_classifier_distinguishes_final_writes() {
    for (method, expected_action) in [
        ("PUT", Action::CreateAndClose),
        ("POST", Action::AppendAndClose),
    ] {
        let request = Request::builder()
            .method(method)
            .uri("/owner-a/orders")
            .header(HEADER_STREAM_CLOSED, "true")
            .body(Body::empty())
            .expect("request");

        let classified = classify_request(request.method(), request.uri(), request.headers())
            .expect("classified final write");

        assert_eq!(classified.action, expected_action);
    }
}

#[test]
fn request_classifier_decodes_resource_path_segments() {
    let request = Request::builder()
        .method("GET")
        .uri("/owner-a/hello%20world")
        .body(Body::empty())
        .expect("request");

    let classified = classify_request(request.method(), request.uri(), request.headers())
        .expect("classified request");

    assert_eq!(
        classified.resource.stream_id.as_deref(),
        Some("hello world")
    );
}

#[tokio::test]
async fn gateway_handle_returns_service_unavailable_without_upstreams() {
    let gateway = Gateway::new(test_config(Vec::new()));
    let req = Request::builder()
        .method("GET")
        .uri("/bucket/stream")
        .body(Body::empty())
        .unwrap();

    let resp = gateway.handle(req).await;

    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(not_applied(&resp), Some("NoUpstream"));
}

#[tokio::test]
async fn gateway_without_access_control_preserves_existing_pass_through_behavior() {
    let upstream = spawn_upstream(Router::new().route(
        "/bucket/stream",
        get(|headers: HeaderMap| async move {
            headers
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("missing")
                .to_owned()
        }),
    ))
    .await;
    let gateway = Gateway::new(test_config(vec![upstream.url.clone()]));
    let request = Request::builder()
        .method("GET")
        .uri("/bucket/stream")
        .header(AUTHORIZATION, "Bearer existing-client-token")
        .body(Body::empty())
        .expect("request");

    let response = gateway.handle(request).await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();
    assert_eq!(&body[..], b"Bearer existing-client-token");
}

#[tokio::test]
async fn access_control_allows_anonymous_public_resource_without_resolving_token() {
    let upstream =
        spawn_upstream(Router::new().route("/public/events", get(|| async { StatusCode::OK })))
            .await;
    let resolver = Arc::new(FixedPrincipalResolver::valid());
    let authorizer = Arc::new(RecordingAuthorizer::new(AuthorizationDecision::Allow));
    let gateway = gateway_with_access_control(
        upstream.url.clone(),
        Arc::clone(&resolver),
        Arc::clone(&authorizer),
    );
    let request = Request::builder()
        .method("GET")
        .uri("/public/events")
        .body(Body::empty())
        .expect("request");

    let response = gateway.handle(request).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(resolver.calls.load(Ordering::Relaxed), 0);
    let requests = authorizer.requests.lock().expect("authorization requests");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].principal, None);
    assert_eq!(requests[0].resource.bucket_id, "public");
    assert_eq!(requests[0].resource.stream_id.as_deref(), Some("events"));
    assert_eq!(requests[0].action, Action::Read);
}

#[tokio::test]
async fn access_control_resolves_bearer_and_does_not_forward_it_upstream() {
    let upstream = spawn_upstream(Router::new().route(
        "/owner-a/events",
        get(|headers: HeaderMap| async move {
            if headers.contains_key(AUTHORIZATION) {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::OK
            }
        }),
    ))
    .await;
    let resolver = Arc::new(FixedPrincipalResolver::valid());
    let authorizer = Arc::new(RecordingAuthorizer::new(AuthorizationDecision::Allow));
    let gateway = gateway_with_access_control(
        upstream.url.clone(),
        Arc::clone(&resolver),
        Arc::clone(&authorizer),
    );
    let request = Request::builder()
        .method("GET")
        .uri("/owner-a/events")
        .header(AUTHORIZATION, "Bearer valid-token")
        .body(Body::empty())
        .expect("request");

    let response = gateway.handle(request).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(resolver.calls.load(Ordering::Relaxed), 1);
    let requests = authorizer.requests.lock().expect("authorization requests");
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0]
            .principal
            .as_ref()
            .map(|principal| principal.subject.as_str()),
        Some("user-1")
    );
}

#[tokio::test]
async fn access_control_conceals_private_resource_before_forwarding() {
    let hits = Arc::new(AtomicUsize::new(0));
    let upstream_hits = Arc::clone(&hits);
    let upstream = spawn_upstream(Router::new().route(
        "/private/events",
        get(move || {
            let upstream_hits = Arc::clone(&upstream_hits);
            async move {
                upstream_hits.fetch_add(1, Ordering::Relaxed);
                StatusCode::OK
            }
        }),
    ))
    .await;
    let resolver = Arc::new(FixedPrincipalResolver::valid());
    let authorizer = Arc::new(RecordingAuthorizer::new(
        AuthorizationDecision::ConcealAsNotFound,
    ));
    let gateway = gateway_with_access_control(upstream.url.clone(), resolver, authorizer);
    let request = Request::builder()
        .method("GET")
        .uri("/private/events")
        .body(Body::empty())
        .expect("request");

    let response = gateway.handle(request).await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(not_applied(&response), Some("NotFound"));
    assert_eq!(hits.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn access_control_rejects_invalid_bearer_before_authorization() {
    let resolver = Arc::new(FixedPrincipalResolver::valid());
    let authorizer = Arc::new(RecordingAuthorizer::new(AuthorizationDecision::Allow));
    let gateway =
        gateway_with_access_control("http://127.0.0.1:1", resolver, Arc::clone(&authorizer));
    let request = Request::builder()
        .method("GET")
        .uri("/owner-a/events")
        .header(AUTHORIZATION, "Basic not-a-bearer")
        .body(Body::empty())
        .expect("request");

    let response = gateway.handle(request).await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response
            .headers()
            .get(WWW_AUTHENTICATE)
            .expect("authenticate challenge"),
        r#"Bearer error="invalid_token""#
    );
    assert_eq!(not_applied(&response), Some("InvalidCredential"));
    assert!(
        authorizer
            .requests
            .lock()
            .expect("authorization requests")
            .is_empty()
    );
}

#[tokio::test]
async fn access_control_fails_closed_for_unclassified_routes() {
    let hits = Arc::new(AtomicUsize::new(0));
    let upstream_hits = Arc::clone(&hits);
    let upstream = spawn_upstream(Router::new().fallback(any(move || {
        let upstream_hits = Arc::clone(&upstream_hits);
        async move {
            upstream_hits.fetch_add(1, Ordering::Relaxed);
            StatusCode::OK
        }
    })))
    .await;
    let resolver = Arc::new(FixedPrincipalResolver::valid());
    let authorizer = Arc::new(RecordingAuthorizer::new(AuthorizationDecision::Allow));
    let gateway = gateway_with_access_control(upstream.url.clone(), resolver, authorizer);

    for uri in [
        "/__ursula/metrics",
        "/future-unclassified-route",
        // Three-segment paths that are not subresources (0.6.0 removed affinity).
        "/owner-a/run-42/queue",
    ] {
        let request = Request::builder()
            .method("GET")
            .uri(uri)
            .body(Body::empty())
            .expect("request");
        let response = gateway.handle(request).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{uri}");
        assert_eq!(not_applied(&response), Some("NotFound"), "{uri}");
    }
    assert_eq!(hits.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn gateway_rejects_body_larger_than_configured_limit_before_forwarding() {
    let hits = Arc::new(AtomicUsize::new(0));
    let app_hits = Arc::clone(&hits);
    let upstream = spawn_upstream(Router::new().route(
        "/bucket/stream",
        post(move || {
            let app_hits = Arc::clone(&app_hits);
            async move {
                app_hits.fetch_add(1, Ordering::Relaxed);
                StatusCode::OK
            }
        }),
    ))
    .await;

    let mut config = test_config(vec![upstream.url.clone()]);
    config.max_request_body_bytes = 4;
    let gateway = Gateway::new(config);
    let req = Request::builder()
        .method("POST")
        .uri("/bucket/stream")
        .body(Body::from(bytes::Bytes::from_static(b"12345")))
        .unwrap();

    let resp = gateway.handle(req).await;

    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(not_applied(&resp), Some("RequestBodyTooLarge"));
    assert_eq!(hits.load(Ordering::Relaxed), 0);
}

/// A request body that fails while the gateway buffers it, as when the
/// client goes away mid-upload, is answered `400` before anything is
/// forwarded.
#[tokio::test]
async fn gateway_refuses_an_unreadable_body_before_forwarding() {
    let hits = Arc::new(AtomicUsize::new(0));
    let upstream_hits = Arc::clone(&hits);
    let upstream = spawn_upstream(Router::new().fallback(any(move || {
        let upstream_hits = Arc::clone(&upstream_hits);
        async move {
            upstream_hits.fetch_add(1, Ordering::Relaxed);
            StatusCode::OK
        }
    })))
    .await;
    let gateway = gateway_for_url(upstream.url.clone());
    let body = Body::from_stream(tokio_stream::iter([Err::<bytes::Bytes, _>(
        std::io::Error::other("client went away"),
    )]));
    let request = Request::builder()
        .method("POST")
        .uri("/bucket/stream")
        .body(body)
        .expect("request");

    let response = gateway.handle(request).await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(not_applied(&response), Some("RequestBodyUnreadable"));
    assert_eq!(hits.load(Ordering::Relaxed), 0);
}

/// A token issuer or an authorizer that is down is answered `503` before
/// anything is forwarded, so the refusal, like a denial, is marked as having
/// applied nothing.
#[tokio::test]
async fn access_control_outages_and_denials_are_marked_not_applied() {
    let hits = Arc::new(AtomicUsize::new(0));
    let upstream_hits = Arc::clone(&hits);
    let upstream = spawn_upstream(Router::new().fallback(any(move || {
        let upstream_hits = Arc::clone(&upstream_hits);
        async move {
            upstream_hits.fetch_add(1, Ordering::Relaxed);
            StatusCode::OK
        }
    })))
    .await;
    let issuer_down = Arc::new(FixedPrincipalResolver {
        calls: AtomicUsize::new(0),
        result: Err(AuthenticationError::Unavailable),
    });
    let authorizer_down = Arc::new(RecordingAuthorizer {
        decision: Err(AuthorizationError::Unavailable),
        requests: Mutex::new(Vec::new()),
    });
    let allow = || Arc::new(RecordingAuthorizer::new(AuthorizationDecision::Allow));
    let valid = || Arc::new(FixedPrincipalResolver::valid());
    let cases = [
        (
            "token issuer down",
            gateway_with_access_control(upstream.url.clone(), issuer_down, allow()),
            StatusCode::SERVICE_UNAVAILABLE,
            "AuthenticationUnavailable",
        ),
        (
            "authorizer down",
            gateway_with_access_control(upstream.url.clone(), valid(), authorizer_down),
            StatusCode::SERVICE_UNAVAILABLE,
            "AuthorizationUnavailable",
        ),
        (
            "denied",
            gateway_with_access_control(
                upstream.url.clone(),
                valid(),
                Arc::new(RecordingAuthorizer::new(AuthorizationDecision::Deny)),
            ),
            StatusCode::FORBIDDEN,
            "Forbidden",
        ),
    ];

    for (case, gateway, status, token) in cases {
        let request = Request::builder()
            .method("POST")
            .uri("/owner-a/events")
            .header(AUTHORIZATION, "Bearer valid-token")
            .body(Body::from("abc"))
            .expect("request");
        let response = gateway.handle(request).await;
        assert_eq!(response.status(), status, "{case}");
        assert_eq!(not_applied(&response), Some(token), "{case}");
    }
    assert_eq!(hits.load(Ordering::Relaxed), 0);
}

fn post_path(path: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .body(Body::from("abc"))
        .expect("request")
}

/// An upstream's marker reaches the client unchanged, and the gateway adds
/// none to an upstream answer without one.
#[tokio::test]
async fn an_upstream_marker_passes_through_unchanged() {
    let upstream = spawn_upstream(
        Router::new()
            .route(
                "/bucket/refused",
                post(|| async {
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        [
                            (RETRY_AFTER.as_str(), "1"),
                            (NOT_APPLIED_HEADER, "RaftLogPressure"),
                        ],
                        r#"{"error":"RaftLogPressure"}"#,
                    )
                }),
            )
            .route(
                "/bucket/unknown",
                post(|| async {
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        [(RETRY_AFTER.as_str(), "1")],
                        "outcome unknown",
                    )
                }),
            ),
    )
    .await;
    let gateway = gateway_for_url(upstream.url.clone());

    let refused = gateway.handle(post_path("/bucket/refused")).await;
    assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(not_applied(&refused), Some("RaftLogPressure"));
    assert_eq!(vouch(&refused), Some("1"));

    // Vouched but unmarked: proves nothing.
    let unknown = gateway.handle(post_path("/bucket/unknown")).await;
    assert_eq!(unknown.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(not_applied(&unknown), None);
    assert_eq!(vouch(&unknown), Some("1"));
}

/// Headers with each of `values` as an `x-ursula-not-applied` value.
fn marked(values: &[&'static str]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for value in values {
        headers.append(NOT_APPLIED_HEADER, HeaderValue::from_static(value));
    }
    headers
}

/// A Raft leader redirect to `location`, with `headers` added.
fn raft_redirect(location: &str, mut headers: HeaderMap) -> (StatusCode, HeaderMap) {
    headers.insert(
        LOCATION,
        HeaderValue::from_str(location).expect("redirect target"),
    );
    headers.insert(HEADER_URSULA_RAFT_LEADER_ID, HeaderValue::from_static("1"));
    (StatusCode::TEMPORARY_REDIRECT, headers)
}

/// An upstream that answers every request on `/bucket/stream` with `status`
/// and `headers`.
async fn spawn_answering(status: StatusCode, headers: HeaderMap) -> TestUpstream {
    spawn_upstream(Router::new().route(
        "/bucket/stream",
        any(move || {
            let headers = headers.clone();
            async move { (status, headers, "answer") }
        }),
    ))
    .await
}

/// A leader address that is not one of the gateway's upstreams.
const UNKNOWN_LEADER: &str = "http://192.0.2.1:4437/bucket/stream";

/// A node can redirect a write after it proposed it (OpenRaft reports the
/// forward after `client_write`), so after a followed redirect the gateway
/// keeps the leader's marker only when the redirect carried one too. It never
/// moves the redirect's own marker onto the leader's answer, an upstream's
/// vouch counts for nothing, and every answer leaves with the gateway's vouch.
#[tokio::test]
async fn a_followed_redirect_keeps_the_leader_marker_only_when_the_redirect_carried_one() {
    /// One followed redirect: what the follower's redirect carries, what the
    /// leader answers, and the markers that reach the client.
    struct Case {
        name: &'static str,
        redirect: HeaderMap,
        leader: (StatusCode, HeaderMap),
        expected: &'static [&'static str],
    }
    let vouched_only = || {
        let mut headers = HeaderMap::new();
        headers.insert(NOT_APPLIED_CHECKED_HEADER, HeaderValue::from_static("1"));
        headers
    };
    let refused = |values: &[&'static str]| (StatusCode::SERVICE_UNAVAILABLE, marked(values));
    let cases = [
        Case {
            name: "unmarked redirect, marked refusal",
            redirect: marked(&[]),
            leader: refused(&["RaftLogPressure"]),
            expected: &[],
        },
        Case {
            name: "marked redirect, marked refusal",
            redirect: marked(&["NotLeader"]),
            leader: refused(&["RaftLogPressure"]),
            expected: &["RaftLogPressure"],
        },
        Case {
            name: "marked redirect, unmarked 503",
            redirect: marked(&["NotLeader"]),
            leader: refused(&[]),
            expected: &[],
        },
        Case {
            name: "unmarked redirect, duplicate markers",
            redirect: marked(&[]),
            leader: refused(&["RaftLogPressure", "WalDiskPressure"]),
            expected: &[],
        },
        Case {
            name: "duplicate redirect markers, duplicate markers",
            redirect: marked(&["NotLeader", "NotLeader"]),
            leader: refused(&["RaftLogPressure", "WalDiskPressure"]),
            expected: &["RaftLogPressure", "WalDiskPressure"],
        },
        Case {
            name: "vouched but unmarked redirect, marked refusal",
            redirect: vouched_only(),
            leader: refused(&["RaftLogPressure"]),
            expected: &[],
        },
        Case {
            name: "unmarked redirect, the leader redirects",
            redirect: marked(&[]),
            leader: raft_redirect(UNKNOWN_LEADER, marked(&["NotLeader"])),
            expected: &[],
        },
        Case {
            name: "marked redirect, the leader redirects",
            redirect: marked(&["NotLeader"]),
            leader: raft_redirect(UNKNOWN_LEADER, marked(&["NotLeader"])),
            expected: &["NotLeader"],
        },
    ];
    for Case {
        name,
        redirect,
        leader: (leader_status, leader_headers),
        expected,
    } in cases
    {
        let leader = spawn_answering(leader_status, leader_headers).await;
        let (status, headers) = raft_redirect(&format!("{}/bucket/stream", leader.url), redirect);
        let follower = spawn_answering(status, headers).await;
        let gateway = Gateway::new(test_config(vec![follower.url.clone(), leader.url.clone()]));
        gateway.remember_leader("/bucket/stream".to_owned(), follower.url.clone());

        let response = gateway.handle(post_path("/bucket/stream")).await;

        assert_eq!(response.status(), leader_status, "{name}");
        assert_eq!(gateway.metrics_snapshot().leader_redirects, 1, "{name}");
        assert_eq!(markers(&response), expected, "{name}");
        assert_eq!(vouch(&response), Some("1"), "{name}");
        if leader_status == StatusCode::TEMPORARY_REDIRECT {
            // The leader's own redirect is not followed again. It reaches the
            // client reduced to its path, without the Raft leader id.
            assert_eq!(
                response.headers().get(LOCATION).expect("location"),
                "/bucket/stream",
                "{name}"
            );
            assert!(
                response
                    .headers()
                    .get(HEADER_URSULA_RAFT_LEADER_ID)
                    .is_none(),
                "{name}"
            );
        }
    }
}

/// A redirect the gateway does not follow, here because its leader is not an
/// upstream, is the only exchange of the attempt. Its marker reaches the
/// client unchanged, with the vouch. Following it is a new attempt.
#[tokio::test]
async fn a_redirect_the_gateway_passes_through_keeps_its_marker() {
    let cases: [(&str, &[&'static str]); 2] = [("marked", &["NotLeader"]), ("unmarked", &[])];
    for (case, values) in cases {
        let (status, headers) = raft_redirect(UNKNOWN_LEADER, marked(values));
        let node = spawn_answering(status, headers).await;
        let gateway = gateway_for_url(node.url.clone());

        let response = gateway.handle(post_path("/bucket/stream")).await;

        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT, "{case}");
        assert_eq!(gateway.metrics_snapshot().leader_redirects, 0, "{case}");
        assert_eq!(
            response.headers().get(LOCATION).expect("location"),
            "/bucket/stream",
            "{case}"
        );
        assert_eq!(markers(&response), values, "{case}");
        assert_eq!(vouch(&response), Some("1"), "{case}");
    }
}

/// A connect failure on the first hop means that no node received the
/// request, so its `503` is marked. On the leader hop of a followed redirect
/// it is not, whether or not the redirect was marked: the gateway does not
/// combine a redirect's marker with a failed second exchange.
#[tokio::test]
async fn a_first_hop_connect_failure_is_marked_and_a_leader_hop_one_is_not() {
    let gone = refusing_upstream().await;

    let first_hop = gateway_for_url(gone.clone())
        .handle(post_path("/bucket/stream"))
        .await;

    assert_eq!(first_hop.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(not_applied(&first_hop), Some("UpstreamUnreachable"));
    assert_eq!(vouch(&first_hop), Some("1"));

    let cases: [(&str, &[&'static str]); 2] = [
        ("unmarked redirect", &[]),
        ("marked redirect", &["NotLeader"]),
    ];
    for (case, values) in cases {
        let (status, headers) = raft_redirect(&format!("{gone}/bucket/stream"), marked(values));
        let follower = spawn_answering(status, headers).await;
        let gateway = Gateway::new(test_config(vec![follower.url.clone(), gone.clone()]));
        gateway.remember_leader("/bucket/stream".to_owned(), follower.url.clone());

        let leader_hop = gateway.handle(post_path("/bucket/stream")).await;

        assert_eq!(
            leader_hop.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "{case}"
        );
        assert_eq!(gateway.metrics_snapshot().leader_redirects, 1, "{case}");
        assert_eq!(not_applied(&leader_hop), None, "{case}");
        assert_eq!(vouch(&leader_hop), Some("1"), "{case}");
    }
}

/// Accepts one connection, reads one whole request from it, the head and the
/// `Content-Length` body, and closes it without answering, as a node that
/// went away after it received a write does. The bytes it read are sent on
/// the returned channel.
async fn hanging_up_upstream() -> (TestUpstream, tokio::sync::oneshot::Receiver<Vec<u8>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind upstream");
    let url = format!("http://{}", listener.local_addr().expect("local addr"));
    let (read, request) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut connection, _peer) = listener.accept().await.expect("accept");
        let mut bytes = Vec::new();
        while !is_whole_request(&bytes) {
            let mut chunk = [0_u8; 1024];
            let len = tokio::io::AsyncReadExt::read(&mut connection, &mut chunk)
                .await
                .expect("read the request");
            assert_ne!(len, 0, "the gateway closed before it sent the request");
            bytes.extend_from_slice(&chunk[..len]);
        }
        drop(connection);
        read.send(bytes).expect("the test awaits the request");
    });
    (TestUpstream { url, task }, request)
}

/// Whether `bytes` hold a whole request: its head and as many body bytes as
/// its `Content-Length` names.
fn is_whole_request(bytes: &[u8]) -> bool {
    let Some(head_len) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
        return false;
    };
    let head = std::str::from_utf8(&bytes[..head_len]).expect("an ASCII request head");
    let body_len = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().expect("a Content-Length"))
        })
        .unwrap_or(0);
    bytes.len() >= head_len.saturating_add(4).saturating_add(body_len)
}

/// A first-hop failure after the request was handed to a connection stays
/// unmarked: the node may have received the request and proposed it. Here
/// the upstream reads the whole request, body included, and closes the
/// connection without answering.
#[tokio::test]
async fn a_first_hop_failure_after_the_request_was_sent_is_not_marked() {
    let (node, request) = hanging_up_upstream().await;

    let response = gateway_for_url(node.url.clone())
        .handle(post_path("/bucket/stream"))
        .await;

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(not_applied(&response), None);
    assert_eq!(vouch(&response), Some("1"));
    let request = request.await.expect("the upstream read a request");
    let request = String::from_utf8(request).expect("an ASCII request");
    assert!(request.starts_with("POST /bucket/stream "), "{request}");
    assert!(request.ends_with("\r\n\r\nabc"), "{request}");
}

/// Every response the gateway returns carries its vouch once: a forwarded
/// answer, its own refusals and errors, a CORS preflight and its metrics.
#[tokio::test]
async fn the_gateway_vouches_for_every_response() {
    let node = spawn_upstream(
        Router::new()
            .route("/bucket/ok", any(|| async { "ok" }))
            .route(
                "/bucket/refused",
                any(|| async {
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        [(NOT_APPLIED_HEADER, "RaftLogPressure")],
                        "refused",
                    )
                }),
            ),
    )
    .await;
    let gone = refusing_upstream().await;
    let mut small_bodies = test_config(vec![node.url.clone()]);
    small_bodies.max_request_body_bytes = 2;
    let mut cors = test_config(vec![node.url.clone()]);
    cors.cors_allowed_origins = vec!["https://app.example".to_owned()];
    let preflight = Request::builder()
        .method("OPTIONS")
        .uri("/bucket/ok")
        .header("origin", "https://app.example")
        .header("access-control-request-method", "POST")
        .body(Body::empty())
        .expect("request");
    let forwarding = gateway_for_url(node.url.clone());

    let cases = [
        (
            "forwarded",
            forwarding.handle(post_path("/bucket/ok")).await,
            StatusCode::OK,
            None,
        ),
        (
            "forwarded refusal",
            forwarding.handle(post_path("/bucket/refused")).await,
            StatusCode::SERVICE_UNAVAILABLE,
            Some("RaftLogPressure"),
        ),
        (
            "own refusal",
            Gateway::new(small_bodies)
                .handle(post_path("/bucket/ok"))
                .await,
            StatusCode::PAYLOAD_TOO_LARGE,
            Some("RequestBodyTooLarge"),
        ),
        (
            "own error, upstream unreachable",
            gateway_for_url(gone).handle(post_path("/bucket/ok")).await,
            StatusCode::SERVICE_UNAVAILABLE,
            Some("UpstreamUnreachable"),
        ),
        (
            "own error, request not built",
            gateway_for_url("not a url")
                .handle(post_path("/bucket/ok"))
                .await,
            StatusCode::BAD_GATEWAY,
            None,
        ),
        (
            "preflight",
            Gateway::new(cors).handle(preflight).await,
            StatusCode::NO_CONTENT,
            None,
        ),
        (
            "metrics",
            forwarding.handle(get_path(GATEWAY_METRICS_PATH)).await,
            StatusCode::OK,
            None,
        ),
    ];
    for (case, response, status, marker) in cases {
        assert_eq!(response.status(), status, "{case}");
        assert_eq!(not_applied(&response), marker, "{case}");
        assert_eq!(vouch(&response), Some("1"), "{case}");
    }
}

/// The vouch is the gateway's own statement. An upstream's is dropped
/// wherever the gateway copies headers, so the client sees only the
/// gateway's, once.
#[tokio::test]
async fn an_upstream_vouch_never_reaches_the_client() {
    let mut headers = marked(&["RaftLogPressure"]);
    headers.append(NOT_APPLIED_CHECKED_HEADER, HeaderValue::from_static("0"));
    headers.append(NOT_APPLIED_CHECKED_HEADER, HeaderValue::from_static("1"));
    let node = spawn_answering(StatusCode::SERVICE_UNAVAILABLE, headers).await;
    let gateway = gateway_for_url(node.url.clone());

    // Everything but the vouch: the upstream's is gone.
    let (parts, _body) = post_path("/bucket/stream").into_parts();
    let forwarded = gateway
        .forward(
            &node.url,
            &parts,
            bytes::Bytes::from_static(b"abc"),
            ResponseTail::default(),
        )
        .await
        .expect("forwarded");
    assert_eq!(markers(&forwarded), ["RaftLogPressure"]);
    assert!(!forwarded.headers().contains_key(NOT_APPLIED_CHECKED_HEADER));

    let response = gateway.handle(post_path("/bucket/stream")).await;
    assert_eq!(markers(&response), ["RaftLogPressure"]);
    assert_eq!(vouch(&response), Some("1"));
}

#[tokio::test]
async fn gateway_follows_leader_redirect_for_get_request() {
    let leader_app = Router::new().route("/bucket/stream", get(|| async { StatusCode::OK }));
    let (leader, follower) = spawn_raft_redirect_upstreams(leader_app).await;
    let gateway = Arc::new(Gateway::new(test_config(vec![
        follower.url.clone(),
        leader.url.clone(),
    ])));

    let req = Request::builder()
        .method("GET")
        .uri("/bucket/stream")
        .header("authorization", "Bearer secret")
        .body(Body::empty())
        .unwrap();

    let (parts, body) = req.into_parts();
    let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let resp = gateway
        .forward(&follower.url, &parts, body_bytes, ResponseTail::default())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn gateway_follows_leader_redirect_for_put_request() {
    let leader_app = Router::new().route("/bucket/stream", put(|| async { StatusCode::OK }));
    let (leader, follower) = spawn_raft_redirect_upstreams(leader_app).await;
    let gateway = Arc::new(Gateway::new(test_config(vec![
        follower.url.clone(),
        leader.url.clone(),
    ])));

    let req = Request::builder()
        .method("PUT")
        .uri("/bucket/stream")
        .header("authorization", "Bearer secret")
        .body(Body::from("payload"))
        .unwrap();

    let (parts, body) = req.into_parts();
    let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let resp = gateway
        .forward(&follower.url, &parts, body_bytes, ResponseTail::default())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn gateway_returns_raft_redirect_when_leader_not_in_upstreams() {
    let leader =
        spawn_upstream(Router::new().route("/bucket/stream", get(|| async { StatusCode::OK })))
            .await;
    let leader_url = format!("{}/bucket/stream", leader.url);
    let follower = spawn_upstream(Router::new().route(
        "/bucket/stream",
        any(move || {
            let leader_url = leader_url.clone();
            async move {
                (
                    StatusCode::TEMPORARY_REDIRECT,
                    [
                        ("location", leader_url),
                        ("x-ursula-raft-leader-id", "1".to_owned()),
                    ],
                    "redirecting",
                )
            }
        }),
    ))
    .await;

    // Gateway only knows about the follower, not the leader.
    let gateway = gateway_for_url(follower.url.clone());
    let req = Request::builder()
        .method("GET")
        .uri("/bucket/stream")
        .body(Body::empty())
        .unwrap();

    let (parts, body) = req.into_parts();
    let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let resp = gateway
        .forward(&follower.url, &parts, body_bytes, ResponseTail::default())
        .await
        .unwrap();

    // Cannot resolve leader → return 307 to client with stripped host.
    assert_eq!(resp.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(resp.headers().get(LOCATION).unwrap(), "/bucket/stream");
    assert!(resp.headers().get(HEADER_URSULA_RAFT_LEADER_ID).is_none());
}

#[tokio::test]
async fn gateway_handle_follows_leader_redirect_through_random_pick() {
    let leader_app = Router::new().route("/bucket/stream", get(|| async { StatusCode::OK }));
    let (leader, follower) = spawn_raft_redirect_upstreams(leader_app).await;
    let gateway = Arc::new(Gateway::new(test_config(vec![
        follower.url.clone(),
        leader.url.clone(),
    ])));

    let req = Request::builder()
        .method("GET")
        .uri("/bucket/stream")
        .body(Body::empty())
        .unwrap();

    // Use handle() so the random pick is exercised.
    let resp = gateway.handle(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn gateway_preserves_path_and_query_with_trailing_upstream_slash() {
    let app = Router::new().route(
        "/bucket/stream",
        get(|uri: Uri| async move {
            (
                StatusCode::OK,
                uri.path_and_query()
                    .map(axum::http::uri::PathAndQuery::as_str)
                    .unwrap_or(uri.path())
                    .to_owned(),
            )
        }),
    );
    let upstream = spawn_upstream(app).await;
    let upstream_url = format!("{}/", upstream.url);
    let gateway = gateway_for_url(upstream_url.clone());
    let req = Request::builder()
        .method("GET")
        .uri("/bucket/stream?offset=now&live=sse")
        .body(Body::empty())
        .unwrap();

    let (parts, body) = req.into_parts();
    let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let resp = gateway
        .forward(&upstream_url, &parts, body_bytes, ResponseTail::default())
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(body_bytes, "/bucket/stream?offset=now&live=sse");
}

#[tokio::test]
async fn gateway_forwards_response_content_encoding_without_decoding() {
    const GZIP_HELLO: &[u8] = &[
        31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 203, 72, 205, 201, 201, 7, 0, 134, 166, 16, 54, 5, 0, 0, 0,
    ];
    let app = Router::new().route(
        "/bucket/stream",
        get(|headers: HeaderMap| async move {
            let accept_encoding = headers
                .get("accept-encoding")
                .cloned()
                .unwrap_or_else(|| "missing".parse().expect("static header"));
            (
                StatusCode::OK,
                [
                    (
                        "content-type",
                        "application/x-ndjson".parse().expect("static header"),
                    ),
                    ("content-encoding", "gzip".parse().expect("static header")),
                    ("x-seen-accept-encoding", accept_encoding),
                ],
                bytes::Bytes::from_static(GZIP_HELLO),
            )
        }),
    );
    let upstream = spawn_upstream(app).await;
    let gateway = gateway_for_url(upstream.url.clone());
    let req = Request::builder()
        .method("GET")
        .uri("/bucket/stream")
        .header("accept-encoding", "gzip")
        .body(Body::empty())
        .expect("build request");

    let (parts, body) = req.into_parts();
    let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let response = gateway
        .forward(&upstream.url, &parts, body_bytes, ResponseTail::default())
        .await
        .expect("forward compressed response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("content-encoding").unwrap(), "gzip");
    assert_eq!(
        response.headers().get("x-seen-accept-encoding").unwrap(),
        "gzip"
    );
    assert_eq!(
        axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("compressed body"),
        GZIP_HELLO
    );
}

#[tokio::test]
async fn gateway_accepts_https_upstream_scheme() {
    let gateway = gateway_for_url("https://127.0.0.1:1");
    let req = Request::builder()
        .method("GET")
        .uri("/bucket/stream")
        .body(Body::empty())
        .unwrap();

    let (parts, body) = req.into_parts();
    let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let err = gateway
        .forward(
            "https://127.0.0.1:1",
            &parts,
            body_bytes,
            ResponseTail::default(),
        )
        .await
        .unwrap_err();

    assert!(
        !err.to_string().contains("only http is supported"),
        "gateway rejected https before attempting upstream connection: {err}"
    );
}

#[tokio::test]
async fn gateway_does_not_apply_response_header_timeout_to_sse_body() {
    let app = Router::new().route(
        "/bucket/stream",
        get(|| async {
            let (tx, rx) = tokio::sync::mpsc::channel(2);
            tokio::spawn(async move {
                tx.send(bytes::Bytes::from_static(b"event: data\ndata: first\n\n"))
                    .await
                    .expect("send first SSE event");
                tokio::time::sleep(Duration::from_millis(120)).await;
                tx.send(bytes::Bytes::from_static(b"event: data\ndata: second\n\n"))
                    .await
                    .expect("send second SSE event");
            });

            let body_stream = ReceiverStream::new(rx).map(Ok::<_, Infallible>);
            (
                StatusCode::OK,
                [("content-type", "text/event-stream")],
                Body::from_stream(body_stream),
            )
        }),
    );
    let upstream = spawn_upstream(app).await;
    let gateway =
        gateway_with_response_header_timeout(upstream.url.clone(), Duration::from_millis(50));
    let req = Request::builder()
        .method("GET")
        .uri("/bucket/stream")
        .body(Body::empty())
        .expect("build request");

    let (parts, body) = req.into_parts();
    let body_bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
    let mut resp = gateway
        .forward(&upstream.url, &parts, body_bytes, ResponseTail::default())
        .await
        .expect("forward SSE request");

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("content-type").expect("content type"),
        "text/event-stream"
    );

    let first = resp
        .body_mut()
        .frame()
        .await
        .expect("first frame")
        .expect("first frame ok")
        .into_data()
        .expect("first frame is data");
    assert_eq!(
        first,
        bytes::Bytes::from_static(b"event: data\ndata: first\n\n")
    );

    let second = tokio::time::timeout(Duration::from_secs(1), resp.body_mut().frame())
        .await
        .expect("second frame before test timeout")
        .expect("second frame")
        .expect("second frame ok")
        .into_data()
        .expect("second frame is data");
    assert_eq!(
        second,
        bytes::Bytes::from_static(b"event: data\ndata: second\n\n")
    );
}

#[tokio::test]
async fn gateway_gives_long_poll_response_headers_timeout_headroom() {
    let upstream = spawn_upstream(Router::new().route(
        "/bucket/stream",
        get(|| async {
            tokio::time::sleep(Duration::from_millis(80)).await;
            StatusCode::NO_CONTENT
        }),
    ))
    .await;
    let gateway =
        gateway_with_response_header_timeout(upstream.url.clone(), Duration::from_millis(50));
    let req = Request::builder()
        .method("GET")
        .uri("/bucket/stream?live=long-poll&timeout_ms=75")
        .body(Body::empty())
        .expect("build request");

    let response = gateway.handle(req).await;

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[test]
fn gateway_caps_long_poll_timeout_headroom_at_server_limit() {
    let gateway =
        gateway_with_response_header_timeout("http://127.0.0.1:1", Duration::from_secs(30));

    assert_eq!(
        gateway.response_header_timeout_for_url(
            "http://127.0.0.1:1/bucket/stream?live=long-poll&timeout_ms=999999"
        ),
        Duration::from_secs(62)
    );
    assert_eq!(
        gateway.response_header_timeout_for_url(
            "http://127.0.0.1:1/bucket/stream?live=sse&timeout_ms=999999"
        ),
        Duration::from_secs(30)
    );
}

// End-to-end shape of the #132/#133 tenant boundary: one shared gateway, a
// static bucket policy, and two tenants whose credentials must not cross.
mod tenant_boundary {
    use super::*;
    use crate::auth::policy::StaticPolicyAuthorizer;

    const POLICY: &str = r#"
        [[bucket]]
        id = "tenant-a"
        owners = [{ issuer = "https://issuer.example", subject = "user-1" }]

        [[bucket]]
        id = "tenant-public"
        public_read = true
    "#;

    fn policy_gateway(upstream_url: &str) -> Gateway {
        Gateway::with_access_control(
            test_config(vec![upstream_url.to_owned()]),
            AccessControl::new(
                Arc::new(FixedPrincipalResolver::valid()),
                Arc::new(StaticPolicyAuthorizer::from_toml_str(POLICY).expect("parse policy")),
            ),
        )
    }

    fn shared_upstream() -> Router {
        Router::new()
            .route("/tenant-a/orders", any(|| async { "tenant-a data" }))
            .route("/tenant-b/orders", any(|| async { "tenant-b data" }))
            .route("/tenant-public/orders", any(|| async { "public data" }))
    }

    async fn send(
        gateway: &Gateway,
        method: &str,
        uri: &str,
        bearer: Option<&str>,
    ) -> axum::response::Response {
        let mut request = Request::builder().method(method).uri(uri);
        if let Some(token) = bearer {
            request = request.header(AUTHORIZATION, format!("Bearer {token}"));
        }
        gateway
            .handle(request.body(Body::empty()).expect("request"))
            .await
    }

    #[tokio::test]
    async fn owner_reaches_their_own_bucket() {
        let upstream = spawn_upstream(shared_upstream()).await;
        let gateway = policy_gateway(&upstream.url);

        let response = send(&gateway, "GET", "/tenant-a/orders", Some("valid-token")).await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = send(&gateway, "POST", "/tenant-a/orders", Some("valid-token")).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn credential_cannot_cross_into_another_tenant() {
        let upstream = spawn_upstream(shared_upstream()).await;
        let gateway = policy_gateway(&upstream.url);

        // `valid-token` belongs to tenant-a's owner. tenant-b both exists
        // upstream and is absent from the policy: either way the caller
        // observes the same 404 surface.
        let response = send(&gateway, "GET", "/tenant-b/orders", Some("valid-token")).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = send(&gateway, "POST", "/tenant-b/orders", Some("valid-token")).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn anonymous_reads_reach_only_public_buckets() {
        let upstream = spawn_upstream(shared_upstream()).await;
        let gateway = policy_gateway(&upstream.url);

        let response = send(&gateway, "GET", "/tenant-public/orders", None).await;
        assert_eq!(response.status(), StatusCode::OK);

        // A private bucket conceals its existence from anonymous probes.
        let response = send(&gateway, "GET", "/tenant-a/orders", None).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn public_visibility_grants_reads_but_never_writes() {
        let upstream = spawn_upstream(shared_upstream()).await;
        let gateway = policy_gateway(&upstream.url);

        let response = send(&gateway, "POST", "/tenant-public/orders", None).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        // A non-owner principal gets no more than the anonymous public grant.
        let response = send(
            &gateway,
            "POST",
            "/tenant-public/orders",
            Some("valid-token"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}

// Gateway quota enforcement and usage accounting (#134/#135).
mod admission_and_usage {
    use super::*;
    use crate::admission::StaticQuotaProvider;
    use crate::auth::policy::StaticPolicyAuthorizer;
    use crate::usage::UsageBatch;
    use crate::usage::UsageClass;
    use crate::usage::UsageCollector;
    use crate::usage::UsageSink;
    use crate::usage::UsageSinkFuture;

    const POLICY: &str = r#"
        [[bucket]]
        id = "tenant-a"
        owners = [{ issuer = "https://issuer.example", subject = "user-1" }]
    "#;

    const QUOTAS: &str = r#"
        [[bucket]]
        id = "tenant-a"
        requests_per_sec = 2
        max_request_body_bytes = 8
    "#;

    struct CapturingSink {
        batches: Mutex<Vec<UsageBatch>>,
    }

    impl UsageSink for CapturingSink {
        fn export<'a>(&'a self, batch: &'a UsageBatch) -> UsageSinkFuture<'a> {
            self.batches
                .lock()
                .expect("batches lock")
                .push(batch.clone());
            Box::pin(async { Ok(()) })
        }
    }

    fn quota_gateway(upstream_url: &str, collector: Arc<UsageCollector>) -> Gateway {
        chunked_quota_gateway(upstream_url, collector, None)
    }

    fn chunked_quota_gateway(
        upstream_url: &str,
        collector: Arc<UsageCollector>,
        usage_chunk_bytes: Option<u64>,
    ) -> Gateway {
        metered_gateway(upstream_url, collector, usage_chunk_bytes).with_quota_provider(Arc::new(
            StaticQuotaProvider::from_toml_str(QUOTAS).expect("parse quotas"),
        ))
    }

    /// Access control and usage, no quotas. Counting units needs several
    /// appends of differing sizes, which the shared quota policy's two
    /// requests per second and eight-byte body would reject before the meter
    /// ever saw them.
    fn metered_gateway(
        upstream_url: &str,
        collector: Arc<UsageCollector>,
        usage_chunk_bytes: Option<u64>,
    ) -> Gateway {
        let mut config = test_config(vec![upstream_url.to_owned()]);
        config.usage_chunk_bytes = usage_chunk_bytes;
        Gateway::with_access_control(
            config,
            AccessControl::new(
                Arc::new(FixedPrincipalResolver::valid()),
                Arc::new(StaticPolicyAuthorizer::from_toml_str(POLICY).expect("parse policy")),
            ),
        )
        .with_usage_collector(collector)
    }

    async fn send(
        gateway: &Gateway,
        method: &str,
        uri: &str,
        body: &'static str,
    ) -> axum::response::Response {
        gateway
            .handle(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .header(AUTHORIZATION, "Bearer valid-token")
                    .body(Body::from(body))
                    .expect("request"),
            )
            .await
    }

    /// An upstream that answers `ok` on `/tenant-a/orders` and counts the
    /// requests that reach it.
    async fn counting_upstream() -> (TestUpstream, Arc<AtomicUsize>) {
        let hits = Arc::new(AtomicUsize::new(0));
        let upstream_hits = Arc::clone(&hits);
        let upstream = spawn_upstream(Router::new().route(
            "/tenant-a/orders",
            any(move || {
                let upstream_hits = Arc::clone(&upstream_hits);
                async move {
                    upstream_hits.fetch_add(1, Ordering::Relaxed);
                    "ok"
                }
            }),
        ))
        .await;
        (upstream, hits)
    }

    #[tokio::test]
    async fn rate_limit_rejects_with_retry_after_and_never_reaches_upstream() {
        let (upstream, hits) = counting_upstream().await;
        let gateway = quota_gateway(&upstream.url, UsageCollector::new());

        assert_eq!(
            send(&gateway, "GET", "/tenant-a/orders", "").await.status(),
            StatusCode::OK
        );
        assert_eq!(
            send(&gateway, "GET", "/tenant-a/orders", "").await.status(),
            StatusCode::OK
        );
        let limited = send(&gateway, "GET", "/tenant-a/orders", "").await;
        assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(not_applied(&limited), Some("TenantRateLimited"));
        assert_eq!(vouch(&limited), Some("1"));
        assert_eq!(
            hits.load(Ordering::Relaxed),
            2,
            "the refusal was not forwarded"
        );
        let retry_after = limited
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .expect("Retry-After header");
        assert!(retry_after >= 1);
    }

    #[tokio::test]
    async fn per_tenant_body_limit_rejects_oversized_appends() {
        let (upstream, hits) = counting_upstream().await;
        let gateway = quota_gateway(&upstream.url, UsageCollector::new());

        let oversized = send(&gateway, "POST", "/tenant-a/orders", "123456789").await;
        assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(not_applied(&oversized), Some("RequestBodyTooLarge"));
        assert_eq!(
            hits.load(Ordering::Relaxed),
            0,
            "the refusal was not forwarded"
        );

        let allowed = send(&gateway, "POST", "/tenant-a/orders", "12345678").await;
        assert_eq!(allowed.status(), StatusCode::OK);
        assert_eq!(not_applied(&allowed), None);
        assert_eq!(hits.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn live_read_limit_rejects_as_not_applied() {
        let (upstream, hits) = counting_upstream().await;
        let gateway = metered_gateway(&upstream.url, UsageCollector::new(), None)
            .with_quota_provider(Arc::new(
                StaticQuotaProvider::from_toml_str(
                    r#"
                    [[bucket]]
                    id = "tenant-a"
                    max_concurrent_live_reads = 1
                    "#,
                )
                .expect("parse quotas"),
            ));
        let tail = "/tenant-a/orders?offset=now&live=long-poll";

        // The first response holds the tenant's only live-read slot until it
        // is dropped.
        let first = send(&gateway, "GET", tail, "").await;
        assert_eq!(first.status(), StatusCode::OK);
        let second = send(&gateway, "GET", tail, "").await;
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(not_applied(&second), Some("TenantLiveReadsExhausted"));
        assert_eq!(
            hits.load(Ordering::Relaxed),
            1,
            "the refusal was not forwarded"
        );
        drop(first);
    }

    #[tokio::test]
    async fn usage_counts_append_ingress_and_read_egress_per_principal() {
        let upstream =
            spawn_upstream(Router::new().route("/tenant-a/orders", any(|| async { "0123456789" })))
                .await;
        let collector = UsageCollector::new();
        let gateway = quota_gateway(&upstream.url, Arc::clone(&collector));

        let append = send(&gateway, "POST", "/tenant-a/orders", "12345678").await;
        assert_eq!(append.status(), StatusCode::OK);
        // Drain the response body so the meter observes completion.
        let _ = append.into_body().collect().await.expect("append body");
        let read = send(&gateway, "GET", "/tenant-a/orders", "").await;
        let read_body = read.into_body().collect().await.expect("read body");
        assert_eq!(read_body.to_bytes().len(), 10);

        let sink = CapturingSink {
            batches: Mutex::new(Vec::new()),
        };
        crate::usage::flush(&collector, &sink).await;
        let batches = sink.batches.lock().expect("batches lock");
        assert_eq!(batches.len(), 1);
        let records = &batches[0].records;

        let append_record = records
            .iter()
            .find(|record| record.key.class == UsageClass::Append)
            .expect("append usage record");
        assert_eq!(append_record.key.bucket_id, "tenant-a");
        assert_eq!(append_record.counters.requests, 1);
        assert_eq!(append_record.counters.request_bytes, 8);
        let principal = append_record.key.principal.as_ref().expect("principal");
        assert_eq!(principal.subject, "user-1");

        let read_record = records
            .iter()
            .find(|record| record.key.class == UsageClass::Read)
            .expect("read usage record");
        assert_eq!(read_record.counters.response_bytes, 10);
        // No unit size configured, so nothing to count.
        assert_eq!(append_record.counters.chunks, 0);
        assert_eq!(read_record.counters.chunks, 0);
    }

    #[tokio::test]
    async fn appends_count_in_units_and_reads_never_do() {
        let upstream =
            spawn_upstream(Router::new().route("/tenant-a/orders", any(|| async { "0123456789" })))
                .await;
        let collector = UsageCollector::new();
        // A four-byte unit keeps the arithmetic legible: 8 bytes is exactly
        // two units, 9 is three, and an empty append is still one.
        let gateway = metered_gateway(&upstream.url, Arc::clone(&collector), Some(4));

        for body in ["12345678", "123456789", ""] {
            let append = send(&gateway, "POST", "/tenant-a/orders", body).await;
            assert_eq!(append.status(), StatusCode::OK);
            let _ = append.into_body().collect().await.expect("append body");
        }
        let read = send(&gateway, "GET", "/tenant-a/orders", "").await;
        let _ = read.into_body().collect().await.expect("read body");

        let sink = CapturingSink {
            batches: Mutex::new(Vec::new()),
        };
        crate::usage::flush(&collector, &sink).await;
        let batches = sink.batches.lock().expect("batches lock");
        let records = &batches[0].records;

        let append_record = records
            .iter()
            .find(|record| record.key.class == UsageClass::Append)
            .expect("append usage record");
        assert_eq!(append_record.counters.requests, 3);
        assert_eq!(append_record.counters.request_bytes, 17);
        assert_eq!(append_record.counters.chunks, 2 + 3 + 1);

        // A read already pays through `response_bytes`; charging it a write
        // unit as well would bill the same request twice.
        let read_record = records
            .iter()
            .find(|record| record.key.class == UsageClass::Read)
            .expect("read usage record");
        assert_eq!(read_record.counters.chunks, 0);
    }
}

mod cors {
    use axum::body::Body;
    use axum::http::Request;
    use axum::http::StatusCode;

    use super::test_config;
    use crate::Gateway;

    const ALLOW_ORIGIN: &str = "access-control-allow-origin";
    const ALLOW_CREDENTIALS: &str = "access-control-allow-credentials";
    const EXPOSE_HEADERS: &str = "access-control-expose-headers";

    fn gateway(origins: &[&str]) -> Gateway {
        let mut config = test_config(vec!["http://upstream.invalid".to_owned()]);
        config.cors_allowed_origins = origins.iter().map(|origin| (*origin).to_owned()).collect();
        Gateway::new(config)
    }

    fn preflight(uri: &str, origin: &str) -> Request<Body> {
        Request::builder()
            .method("OPTIONS")
            .uri(uri)
            .header("origin", origin)
            .header("access-control-request-method", "GET")
            .header("access-control-request-headers", "authorization")
            .body(Body::empty())
            .expect("request")
    }

    /// The property that keeps #133's concealment intact. A preflight arrives
    /// without `Authorization`, so if the reply differed between a real bucket
    /// and a missing one it would be an unauthenticated existence oracle. The
    /// policy is never given the path, but assert the observable behaviour so a
    /// future refactor cannot quietly make it path-dependent.
    #[tokio::test]
    async fn preflight_cannot_distinguish_an_existing_bucket_from_a_missing_one() {
        let gateway = gateway(&["https://app.example"]);

        let existing = gateway
            .handle(preflight("/ten_real/stream", "https://app.example"))
            .await;
        let missing = gateway
            .handle(preflight("/ten_absent/stream", "https://app.example"))
            .await;

        assert_eq!(existing.status(), StatusCode::NO_CONTENT);
        assert_eq!(existing.status(), missing.status());
        assert_eq!(existing.headers(), missing.headers());
    }

    #[tokio::test]
    async fn preflight_from_an_unlisted_origin_carries_no_cors_headers() {
        let gateway = gateway(&["https://app.example"]);

        let response = gateway
            .handle(preflight("/ten_real/stream", "https://evil.example"))
            .await;

        assert!(response.headers().get(ALLOW_ORIGIN).is_none());
    }

    /// `Expose-Headers: *` is what lets a browser read `stream-next-offset` and
    /// the rest of the protocol headers, so a client can paginate at all. It is
    /// only honoured while credentials stay disallowed.
    #[tokio::test]
    async fn credentials_are_never_allowed_so_the_expose_wildcard_holds() {
        let gateway = gateway(&["*"]);

        let response = gateway
            .handle(preflight("/ten_real/stream", "https://any.example"))
            .await;

        assert_eq!(
            response
                .headers()
                .get(ALLOW_ORIGIN)
                .and_then(|value| value.to_str().ok()),
            Some("*")
        );
        assert!(response.headers().get(ALLOW_CREDENTIALS).is_none());
    }

    #[tokio::test]
    async fn an_unconfigured_gateway_adds_nothing() {
        let gateway = gateway(&[]);

        let response = gateway
            .handle(preflight("/ten_real/stream", "https://app.example"))
            .await;

        assert!(response.headers().get(ALLOW_ORIGIN).is_none());
        assert!(response.headers().get(EXPOSE_HEADERS).is_none());
        // Without CORS the request falls through to normal routing rather than
        // being short-circuited as a preflight.
        assert_ne!(response.status(), StatusCode::NO_CONTENT);
    }
}

mod credential_deadline {
    use std::convert::Infallible;

    use axum::Router;
    use axum::body::Body;
    use axum::http::Request;
    use axum::http::StatusCode;
    use axum::routing::get;
    use http_body_util::BodyExt;
    use tokio_stream::StreamExt;
    use tokio_stream::wrappers::ReceiverStream;

    use super::ResponseTail;
    use super::gateway_for_url;
    use super::spawn_upstream;

    /// An upstream that keeps sending SSE events forever, so the only thing that
    /// can end the response is the gateway.
    fn endless_sse() -> Router {
        Router::new().route(
            "/bucket/stream",
            get(|| async {
                let (tx, rx) = tokio::sync::mpsc::channel(1);
                tokio::spawn(async move {
                    loop {
                        if tx
                            .send(bytes::Bytes::from_static(b"event: data\ndata: tick\n\n"))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    }
                });
                (
                    StatusCode::OK,
                    [("content-type", "text/event-stream")],
                    Body::from_stream(ReceiverStream::new(rx).map(Ok::<_, Infallible>)),
                )
            }),
        )
    }

    fn json_upstream() -> Router {
        Router::new().route(
            "/bucket/stream",
            get(|| async {
                (
                    StatusCode::OK,
                    [("content-type", "application/x-ndjson")],
                    Body::from("{\"record\":1}\n"),
                )
            }),
        )
    }

    fn unix_now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after the unix epoch")
            .as_secs()
    }

    async fn forward_with(
        upstream_url: &str,
        tail: ResponseTail,
    ) -> axum::response::Response<Body> {
        let gateway = gateway_for_url(upstream_url.to_owned());
        let req = Request::builder()
            .method("GET")
            .uri("/bucket/stream")
            .body(Body::empty())
            .expect("build request");
        let (parts, body) = req.into_parts();
        let body_bytes = axum::body::to_bytes(body, usize::MAX)
            .await
            .expect("request body");
        gateway
            .forward(upstream_url, &parts, body_bytes, tail)
            .await
            .expect("forward request")
    }

    /// The point of #203: a subscription must not outlive the credential that
    /// authorized it, and the client must be able to tell that is why it ended.
    #[tokio::test]
    async fn an_sse_subscription_ends_with_a_named_event_when_the_credential_expires() {
        let upstream = spawn_upstream(endless_sse()).await;
        let response = forward_with(&upstream.url, ResponseTail {
            credential_expires_at: Some(unix_now()),
            ..ResponseTail::default()
        })
        .await;

        // Bounded on purpose. The upstream never ends, so if the deadline is not
        // armed this collect would hang — and a hang in CI is a worse signal than
        // a failure, since it reads as flakiness rather than a missing feature.
        let body = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            axum::body::to_bytes(response.into_body(), usize::MAX),
        )
        .await
        .expect("the deadline must end the stream rather than leave it open")
        .expect("collect the terminated stream");
        let body = String::from_utf8_lossy(&body);

        assert!(
            body.contains("event: credential-expired"),
            "the stream must say why it ended, not just stop: {body:?}"
        );
        assert!(
            body.ends_with("\n\n"),
            "the terminal frame must be a complete SSE event: {body:?}"
        );
    }

    /// Anonymous public reads carry no credential, so they have no deadline. This
    /// is the regression that would silently break `public_read` tails.
    #[tokio::test]
    async fn an_anonymous_subscription_is_not_given_a_deadline() {
        let upstream = spawn_upstream(endless_sse()).await;
        let response = forward_with(&upstream.url, ResponseTail::default()).await;

        // One frame, not the whole body: the upstream never ends, and that it
        // keeps going is the property under test.
        let mut response = response;
        let first = response
            .body_mut()
            .frame()
            .await
            .expect("first frame")
            .expect("first frame ok")
            .into_data()
            .expect("first frame is data");
        let first = String::from_utf8_lossy(&first);

        assert!(
            first.contains("data: tick"),
            "an anonymous tail should receive upstream data, not a terminal frame: {first:?}"
        );
        assert!(
            !first.contains("credential-expired"),
            "no credential means no deadline: {first:?}"
        );
    }

    /// A credential lapsing mid-transfer must not truncate a body the client is
    /// already committed to reading; only indefinite streams get a deadline.
    #[tokio::test]
    async fn a_non_streaming_response_is_never_truncated() {
        let upstream = spawn_upstream(json_upstream()).await;
        let response = forward_with(&upstream.url, ResponseTail {
            credential_expires_at: Some(unix_now()),
            ..ResponseTail::default()
        })
        .await;

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("collect body");

        assert_eq!(
            String::from_utf8_lossy(&body),
            "{\"record\":1}\n",
            "an already-expired credential must not rewrite a finite response"
        );
    }
}

/// Keepalive derived from the upstream user timeout T (#495).
mod upstream_keepalive {
    use std::num::NonZeroU64;
    use std::time::Duration;

    use super::UpstreamKeepalive;

    fn secs(secs: u64) -> Duration {
        Duration::from_secs(secs)
    }

    fn for_user_timeout(secs: u64) -> UpstreamKeepalive {
        UpstreamKeepalive::for_user_timeout(NonZeroU64::new(secs).unwrap())
    }

    fn keepalive(idle: u64, interval: u64) -> UpstreamKeepalive {
        UpstreamKeepalive {
            idle: secs(idle),
            interval: secs(interval),
            retries: 3,
        }
    }

    // Below 5 s, both steps stay at the 1 s the kernel can express.
    #[test]
    fn one_second_floors_both_steps() {
        assert_eq!(for_user_timeout(1), keepalive(1, 1));
    }

    // The default: probes after 2, 3 and 4 s of silence, closed at about 5 s.
    #[test]
    fn five_seconds_probes_three_times_before_closing() {
        assert_eq!(for_user_timeout(5), keepalive(2, 1));
    }

    #[test]
    fn thirty_seconds_scales_both_steps() {
        assert_eq!(for_user_timeout(30), keepalive(12, 6));
    }

    // Linux closes an idle connection once T passes with a probe unanswered,
    // so all three probes go out only if the third is sent before T. With
    // steps of at least 1 s that needs T >= 4 s: at T = 3 s the third probe
    // would fall at T.
    #[test]
    fn three_probes_go_out_before_the_user_timeout() {
        for t in 4..=600 {
            let k = for_user_timeout(t);
            let third_probe = k.idle.saturating_add(k.interval.saturating_mul(2));
            assert!(third_probe < secs(t), "T = {t} s: {k:?}");
        }
    }
}
