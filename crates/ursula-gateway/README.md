# Ursula Gateway

The `ursula gateway` role is a small HTTP/SSE gateway for Ursula clusters. It gives public
clients one stable HTTP endpoint while keeping internal Ursula node addresses
out of public responses.

The gateway is intentionally thin: it does not own stream routing state or
terminate SSE streams. It keeps two things in memory: the Raft leader it last
learned for each stream (or each Raft group, with `--raft-group-count`), and
which upstreams it is skipping after transport failures. It buffers request
bodies up to a configured limit only so it can replay requests when Ursula
identifies the Raft leader.

## Behavior

- Sends each incoming request to the leader it learned for the request's
  stream, or else to a configured upstream Ursula node picked at random,
  skipping upstreams that recently failed at the transport. See
  [Upstream Selection](#upstream-selection).
- Buffers request bodies up to `--max-request-body-bytes`; larger requests
  receive `413 Payload Too Large` before reaching an upstream.
- Follows Ursula Raft leader redirects internally, so clients never get one.
  See [Redirects](#redirects).
- Returns other redirects to clients and rewrites redirect targets so clients
  continue through the gateway.
- Keeps long-lived SSE reads open after response headers arrive.
- Optionally classifies Durable Streams requests as an explicit
  `bucket/stream/action` resource before forwarding them to provider-neutral
  authentication and authorization hooks.
- Marks its answers to requests that no node can have applied with the
  `x-ursula-not-applied` response header, and sets
  `x-ursula-not-applied-checked: 1` on every response. See
  [Refusals That Applied Nothing](#refusals-that-applied-nothing).

## Upstream Selection

- **Leader cache.** When an upstream answers with Ursula's internal `307` and
  `x-ursula-raft-leader-id`, the gateway follows it (see
  [Redirects](#redirects)). The upstream that then serves the request with a
  success (`2xx`) becomes the route for the stream, or for the stream's Raft
  group with `--raft-group-count`. Any other answer teaches no route, since a
  node gives some before its leadership check. Later requests for the stream
  or group go to the cached leader directly. A route is dropped when its
  upstream redirects, when it answers `503` with `Retry-After` (while it does
  not know the leader, or under backpressure), or when a request to it fails.
  The cache holds up to 16384 routes and starts over when full. Requests
  without a route go to a configured upstream picked at random.
- **Ejection.** An upstream that fails at the transport (it refuses or times
  out the connection, or the exchange breaks) is ejected: requests skip it
  for 10 s. Each failure after a window ejects it again for twice as long,
  up to 60 s. Failures of requests that were already in flight do not
  lengthen a window. An upstream that sends no response headers within
  `--response-header-timeout` is not ejected, because one slow Raft group on
  a live node does the same. A node that vanished fails within
  `--upstream-tcp-user-timeout` instead.
- **Probe.** When a window expires, the next request that selects the
  upstream, along a cached route, at random, or as a search hop after a
  leader redirect, probes it. The other requests
  keep skipping the upstream until the probe answers or fails, or for at most
  `--connect-timeout` plus `--upstream-tcp-user-timeout`, after which the
  next request that selects it probes it again. Any response from the
  upstream, whatever its status, ends its ejection. That includes the
  response to a request that follows a leader redirect to it.
- **Fallback.** If every upstream is ejected or being probed, requests go
  along their cached route, or else to any upstream, rather than to none.
- **Metrics.** `/__ursula/gateway/metrics` reports `upstream_ejections`
  (times an upstream was ejected) and `upstream_ejected` (upstreams skipped
  now, inside a window or with a probe outstanding), next to the leader-cache
  counters `leader_cache_hits`, `leader_cache_misses`, `leader_cache_updates`,
  `leader_cache_evictions` and `leader_cache_entries`, and the redirect
  counters `leader_redirects` (hops that followed a redirect, search hops
  included), `leader_redirect_ns` (time spent by the requests that followed
  one), `leader_searches` (search hops) and `leader_not_found` (requests
  answered `503` because no upstream answered without redirecting). A
  gateway with access control answers the path `404` unless
  `--auth-public-metrics` is set (see
  [Access Control and Logical Tenancy](#access-control-and-logical-tenancy)).

## Access Control and Logical Tenancy

Ursula keeps the public resource model at two levels: `/{bucket}/{stream}`.
Shared hosted deployments can treat the bucket as the logical tenant or owner
namespace, while a standalone deployment can keep using bucket names exactly as
before.

`Gateway::new` does not enable access control. The stock `ursula gateway` role
therefore remains a transparent, tenant-unaware gateway and requires no OAuth
configuration.

A hosted gateway can instead construct `Gateway::with_access_control` with an
`auth::AccessControl` containing:

- a `PrincipalResolver` that verifies a presented OAuth bearer token and
  normalizes it into a provider-neutral principal;
- an `Authorizer` that evaluates the optional principal, explicit bucket and
  stream, and requested action.

Anonymous requests still reach the authorizer, allowing public buckets to be
read without a token. An authorizer can return `ConcealAsNotFound` for a private
bucket the caller cannot see. Validated bearer credentials terminate at the
gateway and are not forwarded to Ursula nodes. Once access control is enabled,
unrecognized and internal Ursula paths fail closed with `404`; a hosted binary
can mount its own health or operator routes outside the gateway fallback.

The gateway's own metrics path, `/__ursula/gateway/metrics`, is one of those
internal paths. `--auth-public-metrics` on `ursula gateway`, or
`AccessControl::with_public_metrics(true)` in a hosted binary, serves the
counters there to every caller, with or without a token, as a gateway without
access control does. The gateway then does not check a presented token on that
path, so an invalid or expired one gets the counters too. The counters hold no
tenant data, but they show traffic volume. Make them public only where
untrusted clients cannot reach the gateway, or where a proxy in front of it
denies `/__ursula/*`. Gateways before 0.7.4 served the path to every caller,
with or without access control.

## Redirects

The gateway follows Ursula `307 Temporary Redirect` responses marked as Raft
leader redirects with `x-ursula-raft-leader-id` itself, so a client never gets
one:

- It sends the request to the upstream that the redirect's `Location` names,
  unless the request already went to that upstream.
- Otherwise, and when the `Location` names no upstream because `--upstream`
  names the nodes otherwise than their peer URLs, it searches: it sends the
  request to the next upstream it has not tried yet, starting after the one
  that redirected, in the configured order and around. It skips ejected
  upstreams, and one whose ejection window has expired becomes the request's
  probe. A search hop that cannot connect sent nothing, so the search goes on
  to the next upstream.
- At most three hops follow redirects, search hops included, and none starts
  later than 5 s after the request's first redirect. A hop under way runs to
  its answer, so a long-poll or a large upload that reached the leader is not
  cut short. Nor does a slow first hop use the 5 s up: a follower can store a
  large body in object storage before it redirects.
- When no upstream answers without redirecting within these limits, the
  client gets `503` with `Retry-After: 1`, as from a node whose group has no
  known leader.

Gateways before 0.7.3 followed one redirect, to a configured upstream only,
and passed any other on to the client with a path-only `Location`.

Each hop sends the request again. Only stream requests are redirected. Two
hops of one request are at most 5 s and the earlier hop's own time apart.
When a node proposed a write before it redirected, so that the write may
already apply, the next hop's copy fares as follows:

- A producer append or close is deduplicated by its `Producer-Id`,
  `Producer-Epoch` and `Producer-Seq`.
- A create answers `200` for a stream that already exists as it asks. If
  another client deleted the stream between the two hops, it creates the
  stream again.
- A close without a body answers as the first close did.
- A snapshot publish or a retention move makes the same change again.
- A delete answers `404`: the stream is gone. An unconditional delete also
  deletes a stream that another client recreated between the two hops. With
  `Stream-Incarnation`, it answers `412` and deletes nothing instead, so send
  it.
- An append with `Stream-Seq`, or one that also closes the stream, is refused
  with `409`, which its client cannot tell apart from a conflict with another
  writer.
- A plain append, without a producer or `Stream-Seq`, applies again: once for
  each node that proposed it and then redirected. Its client gets
  at-least-once delivery, as on any retry of it, and as through gateways
  before 0.7.3, which followed one redirect. Use a producer for exactly-once
  writes.

Other `307` responses remain visible to the client. Their `Location` header is
reduced to the path and query so redirect-following clients such as `curl -L`
continue through the gateway instead of connecting to internal Ursula nodes
directly.

## Refusals That Applied Nothing

The gateway sets the `x-ursula-not-applied` response header, the marker,
when no node can have applied the request: on every request it refuses
before forwarding it, on a request it could not send because the connection
to its first upstream failed, and after redirects that were all marked
(below). The value names the refusal:

| Status | Token | Refusal |
| ------ | ----- | ------- |
| `404` | `NotFound` | Access control does not serve the route, or the authorizer concealed the resource |
| `401` | `InvalidCredential` | The bearer credential is malformed or failed verification |
| `503` | `AuthenticationUnavailable` | The credential could not be verified, for example because the token issuer is down |
| `403` | `Forbidden` | The authorizer denied the request |
| `503` | `AuthorizationUnavailable` | The authorizer could not decide |
| `429` | `TenantRateLimited` | The bucket is over its request rate |
| `429` | `TenantLiveReadsExhausted` | The bucket uses all of its live-read connections |
| `413` | `RequestBodyTooLarge` | The body is over `--max-request-body-bytes` or the bucket's limit |
| `400` | `RequestBodyUnreadable` | The request body could not be read |
| `503` | `NoUpstream` | No upstream is configured |
| `503` | `UpstreamUnreachable` | The connection to the first upstream failed, so the request was never sent. Or the connection to an upstream after redirects failed, and every redirect was marked |
| `503` | `LeaderNotFound` | No upstream answered without redirecting within the [redirect limits](#redirects), and every redirect was marked |

A node's marker passes through unchanged when the gateway sent the request
to one node. After the gateway follows Raft leader redirects, it keeps the
final answer's marker only if every redirect carried one too, because a node
can redirect a write after it proposed it. A node marks only the redirects it
sends before it proposes a write, so a leader's admission refusal reached
through such redirects keeps its marker. The gateway's `503` for an upstream
it could not connect to after redirects, and its `503` when it stops
following redirects, are marked only when every redirect was marked. A hop
that could not connect sent nothing, so it does not count against the
marker. The other
answers the gateway gives when an upstream fails carry no marker: the `502`,
and the `504` for an upstream that sent no response headers in time.

From 0.7.3 the gateway sets `x-ursula-not-applied-checked: 1`, the vouch, on
every response it returns: forwarded responses, its own refusals and its own
errors. It adds the vouch after the redirect rule above decided whether a
node's marker stays, and drops any vouch that came from an upstream. Nodes
never set it. A client counts an attempt as never applied only when the
response carries both headers, or has the status `401`, `402` or `429`.
Gateways before 0.7.3 never send the vouch, so clients ignore their markers,
whatever order gateways and nodes are upgraded or rolled back in. Upgrading
gateways first is still recommended, so that clients keep the benefit of the
marker during a rollout. See
[refusals that applied nothing](https://ursula.tonbo.io/docs/api/overview#refusals-that-applied-nothing).

The vouch covers only the one request the gateway received. A proxy in front
of the gateway must not resend writes, and a client must not let its HTTP
library follow redirects or retry writes on its own.

The vouch also covers only the exchanges the gateway makes itself, so point
`--upstream` at the nodes themselves. A proxy, service-mesh sidecar or
another gateway between the gateway and the nodes must not resend requests,
or the gateway vouches for a marker from a later delivery. For example, a
node can propose a write and then answer `503` without a marker. If a
sidecar resends the request on that `503`, as Istio's default retry policy
did before 1.24, and a node refuses the copy at admission with a marked
`503`, the gateway sees only that refusal and vouches for it. For the same
reason, a 0.7.3 gateway must not sit in front of an older gateway, which
follows redirects itself and passes a leader's marker on after any of them.

Nothing between the gateway and the nodes may answer `401`, `402` or `429`
either, as a sidecar's local rate limit does. A client counts those
statuses as never applied without the headers, and after a redirect without
a marker the gateway returns the leader's status unchanged, although the
node that redirected may have proposed the write.

## SSE Behavior

`live=sse` responses stream through the gateway. `--response-header-timeout`
only covers the time needed to receive response headers; streamed response
bodies remain open.

Example:

```bash
curl -N 'http://127.0.0.1:8080/demo/hello?offset=-1&live=sse'
```

## Run

Start a gateway in front of one or more Ursula HTTP or HTTPS nodes:

```bash
cargo run -p ursula --bin ursula -- gateway \
  --listen 127.0.0.1:8080 \
  --upstream http://127.0.0.1:4437 \
  --upstream http://127.0.0.1:4438 \
  --upstream http://127.0.0.1:4439
```

Then send normal Durable Streams requests to the gateway:

```bash
curl -X PUT http://127.0.0.1:8080/demo
curl -X PUT http://127.0.0.1:8080/demo/hello

curl -X POST http://127.0.0.1:8080/demo/hello \
  -H 'Content-Type: application/octet-stream' \
  --data-binary 'hello world'

curl 'http://127.0.0.1:8080/demo/hello?offset=-1'
curl -N 'http://127.0.0.1:8080/demo/hello?offset=-1&live=sse'
```

## Options

```text
--listen <ADDR>
    Address to bind. Defaults to 0.0.0.0:4437.

--upstream <URL>
    Base URL for an Ursula HTTP or HTTPS node. Repeat once per node. Required.
    Point it at the node itself, by the node's peer URL where you can. A
    leader redirect names the leader by its peer URL, and the gateway follows
    one that names another address only by trying the upstreams it has not
    tried yet. A proxy, service-mesh sidecar or another gateway in between
    must not resend requests, or the gateway vouches for an
    x-ursula-not-applied marker from a later delivery. Nor may it answer
    401, 402 or 429, for example from a local rate limit: a client counts
    those statuses as never applied.

--response-header-timeout <SECONDS>
    Timeout for sending the upstream request and receiving response headers.
    Streamed response bodies such as SSE live reads are not covered.
    Defaults to 30.

--connect-timeout <SECONDS>
    TCP connect timeout per upstream request attempt. Defaults to 5.

--upstream-tcp-user-timeout <SECONDS>
    How long data sent to an upstream may stay unacknowledged before the
    connection closes and its request fails with 502 (Linux TCP_USER_TIMEOUT).
    Idle connections are probed three times within it: first after 2/5 of it,
    then every 1/5, each at least 1 s. 0.7.0 effectively used reqwest's 30 s
    default. Defaults to 5. At least 1.

--max-request-body-bytes <BYTES>
    Maximum request body bytes buffered for leader-redirect replay.
    Larger requests return 413 before upstream forwarding. Defaults to 33554432.

--graceful-shutdown-timeout <SECONDS>
    Maximum graceful shutdown drain time after SIGTERM/CTRL-C. Defaults to 3600.
```

## Operational Notes

- Upstream URLs support `http` and `https`.
- A node that vanishes without closing its connections, such as a
  force-deleted pod or a powered-off host, fails the requests waiting on them
  within `--upstream-tcp-user-timeout` with `502`. The request may have
  reached the node, so the client decides whether to retry it.
- Upstreams that fail at the transport are skipped for a while. See
  [Upstream Selection](#upstream-selection).
- Request bodies are buffered up to `--max-request-body-bytes` because internal
  leader redirects require replaying the request to a different upstream.
- The gateway keeps no durable state and does not maintain cluster membership.
  Its leader cache and upstream ejections live in memory and start empty.
- `RUST_LOG=ursula_gateway=debug` enables request forwarding and redirect logs.

## Verify

```bash
cargo fmt --all -- --check
cargo test -p ursula-gateway
cargo clippy -p ursula-gateway --all-targets -- -D warnings
```
