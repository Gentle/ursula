# Immutable reads through a shared cache

Status: design, not implemented. The protocol is
[extensions §4](../web/src/content/docs/pages/specs/extensions.mdx#4-immutable-reads).

## 1. Why

A reader that starts behind the tail downloads bytes that can no longer change. A SQLite database
attaching on a fresh host is the largest case today (sqlite-vfs.md §9): a 1 GB database fetches a
237 MB snapshot and 74 MB of log after it, and every new host fetches the same bytes from the
leader of the stream's group. A CDN could serve them from an edge near the host, off the Raft
nodes, at a fraction of the egress cost.

Nothing in the protocol lets it do that safely today:

- **The URL does not name an incarnation.** A stream deleted and recreated at the same URL reuses
  its offsets, so a cache keyed by URL would serve the old stream's bytes for the new one. The
  `Stream-Incarnation` precondition is a header, and a header is not part of a cache key.
- **Nobody says which responses are final.** A layer in front has to re-derive it from the request
  and the response (`max_bytes` filled, `Stream-Closed`), and a tail response with
  `Stream-Up-To-Date: true` that a cache stores would tell a later reader it is current when it is
  not.
- **Readers do not share URLs.** Each reader continues from wherever its previous response ended,
  so two hosts reading the same history rarely request the same `offset`.
- **The client has one URL.** The VFS takes one URL per database. Putting that hostname behind a
  CDN proxy adds latency to every write and every tail read, which a cache cannot serve.

The extension is generic on purpose. Nothing in it knows about SQLite: snapshots and history are
Durable Streams concepts, and any client that replays from a snapshot benefits the same way.

## 2. Shape

The extension has four parts, each useful alone:

1. `incarnation={token}` in the URL: the existing precondition, now part of the cache key.
2. `aligned=true`: the server ends a read at the next chunk boundary, so readers converge on the
   same URLs after one response.
3. `Stream-Immutable: true` on a response that can never change for its URL.
4. `Stream-Read-Origin` on `HEAD`: where to send immutable reads, chosen by whoever runs the
   deployment.

The split of responsibilities follows what each layer knows:

| Layer | Knows | Decides |
| --- | --- | --- |
| Ursula node | the stream's state | whether a response is immutable (the marker) |
| Gateway or proxy in front | who may read a bucket, which CDN exists | `Cache-Control` on marked responses, the hint |
| Cache | nothing about streams | stores what `Cache-Control` allows |
| Client | its own trust settings | whether to follow the hint, and checks every response |

The node never relaxes `Cache-Control` (render.rs keeps `no-store`), and never sends the hint. A
self-hosted deployment without a gateway gets correct, uncached behavior.

## 3. Server changes

All in `crates/ursula`, the HTTP layer. The state machine already returns everything needed.

- **`incarnation` parameter.** Parse it next to `incarnation_precondition`. When the header is also
  present they must agree (otherwise 400). The parsed value feeds the same `if_incarnation` path, so
  evaluation order (404, 410 and 416 before 412) is unchanged. Reject it on live reads and
  `offset=now`. The snapshot read takes it the same way.
- **`aligned=true`.** Compute `max_len = next_boundary(offset) - offset` with a 4 MiB grid, below
  the 8 MiB cap. Reject it with `max_bytes` or `live`. The read plan already ends at
  `min(offset + max_len, tail)` and never returns a short read below the tail, so no runtime change
  is needed.
- **The marker.** Set in `read_response` and the snapshot read when the request carried the
  parameter and the response is not up to date, or is closed at the tail. Snapshot reads are always
  marked, since a published offset never changes content.
- **A stable `ETag`.** `read_etag` hashes `closed` for every response, so a non-tail chunk changes
  its `ETag` when the stream is closed later. Hash `closed` only when the response reaches the tail.
  The base protocol needs the `ETag` to vary with closure only where closure is visible.

Tests:

- Unit tests for the marker conditions, the grid, and the parameter conflicts.
- An end-to-end test in `crates/ursula/tests` that reads a stream aligned, closes it, and checks
  that the earlier URLs return the same `ETag`.
- A recreate test that checks an old `incarnation` URL answers 412.

## 4. Client changes (SQLite VFS)

**Setting.** `URSULA_VFS_READ_ORIGIN`: unset follows the hint, `off` ignores it, an `https://`
origin overrides it. The hint is read from the `HEAD` that attach already sends.

**Guards.** Use the read origin only for snapshot reads and history reads, both with
`incarnation`. Follow a hint only when it is `https` and within the stream URL's registrable
domain (decided with a public suffix list), or when it is configured explicitly. Accept a response
only when it is a 200 whose `Stream-Incarnation` matches, and for a snapshot whose offset and digest
match `HEAD`. The VFS already verifies a snapshot's size and checksum before installing it.
Anything else repeats the request at the stream's origin and stops using the read origin for this
attach.

**Attach.**

1. `HEAD` at the stream's origin, as today: snapshot offset and digest, tail, incarnation, hint.
2. The snapshot from the read origin, when one is due (§4.3 of sqlite-vfs.md).
3. History from the file's offset with aligned reads at the read origin, without `consistency`,
   while responses are marked immutable.
4. The rest with today's `consistency=leader` reads at the stream's origin, until up to date.

The reader already reassembles frames that span responses, because the server's 8 MiB cap cuts
reads at arbitrary offsets. Aligned reads only move the cuts. Steady-state tail reads and every
write are unchanged and never touch the read origin.

**Retention.** A cached chunk below the retained offset still returns its bytes where the server
would answer 410. They are correct bytes of the same incarnation, so attach succeeds instead of
restarting from `HEAD`.

Tests:

- A fake read origin that returns the wrong incarnation, a 404, a short body, a stale
  `Stream-Up-To-Date`, and a transport error, each followed by a correct attach from the stream's
  origin.
- A frame that spans a chunk boundary.
- A hint outside the stream's site, which is not followed.

## 5. Deployment

The layer in front turns the marker into caching. For a stream that needs authorization:

- Authorize every request before the cache lookup, and key the cache on the URL plus whatever the
  authorization depends on beyond it.
- Replace `Cache-Control` on marked responses only, for example
  `private, max-age=86400, immutable`, with a lifetime bounded by how long a deleted stream's bytes
  may stay reachable.
- Send `Stream-Read-Origin` on `HEAD` for buckets where it is enabled.

Two hostnames then serve one deployment: the stream's own origin, which needs no CDN in its path,
and the read origin behind the CDN. Large snapshots need a cache that stores objects of their size.

## 6. Compatibility

Every piece is optional on both sides, and the client checks every response, so all four
combinations of old and new are safe:

- A new client and an old server: the server ignores `incarnation` and `aligned`, sends no hint, and
  the client reads as today. With a configured read origin, the incarnation check rejects responses
  from a recreated stream.
- An old client and a new server: nothing changes. A hint is a header the old client ignores.

## 7. Open questions

- Whether to propose the extension upstream as written, once it has run in production.
- Whether `ursulagw` should be able to send the hint, for self-hosted deployments that put a CDN in
  front of it.
- Range requests on large snapshots, so that a download interrupted at the edge resumes.
