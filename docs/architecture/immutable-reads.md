# Immutable reads through a shared cache

The protocol is
[extensions §4](../web/src/content/docs/pages/specs/extensions.mdx#4-immutable-reads).

## 1. Why

A reader that starts behind the tail downloads bytes that can no longer change. A SQLite database
attaching on a fresh host is the largest case (sqlite-vfs.md §9): a 1 GB database fetches a 237 MB
snapshot and 74 MB of log after it, and every new host fetches the same bytes from the leader of
the stream's group. A CDN can serve them from an edge near the host, off the Raft nodes.

Without the extension a cache cannot do that safely:

- **The URL does not name an incarnation.** A stream deleted and recreated at the same URL reuses
  its offsets, so a cache keyed by URL would serve the old stream's bytes for the new one. The
  `Stream-Incarnation` precondition is a header, and a header is not part of a cache key.
- **Nothing says which responses are final.** A tail response with `Stream-Up-To-Date: true` that
  a cache stores tells a later reader it is current when it is not.
- **Readers do not share URLs.** Each reader continues from wherever its previous response ended,
  so two hosts reading the same history rarely request the same `offset`.
- **A client has one URL.** Putting a database's hostname behind a CDN proxy adds latency to every
  write and every tail read, which a cache cannot serve.

The extension knows nothing about SQLite. Snapshots and history are Durable Streams concepts, and
any client that replays from a snapshot can use it.

## 2. Shape

| Part | Set by | Meaning |
| --- | --- | --- |
| `incarnation={token}` | client | The incarnation precondition in the URL, so it is part of the cache key. |
| `aligned=true` | client | The read ends at the next 4 MiB boundary, so readers converge on the same URLs. |
| `Stream-Immutable: true` | node | This response can never change for its URL. |
| `Stream-Read-Origin` on `HEAD` | deployment | Where to send immutable reads. |

Each layer decides what it knows:

| Layer | Knows | Decides |
| --- | --- | --- |
| Ursula node | the stream's state | whether a response is immutable |
| Gateway or proxy in front | who may read a bucket, which CDN exists | `Cache-Control` on marked responses, the hint |
| Cache | nothing about streams | stores what `Cache-Control` allows |
| Client | its own trust settings | whether to follow the hint, and checks every response |

The node keeps `Cache-Control: no-store` on every read and never sends the hint. A deployment with
no layer in front gets correct, uncached behaviour.

## 3. Server

The HTTP layer in `crates/ursula` implements the extension. The state machine needed no change: a
read already ends at `min(offset + max_len, tail)` and never returns less below the tail.

- **`incarnation`.** `incarnation_parameter` parses it as the server issues tokens, a canonical
  decimal with no surrounding whitespace. `read_incarnation_precondition` combines it with the
  header (400 when they disagree) and feeds the one `if_incarnation` path, so 404, 410 and 416 still
  come before 412. Catch-up and snapshot reads take it. Live reads and `offset=now` answer 400.
  `parse_query` answers 400 for a repeated `offset`, `incarnation` or `aligned`.
- **`aligned=true`.** `aligned_read_len` turns the offset into the length up to the next multiple
  of `READ_ALIGNED_CHUNK_BYTES` (4 MiB), and the 8 MiB response cap applies after it. A const
  assertion keeps the chunk within the cap. `aligned` with `max_bytes` or `live` answers 400.
- **The marker.** `read_response` sets `Stream-Immutable: true` on a 200 when the request carried
  the parameter and the response is not up to date or is closed at the tail. `snapshot_response`
  sets it on every 200 that carried the parameter. A 304, a long-poll, a 204 and errors never
  carry it.
- **A stable `ETag`.** `read_etag` hashes closure only for a response that reaches the tail, so a
  chunk below the tail keeps its `ETag` when the stream is closed.

## 4. Client (SQLite VFS)

`clients/sqlite-vfs/src/read_origin.rs` holds the policy, and `attach.rs` uses it.

**Choosing a read origin.** `URSULA_VFS_READ_ORIGIN` takes `off` or an `https://host[:port]`
origin. Unset, attach follows the `Stream-Read-Origin` hint of its `HEAD` when the hint is an https
origin with no path and its host has the stream URL's registrable domain, decided with the public
suffix list (`psl`). An IP address or `localhost` matches only itself. A hint that fails is ignored
and logged once. Every read origin in use is either within the stream's site or configured, so it
receives the same credentials as the stream's origin.

**Attach.**

1. `HEAD` at the stream's origin: snapshot offset and digest, tail, incarnation, hint.
2. The snapshot, when one is due, from the read origin at
   `{path}/snapshot/{offset}?incarnation={inc}`. Accepted only as a 200 of that incarnation whose
   offset and digest match `HEAD`, and whose body then passes the checks a snapshot from the
   stream's origin passes. Without a digest from `HEAD`, the snapshot comes from the stream's
   origin.
3. History from the file's offset at `{path}?offset={o}&aligned=true&incarnation={inc}`, with no
   `consistency` parameter and no incarnation header. Accepted as a 200 of that incarnation with a
   body and a next offset past the requested one. Its `Stream-Up-To-Date` is never read.
4. The rest with `consistency=leader` reads at the stream's origin, until up to date. The claim and
   the catch-up after it always go to the stream's origin.

The history reads leave the read origin at `HEAD`'s tail or after the first response not marked
immutable. Any failed check, any other status (a 412 included) and any transport error send that
request to the stream's origin, end the read origin for the rest of the attach, and are logged once
(`read_origin_fallback`). Nothing from the read origin is taken as a recreated stream: only the
stream origin's 412 means that. One frame buffer spans both origins, so a frame cut at a chunk
boundary is put back together as one cut at the response cap always was.

Steady-state tail reads, writes, the snapshot thread and retention never use the read origin.

**Retention.** A cached chunk below the retained offset still returns its bytes where the server
would answer 410. They are correct bytes of the same incarnation, so attach succeeds.

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

Every part is optional on both sides, and the client checks every response:

- A new client and an old server: the server ignores `incarnation` and `aligned`, marks nothing and
  sends no hint, so the client reads as before. With a configured read origin, the incarnation
  check rejects responses of a recreated stream, and the missing marker ends the read origin after
  one response.
- An old client and a new server: nothing changes. The hint is a header the old client ignores.
- A below-tail chunk of a stream closed before the upgrade gets a new `ETag` once, so a client's
  `If-None-Match` misses once.

## 7. Tests

- `crates/ursula/src/tests.rs`: the marker conditions on catch-up and snapshot reads, the aligned
  grid, every 400, the parameter as the precondition (412, 416, 404 after delete, 412 after
  recreate), the `ETag` across close, and an aligned JSON read.
- `clients/sqlite-vfs`: an attach that reads the snapshot and two chunks at the read origin and
  the tail at the stream's origin, a frame across a chunk boundary, every fallback (wrong
  incarnation, digest mismatch, 404, 412, truncated body, stale `Stream-Up-To-Date`, missing marker,
  transport error) ending in a correct attach that is not taken for a recreate, and the hint
  guards.
