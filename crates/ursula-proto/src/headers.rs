//! Client-plane response headers shared by the server and the gateway.
//!
//! [`NOT_APPLIED_HEADER`] marks an attempt that was refused before anything
//! of it was proposed. [`NOT_APPLIED_CHECKED_HEADER`] is the vouch: a
//! gateway's statement that it applied its redirect rule to the marker. A
//! client trusts the marker only together with the vouch.

/// Response header marking an attempt that was refused before anything of it
/// was proposed.
///
/// Nothing was proposed, so nothing of the attempt can apply later, whatever
/// the status. A layer sets the marker only when it knows this: the server's
/// ingress admission, which refuses a write before it reaches a handler, a
/// node that refused a create, append, close or delete before it proposed
/// the write's one command, a leader redirect included, and the gateway, for
/// the requests it refuses before forwarding them and for a request whose
/// first upstream it could not connect to. The value is a short token naming
/// the refusal, such as `RaftLogPressure`, for logs and diagnostics. The
/// header's presence is the signal.
///
/// The marker speaks for one exchange. A layer that answers one request with
/// several upstream exchanges, as a gateway following a leader redirect does,
/// keeps it only if every exchange carried it, because a node can redirect a
/// write after it proposed it. The marker says nothing about earlier attempts
/// of the same write either.
///
/// A client may treat a response as proof that the attempt applied nothing
/// and cannot apply later only when
///
/// - its status is `401`, `402` or `429`, which a layer in front of the nodes
///   answers before it forwards a request, and which the state machine
///   answers when a stream has no room for a new producer (within the same
///   attempt, a replay of a write that did commit is deduplicated instead),
///   or
/// - it carries both this header and [`NOT_APPLIED_CHECKED_HEADER`].
///
/// Any other response proves nothing, including a `403`, `413` or `503`
/// without both headers.
pub const NOT_APPLIED_HEADER: &str = "x-ursula-not-applied";

/// Response header with which a gateway vouches for [`NOT_APPLIED_HEADER`].
/// Its value is `1`.
///
/// A gateway from 0.7.3 on sets it on every response it returns to its
/// client, after it applied the redirect rule: on forwarded responses, on its
/// own refusals and on its own errors. It removes any vouch that came from an
/// upstream before it adds its own. Nodes never set it.
///
/// The gateway sees only its own exchanges with its upstreams, so they must be
/// the nodes themselves. A proxy, service-mesh sidecar or another gateway
/// between the gateway and the nodes must not resend requests, or the gateway
/// vouches for a marker from a later delivery: a node can propose a write and
/// answer an unmarked `503`, and a resent copy can then be refused at
/// admission with a marked one. For the same reason, a 0.7.3 gateway must not
/// sit in front of an older gateway (see below). Nothing in between may answer
/// `401`, `402` or `429` either, as a sidecar's local rate limit does: those
/// statuses count without the headers, and after a redirect without a marker
/// the gateway returns the leader's status unchanged.
///
/// Gateways before 0.7.3 pass a leader's marker on after any redirect they
/// follow, including one that a node sent after it proposed the write. They
/// never send the vouch, so a client that requires it does not trust their
/// markers, whatever order gateways and nodes are upgraded or rolled back in.
/// A client that talks to the nodes directly gets no vouch either and ignores
/// the marker.
pub const NOT_APPLIED_CHECKED_HEADER: &str = "x-ursula-not-applied-checked";
