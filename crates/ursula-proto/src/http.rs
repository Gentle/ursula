//! Client-plane HTTP signals shared by the server and the gateway.

/// Response header marking a request that was refused before it reached a
/// stream and applied nothing.
///
/// A layer sets it only when it can guarantee that nothing of the request was
/// applied: the server's ingress admission, the gateway before it forwards a
/// request, or a proxy in front of either. A client may then treat the attempt
/// as having applied nothing, whatever the status. Without the header a
/// response promises nothing either way.
///
/// The value is a short token naming the refusal, such as `RaftLogPressure`,
/// for logs and diagnostics. The header's presence is the signal. A layer that
/// answers one request with several upstream exchanges, as a gateway following
/// a leader redirect does, keeps the header only if every exchange carried it.
pub const NOT_APPLIED_HEADER: &str = "x-ursula-not-applied";
