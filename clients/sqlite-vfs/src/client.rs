//! The stream, over blocking HTTP: appends with the idempotent producer, reads, `HEAD`,
//! snapshot and retention requests, and their retries.

use std::fs;
use std::io::Read;
use std::net::IpAddr;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use ureq::http::Uri;
use ureq::tls::Certificate;
use ureq::tls::PemItem;
use ureq::tls::RootCerts;
use ureq::tls::TlsConfig;

use crate::auth;
use crate::config::ca_file;
use crate::config::retry_budget;
use crate::error::Attempt;
use crate::error::Error;
use crate::error::Gone;
use crate::error::WriteRefusal;
use crate::log;
use crate::log::Level;

/// `Producer-Id` prefix; the stream incarnation follows (`producer_id`).
const PRODUCER: &str = "sqlite-ursula-vfs";

const CONTENT_TYPE: &str = "application/octet-stream";

/// The protocol's offset for the beginning of a stream, and "none" for an offset that may be absent
/// (a snapshot, retention, the local state): it sorts before every offset the server mints.
pub(crate) const START: &str = "-1";

fn agent() -> &'static ureq::Agent {
    static A: OnceLock<ureq::Agent> = OnceLock::new();
    A.get_or_init(|| new_agent(Duration::from_secs(10)))
}

/// Snapshot transfers move whole databases: a longer timeout than appends.
fn bulk_agent() -> &'static ureq::Agent {
    static A: OnceLock<ureq::Agent> = OnceLock::new();
    A.get_or_init(|| new_agent(Duration::from_secs(120)))
}

fn new_agent(timeout: Duration) -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_global(Some(timeout))
        .http_status_as_error(false)
        .tls_config(tls())
        .build()
        .into()
}

/// TLS for `https://` stream URLs: the bundled Mozilla roots, or only the certificate authorities
/// in `URSULA_VFS_CA_FILE`. A CA file without a usable certificate trusts nothing (every TLS
/// request then fails), and says why once.
fn tls() -> TlsConfig {
    static T: OnceLock<TlsConfig> = OnceLock::new();
    T.get_or_init(|| {
        let Some(path) = ca_file() else {
            return TlsConfig::builder().build();
        };
        let certs: Vec<Certificate<'static>> = match fs::read(path) {
            Ok(pem) => ureq::tls::parse_pem(&pem)
                .filter_map(|item| match item {
                    Ok(PemItem::Certificate(cert)) => Some(cert),
                    _ => None,
                })
                .collect(),
            Err(e) => {
                log::emit(Level::Error, "ca_file_unreadable", &[
                    ("path", &path),
                    ("error", &e),
                ]);
                Vec::new()
            }
        };
        if certs.is_empty() {
            log::emit(Level::Error, "ca_file_without_certificates", &[(
                "path", &path,
            )]);
        }
        TlsConfig::builder()
            .root_certs(RootCerts::new_with_certs(&certs))
            .build()
    })
    .clone()
}

/// A token went out in clear (`authorized`), and the log said so.
static IN_CLEAR_LOGGED: AtomicBool = AtomicBool::new(false);

/// `req` with the bearer token, when there is one; `refresh` reads the token file again. A token
/// sent over plain HTTP to another host crosses the network in clear: logged once per process.
fn authorized<B>(req: ureq::RequestBuilder<B>, refresh: bool) -> ureq::RequestBuilder<B> {
    let Some(token) = auth::token(refresh) else {
        return req;
    };
    if let Some(uri) = req.uri_ref().filter(|uri| in_clear(uri))
        && !IN_CLEAR_LOGGED.swap(true, Ordering::Relaxed)
    {
        log::emit(Level::Warn, "token_in_clear", &[("url", uri)]);
    }
    req.header("authorization", format!("Bearer {token}"))
}

/// A plain `http://` URL whose host is not a loopback address (or `localhost`).
fn in_clear(uri: &Uri) -> bool {
    let loopback = |host: &str| {
        host.eq_ignore_ascii_case("localhost")
            || host
                .trim_start_matches('[')
                .trim_end_matches(']')
                .parse::<IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    };
    uri.scheme_str() == Some("http") && !uri.host().is_some_and(loopback)
}

/// How an answer refuses a request before it reaches the stream, or fences its owner: the one
/// place a fence is told from a refusal in front of the stream.
enum Refused {
    /// A 401: an authorizer in front of the stream refused the token. Sent again, with the token
    /// read again, within the retry budget.
    Token,
    /// A 402: a layer in front of the stream (an authorizer, a quota or billing service) refuses
    /// the request until something is paid or settled. Final for the request, never a fence, and
    /// nothing of it reached the stream (unless an earlier attempt of an append did: `append`).
    PaymentRequired,
    /// A 403 without `Producer-Epoch`: an authorizer denied the request. Final, and never a fence.
    /// Nothing of it reached the stream only when the answer proves it (`applied_nothing`: marked
    /// `x-ursula-not-applied`, with a gateway's vouch): a filtering proxy can answer 403 in place
    /// of the stream's own answer (see `append`).
    Denied,
    /// A 403 with `Producer-Epoch`: the server's fence, which names the epoch that owns the stream
    /// (only an append can meet it).
    Fenced { current: Option<u64> },
}

fn refused(r: &ureq::http::Response<ureq::Body>) -> Option<Refused> {
    match r.status().as_u16() {
        401 => Some(Refused::Token),
        402 => Some(Refused::PaymentRequired),
        403 if r.headers().contains_key("producer-epoch") => Some(Refused::Fenced {
            current: header_u64(r, "producer-epoch"),
        }),
        403 => Some(Refused::Denied),
        _ => None,
    }
}

/// The response header with which a layer (the server's ingress admission, a gateway, a proxy in
/// front of either) marks its refusal (a 4xx or 5xx) of a request it refused before passing it on:
/// the stream applied nothing of it. The value is a short token naming why (`RaftLogPressure`,
/// say). Older layers never send it. It counts only on a refusal status and with a gateway's vouch
/// (`NOT_APPLIED_CHECKED`): see `applied_nothing`.
const NOT_APPLIED: &str = "x-ursula-not-applied";

/// The response header with which a gateway vouches for `NOT_APPLIED`. Ursula's gateway sets it
/// (to `1`) from 0.7.3 on, on every answer it returns: the answers it forwards, after it applied
/// its own rule to their marker (an answer it got by following a leader redirect keeps the marker
/// only if the redirect carried it too), and its own refusals and errors. It removes any copy that
/// comes from upstream before it adds its own, and a node never sets it. A gateway before 0.7.3
/// relays the marker of an answer it got by following a redirect, which a node can send after it
/// proposed the write, and it never vouches: through it, and without a gateway, a marker proves
/// nothing.
const NOT_APPLIED_CHECKED: &str = "x-ursula-not-applied-checked";

/// How an answer proves that the stream applied nothing of the request it answers (see
/// `applied_nothing`).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Proof {
    /// Its status: 401, 402 or 429.
    Status,
    /// A refusal status (4xx or 5xx) with `x-ursula-not-applied` and a gateway's vouch: the
    /// marker's token, cut to `BODY_SHOWN` bytes like a body. A marker with an empty value still
    /// marks.
    Marked(String),
}

impl Proof {
    /// The refusal that an answer with `status` and `body`, proven this way, stands for: a 401, 402
    /// or 403 keeps its own kind, a vouched marker names any other (`NotApplied`), and a 429
    /// without one is rate limiting.
    pub(crate) fn refusal(self, status: u16, body: String) -> WriteRefusal {
        match (status, self) {
            (401, _) => WriteRefusal::Unauthorized { body },
            (402, _) => WriteRefusal::PaymentRequired { body },
            (403, _) => WriteRefusal::Forbidden { body },
            (_, Proof::Marked(reason)) => WriteRefusal::NotApplied { status, reason },
            (_, Proof::Status) => WriteRefusal::RateLimited { body },
        }
    }
}

/// Whether an answer proves that the stream applied nothing of the request it answers, and how: the
/// one place that decides it (design doc §2). It does when its status is 401 or 402 (a layer in
/// front of the stream refused the request before passing it on) or 429 (the same, or the server's
/// limit on a stream's producers, which refuses only the first append of a producer the stream does
/// not hold: an attempt sent after an earlier one was applied is answered as its duplicate), or
/// when a refusal status (4xx or 5xx) carries both `x-ursula-not-applied` and the vouch
/// `x-ursula-not-applied-checked`. Any other answer proves nothing: a 403, 413 or 503 with only one
/// of the two headers, and a redirect or a success whatever headers it carries (`None`). It reads
/// the status and the headers only, never the body.
pub(crate) fn applied_nothing<B>(r: &ureq::http::Response<B>) -> Option<Proof> {
    let status = r.status();
    let headers = r.headers();
    if let Some(marker) = headers.get(NOT_APPLIED).filter(|_| {
        headers.contains_key(NOT_APPLIED_CHECKED)
            && (status.is_client_error() || status.is_server_error())
    }) {
        let value = marker.as_bytes();
        let shown = value.get(..BODY_SHOWN).unwrap_or(value);
        return Some(Proof::Marked(
            String::from_utf8_lossy(shown).trim().to_owned(),
        ));
    }
    matches!(status.as_u16(), 401 | 402 | 429).then_some(Proof::Status)
}

/// The error of request `op` to `url` that a layer in front of the stream refused (a refused
/// token, a denial, or a 402), never a fence; `None` for any other answer.
fn refusal(op: &'static str, url: &str, r: &mut ureq::http::Response<ureq::Body>) -> Option<Error> {
    let status = r.status().as_u16();
    match refused(r)? {
        Refused::Token | Refused::Denied => Some(unauthorized(op, url, status)),
        Refused::PaymentRequired => Some(refused_request(op, url, WriteRefusal::PaymentRequired {
            body: body_text(r),
        })),
        Refused::Fenced { .. } => None,
    }
}

fn unauthorized(op: &'static str, url: &str, status: u16) -> Error {
    Error::Unauthorized {
        op,
        url: url.to_owned(),
        status,
    }
}

fn refused_request(op: &'static str, url: &str, refusal: WriteRefusal) -> Error {
    Error::Refused {
        op,
        url: url.to_owned(),
        refusal,
    }
}

fn header_u64(r: &ureq::http::Response<ureq::Body>, name: &str) -> Option<u64> {
    r.headers().get(name)?.to_str().ok()?.trim().parse().ok()
}

/// An offset as the server wrote it: opaque, kept verbatim and compared only lexicographically
/// (never parsed or computed with). `None` unless it is usable in a URL path or query and a
/// sidecar as it is: 1 to 64 unreserved URL characters.
pub(crate) fn offset_token(v: &str) -> Option<String> {
    let ok = |b: u8| b.is_ascii_alphanumeric() || b"-._~".contains(&b);
    (!v.is_empty() && v.len() <= 64 && v.bytes().all(ok)).then(|| v.to_owned())
}

pub(crate) fn header_offset(r: &ureq::http::Response<ureq::Body>, name: &str) -> Option<String> {
    offset_token(r.headers().get(name)?.to_str().ok()?.trim())
}

/// The wait before the next retry: no sooner than `retry_after`, and at least `backoff`. `None`
/// when the wait would end past `deadline` (or overflow, for an absurd Retry-After): no retry.
fn next_wait(
    retry_after: Option<Duration>,
    backoff: Duration,
    deadline: Instant,
) -> Option<Duration> {
    let wait = retry_after.map_or(backoff, |after| after.max(backoff));
    Instant::now()
        .checked_add(wait)
        .is_some_and(|end| end <= deadline)
        .then_some(wait)
}

/// Sleeps `wait` (from `next_wait`), then doubles `backoff` (up to 1 s).
fn back_off(wait: Duration, backoff: &mut Duration) {
    std::thread::sleep(wait);
    *backoff = backoff.saturating_mul(2).min(Duration::from_secs(1));
}

/// Waits before the next retry (`next_wait`, then `back_off`). Returns false, without waiting,
/// when there is no retry.
fn pause(retry_after: Option<Duration>, backoff: &mut Duration, deadline: Instant) -> bool {
    let Some(wait) = next_wait(retry_after, *backoff, deadline) else {
        return false;
    };
    back_off(wait, backoff);
    true
}

/// When the retries of a request end: `retry_budget()` from now (now, for a budget past what
/// `Instant` can hold).
fn retry_deadline() -> Instant {
    let now = Instant::now();
    now.checked_add(retry_budget()).unwrap_or(now)
}

/// Sends a request that is safe to repeat (a read, or a create), retrying 429 (rate limiting), 503
/// (overload, or a `consistency=leader` read, HEAD or snapshot read the leader could not confirm
/// with a quorum in time) and a refused token (`Refused::Token`: sent again with the token read
/// again) the way `append` does: no sooner than Retry-After (seconds), with backoff, within
/// `retry_budget()`. Returns the first other answer, or the last retried one once the budget is
/// spent or `stopped` (a re-attach is waiting for the snapshot thread). `send` takes whether to
/// read the token again.
fn send_retrying(
    send: impl Fn(bool) -> Result<ureq::http::Response<ureq::Body>, ureq::Error>,
    stopped: &dyn Fn() -> bool,
) -> Result<ureq::http::Response<ureq::Body>, ureq::Error> {
    let deadline = retry_deadline();
    let mut backoff = Duration::from_millis(20);
    let mut refresh = false;
    loop {
        let r = send(refresh)?;
        refresh = matches!(refused(&r), Some(Refused::Token));
        if !refresh && !matches!(r.status().as_u16(), 429 | 503) {
            return Ok(r);
        }
        let retry_after = header_u64(&r, "retry-after").map(Duration::from_secs);
        if stopped() || !pause(retry_after, &mut backoff, deadline) {
            return Ok(r);
        }
    }
}

pub(crate) enum Append {
    /// Applied (or a duplicate of an applied append); the stream offset after it when known.
    Acked { next: Option<String>, attempts: u32 },
    /// 403 with `Producer-Epoch`: a newer epoch owns the stream.
    Fenced { current: Option<u64> },
    /// 412: the stream is no longer the incarnation the append was sent to (deleted and
    /// recreated); nothing was appended.
    Recreated,
    /// 409 expecting sequence 0: the server does not know this producer: it expired it after 7
    /// idle days (a recreated stream answers 412 first).
    ProducerExpired,
    /// 409 refusing the commit's `Stream-Seq` (not above the stream's last one): another writer
    /// appended with a higher one (see `stream_seq`). `body` is the server's explanation.
    SeqConflict { body: String },
    /// Refused before it reached the stream, and so was every earlier attempt of it (see
    /// `append`): nothing was appended, and the producer sequence is still free.
    Refused(WriteRefusal),
    /// A definite rejection, or no answer within the retry budget.
    Failed(Error),
}

/// The `Producer-Id` of every owner of one stream incarnation: owners of the same incarnation fence
/// each other by epoch (a recreated stream refuses an owner of the deleted one with 412, see
/// `append`).
pub(crate) fn producer_id(incarnation: &str) -> String {
    format!("{PRODUCER}/{incarnation}")
}

/// One idempotent append by `producer` to stream incarnation `incarnation` (sent as the
/// `Stream-Incarnation` precondition): retried with the same producer sequence until the outcome
/// is known.
///
/// A duplicate answer is taken as proof of *our* earlier attempt only for commits (seq >= 1), and
/// only with its receipt (`Stream-Next-Offset`, checked by `commit`): they are sent after a
/// verified claim (see `claim_once`), which makes this owner the only writer of its incarnation's
/// producer at its epoch, so whatever holds (epoch, seq) there is ours. A claim's answer is
/// verified separately. Commits also carry their `Stream-Seq` (see `stream_seq`).
///
/// An attempt whose answer proves that it applied nothing (`applied_nothing`: a 401, 402 or 429, or
/// any answer marked `x-ursula-not-applied` that a gateway vouched for with
/// `x-ursula-not-applied-checked`) is taken to have applied nothing, which every layer in front of
/// the stream must ensure (design doc §2): one that passed the append on and then answered so would
/// have the next commit acknowledged as this one's duplicate, its pages never appended. A marker
/// without the vouch proves nothing: a gateway before 0.7.3 relays the marker of an answer it got
/// by following a leader redirect, which a node can send after it proposed the append. A 403
/// without `Producer-Epoch` counts only when proven: a filtering proxy can answer 403 in place of
/// the stream's own answer, after the stream applied the append, so any other fails the append, as
/// before 0.7.3. While no attempt of the append can have reached the stream, a 402 or a proven
/// final answer (any other 4xx, an authorizer's 403 among them) ends it as [`Append::Refused`] at
/// once, and the retried ones (401, 429, a proven 5xx) end it so once the budget runs out. After
/// an attempt whose outcome is unknown (no answer, or a 5xx that proves nothing), the append may be
/// in the stream, and the next commit, sent with the same sequence, would be acknowledged as its
/// duplicate. Its refusals then prove nothing: a 402 is retried like an unknown outcome, until an
/// answer settles it or the budget runs out ([`Error::AppendUnknown`]), and a 403 fails it.
pub(crate) fn append(
    url: &str,
    incarnation: &str,
    producer: &str,
    body: &[u8],
    epoch: u64,
    seq: u64,
) -> Append {
    let started = Instant::now();
    let deadline = retry_deadline();
    let mut backoff = Duration::from_millis(20);
    let mut attempts: u32 = 0;
    // The last answer refused the token: the next attempt reads it again.
    let mut token_refused = false;
    // An earlier attempt may have reached the stream: its outcome is unknown.
    let mut reached = false;
    loop {
        attempts = attempts.saturating_add(1);
        let mut req = authorized(agent().post(url), std::mem::take(&mut token_refused))
            .header("content-type", CONTENT_TYPE)
            .header("stream-incarnation", incarnation)
            .header("producer-id", producer)
            .header("producer-epoch", epoch.to_string())
            .header("producer-seq", seq.to_string());
        if seq > 0 {
            req = req.header("stream-seq", stream_seq((epoch, seq)));
        }
        let sent = req.send(body);
        let mut retry_after = None;
        // The attempt retried (for the log and the error), and why it applied nothing: `None`
        // when it may have reached the stream.
        let (unknown, refusal) = match sent {
            Ok(mut r) => {
                let status = r.status().as_u16();
                // Rate limiting (429, e.g. ursulagw) and overload (503) are transient: retried
                // with the same producer sequence, no sooner than Retry-After (seconds).
                retry_after = header_u64(&r, "retry-after").map(Duration::from_secs);
                // Whether the attempt applied nothing: every branch below takes it from here.
                let proof = applied_nothing(&r);
                match refused(&r) {
                    Some(Refused::Fenced { current }) => return Append::Fenced { current },
                    // Unproven, the denial may have replaced the stream's answer (see above).
                    Some(Refused::Denied) => {
                        return match proof {
                            Some(proof) if !reached => {
                                Append::Refused(proof.refusal(status, body_text(&mut r)))
                            }
                            _ => Append::Failed(unauthorized("append", url, status)),
                        };
                    }
                    Some(Refused::PaymentRequired) => {
                        let body = body_text(&mut r);
                        match proof.map(|proof| proof.refusal(status, body.clone())) {
                            Some(refusal) if !reached => return Append::Refused(refusal),
                            // An earlier attempt may be in the stream: the outcome is still
                            // unknown.
                            refusal => (Attempt::Status { status, body }, refusal),
                        }
                    }
                    Some(Refused::Token) => {
                        token_refused = true;
                        let body = body_text(&mut r);
                        let refusal = proof.map(|proof| proof.refusal(status, body.clone()));
                        (Attempt::Status { status, body }, refusal)
                    }
                    None => match status {
                        200..=299 => {
                            return Append::Acked {
                                next: header_offset(&r, "stream-next-offset"),
                                attempts,
                            };
                        }
                        // The stream was deleted and recreated: nothing was appended.
                        412 => return Append::Recreated,
                        409 if seq > 0 && header_u64(&r, "producer-expected-seq") == Some(0) => {
                            return Append::ProducerExpired;
                        }
                        // Neither a producer sequence conflict nor a closed stream: the
                        // `Stream-Seq`.
                        409 if seq > 0
                            && !r.headers().contains_key("producer-expected-seq")
                            && !r.headers().contains_key("stream-closed") =>
                        {
                            return Append::SeqConflict {
                                body: body_text(&mut r),
                            };
                        }
                        // A definite rejection (429 is retried, below), final: the append fails
                        // alone when the answer proves it applied nothing and no earlier attempt
                        // can have reached the stream.
                        400..=499 if status != 429 => {
                            let body = body_text(&mut r);
                            return match proof {
                                Some(proof) if !reached => {
                                    Append::Refused(proof.refusal(status, body))
                                }
                                _ => Append::Failed(Error::AppendRejected { status, body }),
                            };
                        }
                        _ => {
                            let body = body_text(&mut r);
                            let refusal = proof.map(|proof| proof.refusal(status, body.clone()));
                            (Attempt::Status { status, body }, refusal)
                        }
                    },
                }
            }
            Err(e) => (Attempt::Transport(Box::new(e)), None),
        };
        // An answer that does not prove the attempt applied nothing (`refusal` is set exactly
        // when `applied_nothing` proved it), or none, may come after the stream applied the
        // append (see above).
        reached |= refusal.is_none();
        // A retried commit is a stall the application sees: say why, attempt by attempt, and
        // whether another attempt follows.
        let wait = next_wait(retry_after, backoff, deadline);
        let event = if wait.is_some() {
            "append_retry"
        } else {
            "append_gave_up"
        };
        log::emit(Level::Warn, event, &[
            ("stream", &url),
            ("attempt", &attempts),
            ("elapsed_ms", &started.elapsed().as_millis()),
            ("reason", &unknown),
        ]);
        let Some(wait) = wait else {
            return match refusal {
                // Every attempt was refused before it reached the stream: nothing was appended.
                Some(refusal) if !reached => Append::Refused(refusal),
                _ if token_refused => Append::Failed(unauthorized("append", url, 401)),
                _ => Append::Failed(Error::AppendUnknown {
                    attempts,
                    last: unknown,
                }),
            };
        };
        back_off(wait, &mut backoff);
    }
}

/// Creates the stream at `url` (idempotent: an existing stream of the same content type answers
/// 200).
pub(crate) fn create_stream(url: &str) -> Result<(), Error> {
    let mut r = send_retrying(
        |refresh| {
            authorized(agent().put(url), refresh)
                .header("content-type", CONTENT_TYPE)
                .send_empty()
        },
        &|| false,
    )
    .map_err(|e| http_error("create", url, e))?;
    if let Some(e) = refusal("create", url, &mut r) {
        return Err(e);
    }
    let status = r.status().as_u16();
    let body = body_text(&mut r);
    if (200..300).contains(&status) {
        Ok(())
    } else {
        Err(Error::Status {
            op: "create",
            url: url.to_owned(),
            status,
            body,
        })
    }
}

/// A request that got no answer.
fn http_error(op: &'static str, url: &str, e: ureq::Error) -> Error {
    Error::Http {
        op,
        url: url.to_owned(),
        source: Box::new(e),
    }
}

/// The most of a response body an error message or a log line shows.
const BODY_SHOWN: usize = 256;

/// The start of a response's body as text, for an error message or a log line: at most
/// `BODY_SHOWN` bytes, followed by `...` when there is more (a gateway's error page, say), and
/// what could be read when the body breaks off.
fn body_text(r: &mut ureq::http::Response<ureq::Body>) -> String {
    let mut bytes = Vec::new();
    let limit = u64::try_from(BODY_SHOWN.saturating_add(1)).unwrap_or(u64::MAX);
    if let Err(_broken) = r.body_mut().as_reader().take(limit).read_to_end(&mut bytes) {
        // Keep what arrived before the break.
    }
    let more = bytes.len() > BODY_SHOWN;
    bytes.truncate(BODY_SHOWN);
    let mut text = String::from_utf8_lossy(&bytes).into_owned();
    if more {
        text.push_str("...");
    }
    text
}

/// One read from `offset` of stream incarnation `incarnation` (a 412 when it is not, see
/// `recreated`): the bytes and the server's offset after them (empty at the tail). Reads
/// the leader's applied state: a follower may lag behind an acknowledged append (a claim, a
/// commit). A 200 without a usable `Stream-Next-Offset` is an error: offsets are never computed.
pub(crate) fn read_from(
    url: &str,
    incarnation: &str,
    offset: &str,
) -> Result<(Vec<u8>, String), Error> {
    let request = format!("{url}?offset={offset}&consistency=leader");
    let mut r = send_retrying(
        |refresh| {
            authorized(agent().get(request.as_str()), refresh)
                .header("stream-incarnation", incarnation)
                .call()
        },
        &|| false,
    )
    .map_err(|e| http_error("read", &request, e))?;
    if let Some(e) = refusal("read", &request, &mut r) {
        return Err(e);
    }
    let status = r.status().as_u16();
    if status == 412 {
        return Err(recreated(url, incarnation));
    }
    let next = header_offset(&r, "stream-next-offset");
    if status == 204 {
        return Ok((Vec::new(), next.unwrap_or_else(|| offset.to_owned())));
    }
    let body = r
        .body_mut()
        .with_config()
        .limit(1 << 30)
        .read_to_vec()
        .map_err(|e| http_error("read the body of", &request, e))?;
    if status == 410 {
        return Err(Error::Gone(Gone::BelowRetention {
            url: url.to_owned(),
            offset: offset.to_owned(),
        }));
    }
    if status == 416 {
        // The server answers 416, not 412, for a stream recreated shorter than `offset` (RFC 9110
        // §13.2.1): only a HEAD tells the two apart, and a recreate rebuilds.
        if head(url, &|| false).is_ok_and(|h| h.incarnation.as_deref() != Some(incarnation)) {
            return Err(recreated(url, incarnation));
        }
        return Err(Error::LostData {
            url: url.to_owned(),
            offset: offset.to_owned(),
        });
    }
    if status != 200 {
        return Err(Error::Status {
            op: "read",
            url: request,
            status,
            body: String::from_utf8_lossy(&body).into_owned(),
        });
    }
    let Some(next) = next else {
        return Err(Error::NoNextOffset {
            op: "read",
            url: request,
        });
    };
    Ok((body, next))
}

/// A read loop's step: `next`, answered by a read at `at` that returned bytes, must sort past `at`.
/// Checked in the loops only, where both are server offsets (or `START`): an older sidecar's
/// unpadded offset, read once to probe for a 416, compares meaninglessly.
pub(crate) fn advanced(url: &str, at: &str, len: usize, next: &str) -> Result<(), Error> {
    if next <= at {
        return Err(Error::OffsetNotAdvanced {
            url: url.to_owned(),
            at: at.to_owned(),
            len,
            next: next.to_owned(),
        });
    }
    Ok(())
}

pub(crate) struct Head {
    /// `START` when absent.
    pub(crate) retained: String,
    /// `None` when absent or `-1` (no snapshot).
    pub(crate) snapshot: Option<String>,
    /// `Stream-Incarnation`: opaque, changes when the stream is deleted and recreated; compared
    /// for equality only. `None` when absent (or unusable in a sidecar or `Producer-Id`): attach
    /// refuses.
    pub(crate) incarnation: Option<String>,
}

/// `stopped` ends the retries of a 429/503 early (see `send_retrying`).
pub(crate) fn head(url: &str, stopped: &dyn Fn() -> bool) -> Result<Head, Error> {
    let mut r = send_retrying(
        |refresh| authorized(agent().head(url), refresh).call(),
        stopped,
    )
    .map_err(|e| http_error("head", url, e))?;
    if let Some(e) = refusal("head", url, &mut r) {
        return Err(e);
    }
    let status = r.status().as_u16();
    if status != 200 {
        return Err(Error::Status {
            op: "head",
            url: url.to_owned(),
            status,
            body: String::new(),
        });
    }
    Ok(Head {
        retained: header_offset(&r, "stream-retained-offset").unwrap_or_else(|| START.into()),
        snapshot: header_offset(&r, "stream-snapshot-offset").filter(|s| s != START),
        incarnation: r
            .headers()
            .get("stream-incarnation")
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|v| !v.is_empty() && !v.contains(char::is_whitespace))
            .map(str::to_owned),
    })
}

/// The snapshot at `offset`; `None` when it does not exist (superseded, or not yet visible here).
/// `stopped` ends the retries of a 429/503 early (see `send_retrying`).
pub(crate) fn get_snapshot(
    url: &str,
    incarnation: &str,
    offset: &str,
    stopped: &dyn Fn() -> bool,
) -> Result<Option<Vec<u8>>, Error> {
    let request = format!("{url}/snapshot/{offset}");
    let mut r = send_retrying(
        |refresh| {
            authorized(bulk_agent().get(request.as_str()), refresh)
                .header("stream-incarnation", incarnation)
                .call()
        },
        stopped,
    )
    .map_err(|e| http_error("get snapshot", &request, e))?;
    if let Some(e) = refusal("get snapshot", &request, &mut r) {
        return Err(e);
    }
    let status = r.status().as_u16();
    if status == 412 {
        return Err(recreated(url, incarnation));
    }
    // A body cut short: the snapshot was superseded and its cold object deleted (after its grace)
    // while it was streaming, or the connection dropped. Either way, start again from `HEAD`.
    let Ok(body) = r.body_mut().with_config().limit(2 << 30).read_to_vec() else {
        return Ok(None);
    };
    match status {
        200 => Ok(Some(body)),
        404 | 410 => Ok(None),
        _ => Err(Error::Status {
            op: "get snapshot",
            url: request,
            status,
            body: String::from_utf8_lossy(&body).into_owned(),
        }),
    }
}

/// `PUT` to stream incarnation `incarnation` (a 412 when it is not) with retries while the outcome
/// is unknown (transport errors, 5xx): both publishing a snapshot and advancing retention are
/// idempotent, so a 402 fails the request alone ([`Error::Refused`]), whatever an earlier attempt
/// did. No other answer refuses it, whatever headers it carries: no answer here decides a producer
/// sequence, so none needs `applied_nothing`, and a marked 413 for a snapshot body over a gateway's
/// limit stays a snapshot failure, which the snapshot thread counts. Returns the status and
/// response. Gives up once `stopped` (a re-attach is waiting for the snapshot thread).
pub(crate) fn put_idempotent(
    url: &str,
    incarnation: &str,
    body: &[u8],
    stopped: &dyn Fn() -> bool,
) -> Result<(u16, ureq::http::Response<ureq::Body>), Error> {
    let deadline = retry_deadline();
    let mut backoff = Duration::from_millis(50);
    // The last answer refused the token: the next attempt reads it again.
    let mut token_refused = false;
    loop {
        let unknown = match authorized(bulk_agent().put(url), std::mem::take(&mut token_refused))
            .header("content-type", CONTENT_TYPE)
            .header("stream-incarnation", incarnation)
            .send(body)
        {
            Ok(mut r) => match (refused(&r), r.status().as_u16()) {
                (Some(Refused::Token), status) => {
                    token_refused = true;
                    Attempt::Status {
                        status,
                        body: body_text(&mut r),
                    }
                }
                (Some(Refused::Denied), status) => return Err(unauthorized("put", url, status)),
                (Some(Refused::PaymentRequired), _) => {
                    return Err(refused_request("put", url, WriteRefusal::PaymentRequired {
                        body: body_text(&mut r),
                    }));
                }
                (_, status) if status < 500 => return Ok((status, r)),
                (_, status) => Attempt::Status {
                    status,
                    body: body_text(&mut r),
                },
            },
            Err(e) => Attempt::Transport(Box::new(e)),
        };
        if Instant::now()
            .checked_add(backoff)
            .is_none_or(|end| end > deadline)
            || stopped()
        {
            return Err(if token_refused {
                unauthorized("put", url, 401)
            } else {
                Error::PutUnknown {
                    url: url.to_owned(),
                    last: unknown,
                }
            });
        }
        std::thread::sleep(backoff);
        backoff = backoff.saturating_mul(2).min(Duration::from_secs(1));
    }
}

/// The error for a 412: `url` is no longer stream incarnation `incarnation`.
pub(crate) fn recreated(url: &str, incarnation: &str) -> Error {
    Error::Recreated {
        url: url.to_owned(),
        incarnation: incarnation.to_owned(),
    }
}

/// A commit's `Stream-Seq`: the owner's (epoch, producer sequence), each zero-padded to 20 digits,
/// so it sorts like the pair. Owners of one incarnation claim ever higher epochs and number commits
/// upwards within one, so every commit's is above every earlier commit's: the server's check
/// (refused unless above the stream's last `Stream-Seq`) then fences a writer outside this protocol
/// that appended with a higher one since this owner's last commit (one with a lower or equal
/// `Stream-Seq` is itself refused). Writers without `Stream-Seq` go unnoticed: offsets are opaque,
/// so the VFS does not check where its frame landed.
pub(crate) fn stream_seq((epoch, seq): (u64, u64)) -> String {
    format!("{epoch:020}{seq:020}")
}

/// A stand-in for the stream in tests: canned answers, in order.
#[cfg(test)]
pub(crate) mod mock {
    use std::io::Read as _;
    use std::io::Write as _;
    use std::net::TcpListener;

    /// Ends an append's retry budget with the answer that carries it: a Retry-After too large to
    /// wait for (or even add to now).
    pub(crate) const LAST: &str = "retry-after: 18446744073709551615";

    /// Answers one connection per answer, in order, and returns each request's head. An empty
    /// answer closes the connection without one: the attempt's outcome is unknown. The request's
    /// body is read whole before the answer: a connection closed with unread bytes is reset, which
    /// can drop the answer before the client reads it.
    pub(crate) fn serve(answers: Vec<String>) -> (String, std::thread::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/b/s", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            answers
                .into_iter()
                .map(|answer| {
                    let (mut conn, _) = listener.accept().unwrap();
                    let mut request = Vec::new();
                    let mut byte = [0u8; 1];
                    while !request.ends_with(b"\r\n\r\n") {
                        conn.read_exact(&mut byte).unwrap();
                        request.push(byte[0]);
                    }
                    let head = String::from_utf8_lossy(&request).into_owned();
                    let len =
                        request_header(&head, "content-length").map_or(0, |n| n.parse().unwrap());
                    conn.read_exact(&mut vec![0u8; len]).unwrap();
                    conn.write_all(answer.as_bytes()).unwrap();
                    head
                })
                .collect()
        });
        (url, server)
    }

    /// An answer without a body: `head` is the status and the headers.
    pub(crate) fn answer(head: &str) -> String {
        format!("HTTP/1.1 {head}\r\nconnection: close\r\ncontent-length: 0\r\n\r\n")
    }

    pub(crate) fn answer_with_body(head: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {head}\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        )
    }

    /// A gateway's vouch for the marker, as a header line of an answer: alone, it proves nothing.
    pub(crate) const VOUCH: &str = "x-ursula-not-applied-checked: 1";

    /// The header line that marks an answer as applying nothing, with `token`, as a node or a
    /// gateway before 0.7.3 sends it: alone, it proves nothing.
    pub(crate) fn marker(token: &str) -> String {
        format!("x-ursula-not-applied: {token}")
    }

    /// The header lines of an answer marked as applying nothing, with `token`, and vouched for by a
    /// gateway from 0.7.3 on: the marker counts.
    pub(crate) fn vouched(token: &str) -> String {
        format!("{}\r\n{VOUCH}", marker(token))
    }

    /// The value of header `name` in a request head `serve` returned.
    pub(crate) fn request_header<'a>(request: &'a str, name: &str) -> Option<&'a str> {
        request.split("\r\n").find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case(name).then(|| value.trim())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read as _;
    use std::io::Write as _;
    use std::net::TcpListener;

    use super::Append;
    use super::Proof;
    use super::append;
    use super::applied_nothing;
    use super::create_stream;
    use super::get_snapshot;
    use super::head;
    use super::in_clear;
    use super::mock::LAST;
    use super::mock::VOUCH;
    use super::mock::answer;
    use super::mock::answer_with_body;
    use super::mock::marker;
    use super::mock::serve;
    use super::mock::vouched;
    use super::put_idempotent;
    use super::read_from;
    use crate::auth::set_token;
    use crate::error::Attempt;
    use crate::error::Error;
    use crate::error::WriteRefusal;

    // A leader read the server could not confirm with a quorum in time answers 503 (Retry-After),
    // and a gateway may rate-limit with 429: catch-up reads and HEAD retry both, as appends do,
    // so attach, the claim check and reclaim never fail on them.
    #[test]
    fn reads_retry_transient_unavailability() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/b/s", listener.local_addr().unwrap());
        let answer = |head: &str, body: &str| {
            format!(
                "HTTP/1.1 {head}\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            )
        };
        let unavailable = answer(
            "503 Service Unavailable\r\nretry-after: 0",
            "leader unknown",
        );
        let answers = [
            unavailable.clone(),
            answer("429 Too Many Requests", ""),
            answer("200 OK\r\nstream-next-offset: 7", "abc"),
            unavailable.clone(),
            answer("200 OK\r\nstream-retained-offset: 2", ""),
            // A Retry-After too large to wait for (or even add to now) ends the retries at once.
            answer(
                "503 Service Unavailable\r\nretry-after: 18446744073709551615",
                "leader unknown",
            ),
            // So does the stop flag (a re-attach waiting for the snapshot thread).
            unavailable,
        ];
        let server = std::thread::spawn(move || {
            answers
                .into_iter()
                .map(|answer| {
                    let (mut conn, _) = listener.accept().unwrap();
                    let mut request = Vec::new();
                    let mut byte = [0u8; 1];
                    while !request.ends_with(b"\r\n\r\n") {
                        conn.read_exact(&mut byte).unwrap();
                        request.push(byte[0]);
                    }
                    conn.write_all(answer.as_bytes()).unwrap();
                    let request = String::from_utf8_lossy(&request);
                    request.split(" HTTP/").next().unwrap().to_owned()
                })
                .collect::<Vec<_>>()
        });
        let (bytes, next) = read_from(&url, "1", "4").unwrap();
        assert_eq!((&bytes[..], next.as_str()), (&b"abc"[..], "7"));
        assert_eq!(head(&url, &|| false).unwrap().retained, "2");
        let refused = get_snapshot(&url, "1", "9", &|| false).unwrap_err();
        assert!(
            matches!(refused, Error::Status { status: 503, .. }),
            "{refused}"
        );
        let refused = head(&url, &|| true).err().unwrap();
        assert!(
            matches!(refused, Error::Status { status: 503, .. }),
            "{refused}"
        );
        let read = "GET /b/s?offset=4&consistency=leader";
        assert_eq!(server.join().unwrap(), [
            read,
            read,
            read,
            "HEAD /b/s",
            "HEAD /b/s",
            "GET /b/s/snapshot/9",
            "HEAD /b/s"
        ]);
    }

    // Every request carries the token. A 401 is an authorizer refusing it: the request is sent
    // again (with the token read again) within the retry budget, a create too. A 403 is a fence
    // only with the epoch that owns the stream (the server's); without it, an authorizer's final
    // denial, never taken for a fence. Marked `x-ursula-not-applied` with a gateway's vouch, it
    // refuses an append none of whose attempts can have reached the stream (nothing was appended).
    // Unmarked (a filtering proxy may have answered it after the stream applied the append), or
    // after an attempt whose outcome is unknown, it fails the append.
    #[test]
    fn auth_refusals_are_retried_or_final_never_fences() {
        set_token(Some("t1"));
        let denied =
            |headers: &str| answer_with_body(&format!("403 Forbidden{headers}"), "read only");
        let marked = format!("\r\n{}", vouched("Forbidden"));
        let (url, server) = serve(vec![
            answer("401 Unauthorized\r\nwww-authenticate: Bearer error=\"invalid_token\""),
            answer("200 OK\r\nstream-next-offset: 9"),
            answer("401 Unauthorized"),
            answer("200 OK\r\nstream-incarnation: i1"),
            answer("403 Forbidden\r\nproducer-epoch: 4"),
            denied(&marked),
            denied(""),
            answer("503 Service Unavailable\r\nretry-after: 0"),
            denied(&marked),
            answer("401 Unauthorized"),
            answer("201 Created"),
        ]);
        let acked = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(acked, Append::Acked { attempts: 2, .. }));
        assert_eq!(
            head(&url, &|| false).unwrap().incarnation.as_deref(),
            Some("i1")
        );
        let fenced = append(&url, "i1", "p", b"x", 1, 2);
        assert!(matches!(fenced, Append::Fenced { current: Some(4) }));
        let denied = append(&url, "i1", "p", b"x", 1, 2);
        assert!(
            matches!(&denied, Append::Refused(WriteRefusal::Forbidden { body }) if body == "read only")
        );
        let unmarked = append(&url, "i1", "p", b"x", 1, 2);
        assert!(matches!(
            unmarked,
            Append::Failed(Error::Unauthorized { status: 403, .. })
        ));
        let after_unknown = append(&url, "i1", "p", b"x", 1, 2);
        assert!(matches!(
            after_unknown,
            Append::Failed(Error::Unauthorized { status: 403, .. })
        ));
        create_stream(&url).unwrap();
        set_token(None);
        let requests = server.join().unwrap();
        assert_eq!(requests.len(), 11);
        for request in &requests {
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("authorization: bearer t1\r\n"),
                "{request}"
            );
        }
    }

    // `applied_nothing` alone decides whether an answer proves that the stream applied nothing: a
    // 401, 402 or 429 by its status, and a refusal (a 4xx or 5xx) by the marker with a gateway's
    // vouch, which then names it. A redirect or a success proves nothing whatever headers it
    // carries, and neither does the marker or the vouch alone.
    #[test]
    fn only_a_refusal_proves_that_nothing_was_applied() {
        let response = |status: u16, headers: &[(&str, &str)]| {
            let mut r = ureq::http::Response::builder().status(status);
            for &(name, value) in headers {
                r = r.header(name, value);
            }
            r.body(()).unwrap()
        };
        let marker = ("x-ursula-not-applied", "RaftLogPressure");
        let vouch = ("x-ursula-not-applied-checked", "1");
        let marked = Some(Proof::Marked("RaftLogPressure".to_owned()));
        for status in [503, 413, 403, 404, 502, 504, 401, 402, 429] {
            let proof = applied_nothing(&response(status, &[marker, vouch]));
            assert_eq!(proof, marked, "{status}");
        }
        for status in [307, 308, 301, 303, 200, 201, 204] {
            let proof = applied_nothing(&response(status, &[marker, vouch]));
            assert_eq!(proof, None, "{status}");
        }
        for status in [401, 402, 429] {
            for headers in [&[][..], &[marker], &[vouch]] {
                let proof = applied_nothing(&response(status, headers));
                assert_eq!(proof, Some(Proof::Status), "{status} {headers:?}");
            }
        }
        for status in [503, 413, 403, 404, 400, 500] {
            for headers in [&[][..], &[marker], &[vouch]] {
                let proof = applied_nothing(&response(status, headers));
                assert_eq!(proof, None, "{status} {headers:?}");
            }
        }
        // An empty marker still marks, and a long one is cut like a body.
        let empty = applied_nothing(&response(503, &[("x-ursula-not-applied", ""), vouch]));
        assert_eq!(empty, Some(Proof::Marked(String::new())));
        let long = "x".repeat(300);
        let cut = applied_nothing(&response(503, &[("x-ursula-not-applied", &long), vouch]));
        assert_eq!(cut, Some(Proof::Marked("x".repeat(256))));
    }

    // A token crosses the network in clear only over plain HTTP to a host other than this one.
    #[test]
    fn tokens_go_in_clear_over_http_to_other_hosts() {
        let clear = |url: &str| in_clear(&url.parse().unwrap());
        assert!(clear("http://ursula.example.com/b/s"));
        assert!(clear("http://10.0.0.7:4437/b/s"));
        assert!(!clear("https://ursula.example.com/b/s"));
        assert!(!clear("http://127.0.0.1:4437/b/s"));
        assert!(!clear("http://localhost:4437/b/s"));
        assert!(!clear("http://[::1]:4437/b/s"));
    }

    // An error message or a log line shows the start of a response body only: a gateway's error
    // page is not repeated whole on every retried attempt.
    #[test]
    fn response_bodies_are_cut_for_messages() {
        let long = "x".repeat(4_000);
        let (url, server) = serve(vec![format!(
            "HTTP/1.1 400 Bad Request\r\nconnection: close\r\ncontent-length: {}\r\n\r\n{long}",
            long.len()
        )]);
        let rejected = append(&url, "i1", "p", b"x", 1, 1);
        let Append::Failed(Error::AppendRejected { status, body }) = rejected else {
            panic!("not rejected");
        };
        assert_eq!(status, 400);
        assert_eq!(body, format!("{}...", "x".repeat(256)));
        server.join().unwrap();
    }

    // A 402 is a layer in front of the stream refusing the request: it fails that request alone,
    // with the layer's explanation, and is never retried, except by an append one of whose earlier
    // attempts may have reached the stream (no answer, or a 5xx): its outcome is still unknown, so
    // it is sent again until an answer settles it. Refusals in front of the stream (401, 402) and
    // rate limiting (429) leave the outcome known.
    #[test]
    fn payment_refusals_fail_the_request_alone() {
        let reason = "{\"detail\":\"quota exhausted\"}";
        let refused = || answer_with_body("402 Payment Required", reason);
        let payment = |r: &Error| match r.refusal() {
            Some(WriteRefusal::PaymentRequired { body }) => body == reason,
            _ => false,
        };
        let (url, server) = serve(vec![
            refused(),
            answer("429 Too Many Requests"),
            refused(),
            answer("401 Unauthorized"),
            refused(),
            // No answer at all: the attempt may have reached the stream.
            String::new(),
            refused(),
            answer("503 Service Unavailable"),
            refused(),
            answer("200 OK\r\nstream-next-offset: 9"),
            refused(),
            refused(),
            refused(),
        ]);
        let first = append(&url, "i1", "p", b"x", 1, 1);
        assert!(
            matches!(&first, Append::Refused(WriteRefusal::PaymentRequired { body }) if body == reason)
        );
        let after_429 = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            after_429,
            Append::Refused(WriteRefusal::PaymentRequired { .. })
        ));
        let after_401 = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            after_401,
            Append::Refused(WriteRefusal::PaymentRequired { .. })
        ));
        let settled = append(&url, "i1", "p", b"x", 1, 1);
        assert!(
            matches!(settled, Append::Acked { next: Some(ref n), attempts: 5 } if n == "9"),
            "a 402 after an unknown outcome is retried"
        );
        let create = create_stream(&url).unwrap_err();
        assert!(matches!(create, Error::Refused { op: "create", .. }) && payment(&create));
        // A HEAD answer has no body: no explanation.
        let head = head(&url, &|| false).err().unwrap();
        assert!(matches!(head, Error::Refused {
            op: "head",
            refusal: WriteRefusal::PaymentRequired { .. },
            ..
        }));
        let put = put_idempotent(&url, "i1", b"", &|| false).err().unwrap();
        assert!(matches!(put, Error::Refused { op: "put", .. }) && payment(&put));
        let requests = server.join().unwrap();
        let methods: Vec<&str> = requests
            .iter()
            .map(|r| r.split(' ').next().unwrap())
            .collect();
        assert_eq!(methods, [
            "POST", "POST", "POST", "POST", "POST", "POST", "POST", "POST", "POST", "POST", "PUT",
            "HEAD", "PUT"
        ]);
    }

    // An append whose outcome stays unknown to the end (a 402 after an attempt that may have
    // reached the stream) fails as unknown, never as refused: the caller must not take the
    // producer sequence for free.
    #[test]
    fn a_payment_refusal_after_an_unknown_outcome_stays_unknown() {
        let (url, server) = serve(vec![
            answer("503 Service Unavailable"),
            answer_with_body(
                &format!("402 Payment Required\r\n{LAST}"),
                "quota exhausted",
            ),
        ]);
        let unknown = append(&url, "i1", "p", b"x", 1, 1);
        let Append::Failed(Error::AppendUnknown { attempts, last }) = unknown else {
            panic!("not unknown");
        };
        assert_eq!(attempts, 2);
        assert!(
            matches!(last, Attempt::Status { status: 402, ref body } if body == "quota exhausted")
        );
        assert_eq!(server.join().unwrap().len(), 2);
    }

    // Answers that applied nothing (401, 429, or a refusal marked `x-ursula-not-applied` with a
    // gateway's vouch) leave the outcome known: when the budget runs out on them, the append is
    // refused, with the last answer's reason (the marker's token when there is one, cut like a
    // body; an empty marker still marks), and the producer sequence is free. A marked final answer
    // refuses it at once.
    #[test]
    fn refusals_that_applied_nothing_refuse_the_append_when_the_budget_runs_out() {
        let marked = |status: &str, token: &str, last: &str| {
            answer(&format!("{status}\r\n{}\r\n{last}", vouched(token)))
        };
        let (url, server) = serve(vec![
            // The token refused to the end, read again before every attempt.
            answer("401 Unauthorized\r\nretry-after: 0"),
            answer_with_body(&format!("401 Unauthorized\r\n{LAST}"), "expired"),
            // The server under pressure, to the end.
            marked(
                "503 Service Unavailable",
                "RaftLogPressure",
                "retry-after: 0",
            ),
            marked("503 Service Unavailable", "RaftLogPressure", LAST),
            // Kinds mixed: the last answer names the refusal.
            marked(
                "503 Service Unavailable",
                "AuthorizationUnavailable",
                "retry-after: 0",
            ),
            answer("429 Too Many Requests\r\nretry-after: 0"),
            answer_with_body(&format!("429 Too Many Requests\r\n{LAST}"), "slow down"),
            // A marked 429 is named by its marker.
            marked("429 Too Many Requests", "TenantRateLimited", LAST),
            // A marked final answer: refused at once. Unmarked: rejected (poisons).
            answer(&format!(
                "413 Payload Too Large\r\n{}",
                vouched("RequestBodyTooLarge")
            )),
            answer("413 Payload Too Large"),
            // A marker without a token, and one with a token too long to show whole.
            marked("503 Service Unavailable", "", LAST),
            marked("503 Service Unavailable", &"x".repeat(300), LAST),
        ]);
        let unauthorized = append(&url, "i1", "p", b"x", 1, 1);
        assert!(
            matches!(&unauthorized, Append::Refused(WriteRefusal::Unauthorized { body }) if body == "expired")
        );
        let pressure = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            &pressure,
            Append::Refused(WriteRefusal::NotApplied { status: 503, reason }) if reason == "RaftLogPressure"
        ));
        let limited = append(&url, "i1", "p", b"x", 1, 1);
        assert!(
            matches!(&limited, Append::Refused(WriteRefusal::RateLimited { body }) if body == "slow down")
        );
        let marked_429 = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            &marked_429,
            Append::Refused(WriteRefusal::NotApplied { status: 429, reason }) if reason == "TenantRateLimited"
        ));
        let too_large = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            &too_large,
            Append::Refused(WriteRefusal::NotApplied { status: 413, reason }) if reason == "RequestBodyTooLarge"
        ));
        let rejected = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            rejected,
            Append::Failed(Error::AppendRejected { status: 413, .. })
        ));
        let Append::Refused(empty) = append(&url, "i1", "p", b"x", 1, 1) else {
            panic!("an empty marker does not mark");
        };
        assert_eq!(empty.to_string(), "503 (not applied)");
        assert!(
            matches!(empty, WriteRefusal::NotApplied { status: 503, .. })
                && empty.reason().is_none()
        );
        let long = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            &long,
            Append::Refused(WriteRefusal::NotApplied { status: 503, reason }) if *reason == "x".repeat(256)
        ));
        assert_eq!(server.join().unwrap().len(), 12);
    }

    // One attempt whose outcome is unknown (no answer, an unmarked 5xx) and the append may be in
    // the stream: refusals before or after it prove nothing about it, vouched for or not, so a
    // budget that runs out fails the append as unknown (or as unauthorized, after a refused token),
    // and a marked final answer is a rejection.
    #[test]
    fn one_unknown_outcome_among_refusals_keeps_the_append_unknown() {
        let pressure = |last: &str| {
            answer(&format!(
                "503 Service Unavailable\r\n{}\r\n{last}",
                vouched("RaftLogPressure")
            ))
        };
        let (url, server) = serve(vec![
            // No answer, then refusals to the end.
            String::new(),
            pressure(LAST),
            // Refusals, then an unmarked 503 to the end.
            pressure("retry-after: 0"),
            answer(&format!("503 Service Unavailable\r\n{LAST}")),
            // An unmarked 503, then a refused token to the end.
            answer("503 Service Unavailable\r\nretry-after: 0"),
            answer(&format!("401 Unauthorized\r\n{LAST}")),
            // An unmarked 503, then a marked final answer.
            answer("503 Service Unavailable\r\nretry-after: 0"),
            answer(&format!(
                "413 Payload Too Large\r\n{}",
                vouched("RequestBodyTooLarge")
            )),
        ]);
        let unknown = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            unknown,
            Append::Failed(Error::AppendUnknown {
                attempts: 2,
                last: Attempt::Status { status: 503, .. }
            })
        ));
        let unknown = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            unknown,
            Append::Failed(Error::AppendUnknown { attempts: 2, .. })
        ));
        let unauthorized = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            unauthorized,
            Append::Failed(Error::Unauthorized { status: 401, .. })
        ));
        let rejected = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            rejected,
            Append::Failed(Error::AppendRejected { status: 413, .. })
        ));
        assert_eq!(server.join().unwrap().len(), 8);
    }

    // A marker counts only with a gateway's vouch (`applied_nothing`), and the vouch alone proves
    // nothing: an answer with one of the two headers is read by its status alone. A 503 is then an
    // unknown outcome, so a budget that runs out on it fails the append as unknown (a poison), and
    // a 403 or a 413 fails it. A 401, 402 or 429 proves by its status that it applied nothing, with
    // either header or none (a 429 marked alone is rate limiting). One attempt marked alone among
    // vouched refusals leaves the append unknown.
    #[test]
    fn a_marker_proves_nothing_without_the_vouch() {
        let unavailable = |headers: &str, last: &str| {
            answer(&format!("503 Service Unavailable\r\n{headers}\r\n{last}"))
        };
        let (url, server) = serve(vec![
            // The marker alone, and the vouch alone, to the end.
            unavailable(&marker("RaftLogPressure"), LAST),
            unavailable(VOUCH, LAST),
            // A denial and a request too large, each with one of the two headers.
            answer(&format!("403 Forbidden\r\n{}", marker("Forbidden"))),
            answer(&format!("403 Forbidden\r\n{VOUCH}")),
            answer(&format!(
                "413 Payload Too Large\r\n{}",
                marker("RequestBodyTooLarge")
            )),
            answer(&format!("413 Payload Too Large\r\n{VOUCH}")),
            // A refused token, a 402 and a 429, marked alone: their status proves it.
            answer(&format!(
                "401 Unauthorized\r\n{}\r\n{LAST}",
                marker("TokenExpired")
            )),
            answer_with_body(
                &format!("402 Payment Required\r\n{}", marker("QuotaExhausted")),
                "quota",
            ),
            answer_with_body(
                &format!(
                    "429 Too Many Requests\r\n{}\r\n{LAST}",
                    marker("TenantRateLimited")
                ),
                "slow down",
            ),
            // Vouched refusals around one marked alone.
            unavailable(&vouched("RaftLogPressure"), "retry-after: 0"),
            unavailable(&marker("RaftLogPressure"), "retry-after: 0"),
            unavailable(&vouched("RaftLogPressure"), LAST),
        ]);
        for _ in 0..2 {
            let unknown = append(&url, "i1", "p", b"x", 1, 1);
            assert!(matches!(
                unknown,
                Append::Failed(Error::AppendUnknown {
                    attempts: 1,
                    last: Attempt::Status { status: 503, .. }
                })
            ));
        }
        for _ in 0..2 {
            let denied = append(&url, "i1", "p", b"x", 1, 1);
            assert!(matches!(
                denied,
                Append::Failed(Error::Unauthorized { status: 403, .. })
            ));
        }
        for _ in 0..2 {
            let rejected = append(&url, "i1", "p", b"x", 1, 1);
            assert!(matches!(
                rejected,
                Append::Failed(Error::AppendRejected { status: 413, .. })
            ));
        }
        let token = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            token,
            Append::Refused(WriteRefusal::Unauthorized { .. })
        ));
        let payment = append(&url, "i1", "p", b"x", 1, 1);
        assert!(
            matches!(&payment, Append::Refused(WriteRefusal::PaymentRequired { body }) if body == "quota")
        );
        let limited = append(&url, "i1", "p", b"x", 1, 1);
        assert!(
            matches!(&limited, Append::Refused(WriteRefusal::RateLimited { body }) if body == "slow down")
        );
        let mixed = append(&url, "i1", "p", b"x", 1, 1);
        assert!(matches!(
            mixed,
            Append::Failed(Error::AppendUnknown { attempts: 3, .. })
        ));
        assert_eq!(server.join().unwrap().len(), 12);
    }
}
