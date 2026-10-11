//! The crate's error type: why an operation failed, or why an attached database is poisoned.
//!
//! Callers act on four classes, through methods rather than message text: [`Error::is_gone`]
//! (the data lies below the stream's retention: attach retries from a newer snapshot),
//! [`Error::is_recreated`] (the stream is another incarnation: attach rebuilds),
//! [`Error::is_fenced`] (another owner or writer holds the stream: `ursula_status` reports
//! `fenced`) and [`Error::refusal`] (the request was refused before it reached the stream, which
//! applied nothing of it, see [`WriteRefusal`]: the operation fails alone, and `ursula_attach`
//! reports it as SQLITE_AUTH for a 402, SQLITE_IOERR_AUTH otherwise). Every other variant fails
//! the operation, or poisons the database until it is attached again.

use std::ffi::CStr;
use std::ffi::c_int;
use std::sync::Arc;

use crate::frame::FrameError;
use crate::frame::PAGE;
use crate::snapshot::SnapshotError;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    // Local files and the host process.
    #[error("{op} {path}: {source}")]
    Io {
        op: &'static str,
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{path:?}: {source}")]
    NulInPath {
        path: String,
        #[source]
        source: std::ffi::NulError,
    },
    #[error(
        "{path} is open by another process (pid {}); close it before attaching",
        .pid.map_or_else(|| "unknown".to_owned(), |pid| pid.to_string())
    )]
    OpenElsewhere {
        path: String,
        pid: Option<libc::pid_t>,
    },
    #[error("{path} is attached by another process ({lock_path}: {source})")]
    AttachedElsewhere {
        path: String,
        lock_path: String,
        #[source]
        source: std::fs::TryLockError,
    },
    #[error("{path} has open connections; close them before attaching")]
    OpenConnections { path: String },
    #[error("{path} is being attached by another thread")]
    AttachInProgress { path: String },
    #[error(
        "{path} is not attached{}",
        .last_failure.as_ref().map_or_else(String::new, |e| format!(": its last attach failed ({e})"))
    )]
    NotAttached {
        path: String,
        last_failure: Option<Arc<Error>>,
    },
    #[error("{path} has content but no readable sidecar ({source}); refusing to attach")]
    NoSidecar {
        path: String,
        #[source]
        source: Box<Error>,
    },
    #[error("{path} is a cache of stream {cached}, not {wanted}; delete it to attach it there")]
    OtherStream {
        path: String,
        cached: String,
        wanted: String,
    },
    /// The files attach to no other URL (`OtherStream`) and a copy of them has no sidecar
    /// (`NoSidecar`), so the data is salvaged by reading a copy as a plain SQLite file.
    #[error(
        "{url} is missing (or hidden from this client's credentials) and {path} holds data: \
         refusing to create an empty stream over it, and keeping the files (to salvage the data, \
         copy {path} and {path}-wal to a new name and export the copy's rows into a newly \
         attached database; to start over, delete {path}, or attach a new file under another \
         URL)"
    )]
    StreamMissing { path: String, url: String },
    #[error(
        "{url} is missing (or hidden from this client's credentials), and a read-only attach never \
         creates a stream"
    )]
    NoStream { url: String },
    #[error("unknown attach mode {mode:?}: expected 'owner' or 'read_only'")]
    AttachMode { mode: String },
    #[error("spawn the snapshot thread: {0}")]
    SpawnSnapshotThread(#[source] std::io::Error),

    // The host's SQLite.
    #[error("xFullPathname({path}): {code}")]
    FullPathname { path: String, code: c_int },
    #[error("open {path}: {message}")]
    SqliteOpen { path: String, message: String },
    #[error("{sql:?}: {message}")]
    SqliteQuery { sql: &'static CStr, message: String },
    #[error("{sql:?}: no result row")]
    NoResultRow { sql: &'static CStr },
    #[error("persist WAL: {0}")]
    PersistWal(c_int),
    #[error("{file} handle of a private connection: {code}")]
    FileHandle { file: &'static str, code: c_int },
    #[error("read db file pages: {code} (file shorter than {pages} pages?)")]
    ReadPages { code: c_int, pages: u32 },
    #[error("the host's SQLite has no {0}")]
    MissingRoutine(&'static str),

    // The stream.
    #[error("{op} {url}: {source}")]
    Http {
        op: &'static str,
        url: String,
        #[source]
        source: Box<ureq::Error>,
    },
    #[error("{op} {url}: {status}{}", with_body(.body))]
    Status {
        op: &'static str,
        url: String,
        status: u16,
        body: String,
    },
    /// A redirect the extension does not retry (any 3xx but 307, a 308 included:
    /// `client::redirected`).
    /// It never follows one: `location` is where it pointed.
    #[error(
        "{op} {url}: {status} redirect{}: the extension never follows redirects (point the stream \
         URL at the server or gateway itself)",
        .location.as_ref().map_or_else(String::new, |l| format!(" to {l}"))
    )]
    Redirected {
        op: &'static str,
        url: String,
        status: u16,
        location: Option<String>,
    },
    #[error("append rejected: {status}{}", with_body(.body))]
    AppendRejected { status: u16, body: String },
    #[error("append: {last} (outcome unknown after {attempts} attempts)")]
    AppendUnknown { attempts: u32, last: Attempt },
    #[error("put {url}: {last}")]
    PutUnknown { url: String, last: Attempt },
    #[error(
        "{op} {url}: {status}: the endpoint refused this client's credentials (see \
         URSULA_VFS_TOKEN_FILE and ursula_set_token)"
    )]
    Unauthorized {
        op: &'static str,
        url: String,
        status: u16,
    },
    /// Refused before it reached the stream, which applied nothing of it (see [`WriteRefusal`]).
    #[error(
        "{op} {url}: {refusal}: a layer in front of the stream refused it, and nothing of it \
         reached the stream"
    )]
    Refused {
        op: &'static str,
        url: String,
        refusal: WriteRefusal,
    },
    #[error("{url} reports no Stream-Incarnation (an older server?); refusing to attach")]
    NoIncarnation { url: String },
    #[error("{op} {url}: no Stream-Next-Offset")]
    NoNextOffset { op: &'static str, url: String },
    #[error("read {url} at {at}: {len} bytes but next offset {next}")]
    OffsetNotAdvanced {
        url: String,
        at: String,
        len: usize,
        next: String,
    },
    #[error("stream {url} ends at {at} inside a frame or before {until:?}")]
    StreamEnded {
        url: String,
        at: String,
        until: Option<String>,
    },
    #[error("claim {url}: the stream ends at {at}, before the claim's end {next}")]
    ClaimCut {
        url: String,
        at: String,
        next: String,
    },
    #[error("claim {url}: {source}")]
    ClaimScan {
        url: String,
        #[source]
        source: ClaimScanError,
    },
    #[error("claim {url}: lost 16 claim races")]
    ClaimRaces { url: String },
    #[error("claim {url}: answered as a commit append (expired producer or Stream-Seq conflict)")]
    ClaimAnswer { url: String },
    #[error("{url}: a frame after {after}: {source}")]
    Frame {
        url: String,
        after: String,
        #[source]
        source: FrameError,
    },
    #[error("encode a frame: {0}")]
    EncodeFrame(#[source] FrameError),
    #[error("snapshot at {offset}: {source}")]
    Snapshot {
        offset: String,
        #[source]
        source: SnapshotError,
    },
    #[error("snapshot at {at} reflects offset {reflects}")]
    SnapshotOffset { at: String, reflects: String },
    #[error("the snapshot at {offset} does not read back")]
    SnapshotNotReadBack { offset: String },
    #[error(
        "read {url} at {offset}: beyond the end of the stream that acknowledged it (the server \
         lost acknowledged data?); the local files are kept (delete them to rebuild)"
    )]
    LostData { url: String, offset: String },
    #[error("gone: {0}")]
    Gone(Gone),
    #[error("fenced: {url} was deleted and recreated (no longer incarnation {incarnation}, 412)")]
    Recreated { url: String, incarnation: String },
    #[error("fenced: {0}")]
    Fenced(Fence),

    // The local files no longer follow the stream: the database is poisoned until re-attached.
    #[error("db file write outside a checkpoint (journal_mode must stay WAL)")]
    DbWriteOutsideCheckpoint,
    #[error("db file truncate outside a checkpoint (journal_mode must stay WAL)")]
    DbTruncateOutsideCheckpoint,
    #[error(
        "WAL write outside a write-locked transaction (locking_mode=EXCLUSIVE is not supported)"
    )]
    WalWriteOutsideTransaction,
    #[error("page size {0} (only {PAGE} is supported)")]
    PageSize(u32),
    #[error("a commit leaves WAL format (journal_mode must stay WAL)")]
    LeavesWal,
    #[error("fsync of the db file before {before} failed ({code})")]
    Fsync { before: &'static str, code: c_int },
    #[error("{what} failed ({code}) after the commit was acknowledged")]
    PostAck { what: &'static str, code: c_int },
    #[error("commit acknowledged but not published locally (mxFrame {mx_frame} < frame {frame})")]
    NotPublished { mx_frame: u32, frame: u32 },
    #[error("append after {offset} acknowledged with next offset {next}, not past it")]
    AckNotPast { offset: String, next: String },
    /// The commit after one refused as never applied (`Db::refuse`), sent at the sequence the
    /// refusal left free, was answered as a duplicate that none of its own attempts explains
    /// (`client::Append::UnexplainedDuplicate`): the refused commit was applied after all, so the
    /// stream holds it and the local files do not. `receipt` is its end in the stream.
    #[error(
        "a commit refused as never applied was applied after all: the next commit, at epoch \
         {epoch} seq {seq} after {offset}, was answered as its duplicate (receipt {receipt}), so a \
         layer in front of the stream refused a write that the stream applied; the stream holds the \
         refused commit and the local file does not: attach the database again (the attach \
         replays it)"
    )]
    RefusedCommitApplied {
        epoch: u64,
        seq: u64,
        offset: String,
        receipt: String,
    },
    #[error("producer expired again right after a re-claim")]
    ProducerExpiredAgain,
    #[error("local WAL write: {0}")]
    LocalWalWrite(c_int),
    #[error(
        "the WAL page at offset {offset} reads back other bytes than this process wrote there (a \
         write-back failed, or something else wrote the file)"
    )]
    LostWrite { offset: i64 },
    #[error("fsync of the {file} failed ({code}): a write-back to it may have been lost")]
    FileSync { file: &'static str, code: c_int },
}

/// The data asked for lies below the stream's retention (or a snapshot was superseded): a
/// re-attach installs the newer snapshot.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Gone {
    #[error("read {url} at {offset}: below the stream's retention")]
    BelowRetention { url: String, offset: String },
    #[error("snapshot {offset} superseded")]
    SnapshotSuperseded { offset: String },
    #[error("{offset} is below the retention {retained} and no newer snapshot is visible")]
    RetentionPassed { offset: String, retained: String },
}

/// Another owner, or a writer outside this protocol, holds the stream.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Fence {
    #[error("epoch {epoch} superseded by {} (403)", epoch_or_unknown(.current))]
    Superseded { epoch: u64, current: Option<u64> },
    #[error(
        "append at {offset} answered as a duplicate without a receipt: another writer (a foreign \
         one using this Producer-Id?) holds producer {producer} at epoch {epoch} past seq {seq}"
    )]
    DuplicateWithoutReceipt {
        offset: String,
        producer: String,
        epoch: u64,
        seq: u64,
    },
    #[error(
        "append after {offset} refused its Stream-Seq {stream_seq}: another writer appended with \
         a higher one ({body})"
    )]
    SeqConflict {
        offset: String,
        stream_seq: String,
        body: String,
    },
    #[error("producer expired and the stream moved past {offset}")]
    ProducerExpiredMoved { offset: String },
    #[error("another owner wrote or claimed while re-claiming")]
    Reclaim,
    #[error("another owner claimed epoch {epoch} during attach; attach again")]
    ClaimedDuringAttach { epoch: u64 },
}

/// The frames read back after a claim do not show where it landed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ClaimScanError {
    #[error(transparent)]
    Frame(#[from] FrameError),
    #[error("the answered offset is not a frame boundary")]
    NotAtBoundary,
}

/// The last answer to a request retried while its outcome was unknown.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Attempt {
    #[error("{status}{}", with_body(.body))]
    Status { status: u16, body: String },
    #[error("{0}")]
    Transport(#[source] Box<ureq::Error>),
}

/// Why a write was refused before it reached the stream, so that the stream applied nothing of it
/// (design doc §2): the write fails alone and never poisons a database (`Db::refuse`). A body is
/// the refusing layer's explanation: its response body (cut short: `client::body_text`), or the
/// token of a vouched marker when the body is empty (`client::Proof::refusal`).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum WriteRefusal {
    /// 402 Payment Required: a layer in front of the stream (an authorizer, a quota or billing
    /// service) refuses writes until something is paid or settled.
    #[error("402 Payment Required{}", with_body(.body))]
    PaymentRequired { body: String },
    /// 401 until the retry budget ran out: an authorizer refused the token, read again before
    /// every attempt.
    #[error(
        "401{}: the endpoint refused this client's credentials until the retry budget ran out \
         (see URSULA_VFS_TOKEN_FILE and ursula_set_token)",
        with_body(.body)
    )]
    Unauthorized { body: String },
    /// 403 without `Producer-Epoch`, marked `x-ursula-not-applied` with a gateway's vouch: an
    /// authorizer denied the write before passing it on (the server's fence names the epoch that
    /// owns the stream, and any other 403 may have replaced the stream's own answer:
    /// `client::append`).
    #[error("403{}: an authorizer denied it", with_body(.body))]
    Forbidden { body: String },
    /// 429 until the retry budget ran out, marked or not.
    #[error("429{}: rate limited until the retry budget ran out", with_body(.body))]
    RateLimited { body: String },
    /// A refusal marked `x-ursula-not-applied` (the server's ingress admission, a gateway or a
    /// proxy refusing it before passing it on) and vouched for by a gateway
    /// (`x-ursula-not-applied-checked`, see `client::applied_nothing`): a final one, or the last
    /// one when the retry budget ran out. A 401, 402, 403 or 429 keeps its own kind. `reason` is
    /// the marker's token (`RaftLogPressure`, say), empty when it has none.
    #[error("{status} (not applied{})", with_token(.reason))]
    NotApplied { status: u16, reason: String },
}

impl WriteRefusal {
    /// What refused the write, as `ursula_status` reports it (`refused_kind`).
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            WriteRefusal::PaymentRequired { .. } => "payment_required",
            WriteRefusal::Unauthorized { .. } => "unauthorized",
            WriteRefusal::Forbidden { .. } => "forbidden",
            WriteRefusal::RateLimited { .. } => "rate_limited",
            WriteRefusal::NotApplied { .. } => "not_applied",
        }
    }

    /// Why, as `ursula_status` reports it (`refused_reason`): the layer's explanation (its body, or
    /// the marker's token when the body is empty), and the marker's token for `NotApplied`,
    /// trimmed; `None` when it sent neither.
    pub(crate) fn reason(&self) -> Option<&str> {
        let reason = match self {
            WriteRefusal::PaymentRequired { body }
            | WriteRefusal::Unauthorized { body }
            | WriteRefusal::Forbidden { body }
            | WriteRefusal::RateLimited { body } => body,
            WriteRefusal::NotApplied { reason, .. } => reason,
        };
        Some(reason.trim()).filter(|reason| !reason.is_empty())
    }
}

impl Error {
    /// The data lies below the stream's retention, or a snapshot was superseded: retry from the
    /// stream's latest snapshot.
    pub(crate) fn is_gone(&self) -> bool {
        matches!(self, Error::Gone(_))
    }

    /// The stream at the path is no longer the incarnation the request was sent to: it was deleted
    /// and recreated, and nothing of the request reached it.
    pub(crate) fn is_recreated(&self) -> bool {
        matches!(self, Error::Recreated { .. })
    }

    /// Another owner, a writer outside this protocol, or another incarnation of the stream holds
    /// it: this owner cannot commit again without a new attach.
    pub(crate) fn is_fenced(&self) -> bool {
        matches!(self, Error::Fenced(_) | Error::Recreated { .. })
    }

    /// Why the request was refused before it reached the stream, which applied nothing of it: the
    /// operation fails alone, and never poisons a database (see `Db::refuse`). `None` for any
    /// other error.
    pub(crate) fn refusal(&self) -> Option<&WriteRefusal> {
        match self {
            Error::Refused { refusal, .. } => Some(refusal),
            _ => None,
        }
    }

    /// The local files may hold other pages than this process wrote (a write-back was lost, or
    /// something else wrote them): nothing read from them may reach the stream, not even in a
    /// snapshot.
    pub(crate) fn is_local_damage(&self) -> bool {
        matches!(
            self,
            Error::Fsync { .. } | Error::LostWrite { .. } | Error::FileSync { .. }
        )
    }
}

/// An epoch a server answer named, or `unknown`.
fn epoch_or_unknown(epoch: &Option<u64>) -> String {
    epoch.map_or_else(|| "unknown".to_owned(), |epoch| epoch.to_string())
}

/// A response body appended to a status, when there is one.
fn with_body(body: &str) -> String {
    let body = body.trim();
    if body.is_empty() {
        String::new()
    } else {
        format!(" {body}")
    }
}

/// `: token` for a marker's token, nothing for an empty one.
fn with_token(token: &str) -> String {
    let token = token.trim();
    if token.is_empty() {
        String::new()
    } else {
        format!(": {token}")
    }
}

#[cfg(test)]
mod tests {
    use super::Error;
    use super::Fence;
    use super::Gone;
    use super::WriteRefusal;

    // Callers act on the class, never on the text: attach retries `Gone`, rebuilds on a recreated
    // stream, and `ursula_status` reports `fenced` for the fence classes only. The texts below are
    // what operators and the e2e suites look for.
    #[test]
    fn classes_and_texts() {
        let recreated = Error::Recreated {
            url: "http://h/b/s".into(),
            incarnation: "i1".into(),
        };
        assert!(recreated.is_recreated() && recreated.is_fenced() && !recreated.is_gone());
        assert!(recreated.to_string().contains("deleted and recreated"));
        let superseded = Error::Fenced(Fence::Superseded {
            epoch: 2,
            current: Some(3),
        });
        assert!(superseded.is_fenced() && !superseded.is_recreated());
        assert!(superseded.to_string().ends_with("superseded by 3 (403)"));
        let unknown = Error::Fenced(Fence::Superseded {
            epoch: 2,
            current: None,
        });
        assert!(unknown.to_string().ends_with("superseded by unknown (403)"));
        let gone = Error::Gone(Gone::SnapshotSuperseded { offset: "7".into() });
        assert!(gone.is_gone() && !gone.is_fenced() && !gone.is_recreated());
        let local = Error::LocalWalWrite(10);
        assert!(!local.is_fenced() && !local.is_gone() && !local.is_recreated());
        // A lost write-back (found on a read, or reported by an fsync) keeps every page of the
        // local files out of a snapshot; a fence does not.
        let lost_page = Error::LostWrite { offset: 56 };
        let lost_sync = Error::FileSync {
            file: "WAL",
            code: 10,
        };
        assert!(lost_page.is_local_damage() && lost_sync.is_local_damage());
        assert!(!lost_page.is_fenced() && !superseded.is_local_damage());
        assert!(!local.is_local_damage());
        let missing = Error::StreamMissing {
            path: "/data/app.db".into(),
            url: "http://h/b/s".into(),
        }
        .to_string();
        assert!(missing.starts_with("http://h/b/s is missing") && missing.contains("holds data"));
        assert!(missing.contains("attach a new file under another URL"));
        let lost = Error::LostData {
            url: "http://h/b/s".into(),
            offset: "9".into(),
        };
        assert!(!lost.is_gone() && lost.to_string().contains("lost acknowledged data"));
        // A refusal in front of the stream is its own class: never a fence, never gone, never
        // damage. The 402's text is the one 0.7.1 introduced.
        let payment = WriteRefusal::PaymentRequired {
            body: "{\"detail\":\"quota exhausted\"}".into(),
        };
        let refused = Error::Refused {
            op: "claim",
            url: "http://h/b/s".into(),
            refusal: payment.clone(),
        };
        assert_eq!(refused.refusal(), Some(&payment));
        assert!(!refused.is_fenced() && !refused.is_gone() && !refused.is_local_damage());
        assert_eq!(
            refused.to_string(),
            "claim http://h/b/s: 402 Payment Required {\"detail\":\"quota exhausted\"}: a layer in front \
             of the stream refused it, and nothing of it reached the stream"
        );
        assert_eq!(superseded.refusal(), None);
        assert_eq!(local.refusal(), None);
        let unauthorized = Error::Unauthorized {
            op: "head",
            url: "http://h/b/s".into(),
            status: 401,
        };
        assert_eq!(unauthorized.refusal(), None);
        let pressure = Error::Refused {
            op: "claim",
            url: "http://h/b/s".into(),
            refusal: WriteRefusal::NotApplied {
                status: 503,
                reason: "RaftLogPressure".into(),
            },
        };
        assert_eq!(
            pressure.to_string(),
            "claim http://h/b/s: 503 (not applied: RaftLogPressure): a layer in front of the \
             stream refused it, and nothing of it reached the stream"
        );
        // A refused commit applied after all poisons: neither a fence nor damage nor a refusal.
        let applied = Error::RefusedCommitApplied {
            epoch: 2,
            seq: 5,
            offset: "9".into(),
            receipt: "12".into(),
        };
        assert!(!applied.is_fenced() && !applied.is_local_damage() && applied.refusal().is_none());
        assert_eq!(
            applied.to_string(),
            "a commit refused as never applied was applied after all: the next commit, at epoch 2 \
             seq 5 after 9, was answered as its duplicate (receipt 12), so a layer in front of the \
             stream refused a write that the stream applied; the stream holds the refused commit \
             and the local file does not: attach the database again (the attach replays it)"
        );
        // A redirect the extension does not retry fails the request: never a fence, never gone.
        let redirected = Error::Redirected {
            op: "head",
            url: "http://h/b/s".into(),
            status: 301,
            location: Some("https://h/b/s".into()),
        };
        assert!(!redirected.is_fenced() && !redirected.is_gone());
        assert!(redirected.refusal().is_none() && !redirected.is_recreated());
        assert_eq!(
            redirected.to_string(),
            "head http://h/b/s: 301 redirect to https://h/b/s: the extension never follows \
             redirects (point the stream URL at the server or gateway itself)"
        );
    }

    // `ursula_status` reports a refusal's kind (stable names) and its reason: the marker's token or
    // the layer's explanation, `None` when it sent none.
    #[test]
    fn refusal_kinds_and_reasons() {
        let body = |b: &str| b.to_owned();
        let cases = [
            (
                WriteRefusal::PaymentRequired {
                    body: body("quota"),
                },
                "payment_required",
                Some("quota"),
            ),
            (
                WriteRefusal::Unauthorized { body: body("") },
                "unauthorized",
                None,
            ),
            (
                WriteRefusal::Forbidden {
                    body: body(" denied \n"),
                },
                "forbidden",
                Some("denied"),
            ),
            (
                WriteRefusal::RateLimited { body: body("slow") },
                "rate_limited",
                Some("slow"),
            ),
            (
                WriteRefusal::NotApplied {
                    status: 503,
                    reason: body("WalDiskPressure"),
                },
                "not_applied",
                Some("WalDiskPressure"),
            ),
        ];
        for (refusal, kind, reason) in cases {
            assert_eq!((refusal.kind(), refusal.reason()), (kind, reason));
        }
    }
}
