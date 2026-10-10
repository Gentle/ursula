//! Settings and test hooks read from the environment (`URSULA_VFS_*`), once per process.

use std::sync::OnceLock;
use std::time::Duration;

use crate::log;
use crate::log::Level;

pub(crate) fn abort_after_ack() -> Option<u64> {
    static V: OnceLock<Option<u64>> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("URSULA_VFS_ABORT_AFTER_ACK")
            .ok()
            .and_then(|v| v.parse().ok())
    })
}

pub(crate) fn retry_budget() -> Duration {
    static V: OnceLock<Duration> = OnceLock::new();
    *V.get_or_init(|| {
        Duration::from_millis(
            std::env::var("URSULA_VFS_RETRY_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(30_000),
        )
    })
}

/// `URSULA_VFS_IDLE_MS`: how long a connection is kept after its last request, for the next one
/// to reuse, in milliseconds (90 s by default, see `client::agent`). 0 never reuses one. A value
/// that is not a number of milliseconds keeps the default, logged once (`setting_adjusted`).
pub(crate) fn idle_age() -> Duration {
    const DEFAULT_MS: u64 = 90_000;
    static V: OnceLock<Duration> = OnceLock::new();
    *V.get_or_init(|| {
        let Ok(value) = std::env::var("URSULA_VFS_IDLE_MS") else {
            return Duration::from_millis(DEFAULT_MS);
        };
        let ms = value.trim().parse().unwrap_or_else(|_not_a_number| {
            log::emit(Level::Warn, "setting_adjusted", &[
                ("setting", &"URSULA_VFS_IDLE_MS"),
                ("value", &value),
                ("used", &DEFAULT_MS),
            ]);
            DEFAULT_MS
        });
        Duration::from_millis(ms)
    })
}

/// `URSULA_VFS_PANIC_IN`: what panics on purpose at every call, for tests of `guard::contain`: a
/// file method by its name (`x_write`, `x_close`, ...), a SQL function (`ursula_status`, ...),
/// `sqlite3_extension_init`, or the attach step `attach_files`. Unset outside tests. Unit tests set
/// it per thread instead (`set_panic_in_for_tests`).
pub(crate) fn panic_in() -> Option<&'static str> {
    #[cfg(test)]
    if let Some(entry) = PANIC_IN_FOR_TESTS.get() {
        return Some(entry);
    }
    static V: OnceLock<Option<String>> = OnceLock::new();
    V.get_or_init(|| std::env::var("URSULA_VFS_PANIC_IN").ok())
        .as_deref()
}

#[cfg(test)]
thread_local! {
    static PANIC_IN_FOR_TESTS: std::cell::Cell<Option<&'static str>> =
        const { std::cell::Cell::new(None) };
}

/// `panic_in()` on this thread, for unit tests: the environment is read once per process.
#[cfg(test)]
pub(crate) fn set_panic_in_for_tests(entry: Option<&'static str>) {
    PANIC_IN_FOR_TESTS.set(entry);
}

/// The largest commit frame the extension sends by default: the server refuses a request body over
/// 32 MiB, and so does a gateway by default (`--max-request-body-bytes`).
pub(crate) const DEFAULT_MAX_FRAME_BYTES: usize = 32 << 20;

/// The largest commit frame the extension sends (`client::append`): `URSULA_VFS_MAX_FRAME_BYTES`, or
/// [`DEFAULT_MAX_FRAME_BYTES`]. Lower it to a smaller limit in front of the server.
pub(crate) fn max_frame_bytes() -> usize {
    static V: OnceLock<usize> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("URSULA_VFS_MAX_FRAME_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MAX_FRAME_BYTES)
    })
}

pub(crate) fn snapshot_min_bytes() -> u64 {
    static V: OnceLock<u64> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("URSULA_VFS_SNAPSHOT_MIN_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8 << 20)
    })
}

pub(crate) fn fail_post_ack() -> Option<u64> {
    static V: OnceLock<Option<u64>> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("URSULA_VFS_FAIL_POST_ACK")
            .ok()
            .and_then(|v| v.parse().ok())
    })
}

pub(crate) fn abort_in_replay() -> Option<u64> {
    static V: OnceLock<Option<u64>> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("URSULA_VFS_ABORT_IN_REPLAY")
            .ok()
            .and_then(|v| v.parse().ok())
    })
}

pub(crate) fn first_claim_epoch() -> Option<u64> {
    static V: OnceLock<Option<u64>> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("URSULA_VFS_FIRST_CLAIM_EPOCH")
            .ok()
            .and_then(|v| v.parse().ok())
    })
}

/// `URSULA_VFS_CA_FILE`: a PEM bundle of the certificate authorities trusted for `https://` stream
/// URLs, instead of the bundled Mozilla roots.
pub(crate) fn ca_file() -> Option<&'static str> {
    static V: OnceLock<Option<String>> = OnceLock::new();
    V.get_or_init(|| non_empty("URSULA_VFS_CA_FILE")).as_deref()
}

/// `URSULA_VFS_TOKEN_FILE`: a file holding the bearer token every request carries (see `auth`).
pub(crate) fn token_file() -> Option<&'static str> {
    static V: OnceLock<Option<String>> = OnceLock::new();
    V.get_or_init(|| non_empty("URSULA_VFS_TOKEN_FILE"))
        .as_deref()
}

fn non_empty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}
