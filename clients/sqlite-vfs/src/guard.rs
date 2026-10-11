//! Panics stop at the C entry points. Unwinding out of an `extern "C"` function aborts the process,
//! and here the process is the host's (a Node or Python program that loaded the extension), so
//! every entry point runs its body through [`contain`]: a panic becomes the entry point's error
//! code, logged, instead of taking the host down.

use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

use crate::config::panic_in;
use crate::log;
use crate::log::Level;

// `contain` relies on unwinding: built to abort, a panic would still kill the host process.
#[cfg(panic = "abort")]
compile_error!(
    "sqlite-ursula-vfs must be built with panic = \"unwind\": its C entry points contain panics, \
     and an aborting panic would kill the host process"
);

/// Runs `body`, the body of C entry point `entry` (or of a step inside one, such as
/// `attach_files`). A panic in it is logged (`event=panic`) and answered with `on_panic()` instead
/// of unwinding into SQLite. `on_panic` itself must not panic: nothing would stop that one.
/// `URSULA_VFS_PANIC_IN=<entry>` makes `entry` panic on purpose, for tests, before `body` runs.
pub(crate) fn contain<R>(
    entry: &'static str,
    on_panic: impl FnOnce() -> R,
    body: impl FnOnce() -> R,
) -> R {
    // Unwind safety is each caller's to ensure: a panic may leave state that `body` changed half
    // changed, and the caller must not use that state as if it were whole. A file method poisons
    // its database (`vfs::poison_after_panic`), so no commit builds on it; `attach` takes a
    // panic as a failed attach, which drops the binding and the attach in progress; a SQL
    // function and the extension's init change nothing a later call reads.
    let run = AssertUnwindSafe(|| {
        if panic_in().is_some_and(|name| name == entry) {
            panic_on_purpose(entry);
        }
        body()
    });
    match catch_unwind(run) {
        Ok(answer) => answer,
        Err(payload) => {
            log::emit(Level::Error, "panic", &[
                ("entry", &entry),
                ("message", &message(payload.as_ref())),
            ]);
            on_panic()
        }
    }
}

/// Panics, for `URSULA_VFS_PANIC_IN`.
#[expect(
    clippy::panic,
    reason = "URSULA_VFS_PANIC_IN makes an entry point panic on purpose, so tests can check that \
              `contain` stops the panic"
)]
fn panic_on_purpose(entry: &'static str) -> ! {
    std::panic::panic_any(format!("URSULA_VFS_PANIC_IN={entry}"))
}

/// What a panic said: its message when it carried text.
fn message(payload: &(dyn Any + Send)) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        (*text).to_owned()
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text.clone()
    } else {
        "a panic without a message".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::contain;
    use super::message;

    // A body that returns is answered as it returns; one that panics is answered with the fallback,
    // and the panic stops here instead of unwinding further.
    #[test]
    fn a_panic_becomes_the_fallback_answer() {
        assert_eq!(contain("ok", || -1, || 7), 7);
        let answer = contain("boom", || -1, || -> i32 { std::panic::panic_any("boom") });
        assert_eq!(answer, -1);
        let mut fell_back = false;
        contain(
            "unit",
            || fell_back = true,
            || std::panic::panic_any(String::from("x")),
        );
        assert!(fell_back);
    }

    #[test]
    fn a_panic_message_is_its_text() {
        assert_eq!(message(&"static text"), "static text");
        assert_eq!(message(&String::from("owned text")), "owned text");
        assert_eq!(message(&42_u8), "a panic without a message");
    }
}
