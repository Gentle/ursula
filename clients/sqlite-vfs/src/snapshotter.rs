//! The per-database snapshot thread: snapshots, their read-back, and retention.

use std::sync::Condvar;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use crate::client::body_text;
use crate::client::get_snapshot;
use crate::client::head;
use crate::client::header_offset;
use crate::client::put_idempotent;
use crate::client::recreated;
use crate::config::snapshot_retry;
use crate::db::Db;
use crate::db::lock;
use crate::error::Error;
use crate::host::Checkpoint;
use crate::host::Private;
use crate::log;
use crate::log::Level;
use crate::snapshot;
use crate::stats::SnapshotStat;

/// Wakes an attached database's snapshot thread.
#[derive(Default)]
pub(crate) struct Snapper {
    /// (requested, stopped)
    pub(crate) state: Mutex<(bool, bool)>,
    pub(crate) cv: Condvar,
    /// With the database's mutex: a commit waits for the snapshot window to close, the snapshot
    /// for an acknowledged commit to be published.
    pub(crate) window_cv: Condvar,
}

/// Closes the snapshot window when dropped.
struct Window<'a>(&'a Mutex<Db>);

impl Drop for Window<'_> {
    fn drop(&mut self) {
        let mut d = lock(self.0);
        d.window = false;
        d.snapper.window_cv.notify_all();
    }
}

impl Snapper {
    pub(crate) fn request(&self) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).0 = true;
        self.cv.notify_one();
    }

    pub(crate) fn stopped(&self) -> bool {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).1
    }

    pub(crate) fn stop(&self) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).1 = true;
        self.cv.notify_one();
    }

    /// Waits for a request (true) or the stop (false).
    pub(crate) fn wait(&self) -> bool {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if s.1 {
                return false;
            }
            if std::mem::take(&mut s.0) {
                return true;
            }
            s = self.cv.wait(s).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Waits for a request or `deadline` (true), or the stop (false), whichever comes first.
    pub(crate) fn wait_until(&self, deadline: Instant) -> bool {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if s.1 {
                return false;
            }
            if std::mem::take(&mut s.0) {
                return true;
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return true;
            }
            s = self
                .cv
                .wait_timeout(s, left)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }

    /// Sleeps for `d` unless stopped first (false).
    pub(crate) fn pause(&self, d: Duration) -> bool {
        let s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let (s, _) = self
            .cv
            .wait_timeout_while(s, d, |s| !s.1)
            .unwrap_or_else(|e| e.into_inner());
        !s.1
    }
}

/// What one snapshot attempt did (`snapshot_once`).
#[derive(Clone, Copy, Debug)]
enum Outcome {
    /// Published at the window's offset and read back.
    Published,
    /// A snapshot at or past the window's offset exists already (another owner's, or this file's
    /// before a re-attach).
    Covered,
    /// Nothing to take: not due, the database poisoned, or the thread stopped.
    Skipped,
    /// The stream is another incarnation now: the owner is fenced, and nothing of it landed there.
    Fenced,
    /// A write-back to the local files may have been lost: the owner is poisoned, and nothing was
    /// published.
    Damaged,
    /// Not possible right now (a reader pins WAL frames the checkpoint needs, or another
    /// checkpoint kept it busy): try again shortly.
    Busy,
    /// A layer in front of the stream refused the publish or the retention move with 402 Payment
    /// Required (`Db::writes_refused`): no snapshot is taken until a commit is acknowledged again
    /// (`Db::snapshot_wanted`), which requests one if it is still due (nothing adds to the log
    /// meanwhile).
    Refused,
}

/// The longest wait between two failed snapshot attempts (`SnapshotFailing`).
const RETRY_MAX: Duration = Duration::from_secs(3600);

/// Snapshot attempts that failed in a row, since the last one that published a snapshot at its
/// offset or found one there (`Db::snapshot_failing`).
///
/// Each failure doubles the wait before the next attempt, from `snapshot_retry()` (100 ms) up to
/// `RETRY_MAX` (1 h). An attempt reads, compresses and sends the whole database, and a body over a
/// size limit of the server or of a layer in front of it (answered 413, or reset or timed out by
/// a proxy) fails the same way each time until the database shrinks. So a database that shrank by
/// more than a quarter since the last failure (a `VACUUM`) tries again without waiting.
pub(crate) struct SnapshotFailing {
    /// How many failed in a row.
    pub(crate) count: u32,
    /// Why the last one failed.
    pub(crate) error: String,
    /// The database's size, in pages, at the last failure.
    pub(crate) pages: u32,
    /// The wait after the last failure.
    pub(crate) wait: Duration,
    /// The next attempt starts no sooner, unless the database shrank (`holds_back`).
    pub(crate) until: Instant,
}

impl SnapshotFailing {
    /// Whether an attempt at `now` waits, with the database at `pages`: until `until`, unless it is
    /// below three quarters of its size at the last failure.
    pub(crate) fn holds_back(&self, pages: u32, now: Instant) -> bool {
        let shrank = u64::from(pages).saturating_mul(4) < u64::from(self.pages).saturating_mul(3);
        now < self.until && !shrank
    }
}

/// The snapshot health `ursula_status` reports, and the wait before the next attempt, after an
/// attempt that ended at `now`: a snapshot at its offset (one published, or one already there)
/// ends a run of failures, an error extends it (`SnapshotFailing`), and any other outcome leaves
/// it as it was (nothing was published, nor did anything fail).
fn record_attempt(d: &mut Db, attempt: &Result<Outcome, Error>, now: Instant) {
    match attempt {
        Ok(Outcome::Published | Outcome::Covered) => d.snapshot_failing = None,
        Ok(
            Outcome::Skipped
            | Outcome::Fenced
            | Outcome::Damaged
            | Outcome::Busy
            | Outcome::Refused,
        ) => {}
        Err(e) => {
            let (count, wait) = match &d.snapshot_failing {
                Some(last) => (
                    last.count.saturating_add(1),
                    last.wait.saturating_mul(2).min(RETRY_MAX),
                ),
                None => (1, snapshot_retry().min(RETRY_MAX)),
            };
            d.snapshot_failing = Some(SnapshotFailing {
                count,
                error: e.to_string(),
                pages: d.pages,
                wait,
                until: now.checked_add(wait).unwrap_or(now),
            });
        }
    }
}

pub(crate) fn snapshot_loop(db: &Mutex<Db>, snapper: &Snapper) {
    // A pinned or busy checkpoint clears up quickly.
    let mut busy = Duration::from_millis(10);
    while snapper.wait() {
        // After a failure the next attempt waits (`SnapshotFailing`): a request meanwhile (a commit
        // while a snapshot is due) only checks whether the database shrank.
        loop {
            let until = {
                let d = lock(db);
                match &d.snapshot_failing {
                    Some(failing) if failing.holds_back(d.pages, Instant::now()) => failing.until,
                    _ => break,
                }
            };
            if !snapper.wait_until(until) {
                return;
            }
        }
        let attempt = snapshot_once(db, snapper);
        let (path, url, failing, due) = {
            let mut d = lock(db);
            record_attempt(&mut d, &attempt, Instant::now());
            let failing = d.snapshot_failing.as_ref().map(|f| (f.count, f.wait));
            (d.path.clone(), d.url.clone(), failing, d.snapshot_due())
        };
        match attempt {
            Ok(Outcome::Busy) => {
                if !snapper.pause(busy) {
                    return;
                }
                busy = busy.saturating_mul(2).min(Duration::from_secs(1));
                if lock(db).snapshot_due() {
                    snapper.request();
                }
            }
            Ok(
                Outcome::Published
                | Outcome::Covered
                | Outcome::Skipped
                | Outcome::Fenced
                | Outcome::Damaged
                | Outcome::Refused,
            ) => busy = Duration::from_millis(10),
            Err(e) => {
                let (failures, wait) = failing.unwrap_or_default();
                log::emit(Level::Warn, "snapshot_failed", &[
                    ("file", &path),
                    ("stream", &url),
                    ("failures", &failures),
                    ("retry_ms", &wait.as_millis()),
                    ("reason", &e),
                ]);
                // Taken once the wait ends (above), if still due then.
                if due {
                    snapper.request();
                }
            }
        }
    }
}

/// One snapshot attempt (see the crate docs), and what it did.
///
/// The image is exactly the stream's state at `offset`. The window opens once every acknowledged
/// commit is published locally (`!committed`); until then the next commit waits for it at its
/// commit point (`window_wanted`, bounded by `WINDOW_WAIT`), so a writer committing back to back
/// cannot starve it: a due snapshot opens its window within one transaction. It keeps any further commit from being
/// acknowledged (it waits at its commit point) until the read transaction has started, so the
/// checkpoint moves every WAL frame up to `offset` into the db file and the read transaction sees
/// the db file alone at `offset`. While the read transaction lasts no checkpoint can write newer
/// frames into the db file (a reader at mark 0 blocks backfill, one at a later mark caps it) and no
/// closing connection can checkpoint (that needs an EXCLUSIVE lock), so the pages copied are those
/// of `offset`. Commits wait only for the checkpoint and the start of the read transaction.
fn snapshot_once(db: &Mutex<Db>, snapper: &Snapper) -> Result<Outcome, Error> {
    let stopped = || snapper.stopped();
    let started = Instant::now();
    let path = lock(db).path.clone();
    let conn = Private::open(&path, false)?;
    // Closing as the last connection keeps the WAL (as the attached connections do).
    conn.persist_wal()?;
    // Backfill outside the window, so the checkpoint inside it (which commits wait for) only
    // covers the frames committed in between.
    conn.checkpoint(c"PRAGMA wal_checkpoint(PASSIVE)")?;
    let (url, incarnation, offset, epoch, pages, log) = {
        let mut d = lock(db);
        loop {
            // Not while writes are refused before they reach the stream (a refused commit, or
            // `Outcome::Refused`): see `Db::snapshot_wanted`.
            if !d.snapshot_wanted() || stopped() {
                d.window_wanted = false;
                snapper.window_cv.notify_all();
                return Ok(Outcome::Skipped);
            }
            // Not while a commit is acknowledged but unpublished, nor while another connection
            // (typically the writer's auto-checkpoint, right after its commit) holds the
            // checkpoint lock, which would make the checkpoint below busy.
            if !d.committed && d.checkpoint_started.is_none() {
                break;
            }
            // The next commit waits for the window (see `window_wanted`).
            d.window_wanted = true;
            d = snapper
                .window_cv
                .wait_timeout(d, Duration::from_millis(100))
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        d.window_wanted = false;
        d.window = true;
        (
            d.url.clone(),
            d.incarnation.clone(),
            d.offset.clone(),
            d.epoch,
            d.pages,
            d.log,
        )
    };
    let window = Window(db);
    // A checkpoint started by another connection after the window opened makes this one busy
    // (it gets no busy handler): retry briefly, it only covers frames up to `offset` too.
    let mut tries: u32 = 0;
    loop {
        match conn.checkpoint(c"PRAGMA wal_checkpoint(PASSIVE)")? {
            Checkpoint::Done => break,
            Checkpoint::Busy if tries < 50 => {
                tries = tries.saturating_add(1);
                std::thread::sleep(Duration::from_millis(2));
            }
            _ => return Ok(Outcome::Busy),
        }
    }
    conn.query(c"BEGIN")?;
    conn.query(c"SELECT count(*) FROM sqlite_schema")?;
    drop(window);
    let copy_started = Instant::now();
    let image = conn.read_pages(pages)?;
    conn.query(c"COMMIT")?; // ends the read transaction
    let copy = copy_started.elapsed();
    // The checkpoints above read WAL pages, and `read_pages` db pages, outside the VFS's check of
    // pages read back (`overlay_read`): a page whose write-back failed reads back older bytes, and
    // once published, the next snapshot's retention move makes the damage permanent. The commit
    // path never fsyncs the WAL, and the db file only before the WAL restarts, so such an error is
    // usually unreported yet: fsyncs after the reads report it (on Linux), unless the owner found
    // it first (a lost page read back, a failed fsync of the db file) and is poisoned (`damaged`,
    // kept even when a fence poisoned it before).
    let synced = conn.sync_files();
    drop(conn);
    match synced {
        Err(e) if e.is_local_damage() => {
            lock(db).set_poisoned(e, Some(&format!("snapshot at {offset} not published")));
            return Ok(Outcome::Damaged);
        }
        Err(e) => return Err(e),
        Ok(()) => {}
    }
    if lock(db).damaged() {
        return Ok(Outcome::Damaged);
    }
    let body = snapshot::encode(&offset, epoch, &image).map_err(|source| Error::Snapshot {
        offset: offset.clone(),
        source,
    })?;
    lock(db).snapshot_bytes = Some(body.len());
    // This state is a prefix of the incarnation it was attached to, not of a stream recreated at
    // the same path since: the publish and the retention move carry the incarnation, so the
    // server refuses them there (412), and this owner's commits stop now rather than at its next
    // append (which the recreated stream refuses the same way).
    let fence = |what: String, e: Error| {
        // Not `poison()`: the overlay belongs to a write transaction that may be in flight; it
        // clears it itself when it ends, and `x_write` refuses its commit (`poisoned`).
        lock(db).set_poisoned(e, Some(&what));
    };
    // A 402 in front of the stream: nothing was published or moved (`Outcome::Refused`).
    let refused = |e: Error, during: &str| match e {
        Error::Refused { refusal, .. } => {
            lock(db).writes_refused(refusal, during);
            Ok(Outcome::Refused)
        }
        e => Err(e),
    };
    let published = match put_idempotent(
        &format!("{url}/snapshot/{offset}"),
        &incarnation,
        &body,
        &stopped,
    ) {
        Ok(answer) => answer,
        Err(e) => return refused(e, "snapshot publish"),
    };
    match published {
        (200..=299, _) => {}
        (412, _) => {
            fence(
                format!("snapshot at {offset} not published"),
                recreated(&url, &incarnation),
            );
            return Ok(Outcome::Fenced);
        }
        (409 | 410, _) => {
            // A snapshot at or past `offset` exists (another owner's, or this file's before a
            // re-attach): it covers the log counted up to the window, unless the stream is
            // another incarnation by now.
            let head = head(&url, &stopped)?;
            if head.incarnation.as_deref() != Some(incarnation.as_str()) {
                fence(
                    format!("snapshot at {offset} not published"),
                    recreated(&url, &incarnation),
                );
                return Ok(Outcome::Fenced);
            }
            let newer = head.snapshot;
            let mut d = lock(db);
            if let Some(newer) = newer.filter(|n| *n > d.snapshot) {
                d.snapshot = newer;
                d.snapshot_published_at = Some(Instant::now());
            }
            d.log = d.log.saturating_sub(log);
            return Ok(Outcome::Covered);
        }
        (413, mut r) => {
            return Err(Error::SnapshotTooLarge {
                url: format!("{url}/snapshot/{offset}"),
                bytes: body.len(),
                body: body_text(&mut r),
            });
        }
        (status, mut r) => {
            return Err(Error::Status {
                op: "publish snapshot",
                url: format!("{url}/snapshot/{offset}"),
                status,
                body: r.body_mut().read_to_string().unwrap_or_default(),
            });
        }
    }
    // Nothing relies on the snapshot before it reads back intact (a follower may not show it yet).
    let mut verified = false;
    for i in 1..=20 {
        if stopped() {
            return Ok(Outcome::Skipped);
        }
        match get_snapshot(&url, &incarnation, &offset, &stopped) {
            Ok(read) if read.as_deref() == Some(&body[..]) => {
                verified = true;
                break;
            }
            Ok(_) => {}
            Err(e) if e.is_recreated() => {
                fence(format!("snapshot at {offset} not read back"), e);
                return Ok(Outcome::Fenced);
            }
            Err(e) => return Err(e),
        }
        std::thread::sleep(Duration::from_millis(25_u64.saturating_mul(i)));
    }
    if !verified {
        return Err(Error::SnapshotNotReadBack { offset });
    }
    let (previous, retained) = {
        let mut d = lock(db);
        let previous = d.snapshot.clone();
        if offset > d.snapshot {
            d.snapshot.clone_from(&offset);
        }
        d.log = d.log.saturating_sub(log);
        d.snapshot_published_at = Some(Instant::now());
        d.stats.snapshot(SnapshotStat {
            offset: offset.clone(),
            bytes: body.len(),
            raw: image.len(),
            copy,
            total: started.elapsed(),
        });
        (previous, d.retained.clone())
    };
    // Retention trails one snapshot behind: a host that read the previous snapshot (or whose file
    // is past it) still finds the frames after it, and the newer snapshot has read back.
    if previous > retained && previous < offset {
        let moved = match put_idempotent(
            &format!("{url}/retention/{previous}"),
            &incarnation,
            &[],
            &stopped,
        ) {
            Ok(answer) => answer,
            Err(e) => return refused(e, "retention move"),
        };
        match moved {
            (412, _) => {
                fence(
                    format!("retention not moved to {previous}"),
                    recreated(&url, &incarnation),
                );
                return Ok(Outcome::Fenced);
            }
            (200..=299, r) => {
                let effective = header_offset(&r, "stream-retained-offset").unwrap_or(previous);
                let mut d = lock(db);
                if effective > d.retained {
                    d.retained = effective;
                }
            }
            // Already past it (another owner, or this file before a re-attach).
            (409 | 410, _) => {}
            (status, mut r) => {
                return Err(Error::Status {
                    op: "move retention",
                    url: format!("{url}/retention/{previous}"),
                    status,
                    body: r.body_mut().read_to_string().unwrap_or_default(),
                });
            }
        }
    }
    Ok(Outcome::Published)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::time::Instant;

    use super::Outcome;
    use super::RETRY_MAX;
    use super::Snapper;
    use super::record_attempt;
    use crate::db::Db;
    use crate::error::Error;

    fn failed() -> Result<Outcome, Error> {
        Err(Error::SnapshotNotReadBack { offset: "7".into() })
    }

    // Only a snapshot at the offset (published, or one already there) ends a run of failures: an
    // attempt that published nothing (skipped, fenced, damaged local files, busy, refused) leaves
    // the run, its last error and its wait as they were.
    #[test]
    fn only_a_snapshot_at_the_offset_ends_a_run_of_failures() {
        let mut d = Db::for_tests();
        let now = Instant::now();
        record_attempt(&mut d, &failed(), now);
        record_attempt(&mut d, &failed(), now);
        let until = d.snapshot_failing.as_ref().map(|f| (f.count, f.until));
        assert_eq!(until.map(|(count, _)| count), Some(2));
        for outcome in [
            Outcome::Skipped,
            Outcome::Fenced,
            Outcome::Damaged,
            Outcome::Busy,
            Outcome::Refused,
        ] {
            record_attempt(&mut d, &Ok(outcome), now);
            let failing = d.snapshot_failing.as_ref().map(|f| (f.count, f.until));
            assert_eq!(failing, until, "{outcome:?}");
        }
        record_attempt(&mut d, &Ok(Outcome::Covered), now);
        assert!(d.snapshot_failing.is_none());
        record_attempt(&mut d, &failed(), now);
        record_attempt(&mut d, &Ok(Outcome::Published), now);
        assert!(d.snapshot_failing.is_none());
    }

    // Each failure doubles the wait before the next attempt, from 100 ms up to an hour, and a run
    // that ends starts the next one from 100 ms again. The run keeps the last error and the
    // database's size at the last failure.
    #[test]
    fn failures_double_the_wait_up_to_an_hour() {
        let mut d = Db::for_tests();
        d.pages = 400;
        let now = Instant::now();
        let mut waits = Vec::new();
        for _ in 0..18 {
            record_attempt(&mut d, &failed(), now);
            let failing = d.snapshot_failing.as_ref().unwrap();
            assert_eq!(failing.until, now + failing.wait);
            waits.push(failing.wait);
        }
        assert_eq!(waits[0], Duration::from_millis(100));
        assert_eq!(waits[1], Duration::from_millis(200));
        assert_eq!(waits[15], Duration::from_millis(100 << 15));
        assert_eq!(waits[16], RETRY_MAX);
        assert_eq!(waits[17], RETRY_MAX);
        let failing = d.snapshot_failing.as_ref().unwrap();
        assert_eq!((failing.count, failing.pages), (18, 400));
        assert_eq!(failing.error, "the snapshot at 7 does not read back");
        record_attempt(&mut d, &Ok(Outcome::Published), now);
        record_attempt(&mut d, &failed(), now);
        assert_eq!(
            d.snapshot_failing.as_ref().map(|f| (f.count, f.wait)),
            Some((1, Duration::from_millis(100)))
        );
    }

    // The next attempt waits for the end of the wait, unless the database shrank below three
    // quarters of its size at the last failure (a VACUUM): a body over a size limit would fail the
    // same way before. A database that grew, or shrank less, waits, and so does one that was empty.
    #[test]
    fn a_failure_holds_the_next_attempt_back_until_its_wait_ends_or_the_database_shrinks() {
        let mut d = Db::for_tests();
        d.pages = 400;
        let now = Instant::now();
        record_attempt(&mut d, &failed(), now);
        let failing = d.snapshot_failing.as_ref().unwrap();
        for pages in [400, 401, 4000, 300] {
            assert!(failing.holds_back(pages, now), "{pages}");
        }
        assert!(!failing.holds_back(299, now));
        assert!(!failing.holds_back(0, now));
        assert!(!failing.holds_back(400, failing.until));
        d.pages = 0;
        record_attempt(&mut d, &failed(), now);
        assert!(d.snapshot_failing.as_ref().unwrap().holds_back(0, now));
    }

    // The wait between failed attempts ends at a request (to check whether the database shrank),
    // at its deadline, or at the stop.
    #[test]
    fn a_wait_until_ends_at_a_request_its_deadline_or_the_stop() {
        let snapper = Snapper::default();
        let later = Instant::now() + Duration::from_secs(3600);
        snapper.request();
        assert!(snapper.wait_until(later));
        let started = Instant::now();
        assert!(snapper.wait_until(started + Duration::from_millis(20)));
        assert!(started.elapsed() >= Duration::from_millis(20));
        snapper.stop();
        assert!(!snapper.wait_until(later));
    }
}
