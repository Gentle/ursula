//! The measurements `ursula_stats` drains: per commit, per checkpoint and per snapshot of an
//! attached database, at most [`KEPT`] of each kind between drains.

use std::collections::VecDeque;
use std::time::Duration;

/// The most measurements of each kind an attached database keeps between two drains: 8,192
/// commits take 512 KiB (64 bytes each), checkpoints 128 KiB. A power of two: the queues grow by
/// doubling, so their capacity stops at `KEPT` too.
pub(crate) const KEPT: usize = 8_192;

/// One acknowledged commit.
pub(crate) struct CommitStat {
    /// Frame bytes on the stream (compressed), record bytes, and page images in the record.
    pub(crate) bytes: usize,
    pub(crate) raw: usize,
    pub(crate) pages: usize,
    /// Append attempts: more than one after a retry.
    pub(crate) attempts: u32,
    /// The append requests alone, and the whole commit hook (frame build, append, local WAL write).
    pub(crate) append: Duration,
    pub(crate) vfs: Duration,
}

/// One snapshot, published and read back.
pub(crate) struct SnapshotStat {
    /// The stream offset it reflects.
    pub(crate) offset: String,
    /// Body bytes (compressed), and database bytes.
    pub(crate) bytes: usize,
    pub(crate) raw: usize,
    /// The page copy under the read transaction, and the whole snapshot.
    pub(crate) copy: Duration,
    pub(crate) total: Duration,
}

/// An attached database's measurements since the last drain, oldest first: the newest [`KEPT`]
/// commits, checkpoints and snapshots. Past that, each new one drops the oldest of its kind, so a
/// database nobody drains holds a bounded amount, and the next drain counts what was dropped
/// ([`Dropped`]).
#[derive(Default)]
pub(crate) struct Stats {
    commits: VecDeque<CommitStat>,
    /// How long each checkpoint held the checkpoint lock.
    checkpoints: VecDeque<Duration>,
    snapshots: VecDeque<SnapshotStat>,
    dropped: Dropped,
}

/// What [`Stats`] dropped since the last drain to keep at most [`KEPT`] of each kind. The dropped
/// commits' frame bytes and append retries are summed, so totals built from the drains (the
/// counters of the TypeScript package's `instrument`) count every commit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Dropped {
    pub(crate) commits: u64,
    /// The dropped commits' frame bytes, and their append attempts beyond the first.
    pub(crate) commit_bytes: u64,
    pub(crate) append_retries: u64,
    pub(crate) checkpoints: u64,
    pub(crate) snapshots: u64,
}

/// What a drain hands over: everything kept since the last one, oldest first, and what was dropped.
pub(crate) struct Drained {
    pub(crate) commits: VecDeque<CommitStat>,
    pub(crate) checkpoints: VecDeque<Duration>,
    pub(crate) snapshots: VecDeque<SnapshotStat>,
    pub(crate) dropped: Dropped,
}

impl Stats {
    pub(crate) fn commit(&mut self, commit: CommitStat) {
        if let Some(oldest) = keep(&mut self.commits, commit) {
            let d = &mut self.dropped;
            d.commits = d.commits.saturating_add(1);
            d.commit_bytes = d.commit_bytes.saturating_add(oldest.bytes as u64);
            d.append_retries = d
                .append_retries
                .saturating_add(u64::from(oldest.attempts.saturating_sub(1)));
        }
    }

    /// A checkpoint ended after holding the checkpoint lock for `held`.
    pub(crate) fn checkpoint(&mut self, held: Duration) {
        if keep(&mut self.checkpoints, held).is_some() {
            self.dropped.checkpoints = self.dropped.checkpoints.saturating_add(1);
        }
    }

    pub(crate) fn snapshot(&mut self, snapshot: SnapshotStat) {
        if keep(&mut self.snapshots, snapshot).is_some() {
            self.dropped.snapshots = self.dropped.snapshots.saturating_add(1);
        }
    }

    /// Hands everything over and starts again from nothing, which frees what it held.
    pub(crate) fn drain(&mut self) -> Drained {
        let Stats {
            commits,
            checkpoints,
            snapshots,
            dropped,
        } = std::mem::take(self);
        Drained {
            commits,
            checkpoints,
            snapshots,
            dropped,
        }
    }
}

/// Appends `item`, first dropping the oldest (returned) when `kept` already holds [`KEPT`].
fn keep<T>(kept: &mut VecDeque<T>, item: T) -> Option<T> {
    let oldest = if kept.len() >= KEPT {
        kept.pop_front()
    } else {
        None
    };
    kept.push_back(item);
    oldest
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::CommitStat;
    use super::Dropped;
    use super::KEPT;
    use super::SnapshotStat;
    use super::Stats;

    /// A commit told apart by its frame bytes (`i`), with no record bytes, so a sum of the wrong
    /// field shows.
    fn commit(i: usize, attempts: u32) -> CommitStat {
        CommitStat {
            bytes: i,
            raw: 0,
            pages: 1,
            attempts,
            append: Duration::ZERO,
            vfs: Duration::ZERO,
        }
    }

    /// A snapshot told apart by its body bytes (`i`).
    fn snapshot(i: usize) -> SnapshotStat {
        SnapshotStat {
            offset: format!("{i:020}"),
            bytes: i,
            raw: i,
            copy: Duration::ZERO,
            total: Duration::ZERO,
        }
    }

    // A drain hands over the newest commits, oldest first, and counts the older ones it dropped,
    // with their frame bytes and append retries. Then it starts again from nothing.
    #[test]
    fn a_drain_hands_over_the_newest_in_order_and_counts_the_rest() {
        let mut s = Stats::default();
        let total = KEPT + 4;
        for i in 0..total {
            s.commit(commit(i, if i == 1 { 3 } else { 1 }));
        }
        let d = s.drain();
        assert_eq!(d.commits.len(), KEPT);
        assert!(d.commits.iter().map(|c| c.bytes).eq(4..total));
        assert_eq!(d.dropped, Dropped {
            commits: 4,
            commit_bytes: (0..4).sum(),
            append_retries: 2,
            checkpoints: 0,
            snapshots: 0,
        });
        let d = s.drain();
        assert!(d.commits.is_empty() && d.checkpoints.is_empty() && d.snapshots.is_empty());
        assert_eq!(d.dropped, Dropped::default());
        s.commit(commit(7, 1));
        s.checkpoint(Duration::from_micros(8));
        s.snapshot(snapshot(9));
        let d = s.drain();
        assert!(d.commits.iter().map(|c| c.bytes).eq([7]));
        assert_eq!(d.checkpoints, [Duration::from_micros(8)]);
        assert!(d.snapshots.iter().map(|s| s.bytes).eq([9]));
        assert_eq!(d.dropped, Dropped::default());
    }

    // A database nobody drains keeps at most `KEPT` of each kind, the newest, in queues no larger:
    // 512 KiB of commits (no longer 64 MB) however long it runs.
    #[test]
    fn a_database_nobody_drains_keeps_a_bounded_number_of_each_kind() {
        assert!(size_of::<CommitStat>() <= 64);
        let mut s = Stats::default();
        let total = 3 * KEPT;
        for i in 0..total {
            s.commit(commit(i, 3));
            s.checkpoint(Duration::from_micros(i as u64));
            s.snapshot(snapshot(i));
            assert!(s.commits.len() <= KEPT);
            assert!(s.checkpoints.len() <= KEPT);
            assert!(s.snapshots.len() <= KEPT);
        }
        assert!(s.commits.capacity() <= KEPT, "{}", s.commits.capacity());
        assert!(s.checkpoints.capacity() <= KEPT);
        assert!(s.snapshots.capacity() <= KEPT);
        let d = s.drain();
        let newest = 2 * KEPT..total;
        assert!(d.commits.iter().map(|c| c.bytes).eq(newest.clone()));
        assert!(
            d.checkpoints
                .iter()
                .map(|c| c.as_micros() as usize)
                .eq(newest.clone())
        );
        assert!(d.snapshots.iter().map(|s| s.bytes).eq(newest));
        let dropped = 2 * KEPT as u64;
        assert_eq!(d.dropped, Dropped {
            commits: dropped,
            commit_bytes: (0..dropped).sum(),
            append_retries: 2 * dropped,
            checkpoints: dropped,
            snapshots: dropped,
        });
    }
}
