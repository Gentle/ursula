// An owner's health for operators: `ursula_status` reports commits, retries, the log since the
// latest snapshot against the size a snapshot is taken at, the snapshot's age and failures, and the
// attach time; `instrument()` turns them into OpenTelemetry-shaped metrics; and the extension logs
// one logfmt line per event on stderr.
import { expect, it } from "vitest";
import { attach, instrument, status } from "../src/index.ts";
import { freshFile } from "./helpers.ts";
import { openPlain, recordingMeter, runChild, streamPath, ursulaUrl } from "./kit.ts";

it("status reports an owner's health, and instrument() reports it as metrics", () => {
	const file = freshFile();
	attach(file, ursulaUrl() + streamPath());
	const { meter, records, callbacks, observe } = recordingMeter();
	const stop = instrument(file, meter, { intervalMs: 60_000 });
	const db = openPlain(file);
	db.exec("CREATE TABLE t(x TEXT)");
	for (let i = 0; i < 5; i++) db.exec(`INSERT INTO t VALUES ('r${i}')`);
	db.close();
	const s = status(file);
	expect(s).toMatchObject({ commits: 6, append_retries: 0, snapshot_failures: 0, snapshot_error: null, snapshot_age_ms: null });
	expect(s.attach_ms).toBeGreaterThanOrEqual(0);
	expect(s.log_bytes).toBeGreaterThan(0);
	expect(s.snapshot_due_bytes).toBeGreaterThanOrEqual(s.log_bytes);
	expect(observe("sqlite_ursula.log.bytes")).toBe(s.log_bytes);
	expect(observe("sqlite_ursula.poisoned")).toBe(0);
	expect(observe("sqlite_ursula.snapshot.age")).toBeUndefined();
	// No snapshot body built yet: no value.
	expect(s.snapshot_bytes).toBeNull();
	expect(observe("sqlite_ursula.snapshot.bytes")).toBeUndefined();
	stop();
	expect(records.get("sqlite_ursula.commit.duration")).toHaveLength(6);
	expect(records.get("sqlite_ursula.append.duration")).toHaveLength(6);
	expect(records.get("sqlite_ursula.append.retries")).toBeUndefined();
	expect((records.get("sqlite_ursula.stream.bytes") ?? []).reduce((a, b) => a + b, 0)).toBeGreaterThan(0);
	expect(callbacks.size).toBe(0);
});

// Without `intervalMs`, instrument() drains every second: a commit reaches the histograms without a
// stop.
it("instrument() drains every second by default", async () => {
	const file = freshFile();
	attach(file, ursulaUrl() + streamPath());
	const { meter, records } = recordingMeter();
	const stop = instrument(file, meter);
	try {
		const db = openPlain(file);
		db.exec("CREATE TABLE t(x INTEGER)");
		db.close();
		for (let i = 0; i < 30 && records.get("sqlite_ursula.commit.duration") === undefined; i++) await new Promise((r) => setTimeout(r, 100));
		expect(records.get("sqlite_ursula.commit.duration")).toHaveLength(1);
	} finally {
		stop();
	}
});

// The extension keeps the newest 8,192 commits between two drains. Commits dropped past that still
// reach the counters: their frame bytes in one add, and their number in `commit.unmeasured`, while
// the histograms get the 8,192 kept.
it("instrument() counts the commits dropped between two drains", () => {
	const file = freshFile();
	attach(file, ursulaUrl() + streamPath());
	const { meter, records } = recordingMeter();
	const stop = instrument(file, meter, { intervalMs: 60_000 });
	const db = openPlain(file);
	db.exec("CREATE TABLE t(x INTEGER)");
	const insert = db.prepare("INSERT INTO t VALUES (?)");
	for (let i = 0; i < 8_192 + 4; i++) insert.run(i);
	db.close();
	expect(status(file).commits).toBe(8_192 + 5);
	stop();
	expect(records.get("sqlite_ursula.commit.duration")).toHaveLength(8_192);
	expect(records.get("sqlite_ursula.append.duration")).toHaveLength(8_192);
	const bytes = records.get("sqlite_ursula.stream.bytes") ?? [];
	expect(bytes).toHaveLength(8_192 + 1);
	expect(bytes.at(-1)).toBeGreaterThan(0);
	expect(records.get("sqlite_ursula.commit.unmeasured")).toEqual([5]);
});

it("snapshots show in the age and failure fields, and a poisoned owner logs a logfmt event", async () => {
	const url = ursulaUrl() + streamPath();
	// One row rewritten: the database stays small while the log outgrows it, so snapshots are taken.
	const rows = Array.from({ length: 20 }, () => "INSERT OR REPLACE INTO t VALUES (1, hex(randomblob(800)))");
	const child = runChild(freshFile(), url, ["CREATE TABLE t(k INTEGER PRIMARY KEY, x TEXT)", ...rows, "@sleep:2000", "INSERT INTO t VALUES (100, 'after')"], {
		URSULA_VFS_SNAPSHOT_MIN_BYTES: "1",
		CHILD_EXIT: "1",
	});
	await child.waitFor((l) => l.step === rows.length + 1 && l.phase === "start");
	// A new owner claims the stream while the child sleeps: the child's next commit is fenced.
	attach(freshFile(), url);
	const done = await child.waitFor((l) => l.done === true);
	expect(done.poisoned).toBe(true);
	expect(done.health?.commits).toBe(rows.length + 1);
	expect(done.health?.snapshot_age_ms).not.toBeNull();
	expect(done.health).toMatchObject({ snapshot_failures: 0, snapshot_error: null });
	await child.exited;
	expect(child.stderr()).toMatch(/^sqlite-ursula-vfs level=warn event=poisoned file=\S+ stream=\S+ fenced=true reason="fenced: epoch \d+ superseded by \d+ \(403\)"$/m);
});
