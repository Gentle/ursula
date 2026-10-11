// A snapshot body over a request size limit in front of the stream (ursulagw's
// --max-request-body-bytes, a proxy's) fails every time until the database shrinks: with a 413 when
// it is a little over the limit, with the connection closed under it when it is far over (the
// gateway stops reading at its limit, #540). The owner reports it (`snapshot_error`,
// `snapshot_bytes`) and holds the next attempt back: commits meanwhile do not send it again, and
// once the database has shrunk by more than a quarter (a VACUUM) the next commit has it published.
// A large body that fails while the stream still answers a HEAD is taken for one too large, and one
// that fails while the stream does not is an outage. In an outage a large publish whose outcome
// stays unknown is sent twice at most per attempt, a commit brings the next attempt forward once,
// and once the layer accepts it again the snapshot thread publishes on its own. A retention move
// that fails is logged and tried again after the next snapshot. The layer is `gate.mjs`, or a real
// `ursula gateway`. A failed attempt waits a minute here (100 ms by default), doubling, up to an
// hour for a body too large and five minutes otherwise, give or take a fifth, unless the database
// shrinks.
import { writeFileSync } from "node:fs";
import { afterAll, afterEach, beforeAll, expect, it } from "vitest";
import type { AttachStatus } from "../src/index.ts";
import { freshFile } from "./helpers.ts";
import { type Child, Gate, runChild, startGateway, streamPath, ursulaUrl } from "./kit.ts";

/** The request size limit: the database's snapshot is over it. The gate applies it to PUTs alone, so the commit that fills the database (1.6 MB) passes it. */
const LIMIT = 512 * 1024;
/** A snapshot is due once the log outgrows the database, and the wait after a failed one is a minute. */
const ENV = { URSULA_VFS_SNAPSHOT_MIN_BYTES: "1", URSULA_VFS_SNAPSHOT_RETRY_MS: "60000", CHILD_EXIT: "1" };

let gate: Gate;
beforeAll(async () => {
	gate = await Gate.start(ursulaUrl());
});
afterEach(async () => {
	await gate.refuse(null);
});
afterAll(async () => {
	await gate.close();
});

/** 24 rows of 64 KiB of random bytes in one commit: a database of about 1.6 MB, which compresses to as much. */
const FILL = ["CREATE TABLE t(k INTEGER PRIMARY KEY, v BLOB)", "WITH RECURSIVE n(k) AS (SELECT 0 UNION ALL SELECT k + 1 FROM n WHERE k < 23) INSERT INTO t SELECT k, randomblob(65536) FROM n"];
/** `n` commits that each rewrite one of the 24 rows: the database keeps its size while the log grows, so a snapshot is due after the first ones. */
const rewrites = (n: number, from = 0): string[] => Array.from({ length: n }, (_, i) => `INSERT OR REPLACE INTO t VALUES (${(from + i) % 24}, randomblob(65536))`);

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** Requests the gate refused so far (over every test of this file). */
const refusedSoFar = async (): Promise<number> => (await gate.counts()).refused;

/** Waits until the gate has refused `n` requests since it had refused `before`. */
async function refusedSince(before: number, n: number): Promise<void> {
	for (let i = 0; i < 300; i++) {
		if ((await refusedSoFar()) - before >= n) return;
		await sleep(100);
	}
	throw new Error(`the gate refused ${(await refusedSoFar()) - before} requests, not ${n}`);
}

/** Waits until the child logged its `n`-th snapshot failure in a row, and returns the wait it logged before the next attempt and what the attempt failed on. */
async function failedSnapshots(child: Child, n: number, ms = 30_000): Promise<{ retryMs: number; kind: string }> {
	const logged = new RegExp(`event=snapshot_failed .* failures=${n} retry_ms=(\\d+) kind=(\\w+) `);
	for (let i = 0; i < ms / 100; i++) {
		const hit = logged.exec(child.stderr());
		if (hit !== null) return { retryMs: Number(hit[1]), kind: hit[2] ?? "" };
		await sleep(100);
	}
	throw new Error(`no snapshot failure ${n} logged: ${child.stderr()}`);
}

/**
 * Waits until no snapshot attempt is under way while the gate refuses the publish and the probe: the
 * child logged `n` failures in a row (`atLeast` at least), and the gate refused the three requests of
 * each attempt (the body twice, then the probe). Returns `n` and the wait the child logged after the
 * last one.
 */
async function betweenAttempts(child: Child, before: number, atLeast: number): Promise<{ n: number; retryMs: number; kind: string }> {
	for (let i = 0; i < 600; i++) {
		const last = [...child.stderr().matchAll(/event=snapshot_failed .* failures=(\d+) retry_ms=(\d+) kind=(\w+) /g)].at(-1);
		const n = Number(last?.[1] ?? 0);
		if (n >= atLeast && (await refusedSoFar()) - before === 3 * n) return { n, retryMs: Number(last?.[2]), kind: last?.[3] ?? "" };
		await sleep(50);
	}
	throw new Error(`no attempt ended with its three requests refused: ${child.stderr()}`);
}

/** The latest snapshot and the retention of the stream at `path`, from the node. */
async function offsets(path: string): Promise<{ snapshot: string; retained: string }> {
	const head = await fetch(ursulaUrl() + path, { method: "HEAD" });
	return { snapshot: head.headers.get("stream-snapshot-offset") ?? "-1", retained: head.headers.get("stream-retained-offset") ?? "-1" };
}

/** Waits until the stream at `path` has a snapshot past `after`, and returns it. */
async function snapshotPast(path: string, after: string, ms = 30_000): Promise<string> {
	for (let i = 0; i < ms / 50; i++) {
		const { snapshot } = await offsets(path);
		if (snapshot !== "-1" && snapshot > after) return snapshot;
		await sleep(50);
	}
	throw new Error(`no snapshot past ${after} at ${path}`);
}

/** The `ursula_status` the child printed at its `step`. */
async function statusAt(child: Child, step: number): Promise<AttachStatus> {
	const line = await child.waitFor((l) => l.step === step && l.status !== undefined);
	return line.status as AttachStatus;
}

it("a snapshot over a request size limit is reported and not sent again until the database shrinks, then published", async () => {
	const path = streamPath();
	const file = freshFile();
	const [refused, shrink] = [`${file}.refused`, `${file}.shrink`];
	const before = await refusedSoFar();
	await gate.refuse("", { status: 413, methods: ["PUT"], over: LIMIT });
	const first = [...FILL, ...rewrites(20)];
	const held = [...rewrites(10, 40), "@sleep:2000"];
	const steps = [...first, `@wait:${refused}`, "@status", ...held, "@status", `@wait:${shrink}`, "DELETE FROM t WHERE k >= 2", "VACUUM", "@sleep:3000", "@status"];
	const atRefused = first.length + 1;
	const afterHeld = atRefused + held.length + 1;
	const atEnd = steps.length - 1;
	const child = runChild(file, gate.url + path, steps, ENV);

	// The snapshot due during the first commits went out once and was refused, for its size.
	await refusedSince(before, 1);
	expect((await failedSnapshots(child, 1)).kind).toBe("size");
	writeFileSync(refused, "");
	const s = await statusAt(child, atRefused);
	expect(s).toMatchObject({ poisoned: false, writes_refused: false, snapshot: "-1", snapshot_failures: 1 });
	expect(s.snapshot_error).toMatch(/^publish snapshot .+: 413: a body of \d+ bytes is over the request size limit/);
	expect(s.snapshot_bytes).toBeGreaterThan(LIMIT);
	expect(s.snapshot_error).toContain(`a body of ${s.snapshot_bytes} bytes`);

	// Every commit since requested the snapshot (it is still due), and none sent it again.
	const h = await statusAt(child, afterHeld);
	expect(h).toMatchObject({ snapshot: "-1", snapshot_failures: 1, snapshot_bytes: s.snapshot_bytes });
	expect(h.log_bytes).toBeGreaterThan(h.snapshot_due_bytes);
	expect((await refusedSoFar()) - before).toBe(1);
	expect(child.lines.filter((l) => l.ok === false)).toEqual([]);

	// A VACUUM shrinks the database below three quarters of its size: the next commit's request has
	// the snapshot taken at once, its body below the limit, and published.
	writeFileSync(shrink, "");
	const done = await statusAt(child, atEnd);
	expect(child.lines.filter((l) => l.ok === false)).toEqual([]);
	expect(done).toMatchObject({ poisoned: false, snapshot_failures: 0, snapshot_error: null });
	expect(done.snapshot).not.toBe("-1");
	expect(done.snapshot_bytes).toBeLessThan(LIMIT);
	expect((await refusedSoFar()) - before).toBe(1);
	const head = await fetch(ursulaUrl() + path, { method: "HEAD" });
	expect(head.headers.get("stream-snapshot-offset")).toBe(done.snapshot);
	await child.exited;
});

// The gate answers 503 to the snapshot's PUT and to the HEAD that probes the stream after it, an
// outage: each attempt sends the 1.6 MB body twice at most. While nothing commits, the next attempt
// waits (a minute here). The first commit after it brings that attempt forward, to within 30 s (the
// stream takes writes again), once a run: the commits after it do not.
it("a large snapshot publish whose outcome stays unknown is sent twice at most, and a commit brings the next attempt forward once", async () => {
	const path = streamPath();
	const file = freshFile();
	const [armed, checked, second] = [`${file}.armed`, `${file}.checked`, `${file}.second`];
	// The attach's HEAD and the fill pass the gate. The commit that makes the snapshot due (the
	// second rewrite) is the last before a wait, so none runs between the first failure and its end.
	const first = [...FILL, `@wait:${armed}`, ...rewrites(2)];
	const steps = [...first, `@wait:${checked}`, "@status", ...rewrites(10, 40), `@wait:${second}`, "@status"];
	const child = runChild(file, gate.url + path, steps, ENV);
	await child.waitFor((l) => l.step === FILL.length && l.phase === "start");
	const before = await refusedSoFar();
	await gate.refuse("", { status: 503, methods: ["PUT", "HEAD"] });
	writeFileSync(armed, "");

	// The first attempt sent the body twice, then probed the stream, which did not answer: an
	// outage. Nothing more went out.
	expect((await failedSnapshots(child, 1)).kind).toBe("outage");
	await sleep(1500);
	expect((await refusedSoFar()) - before).toBe(3);
	writeFileSync(checked, "");
	const s = await statusAt(child, first.length + 1);
	expect(s).toMatchObject({ poisoned: false, snapshot: "-1", snapshot_failures: 1 });
	expect(s.log_bytes).toBeGreaterThan(s.snapshot_due_bytes);

	// The first commit since brought the second attempt forward from about a minute to within 30 s,
	// and the nine after it did not bring a third.
	expect((await failedSnapshots(child, 2, 40_000)).kind).toBe("outage");
	await sleep(1500);
	expect((await refusedSoFar()) - before).toBe(6);
	writeFileSync(second, "");
	const done = await statusAt(child, steps.length - 1);
	expect(child.lines.filter((l) => l.ok === false)).toEqual([]);
	expect(done).toMatchObject({ poisoned: false, snapshot: "-1", snapshot_failures: 2 });
	expect(done.snapshot_error).toMatch(/^put .+\/snapshot\/\S+: 503$/);
	expect((await refusedSoFar()) - before).toBe(6);
	await child.exited;
});

// Without the minute here: the first wait is a second, doubling. The gate refuses the publish and the
// probe, an outage, and opens only between two attempts, so the publish comes from an attempt the
// snapshot thread started on its own, with no commit to request it, no sooner than the wait it
// logged.
it("after an outage the snapshot thread publishes on its own once the layer accepts it", async () => {
	const path = streamPath();
	const file = freshFile();
	const [armed, done] = [`${file}.armed`, `${file}.done`];
	const steps = [...FILL, `@wait:${armed}`, ...rewrites(20), `@wait:${done}`];
	const child = runChild(file, gate.url + path, steps, { URSULA_VFS_SNAPSHOT_MIN_BYTES: "1", URSULA_VFS_SNAPSHOT_RETRY_MS: "1000", CHILD_EXIT: "1" });
	await child.waitFor((l) => l.step === FILL.length && l.phase === "start");
	const before = await refusedSoFar();
	await gate.refuse("", { status: 503, methods: ["PUT", "HEAD"] });
	writeFileSync(armed, "");
	await child.waitFor((l) => l.step === steps.length - 1 && l.phase === "start");
	const { n, retryMs, kind } = await betweenAttempts(child, before, 2);
	await gate.refuse(null);
	const opened = Date.now();
	expect((await refusedSoFar()) - before, "an attempt went out before the gate opened").toBe(3 * n);
	expect(kind).toBe("outage");
	const snapshot = await snapshotPast(path, "-1");
	expect(Date.now() - opened, child.stderr()).toBeGreaterThanOrEqual(0.8 * retryMs);
	expect(child.stderr()).not.toMatch(new RegExp(`failures=${n + 1} `));
	writeFileSync(done, "");
	expect(await child.exited).toMatchObject({ code: 0 });
	expect(child.lines.filter((l) => l.ok === false)).toEqual([]);
	expect((await offsets(path)).snapshot).toBe(snapshot);
});

// A retention move that fails does not fail the snapshot published before it: the move is logged and
// tried again after the next snapshot. The gate answers 503 to the moves alone, until the retry
// budget (two seconds here) runs out.
it("a failed retention move is logged, and the move after the next snapshot succeeds", async () => {
	const path = streamPath();
	const file = freshFile();
	const [armed, failed, moved] = [`${file}.armed`, `${file}.failed`, `${file}.moved`];
	// Each run of 30 rewrites outgrows the database, so each makes a snapshot due.
	const steps = [...FILL, ...rewrites(2), `@wait:${armed}`, ...rewrites(30, 2), `@wait:${failed}`, "@status", ...rewrites(30, 32), `@wait:${moved}`, "@status"];
	const child = runChild(file, gate.url + path, steps, { URSULA_VFS_SNAPSHOT_MIN_BYTES: "1", URSULA_VFS_RETRY_MS: "2000", CHILD_EXIT: "1" });
	// The first snapshot moves no retention: there is none before it.
	const first = await snapshotPast(path, "-1");
	const { retained } = await offsets(path);
	await gate.refuse("", { status: 503, methods: ["PUT"], path: "/retention/" });
	writeFileSync(armed, "");

	// The second snapshot is published, and its move to the first fails.
	for (let i = 0; i < 300 && !child.stderr().includes("event=retention_failed "); i++) await sleep(100);
	expect(child.stderr()).toMatch(/event=retention_failed .*reason="put .+\/retention\/\S+: 503"/);
	const second = await snapshotPast(path, first);
	expect(await offsets(path)).toEqual({ snapshot: second, retained });
	await gate.refuse(null);
	writeFileSync(failed, "");
	const s = await statusAt(child, steps.indexOf(`@wait:${failed}`) + 1);
	// The owner moved no retention yet (`-1`).
	expect(s).toMatchObject({ poisoned: false, snapshot: second, retained: "-1", snapshot_failures: 0, snapshot_error: null });

	// The third snapshot moves retention to the second.
	const third = await snapshotPast(path, second);
	for (let i = 0; i < 100 && (await offsets(path)).retained !== second; i++) await sleep(100);
	expect(await offsets(path)).toEqual({ snapshot: third, retained: second });
	writeFileSync(moved, "");
	const done = await statusAt(child, steps.length - 1);
	expect(done).toMatchObject({ poisoned: false, snapshot: third, retained: second, snapshot_failures: 0, snapshot_error: null });
	expect(child.stderr().match(/event=retention_failed /g)).toHaveLength(1);
	expect(child.lines.filter((l) => l.ok === false)).toEqual([]);
	await child.exited;
});

// Through a real gateway. Commits stay below its limit (one 64 KiB row each), while the snapshot of
// the database is over it. No snapshot is due before the log passes 1.4 MB, so the first body over
// the limit is over 1 MiB too (a smaller one would be sent again for the whole retry budget). A body
// a little over the limit gets a readable 413, and one far over it usually meets a closed
// connection, a failed write whose error depends on the platform (`Broken pipe`, `Connection
// reset`, `Invalid argument` on macOS). The gateway still answers the probe's HEAD, so either is
// the body's size, and the backoff holds until a VACUUM has the snapshot published.
it("through ursula gateway, a snapshot over its body limit waits until the database shrinks", async () => {
	const gateway = await startGateway([ursulaUrl()], ["--max-request-body-bytes", String(LIMIT)]);
	let child: Child | undefined;
	try {
		const path = streamPath();
		const file = freshFile();
		const [failed, shrink] = [`${file}.failed`, `${file}.shrink`];
		const fill = ["CREATE TABLE t(k INTEGER PRIMARY KEY, v BLOB)", ...rewrites(24), ...rewrites(40, 24)];
		const steps = [...fill, `@wait:${failed}`, "@status", `@wait:${shrink}`, "DELETE FROM t WHERE k >= 2", "VACUUM", "@sleep:3000", "@status"];
		child = runChild(file, gateway.url + path, steps, { ...ENV, URSULA_VFS_SNAPSHOT_MIN_BYTES: "1400000" });
		expect((await failedSnapshots(child, 1)).kind).toBe("size");
		writeFileSync(failed, "");
		const s = await statusAt(child, fill.length + 1);
		expect(s).toMatchObject({ poisoned: false, snapshot: "-1", snapshot_failures: 1 });
		expect(s.snapshot_bytes).toBeGreaterThan(1 << 20);
		expect(s.snapshot_error).toMatch(/: 413: a body of \d+ bytes is over the request size limit|: io: /);
		writeFileSync(shrink, "");
		const done = await statusAt(child, steps.length - 1);
		expect(child.lines.filter((l) => l.ok === false)).toEqual([]);
		expect(done).toMatchObject({ poisoned: false, snapshot_failures: 0, snapshot_error: null });
		expect(done.snapshot_bytes).toBeLessThan(LIMIT);
		await child.exited;
	} finally {
		child?.proc.kill("SIGKILL");
		gateway.proc.kill("SIGKILL");
	}
});
