// A snapshot body over a request size limit in front of the stream (ursulagw's
// --max-request-body-bytes, a proxy's) is refused with 413 every time until the database shrinks. The
// owner reports it (`snapshot_error`, `snapshot_bytes`) and holds the next attempt back: commits
// meanwhile do not send it again, and once the database has shrunk by more than a quarter (a VACUUM)
// the next commit has it published. A publish whose outcome stays unknown is sent twice at most per
// attempt. The layer is `gate.mjs`, and a failed attempt waits a minute here (100 ms by default,
// doubling up to an hour) unless the database shrinks.
import { writeFileSync } from "node:fs";
import { afterAll, afterEach, beforeAll, expect, it } from "vitest";
import type { AttachStatus } from "../src/index.ts";
import { freshFile } from "./helpers.ts";
import { type Child, Gate, runChild, streamPath, ursulaUrl } from "./kit.ts";

/** The gate's request size limit: every commit stays below it, the database's snapshot does not. */
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

	// The snapshot due during the first commits went out once and was refused.
	await refusedSince(before, 1);
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

it("a snapshot publish whose outcome stays unknown is sent twice at most, then waits", async () => {
	const path = streamPath();
	const file = freshFile();
	const checked = `${file}.checked`;
	const before = await refusedSoFar();
	await gate.refuse("", { status: 503, methods: ["PUT"], over: LIMIT });
	const first = [...FILL, ...rewrites(20)];
	const steps = [...first, `@wait:${checked}`, ...rewrites(10, 40), "@sleep:2000", "@status"];
	const child = runChild(file, gate.url + path, steps, ENV);

	await refusedSince(before, 2);
	await sleep(1500);
	expect((await refusedSoFar()) - before).toBe(2);
	writeFileSync(checked, "");
	const s = await statusAt(child, steps.length - 1);
	expect(child.lines.filter((l) => l.ok === false)).toEqual([]);
	expect(s).toMatchObject({ poisoned: false, snapshot: "-1", snapshot_failures: 1 });
	expect(s.snapshot_error).toMatch(/^put .+\/snapshot\/\S+: 503$/);
	expect((await refusedSoFar()) - before).toBe(2);
	await child.exited;
});
