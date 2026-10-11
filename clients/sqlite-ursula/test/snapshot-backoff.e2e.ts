// A snapshot body over a request size limit in front of the stream (ursulagw's
// --max-request-body-bytes, a proxy's) fails every time until the database shrinks: with a 413 when
// it is a little over the limit, with the connection closed under it when it is far over (the
// gateway stops reading at its limit, #540). The owner reports it (`snapshot_error`,
// `snapshot_bytes`) and holds the next attempt back: commits meanwhile do not send it again, and
// once the database has shrunk by more than a quarter (a VACUUM) the next commit has it published.
// A large publish whose outcome stays unknown is sent twice at most per attempt, and once the cause
// is gone the snapshot thread publishes on its own. The layer is `gate.mjs`, or a real `ursula
// gateway`, and a failed attempt waits a minute here (100 ms by default, doubling, up to an hour
// for a body too large and five minutes otherwise) unless the database shrinks.
import { type ChildProcess, spawn } from "node:child_process";
import { writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { resolve } from "node:path";
import { afterAll, afterEach, beforeAll, expect, it } from "vitest";
import type { AttachStatus } from "../src/index.ts";
import { freshFile } from "./helpers.ts";
import { type Child, Gate, runChild, streamPath, ursulaUrl } from "./kit.ts";

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

/** Waits until the child logged its `n`-th snapshot failure in a row, with the wait before the next attempt. */
async function failedSnapshots(child: Child, n: number): Promise<void> {
	const logged = new RegExp(`event=snapshot_failed .* failures=${n} retry_ms=\\d+ `);
	for (let i = 0; i < 300; i++) {
		if (logged.test(child.stderr())) return;
		await sleep(100);
	}
	throw new Error(`no snapshot failure ${n} logged: ${child.stderr()}`);
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
	await failedSnapshots(child, 1);
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

// The gate answers 503, an outage: each attempt sends the 1.6 MB body twice at most. While nothing
// commits, the next attempt waits. The first commit after it brings that attempt forward (the stream
// takes writes again), once: the commits after it do not.
it("a large snapshot publish whose outcome stays unknown is sent twice at most, and a commit brings the next attempt forward once", async () => {
	const path = streamPath();
	const file = freshFile();
	const checked = `${file}.checked`;
	const before = await refusedSoFar();
	await gate.refuse("", { status: 503, methods: ["PUT"], over: LIMIT });
	const first = [...FILL, ...rewrites(20)];
	const steps = [...first, `@wait:${checked}`, ...rewrites(10, 40), "@sleep:2000", "@status"];
	const child = runChild(file, gate.url + path, steps, ENV);

	await refusedSince(before, 2);
	await failedSnapshots(child, 1);
	await sleep(1500);
	expect((await refusedSoFar()) - before).toBe(2);
	writeFileSync(checked, "");
	const s = await statusAt(child, steps.length - 1);
	expect(child.lines.filter((l) => l.ok === false)).toEqual([]);
	expect(s).toMatchObject({ poisoned: false, snapshot: "-1", snapshot_failures: 2 });
	expect(s.snapshot_error).toMatch(/^put .+\/snapshot\/\S+: 503$/);
	expect((await refusedSoFar()) - before).toBe(4);
	await child.exited;
});

// Without the minute here: the default first wait (100 ms), doubling. Once the layer accepts the
// publish again, the snapshot thread publishes on its own, with no commit to request it.
it("after an outage the snapshot thread publishes on its own once the layer accepts it", async () => {
	const path = streamPath();
	const file = freshFile();
	const done = `${file}.done`;
	const before = await refusedSoFar();
	await gate.refuse("", { status: 503, methods: ["PUT"], over: LIMIT });
	const steps = [...FILL, ...rewrites(20), `@wait:${done}`];
	const child = runChild(file, gate.url + path, steps, { URSULA_VFS_SNAPSHOT_MIN_BYTES: "1", CHILD_EXIT: "1" });
	await child.waitFor((l) => l.step === steps.length - 1 && l.phase === "start");
	await refusedSince(before, 2);
	await gate.refuse(null);
	let snapshot = "-1";
	for (let i = 0; i < 300 && snapshot === "-1"; i++) {
		await sleep(100);
		snapshot = (await fetch(ursulaUrl() + path, { method: "HEAD" })).headers.get("stream-snapshot-offset") ?? "-1";
	}
	expect(snapshot, child.stderr()).not.toBe("-1");
	writeFileSync(done, "");
	expect(await child.exited).toMatchObject({ code: 0 });
	expect(child.lines.filter((l) => l.ok === false)).toEqual([]);
});

/** A free port on 127.0.0.1. */
const freePort = (): Promise<number> =>
	new Promise((res) => {
		const server = createServer();
		server.listen(0, "127.0.0.1", () => {
			const address = server.address();
			const port = typeof address === "object" && address !== null ? address.port : 0;
			server.close(() => res(port));
		});
	});

/** An `ursula gateway` in front of the node with a request body limit of `limit` bytes. */
async function startGateway(limit: number): Promise<{ url: string; proc: ChildProcess }> {
	const port = await freePort();
	const args = ["gateway", "--listen", `127.0.0.1:${port}`, "--upstream", ursulaUrl(), "--raft-group-count", "4", "--max-request-body-bytes", String(limit)];
	const proc = spawn(resolve(process.env.URSULA_BIN ?? ""), args, { stdio: "ignore" });
	const url = `http://127.0.0.1:${port}`;
	for (let i = 0; ; i++) {
		if (await fetch(`${url}/`).then(() => true, () => false)) return { url, proc };
		if (i > 100 || proc.exitCode !== null) {
			proc.kill("SIGKILL");
			throw new Error("the gateway did not start");
		}
		await sleep(100);
	}
}

// Through a real gateway. Commits stay below its limit (one 64 KiB row each), while the snapshot of
// the 1.6 MB database is over it. A body a little over the limit gets a readable 413, and one far
// over it usually meets a closed connection, a failed write whose error depends on the platform
// (`Broken pipe`, `Connection reset`, `Invalid argument` on macOS): the backoff holds either way,
// and after a VACUUM the snapshot is published.
it("through ursula gateway, a snapshot over its body limit waits until the database shrinks", async () => {
	const gateway = await startGateway(LIMIT);
	let child: Child | undefined;
	try {
		const path = streamPath();
		const file = freshFile();
		const shrink = `${file}.shrink`;
		const fill = ["CREATE TABLE t(k INTEGER PRIMARY KEY, v BLOB)", ...rewrites(24), ...rewrites(40, 24)];
		const steps = [...fill, "@status", `@wait:${shrink}`, "DELETE FROM t WHERE k >= 2", "VACUUM", "@sleep:3000", "@status"];
		child = runChild(file, gateway.url + path, steps, ENV);
		await failedSnapshots(child, 1);
		const s = await statusAt(child, fill.length);
		expect(s).toMatchObject({ poisoned: false, snapshot_failures: 1 });
		expect(s.snapshot_bytes).toBeGreaterThan(LIMIT);
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
