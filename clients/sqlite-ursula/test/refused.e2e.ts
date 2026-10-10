// Writes refused before they reach the stream for other reasons than a 402: answers marked
// `x-ursula-not-applied` (the server's admission while it is under write pressure, a gateway that
// cannot authorize a request), a token refused until the retry budget runs out, and an authorizer's
// 403. Nothing of such a write was applied, so it fails alone, as a refused 402 does: the commit is
// rolled back but the file is not poisoned, reads go on, and the next commit goes out as usual and
// succeeds once the refusals stop. An attempt whose outcome is unknown among the refusals still
// poisons: the commit may be in the stream. The refusing layer is `gate.mjs`.
import { writeFileSync } from "node:fs";
import { afterAll, afterEach, beforeAll, expect, it } from "vitest";
import { attach, openUrsulaPiStorage, status, UrsulaPaymentRequiredError, UrsulaReplicationError, UrsulaWriteRefusedError } from "../src/index.ts";
import { openHarness, textTurn } from "./harness-kit.ts";
import { ctx, freshFile } from "./helpers.ts";
import { type Child, type ChildLine, Gate, openPlain, runChild, streamPath, ursulaUrl } from "./kit.ts";

/** SQLITE_IOERR_AUTH: a commit refused before it reached the stream. */
const COMMIT_REFUSED = 7178;
/** SQLITE_IOERR_WRITE: a commit that poisons the file. */
const POISONED = 778;
/** The server's answer while its Raft log is over its hard limit, marked as applying nothing. */
const PRESSURE = { status: 503, headers: { "x-ursula-not-applied": "RaftLogPressure", "retry-after": "1" } };
const PRESSURE_BODY = '{"error":"RaftLogPressure"}';
/** A retry budget that runs out after two attempts answered `Retry-After: 1`. */
const SHORT_BUDGET = { URSULA_VFS_RETRY_MS: "1500" };

let gate: Gate;
beforeAll(async () => {
	gate = await Gate.start(ursulaUrl());
});
// A test that fails midway leaves the gate refusing: the next test starts with it forwarding.
afterEach(async () => {
	await gate.refuse(null);
});
afterAll(async () => {
	await gate.close();
});

const xs = (file: string): string[] => {
	const db = openPlain(file);
	try {
		return (db.prepare("SELECT x FROM t ORDER BY x").all() as { x: string }[]).map((r) => r.x);
	} finally {
		db.close();
	}
};
const step = (child: Child, n: number): ChildLine | undefined => child.lines.find((l) => l.step === n && l.ok !== undefined);
const errcode = (error: unknown): unknown => (error as { errcode?: unknown } | undefined)?.errcode;

it("a commit refused under write pressure, or with its token refused, until the budget runs out fails alone, and the next commit succeeds once the refusals stop", async () => {
	const path = streamPath();
	const file = freshFile();
	const [pressure, token, accepting] = [`${file}.pressure`, `${file}.token`, `${file}.accepting`];
	const steps = [
		"CREATE TABLE t(x TEXT)",
		"INSERT INTO t VALUES ('a1')",
		"@status",
		`@wait:${pressure}`,
		"INSERT INTO t VALUES ('a2')",
		"@query:SELECT x FROM t ORDER BY x",
		"@status",
		`@wait:${token}`,
		"BEGIN; INSERT INTO t VALUES ('a3'); INSERT INTO t VALUES ('a4'); COMMIT",
		"@status",
		`@wait:${accepting}`,
		"INSERT INTO t VALUES ('a5')",
	];
	const child = runChild(file, gate.url + path, steps, { CHILD_EXIT: "1", ...SHORT_BUDGET });
	await child.waitFor((l) => l.step === 3 && l.phase === "start");
	await gate.refuse(PRESSURE_BODY, PRESSURE);
	writeFileSync(pressure, "");
	await child.waitFor((l) => l.step === 7 && l.phase === "start");
	await gate.refuse("", { status: 401 });
	writeFileSync(token, "");
	await child.waitFor((l) => l.step === 10 && l.phase === "start");
	await gate.refuse(null);
	writeFileSync(accepting, "");
	const done = await child.waitFor((l) => l.done === true);
	expect((await child.exited).code, child.stderr()).toBe(0);

	const before = step(child, 2)?.status;
	expect(step(child, 4)).toMatchObject({ ok: false, errcode: COMMIT_REFUSED });
	expect(step(child, 5)?.rows).toEqual([{ x: "a1" }]);
	const underPressure = step(child, 6)?.status;
	expect(underPressure).toMatchObject({
		poisoned: false,
		fenced: false,
		writes_refused: true,
		refused_kind: "not_applied",
		refused_reason: "RaftLogPressure",
		payment_required: false,
		payment_reason: null,
	});
	// Nothing of the refused commit moved the owner: same epoch, same offset.
	expect([underPressure?.epoch, underPressure?.offset]).toEqual([before?.epoch, before?.offset]);
	expect(step(child, 8)).toMatchObject({ ok: false, errcode: COMMIT_REFUSED });
	expect(step(child, 9)?.status).toMatchObject({ poisoned: false, writes_refused: true, refused_kind: "unauthorized", refused_reason: null, offset: before?.offset });
	expect(step(child, 11)?.ok).toBe(true);
	expect(done).toMatchObject({ poisoned: false, fenced: false, writes_refused: false, refused_kind: null, refused_reason: null, epoch: before?.epoch });
	expect(done.attempts).toEqual([1, 1, 1]);
	const log = child.stderr();
	expect(log).toMatch(/event=writes_refused file=\S+ stream=\S+ during=commit kind=not_applied reason="503 \(not applied: RaftLogPressure\)"/);
	expect(log).toMatch(/event=writes_refused file=\S+ stream=\S+ during=commit kind=unauthorized reason=/);
	expect(log).toMatch(/event=writes_resumed file=\S+ stream=\S+$/m);
	expect(log).not.toMatch(/event=poisoned/);

	// The local files still match their sidecar, and the stream holds the accepted commits alone.
	attach(file, ursulaUrl() + path);
	expect(status(file)).toMatchObject({ local: done.offset, installed: "-1" });
	expect(xs(file)).toEqual(["a1", "a5"]);
	const copy = freshFile();
	attach(copy, ursulaUrl() + path);
	expect(xs(copy)).toEqual(["a1", "a5"]);
});

// The commit's first attempt reaches the stream but its answer is lost, and every retry is refused
// as applying nothing until the budget runs out: the refusals say nothing about the first attempt,
// which may be (and here is) in the stream. The owner is poisoned, never refused: sent again with
// the same producer sequence, the next commit would be acknowledged as a duplicate of this one.
it("an append whose answer was lost and whose retries are refused as applying nothing still poisons", async () => {
	const path = streamPath();
	const file = freshFile();
	const armed = `${file}.armed`;
	const child = runChild(file, gate.url + path, ["CREATE TABLE t(x TEXT)", `@wait:${armed}`, "INSERT INTO t VALUES ('once')"], { CHILD_EXIT: "1", ...SHORT_BUDGET });
	await child.waitFor((l) => l.step === 1 && l.phase === "start");
	const before = await gate.counts();
	await gate.dropNextPostThenRefuse(PRESSURE_BODY, 100, PRESSURE);
	writeFileSync(armed, "");
	const done = await child.waitFor((l) => l.done === true);
	expect((await child.exited).code, child.stderr()).toBe(0);
	const after = await gate.counts();
	expect(after.dropped - before.dropped).toBe(1);
	expect(after.refused - before.refused).toBeGreaterThanOrEqual(1);
	expect(step(child, 2)).toMatchObject({ ok: false, errcode: POISONED });
	expect(done).toMatchObject({ poisoned: true, fenced: false, writes_refused: false, refused_kind: null });
	expect(done.reason).toMatch(/outcome unknown/);
	// The lost attempt was applied: a rebuild from the stream holds it, once.
	const copy = freshFile();
	attach(copy, ursulaUrl() + path);
	expect(xs(copy)).toEqual(["once"]);
});

// An authorizer's 403 (without `Producer-Epoch`, which only the server's fence carries) is final and
// applied nothing: the commit fails alone at once, without waiting out the budget. An attach whose
// claim it refuses claims nothing and throws UrsulaWriteRefusedError, and a read-only attach reads the
// database meanwhile.
it("an authorizer's 403 fails a commit alone at once, and an attach it refuses throws UrsulaWriteRefusedError", async () => {
	const path = streamPath();
	const file = freshFile();
	const [refusing, accepting] = [`${file}.refusing`, `${file}.accepting`];
	const steps = ["CREATE TABLE t(x TEXT)", `@wait:${refusing}`, "INSERT INTO t VALUES ('d1')", "@status", `@wait:${accepting}`, "INSERT INTO t VALUES ('d2')"];
	const child = runChild(file, gate.url + path, steps, { CHILD_EXIT: "1" });
	await child.waitFor((l) => l.step === 1 && l.phase === "start");
	await gate.refuse("write access denied", { status: 403 });
	writeFileSync(refusing, "");
	await child.waitFor((l) => l.step === 4 && l.phase === "start");
	await gate.refuse(null);
	writeFileSync(accepting, "");
	const done = await child.waitFor((l) => l.done === true);
	expect((await child.exited).code, child.stderr()).toBe(0);
	expect(step(child, 2)).toMatchObject({ ok: false, errcode: COMMIT_REFUSED });
	expect(step(child, 3)?.status).toMatchObject({ poisoned: false, fenced: false, writes_refused: true, refused_kind: "forbidden", refused_reason: "write access denied" });
	expect(step(child, 5)?.ok).toBe(true);
	expect(done).toMatchObject({ poisoned: false, writes_refused: false, attempts: [1, 1] });
	expect(child.stderr()).not.toMatch(/event=append_retry/);

	await gate.refuse("write access denied", { status: 403 });
	const reader = freshFile();
	let error: unknown;
	try {
		attach(reader, gate.url + path);
	} catch (e) {
		error = e;
	}
	expect(error).toBeInstanceOf(UrsulaWriteRefusedError);
	expect(error).not.toBeInstanceOf(UrsulaPaymentRequiredError);
	expect(error).toMatchObject({ kind: null, reason: null });
	expect((error as Error).message).toMatch(/claim \S+: 403 write access denied: an authorizer denied it/);
	expect(errcode((error as Error).cause)).toBe(COMMIT_REFUSED);
	expect(() => openPlain(reader)).toThrow(/unable to open database file/);
	attach(reader, gate.url + path, { readOnly: true });
	expect(xs(reader)).toEqual(["d2"]);
});

it("Pi: a commit an authorizer refuses with 403 rejects with UrsulaWriteRefusedError, and the storage commits again once it is allowed", async () => {
	const storage = await openUrsulaPiStorage(freshFile(), gate.url + streamPath());
	await gate.refuse("write access denied", { status: 403 });
	const error = await openHarness(storage).then(
		() => undefined,
		(e: unknown) => e,
	);
	await gate.refuse(null);
	expect(error).toBeInstanceOf(UrsulaWriteRefusedError);
	expect(error).not.toBeInstanceOf(UrsulaPaymentRequiredError);
	expect(error).not.toBeInstanceOf(UrsulaReplicationError);
	expect(error).toMatchObject({ kind: "forbidden", reason: "write access denied" });
	const { harness, root } = await openHarness(storage);
	await textTurn(root, 1);
	await harness.close(ctx);
	await storage.close(ctx);
});
