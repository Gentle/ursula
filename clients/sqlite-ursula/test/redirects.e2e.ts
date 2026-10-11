// Leader redirects through a gateway that checks tokens, while every group's leadership moves
// (3 nodes only). This gateway reaches the nodes as `localhost`, while the peer URLs that leader
// redirects name say `127.0.0.1`, so no redirect names one of its upstreams. A gateway before 0.7.3
// passed each such redirect on with a path-only Location. From 0.7.3 it looks for the leader among
// the upstreams it has not reached yet, and answers 503 when it finds none within its limits, so no
// redirect reaches the client (#460). Meanwhile a loop hands every group's leadership to the next
// node, again and again with a short pause, through the nodes' admin API: from before an owner's
// attach, through its commits and snapshot read-backs, a second host's attach from the snapshot, and
// the owner's attach of its own files again, and the test checks that leaderships moved while the
// owner committed and while it waited for its snapshots. Nothing is poisoned, every commit goes
// through, every host reads the same rows, the gateway looked for leaders among its upstreams, and
// a proxy in front of it saw no redirect.
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer, request, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, expect, inject, it } from "vitest";
import { freshFile } from "./helpers.ts";
import { AuthGateway, type Child, type ChildLine, runChild, streamPath } from "./kit.ts";

const nodes = inject("ursulaNodes");
let dir: string | undefined;
let gateway: AuthGateway | undefined;
let proxy: RedirectCounter | undefined;
let moving: { stop(): Promise<number> } | undefined;

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** A node's view of its Raft groups (`/__ursula/metrics` on its admin listener). */
interface NodeView {
	process_incarnation: string;
	raft_groups: { raft_group_id: number; current_leader: number | null; voter_ids: number[] }[];
}

/**
 * Hands every group to the next voter after its leader, once (the views are taken before any move),
 * through the leader's admin API: the groups whose leader accepted the move.
 */
async function moveLeaderships(): Promise<number[]> {
	const views = await Promise.all(
		nodes.map(async (node) => {
			const metrics = await fetch(`${node.admin}/__ursula/metrics`, { signal: AbortSignal.timeout(5000) });
			return { node, view: (await metrics.json()) as NodeView };
		}),
	);
	const moved: number[] = [];
	for (const { node, view } of views) {
		for (const group of view.raft_groups) {
			if (group.current_leader !== node.id) continue;
			const voters = [...group.voter_ids].sort((a, b) => a - b);
			const next = voters[(voters.indexOf(node.id) + 1) % voters.length];
			const r = await fetch(`${node.admin}/__ursula/raft/${group.raft_group_id}/leader/transfer/${next}`, {
				method: "POST",
				headers: { "x-ursula-process-incarnation": view.process_incarnation },
				signal: AbortSignal.timeout(5000),
			});
			const answer = (await r.json().catch(() => ({}))) as { transferred?: boolean };
			if (r.ok && answer.transferred === true) moved.push(group.raft_group_id);
		}
	}
	return moved;
}

/**
 * Moves every group's leadership, then waits `pauseMs`, until `stop`, which returns the moves made.
 * `moved` counts them so far, and `byGroup` per group.
 */
function moveLeadershipsUntilStopped(pauseMs: number): { moved(): number; byGroup(): Map<number, number>; stop(): Promise<number> } {
	let running = true;
	let moved = 0;
	const byGroup = new Map<number, number>();
	const loop = (async () => {
		while (running) {
			for (const group of await moveLeaderships().catch(() => [])) {
				moved++;
				byGroup.set(group, (byGroup.get(group) ?? 0) + 1);
			}
			await sleep(pauseMs);
		}
	})();
	return {
		moved: () => moved,
		byGroup: () => new Map(byGroup),
		stop: async () => {
			running = false;
			await loop;
			return moved;
		},
	};
}

/** The groups (of `E2E_GROUPS`) that did not move between two `byGroup` counts. */
function unmoved(from: Map<number, number>, to: Map<number, number>): number[] {
	const groups = Number(process.env.E2E_GROUPS ?? 4);
	return Array.from({ length: groups }, (_, g) => g).filter((g) => (to.get(g) ?? 0) <= (from.get(g) ?? 0));
}

/** Waits until the loop has made `n` more moves than `from`. */
async function movedSince(loop: { moved(): number }, from: number, n: number): Promise<void> {
	for (let i = 0; loop.moved() - from < n; i++) {
		if (i > 600) throw new Error(`${loop.moved() - from} leadership moves in a minute, not ${n}`);
		await sleep(100);
	}
}

/**
 * What a request does, for counting: an append is a claim (producer sequence 0) or a commit, a
 * request to a snapshot or to retention is named so, any other by its method (a `HEAD`, a catch-up
 * read, a create).
 */
function operation(method: string | undefined, path: string | undefined, seq: string | string[] | undefined): string {
	if (method === "POST") return seq === "0" ? "claim" : "commit";
	if (path?.includes("/snapshot/") === true) return `${method} snapshot`;
	if (path?.includes("/retention/") === true) return "retention";
	return method ?? "?";
}

/** An HTTP proxy in front of the gateway that counts the redirects and `503`s it passes on, by operation. */
class RedirectCounter {
	private readonly server: Server;
	private readonly seen = new Map<string, number>();
	readonly url: string;

	private constructor(server: Server, url: string) {
		this.server = server;
		this.url = url;
	}

	static async start(target: string): Promise<RedirectCounter> {
		const t = new URL(target);
		let counter: RedirectCounter | undefined;
		const server = createServer((req, res) => {
			const up = request({ host: t.hostname, port: t.port, method: req.method, path: req.url, headers: req.headers }, (answer) => {
				const status = answer.statusCode ?? 502;
				if (status === 307 || status === 503) {
					const kind = `${status} ${operation(req.method, req.url, req.headers["producer-seq"])}`;
					counter?.seen.set(kind, (counter.seen.get(kind) ?? 0) + 1);
				}
				res.writeHead(status, answer.headers);
				answer.pipe(res);
			});
			up.on("error", () => res.destroy());
			req.pipe(up);
		});
		const port = await new Promise<number>((res) => server.listen(0, "127.0.0.1", () => res((server.address() as AddressInfo).port)));
		counter = new RedirectCounter(server, `http://127.0.0.1:${port}`);
		return counter;
	}

	/** Answers with `status` passed on, to requests of any kind (see `operation`). */
	count(status: number): number {
		let n = 0;
		for (const [kind, seen] of this.seen) if (kind.startsWith(`${status} `)) n += seen;
		return n;
	}

	summary(): string {
		return JSON.stringify(Object.fromEntries(this.seen));
	}

	async close(): Promise<void> {
		this.server.closeAllConnections();
		await new Promise<void>((r) => this.server.close(() => r()));
	}
}

beforeAll(async () => {
	if (nodes.length !== 3) return;
	dir = mkdtempSync(join(tmpdir(), "sqlite-ursula-redirects-"));
	const unresolvable = nodes.map((node) => {
		const url = new URL(node.url);
		url.hostname = "localhost";
		return url.origin;
	});
	gateway = await AuthGateway.start(unresolvable, dir);
	proxy = await RedirectCounter.start(gateway.url);
});

afterAll(async () => {
	// A test that timed out leaves its loop running: it must not move leaderships under the next file.
	await moving?.stop();
	gateway?.close();
	await proxy?.close();
	if (dir !== undefined) rmSync(dir, { recursive: true, force: true });
});

const ROWS = "@query:SELECT k, hex(substr(v, 1, 16)) AS head FROM t ORDER BY k";

/** The child's last `done` line, once every step went through and it exited cleanly. */
async function finished(child: Child, ms: number): Promise<{ done: ChildLine; rows: unknown }> {
	const done = await child.waitFor((l) => l.done === true, ms);
	expect(child.lines.filter((l) => l.ok === false), child.stderr()).toEqual([]);
	expect(done, child.stderr()).toMatchObject({ poisoned: false, fenced: false });
	expect((await child.exited).code, child.stderr()).toBe(0);
	return { done, rows: child.lines.find((l) => l.rows !== undefined)?.rows };
}

it.runIf(nodes.length === 3)(
	"attach, commits and snapshot read-backs go through a gateway that finds moving leaders itself, and no redirect reaches the client",
	async () => {
		const counter = proxy as RedirectCounter;
		const url = counter.url + streamPath();
		const token = join(dir as string, "token");
		writeFileSync(token, `${(gateway as AuthGateway).mint()}\n`);
		// A snapshot whenever the log outgrows the database (about 80 KiB): every few commits, which
		// are paced so that the snapshot thread keeps up.
		const env = { URSULA_VFS_TOKEN_FILE: token, URSULA_VFS_SNAPSHOT_MIN_BYTES: "65536", CHILD_EXIT: "1" };
		// Each pass moves every group once (one of them is the stream's) and takes a few
		// milliseconds: with a 250 ms pause between passes, every group's leadership moves about four
		// times a second while the owner attaches, commits and reads its snapshots back.
		const loop = moveLeadershipsUntilStopped(250);
		moving = loop;
		let moved = 0;
		let snapshots = 0;
		try {
			await movedSince(loop, 0, 1);
			const steps = ["CREATE TABLE t(k INTEGER PRIMARY KEY, v BLOB)"];
			for (let i = 0; i < 40; i++) steps.push(`INSERT OR REPLACE INTO t VALUES (${i % 4}, randomblob(16384))`, "@sleep:100");
			const lastCommit = steps.length - 2;
			// Under these moves one snapshot can take seconds (its requests meet a 503 with Retry-After: 1
			// from a group between leaders): the owner waits for two before it exits.
			steps.push("@snapshots:2", "@sleep:2000", ROWS);
			const file = freshFile();
			const started = loop.byGroup();
			const child = runChild(file, url, steps, env);
			await child.waitFor((l) => l.step === lastCommit && l.ok !== undefined, 180_000);
			const committed = loop.byGroup();
			const owner = await finished(child, 180_000);
			// Every group's leadership, the stream's included, moved while the owner attached and
			// committed, and while it waited for its snapshots, published and read back.
			expect(unmoved(started, committed), "groups that did not move during the commits").toEqual([]);
			expect(unmoved(committed, loop.byGroup()), "groups that did not move during the snapshots").toEqual([]);
			snapshots = owner.done.snapshots ?? 0;
			expect(snapshots).toBeGreaterThanOrEqual(2);
			expect(owner.rows).toHaveLength(4);

			// A second host installs the latest snapshot and replays the tail, then claims the stream.
			const host = await finished(runChild(freshFile(), url, [ROWS], env), 90_000);
			expect(host.done.installed).not.toBe("-1");
			expect(host.rows).toEqual(owner.rows);

			// A read-only host (it claims nothing) installs the snapshot too.
			const reader = await finished(runChild(freshFile(), url, [ROWS], { ...env, CHILD_ATTACH_MODE: "read_only" }), 90_000);
			expect(reader.done.installed).not.toBe("-1");
			expect(reader.rows).toEqual(owner.rows);

			// The owner attaches its own files again (they hold data: a stream hidden behind a 404
			// would be refused as missing), takes the stream back and commits.
			const again = await finished(runChild(file, url, ["INSERT INTO t VALUES (4, x'00')", ROWS], env), 90_000);
			expect(again.done.local).not.toBe("-1");
			expect(again.rows).toEqual([...(owner.rows as object[]), { k: 4, head: "00" }]);
		} finally {
			moved = await loop.stop();
		}
		const metrics = (await (await fetch(`${(gateway as AuthGateway).url}/__ursula/gateway/metrics`)).json()) as {
			leader_redirects: number;
			leader_searches: number;
			leader_not_found: number;
		};
		console.log(`leadership moves: ${moved}; passed on: ${counter.summary()}; gateway: ${JSON.stringify(metrics)}; snapshots: ${snapshots}`);
		expect(moved).toBeGreaterThanOrEqual(4);
		// The gateway looked for leaders among its upstreams (every redirect it followed was a
		// search hop here), and passed no redirect on.
		expect(metrics.leader_searches).toBeGreaterThan(0);
		expect(counter.count(307)).toBe(0);
	},
	600_000,
);
