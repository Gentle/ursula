// A commit that cannot reach its endpoint: every attempt fails before any byte of it goes out (here
// the endpoint refuses connections). Nothing of it was sent, so nothing was applied: it fails alone
// as `unreachable`, the file is not poisoned, and the next commit succeeds once the endpoint is back.
// Before 0.7.4 such a commit ended as an unknown outcome and poisoned the file. The endpoint is a TCP
// forwarder to the server that the test turns off (connections refused) and on again.
import { writeFileSync } from "node:fs";
import { type AddressInfo, connect, createServer, type Server, type Socket } from "node:net";
import { setTimeout as sleep } from "node:timers/promises";
import { afterAll, beforeAll, expect, it } from "vitest";
import { attach, status } from "../src/index.ts";
import { freshFile } from "./helpers.ts";
import { type Child, type ChildLine, openPlain, runChild, streamPath, ursulaUrl } from "./kit.ts";

/** SQLITE_IOERR_AUTH: a commit refused, or never sent, so that it applied nothing. */
const COMMIT_REFUSED = 7178;

/** A TCP forwarder to `target` on a fixed loopback port, which refuses connections while off. */
class Switch {
	private server: Server | undefined;
	private readonly sockets = new Set<Socket>();
	private port = 0;
	private readonly target: URL;

	private constructor(target: string) {
		this.target = new URL(target);
	}

	static async start(target: string): Promise<Switch> {
		const s = new Switch(target);
		await s.on();
		return s;
	}

	get url(): string {
		return `http://127.0.0.1:${this.port}`;
	}

	/** Accepts connections again, on the same port. */
	async on(): Promise<void> {
		const server = createServer((client) => {
			const upstream = connect(Number(this.target.port), this.target.hostname);
			for (const socket of [client, upstream]) {
				this.sockets.add(socket);
				socket.on("close", () => this.sockets.delete(socket)).on("error", () => {});
			}
			client.pipe(upstream).pipe(client);
		});
		await new Promise<void>((res) => server.listen(this.port, "127.0.0.1", () => res()));
		this.port = (server.address() as AddressInfo).port;
		this.server = server;
	}

	/** Stops listening, so that connections are refused, and closes the open ones. */
	async off(): Promise<void> {
		const server = this.server;
		this.server = undefined;
		await new Promise<void>((res) => {
			if (server === undefined) return res();
			server.close(() => res());
			for (const socket of this.sockets) socket.destroy();
		});
	}
}

let endpoint: Switch;
beforeAll(async () => {
	endpoint = await Switch.start(ursulaUrl());
});
afterAll(async () => {
	await endpoint.off();
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

it("a commit whose every attempt is refused a connection fails alone as unreachable, and the next one succeeds once the endpoint is back", async () => {
	const path = streamPath();
	const file = freshFile();
	const [down, up] = [`${file}.down`, `${file}.up`];
	const steps = [
		"CREATE TABLE t(x TEXT)",
		"INSERT INTO t VALUES ('a1')",
		"@status",
		`@wait:${down}`,
		"INSERT INTO t VALUES ('a2')",
		"@status",
		`@wait:${up}`,
		"INSERT INTO t VALUES ('a3')",
	];
	const child = runChild(file, endpoint.url + path, steps, { CHILD_EXIT: "1", URSULA_VFS_RETRY_MS: "1500" });
	await child.waitFor((l) => l.step === 3 && l.phase === "start");
	await endpoint.off();
	// The client's pooled connection is closed before the next commit, which then opens a new one.
	await sleep(200);
	writeFileSync(down, "");
	await child.waitFor((l) => l.step === 6 && l.phase === "start");
	await endpoint.on();
	writeFileSync(up, "");
	const done = await child.waitFor((l) => l.done === true);
	expect((await child.exited).code, child.stderr()).toBe(0);

	const before = step(child, 2)?.status;
	expect(step(child, 4)).toMatchObject({ ok: false, errcode: COMMIT_REFUSED });
	const unreachable = step(child, 5)?.status;
	expect(unreachable).toMatchObject({ poisoned: false, fenced: false, writes_refused: true, refused_kind: "unreachable", payment_required: false });
	expect(unreachable?.refused_reason).toMatch(/refused/i);
	// Nothing of the commit moved the owner: same epoch, same offset.
	expect([unreachable?.epoch, unreachable?.offset]).toEqual([before?.epoch, before?.offset]);
	expect(step(child, 7)?.ok).toBe(true);
	expect(done).toMatchObject({ poisoned: false, fenced: false, writes_refused: false, refused_kind: null, epoch: before?.epoch });
	const log = child.stderr();
	expect(log).toMatch(/event=writes_refused file=\S+ stream=\S+ during=commit kind=unreachable reason=/);
	expect(log).toMatch(/event=writes_resumed file=\S+ stream=\S+$/m);
	expect(log).not.toMatch(/event=poisoned/);

	// The local files still match their sidecar, and the stream holds the accepted commits alone.
	attach(file, ursulaUrl() + path);
	expect(status(file)).toMatchObject({ local: done.offset, installed: "-1" });
	expect(xs(file)).toEqual(["a1", "a3"]);
	const copy = freshFile();
	attach(copy, ursulaUrl() + path);
	expect(xs(copy)).toEqual(["a1", "a3"]);
});
