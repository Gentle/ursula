// Attach through a read origin (immutable reads, docs/architecture/immutable-reads.md §4). A caching
// HTTPS proxy on loopback stands in for a CDN in front of the server: it forwards every request,
// stores each 200 marked `Stream-Immutable: true` under its full URL, and serves it from there
// afterwards. Fresh hosts attach a database with a published snapshot and more than two 4 MiB
// chunks of log after it, with `URSULA_VFS_READ_ORIGIN` naming the proxy (https with a throwaway
// CA: the setting takes https origins only). They read the snapshot and the history up to the tail
// there, the second from the proxy's cache, and everything else at the stream's own origin. A
// proxy that answers one chunk wrong (another incarnation, a 503) makes the attach read the rest at
// the stream's origin, with the same rows, and is not taken for a recreated stream.
import { mkdtempSync, rmSync } from "node:fs";
import type { IncomingHttpHeaders, OutgoingHttpHeaders } from "node:http";
import { request } from "node:http";
import { createServer, type Server } from "node:https";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, beforeEach, expect, it } from "vitest";
import { freshFile } from "./helpers.ts";
import { type Child, type ChildLine, loopbackCertificate, runChild, streamPath, ursulaUrl } from "./kit.ts";

/** What the proxy saw: a snapshot read, an aligned history read, or anything else. */
type Kind = "snapshot" | "aligned" | "other";

/** How the proxy answers the aligned read it is told to get wrong. */
type Wrong = "incarnation" | "unavailable";

interface Seen {
	readonly method: string;
	readonly url: string;
	readonly kind: Kind;
	readonly cached: boolean;
}

interface Stored {
	readonly headers: IncomingHttpHeaders;
	readonly body: Buffer;
}

const kindOf = (method: string, url: string): Kind => {
	const { pathname, searchParams } = new URL(url, "https://read-origin");
	if (method !== "GET" || !searchParams.has("incarnation")) return "other";
	if (/\/snapshot\/[^/]+$/.test(pathname)) return "snapshot";
	return searchParams.get("aligned") === "true" ? "aligned" : "other";
};

/** Hop-by-hop headers, which a stored answer does not replay. */
const HOP = new Set(["connection", "keep-alive", "transfer-encoding"]);

/** A caching HTTPS proxy in front of `target` (see the file comment). */
class CachingProxy {
	readonly seen: Seen[] = [];
	private readonly cache = new Map<string, Stored>();
	/** Answers the `index`th aligned read (from 0) since the last `reset` wrong, without storing it. */
	wrong: { index: number; how: Wrong } | undefined;
	private aligned = 0;
	url = "";
	private server: Server | undefined;

	async start(target: string, tls: { key: Buffer; cert: Buffer }): Promise<void> {
		const upstream = new URL(target);
		this.server = createServer(tls, (req, res) => {
			const method = req.method ?? "GET";
			const url = req.url ?? "/";
			const kind = kindOf(method, url);
			const answer = (status: number, headers: IncomingHttpHeaders, body: Buffer): void => {
				const sent: OutgoingHttpHeaders = {};
				for (const [name, value] of Object.entries(headers)) if (!HOP.has(name) && value !== undefined) sent[name] = value;
				sent["content-length"] = body.length;
				res.writeHead(status, sent).end(method === "HEAD" ? undefined : body);
			};
			const index = kind === "aligned" ? this.aligned++ : -1;
			const wrong = this.wrong !== undefined && this.wrong.index === index ? this.wrong.how : undefined;
			if (wrong === "unavailable") {
				req.resume();
				this.seen.push({ method, url, kind, cached: false });
				answer(503, { "retry-after": "1" }, Buffer.from("unavailable"));
				return;
			}
			const stored = this.cache.get(url);
			if (stored !== undefined && wrong === undefined) {
				req.resume();
				this.seen.push({ method, url, kind, cached: true });
				answer(200, stored.headers, stored.body);
				return;
			}
			const up = request({ host: upstream.hostname, port: upstream.port, method, path: url, headers: { ...req.headers, host: upstream.host } }, (r) => {
				const chunks: Buffer[] = [];
				r.on("data", (c: Buffer) => chunks.push(c));
				r.on("end", () => {
					const body = Buffer.concat(chunks);
					this.seen.push({ method, url, kind, cached: false });
					if (wrong === "incarnation") {
						answer(r.statusCode ?? 502, { ...r.headers, "stream-incarnation": `${Number(r.headers["stream-incarnation"] ?? 0) + 1}` }, body);
						return;
					}
					if (r.statusCode === 200 && r.headers["stream-immutable"] === "true") this.cache.set(url, { headers: r.headers, body });
					answer(r.statusCode ?? 502, r.headers, body);
				});
			});
			up.on("error", (e) => res.writeHead(502).end(String(e)));
			req.pipe(up);
		});
		const port = await new Promise<number>((res) => this.server?.listen(0, "127.0.0.1", () => res((this.server?.address() as AddressInfo).port)));
		this.url = `https://127.0.0.1:${port}`;
	}

	/** Forgets what it saw (not what it stored) and the wrong answer. */
	reset(): void {
		this.seen.length = 0;
		this.aligned = 0;
		this.wrong = undefined;
	}

	stored(): number {
		return this.cache.size;
	}

	async close(): Promise<void> {
		this.server?.closeAllConnections();
		await new Promise<void>((r) => (this.server === undefined ? r() : this.server.close(() => r())));
	}
}

const dir = mkdtempSync(join(tmpdir(), "sqlite-ursula-read-origin-"));
const proxy = new CachingProxy();
let caFile = "";

beforeAll(async () => {
	const { key, cert, ca } = loopbackCertificate(dir);
	caFile = ca;
	await proxy.start(ursulaUrl(), { key, cert });
});

afterAll(async () => {
	await proxy.close();
	rmSync(dir, { recursive: true, force: true });
});

beforeEach(() => proxy.reset());

const MiB = 1 << 20;
/** No snapshot from the hosts that write the bulk or attach: the one the first writer published stays the latest. */
const NO_SNAPSHOT = "1000000000000";
/** Every row of both tables, by its length and its first and last bytes, and the integrity check. */
const ROWS = "@query:SELECT 'a' AS t, k, length(v) AS n, hex(substr(v, 1, 16)) AS h, hex(substr(v, -16)) AS z FROM a UNION ALL SELECT 'b', k, length(v), hex(substr(v, 1, 16)), hex(substr(v, -16)) FROM b ORDER BY 1, 2";
const INTEGRITY = "@query:PRAGMA integrity_check";

/** Runs `child` to its end: its `done` line, the rows of its first `@query` step, and its log. */
async function finished(child: Child): Promise<{ done: ChildLine; rows: unknown; log: string }> {
	const done = await child.waitFor((l) => l.done === true, 60_000);
	const { code } = await child.exited;
	expect(code, child.stderr()).toBe(0);
	expect(child.lines.filter((l) => l.ok === false)).toEqual([]);
	const queries = child.lines.filter((l) => l.rows !== undefined);
	expect(queries.at(-1)?.rows).toEqual([{ integrity_check: "ok" }]);
	return { done, rows: queries.at(0)?.rows, log: child.stderr() };
}

async function head(url: string): Promise<{ tail: string; snapshot: string; incarnation: string }> {
	const r = await fetch(url, { method: "HEAD" });
	expect(r.status).toBe(200);
	const h = (name: string): string => r.headers.get(name) ?? "";
	return { tail: h("stream-next-offset"), snapshot: h("stream-snapshot-offset"), incarnation: h("stream-incarnation") };
}

/**
 * A stream whose latest snapshot is followed by `bulk` MiB of log: a first writer publishes a
 * snapshot of a small database, then a second takes the file over and writes 1 MiB rows.
 */
async function database(bulk: number): Promise<{ url: string; rows: unknown; snapshot: string; incarnation: string }> {
	const url = ursulaUrl() + streamPath();
	const file = freshFile();
	// One row rewritten: the log outgrows the small database, so a snapshot is due.
	const small = ["CREATE TABLE a(k INTEGER PRIMARY KEY, v BLOB)", "CREATE TABLE b(k INTEGER PRIMARY KEY, v BLOB)"];
	for (let i = 0; i < 20; i++) small.push(`INSERT OR REPLACE INTO a VALUES (${i % 2}, randomblob(800))`);
	await finished(runChild(file, url, [...small, "@snapshots:1", INTEGRITY], { URSULA_VFS_SNAPSHOT_MIN_BYTES: "1", CHILD_EXIT: "1" }));
	const before = await head(url);
	expect(before.snapshot).not.toBe("");
	const rows = Array.from({ length: bulk }, (_, i) => `INSERT INTO b VALUES (${i}, randomblob(${MiB}))`);
	const writer = await finished(runChild(file, url, [...rows, ROWS, INTEGRITY], { URSULA_VFS_SNAPSHOT_MIN_BYTES: NO_SNAPSHOT, CHILD_EXIT: "1" }));
	const after = await head(url);
	expect(after.snapshot).toBe(before.snapshot);
	expect(Number(after.tail) - Number(after.snapshot)).toBeGreaterThan(bulk * MiB);
	return { url, rows: writer.rows, snapshot: after.snapshot, incarnation: after.incarnation };
}

/** A fresh host attaches `url` (and claims it) with the proxy as its read origin. */
const attachThroughProxy = (url: string): Promise<{ done: ChildLine; rows: unknown; log: string }> =>
	finished(
		runChild(freshFile(), url, [ROWS, INTEGRITY], {
			URSULA_VFS_READ_ORIGIN: proxy.url,
			URSULA_VFS_CA_FILE: caFile,
			URSULA_VFS_SNAPSHOT_MIN_BYTES: NO_SNAPSHOT,
			CHILD_EXIT: "1",
		}),
	);

const count = (kind: Kind, cached?: boolean): number => proxy.seen.filter((s) => s.kind === kind && (cached === undefined || s.cached === cached)).length;

/** Only the snapshot `GET` and aligned history reads of incarnation `incarnation` reached the proxy, never with `consistency`. */
function onlyImmutableReads(incarnation: string): void {
	expect(proxy.seen.filter((s) => s.kind === "other")).toEqual([]);
	for (const { url } of proxy.seen) {
		const query = new URL(url, "https://read-origin").searchParams;
		expect(query.get("incarnation"), url).toBe(incarnation);
		expect(query.has("consistency"), url).toBe(false);
	}
}

/** Attach neither rebuilt nor took the stream for a recreated one. */
function notRebuilt(log: string): void {
	expect(log).not.toMatch(/event=(attach_rebuilding|local_files_discarded|setting_adjusted|read_origin_ignored)/);
	expect(log).not.toMatch(/recreated/);
}

it("fresh hosts read the snapshot and the history behind the tail at the read origin, the second from its cache", async () => {
	const db = await database(12);
	const storedBefore = proxy.stored();

	const first = await attachThroughProxy(db.url);
	expect(first.rows).toEqual(db.rows);
	expect(first.done).toMatchObject({ installed: db.snapshot, poisoned: false });
	expect(first.log).not.toMatch(/event=read_origin_fallback/);
	notRebuilt(first.log);
	onlyImmutableReads(db.incarnation);
	expect(count("snapshot", false)).toBe(1);
	// From the snapshot to the first 4 MiB boundary, at least two whole chunks, and the read that
	// reaches the tail, which is not immutable: every read but that one was stored.
	const aligned = count("aligned");
	expect(aligned).toBeGreaterThanOrEqual(4);
	expect(count("aligned", true)).toBe(0);
	expect(proxy.stored() - storedBefore).toBe(1 + aligned - 1);

	// The second host's reads are the same up to the last chunk below the tail, served from the cache.
	proxy.reset();
	const second = await attachThroughProxy(db.url);
	expect(second.rows).toEqual(db.rows);
	expect(second.done).toMatchObject({ installed: db.snapshot, poisoned: false });
	expect(second.log).not.toMatch(/event=read_origin_fallback/);
	notRebuilt(second.log);
	onlyImmutableReads(db.incarnation);
	expect(count("snapshot", true)).toBe(1);
	expect(count("snapshot", false)).toBe(0);
	expect(count("aligned", true)).toBe(aligned - 1);
	expect(count("aligned", false)).toBe(1);
	expect(proxy.stored() - storedBefore).toBe(aligned);
}, 120_000);

it.each<[Wrong, RegExp]>([
	["incarnation", /reason="Stream-Incarnation Some\(\\"\d+\\"\), not the incarnation requested"/],
	["unavailable", /reason="status 503"/],
])("a chunk the read origin answers wrong (%s) is read at the stream's origin, with the rest of the attach", async (how, reason) => {
	const db = await database(10);
	proxy.wrong = { index: 1, how };
	const host = await attachThroughProxy(db.url);
	expect(host.rows).toEqual(db.rows);
	expect(host.done).toMatchObject({ installed: db.snapshot, poisoned: false });
	notRebuilt(host.log);
	const fallbacks = host.log.split("\n").filter((l) => l.includes("event=read_origin_fallback"));
	expect(fallbacks).toHaveLength(1);
	expect(fallbacks[0]).toMatch(reason);
	// Nothing after the wrong answer went to the read origin.
	onlyImmutableReads(db.incarnation);
	expect(count("snapshot")).toBe(1);
	expect(count("aligned")).toBe(2);
}, 120_000);
