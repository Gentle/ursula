// The extension over TLS, through an `ursula gateway` that checks bearer tokens (RFC 9068 access
// tokens from a JWKS this test serves), in front of the stack under test. An HTTPS proxy with a
// certificate from a throwaway CA terminates TLS in front of the gateway. Each owner runs in a
// child process, because the CA file and the token file are read from its environment.
import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { type Server, request as httpRequest } from "node:http";
import { createServer as createHttpsServer } from "node:https";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, expect, inject, it } from "vitest";
import { attach } from "../src/index.ts";
import { freshFile } from "./helpers.ts";
import { AuthGateway, type Child, loopbackCertificate, openPlain, runChild, streamPath, ursulaUrl } from "./kit.ts";

const dir = mkdtempSync(join(tmpdir(), "sqlite-ursula-auth-"));
let secureUrl = "";
let caFile = "";
const servers: Server[] = [];
let gateway: AuthGateway | undefined;

const listen = (server: Server): Promise<number> =>
	new Promise((res) => server.listen(0, "127.0.0.1", () => res((server.address() as AddressInfo).port)));

/** An access token the gateway accepts, valid for an hour. */
const mint = (): string => (gateway as AuthGateway).mint();

beforeAll(async () => {
	gateway = await AuthGateway.start(inject("ursulaNodes").map((node) => node.url), dir);
	const gatewayPort = new URL(gateway.url).port;
	const { key, cert, ca } = loopbackCertificate(dir);
	caFile = ca;
	const proxy = createHttpsServer({ key, cert }, (req, res) => {
		const upstream = httpRequest({ host: "127.0.0.1", port: gatewayPort, method: req.method, path: req.url, headers: req.headers }, (answer) => {
			res.writeHead(answer.statusCode ?? 502, answer.headers);
			answer.pipe(res);
		});
		upstream.on("error", (e) => res.writeHead(502).end(String(e)));
		req.pipe(upstream);
	});
	servers.push(proxy);
	secureUrl = `https://127.0.0.1:${await listen(proxy)}`;
});

afterAll(() => {
	gateway?.close();
	for (const server of servers) server.close();
	rmSync(dir, { recursive: true, force: true });
});

const rows = (file: string): string[] => {
	const db = openPlain(file);
	try {
		return (db.prepare("SELECT x FROM t ORDER BY rowid").all() as { x: string }[]).map((r) => r.x);
	} finally {
		db.close();
	}
};

const tokenFile = (token: string): string => {
	const file = join(dir, `token-${randomUUID()}`);
	writeFileSync(file, `${token}\n`);
	return file;
};

const done = async (child: Child): Promise<void> => {
	const { code } = await child.exited;
	expect(code, child.stderr()).toBe(0);
};

it("over TLS, through a gateway that checks tokens, an owner with a token file commits", async () => {
	const path = streamPath();
	const env = { URSULA_VFS_TOKEN_FILE: tokenFile(mint()), URSULA_VFS_CA_FILE: caFile, CHILD_EXIT: "1" };
	const child = runChild(freshFile(), secureUrl + path, ["CREATE TABLE t(x TEXT)", "INSERT INTO t VALUES ('r1')", "INSERT INTO t VALUES ('r2')"], env);
	expect(await child.waitFor((l) => l.done === true)).toMatchObject({ poisoned: false });
	await done(child);
	const copy = freshFile();
	attach(copy, ursulaUrl() + path);
	expect(rows(copy)).toEqual(["r1", "r2"]);
});

// A refused token (401) is no fence: the commit is sent again, with the token file read again,
// until the file holds a token the gateway accepts, and the owner is not poisoned.
it("a token refused mid-run is read again until the file holds a good one", async () => {
	const path = streamPath();
	const token = tokenFile(mint());
	const env = { URSULA_VFS_TOKEN_FILE: token, URSULA_VFS_CA_FILE: caFile, URSULA_VFS_RETRY_MS: "20000", CHILD_EXIT: "1" };
	const child = runChild(freshFile(), secureUrl + path, ["CREATE TABLE t(x TEXT)", "INSERT INTO t VALUES ('r1')", "@sleep:500", "INSERT INTO t VALUES ('r2')"], env);
	await child.waitFor((l) => l.step === 2 && l.phase === "start");
	writeFileSync(token, "not-a-token\n");
	await child.waitFor((l) => l.step === 3 && l.phase === "start");
	await new Promise((r) => setTimeout(r, 1500));
	writeFileSync(token, `${mint()}\n`);
	const last = await child.waitFor((l) => l.done === true);
	expect(last).toMatchObject({ poisoned: false });
	expect(last.attempts?.at(-1)).toBeGreaterThan(1);
	await done(child);
	const copy = freshFile();
	attach(copy, ursulaUrl() + path);
	expect(rows(copy)).toEqual(["r1", "r2"]);
});

it("without a token the gateway hides the stream, and an untrusted CA fails the handshake", async () => {
	const path = streamPath();
	const anonymous = runChild(freshFile(), secureUrl + path, ["CREATE TABLE t(x TEXT)"], { URSULA_VFS_CA_FILE: caFile, CHILD_EXIT: "1" });
	expect((await anonymous.exited).code).not.toBe(0);
	expect(anonymous.stderr()).toMatch(/create https:\/\/127\.0\.0\.1:\d+\/sqlite-e2e\/\S+: 404/);
	expect((await fetch(ursulaUrl() + path, { method: "HEAD" })).status).toBe(404);
	const untrusting = runChild(freshFile(), secureUrl + path, ["CREATE TABLE t(x TEXT)"], { URSULA_VFS_TOKEN_FILE: tokenFile(mint()), CHILD_EXIT: "1" });
	expect((await untrusting.exited).code).not.toBe(0);
	expect(untrusting.stderr()).toMatch(/certificate|issuer/i);
});
