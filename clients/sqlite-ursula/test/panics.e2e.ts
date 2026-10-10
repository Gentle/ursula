// A panic inside the extension stops at its C entry point: the call fails with an SQLite error,
// and the host process goes on instead of aborting, which unwinding out of an `extern "C"`
// function would do. `URSULA_VFS_PANIC_IN=<entry>` makes one entry point panic on purpose. Each
// case runs in its own Node process, since the extension reads the setting once per process.
import { spawnSync } from "node:child_process";
import { expect, it } from "vitest";
import { freshFile } from "./helpers.ts";
import { streamPath, ursulaUrl, vfsPath } from "./kit.ts";

/** Runs `script` in a new Node process with the extension's path in `EXT`, and the given env. */
const run = (script: string, env: Record<string, string>) => {
	const r = spawnSync(process.execPath, ["--input-type=module", "-e", script], {
		env: { ...process.env, EXT: vfsPath(), ...env },
		encoding: "utf8",
	});
	return { signal: r.signal, code: r.status, stderr: r.stderr, out: JSON.parse(r.stdout.trim().split("\n").at(-1) ?? "{}") };
};

it("a panic in a SQL function fails the call, and the process goes on", () => {
	const r = run(
		`import { DatabaseSync } from "node:sqlite";
		const c = new DatabaseSync(":memory:", { allowExtension: true });
		c.loadExtension(process.env.EXT);
		const out = {};
		try { c.prepare("SELECT ursula_status(?) AS s").get("/nonexistent"); out.call = "returned"; } catch (e) { out.call = String(e); }
		out.after = c.prepare("SELECT 41 + 1 AS n").get().n;
		console.log(JSON.stringify(out));`,
		{ URSULA_VFS_PANIC_IN: "ursula_status" },
	);
	expect(r.signal, r.stderr).toBeNull();
	expect(r.code, r.stderr).toBe(0);
	expect(r.out.call).toMatch(/ursula_status: ursula_status panicked inside the extension/);
	expect(r.out.after).toBe(42);
	expect(r.stderr).toMatch(/event=panic entry=ursula_status message="URSULA_VFS_PANIC_IN=ursula_status"/);
});

it("a panic in a file method fails it with an I/O error, and the process goes on", () => {
	const r = run(
		`import { DatabaseSync } from "node:sqlite";
		const c = new DatabaseSync(":memory:", { allowExtension: true });
		c.loadExtension(process.env.EXT);
		const out = {};
		try {
			c.prepare("SELECT ursula_attach(?, ?) AS n").get(process.env.FILE, process.env.URL);
			const db = new DatabaseSync(process.env.FILE);
			db.exec("PRAGMA journal_mode=WAL");
			db.exec("CREATE TABLE t(x TEXT)");
			db.exec("INSERT INTO t VALUES ('a1')");
			out.write = "committed";
		} catch (e) {
			out.write = String(e);
			out.errcode = e.errcode;
		}
		out.after = c.prepare("SELECT 41 + 1 AS n").get().n;
		console.log(JSON.stringify(out));`,
		{ URSULA_VFS_PANIC_IN: "x_write", FILE: freshFile(), URL: ursulaUrl() + streamPath() },
	);
	expect(r.signal, r.stderr).toBeNull();
	expect(r.code, r.stderr).toBe(0);
	// Wherever the first write happens (attach or the first commit), it fails with an SQLite error.
	expect(r.out.write).not.toBe("committed");
	expect(r.out.after).toBe(42);
	expect(r.stderr).toMatch(/event=panic entry=x_write message="URSULA_VFS_PANIC_IN=x_write"/);
});
