// SQLite replicated through an Ursula stream by the sqlite-ursula-vfs loadable extension
// (clients/sqlite-vfs). The extension registers the "ursula" VFS as SQLite's default; after
// `attach(path, streamUrl)` every connection that opens `path` (plain node:sqlite, Pi's official node
// driver) is replicated: each commit is appended to the stream before it reaches the local WAL.
import { DatabaseSync } from "node:sqlite";
import { type SqliteExecutor, SqliteStorage } from "@earendil-works/pi-durable/storage/sqlite";
import { type NodeSqliteStorageOptions, openNodeSqliteDatabase } from "@earendil-works/pi-durable/storage/sqlite/node";
import { prebuiltExtension } from "./platform.ts";

let control: DatabaseSync | undefined;

/**
 * Loads the extension once per process and returns the control connection. The library is `path`,
 * else SQLITE_URSULA_VFS when set, else the prebuilt one for this platform
 * (`@tonbo/sqlite-ursula-<platform>`, an optional dependency).
 */
export function loadUrsulaVfs(path = process.env.SQLITE_URSULA_VFS): DatabaseSync {
	if (control !== undefined) return control;
	const library = path === undefined || path.length === 0 ? prebuiltExtension() : path;
	const db = new DatabaseSync(":memory:", { allowExtension: true });
	db.loadExtension(library);
	control = db;
	return db;
}

/** SQLITE_READONLY: a write to a file attached read-only. */
const SQLITE_READONLY = 8;
/** SQLITE_AUTH: `ursula_attach` refused with 402 in front of the stream. */
const SQLITE_AUTH = 23;
/** SQLITE_IOERR_AUTH: a commit, or the claim of an `ursula_attach`, refused before it reached the stream (an attach refused with 402 gets SQLITE_AUTH). */
const SQLITE_IOERR_AUTH = 7178;

/** The SQLite result code of an error node:sqlite threw (`errcode`), if it is one. */
function errcode(error: unknown): number | undefined {
	const code = (error as { errcode?: unknown } | null | undefined)?.errcode;
	return typeof code === "number" ? code : undefined;
}

export interface AttachOptions {
	/**
	 * Attach without claiming the stream. Nothing is ever appended (no claim, commit or snapshot), so
	 * no owner is fenced, and a layer in front of the stream that refuses writes does not stop it.
	 * Every write to the file fails with SQLITE_READONLY ("attempt to write a readonly database").
	 * A failed COMMIT leaves no transaction open, but a statement of an explicit transaction whose
	 * changes outgrow the page cache fails before COMMIT, and SQLite leaves that transaction open:
	 * run ROLLBACK after such an error (until then even reads on the connection fail).
	 * The file holds the stream as of the attach: attach again to catch up. A missing stream is an
	 * error, never created.
	 */
	readonly readOnly?: boolean;
}

/**
 * Catches `file` up from the stream (installing the stream's latest snapshot first when the file is
 * missing or behind it), claims the stream for this process (fencing every earlier owner)
 * and attaches the file. No connection to `file` may be open; the process keeps a host lock on the file
 * for its lifetime. Returns the stream offset the file reflects (a {@link StreamOffset}). A file that has a sidecar (`<file>-ursula`,
 * written by its first attach) opens in this process only while attached here: not before an attach
 * of it succeeds, nor after one fails (unless refused up front for open connections, another thread
 * attaching it, or another process holding it, which leaves the file as it was).
 *
 * With `{ readOnly: true }` it claims nothing (see {@link AttachOptions.readOnly}). An attach that a
 * layer in front of the stream refuses with `402 Payment Required` throws
 * {@link UrsulaPaymentRequiredError}, and one whose claim is refused before it reaches the stream in
 * another way (see {@link WriteRefusedKind}) throws {@link UrsulaWriteRefusedError}. Either way nothing
 * was claimed: attach again later, and a read-only attach can still read the database meanwhile.
 */
export function attach(file: string, streamUrl: string, options: AttachOptions = {}): StreamOffset {
	const sql = options.readOnly === true ? "SELECT ursula_attach(?, ?, 'read_only') AS n" : "SELECT ursula_attach(?, ?) AS n";
	try {
		const row = loadUrsulaVfs().prepare(sql).get(file, streamUrl) as { n: string };
		return row.n;
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		if (errcode(error) === SQLITE_AUTH) throw new UrsulaPaymentRequiredError(message, null, error);
		if (errcode(error) === SQLITE_IOERR_AUTH) throw new UrsulaWriteRefusedError(message, null, null, error);
		throw error;
	}
}

/**
 * Sets the bearer token every request of the extension carries from now on, process-wide (for an
 * endpoint behind `ursula gateway --auth-*`). `null` goes back to the file named by
 * `URSULA_VFS_TOKEN_FILE`, which is read again whenever it changes and after a refused token (401).
 *
 * Call it with a fresh token before the current one expires. node:sqlite is synchronous, so a
 * commit whose token is refused retries on the thread that would call `setToken`: it waits out the
 * retry budget (`URSULA_VFS_RETRY_MS`, 30 s by default) and fails alone, as a write refused before
 * it reached the stream (`refused_kind` `unauthorized`): set a fresh token and commit again. For
 * tokens that expire on their own schedule, prefer the token file.
 */
export function setToken(token: string | null): void {
	loadUrsulaVfs().prepare("SELECT ursula_set_token(?)").get(token ?? "");
}

/**
 * A stream offset as the server wrote it (`Stream-Next-Offset`): opaque. Compare two offsets of one
 * stream only as strings (lexicographically, e.g. `a < b`); never parse or do arithmetic on them.
 * `"-1"` is the beginning of the stream, and stands for "none" where an offset may be absent.
 */
export type StreamOffset = string;

export interface AttachStatus {
	/** Stream offset after the last acknowledged commit. */
	readonly offset: StreamOffset;
	/** This owner's producer epoch; read-only, the highest epoch claimed up to `offset`. */
	readonly epoch: number;
	/** Every later commit fails until the file is re-attached. */
	readonly poisoned: boolean;
	/** Poisoned, and a newer owner claimed the stream, the stream was deleted and recreated, or another writer appended with a higher `Stream-Seq` (the first failure or a later one). Never reset until the file is attached again. */
	readonly fenced: boolean;
	/** Why it is poisoned: the first failure, which a later one never replaces. */
	readonly reason: string | null;
	/** Attached with `{ readOnly: true }`: nothing is ever appended, and every write fails with SQLITE_READONLY. */
	readonly read_only: boolean;
	/** A layer in front of the stream refused the latest write with `402 Payment Required` (a commit failed with {@link UrsulaPaymentRequiredError}, or a snapshot waits). It does not poison the file, and the next acknowledged commit clears it. Always `false` while `poisoned`, since a poisoned file needs a re-attach whatever the layer does. */
	readonly payment_required: boolean;
	/** The layer's explanation: the `402` response's body (its first 256 bytes, then `...` when it is longer), or the token of `x-ursula-not-applied` when the body is empty (`null` when it sent neither, or without a refusal). */
	readonly payment_reason: string | null;
	/** The latest write was refused before it reached the stream, so nothing of it was applied (a commit failed with {@link UrsulaWriteRefusedError}, or a snapshot waits). It does not poison the file, and the next acknowledged commit clears it. Always `false` while `poisoned`. A `402` shows here and in `payment_required`. Absent with an extension before 0.7.3 (an older library named by `SQLITE_URSULA_VFS`), like the two fields below. */
	readonly writes_refused?: boolean;
	/** What refused the latest write (see {@link WriteRefusedKind}); `null` without a refusal. Absent with an extension before 0.7.3. */
	readonly refused_kind?: WriteRefusedKind | null;
	/** Why: the refusing layer's response body (its first 256 bytes, then `...` when it is longer), or the token of `x-ursula-not-applied` (such as `RaftLogPressure`) when the body is empty, and always for `not_applied`; `null` when it sent neither, or without a refusal. Absent with an extension before 0.7.3. */
	readonly refused_reason?: string | null;
	/** Offset of the latest snapshot known readable (published and read back, or found at attach); `"-1"` for none. */
	readonly snapshot: StreamOffset;
	/** Retention this owner advanced the stream to (`"-1"`: none yet). */
	readonly retained: StreamOffset;
	/** Stream offset of the local state attach started from; `"-1"` when it rebuilt the file from nothing (a fresh host, or local files it could not trust and discarded: another boot, a replaced file, a sidecar ahead of its WAL, another incarnation of the stream: deleted and recreated). */
	readonly local: StreamOffset;
	/** Offset of the snapshot attach installed (`"-1"`: none). */
	readonly installed: StreamOffset;
	/** How long the attach took, in milliseconds. */
	readonly attach_ms: number;
	/** Commits acknowledged since the attach. */
	readonly commits: number;
	/** Append attempts beyond the first (unknown outcomes, 429, 503, a refused token), over those commits. */
	readonly append_retries: number;
	/** Bytes of log since the latest snapshot. A rebuild replays them, so they should stay near `snapshot_due_bytes`. */
	readonly log_bytes: number;
	/** The log a snapshot is taken at: the database's size, or `URSULA_VFS_SNAPSHOT_MIN_BYTES` if larger. */
	readonly snapshot_due_bytes: number;
	/** Milliseconds since this owner last saw a snapshot published; `null` for none since the attach. */
	readonly snapshot_age_ms: number | null;
	/** Snapshot attempts that failed in a row (0 once one succeeds), and the last failure. */
	readonly snapshot_failures: number;
	readonly snapshot_error: string | null;
}

export function status(file: string): AttachStatus {
	const row = loadUrsulaVfs().prepare("SELECT ursula_status(?) AS s").get(file) as { s: string };
	return JSON.parse(row.s) as AttachStatus;
}

/** One replicated commit, as measured inside the VFS. */
export interface VfsCommitStat {
	/** Frame bytes on the stream (compressed). */
	readonly bytes: number;
	/** Uncompressed record bytes. */
	readonly raw: number;
	/** Page images in the record. */
	readonly pages: number;
	/** Append requests (more than one after an unknown outcome). */
	readonly attempts: number;
	/** The append requests alone. */
	readonly append_us: number;
	/** Commit hook: record build + append + local WAL write. */
	readonly vfs_us: number;
}

/** One published (and read back) snapshot. */
export interface VfsSnapshotStat {
	/** Stream offset it reflects. */
	readonly offset: StreamOffset;
	/** Body bytes (compressed). */
	readonly bytes: number;
	/** Database bytes. */
	readonly raw: number;
	/** Page copy under the read transaction. */
	readonly copy_us: number;
	/** Checkpoint, copy, compression, publish and read-back. */
	readonly total_us: number;
}

/**
 * What the extension dropped since the last drain to keep at most 10,000 of each kind. The dropped
 * commits' frame bytes and append retries are counted, so totals built from the drains count every
 * commit; their durations are lost.
 */
export interface VfsDropped {
	readonly commits: number;
	/** The dropped commits' frame bytes. */
	readonly commit_bytes: number;
	/** The dropped commits' append attempts beyond the first. */
	readonly append_retries: number;
	readonly checkpoints: number;
	readonly snapshots: number;
}

export interface VfsStats {
	readonly commits: VfsCommitStat[];
	/** Checkpoints of the file since the last drain (time holding the checkpoint lock). */
	readonly checkpoints_us: number[];
	readonly snapshots: VfsSnapshotStat[];
	/** What was dropped since the last drain. An extension before 0.7.3 keeps everything and reports none. */
	readonly dropped?: VfsDropped;
}

/**
 * Drains the per-commit, per-checkpoint and per-snapshot stats of an attached file: everything kept
 * since the last drain, oldest first. The extension keeps the newest 10,000 of each kind between
 * drains, about 640 KB of commits, so a file nobody drains holds a bounded amount; `dropped` counts
 * the rest.
 */
export function drainStats(file: string): VfsStats {
	const row = loadUrsulaVfs().prepare("SELECT ursula_stats(?) AS s").get(file) as { s: string };
	return JSON.parse(row.s) as VfsStats;
}

/**
 * A commit the VFS could not replicate: fenced (by a newer owner, because the stream was deleted and
 * recreated, or by another writer's append with a higher `Stream-Seq`), rejected, or with no answer in
 * time.
 */
export class UrsulaReplicationError extends Error {
	readonly fenced: boolean;
	constructor(message: string, fenced: boolean, cause: unknown) {
		super(message, { cause });
		this.name = "UrsulaReplicationError";
		this.fenced = fenced;
	}
}

/**
 * What refused a write before it reached the stream (`refused_kind` in {@link AttachStatus}):
 *
 * - `payment_required`: a layer in front of the stream answered `402 Payment Required`;
 * - `unauthorized`: the token was refused (`401`) until the retry budget ran out;
 * - `forbidden`: an authorizer denied the write with a `403` marked `x-ursula-not-applied`, which a
 *   gateway vouched for with `x-ursula-not-applied-checked` (any other `403` fails the commit and
 *   poisons the file, since a filtering proxy can send one after the stream applied the write);
 * - `rate_limited`: `429` until the retry budget ran out, marked or not;
 * - `not_applied`: other answers marked `x-ursula-not-applied` and vouched for by a gateway (the server
 *   under write pressure, or a gateway that could not authorize the request) until the retry budget
 *   ran out, or a marked final answer such as a `413`.
 *
 * The marker counts only with the vouch, which `ursula gateway` adds from 0.7.3 on. Without such a
 * gateway in front of the servers, only a `401`, `402` or `429` refuses a write this way.
 */
export type WriteRefusedKind = "payment_required" | "unauthorized" | "forbidden" | "rate_limited" | "not_applied";

/**
 * A write refused before it reached the stream, so that nothing of it was applied: by a layer in front
 * of the stream (an authorizer, a quota or billing service, a gateway or a proxy), or by the server's
 * admission while it is under write pressure. Unlike an {@link UrsulaReplicationError}, it leaves the
 * file usable. A refused commit was rolled back, but the file is not poisoned: reads go on from the
 * local file, and later commits are sent as usual and succeed once the refusals stop, with no new
 * attach. A refused attach claimed nothing: attach again later, or with `{ readOnly: true }` to read
 * the database meanwhile.
 *
 * With plain node:sqlite, a refused commit throws SQLite's error with `errcode` 7178
 * (SQLITE_IOERR_AUTH), and `status(file)` names the refusal (`refused_kind`, `refused_reason`). A
 * refused `ursula_attach` throws one with `errcode` 7178 too, or 23 (SQLITE_AUTH) for a `402`;
 * `attach` turns them into this error and {@link UrsulaPaymentRequiredError}.
 */
export class UrsulaWriteRefusedError extends Error {
	/** What refused it, for a refused commit. `null` for an attach refused other than with a `402`, whose message names it. */
	readonly kind: WriteRefusedKind | null;
	/** Why (`refused_reason`), for a refused commit: `null` when the layer sent no explanation, and for a refused attach, whose message carries it. */
	readonly reason: string | null;
	constructor(message: string, kind: WriteRefusedKind | null, reason: string | null, cause: unknown) {
		super(message, { cause });
		this.name = "UrsulaWriteRefusedError";
		this.kind = kind;
		this.reason = reason;
	}
}

/**
 * A write refused with `402 Payment Required` by a layer in front of the stream, such as an authorizer
 * or a quota or billing service: the {@link UrsulaWriteRefusedError} of kind `payment_required`.
 * Nothing of it reached the stream, and the file stays usable. Its `reason` is the layer's explanation
 * for a refused commit, as `payment_reason` reports it (`null` when the layer sent none). It is always
 * `null` for a refused attach, whose message carries it.
 */
export class UrsulaPaymentRequiredError extends UrsulaWriteRefusedError {
	constructor(message: string, reason: string | null, cause: unknown) {
		super(message, "payment_required", reason, cause);
		this.name = "UrsulaPaymentRequiredError";
	}
}

/** {@link openUrsulaPiStorage}'s options: Pi's node driver settings, and how to attach. */
export type UrsulaPiStorageOptions = NodeSqliteStorageOptions & AttachOptions;

/**
 * Pi Durable's official node SqliteStorage on a replicated file: attaches `file` to `streamUrl`, then
 * opens it with Pi's node driver, unmodified. A commit the VFS fails surfaces as an
 * `UrsulaReplicationError` (SQLite has already rolled the transaction back, so the driver's own
 * rollback attempt would otherwise turn it into an AggregateError). The storage is unusable for
 * writes after that; re-open it to take the stream over again (after a delete and recreate, re-opening
 * rebuilds the file from the new stream).
 *
 * A commit refused before it reached the stream surfaces as an {@link UrsulaWriteRefusedError} instead
 * ({@link UrsulaPaymentRequiredError} for a `402`), and the storage stays usable: the next commit goes
 * out as usual. With `{ readOnly: true }` the file is attached read-only: the storage opens on a
 * database Pi has already written, its reads work, and every commit that changes it fails with
 * SQLITE_READONLY.
 */
export async function openUrsulaPiStorage(file: string, streamUrl: string, options: UrsulaPiStorageOptions = {}): Promise<SqliteStorage> {
	const { readOnly, ...sqlite } = options;
	attach(file, streamUrl, readOnly === undefined ? {} : { readOnly });
	const db = await openNodeSqliteDatabase(file, sqlite);
	const transaction = db.transaction.bind(db);
	db.transaction = <T>(callback: (transaction: SqliteExecutor) => Promise<T>): Promise<T> =>
		transaction(callback).catch((error: unknown) => {
			const cause = error instanceof AggregateError ? error.errors[0] : error;
			const s = status(file);
			// Read-only: SQLite's own error, without the driver's failed ROLLBACK. The transaction is
			// rolled back either way: by SQLite when COMMIT failed, by the driver's ROLLBACK when a
			// statement failed before it.
			if (s.read_only && errcode(cause) === SQLITE_READONLY) throw cause;
			// Poisoned (by this commit, or by the snapshot thread since a refusal): the storage needs a
			// new attach whatever the error, a refusal's code included.
			if (s.poisoned) throw new UrsulaReplicationError(`commit not replicated: ${s.reason ?? "unknown"}`, s.fenced, cause);
			if (errcode(cause) === SQLITE_IOERR_AUTH) {
				if (s.payment_required) {
					const why = s.payment_reason === null ? "" : `: ${s.payment_reason}`;
					throw new UrsulaPaymentRequiredError(`commit refused with 402 Payment Required in front of the stream${why}`, s.payment_reason, cause);
				}
				// An extension before 0.7.3 reports none of these: it refuses with 402 alone.
				const kind = s.refused_kind ?? null;
				const reason = s.refused_reason ?? null;
				const why = `${kind === null ? "" : `: ${kind}`}${reason === null ? "" : ` (${reason})`}`;
				throw new UrsulaWriteRefusedError(`commit refused before it reached the stream${why}`, kind, reason, cause);
			}
			throw error;
		});
	return SqliteStorage.open(db);
}

/** Attributes on a measurement, as OpenTelemetry takes them. */
export type MetricAttributes = Readonly<Record<string, string | number | boolean>>;

/**
 * The part of an OpenTelemetry `Meter` that {@link instrument} uses. A `Meter` from
 * `@opentelemetry/api` satisfies it; the package does not depend on OpenTelemetry.
 */
export interface MeterLike {
	createHistogram(name: string, options?: { description?: string; unit?: string }): { record(value: number, attributes?: MetricAttributes): void };
	createCounter(name: string, options?: { description?: string; unit?: string }): { add(value: number, attributes?: MetricAttributes): void };
	createObservableGauge(
		name: string,
		options?: { description?: string; unit?: string },
	): {
		addCallback(callback: (result: { observe(value: number, attributes?: MetricAttributes): void }) => void): void;
		removeCallback(callback: (result: { observe(value: number, attributes?: MetricAttributes): void }) => void): void;
	};
}

/**
 * Reports an attached file's health as metrics on `meter`, until the returned function is called:
 *
 * - histograms `sqlite_ursula.commit.duration` (the VFS's commit hook: frame build, append, local
 *   WAL write) and `sqlite_ursula.append.duration`, in ms;
 * - counters `sqlite_ursula.append.retries` and `sqlite_ursula.stream.bytes`;
 * - gauges `sqlite_ursula.log.bytes`, `sqlite_ursula.snapshot.due_bytes`, `sqlite_ursula.snapshot.age`
 *   (ms), `sqlite_ursula.snapshot.failures`, `sqlite_ursula.poisoned`, `sqlite_ursula.fenced`,
 *   `sqlite_ursula.payment_required` and `sqlite_ursula.writes_refused` (0 or 1; the latter is not
 *   measured with an extension before 0.7.3, which does not report it).
 *
 * Every measurement carries `sqlite.file` and `attributes`. The per-commit numbers come from
 * {@link drainStats} every `intervalMs` (10 s by default), so do not call `drainStats` on the same
 * file elsewhere while it runs.
 */
export function instrument(file: string, meter: MeterLike, options: { intervalMs?: number; attributes?: MetricAttributes } = {}): () => void {
	const attributes: MetricAttributes = { "sqlite.file": file, ...options.attributes };
	const commit = meter.createHistogram("sqlite_ursula.commit.duration", { unit: "ms", description: "VFS commit hook: frame build, append, local WAL write" });
	const append = meter.createHistogram("sqlite_ursula.append.duration", { unit: "ms", description: "Append requests of a commit" });
	const retries = meter.createCounter("sqlite_ursula.append.retries", { description: "Append attempts beyond the first" });
	const bytes = meter.createCounter("sqlite_ursula.stream.bytes", { unit: "By", description: "Frame bytes appended to the stream" });
	const drain = (): void => {
		let stats: VfsStats;
		try {
			stats = drainStats(file);
		} catch {
			return; // not attached (any more)
		}
		for (const c of stats.commits) {
			commit.record(c.vfs_us / 1000, attributes);
			append.record(c.append_us / 1000, attributes);
			if (c.attempts > 1) retries.add(c.attempts - 1, attributes);
			bytes.add(c.bytes, attributes);
		}
		// Commits dropped between two drains still count, though their durations are lost.
		const dropped = stats.dropped;
		if (dropped !== undefined) {
			if (dropped.append_retries > 0) retries.add(dropped.append_retries, attributes);
			if (dropped.commit_bytes > 0) bytes.add(dropped.commit_bytes, attributes);
		}
	};
	const gauges: [string, string | undefined, (s: AttachStatus) => number | null][] = [
		["sqlite_ursula.log.bytes", "By", (s) => s.log_bytes],
		["sqlite_ursula.snapshot.due_bytes", "By", (s) => s.snapshot_due_bytes],
		["sqlite_ursula.snapshot.age", "ms", (s) => s.snapshot_age_ms],
		["sqlite_ursula.snapshot.failures", undefined, (s) => s.snapshot_failures],
		["sqlite_ursula.poisoned", undefined, (s) => (s.poisoned ? 1 : 0)],
		["sqlite_ursula.fenced", undefined, (s) => (s.fenced ? 1 : 0)],
		["sqlite_ursula.payment_required", undefined, (s) => (s.payment_required ? 1 : 0)],
		// An extension before 0.7.3 does not report it: no measurement rather than a false 0.
		["sqlite_ursula.writes_refused", undefined, (s) => (s.writes_refused === undefined ? null : s.writes_refused ? 1 : 0)],
	];
	const observed = gauges.map(([name, unit, value]) => {
		const gauge = meter.createObservableGauge(name, unit === undefined ? {} : { unit });
		const callback = (result: { observe(value: number, attributes?: MetricAttributes): void }): void => {
			let s: AttachStatus;
			try {
				s = status(file);
			} catch {
				return;
			}
			const v = value(s);
			if (v !== null) result.observe(v, attributes);
		};
		gauge.addCallback(callback);
		return () => gauge.removeCallback(callback);
	});
	const timer = setInterval(drain, options.intervalMs ?? 10_000);
	timer.unref();
	return () => {
		clearInterval(timer);
		drain();
		for (const remove of observed) remove();
	};
}
