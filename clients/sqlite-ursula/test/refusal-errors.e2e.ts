// How a commit that failed with SQLITE_IOERR_AUTH maps to an error through `openUrsulaPiStorage`,
// from `status(file)` right after it (`refusedCommitError`). A pure function: the extension reads
// URSULA_VFS_MAX_FRAME_BYTES once per process, so a too-large transaction while a layer refuses with
// 402 is hard to reach in one.
import { expect, it } from "vitest";
import { refusedCommitError, UrsulaPaymentRequiredError, UrsulaWriteRefusedError } from "../src/errors.ts";

const cause = new Error("SQLITE_IOERR_AUTH");

it("a transaction refused as too_large while a layer refuses with 402 is the transaction's own refusal", () => {
	const e = refusedCommitError(
		{
			payment_required: true,
			payment_reason: "quota",
			refused_kind: "too_large",
			refused_reason: "41943104-byte frame over the 33554432-byte frame limit",
		},
		cause,
	);
	expect(e).toBeInstanceOf(UrsulaWriteRefusedError);
	expect(e).not.toBeInstanceOf(UrsulaPaymentRequiredError);
	expect(e.kind).toBe("too_large");
	expect(e.reason).toBe("41943104-byte frame over the 33554432-byte frame limit");
	expect(e.cause).toBe(cause);
});

it("a 402 is an UrsulaPaymentRequiredError, from an extension of 0.7.3 or one before it", () => {
	for (const s of [
		{ payment_required: true, payment_reason: "quota", refused_kind: "payment_required" as const, refused_reason: "quota" },
		// Before 0.7.3 the extension reports no kind, and refuses with 402 alone.
		{ payment_required: true, payment_reason: "quota" },
	]) {
		const e = refusedCommitError(s, cause);
		expect(e).toBeInstanceOf(UrsulaPaymentRequiredError);
		expect(e.kind).toBe("payment_required");
		expect(e.reason).toBe("quota");
	}
	const other = refusedCommitError({ payment_required: false, payment_reason: null, refused_kind: "not_applied", refused_reason: "RaftLogPressure" }, cause);
	expect(other).not.toBeInstanceOf(UrsulaPaymentRequiredError);
	expect([other.kind, other.reason]).toEqual(["not_applied", "RaftLogPressure"]);
});
