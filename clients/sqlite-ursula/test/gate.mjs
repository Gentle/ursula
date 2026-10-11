// An HTTP layer in front of the node that refuses writes on demand without forwarding them, the way an
// authorizer, a quota or billing service, or the server's own admission under pressure may: with 402
// Payment Required by default, or with any status and headers (an authorizer's 403, a 503 marked
// `x-ursula-not-applied`). It runs in a process of its own, so the test process can attach and commit
// through it synchronously (node:sqlite blocks the event loop meanwhile).
//
// argv: the node's URL. Prints {"port": n} once it listens. Control requests, never forwarded:
//   PUT /__gate {"refuse": body | null, "limit"?: n, "methods"?: ["POST", "PUT"], "status"?: 402,
//                "headers"?: {name: value}, "dropNextPost"?: {"refuse": body, "limit"?: n,
//                "status"?: n, "headers"?: {...}} | null, "forwardNextPost"?: {...} | null}
//   GET /__gate -> {"refused": n, "forwarded": n, "dropped": n, "replaced": n, "droppedStatuses": [n],
//                   "replacedStatuses": [n]}
// While `refuse` is set, requests with one of `methods` (writes: POST and PUT by default) are answered
// `status` (402 by default) with `headers` and that body without being forwarded, `limit` times at most
// (then `refuse` clears). With `dropNextPost`, the next POST is forwarded but its answer is cut (its
// outcome is unknown to the client), and its refusal is armed at once. With `forwardNextPost` (the same
// fields), the next POST is forwarded and the node's answer is replaced by that refusal, which breaks the
// rule for refusals (the node applied the write, and the client is told it did not), and its refusal is
// armed at once for the writes after it. The node's status for each answer cut or replaced is kept, in
// order (`droppedStatuses`, `replacedStatuses`).
import { createServer, request } from "node:http";

const target = new URL(process.argv[2]);
const state = { refuse: null, limit: Number.POSITIVE_INFINITY, methods: ["POST", "PUT"], status: 402, headers: {}, dropNextPost: null, forwardNextPost: null, refused: 0, forwarded: 0, dropped: 0, replaced: 0, droppedStatuses: [], replacedStatuses: [] };

const arm = ({ refuse, limit, methods, status, headers }) => {
	state.refuse = refuse;
	state.limit = limit ?? Number.POSITIVE_INFINITY;
	state.methods = methods ?? ["POST", "PUT"];
	state.status = status ?? 402;
	state.headers = headers ?? {};
};

const server = createServer((req, res) => {
	if (req.url === "/__gate") {
		if (req.method === "GET") {
			const { refused, forwarded, dropped, replaced, droppedStatuses, replacedStatuses } = state;
			res.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify({ refused, forwarded, dropped, replaced, droppedStatuses, replacedStatuses }));
			return;
		}
		let body = "";
		req.on("data", (chunk) => {
			body += chunk;
		});
		req.on("end", () => {
			const control = JSON.parse(body);
			if ("refuse" in control) arm(control);
			if ("dropNextPost" in control) state.dropNextPost = control.dropNextPost;
			if ("forwardNextPost" in control) state.forwardNextPost = control.forwardNextPost;
			res.writeHead(204).end();
		});
		return;
	}
	if (state.refuse !== null && state.methods.includes(req.method)) {
		state.refused++;
		const body = state.refuse;
		state.limit--;
		if (state.limit <= 0) state.refuse = null;
		req.resume();
		res.writeHead(state.status, { "content-type": "application/json", ...state.headers }).end(body);
		return;
	}
	let drop = false;
	if (req.method === "POST" && state.dropNextPost !== null) {
		drop = true;
		arm(state.dropNextPost);
		state.dropNextPost = null;
	}
	let replace = null;
	if (req.method === "POST" && state.forwardNextPost !== null) {
		replace = state.forwardNextPost;
		arm(replace);
		state.forwardNextPost = null;
	}
	state.forwarded++;
	const up = request({ host: target.hostname, port: target.port, method: req.method, path: req.url, headers: req.headers }, (answer) => {
		if (drop) {
			state.dropped++;
			state.droppedStatuses.push(answer.statusCode);
			answer.resume();
			answer.on("end", () => res.socket?.destroy());
			return;
		}
		if (replace !== null) {
			const { refuse, status, headers } = replace;
			answer.resume();
			answer.on("end", () => {
				state.replaced++;
				state.replacedStatuses.push(answer.statusCode);
				res.writeHead(status ?? 402, { "content-type": "application/json", ...headers }).end(refuse);
			});
			return;
		}
		res.writeHead(answer.statusCode ?? 502, answer.headers);
		answer.pipe(res);
	});
	up.on("error", () => res.destroy());
	req.pipe(up);
});

server.listen(0, "127.0.0.1", () => {
	process.stdout.write(`${JSON.stringify({ port: server.address().port })}\n`);
});
