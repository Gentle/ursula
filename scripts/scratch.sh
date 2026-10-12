#!/usr/bin/env bash
# #503 item 1 fix: format, clippy, then the readiness test 2400 times, 8 at a time, on a loaded
# runner. Unfixed, 2 of 1200 (x64) and 3 of 1200 (arm64) runs met the unready window (38206968068).
set -uo pipefail
arch=${1:-x64}
mkdir -p scratch-out
fail=0
run() {
  echo "::group::$*"
  "$@" 2>&1 | tee -a scratch-out/log.txt
  local s=${PIPESTATUS[0]}
  echo "::endgroup::"
  echo "[$s] $arch: $*" | tee -a scratch-out/summary.txt
  [ "$s" -eq 0 ] || fail=1
}
run cargo fmt --all -- --check
run cargo clippy -p ursula --all-targets -- -D warnings
cargo test -p ursula --lib --no-run 2>&1 | tail -2
BIN=$(cargo test -p ursula --lib --no-run --message-format=json 2>/dev/null | jq -r 'select(.profile.test == true and .target.name == "ursula" and (.target.kind | index("lib"))) | .executable' | tail -1)
run "$BIN" server::tests::a_single_node_durable_server_becomes_ready --exact
busy=()
for _ in $(seq 1 6); do (while :; do :; done) & busy+=($!); done
loops=()
for j in $(seq 1 8); do
  (for _ in $(seq 1 300); do
     "$BIN" server::tests::a_single_node_durable_server_becomes_ready --exact 2>&1 | grep -E "panicked|test result|left:|right:"
   done > "scratch-out/loop-$j.txt") &
  loops+=($!)
done
wait "${loops[@]}"
kill "${busy[@]}" 2>/dev/null
runs=$(cat scratch-out/loop-*.txt | grep -c 'test result')
ok=$(cat scratch-out/loop-*.txt | grep -c 'test result: ok')
echo "[$([ "$runs" = 2400 ] && [ "$ok" = 2400 ] && echo 0 || echo 1)] $arch: loop runs $runs passed $ok" | tee -a scratch-out/summary.txt
[ "$runs" = 2400 ] && [ "$ok" = 2400 ] || fail=1
grep -h -A2 "panicked" scratch-out/loop-*.txt | head -12
echo "--- summary"; cat scratch-out/summary.txt
exit $fail
