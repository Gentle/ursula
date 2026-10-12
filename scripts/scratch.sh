#!/usr/bin/env bash
# #553 review round: format, clippy, rustdoc, the gateway's tests, refused_bodies ten times, and two
# mutants the new tests must kill.
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
killed() {
  local name=$1; shift
  echo "::group::mutant $name: $*"
  "$@" 2>&1 | tee -a scratch-out/log.txt | grep -E "^test .*(ok|FAILED)$|panicked|left:|right:|test result|running [0-9]+ test" | head -40
  local s=${PIPESTATUS[0]}
  echo "::endgroup::"
  if [ "$s" -ne 0 ]; then echo "[0] $arch: mutant $name killed" | tee -a scratch-out/summary.txt
  else echo "[1] $arch: mutant $name SURVIVED" | tee -a scratch-out/summary.txt; fail=1; fi
}
run cargo fmt --all -- --check
run cargo clippy -p ursula-gateway --all-targets -- -D warnings
echo "::group::rustdoc"
cargo doc -p ursula-gateway --no-deps 2>&1 | tee scratch-out/doc.txt | tail -3
echo "[$(grep -c '^warning' scratch-out/doc.txt | sed 's/^0$/0/')] $arch: rustdoc warnings (count in brackets)" | tee -a scratch-out/summary.txt
echo "::endgroup::"
run cargo test -p ursula-gateway
for i in $(seq 1 10); do
  run cargo test -p ursula-gateway --lib refused_bodies
done

# G1: a dropped reading not counted (the counting before this round).
python3 - <<'PY'
p = "crates/ursula-gateway/src/lib.rs"
s = open(p).read()
old = "            cut.whole = drained == Drained::Whole;\n"
assert s.count(old) == 1
s = s.replace(old, "            if drained != Drained::Whole {\n                self.metrics.refusal_drains_cut.fetch_add(1, Ordering::Relaxed);\n            }\n")
built = "                cut: &self.metrics.refusal_drains_cut,\n                whole: false,\n"
assert s.count(built) == 1
s = s.replace(built, "                cut: &self.metrics.refusal_drains_cut,\n                whole: true,\n")
open(p, "w").write(s)
print("G1 applied")
PY
killed G1-dropped-reading-not-counted cargo test -p ursula-gateway --lib refused_bodies::a_reading_dropped_before_the_body_ended_counts_as_cut
git checkout crates/ursula-gateway/src/lib.rs

# G2: every refusal from the head closes its connection.
python3 - <<'PY'
p = "crates/ursula-gateway/src/lib.rs"
s = open(p).read()
old = "            debug!(?drained, \"read the body of a refused request\");\n        }\n        refusal.into_response()\n"
assert s.count(old) == 1
s = s.replace(old, "            debug!(?drained, \"read the body of a refused request\");\n        }\n        let mut response = refusal.into_response();\n        response.headers_mut().insert(axum::http::header::CONNECTION, HeaderValue::from_static(\"close\"));\n        response\n")
open(p, "w").write(s)
print("G2 applied")
PY
killed G2-refusal-closes-connection cargo test -p ursula-gateway --lib refused_bodies::the_connection_carries_the_next_request_after_a_whole_reading
git checkout crates/ursula-gateway/src/lib.rs
echo "--- summary"; cat scratch-out/summary.txt
exit $fail
