#!/usr/bin/env bash
# #529, final tree: format, clippy, the operator snapshot tests, then the mutation that stops
# triggering again after a dropped trigger (M2), which the running-build test must catch.
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
caught() {
  local name=$1; shift
  echo "::group::mutation $name: $*"
  "$@" 2>&1 | tee -a scratch-out/log.txt | grep -E "^test .*(ok|FAILED)$|panicked|left:|right:|test result|running [0-9]+ test" | head -40
  local s=${PIPESTATUS[0]}
  echo "::endgroup::"
  if [ "$s" -ne 0 ]; then echo "[0] $arch: mutation $name caught" | tee -a scratch-out/summary.txt
  else echo "[1] $arch: mutation $name NOT caught" | tee -a scratch-out/summary.txt; fail=1; fi
}
run cargo fmt --all -- --check
run cargo clippy -p ursula-raft -p ursula --all-targets -- -D warnings
run cargo test -p ursula --lib snapshot_driver -- --nocapture
python3 - <<'PY'
p = "crates/ursula/src/lib.rs"
s = open(p).read()
old = """                bootstrap::Handoff::Unclaimed { .. } => {
                    coordinator.build_ended(raft_group_id.0).await;
                }"""
assert s.count(old) == 1
s = s.replace(old, """                bootstrap::Handoff::Unclaimed { .. } => return Ok(()),""")
open(p, "w").write(s)
print("M2 applied")
PY
caught M2-no-retrigger cargo test -p ursula --lib tests::snapshot_driver::an_operator_snapshot_builds_again_after_a_running_build -- --exact
git checkout crates/ursula/src/lib.rs
echo "--- summary"; cat scratch-out/summary.txt
exit $fail
