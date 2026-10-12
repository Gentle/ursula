#!/usr/bin/env bash
# #529: format, clippy, the snapshot driver and coordinator tests (with the new operator snapshot
# tests), then two mutations of the admin handler that the new tests must catch.
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
# Expect a failure: a mutation the tests must catch.
caught() {
  local name=$1; shift
  echo "::group::mutation $name: $*"
  "$@" 2>&1 | tee -a scratch-out/log.txt | grep -E "^test .*(ok|FAILED)$|panicked|left:|right:|test result" | head -40
  local s=${PIPESTATUS[0]}
  echo "::endgroup::"
  if [ "$s" -ne 0 ]; then echo "[0] $arch: mutation $name caught" | tee -a scratch-out/summary.txt
  else echo "[1] $arch: mutation $name NOT caught" | tee -a scratch-out/summary.txt; fail=1; fi
}
run cargo fmt --all -- --check
run cargo clippy -p ursula-raft -p ursula --all-targets -- -D warnings
run cargo test -p ursula --lib snapshot_driver
run cargo test -p ursula-raft --lib state_machine
run cargo test -p ursula --lib snapshot_driver::an_operator_snapshot

# M1: the handler as before #529: trigger without taking a permit.
python3 - <<'PY'
p = "crates/ursula/src/lib.rs"
s = open(p).read()
start = s.index("            let permit = match coordinator.acquire().await {")
end = s.index("        }\n    })\n    .await;\n    match started {")
new = """            if let Err(err) = raft.trigger().snapshot().await {
                return Err(format!("trigger raft snapshot: {err}"));
            }
            return Ok(());
"""
s = s[:start] + new + s[end:]
open(p, "w").write(s)
print("M1 applied")
PY
caught M1-no-permit cargo test -p ursula --lib snapshot_driver::an_operator_snapshot
for t in an_operator_snapshot_waits_for_a_build_permit an_operator_snapshot_answers_503_while_every_permit_stays_in_use an_operator_snapshot_builds_again_after_a_running_build; do
  caught "M1-$t" cargo test -p ursula --lib "snapshot_driver::$t" -- --exact
done
git checkout crates/ursula/src/lib.rs

# M2: no second trigger after a dropped one.
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
caught M2-no-retrigger cargo test -p ursula --lib snapshot_driver::an_operator_snapshot_builds_again_after_a_running_build -- --exact
git checkout crates/ursula/src/lib.rs
echo "--- summary"; cat scratch-out/summary.txt
exit $fail
