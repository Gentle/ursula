#!/usr/bin/env bash
# #558 restacked on #555's 026ef65c: format, clippy, the coordinator units (also under madsim), the
# driver and operator snapshot node tests (three times), then mutations, each of which the named
# tests must catch.
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
  timeout 600 "$@" 2>&1 | tee -a scratch-out/log.txt "scratch-out/mutant-$name.txt" | grep -E "^test .*(ok|FAILED)$|panicked|left:|right:|test result|running [0-9]+ test|error\[E" | head -40
  local s=${PIPESTATUS[0]}
  echo "::endgroup::"
  if [ "$s" -ne 0 ] && ! grep -q "error\[E" "scratch-out/mutant-$name.txt"; then echo "[killed] $arch: mutant $name" | tee -a scratch-out/summary.txt
  elif [ "$s" -ne 0 ]; then echo "[BROKEN] $arch: mutant $name did not compile" | tee -a scratch-out/summary.txt; fail=1
  else echo "[SURVIVED] $arch: mutant $name" | tee -a scratch-out/summary.txt; fail=1; fi
}
mutate() {
  local name=$1 file=$2 expr=$3
  git checkout -- crates
  perl -0777 -pi -e "$expr" "$file"
  if git diff --quiet -- "$file"; then echo "[BROKEN] $arch: mutation $name changed nothing" | tee -a scratch-out/summary.txt; fail=1; return 1; fi
  git diff -- "$file"
}
SM=crates/ursula-raft/src/state_machine.rs
FA=crates/ursula-raft/src/engine/factory.rs
DR=crates/ursula/src/bootstrap/snapshot.rs
LIB=crates/ursula/src/lib.rs

run cargo fmt --all -- --check
run cargo clippy -p ursula-raft -p ursula --all-targets -- -D warnings
run cargo test -p ursula-raft --lib
run env CARGO_TARGET_DIR=target/madsim RUSTFLAGS="--cfg madsim" cargo test -p ursula-raft --lib
for i in 1 2 3; do run cargo test -p ursula --lib tests::snapshot_driver; done

mutate no-retrigger $LIB 's/                Handoff::Unclaimed => coordinator\.build_ended\(raft_group_id\.0\)\.await,/                Handoff::Unclaimed => return Ok(()),/' &&
  caught no-retrigger cargo test -p ursula --lib tests::snapshot_driver::an_operator_snapshot_builds_again_after_a_running_build -- --exact
mutate busy-as-claimed $LIB 's/                Handoff::Busy => \{\n                    coordinator\.handoff_settled\(raft_group_id\.0\)\.await;\n                    coordinator\.build_ended\(raft_group_id\.0\)\.await;\n                \}/                Handoff::Busy => return Ok(()),/' &&
  caught busy-as-claimed cargo test -p ursula --lib tests::snapshot_driver::an_operator_snapshot_waits_for_a_pending_handoff -- --exact
mutate no-reclaim-wake $SM 's/        if taken \{\n            \/\/ Wakes the waits on hand-offs\.\n            self\.inner\.builds\.send_modify\(\|_builds\| \{\}\);\n        \}\n//' &&
  caught no-reclaim-wake cargo test -p ursula-raft --lib a_pending_handoff_settles_when_claimed_or_taken_back
mutate no-reclaim-wake-node $SM 's/        if taken \{\n            \/\/ Wakes the waits on hand-offs\.\n            self\.inner\.builds\.send_modify\(\|_builds\| \{\}\);\n        \}\n//' &&
  caught no-reclaim-wake-node cargo test -p ursula --lib tests::snapshot_driver::an_operator_snapshot_waits_for_a_pending_handoff -- --exact
git checkout -- crates
git status --short | tee -a scratch-out/summary.txt
echo "--- summary"; cat scratch-out/summary.txt
exit $fail
