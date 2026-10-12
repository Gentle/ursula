#!/usr/bin/env bash
# #555 review round 3: format, clippy, the coordinator units (also under madsim), the driver units
# and its node tests (three times), then mutations of this round's code, each of which the named
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

run cargo fmt --all -- --check
run cargo clippy -p ursula-raft -p ursula-config -p ursula --all-targets -- -D warnings
run cargo test -p ursula-raft --lib
run env CARGO_TARGET_DIR=target/madsim RUSTFLAGS="--cfg madsim" cargo test -p ursula-raft --lib
run cargo test -p ursula --lib bootstrap::snapshot
for i in 1 2 3; do run cargo test -p ursula --lib tests::snapshot_driver; done

mutate drop-keeps-permit $SM 's/    fn drop\(&mut self\) \{\n        self\.reclaim\(\);\n    \}/    fn drop(\&mut self) {}/' &&
  caught drop-keeps-permit cargo test -p ursula-raft --lib a_dropped_or_panicking_handoff_gives_its_permit_back
mutate second-replaces-first $SM 's/        if handoffs\.contains_key\(&raft_group_id\) \{\n            return Err\(permit\);\n        \}\n//' &&
  caught second-replaces-first cargo test -p ursula-raft --lib a_group_takes_one_handoff_at_a_time
mutate reclaim-any-token $SM 's/        let ours = handoffs\n            \.get\(&raft_group_id\)\n            \.is_some_and\(\|handed\| handed\.token == token\);/        let ours = handoffs.contains_key(\&raft_group_id);/' &&
  caught reclaim-any-token cargo test -p ursula-raft --lib a_group_takes_one_handoff_at_a_time
mutate no-held-retick $DR 's/ \|\| held > 0\)\)/))/' &&
  caught no-held-retick cargo test -p ursula --lib a_pressure_pass_plans_again_soon_while_builds_start_or_run
git checkout -- crates
git status --short | tee -a scratch-out/summary.txt
echo "--- summary"; cat scratch-out/summary.txt
exit $fail
