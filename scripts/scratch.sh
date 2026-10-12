#!/usr/bin/env bash
# #555 review: format, clippy, the coordinator and engine-config units (also under madsim), the
# config units, the driver units and its node tests (three times), then mutations, each of which
# the named tests must catch.
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
  "$@" 2>&1 | tee -a scratch-out/log.txt "scratch-out/mutant-$name.txt" | grep -E "^test .*(ok|FAILED)$|panicked|left:|right:|test result|running [0-9]+ test|error\[E" | head -40
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
run cargo test -p ursula-config --lib
run cargo test -p ursula --lib bootstrap::snapshot
for i in 1 2 3; do run cargo test -p ursula --lib tests::snapshot_driver; done

mutate trigger-failure-parks $SM 's/            self\.reclaim_handoff\(raft_group_id\);\n            return Handoff::TriggerFailed\(err\);/            return Handoff::TriggerFailed(err);/' &&
  caught trigger-failure-parks cargo test -p ursula-raft --lib a_handed_off_permit_comes_back_unless_a_build_claims_it
mutate unclaimed-parks $SM 's/        if self\.reclaim_handoff\(raft_group_id\) \{\n            Handoff::Unclaimed/        if self.handoff_pending(raft_group_id) {\n            Handoff::Unclaimed/' &&
  caught unclaimed-parks cargo test -p ursula-raft --lib a_handed_off_permit_comes_back_unless_a_build_claims_it
mutate conversion-drops-default $FA 's/            snapshot_build_max_concurrency: cfg\.snapshot_build_max_concurrency,/            snapshot_build_max_concurrency: 1,/' &&
  caught conversion-drops-default cargo test -p ursula-raft --lib engine_config_carries_the_configs_build_concurrency
mutate conversion-drops-default-node $FA 's/            snapshot_build_max_concurrency: cfg\.snapshot_build_max_concurrency,/            snapshot_build_max_concurrency: 1,/' &&
  caught conversion-drops-default-node cargo test -p ursula --lib tests::snapshot_driver::the_driver_uses_every_build_slot_at_the_hard_limit -- --exact
mutate no-retick $DR 's/                pause = next_pause\(interval, plan\.pressure, triggered, building\.len\(\), outgrown\);/                pause = interval;/' &&
  caught no-retick cargo test -p ursula --lib tests::snapshot_driver::a_pass_waiting_for_a_slot_escalates_with_the_log -- --exact
mutate planner-ignores-held $DR 's/\.or_else\(\|\| held\.contains\(&snapshot\.raft_group_id\)\.then_some\(0\)\)/.or_else(|| held.contains(\&u32::MAX).then_some(0))/' &&
  caught planner-ignores-held cargo test -p ursula --lib tests::snapshot_driver::snapshot_driver_escalates_past_the_budget_around_builds_in_flight -- --exact
mutate no-doubling $DR 's/hold\.next = hold\.next\.saturating_mul\(2\)\.min\(MAX_HOLD_PASSES\);/hold.next = hold.next.min(MAX_HOLD_PASSES);/' &&
  caught no-doubling cargo test -p ursula --lib a_group_whose_trigger_went_unclaimed_sits_out_the_next_passes
mutate unclaimed-not-held $DR 's/                hold\.passes = hold\.next;\n/                hold.passes = 0;\n/' &&
  caught unclaimed-not-held cargo test -p ursula --lib a_group_whose_trigger_went_unclaimed_sits_out_the_next_passes
mutate no-cap $DR 's/hold\.next = hold\.next\.saturating_mul\(2\)\.min\(MAX_HOLD_PASSES\);/hold.next = hold.next.saturating_mul(2);/' &&
  caught no-cap cargo test -p ursula --lib a_group_whose_trigger_went_unclaimed_sits_out_the_next_passes
git checkout -- crates
git status --short | tee -a scratch-out/summary.txt
echo "--- summary"; cat scratch-out/summary.txt
exit $fail
