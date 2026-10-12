#!/usr/bin/env bash
# #560: fmt, clippy, tests, and the two mutants of the follow-ups (the past-limit single flight,
# the floor after a failure gives way). $1 is x64 or macos.
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
mutant() {
  local name=$1; shift
  echo "::group::mutant $name: $*"
  timeout 300 "$@" 2>&1 | tee -a scratch-out/log.txt "scratch-out/mutant-$name.txt" | grep -E "^test .*(ok|FAILED)$|panicked|left:|right:|test result|error\[|FAIL|✓|×" | head -40
  local s=${PIPESTATUS[0]}
  echo "::endgroup::"
  if [ "$s" -ne 0 ] && ! grep -q "error\[E" "scratch-out/mutant-$name.txt"; then echo "[killed] $arch: mutant $name" | tee -a scratch-out/summary.txt
  elif [ "$s" -ne 0 ]; then echo "[BROKEN] $arch: mutant $name did not compile" | tee -a scratch-out/summary.txt; fail=1
  else echo "[SURVIVED] $arch: mutant $name" | tee -a scratch-out/summary.txt; fail=1; fi
}
V=clients/sqlite-vfs
mutate() {
  git checkout -- $V/src
  local file expr
  case "$1" in
    no-expired-flight) file=$V/src/net.rs; expr='s/        \(None, None, Some\(good\)\) if entry\.looking\.is_some\(\) => Plan::Use\(good\.addrs\.clone\(\)\),\n//' ;;
    no-giveway-floor) file=$V/src/net.rs; expr='s/        entry\.next = entry\.next\.max\(later\(ended, DNS_RETRY\)\);\n//' ;;
    *) echo "unknown mutation $1" >&2; return 1 ;;
  esac
  perl -0777 -pi -e "$expr" "$file"
  if git diff --quiet -- "$file"; then echo "mutation $1 changed nothing" >&2; return 1; fi
  git diff -- "$file"
}
unit_mutant() {
  mutate "$1" || { echo "[BROKEN] $arch: mutation $1" | tee -a scratch-out/summary.txt; fail=1; return; }
  mutant "$1" bash -c "cd $V && cargo test --lib -- $2"
  git checkout -- $V/src
}

run bash -c "cd $V && cargo fmt -- --check"
run bash -c "cd $V && cargo clippy --all-targets -- -D warnings"
run bash -c "cd $V && cargo test"
for i in 1 2 3 4 5; do run bash -c "cd $V && cargo test --lib -- net:: a_failed_connection requests_to_a_host"; done
unit_mutant no-expired-flight "past_the_stale_limit_one_request_at_a_time_looks_a_host_up"
unit_mutant no-giveway-floor "a_lookup_that_fails_gives_way_to_a_success_since_it_started"
git status --short | tee -a scratch-out/summary.txt
echo "--- summary"; cat scratch-out/summary.txt
exit $fail
