#!/usr/bin/env bash
# #559 review round 2: fmt, clippy, tests, mutants of the restructured cache (the reviewer's four:
# an empty Looking::drop, plan on a cloned entry, due() deleting the entry, no stale cap; and the
# due floor, the negative cache, the lookup id, the cached failure's kind) and of the earlier
# parts, and on x64 the e2e. $1 is x64 or macos.
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
    no-classify) file=$V/src/client.rs; expr='s/let \(e, sent_nothing\) = net::sent_nothing\(e\);/let (e, sent_nothing) = (net::sent_nothing(e).0, false);/' ;;
    all-unsent) file=$V/src/client.rs; expr='s/let \(e, sent_nothing\) = net::sent_nothing\(e\);/let (e, sent_nothing) = (net::sent_nothing(e).0, true);/' ;;
    no-cache) file=$V/src/net.rs; expr='s/\(Some\(good\), _\) if now < entry\.next \|\| entry\.looking/(Some(good), _) if entry.looking/' ;;
    no-connect-mark) file=$V/src/net.rs; expr='s/self\.resolver\.due\(details\.uri\);\n        Err\(not_sent\(e\)\)/self.resolver.due(details.uri);\n        Err(e)/' ;;
    no-stale) file=$V/src/net.rs; expr='s/                match stand_in \{/                match None::<Good> {/' ;;
    not-idempotent) file=$V/src/net.rs; expr='s/    if is_not_sent\(&e\) \{\n        return e;\n    \}\n//' ;;
    no-due) file=$V/src/net.rs; expr='s/        self\.resolver\.due\(details\.uri\);\n//' ;;
    no-single-flight) file=$V/src/net.rs; expr='s/if now < entry\.next \|\| entry\.looking\.is_some\(\) =>/if now < entry.next =>/' ;;
    empty-drop) file=$V/src/net.rs; expr='s/            entry\.looking = None;\n/            let _ = &entry.looking;\n/' ;;
    cloned-plan) file=$V/src/net.rs; expr='s/match plan\(entry, now, id\) \{/match plan(&mut entry.clone(), now, id) {/' ;;
    due-deletes) file=$V/src/net.rs; expr='s/        if let Some\(entry\) = self\.hosts\(\)\.get_mut\(&host\) \{\n            entry\.next = due_at\(entry, Instant::now\(\)\);\n        \}\n/        self.hosts().remove(&host);\n/' ;;
    no-cap) file=$V/src/net.rs; expr='s/    retry\.min\(stale\)/    let _ = stale;\n    retry/' ;;
    no-floor) file=$V/src/net.rs; expr='s/    entry\.next\.min\(now\.max\(floor\)\)/    let _ = floor;\n    entry.next.min(now)/' ;;
    no-negative-cache) file=$V/src/net.rs; expr='s/        \(None, Some\(failure\)\) if now < entry\.next => Plan::Fail\(failure\),\n//' ;;
    any-id-clears) file=$V/src/net.rs; expr='s/            && entry\.looking == Some\(self\.id\)\n/            && self.id == self.id\n/' ;;
    failure-kind) file=$V/src/net.rs; expr='s/ureq::Error::Timeout\(_\) => Failure::TimedOut,/ureq::Error::Timeout(_) => Failure::NotFound,/' ;;
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
unit_mutant no-classify "append_whose_every_attempt append_whose_lookups failure_before_sending"
unit_mutant all-unsent "a_failure_after_connecting_keeps_the_append_unknown"
unit_mutant no-cache "requests_within_30_s requests_to_a_host_share_one_lookup"
unit_mutant no-connect-mark "an_append_whose_every_attempt_fails_before_sending_fails_alone"
unit_mutant no-stale "a_failed_lookup_uses_the_last_addresses"
unit_mutant not-idempotent "a_failure_is_marked_once a_proxy_that_does_not_resolve"
unit_mutant no-due "a_failed_connection"
unit_mutant no-single-flight "one_request_at_a_time_looks_a_host_up"
unit_mutant empty-drop "requests_within_30_s a_failed_lookup_uses_the_last_addresses"
unit_mutant cloned-plan "one_request_at_a_time_looks_a_host_up"
unit_mutant due-deletes "a_failed_connection_makes_the_lookup_due"
unit_mutant no-cap "near_the_stale_limit"
unit_mutant no-floor "a_failed_connection"
unit_mutant no-negative-cache "a_failed_lookup_with_no_addresses an_append_whose_lookups_time_out"
unit_mutant any-id-clears "a_lookup_that_ends_leaves_the_host_marked"
unit_mutant failure-kind "a_failed_lookup_with_no_addresses"

if [ "$arch" = x64 ]; then
  e2e() { local so=$1; shift; (cd clients/sqlite-ursula && URSULA_BIN=/tmp/ursula SQLITE_URSULA_VFS="$so" npx vitest --run "$@"); }
  run bash -c 'cargo build -p ursula --bin ursula && cp target/debug/ursula /tmp/ursula'
  run bash -c "cd $V && SQLITE3_LIB_DIR=/nonexistent cargo build --locked --release && cp target/release/libsqlite_ursula_vfs.so /tmp/vfs.so"
  run bash -c 'cd clients/sqlite-ursula && npm ci && npm run typecheck'
  for i in 1 2 3; do run e2e /tmp/vfs.so test/unreachable.e2e.ts; done
  run e2e /tmp/vfs.so test/refused.e2e.ts
  run e2e /tmp/vfs.so test/crash.e2e.ts
fi
git status --short | tee -a scratch-out/summary.txt
echo "--- summary"; cat scratch-out/summary.txt
exit $fail
