#!/usr/bin/env bash
# #559 review round 3: fmt, clippy, tests, mutants of the round's five items (single flight with
# no stand-in, settling from the entry at the lookup's end, the kept failure's text, kind and OS
# code, the floor's input) and of the earlier parts, and on x64 the e2e. $1 is x64 or macos.
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
    no-cache) file=$V/src/net.rs; expr='s/let waits = now < entry\.next \|\| entry\.looking\.is_some\(\);/let waits = entry.looking.is_some();/' ;;
    no-connect-mark) file=$V/src/net.rs; expr='s/self\.resolver\.due\(details\.uri\);\n        Err\(not_sent\(e\)\)/self.resolver.due(details.uri);\n        Err(e)/' ;;
    no-stale) file=$V/src/net.rs; expr='s/    match entry\.stand_in\(ended\)\.cloned\(\) \{/    match None::<Good> {/' ;;
    not-idempotent) file=$V/src/net.rs; expr='s/    if is_not_sent\(&e\) \{\n        return e;\n    \}\n//' ;;
    no-due) file=$V/src/net.rs; expr='s/        self\.resolver\.due\(details\.uri\);\n//' ;;
    no-single-flight) file=$V/src/net.rs; expr='s/let waits = now < entry\.next \|\| entry\.looking\.is_some\(\);/let waits = now < entry.next;/' ;;
    empty-drop) file=$V/src/net.rs; expr='s/            entry\.looking = None;\n/            let _ = &entry.looking;\n/' ;;
    cloned-plan) file=$V/src/net.rs; expr='s/match plan\(entry, started, id\) \{/match plan(&mut entry.clone(), started, id) {/' ;;
    due-deletes) file=$V/src/net.rs; expr='s/        if let Some\(entry\) = self\.hosts\(\)\.get_mut\(&host\) \{\n            entry\.next = due_at\(entry, Instant::now\(\)\);\n        \}\n/        self.hosts().remove(&host);\n/' ;;
    no-cap) file=$V/src/net.rs; expr='s/later\(ended, DNS_RETRY\)\.min\(later\(good\.at, DNS_STALE_LIMIT\)\)/later(ended, DNS_RETRY)/' ;;
    no-floor) file=$V/src/net.rs; expr='s/entry\.next\.min\(now\.max\(later\(entry\.ended, DNS_RETRY\)\)\)/entry.next.min(now)/' ;;
    no-negative-cache) file=$V/src/net.rs; expr='s/        \(None, Some\(failure\)\) if waits => Plan::Fail\(failure\.clone\(\)\),\n//' ;;
    any-id-clears) file=$V/src/net.rs; expr='s/            && entry\.looking == Some\(self\.id\)\n/            && self.id == self.id\n/' ;;
    failure-kind) file=$V/src/net.rs; expr='s/ureq::Error::Timeout\(timeout\) => Some\(Failure::TimedOut\(\*timeout\)\),/ureq::Error::Timeout(_) => Some(Failure::TimedOut(ureq::Timeout::Resolve)),/' ;;
    flight-needs-stand-in) file=$V/src/net.rs; expr='s/\(None, Some\(failure\)\) if waits =>/(None, Some(failure)) if now < entry.next =>/' ;;
    stand-in-at-start) file=$V/src/net.rs; expr='s/    match entry\.stand_in\(ended\)\.cloned\(\) \{/    match entry.stand_in(started).cloned() {/' ;;
    failure-overrides-success) file=$V/src/net.rs; expr='s/\.filter\(\|good\| good\.at >= started\)/.filter(|_| started > ended)/' ;;
    no-ended|no-ended-settle) file=$V/src/net.rs; expr='s/\) -> Settled \{\n    entry\.ended = ended;\n/) -> Settled {\n/' ;;
    kind-only) file=$V/src/net.rs; expr='s/None => Failure::Io\(e\.kind\(\), e\.to_string\(\)\),/None => Failure::Io(e.kind(), e.kind().to_string()),/' ;;
    no-os-code) file=$V/src/net.rs; expr='s/Some\(code\) => Failure::Os\(code\),/Some(_) => Failure::Io(e.kind(), e.to_string()),/' ;;
    success-keeps-failure) file=$V/src/net.rs; expr='s/            entry\.failed = None;\n            entry\.next = later\(ended, DNS_FRESH\);/            entry.next = later(ended, DNS_FRESH);/' ;;
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
unit_mutant no-stale "a_failed_lookup_uses_the_last_addresses a_lookup_that_fails_settles_on"
unit_mutant not-idempotent "a_failure_is_marked_once a_proxy_that_does_not_resolve"
unit_mutant no-due "a_failed_connection"
unit_mutant no-single-flight "one_request_at_a_time"
unit_mutant empty-drop "requests_within_30_s a_failed_lookup_uses_the_last_addresses"
unit_mutant cloned-plan "one_request_at_a_time_looks_a_host_up"
unit_mutant due-deletes "a_failed_connection_makes_the_lookup_due"
unit_mutant no-cap "near_the_stale_limit a_lookup_that_fails_settles_on"
unit_mutant no-floor "a_failed_connection"
unit_mutant no-negative-cache "a_failed_lookup_with_no_addresses an_append_whose_lookups_time_out"
unit_mutant any-id-clears "a_lookup_that_ends_leaves_the_host_marked"
unit_mutant failure-kind "a_kept_failure_prints"
unit_mutant flight-needs-stand-in "one_request_at_a_time_looks_up_a_host_with_no_addresses"
unit_mutant stand-in-at-start "a_lookup_that_fails_settles_on"
unit_mutant failure-overrides-success "a_lookup_that_fails_gives_way"
unit_mutant no-ended "a_failed_connection_makes_the_lookup_due_after_the_retry_and_keeps_the_addresses"
unit_mutant no-ended-settle "a_lookup_that"
unit_mutant kind-only "a_kept_failure_prints"
unit_mutant no-os-code "a_kept_failure_prints"
unit_mutant success-keeps-failure "a_lookup_that_succeeds"

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
