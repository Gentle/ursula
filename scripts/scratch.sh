#!/usr/bin/env bash
# The VFS DNS fix: a cached resolver, and attempts that failed before they went out counted as
# applying nothing. VFS fmt/clippy/tests, mutants of each part (unit tests), and on x64 the e2e of a
# commit that cannot reach its endpoint, with its neighbours, and against a VFS without the
# classification. $1 is x64, arm64 or macos.
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
  "$@" 2>&1 | tee -a scratch-out/log.txt "scratch-out/mutant-$name.txt" | grep -E "^test .*(ok|FAILED)$|panicked|left:|right:|test result|FAIL|✓|×" | head -40
  local s=${PIPESTATUS[0]}
  echo "::endgroup::"
  if [ "$s" -ne 0 ]; then echo "[killed] $arch: mutant $name" | tee -a scratch-out/summary.txt
  else echo "[SURVIVED] $arch: mutant $name" | tee -a scratch-out/summary.txt; fail=1; fi
}
V=clients/sqlite-vfs
mutate() {
  git checkout -- $V/src
  local file expr
  case "$1" in
    no-classify) file=$V/src/client.rs; expr='s/let \(e, sent_nothing\) = net::sent_nothing\(e\);/let (e, sent_nothing) = (net::sent_nothing(e).0, false);/' ;;
    all-unsent) file=$V/src/client.rs; expr='s/let \(e, sent_nothing\) = net::sent_nothing\(e\);/let (e, sent_nothing) = (net::sent_nothing(e).0, true);/' ;;
    no-cache) file=$V/src/net.rs; expr='s/\.filter\(\|last\| fresh\(Some\(last\), now\)\)/.filter(|_| false)/' ;;
    no-connect-mark) file=$V/src/net.rs; expr='s/Err\(e\) => Err\(not_sent\(e\)\),/Err(e) => Err(e),/' ;;
    no-stale) file=$V/src/net.rs; expr='s/Err\(e\) => match last\.filter\(\|last\| stands_in\(last, now\)\) \{/Err(e) => match None::<Looked> {/' ;;
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
unit_mutant no-classify "append_whose_every_attempt append_whose_lookups failure_before_sending"
unit_mutant all-unsent "a_failure_after_connecting_keeps_the_append_unknown"
unit_mutant no-cache "requests_to_a_host_share_one_lookup net::"
unit_mutant no-connect-mark "an_append_whose_every_attempt_fails_before_sending_fails_alone"
unit_mutant no-stale "a_failed_lookup_uses_the_last_addresses"

if [ "$arch" = x64 ]; then
  VFS=$GITHUB_WORKSPACE/$V/target/release/libsqlite_ursula_vfs.so
  e2e() { local so=$1; shift; (cd clients/sqlite-ursula && URSULA_BIN=/tmp/ursula SQLITE_URSULA_VFS="$so" npx vitest --run "$@"); }
  run bash -c 'cargo build -p ursula --bin ursula && cp target/debug/ursula /tmp/ursula'
  run bash -c "cd $V && SQLITE3_LIB_DIR=/nonexistent cargo build --locked --release && cp target/release/libsqlite_ursula_vfs.so /tmp/vfs-fixed.so"
  run bash -c 'cd clients/sqlite-ursula && npm ci && npm run typecheck'
  for i in 1 2 3; do run e2e /tmp/vfs-fixed.so test/unreachable.e2e.ts; done
  run e2e /tmp/vfs-fixed.so test/refused.e2e.ts
  run e2e /tmp/vfs-fixed.so test/crash.e2e.ts
  run e2e /tmp/vfs-fixed.so test/payment.e2e.ts
  if mutate no-classify; then
    run bash -c "cd $V && SQLITE3_LIB_DIR=/nonexistent cargo build --locked --release && cp target/release/libsqlite_ursula_vfs.so /tmp/vfs-no-classify.so"
    git checkout -- $V/src
    mutant no-classify-e2e e2e /tmp/vfs-no-classify.so test/unreachable.e2e.ts
  else
    echo "[BROKEN] $arch: mutation no-classify for the e2e" | tee -a scratch-out/summary.txt; fail=1
  fi
fi
git status --short | tee -a scratch-out/summary.txt
echo "--- summary"; cat scratch-out/summary.txt
exit $fail
