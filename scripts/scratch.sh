#!/usr/bin/env bash
# The VFS DNS fix after the clippy fix in net.rs's tests: fmt, clippy, tests. $1 is x64 or macos.
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
V=clients/sqlite-vfs
run bash -c "cd $V && cargo fmt -- --check"
run bash -c "cd $V && cargo clippy --all-targets -- -D warnings"
run bash -c "cd $V && cargo test"
if [ "$arch" = x64 ]; then
  run bash -c 'cd clients/sqlite-ursula && npm ci && npm run typecheck'
fi
echo "--- summary"; cat scratch-out/summary.txt
exit $fail
