#!/usr/bin/env bash
# #503 item 1: which readiness reason answers 503 right after a fresh single-node durable server's
# first create? Patches the test (scratch only) to poll readiness after the PUT until it answers
# 200, printing the first unready body, then runs it 1200 times, 8 at a time, on a loaded runner.
set -uo pipefail
arch=${1:-x64}
mkdir -p scratch-out
python3 - <<'EOF'
p = "crates/ursula/src/server.rs"
s = open(p).read()
old = """        assert_eq!(created.status(), StatusCode::CREATED);
        assert_eq!(ready(client).await, StatusCode::OK);
"""
new = """        assert_eq!(created.status(), StatusCode::CREATED);
        let after = std::time::Instant::now();
        let mut unready = 0_u32;
        loop {
            let response = client
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(crate::READINESS_PATH)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let status = response.status();
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            if status == StatusCode::OK {
                if unready > 0 {
                    println!("READY-AGAIN after {:?} and {unready} unready answers", after.elapsed());
                }
                break;
            }
            unready += 1;
            if unready <= 2 {
                println!(
                    "UNREADY-AFTER-CREATE #{unready} at {:?}: {}",
                    after.elapsed(),
                    String::from_utf8_lossy(&body)
                );
            }
            assert!(after.elapsed() < std::time::Duration::from_secs(10), "still unready after 10 s");
            tokio::task::yield_now().await;
        }
"""
assert s.count(old) == 1, s.count(old)
open(p, "w").write(s.replace(old, new))
print("patched")
EOF
cargo test -p ursula --lib --no-run 2>&1 | tail -3
BIN=$(cargo test -p ursula --lib --no-run --message-format=json 2>/dev/null | jq -r 'select(.profile.test == true and .target.name == "ursula" and (.target.kind | index("lib"))) | .executable' | tail -1)
echo "binary: $BIN"
"$BIN" server::tests::a_single_node_durable_server_becomes_ready --exact --nocapture 2>&1 | tail -5

busy=()
for _ in $(seq 1 6); do (while :; do :; done) & busy+=($!); done
loops=()
for j in $(seq 1 8); do
  (for _ in $(seq 1 150); do
     "$BIN" server::tests::a_single_node_durable_server_becomes_ready --exact --nocapture 2>&1 \
       | grep -E "UNREADY|READY-AGAIN|panicked|test result"
   done > "scratch-out/loop-$j.txt") &
  loops+=($!)
done
wait "${loops[@]}"
kill "${busy[@]}" 2>/dev/null
echo "--- $arch: runs $(cat scratch-out/loop-*.txt | grep -c 'test result') passed $(cat scratch-out/loop-*.txt | grep -c 'test result: ok') unready $(cat scratch-out/loop-*.txt | grep -c 'UNREADY-AFTER-CREATE #1')" | tee scratch-out/summary.txt
grep -h "UNREADY-AFTER-CREATE #1" scratch-out/loop-*.txt | head -5
grep -h "READY-AGAIN" scratch-out/loop-*.txt | head -10
grep -h "panicked" scratch-out/loop-*.txt | head -5
exit 0
