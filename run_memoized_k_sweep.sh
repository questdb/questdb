#!/usr/bin/env bash
# Sweep the memoized dense-timestamp fallback threshold K on the cliff shape (dense_sym = 10000 rows/ts),
# plus a sparse-ts control, to (a) prove the fallback eliminates the cliff and (b) pick the default K.
set -u
cd /data/questdb-oss
JAR=benchmarks/target/benchmarks.jar
JVM_ARGS="--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.time.zone=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED"
OUT=/data/asofbench/memoized_k_summary.txt
: > "$OUT"

run() { # label, dist, algo, kprop
  local label="$1" dist="$2" algo="$3" k="$4"
  local LOG=/data/asofbench/memk_${label}.log
  java $JVM_ARGS -Dasof.bench.memoized.k=${k} \
    -cp "$JAR" org.questdb.AsOfJoinAlgorithmBenchmark \
    "AsOfJoinAlgorithmBenchmark.run" -p dist=${dist} -p algo=${algo} -wi 1 -i 2 -r 1 -f 0 \
    > "$LOG" 2>&1 &
  local JPID=$!
  until grep -qiE "Run complete|Exception|Error:" "$LOG" 2>/dev/null; do sleep 3; done
  sleep 1; kill "$JPID" 2>/dev/null
  for _ in $(seq 1 20); do fuser /tmp/asof-adaptive-bench/tables.d.lock >/dev/null 2>&1 || break; sleep 1; done
  kill -9 "$JPID" 2>/dev/null
  echo "===== ${label}: dist=${dist} algo=${algo} K=${k} =====" >> "$OUT"
  awk '/^Benchmark /{p=1} p{print}' "$LOG" >> "$OUT"
  echo "" >> "$OUT"
}

# Cliff shape: dense_sym (10000 rows/ts). memoized with fallback disabled (K=MAX) vs various K; Dense reference.
run densesym_memo_Kmax   dense_sym memoized 2000000000
run densesym_memo_K16384 dense_sym memoized 16384
run densesym_memo_K4096  dense_sym memoized 4096
run densesym_memo_K256   dense_sym memoized 256
run densesym_memo_K64    dense_sym memoized 64
run densesym_dense_ref   dense_sym default  2000000000
# Sparse-ts control: unique_ts. memoized must NOT be hurt by a low K (runs are length 1 -> never trips).
run uniquets_memo_Kmax   unique_ts memoized 2000000000
run uniquets_memo_K64    unique_ts memoized 64
echo "ALL DONE" >> "$OUT"
