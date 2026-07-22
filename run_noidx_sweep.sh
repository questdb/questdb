#!/usr/bin/env bash
# No-index case: best algo when asof_index is unavailable (slave symbol not indexed).
# Selective 1000-row master vs 2M slave; sweep symbol cardinality; compare default(Dense) vs memoized vs fast.
set -u
cd /data/questdb-oss
JAR=benchmarks/target/benchmarks.jar
JVM_ARGS="--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.time.zone=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED"
OUT=/data/asofbench/noidx_sweep_summary.txt
: > "$OUT"
for CARD in 100000 1000 10; do
  LOG=/data/asofbench/noidx_sweep_card${CARD}.log
  java $JVM_ARGS -Dasof.bench.right.rows=2000000 -Dasof.bench.idx.card=${CARD} -Dasof.bench.master.rows=1000 \
    -cp "$JAR" org.questdb.AsOfJoinAlgorithmBenchmark \
    "AsOfJoinAlgorithmBenchmark.run" -p dist=noidx_sweep -p algo=default,memoized,fast -wi 2 -i 3 -r 1 -f 0 \
    > "$LOG" 2>&1 &
  JPID=$!
  until grep -qiE "Run complete|Exception|Error:" "$LOG" 2>/dev/null; do sleep 3; done
  sleep 1
  kill "$JPID" 2>/dev/null
  for _ in $(seq 1 20); do
    fuser /tmp/asof-adaptive-bench/tables.d.lock >/dev/null 2>&1 || break
    sleep 1
  done
  kill -9 "$JPID" 2>/dev/null
  echo "===== cardinality=${CARD} (rows/symbol ~ $((2000000/CARD)); master=1000, NO INDEX) =====" >> "$OUT"
  awk '/^Benchmark /{p=1} p{print}' "$LOG" >> "$OUT"
  echo "" >> "$OUT"
done
echo "ALL DONE" >> "$OUT"
