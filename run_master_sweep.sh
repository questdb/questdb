#!/usr/bin/env bash
# Sweep master (left) row count to find the asof_index vs Dense master/slave-ratio crossover.
# Slave fixed at 2M, indexed symbol cardinality fixed at 1000 (mid-range).
set -u
cd /data/questdb-oss
JAR=benchmarks/target/benchmarks.jar
JVM_ARGS="--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.time.zone=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED"
OUT=/data/asofbench/master_sweep_summary.txt
: > "$OUT"
for M in 1000 10000 100000 500000 2000000; do
  LOG=/data/asofbench/master_sweep_m${M}.log
  java $JVM_ARGS -Dasof.bench.right.rows=2000000 -Dasof.bench.idx.card=1000 -Dasof.bench.master.rows=${M} \
    -cp "$JAR" org.questdb.AsOfJoinAlgorithmBenchmark \
    "AsOfJoinAlgorithmBenchmark.run" -p dist=idx_sweep -p algo=default,index -wi 2 -i 3 -r 1 -f 0 \
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
  echo "===== master_rows=${M} (slave 2M; master/slave ratio ~ $(awk "BEGIN{printf \"%.4f\", ${M}/2000000}")) =====" >> "$OUT"
  awk '/^Benchmark /{p=1} p{print}' "$LOG" >> "$OUT"
  echo "" >> "$OUT"
done
echo "ALL DONE" >> "$OUT"
