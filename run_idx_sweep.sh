#!/usr/bin/env bash
# Sweep indexed-symbol cardinality to find the asof_index vs Dense crossover.
set -u
cd /data/questdb-oss
JAR=benchmarks/target/benchmarks.jar
JVM_ARGS="--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.time.zone=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED"
OUT=/data/asofbench/idx_sweep_summary.txt
: > "$OUT"
for CARD in 100000 10000 1000 100 10; do
  LOG=/data/asofbench/idx_sweep_card${CARD}.log
  java $JVM_ARGS -Dasof.bench.right.rows=2000000 -Dasof.bench.idx.card=${CARD} \
    -cp "$JAR" org.questdb.AsOfJoinAlgorithmBenchmark \
    "AsOfJoinAlgorithmBenchmark.run" -p dist=idx_sweep -p algo=default,index -wi 2 -i 3 -r 1 -f 0 \
    > "$LOG" 2>&1 &
  JPID=$!
  # wait for completion marker, then kill the (hanging) JVM
  until grep -qiE "Run complete|Exception|Error:" "$LOG" 2>/dev/null; do sleep 3; done
  sleep 1
  kill "$JPID" 2>/dev/null
  # wait for the (hanging) JVM to die and release the table-registry lock before the next run
  for _ in $(seq 1 20); do
    fuser /tmp/asof-adaptive-bench/tables.d.lock >/dev/null 2>&1 || break
    sleep 1
  done
  kill -9 "$JPID" 2>/dev/null
  echo "===== cardinality=${CARD} (rows/symbol ~ $((2000000/CARD))) =====" >> "$OUT"
  awk '/^Benchmark /{p=1} p{print}' "$LOG" >> "$OUT"
  echo "" >> "$OUT"
done
echo "ALL DONE" >> "$OUT"
