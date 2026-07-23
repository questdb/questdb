#!/usr/bin/env bash
# Before/after ASOF default benchmark: for every main data shape, time the OLD default
# (/*+ asof_fast */, the pre-flip keyed default) against the NEW default (no hint = auto
# Dense / DenseSingleSymbol / auto-index / auto-memoized). Same JVM, same data, same binary:
# only the cursor factory differs -> a clean A/B that isolates the algorithm change.
#
# Proves two things across all shapes: (1) big wins where the old Fast default cliffed, and
# (2) NO regression on Fast-favourable / untouched shapes (unique_ts, sparse_tail, multikey, nokey).
set -uo pipefail

ROOT=/data/questdb-oss
OUTDIR=/data/asofbench
JAR="$ROOT/benchmarks/target/benchmarks.jar"
CSV="$OUTDIR/before_after.csv"
LOG="$OUTDIR/before_after.log"
SUMMARY="$OUTDIR/before_after_summary.txt"

# full data: 300k for the dense/unique/multikey shapes, 2M right for the illiquid/index/tail shapes.
ROWS=${ROWS:-300000}
RIGHT=${RIGHT:-2000000}
DENSE=${DENSE:-10000}
SHAPES=${SHAPES:-dense_ts,unique_ts,dense_sym,illiquid_sym,multikey_sym,nokey,sparse_tail,illiquid_idx}
WI=${WI:-1}   # warmup iterations
II=${II:-2}   # measurement iterations (= JMH samples)

mkdir -p "$OUTDIR"
# Kill any hung bench JVM and clear the lock dir (the pool is non-daemon; a stale run holds the lock).
pkill -9 -f AsOfJoinAlgorithmBenchmark 2>/dev/null
sleep 2
rm -rf /tmp/asof-adaptive-bench

JVM_ARGS="--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.time.zone=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED"

echo "running before/after: shapes=$SHAPES rows=$ROWS right=$RIGHT dense=$DENSE"
java $JVM_ARGS -Dasof.bench.rows="$ROWS" -Dasof.bench.right.rows="$RIGHT" -Dasof.bench.dense="$DENSE" \
     -cp "$JAR" org.questdb.AsOfJoinAlgorithmBenchmark \
     "AsOfJoinAlgorithmBenchmark.run" -p dist="$SHAPES" -p algo=fast,default \
     -wi "$WI" -i "$II" -f 0 -rf csv -rff "$CSV" 2>&1 | tee "$LOG"

echo "=== parsing $CSV -> $SUMMARY ==="
# CSV cols: "Benchmark","Mode","Threads","Samples","Score","Score Error","Unit","Param: algo","Param: dist"
awk -F',' '
  function strip(s){ gsub(/"/,"",s); return s }
  NR==1 {
    for (i=1;i<=NF;i++){ h=strip($i); if(h=="Score")sc=i; if(h ~ /Param: algo/)ai=i; if(h ~ /Param: dist/)di=i }
    next
  }
  {
    algo=strip($ai); dist=strip($di); score=strip($sc)+0
    t[dist","algo]=score; if(!(dist in seen)){seen[dist]=1; order[++n]=dist}
  }
  END {
    printf "%-14s %14s %14s %10s   %s\n","shape","OLD fast(ms)","NEW default(ms)","speedup","verdict"
    printf "%-14s %14s %14s %10s   %s\n","-----","------------","---------------","-------","-------"
    for(k=1;k<=n;k++){
      d=order[k]; f=t[d",fast"]; nd=t[d",default"]
      sp=(nd>0)? f/nd : 0
      verdict=(sp>=1.5)?"WIN "sprintf("%.0fx",sp):((sp>=0.95)?"no-harm":"REGRESSION")
      printf "%-14s %14.3f %14.3f %9.1fx   %s\n", d, f, nd, sp, verdict
    }
  }
' "$CSV" | tee "$SUMMARY"

# Kill the JVM if it is still up (main() now exits cleanly, but belt-and-braces).
pkill -9 -f AsOfJoinAlgorithmBenchmark 2>/dev/null
echo "done -> $SUMMARY"
