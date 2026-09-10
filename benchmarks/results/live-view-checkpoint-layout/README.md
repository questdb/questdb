# Live-view checkpoint layout removal: performance acceptance matrix

Everything needed to repeat the matrix: the harness changes are in the benchmarks module,
the driver and the aggregator are here, and [RESULTS.md](RESULTS.md) holds a measured run
with the machine and settings that produced it.

Raw per-run files are not committed - 360 of them, about 12 MB - and `run-matrix.sh`
regenerates them. `RESULTS.md` carries every aggregated cell, including the full gate
table.

## What is measured

`LiveViewSteadyStateBenchmark` seeds `K` rows into `K` round-robin accounts, so the view
holds exactly `K` live keys and none of them ever falls behind the frontier, then ingests
`--batches` commits of 1000 rows each. Consecutive rows take consecutive accounts, so one
commit touches exactly 1000 distinct existing keys: the changed-key domain stays at 1000
while `K` moves, which is what separates total state size from the changed-key domain.
`--checkpoint-rows` matches the commit size, so one commit seals exactly one boundary and
one batch row of the output is one seal.

`--restart=true` adds a restore and its first reseal at the tail of each run, from the
state the steady batches left behind, reported on the `# restore` line.

`--oracle=true` ends the run by comparing the view's complete output with an independent
query over the base table - the anchored window restated as a plain window function
partitioned by the account and its anchor bucket - and reports both directions of the
disagreement on the `# oracle` line. A run whose rows are wrong is not a valid timing
sample, which is why every repair run carries it.

| Matrix row | Shape argument | What it holds |
| --- | --- | --- |
| Anchor-only | `--shape=decimal-sum` | An anchored DECIMAL SUM. DECIMAL is outside every inline family, so the plan carries zero components: an eight-byte window payload holding the anchor value, plus one function root. |
| Anchor-only control | `--shape=sum-unfused` | The same arithmetic over an expression rather than a column reference, which the plan declines for want of an argument key. |
| Narrow | `--shape=sum` | One 16-byte SUM/COUNT component. |
| Narrow | `--shape=sum-avg-count` | Three projections onto one 16-byte component. |
| Narrow | `--shape=count-star-key --null-key-percent=5` | `count(*)` beside a guarded `count(account_id)`, with a NULL key partition the guard has to leave at zero. |
| Wide, below the budget | `--shape=sum --sum-columns=14` | 15 components, 248 payload bytes against the 256-byte inline leaf budget. |
| Wide, at the budget | `--shape=count --sum-columns=15` | 16 components, exactly 256 payload bytes. |
| Wide, above the budget | `--shape=sum --sum-columns=15` | 16 components of which 15 are durable; the sixteenth stays a runtime-only member with a function root of its own. |
| Residual-heavy | `--shape=residual` | An anchored DOUBLE SUM that fuses, beside a DECIMAL SUM, a bounded ROWS frame and a bounded RANGE frame that do not. The RANGE call is ring-backed, which `freezeFunction` excludes from dirty-key capture, so its root scans complete on every seal. |

Each row runs with `--fusion=true` and `--fusion=false`. The two are the paired columns
the matrix is read in: the storage layout no longer follows the runtime binding, so the
same shape must publish the same manifest and the same component images under both.

## The repair cell

`run-matrix.sh ... repair` runs the closed-segment repair cell instead of the steady rows.
A one-minute anchor (`--anchor-period=1m`) with 1000 rows per minute (`--ts-step-us=60000`)
makes every batch one closed anchor segment and one checkpoint boundary. From batch 20 on
(`--o3-from-batch=20`) every commit carries exactly one late row - every 1000th row
(`--o3-percent=0.1`), ten minutes behind its position (`--o3-lag=10m`) - so each measured
batch is a one-key correction inside a closed segment with ten checkpoints sealed above
it. The base column is indexed (`--index=true`) so the keyed route can be priced at all.

| Cell | Route | Runs on |
| --- | --- | --- |
| `repair-closed-whole` | `--repair-keyed-replay=false`: the corrected segment is replayed whole | both revisions |
| `repair-closed-keyed` | `--repair-keyed-replay=true`: the correction follows its key through the posting index | candidate only |

The baseline runs only the whole-range control: its keyed route with fusion off is the
defective one the layout removal fixed, and the handoff excludes it as a reference. The
aggregator therefore reads the candidate's keyed cell against the baseline's whole-range
cell and marks the row as a route comparison, and reads the candidate's whole-range cell
against the same baseline cell for the like-for-like comparison. Per repair batch the
harness's `refresh_ms` is the repair's latency, `o3_scan_rows` the base rows its replay
read, `lv_phys_rows` the live-view rows its publication wrote and `repair` the route it
took; the aggregator reports the last three beside the gates.

At 1,000 keys every account appears in every minute, so nothing ages out and the live
domain stays at 1,000. At 10,000 keys an account appears once in ten minutes and falls
behind the frontier in between, but the default compaction thresholds are never reached
in 110 batches, so no key is evicted and the domain stays at 10,000 as well; lower
`--compact-threshold` and `--compact-stale-percent` to make the sweep fire.

## The churn cell

`run-matrix.sh ... churn` is the add/remove-keys, cross-anchor-boundary run the matrix names
beside the steady rows. The steady rows recycle `K` accounts forever, so nothing is ever added
or evicted; the churn rows slide a `K`-account window (`--account-window=K`) over an anchor
bucket of exactly `K` rows - a `K/1000`-minute anchor at 1000 rows per minute - so that half
of a bucket's accounts recur from the bucket before it and half are new. The half left behind
falls behind the frontier, and the sweep at the next bucket boundary evicts it: `K/2` keys
added and `K/2` evicted per bucket, over a live domain that moves between `K` and `1.5 K`.
The compaction thresholds are lowered (`--compact-threshold=1000
--compact-stale-percent=25`) so that sweep fires at all; the shipped defaults need 100,000
stale keys and never do at these sizes.

A bucket is `K/1000` batches, so at 10,000 keys the sweep fires every 10 batches and at
100,000 every 100. The run is `CHURN_BUCKETS` buckets long, six by default (110 batches at
10,000 keys, 610 at 100,000), which leaves five measured sweeps at 100,000 keys once the
first boundary after the seed has gone into the warm-up. The residual-heavy shape runs two
buckets at 100,000 keys (`CHURN_BUCKETS=2`, 210 batches, one measured sweep per run): its
ring-backed residual scans the whole map on every seal, so a bucket of 100 batches costs
two minutes there. Every seal of the run is then one of three kinds, and all three are in
the steady gates: over existing keys, over keys the batch just added, or - once per bucket -
the seal after a sweep, which stays incremental but carries one removal per evicted key on top
of the keys it imaged. The aggregator gates that last kind separately as `swept_seal_ms_median`
and reports the sweep's own `sweep_ms`, the keys it evicted and what the seal after it walked
(`win_visited` = `win_imaged` + `win_removed` on a correct incremental seal). Every churn run
ends with the result oracle, since an eviction that dropped state the view still needed would
show as wrong rows rather than as time.

The warm-up seal after the first sweep is larger than the rest - the seed's rows straddle two
buckets, so the first sweep evicts more than `K/2` - which is one more reason the first ten
batches are dropped.

## The cold-restore cell

`run-matrix.sh ... cold-restore` repeats the steady rows with `--restart-cache=cold`. The
run is the same; the restart at its end differs. A warm restart - every earlier cell - reads
its checkpoint back on cold code (the JVM restores once) over a warm page cache (the pages its
own seals just wrote). The cold one first releases the engine's pooled readers and writers,
then fsyncs every file under the database root and advises it `POSIX_FADV_DONTNEED`, so the
restore also pays the disk. `# restore` reports `cache=cold` and what was advised, and the
aggregator fails a cold cell whose runs did not all report it.

`posix_fadvise` is advice, so the eviction was checked once with [residency.py](residency.py)
against a run paused after it: the checkpoint tree read 0 of 14.4 MB resident at 100,000 keys
and the whole root 14%. What stays resident are the symbol-map files (`account_id.k/.v/.c/.o`)
of the base and the view, about 12 MB at 100,000 keys, which the WAL writer pool keeps mapped
and the eviction cannot reach; they are the same on both revisions and are not what the restore
reads. Only the restore figures of a cold cell are read - its steady batches are the steady
cell's workload again. Linux only.

## Structural evidence

The candidate's per-batch output carries the capture ledger:

| Column | Meaning |
| --- | --- |
| `win_caps`, `win_inc` | window roots the batch's seal froze, and how many of them were incremental |
| `win_visited` | rows the window walk read - the dirty map's for an incremental capture, the whole anchor map's for a complete one |
| `win_imaged`, `win_removed` | keys the window root published an entry for, and keys it named as removals |
| `fn_roots`, `fn_inc` | function roots the seal froze, and how many were incremental |
| `fn_visited`, `fn_imaged` | rows those roots' walks read, and keys they imaged |
| `alloc_mb` | Java allocation of the batch's refresh, on the thread that ran it |

`win_visited` against `map_rows` is the structural gate: a steady incremental seal must
read the 1000 keys the batch changed rather than the `K` the view holds. Neither the
published artifacts nor the elapsed time can carry that claim - an incremental root and a
complete one both name the whole live domain, and a complete walk of a small domain beats
an incremental walk that had to map an older segment to compare against.

These columns are candidate-only production instrumentation. The baseline reports -1 for
all of them; see `baseline-harness.patch`.

## Running it

```bash
# candidate
mvn -pl benchmarks -am package -o -DskipTests -Dmaven.test.skip=true
./run-matrix.sh benchmarks/target/benchmarks.jar /tmp/matrix/candidate "10000 100000" 5
# the 1,000,000-key scaling run covers the anchor-only and single-SUM shapes only
MATRIX_SHAPES=anchor-only-decimal,anchor-only-unfused-control,narrow-sum \
    ./run-matrix.sh benchmarks/target/benchmarks.jar /tmp/matrix/candidate "1000000" 5
# the repair cell
./run-matrix.sh benchmarks/target/benchmarks.jar /tmp/matrix/candidate "1000 10000" 5 repair
# the add/remove-keys cell and the cold-cache restore, both over every steady shape
./run-matrix.sh benchmarks/target/benchmarks.jar /tmp/matrix/candidate "10000 100000" 5 churn
./run-matrix.sh benchmarks/target/benchmarks.jar /tmp/matrix/candidate "10000 100000" 5 cold-restore

# baseline: the same harness with the candidate-only pieces removed
git clone --local --no-checkout . /tmp/baseline-repo
git -C /tmp/baseline-repo checkout --detach 6a2c656028
cp benchmarks/src/main/java/org/questdb/LiveViewSteadyStateBenchmark.java \
   benchmarks/src/main/java/module-info.java /tmp/baseline-repo/benchmarks/src/main/java/...
git -C /tmp/baseline-repo apply .../baseline-harness.patch
(cd /tmp/baseline-repo && mvn -pl benchmarks -am package -o -DskipTests -Dmaven.test.skip=true)
./run-matrix.sh /tmp/baseline-repo/benchmarks/target/benchmarks.jar /tmp/matrix/baseline "10000 100000" 5
MATRIX_SHAPES=repair-closed-whole \
    ./run-matrix.sh /tmp/baseline-repo/benchmarks/target/benchmarks.jar /tmp/matrix/baseline "1000 10000" 5 repair
./run-matrix.sh /tmp/baseline-repo/benchmarks/target/benchmarks.jar /tmp/matrix/baseline "10000 100000" 5 churn
./run-matrix.sh /tmp/baseline-repo/benchmarks/target/benchmarks.jar /tmp/matrix/baseline "10000 100000" 5 cold-restore

./summarize-matrix.py /tmp/matrix/baseline /tmp/matrix/candidate --md
```

Every run passes `-Dout=quiet-log.conf`, which sends the server log to a file. The default
configuration writes it to stdout on a writer thread that does not share `System.out`'s
lock, so a log record lands inside a printf'd report line often enough to corrupt a run's
output; the driver does this for you.

Run the two revisions on the same machine and filesystem with the same JVM, heap, worker
count, input, maintenance settings and cache policy. `run-matrix.sh` fixes every one of
those except the machine.

`run-matrix.sh` skips a run whose output file already exists, so calling it with `RUNS`
equal to 1, 2, 3, ... alternately for the two jars interleaves the revisions run by run.
`MATRIX_FUSION=true|false` restricts a call to one runtime mode, for re-measuring one cell.
Each run's stderr goes to a `.err` file beside its output, and a run that fails keeps its
partial output under `.failed.tsv`; the aggregator reads neither.
The one-shot measurements - the complete seal after the seed, the restore and its first
reseal - are read once per JVM on cold code, and a run-to-run spread of 20% on them is
ordinary at 10,000 keys; the remaining cells were measured this way with 15 runs per cell
so that a drift in the machine lands on both sides alike and the medians settle. The churn
and cold-restore cells were measured the same way, with the load average logged before each
revision's turn:

```bash
for run in 1 2 3 4 5; do
  for mode in churn cold-restore; do
    for rev in baseline candidate; do
      jar=benchmarks/target/benchmarks.jar
      [ "$rev" = baseline ] && jar=/tmp/baseline-repo/benchmarks/target/benchmarks.jar
      echo "$(date +%T) loadavg=$(cut -d' ' -f1-3 /proc/loadavg) $rev $mode run=$run" >> /tmp/matrix/load.log
      ./run-matrix.sh "$jar" "/tmp/matrix/$rev" "10000 100000" "$run" "$mode"
    done
  done
done
```
