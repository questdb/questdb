# Live-view checkpoint layout removal: performance acceptance matrix

Everything needed to repeat the matrix: the harness changes are in the benchmarks module,
the driver and the aggregator are here, and `RESULTS.md` holds a run's output with the
machine and settings it was produced on.

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

# baseline: the same harness with the candidate-only pieces removed
git clone --local --no-checkout . /tmp/baseline-repo
git -C /tmp/baseline-repo checkout --detach 6a2c656028
cp benchmarks/src/main/java/org/questdb/LiveViewSteadyStateBenchmark.java \
   benchmarks/src/main/java/module-info.java /tmp/baseline-repo/benchmarks/src/main/java/...
git -C /tmp/baseline-repo apply .../baseline-harness.patch
(cd /tmp/baseline-repo && mvn -pl benchmarks -am package -o -DskipTests -Dmaven.test.skip=true)
./run-matrix.sh /tmp/baseline-repo/benchmarks/target/benchmarks.jar /tmp/matrix/baseline "10000 100000" 5

./summarize-matrix.py /tmp/matrix/baseline /tmp/matrix/candidate --md
```

Run the two revisions on the same machine and filesystem with the same JVM, heap, worker
count, input, maintenance settings and cache policy. `run-matrix.sh` fixes every one of
those except the machine.
