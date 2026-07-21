# HORIZON JOIN false-sharing evidence

This directory contains the retained text and CSV evidence for the
`AsyncHorizonTimestampIterator` false-sharing case study. The source baseline
is QuestDB commit `f2c567869d623d776947f1b43f6caac5ddac4600`; the candidate
fix adds a 64-byte padding superclass and a layout regression test.

The comparison uses QuestDB's bundled jemalloc. The two shaded benchmark jars
contain 13,353 common file entries. Exactly one common class changed and one
padding class was added; see `jar-diff.txt`.

## Workload

`setup.sql` creates two synthetic two-million-row, timestamp-ordered tables.
`query.sql` calculates a four-point markout curve with HORIZON JOIN:

```sql
SELECT
    h.offset / 1000 AS horizon_ms,
    avg(q.mid - t.price) AS avg_markout
FROM markout_trades t
HORIZON JOIN markout_quotes q
    LIST (0, 1000U, 2000U, 3000U) AS h
ORDER BY h.offset;
```

The plan contains `Async Horizon Join workers: 15 offsets: 4`. Each request
returns the same four rows:

```text
horizon_ms  avg_markout
0           0.0
1           0.9999995
2           1.9999985
3           2.999997
```

The server was restricted to physical CPUs 0-14. Its 15 query workers were
pinned one per CPU; a persistent HTTP client ran on physical CPU 15. SMT
siblings 16-31 were unused. `server.conf.fragment` contains the relevant
configuration.

## Host

- AMD Ryzen 9 9950X, 16 physical cores / 32 hardware threads
- one socket and one NUMA node
- Linux `7.0.0-27-generic`
- `perf 7.0.12`
- OpenJDK `25.0.3+9-2-26.04.2-Ubuntu`
- bundled jemalloc SHA-256
  `19554c54e4c942d238c7271a150320f5b8673ff7f24b0bf6ae92a6875ba7f4b1`

Every server process was started with an explicit `LD_PRELOAD`. The runner
then required the exact jemalloc pathname to appear in `/proc/<pid>/maps`.

## End-to-end result

Six fresh server processes per variant ran in the counterbalanced order
`B F F B B F F B B F F B`. Each process served 100 unrecorded warmups and
500 recorded sequential requests over one persistent loopback connection.
All 6,000 recorded requests returned an invariant result.

| variant | per-process means (ms/request) | median |
| --- | --- | ---: |
| baseline | 26.007, 23.666, 27.862, 23.306, 31.280, 24.505 | 25.256 |
| fixed | 20.722, 15.491, 15.411, 15.351, 18.975, 16.150 | 15.820 |

The median end-to-end latency fell **37.4%**, equivalent to **1.60x** the
throughput for this sequential-request workload. `e2e-results.csv` retains
every process result rather than only the aggregate.

## `perf c2c` result

After 100 warmup requests, each variant served another 500 requests while
`perf c2c record` was attached to the server PID. On this AMD host, `perf`
selected `ibs_op/ldlat=0/` at a sample frequency of 4,000.

| sampled statistic | baseline | fixed | reduction |
| --- | ---: | ---: | ---: |
| load operations | 236,538 | 170,685 | - |
| local HITM | 2,387 | 23 | - |
| remote HITM | 48 | 4 | - |
| total HITM | 2,435 | 27 | 98.9% |
| HITM / 1,000 sampled loads | 10.294 | 0.158 | 98.5% |
| HITM in `processHorizonTimestamps` | 2,259 | 0 | 100% |

The target method accounts for 92.8% of the baseline HITM samples. The 2,259
target samples collapse onto 11 hot lines shared by adjacent query-worker
pairs, plus three singleton samples. `hot-cache-lines.csv` retains that
grouping.

HITM counts are samples, not exact cache-to-cache transfer counts. The
normalized ratio is useful because the fixed recording finishes sooner and
therefore contains fewer samples. This is a one-socket host, so the small
`remote HITM` count must not be read as cross-socket traffic.

## From a hot line to a Java object

`BaseAsyncHorizonJoinAtom` allocates one iterator per worker in a tight loop.
Each iterator constructor immediately allocates two `long[4]` heaps and one
`int[4]` heap. A separate HotSpot HPROF dump under the same query shape showed
these iterator/array groups allocated back-to-back. The HPROF file is not
checked in; its hash is in `artifact-sha256.txt`.

The original mutable fields start at object offsets 12 and 16. The first
padding attempt added eight `long` fields in a superclass, but HotSpot filled
the four-byte hole after the object header with the subclass's
`currentOffsetIdx`; the hot field remained at offset 12. The final superclass
adds an `int p0` before its eight longs, moving every iterator field to offset
80 or later. `field-layout.txt` records all three layouts.

An exact-query diagnostic recording rejected that first attempt: 1,052 total
HITM samples remained, 942 of them in `processHorizonTimestamps`. Those values
are retained separately in `failed-first-fix.txt`; they are not mixed into the
final baseline/fixed table.

The fix is intentionally guarded by
`AsyncHorizonTimestampIteratorTest.testLiveFieldsDoNotUseObjectBoundaryCacheLine`.
It checks the actual field offsets on the build JVM and fails if any live
iterator field moves back into the first 64 bytes.

## Reproduce

1. Put the settings from `server.conf.fragment` in the benchmark dbroot.
2. Start QuestDB once and execute `setup.sql`.
3. Build baseline and fixed shaded benchmark jars in separate worktrees.
4. Run the end-to-end comparison:

   ```bash
   ./run-e2e.sh BASELINE_JAR FIXED_JAR DB_ROOT OUTPUT_DIR
   ```

5. Run the two `perf c2c` recordings:

   ```bash
   ./run-c2c.sh BASELINE_JAR FIXED_JAR DB_ROOT OUTPUT_DIR
   ```

Both runners accept `SERVER_CPUS`, `CLIENT_CPU`, `WARMUPS`, and `REQUESTS`
environment overrides. They default to the topology and request counts above.
The scripts deliberately accept two prebuilt jars: they never mutate the
caller's checkout to manufacture a baseline.

For each c2c variant, `run-c2c.sh` writes the raw recording, copied JIT map,
compact statistics, full `perf c2c` report, decoded HITM samples and grouped
cache-line CSV beneath the caller-supplied output directory.

Raw `perf.data`, JIT maps, heap dumps, server logs, and `/proc/<pid>/maps`
files remain outside git because they are large or process-specific. Hashes of
the exact recordings used for the tables above are retained in
`artifact-sha256.txt`.

## Retained files

- `setup.sql`, `query.sql`, `server.conf.fragment`: workload definition
- `query_client.py`: persistent HTTP client with result-invariance checking
- `run-e2e.sh`: six-process-per-variant counterbalanced benchmark
- `run-c2c.sh`: warmup, JIT map, recording, and report workflow
- `group_hitm.py`: groups raw `perf script` HITM samples into 64-byte lines
- `summarize.py`: rebuilds result CSVs from an external run directory
- `e2e-results.csv`: all 12 process results
- `c2c-results.csv`: normalized recording summary
- `c2c-*-stats.txt`: verbatim `perf c2c report --stats` output
- `hot-cache-lines.csv`: baseline target samples grouped at 64-byte boundaries
- `tutorial-raw-output.txt`: compact, verbatim output from the recorded session
- `field-layout.txt`: original, failed-padding, and corrected field offsets
- `failed-first-fix.txt`: the diagnostic rerun that rejected long-only padding
- `jar-diff.txt`, `artifact-sha256.txt`: isolation and artifact identity
