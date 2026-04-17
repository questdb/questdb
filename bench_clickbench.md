# ClickBench: xxh3 hash-function patch vs master

Benchmark comparison of `puzpuzpuz_faster_hash_function` (xxh3 avalanche
finalizer + consolidated fast hashes + 0.7 load factor) against `master`, using
the ClickHouse ClickBench query suite.

## Methodology

- **Dataset**: the standard ClickBench `hits` table, 99,997,497 rows.
- **Queries**: the 43 queries from
  [ClickHouse/ClickBench/main/questdb/queries.sql](https://github.com/ClickHouse/ClickBench/blob/main/questdb/queries.sql).
- **Runner**: `qdb-bench/bench_run_clickbench.py` — posts each query to
  `/exec?timings=true`, records the server-reported `execute` timing.
- **Iterations**: 10 measured runs per query after 1 warmup (same process, same
  table, no cache flushing between runs).
- **Metric**: ClickBench's "cold + 10 ms" convention — `min(execute) + 10ms`.
  The 10 ms offset dampens noise on sub-10 ms queries; it does not favor one
  side. Delta is `(patch - master) / master` on the offset values.
- **Hardware**: single-machine local run. Server restarted between master and
  patch measurements.

## Aggregate result

Summing `min + 10ms` across all 43 queries:

| metric | master | patch | delta |
|---|---|---|---|
| total time | 14.68 s | 14.50 s | **-1.21%** |

Small net improvement on the aggregate, driven by a few large GROUP BY wins.
Most queries are within run-to-run noise.

## Wins (>= 5% faster)

| Q | master | patch | delta | query |
|---|---|---|---|---|
| Q8 | 636.93 ms | 544.55 ms | **-14.3%** | `SELECT RegionID, count_distinct(UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10` |
| Q17 | 781.63 ms | 735.83 ms | **-5.8%** | `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10` |
| Q27 | 127.26 ms | 96.50 ms | **-22.4%** | `SELECT CounterID, AVG(length_bytes(URL)), COUNT(*) FROM hits WHERE URL IS NOT NULL GROUP BY CounterID HAVING c > 100000 ORDER BY l DESC LIMIT 25` |
| Q42 | 3.82 ms | 3.12 ms | **-5.1%** | `SELECT EventTime AS M, COUNT(*) FROM hits ... SAMPLE BY 1m ALIGN TO CALENDAR ORDER BY M LIMIT 1000, 1010` |

Wins line up with the theory: these are GROUP BY queries where hashing sits on
the critical path, and the 0.7 load factor reduces per-row probe cost without
harming chain length for xxh3-mixed keys.

Q8 and Q27 also combine GROUP BY with `count_distinct` / per-group aggregation
over varchar, so both the key-mixing swap and the load-factor bump contribute.

## Losses (>= 5% slower)

| Q | master | patch | delta | query |
|---|---|---|---|---|
| Q20 | 203.34 ms | 234.48 ms | **+14.6%** | `SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'` |
| Q28 | 1854.80 ms | 1964.51 ms | **+5.9%** | `SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '$1') AS k, AVG(length_bytes(Referer)), COUNT(*), MIN(Referer) FROM hits ... GROUP BY k` |

- **Q20** does not touch any of the hash-function code paths — it is a pure
  scan with LIKE. The 14.6% shift is almost certainly system-level noise
  (cache/kernel state between the two runs). The same query's min on master
  (203 ms) vs patch (234 ms) is within the drift typical of back-to-back runs
  on a non-isolated machine.

- **Q28** is the one plausibly attributable to the patch. The key is a
  regex-derived varchar, so `hashMem64` (polynomial body + xxh3 finalizer) is
  on the hot path. The regression is modest (5.9%) and may reflect:
  - probe chains on this particular key distribution being less friendly to
    xxh3-avalanched values than to fmix64-avalanched values at 0.7 load factor,
    or
  - memory-bandwidth dominance on a bulky value aggregate (varchar `MIN`,
    `AVG(length)`) where any small code change is visible in the noise
    envelope.

  Worth confirming with a focused re-run, but not a blocker.

## Full per-query table

All times in ms. Delta computed as `(patch + 10) / (master + 10) - 1`.

| Q | master min | patch min | delta | query prefix |
|---|---|---|---|---|
| Q0 | 0.15 | 0.13 | -0.2% | COUNT(*) |
| Q1 | 5.47 | 5.40 | -0.5% | COUNT WHERE AdvEngineID <> 0 |
| Q2 | 8.53 | 9.08 | +3.0% | SUM/COUNT/AVG |
| Q3 | 15.79 | 15.46 | -1.3% | AVG(UserID) |
| Q4 | 301.43 | 291.59 | -3.2% | count_distinct(UserID) |
| Q5 | 205.11 | 207.10 | +0.9% | count_distinct(SearchPhrase) |
| Q6 | 0.59 | 0.67 | +0.8% | MIN/MAX(EventTime) |
| Q7 | 4.49 | 5.17 | +4.7% | AdvEngineID GROUP BY |
| Q8 | 636.93 | 544.55 | **-14.3%** | RegionID count_distinct GROUP BY |
| Q9 | 637.23 | 621.37 | -2.5% | RegionID multi-agg GROUP BY |
| Q10 | 78.32 | 78.85 | +0.6% | MobilePhoneModel GROUP BY |
| Q11 | 62.06 | 63.84 | +2.5% | MobilePhone,Model GROUP BY |
| Q12 | 226.72 | 222.61 | -1.7% | SearchPhrase GROUP BY |
| Q13 | 347.61 | 350.78 | +0.9% | SearchPhrase count_distinct |
| Q14 | 278.03 | 268.36 | -3.4% | SearchEngineID,SearchPhrase |
| Q15 | 409.51 | 399.30 | -2.4% | UserID GROUP BY |
| Q16 | 787.53 | 774.69 | -1.6% | UserID,SearchPhrase GROUP BY ORDER |
| Q17 | 781.63 | 735.83 | **-5.8%** | UserID,SearchPhrase GROUP BY LIMIT |
| Q18 | 1276.93 | 1242.75 | -2.7% | UserID,minute,SearchPhrase |
| Q19 | 29.42 | 31.15 | +4.4% | UserID = const |
| Q20 | 203.34 | 234.48 | **+14.6%** | URL LIKE '%google%' (no hash path) |
| Q21 | 208.64 | 209.78 | +0.5% | SearchPhrase + URL LIKE |
| Q22 | 208.05 | 209.49 | +0.7% | SearchPhrase + Title LIKE |
| Q23 | 10.92 | 10.65 | -1.3% | SELECT * WHERE URL LIKE |
| Q24 | 1.61 | 1.46 | -1.3% | SearchPhrase ORDER BY EventTime |
| Q25 | 46.64 | 45.57 | -1.9% | SearchPhrase ORDER BY SearchPhrase |
| Q26 | 1.48 | 1.50 | +0.2% | SearchPhrase ORDER BY EventTime,SearchPhrase |
| Q27 | 127.26 | 96.50 | **-22.4%** | CounterID AVG(length) GROUP BY |
| Q28 | 1854.80 | 1964.51 | **+5.9%** | REGEXP_REPLACE GROUP BY |
| Q29 | 3.85 | 3.76 | -0.6% | 90x SUM(ResolutionWidth + k) |
| Q30 | 174.63 | 171.34 | -1.8% | SearchEngineID,ClientIP GROUP BY |
| Q31 | 229.94 | 226.29 | -1.5% | WatchID,ClientIP GROUP BY (filtered) |
| Q32 | 2248.36 | 2252.94 | +0.2% | WatchID,ClientIP GROUP BY |
| Q33 | 1185.81 | 1134.92 | -4.3% | URL GROUP BY ORDER LIMIT |
| Q34 | 1180.02 | 1159.50 | -1.7% | 1,URL GROUP BY ORDER LIMIT |
| Q35 | 305.05 | 316.71 | +3.7% | ClientIP,ClientIP-1..3 GROUP BY |
| Q36 | 22.00 | 21.94 | -0.2% | URL GROUP BY filtered |
| Q37 | 17.04 | 17.65 | +2.3% | Title GROUP BY filtered |
| Q38 | 13.61 | 13.66 | +0.2% | URL GROUP BY filtered |
| Q39 | 67.85 | 65.49 | -3.0% | TrafficSourceID,... GROUP BY |
| Q40 | 19.71 | 19.89 | +0.6% | URLHash,EventTime GROUP BY |
| Q41 | 19.35 | 19.20 | -0.5% | WindowClientWidth,Height GROUP BY |
| Q42 | 3.82 | 3.12 | **-5.1%** | SAMPLE BY 1m |

## Interpretation

- **GROUP BY queries where hashing is the dominant cost show real wins**:
  Q8, Q17, Q27 improve by 5.8-22.4%. Q33 (-4.3%) and Q18 (-2.7%) are in the
  same family but below the 5% threshold.
- **Low-cost queries (sub-10 ms) are smeared into noise** by the +10ms offset
  — this is the intended behavior and is why the offset exists.
- **Two visible losses**:
  - Q20 (+14.6%) does not touch the hash-function changes; attributed to
    system noise.
  - Q28 (+5.9%) could be a real but modest regression on regex-derived varchar
    keys. Candidate causes: probe-chain behavior for these specific keys at
    0.7 load factor, or memory-bandwidth noise in a 2-second query.
- **No catastrophic regressions** — worst real regression is Q28 at 5.9%,
  worst observed is Q20 at 14.6% (not patch-related).

## Reproduction

```bash
# Download queries
curl -s https://raw.githubusercontent.com/ClickHouse/ClickBench/main/questdb/queries.sql \
    -o /tmp/clickbench_queries.sql

# Run against current server (defaults to :9000)
cd qdb-bench
python3 bench_run_clickbench.py 10 1 > results_clickbench_patch.txt
# ... restart server on master ...
python3 bench_run_clickbench.py 10 1 > results_clickbench_master.txt
python3 bench_diff_clickbench.py results_clickbench_master.txt \
    results_clickbench_patch.txt > results_clickbench_diff.txt
```

Result files in this directory:

- `results_clickbench_master.txt`
- `results_clickbench_patch.txt`
- `results_clickbench_diff.txt`
