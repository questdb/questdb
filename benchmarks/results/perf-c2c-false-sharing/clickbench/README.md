# ClickBench `perf c2c` query sampling

This sample checks whether several representative queries expose another
concentrated, query-owned false-sharing cache line after the
`SimpleMapValue` padding fix.

## Setup

- 99,997,497-row ClickBench `hits` table
- QuestDB server pinned to physical CPUs 0-14; persistent HTTP client pinned
  to physical CPU 15; SMT siblings unused
- QuestDB's bundled Linux x86-64 jemalloc explicitly preloaded
- five-second `perf c2c record` window at 4,000 samples/second on CPUs 0-15
- portable `mem-ldst` selector, resolved by `perf` to `ibs_op/ldlat=0/`
- fresh server process for every sample
- fixed QuestDB build for the sampled ClickBench queries
- baseline and fixed builds for the known false-sharing calibration query

The sampled queries cover a non-keyed aggregate, low- and high-cardinality
keyed group-bys, a composite-key group-by, and two varchar-key group-bys. The
full SQL text and execution plan are retained beside each raw recording.

`sample-summary.csv` reports total HITM normalized by sampled loads. The ratio
is useful for comparing these recordings, but it is not an exact hardware
event rate.

## Result

The baseline calibration is the only confirmed false-sharing signature. Its
adjacent `SimpleMapValue` lines account for thousands of HITM samples. Padding
reduces total HITM per 1,000 sampled loads from 40.812 to 5.260 and removes the
adjacent-line pattern.

None of Q2, Q8, Q12, Q15, or Q32 exposes another concentrated query-owned
cache line. Their most visible individual lines are either QuestDB background
job synchronization (`SynchronizedJob` and `QueryTracingJob`) or isolated
samples spread across the group-by maps. Those are not the independent-field,
same-line pattern seen in the calibration.

Q36 initially looked suspicious. A native line attributed to
`UnorderedVarcharMap` carried 95 HITM samples, or 20.9% of the recording's
HITM. A fresh-JVM repeat found the same pattern at a different virtual address:
138 HITM samples, or 31.9% of that recording's HITM.

The detailed trace does not support a false-sharing diagnosis:

- the same virtual line is sampled in both `FastGroupByAllocator.close()` and
  `UnorderedVarcharMap.reopen()`;
- samples move across all query workers as native directories are freed and
  recycled by jemalloc;
- each of the 256 destination shards is exclusively owned by one merge task at
  a time, so two merge workers do not concurrently update one live shard map;
- `perf c2c` groups by address and therefore combines samples from different
  allocation lifetimes when jemalloc reuses an address.

`VarcharMapAllocationLayoutProbe.java` separately checks the tempting
32-byte-allocation explanation. It creates 4,352 lazy varchar maps and reopens
them concurrently on 15 threads. Eight fresh jemalloc JVMs found zero cache
lines containing live byte-sink allocations owned by different workers. Each
run did place some allocations from multiple maps on one line (3 to 574 such
lines), but all maps on each such line belonged to the same worker and were
accessed serially.

This is real cache movement caused by allocation and task handoff, but the
recording does not show two simultaneously live, independent values sharing a
cache line. It is a useful `perf c2c` false-positive case, not a second
confirmed false-sharing bug.

## Artifacts

The raw data is retained outside the repository at:

```text
/tmp/questdb-clickbench-false-sharing-20260721/c2c-official-samples
```

Each sample directory contains `perf.data`, the JIT perf map, the full and
summary `perf c2c` reports, the query, the plan, client results, server log,
recording metadata, and allocator proof from `/proc/<pid>/maps`.

Run `run-c2c-samples.sh` with the baseline jar, fixed jar, cloned database
root, ClickBench `queries.sql`, and output directory to reproduce the set. The
script resumes without replacing complete recordings. Compile
`VarcharMapAllocationLayoutProbe.java` against the fixed shaded jar and run it
with the bundled jemalloc preloaded to repeat the live-allocation check.
