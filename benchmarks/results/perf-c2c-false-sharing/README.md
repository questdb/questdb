# `perf c2c` false-sharing case study evidence

This directory contains the retained text and CSV evidence for the QuestDB
`SimpleMapValue` false-sharing case study. The code under test was QuestDB
commit `f2c567869d623d776947f1b43f6caac5ddac4600` plus the isolated padding fix.
The primary results explicitly preload QuestDB's bundled jemalloc, matching the
allocator selected by the packaged Linux x86-64 launcher.

## Host and workload

- AMD Ryzen 9 9950X, 16 physical cores / 32 hardware threads, one socket and
  one NUMA node
- Linux `7.0.0-27-generic`, `perf 7.0.12`
- OpenJDK `25.0.3+9-2-26.04.2-Ubuntu`
- bundled jemalloc SHA-256
  `19554c54e4c942d238c7271a150320f5b8673ff7f24b0bf6ae92a6875ba7f4b1`
- 20 million rows in 16 day partitions
- query: `SELECT sum(px) FROM ref WHERE sym = 'sel50'`
- `sel50` selects exactly half the generated rows
- JMH: 16 query workers on CPUs 0-15
- HTTP: the whole server on physical cores 0-14 and the persistent client on
  physical core 15; CPUs 16-31, the SMT siblings, were unused

The exact generator and query routing live in
`CoveredIndexDecodeBenchmark.java`. `PARALLEL_REF` selects the non-indexed
`ref` twin. Both JMH and the HTTP server used a maximum page-frame size of
100,000 rows. The HTTP server used this relevant configuration:

```properties
shared.network.worker.count=1
shared.query.worker.count=15
shared.write.worker.count=1
cairo.sql.page.frame.max.rows=100000
line.tcp.enabled=false
pg.enabled=false
telemetry.enabled=false
```

Both rerun scripts set `LD_PRELOAD` themselves. All 12 HTTP server processes
were checked through `/proc/<pid>/maps`; the retained files in
`allocator-maps/` show the exact bundled library mapped in every process.
Restricting the JVM to a subset of the online CPUs made this jemalloc build
disable its per-CPU arena mode. The `SimpleMapValue` objects are allocated
sequentially by one setup thread, and `allocator-spacing.txt` records the
resulting size-class layout directly.

## Independent-process results

The two shaded jars were built from the same tree and benchmark harness. They
have identical `Misc` and benchmark class files and differ only in the
`SimpleMapValue` class file: the baseline has the original 32-byte allocation;
the fixed jar adds one 64-byte gap. The layout fix deliberately uses QuestDB's
existing 64-byte cache-line contract. Six independent JVM/server processes per
variant ran in the counterbalanced order `ABBA ABBA ABBA`.

JMH used three one-second warmup iterations followed by eight one-second
measurement iterations in every process. These are the six process means:

| variant | process means (ms/op) | mean | process SD |
| --- | --- | ---: | ---: |
| baseline | 6.445, 6.066, 6.242, 6.247, 6.301, 6.392 | 6.282 | 0.133 |
| fixed | 2.462, 2.493, 2.453, 2.456, 2.471, 2.463 | 2.466 | 0.014 |

The mean latency reduction is 60.7%. The standard deviations above describe
variation between the six independent process means, not JMH's within-process
confidence interval.

Each HTTP process served 100 unrecorded warmups and 1,000 recorded sequential
requests over one persistent loopback connection. All 12,000 recorded requests
returned HTTP 200 and kept the connection open.

| variant | process means (ms) | mean | process SD |
| --- | --- | ---: | ---: |
| baseline | 6.868, 5.782, 5.944, 6.092, 5.834, 6.750 | 6.212 | 0.476 |
| fixed | 3.026, 3.040, 3.039, 3.035, 2.996, 2.997 | 3.022 | 0.021 |

The end-to-end mean latency reduction is 51.3%. The median of the six per-trial
p50 values moved from 5.698 ms to 2.929 ms; the median per-trial p95 moved from
6.873 ms to 3.429 ms.

## `perf c2c`

Both three-second recordings used the event reported by `perf` as
`ibs_op/ldlat=0/`, at sample frequency 4,000, on CPUs 0-15. The event includes
IBS Op load and store samples; `ldlat=0` means the load-latency filter was
disabled. The raw recordings had zero lost samples.

| statistic | baseline | fixed |
| --- | ---: | ---: |
| total records | 176,342 | 191,759 |
| sampled loads | 31,055 | 72,121 |
| local HITM | 288 | 33 |
| remote HITM | 10 | 5 |
| total HITM / 1,000 sampled loads | 9.596 | 0.527 |

The normalized HITM count fell 94.5%. This is a ratio of sampled categories,
not an exact hardware-event rate. On this single-socket, single-NUMA-node host,
`remote HITM` does not imply a remote socket.

The raw `perf.data`, JIT maps and full text reports are not checked in because
they total about 54 MB. Their hashes are retained so an external archive can be
verified:

```text
1f1932fc1dc670ab0d423542911d37180ae70abab9dd2c00ffdea74e4b4f1d5f  baseline perf.data
ab819a0061f3d45a7742c5793d278fdbf22be4e0cfb9790ab7f5745351d13b31  fixed perf.data
113bd8526f9805865904dbdab082c50b849bcd70e2ea4f346327d6da9bfe8f8c  baseline perf map
e7d8325b6109ba165c96cbf71e9cb1dd46234ca6231e0320a73d03aa68d210d4  fixed perf map
b3a24ef53c4816bbc9a0ccf8fe919480830f16ed93b1b8ccefd8adcf4cd0a0ce  baseline full c2c text report
764e5d9a09019095396601e304d500d853b63c94d3ca16f167821ad6618a53b1  fixed full c2c text report
```

Set `DEBUGINFOD_URLS=` when decoding these local JIT traces; otherwise `perf`
may pause while trying Ubuntu's remote debuginfod service.

## Files

- `jmh-raw.csv`: all eight measured JMH iterations from all 12 JVMs
- `jmh-processes/`: original JMH JSON and human-readable log from every JVM
- `http-raw.csv`: all 12,000 measured HTTP requests
- `summary.csv`: per-process JMH and HTTP summaries
- `allocator-spacing.txt`: fresh-JVM baseline allocation addresses and deltas
- `allocator-maps/`: `/proc/<pid>/maps` proof for all 12 HTTP server processes
- `c2c-*-stats.txt`: verbatim `perf c2c report --stats` output
- `perf-*-header.txt`: verbatim recording metadata, event configuration and
  host topology from `perf report --header-only -I`
- `run-jmh-trials.sh`: independent-JVM ABBA schedule
- `run-http-trials.sh` and `http_bench.py`: isolated server/client ABBA schedule
- `summarize.py`: rebuilds the combined CSV files from per-process raw outputs
- `clickbench/`: scripts, normalized sample results, and interpretation for the
  99,997,497-row ClickBench `perf c2c` query sweep

The shaded-jar hashes were:

```text
00a7ad8e98e3c3b27d7242fc26cb89d2acc224a8f8595f20a8625d2abb26c842  baseline.jar
20493895e6b3c6288bd3b23ee2d87e51bac81aef575402650eb10c25d831d07d  fixed.jar
```
