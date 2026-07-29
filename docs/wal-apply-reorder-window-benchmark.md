# WAL Apply Reorder Window Benchmark

Date: 2026-07-29

Source: working tree based on `0e12b8665922`

Host:

- AMD Ryzen 9 9950X, 16 cores / 32 threads
- Linux 7.0.0-28-generic
- Amazon Corretto 26.0.1
- `/mnt/pcie5` on `/dev/nvme0n1p2`

The benchmark uses four WAL tables. Each iteration commits a 250,000-row
newer-timestamp transaction and a 250,000-row older-timestamp transaction per
table. The disabled runs drain WAL between the commits. The enabled run
consumes one apply notification between commits and uses a 50ms reorder
window. Every table is configured so that the two-transaction pair fits the
existing WAL block limit.

Each scenario has three warmup iterations and twenty measured iterations,
for 40 million measured logical rows. The run order is disabled A, enabled,
disabled B.

```bash
/home/jara/.sdkman/candidates/java/26.0.1-amzn/bin/java \
  -ea \
  --sun-misc-unsafe-memory-access=allow \
  --enable-native-access=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.time.zone=ALL-UNNAMED \
  --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
  -cp benchmarks/target/benchmarks.jar \
  org.questdb.WalApplyReorderWindowBenchmark \
  /mnt/pcie5 250000 20 3 50000 4
```

| Scenario | Window | Logical rows | Physical rows | Write amplification | O3 commits | Apply commits | End-to-end rows/s | Apply rows/s | Process CPU | JVM allocation | Visibility p50 | Visibility p99 | Max table spread |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| disabled A | 0 | 40,000,000 | 60,000,000 | 1.500 | 160 | 160 | 63,220,721 | 141,214,215 | 1,140ms | 0.254MiB | 10.849ms | 17.576ms | 8.385ms |
| enabled | 50ms | 40,000,000 | 40,000,000 | 1.000 | 80 | 80 | 19,266,159 | 54,893,275 | 1,700ms | 0.207MiB | 70.844ms | 103.487ms | 42.817ms |
| disabled B | 0 | 40,000,000 | 60,000,000 | 1.500 | 160 | 160 | 69,561,211 | 167,993,868 | 1,010ms | 0.159MiB | 8.756ms | 15.436ms | 7.080ms |

The enabled run reduced physical rows by 33.33% and halved the O3 and table
apply commit counts. End-to-end throughput includes the intentional 50ms wait
and was 70.98% below the mean disabled result.

This narrow-table workload did not show a CPU-throughput win: process CPU per
row was 58.14% above the mean disabled result, and apply throughput was lower.
Allocation per row was 0.31% above the disabled mean, well inside the 37.25%
spread between the two tiny disabled allocation samples. Disabled A-to-B
drift was +10.03% for throughput and -11.40% for CPU per row.

These results establish the storage-write reduction and latency trade-off.
They do not justify a nonzero production default or establish a disabled-mode
regression budget. A merge-time budget needs a dedicated before/after run with
enough repetitions to characterize host variance.
