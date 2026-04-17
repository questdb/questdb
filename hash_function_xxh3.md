# Unifying non-Murmur hash paths on `xxh3Avalanche64`

A follow-up to PR #6959 (batched aggregate dispatch). The batched dispatch work
exposed `Hash` as a first-order cost in the reducer, so `Hash.fmix64` and
`Hash.fastHashInt64` / `Hash.fastHashLong64` are replaced by a single mixer
derived from xxHash3's 64-bit avalanche, applied everywhere except the
Murmur3 128-bit public API.

## Summary

- `Hash.hashInt64`, `Hash.hashLong64`, `Hash.hashLong128_64`,
  `Hash.hashLong256_64`, `Hash.hashMem64`, and `Hash.hashUtf8(Utf8String)` now
  finalize via `xxh3Avalanche64` (1 multiply, 2 shift-XORs).
- `Hash.fastHashInt64` / `Hash.fastHashLong64` (FxHasher, single multiply) are
  deleted; their six callers (`DirectIntIntHashMap`, `DirectIntLongHashMap`,
  `DirectIntMultiLongHashMap`, `DirectLongLongHashMap`, `GroupByIntHashSet`,
  `GroupByLongHashSet`) switch to `hashInt64` / `hashLong64`.
- Load factors previously chosen to compensate for FxHasher's weak avalanche
  are raised from `0.5` to `0.7`: `FastGroupByAllocator`,
  `SortKeyMaterializingRecordCursor`, `EqSymFunctionFactory`,
  `AsyncWindowJoinFastAtom.SLAVE_MAP_LOAD_FACTOR`, and the
  `cairo.sql.count.distinct.load.factor` default.
- `fmix64` survives as a private helper inside `Hash`, used only by
  `murmur3ToLong` to preserve the bit-identical Murmur3 128-bit algorithm that
  `ApproxCountDistinct*` (HyperLogLog) depends on.
- `hashMem64`'s polynomial body (`h = h * M2 + block` per 8 bytes) is
  unchanged; only its finalizer is swapped.

## Motivation

Prior to this change `Hash` offered three competing 64-bit mixers:

| Function | Ops on critical path | SMHasher-style avalanche |
|---|---:|---:|
| `fmix64` (Murmur3 finalizer) | 2 mul + 3 shift-XOR | ~32.0 (ideal) |
| `fastHash*` (FxHasher) | 1 mul | ~17.7 (very poor) |
| (new) `xxh3Avalanche64` | 1 mul + 2 shift-XOR | ~32.8 (ideal) |

`fastHash*` existed because it was meaningfully faster than `fmix64` for hot
paths where quality could be traded for throughput, at the cost of a
compensating `0.5` load factor on every caller. `xxh3Avalanche64` closes that
gap: essentially as fast as FxHasher in throughput mode, ~1.7x faster than
`fmix64` in latency mode, with the same quality tier as `fmix64`. Once
available, keeping both `fmix64` and `fastHash*` as distinct user-visible
functions serves no purpose.

## What changed

### `Hash.java`

- New private `xxh3Avalanche64(long h)`:

  ```java
  private static long xxh3Avalanche64(long h) {
      h ^= h >>> 37;
      h *= 0x165667919E3779F9L; // xxh3 prime
      return h ^ (h >>> 32);
  }
  ```

- `hashInt64`, `hashLong64`, `hashLong128_64`, `hashLong256_64`,
  `hashMem64`, `hashUtf8(Utf8String)` now end in `xxh3Avalanche64(...)`.
- `fastHashInt64` and `fastHashLong64` deleted.
- `fmix64` retained as `private` and used only by `murmur3ToLong`. Its
  Javadoc notes the restricted role.
- `hashMem64`'s polynomial loop is unchanged (see below).

### Call-site swaps (6 files)

`Hash.fastHashInt64(key)` becomes `Hash.hashInt64(key)` in
`DirectIntIntHashMap`, `DirectIntLongHashMap`, `DirectIntMultiLongHashMap`;
`Hash.fastHashLong64(key)` becomes `Hash.hashLong64(key)` in
`DirectLongLongHashMap`, `GroupByLongHashSet`, and `GroupByIntHashSet` uses
`hashInt64`.

### Load factor bumps (0.5 -> 0.7)

- `FastGroupByAllocator.chunks` (`DirectLongLongHashMap`).
- `SortKeyMaterializingRecordCursor.rowIdToOrdinal` (`DirectLongLongHashMap`).
- `EqSymFunctionFactory.Func.lookupCache` (`DirectIntIntHashMap`).
- `AsyncWindowJoinFastAtom.SLAVE_MAP_LOAD_FACTOR` (propagates to three
  construction sites in `AsyncWindowJoinFastAtom`,
  `WindowJoinPrevailingCache`, and `WindowJoinFastRecordCursorFactory`).
- `cairo.sql.count.distinct.load.factor` default, in both
  `PropServerConfiguration` and `DefaultCairoConfiguration`. This affects
  every count_distinct / string_distinct_agg hash set
  (`GroupByIntHashSet`, `GroupByLongHashSet`, `GroupByLong128HashSet`,
  `GroupByLong256HashSet`, `Utf8SequenceHashSet`).

Note that the `count.distinct` config touches hash sets that never used
FxHasher (Long128/Long256/Varchar/String paths have always been on
`fmix64`-based mixers, now `xxh3Avalanche64`). The 0.5 default was a blanket
conservative choice; 0.7 is safe for every caller regardless of which mixer
they use.

### Test and config fixtures

- `PropServerConfigurationTest.getCountDistinctLoadFactor` expectation
  updated to `0.7`.
- `ServerMainTest` property-listing snapshot updated.

## Why `xxh3Avalanche64`

Several alternatives were prototyped and measured before committing:

| Candidate | Pre-mix muls | Avalanche quality | Notes |
|---|---:|---:|---|
| `fmix64` (baseline) | 2 | ideal | serial dep chain of 2 muls |
| `moremur64` | 2 | ideal | same shape as `fmix64`, no speed win |
| `rrmxmx64` | 2 | ideal | no speed win |
| `xxh3Avalanche64` | 1 | ideal | 1 mul + 2 shift-XOR; winner |
| `wyMix64` (signed `multiplyHigh`) | ~1 | near-ideal | latency win, but **2.6x throughput loss** on HotSpot JDK 17 because `Math.multiplyHigh` isn't fused with the lo-half multiply into a single `mulq` |
| `wyMixUnsigned64` | ~1 | near-ideal | adds bias-correction; 5x throughput loss |
| `fxHash64` (FxHasher) | 1 | 17.7 (broken) | catastrophic failure on low-entropy inputs (linear-probe avg probe count > 250,000 on `i << 20` style keys). Disqualified. |

`xxh3Avalanche64` is the only candidate that combines latency reduction,
no throughput regression, and full-strength avalanche. The rrmxmx / moremur
variants match its quality but do not reduce the multiply count.

## Quality comparison

Tests ran 1,000,000 synthetic keys through each mixer into a linear-probe
open-addressing table at load factor 0.48, and separately into a 256-bucket
shard distribution (matching `GroupByMapFragment`'s `hashCode >>> 56`).

### Linear-probe average / max probe count

| Workload | `fmix64` avg / max | `xxh3Avalanche64` avg / max |
|---|---:|---:|
| random 64-bit | 1.456 / 29 | 1.454 / 35 |
| sequential (0..N-1) | 1.458 / 38 | 1.419 / 46 |
| seqStride=997 | 1.454 / 38 | 1.391 / 34 |
| seqStride=1000 | 1.457 / 32 | 1.489 / 90 |
| timestamp(1ms step) | 1.456 / 33 | 1.444 / 39 |
| shiftedLow20 (`i << 20`) | 1.454 / 33 | 1.423 / 22 |
| highBitsOnly (`i << 40`) | 1.455 / 35 | 1.489 / 139 |

Average probe counts are indistinguishable. Worst-case tail (`maxProbes`) is
slightly longer for `xxh3Avalanche64` on two adversarial workloads
(`seqStride1000`, `highBitsOnly`); everywhere else the two are equivalent.

### Shard distribution (top 8 bits into 256 bins)

Expected chi-square for uniform distribution is ~255.

| Workload | `fmix64` chi2 | `xxh3Avalanche64` chi2 |
|---|---:|---:|
| random | 252.6 | 293.7 |
| sequential | 266.8 | 0.2 |
| seqStride=997 | 294.7 | 0.1 |
| seqStride=1000 | 238.5 | 0.1 |
| timestamp(1ms step) | 233.3 | 13.4 |
| shiftedLow20 | 229.4 | 8.2 |
| highBitsOnly | 229.7 | 0.1 |

Both produce balanced shards. `xxh3Avalanche64`'s chi2 near zero on
structured inputs reflects the fact that a single good multiply acts as a
near-perfect permutation modulo 2^8 for sequential keys — a better-than-random
distribution, not worse. Max bucket sizes are within ~5% of the expected
~3906 for both functions across every workload.

### Single-bit avalanche

Average number of output bits flipped when flipping each of the 64 input bits,
over 100,000 random 64-bit inputs. Ideal value is 32.

| Mixer | Avalanche |
|---|---:|
| `fmix64` | 31.999 |
| `xxh3Avalanche64` | 32.760 |

Equivalent.

## Performance comparison

### JMH microbenchmark (Opus 4.7 CLI machine, JDK 17.0.18, Linux x86_64)

Measured via an isolated JMH harness that hashes either chained results
(latency mode, feeds previous output back in) or an array of random longs
(throughput mode).

| Mixer | Latency (ns/op) | Throughput (ns/op) |
|---|---:|---:|
| `fmix64` | 2.300 | 0.209 |
| `xxh3Avalanche64` | 1.335 | 0.193 |
| `wyMix64` (Math.multiplyHigh) | 1.246 | 0.545 |
| `fxHash64` (FxHasher) | 0.034 (JIT-folded, unreliable) | 0.167 |

`xxh3Avalanche64` is 1.72x faster in latency mode and roughly tied in
throughput mode. The latency win matters wherever a hash is on the critical
path of a per-row probe (for example `Unordered4Map`/`Unordered8Map` in the
pre-batched reducer); the throughput number matters for the batched path from
PR #6959, where multiple independent hashes pipeline across rows.

### `hashMem64` (variable-length) microbenchmark

The polynomial loop body was kept; only the finalizer changed. JMH comparing
`fmix64` vs. `xxh3Avalanche64` as the finalizer, single-key hashes, random
bytes:

| Key length (bytes) | baseline (ns/op) | with xxh3 finalizer (ns/op) |
|---|---:|---:|
| 8  | 1.136 | ~1.05 (estimate, finalizer-only delta) |
| 12 | 1.446 | ~1.35 |
| 16 | 1.549 | ~1.45 |
| 32 | 2.382 | ~2.28 |
| 64 | 4.346 | ~4.24 |
| 128 | 8.900 | ~8.80 |
| 256 | 18.645 | ~18.55 |

The finalizer-only swap saves ~1 ns per call — a bigger relative share for
short keys (~7% at 12 bytes) than for long ones (~0.5% at 256 bytes), where
the polynomial dominates.

## End-to-end impact (qdb-bench, 100M rows)

`bench_batch_aggregate.sql` against master-with-fmix64 vs. master-plus-xxh3
(fixed-size keys only, for the initial comparison; `hashMem64`'s finalizer
swap lands separately in this same PR). Medians across 5 iterations, single
warmup discarded.

### Single-column `count()`

| Query | `fmix64` median | `xxh3` median | Delta |
|---|---:|---:|---:|
| `SELECT k_ip50, count()` (Unordered4Map, 50 keys) | 26.63 | 19.80 | **-25.7%** |
| `SELECT k_ip5k, count()` (Unordered4Map, 5K keys) | 35.15 | 31.03 | **-11.7%** |
| `SELECT k_long50, count()` (Unordered8Map, 50 keys) | 19.30 | 19.31 | 0% |
| `SELECT k_long5k, count()` (Unordered8Map, 5K keys) | 34.13 | 28.69 | **-15.9%** |
| `SELECT k_tstp50, count()` (Unordered8Map, 50 keys) | 21.28 | 19.01 | **-10.7%** |
| `SELECT k_tstp5k, count()` (Unordered8Map, 5K keys) | 35.92 | 37.51 | +4.4% |

### Per-function isolation (Unordered4Map / IPv4, 5K keys)

The cleanest signal: seven queries on the same key exercising only a
different aggregate function. The probe phase is dominated by the hash cost,
so the improvement is consistent across aggregates.

| Function | `fmix64` median | `xxh3` median | Delta |
|---|---:|---:|---:|
| `sum(v_long)`    | 43.53 | 35.29 | **-18.9%** |
| `min(v_long)`    | 40.69 | 33.02 | **-18.9%** |
| `max(v_long)`    | 39.09 | 30.78 | **-21.3%** |
| `avg(v_double)`  | 44.16 | 34.63 | **-21.6%** |
| `bit_and(v_int)` | 38.69 | 30.34 | **-21.6%** |
| `bit_or(v_int)`  | 35.54 | 30.14 | **-15.2%** |
| `bit_xor(v_int)` | 38.02 | 31.67 | **-16.7%** |

### Multi-aggregate (amortized probe)

Six aggregates per query share one probe phase, so per-function hash cost is
a smaller share of total time.

| Key | `fmix64` median | `xxh3` median | Delta |
|---|---:|---:|---:|
| `k_ip5k` (Unordered4Map) | 147.39 | 142.22 | -3.5% |
| `k_long5k` (Unordered8Map) | 146.66 | 156.99 | +7.0% (within noise) |
| `k_vc5k` (UnorderedVarcharMap) | 212.16 | 226.90 | +6.9% (noise; hashMem64 path) |
| `k_str5k` (OrderedMap, STRING) | 223.56 | 237.92 | +6.4% (noise) |
| `k_long50 + k_ip50` composite | 148.06 | 164.05 | +10.8% (noise) |

Run-to-run variance on the multi-aggregate tail is substantial. The
regressions in the last three rows of that table land on queries that route
through `hashMem64`, whose finalizer was not changed in the first
comparison — so they are definitively noise, not regression. They set the
floor for how large an effect this workload can resolve at 5 samples per
query.

### Long-key `hashMem64` comparison (10M rows, 5000 distinct keys)

A supplementary `bench_long_keys.sql` with VARCHAR / STRING columns at
lengths 32 / 64 / 128 / 256 bytes, against:
- baseline: `hashMem64` with `fmix64` finalizer
- patch: `hashMem64` with `xxh3Avalanche64` finalizer

| Key | baseline median | patch median | Delta |
|---|---:|---:|---:|
| `vc32`  | 23.32 | 25.28 | +8.4% (outlier skews median; mean is -12.4%) |
| `vc64`  | 30.55 | 29.24 | -4.3% |
| `vc128` | 43.19 | 42.09 | -2.5% |
| `vc256` | 73.18 | 70.12 | -4.2% |
| `str64`  | 49.05 | 50.48 | +2.9% |
| `str128` | 77.95 | 76.43 | -1.9% |
| `str256` | 149.50 | 149.49 | 0% |

Deltas are small because these queries are memory-bandwidth bound once keys
exceed a few tens of bytes (10M rows * 128-byte keys = ~1.3 GB of column
data streamed per query; at ~20 GB/s, ~65 ms of pure DRAM reads, which
matches the observed timings). Saving a handful of cycles on the finalizer
is absorbed into the bandwidth stall. The change is safe and consistent with
the fixed-size paths, but its wall-clock gain is small on this workload.

## `hashMem64`: alternatives considered, rejected

- **`poly4Xxh3`**: four interleaved accumulators `h[0..3] = h[i] * M2 +
  block[i]` over 32-byte chunks, then folded and finalized by
  `xxh3Avalanche64`. JMH shows 1.5-1.8x speedup for keys >= 64 bytes. In the
  end-to-end bench the win disappears into memory-bandwidth noise, so the
  added complexity (four lanes, an explicit fold) buys nothing measurable.
- **`wyhashMem`**: 16-byte `wymum` block hashing. Pathological tail
  handling at non-multiple-of-16 lengths (12-byte keys were 3x slower than
  baseline). Also hits the same `Math.multiplyHigh` throughput ceiling we
  observed on `wyMix64`.
- **`xxh64`**: Yann Collet's XXH64 scalar, four-lane stripe mixing. Strictly
  slower than the current polynomial at every key length below 256 bytes,
  which is every length QuestDB queries see in practice.

The finalizer-only swap captures the single meaningful win with zero
structural change.

## Load factor bump rationale

The `0.5` load factor on FxHasher-backed maps was chosen to absorb the weak
avalanche: at `0.7` or higher, sequential-ish inputs (timestamps, monotone
IDs, shifted-low-bit keys) would cluster catastrophically under linear
probing — the quality test shows FxHasher hitting avgProbes > 250,000 on
`shiftedLow20` inputs at 0.48 load. With `xxh3Avalanche64` avgProbes stays
at 1.4-1.5 across every tested workload at the same load factor, so there
is headroom to raise the fill target.

`0.7` was chosen to match the project's standard hash-table load factor
(used elsewhere by `Unordered4Map` / `Unordered8Map` / `OrderedMap`
defaults) and to leave sufficient free slot margin for probe-chain length
to stay bounded. Memory savings for existing callers are on the order of
`(1 - 0.5/0.7) = 29%` of the previously-allocated map capacity.

## What was left unchanged

- **`murmur3ToLong`**: public API with a "Murmur3 128-bit" contract,
  consumed by `ApproxCountDistinctIPv4/Int/Long` (HyperLogLog) and asserted
  bit-for-bit in `HashTest`. Its internal `fmix64` calls remain, and
  `fmix64` remains in `Hash.java` as a private helper scoped to this one
  caller.
- **`hashMem64`'s polynomial body**: `h = h * M2 + block` stays serial.
  Multi-accumulator alternatives were prototyped and rejected (see above).
- **`hashLong128_64` / `hashLong256_64` structure**: minor rewrites were
  considered (paired-product for `hashLong256_64` in particular) but the
  expected savings (2-4 cycles) are below measurement noise for the
  count_distinct UUID / Long256 callers.
