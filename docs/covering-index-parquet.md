# Covering index in Parquet form

A covering POSTING index normally lives in native sidecar files next to the
partition it indexes. When that partition is converted to Parquet, those
sidecars are hard-linked into the Parquet partition directory and stay native —
so the index does not travel with the partition into cold storage, replication
or an S3 round trip.

This feature seals the index into Parquet instead, so it travels with the data.

## Enabling it

```
cairo.posting.index.parquet.partition.format=parquet
```

Default is `native`, which is the behaviour QuestDB has always had. Nothing
changes for an existing installation until the property is set.

**The property describes what the NEXT seal will write. It never describes what
a partition already carries.** A partition sealed while the property was
`parquet` keeps its Parquet-form index after the property is set back to
`native`, and vice versa. Reads dispatch on what is published in the partition's
`_pm`, not on the property, so flipping it does not change how existing
partitions are read — only how the next `ADD INDEX`, reseal or partition
conversion writes.

Source: `PropertyKey.java:109`, `PropServerConfiguration.java:1700`. An
unrecognised value falls back to `native`.

## What appears on disk

Inside the partition directory, per indexed column:

| File | Contents |
| --- | --- |
| `<col>.pidx.<indexTxn>.parquet` | the index itself — one row per posting, key-major |
| `<col>.pidx.<indexTxn>._im` | a metadata sidecar: the key directory, row-id zone maps and row-group offsets |

`<indexTxn>` names the generation. Both files are published together as a token
in the partition's `_pm`, and a reader binds to the pair that token names.

The native `.pk` / `.pv` / `.pc*` chain is **not** written for such a partition.

Source: `ParquetIndexSeal.java:107,115`.

## Read performance

Measured against an OPTIMISED Rust build (`-P qdbr-release`; the default maven
build leaves the Rust library unoptimised and inflates every number here by
roughly 2x).

**SQL is at parity.** Against the like-for-like arm -- the same partition
converted to parquet, keeping a NATIVE index -- so the parquet data format's own
cost is not charged to the index form:

| Query | vs native index |
| --- | --- |
| covered `WHERE` | 1.09x faster |
| `count()`, `SELECT DISTINCT`, `IN` list, wide table, O3 | parity |
| `LATEST ON` **with no WHERE** | parity -- but see below: that shape never touches the index |
| `LATEST ON` **naming values** | **2.9x SLOWER than using no index**; see "Where the parquet form LOSES" |
| `sum()`, residual filter | parity |
| `avg()` | 1.14x |
| covered gather (`sidecarRead`) | parity; INT faster |

**Index-level reads.** Parquet against native.

> **Superseded.** The table below predates two changes: the suite now forks one
> JVM per benchmark (it previously ran in-process, where benchmarks contaminate
> each other -- a 5,000-key scan read 3.11x SLOWER alongside two others and 1.73x
> FASTER alone), and the backward reader has since been given the forward
> reader's fast paths. Current numbers, forked, 3 warmup + 5 measurement
> iterations: point reads parity at 16 and 200k keys, 3.3x slower at 2k, 2.2x at
> 1M; scans 1.45-1.61x FASTER at 200k and 1M; range reads 1.1x at 16 rising to
> 4.4x at 1M. Both directions now agree cell for cell. Reproduce with the command
> at the end of this section.

| Rows / distinct keys | point read | scan | range |
| --- | --- | --- | --- |
| 400k / 16 | **1.93x faster** | **1.90x faster** | **1.79x faster** |
| 1.02M / 512 | **1.88x faster** | **1.86x faster** | **1.56x faster** |
| 1M / 500 (zipf) | **1.49x faster** | **1.51x faster** | **3.10x faster** |
| 2M / 2,000 | **1.72x faster** | **1.66x faster** | 1.11x |
| 1M / 5,000 | **1.60x faster** | **1.72x faster** | 1.52x |
| 1M / 10,000 | **1.84x faster** | **2.25x faster** | 1.42x |
| 1.2M / 200,000 | 1.22x | **2.44x faster** | 3.24x |
| 2M / 500,000 | 1.77x | **1.71x faster** | 3.82x |
| 2M / 1,000,000 | 2.35x | **1.76x faster** | 4.63x |

Scans beat the native chain at every cardinality. Point reads beat it to ten
thousand keys, and range reads to a thousand.

**What is left is the `_im` checksum, not the read path.** The nine slower cells
are all high-cardinality point and range reads, and both are dominated by reader
BIND: the checksum covers the whole `_im`, whose key directory is 4 MB at a
million keys. Skipping it takes a million-key point read from 2.34x to 1.33x and
a range read from 4.52x to 2.14x. It is left on, and the number to keep in mind
is that the cost is per MAPPING -- a pooled TableReader maps once per
partition-open and serves many queries, where these benchmarks open a reader
every iteration and then make as few as 500 lookups.

### When to use it: cheap per row, expensive per key lookup

The single rule that predicts every number in this document: **the parquet form
costs more per KEY LOOKUP than the native chain and no more per ROW.** A cursor
that binds once and walks a long run amortises that; a query that opens hundreds
of cursors and reads a few rows from each does not.

| workload | parquet form |
| --- | --- |
| scans over many rows per key | **1.45-1.62x FASTER** at 200k and 1M keys |
| covered `WHERE` | parity at 16 / 2,000 / 200,000 keys |
| indexed `LATEST ON` (one short cursor per key) | **2.7-4.4x slower**, and worse than no index |

Measured against the native chain on the same data, by rows per key -- note this
is NOT monotonic, which is why a single threshold does not capture it:

| rows per key | 25,000 | 2,000 | 1,000 | 6 | 4 | 2 |
| --- | --- | --- | --- | --- | --- | --- |
| point read | 1.05x | 2.98x | 3.43x | 1.13x | 1.63x | 2.28x |

The band around 1,000-2,000 rows per key is **not explained**. Ruled out:
row-group planning (`TARGET_ROW_GROUP_ROWS` gives every fixture 100k-row groups,
one PLAIN page, direct read available), `questdb.idx.rgkeys` (inert, groups close
on the target first), `questdb.idx.rgminrows` and `questdb.idx.page` (both make
it worse -- a smaller page costs the direct-read path), and the whole-group
decode threshold (disabling it entirely moved 201.8 to 205.1 ops/s). The
high-cardinality end IS explained: `_im` directory cold misses, which grow with
the key count.

Closing either end means reducing per-cursor bind cost, which is QuestDB's shared
cursor layer rather than anything inside this format.

### Where the parquet form LOSES: indexed LATEST ON

`LATEST ON` that names its symbol values compiles to
`CoveringIndex op: latest on`, one backward `seekToLast()` per key. That shape --
hundreds of very short cursors -- is the worst case for the parquet form,
because its cost is per-cursor BIND and there is nothing to amortise it over.

Measured on 400k rows, 16 keys, `WHERE sym IN (...) LATEST ON`, all three arms
holding the SAME parquet DATA so only the index form differs:

| index form on a parquet partition | us/query |
| --- | --- |
| native chain, hard-linked into the parquet partition | ~82 |
| **no index at all (frame backward scan)** | **~94** |
| parquet form | ~271 |

So on this query the parquet form is 3.3x slower than the native-form index and
2.9x slower than using NO index -- it actively hurts. Through SQL, at P400K:
native 43,115 ops/s, parquet data + native index 17,228, parquet data + parquet
index 4,971.

This does not generalise. Scans bind once and walk millions of rows, which is
why they are 1.45-1.61x FASTER at 200k and 1M keys, and covered `WHERE` measures
at parity. It is specifically the many-short-cursors pattern that breaks.

**This was invisible for most of the work.** The suite's `latest_on` shape has no
`WHERE`, so it compiles to a frame backward scan and never touches the index at
all -- the index column read as parity for it while the indexed form was 2.9x
worse. `latest_on_indexed` was added to close that hole. A benchmark arm that
cannot reach the code it claims to measure reports parity, which is
indistinguishable from good news.

Closing this means reducing per-cursor bind cost, not traversal cost -- the same
per-mapping cost the high-cardinality point reads pay, seen from another angle.

### Mechanisms tried and rejected

Recorded so they are not retried. Each was fully implemented and measured, not
reasoned about.

| Mechanism | Result | Why |
| --- | --- | --- |
| Delta-pack `row_id` | 65x smaller, point reads **40-44x worse** | a delta block decodes from its start, so a point read pays for the whole block |
| Narrow `row_id` to INT | all tests green, four faster cells turned **slower** | the width branch lands in the `hasNext` fast path |
| Flat per-key offset array in `_im` | not built; **ceiling measured at 17.1%, reachable part 4.2%** | see below |
| Cap the parquet form by cardinality | not built | the range-read crossover sits between 512 and 2,000 keys, so parity everywhere means refusing the parquet form above ~512 distinct keys, which removes the feature rather than fixing it |

`key_id` keeps its delta packing: it is written for layout and never read back,
so the decode cost does not apply.

**Why the flat array was not built.** Profiling S7 (1M keys, 2M rows) with the
`_im` checksum skipped, so the reader BIND cost does not mask the lookup path:

| Where a point read spends itself | share | parquet-specific? |
| --- | --- | --- |
| `hasNext`, emitting rows | 52.1% | no -- the native chain does this too |
| `seekFirstAtLeast`, first touch of the row-id data | 25.9% | no -- native takes the same miss reading `.pv` |
| `getKeyRowRangeInGroup` + `getRowGroupRangeForKey` | 17.1% | yes |
| `rowIdDataOffset`, pruning, other | ~9% | partly |

17.1% is the whole prize, and a flat array cannot take most of it. 12.9% of it is
one cold miss into the 4 MB key directory, and a 4 MB flat array takes the
identical miss. What genuinely disappears is `getRowGroupRangeForKey`'s two
binary searches -- 4.2% -- and a by-row group search comes back in their place to
locate the page. On the 1.33x this configuration measures, spending all 4.2%
lands at ~1.28x. A format change is not worth that.

`seekFirstAtLeast` looking expensive is a red herring: at 1M keys a key's run is
about two rows, so the binary search runs a single iteration and what the profile
is showing is the cache miss underneath it, not the search.

What remains is the parquet page layer. A lookup walks directory -> row group ->
page offset -> data where native walks `.pk` -> `.pv`, and that extra layer is
what makes the index portable. Closing it means work outside this format --
QuestDB's shared cursor layer, or an integer encoding that survives random
access, which parquet does not offer.

Reproduce with `PostingIndexBenchmarkSuite`, whose `POSTING_PARQUET` and
`covering_parquet` arms build the same fixture through `ParquetIndexSeal`, and
whose `sqlQuery` carries a `storage` arm separating the parquet data format's
cost from the index form's:

Every `-D` must come BEFORE `-cp` and the main class. After the main class they
are program arguments, the JVM never sets them, and the suite silently ignores
the pinning and runs its whole matrix -- around forty minutes that looks like a
hung command rather than a mistake:

```
mvn -Plocal-client -Pbuild-rust-library -Pqdbr-release -pl benchmarks -am package -DskipTests
java -Dquestdb.suite.bench=indexPointRead \
    -Dquestdb.suite.bench.scenario=P400K,S1 \
    -Dquestdb.suite.bench.format=POSTING,POSTING_PARQUET \
    -cp benchmarks/target/benchmarks.jar org.questdb.PostingIndexBenchmarkSuite
```

## Size

The index parquet is written **uncompressed** by default, which is what buys the
read numbers above: an uncompressed page is never decompressed, so a lookup reads
its key's rows rather than the page they sit in. `key_id` is delta packed, worth
about 1.5x, so the file lands within roughly 2x of what LZ4_RAW would produce
while reading 2-2.3x faster.

To trade back:

```
cairo.posting.index.parquet.compression.codec=LZ4_RAW
```

The data page size follows the codec, so no second change is needed.

## Covered column restrictions

`INCLUDE (...)` columns must be fixed-width. The seal refuses var-size and
symbol covered columns outright, with:

```
parquet covering index does not support this covered column type [column=..., type=...]
```

Source: `ParquetIndexSeal.java:472`.

## Downgrading

**Convert affected partitions back to native before downgrading to a QuestDB
version that predates this feature.**

A footer carrying a covering token sets a required feature bit, so an older
build rejects the partition's `_pm` rather than reading it. That is deliberate.
Without the bit, an older build would ignore the covering section it does not
understand, conclude no index is published, and read the partition through the
native chain — which the seal discarded. That returns **no rows, with no
error**. Failing loudly is recoverable; a silent empty result is not.

To convert back:

```sql
ALTER TABLE <table> CONVERT PARTITION TO NATIVE LIST '<partition>';
```

Source: `types.rs` `FooterFeatureFlags::COVERING_INDEX_REQUIRED_BIT`.

### Also drain the purge log before downgrading

**Known gap, not yet fixed in code.** Two of the three persisted stores refuse
themselves to an older build: the `_pm` sets a required feature bit, and the
spill file bumps its format word so an older build discards it. The purge log
`sys.posting_seal_purge_log` has no such guard.

`artifact_form` was *appended* as a column, and `CREATE TABLE IF NOT EXISTS`
does not validate a schema, so an older build reads the same table, finds every
positional column it expects, and silently ignores the one that says the row
refers to a Parquet-form artifact. It then runs that row down the **native**
unlink path, deleting `<col>.pv.<cnt>.<sealTxn>` and `.pc*` files chosen by the
same numbers. The failure direction here is deletion, not a leak — which is why
it is called out separately from the rule above.

Before downgrading, make sure the log has no open rows:

```sql
SELECT count() FROM sys.posting_seal_purge_log WHERE completed IS NULL;
```

Convert the affected partitions back to native first, let
`PostingSealPurgeJob` drain what it queued, and confirm that count is zero.

The narrow condition is a Parquet `index_txn` that happens to equal a live
native chain generation for the same column, column-name txn and partition.
That collision has not been constructed, so the risk is bounded but not
theoretical.

Source: `PostingSealPurgeJob.java` `createLogTable` / `ensureArtifactFormColumn`,
`PostingSealPurgeOperator.java` (native unlink naming).

## Recovering a damaged index

Errors naming an unreadable `_im`, a payload kind this build does not decode, an
`_im` whose size disagrees with the published token, or an unaddressable index
Parquet all end with the recovery:

```sql
ALTER TABLE <table> ALTER COLUMN <column> DROP INDEX;
ALTER TABLE <table> ALTER COLUMN <column> ADD INDEX TYPE POSTING INCLUDE (...);
```

or take the partition back to native as above. Nothing repairs these
automatically — the query fails until one of the two is done.

Source: `AbstractParquetPostingIndexReader.RECOVERY_HINT`.

## Operational notes

- **Artifact retirement is asynchronous.** Superseded pairs are removed by
  `PostingSealPurgeJob` once no reader can still reach them, so a partition
  directory can briefly hold more than one `<col>.pidx.*` generation. This is
  expected, not a leak.
- **A `DROP INDEX` interrupted by a crash finishes on the next writer open.**
  The drop records its intent, so the token and its artifacts are reclaimed
  when the table is next opened for writing.
- **`_pm` grows by one footer per publish** and is reset when the O3 rewrite
  trigger fires (`cairo.partition.encoder.parquet.o3.rewrite.unused.ratio` and
  `.max.bytes`). Raising those far above their defaults removes that reset, and
  the file grows without bound.
