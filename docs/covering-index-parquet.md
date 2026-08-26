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

**At parity with the native index across the SQL benchmarks, and ahead of it on
low- and mid-cardinality point reads.** Measured against an OPTIMISED Rust build
(`-P qdbr-release`; the default maven build is unoptimised and inflates every
number here roughly 2x).

Against the like-for-like arm -- the same partition converted to parquet,
keeping a native index -- so the parquet DATA format's own cost is not charged
to the index form:

| Query | vs native index |
| --- | --- |
| covered `WHERE` | 1.09x faster |
| `count()` | parity |
| `sum()`, residual filter | parity |
| `avg()` | 1.14x |
| `SELECT DISTINCT` | parity |
| `LATEST ON`, `IN` list, wide table, O3 | parity |

Index-level reads over the posting benchmark suite's cardinality scenarios,
parquet against native:

| Rows / distinct keys | point read | scan |
| --- | --- | --- |
| 400k / 16 | **1.59x faster** | 3.9x |
| 1M / 500 (zipf) | **1.84x faster** | 1.4x |
| 1.02M / 512 | **1.10x faster** | 4.0x |
| 2M / 2,000 | 1.16x | 4.3x |
| 1M / 5,000 | 2.4x | 4.2x |
| 1.2M / 200,000 | 8.9x | 2.0x |
| 2M / 500,000 | 8.8x | 2.1x |
| 2M / 1,000,000 | 9.5x | 2.5x |

**Where it is still slower, and why.** A parquet read costs one decode call per
key -- a JNI crossing, a thrift page header, a buffer -- against the native
index's direct mapped read. That fixed cost disappears into the work when a key
is wide, which is why low and mid cardinality are at or ahead of parity. It
dominates when keys are narrow: at a million distinct keys a key holds two rows,
and the call costs more than the rows it returns. Scans avoid it -- consecutive
keys share a row group, which is decoded once and served from -- so they stay
near 2x however narrow the keys get. Random point reads over very high
cardinality cannot, and remain the one case that is materially slower.

Reproduce with `PostingIndexBenchmarkSuite`, whose `POSTING_PARQUET` and
`covering_parquet` arms build the same fixture through `ParquetIndexSeal`, and
whose `sqlQuery` carries a `storage` arm separating the parquet data format's
cost from the index form's:

```
mvn -Plocal-client -Pbuild-rust-library -Pqdbr-release -pl benchmarks -am package -DskipTests
java -cp benchmarks/target/benchmarks.jar org.questdb.PostingIndexBenchmarkSuite \
    -Dquestdb.suite.bench=indexPointRead -Dquestdb.suite.bench.scenario=P400K,S1 \
    -Dquestdb.suite.bench.format=POSTING,POSTING_PARQUET
```

## Size

The index parquet is written **uncompressed** by default, which is what buys the
read parity above -- an uncompressed page is never decompressed, so a lookup
reads its key's rows rather than the page they sit in. It costs about 1.5x the
size on a covered `DOUBLE`, up to ~2.5x on row-id-only data.

For a feature whose purpose is that the index travels, that is a real trade. To
take it back:

```
cairo.posting.index.parquet.compression.codec=LZ4_RAW
```

The data page size follows the codec, so no second change is needed. Expect
roughly 2-2.3x slower reads for it.

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
