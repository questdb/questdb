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
| `count()`, `SELECT DISTINCT`, `LATEST ON`, `IN` list, wide table, O3 | parity |
| `sum()`, residual filter | parity |
| `avg()` | 1.14x |
| covered gather (`sidecarRead`) | parity; INT faster |

**Index-level reads.** Parquet against native, each scenario in its own JVM --
running the whole suite in one leaves the million-key fixtures' memory pressure
on everything measured after them:

| Rows / distinct keys | point read | scan | range |
| --- | --- | --- | --- |
| 400k / 16 | **1.56x faster** | 1.57x | 2.73x |
| 1.02M / 512 | **1.66x faster** | 1.86x | 2.69x |
| 1M / 500 (zipf) | **1.26x faster** | **1.53x faster** | **1.11x faster** |
| 2M / 2,000 | **1.49x faster** | 2.08x | 4.84x |
| 1M / 5,000 | **1.25x faster** | 1.88x | 7.81x |
| 1M / 10,000 | 1.00x | **1.48x faster** | 2.39x |
| 1.2M / 200,000 | 2.0x | **1.82x faster** | 6.00x |
| 2M / 500,000 | 2.9x | **1.65x faster** | 6.69x |
| 2M / 1,000,000 | 4.0x | **1.73x faster** | 7.03x |

Point reads beat the native chain up to five thousand keys and level with it at
ten thousand; scans beat it from ten thousand upwards.

**What still costs.** `row_id` is PLAIN where the native chain delta-FoR packs
its postings, so anything that walks a long run of them -- a windowed range scan
above all -- moves several times the bytes native does. Delta packing was tried
and reverted: a delta block decodes from its start, so it destroys the random
access the same column needs (point reads went to 40-44x). Parquet offers no
packed integer encoding that survives random access, which is where the
remaining range-read gap comes from.

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
