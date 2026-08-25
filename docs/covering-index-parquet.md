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

**The Parquet form is still slower to read than the native one**, and the gap
widens with the number of distinct keys a query touches rather than with the
rows it returns. Measured over the posting benchmark suite's own fixtures, both
arms holding identical data so the index form is the only difference:

| Benchmark | Fixture | Native | Parquet | Ratio |
| --- | --- | --- | --- | --- |
| `indexPointRead` | 400k rows, 16 keys | 1198 ops/s | 157 ops/s | 7.6x |
| `indexPointRead` | 2M rows, 2000 keys | 240 ops/s | 5.0 ops/s | 48x |
| `indexScanRead` | 400k rows, 16 keys | 3060 ops/s | 171 ops/s | 17.9x |
| `indexRangeRead` | 400k rows, 16 keys | 6700 ops/s | 168 ops/s | 40x |
| `sidecarRead` covered gather, 200 keys | 1M rows, 500 keys | 278 ops/s | 20 ops/s | 14x |

Reproduce with `PostingIndexBenchmarkSuite`, whose `POSTING_PARQUET` and
`covering_parquet` arms build the same fixture through
`ParquetIndexSeal`:

```
java -cp benchmarks/target/benchmarks.jar org.questdb.PostingIndexBenchmarkSuite \
    -Dquestdb.suite.bench=indexPointRead -Dquestdb.suite.bench.scenario=P400K \
    -Dquestdb.suite.bench.format=POSTING,POSTING_PARQUET
```

**Read cost is per key touched, not per row returned.** A key's postings live in
one contiguous run of one row group; resolving and decoding that run costs
roughly a fixed amount regardless of how many rows the key has. Queries over a
few hot keys therefore pay little; queries sweeping thousands of distinct keys
pay thousands of times that. Plan for the trade accordingly: the feature is
aimed at partitions that travel, not at high-cardinality key sweeps.

So the feature trades read latency for portability: the index travels with the
partition into cold storage, replication and S3, and reads cost more. Enable it
where that trade is the one you want, not by default -- which is why the
default is `native`.

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
