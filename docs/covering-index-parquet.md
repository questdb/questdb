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

**The Parquet form is currently slower to read than the native one.** Measured
on 400k rows in one partition, 16 distinct keys, covered columns
`price DOUBLE` + `qty LONG`, median of 20 iterations after 5 warm-up, both arms
holding identical data in a Parquet partition so the index form is the only
difference:

| Query shape | Native | Parquet | Ratio |
| --- | --- | --- | --- |
| `count()` on a hot key (metadata only) | 535 us | 3800 us | 6.7-7.1x |
| covered gather, hot key | 927 us | 6142 us | 5.1-6.6x |
| covered gather, cold key (pruning) | 408 us | 735 us | 1.6-1.8x |

Three runs agree on the direction and on the cold-key ratio. The hot-key
gather moved between 6.63x and 5.09x across runs, so treat these as ranges.

So the feature trades read latency for portability: the index travels with the
partition into cold storage, replication and S3, and reads cost more. Enable it
where that trade is the one you want, not by default -- which is why the
default is `native`.

Reproduce with:

```
mvn -pl core -Pbuild-rust-library -Dtest='ParquetCoveringIndexBenchTest' \
    -DfailIfNoSpecifiedTests=false -Dquestdb.bench=true test
```

Source: `ParquetCoveringIndexBenchTest.java`.

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
