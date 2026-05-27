# QuestDB command line utils

A collection of low-level CLI utilities for inspecting, repairing, migrating,
and salvaging QuestDB data outside the running database.

## Building

The module ships as a shaded JAR with all dependencies. To build it run:

```bash
mvn clean package
```

The artifact lands at `utils/target/utils-<version>-SNAPSHOT.jar`. Pre-built
binaries from a QuestDB release tarball already contain this jar in the same
directory layout.

## Running

The shaded JAR does not declare a `Main-Class`, so the tools are invoked by
classpath, not via `java -jar`:

```bash
java -cp utils.jar io.questdb.cliutil.<ToolClassName> [options]
```

On Java 17+ some tools need module-access flags to use `sun.misc.Unsafe` and
native memory. If you see `IllegalAccessError`, `module access` errors or
`UnsupportedOperationException` from `Unsafe`, prepend:

```
--sun-misc-unsafe-memory-access=allow \
--enable-native-access=ALL-UNNAMED \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED
```

These are the same flags QuestDB itself uses (see `core/pom.xml`'s surefire
config).

---

### TxSerializer

Serializes binary `_txn` file to / from readable JSON format. Primary usage to
investigate storage issues

#### Usage

```
io.questdb.cliutil.TxSerializer -d <txn_path> | -s <json_path> <txn_path>
```

- `-d` option prints contents of `_txn` file to std output in JSON format
- `-s` option transforms existing JSON file into binary \_txn format

#### Examples

```bash
java -cp utils.jar io.questdb.cliutil.TxSerializer -d /questdb-root/db/trades-COINBASE/_txn > /questdb-root/db/trades-COINBASE/txn.json

java -cp utils.jar io.questdb.cliutil.TxSerializer -s /questdb-root/db/trades-COINBASE/txn.json /questdb-root/db/trades-COINBASE/_txnCopy
```

### Rebuild index

Rebuilds table indexes

#### Usage

```
io.questdb.cliutil.RebuildIndex <table_path> [-p <partition_name>] [-c <column_name>]
```

- `<table_path>` full path to the table
- `-c` column name, optional. If omitted, all indexed columns will have indexes
  rebuilt
- `-p` optional partition name. If omitted, all partitions will be affected

#### Examples

```bash
java -cp utils.jar io.questdb.cliutil.IndexBuilder /questdb-root/db/trades-COINBASE

java -cp utils.jar io.questdb.cliutil.IndexBuilder /questdb-root/db/trades-COINBASE -c symbol

java -cp utils.jar io.questdb.cliutil.IndexBuilder /questdb-root/db/trades-COINBASE -p 2022-03-21

java -cp utils.jar io.questdb.cliutil.IndexBuilder /questdb-root/db/trades-COINBASE -p 2022-03-21 -c symbol
```

### Rebuild String column index `.i` file

Rebuilds String column `.i` file from `.d` file, helpful when `.i` file is corrupted

#### Usage

```
io.questdb.cliutil.RecoverVarIndex <table_path> [-p <partition_name>] [-c <column_name>]
```

- `<table_path>` full path to the table
- `-c` column name, optional. If omitted, all string columns will have `.i` file
  rebuild
- `-p` optional partition name. If omitted, all partitions will be affected

#### Examples

```bash
java -cp utils.jar io.questdb.cliutil.RecoverVarIndex /questdb-root/db/trades-COINBASE

java -cp utils.jar io.questdb.cliutil.RecoverVarIndex /questdb-root/db/trades-COINBASE -c stringColumn

java -cp utils.jar io.questdb.cliutil.RecoverVarIndex /questdb-root/db/trades-COINBASE -p 2022-03-21

java -cp utils.jar io.questdb.cliutil.RecoverVarIndex /questdb-root/db/trades-COINBASE -p 2022-03-21 -c stringColumn
```

### Copy table from one instance to another using Postgres wire to read and ILP to write

Copies all the data from one QuestDB instance to another. Uses Postgres wire to select the data and ILP to insert it.
Useful to migrate data to the running instance.

#### Usage

```
io.questdb.cliutil.Table2Ilp -d <destination_table_name> -dc <destination_ilp_host_port> -s <source_select_query> -sc <source_pg_connection_string>
                               [-sts <timestamp_column>] [-sym <symbol_columns>] [-dauth <ilp_auth_key:ilp_auth_token>] [-dtls]
```

- `-d` destination table name
- `-dilp` destination ILP connection string, e.g. `https::addr=localhost:9000;username=admin;password=quest;`
- `-s` source select query, e.g. `trades` or `trades WHERE timestamp in '2021-01'`
- `-sc` source connection string, e.g. `jdbc:pgsql://localhost:8812/qdb`
- `-sts` source designated timestamp column name, defaults to `timestamp`
- `-sym` comma separated list of symbol columns, e.g. `symbol,exchange`

#### Examples

```bash
java -cp utils.jar io.questdb.cliutil.Table2Ilp -d trades -dilp "https::addr=localhost:9000;username=admin;password=quest;" -s "trades WHERE start_time in '2022-06'" \ 
     -sc "jdbc:postgresql://localhost:9812/qdb?user=account&password=secret&ssl=false" \
     -sym "ticker,exchagne" -sts start_time

```

### Export un-applied WAL data to Parquet (WalToParquet)

Forensic recovery tool. Walks the WAL directories of every WAL table in a
QuestDB data root and exports each un-purged segment to a Parquet file. Useful
when a table is suspended or its committed partitions are corrupt, and you want
to extract whatever still lives in the WAL before the table is dropped and
restored.

Strict read-only against the data root. No QuestDB engine is booted, no WAL is
applied, no files are written or locked under the source tree. Safe to run
against a live, running QuestDB instance under snapshot semantics: anything
visible in `_txnlog` at the start of the walk is in scope, anything appended
after is ignored.

#### Usage

```
io.questdb.cliutil.WalToParquet --db-root <path> [options]
```

- `--db-root <path>` QuestDB data root (the directory containing per-table
  directories like `mytable~5`). Required.
- `--output-dir <path>` Where to write Parquet files and the per-table manifest.
  Required for actual export; omit for a dry-run inspection.
- `--table-dir <dirName>` Process only this one table directory (skips
  auto-discovery).
- `--table-name <name>` Logical table name override for single-table mode.
- `--table-id <id>` Numeric table id override for single-table mode.
- `--include-system` Also process `sys.*` tables (off by default).
- `--include-empty` Also report tables whose WALs are fully purged (off by
  default).
- `--no-shoulder` Skip the per-row provenance columns
  (`_wal_id`, `_segment_id`, `_segment_txn`, `_txnSeq_`, `_commit_ts`) that
  are added by default to help downstream deduplication. `_txnSeq_` is the
  global QuestDB sequencer transaction id (= seqTxn) that originally wrote
  the row, and matches the `seqTxn` field in `<tableName>__sql_log.json` so
  operators can align row provenance with non-data transactions.

#### Output

Each WAL segment becomes one Parquet file:

```
<output-dir>/<tableName>__wal<walId>__seg<segId>__seqTxn<lo>-<hi>.parquet
```

Two JSON sidecars sit next to the data files:

- `<tableName>__manifest.json` lists every segment the tool considered
  (written, skipped, or partial), every structural-change transaction in
  seqTxn order, the txnlog format version and applied/maxTxn watermarks, and
  per-segment reasons when recovery was less than complete.
- `<tableName>__sql_log.json` is emitted whenever the WAL contains non-DATA
  transactions (UPDATE, ALTER TABLE, TRUNCATE, view/mat-view events). For
  each, it captures `(walId, segmentId, segmentTxn, seqTxn, commitTimestamp,
  type, sql)`. These transactions do not materialise as rows in the Parquet
  output (their effect would require replay against a live table), so the
  sidecar is the only record. Cross-reference the sidecar's `seqTxn` field
  against the `_txnSeq_` shoulder column in the Parquet files to locate
  which data rows the statement would have applied to.

#### Per-txn SYMBOL handling

QuestDB's WAL writer may assign the same numeric symbol code to different
strings across transactions inside one segment (for example after the table
was suspended and resumed, the WAL writer's local symbol space can reset and
code `0` in batch 2 means a different string than code `0` in batch 1). The
tool walks `_event` to build per-txn snapshots of each SYMBOL column's
dictionary, resolves every row using **its own transaction's** snapshot, then
deduplicates strings into a single global dictionary with newly assigned
codes for the Parquet output. Rows are correctly attributed regardless of
code reuse.

#### Tiered fallback (file-name suffixes)

The tool degrades gracefully when the source is partially damaged. The output
filename and manifest status reflect which fallback was needed:

| Suffix | Trigger |
|---|---|
| (none) | Tier 1. Everything intact: `_txnlog`, `_event`, `_meta`, all column files. |
| `__tier3.parquet` | Segment's `_event` is missing or corrupt. Row count is derived from the timestamp `.d` file size; new symbols added in the segment lost (only the base symbol-table snapshot recovers). |
| `__tier2.parquet` | Whole-table `_txnlog` is missing or corrupt. Filesystem scan of `wal*/N/` finds segments; cross-segment seqTxn ordering is unknown. |
| `__tier2__tier3.parquet` | Both of the above. |

Additionally, partial column-file loss within a segment (an individual `.d` or
`.i` is gone) is reported in the manifest's `skippedColumns` list per segment.
A corrupt `_meta` triggers a peer-segment schema substitution (the schema is
borrowed from another segment under the same table); the manifest flags this.

#### Examples

Discover every WAL table in the data root and export everything still
on disk:

```bash
java -cp utils.jar io.questdb.cliutil.WalToParquet \
    --db-root /questdb-root/db \
    --output-dir /tmp/wal_recovery
```

Process one specific table directory:

```bash
java -cp utils.jar io.questdb.cliutil.WalToParquet \
    --db-root /questdb-root/db \
    --table-dir 'mytable~17' \
    --output-dir /tmp/wal_recovery
```

Inspect only (no Parquet output, print structure and segment list to stdout):

```bash
java -cp utils.jar io.questdb.cliutil.WalToParquet --db-root /questdb-root/db
```

Include system tables and tables with no un-purged data:

```bash
java -cp utils.jar io.questdb.cliutil.WalToParquet \
    --db-root /questdb-root/db \
    --output-dir /tmp/wal_recovery \
    --include-system --include-empty
```

Verify recovered data via the running instance (the Parquet must be inside
the instance's `import/` directory for `parquet_scan` to read it):

```bash
cp /tmp/wal_recovery/mytable__wal4__seg0__seqTxn14-16.parquet \
   /questdb-root/import/recovered.parquet
curl -G "http://localhost:9000/exec" \
    --data-urlencode "query=select count(*) from parquet_scan('recovered.parquet')"
```
