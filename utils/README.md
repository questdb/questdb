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
  (`_wal_id`, `_segment_id`, `_segment_txn`, `_txnSeq_`, `_commit_ts`,
  `_recovery_status_`) that are added by default to help downstream
  deduplication. `_txnSeq_` is the global QuestDB sequencer transaction id
  (= seqTxn) that originally wrote the row, and matches the `seqTxn` field
  in `<tableName>__sql_log.json` so operators can align row provenance with
  non-data transactions. `_recovery_status_` is one of `unapplied` /
  `applied_unpurged` / `unknown`, derived by comparing each row's
  `_txnSeq_` against the table's `appliedSeqTxn` watermark read from `_txn`
  at the table root.

#### Output

Each WAL segment becomes one Parquet file:

```
<output-dir>/<tableName>__wal<walId>__seg<segId>__seqTxn<lo>-<hi>.parquet
```

Three JSON sidecars sit next to the data files:

- `<tableName>__manifest.json` lists every segment the tool considered
  (written, skipped, or partial), every structural-change transaction in
  seqTxn order, the txnlog format version and applied/maxTxn watermarks,
  the `appliedSeqTxn` watermark read from `_txn`, and per-segment reasons
  when recovery was less than complete. Note: in tier-2 mode (`_txnlog`
  unreadable, see below), the rows themselves are still recovered via the
  filesystem scan into a `__tier2.parquet` file; what's lost is the
  txnlog-derived metadata. `manifest.structuralChanges` and
  `manifest.sqlStatements` are empty in tier-2 because both lists are
  populated by walking the txnlog cursor. The schema-per-`structureVersion`
  view in `__schemas.json` is still emitted from each segment's own `_meta`,
  so the schema lineage is preserved even when the ALTER history is not.
- `<tableName>__sql_log.json` is emitted whenever the WAL contains non-DATA
  transactions stored as SQL events (UPDATE, ALTER TABLE, TRUNCATE,
  view/mat-view events). For each, it captures `(walId, segmentId,
  segmentTxn, seqTxn, commitTimestamp, type, sql, error)`. These
  transactions do not materialise as rows in the Parquet output (their
  effect would require replay against a live table), so the sidecar is
  the only record. Cross-reference the sidecar's `seqTxn` field against
  the `_txnSeq_` shoulder column in the Parquet files to locate which
  data rows the statement would have applied to.

  Failure handling is per-event. There are two failure shapes:

  - **Body parse failure with known type:** the header was valid and we
    have type, walId, segmentId, segmentTxn, seqTxn, commitTimestamp.
    The body (e.g., the SQL text for an `SQL` event) couldn't be parsed.
    The entry preserves all the header context and carries the
    underlying exception message in the `error` field. Iteration
    continues; the cursor is still aligned.
  - **Header/cursor failure or unknown type byte:** the cursor's
    `hasNext()` reads the full record including dispatching on the type
    byte, so a type the OSS build doesn't know about (a future or
    Enterprise variant) throws inside cursor advance. The sidecar gets
    one `type="UNKNOWN_EVENT_UNREADABLE"` entry pinpointing
    `(walId, segmentId)`, the underlying exception in `error`, and
    `segmentTxn=-1` (we don't have it). Event collection for the
    segment stops here because the cursor's position is then undefined.
    Data recovery for the segment still proceeds via the tier-3
    fallback if applicable.

  If Enterprise stores `GRANT`/`CREATE USER` as ordinary
  `WalTxnType.SQL` events with the standard OSS framing, the SQL text
  is captured verbatim and the operator can replay it in an Enterprise
  build later. The fallback above only kicks in for genuinely new event
  type bytes or incompatible framings.
- `<tableName>__schemas.json` is emitted whenever the table has at least
  one segment with readable metadata. It records the column list at each
  distinct structureVersion observed across the table's WAL segments,
  emitted in ascending numeric order so the file is deterministic across
  runs. Each entry has `name`, `type`, `writerIndex`, and
  `isDesignatedTimestamp`. To map a recovered Parquet file back to its
  schema, look up the file's segment in `__manifest.json`, read the
  segment's `structureVersion` field, and resolve that key in
  `__schemas.json`. Structural-change transactions (ADD COLUMN / DROP
  COLUMN / RENAME COLUMN) are recorded in `__manifest.json` as
  `{seqTxn, commitTimestamp}` markers but the original ALTER SQL
  statements are not currently extracted - the resulting schemas in this
  file are the authoritative source.

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
| `__tier3.parquet` | Segment's `_event` is missing or corrupt. Row count is derived from the txnlog's per-txn row counts (V2 sequencer format only). New symbols added in the segment are lost (only the base symbol-table snapshot recovers). For V1 sequencer format (the QuestDB default — `cairo.default.sequencer.part.txn.count=0`), the txnlog does not carry per-txn row counts and the segment is marked unrecoverable with status `skipped_event_unreadable_no_row_count` rather than fabricating row counts from WAL column file lengths (which are mmap-preallocated and report capacity, not committed appends). |
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

#### Notes on dependencies

`WalToParquet` uses Gson (`com.google.code.gson:gson`) to emit the three
JSON sidecars. QuestDB's core engine is zero-third-party-dependency on
data paths, but this tool is a standalone offline CLI: it runs once per
recovery, never on a query path, and emits JSON only at the end of each
table's walk. Gson is already on the `utils` module classpath through
existing tools, so this PR does not add a new dependency. If the
zero-deps invariant tightens in the future, the sidecars can be
hand-rolled with `io.questdb.std.str.StringSink`; nothing in the
sidecars' shape requires Gson specifically.
