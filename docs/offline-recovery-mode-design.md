# Offline Recovery Mode Design (Phase 1)

## 1. Problem Statement

When `fsync` is effectively disabled (for example `cairo.commit.mode=nosync`) and power is lost abruptly, table metadata files (especially `_txn`) may be left partially updated.

Common high-impact corruption pattern: variable-size column files become inconsistent, where AUX (`.i`) and DATA (`.d`) no longer match (for example DATA file truncated but AUX still points past EOF). This affects `STRING`, `VARCHAR`, `BINARY`, and `ARRAY` columns.

Current startup (`questdb.sh` -> `io.questdb.ServerMain`) assumes a normal runtime path and can trigger migrations, checkpoint logic, and full engine initialization. For recovery workflows, we want a mode that:

1. does not start the database runtime,
2. provides a simple text interface,
3. lists tables,
4. shows per-table state based on `_txn` (including partitions), similar to what `TxSerializer` exposes.

## 2. Scope

### In scope (Phase 1)

- New offline entrypoint executable from `questdb`.
- Read-only inspection mode.
- Interactive text UI with:
  - table listing,
  - table `_txn` state view.
- Clear reporting of corrupt or unreadable table metadata.

### Out of scope (Phase 1)

- Any write or repair action (`truncate`, rewrite `_txn`, auto-fix).
- Automatic integrity correction.
- Variable-size payload integrity checks (`.i`/`.d` consistency for `STRING`/`VARCHAR`/`BINARY`/`ARRAY`).
- Full SQL availability.

## 3. Key Findings From Codebase

- Runtime boot path is `core/src/main/bin/questdb.sh` using `JAVA_MAIN="io.questdb/io.questdb.ServerMain"` from `core/src/main/bin/env.sh`.
- `ServerMain` constructs `Bootstrap` and `CairoEngine` and starts services/jobs.
- `CairoEngine` constructor performs migration/checkpoint/registry logic; this is too invasive for offline recovery.
- `TxSerializer` (`utils/src/main/java/io/questdb/cliutil/TxSerializer.java`) already decodes `_txn`, but it lives in `utils` (not runtime jar path used by `questdb.sh`).
- `TxReader` (`core/src/main/java/io/questdb/cairo/TxReader.java`) provides robust `_txn` access and partition detail flags.
- `TxReader` is mmap-based; for crash-recovery on truncated files, a bounded fd-read path is safer as primary parsing strategy.
- Table discovery in runtime relies on table registry + filesystem reconciliation (`TableNameRegistryStore`), but for recovery we should tolerate registry inconsistency and fall back to filesystem scanning.

## 4. Design Principles

- **Offline and non-invasive**: no `ServerMain`, no `CairoEngine`, no migrations, no background jobs.
- **Read-only behavior**: do not modify db metadata or create startup artifacts.
- **Corruption-tolerant**: partial results are acceptable; failures are localized per table.
- **Defensive-first parsing**: never trust file sizes, offsets, counts, or strings from on-disk metadata.
- **No hard crash on bad data**: malformed table files must not terminate the recovery session.
- **Extensible command model**: easy to add future commands (`check`, `truncate`, `repair`).

## 5. Proposed User Experience

## 5.1 Entry command

Target command:

```bash
questdb.sh recover -d <root>
```

Equivalent Java main:

```bash
java -p questdb.jar -m io.questdb/io.questdb.RecoveryMain -d <root>
```

## 5.2 Interactive prompt

On launch:

- resolve root + db paths,
- acquire safety lock (if configured),
- discover tables,
- show short help.

Initial command set:

- `tables` : list discovered tables.
- `show <name|index>` : show `_txn` state for one table.
- `help` : command help.
- `quit` / `exit` : leave recovery mode.

## 6. Architecture (Phase 1)

### 6.1 Main classes

- `RecoveryMain`
  - CLI parsing (`-d`, optional `--db`, `--verbose`).
  - lifecycle and exit codes.
- `RecoverySession`
  - REPL loop and command dispatch.
- `TableDiscoveryService`
  - discovers tables from db root directories.
  - reads `_name` / `_meta` hints where available.
- `TxnStateService`
  - reads and formats `_txn` state for a selected table.
- `ConsoleRenderer`
  - renders compact tables + detailed state blocks.

Implementation note:

- all phase-1 classes live under `io.questdb.recovery` in `core` module.

### 6.2 Data objects

- `DiscoveredTable`
  - table name, dir name, inferred type/wal flags (when available), status.
- `TxnState`
  - txn-level fields (`txn`, `seqTxn`, row counts, versions, lag fields, symbol counts).
- `PartitionState`
  - partition timestamp, name txn, size, parquet/read-only flags, parquet file size.
- `ReadIssue`
  - severity + message + optional cause info.

### 6.3 Defensive guardrails

- Every table read path is wrapped in a per-table `try/catch`; failure in one table must not abort `tables` or `show` for other tables.
- `_txn` and `_meta` parsing must validate boundaries before each read:
  - file length minimum checks,
  - offset + width overflow checks,
  - section size sanity checks (symbols, partition section),
  - count sanity checks derived from file size, not trusted header values.
- Corrupt values are reported, not asserted:
  - no recovery logic relying on JVM assertions,
  - convert parser failures into `ReadIssue` entries.
- Parser output is capped to prevent memory blowups on garbage metadata:
  - max displayed partitions/symbols per table (configurable),
  - truncated output with explicit `TRUNCATED_OUTPUT` issue.
- Session-level crash containment:
  - unexpected runtime exceptions are caught at command boundary,
  - session continues after printing command failure summary.

Reader strategy guardrail:

- phase-1 source-of-truth parser for `_txn` is a dedicated bounded reader (`BoundedTxnReader`) using fd reads (`read`/`pread` style), not mmap.
- `TxReader` may be used as an optional secondary parser only after size-stability checks pass; it never gates command success.

## 7. Data Flow

## 7.1 DB path resolution

Input is install root (`-d <root>`), then:

1. attempt to read `<root>/conf/server.conf` for `cairo.root`,
2. if relative, resolve relative to install root,
3. fallback to `<root>/db` if config missing/unreadable.

This avoids full `Bootstrap` initialization while matching existing config behavior as closely as possible.

## 7.2 Safety lock

Acquire exclusive lock on `${dbRoot}/tables.d.lock` using existing file-lock utilities.

- If lock file is missing: attempt best-effort creation and continue.
- If lock file cannot be created/opened due filesystem state or permissions: continue with warning (`LOCK_UNAVAILABLE`), do not fail startup.
- If lock is present but acquisition fails due active holder: refuse to enter recovery mode by default and print "database appears to be running".
- No override mode for an actively held lock in recovery mode. Operator must stop the running database first.

## 7.3 Table discovery

Primary strategy (filesystem-first, corruption-tolerant):

1. iterate direct subdirectories of `dbRoot`,
2. classify entries:
   - `HAS_TXN`: `<dir>/_txn` exists,
   - `WAL_ONLY`: main `_txn` missing, but WAL/sequencer artifacts exist (for example `<dir>/txn_seq/_txnlog`),
   - `NO_TXN`: table-like directory discovered but no readable `_txn`,
3. determine display name:
   - `_name` file from the table directory (`<dir>/_name`) if present and valid,
   - otherwise derive from directory name using the same rule as `TableUtils.getTableNameFromDirName()` (strip from first `~` to end),
4. read `_meta` opportunistically for partitioning and timestamp type hints.

Why filesystem-first:

- `tables.d.*` may be stale/truncated after crash.
- directory scan still surfaces data that exists physically.
- unreadable directory entries are skipped with warning, not fatal.

## 7.4 `_txn` state extraction

Preferred path:

1. obtain `partitionBy` and `timestampType` from `_meta` (if possible),
2. parse `_txn` with bounded fd reads (`BoundedTxnReader`) using strict range checks on every field access,
3. load all txn fields and partition entries,
4. render human-readable + raw numeric values.

Fallback path:

- if `_meta` is missing/corrupt, decode `_txn` in raw mode (no partition-name formatting by partition granularity).
- if `_txn` itself is unreadable, show structured error (`CORRUPT_TXN`/`MISSING_TXN`).
- if `_txn` is partially readable, return partial header/partition data with `PARTIAL_READ` issue instead of failing command.
- if optional secondary `TxReader` parsing fails, keep bounded-reader result and append informational issue (`TXREADER_SKIPPED` or `TXREADER_FAILED`).

Memory-mapped safety policy:

- recovery parsing never requires mmap to succeed.
- if mmap is used in any optional path, mapping must be bounded to current file length and every accessed offset must be checked in-range before dereference.

Note: `_txn` inspection is metadata-centric and does not guarantee payload file integrity. Output should explicitly mark this as "txn view only" when no payload validation was run.

## 7.5 Variable-size AUX/DATA mismatch handling

This corruption class is a primary target for **Phase 2** integrity checks and must be handled defensively.

Phase 1 behavior:

- `show` remains `_txn`-centric and does not validate var-file payload integrity.
- output should explicitly indicate that var-file integrity was not checked.

Planned check model:

1. For each partition and each var-size column (`STRING`, `VARCHAR`, `BINARY`, `ARRAY`), compute expected stored row count from partition row count and column top.
2. Validate AUX file minimum size from column driver semantics:
   - `STRING`/`BINARY`: N+1 aux offsets,
   - `VARCHAR`/`ARRAY`: N aux entries.
3. Read terminal offset/size using the type driver (`getDataVectorSizeAtFromFd`) and compare with DATA file length.
4. Optionally run deeper validation:
   - monotonic offset progression,
   - offsets and lengths never exceed DATA EOF,
   - detect sparse/holey vectors (`isSparseDataVector`) when safe to map.
5. Emit structured issues per partition/column; never abort the whole table scan.
6. Cross-check `_meta` column declarations against discovered partition column files and emit metadata/file-count mismatch issues.

Severity rule:

- any detected AUX/DATA mismatch is classified as `ERROR` (not `WARN`).

Recommended issue codes for this class:

- `AUX_FILE_MISSING`
- `DATA_FILE_MISSING`
- `AUX_TRUNCATED`
- `DATA_TRUNCATED`
- `AUX_DATA_SIZE_MISMATCH`
- `AUX_OFFSET_INVALID`
- `AUX_VECTOR_SPARSE`
- `COLUMN_TOP_MISMATCH`
- `META_COLUMN_FILES_MISMATCH`

Recommended issue codes for phase 1:

- `MISSING_FILE`
- `SHORT_FILE`
- `INVALID_OFFSET`
- `INVALID_COUNT`
- `OUT_OF_RANGE`
- `CORRUPT_TXN`
- `CORRUPT_META`
- `PARTIAL_READ`
- `IO_ERROR`
- `TRUNCATED_OUTPUT`
- `META_COLUMN_COUNT_MISMATCH`
- `TXREADER_SKIPPED`
- `TXREADER_FAILED`

## 8. Output Design

## 8.1 `tables` output

Columns (initial):

- index
- table_name
- dir_name
- discovery_state (`HAS_TXN`, `WAL_ONLY`, `NO_TXN`)
- type (table/view/matview/unknown)
- wal (true/false/unknown)
- status (`OK`, `WARN`, `ERROR`)

Status mapping rule:

- in Phase 2, any var-file mismatch issue (`AUX_DATA_SIZE_MISMATCH`, `DATA_TRUNCATED`, `AUX_OFFSET_INVALID`, etc.) forces table status to `ERROR`.

## 8.2 `show` output

Sections:

1. Table identity and discovered metadata.
2. `_txn` header state:
   - `txn`, `seqTxn`,
   - `transientRowCount`, `fixedRowCount`, `rowCount`,
   - `minTimestamp`, `maxTimestamp`,
   - `dataVersion`, `structureVersion`, `partitionTableVersion`, `columnVersion`, `truncateVersion`,
   - lag fields + checksum where available.
3. Table metadata and file-level diagnostics:
   - `walEnabled` from `_meta` when readable,
   - `_txn` file size in bytes,
   - `_meta` file size in bytes.
4. Symbols section (`dense symbol index`, `count`, `transient/uncommitted count`).
5. Partitions section (one row per attached partition):
   - index, timestamp, nameTxn, size, parquet/readOnly flags, parquet file size.
6. Issues section (if partial/corrupt reads occurred).

## 9. Integration Plan

## 9.1 Shell integration

Extend `core/src/main/bin/questdb.sh` with a new command branch:

- `recover` command,
- reuses root arg handling (`-d`),
- sets `JAVA_MAIN` to recovery main class.

## 9.2 Main class location

Place recovery classes in `core` module (same runtime artifact as `ServerMain`) so the command is available in packaged QuestDB distribution.

## 10. Testing Strategy

### Unit tests

- db root resolution (absolute/relative/missing config).
- directory-based table discovery and name fallback logic.
- `_txn` parsing for normal and malformed files.
- renderer formatting for table list and detailed output.
- raw decoder bounds checking for random offsets/counts.
- command-level exception containment (session survives parser/runtime exceptions).

### Integration tests

- temporary db root with synthetic table directories:
  - valid `_txn` + `_meta`,
  - missing `_meta`,
  - truncated `_txn`,
  - `_txn` with garbage bytes and inconsistent section sizes,
  - `_txn` with corrupted partition counts claiming very large partition sections,
  - `_meta` with invalid timestamp index/column counts,
  - var-size column aux/data mismatch (`.i` and `.d` inconsistent) (Phase 2),
  - truncated var-size DATA while AUX still references prior offsets (Phase 2),
  - truncated AUX with intact DATA (Phase 2),
  - renamed WAL-style dir names.
- lock behavior when `tables.d.lock` is already held.
- startup/read behavior on read-only filesystem mounts.

### Fuzz tests

- fuzz `_txn` and `_meta` byte streams with random mutations (truncate, extend, flip bits, junk suffixes).
- assert no JVM crash / OOM / infinite loop.
- assert command returns structured issues and remains interactive.

### Safety checks

- CI assertions:
  - verify no file writes in normal recovery-mode execution,
  - verify no calls to migration/checkpoint/runtime startup paths,
  - verify recovery mode works on read-only filesystem roots.

## 11. Future Extensions (Post-Phase 1)

- `check <table>`: integrity checks across `_txn`, `_meta`, `_cv`, partition directories.
- `check <table> --var-columns`: focused integrity checks for `STRING`/`VARCHAR`/`BINARY`/`ARRAY` AUX/DATA consistency.
- `truncate <table> ...`: guided repair action with dry-run + confirmation.
- `repair-txn`: regenerate or patch `_txn` from detected filesystem truth.
- JSON export mode for support bundles.

The proposed structure keeps these as additional commands without redesigning the entrypoint.

## 12. Open Questions

1. Should `tables` include system tables by default, or hide them unless explicitly requested?
2. For the `show` command, do you prefer only human-readable timestamps, or both human-readable and raw epoch values?
3. Should phase 1 include non-interactive one-shot commands (for example `recover --list`, `recover --show <table>`) in addition to interactive mode?
4. Is Linux shell entry (`questdb.sh recover`) sufficient initially, or should we design simultaneous Windows launcher support in the first implementation?
