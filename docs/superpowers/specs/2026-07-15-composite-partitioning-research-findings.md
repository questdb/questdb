# QuestDB Composite (time + symbol) Partitioning — Research Findings

Grounded in `/home/nick/claude/hub/questdb` @ `2041d82a18`. Every claim below is anchored to
`file:line` verified by direct file reads across 7 parallel research agents. Prior-art from web
sources (cited inline). **Research-integrity note:** two agent tool-outputs exhibited the known
tool-output tampering pattern (foreign `【F:†】` citation format on one; silent `rg` token-rewriting
of "split"/"o3.partition.split" on another). Both were handled by trusting only `Read`-tool file
contents; the first agent was re-dispatched. No conclusions rest on tampered output.

Naming note: **"sub-partition" is already taken** internally — it means the O3 *temporal* splits of a
single time partition (`TableWriter.java:13589`, the `2023-01-01.5` dirs). This feature needs a
distinct name (candidate: **composite partitioning** / **partition dimensions** / **space partitioning**).

---

## 1. Current architecture is time-only, single-`int` `partitionBy` — end to end

- `PartitionBy` is an all-static int-enum: `DAY=0, MONTH=1, YEAR=2, NONE=3, HOUR=4, WEEK=5, NOT_APPLICABLE=6`
  (`cairo/PartitionBy.java:46-55`). Every bucket method is `(int timestampType, int partitionBy)` — **no column
  argument anywhere** (`PartitionBy.java:71-105`, `TimestampDriver.java:395-401`). Class comment literally scopes
  it to "time partitioning."
- Parser reads **exactly one literal**: `SqlParser.parseCreateTablePartition` (`SqlParser.java:2227-2233`). No list grammar.
- The value is a plain `int` from parse → operation → metadata: `CreateTableOperationImpl.partitionBy` (`:94`),
  `TableStructure.getPartitionBy():int` (`TableStructure.java:54`), `CairoTable.partitionBy` (`:47`),
  `TableReaderMetadata`/`TableWriterMetadata` (`:318`/`:154`).
- Validation: partitioned ⇒ designated timestamp required (`SqlParser.java:1709`, `CreateTableOperationImpl.java:688`);
  WAL ⇒ partitioned (`CairoEngine.java:2394`); mat view ⇒ partitioned (`SqlParser.java:1559`); DEDUP ⇒ WAL
  (`SqlCompilerImpl.java:1301`); TTL ⇒ partitioned + integer multiple of interval (`PartitionBy.java:124`).

## 2. Metadata persistence is LOW-friction to extend

- `_meta` offset 4 = `META_OFFSET_PARTITION_BY` single INT (`TableUtils.java:128`, written `:2623`).
- **~75 reserved header bytes** free at offsets 53–127 (`writeMetadata` jumps to `META_OFFSET_COLUMN_TYPES=128` at `:2639`).
- **Additive minor-version mechanism** `META_FORMAT_MINOR_VERSION_LATEST=2` (`TableUtils.java:117`) — exactly how
  TTL and TABLE_FORMAT were added **without** bumping major `ColumnType.VERSION=426` or breaking old tables.
- Precedent for variable-length trailing metadata: the covering-index block (`TableUtils.java:2670-2678`).
- ⇒ A second dimension can be persisted backward-compatibly. Major bump + `mig/` migration only if we touch existing offsets.

## 3. Disk layout — nested subdir is the natural fit; symbol dict is already favorable

- Path scheme: `dbRoot/<tableName>~<tableId>/<partitionDir>/<columnFile>` (`TableUtils.getTableDir` `:1081`).
- Partition dir = date-format (DAY `yyyy-MM-dd`, HOUR `yyyy-MM-ddটHH`, MONTH `yyyy-MM`, WEEK `YYYY-Www`, YEAR `yyyy`)
  + optional `.<nameTxn>` suffix on O3 split/convert (`PartitionBy.setSinkForPartition:99`, `TableUtils.setSinkForNativePartition:2452`).
- Per-partition files: `<col>.d` (data / symbol int-keys), `<col>.i` (var-len aux), `<col>.k`/`.v` (bitmap index).
- Table-root files: `_meta`, `_txn`, `_cv`, `_name`, `_todo_`, `_txn_scoreboard`, and the **GLOBAL symbol dictionary**
  `<col>.c/.o/.k/.v` shared by all partitions (`SymbolMapWriter` created at `path.trimTo(pathSize)`, `TableWriter.java:5542`).
- **Layout A — nested subdir** `…/2023-01-01/<symbolKey>/price.d`: cleanly extends the path builders; column-file
  builders (`dFile`/`iFile`/`keyFileName`) take an arbitrary prefix → **no change** needed. FAVORED.
- **Layout B — flat encoded** `…/2023-01-01.<symbolKey>/`: **collides** with the `.<nameTxn>` split suffix grammar
  (`setSinkForNativePartition:2454`, `parsePartitionDirName` trims to date-pattern length). Ruled out.
- **Global symbol dict is favorable**: partitions store only dense int keys; a `(day, symbol)` cell reuses the same
  key space with zero dictionary duplication. Caveat: dict is append-only & table-wide ⇒ dropping a symbol cell
  can't reclaim dictionary entries.

## 4. The central 1-D data structures (the load-bearing rewrites)

1. **`TxReader.attachedPartitions`** — flat `LongList`, **4 longs/partition** (`LONGS_PER_TX_ATTACHED_PARTITION`),
   sorted ascending by a **single** partition-floor timestamp (`PARTITION_TS_OFFSET=0`, `TxReader.java:74-75`).
   Every lookup is `binarySearchBlock` on that one timestamp key (`:858-861`, `getPartitionIndex:325`,
   `getPartitionTimestampByTimestamp:387`). THE central axis.
2. **`ColumnVersionWriter`** — keyed by `(partitionTimestamp, columnIndex)`, timestamp-sorted binary search
   (`ColumnVersionWriter.java:283`, reader `ColumnVersionReader.java:110-128`). Holds per-(partition,column)
   nameTxn + column-top.
3. **`TableReader.openPartitionInfo`** — mirror array, 8-long slots, `getPartitionIndexByTimestamp` binary search
   on the timestamp slot (`TableReader.java:487-495`); `getColumnBase = partitionIndex << columnCountShl` positional
   column mapping (`:347-349`).
4. **`TableWriter.partitionTimestampHi`** scalar boundary (`:387`) + append routing test (`:2727`) + `openPartition`
   opening **one** column set (`:9042`).
5. **`TxWriter.switchPartitions`** — appends new slot at array tail assuming strictly increasing timestamp (`:510-530`).

## 5. Write path — WAL-only scoping concentrates the change

- In-order append hot path: scalar `partitionTimestampHi` boundary check; `openPartition` opens exactly ONE set of
  column memories. Making this 2-D would need N concurrently-open column sets — **the nasty part**.
- **WAL apply BYPASSES the in-order path.** WAL segments are flat, append-ordered, partition-agnostic (roll on row
  count; `WalWriter.java:459-479, 1833-1850`) — **no WAL format change needed** for a symbol dimension. All routing
  is deferred to apply.
- **Both direct commit AND WAL apply converge on `TableWriter.processO3Block`** (`:9404`), which sorts the batch by
  designated timestamp only (`Vect.radixSort…`, `:8026`) and routes via `getPartitionTimestampByTimestamp` (`:9440`).
  Direct: `commit → o3Commit → processO3Block` (`:8161`). WAL: `commitWalInsertTransactions → processWalCommit →
  processWalCommitFinishApply → processO3Block` (`:11056`). Mat-view REPLACE_RANGE refresh lands here too.
  ⇒ **Scoping to WAL tables** (already required for mat views, dedup, and the modern default) concentrates routing
  into `processO3Block` and sidesteps the N-open-columns problem.
- Split (`cairo.o3.partition.split.min.size` default 50 MB, `O3PartitionJob.java:1496`), squash (auto on every commit;
  `last.max.splits=20`, `mid.max.splits=1`; `TableWriter.java:13604`), dedup (needs ts as key; handles multi-column
  keys within a ts group; `O3PartitionJob.java:1875-2098`) are all 1-D-timestamp but compose *within* a cell.
- Symbol interning: `putSym` writes the global-map int key into `.d` (`TableWriter.java:15390`). Global map **helps**
  storage, gives **no** routing help, and complicates cell lifecycle/GC.

## 6. Read path — the perf thesis, and the exact hook

- **Today `WHERE symbol='X'` prunes partitions by TIME ONLY.** The partition-set (frame) factory is chosen purely
  from the timestamp interval — `IntervalPartitionFrameCursorFactory` if `hasIntervalFilters()` else `FullPartition…`
  (`SqlCodeGenerator.java:10344/10357`); **`keyColumn` is not consulted for partition selection**. The symbol key
  then drives a **per-partition bitmap index seek** across *every* time-selected partition
  (`SymbolIndexRowCursorFactory.getCursor` → `pageFrame.getIndexReader(...)`, once per frame; driver loop
  `PageFrameRecordCursorImpl.java:126-142`). Net: N index seeks even when only a few partitions hold `X`.
- **Time pruning lives in** `AbstractIntervalPartitionFrameCursor.cullPartitions` (interval → partition-index window
  by `getPartitionIndexByTimestamp`, `:196-207`) and the per-frame min/max skip in `IntervalFwdPartitionFrameCursor.next`.
- **Insertion point for symbol pruning:** `SqlCodeGenerator.java:10375–10631` (the `intrinsicModel.keyColumn != null`
  block) — symbol keys are already resolved here but used only to build a *row* cursor; thread them into the
  *frame-cursor factory* (`:10344/10357`) so it enumerates only matching partition directories. LATEST-ON twin at `:6487`.
- **Existing precedent for non-time pruning:** Parquet **row-group skipping by min/max stats**
  (`ParquetRowGroupFilter.canSkipRowGroup`, `:72-91`) — already skips sub-partition units by a non-time predicate,
  just only *within* a partition. Symbol partitioning generalizes it to whole directories.
- **LATEST ON** benefits big: `LATEST ON … WHERE symbol='X'` could open just the newest `(*, X)` cell and read one
  row, instead of walking time partitions newest-first index-seeking each (`LatestByAllIndexedRecordCursor.java:234`).
- **Ordering caveat:** `PageFrameSequence.buildAddressCache` numbers frames 0..N-1 in time order and ordered collect
  assumes frame-index == time order (`:496-501, 547-571`). A 2-D enumeration must preserve time order within the
  selected symbol set for SAMPLE BY / ordered scans.

## 7. Indexes — the cost multiplier == the prior-art failure mode

- `.k`/`.v` are **per partition** (`BitmapIndexUtils.keyFileName/valueFileName`; reader cache one-per-partition
  `TableReader.java:408-413`). No cross-partition sharing.
- Cost per distinct symbol value **per partition**: one 32-byte `.k` slot (`getKeyEntryOffset=key*32+64`) + a whole
  `.v` block reserved (default `blockValueCount=256` ⇒ **~2 KB minimum** even for a single occurrence,
  `BitmapIndexWriter.java:471-487`). A table with P partitions pays this **P times**.
- **The index code has no notion that a column is constant within a partition** (Q1: confirmed none). If we partition
  BY symbol S, an index on S is pointless (one key, whole-partition `.v` chain) — **so skip building it**.
- A 2-D scheme multiplies partition directories (time × S), and **each `(time,S)` cell gets its own full `.k/.v` set**
  for *other* indexed symbols ⇒ per-cell fixed overhead + small-files blowup for high-cardinality S. **This is the
  concrete mechanism of the universal high-cardinality failure mode.**
- Read-side "use index vs scan" gate: `WhereClauseParser.columnIsPreferredOrIndexedAndNotPartOfMultiColumnLatestBy`
  (`:2443-2453`). For partition-key S: decline to promote S to an index scan, prune instead. Write-side skip gate:
  `indexBlockCapacity > -1` / `metadata.isColumnIndexed` (`O3CopyJob.java:619`, `TableWriter.java:5218`).

## 8. Materialized views — inherit structurally free; refresh loses pruning unless made symbol-aware

- A mat view **IS a normal partitioned WAL table** with its **own** `PARTITION BY` (own `_meta` + `_mv` sidecar;
  `CairoEngine.createMatView → createTableOrViewOrMatViewUnsecure`; auto-derived from sampling interval if omitted,
  `CreateMatViewOperationImpl.java:687-710`). ⇒ views can be composite-partitioned with no new storage machinery.
- **Incremental refresh is 100% time-interval driven**: `WalTxnRangeLoader` extracts only ts min/max per base txn
  (`:163-172`); `setRange(tsLo,tsHi)` (`MatViewRefreshJob.java:1132`); write-back is a REPLACE_RANGE over `[tsLo,tsHi)`
  (`:1144`). Over a composite base it stays **correct** but **recomputes all symbols in the touched time range** —
  losing the symbol-pruning benefit unless the loader/range/REPLACE_RANGE bounds become symbol-aware.
- Known gap: base `DROP/DETACH PARTITION` + rows-affected `UPDATE` commit as non-data txns the interval scan skips
  (`WalTxnRangeLoader.java:153-157`) — **dropping a symbol cell on the base would widen this existing gap**.

## 9. Prior art — the recurring lessons (all cited in the prior-art agent report)

- **Universal failure mode:** high-cardinality categorical partition key → too many tiny partitions/files → metadata
  + planning + I/O blowup. Numeric ceilings converge: ClickHouse ≲1k partitions (key card <1k–10k), Delta/Hive
  ≤1k–2k, InfluxDB <10k total.
- **Universal safety valve:** bound cardinality by **bucketing** `bucket(N, col)` / `hash % N` — Oracle
  `SUBPARTITION BY HASH(col) SUBPARTITIONS N`, ClickHouse, Timescale `by_hash('col',N)`, Influx tag-bucket `col,N`,
  Druid `numShards`, Pinot `numPartitions`, Iceberg `bucket(N,col)`. **Never raw value-per-partition on high card.**
- **Partitioning ≠ indexing:** the sort key (QuestDB's designated timestamp) stays the row-level accelerator;
  partitioning is a coarse *pruning + parallelism* layer. Only claim pruning for equality/IN (Pinot) or
  range-over-transform (Iceberg) predicates.
- **Ergonomic winner = Iceberg hidden partitioning:** transforms in DDL (`PARTITIONED BY (days(ts), bucket(16,id),
  category)`), partition values in **metadata not directory names**, filters on the *natural* column auto-derive the
  partition filter, and **partition evolution without rewrite** (each manifest tagged with its spec-id).
- **Steal:** track partition values in metadata (not dir-per-value) to prune without directory listing; Oracle's
  "time partition = logical container, categorical = physical segment" is a clean framing (QuestDB's physical unit
  today is the time-partition dir; a cell becomes the new physical unit inside it).

---

## Design decision surface (forks that change the build)

- **A. Ingestion scope:** WAL-only (concentrates change in `processO3Block`; recommended) vs also non-WAL legacy path.
- **B. Cardinality safety:** raw value-per-partition (matches floated syntax, dangerous on high card) vs mandatory
  bucketing `bucket(N,col)` (safe) vs both (Iceberg-style transforms) vs unbounded.
- **C. Syntax:** `PARTITION BY DAY, sym1, sym2` / `… sym1, sym2, DAY` / `timestamp(DAY), sym1, sym2` /
  Iceberg-style transforms `days(ts), bucket(16, sym)`. Time-first vs time-anywhere. Interaction with B.
- **D. Disk layout:** nested subdir `2023-01-01/<key>/` (recommended, natural fit) — mostly a technical call.
- **E. Primary goal:** query pruning / parallel ingest+I/O / storage tiering + lifecycle (per-cell placement, drop,
  retention) / multi-tenant isolation. Reforks priorities.
- **F. Edition:** OSS core vs Enterprise (tiering/replication/object-store coupling).
- **G. Evolution:** new-tables-only (Timescale "empty hypertable only") vs ALTER to add/drop a dimension
  (Iceberg-style) vs convert existing tables.
