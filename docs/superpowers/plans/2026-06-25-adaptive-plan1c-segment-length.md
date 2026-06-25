# Adaptive Plan 1c — WAL segment-length validation (audit #6)

> Execute via superpowers:subagent-driven-development. Plan 3 of the integrity foundation.

**Goal:** Before WAL apply maps a segment column to the committed `[rowLo,rowHi)` row range (which comes only from the WAL-e events file), validate the on-disk segment column file is long enough. A torn/short segment tail (WAL-e/sequencer for txn K persisted but the segment `.d/.i` tail didn't — asymmetric flush, common under NOSYNC) currently maps past EOF → silent zeros/garbage rows or SIGBUS. Fix = detect → throw `CairoException` → the existing apply suspend path triggers (it's correct, just under-triggered). Recovery-time validation; helps every commit mode.

**Audit #6 (verbatim):** "WAL apply trusts WAL-e row range; segment `.d/.i` sizes never validated before mapping. Committed `[rowLo,rowHi)` comes only from the WAL-e events file; segment columns are mapped to that range with no `hi<=ff.length` check… **Fix:** validate segment file lengths against the declared range before mapping → throw → table suspends."

**Files (this branch):**
- `core/src/main/java/io/questdb/cairo/TableWriterSegmentFileCache.java` — the segment-column open/map path (audit pointed ~lines 239-291). This is where each WAL segment column file is mmapped to a size derived from `rowHi`. Add the length guard here, before/at the `configureDataMemOM` calls.
- `core/src/main/java/io/questdb/cairo/{ColumnTypeDriver,VarcharTypeDriver,StringTypeDriver,ArrayTypeDriver}.java` — `configureDataMemOM` is the per-type mapping entry; the fixed-width vs var-len (aux/data) required sizes are computed here. The guard must cover both fixed (`rowHi * typeSize`) and var-len (the aux `.i` length-prefix → data `.d` extent) layouts.
- `core/src/main/java/io/questdb/cairo/wal/WalTxnDetails.java:717-719` — source of the declared `[rowLo,rowHi)` (`commitInfo.getStartRowID()/getEndRowID()`), for reference.

**Design:** at the segment-column map site, compute the minimum required file length for the declared `rowHi` (fixed: `rowHi * columnTypeSize`; var-len: validate the `.i`/aux file covers `rowHi` entries AND the `.d`/data file covers the offset the last aux entry points to — mirror the existing partition-open validation rigor noted in `CORRUPTION_AUDIT.md §3` / `attachPartitionCheckFilesMatchVarSizeColumn`). If `required > ff.length(fd)` → throw `CairoException.critical(METADATA_VALIDATION).put("WAL segment column too short for committed row range …")`. Production-grade throw (not an `assert`), so it fires in embedded builds too.

**Tasks (TDD):**
1. Fixed-width column guard: build a WAL table, commit a txn, truncate a segment `.d` file shorter than `rowHi * size`, `drainWalQueue()` → assert `engine.getTableSequencerAPI().isSuspended(tt)` (instead of today's silent zeros / SIGBUS). Implement the fixed-width length check.
2. Var-len column guard (VARCHAR/STRING): same, truncate the `.d` (data) or `.i` (aux) of a varchar segment column → suspend. Implement the var-len check (aux covers rowHi; data covers last aux offset).
3. Negative control: the truncation must be UNDETECTED before the fix (silent apply / no suspend) and detected after — verify, don't fudge.
4. Regression: WAL apply + failure suites green (`WalTableSqlTest`, `WalTableFailureTest`, `WalWriterTest`, varchar/string WAL tests).

**Also (closes the Plan-1 final-review torn-tail gap):** add a "truncate the last `_event` record mid-body" test if convenient — a short `_event` tail should now be caught by this segment/length validation or the Plan-1 `_event` CRC. (Optional; primary scope is segment `.d/.i`.)

**Back-compat:** pure detection (throw instead of map-past-EOF). No format change. Behavior change only on already-corrupt/torn segments (previously silent → now loud suspend), which is the intended improvement.
