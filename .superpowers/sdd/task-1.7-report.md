# Task 1.7 Report: Store DELETE as a WAL SQL txn + dispatch at apply

## Summary

Made `DeleteOperation` storable in the WAL as a `SQL` transaction (mirroring `UpdateOperation`) and dispatched at apply time. Added `apply(DeleteOperation)` to the `TableWriterAPI` interface and gave a body to every implementor found in the tree (3 total, confirmed by both a multiline `implements` search and a clean `test-compile`).

**Commit**: `f1a37d1c4f` - "feat(delete): store DELETE as WAL SQL txn and dispatch to executeDelete at apply"

## Edits (5 files from the brief + 1 extra implementor found by compile-search)

1. **`core/src/main/java/io/questdb/cairo/wal/WalWriter.java`** — added `apply(DeleteOperation)` immediately before `apply(UpdateOperation)` (was at old line ~272-274; kept the `// Returns table transaction number` comment attached to the UpdateOperation method it originally documented). Body is a verbatim mirror of `apply(UpdateOperation)`: `operation.authorize()`, reject with `CairoException.critical(0)` if `inTransaction()` (uncommitted inserts), else `return applyNonStructural(operation, true)`. `applyNonStructural` takes `AbstractOperation`, and `DeleteOperation extends AbstractOperation`, so no signature friction. Added `import io.questdb.griffin.engine.ops.DeleteOperation;` next to the existing `AlterOperation`/`UpdateOperation` imports.

2. **`core/src/main/java/io/questdb/cairo/TableWriterAPI.java`** — declared `long apply(DeleteOperation operation);` between the existing `apply(AlterOperation...)` and `apply(UpdateOperation)` declarations (keeps the file's alphabetical-by-param-type convention: Alter → Delete → Update). Added the `DeleteOperation` import next to `AlterOperation`/`UpdateOperation`.

3. **`core/src/main/java/io/questdb/cairo/TableWriter.java`** — added the throwing `apply(DeleteOperation)` impl immediately before `apply(UpdateOperation)` (old line ~942), verbatim per the brief: `operation.authorize()` then `return operation.apply(this, true)`. This delegates into `DeleteOperation.apply(MetadataService, boolean)`, which (from Task 1.4, already in the tree) unconditionally throws `CairoException.nonCritical("DELETE is only supported on WAL tables...")` — so this is a real throwing path, not just a comment claiming it throws. Added the `DeleteOperation` import.

4. **`core/src/main/java/io/questdb/cairo/wal/ApplyWal2TableJob.java`** — added `case CMD_DELETE_TABLE:` in `processWalSql`'s switch, directly after `case CMD_UPDATE_TABLE:`, calling `operationExecutor.executeDelete(tableWriter, sql, seqTxn)` and setting `mvRefreshTask.operation = MatViewRefreshTask.INVALIDATE` / `invalidationReason = DeleteOperation.MAT_VIEW_INVALIDATION_REASON` when `deleted > 0`, mirroring the UPDATE case exactly. Added both imports named in the brief: `import io.questdb.griffin.engine.ops.DeleteOperation;` (placed alphabetically before `UpdateOperation`) and `import static io.questdb.tasks.TableWriterTask.CMD_DELETE_TABLE;` (placed alphabetically between the existing `CMD_ALTER_TABLE` and `CMD_UPDATE_TABLE` static imports).

5. **`core/src/main/java/io/questdb/cairo/wal/OperationExecutor.java`** — added the `executeDelete` stub between `executeAlter` and `executeUpdate` (matches the file's alphabetical method ordering: acquireMemoryTracker, close, executeAlter, executeDelete, executeUpdate, getBindVariableService, ...). Body is exactly the brief's stub: `throw new UnsupportedOperationException("executeDelete not implemented yet");`. Real body deferred to Task 1.8; no test in the current suite drains/applies a DELETE yet, so this stub is never exercised.

6. **`core/src/test/java/io/questdb/test/cutlass/line/tcp/SymbolCacheTest.java`** (not in the brief's file list — found via compile-search, see below) — the private record `TestTableWriterAPI` implements `TableWriterAPI` directly as a test double. Added `apply(DeleteOperation operation) { return 0; }` immediately before its existing `apply(UpdateOperation operation) { return 0; }`, matching that sibling method's no-op pattern (this double doesn't throw for any `apply` overload — it's a pure stub used to drive `SymbolCache` tests, not real DELETE/UPDATE semantics). Added the `DeleteOperation` import next to `AlterOperation`/`UpdateOperation`.

## Implementors of `TableWriterAPI` found and touched

Confirmed the complete, authoritative set two ways: (a) `rg -U '(?s)(class|record)\s+\w+[^{]*\bimplements\b[^{]*\bTableWriterAPI\b' -g '*.java'` across the **entire repo** (all modules: core, benchmarks, compat, utils — only core has any `TableWriterAPI` reference at all), and (b) a clean `mvn -q -pl core -am test-compile` after adding the interface method, which would have failed loudly ("is not abstract and does not override abstract method") on any implementor left without a body.

| Implementor | File | Impl style |
|---|---|---|
| `WalWriter` | `core/src/main/java/io/questdb/cairo/wal/WalWriter.java` | Real: store as SQL via `applyNonStructural` (WAL path) |
| `TableWriter` | `core/src/main/java/io/questdb/cairo/TableWriter.java` | Throwing (v1: non-WAL DELETE unsupported) |
| `TestTableWriterAPI` (private record) | `core/src/test/java/io/questdb/test/cutlass/line/tcp/SymbolCacheTest.java` | No-op stub (`return 0`), mirrors its own `apply(UpdateOperation)` |

No other implementors exist. `CompiledQueryImpl.java` and `AbstractCairoTest.java` etc. reference `TableWriterAPI` only as a parameter/field type, not as an `implements` target — confirmed by the multiline regex search above, which returned exactly these 3 files.

## Compile result

- `mvn -q -pl core -am compile` — **BUILD SUCCESS** (exit 0, no errors; only pre-existing unrelated `sun.misc.Unsafe` deprecation warnings from Guava/Maven internals).
- `mvn -q -pl core -am test-compile` — **BUILD SUCCESS** (exit 0, no errors). This is the acceptance criterion from the brief and also the mechanism that proved the `SymbolCacheTest` test double was the only test-source implementor needing an edit.
- `git diff --check` — clean, no whitespace errors.

## Self-review (from the task prompt)

- **Does `WalWriter.apply(DeleteOperation)` faithfully mirror `apply(UpdateOperation)`?** Yes — same authorize/inTransaction-guard/applyNonStructural(op, true) shape, only the exception message text differs ("cannot delete from table with uncommitted inserts" vs "cannot update..."), which is intentional and matches the brief verbatim.
- **Added the interface method next to the existing `apply(UpdateOperation)` declaration?** Yes, immediately above it in `TableWriterAPI.java`, preserving the file's alphabetical-by-parameter-type convention (Alter, Delete, Update).
- **Did every `TableWriterAPI` implementor get a body?** Yes — all 3 (`WalWriter`, `TableWriter`, `TestTableWriterAPI`), verified by both static search and a green `test-compile`.
- **New source carrying the Apache license header?** N/A — no new files were created in this task, only existing files edited. Verified `git status` shows no untracked `.java` files from this work (only the pre-existing untracked `.superpowers/` directory, unrelated).
- **Formatting stays clean?** `git diff --check` reports no whitespace issues; every insertion follows the surrounding file's 4-space indentation, brace style, and import ordering.

## Concerns

None blocking. Two judgment calls worth flagging for reviewers:

1. **`SymbolCacheTest.TestTableWriterAPI` was not in the brief's file list.** The brief anticipated this class of finding ("Compile-find any OTHER implementors... give each a throwing impl consistent with how it handles `apply(UpdateOperation)`"). I followed the "consistent with how it handles `apply(UpdateOperation)`" clause literally: since this double's `apply(UpdateOperation)` is a no-op returning `0` (not a throw), I made `apply(DeleteOperation)` the same no-op shape rather than force a `throw`. If a reviewer intended literally every implementor to throw regardless of local convention, this one line would need to change to a throw — but that would break the "consistent with" instruction and this double's established pattern (none of its `apply` overloads throw).
2. **`executeDelete` stub is intentionally unreachable by the current test suite** (per the task description) — there's no negative-path or positive-path test added in this task, by design; real coverage arrives with Task 1.8's real implementation.
