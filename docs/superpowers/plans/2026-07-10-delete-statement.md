# DELETE statement — v1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `DELETE FROM <table> WHERE <predicate>` on WAL tables in QuestDB OSS + Enterprise, using the existing replace-commit machinery, deferred to WAL apply like UPDATE.

**Architecture:** A `DELETE` is parsed into a new `ExecutionModel.DELETE`, compiled into a `DeleteOperation`, and stored in the WAL as a `SQL` transaction (`CMD_DELETE_TABLE`) — exactly like UPDATE. At WAL-apply time, `OperationExecutor.executeDelete` recompiles the predicate and, per affected partition, removes the matched rows. The correct-but-unoptimized core (Phase 1) rewrites each affected partition with its **survivors** (`SELECT * WHERE NOT(pred)`) via a new `TableWriter.replaceRange(lo, hi, survivorCursor)` primitive that exposes the existing `WAL_DEDUP_MODE_REPLACE_RANGE` apply path to a cursor row-source. Later phases add a whole-partition-drop fast path (Phase 2, Parquet-safe), a Parquet convert-to-native fallback for rewrites (Phase 3), and full Enterprise permission + replication support (Phase 4).

**Tech Stack:** Java 17+ (QuestDB core, package `io.questdb.*`); Enterprise Java (package `com.questdb.*`, depends on the OSS jar); Maven; JUnit 4 with QuestDB's `AbstractCairoTest` + fluent `assertQuery(...)`. No Rust changes in v1 (the Rust `WalTxnType` enum is untouched — DELETE reuses the existing `SQL` txn byte).

## Global Constraints

- **Repos:** OSS worktree `/home/nick/claude/wt/oss/delete-statement` (branch `delete-statement`, off `master`). Enterprise `/home/nick/claude/hub/questdb-enterprise` (Phase 4 only).
- **WAL-only in v1.** Non-WAL tables, joins/`USING`/multi-table, and DELETE targeting a materialized view are all rejected at compile with a clear message.
- **WHERE is mandatory.** Bare `DELETE FROM t` → error pointing to `TRUNCATE TABLE t`.
- **`delete` is NOT a reserved keyword.** Recognize it statement-initially via `isDeleteKeyword`; do **not** add it to `SqlKeywords.KEYWORDS` (mirrors `truncate`).
- **No new WAL txn-type byte.** DELETE is a `WalTxnType.SQL` transaction; the Rust `WalTxnType` enum stays untouched (keeps Enterprise replication free).
- **Test style:** fluent `assertQuery(...)` / `QueryAssertion`, never raw `printSql` + `TestUtils.assertEquals`. WAL tests drain via `drainWalQueue()`.
- **Build/format:** build & test with the repo's configured JDK via `mvn`. CI runs the IntelliJ formatter + `git diff --exit-code` — keep formatting clean. Commit frequently.
- **OSS↔Enterprise version coupling:** Phase 4 builds Enterprise against the OSS jar. After finishing OSS phases, `mvn -q -pl core -am install -DskipTests` in the OSS worktree so the Enterprise module resolves the updated `org.questdb:questdb` snapshot. If Enterprise CI complains about an OSS version mismatch, bump `ossversion` / merge Enterprise `main` per the repo's usual companion-branch process.
- **Spec:** `docs/superpowers/specs/2026-07-10-delete-statement-design.md` (anchor map in §16 there).

---

## Phase 1 — Correct DELETE on non-Parquet WAL tables (survivor-replace)

End state: `DELETE FROM t WHERE <pred>` works correctly on non-Parquet WAL tables for both time-range and arbitrary predicates (unoptimized: every affected partition is rewritten with its survivors; fully-covered partitions become empty and are dropped by the existing replace path). Non-WAL / mat-view / no-WHERE rejected. Parquet partitions error via the existing guard (fixed in Phases 2–3).

### Task 1.1: Register the DELETE command + compiled-query type

**Files:**
- Modify: `core/src/main/java/io/questdb/tasks/TableWriterTask.java:36-43,61-68`
- Modify: `core/src/main/java/io/questdb/griffin/CompiledQuery.java:63-73` (constants tail)
- Modify: `core/src/main/java/io/questdb/griffin/model/ExecutionModel.java`

**Interfaces:**
- Produces: `TableWriterTask.CMD_DELETE_TABLE` (int); `CompiledQuery.DELETE` (short); `ExecutionModel.DELETE` (int).

- [ ] **Step 1: Add the WAL command constant.** In `TableWriterTask.java`, add to the constant block:

```java
    public static final int CMD_DELETE_TABLE = 5;
```

(Values in use: `CMD_UNUSED=1, CMD_ALTER_TABLE=2, CMD_UPDATE_TABLE=3, CMD_STORAGE_POLICY=4`. `5` is free.) Then extend `getCommandName`:

```java
    public static String getCommandName(int cmd) {
        return switch (cmd) {
            case CMD_ALTER_TABLE -> "ALTER TABLE";
            case CMD_STORAGE_POLICY -> "STORAGE POLICY";
            case CMD_UPDATE_TABLE -> "UPDATE TABLE";
            case CMD_DELETE_TABLE -> "DELETE TABLE";
            default -> "UNKNOWN COMMAND";
        };
    }
```

- [ ] **Step 2: Add the CompiledQuery type.** In `CompiledQuery.java`, change the tail of the constants block from:

```java
    short TABLE_REBASE = ALTER_STORAGE_POLICY + 1; // 38
    short EMPTY = TABLE_REBASE + 1;
    short TYPES_COUNT = EMPTY;
```

to:

```java
    short TABLE_REBASE = ALTER_STORAGE_POLICY + 1; // 38
    short DELETE = TABLE_REBASE + 1; // 39
    short EMPTY = DELETE + 1;
    short TYPES_COUNT = EMPTY;
```

- [ ] **Step 3: Add the execution-model type.** In `ExecutionModel.java`, change:

```java
    int COMPILE_VIEW = CREATE_VIEW + 1;     // 10
    int MAX = COMPILE_VIEW + 1;
```

to:

```java
    int COMPILE_VIEW = CREATE_VIEW + 1;     // 10
    int DELETE = COMPILE_VIEW + 1;          // 11
    int MAX = DELETE + 1;
```

and add to the `typeNameMap` static initializer, after the `COMPILE_VIEW` line:

```java
            typeNameMap[ExecutionModel.DELETE] = "Delete from";
```

- [ ] **Step 4: Compile OSS to verify it still builds.**

Run: `cd /home/nick/claude/wt/oss/delete-statement && mvn -q -pl core -am compile`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit.**

```bash
git add core/src/main/java/io/questdb/tasks/TableWriterTask.java core/src/main/java/io/questdb/griffin/CompiledQuery.java core/src/main/java/io/questdb/griffin/model/ExecutionModel.java
git commit -m "feat(delete): register DELETE command, compiled-query and execution-model types"
```

### Task 1.2: `isDeleteKeyword`

**Files:**
- Modify: `core/src/main/java/io/questdb/griffin/SqlKeywords.java` (near `isUpdateKeyword`, ~line 2457)
- Test: `core/src/test/java/io/questdb/test/griffin/SqlKeywordsTest.java` (create if absent; otherwise add a method)

**Interfaces:**
- Produces: `SqlKeywords.isDeleteKeyword(CharSequence): boolean`

- [ ] **Step 1: Write the failing test.** Create/extend `SqlKeywordsTest`:

```java
package io.questdb.test.griffin;

import io.questdb.griffin.SqlKeywords;
import org.junit.Assert;
import org.junit.Test;

public class SqlKeywordsTest {
    @Test
    public void testIsDeleteKeyword() {
        Assert.assertTrue(SqlKeywords.isDeleteKeyword("delete"));
        Assert.assertTrue(SqlKeywords.isDeleteKeyword("DELETE"));
        Assert.assertTrue(SqlKeywords.isDeleteKeyword("Delete"));
        Assert.assertFalse(SqlKeywords.isDeleteKeyword("delet"));
        Assert.assertFalse(SqlKeywords.isDeleteKeyword("deleted"));
        Assert.assertFalse(SqlKeywords.isDeleteKeyword("update"));
    }
}
```

- [ ] **Step 2: Run it, verify it fails to compile / fails.**

Run: `mvn -q -pl core test -Dtest=SqlKeywordsTest#testIsDeleteKeyword`
Expected: FAIL (`isDeleteKeyword` not defined).

- [ ] **Step 3: Implement.** In `SqlKeywords.java`, next to `isUpdateKeyword`:

```java
    public static boolean isDeleteKeyword(CharSequence tok) {
        return tok.length() == 6
                && (tok.charAt(0) | 32) == 'd'
                && (tok.charAt(1) | 32) == 'e'
                && (tok.charAt(2) | 32) == 'l'
                && (tok.charAt(3) | 32) == 'e'
                && (tok.charAt(4) | 32) == 't'
                && (tok.charAt(5) | 32) == 'e';
    }
```

- [ ] **Step 4: Run, verify pass.**

Run: `mvn -q -pl core test -Dtest=SqlKeywordsTest#testIsDeleteKeyword`
Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add core/src/main/java/io/questdb/griffin/SqlKeywords.java core/src/test/java/io/questdb/test/griffin/SqlKeywordsTest.java
git commit -m "feat(delete): add isDeleteKeyword recognizer"
```

### Task 1.3: `SecurityContext.authorizeTableDelete` (OSS)

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/SecurityContext.java` (near `authorizeTableTruncate`, line 171)
- Modify: `core/src/main/java/io/questdb/cairo/security/AllowAllSecurityContext.java` (near line 228)
- Modify: `core/src/main/java/io/questdb/cairo/security/ReadOnlySecurityContext.java` (near line 263)
- Modify: **every other OSS `SecurityContext` implementor the compiler reports** (add the method following the AllowAll no-op / ReadOnly throw pattern).

**Interfaces:**
- Produces: `SecurityContext.authorizeTableDelete(TableToken tableToken)` — table-scoped (no column list), abstract.

- [ ] **Step 1: Add the abstract method.** In `SecurityContext.java`, directly after `void authorizeTableTruncate(TableToken tableToken);`:

```java
    void authorizeTableDelete(TableToken tableToken);
```

- [ ] **Step 2: Compile OSS to enumerate the implementors that now fail.**

Run: `mvn -q -pl core -am compile`
Expected: FAIL — one "does not override abstract method authorizeTableDelete" error per implementor. Record the list.

- [ ] **Step 3: Implement in `AllowAllSecurityContext` (no-op):**

```java
    @Override
    public void authorizeTableDelete(TableToken tableToken) {
    }
```

- [ ] **Step 4: Implement in `ReadOnlySecurityContext` (throw):**

```java
    @Override
    public void authorizeTableDelete(TableToken tableToken) {
        throw CairoException.authorization().put("Write permission denied").setCacheable(true);
    }
```

- [ ] **Step 5: Implement in every other OSS implementor the compiler reported.** Use the no-op form for "allow-all"-style contexts and the throwing form for read-only/deny contexts. Re-run `mvn -q -pl core -am compile` until BUILD SUCCESS.

- [ ] **Step 6: Commit.**

```bash
git add core/src/main/java/io/questdb/cairo/SecurityContext.java core/src/main/java/io/questdb/cairo/security/
git commit -m "feat(delete): add SecurityContext.authorizeTableDelete (OSS impls)"
```

### Task 1.4: `DeleteOperation`

**Files:**
- Create: `core/src/main/java/io/questdb/griffin/engine/ops/DeleteOperation.java`

**Interfaces:**
- Consumes: `AbstractOperation`, `TableWriterTask.CMD_DELETE_TABLE`, `SecurityContext.authorizeTableDelete`.
- Produces: `DeleteOperation` with a public constructor `DeleteOperation(TableToken, int tableId, long tableVersion, int tableNamePosition, @Nullable RecordCursorFactory survivorFactory)`, `getSurvivorFactory()`, `MAT_VIEW_INVALIDATION_REASON`, `authorize()` calling `authorizeTableDelete`, `isStructural()==false`, `apply(MetadataService,...)` throwing (WAL-only; direct/non-WAL path unsupported in v1).

- [ ] **Step 1: Write the class.** Model on `UpdateOperation`. DELETE carries no column list; on WAL tables the factory is discarded (SQL text replays at apply), so `survivorFactory` is usually null. `apply(MetadataService,...)` is only reached on the non-WAL immediate path, which v1 does not support, so it throws.

```java
package io.questdb.griffin.engine.ops;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.AsyncWriterCommand;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.MetadataService;
import io.questdb.std.Misc;
import io.questdb.tasks.TableWriterTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.tasks.TableWriterTask.CMD_DELETE_TABLE;

public class DeleteOperation extends AbstractOperation {
    public static final String MAT_VIEW_INVALIDATION_REASON = "delete operation";
    private RecordCursorFactory survivorFactory;

    public DeleteOperation(
            @NotNull TableToken tableToken,
            int tableId,
            long tableVersion,
            int tableNamePosition,
            @Nullable RecordCursorFactory survivorFactory
    ) {
        init(CMD_DELETE_TABLE, TableWriterTask.getCommandName(CMD_DELETE_TABLE), tableToken, tableId, tableVersion, tableNamePosition);
        this.survivorFactory = survivorFactory;
    }

    @Override
    public long apply(MetadataService svc, boolean contextAllowsAnyStructureChanges) {
        // v1 supports WAL tables only; the WAL-apply path uses OperationExecutor.executeDelete,
        // not this method. A direct (non-WAL) apply is rejected at compile time, so reaching here
        // is a programming error.
        throw CairoException.nonCritical()
                .put("DELETE is only supported on WAL tables [table=")
                .put(getTableToken().getTableName())
                .put(']');
    }

    @Override
    public void authorize() {
        final SecurityContext securityContext = this.securityContext;
        if (securityContext == null) {
            throw CairoException.nonCritical()
                    .put("delete security context is empty [table=")
                    .put(getTableToken().getTableName())
                    .put(']');
        }
        securityContext.authorizeTableDelete(getTableToken());
    }

    @Override
    public void close() {
        survivorFactory = Misc.free(survivorFactory);
    }

    @Override
    public AsyncWriterCommand deserialize(TableWriterTask task) {
        return task.getAsyncWriterCommand();
    }

    public RecordCursorFactory getSurvivorFactory() {
        return survivorFactory;
    }

    @Override
    public boolean isStructural() {
        return false;
    }

    @Override
    public String matViewInvalidationReason() {
        return MAT_VIEW_INVALIDATION_REASON;
    }

    @Override
    public void serialize(TableWriterTask task) {
        super.serialize(task);
        task.setAsyncWriterCommand(this);
    }
}
```

- [ ] **Step 2: Compile.**

Run: `mvn -q -pl core -am compile`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit.**

```bash
git add core/src/main/java/io/questdb/griffin/engine/ops/DeleteOperation.java
git commit -m "feat(delete): add DeleteOperation"
```

### Task 1.5: Parse + compile DELETE to a `DeleteOperation` (compile-level, WAL-only, validation)

**Files:**
- Modify: `core/src/main/java/io/questdb/griffin/SqlParser.java` (dispatch in `parse(...)` ~line 5561; add `parseDelete`)
- Modify: `core/src/main/java/io/questdb/griffin/SqlCompilerImpl.java` (`compileUsingModel` switch ~line 3891; a `generateDelete` helper)
- Modify: `core/src/main/java/io/questdb/griffin/CompiledQueryImpl.java` (dispatcher, `ofDelete`, `getDeleteOperation`, `execute()` case, `closeAllButSelect`)
- Modify: `core/src/main/java/io/questdb/griffin/CompiledQuery.java` (add `getDeleteOperation()` declaration)
- Test: `core/src/test/java/io/questdb/test/griffin/DeleteTest.java` (create)

**Interfaces:**
- Consumes: `ExecutionModel.DELETE`, `DeleteOperation`, `CompiledQuery.DELETE`.
- Produces: a `DELETE` statement compiling to `CompiledQuery.DELETE`; `CompiledQuery.getDeleteOperation(): DeleteOperation`; validation errors for no-WHERE, non-WAL, mat-view, non-existent column.

**Design note on the model:** DELETE reuses the UPDATE query-model shape minus the SET list. The simplest robust approach: build a `QueryModel` with `modelType = ExecutionModel.DELETE`, the target table name/alias, and the WHERE clause attached to a nested model (so the existing optimiser/where-clause machinery applies). At `generateDelete` time on a WAL table, validate and produce a `DeleteOperation` with a **null** factory (SQL text replays at apply); on the WAL-apply pass (`isWalApplication()`), build the survivor factory (Task 1.8 consumes it). For v1 the compile-time factory is not needed by the executor (the executor recompiles survivors itself), so `generateDelete` may always pass `null` and only *validate*.

- [ ] **Step 1: Write the failing tests.** Create `DeleteTest` (WAL-parameterized like `UpdateTest`). Start with compile-level behavior:

```java
package io.questdb.test.griffin;

import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class DeleteTest extends AbstractCairoTest {

    @Test
    public void testDeleteCompilesToDeleteType() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x int) timestamp(ts) partition by DAY WAL");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery cc = compiler.compile("DELETE FROM t WHERE x = 1", sqlExecutionContext);
                Assert.assertEquals(CompiledQuery.DELETE, cc.getType());
                Assert.assertNotNull(cc.getDeleteOperation());
            }
        });
    }

    @Test
    public void testDeleteRequiresWhere() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x int) timestamp(ts) partition by DAY WAL");
            try {
                execute("DELETE FROM t");
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "WHERE");
            }
        });
    }

    @Test
    public void testDeleteRejectsNonWal() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x int) timestamp(ts) partition by DAY BYPASS WAL");
            try {
                execute("DELETE FROM t WHERE x = 1");
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "WAL");
            }
        });
    }

    @Test
    public void testDeleteRejectsUnknownColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (ts timestamp, x int) timestamp(ts) partition by DAY WAL");
            try {
                execute("DELETE FROM t WHERE nope = 1");
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "nope");
            }
        });
    }
}
```

- [ ] **Step 2: Run, verify they fail.**

Run: `mvn -q -pl core test -Dtest=DeleteTest`
Expected: FAIL (DELETE not recognized / `getDeleteOperation` missing).

- [ ] **Step 3: Add the parser dispatch + `parseDelete`.** In `SqlParser.parse(...)`, next to the `isUpdateKeyword` branch (~line 5561):

```java
        if (isDeleteKeyword(tok)) {
            return parseDelete(lexer, sqlParserCallback);
        }
```

Implement `parseDelete` modeled on `parseUpdate`/`parseDmlUpdate` but with no SET clause: expect `FROM`, parse the table name (with optional alias), then require `WHERE` and parse the predicate onto a nested model; set `modelType = ExecutionModel.DELETE`. If the next token after the table is not `WHERE`, raise `SqlException.$(pos, "WHERE clause is required for DELETE; use TRUNCATE TABLE to remove all rows")`. (Read the current `parseDmlUpdate`/`parseUpdateClause` bodies for the exact `QueryModel`/pool wiring to mirror.)

- [ ] **Step 4: Add the compiler model dispatch + `generateDelete`.** In `SqlCompilerImpl.compileExecutionModel0(...)` add a `case ExecutionModel.DELETE:` mirroring UPDATE's (optimise the nested WHERE, resolve the table token via `executionContext.getTableToken(...)`, open `getMetadataForWrite`). In `compileUsingModel(...)`'s switch add:

```java
                case ExecutionModel.DELETE:
                    compiledQuery.ofDelete(generateDelete((QueryModel) model, executionContext));
                    break;
```

Implement `generateDelete(QueryModel model, SqlExecutionContext executionContext)`:
1. Resolve `TableToken tableToken = executionContext.getTableToken(model.getTableName())`.
2. Open `try (TableRecordMetadata metadata = executionContext.getMetadataForWrite(tableToken))`.
3. Reject non-WAL: `if (!metadata.isWalEnabled()) throw SqlException.$(model.getTableNamePosition(), "DELETE is only supported on WAL tables");`
4. Reject mat view: use the same guard UPDATE/INSERT uses (search for the `isMatView`/materialized-view rejection in the compiler and copy it) → `throw SqlException.$(pos, "cannot delete from materialized view");`
5. Validate the WHERE predicate compiles against `metadata` (build and immediately close a filter factory via the existing where-clause machinery, or call the shared validation the optimiser exposes) so unknown columns / bad types fail here.
6. Return `new DeleteOperation(tableToken, metadata.getTableId(), metadata.getMetadataVersion(), model.getTableNamePosition(), null)`.

- [ ] **Step 5: Wire `CompiledQueryImpl`.** Add the declaration to `CompiledQuery.java` after `getUpdateOperation()`:

```java
    DeleteOperation getDeleteOperation();
```

In `CompiledQueryImpl`: add a field `private DeleteOperation deleteOp;`, add a `deleteOperationDispatcher` in the constructor mirroring `updateOperationDispatcher`:

```java
        deleteOperationDispatcher = new OperationDispatcher<>(engine, "sync 'DELETE' execution") {
            @Override
            protected long apply(DeleteOperation operation, TableWriterAPI writerAPI) {
                try {
                    return writerAPI.apply(operation);
                } finally {
                    operation.clearSecurityContext();
                }
            }
        };
```

declare the field `private final OperationDispatcher<DeleteOperation> deleteOperationDispatcher;`, add:

```java
    @Override
    public DeleteOperation getDeleteOperation() {
        return deleteOp;
    }

    public void ofDelete(DeleteOperation deleteOperation) {
        this.deleteOp = deleteOperation;
        this.type = DELETE;
        this.isExecutedAtParseTime = false;
    }
```

add the `execute()` case (mirror UPDATE):

```java
            case DELETE:
                deleteOp.withSqlStatement(sqlStatement);
                return deleteOperationDispatcher.execute(deleteOp, sqlExecutionContext, eventSubSeq, closeOnDone);
```

and the `closeAllButSelect()` case:

```java
            case CompiledQuery.DELETE:
                Misc.free(deleteOp);
                break;
```

`writerAPI.apply(DeleteOperation)` is added in Task 1.7 — until then, add a stub overload so this compiles (see Task 1.7 note), or implement Task 1.7 before compiling this task.

- [ ] **Step 6: Run the compile-level tests.**

Run: `mvn -q -pl core test -Dtest=DeleteTest#testDeleteCompilesToDeleteType+testDeleteRequiresWhere+testDeleteRejectsNonWal+testDeleteRejectsUnknownColumn`
Expected: PASS. (`testDeleteExecutes*` come in Task 1.8.)

- [ ] **Step 7: Commit.**

```bash
git add core/src/main/java/io/questdb/griffin/SqlParser.java core/src/main/java/io/questdb/griffin/SqlCompilerImpl.java core/src/main/java/io/questdb/griffin/CompiledQuery.java core/src/main/java/io/questdb/griffin/CompiledQueryImpl.java core/src/test/java/io/questdb/test/griffin/DeleteTest.java
git commit -m "feat(delete): parse+compile DELETE to DeleteOperation (WAL-only, validated)"
```

### Task 1.6: Register the DELETE query executors in the HTTP + PG processors

**Files:**
- Modify: `core/src/main/java/io/questdb/cutlass/http/processors/JsonQueryProcessor.java` (~line 123 registration; assert ~line 150)
- Modify: `core/src/main/java/io/questdb/cutlass/pgwire/PGPipelineEntry.java` (command-tag + row-count switches, ~lines 3504, 3830)

**Interfaces:**
- Produces: the `CompiledQuery.TYPES_COUNT`-completeness assert in `JsonQueryProcessor` passes with `DELETE` registered; PG-wire reports a `DELETE n` command tag.

- [ ] **Step 1: Read** `JsonQueryProcessor` around the executor registration + the `queryExecutors.size() == CompiledQuery.TYPES_COUNT + 1` assert, and the UPDATE registration line, to learn the exact `extendAndSet(...)` idiom.

- [ ] **Step 2: Register a DELETE executor.** Add, mirroring UPDATE's registration (which reports affected rows), a `CompiledQuery.DELETE` entry pointing to the same "update/DML affected rows" executor UPDATE uses (DELETE returns an affected-row count the same way). If UPDATE uses a dedicated `executeUpdate` method there, add an analogous `executeDelete` that runs the operation future and reports `affectedRowsCount`.

- [ ] **Step 3: PG-wire tag.** In `PGPipelineEntry`, add `CompiledQuery.DELETE` to the switches that map a compiled type to a command tag and to row-count reporting, emitting the tag `DELETE` (Postgres form `DELETE <rows>`). Mirror the UPDATE cases.

- [ ] **Step 4: Run the processor assertion path.**

Run: `mvn -q -pl core test -Dtest=JsonQueryProcessor*Test` (pick an existing JsonQueryProcessor test that boots the processor so the `TYPES_COUNT` assert executes)
Expected: PASS (no assertion error about executor count).

- [ ] **Step 5: Commit.**

```bash
git add core/src/main/java/io/questdb/cutlass/http/processors/JsonQueryProcessor.java core/src/main/java/io/questdb/cutlass/pgwire/PGPipelineEntry.java
git commit -m "feat(delete): register DELETE executors in HTTP and PG-wire processors"
```

### Task 1.7: Store DELETE as a WAL SQL txn + dispatch at apply

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/wal/WalWriter.java` (add `apply(DeleteOperation)` near `apply(UpdateOperation)` line 272)
- Modify: `core/src/main/java/io/questdb/cairo/TableWriterAPI.java` (declare `apply(DeleteOperation)`)
- Modify: `core/src/main/java/io/questdb/cairo/TableWriter.java` (add `apply(DeleteOperation)` near line 942 — throws, since non-WAL DELETE is unsupported in v1)
- Modify: `core/src/main/java/io/questdb/cairo/wal/ApplyWal2TableJob.java` (`processWalSql` — add `case CMD_DELETE_TABLE`, ~line 887)
- Modify: `core/src/main/java/io/questdb/cairo/wal/OperationExecutor.java` (add `executeDelete` — stub in this task)

**Interfaces:**
- Consumes: `DeleteOperation`, `CMD_DELETE_TABLE`, `applyNonStructural`.
- Produces: `WalWriter.apply(DeleteOperation): long` (stores SQL text, returns seqTxn); `OperationExecutor.executeDelete(TableWriter, CharSequence sql, long seqTxn): long` (stub → real in Task 1.8).

- [ ] **Step 1: `WalWriter.apply(DeleteOperation)`** (mirror `apply(UpdateOperation)`):

```java
    @Override
    public long apply(DeleteOperation operation) {
        operation.authorize();
        if (inTransaction()) {
            throw CairoException.critical(0).put("cannot delete from table with uncommitted inserts [table=")
                    .put(tableToken.getTableName()).put(']');
        }
        return applyNonStructural(operation, true);
    }
```

- [ ] **Step 2: Declare in `TableWriterAPI`** the `long apply(DeleteOperation operation);` method (mirror the `apply(UpdateOperation)` declaration), and add the throwing `TableWriter` implementation near line 942:

```java
    @Override
    public long apply(DeleteOperation operation) {
        // v1: non-WAL DELETE is rejected at compile; a direct TableWriter apply is unsupported.
        operation.authorize();
        return operation.apply(this, true); // DeleteOperation.apply throws for the non-WAL path
    }
```

- [ ] **Step 3: Stub `OperationExecutor.executeDelete`** (mirror `executeUpdate`; real body in Task 1.8):

```java
    public long executeDelete(TableWriter tableWriter, CharSequence deleteSql, long seqTxn) throws SqlException {
        throw new UnsupportedOperationException("executeDelete not implemented yet");
    }
```

- [ ] **Step 4: Dispatch in `ApplyWal2TableJob.processWalSql`.** Add after the `CMD_UPDATE_TABLE` case:

```java
                        case CMD_DELETE_TABLE:
                            final long deleted = operationExecutor.executeDelete(tableWriter, sql, seqTxn);
                            if (deleted > 0) {
                                mvRefreshTask.operation = MatViewRefreshTask.INVALIDATE;
                                mvRefreshTask.invalidationReason = DeleteOperation.MAT_VIEW_INVALIDATION_REASON;
                            }
                            return;
```

Add the import `import static io.questdb.tasks.TableWriterTask.CMD_DELETE_TABLE;` and `import io.questdb.griffin.engine.ops.DeleteOperation;`.

- [ ] **Step 5: Compile.**

Run: `mvn -q -pl core -am compile`
Expected: BUILD SUCCESS.

- [ ] **Step 6: Commit.**

```bash
git add core/src/main/java/io/questdb/cairo/wal/WalWriter.java core/src/main/java/io/questdb/cairo/TableWriterAPI.java core/src/main/java/io/questdb/cairo/TableWriter.java core/src/main/java/io/questdb/cairo/wal/ApplyWal2TableJob.java core/src/main/java/io/questdb/cairo/wal/OperationExecutor.java
git commit -m "feat(delete): store DELETE as WAL SQL txn and dispatch to executeDelete at apply"
```

### Task 1.8 (SPIKE + impl): `TableWriter.replaceRange` primitive

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableWriter.java` (new method `replaceRange`, adapting `processWalCommitDedupReplace`, lines 10907-11021)
- Test: `core/src/test/java/io/questdb/test/cairo/TableWriterReplaceRangeDirectTest.java` (create)

**Interfaces:**
- Produces: `TableWriter.replaceRange(long replaceRangeLoTs, long replaceRangeHiExclTs, @Nullable RecordCursor survivorCursor, RecordToRowCopier copier, int timestampCursorIndex): long` — replaces all rows in `[lo, hiExcl)` with the survivor rows from the cursor (or empties the range if `survivorCursor == null`), reusing the replace-range O3 partition surgery; returns the number of rows deleted (rows-in-range-before minus survivors-written).

**Spike decision (do this first):** choose between:
- **(a) In-place cursor ingestion** — refactor `processWalCommitDedupReplace` so its row source can be a `RecordCursor` fed through `RecordToRowCopier` into the O3 staging (`o3MemColumns`) instead of a mmapped WAL segment, then run `processWalCommitFinishApply(..., dedupMode=REPLACE_RANGE, lag min/max = lo/hi)`. Least data movement.
- **(b) Scratch-segment** — write survivors to a temporary WAL segment via a private WalWriter-like path, then feed it through the existing `processWalCommitDedupReplace` unchanged.

Prototype both minimally against the empty-range case (survivorCursor == null); pick whichever reaches a green empty-range delete with the least new O3 code. Record the choice in a comment at the top of `replaceRange` and in `docs/superpowers/specs/2026-07-10-delete-statement-design.md` §8.

- [ ] **Step 1: Write the failing empty-range test.** This proves `replaceRange(lo, hi, null, ...)` deletes a sub-range in place (mirror `WalWriterReplaceRangeTest`'s NOT-BETWEEN reference approach, but invoke `replaceRange` directly on a `TableWriter`):

```java
package io.questdb.test.cairo;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class TableWriterReplaceRangeDirectTest extends AbstractCairoTest {
    @Test
    public void testReplaceRangeEmptyDeletesSubRange() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, x long) timestamp(ts) partition by DAY BYPASS WAL");
            execute("insert into src select (x*60*1000000L)::timestamp, x from long_sequence(200)");
            execute("create table ref as (select * from src where ts not between " +
                    "'1970-01-01T01:00:00.000000Z' and '1970-01-01T02:00:00.000000Z') " +
                    "timestamp(ts) partition by DAY BYPASS WAL");

            TableToken tt = engine.verifyTableName("src");
            long lo = io.questdb.std.datetime.microtime.MicrosTimestampDriver.floor("1970-01-01T01:00:00.000000Z");
            long hiExcl = io.questdb.std.datetime.microtime.MicrosTimestampDriver.floor("1970-01-01T02:00:00.000001Z");
            try (TableWriter w = getWriter(tt)) {
                w.replaceRange(lo, hiExcl, null, null, w.getMetadata().getTimestampIndex());
            }

            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "ref", "src", LOG);
        });
    }
}
```

- [ ] **Step 2: Run, verify it fails.**

Run: `mvn -q -pl core test -Dtest=TableWriterReplaceRangeDirectTest#testReplaceRangeEmptyDeletesSubRange`
Expected: FAIL (`replaceRange` not defined).

- [ ] **Step 3: Implement `replaceRange` (empty-range path first).** Following the spike decision, add a method that sets `this.dedupMode = WAL_DEDUP_MODE_REPLACE_RANGE`, `txWriter.setLagMinTimestamp(lo)`, `txWriter.setLagMaxTimestamp(hiExcl - 1)`, runs the same finish/apply path `processWalCommitDedupReplace` uses for its empty branch (`processWalCommitFinishApply(0,0,0,0,...)`), commits, and resets `dedupMode`. Reuse `commit()`/`commitTxWriter()` as `removePartition` does. Return the deleted count (compute as pre-range-count when survivorCursor==null). Keep the `RecordCursor` path unimplemented (throw) for now — Task 1.9 adds it.

- [ ] **Step 4: Run, verify pass.**

Run: `mvn -q -pl core test -Dtest=TableWriterReplaceRangeDirectTest#testReplaceRangeEmptyDeletesSubRange`
Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add core/src/main/java/io/questdb/cairo/TableWriter.java core/src/test/java/io/questdb/test/cairo/TableWriterReplaceRangeDirectTest.java docs/superpowers/specs/2026-07-10-delete-statement-design.md
git commit -m "feat(delete): add TableWriter.replaceRange primitive (empty-range path) [spike: option <a|b>]"
```

### Task 1.9: `replaceRange` survivor-cursor path

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableWriter.java` (`replaceRange` — cursor branch)
- Test: `core/src/test/java/io/questdb/test/cairo/TableWriterReplaceRangeDirectTest.java` (add a survivors test)

**Interfaces:**
- Consumes: `RecordCursor`, `RecordToRowCopier` (build via `RecordToRowCopierUtils`/`getRecordToRowCopier` as `MatViewRefreshJob` does).
- Produces: `replaceRange` with a non-null cursor replaces `[lo,hi)` with the cursor's rows.

- [ ] **Step 1: Write the failing survivors test.** Replace a range with a filtered subset and compare to a NOT-matching reference (mirror the mat-view copier loop: `newRow(ts)` / `copier.copy(ctx, record, row)` / `row.append()` for each survivor, then finish-apply):

```java
    @Test
    public void testReplaceRangeSurvivorsRewritesPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, x long, s symbol) timestamp(ts) partition by DAY BYPASS WAL");
            execute("insert into src select (x*60*1000000L)::timestamp, x, rnd_symbol('a','b','c') from long_sequence(300)");
            // survivors of "delete where x % 2 = 0" within the whole table:
            execute("create table ref as (select * from src where not (x % 2 = 0)) timestamp(ts) partition by DAY BYPASS WAL");
            // (implementation invokes replaceRange per partition with a survivor cursor built from
            //  'select * from src where not (x % 2 = 0) and ts >= partLo and ts < partHi')
            // Drive it through the executor in DeleteTest instead if a standalone copier harness is heavy.
        });
    }
```

If a standalone copier harness is too heavy, mark this test `@Ignore` with a pointer and rely on the end-to-end `DeleteTest` in Task 1.10 to exercise the cursor path (the executor builds the copier). Prefer the end-to-end coverage.

- [ ] **Step 2: Implement the cursor branch.** When `survivorCursor != null`: for the given `[lo,hi)`, iterate the cursor, `newRow(record.getTimestamp(timestampCursorIndex))`, `copier.copy(...)`, `row.append()`; then run the replace finish/apply exactly as the empty branch but with the appended rows (the sorted/unsorted handling already exists in `processWalCommitDedupReplace`). Assert each row's ts is within `[lo, hi)` (copy the mat-view guard). Reuse `getRecordToRowCopier(this, factory, compiler)` pattern for building the copier at the call site (executor, Task 1.10).

- [ ] **Step 3: Run.**

Run: `mvn -q -pl core test -Dtest=TableWriterReplaceRangeDirectTest`
Expected: PASS (or the survivors test `@Ignore`d with end-to-end coverage deferred to 1.10).

- [ ] **Step 4: Commit.**

```bash
git add core/src/main/java/io/questdb/cairo/TableWriter.java core/src/test/java/io/questdb/test/cairo/TableWriterReplaceRangeDirectTest.java
git commit -m "feat(delete): replaceRange survivor-cursor path"
```

### Task 1.10: `executeDelete` — per-partition survivor-replace (end-to-end DELETE)

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/wal/OperationExecutor.java` (`executeDelete` real body)
- Test: `core/src/test/java/io/questdb/test/griffin/DeleteTest.java` (add execution tests)

**Interfaces:**
- Consumes: `WalApplySqlExecutionContext` (already in `OperationExecutor`), `TableWriter.replaceRange`, `getRecordToRowCopier`.
- Produces: correct end-to-end DELETE on non-Parquet WAL tables.

**Executor algorithm (v1 Phase 1):**
1. Recompile the DELETE's WHERE against `tableWriter` under `executionContext` (`isWalApplication()==true`), extracting the boolean predicate.
2. Determine the candidate partition set: if the predicate carries a designated-timestamp interval bound, restrict to partitions intersecting it; else all partitions.
3. For each candidate partition `[partLo, partHi)` (partHi exclusive = next partition floor, or `maxTimestamp+1` for the last):
   a. Compile a survivor cursor: `SELECT * FROM <table> WHERE NOT (<pred>) AND ts >= partLo AND ts < partHi` ordered by ts.
   b. Build a `RecordToRowCopier` from the cursor to `tableWriter`.
   c. Call `tableWriter.replaceRange(partLo, partHi, survivorCursor, copier, cursorTsIndex)` and accumulate the returned deleted count.
4. Return the total deleted count.

(Phase 2 replaces 3a–3c with a whole-partition-drop fast path when a partition is fully covered; Phase 3 adds the Parquet convert-fallback around 3c.)

- [ ] **Step 1: Write the failing end-to-end tests** in `DeleteTest` (WAL-parameterized; drain via the base overrides like `UpdateTest`):

```java
    @Test
    public void testDeleteByArbitraryCondition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select (x*60*1000000L)::timestamp ts, x, rnd_symbol('a','b') s " +
                    "from long_sequence(10)) timestamp(ts) partition by DAY WAL");
            drainWalQueue();
            execute("DELETE FROM t WHERE x % 2 = 0");
            drainWalQueue();
            assertQuery("count\n5\n", "select count(*) from t", false, true);
            assertQuery("ts\tx\ts\n", "select * from t where x % 2 = 0", "ts", true, false);
        });
    }

    @Test
    public void testDeleteByTimeRangeAcrossPartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select (x*3600*1000000L)::timestamp ts, x " +
                    "from long_sequence(96)) timestamp(ts) partition by DAY WAL"); // 4 days
            drainWalQueue();
            execute("DELETE FROM t WHERE ts < '1970-01-03T00:00:00.000000Z'");
            drainWalQueue();
            assertQuery("min\n1970-01-03T00:00:00.000000Z\n", "select min(ts) from t", false, true);
        });
    }

    @Test
    public void testDeleteNoMatchIsNoOp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select (x*60*1000000L)::timestamp ts, x from long_sequence(10)) " +
                    "timestamp(ts) partition by DAY WAL");
            drainWalQueue();
            execute("DELETE FROM t WHERE x > 1000");
            drainWalQueue();
            assertQuery("count\n10\n", "select count(*) from t", false, true);
        });
    }

    @Test
    public void testDeleteEverythingEmptiesTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select (x*3600*1000000L)::timestamp ts, x from long_sequence(48)) " +
                    "timestamp(ts) partition by DAY WAL");
            drainWalQueue();
            execute("DELETE FROM t WHERE ts >= '1970-01-01T00:00:00.000000Z'");
            drainWalQueue();
            assertQuery("count\n0\n", "select count(*) from t", false, true);
        });
    }
```

- [ ] **Step 2: Run, verify they fail** (executor still throws / returns nothing).

Run: `mvn -q -pl core test -Dtest=DeleteTest`
Expected: FAIL.

- [ ] **Step 3: Implement `executeDelete`** per the algorithm above, mirroring `executeUpdate`'s recompile scaffolding (the `try (SqlCompiler compiler = engine.getSqlCompiler())` + `remapTableNameResolutionTo` + retry-on-`TableReferenceOutOfDateException` loop) and `MatViewRefreshJob.insertAsSelect`'s copier loop for the per-partition survivor scan. Enumerate partitions via `tableWriter`'s partition metadata (`getPartitionCount`, `getPartitionTimestamp`, `getPartitionSize`).

- [ ] **Step 4: Run, verify pass.**

Run: `mvn -q -pl core test -Dtest=DeleteTest`
Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add core/src/main/java/io/questdb/cairo/wal/OperationExecutor.java core/src/test/java/io/questdb/test/griffin/DeleteTest.java
git commit -m "feat(delete): executeDelete per-partition survivor-replace (end-to-end DELETE)"
```

### Task 1.11: Correctness matrix — dedup, O3/unordered, symbols, indexed, concurrency, mat-view

**Files:**
- Test: `core/src/test/java/io/questdb/test/griffin/DeleteTest.java` (extend)

**Interfaces:** none new — hardening.

- [ ] **Step 1: Add tests** (each: create WAL table, seed, DELETE, drain, `assertQuery`). Cover:
  - **Dedup table:** `create table ... dedup upsert keys(ts, s)`; delete a subset; assert survivors correct and still unique.
  - **O3 / unordered survivors:** seed with out-of-order inserts spanning a partition; delete a middle band; assert order + contents via a NOT-matching reference.
  - **Symbol + indexed symbol columns:** table with `symbol index`; delete by symbol value; assert index still returns correct rows (`WHERE s = 'a'`).
  - **Concurrency / serial semantics:** compile+store a DELETE (WHERE band) but, before draining, `INSERT` a row into the deleted band; drain; assert the later insert **survives** (delete was sequenced first). Use the compile-then-insert-then-drain ordering from `UpdateTest#testUpdateReadonlyFailsAtExecutionTime`'s explicit-drain style.
  - **Mat-view invalidation:** create an incremental mat view over `t`; DELETE base rows; drain; assert the mat view refreshes/invalidates to match.

- [ ] **Step 2: Run.**

Run: `mvn -q -pl core test -Dtest=DeleteTest`
Expected: PASS.

- [ ] **Step 3: Commit.**

```bash
git add core/src/test/java/io/questdb/test/griffin/DeleteTest.java
git commit -m "test(delete): dedup, O3, symbol/index, concurrency, mat-view invalidation"
```

---

## Phase 2 — Whole-partition-drop fast path (cheap + Parquet-safe drops)

End state: fully-covered partitions are removed via `removePartition` (O(1), no survivor scan) instead of survivor-replace; this also makes **whole-partition** time-range deletes work on Parquet partitions (drop needs no rewrite).

### Task 2.1: Fast-path fully-covered partitions in `executeDelete`

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/wal/OperationExecutor.java` (`executeDelete`)
- Test: `core/src/test/java/io/questdb/test/griffin/DeleteTest.java`

- [ ] **Step 1: Write the failing/asserting tests.**
  - Parquet full drop works: create WAL table across ≥2 days, `ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '<day1>'`, drain; `DELETE FROM t WHERE ts < '<day2>'`; drain; assert day1 gone and no "not supported for Parquet" error. (Mirror `ParquetRowGroupPruningTest`'s convert usage + `WalAlterTableSqlTest` drain.)
  - A "covered-partition uses drop not rewrite" behavioral check: delete a full partition and assert the result equals a NOT-BETWEEN reference (functional equivalence; the optimization itself is validated by the Parquet case passing without the guard firing).

```java
    @Test
    public void testDeleteWholeParquetPartitionByTimeRange() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select (x*3600*1000000L)::timestamp ts, x from long_sequence(72)) " +
                    "timestamp(ts) partition by DAY WAL"); // 3 days
            drainWalQueue();
            execute("alter table t convert partition to parquet list '1970-01-01'");
            drainWalQueue();
            execute("DELETE FROM t WHERE ts < '1970-01-02T00:00:00.000000Z'");
            drainWalQueue();
            assertQuery("min\n1970-01-02T00:00:00.000000Z\n", "select min(ts) from t", false, true);
        });
    }
```

- [ ] **Step 2: Run, verify the Parquet test fails** with "commit replace mode is not supported for Parquet partitions" (Phase 1 routed it through replaceRange).

Run: `mvn -q -pl core test -Dtest=DeleteTest#testDeleteWholeParquetPartitionByTimeRange`
Expected: FAIL (Parquet guard).

- [ ] **Step 3: Implement the fast path.** In `executeDelete`, before the survivor-replace branch, detect a **fully-covered** partition — i.e. the delete predicate reduces to a designated-timestamp interval and the partition's `[minTs, maxTs] ⊆ interval` (or, generally, the survivor query over the partition would return zero rows). For fully-covered partitions call `tableWriter.removePartition(partitionTimestamp)` and add the partition's row count to the deleted total, instead of `replaceRange`. Only fall through to survivor-replace for partial/arbitrary partitions.

- [ ] **Step 4: Run, verify pass** (Parquet whole-drop works now; all prior `DeleteTest` still green).

Run: `mvn -q -pl core test -Dtest=DeleteTest`
Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add core/src/main/java/io/questdb/cairo/wal/OperationExecutor.java core/src/test/java/io/questdb/test/griffin/DeleteTest.java
git commit -m "feat(delete): whole-partition-drop fast path (Parquet-safe time-range drops)"
```

---

## Phase 3 — Parquet convert-to-native fallback for rewrites

End state: a DELETE that must **rewrite** a Parquet partition (boundary trim or arbitrary-condition match) converts it to native first, in the same atomic apply, then rewrites — so DELETE works on all Parquet partitions.

### Task 3.1: Convert-fallback around `replaceRange`

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/wal/OperationExecutor.java` (`executeDelete`)
- Test: `core/src/test/java/io/questdb/test/griffin/DeleteTest.java`

- [ ] **Step 1: Write the failing test** — arbitrary/boundary delete on a Parquet partition:

```java
    @Test
    public void testDeleteArbitraryOnParquetPartitionConverts() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select (x*3600*1000000L)::timestamp ts, x from long_sequence(48)) " +
                    "timestamp(ts) partition by DAY WAL"); // 2 days
            drainWalQueue();
            execute("alter table t convert partition to parquet list '1970-01-01'");
            drainWalQueue();
            // deletes some rows WITHIN the parquet partition -> requires a rewrite
            execute("DELETE FROM t WHERE x % 2 = 0 AND ts < '1970-01-02T00:00:00.000000Z'");
            drainWalQueue();
            assertQuery("count\n36\n", "select count(*) from t", false, true); // 24 - 12 deleted + 24
            assertQuery("count\n0\n", "select count(*) from t where x % 2 = 0 and ts < '1970-01-02T00:00:00.000000Z'", false, true);
        });
    }
```

- [ ] **Step 2: Run, verify it fails** (Parquet guard on the rewrite).

Run: `mvn -q -pl core test -Dtest=DeleteTest#testDeleteArbitraryOnParquetPartitionConverts`
Expected: FAIL ("not supported for Parquet partitions").

- [ ] **Step 3: Implement the fallback.** In `executeDelete`, when a partition needs `replaceRange` (partial/arbitrary, not a full drop) **and** the partition is Parquet (`tableWriter.getPartitionFormat(pi) == PartitionFormat.PARQUET`), call `tableWriter.convertPartitionParquetToNative(partitionTimestamp, false)` to queue the un-tier, and after processing the batch of such partitions call `tableWriter.commitPendingParquetToNativeConversions()` before the `replaceRange` calls — mirroring `ConvertOperatorImpl`'s pre-pass (queue with `doCommit=false`, then one batched flush). Ensure the flush happens within the same `executeDelete` apply so it stays in one atomic transaction (see spec §14.6). Then `replaceRange` as normal (now native).

- [ ] **Step 4: Run, verify pass.**

Run: `mvn -q -pl core test -Dtest=DeleteTest`
Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add core/src/main/java/io/questdb/cairo/wal/OperationExecutor.java core/src/test/java/io/questdb/test/griffin/DeleteTest.java
git commit -m "feat(delete): Parquet convert-to-native fallback for partition rewrites"
```

### Task 3.2: Crash-safety / single-commit verification

**Files:**
- Test: `core/src/test/java/io/questdb/test/griffin/DeleteTest.java`

- [ ] **Step 1: Add a WAL-replay idempotence test** — run a DELETE that mixes a whole-partition drop, a boundary trim, and a Parquet convert-fallback in one statement across several partitions; drain; assert the result matches a NOT-matching reference. Then (using the engine's release/reopen pattern from `WalAlterTableSqlTest#testReleaseAndReopenWriters`) `engine.releaseInactive()` and re-drain to confirm re-apply is idempotent and the table is not suspended (`engine.getTableSequencerAPI().isSuspended(tt)` is false).

- [ ] **Step 2: Run.**

Run: `mvn -q -pl core test -Dtest=DeleteTest`
Expected: PASS. If the table suspends or re-apply diverges, the executor is forcing an intermediate commit — restructure `executeDelete` to a single writer commit at the end (see spec §14.6 / §17.6).

- [ ] **Step 3: Commit.**

```bash
git add core/src/test/java/io/questdb/test/griffin/DeleteTest.java
git commit -m "test(delete): mixed-strategy DELETE is atomic and re-apply-idempotent"
```

### Task 3.3: OSS full regression + format check

- [ ] **Step 1: Run the DELETE + adjacent suites.**

Run: `mvn -q -pl core test -Dtest=DeleteTest,UpdateTest,WalWriterReplaceRangeTest,WalAlterTableSqlTest,ParquetRowGroupPruningTest`
Expected: PASS.

- [ ] **Step 2: Format check** (CI runs the IntelliJ formatter + `git diff --exit-code`). Apply the repo's formatter to all touched files and ensure a clean `git status`.

- [ ] **Step 3: Commit any formatting.**

```bash
git add -A && git commit -m "style(delete): apply formatter" || echo "nothing to format"
```

---

## Phase 4 — Enterprise: DELETE permission + replication

End state: `Permission.DELETE` exists; GRANT/REVOKE/SHOW work; `authorizeTableDelete` is enforced per security context; a DELETE replicates primary→replica; a direct DELETE on a replica is rejected. Work happens in `/home/nick/claude/hub/questdb-enterprise`.

**Prerequisite:** `mvn -q -pl core -am install -DskipTests` in the OSS worktree so the Enterprise module compiles against the OSS jar carrying `authorizeTableDelete` + `CompiledQuery.DELETE`.

### Task 4.1: `Permission.DELETE`

**Files:**
- Modify: `questdb-ent/src/main/java/com/questdb/security/Permission.java`
- Test: `questdb-ent/src/test/java/com/questdb/acl/` (add or extend a permissions test)

**Interfaces:**
- Produces: `Permission.DELETE` exponent; name↔exponent maps; `ALL_TABLE`/`ALL` membership.

- [ ] **Step 1: Add the constant** at the next free exponent (above the frozen legacy boundary of 62), changing:

```java
    public static final int SET_TABLE_FORMAT = 72;
    public static final int EXP_MAX = SET_TABLE_FORMAT + 1;
```

to:

```java
    public static final int SET_TABLE_FORMAT = 72;
    public static final int DELETE = 73;
    public static final int EXP_MAX = DELETE + 1;
```

- [ ] **Step 2: Register the name maps.** Add near the other `namePermissionMap.put(...)`:

```java
        namePermissionMap.put("DELETE", DELETE);
```

and near the other `permissionNameMap.put(...)`:

```java
        permissionNameMap.put(DELETE, "DELETE");
```

- [ ] **Step 3: Add to the masks.** In both `ALL_TABLE` and `ALL`, add `DELETE` to the "table-only permissions" group (next to `INSERT, DROP_TABLE, RENAME_TABLE, TRUNCATE_TABLE`):

```java
            INSERT, DELETE,
```

- [ ] **Step 4: Write a test** asserting the permission round-trips by name and is table-scoped (mirror an existing `Permission` test — e.g. `Permission.typeOf("DELETE") == Permission.DELETE`, `getPermissionCount()` incremented, and DELETE is in `ALL_TABLE`).

- [ ] **Step 5: Compile Enterprise.**

Run: `cd /home/nick/claude/hub/questdb-enterprise && mvn -q -pl questdb-ent -am compile`
Expected: FAIL — the OSS interface now has an unimplemented `authorizeTableDelete` in the Enterprise security contexts (fixed in Task 4.2). This is expected; proceed to 4.2 before running tests.

- [ ] **Step 6: Commit.**

```bash
cd /home/nick/claude/hub/questdb-enterprise
git add questdb-ent/src/main/java/com/questdb/security/Permission.java questdb-ent/src/test/java/com/questdb/acl/
git commit -m "feat(delete): add Permission.DELETE (name maps + ALL_TABLE/ALL)"
```

### Task 4.2: `authorizeTableDelete` in the Enterprise security contexts

**Files:**
- Modify: `questdb-ent/src/main/java/com/questdb/security/EntSecurityContextBase.java` (near `authorizeTableTruncate`, line 555)
- Modify: `questdb-ent/src/main/java/com/questdb/security/DispatchingSecurityContext.java` (near line 566)
- Modify: `questdb-ent/src/main/java/com/questdb/security/AbstractReplicaSecurityContext.java` (near line 294)
- Modify: any other Enterprise `SecurityContext` implementor the compiler reports (e.g. `AdminSecurityContext` if it overrides table ops).

- [ ] **Step 1: `EntSecurityContextBase`** (mirror `authorizeTableTruncate` — table-scoped, protected-table check):

```java
    @Override
    public void authorizeTableDelete(TableToken tableToken) {
        checkNotProtectedTable(tableToken);
        refreshAndCheckAccessList().checkPermission(DELETE, tableToken);
    }
```

Add the `import static com.questdb.security.Permission.DELETE;` if permissions are statically imported (match how `TRUNCATE_TABLE` is referenced in that file).

- [ ] **Step 2: `DispatchingSecurityContext`** (delegate):

```java
    @Override
    public void authorizeTableDelete(TableToken tableToken) {
        delegate().authorizeTableDelete(tableToken);
    }
```

- [ ] **Step 3: `AbstractReplicaSecurityContext`** (read-only throw):

```java
    @Override
    public void authorizeTableDelete(TableToken tableToken) {
        deniedOnReplica();
    }
```

- [ ] **Step 4: Compile Enterprise; implement in any remaining reported implementor.**

Run: `mvn -q -pl questdb-ent -am compile`
Expected: BUILD SUCCESS (after covering all implementors).

- [ ] **Step 5: Commit.**

```bash
git add questdb-ent/src/main/java/com/questdb/security/
git commit -m "feat(delete): enforce authorizeTableDelete across Enterprise security contexts"
```

### Task 4.3: Enterprise DELETE permission test

**Files:**
- Test: `questdb-ent/src/test/java/com/questdb/acl/PGWireAclTest.java` (add a method) or a new ACL test class

- [ ] **Step 1: Write the test** — mirror `testRevokingColumnLevelUpdatePermissionFailsPreparedStatement`: create a user, GRANT `SELECT` + `INSERT`, seed data, attempt `DELETE FROM t WHERE ...` → expect `Access denied ... [DELETE on ...]`; then `GRANT DELETE ON t` and assert it now succeeds; `REVOKE DELETE` and assert it fails again. Use `assertGrant`/`assertRevoke` with a table-level permission model for DELETE.

- [ ] **Step 2: Run.**

Run: `cd /home/nick/claude/hub/questdb-enterprise && mvn -q -pl questdb-ent test -Dtest=PGWireAclTest#<newMethod>`
Expected: PASS.

- [ ] **Step 3: Commit.**

```bash
git add questdb-ent/src/test/java/com/questdb/acl/
git commit -m "test(delete): GRANT/REVOKE DELETE enforcement"
```

### Task 4.4: Replication tests — replica reject + primary→replica convergence

**Files:**
- Test: `questdb-ent/src/test/java/com/questdb/lifecycle/ReadOnlyWriterAcquireRefusalTest.java` (or a new test) for replica reject
- Test: a new test under `questdb-ent/src/test/java/com/questdb/cairo/wal/transfer/` extending `AbstractReplicationTest` for convergence

- [ ] **Step 1: Replica-reject test** — mirror `testWalWriterAcquireRefusedOnReplica`: on a REPLICA, issuing a client `DELETE` is refused (the WAL writer acquisition / authorization is read-only). Assert the appropriate read-only error.

- [ ] **Step 2: Convergence test** — mirror `ReplicationParquetAllNullDeltaTest.testConvertAllNullDeltaPageReplicates`: start primary + replica; on the primary create a WAL table, insert, run a `DELETE FROM t WHERE ...` (include a case that drops a whole partition and a case that rewrites one); `waitTableReplicated(primaryMain, replicaMain, token)`; then assert identical `SELECT count(), sum(...)` on **both** nodes via `QueryAssertion`.

```java
    @Test
    public void testDeleteReplicates() throws Exception {
        assertMemoryLeak(() -> {
            primaryMain = createPrimary(root);
            replicaMain = createEntServerMain(replicaRoot, false);
            try {
                primaryMain.start();
                replicaMain.start();
                final String t = getTestName();
                primaryMain.execute("CREATE TABLE " + t + " (ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                primaryMain.execute("INSERT INTO " + t + " SELECT (x*3600*1000000L)::timestamp, x FROM long_sequence(72)");
                primaryMain.execute("DELETE FROM " + t + " WHERE x % 2 = 0");
                final TableToken token = primaryMain.getEngine().verifyTableName(t);
                waitTableReplicated(primaryMain, replicaMain, token);
                assertEventually(() -> new QueryAssertion(primaryMain.getEngine(), primaryMain.getSqlExecutionContext(), null, "SELECT count(), sum(x) FROM " + t)
                        .noLeakCheck().noMemoryUsageCheck().inferRandomAccess().expectSize()
                        .returns("count\tsum\n36\t1332\n"));
                assertEventually(() -> new QueryAssertion(replicaMain.getEngine(), replicaMain.getSqlExecutionContext(), null, "SELECT count(), sum(x) FROM " + t)
                        .noLeakCheck().noMemoryUsageCheck().inferRandomAccess().expectSize()
                        .returns("count\tsum\n36\t1332\n"));
            } finally {
                replicaMain = Misc.free(replicaMain);
                primaryMain = Misc.free(primaryMain);
            }
        });
    }
```

(Adjust the expected `sum` to the surviving odd `x` in `1..72`.)

- [ ] **Step 3: Run.**

Run: `mvn -q -pl questdb-ent test -Dtest=ReadOnlyWriterAcquireRefusalTest,<newReplicationTest>`
Expected: PASS.

- [ ] **Step 4: Commit.**

```bash
git add questdb-ent/src/test/java/com/questdb/
git commit -m "test(delete): replica-reject + primary->replica convergence"
```

---

## Self-Review (completed by author)

**Spec coverage** (spec §→task):
- §4 SQL surface / WHERE mandatory / non-WAL / mat-view → 1.2, 1.5. `delete` unreserved → 1.2.
- §5–6 uniform deferred front-end → 1.1, 1.4, 1.5, 1.6, 1.7.
- §7 apply-time strategies → 1.10 (survivor), 2.1 (drop fast path), 3.1 (Parquet fallback).
- §8 `replaceRange` primitive + spike → 1.8, 1.9.
- §9 Parquet: v1 P2 fallback → 3.1; full-drop Parquet-safe → 2.1.
- §10 mat-view invalidation → 1.7 (dispatch) + 1.11 (test); dedup → 1.11.
- §11 Enterprise permission + authorize + replication-free → 4.1, 4.2, 4.3, 4.4.
- §12 concurrency/serial semantics → 1.11; affected count → 1.6 + 1.10 (returned count).
- §13 testing matrix → 1.11, 3.2, 3.3, 4.3, 4.4.
- §14.6 / §17.6 single-commit crash-safety → 3.2.
- §17.5 full-table emptying → 1.10 (`testDeleteEverythingEmptiesTable`).

**Placeholder scan:** the only deliberately open items are the Task 1.8 spike (a-vs-b, resolved by prototyping with a concrete acceptance test) and the "compiler-reports-the-list" instructions for enumerating `SecurityContext` implementors (1.3) and mat-view rejection reuse (1.5) — both are resolved by compiling and by reading the cited existing call sites, not hand-waved behavior.

**Type consistency:** `DeleteOperation` ctor and `getSurvivorFactory` (1.4) match usage in 1.5/1.7; `replaceRange(long, long, RecordCursor, RecordToRowCopier, int)` is defined in 1.8 and consumed in 1.10; `executeDelete(TableWriter, CharSequence, long)` stub (1.7) matches real (1.10) and the dispatch (1.7); `authorizeTableDelete(TableToken)` signature identical across 1.3/4.2; `Permission.DELETE` (4.1) consumed in 4.2/4.3; `CompiledQuery.DELETE` / `ExecutionModel.DELETE` / `CMD_DELETE_TABLE` consistent across 1.1/1.5/1.7.

**Open risks carried into execution:** the exact `QueryModel` wiring for `parseDelete` (1.5) and the `replaceRange` internals (1.8) require reading the cited verbatim sources during execution; both have concrete acceptance tests gating them.
