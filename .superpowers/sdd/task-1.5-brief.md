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

