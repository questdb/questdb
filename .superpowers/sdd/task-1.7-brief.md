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

