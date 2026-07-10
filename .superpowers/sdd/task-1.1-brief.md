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

