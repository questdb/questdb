---
name: debug-fuzz
description: Debug a failing WAL fuzz test involving column conversions and parquet partitions
argument-hint: [test name, seeds, or failure log]
allowed-tools: Bash(mvn *), Read, Grep, Glob, Agent
---

Debug the fuzz test failure described by `$ARGUMENTS`.

## Background

WAL fuzz tests (`WalWriterFuzzTest`, `AbstractFuzzTest`) are nearly deterministic with fixed
seeds. The same seeds produce the same sequence of operations. Failures reproduce reliably
once you have the seeds.

The test applies the same randomly-generated transactions three ways:
1. **Non-WAL table** — direct writer, ground truth
2. **WAL sequential** — single WAL writer
3. **WAL parallel** ple concurrent WAL writers

`TestUtils.assertSqlCursors()` compares WAL tables against non-WAL. The assertion format is:
`Row N column COL[TYPE] expected:<X> but was:<Y>`.

## Step 1: Get the seeds

Find the random seeds in the test output:
```
random seeds: 137113830825708L, 1776424558803L
```

If the user provided a failure log, extract the seeds from there and compare them against
the seeds currently hardcoded in the test method. If the test uses `generateRandom(LOG)`
(no fixed seeds), note this — you'll need to hardcode the failing seeds to reproduce later.

If the seeds are already hardcoded in the test and match the failure log, no reproduction
step is needed — proceed directly to tracing.

## Step 3: Trace the column lifecycle

This is the most important step for conversion-related failures. For each column involved
in the assertion failure, build a chronological timeline of:

1. **Column add** — when the column was added, which partitions already existed.
   Partitions created before the column was added have a **column top** (the column starts
   at a row offset within that partition, not at row 0).

2. **Column delete / rename** — removals and renames that affect the column index chain.

3. **Column type conversions** (`ALTER COLUMN TYPE`) — each conversion creates a new column
   index via `replacingIndex`. Track the full chain of types (e.g., INT → STRING → DATE).
   Key file: `ConvertOperatorImpl.java`.

4. **Partition format conversions** — `CONVERT PARTITION TO PARQUET` and `TO NATIVE`.
   A parquet partition freezes the column type at conversion time. If a type change happened
   after parquet conversion, the parquet partition stores the **old** type. The
   `ConvertOperatorImpl` pre-pass converts parquet partitions to native when needed
   (chained conversions, symbol targets).

5. **Partition truncations** (`DROP PARTITION`) — partitions removed mid-sequence change
   what data exists when later operations run.

Use `Grep` to search the fuzz generator and operation classes for LOG statements that
trace these operations:

```
core/src/test/java/io/questdb/test/cairo/fuzz/FuzzChangeColumnTypeOperation.java
core/src/test/java/io/questdb/test/cairo/fuzz/FuzzConvertPartitionToParquetOperation.java
core/src/test/java/io/questdb/test/cairo/fuzz/FuzzConvertPartitionToNativeOperation.java
core/src/test/java/io/questdb/test/cairo/fuzz/FuzzTransactionGenerator.java
core/src/main/java/io/questdb/griffin/ConvertOperatorImpl.java
```

## Step 4: Cross-reference with the failure

Map the failing assertion back to the timeline:
- Which partition holds the failing row (based on its timestamp)?
- Was that partition in parquet format at any point?
- What type did the column have when the partition was converted to/from parquet?
- Does the column have a column top in that partition (was it added after the partition)?

## Step 5: Add debug logging

**Do NOT change fuzz counts, probabilities, or feature gates.** Fuzz tests are extremely
sensitive to any modification — even a tiny change alters the random sequence and produces
completely different transactions, making the failure non-reproducible.

Instead, add temporary debug logging to dump table state after each transaction. Useful
things to log after each WAL apply / transaction commit:

- **Row count** per partition (native and parquet)
- **Column tops** for the failing column (and its `replacingIndex` chain) across all partitions
- **Partition list** with format (native/parquet) and timestamp ranges
- **Column metadata**: type, index in metadata, `replacingIndex` chain
- **Partition format** changes (log when a partition switches between native and parquet)

Add logging in `AbstractFuzzTest.runFuzz()` or in the WAL apply loop. For example, after
each transaction is applied, iterate over partitions and dump the state of the columns
involved in the failure.

Also useful:
```java
node1.setProperty(PropertyKey.DEBUG_WAL_APPLY_BLOCK_FAILURE_NO_RETRY, true);
```

## Step 6: Identify root cause

Common failure patterns:

| Symptom | Likely cause | Where to look |
|---------|-------------|---------------|
| Data mismatch after parquet round-trip | Parquet decoder type mapping | `core/rust/qdbr/src/parquet_read/decode.rs` |
| Wrong value in converted column | Pre-pass missed a parquet partition | `ConvertOperatorImpl.java` pre-pass logic |
| Symbol column errors from parquet | Parquet not converted to native before symbol conversion | `ConvertOperatorImpl.java` `isTargetSymbol` check |
| NULL vs sentinel mismatch | Type conversion doesn't handle NULL for non-nullable types | Conversion logic for BYTE/SHORT/BOOLEAN |
| Column top mismatch | Column top not propagated to new column index | `columnVersionWriter.upsertColumnTop()` in `ConvertOperatorImpl` |

## Key files

- `core/src/test/java/io/questdb/test/cairo/fuzz/WalWriterFuzzTest.java` — test entry points
- `core/src/test/java/io/questdb/test/cairo/fuzz/AbstractFuzzTest.java` — `runFuzz()`, comparison logic
- `core/src/test/java/io/questdb/test/cairo/fuzz/FuzzTransactionGenerator.java` — operation generation
- `core/src/test/java/io/questdb/test/cairo/fuzz/FuzzChangeColumnTypeOperation.java` — type change logic
- `core/src/main/java/io/questdb/griffin/ConvertOperatorImpl.java` — column type conversion engine
- `core/src/main/java/io/questdb/cairo/TableWriter.java` — partition format, column tops
- `core/rust/qdbr/src/parquet_read/decode.rs` — parquet type decoding

## Running

```bash
# Single method from repo root
mvn test --batch-mode \
  -Dtest.include="%regex[.*WalWriterFuzzTest.*]" \
  -Dtest="**/WalWriterFuzzTest.java#testConvertPartitionToParquet" \
  -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false \
  -pl questdb/core
```
