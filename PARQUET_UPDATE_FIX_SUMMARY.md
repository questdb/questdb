# Fix for UPDATE on Parquet Partitions Bug

## Problem
When executing an UPDATE statement on a WAL table that contains Parquet partitions (created via `ALTER TABLE CONVERT PARTITION TO PARQUET`), the table would become suspended. This happened because the code threw a critical exception when encountering a read-only (Parquet) partition, which caused the ApplyWal2TableJob to suspend the entire table.

## Reproduction Steps
```sql
CREATE TABLE 'test' (
  id SYMBOL CAPACITY 10 NOCACHE,
  mySymbol SYMBOL,
  dateTime TIMESTAMP
) timestamp(dateTime) PARTITION BY DAY WAL;

INSERT INTO test VALUES ('1', NULL, '2025-10-01T07:20:06.948000Z');
INSERT INTO test VALUES ('2', NULL, '2025-10-02T07:20:06.948000Z');

ALTER TABLE test CONVERT PARTITION TO PARQUET WHERE dateTime <= '2025-10-01';

UPDATE test SET mySymbol='TEST' WHERE mySymbol IS NULL;
```

**Result Before Fix:** Table becomes suspended due to critical exception.

## Root Cause
In `UpdateOperatorImpl.java`, when the UPDATE operation encountered a read-only partition (Parquet), it threw a `CairoException.critical()` which was caught by the `ApplyWal2TableJob` error handler. Critical exceptions cause the table to be suspended for safety reasons.

The problematic code was:
```java
if (rowPartitionIndex != partitionIndex) {
    if (tableWriter.isPartitionReadOnly(rowPartitionIndex)) {
        throw CairoException.critical(0)
                .put("cannot update read-only partition [table=")...
    }
    ...
}
```

## Solution
Instead of throwing a critical exception that suspends the table, the fix makes UPDATE operations skip rows in read-only (Parquet) partitions gracefully. This allows:
1. Rows in writable (native) partitions to be updated successfully
2. The table to remain operational (not suspended)
3. Consistent behavior with the immutable nature of Parquet partitions

## Changes Made

### 1. UpdateOperatorImpl.java
**File:** `core/src/main/java/io/questdb/griffin/UpdateOperatorImpl.java`

**Change:** Modified the partition read-only check to skip rows instead of throwing an exception.

**Before:**
```java
final int rowPartitionIndex = Rows.toPartitionIndex(rowId);
final long currentRow = Rows.toLocalRowID(rowId);

if (rowPartitionIndex != partitionIndex) {
    if (tableWriter.isPartitionReadOnly(rowPartitionIndex)) {
        throw CairoException.critical(0)
                .put("cannot update read-only partition [table=")...
                .put(']');
    }
    ...
}
```

**After:**
```java
final int rowPartitionIndex = Rows.toPartitionIndex(rowId);
final long currentRow = Rows.toLocalRowID(rowId);

// Skip rows in read-only partitions (e.g., Parquet partitions)
if (tableWriter.isPartitionReadOnly(rowPartitionIndex)) {
    LOG.info()
            .$("skipping row in read-only partition [table=")...
            .I$();
    continue;
}

if (rowPartitionIndex != partitionIndex) {
    ...
}
```

**Key points:**
- Check is moved BEFORE the partition switch logic
- Uses `continue` to skip the row instead of throwing an exception
- Logs at INFO level for visibility
- Allows UPDATE to process remaining rows in writable partitions

### 2. UpdateTest.java (Test Added)
**File:** `core/src/test/java/io/questdb/test/griffin/UpdateTest.java`

**Added test:** `testUpdateSkipsParquetPartitions()`

This test verifies:
1. Parquet partition is correctly skipped during UPDATE
2. Native partition is successfully updated
3. Table remains operational (not suspended)
4. Correct number of rows are updated (only those in writable partitions)

## Behavior After Fix

**Result After Fix:** 
- Row in Parquet partition (2025-10-01) is **skipped** (remains NULL)
- Row in native partition (2025-10-02) is **updated** successfully
- Table remains **operational** and not suspended
- UPDATE returns affected row count = 1 (only the writable partition)

```
id | mySymbol | dateTime
---|----------|-------------------------
1  | NULL     | 2025-10-01T07:20:06.948Z
2  | TEST     | 2025-10-02T07:20:06.948Z
```

## Impact
- **Minimal code change:** Only modified the error handling in one location
- **Safe behavior:** Parquet partitions remain immutable as designed
- **Improved resilience:** Tables don't get suspended for attempting to update read-only partitions
- **Better UX:** Users can run UPDATE statements without worrying about table suspension

## Testing
Run the test:
```bash
mvn test -Dtest=UpdateTest#testUpdateSkipsParquetPartitions -pl core
```

The test covers the exact reproduction scenario and verifies the fix.

