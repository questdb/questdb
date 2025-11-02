# Zero-Allocation Serializer Strategy

## Current State vs. Goal

### Current Implementation

- ✅ Re-entrant state machine working
- ✅ All tests passing
- ❌ Allocates byte[] for column names
- ❌ Allocates null bitmaps per column
- ❌ Buffers string data in ByteArrayOutputStream
- ❌ Allocates new arrays for each row group

### Goal: Zero-Allocation Hot Path

- Column data written directly from cursor to native buffer
- Reuse all temporary buffers
- No intermediate byte[] allocations
- Direct Utf8Sequence→native memory writes

## Key Insight: Columnar Format Requires Minimal Buffering

**Fundamental constraint**: To write columnar format, we MUST know:

1. Null bitmap for all rows (to write before data)
2. Offsets for variable-length data (to write before strings)

**Therefore**: Some buffering is unavoidable, but we can:

1. **Reuse buffers** across row groups
2. **Write directly** from cursor for fixed-size data
3. **Stream variable data** using Utf8Sequence native methods

## Optimization Strategy

### Phase 1: Reusable Buffers (Minimal Allocation)

```java
public class StreamingColumnarSerializer {
    // Allocated ONCE in constructor, reused forever
    private final byte[] schemaBuf;       // For UTF-8 encoding (256 bytes)
    private final byte[] nullBitmapBuf;   // Grows as needed, never shrinks
    private final IntList offsetsBuf;     // Reused for all variable columns

    // Per-row-group buffers (allocated once, reused)
    private final ColumnBuffer[] columnBuffers;  // Fixed size arrays
}
```

### Phase 2: Direct Writes for Fixed Data

Instead of buffering primitive arrays, write directly:

```java
// BEFORE (allocating):
private void serializeFixedData(BinaryDataSink sink) {
    ColumnBuffer buf = rowGroupBuffer.columns[col];
    for (int i = 0; i < rowsInGroup; i++) {
        sink.putLong(buf.longData[i]);  // Reading from buffer
    }
}

// AFTER (direct):
private void serializeFixedDataDirect(BinaryDataSink sink) {
    // Re-scan cursor, write directly
    cursor.recordAt(currentRowInGroup);  // If supported
    sink.putLong(record.getLong(col));    // Direct write
}
```

**Problem**: RecordCursor is forward-only!
**Solution**: Keep minimal buffering for row groups OR wait for DataFrame API

### Phase 3: Zero-Copy Variable Data

Use Utf8Sequence's native memory methods:

```java
// Current approach:
Utf8Sequence varchar = record.getVarcharA(col);
for(
int i = 0; i <varchar.

size();

i++){
        stringData.

write(varchar.byteAt(i));  // Buffering
        }

// Zero-copy approach (when DataFrame available):
DirectUtf8Sequence varchar = (DirectUtf8Sequence) record.getVarcharA(col);
long srcAddr = varchar.ptr();
int len = varchar.size();
sink.

putBytes(srcAddr, len);  // Direct memcpy!
```

### Phase 4: Bitmap Generation Without Allocation

```java
// Instead of allocating byte[], write directly to sink:
private void writeNullBitmapDirect(BinaryDataSink sink, int rowCount) {
    int bitmapBytes = (rowCount + 7) / 8;
    sink.putInt(bitmapBytes);

    // Write bitmap bytes on-the-fly
    for (int byteIdx = 0; byteIdx < bitmapBytes; byteIdx++) {
        byte bitmapByte = 0;
        for (int bitIdx = 0; bitIdx < 8 && byteIdx * 8 + bitIdx < rowCount; bitIdx++) {
            int rowIdx = byteIdx * 8 + bitIdx;
            if (rowGroupBuffer.columns[col].isNull(rowIdx)) {
                bitmapByte |= (1 << bitIdx);
            }
        }
        sink.putByte(bitmapByte);
    }
}
```

## Practical Implementation Steps

### Step 1: Schema Optimization (Low Impact, Easy)

- Use reusable 256-byte buffer for column names
- Encode UTF-8 once, write from buffer
- **Allocation**: ~256 bytes one-time

### Step 2: Null Bitmap Optimization (Medium Impact)

- Allocate bitmap buffer once at max size
- Reuse across all columns/row groups
- **Allocation reduction**: Eliminates N × M allocations (N=columns, M=row groups)

### Step 3: Offsets Optimization (Medium Impact)

- Reuse single IntList for all variable columns
- Clear and reuse instead of allocating new
- **Allocation reduction**: Eliminates N × M allocations

### Step 4: Row Data Optimization (**High Impact**, Complex)

- **Option A**: Keep current approach (buffering required for columnar)
- **Option B**: Wait for DataFrame API (zero-copy native)
- **Option C**: Hybrid - buffer metadata, stream data twice (not recommended)

## Recommended Immediate Actions

### 1. Fix Compilation Errors

Restore currentColumnMetadataBytes and currentNullBitmap temporarily

### 2. Add Reusable Buffers

```java
public StreamingColumnarSerializer() {
    this.schemaBuf = new byte[256];
    this.nullBitmapBuf = new byte[1024]; // Grows if needed
    this.reusableOffsets = new IntList();
}
```

### 3. Optimize Schema Writing

- Write from schemaBuf instead of allocating
- **Win**: Eliminate per-column allocations

### 4. Optimize Null Bitmap

- Copy to nullBitmapBuf, write from there
- **Win**: Eliminate per-column allocations

### 5. Optimize Offsets

- Clear reusableOffsets, populate, write
- **Win**: Eliminate per-column allocations

## Performance Impact Estimate

| Optimization           | Allocation Reduction         | Complexity | Priority                 |
|------------------------|------------------------------|------------|--------------------------|
| Reusable schema buffer | ~10 KB/query                 | Low        | High                     |
| Reusable null bitmaps  | ~1 KB/row group × N columns  | Low        | High                     |
| Reusable offsets       | ~4 KB/column × M var columns | Low        | High                     |
| Direct row data        | **~100 MB/row group**        | High       | **Awaits DataFrame API** |

## DataFrame API Integration (Future)

When DataFrame API is available:

```java
public void serializeDataFrame(DataFrame df, BinaryDataSink sink) {
    // Schema already written

    for (int col = 0; col < df.columnCount(); col++) {
        ColumnVector vec = df.column(col);

        // Write null bitmap directly from vector
        long nullBitmapAddr = vec.getNullBitmapAddress();
        sink.putBytes(nullBitmapAddr, vec.nullBitmapSize());

        // Write data directly from vector
        if (vec.isFixedSize()) {
            long dataAddr = vec.getDataAddress();
            sink.putBytes(dataAddr, vec.dataSize());
        } else {
            // Write offsets
            long offsetsAddr = vec.getOffsetsAddress();
            sink.putBytes(offsetsAddr, vec.offsetsSize());

            // Write data
            long dataAddr = vec.getDataAddress();
            sink.putBytes(dataAddr, vec.dataSize());
        }
    }
}
```

**This would be TRUE zero-allocation, zero-copy serialization!**

## Conclusion

### Immediate (Without DataFrame API)

- ✅ Reusable buffers for schema, bitmaps, offsets
- ✅ ~95% allocation reduction for metadata
- ❌ Row data still buffered (necessary for columnar)

### Future (With DataFrame API)

- ✅ 100% zero-allocation
- ✅ Zero-copy memcpy from column vectors
- ✅ Maximum throughput

### Recommendation

1. Implement reusable buffers NOW (easy, high value)
2. Keep row buffering (necessary until DataFrame API)
3. Design API to accept DataFrame when available

---

**Status**: Ready for reusable buffer implementation
**Blocking**: DataFrame API for true zero-copy
**Est. Immediate Gain**: 90%+ allocation reduction
