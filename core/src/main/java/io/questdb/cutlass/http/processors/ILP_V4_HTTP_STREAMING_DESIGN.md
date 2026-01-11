# ILP v4 HTTP Streaming Processing Design

## Overview

This document outlines the design for enabling streaming processing of ILP v4 HTTP requests, allowing table blocks to be processed before the complete HTTP payload is received.

## Current Architecture

### Request Flow (Non-Streaming)

```
HTTP POST /write/v4
    │
    ▼
┌─────────────────────────────────────┐
│  onHeadersReady()                   │
│  - Allocate native buffer           │
└─────────────────────────────────────┘
    │
    ▼ (repeated for each chunk)
┌─────────────────────────────────────┐
│  onChunk(lo, hi)                    │
│  - Copy to buffer                   │
│  - Grow buffer if needed            │
│  - NO PROCESSING                    │
└─────────────────────────────────────┘
    │
    ▼ (after Content-Length bytes received)
┌─────────────────────────────────────┐
│  onRequestComplete()                │
│  - Decode entire message            │
│  - Process ALL table blocks         │
│  - Append ALL to WAL                │
│  - Commit                           │
│  - Send response                    │
└─────────────────────────────────────┘
```

### Current Classes

| Class | Responsibility |
|-------|----------------|
| `IlpV4HttpProcessor` | HTTP request handler, delegates to state |
| `IlpV4HttpProcessorState` | Buffers entire payload, processes on completion |
| `IlpV4MessageDecoder` | Decodes complete message, returns `IlpV4DecodedMessage` |
| `IlpV4DecodedMessage` | Holds array of all decoded table blocks |

### Memory Characteristics

- **Peak memory**: O(total HTTP payload size)
- **Processing latency**: Must wait for complete payload
- **Large request vulnerability**: Single large request blocks processing

---

## Issues with Current Design

### Issue 1: Full Payload Buffering

**Problem**: The entire HTTP request body must be buffered before any processing begins.

```java
// IlpV4HttpProcessorState.java
public void addData(long lo, long hi) {
    int len = (int) (hi - lo);
    ensureCapacity(bufferPosition + len);  // May trigger realloc
    Unsafe.copyMemory(lo, bufferAddress + bufferPosition, len);
    bufferPosition += len;
    // No processing happens here
}
```

**Impact**:
- Memory usage scales with request size
- Large payloads cause memory pressure
- Latency to first WAL write equals full receive time

### Issue 2: Processing Blocked on I/O

**Problem**: WAL appending cannot begin until all bytes received.

```java
// IlpV4HttpProcessor.java
@Override
public void onRequestComplete(HttpConnectionContext context) {
    state.processMessage();  // Processing starts HERE
    state.commit();
}
```

**Impact**:
- I/O wait time + processing time are sequential, not overlapped
- Cannot utilize CPU while waiting for network

### Issue 3: Decoder Requires Complete Message

**Problem**: `IlpV4MessageDecoder.decode()` validates total message length upfront.

```java
// IlpV4MessageDecoder.java
public IlpV4DecodedMessage decode(long address, int length) {
    messageHeader.parse(address, length);
    long totalLength = messageHeader.getTotalLength();

    if (length < totalLength) {
        throw IlpV4ParseException.create(
            ErrorCode.INSUFFICIENT_DATA,
            "incomplete message"
        );
    }
    // ...
}
```

**Impact**:
- Cannot use existing decoder for incremental parsing
- Decoder API assumes random access to complete message

### Issue 4: Table Block Size Unknown

**Problem**: The wire format does not include table block sizes. Size must be computed by parsing.

**Table Block Structure**:
```
┌─────────────────────────────────────┐
│ Table Header                        │
│ ├─ name_length: varint              │
│ ├─ name: UTF-8 bytes                │
│ ├─ row_count: varint                │
│ └─ column_count: varint             │
├─────────────────────────────────────┤
│ Schema Section                      │
│ ├─ mode: 0x00 (full) or 0x01 (ref)  │
│ └─ columns[] or schema_hash         │
├─────────────────────────────────────┤
│ Column Data Section                 │
│ └─ column_data[0..N]                │
│     (size depends on type & rows)   │
└─────────────────────────────────────┘
```

**Impact**:
- Cannot skip to next table block without parsing current one
- Cannot pre-allocate exact buffer for table block

### Issue 5: Variable-Length Column Data

**Problem**: Several column types have data-dependent sizes.

| Column Type | Size Determination |
|-------------|-------------------|
| BOOLEAN | `ceil(rowCount / 8)` bytes (bitmap) |
| Fixed-width (INT, LONG, etc.) | `rowCount * typeSize` |
| STRING/VARCHAR | Offset array + variable string data |
| SYMBOL | Dictionary (variable) + indices (varint per row) |
| TIMESTAMP (Gorilla) | Bit-packed, size unknown until decoded |
| GEOHASH | `rowCount * ceil(precision / 8)` |

**Impact**:
- STRING, SYMBOL, and Gorilla TIMESTAMP sizes cannot be pre-calculated
- Must parse column data to determine size

### Issue 6: Gorilla Timestamp Size Uncertainty

**Problem**: Gorilla-encoded timestamps use variable-bit encoding. The compressed size depends on actual timestamp deltas.

```
Encoding bits per value:
- DoD = 0:           1 bit
- DoD in [-63,64]:   9 bits
- DoD in [-255,256]: 12 bits
- DoD in [-2047,2048]: 16 bits
- Otherwise:         36 bits
```

**Impact**:
- Cannot calculate Gorilla data size without decoding
- Would need to decode twice (size calculation + value extraction)

### Issue 7: Error Handling Mid-Stream

**Problem**: If streaming commits table blocks incrementally, an error on table block N leaves blocks 0..N-1 committed.

**Current behavior**: All-or-nothing (atomic across all tables)
**Streaming behavior**: Partial commit risk

**Impact**:
- Semantic change in failure behavior
- May need transaction rollback capability

---

## Suggested Design

### Design Goals

1. **Reduce peak memory**: Buffer at most one table block, not entire payload
2. **Overlap I/O and processing**: Start WAL writes while receiving data
3. **Maintain atomicity**: Commit all or nothing on request completion
4. **Backward compatible**: No wire format changes required (Phase 1)

### High-Level Architecture

```
HTTP POST /write/v4
    │
    ▼
┌─────────────────────────────────────┐
│  onHeadersReady()                   │
│  - Initialize streaming state       │
│  - State = WAITING_MESSAGE_HEADER   │
└─────────────────────────────────────┘
    │
    ▼ (repeated for each chunk)
┌─────────────────────────────────────┐
│  onChunk(lo, hi)                    │
│  - Feed bytes to state machine      │
│  - Parse message header (12 bytes)  │
│  - For each table block:            │
│    - Buffer until complete          │
│    - Process immediately            │
│    - Append to WAL (uncommitted)    │
│    - Reset block buffer             │
└─────────────────────────────────────┘
    │
    ▼ (after Content-Length bytes received)
┌─────────────────────────────────────┐
│  onRequestComplete()                │
│  - Verify all tables processed      │
│  - Commit WAL transaction           │
│  - Send response                    │
└─────────────────────────────────────┘
```

### State Machine

```
                    ┌──────────────────────┐
                    │  WAITING_MSG_HEADER  │
                    │  (need 12 bytes)     │
                    └──────────┬───────────┘
                               │ header complete
                               ▼
                    ┌──────────────────────┐
          ┌────────│  WAITING_TABLE_HDR   │
          │        │  (variable size)     │
          │        └──────────┬───────────┘
          │                   │ header complete, size known
          │                   ▼
          │        ┌──────────────────────┐
          │        │  BUFFERING_TABLE     │
          │        │  (accumulate bytes)  │
          │        └──────────┬───────────┘
          │                   │ table complete
          │                   ▼
          │        ┌──────────────────────┐
          │        │  PROCESS_TABLE       │
          │        │  - decode            │
          │        │  - append to WAL     │
          │        └──────────┬───────────┘
          │                   │
          │     more tables?  │
          └───────────────────┘
                               │ all tables done
                               ▼
                    ┌──────────────────────┐
                    │  DONE                │
                    └──────────────────────┘
```

### New Class: `IlpV4HttpStreamingState`

```java
public class IlpV4HttpStreamingState {

    // Parse state machine
    enum State {
        WAITING_MESSAGE_HEADER,
        WAITING_TABLE_HEADER,
        BUFFERING_TABLE_DATA,
        PROCESSING_TABLE,
        DONE,
        ERROR
    }

    // Message header (parsed once)
    private final IlpV4MessageHeader messageHeader;
    private boolean messageHeaderParsed;
    private int tableCount;
    private int tablesProcessed;
    private boolean gorillaEnabled;

    // Table block buffer (reused for each table)
    private long blockBuffer;
    private int blockBufferCapacity;
    private int blockBufferPosition;
    private int blockBytesNeeded;      // -1 if unknown

    // Table header parsing state
    private final IlpV4TableHeader tableHeader;
    private boolean tableHeaderParsed;
    private IlpV4Schema currentSchema;

    // Streaming cursor (reused)
    private final IlpV4TableBlockCursor tableBlockCursor;
    private final IlpV4SchemaCache schemaCache;

    // WAL appending
    private final IlpV4WalAppender walAppender;
    private final IlpV4HttpTudCache tudCache;

    // Current state
    private State state;
    private Status status;
    private final StringSink errorMessage;
}
```

### Key Methods

#### `addData(long lo, long hi)`

```java
public void addData(long lo, long hi) {
    long pos = lo;

    while (pos < hi && state != State.ERROR && state != State.DONE) {
        switch (state) {
            case WAITING_MESSAGE_HEADER:
                pos = consumeMessageHeader(pos, hi);
                break;

            case WAITING_TABLE_HEADER:
                pos = consumeTableHeader(pos, hi);
                break;

            case BUFFERING_TABLE_DATA:
                pos = consumeTableData(pos, hi);
                if (blockBufferPosition >= blockBytesNeeded) {
                    processCurrentTable();
                }
                break;
        }
    }
}
```

#### `consumeMessageHeader()`

```java
private long consumeMessageHeader(long pos, long hi) {
    int needed = HEADER_SIZE - blockBufferPosition;
    int available = (int)(hi - pos);
    int toCopy = Math.min(needed, available);

    Unsafe.copyMemory(pos, blockBuffer + blockBufferPosition, toCopy);
    blockBufferPosition += toCopy;

    if (blockBufferPosition >= HEADER_SIZE) {
        messageHeader.parse(blockBuffer, HEADER_SIZE);
        tableCount = messageHeader.getTableCount();
        gorillaEnabled = messageHeader.isGorillaEnabled();
        messageHeaderParsed = true;

        // Reset for first table
        blockBufferPosition = 0;
        state = State.WAITING_TABLE_HEADER;
    }

    return pos + toCopy;
}
```

#### `consumeTableHeader()`

Parse enough bytes to determine table block size.

```java
private long consumeTableHeader(long pos, long hi) {
    // Buffer bytes until we can parse table header + schema
    int available = (int)(hi - pos);
    int toCopy = Math.min(available, blockBufferCapacity - blockBufferPosition);

    Unsafe.copyMemory(pos, blockBuffer + blockBufferPosition, toCopy);
    blockBufferPosition += toCopy;

    // Try to parse table header
    if (tryParseTableHeader()) {
        // Calculate total bytes needed for this table block
        blockBytesNeeded = calculateTableBlockSize();
        state = State.BUFFERING_TABLE_DATA;
    }

    return pos + toCopy;
}
```

#### `calculateTableBlockSize()`

```java
private int calculateTableBlockSize() {
    int size = tableHeader.getBytesConsumed();

    // Add schema section size
    size += currentSchema.encodedSize();  // Or 9 for reference mode

    // Add column data sizes
    IlpV4ColumnDef[] columns = currentSchema.getColumns();
    int rowCount = (int) tableHeader.getRowCount();

    for (IlpV4ColumnDef col : columns) {
        size += calculateColumnDataSize(col, rowCount);
    }

    return size;
}
```

#### `calculateColumnDataSize()`

**This is the challenging part** - see Issues section.

```java
private int calculateColumnDataSize(IlpV4ColumnDef col, int rowCount) {
    boolean nullable = col.isNullable();
    int nullBitmapSize = nullable ? ((rowCount + 7) / 8) : 0;

    int type = col.getTypeCode() & TYPE_MASK;

    switch (type) {
        case TYPE_BOOLEAN:
            return nullBitmapSize + ((rowCount + 7) / 8);

        case TYPE_BYTE:
            return nullBitmapSize + rowCount;

        case TYPE_SHORT:
            return nullBitmapSize + rowCount * 2;

        case TYPE_INT:
        case TYPE_FLOAT:
            return nullBitmapSize + rowCount * 4;

        case TYPE_LONG:
        case TYPE_DOUBLE:
        case TYPE_DATE:
            return nullBitmapSize + rowCount * 8;

        case TYPE_UUID:
            return nullBitmapSize + rowCount * 16;

        case TYPE_LONG256:
            return nullBitmapSize + rowCount * 32;

        case TYPE_STRING:
        case TYPE_VARCHAR:
        case TYPE_SYMBOL:
        case TYPE_TIMESTAMP:  // with Gorilla
        case TYPE_GEOHASH:
            // CANNOT PRE-CALCULATE - see Variable Size Strategy
            return -1;

        default:
            throw new IllegalArgumentException("Unknown type: " + type);
    }
}
```

### Variable-Size Column Strategy

For columns where size cannot be pre-calculated:

#### Option A: Incremental Parsing

Parse column data byte-by-byte, tracking state:

```java
class ColumnParseState {
    int phase;           // 0=nullBitmap, 1=offsets, 2=data, etc.
    int bytesConsumed;
    int bytesNeeded;     // For current phase, -1 if variable
}
```

**Pros**: True streaming, minimal buffering
**Cons**: Complex state machine per column type

#### Option B: Conservative Estimation

Estimate maximum possible size:

```java
private int estimateColumnDataSize(IlpV4ColumnDef col, int rowCount) {
    // STRING: assume max avg string length
    // SYMBOL: assume max dictionary size
    // TIMESTAMP: assume worst-case Gorilla (36 bits per value)
    return pessimisticEstimate;
}
```

**Pros**: Simple implementation
**Cons**: May over-buffer significantly

#### Option C: Buffer Until Column Boundary

For variable-size columns, buffer entire column data section:

```java
private long consumeColumnData(long pos, long hi, int colIndex) {
    if (isVariableSizeColumn(colIndex)) {
        // Buffer all remaining table data
        return bufferRemaining(pos, hi);
    }
    // Fixed-size: calculate exact bytes
    return bufferExact(pos, hi, knownSize);
}
```

**Pros**: Moderate complexity
**Cons**: Still buffers variable columns fully

#### Option D: Wire Format Enhancement (Phase 2)

Add table block size to wire format:

```
TABLE BLOCK (enhanced):
├─ block_size: uint32        ← NEW
├─ table_header
├─ schema_section
└─ column_data
```

**Pros**: Trivial to implement streaming
**Cons**: Wire format change, backward compatibility

### Recommended Approach

**Phase 1**: Implement Option C (buffer at column boundary)
- Stream fixed-size columns
- Buffer entire table for variable-size columns
- No wire format changes

**Phase 2**: Implement Option D (wire format enhancement)
- Add block_size field
- True streaming for all column types
- Require client upgrade

---

## Memory Comparison

| Scenario | Current | Streaming (Phase 1) |
|----------|---------|---------------------|
| 10 tables, 1MB each | 10 MB | ~1 MB (largest table) |
| 1 table, 10MB | 10 MB | 10 MB (no improvement) |
| 100 tables, 100KB each | 10 MB | ~100 KB |

**Best case improvement**: O(N) → O(1) where N = number of tables

---

## Transaction Semantics

### Current Behavior
- All table blocks decoded
- All appended to WAL
- Single commit at end
- On error: nothing committed

### Streaming Behavior
- Table blocks processed incrementally
- Each appended to WAL (uncommitted)
- Single commit at `onRequestComplete()`
- On error: WAL transaction rolled back

**Key**: WAL transaction must span entire request, commit only on success.

```java
// onRequestComplete()
public void onRequestComplete() {
    if (tablesProcessed != tableCount) {
        rollbackWalTransaction();
        reject(Status.PARSE_ERROR, "incomplete message");
        return;
    }

    if (status == Status.OK) {
        commitWalTransaction();
    } else {
        rollbackWalTransaction();
    }
}
```

---

## Implementation Phases

### Phase 1: Table-Level Streaming

**Scope**:
- Parse message header immediately
- Buffer one table block at a time
- Process and append each table before buffering next
- Commit all at request completion

**Changes**:
1. New `IlpV4HttpStreamingState` class
2. Modify `IlpV4HttpProcessor.onChunk()`
3. Add table block size calculation
4. Integrate with existing `IlpV4TableBlockCursor`

**Limitations**:
- Variable-size columns still fully buffered
- No benefit for single-table requests

### Phase 2: Wire Format Enhancement

**Scope**:
- Add `block_size` field to table block header
- True streaming for all column types
- Row-level processing possible

**Changes**:
1. Update wire format specification
2. Update `IlpV4TableBlockEncoder` (sender)
3. Update streaming state to use block_size
4. Version negotiation for backward compatibility

### Phase 3: Row-Level Streaming (Future)

**Scope**:
- Process rows as they arrive
- Sub-table-block granularity
- Minimal memory footprint

**Changes**:
1. Incremental column data parsing
2. Row-by-row WAL appending
3. Significant complexity increase

---

## Open Questions

1. **Timeout handling**: What happens if client stalls mid-table-block?
   - Current: Connection timeout applies to entire request
   - Streaming: May need per-table-block timeout

2. **Progress reporting**: Should we report partial progress on long requests?
   - Could add response header with tables-processed count

3. **Backpressure**: If WAL append is slow, should we slow chunk consumption?
   - Current: Not applicable (batch processing)
   - Streaming: May need flow control

4. **Memory limits**: How to handle table blocks larger than max buffer?
   - Option: Reject request if any table block exceeds limit
   - Option: Fall back to non-streaming for large blocks

---

## References

- `IlpV4HttpProcessor.java` - Current HTTP handler
- `IlpV4HttpProcessorState.java` - Current state management
- `IlpV4MessageDecoder.java` - Message decoding
- `IlpV4TableBlockCursor.java` - Streaming cursor (already implemented)
- `IlpV4StreamingDecoder.java` - Streaming decoder (already implemented)
