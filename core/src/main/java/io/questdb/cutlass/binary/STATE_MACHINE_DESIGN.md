# Streaming Columnar Serializer - State Machine Design

## Overview

The `StreamingColumnarSerializer` is a **fully re-entrant state machine** designed for serializing SQL query results
over non-blocking network connections. It can be interrupted at any point when the output buffer fills up and seamlessly
resumed later from the exact same position.

## Key Design Requirements

1. **Re-entrancy**: Can be called multiple times, picking up exactly where it left off
2. **Non-blocking**: Never blocks waiting for buffer space
3. **Zero Data Loss**: All state preserved across interruptions
4. **Memory Bounded**: Fixed memory usage regardless of result set size
5. **Network Optimized**: Designed for `send()` calls that may throw when client is slow
6. **Columnar Streaming**: Efficiently handles large datasets via row groups

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│          StreamingColumnarSerializer                        │
│          (State Machine Core)                               │
│                                                             │
│  ┌──────────────┐   ┌───────────────┐   ┌──────────────┐  │
│  │   Current    │   │ Progress      │   │  Row Group   │  │
│  │   State      │   │ Counters      │   │  Buffer      │  │
│  │   Enum       │   │               │   │              │  │
│  └──────────────┘   └───────────────┘   └──────────────┘  │
│                                                             │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
          ┌────────────────────────┐
          │   BinaryDataSink       │
          │   (Buffer Abstraction) │
          └────────────────────────┘
                       │
                       ▼
          ┌────────────────────────┐
          │   Native Memory Buffer │
          │   (via Unsafe)         │
          └────────────────────────┘
                       │
                       ▼
             Network Send (non-blocking)
```

## State Machine States

The serializer progresses through these states sequentially:

```
UNINITIALIZED
    │
    ▼
HEADER ────────────────────────────────┐
    │                                   │
    ▼                                   │ Write magic,
SCHEMA_TYPES                           │ version,
    │                                   │ column count
    │ (all column types + metadata)    │
    ▼                                   │
SCHEMA_NAMES                           │
    │                                   │
    │ (all column names, optional)     │
    ▼                                   │
ROW_GROUP_START ◄──────────────┐      │
    │                          │      │
    ▼                          │      │
ROW_GROUP_HEADER               │      │
    │                          │      │
    ▼                          │      │
COLUMN_NULL_BITMAP_LENGTH ◄────┼──┐   │
    │                          │  │   │
    ▼                          │  │   │
COLUMN_NULL_BITMAP_DATA        │  │   │
    │                          │  │   │
    ▼                          │  │   │
COLUMN_OFFSETS_LENGTH*         │  │   │
    │                          │  │   │
    ▼                          │  │   │
COLUMN_OFFSETS_DATA*           │  │   │
    │                          │  │   │
    ▼                          │  │   │
COLUMN_DATA_LENGTH             │  │   │
    │                          │  │   │
    ├──FIXED──► COLUMN_DATA_FIXED     │
    │                          │  │   │
    └──VARIABLE─► COLUMN_DATA_VARIABLE│
                   │           │  │   │
                   └───────────┘  │   │
                   (next column)  │   │
                                  │   │
    ┌─────────────────────────────┘   │
    │ (all columns done)               │
    ▼                                   │
ROW_GROUP_START ─────────────────────┘
    │ (no more rows)
    ▼
END_MARKER
    │
    ▼
COMPLETE

* Only for variable-length types (STRING, VARCHAR, BINARY)
```

## State Descriptions

### 1. HEADER

**Purpose**: Write file header
**Writes**:

- Magic number `"SCBF"` (4 bytes)
- Version `1` (2 bytes)
- Column count (4 bytes)

**State Variables**:

- `stateProgress`: Tracks byte position within header (0-6)

**Re-entry**: Can resume from any byte position

**Transitions**: → SCHEMA_TYPES

### 2. SCHEMA_TYPES

**Purpose**: Write all column types and type-specific metadata in sequence
**Writes**: For each column (0 to N-1):

- Column type as int (4 bytes)
- Metadata length as int (4 bytes)
- Metadata bytes (0-5 bytes depending on type)

**State Variables**:

- `currentColumn`: Which column (0 to N-1)
- `stateProgress`: Position within current column's type/metadata (0-1)

**Re-entry**: Can resume from any column

**Design Notes**:

- All type information written together before any names
- Enables readers to parse data without reading column names
- Type-specific metadata examples:
    - TIMESTAMP: precision byte (0=micros, 1=nanos)
    - GEOHASH: precision bits (1-60)
    - Others: empty metadata

**Transitions**: When all types written → SCHEMA_NAMES

### 3. SCHEMA_NAMES

**Purpose**: Write all column names using efficient UTF-8 encoding
**Writes**: For each column (0 to N-1):

- Name length as int (4 bytes)
- UTF-8 encoded name bytes (variable)

**State Variables**:

- `currentColumn`: Which column (0 to N-1)
- `utf8Adapter`: Reusable BinaryDataSinkUtf8Adapter instance

**Re-entry**: Uses bookmark/rollback for atomic column name writes

**Implementation Details**:

- Uses `Utf8s.encodeUtf16WithLimit()` for efficient encoding
- Assumes buffer is large enough for single column name
- If name doesn't fit: rollback to bookmark and throw exception
- Adapter wraps BinaryDataSink to implement Utf8Sink interface

**Design Notes**:

- Column names optional for readers that don't need column-by-name lookup
- Readers can skip this section entirely and use column indices
- Separated from types to enable future optimization (e.g., compress names)

**Transitions**: When all names written → ROW_GROUP_START

### 4. ROW_GROUP_START

**Purpose**: Read next batch of rows into memory
**Writes**: Nothing (internal state transition)

**Actions**:

1. Read up to `rowGroupSize` rows from cursor
2. Store in `rowGroupBuffer` (columnar)
3. If rows read → ROW_GROUP_HEADER
4. Else (cursor exhausted) → END_MARKER

**State Variables**:

- `rowsInCurrentGroup`: Number of rows in buffer
- `currentRowInGroup`: Current row being serialized
- `cursorExhausted`: Boolean flag

**Re-entry**: N/A (no I/O, completes immediately)

### 5. ROW_GROUP_HEADER

**Purpose**: Write row group header
**Writes**:

- Row count for this group (4 bytes)

**State Variables**:

- `rowsInCurrentGroup`: Value to write

**Re-entry**: Atomic

**Transitions**: → COLUMN_NULL_BITMAP_LENGTH

### 6. COLUMN_NULL_BITMAP_LENGTH

**Purpose**: Write null bitmap size
**Writes**:

- Bitmap length in bytes (4 bytes)

**Actions**:

- Prepare `currentNullBitmap` from column buffer
- Calculate length = `(rowCount + 7) / 8`

**State Variables**:

- `currentColumnInGroup`: Which column (0 to N-1)
- `currentNullBitmap`: Cached bitmap bytes
- `currentNullBitmapPos`: 0

**Re-entry**: Atomic

### 7. COLUMN_NULL_BITMAP_DATA

**Purpose**: Write null bitmap bytes
**Writes**:

- Bitmap bytes (variable length)

**State Variables**:

- `currentNullBitmap`: Bitmap data
- `currentNullBitmapPos`: Current byte index

**Re-entry**: Can resume mid-bitmap

**Transitions**:

- Variable-length type → COLUMN_OFFSETS_LENGTH
- Fixed-size type → COLUMN_DATA_LENGTH

### 8. COLUMN_OFFSETS_LENGTH (Variable-length types only)

**Purpose**: Write offsets array size
**Writes**:

- Offsets array length in bytes (4 bytes)
- Length = `(rowCount + 1) × 4`

**State Variables**:

- `currentOffsets`: IntList from column buffer

**Re-entry**: Atomic

### 9. COLUMN_OFFSETS_DATA (Variable-length types only)

**Purpose**: Write offset values
**Writes**:

- Array of int32 offsets (variable length)

**State Variables**:

- `currentOffsets`: IntList
- `currentOffsetIndex`: Current offset being written

**Re-entry**: Can resume mid-offsets

### 10. COLUMN_DATA_LENGTH

**Purpose**: Write column data size
**Writes**:

- Data length in bytes (4 bytes)

**Actions**:

- Calculate length based on column type
- Fixed types: `rowCount × typeSize`
- Variable types: `stringData.size()`

**State Variables**:

- `currentDataLength`: Calculated length

**Re-entry**: Atomic

**Transitions**:

- Variable-length type → COLUMN_DATA_VARIABLE
- Fixed-size type → COLUMN_DATA_FIXED

### 11. COLUMN_DATA_FIXED (Fixed-size types)

**Purpose**: Write fixed-size column data
**Writes**:

- Raw primitive values (rowCount × typeSize bytes)

**State Variables**:

- `currentDataPos`: Current row index being written

**Re-entry**: Can resume mid-column (at row boundary)

**Handling by Type**:

- BOOLEAN/BYTE/GEOBYTE: 1 byte per value
- SHORT/CHAR/GEOSHORT: 2 bytes per value
- INT/IPv4/GEOINT/FLOAT: 4 bytes per value
- LONG/DATE/TIMESTAMP/GEOLONG/DOUBLE: 8 bytes per value
- LONG128/UUID: 16 bytes per value (2 longs)
- LONG256: 32 bytes per value (4 longs)

### 12. COLUMN_DATA_VARIABLE (Variable-length types)

**Purpose**: Write variable-length data **without per-value length prefixes**
**Writes**:

- Concatenated raw bytes (no framing)

**Key Design**:

- NO length prefix per string/binary value
- Reader uses offset array to determine boundaries
- Enables splitting large values across buffer flushes

**State Variables**:

- `currentDataPos`: Byte position in concatenated data

**Re-entry**: Can resume mid-value

**Example**:

```
Strings: ["hello", "world", "foo"]
Offsets: [0, 5, 10, 13]  (written earlier)
Data:    "helloworldfoo"  (written here, no separators)
```

### 13. END_MARKER

**Purpose**: Signal end of stream
**Writes**:

- Row count `-1` (4 bytes)

**Re-entry**: Atomic

**Transitions**: → COMPLETE

### 14. COMPLETE

**Purpose**: Terminal state
**Writes**: Nothing

**State Variables**:

- `complete = true`

## Re-entrancy Mechanism

### How It Works

When `serialize(sink)` is called:

1. **Resume from current state**: State machine continues from `state` enum
2. **Use progress counters**: Each state uses `stateProgress`, `currentDataPos`, etc.
3. **Exception on buffer full**: `NoSpaceLeftInResponseBufferException` thrown
4. **State preserved**: All counters remain intact
5. **Next call continues**: Caller provides new buffer, serializer resumes

### Example Re-entry Scenario

```java
// Initial state
state =
COLUMN_DATA_VARIABLE
        currentDataPos = 1024  // 1KB into a 10KB string

// Call 1: Buffer has 100 bytes available
serialize(sink);
// Writes 100 bytes
// currentDataPos = 1124
// Throws NoSpaceLeftInResponseBufferException

// Caller flushes network buffer

// Call 2: Buffer has 1000 bytes available
serialize(sink);
// Resumes from currentDataPos = 1124
// Writes 1000 bytes
// currentDataPos = 2124
// Throws again...

// This continues until currentDataPos = 10240 (all data written)
// Then state transitions to next column or row group
```

### Critical Invariants

1. **Atomicity Guarantee**: Primitives up to 256 bits always fit in buffer
2. **Progress Monotonicity**: State never regresses
3. **Idempotency**: Re-calling with same buffer position produces same output
4. **Zero Data Loss**: No data discarded on exception

## Variable-Length Data Handling

### Why No Length Prefixes?

Traditional approach (with prefixes):

```
┌────────┬──────────────┬────────┬──────────────┐
│ len=5  │ "hello"      │ len=5  │ "world"      │
└────────┴──────────────┴────────┴──────────────┘
```

Problem: If `"hello"` doesn't fit in buffer, we've already written `len=5`. On resume, we'd duplicate the length.

Our approach (with offsets):

```
Offsets (written first):
┌───┬───┬────┐
│ 0 │ 5 │ 10 │
└───┴───┴────┘

Data (written second):
┌──────────────┬──────────────┐
│ "hello"      │ "world"      │
└──────────────┴──────────────┘
```

Advantages:

- Offsets written once, completely, before data
- Data stream can be interrupted anywhere
- Resume writes from exact byte position
- No duplication, no corruption

### Variable-Length State Sequence

```
COLUMN_OFFSETS_LENGTH
    │ Write: total bytes in offsets array
    ▼
COLUMN_OFFSETS_DATA
    │ Write: [offset₀, offset₁, ..., offsetₙ]
    │ Can be interrupted mid-array
    ▼
COLUMN_DATA_LENGTH
    │ Write: total bytes in data blob
    ▼
COLUMN_DATA_VARIABLE
    │ Write: concatenated raw bytes
    │ Can be interrupted anywhere, even mid-string
    ▼
(next column or row group)
```

## Memory Management

### Row Group Buffer

- **Size**: Fixed at initialization (`rowGroupSize` rows)
- **Structure**: Array of `ColumnBuffer` (one per column)
- **Lifecycle**:
    - Filled in `ROW_GROUP_START`
    - Read during `COLUMN_*` states
    - Cleared before next row group

### Column Buffer

Per column, stores:

- **Null bitmap**: `(rowGroupSize + 7) / 8` bytes
- **Fixed-size data**: Type-specific arrays (e.g., `long[]` for LONG)
- **Variable-size data**:
    - `IntList offsets`: Offset into data blob
    - `ByteArrayOutputStream stringData`: Concatenated data

### Memory Footprint

For `R` rows in group, `C` columns:

```
Null bitmaps:    C × ceil(R/8) bytes
Fixed data:      C × R × typeSize bytes
Variable data:   C × R × avgStringSize bytes
Offsets:         C × (R+1) × 4 bytes
```

Example (1000 rows, 10 columns, avg 20-byte strings):

- Null: 10 × 125 = 1.25 KB
- Fixed (8-byte longs): 10 × 1000 × 8 = 80 KB
- Variable: 10 × 1000 × 20 = 200 KB
- Offsets: 10 × 1001 × 4 = 40 KB
- **Total**: ~321 KB per row group

## Performance Characteristics

### Time Complexity

- **Per row group**: O(R × C) where R = rows, C = columns
- **Total**: O(N × C) where N = total rows
- **State transition**: O(1)

### Space Complexity

- **Row group buffer**: O(R × C × avgValueSize)
- **State variables**: O(C) for schema caching
- **Independent of total row count**: ✓

### Network Efficiency

- **No retransmissions**: State machine never re-sends data
- **Minimal overhead**: Only exception handling
- **Batching**: Row groups amortize syscall cost
- **Streaming**: First bytes sent immediately (schema)

## Thread Safety

**NOT thread-safe**. Serializer maintains mutable state:

- `state` enum
- Progress counters
- Row group buffer

**Usage**: One serializer instance per connection/thread.

## Error Handling

### Exception Types

1. **NoSpaceLeftInResponseBufferException**: Buffer full (expected, handled)
2. **IllegalStateException**: Invalid state transition (programming error)
3. **SqlException**: Cursor error (propagated to caller)

### Recovery Strategy

```java
try{
        while(!serializer.isComplete()){
        sink.

of(buffer, bufferSize);
        serializer.

serialize(sink);

        if(sink.

position() >0){

flushToNetwork(buffer, sink.position());
        }
        }
        }catch(
NoSpaceLeftInResponseBufferException e){

// This is normal - buffer filled up
flushToNetwork(buffer, sink.position());
        // Loop continues with fresh buffer
        }
```

## Testing Strategies

### Unit Tests

1. **State transitions**: Verify each state moves to correct next state
2. **Re-entrancy**: Small buffers (e.g., 64 bytes) force many interruptions
3. **Data integrity**: Compare output with non-streaming serializer
4. **Edge cases**: Empty result sets, single-row groups, NULL values

### Integration Tests

1. **Large datasets**: 1M+ rows to verify memory bounds
2. **Network simulation**: Inject buffer full exceptions randomly
3. **Round-trip**: Serialize then deserialize, verify equality
4. **Performance**: Measure throughput vs traditional streaming

## Future Enhancements

1. **Zero-copy strings**: Use DirectUtf8Sequence addresses instead of copying
2. **Compression**: Per-column compression (LZ4, Snappy)
3. **Dictionary encoding**: For low-cardinality string columns
4. **Parallel row groups**: Multiple threads filling buffers
5. **Adaptive row group sizing**: Based on network throughput

## References

- Apache Parquet: Columnar storage inspiration
- QuestDB Network Stack: `NoSpaceLeftInResponseBufferException` pattern
- State Machine Pattern: Classic CS design pattern
- Re-entrant Functions: POSIX standards

---

**Document Version**: 1.0
**Author**: QuestDB Engineering
**Date**: 2025-11-01
