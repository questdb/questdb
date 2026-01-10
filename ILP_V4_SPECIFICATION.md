# ILP v4 Protocol Specification

QuestDB's columnar binary wire protocol for high-throughput time-series ingestion.

## 1. Overview

### 1.1 Goals

- **Bandwidth efficiency**: Columnar layout eliminates repeated column names; binary encoding reduces size
- **Server CPU reduction**: Pre-transposed data requires no parsing or row-to-column conversion
- **Multi-table batching**: Single network round-trip for writes to multiple tables
- **Zero external dependencies**: Self-contained format, no FlatBuffers/Protobuf/Arrow libraries required
- **Backward compatibility**: Existing sender API preserved; wire format change is internal

### 1.2 Non-Goals

- Zero-copy decoding (acceptable to copy into WAL buffers)
- Streaming within a batch (entire batch must arrive before processing)
- Query responses (this protocol is ingestion-only)

---

## 2. Protocol Negotiation

### 2.1 Connection Handshake

ILP v4 uses a capability-based handshake on connection establishment.

```
Client → Server: CAPABILITY_REQUEST
Server → Client: CAPABILITY_RESPONSE
```

**CAPABILITY_REQUEST** (8 bytes):
```
┌────────────────────────────────────────────┐
│ Magic: "ILP?" (4 bytes)                    │
│ Min version supported: uint8               │
│ Max version supported: uint8               │
│ Flags: uint16                              │
│   bit 0: supports LZ4                      │
│   bit 1: supports Zstd                     │
│   bit 2: supports Gorilla timestamps       │
│   bits 3-15: reserved                      │
└────────────────────────────────────────────┘
```

**CAPABILITY_RESPONSE** (8 bytes):
```
┌────────────────────────────────────────────┐
│ Magic: "ILP!" (4 bytes)                    │
│ Negotiated version: uint8                  │
│ Reserved: uint8                            │
│ Flags: uint16 (server capabilities)        │
└────────────────────────────────────────────┘
```

If the server does not support ILP v4, it responds with `"ILP0"` magic, signaling fallback to text protocol.

### 2.2 Version Selection

The server selects the highest mutually supported version:
```
negotiated_version = min(client_max_version, server_max_version)
```

If `negotiated_version < client_min_version`, the connection fails.

---

## 3. Wire Format

### 3.1 Message Structure

Each ILP v4 message is a self-contained batch:

```
┌─────────────────────────────────────────────────────────────┐
│ MESSAGE HEADER (12 bytes)                                   │
├─────────────────────────────────────────────────────────────┤
│ TABLE BLOCK 1                                               │
├─────────────────────────────────────────────────────────────┤
│ TABLE BLOCK 2                                               │
├─────────────────────────────────────────────────────────────┤
│ ...                                                         │
├─────────────────────────────────────────────────────────────┤
│ TABLE BLOCK N                                               │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 Message Header

```
┌─────────────────────────────────────────────────────────────┐
│ Offset │ Size    │ Field                                    │
├────────┼─────────┼──────────────────────────────────────────┤
│ 0      │ 4 bytes │ Magic: "ILP4"                            │
│ 4      │ 1 byte  │ Version: uint8 (current: 1)              │
│ 5      │ 1 byte  │ Flags: uint8                             │
│        │         │   bit 0: compressed (LZ4)                │
│        │         │   bit 1: compressed (Zstd)               │
│        │         │   bit 2: Gorilla timestamps enabled      │
│        │         │   bits 3-7: reserved                     │
│ 6      │ 2 bytes │ Table count: uint16 (little-endian)      │
│ 8      │ 4 bytes │ Payload length: uint32 (little-endian)   │
└─────────────────────────────────────────────────────────────┘
```

If compression flag is set, bytes 12 onwards are compressed. Decompress before parsing table blocks.

### 3.3 Table Block

Each table block contains all rows for a single table:

```
┌─────────────────────────────────────────────────────────────┐
│ TABLE HEADER                                                │
├─────────────────────────────────────────────────────────────┤
│ SCHEMA SECTION                                              │
├─────────────────────────────────────────────────────────────┤
│ COLUMN DATA SECTION                                         │
└─────────────────────────────────────────────────────────────┘
```

#### 3.3.1 Table Header

```
┌─────────────────────────────────────────────────────────────┐
│ Table name length: varint                                   │
│ Table name: UTF-8 bytes                                     │
│ Row count: varint                                           │
│ Column count: varint                                        │
└─────────────────────────────────────────────────────────────┘
```

#### 3.3.2 Schema Section

The schema section defines column metadata. Two modes:

**Full Schema** (schema_mode = 0x00):
```
┌─────────────────────────────────────────────────────────────┐
│ Schema mode: 0x00 (1 byte)                                  │
│ For each column:                                            │
│   Column name length: varint                                │
│   Column name: UTF-8 bytes                                  │
│   Column type: uint8 (see type codes)                       │
└─────────────────────────────────────────────────────────────┘
```

**Schema Reference** (schema_mode = 0x01):
```
┌─────────────────────────────────────────────────────────────┐
│ Schema mode: 0x01 (1 byte)                                  │
│ Schema hash: uint64 (little-endian)                         │
└─────────────────────────────────────────────────────────────┘
```

Schema hash is computed as XXH64 over the full schema bytes. Server caches schemas by (table_name, hash). If server doesn't recognize the hash, it responds with `SCHEMA_REQUIRED` error.

#### 3.3.3 Column Type Codes

| Code | Type | Description |
|------|------|-------------|
| 0x01 | BOOLEAN | 1 bit per value (packed) |
| 0x02 | BYTE | int8 |
| 0x03 | SHORT | int16 little-endian |
| 0x04 | INT | int32 little-endian |
| 0x05 | LONG | int64 little-endian |
| 0x06 | FLOAT | IEEE 754 float32 |
| 0x07 | DOUBLE | IEEE 754 float64 |
| 0x08 | STRING | Length-prefixed UTF-8 |
| 0x09 | SYMBOL | Dictionary-encoded string |
| 0x0A | TIMESTAMP | int64 microseconds since epoch |
| 0x0B | DATE | int64 milliseconds since epoch |
| 0x0C | UUID | 16 bytes (big-endian) |
| 0x0D | LONG256 | 32 bytes (big-endian) |
| 0x0E | GEOHASH | varint bits + packed geohash |
| 0x0F | VARCHAR | Length-prefixed UTF-8 (aux storage) |

High bit (0x80) indicates nullable column.

---

## 4. Column Encoding

### 4.1 Null Bitmap

For nullable columns, a null bitmap precedes the data:

```
┌─────────────────────────────────────────────────────────────┐
│ Null bitmap: ceil(row_count / 8) bytes                      │
│   bit[i] = 1 means row[i] is NULL                           │
│   Bit order: LSB first within each byte                     │
└─────────────────────────────────────────────────────────────┘
```

### 4.2 Fixed-Width Types

BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DATE, UUID, LONG256:

```
┌─────────────────────────────────────────────────────────────┐
│ [Null bitmap if nullable]                                   │
│ Values: row_count * sizeof(type) bytes                      │
│   All little-endian                                         │
│   NULL positions contain undefined bytes (skip via bitmap)  │
└─────────────────────────────────────────────────────────────┘
```

### 4.3 Boolean

```
┌─────────────────────────────────────────────────────────────┐
│ [Null bitmap if nullable]                                   │
│ Value bits: ceil(row_count / 8) bytes                       │
│   bit[i] = boolean value for row[i]                         │
│   LSB first within each byte                                │
└─────────────────────────────────────────────────────────────┘
```

### 4.4 Timestamp (Gorilla Encoding)

When Gorilla timestamps are enabled (flag bit 2):

```
┌─────────────────────────────────────────────────────────────┐
│ [Null bitmap if nullable]                                   │
│ First timestamp: int64 (8 bytes, little-endian)             │
│ Second timestamp: int64 (8 bytes, little-endian)            │
│ Remaining timestamps: bit-packed delta-of-delta             │
└─────────────────────────────────────────────────────────────┘
```

**Delta-of-delta encoding:**

```
Let D = (t[n] - t[n-1]) - (t[n-1] - t[n-2])

if D == 0:
    write '0'                          (1 bit)
elif D in [-63, 64]:
    write '10' + signed 7-bit value    (9 bits)
elif D in [-255, 256]:
    write '110' + signed 9-bit value   (12 bits)
elif D in [-2047, 2048]:
    write '1110' + signed 12-bit value (16 bits)
else:
    write '1111' + signed 32-bit value (36 bits)
```

Signed values use two's complement. Bits are packed LSB-first, spanning byte boundaries.

**Fallback for irregular data:**

If delta-of-delta would exceed 32 bits, or data is out-of-order, fall back to uncompressed int64 array and clear the Gorilla flag for that column:

```
┌─────────────────────────────────────────────────────────────┐
│ Encoding flag: 0x00 (uncompressed) or 0x01 (Gorilla)        │
│ [Data according to flag]                                    │
└─────────────────────────────────────────────────────────────┘
```

### 4.5 String / VARCHAR

```
┌─────────────────────────────────────────────────────────────┐
│ [Null bitmap if nullable]                                   │
│ Offset array: (row_count + 1) * uint32                      │
│   offset[0] = 0                                             │
│   offset[i+1] = end position of string[i]                   │
│   string[i] bytes = data[offset[i]..offset[i+1]]            │
│ String data: concatenated UTF-8 bytes                       │
└─────────────────────────────────────────────────────────────┘
```

### 4.6 Symbol (Dictionary Encoding)

Symbols use a per-batch dictionary:

```
┌─────────────────────────────────────────────────────────────┐
│ [Null bitmap if nullable]                                   │
│ Dictionary size: varint                                     │
│ For each dictionary entry:                                  │
│   String length: varint                                     │
│   String data: UTF-8 bytes                                  │
│ Value array: row_count * varint (dictionary indices)        │
│   Index 0 = first dictionary entry                          │
│   Index -1 (max varint) = NULL (alternative to bitmap)      │
└─────────────────────────────────────────────────────────────┘
```

---

## 5. Varint Encoding

Variable-length integer encoding (unsigned):

```
while value >= 0x80:
    write (value & 0x7F) | 0x80
    value >>= 7
write value & 0x7F
```

| Value Range | Bytes |
|-------------|-------|
| 0 - 127 | 1 |
| 128 - 16383 | 2 |
| 16384 - 2097151 | 3 |
| 2097152 - 268435455 | 4 |
| larger | 5+ |

For signed integers, use ZigZag encoding before varint:
```
zigzag(n) = (n << 1) ^ (n >> 63)
```

---

## 6. Compression

### 6.1 LZ4 (Flag bit 0)

When LZ4 compression is enabled:

```
┌─────────────────────────────────────────────────────────────┐
│ MESSAGE HEADER (12 bytes, uncompressed)                     │
├─────────────────────────────────────────────────────────────┤
│ Uncompressed size: uint32 (little-endian)                   │
│ LZ4 compressed payload                                      │
└─────────────────────────────────────────────────────────────┘
```

Use LZ4 block format (not frame format) for minimal overhead.

### 6.2 Zstd (Flag bit 1)

Same structure as LZ4, using Zstd block compression. Prefer compression level 1-3 for speed.

### 6.3 Compression Selection

Recommended heuristics:
- Batch < 1KB: no compression (overhead exceeds benefit)
- Batch 1KB - 64KB: LZ4
- Batch > 64KB: Zstd level 1

---

## 7. Schema Evolution

### 7.1 Adding Columns

New columns can be added by sending full schema with additional columns. Server auto-creates missing columns with NULL for existing rows.

### 7.2 Column Type Mismatch

If sent type differs from table schema:
- Implicit widening allowed: BYTE→SHORT→INT→LONG, FLOAT→DOUBLE
- Other mismatches: `SCHEMA_MISMATCH` error, batch rejected

### 7.3 Missing Columns

Columns present in table but absent from batch: filled with NULL.

### 7.4 Schema Hash Invalidation

Server invalidates cached schema hash when:
- Table schema changes (DDL)
- Table is dropped and recreated
- Server restart

Client should handle `SCHEMA_REQUIRED` by re-sending full schema.

---

## 8. Error Handling

### 8.1 Response Format

After each batch, server sends a response:

```
┌─────────────────────────────────────────────────────────────┐
│ Status code: uint8                                          │
│ [Error payload if status != 0]                              │
└─────────────────────────────────────────────────────────────┘
```

### 8.2 Status Codes

| Code | Name | Description |
|------|------|-------------|
| 0x00 | OK | Batch accepted |
| 0x01 | PARTIAL | Some rows failed (see payload) |
| 0x02 | SCHEMA_REQUIRED | Schema hash not recognized |
| 0x03 | SCHEMA_MISMATCH | Column type incompatible |
| 0x04 | TABLE_NOT_FOUND | Table doesn't exist (if auto-create disabled) |
| 0x05 | PARSE_ERROR | Malformed message |
| 0x06 | INTERNAL_ERROR | Server error |
| 0x07 | OVERLOADED | Back-pressure, retry later |

### 8.3 Partial Failure

For PARTIAL status:
```
┌─────────────────────────────────────────────────────────────┐
│ Failed table count: varint                                  │
│ For each failed table:                                      │
│   Table index: varint (0-based index in batch)              │
│   Error code: uint8                                         │
│   Error message length: varint                              │
│   Error message: UTF-8 bytes                                │
└─────────────────────────────────────────────────────────────┘
```

### 8.4 Retry Semantics

- `SCHEMA_REQUIRED`: Retry with full schema
- `OVERLOADED`: Exponential backoff, retry same batch
- `PARTIAL`: Log failed tables, continue with next batch
- Other errors: Log and optionally retry

---

## 9. Flow Control

### 9.1 Batch Size Limits

| Limit | Default | Configurable |
|-------|---------|--------------|
| Max batch size | 16 MB | Yes |
| Max tables per batch | 256 | Yes |
| Max rows per table | 1,000,000 | Yes |
| Max columns per table | 2,048 | No (QuestDB limit) |

### 9.2 Back-Pressure

When server returns `OVERLOADED`:
1. Client waits `min(retry_delay * 2^attempt, max_delay)`
2. Default: `retry_delay=100ms`, `max_delay=10s`
3. After `max_retries` (default: 10), fail the batch

### 9.3 Pipelining

Clients MAY send multiple batches before receiving responses. Server processes in order and responds in order. Recommended max in-flight batches: 4.

---

## 10. Examples

### 10.1 Simple Batch

Two rows to `trades` table:

```
trades,symbol=ETH-USD price=2615.54 1704067200000000
trades,symbol=BTC-USD price=42000.00 1704067200000001
```

ILP v4 encoding (hex, annotated):

```
# Header (12 bytes)
49 4C 50 34          # Magic: "ILP4"
01                   # Version: 1
04                   # Flags: Gorilla timestamps
01 00                # Table count: 1
XX XX XX XX          # Payload length

# Table block
06                   # Table name length: 6
74 72 61 64 65 73    # Table name: "trades"
02                   # Row count: 2
03                   # Column count: 3

# Schema (full)
00                   # Schema mode: full
06 73 79 6D 62 6F 6C # Column: "symbol", length 6
09                   # Type: SYMBOL
05 70 72 69 63 65    # Column: "price", length 5
07                   # Type: DOUBLE
09 74 69 6D 65 73 74 61 6D 70  # Column: "timestamp", length 9
0A                   # Type: TIMESTAMP

# Column: symbol (dictionary encoded)
02                   # Dictionary size: 2
07 45 54 48 2D 55 53 44  # "ETH-USD"
07 42 54 43 2D 55 53 44  # "BTC-USD"
00                   # Row 0 → index 0
01                   # Row 1 → index 1

# Column: price (double, 8 bytes each)
71 3D 0A D7 A3 70 A4 40  # 2615.54
00 00 00 00 00 80 E4 40  # 42000.00

# Column: timestamp (Gorilla encoded)
01                   # Encoding: Gorilla
00 80 1E 4C 54 86 05 00  # First timestamp: 1704067200000000
01 80 1E 4C 54 86 05 00  # Second timestamp: 1704067200000001
                         # (only 2 rows, no delta-of-delta needed)
```

### 10.2 Multi-Table Batch

```
trades,symbol=ETH-USD price=2615.54 1704067200000000
metrics,host=server1 cpu=45.2,mem=8192 1704067200000000
```

Header table count = 2, followed by two table blocks.

---

## 11. Implementation Notes

### 11.1 Client-Side Buffering

Recommended client implementation:

```
class ILP4Sender:
    buffers: Map<TableName, TableBuffer>

    def table(name):
        return buffers.getOrCreate(name)

    def flush():
        batch = encode_batch(buffers)
        send(batch)
        response = receive()
        handle_response(response)
        buffers.clear()
```

Each `TableBuffer` accumulates rows in columnar form:
- Symbol columns: maintain local dictionary
- Other columns: append to typed arrays
- Timestamps: store raw values, encode on flush

### 11.2 Server-Side Processing

```
def process_batch(batch):
    for table_block in batch.tables:
        schema = resolve_schema(table_block)
        writer = get_wal_writer(table_block.table_name)
        writer.write_columnar(table_block.columns, schema)
```

Key optimization: columnar data can be memcpy'd directly to WAL segments when types match.

### 11.3 Memory Estimation

Client-side buffer memory per table:
```
memory = sum(column_size * row_count for column in columns)
       + dictionary_overhead  # ~50 bytes per unique symbol
       + offset_arrays        # 4 bytes per string per row
```

---

## 12. Security Considerations

### 12.1 Input Validation

Server MUST validate:
- Magic bytes match expected value
- Payload length ≤ max_batch_size
- Table count ≤ max_tables_per_batch
- Row count ≤ max_rows_per_table
- Column count ≤ 2048
- String lengths ≤ max_string_length (default 1MB)
- UTF-8 validity for all strings

### 12.2 Resource Exhaustion

- Decompression bombs: check uncompressed size before decompressing
- Dictionary attacks: limit symbol dictionary size per batch
- Memory: pre-allocate based on header sizes, reject oversized batches early

### 12.3 Authentication

ILP v4 uses the same authentication as ILP/TCP:
- TLS for transport encryption
- Token-based authentication in connection handshake
- Existing `ilp.auth` configuration applies

---

## 13. Migration Path

### 13.1 Protocol Detection

Server listens on same port for both protocols:
- ILP v4: starts with `"ILP?"` (capability request) or `"ILP4"` (direct batch)
- ILP v1-v3 (text): starts with table name (alphanumeric)

### 13.2 Client Upgrade

Recommended rollout:
1. Server upgrade: add ILP v4 support alongside text protocol
2. Client libraries: add ILP v4 encoder behind feature flag
3. Gradual enablement: test with non-critical workloads
4. Default switch: make ILP v4 default in next major version

### 13.3 Fallback

If capability negotiation fails (old server), client falls back to text protocol automatically.

---

## 14. Performance Expectations

### 14.1 Wire Size Reduction

Compared to text ILP (baseline 100%):

| Scenario | Text ILP | ILP v4 | ILP v4 + LZ4 |
|----------|----------|--------|--------------|
| Numeric-heavy (sensors) | 100% | ~35% | ~25% |
| String-heavy (logs) | 100% | ~60% | ~40% |
| Symbol-heavy (trades) | 100% | ~30% | ~22% |
| Timestamp-regular (1s interval) | 100% | ~20% | ~18% |

### 14.2 CPU Impact

| Operation | Text ILP | ILP v4 |
|-----------|----------|--------|
| Client encode | 1x | 1.5x (columnar transpose) |
| Network transfer | 1x | 0.3x (smaller payload) |
| Server decode | 1x | 0.2x (no parsing) |
| WAL write | 1x | 0.8x (columnar memcpy) |
| **Total** | 1x | ~0.5x |

Note: Actual results depend on workload. Benchmark with representative data.

---

## Appendix A: Varint Reference Implementation

```java
public static int writeVarint(byte[] buf, int pos, long value) {
    while ((value & ~0x7FL) != 0) {
        buf[pos++] = (byte) ((value & 0x7F) | 0x80);
        value >>>= 7;
    }
    buf[pos++] = (byte) value;
    return pos;
}

public static long readVarint(ByteBuffer buf) {
    long result = 0;
    int shift = 0;
    byte b;
    do {
        b = buf.get();
        result |= (long) (b & 0x7F) << shift;
        shift += 7;
    } while ((b & 0x80) != 0);
    return result;
}
```

---

## Appendix B: Gorilla Timestamp Reference Implementation

```java
public class GorillaTimestampEncoder {
    private long prevTimestamp;
    private long prevDelta;
    private BitWriter bits;

    public void encode(long timestamp) {
        long delta = timestamp - prevTimestamp;
        long deltaOfDelta = delta - prevDelta;

        if (deltaOfDelta == 0) {
            bits.write(0, 1);  // '0'
        } else if (deltaOfDelta >= -63 && deltaOfDelta <= 64) {
            bits.write(0b10, 2);
            bits.writeSigned(deltaOfDelta, 7);
        } else if (deltaOfDelta >= -255 && deltaOfDelta <= 256) {
            bits.write(0b110, 3);
            bits.writeSigned(deltaOfDelta, 9);
        } else if (deltaOfDelta >= -2047 && deltaOfDelta <= 2048) {
            bits.write(0b1110, 4);
            bits.writeSigned(deltaOfDelta, 12);
        } else {
            bits.write(0b1111, 4);
            bits.writeSigned(deltaOfDelta, 32);
        }

        prevDelta = delta;
        prevTimestamp = timestamp;
    }
}
```

---

## Appendix C: Schema Hash Computation

```java
public static long computeSchemaHash(List<Column> columns) {
    XXHash64 hasher = XXHash64.create();
    for (Column col : columns) {
        hasher.update(col.name.getBytes(UTF_8));
        hasher.update((byte) col.type);
    }
    return hasher.getValue();
}
```

Use XXH64 for speed. Schema hash is for caching optimization only; collisions are handled by re-sending full schema.