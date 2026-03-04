# QuestWire Protocol (QWP) Specification

QuestWire Protocol (QWP), internally referred to as **ILP v4**, is a columnar
binary ingestion protocol designed for high-throughput, zero-GC data streaming
into QuestDB. It operates over WebSocket (binary frames) at the endpoint
`/write/v4`.

## Transport

QWP uses RFC 6455 WebSocket binary frames. The client initiates an HTTP GET
request to `/write/v4` with standard WebSocket upgrade headers. After the 101
Switching Protocols handshake, all communication uses binary frames.

## Message Format

Every message begins with a fixed 12-byte header followed by a variable-length
payload.

### Message Header (12 bytes)

```
Offset  Size  Type       Field
─────── ───── ────────── ─────────────────────────────────────
0       4     uint32 LE  Magic: 0x34504C49 ("ILP4" in ASCII)
4       1     uint8      Version (currently 0x01)
5       1     uint8      Flags (see below)
6       2     uint16 LE  Table count (number of table blocks)
8       4     uint32 LE  Payload length in bytes
```

**Total message size** = 12 + payload length.

### Flags Byte

| Bit | Mask   | Name                  | Description                                           |
|-----|--------|-----------------------|-------------------------------------------------------|
| 0   | `0x01` | `FLAG_LZ4`            | LZ4 compression on payload                            |
| 1   | `0x02` | `FLAG_ZSTD`           | Zstd compression on payload                           |
| 2   | `0x04` | `FLAG_GORILLA`        | Gorilla delta-of-delta encoding for timestamp columns  |
| 3   | `0x08` | `FLAG_DELTA_SYMBOL_DICT` | Delta symbol dictionary mode enabled                |
| 4-7 |        |                       | Reserved (must be 0)                                  |

Constraints:
- `FLAG_LZ4` and `FLAG_ZSTD` are mutually exclusive.
- When `FLAG_GORILLA` is set, each TIMESTAMP/TIMESTAMP_NANOS column data section
  includes a 1-byte encoding tag (0x00 = uncompressed, 0x01 = Gorilla).

### Other Magic Values

| Magic bytes  | uint32 LE    | Purpose                             |
|-------------|--------------|-------------------------------------|
| `ILP4`      | `0x34504C49` | Data message                        |
| `ILP?`      | `0x3F504C49` | Capability request (8 bytes total)  |
| `ILP!`      | `0x21504C49` | Capability response (8 bytes total) |
| `ILP0`      | `0x30504C49` | Fallback (old server)               |

## Payload Structure

```
┌─────────────────────────────────────────┐
│ [Delta Symbol Dictionary] (if flag set) │
│ [Table Block 0]                         │
│ [Table Block 1]                         │
│ ...                                     │
│ [Table Block N-1]                       │
└─────────────────────────────────────────┘
```

### Delta Symbol Dictionary (optional)

Present only when `FLAG_DELTA_SYMBOL_DICT` (0x08) is set. Appears at the start
of the payload, before any table blocks.

```
┌──────────────────────────────────────────────────────────────┐
│ delta_start:    varint   Starting global ID for this delta   │
│ delta_count:    varint   Number of new entries               │
│ For each new entry:                                          │
│   name_length:  varint   UTF-8 byte length                   │
│   name_bytes:   bytes    UTF-8 encoded symbol string         │
└──────────────────────────────────────────────────────────────┘
```

The client maintains a global symbol dictionary mapping symbol strings to
sequential integer IDs (starting from 0). On each batch, only newly added
symbols (the "delta") are transmitted. The server accumulates these entries
across batches for the lifetime of the connection.

### Table Block

Each table block contains data for one table:

```
┌─────────────────────────────────────────────────────┐
│ Table Header                                        │
│   table_name_len:  varint    UTF-8 byte length      │
│   table_name:      bytes     UTF-8 table name       │
│   row_count:       varint    Number of rows          │
│   column_count:    varint    Number of columns       │
│                                                     │
│ Schema Section                                      │
│   schema_mode:     uint8     0x00=full, 0x01=ref    │
│   [Full schema OR schema reference]                 │
│                                                     │
│ Column Data (repeated for each column)              │
│   [null bitmap if nullable]                         │
│   [type-specific encoded values]                    │
└─────────────────────────────────────────────────────┘
```

## Schema Section

### Full Schema Mode (0x00)

Sent on the first batch for a given table, or when the server responds with
`SCHEMA_REQUIRED`.

```
┌─────────────────────────────────────────────────────┐
│ 0x00                                                │
│ For each column:                                    │
│   col_name_len:   varint     UTF-8 byte length      │
│   col_name:       bytes      UTF-8 column name      │
│   type_code:      uint8      Type + nullable flag   │
└─────────────────────────────────────────────────────┘
```

The `type_code` byte encodes both the column type and nullability:
- Bits 0-6: Column type (0x01 - 0x16)
- Bit 7 (0x80): Nullable flag (1 = nullable)

A column with an **empty name** (length 0) and type TIMESTAMP denotes the
designated timestamp column.

### Schema Reference Mode (0x01)

Used for subsequent batches when the server has already cached the schema:

```
┌─────────────────────────────────────────┐
│ 0x01                                    │
│ schema_hash:   int64 LE    XXH64 hash   │
└─────────────────────────────────────────┘
```

The schema hash is computed with XXH64 over all column name bytes and type codes.
If the server does not recognize the hash, it responds with `SCHEMA_REQUIRED`,
and the client resends with full schema mode.

## Column Types

| Code   | Name             | Wire Size       | Byte Order    | Description                                        |
|--------|------------------|-----------------|---------------|----------------------------------------------------|
| `0x01` | BOOLEAN          | 1 bit/value     | LSB-first     | Bit-packed, 8 values per byte                      |
| `0x02` | BYTE             | 1 byte          | N/A           | Signed int8                                        |
| `0x03` | SHORT            | 2 bytes         | Little-endian | Signed int16                                       |
| `0x04` | INT              | 4 bytes         | Little-endian | Signed int32                                       |
| `0x05` | LONG             | 8 bytes         | Little-endian | Signed int64                                       |
| `0x06` | FLOAT            | 4 bytes         | Little-endian | IEEE 754 float32                                   |
| `0x07` | DOUBLE           | 8 bytes         | Little-endian | IEEE 754 float64                                   |
| `0x08` | STRING           | Variable        | Little-endian | Offset array + UTF-8 data                          |
| `0x09` | SYMBOL           | Variable        | N/A           | Dictionary-encoded string                          |
| `0x0A` | TIMESTAMP        | 8 bytes*        | Little-endian | int64 microseconds since Unix epoch                |
| `0x0B` | DATE             | 8 bytes         | Little-endian | int64 milliseconds since Unix epoch                |
| `0x0C` | UUID             | 16 bytes        | Big-endian    | 128-bit UUID (hi, lo)                              |
| `0x0D` | LONG256          | 32 bytes        | Big-endian    | 256-bit integer                                    |
| `0x0E` | GEOHASH          | Variable        | Little-endian | Varint precision + packed values                   |
| `0x0F` | VARCHAR          | Variable        | Little-endian | Same wire format as STRING                         |
| `0x10` | TIMESTAMP_NANOS  | 8 bytes*        | Little-endian | int64 nanoseconds since Unix epoch                 |
| `0x11` | DOUBLE_ARRAY     | Variable        | Little-endian | N-dimensional array of float64                     |
| `0x12` | LONG_ARRAY       | Variable        | Little-endian | N-dimensional array of int64                       |
| `0x13` | DECIMAL64        | 8 bytes + scale | Big-endian    | 64-bit scaled integer (18-digit precision)         |
| `0x14` | DECIMAL128       | 16 bytes + scale| Big-endian    | 128-bit scaled integer (38-digit precision)        |
| `0x15` | DECIMAL256       | 32 bytes + scale| Big-endian    | 256-bit scaled integer (77-digit precision)        |
| `0x16` | CHAR             | 2 bytes         | Little-endian | Single UTF-16 code unit                            |

\* TIMESTAMP and TIMESTAMP_NANOS may use Gorilla encoding when `FLAG_GORILLA` is set.

## Null Bitmap

For nullable columns (type code has bit 7 set), a null bitmap precedes the
column values:

```
Size: ceil(row_count / 8) bytes
Bit order: LSB-first within each byte
Bit = 1 → row is NULL
Bit = 0 → row has a value
```

Example: 10 rows, rows 0, 2, 9 are NULL:
```
Byte 0: 0b00000101  (bits 0 and 2 set)
Byte 1: 0b00000010  (bit 1 set = row 9)
```

Non-null values are stored densely in the column data section. The decoder uses
the bitmap to expand sparse non-null values into dense storage.

## Column Encoding Details

### Fixed-Width Types

BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DATE, CHAR are written as contiguous
arrays of their respective sizes in little-endian byte order. The encoder uses
bulk memory copy when wire format matches native byte order.

### BOOLEAN (Bit-Packed)

Values are packed 8 per byte, LSB-first. For `N` values, `ceil(N/8)` bytes are
written.

```
Byte layout for values [true, false, true, true, false, false, false, true]:
  0b10001101 = 0x8D
```

### STRING / VARCHAR

```
┌──────────────────────────────────────────────────────┐
│ Offset array: (value_count + 1) x uint32 LE          │
│   offsets[0] = 0                                     │
│   offsets[i] = byte offset of end of string i        │
│ UTF-8 data: concatenated string bytes                │
└──────────────────────────────────────────────────────┘
```

String `i` spans bytes `data[offsets[i] .. offsets[i+1])`.

### SYMBOL (Dictionary-Encoded)

#### Per-Table Dictionary Mode (default)

```
┌───────────────────────────────────────────┐
│ dict_size:     varint   Number of entries │
│ For each entry:                           │
│   str_len:     varint   UTF-8 byte length │
│   str_bytes:   bytes    UTF-8 string      │
│ For each row:                             │
│   index:       varint   Dictionary index  │
└───────────────────────────────────────────┘
```

#### Global Delta Dictionary Mode (FLAG_DELTA_SYMBOL_DICT)

When the delta symbol dictionary flag is set, symbol columns use global integer
IDs instead of per-table dictionaries. The dictionary entries are sent in the
message-level delta dictionary section (see above). Column data consists of
varint-encoded global IDs only.

```
┌───────────────────────────────────────────┐
│ For each row:                             │
│   global_id:   varint   Global symbol ID  │
└───────────────────────────────────────────┘
```

### TIMESTAMP / TIMESTAMP_NANOS

When `FLAG_GORILLA` is set, the column data begins with a 1-byte encoding tag:

| Tag    | Encoding      |
|--------|---------------|
| `0x00` | Uncompressed  |
| `0x01` | Gorilla       |

**Uncompressed**: `count` x int64 values in little-endian.

**Gorilla delta-of-delta encoding**:

```
┌───────────────────────────────────────────────────────┐
│ t[0]:    int64 LE    First timestamp (uncompressed)   │
│ t[1]:    int64 LE    Second timestamp (uncompressed)  │
│ t[2..N]: bit-packed  Delta-of-delta encoded values    │
└───────────────────────────────────────────────────────┘
```

Delta-of-delta calculation:
```
delta[i]    = t[i] - t[i-1]
DoD[i]      = delta[i] - delta[i-1]
```

Encoding buckets (bits are written LSB-first):

| Condition                  | Prefix  | Value Bits | Total Bits |
|----------------------------|---------|------------|------------|
| DoD == 0                   | `0`     | 0          | 1          |
| DoD in [-64, 63]           | `10`    | 7 (signed) | 9          |
| DoD in [-256, 255]         | `110`   | 9 (signed) | 12         |
| DoD in [-2048, 2047]       | `1110`  | 12 (signed)| 16         |
| Otherwise                  | `1111`  | 32 (signed)| 36         |

The bit stream is padded to a byte boundary at the end. If any DoD value exceeds
the 32-bit signed integer range, the encoder falls back to uncompressed mode.

When `FLAG_GORILLA` is **not** set, timestamp columns are written as plain
uncompressed int64 arrays without the encoding tag byte.

### UUID

16 bytes per value in **big-endian** order: 8 bytes high, 8 bytes low.

### LONG256

32 bytes per value in **big-endian** order: four int64 values (highest to lowest).

### GEOHASH

```
┌───────────────────────────────────────────────────────┐
│ precision:   varint     Bit precision (1-60)          │
│ For each row:                                         │
│   value:     ceil(precision/8) bytes, little-endian   │
└───────────────────────────────────────────────────────┘
```

### DOUBLE_ARRAY / LONG_ARRAY

N-dimensional arrays, row-major order:

```
┌───────────────────────────────────────────────────────┐
│ For each row:                                         │
│   n_dims:      uint8          Number of dimensions    │
│   dim_lengths: n_dims x int32 LE   Length per dim     │
│   values:      product(dims) x element, LE            │
│                (float64 for DOUBLE_ARRAY,              │
│                 int64 for LONG_ARRAY)                  │
└───────────────────────────────────────────────────────┘
```

### DECIMAL64 / DECIMAL128 / DECIMAL256

Decimal columns include a 1-byte scale prefix before the value array:

```
┌──────────────────────────────────────────────────────┐
│ scale:     uint8                                     │
│ For each row:                                        │
│   value:   8/16/32 bytes, big-endian                 │
│            (unscaled integer representation)         │
└──────────────────────────────────────────────────────┘
```

| Type        | Value Size | Precision     |
|-------------|------------|---------------|
| DECIMAL64   | 8 bytes    | 18 digits     |
| DECIMAL128  | 16 bytes   | 38 digits     |
| DECIMAL256  | 32 bytes   | 77 digits     |

## Variable-Length Integer (Varint) Encoding

QWP uses unsigned LEB128 (Little Endian Base 128) for all variable-length
integers:

- Values are split into 7-bit groups, least significant group first.
- Each byte uses bit 7 (`0x80`) as a continuation flag:
  - Set (1) = more bytes follow.
  - Clear (0) = last byte.
- Maximum encoding length: 10 bytes for a 64-bit value.

Examples:
```
Value     Encoded bytes
───────── ─────────────
0         [0x00]
1         [0x01]
127       [0x7F]
128       [0x80, 0x01]
300       [0xAC, 0x02]
16384     [0x80, 0x80, 0x01]
```

## ZigZag Encoding

Used to map signed integers to unsigned for efficient varint encoding:

```
encode(n) = (n << 1) ^ (n >> 63)    // 64-bit
decode(n) = (n >>> 1) ^ -(n & 1)

 0 →  0
-1 →  1
 1 →  2
-2 →  3
 2 →  4
```

## Response Format

The server responds to each batch with a binary WebSocket frame:

### OK Response (1 byte)

```
┌────────────────────────┐
│ status: uint8  (0x00)  │
└────────────────────────┘
```

### Error Response

```
┌───────────────────────────────────────────────────────┐
│ status:      uint8          Status code               │
│ msg_len:     varint         Error message length       │
│ msg_bytes:   bytes          UTF-8 error message        │
└───────────────────────────────────────────────────────┘
```

### Partial Failure Response (status 0x01)

```
┌───────────────────────────────────────────────────────┐
│ status:        uint8   (0x01)                         │
│ failed_count:  varint  Number of failed tables         │
│ For each failed table:                                │
│   table_index: varint  0-based index in batch          │
│   error_code:  uint8   Per-table error code            │
│   msg_len:     varint  Error message length             │
│   msg_bytes:   bytes   UTF-8 error message             │
└───────────────────────────────────────────────────────┘
```

### Status Codes

| Code   | Name              | Retriable | Description                                          |
|--------|-------------------|-----------|------------------------------------------------------|
| `0x00` | OK                | -         | Batch accepted                                       |
| `0x01` | PARTIAL           | No        | Some rows failed; error payload has per-table details |
| `0x02` | SCHEMA_REQUIRED   | Yes       | Schema hash not recognized; resend with full schema   |
| `0x03` | SCHEMA_MISMATCH   | No        | Column type incompatible with existing table          |
| `0x04` | TABLE_NOT_FOUND   | No        | Table does not exist (auto-create disabled)           |
| `0x05` | PARSE_ERROR       | No        | Malformed message                                    |
| `0x06` | INTERNAL_ERROR    | No        | Server-side error                                    |
| `0x07` | OVERLOADED        | Yes       | Back-pressure; client should retry with backoff       |

## Protocol Limits

| Limit                    | Default Value |
|--------------------------|---------------|
| Max batch size           | 16 MB         |
| Max tables per batch     | 256           |
| Max rows per table       | 1,000,000     |
| Max columns per table    | 2,048         |
| Max table name length    | 127 bytes     |
| Max column name length   | 127 bytes     |
| Max string value length  | 1 MB          |
| Max in-flight batches    | 4             |

## Client Operation

### Double-Buffered Async I/O

The client uses double-buffered microbatches:

1. The user thread writes rows to the **active** buffer.
2. When a buffer reaches its threshold (row count, byte size, or age), the
   client seals it and enqueues it for sending.
3. A dedicated I/O thread sends batches over the WebSocket.
4. The client swaps to the other buffer so writing can continue without blocking.

### Auto-Flush Triggers

| Trigger             | Default    |
|---------------------|------------|
| Row count           | 500 rows   |
| Byte size           | 1 MB       |
| Time since first row| 100 ms     |

### Schema Caching

- First batch for a given table: full schema mode (0x00).
- Subsequent batches: schema reference mode (0x01) with 8-byte XXH64 hash.
- If the server returns SCHEMA_REQUIRED, the client resends with full schema.

### Symbol Dictionary Lifecycle

- The client maintains a global symbol dictionary across all tables/columns.
- Symbol IDs are assigned sequentially starting from 0.
- Each batch sends only the **delta** (newly added symbols since the last batch).
- The server accumulates these deltas for the lifetime of the connection.
- Upon connection loss, both sides reset the dictionary.

## Endianness Summary

| Category                         | Byte Order    |
|----------------------------------|---------------|
| Message header fields            | Little-endian |
| Varint-encoded values            | LEB128 (LE)   |
| Fixed-width numerics (most)      | Little-endian |
| UUID                             | Big-endian    |
| LONG256                          | Big-endian    |
| DECIMAL64 / DECIMAL128 / DECIMAL256 | Big-endian |
| String offsets                   | Little-endian |
| Schema hash                      | Little-endian |

## Worked Example

A message with 1 table ("sensors"), 2 rows, 3 columns (symbol "host", double
"temp", timestamp):

```
Header (12 bytes):
  49 4C 50 34   -- Magic: "ILP4"
  01            -- Version: 1
  0C            -- Flags: 0x04 (Gorilla) | 0x08 (Delta Symbol Dict)
  01 00         -- Table count: 1
  XX XX XX XX   -- Payload length (computed)

Payload:
  Delta Symbol Dictionary:
    00          -- delta_start = 0
    02          -- delta_count = 2
    07 73 65 72 76 65 72 31  -- "server1" (len=7)
    07 73 65 72 76 65 72 32  -- "server2" (len=7)

  Table Block:
    Table Header:
      07 73 65 6E 73 6F 72 73  -- table name "sensors" (len=7)
      02                       -- row_count = 2
      03                       -- column_count = 3

    Schema (full mode):
      00                       -- schema_mode = FULL
      04 68 6F 73 74  09       -- "host" : SYMBOL
      04 74 65 6D 70  07       -- "temp" : DOUBLE
      00              0A       -- "" : TIMESTAMP (designated)

    Column 0 (SYMBOL, global IDs):
      00                       -- row 0: global ID 0
      01                       -- row 1: global ID 1

    Column 1 (DOUBLE, 2 x 8 bytes LE):
      66 66 66 66 66 E6 56 40  -- 91.6
      9A 99 99 99 99 19 57 40  -- 92.4

    Column 2 (TIMESTAMP, Gorilla):
      01                       -- encoding = Gorilla
      [8 bytes LE: t0]
      [8 bytes LE: t1]
```