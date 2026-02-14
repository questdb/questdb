# ILPv4 Binary Wire Protocol Specification

This document describes the ILPv4 (InfluxDB Line Protocol version 4) binary wire format as implemented in QuestDB. This specification is intended to enable alternative implementations to interoperate with QuestDB.

## Table of Contents

1. [Overview](#1-overview)
2. [Byte Ordering](#2-byte-ordering)
3. [Variable-Length Integer Encoding (Varint)](#3-variable-length-integer-encoding-varint)
4. [Message Structure](#4-message-structure)
5. [Table Block Structure](#5-table-block-structure)
6. [Schema Definition](#6-schema-definition)
7. [Column Types](#7-column-types)
8. [Null Bitmap](#8-null-bitmap)
9. [Column Data Encoding](#9-column-data-encoding)
10. [Compression](#10-compression)
11. [Response Format](#11-response-format)
12. [Protocol Limits](#12-protocol-limits)
13. [Examples](#13-examples)

---

## 1. Overview

ILPv4 is a binary protocol for high-performance time-series data ingestion. Key features:

- **Column-oriented encoding**: All values for a column are stored contiguously
- **Batch processing**: Multiple tables and rows per message
- **Optional compression**: LZ4 or Zstd at the message level
- **Gorilla timestamp compression**: Delta-of-delta encoding for timestamps
- **Schema caching**: Reference previously sent schemas by hash

### Magic Bytes

| Magic | Hex Value | Description |
|-------|-----------|-------------|
| `ILP4` | `0x34504C49` | Standard message (little-endian int32) |
| `ILP?` | `0x3F504C49` | Capability request |
| `ILP!` | `0x21504C49` | Capability response |
| `ILP0` | `0x30504C49` | Fallback (server doesn't support v4) |

---

## 2. Byte Ordering

### Little-Endian (default)

Most numeric types use **little-endian** byte ordering:
- Message header fields
- `BYTE`, `SHORT`, `INT`, `LONG`
- `FLOAT`, `DOUBLE`
- `TIMESTAMP`, `TIMESTAMP_NANOS`, `DATE`
- Array dimension lengths and values
- String offset arrays

### Big-Endian (exceptions)

The following types use **big-endian** byte ordering:
- `UUID` (16 bytes)
- `LONG256` (32 bytes)
- `DECIMAL64`, `DECIMAL128`, `DECIMAL256` (unscaled values)

---

## 3. Variable-Length Integer Encoding (Varint)

ILPv4 uses **unsigned LEB128** (Little Endian Base 128) encoding for variable-length integers.

### Encoding Rules

- Values are split into 7-bit groups, LSB first
- Each byte uses the high bit (`0x80`) as a continuation flag
- If high bit is set (1), more bytes follow
- If high bit is clear (0), this is the last byte
- Maximum: 10 bytes for 64-bit values

### Encoding Algorithm

```
while (value & ~0x7F) != 0:
    output_byte((value & 0x7F) | 0x80)
    value >>>= 7
output_byte(value)
```

### Examples

| Value | Encoded Bytes |
|-------|---------------|
| 0 | `0x00` |
| 1 | `0x01` |
| 127 | `0x7F` |
| 128 | `0x80 0x01` |
| 255 | `0xFF 0x01` |
| 300 | `0xAC 0x02` |
| 16384 | `0x80 0x80 0x01` |

### Decoding Algorithm

```
result = 0
shift = 0
do:
    byte = read_byte()
    result |= (byte & 0x7F) << shift
    shift += 7
while (byte & 0x80) != 0
return result
```

---

## 4. Message Structure

### Message Header (12 bytes, fixed)

```
Offset  Size  Type    Field           Description
──────────────────────────────────────────────────────────
0       4     int32   magic           "ILP4" (0x34504C49, LE)
4       1     uint8   version         Protocol version (0x01)
5       1     uint8   flags           Compression/encoding flags
6       2     uint16  table_count     Number of table blocks (LE)
8       4     uint32  payload_length  Payload size in bytes (LE)
```

### Flags Byte

| Bit | Mask | Description |
|-----|------|-------------|
| 0 | `0x01` | LZ4 compression enabled |
| 1 | `0x02` | Zstd compression enabled |
| 2 | `0x04` | Gorilla timestamp encoding enabled |
| 3-7 | | Reserved (must be 0) |

**Constraint**: Bits 0 and 1 are mutually exclusive (cannot have both LZ4 and Zstd).

### Complete Message Layout

```
┌─────────────────────────────────────────┐
│ Message Header (12 bytes)               │
├─────────────────────────────────────────┤
│ Payload (variable)                      │
│   ├─ Table Block 0                      │
│   ├─ Table Block 1                      │
│   └─ ... Table Block N-1                │
└─────────────────────────────────────────┘
```

If compression is enabled, the entire payload is compressed as one unit.

---

## 5. Table Block Structure

Each table block contains data for a single table.

```
┌─────────────────────────────────────────┐
│ Table Header (variable)                 │
├─────────────────────────────────────────┤
│ Schema Section (variable)               │
├─────────────────────────────────────────┤
│ Column Data (variable)                  │
│   ├─ Column 0 data                      │
│   ├─ Column 1 data                      │
│   └─ ... Column N-1 data                │
└─────────────────────────────────────────┘
```

### Table Header

| Field | Type | Description |
|-------|------|-------------|
| name_length | varint | Table name length in bytes |
| name | UTF-8 | Table name (max 127 bytes) |
| row_count | varint | Number of rows in this block |
| column_count | varint | Number of columns |

---

## 6. Schema Definition

### Schema Mode Byte

| Value | Mode | Description |
|-------|------|-------------|
| `0x00` | Full | Complete schema follows inline |
| `0x01` | Reference | Schema hash lookup (8-byte hash) |

### Full Schema Mode (`0x00`)

```
┌─────────────────────────────────────────┐
│ mode_byte: 0x00                         │
├─────────────────────────────────────────┤
│ Column Definition 0                     │
│   ├─ name_length: varint                │
│   ├─ name: UTF-8 bytes                  │
│   └─ type_code: uint8                   │
├─────────────────────────────────────────┤
│ Column Definition 1 ...                 │
└─────────────────────────────────────────┘
```

### Reference Schema Mode (`0x01`)

```
┌─────────────────────────────────────────┐
│ mode_byte: 0x01                         │
├─────────────────────────────────────────┤
│ schema_hash: int64 (LE)                 │
└─────────────────────────────────────────┘
```

The server caches schemas by XXH64 hash. If the hash is not found, the server responds with `STATUS_SCHEMA_REQUIRED (0x02)`.

---

## 7. Column Types

### Type Code Table

| Code | Hex | Type | Size | Endian | Description |
|------|-----|------|------|--------|-------------|
| 1 | `0x01` | BOOLEAN | 1 bit | N/A | Bit-packed boolean |
| 2 | `0x02` | BYTE | 1 | N/A | Signed 8-bit integer |
| 3 | `0x03` | SHORT | 2 | LE | Signed 16-bit integer |
| 4 | `0x04` | INT | 4 | LE | Signed 32-bit integer |
| 5 | `0x05` | LONG | 8 | LE | Signed 64-bit integer |
| 6 | `0x06` | FLOAT | 4 | LE | IEEE 754 single precision |
| 7 | `0x07` | DOUBLE | 8 | LE | IEEE 754 double precision |
| 8 | `0x08` | STRING | var | N/A | Length-prefixed UTF-8 |
| 9 | `0x09` | SYMBOL | var | N/A | Dictionary-encoded string |
| 10 | `0x0A` | TIMESTAMP | 8 | LE | Microseconds since epoch |
| 11 | `0x0B` | DATE | 8 | LE | Milliseconds since epoch |
| 12 | `0x0C` | UUID | 16 | **BE** | RFC 4122 UUID |
| 13 | `0x0D` | LONG256 | 32 | **BE** | 256-bit integer |
| 14 | `0x0E` | GEOHASH | var | N/A | Geospatial hash |
| 15 | `0x0F` | VARCHAR | var | N/A | Length-prefixed UTF-8 (aux storage) |
| 16 | `0x10` | TIMESTAMP_NANOS | 8 | LE | Nanoseconds since epoch |
| 17 | `0x11` | DOUBLE_ARRAY | var | LE | N-dimensional double array |
| 18 | `0x12` | LONG_ARRAY | var | LE | N-dimensional long array |
| 19 | `0x13` | DECIMAL64 | 8 | **BE** | Decimal (18 digits precision) |
| 20 | `0x14` | DECIMAL128 | 16 | **BE** | Decimal (38 digits precision) |
| 21 | `0x15` | DECIMAL256 | 32 | **BE** | Decimal (77 digits precision) |

> **Note**: Type codes 0x11-0x15 (arrays and decimals) are defined in the protocol constants but may have limited support in current implementations. Check server capabilities before using these types.

### Nullable Flag

The high bit (`0x80`) of the type code indicates a nullable column:

```
type_code = base_type | 0x80  // nullable
type_code = base_type         // not nullable
```

To extract the base type: `base_type = type_code & 0x7F`

---

## 8. Null Bitmap

For nullable columns, a null bitmap precedes the column data.

### Format

- **Size**: `ceil(row_count / 8)` bytes
- **Bit order**: LSB first within each byte
- **Semantics**: bit = 1 means row is NULL, bit = 0 means row has value

### Layout

```
Byte 0:  [row7][row6][row5][row4][row3][row2][row1][row0]
Byte 1:  [row15][row14][row13][row12][row11][row10][row9][row8]
...
```

### Example

For 10 rows where rows 0, 2, and 9 are null:

```
Byte 0: 0b00000101 = 0x05  (bits 0 and 2 set)
Byte 1: 0b00000010 = 0x02  (bit 1 set = row 9)
```

### Accessing Null Status

```
byte_index = row_index / 8
bit_index = row_index % 8
is_null = (bitmap[byte_index] & (1 << bit_index)) != 0
```

---

## 9. Column Data Encoding

### Fixed-Width Types

For BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, TIMESTAMP, TIMESTAMP_NANOS, DATE, UUID, LONG256:

```
┌─────────────────────────────────────────┐
│ [Null bitmap if nullable]               │
├─────────────────────────────────────────┤
│ Values (only non-null rows)             │
│   value[0], value[1], ... value[N-1]    │
│   where N = row_count - null_count      │
└─────────────────────────────────────────┘
```

**Important**: Only non-null values are stored. The null bitmap indicates which row indices are null.

### Boolean Type (`0x01`)

Booleans are bit-packed, 8 values per byte, LSB first:

```
┌─────────────────────────────────────────┐
│ [Null bitmap if nullable]               │
├─────────────────────────────────────────┤
│ Packed boolean values                   │
│   Size: ceil((row_count - null_count) / 8) bytes │
└─────────────────────────────────────────┘
```

### String Type (`0x08`)

```
┌─────────────────────────────────────────┐
│ [Null bitmap if nullable]               │
├─────────────────────────────────────────┤
│ Offset array: (value_count + 1) × 4 bytes │
│   offset[0] = 0                         │
│   offset[i+1] = end of string[i]        │
├─────────────────────────────────────────┤
│ String data: concatenated UTF-8 bytes   │
└─────────────────────────────────────────┘
```

- `value_count = row_count - null_count`
- Offsets are uint32, little-endian
- String `i` spans bytes `[offset[i], offset[i+1])`

### Symbol Type (`0x09`)

Dictionary-encoded strings for low-cardinality columns:

```
┌─────────────────────────────────────────┐
│ [Null bitmap if nullable]               │
├─────────────────────────────────────────┤
│ dictionary_size: varint                 │
├─────────────────────────────────────────┤
│ Dictionary entries:                     │
│   For each entry:                       │
│     entry_length: varint                │
│     entry_data: UTF-8 bytes             │
├─────────────────────────────────────────┤
│ Value indices:                          │
│   For each non-null row:                │
│     dict_index: varint                  │
└─────────────────────────────────────────┘
```

- Dictionary indices are 0-based
- When a null bitmap is present, only non-null rows have indices written
- Without a null bitmap, `Long.MAX_VALUE` as an index indicates null

### Timestamp Type (`0x0A`)

Timestamps support two encoding modes indicated by a flag byte:

| Flag | Mode | Description |
|------|------|-------------|
| `0x00` | Uncompressed | Array of int64 values (only non-null values) |
| `0x01` | Gorilla | Delta-of-delta compressed |

**Uncompressed mode (`0x00`):**

```
┌─────────────────────────────────────────┐
│ [Null bitmap if nullable]               │
├─────────────────────────────────────────┤
│ encoding_flag: uint8 (0x00)             │
├─────────────────────────────────────────┤
│ Timestamp values (non-null only):       │
│   value_count × int64 (LE)              │
└─────────────────────────────────────────┘
```

**Gorilla mode (`0x01`):**

```
┌─────────────────────────────────────────┐
│ [Null bitmap if nullable]               │
├─────────────────────────────────────────┤
│ encoding_flag: uint8 (0x01)             │
├─────────────────────────────────────────┤
│ first_timestamp: int64 (LE)             │
├─────────────────────────────────────────┤
│ second_timestamp: int64 (LE)            │
├─────────────────────────────────────────┤
│ Bit-packed delta-of-deltas:             │
│   For timestamps 3..N                   │
└─────────────────────────────────────────┘
```

#### Gorilla Delta-of-Delta Encoding

```
D = (t[n] - t[n-1]) - (t[n-1] - t[n-2])

Encoding:
  D == 0:              '0'                    (1 bit)
  D in [-63, 64]:      '10' + 7-bit signed    (9 bits)
  D in [-255, 256]:    '110' + 9-bit signed   (12 bits)
  D in [-2047, 2048]:  '1110' + 12-bit signed (16 bits)
  otherwise:           '1111' + 32-bit signed (36 bits)
```

### Array Types (`0x11`, `0x12`)

N-dimensional arrays of DOUBLE or LONG:

```
┌─────────────────────────────────────────┐
│ element_type: uint8                     │
├─────────────────────────────────────────┤
│ n_dims: uint8 (1-255)                   │
├─────────────────────────────────────────┤
│ shape: n_dims × int32 (LE)              │
│   dim_0_length, dim_1_length, ...       │
├─────────────────────────────────────────┤
│ Flattened values (row-major order)      │
│   total = product(shape) × elem_size    │
└─────────────────────────────────────────┘
```

### Decimal Types (`0x13`, `0x14`, `0x15`)

> **Implementation Status**: Decimal types are defined in the protocol but may have limited support. The scale handling described below is the intended design; check actual implementation for current support.

```
┌─────────────────────────────────────────┐
│ [Null bitmap if nullable]               │
├─────────────────────────────────────────┤
│ Unscaled values (big-endian):           │
│   DECIMAL64:  8 bytes × value_count     │
│   DECIMAL128: 16 bytes × value_count    │
│   DECIMAL256: 32 bytes × value_count    │
└─────────────────────────────────────────┘
```

Decimal values are stored as big-endian two's complement integers. The scale (number of decimal places) must be agreed upon out-of-band or via table schema.

### GeoHash Type (`0x0E`)

```
┌─────────────────────────────────────────┐
│ [Null bitmap if nullable]               │
├─────────────────────────────────────────┤
│ precision_bits: varint (1-60)           │
├─────────────────────────────────────────┤
│ Packed geohash values:                  │
│   bytes_per_value = ceil(precision/8)   │
│   total = bytes_per_value × row_count   │
└─────────────────────────────────────────┘
```

> **Note**: Unlike other types, GeoHash stores values for ALL rows (including nulls). The null bitmap only indicates which values should be treated as null when reading.

---

## 10. Compression

### LZ4 Compression (flag `0x01`)

When LZ4 flag is set:
- The entire payload after the 12-byte header is LZ4 compressed
- Use standard LZ4 frame format
- Decompress before parsing table blocks

### Zstd Compression (flag `0x02`)

When Zstd flag is set:
- The entire payload after the 12-byte header is Zstd compressed
- Use standard Zstd frame format
- Decompress before parsing table blocks

---

## 11. Response Format

### Response Header

```
┌─────────────────────────────────────────┐
│ status_code: uint8                      │
├─────────────────────────────────────────┤
│ [Error payload if status != 0x00]       │
└─────────────────────────────────────────┘
```

### Status Codes

| Code | Hex | Name | Description | Retriable |
|------|-----|------|-------------|-----------|
| 0 | `0x00` | OK | Batch accepted | N/A |
| 1 | `0x01` | PARTIAL | Some rows failed | No |
| 2 | `0x02` | SCHEMA_REQUIRED | Schema hash not found | Yes |
| 3 | `0x03` | SCHEMA_MISMATCH | Column type incompatible | No |
| 4 | `0x04` | TABLE_NOT_FOUND | Table doesn't exist | No |
| 5 | `0x05` | PARSE_ERROR | Malformed message | No |
| 6 | `0x06` | INTERNAL_ERROR | Server error | No |
| 7 | `0x07` | OVERLOADED | Back-pressure | Yes |

### Error Payload (status != 0x00)

For non-partial errors:
```
error_message_length: varint
error_message: UTF-8 bytes
```

For PARTIAL status (0x01):
```
failed_table_count: varint
For each failed table:
    table_index: varint
    error_code: uint8
    error_message_length: varint
    error_message: UTF-8 bytes
```

---

## 12. Protocol Limits

| Limit | Default Value |
|-------|---------------|
| Maximum batch size | 16 MB |
| Maximum tables per batch | 256 |
| Maximum rows per table | 1,000,000 |
| Maximum columns per table | 2,048 |
| Maximum table name length | 127 bytes (UTF-8) |
| Maximum column name length | 127 bytes (UTF-8) |
| Maximum string length | 1 MB |
| Maximum in-flight batches | 4 |
| Initial receive buffer | 64 KB |

---

## 13. Examples

### Example 1: Simple Message with One Table

Table: `sensors`, 2 rows, 3 columns: `id` (LONG), `value` (DOUBLE), `ts` (TIMESTAMP)

**Wire format (hex):**

```
# Header (12 bytes)
49 4C 50 34  # Magic: "ILP4" (LE)
01           # Version: 1
00           # Flags: none
01 00        # Table count: 1 (LE uint16)
XX XX XX XX  # Payload length (LE uint32)

# Table Block
07           # Table name length: 7
73 65 6E 73 6F 72 73  # "sensors" UTF-8
02           # Row count: 2
03           # Column count: 3

# Schema (full mode)
00           # Schema mode: full

# Column 0: id
02           # Name length: 2
69 64        # "id" UTF-8
05           # Type: LONG (not nullable)

# Column 1: value
05           # Name length: 5
76 61 6C 75 65  # "value" UTF-8
07           # Type: DOUBLE (not nullable)

# Column 2: ts
02           # Name length: 2
74 73        # "ts" UTF-8
0A           # Type: TIMESTAMP (not nullable)

# Column 0 data (LONG, 2 values, 16 bytes)
01 00 00 00 00 00 00 00  # id=1
02 00 00 00 00 00 00 00  # id=2

# Column 1 data (DOUBLE, 2 values, 16 bytes)
CD CC CC CC CC CC F4 3F  # value=1.3
9A 99 99 99 99 99 01 40  # value=2.2

# Column 2 data (TIMESTAMP, 2 values, 17 bytes)
00                       # encoding_flag: 0x00 (uncompressed)
00 E4 0B 54 02 00 00 00  # ts=10000000000 microseconds
80 1A 06 00 00 00 00 00  # ts=400000 microseconds
```

### Example 2: Nullable Column

Table with nullable STRING column, 4 rows where row 1 is null:

```
# Null bitmap for 4 rows where row 1 is null
02           # 0b00000010 - bit 1 set

# Offset array (3 non-null values = 4 offsets)
00 00 00 00  # offset[0] = 0  (start of "foo")
03 00 00 00  # offset[1] = 3  (end of "foo", start of "bar")
06 00 00 00  # offset[2] = 6  (end of "bar", start of "baz")
09 00 00 00  # offset[3] = 9  (end of "baz")

# String data (concatenated UTF-8)
66 6F 6F     # "foo" (row 0)
62 61 72     # "bar" (row 2, since row 1 is null)
62 61 7A     # "baz" (row 3)
```

### Example 3: Symbol Column

3 rows with values: "us", "eu", "us"

```
# Dictionary
02           # Dictionary size: 2 entries

02           # Entry 0 length: 2
75 73        # "us"

02           # Entry 1 length: 2
65 75        # "eu"

# Value indices
00           # Row 0: index 0 ("us")
01           # Row 1: index 1 ("eu")
00           # Row 2: index 0 ("us")
```

---

## Reference Implementation

The authoritative implementation is in QuestDB's Java codebase:

- `core/src/main/java/io/questdb/cutlass/http/qwp/QwpConstants.java` - Protocol constants
- `core/src/main/java/io/questdb/cutlass/http/qwp/QwpMessageHeader.java` - Header parsing
- `core/src/main/java/io/questdb/cutlass/http/qwp/QwpTableHeader.java` - Table header parsing
- `core/src/main/java/io/questdb/cutlass/http/qwp/QwpVarint.java` - Varint encoding/decoding
- `core/src/main/java/io/questdb/cutlass/http/qwp/QwpNullBitmap.java` - Null bitmap utilities
- `core/src/main/java/io/questdb/cutlass/http/qwp/Qwp*Decoder.java` - Type-specific decoders
- `core/src/main/java/io/questdb/cutlass/line/tcp/ArrayBinaryFormatParser.java` - Array parsing

---

## Version History

| Version | Description |
|---------|-------------|
| 1 (`0x01`) | Initial binary protocol release |
