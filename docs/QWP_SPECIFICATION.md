# QuestWire Protocol (QWP) Specification

QuestWire Protocol (QWP) is a columnar binary ingestion protocol designed for
high-throughput, zero-GC data streaming into QuestDB. This specification is
intended to enable alternative implementations to interoperate with QuestDB.

## Table of Contents

1. [Overview](#1-overview)
2. [Transport](#2-transport)
3. [Version Negotiation](#3-version-negotiation)
4. [Byte Ordering](#4-byte-ordering)
5. [Variable-Length Integer Encoding (Varint)](#5-variable-length-integer-encoding-varint)
6. [ZigZag Encoding](#6-zigzag-encoding)
7. [Message Structure](#7-message-structure)
8. [Table Block Structure](#8-table-block-structure)
9. [Schema Definition](#9-schema-definition)
10. [Column Types](#10-column-types)
11. [Null Handling](#11-null-handling)
12. [Column Data Encoding](#12-column-data-encoding)
13. [Response Format](#13-response-format)
14. [Protocol Limits](#14-protocol-limits)
15. [Client Operation](#15-client-operation)
16. [Examples](#16-examples)
17. [Reference Implementation](#17-reference-implementation)
18. [Version History](#18-version-history)

## 1. Overview

QWP is a binary protocol for high-performance time-series data ingestion. Key
features:

- **Column-oriented encoding**: All values for a column are stored contiguously
- **Batch processing**: Multiple tables and rows per message
- **Gorilla timestamp compression**: Delta-of-delta encoding for timestamps
- **Schema references**: Reference previously sent schemas by numeric ID

### Magic Bytes

Every QWP message begins with a 4-byte magic value identifying the protocol.

| Magic  | Hex Value      | Description           |
|--------|----------------|-----------------------|
| `QWP1` | `0x31505751`   | Standard data message |

Version negotiation is handled entirely via HTTP upgrade headers (see §3), not
via binary magics.

## 2. Transport

### WebSocket

QWP uses RFC 6455 WebSocket binary frames. The client initiates an HTTP GET
request to either `/write/v4` or `/api/v4/write` with standard WebSocket upgrade
headers. After the 101 Switching Protocols handshake, all communication uses
binary frames.

### UDP

UDP has no HTTP upgrade handshake. Each datagram is self-describing: the server
inspects the version byte in the message header and processes or drops the
datagram accordingly.

## 3. Version Negotiation

When QWP operates over WebSocket, the client and server negotiate the protocol
version during the HTTP upgrade handshake.

### Client Request Headers

| Header              | Required | Description                                                                         |
|---------------------|----------|-------------------------------------------------------------------------------------|
| `X-QWP-Max-Version` | No       | Maximum QWP version the client supports (positive integer). Defaults to 1 if absent.|
| `X-QWP-Client-Id`   | No       | Free-form client identifier (e.g., `java/1.0.2`, `python/0.9.1`).                  |

### Server Response Header

| Header          | Description                                         |
|-----------------|-----------------------------------------------------|
| `X-QWP-Version` | The QWP version selected for this connection.       |

The server selects the version as `min(clientMax, serverMax)`. The selected
version is never higher than either side's maximum. The server may also consider
the `X-QWP-Client-Id` when selecting the version.

### Connection-Level Contract

All QWP messages on a connection must use the negotiated version in the version
byte (offset 4) of the message header. The server validates every incoming
message against the negotiated version and rejects any message whose version
byte does not match with a parse error.

## 4. Byte Ordering

All multi-byte numeric values are **little-endian**. Variable-length integers
use unsigned LEB128 (see §5).

## 5. Variable-Length Integer Encoding (Varint)

QWP uses **unsigned LEB128** (Little Endian Base 128) encoding for
variable-length integers.

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

### Examples

| Value | Encoded Bytes      |
|-------|--------------------|
| 0     | `0x00`             |
| 1     | `0x01`             |
| 127   | `0x7F`             |
| 128   | `0x80 0x01`        |
| 255   | `0xFF 0x01`        |
| 300   | `0xAC 0x02`        |
| 16384 | `0x80 0x80 0x01`   |

## 6. ZigZag Encoding

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

## 7. Message Structure

### Message Header (12 bytes, fixed)

```
Offset  Size  Type    Field           Description
──────────────────────────────────────────────────────────
0       4     int32   magic           "QWP1" (0x31505751)
4       1     uint8   version         Protocol version (0x01)
5       1     uint8   flags           Encoding flags
6       2     uint16  table_count     Number of table blocks
8       4     uint32  payload_length  Payload size in bytes
```

**Total message size** = 12 + payload length.

### Flags Byte

| Bit | Mask   | Name                     | Description                                           |
|-----|--------|--------------------------|-------------------------------------------------------|
| 0-1 |        |                          | Reserved (must be 0)                                  |
| 2   | `0x04` | `FLAG_GORILLA`           | Gorilla delta-of-delta encoding for timestamp columns |
| 3   | `0x08` | `FLAG_DELTA_SYMBOL_DICT` | Delta symbol dictionary mode enabled                  |
| 4-7 |        |                          | Reserved (must be 0)                                  |

### Complete Message Layout

```
┌─────────────────────────────────────────┐
│ Message Header (12 bytes)               │
├─────────────────────────────────────────┤
│ Payload (variable)                      │
│   ├─ [Delta Symbol Dictionary] (if 0x08)│
│   ├─ Table Block 0                      │
│   ├─ Table Block 1                      │
│   └─ ... Table Block N-1                │
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
across batches for the lifetime of the connection. Symbol columns in delta mode
contain varint-encoded global IDs instead of per-column dictionaries.

## 8. Table Block Structure

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

| Field        | Type   | Description                   |
|--------------|--------|-------------------------------|
| name_length  | varint | Table name length in bytes    |
| name         | UTF-8  | Table name (max 127 bytes)    |
| row_count    | varint | Number of rows in this block  |
| column_count | varint | Number of columns             |

## 9. Schema Definition

### Schema Mode Byte

| Value  | Mode      | Description                                    |
|--------|-----------|------------------------------------------------|
| `0x00` | Full      | Schema ID + complete column definitions inline |
| `0x01` | Reference | Schema ID only (lookup from registry)          |

### Full Schema Mode (`0x00`)

Sent the first time a table's schema appears on a connection, or whenever the
column set changes.

```
┌─────────────────────────────────────────┐
│ mode_byte: 0x00                         │
├─────────────────────────────────────────┤
│ schema_id: varint                       │
├─────────────────────────────────────────┤
│ Column Definition 0                     │
│   ├─ name_length: varint                │
│   ├─ name: UTF-8 bytes                  │
│   └─ type_code: uint8                   │
├─────────────────────────────────────────┤
│ Column Definition 1 ...                 │
└─────────────────────────────────────────┘
```

Schema IDs are non-negative integers assigned by the client and scoped to the
lifetime of a single connection. They are global across all tables on the
connection (not per-table). Clients typically assign them sequentially starting
at 0, but the server does not require any particular ordering.

The `type_code` byte contains the column type (0x01 through 0x16).

A column with an **empty name** (length 0) and type TIMESTAMP denotes the
designated timestamp column.

### Reference Schema Mode (`0x01`)

Used for subsequent batches when the server has already registered the schema.

```
┌─────────────────────────────────────────┐
│ mode_byte: 0x01                         │
├─────────────────────────────────────────┤
│ schema_id: varint                       │
└─────────────────────────────────────────┘
```

The server looks up the schema by its ID in the per-connection schema registry.
Full-mode schemas may arrive in any order and may re-register an existing ID;
the server accepts any ID within the per-connection schema-ID limit.

## 10. Column Types

### Type Code Table

| Code | Hex    | Type            | Size    | Description                        |
|------|--------|-----------------|---------|------------------------------------|
| 1    | `0x01` | BOOLEAN         | 1 bit   | Bit-packed boolean                 |
| 2    | `0x02` | BYTE            | 1       | Signed 8-bit integer               |
| 3    | `0x03` | SHORT           | 2       | Signed 16-bit integer              |
| 4    | `0x04` | INT             | 4       | Signed 32-bit integer              |
| 5    | `0x05` | LONG            | 8       | Signed 64-bit integer              |
| 6    | `0x06` | FLOAT           | 4       | IEEE 754 single precision          |
| 7    | `0x07` | DOUBLE          | 8       | IEEE 754 double precision          |
| 9    | `0x09` | SYMBOL          | var     | Dictionary-encoded string          |
| 10   | `0x0A` | TIMESTAMP       | 8       | Microseconds since epoch           |
| 11   | `0x0B` | DATE            | 8       | Milliseconds since epoch           |
| 12   | `0x0C` | UUID            | 16      | RFC 4122 UUID                      |
| 13   | `0x0D` | LONG256         | 32      | 256-bit integer                    |
| 14   | `0x0E` | GEOHASH         | var     | Geospatial hash                    |
| 15   | `0x0F` | VARCHAR         | var     | Length-prefixed UTF-8 (aux storage)|
| 16   | `0x10` | TIMESTAMP_NANOS | 8       | Nanoseconds since epoch            |
| 17   | `0x11` | DOUBLE_ARRAY    | var     | N-dimensional double array         |
| 18   | `0x12` | LONG_ARRAY      | var     | N-dimensional long array           |
| 19   | `0x13` | DECIMAL64       | 8       | Decimal (18 digits precision)      |
| 20   | `0x14` | DECIMAL128      | 16      | Decimal (38 digits precision)      |
| 21   | `0x15` | DECIMAL256      | 32      | Decimal (77 digits precision)      |
| 22   | `0x16` | CHAR            | 2       | Single UTF-16 code unit            |

Code `0x08` is unassigned. It was previously STRING, which has been removed;
senders should use VARCHAR (`0x0F`) for text columns.

TIMESTAMP and TIMESTAMP_NANOS may use Gorilla encoding when `FLAG_GORILLA` is
set (see [Column Data Encoding](#12-column-data-encoding)).

## 11. Null Handling

Each column's data section begins with a 1-byte **null flag**. The flag tells
the decoder how to interpret what follows:

- `0x00` -- no bitmap follows. The column data contains one value per row
  (`row_count` values total). If the column has null rows, they are represented
  by a type-specific sentinel encoded in place.
- Any nonzero value -- a null bitmap follows immediately after the flag byte,
  and the column data contains only `value_count = row_count - null_count`
  non-null values, densely packed. The bitmap identifies which row indices are
  null.

The choice between these two strategies is made **per column** by the encoder,
and the decoder must support both. Sentinel mode avoids the per-row bitmap
overhead, and bitmap mode avoids writing any data for null rows. The wider the
column element, the more likely it is to get a more compact encoding using the
null bitmap.

Sentinel mode requires the type to have a dedicated null representation
available; it is not applicable to types whose full value range is meaningful
payload (e.g. VARCHAR, SYMBOL).

### Bitmap Format

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

### Column Data Layout (all types)

```
┌──────────────────────────────────────────────────────────────┐
│ null_flag:     uint8     0 = no bitmap, nonzero = bitmap     │
│ [null bitmap:  ceil(row_count/8) bytes if flag != 0]         │
│ Column values:                                               │
│   - flag == 0 : row_count entries (null rows = sentinel)     │
│   - flag != 0 : value_count non-null entries, densely packed │
│                 (value_count = row_count - null_count)       │
└──────────────────────────────────────────────────────────────┘
```

### Reference Implementation Null Strategy

The reference Java WebSocket client and the Go client make the same per-column
choice:

| Strategy | Types                               |
|----------|-------------------------------------|
| Sentinel | BOOLEAN, BYTE, SHORT, CHAR, GEOHASH |
| Bitmap   | INT, LONG, FLOAT, DOUBLE, VARCHAR, SYMBOL, TIMESTAMP, TIMESTAMP_NANOS, DATE, UUID, LONG256, DECIMAL64, DECIMAL128, DECIMAL256, DOUBLE_ARRAY, LONG_ARRAY |

The reference Java UDP client additionally uses sentinel mode for LONG and
DOUBLE (encoding null rows as `Long.MIN_VALUE` and `NaN` respectively).

Alternative implementations are free to make different per-column choices, as
long as the `null_flag` value accurately describes the data that follows. A
column with no null rows produces identical output under either strategy
(`null_flag = 0`, `row_count` values).

#### Reference Sentinel Values

When the reference implementations emit sentinel mode (`null_flag = 0`), null
rows are encoded as:

| Type    | Sentinel                                                                                                        |
|---------|-----------------------------------------------------------------------------------------------------------------|
| BOOLEAN | bit `0` (false)                                                                                                 |
| BYTE    | `0x00`                                                                                                          |
| SHORT   | `0x0000`                                                                                                        |
| CHAR    | `0x0000`                                                                                                        |
| GEOHASH | all-ones: int64 `-1` (`0xFFFF…FFFF`), truncated to the column's per-value byte width `ceil(precision_bits / 8)` |

## 12. Column Data Encoding

### Fixed-Width Types

For BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DATE, CHAR: values are written as
contiguous arrays of their respective sizes.

```
┌────────────────────────────────────────────────────┐
│ [Null flag + bitmap (see §11)]                     │
├────────────────────────────────────────────────────┤
│ Values:                                            │
│   value[0], value[1], ... value[N-1]               │
│   where N = row_count                if flag == 0  │
│        or N = row_count - null_count if flag != 0  │
└────────────────────────────────────────────────────┘
```

The number of values depends on the null strategy chosen for the column (see
§11). In sentinel mode (`null_flag == 0`) all `row_count` values are written,
with the type's sentinel marking null rows. In bitmap mode (`null_flag != 0`)
only the non-null values are written, densely packed.

The reference implementation uses sentinel mode for BYTE, SHORT, and CHAR, and
bitmap mode for INT, LONG, FLOAT, DOUBLE, and DATE.

### Boolean Type (`0x01`)

Values are bit-packed, 8 per byte, LSB-first. `ceil(N/8)` bytes are written,
where `N = row_count` in sentinel mode (`null_flag == 0`) or
`N = row_count - null_count` in bitmap mode. The reference implementation uses
sentinel mode for BOOLEAN: null rows appear as bit `0` (false).

```
Byte layout for values [true, false, true, true, false, false, false, true]:
  0b10001101 = 0x8D
```

### VARCHAR Type (`0x0F`)

```
┌──────────────────────────────────────────┐
│ [Null flag + bitmap (see §11)]           │
├──────────────────────────────────────────┤
│ Offset array: (value_count + 1) x 4 bytes│
│   offset[0] = 0                          │
│   offset[i+1] = end of string[i]         │
├──────────────────────────────────────────┤
│ String data: concatenated UTF-8 bytes    │
└──────────────────────────────────────────┘
```

- `value_count = row_count - null_count`
- Offsets are uint32
- String `i` spans bytes `[offset[i], offset[i+1])`

### Symbol Type (`0x09`)

Dictionary-encoded strings for low-cardinality columns.

#### Per-Table Dictionary Mode (UDP)

```
┌─────────────────────────────────────────┐
│ [Null flag + bitmap (see §11)]          │
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

Per-Table Dictionary Mode is used by UDP because datagrams cannot rely on a
connection-scoped dictionary persisting across messages.

#### Global Delta Dictionary Mode (WebSocket, `FLAG_DELTA_SYMBOL_DICT`)

When the delta symbol dictionary flag is set, symbol columns use global integer
IDs instead of per-table dictionaries. The dictionary entries are sent in the
message-level delta dictionary section (see [§7](#7-message-structure)). Column
data consists of varint-encoded global IDs only.

WebSocket clients set `FLAG_DELTA_SYMBOL_DICT` on every message and use this
mode exclusively.

```
┌───────────────────────────────────────────┐
│ For each non-null row:                    │
│   global_id:   varint   Global symbol ID  │
└───────────────────────────────────────────┘
```

### Timestamp Type (`0x0A`, `0x10`)

When `FLAG_GORILLA` (0x04) is set in the message header flags, timestamp columns
include a 1-byte encoding flag after the null bitmap. When `FLAG_GORILLA` is
**not** set, there is no encoding flag -- timestamps are written as plain
uncompressed int64 arrays.

#### Without FLAG_GORILLA (no encoding flag)

```
┌─────────────────────────────────────────┐
│ [Null flag + bitmap (see §11)]          │
├─────────────────────────────────────────┤
│ Timestamp values (non-null only):       │
│   value_count x int64                   │
└─────────────────────────────────────────┘
```

#### With FLAG_GORILLA (encoding flag present)

| Flag   | Mode         | Description                                    |
|--------|--------------|------------------------------------------------|
| `0x00` | Uncompressed | Array of int64 values (only non-null values)   |
| `0x01` | Gorilla      | Delta-of-delta compressed                      |

**Uncompressed mode (`0x00`):**

```
┌─────────────────────────────────────────┐
│ [Null flag + bitmap (see §11)]          │
├─────────────────────────────────────────┤
│ encoding_flag: uint8 (0x00)             │
├─────────────────────────────────────────┤
│ Timestamp values (non-null only):       │
│   value_count x int64                   │
└─────────────────────────────────────────┘
```

**Gorilla mode (`0x01`):**

```
┌─────────────────────────────────────────┐
│ [Null flag + bitmap (see §11)]          │
├─────────────────────────────────────────┤
│ encoding_flag: uint8 (0x01)             │
├─────────────────────────────────────────┤
│ first_timestamp: int64                  │
├─────────────────────────────────────────┤
│ second_timestamp: int64                 │
├─────────────────────────────────────────┤
│ Bit-packed delta-of-deltas:             │
│   For timestamps 3..N                   │
└─────────────────────────────────────────┘
```

#### Gorilla Delta-of-Delta Encoding

```
delta[i] = t[i] - t[i-1]
DoD[i]   = delta[i] - delta[i-1]
```

Encoding buckets (bits are written LSB-first):

| Condition                  | Prefix  | Value Bits  | Total Bits |
|----------------------------|---------|-------------|------------|
| DoD == 0                   | `0`     | 0           | 1          |
| DoD in [-64, 63]           | `10`    | 7 (signed)  | 9          |
| DoD in [-256, 255]         | `110`   | 9 (signed)  | 12         |
| DoD in [-2048, 2047]       | `1110`  | 12 (signed) | 16         |
| Otherwise                  | `1111`  | 32 (signed) | 36         |

The bit stream is padded to a byte boundary at the end. If any DoD value exceeds
the 32-bit signed integer range, the encoder falls back to uncompressed mode.

### UUID Type (`0x0C`)

16 bytes per value: 8 bytes low, then 8 bytes high.

### LONG256 Type (`0x0D`)

32 bytes per value: four int64 values, least significant first.

### GeoHash Type (`0x0E`)

```
┌──────────────────────────────────────────────────┐
│ [Null flag + bitmap (see §11)]                   │
├──────────────────────────────────────────────────┤
│ precision_bits: varint (1-60)                    │
├──────────────────────────────────────────────────┤
│ Packed geohash values:                           │
│   bytes_per_value = ceil(precision/8)            │
│   total = bytes_per_value x N                    │
│     where N = row_count              if flag == 0│
│          or N = row_count - null_count if flag != 0│
└──────────────────────────────────────────────────┘
```

The reference implementation uses sentinel mode for GEOHASH: null rows are
encoded as all-ones (int64 `-1`) truncated to `bytes_per_value`.

### Array Types (`0x11`, `0x12`)

N-dimensional arrays of DOUBLE or LONG, row-major order:

```
┌────────────────────────────────────────--------------─┐
│ For each row:                                         │
│   n_dims:      uint8          Number of dimensions    │
│   dim_lengths: n_dims x int32      Length per dim     │
│   values:      product(dims) x element                │
│                (float64 for DOUBLE_ARRAY,             │
│                 int64 for LONG_ARRAY)                 │
└───────────────────────────────────────--------------──┘
```

### Decimal Types (`0x13`, `0x14`, `0x15`)

Decimal values are stored as two's complement integers. The scale (number of
decimal places) is a 1-byte prefix in the column data section, shared by all
values in the column.

```
┌─────────────────────────────────────────┐
│ [Null flag + bitmap (see §11)]          │
├─────────────────────────────────────────┤
│ scale: uint8                            │
├─────────────────────────────────────────┤
│ Unscaled values:                        │
│   DECIMAL64:  8 bytes x value_count     │
│   DECIMAL128: 16 bytes x value_count    │
│   DECIMAL256: 32 bytes x value_count    │
└─────────────────────────────────────────┘
```

| Type        | Value Size | Precision  |
|-------------|------------|------------|
| DECIMAL64   | 8 bytes    | 18 digits  |
| DECIMAL128  | 16 bytes   | 38 digits  |
| DECIMAL256  | 32 bytes   | 77 digits  |

## 13. Response Format

Every response includes a 1-byte status code and an 8-byte sequence number that
correlates the response with the original request.

### OK Response (9 bytes)

```
┌──────────────────────────────────────────────────────┐
│ status:    uint8   (0x00)                            │
│ sequence:  int64          Request sequence number    │
└──────────────────────────────────────────────────────┘
```

### Error Response (11 + msg_len bytes)

```
┌──────────────────────────────────────────────────────┐
│ status:    uint8          Status code                │
│ sequence:  int64          Request sequence number    │
│ msg_len:   uint16         Error message length       │
│ msg_bytes: bytes          UTF-8 error message        │
└──────────────────────────────────────────────────────┘
```

### Status Codes

| Code | Hex    | Name            | Description                                          |
|------|--------|-----------------|------------------------------------------------------|
| 0    | `0x00` | OK              | Batch accepted                                       |
| 3    | `0x03` | SCHEMA_MISMATCH | Column type incompatible with existing table         |
| 5    | `0x05` | PARSE_ERROR     | Malformed message                                    |
| 6    | `0x06` | INTERNAL_ERROR  | Server-side error                                    |
| 8    | `0x08` | SECURITY_ERROR  | Authorization failure                                |
| 9    | `0x09` | WRITE_ERROR     | Write failure (e.g., table not accepting writes)     |

## 14. Protocol Limits

| Limit                         | Default Value |
|-------------------------------|---------------|
| Max batch size                | 16 MB         |
| Max tables per connection     | 10,000        |
| Max rows per table            | 1,000,000     |
| Max columns per table         | 2,048         |
| Max table name length         | 127 bytes     |
| Max column name length        | 127 bytes     |
| Max in-flight batches         | 128           |
| Max symbol dictionary entries | 1,000,000     |

The header's `table_count` field is a uint16, so the protocol ceiling for tables
per message is 65,535 regardless of the configured limit. Individual string
values have no dedicated length limit; they are bounded only by the max batch
size.

The symbol dictionary limit applies per column in Per-Table Dictionary Mode and
per connection in Global Delta Dictionary Mode (see §12). Exceeding it causes
the server to reject the message with `PARSE_ERROR`.

## 15. Client Operation

### Double-Buffered Async I/O

The client uses double-buffered microbatches:

1. The user thread writes rows to the **active** buffer.
2. When a buffer reaches its threshold (row count, byte size, or age), the
   client seals it and enqueues it for sending.
3. A dedicated I/O thread sends batches over the WebSocket.
4. The client swaps to the other buffer so writing can continue without
   blocking.

### Auto-Flush Triggers

| Trigger              | Default    |
|----------------------|------------|
| Row count            | 1,000 rows |
| Byte size            | disabled   |
| Time since first row | 100 ms     |

### Schema Registry

- First batch for a given table: full schema mode (0x00) with a new schema ID.
- Subsequent batches with an unchanged column set: schema reference mode (0x01)
  with the same ID.
- When a table gains a column, the client assigns a new schema ID and sends it
  in full mode.
- Schema IDs are global per connection, not per table; the server registers them
  in a per-connection registry.
- On reconnect both sides reset: the client reassigns IDs from 0 and the server
  clears its registry.

### Symbol Dictionary Lifecycle

- The client maintains a global symbol dictionary across all tables/columns.
- Symbol IDs are assigned sequentially starting from 0.
- Each batch sends only the **delta** (newly added symbols since the last
  batch).
- The server accumulates these deltas for the lifetime of the connection.
- Upon connection loss, both sides reset the dictionary.

## 16. Examples

### Example 1: Simple Message with One Table

Table: `sensors`, 2 rows, 3 columns: `id` (LONG), `value` (DOUBLE), `ts`
(TIMESTAMP). No nulls.

```
# Header (12 bytes)
51 57 50 31  # Magic: "QWP1"
01           # Version: 1
00           # Flags: none
01 00        # Table count: 1
XX XX XX XX  # Payload length

# Table Block
07           # Table name length: 7
73 65 6E 73 6F 72 73  # "sensors" UTF-8
02           # Row count: 2
03           # Column count: 3

# Schema (full mode)
00           # Schema mode: full
00           # Schema ID: 0

# Column 0: id
02           # Name length: 2
69 64        # "id" UTF-8
05           # Type: LONG

# Column 1: value
05           # Name length: 5
76 61 6C 75 65  # "value" UTF-8
07           # Type: DOUBLE

# Column 2: ts
02           # Name length: 2
74 73        # "ts" UTF-8
0A           # Type: TIMESTAMP

# Column 0 data (LONG, no nulls, 2 values)
00                       # null_flag: 0x00 (no nulls)
01 00 00 00 00 00 00 00  # id=1
02 00 00 00 00 00 00 00  # id=2

# Column 1 data (DOUBLE, no nulls, 2 values)
00                       # null_flag: 0x00 (no nulls)
CD CC CC CC CC CC F4 3F  # value=1.3
9A 99 99 99 99 99 01 40  # value=2.2

# Column 2 data (TIMESTAMP, no nulls, uncompressed, 2 values)
00                       # null_flag: 0x00 (no nulls)
00 E4 0B 54 02 00 00 00  # ts=10000000000 microseconds
80 1A 06 00 00 00 00 00  # ts=400000 microseconds
```

### Example 2: Nullable VARCHAR Column

Table with nullable VARCHAR column, 4 rows where row 1 is null:

```
# Null flag + bitmap for 4 rows where row 1 is null
01           # null_flag: nonzero = bitmap follows
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

3 rows with values: "us", "eu", "us" (per-table dictionary mode):

```
# Null flag
00           # null_flag: 0x00 (no nulls)

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

### Example 4: Multi-Table with Gorilla + Delta Symbol Dictionary

A message with 1 table ("sensors"), 2 rows, 3 columns (symbol "host", double
"temp", designated timestamp):

```
Header (12 bytes):
  51 57 50 31   -- Magic: "QWP1"
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
      00                       -- schema_id = 0
      04 68 6F 73 74  09       -- "host" : SYMBOL
      04 74 65 6D 70  07       -- "temp" : DOUBLE
      00              0A       -- "" : TIMESTAMP (designated)

    Column 0 (SYMBOL, global IDs):
      00                       -- null_flag: no nulls
      00                       -- row 0: global ID 0
      01                       -- row 1: global ID 1

    Column 1 (DOUBLE, 2 x 8 bytes):
      00                       -- null_flag: no nulls
      66 66 66 66 66 E6 56 40  -- 91.6
      9A 99 99 99 99 19 57 40  -- 92.4

    Column 2 (TIMESTAMP, Gorilla):
      00                       -- null_flag: no nulls
      01                       -- encoding = Gorilla
      [8 bytes: t0]
      [8 bytes: t1]
```

## 17. Reference Implementation

The authoritative implementation lives in QuestDB's Java codebase under
`core/src/main/java/io/questdb/cutlass/qwp/protocol/`. That directory contains
the header and varint parsers, the schema registry, the message and table-block
cursors, and the type-specific column decoders.

## 18. Version History

| Version        | Description                        |
|----------------|------------------------------------|
| 1 (`0x01`)     | Initial binary protocol release    |
