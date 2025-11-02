# Streaming Columnar Binary Format (SCBF) Specification

## Overview

The Streaming Columnar Binary Format (SCBF) is a columnar serialization format designed for efficient network
transmission of SQL query results. It supports all QuestDB column types and uses a streaming architecture with row
groups for memory-bounded operation.

**Version**: 1
**Endianness**: Little-endian
**File Extension**: `.scbf` (suggested)

## Key Features

- **Columnar storage**: Data organized by column for efficient compression and processing
- **Streaming**: Supports unknown result set sizes via row groups
- **Type metadata**: Comprehensive type information including precision and geohash bits
- **Separated schema**: Column types and names stored separately for flexible parsing
- **No per-value framing**: Variable-length data stored without length prefixes using offset arrays
- **Null bitmaps**: Efficient null value representation
- **Network optimized**: Designed for non-blocking network transmission

## File Structure

```
┌─────────────────────────────────────────────────────────────┐
│                          HEADER                              │
│  - Magic number (4 bytes): "SCBF"                           │
│  - Version (2 bytes): 1                                      │
│  - Column count (4 bytes)                                    │
├─────────────────────────────────────────────────────────────┤
│                       SCHEMA: TYPES                          │
│  For each column:                                            │
│    - Column type (4 bytes)                                   │
│    - Metadata length (4 bytes)                               │
│    - Metadata bytes (0-N bytes)                              │
├─────────────────────────────────────────────────────────────┤
│                       SCHEMA: NAMES                          │
│  For each column:                                            │
│    - Name length in bytes (4 bytes)                          │
│    - UTF-8 encoded name (N bytes)                            │
├─────────────────────────────────────────────────────────────┤
│                      ROW GROUP 1                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ Row Group Header                                       │  │
│  │   - Row count (4 bytes)                                │  │
│  ├───────────────────────────────────────────────────────┤  │
│  │ For each column:                                       │  │
│  │   ┌─────────────────────────────────────────────────┐ │  │
│  │   │ Null Bitmap                                      │ │  │
│  │   │   - Length (4 bytes)                             │ │  │
│  │   │   - Bitmap data (N bytes)                        │ │  │
│  │   ├─────────────────────────────────────────────────┤ │  │
│  │   │ Offsets Array (variable-length types only)      │ │  │
│  │   │   - Length in bytes (4 bytes)                    │ │  │
│  │   │   - Offset values (rowCount+1 × 4 bytes)         │ │  │
│  │   ├─────────────────────────────────────────────────┤ │  │
│  │   │ Column Data                                      │ │  │
│  │   │   - Data bytes (N bytes, no length prefix)       │ │  │
│  │   └─────────────────────────────────────────────────┘ │  │
│  └───────────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                      ROW GROUP 2                             │
│  (same structure as Row Group 1)                             │
├─────────────────────────────────────────────────────────────┤
│                         ...                                  │
├─────────────────────────────────────────────────────────────┤
│                      ROW GROUP N                             │
│  (same structure as Row Group 1)                             │
├─────────────────────────────────────────────────────────────┤
│                      END MARKER                              │
│  - Row count: -1 (4 bytes)                                   │
└─────────────────────────────────────────────────────────────┘
```

## Data Types

All multi-byte values are stored in **little-endian** byte order.

### Primitive Types

| Type   | Size (bytes) | Description               |
|--------|--------------|---------------------------|
| byte   | 1            | Signed 8-bit integer      |
| short  | 2            | Signed 16-bit integer     |
| int    | 4            | Signed 32-bit integer     |
| long   | 8            | Signed 64-bit integer     |
| float  | 4            | IEEE 754 single precision |
| double | 8            | IEEE 754 double precision |

## Detailed Section Descriptions

### 1. Header

**Purpose**: Identifies file format and provides column count.

```
Offset  Size  Field           Description
------  ----  --------------  ----------------------------------
0       4     Magic           ASCII "SCBF" (0x53 0x43 0x42 0x46)
4       2     Version         Format version (currently 1)
6       4     Column Count    Number of columns (N)
```

**Total Size**: 10 bytes

### 2. Schema: Types Section

**Purpose**: Describes column types.

For each column (repeated N times):

```
Offset  Size  Field              Description
------  ----  -----------------  --------------------------------
0       4     Column Type        QuestDB ColumnType int (encodes type + precision/bits)
```

The column type int encodes all necessary information:

- Base type (BOOLEAN, INT, STRING, TIMESTAMP, GEOHASH, etc.)
- For TIMESTAMP: precision (micros vs nanos) encoded in the int
- For GEOHASH types: precision bits (1-60) encoded in the int

**Design Note**: No separate metadata needed - everything is in the columnType int.

### 3. Schema: Names Section

**Purpose**: Provides column names for column-by-name lookup (optional for readers).

For each column (repeated N times):

```
Offset  Size  Field              Description
------  ----  -----------------  --------------------------------
0       4     Name Length        UTF-8 byte length (L bytes)
4       L     Column Name        UTF-8 encoded column name
```

**Design Note**: Readers can skip this entire section if they only need column indices.

### 4. Row Groups

A row group contains a batch of rows (typically 1000-10000 rows). This enables:

- Memory-bounded operation
- Incremental processing
- Network streaming

#### Row Group Header

```
Offset  Size  Field              Description
------  ----  -----------------  --------------------------------
0       4     Row Count          Number of rows in this group (R)
                                 -1 indicates END MARKER
```

#### Per-Column Data

For each column in the row group:

##### A. Null Bitmap

```
Offset  Size  Field              Description
------  ----  -----------------  --------------------------------
0       B     Bitmap Data        Null flags, 1 bit per row
```

**Bitmap Encoding**:

- `B = (R + 7) / 8` bytes (calculable from row count R)
- Bit N represents row N (0-indexed)
- Bit value 1 = NULL, 0 = NOT NULL
- Bits stored in little-endian byte order
- Example: Row 0 = bit 0 of byte 0, Row 8 = bit 0 of byte 1
- **No length prefix**: Reader calculates length from row count

##### B. Offsets Array (Variable-Length Types Only)

**Applies to**: STRING, SYMBOL, VARCHAR, BINARY

```
Offset  Size  Field              Description
------  ----  -----------------  --------------------------------
0       4×O   Offset Values      Array of R+1 int32 offsets
```

**Offset Array Structure**:

- Contains R+1 offsets (one per row, plus final offset)
- Array length is (R+1)×4 bytes (calculable from row count R)
- Offset[0] = 0 (always)
- Offset[i+1] - Offset[i] = byte length of value at row i
- Offset[R] = total data length
- All offsets are relative to start of data section
- **No length prefix**: Reader calculates length from row count

**Example**:

```
Strings: ["hello", "world", "foo"]
Offsets: [0, 5, 10, 13]
```

##### C. Column Data

```
Offset  Size  Field              Description
------  ----  -----------------  --------------------------------
0       D     Data Bytes         Column data (no length prefix)
```

**Fixed-Size Type Encoding**:

- Data contains R values packed consecutively
- Each value is typeSize bytes
- Total size: D = R × typeSize (calculable from row count and type)
- No length prefix needed - size is deterministic

**Variable-Length Type Encoding**:

- Data contains concatenated raw bytes (NO per-value length prefixes)
- Use offset array to determine value boundaries
- NULL values have zero-length (offset[i+1] == offset[i])
- Total size: D = offsets[R] (last offset value IS the data length)

**Data Encoding by Type**:

| Type         | Encoding                        | Size per Value |
|--------------|---------------------------------|----------------|
| BOOLEAN      | 0=false, 1=true                 | 1 byte         |
| BYTE         | Two's complement                | 1 byte         |
| SHORT        | Two's complement, little-endian | 2 bytes        |
| CHAR         | UTF-16 code unit, little-endian | 2 bytes        |
| INT          | Two's complement, little-endian | 4 bytes        |
| IPv4         | Network byte order (big-endian) | 4 bytes        |
| LONG         | Two's complement, little-endian | 8 bytes        |
| DATE         | Milliseconds since epoch        | 8 bytes        |
| TIMESTAMP    | Micros or nanos since epoch     | 8 bytes        |
| FLOAT        | IEEE 754 single, little-endian  | 4 bytes        |
| DOUBLE       | IEEE 754 double, little-endian  | 8 bytes        |
| LONG256      | 4× int64, little-endian         | 32 bytes       |
| UUID/LONG128 | Low 64 bits, then high 64 bits  | 16 bytes       |
| GEOBYTE      | Encoded geohash                 | 1 byte         |
| GEOSHORT     | Encoded geohash                 | 2 bytes        |
| GEOINT       | Encoded geohash                 | 4 bytes        |
| GEOLONG      | Encoded geohash                 | 8 bytes        |
| STRING       | UTF-8 bytes, no terminator      | Variable       |
| SYMBOL       | UTF-8 bytes, no terminator      | Variable       |
| VARCHAR      | UTF-8 bytes, no terminator      | Variable       |
| BINARY       | Raw bytes                       | Variable       |

### 5. End Marker

Signals end of all row groups.

```
Offset  Size  Field              Description
------  ----  -----------------  --------------------------------
0       4     Row Count          -1 (0xFFFFFFFF)
```

## Reading Algorithm

### Pseudocode for Reader

```python
# Read header
magic = read_bytes(4)
assert magic == b"SCBF"
version = read_int16()
column_count = read_int32()

# Read schema types
column_types = []
for i in range(column_count):
    col_type = read_int32()  # Column type int (encodes type + precision/bits)
    column_types.append(col_type)

# Read schema names
column_names = []
for i in range(column_count):
    name_len = read_int32()
    name = read_utf8(name_len)
    column_names.append(name)

# Read row groups
while True:
    row_count = read_int32()

    if row_count == -1:
        break  # End marker

    # Read each column in row group
    for col_idx in range(column_count):
        # Read null bitmap (length calculated from row count)
        bitmap_len = (row_count + 7) // 8
        null_bitmap = read_bytes(bitmap_len)

        # Read offsets for variable-length types
        if is_variable_length(column_types[col_idx]):
            # Offsets length is (row_count + 1) * 4, no need to read it
            offsets_count = row_count + 1
            offsets = read_int32_array(offsets_count)
            # Data length is last offset value
            data_len = offsets[-1]
        else:
            # Calculate data length from row count and type size
            data_len = row_count * type_size(column_types[col_idx])

        # Read column data (no length prefix)
        data = read_bytes(data_len)

        # Process rows
        for row in range(row_count):
            if is_null(null_bitmap, row):
                value = None
            elif is_variable_length(column_types[col_idx]):
                start = offsets[row]
                end = offsets[row + 1]
                value = decode_value(data[start:end], column_types[col_idx])
            else:
                offset = row * type_size(column_types[col_idx])
                value = decode_fixed(data, offset, column_types[col_idx])

            yield (row, col_idx, value)
```

## Examples

### Example 1: Simple Integer Column

**Schema**:

- Column 0: INT, name "id"

**Data**: 3 rows [1, 2, 3], no nulls

```
Header:
  53 43 42 46              # "SCBF"
  01 00                    # Version 1
  01 00 00 00              # 1 column

Schema Types:
  05 00 00 00              # INT type

Schema Names:
  02 00 00 00              # 2 bytes
  69 64                    # "id" in UTF-8

Row Group:
  03 00 00 00              # 3 rows

  Column 0:
    Null Bitmap:
      00                   # 1 byte, no nulls (000)

    Data (no length prefix):
      01 00 00 00          # 1
      02 00 00 00          # 2
      03 00 00 00          # 3

End Marker:
  FF FF FF FF              # -1
```

### Example 2: Variable-Length String Column

**Schema**:

- Column 0: STRING, name "name"

**Data**: 2 rows ["hello", "world"], no nulls

```
Header:
  53 43 42 46              # "SCBF"
  01 00                    # Version 1
  01 00 00 00              # 1 column

Schema Types:
  0B 00 00 00              # STRING type (11)

Schema Names:
  04 00 00 00              # 4 bytes
  6E 61 6D 65              # "name" in UTF-8

Row Group:
  02 00 00 00              # 2 rows

  Column 0:
    Null Bitmap:
      00                   # 1 byte, no nulls

    Offsets (length = (2+1)×4 = 12 bytes, no prefix):
      00 00 00 00          # Offset 0: 0
      05 00 00 00          # Offset 1: 5 ("hello" is 5 bytes)
      0A 00 00 00          # Offset 2: 10 (data length = 10 bytes)

    Data (no length prefix, size = offsets[2] = 10):
      68 65 6C 6C 6F       # "hello"
      77 6F 72 6C 64       # "world"

End Marker:
  FF FF FF FF              # -1
```

### Example 3: Multiple Columns with Nulls

**Schema**:

- Column 0: INT, name "id"
- Column 1: STRING, name "name"

**Data**: 3 rows [(1, "alice"), (2, NULL), (3, "bob")]

```
Header:
  53 43 42 46              # "SCBF"
  01 00                    # Version 1
  02 00 00 00              # 2 columns

Schema Types:
  05 00 00 00              # INT
  0B 00 00 00              # STRING

Schema Names:
  02 00 00 00              # 2 bytes
  69 64                    # "id"
  04 00 00 00              # 4 bytes
  6E 61 6D 65              # "name"

Row Group:
  03 00 00 00              # 3 rows

  Column 0 (id):
    Null Bitmap:
      00                   # 1 byte, no nulls

    Data (no length prefix, size = 3 * 4 = 12):
      01 00 00 00          # 1
      02 00 00 00          # 2
      03 00 00 00          # 3

  Column 1 (name):
    Null Bitmap:
      02                   # 1 byte, row 1 is NULL (binary: 010)

    Offsets (length = (3+1)×4 = 16 bytes, no prefix):
      00 00 00 00          # Offset 0: 0
      05 00 00 00          # Offset 1: 5 ("alice")
      05 00 00 00          # Offset 2: 5 (NULL, zero length)
      08 00 00 00          # Offset 3: 8 (data length)

    Data (no length prefix, size = offsets[3] = 8):
      61 6C 69 63 65       # "alice"
      62 6F 62              # "bob"

End Marker:
  FF FF FF FF              # -1
```

## Design Rationale

### Why Separate Types and Names?

1. **Flexible parsing**: Readers that only need column indices can skip name section
2. **Reduced overhead**: Name strings don't pollute type metadata
3. **Future optimization**: Names could be compressed independently

### Why No Length Prefixes?

**No per-value length prefixes for variable-length types:**

1. **Network efficiency**: Reduces bytes transmitted
2. **Simpler splitting**: Large values can be split across network packets
3. **Reader simplicity**: Use offset array instead of tracking position
4. **Compatibility**: Standard columnar format pattern (like Arrow, Parquet)

**No column data length prefix:**

1. **Fixed-size types**: Length = rowCount × typeSize (deterministic)
2. **Variable-length types**: Length = offsets[rowCount] (last offset)
3. **Redundancy elimination**: Information already present in format
4. **Simpler format**: Fewer fields to parse

**No offsets array length prefix:**

1. **Deterministic length**: Length = (rowCount + 1) × 4 bytes
2. **Row count known**: Already read from row group header
3. **Redundancy elimination**: Calculated value, not stored
4. **Format simplification**: One less field per variable-length column

### Why Row Groups?

1. **Memory bounded**: Don't need to buffer entire result set
2. **Progressive rendering**: Can display rows as they arrive
3. **Network friendly**: Natural chunking for send() calls
4. **Error recovery**: Partial results on connection failure

## Compatibility Notes

- **Version checking**: Readers MUST check version field
- **Unknown types**: Readers SHOULD skip unknown column types gracefully
- **Metadata evolution**: Future versions may add metadata fields
- **Endianness**: Format is little-endian only (network byte order NOT used except IPv4)

## Implementation Notes

- Writers should use row group size of 1000-10000 rows
- NULL representation uses sentinel values + bitmap for correctness
- Variable-length data written without intermediate buffering
- UTF-8 encoding uses efficient streaming encoders

---

**Document Version**: 1.0
**Last Updated**: 2025-11-01
**Format Version**: 1
