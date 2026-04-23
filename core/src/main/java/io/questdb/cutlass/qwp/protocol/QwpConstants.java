/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cutlass.qwp.protocol;

/**
 * Constants for the QWP v1 binary protocol.
 * <p>
 * All little-endian encoding in this protocol is byte-level: multi-byte and
 * multi-word values are stored with the least significant byte at the lowest
 * address.
 */
public final class QwpConstants {

    public static final byte COMPRESSION_NONE = 0;
    public static final byte COMPRESSION_ZSTD = 1;
    // Server-side clamp for the level requested by the client via
    // X-QWP-Accept-Encoding: zstd;level=N. zstd levels 10 and above drop to
    // <20 MB/s compress speed, which lets a slow/malicious client pin a
    // worker thread for seconds per batch.
    public static final int COMPRESSION_ZSTD_MAX_LEVEL = 9;
    public static final int COMPRESSION_ZSTD_MIN_LEVEL = 1;
    /**
     * Default maximum batch size in bytes (16 MB).
     */
    public static final int DEFAULT_MAX_BATCH_SIZE = 16 * 1024 * 1024;

    /**
     * Default maximum bytes the egress connection-scoped SYMBOL dict may hold
     * before the server emits a {@code CACHE_RESET} and starts over.
     */
    public static final int DEFAULT_MAX_EGRESS_DICT_HEAP_BYTES = 8 * 1024 * 1024;
    /**
     * Default maximum entry count the egress connection-scoped SYMBOL dict may
     * hold before the server emits a {@code CACHE_RESET} and starts over.
     */
    public static final int DEFAULT_MAX_EGRESS_DICT_ENTRIES = 100_000;
    /**
     * Default soft cap on distinct schemas registered by an egress connection.
     * On exceeding this threshold the server emits a {@code CACHE_RESET} and
     * resets the schema-fingerprint cache; the connection remains usable and
     * continues with fresh schema ids. Tighter than the shared
     * {@link #DEFAULT_MAX_SCHEMAS_PER_CONNECTION} because egress-side state is
     * fully server-owned and reusable.
     */
    public static final int DEFAULT_MAX_EGRESS_SCHEMAS_PER_CONNECTION = 4_096;
    /**
     * Default maximum rows per table in a batch.
     */
    public static final int DEFAULT_MAX_ROWS_PER_TABLE = 1_000_000;
    /**
     * Default maximum number of distinct schemas registered on a single connection.
     */
    public static final int DEFAULT_MAX_SCHEMAS_PER_CONNECTION = 65_535;
    /**
     * Default maximum number of distinct tables per connection or UDP receiver.
     */
    public static final int DEFAULT_MAX_TABLES_PER_CONNECTION = 10_000;
    /**
     * Flag bit: Delta symbol dictionary encoding enabled.
     * When set, symbol columns use global IDs and send only new dictionary entries.
     */
    public static final byte FLAG_DELTA_SYMBOL_DICT = 0x08;
    /**
     * Flag bit: Gorilla timestamp encoding enabled.
     */
    public static final byte FLAG_GORILLA = 0x04;
    /**
     * Flag bit: the region starting at {@code delta_symbol_dict} (or the first
     * table block if no delta dict is present) is zstd-compressed. The prelude
     * remains uncompressed so the I/O thread can dispatch on msg_kind / batch_seq
     * without paying the decompress cost. Set only on {@code RESULT_BATCH} frames
     * and only after the handshake negotiated {@code zstd}.
     */
    public static final byte FLAG_ZSTD = 0x10;
    /**
     * Offset of flags byte in header.
     */
    public static final int HEADER_OFFSET_FLAGS = 5;
    /**
     * Offset of magic bytes in header (4 bytes).
     */
    public static final int HEADER_OFFSET_MAGIC = 0;
    /**
     * Offset of payload length (uint32, little-endian) in header.
     */
    public static final int HEADER_OFFSET_PAYLOAD_LENGTH = 8;
    /**
     * Offset of table count (uint16, little-endian) in header.
     */
    public static final int HEADER_OFFSET_TABLE_COUNT = 6;
    /**
     * Offset of version byte in header.
     */
    public static final int HEADER_OFFSET_VERSION = 4;
    /**
     * Size of the message header in bytes.
     */
    public static final int HEADER_SIZE = 12;
    /**
     * Magic bytes for QWP v1 message: "QWP1" (ASCII).
     */
    public static final int MAGIC_MESSAGE = 0x31505751; // "QWP1" in little-endian
    /**
     * Maximum columns per table (QuestDB limit).
     */
    public static final int MAX_COLUMNS_PER_TABLE = 2048;
    /**
     * Maximum column name length in bytes.
     */
    public static final int MAX_COLUMN_NAME_LENGTH = 127;
    /**
     * Maximum symbol dictionary entries per column or per connection.
     */
    public static final int MAX_SYMBOL_DICTIONARY_SIZE = 1_000_000;
    /**
     * Maximum table name length in bytes.
     */
    public static final int MAX_TABLE_NAME_LENGTH = 127;
    /**
     * Schema mode: Full schema included.
     */
    public static final byte SCHEMA_MODE_FULL = 0x00;
    /**
     * Status: Egress-only. Query aborted because the client sent a {@code CANCEL}
     * frame or the server invoked explicit cancellation.
     */
    public static final byte STATUS_CANCELLED = 0x0A;
    /**
     * Status: Server error.
     */
    public static final byte STATUS_INTERNAL_ERROR = 0x06;
    /**
     * Status: Egress-only. Query aborted because a server-side limit was hit
     * (query timeout, memory cap, circuit breaker, OOM).
     */
    public static final byte STATUS_LIMIT_EXCEEDED = 0x0B;
    /**
     * Status: Batch accepted successfully.
     */
    public static final byte STATUS_OK = 0x00;
    /**
     * Status: Malformed message.
     */
    public static final byte STATUS_PARSE_ERROR = 0x05;
    /**
     * Status: Column type incompatible.
     */
    public static final byte STATUS_SCHEMA_MISMATCH = 0x03;
    /**
     * Status: Authorization failure.
     */
    public static final byte STATUS_SECURITY_ERROR = 0x08;
    /**
     * Status: Write failure (e.g., table not accepting writes).
     */
    public static final byte STATUS_WRITE_ERROR = 0x09;
    /**
     * Column type: BINARY (length-prefixed opaque bytes).
     * Wire format: identical to VARCHAR — (N+1) x uint32 offsets + concatenated bytes.
     */
    public static final byte TYPE_BINARY = 0x17;
    /**
     * Column type: BOOLEAN (1 bit per value, packed).
     */
    public static final byte TYPE_BOOLEAN = 0x01;
    /**
     * Column type: BYTE (int8).
     */
    public static final byte TYPE_BYTE = 0x02;
    /**
     * Column type: CHAR (2-byte UTF-16 code unit).
     */
    public static final byte TYPE_CHAR = 0x16;
    /**
     * Column type: DATE (int64 milliseconds since epoch).
     */
    public static final byte TYPE_DATE = 0x0B;
    /**
     * Column type: DECIMAL128 (16 bytes, 38 digits precision).
     * Wire format: [scale (1B in schema)] + [little-endian unscaled value (16B)]
     */
    public static final byte TYPE_DECIMAL128 = 0x14;
    /**
     * Column type: DECIMAL256 (32 bytes, 77 digits precision).
     * Wire format: [scale (1B in schema)] + [little-endian unscaled value (32B)]
     */
    public static final byte TYPE_DECIMAL256 = 0x15;
    /**
     * Column type: DECIMAL64 (8 bytes, 18 digits precision).
     * Wire format: [scale (1B in schema)] + [little-endian unscaled value (8B)]
     */
    public static final byte TYPE_DECIMAL64 = 0x13;
    /**
     * Column type: DOUBLE (IEEE 754 float64).
     */
    public static final byte TYPE_DOUBLE = 0x07;
    /**
     * Column type: DOUBLE_ARRAY (N-dimensional array of IEEE 754 float64).
     * Wire format: [nDims (1B)] [dim1_len (4B)]...[dimN_len (4B)] [flattened values (LE)]
     */
    public static final byte TYPE_DOUBLE_ARRAY = 0x11;
    /**
     * Column type: FLOAT (IEEE 754 float32).
     */
    public static final byte TYPE_FLOAT = 0x06;
    /**
     * Column type: GEOHASH (varint bits + packed geohash).
     */
    public static final byte TYPE_GEOHASH = 0x0E;
    /**
     * Column type: INT (int32, little-endian).
     */
    public static final byte TYPE_INT = 0x04;
    /**
     * Column type: IPv4 (32-bit address). Wire format: 4 bytes LE, identical to INT.
     * NULL is signalled via the standard null bitmap; the int payload for non-null rows
     * is the address bits (network byte order on the wire? No — little-endian like INT).
     * Note: QuestDB stores IPv4 NULL as the bit pattern 0 (i.e. 0.0.0.0), so the address
     * 0.0.0.0 cannot be represented as non-null in QuestDB regardless of the wire type.
     */
    public static final byte TYPE_IPV4 = 0x18;
    /**
     * Column type: LONG (int64, little-endian).
     */
    public static final byte TYPE_LONG = 0x05;
    /**
     * Column type: LONG256 (32 bytes, little-endian).
     */
    public static final byte TYPE_LONG256 = 0x0D;

    /**
     * Column type: LONG_ARRAY (N-dimensional array of int64).
     * Wire format: [nDims (1B)] [dim1_len (4B)]...[dimN_len (4B)] [flattened values (LE)]
     */
    public static final byte TYPE_LONG_ARRAY = 0x12;
    /**
     * Column type: SHORT (int16, little-endian).
     */
    public static final byte TYPE_SHORT = 0x03;
    /**
     * Column type: SYMBOL (dictionary-encoded string).
     */
    public static final byte TYPE_SYMBOL = 0x09;
    /**
     * Column type: TIMESTAMP (int64 microseconds since epoch).
     * Use this for timestamps beyond nanosecond range (year > 2262).
     */
    public static final byte TYPE_TIMESTAMP = 0x0A;
    /**
     * Column type: TIMESTAMP_NANOS (int64 nanoseconds since epoch).
     * Use this for full nanosecond precision (limited to years 1677-2262).
     */
    public static final byte TYPE_TIMESTAMP_NANOS = 0x10;
    /**
     * Column type: UUID (16 bytes, little-endian).
     */
    public static final byte TYPE_UUID = 0x0C;

    /**
     * Column type: VARCHAR (length-prefixed UTF-8, aux storage).
     */
    public static final byte TYPE_VARCHAR = 0x0F;
    /**
     * Current protocol version.
     */
    public static final byte VERSION_1 = 1;
    /**
     * Maximum protocol version supported by this build.
     */
    public static final byte MAX_SUPPORTED_VERSION = VERSION_1;

    private QwpConstants() {
        // utility class
    }

    /**
     * Returns the per-value size in bytes as encoded on the wire. BOOLEAN returns 0
     * because it is bit-packed (1 bit per value). GEOHASH returns -1 because it uses
     * variable-width encoding (varint precision + ceil(precision/8) bytes per value).
     *
     * @param typeCode the column type code (without nullable flag)
     * @return size in bytes, 0 for bit-packed (BOOLEAN), or -1 for variable-width types
     */
    public static int getFixedTypeSize(byte typeCode) {
        return switch (typeCode) {
            case TYPE_BOOLEAN -> 0; // Special: bit-packed
            case TYPE_BYTE -> 1;
            case TYPE_SHORT, TYPE_CHAR -> 2;
            case TYPE_INT, TYPE_FLOAT -> 4;
            case TYPE_LONG, TYPE_DOUBLE, TYPE_TIMESTAMP, TYPE_TIMESTAMP_NANOS, TYPE_DATE, TYPE_DECIMAL64 -> 8;
            case TYPE_UUID, TYPE_DECIMAL128 -> 16;
            case TYPE_LONG256, TYPE_DECIMAL256 -> 32;
            case TYPE_GEOHASH -> -1; // Variable width: varint precision + packed values
            default -> -1; // Variable width
        };
    }

    /**
     * Returns a human-readable name for the type code.
     *
     * @param typeCode QWP v1 type code
     * @return human-readable type name
     */
    public static String getTypeName(byte typeCode) {
        return switch (typeCode) {
            case TYPE_BOOLEAN -> "BOOLEAN";
            case TYPE_BYTE -> "BYTE";
            case TYPE_SHORT -> "SHORT";
            case TYPE_CHAR -> "CHAR";
            case TYPE_BINARY -> "BINARY";
            case TYPE_IPV4 -> "IPv4";
            case TYPE_INT -> "INT";
            case TYPE_LONG -> "LONG";
            case TYPE_FLOAT -> "FLOAT";
            case TYPE_DOUBLE -> "DOUBLE";
            case TYPE_VARCHAR -> "VARCHAR";
            case TYPE_SYMBOL -> "SYMBOL";
            case TYPE_TIMESTAMP -> "TIMESTAMP";
            case TYPE_TIMESTAMP_NANOS -> "TIMESTAMP_NANOS";
            case TYPE_DATE -> "DATE";
            case TYPE_UUID -> "UUID";
            case TYPE_LONG256 -> "LONG256";
            case TYPE_GEOHASH -> "GEOHASH";
            case TYPE_DOUBLE_ARRAY -> "DOUBLE_ARRAY";
            case TYPE_LONG_ARRAY -> "LONG_ARRAY";
            case TYPE_DECIMAL64 -> "DECIMAL64";
            case TYPE_DECIMAL128 -> "DECIMAL128";
            case TYPE_DECIMAL256 -> "DECIMAL256";
            default -> "UNKNOWN(" + typeCode + ")";
        };
    }

    /**
     * Returns true if the type code represents a fixed-width type.
     *
     * @param typeCode the column type code (without nullable flag)
     * @return true if fixed-width
     */
    public static boolean isFixedWidthType(byte typeCode) {
        return switch (typeCode) {
            case TYPE_BOOLEAN, TYPE_BYTE, TYPE_SHORT, TYPE_CHAR,
                 TYPE_INT, TYPE_LONG, TYPE_FLOAT, TYPE_DOUBLE,
                 TYPE_TIMESTAMP, TYPE_TIMESTAMP_NANOS, TYPE_DATE,
                 TYPE_UUID, TYPE_LONG256,
                 TYPE_DECIMAL64, TYPE_DECIMAL128, TYPE_DECIMAL256 -> true;
            default -> false;
        };
    }
}
