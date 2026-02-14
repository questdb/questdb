/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
 * Constants for the ILP v4 binary protocol.
 */
public final class QwpConstants {

    // ==================== Magic Bytes ====================

    /**
     * Magic bytes for ILP v4 message: "ILP4" (ASCII).
     */
    public static final int MAGIC_MESSAGE = 0x34504C49; // "ILP4" in little-endian

    /**
     * Magic bytes for capability request: "ILP?" (ASCII).
     */
    public static final int MAGIC_CAPABILITY_REQUEST = 0x3F504C49; // "ILP?" in little-endian

    /**
     * Magic bytes for capability response: "ILP!" (ASCII).
     */
    public static final int MAGIC_CAPABILITY_RESPONSE = 0x21504C49; // "ILP!" in little-endian

    /**
     * Magic bytes for fallback response (old server): "ILP0" (ASCII).
     */
    public static final int MAGIC_FALLBACK = 0x30504C49; // "ILP0" in little-endian

    // ==================== Header Structure ====================

    /**
     * Size of the message header in bytes.
     */
    public static final int HEADER_SIZE = 12;

    /**
     * Offset of magic bytes in header (4 bytes).
     */
    public static final int HEADER_OFFSET_MAGIC = 0;

    /**
     * Offset of version byte in header.
     */
    public static final int HEADER_OFFSET_VERSION = 4;

    /**
     * Offset of flags byte in header.
     */
    public static final int HEADER_OFFSET_FLAGS = 5;

    /**
     * Offset of table count (uint16, little-endian) in header.
     */
    public static final int HEADER_OFFSET_TABLE_COUNT = 6;

    /**
     * Offset of payload length (uint32, little-endian) in header.
     */
    public static final int HEADER_OFFSET_PAYLOAD_LENGTH = 8;

    // ==================== Protocol Version ====================

    /**
     * Current protocol version.
     */
    public static final byte VERSION_1 = 1;

    // ==================== Flag Bits ====================

    /**
     * Flag bit: LZ4 compression enabled.
     */
    public static final byte FLAG_LZ4 = 0x01;

    /**
     * Flag bit: Zstd compression enabled.
     */
    public static final byte FLAG_ZSTD = 0x02;

    /**
     * Flag bit: Gorilla timestamp encoding enabled.
     */
    public static final byte FLAG_GORILLA = 0x04;

    /**
     * Flag bit: Delta symbol dictionary encoding enabled.
     * When set, symbol columns use global IDs and send only new dictionary entries.
     */
    public static final byte FLAG_DELTA_SYMBOL_DICT = 0x08;

    /**
     * Mask for compression flags (bits 0-1).
     */
    public static final byte FLAG_COMPRESSION_MASK = FLAG_LZ4 | FLAG_ZSTD;

    // ==================== Column Type Codes ====================

    /**
     * Column type: BOOLEAN (1 bit per value, packed).
     */
    public static final byte TYPE_BOOLEAN = 0x01;

    /**
     * Column type: BYTE (int8).
     */
    public static final byte TYPE_BYTE = 0x02;

    /**
     * Column type: SHORT (int16, little-endian).
     */
    public static final byte TYPE_SHORT = 0x03;

    /**
     * Column type: INT (int32, little-endian).
     */
    public static final byte TYPE_INT = 0x04;

    /**
     * Column type: LONG (int64, little-endian).
     */
    public static final byte TYPE_LONG = 0x05;

    /**
     * Column type: FLOAT (IEEE 754 float32).
     */
    public static final byte TYPE_FLOAT = 0x06;

    /**
     * Column type: DOUBLE (IEEE 754 float64).
     */
    public static final byte TYPE_DOUBLE = 0x07;

    /**
     * Column type: STRING (length-prefixed UTF-8).
     */
    public static final byte TYPE_STRING = 0x08;

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
     * Column type: DATE (int64 milliseconds since epoch).
     */
    public static final byte TYPE_DATE = 0x0B;

    /**
     * Column type: UUID (16 bytes, big-endian).
     */
    public static final byte TYPE_UUID = 0x0C;

    /**
     * Column type: LONG256 (32 bytes, big-endian).
     */
    public static final byte TYPE_LONG256 = 0x0D;

    /**
     * Column type: GEOHASH (varint bits + packed geohash).
     */
    public static final byte TYPE_GEOHASH = 0x0E;

    /**
     * Column type: VARCHAR (length-prefixed UTF-8, aux storage).
     */
    public static final byte TYPE_VARCHAR = 0x0F;

    /**
     * Column type: DOUBLE_ARRAY (N-dimensional array of IEEE 754 float64).
     * Wire format: [nDims (1B)] [dim1_len (4B)]...[dimN_len (4B)] [flattened values (LE)]
     */
    public static final byte TYPE_DOUBLE_ARRAY = 0x11;

    /**
     * Column type: LONG_ARRAY (N-dimensional array of int64).
     * Wire format: [nDims (1B)] [dim1_len (4B)]...[dimN_len (4B)] [flattened values (LE)]
     */
    public static final byte TYPE_LONG_ARRAY = 0x12;

    /**
     * Column type: DECIMAL64 (8 bytes, 18 digits precision).
     * Wire format: [scale (1B in schema)] + [big-endian unscaled value (8B)]
     */
    public static final byte TYPE_DECIMAL64 = 0x13;

    /**
     * Column type: DECIMAL128 (16 bytes, 38 digits precision).
     * Wire format: [scale (1B in schema)] + [big-endian unscaled value (16B)]
     */
    public static final byte TYPE_DECIMAL128 = 0x14;

    /**
     * Column type: DECIMAL256 (32 bytes, 77 digits precision).
     * Wire format: [scale (1B in schema)] + [big-endian unscaled value (32B)]
     */
    public static final byte TYPE_DECIMAL256 = 0x15;

    /**
     * Column type: CHAR (2-byte UTF-16 code unit).
     */
    public static final byte TYPE_CHAR = 0x16;

    /**
     * High bit indicating nullable column.
     */
    public static final byte TYPE_NULLABLE_FLAG = (byte) 0x80;

    /**
     * Mask for type code without nullable flag.
     */
    public static final byte TYPE_MASK = 0x7F;

    // ==================== Schema Mode ====================

    /**
     * Schema mode: Full schema included.
     */
    public static final byte SCHEMA_MODE_FULL = 0x00;

    /**
     * Schema mode: Schema reference (hash lookup).
     */
    public static final byte SCHEMA_MODE_REFERENCE = 0x01;

    // ==================== Response Status Codes ====================

    /**
     * Status: Batch accepted successfully.
     */
    public static final byte STATUS_OK = 0x00;

    /**
     * Status: Some rows failed (partial failure).
     */
    public static final byte STATUS_PARTIAL = 0x01;

    /**
     * Status: Schema hash not recognized.
     */
    public static final byte STATUS_SCHEMA_REQUIRED = 0x02;

    /**
     * Status: Column type incompatible.
     */
    public static final byte STATUS_SCHEMA_MISMATCH = 0x03;

    /**
     * Status: Table doesn't exist (auto-create disabled).
     */
    public static final byte STATUS_TABLE_NOT_FOUND = 0x04;

    /**
     * Status: Malformed message.
     */
    public static final byte STATUS_PARSE_ERROR = 0x05;

    /**
     * Status: Server error.
     */
    public static final byte STATUS_INTERNAL_ERROR = 0x06;

    /**
     * Status: Back-pressure, retry later.
     */
    public static final byte STATUS_OVERLOADED = 0x07;

    // ==================== Default Limits ====================

    /**
     * Default maximum batch size in bytes (16 MB).
     */
    public static final int DEFAULT_MAX_BATCH_SIZE = 16 * 1024 * 1024;

    /**
     * Default maximum tables per batch.
     */
    public static final int DEFAULT_MAX_TABLES_PER_BATCH = 256;

    /**
     * Default maximum rows per table in a batch.
     */
    public static final int DEFAULT_MAX_ROWS_PER_TABLE = 1_000_000;

    /**
     * Maximum columns per table (QuestDB limit).
     */
    public static final int MAX_COLUMNS_PER_TABLE = 2048;

    /**
     * Maximum table name length in bytes.
     */
    public static final int MAX_TABLE_NAME_LENGTH = 127;

    /**
     * Maximum column name length in bytes.
     */
    public static final int MAX_COLUMN_NAME_LENGTH = 127;

    /**
     * Default maximum string length in bytes (1 MB).
     */
    public static final int DEFAULT_MAX_STRING_LENGTH = 1024 * 1024;

    /**
     * Default initial receive buffer size (64 KB).
     */
    public static final int DEFAULT_INITIAL_RECV_BUFFER_SIZE = 64 * 1024;

    /**
     * Maximum in-flight batches for pipelining.
     */
    public static final int DEFAULT_MAX_IN_FLIGHT_BATCHES = 4;

    // ==================== Capability Negotiation ====================

    /**
     * Size of capability request in bytes.
     */
    public static final int CAPABILITY_REQUEST_SIZE = 8;

    /**
     * Size of capability response in bytes.
     */
    public static final int CAPABILITY_RESPONSE_SIZE = 8;

    private QwpConstants() {
        // utility class
    }

    /**
     * Returns true if the type code represents a fixed-width type.
     *
     * @param typeCode the column type code (without nullable flag)
     * @return true if fixed-width
     */
    public static boolean isFixedWidthType(byte typeCode) {
        int code = typeCode & TYPE_MASK;
        return code == TYPE_BOOLEAN ||
                code == TYPE_BYTE ||
                code == TYPE_SHORT ||
                code == TYPE_CHAR ||
                code == TYPE_INT ||
                code == TYPE_LONG ||
                code == TYPE_FLOAT ||
                code == TYPE_DOUBLE ||
                code == TYPE_TIMESTAMP ||
                code == TYPE_TIMESTAMP_NANOS ||
                code == TYPE_DATE ||
                code == TYPE_UUID ||
                code == TYPE_LONG256;
    }

    /**
     * Returns the size in bytes for fixed-width types.
     *
     * @param typeCode the column type code (without nullable flag)
     * @return size in bytes, or -1 for variable-width types
     */
    public static int getFixedTypeSize(byte typeCode) {
        int code = typeCode & TYPE_MASK;
        return switch (code) {
            case TYPE_BOOLEAN -> 0; // Special: bit-packed
            case TYPE_BYTE -> 1;
            case TYPE_SHORT, TYPE_CHAR -> 2;
            case TYPE_INT, TYPE_FLOAT -> 4;
            case TYPE_LONG, TYPE_DOUBLE, TYPE_TIMESTAMP, TYPE_TIMESTAMP_NANOS, TYPE_DATE -> 8;
            case TYPE_UUID -> 16;
            case TYPE_LONG256 -> 32;
            default -> -1; // Variable width
        };
    }

    /**
     * Returns a human-readable name for the type code.
     *
     * @param typeCode the column type code
     * @return type name
     */
    public static String getTypeName(byte typeCode) {
        int code = typeCode & TYPE_MASK;
        boolean nullable = (typeCode & TYPE_NULLABLE_FLAG) != 0;
        String name = switch (code) {
            case TYPE_BOOLEAN -> "BOOLEAN";
            case TYPE_BYTE -> "BYTE";
            case TYPE_SHORT -> "SHORT";
            case TYPE_CHAR -> "CHAR";
            case TYPE_INT -> "INT";
            case TYPE_LONG -> "LONG";
            case TYPE_FLOAT -> "FLOAT";
            case TYPE_DOUBLE -> "DOUBLE";
            case TYPE_STRING -> "STRING";
            case TYPE_SYMBOL -> "SYMBOL";
            case TYPE_TIMESTAMP -> "TIMESTAMP";
            case TYPE_TIMESTAMP_NANOS -> "TIMESTAMP_NANOS";
            case TYPE_DATE -> "DATE";
            case TYPE_UUID -> "UUID";
            case TYPE_LONG256 -> "LONG256";
            case TYPE_GEOHASH -> "GEOHASH";
            case TYPE_VARCHAR -> "VARCHAR";
            case TYPE_DOUBLE_ARRAY -> "DOUBLE_ARRAY";
            case TYPE_LONG_ARRAY -> "LONG_ARRAY";
            case TYPE_DECIMAL64 -> "DECIMAL64";
            case TYPE_DECIMAL128 -> "DECIMAL128";
            case TYPE_DECIMAL256 -> "DECIMAL256";
            default -> "UNKNOWN(" + code + ")";
        };
        return nullable ? name + "?" : name;
    }
}
