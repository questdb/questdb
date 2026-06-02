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

import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

import java.nio.charset.StandardCharsets;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.MAX_COLUMN_NAME_LENGTH;

/**
 * Represents a QWP table schema (immutable, safe for caching).
 * <p>
 * A schema is an ordered list of column definitions. Columns are always carried
 * inline on the wire: each is encoded as {@code name_len:varint, name:utf8,
 * type_code:u8}. The schema section carries no mode byte and no schema id; the
 * column count is read from the enclosing table header.
 */
public final class QwpSchema {

    private final ObjList<QwpColumnDef> columns;

    private QwpSchema(ObjList<QwpColumnDef> columns) {
        this.columns = columns;
    }

    /**
     * Creates a schema from column definitions.
     *
     * @param columns the column definitions
     * @return the schema
     */
    public static QwpSchema create(QwpColumnDef[] columns) {
        ObjList<QwpColumnDef> list = new ObjList<>(columns.length);
        for (QwpColumnDef col : columns) {
            list.add(col);
        }
        return new QwpSchema(list);
    }

    /**
     * Parses a schema section (inline columns) from a byte array.
     *
     * @param buf         buffer
     * @param bufOffset   starting offset
     * @param length      available bytes
     * @param columnCount expected number of columns
     * @return parse result
     * @throws QwpParseException if parsing fails
     */
    public static ParseResult parse(byte[] buf, int bufOffset, int length, int columnCount) throws QwpParseException {
        return parseColumnsFromArray(buf, bufOffset, length, columnCount, 0);
    }

    /**
     * Parses a schema section (inline columns) from direct memory (zero-allocation version).
     * <p>
     * This method populates the provided {@link ParseResult} instead of allocating a new one.
     * Use this on hot paths where allocation must be avoided.
     *
     * @param address     memory address
     * @param length      available bytes
     * @param columnCount expected number of columns (from table header)
     * @param result      reusable parse result to populate
     * @throws QwpParseException if parsing fails
     */
    public static void parse(long address, int length, int columnCount, ParseResult result) throws QwpParseException {
        parseColumns(address, length, columnCount, 0, result);
    }

    /**
     * Parses a schema section (inline columns) from direct memory.
     * <p>
     * Convenience method that allocates a new {@link ParseResult}. For zero-allocation
     * parsing, use {@link #parse(long, int, int, ParseResult)} instead.
     *
     * @param address     memory address
     * @param length      available bytes
     * @param columnCount expected number of columns (from table header)
     * @return parse result
     * @throws QwpParseException if parsing fails
     */
    public static ParseResult parse(long address, int length, int columnCount) throws QwpParseException {
        ParseResult result = new ParseResult();
        parse(address, length, columnCount, result);
        return result;
    }

    /**
     * Encodes this schema's inline columns to direct memory.
     *
     * @param address destination address
     * @return address after the encoded columns
     */
    public long encode(long address) {
        long pos = address;
        for (int i = 0, n = columns.size(); i < n; i++) {
            QwpColumnDef col = columns.getQuick(i);
            byte[] nameBytes = col.getNameUtf8();
            pos = QwpVarint.encode(pos, nameBytes.length);
            for (byte b : nameBytes) {
                Unsafe.putByte(pos++, b);
            }
            Unsafe.putByte(pos++, col.getWireTypeCode());
        }
        return pos;
    }

    /**
     * Encodes this schema's inline columns to a byte array.
     *
     * @param buf    destination buffer
     * @param offset starting offset
     * @return offset after the encoded columns
     */
    public int encode(byte[] buf, int offset) {
        for (int i = 0, n = columns.size(); i < n; i++) {
            QwpColumnDef col = columns.getQuick(i);
            byte[] nameBytes = col.getNameUtf8();
            offset = QwpVarint.encode(buf, offset, nameBytes.length);
            System.arraycopy(nameBytes, 0, buf, offset, nameBytes.length);
            offset += nameBytes.length;
            buf[offset++] = col.getWireTypeCode();
        }
        return offset;
    }

    /**
     * Computes the encoded size in bytes for this schema's inline columns.
     *
     * @return encoded size
     */
    public int encodedSize() {
        int size = 0;
        for (int i = 0, n = columns.size(); i < n; i++) {
            QwpColumnDef col = columns.getQuick(i);
            byte[] nameBytes = col.getNameUtf8();
            size += QwpVarint.encodedLength(nameBytes.length);
            size += nameBytes.length;
            size += 1; // type code
        }
        return size;
    }

    /**
     * Gets the column definition at the specified index.
     *
     * @param index column index
     * @return column definition
     */
    public QwpColumnDef getColumn(int index) {
        return columns.getQuick(index);
    }

    /**
     * Gets the number of columns in this schema.
     */
    public int getColumnCount() {
        return columns.size();
    }

    /**
     * Gets all column definitions.
     * <p>
     * Returns the internal list directly (no copy) for zero-allocation access.
     * Callers must not modify the returned list.
     *
     * @return column definitions list (do not modify)
     */
    public ObjList<QwpColumnDef> getColumns() {
        return columns;
    }

    private static void parseColumns(long address, int length, int columnCount, int offset, ParseResult result) throws QwpParseException {
        ObjList<QwpColumnDef> columns = new ObjList<>(columnCount);
        QwpVarint.DecodeResult decodeResult = result.decodeResult;
        long limit = address + length; // Absolute end address

        for (int i = 0; i < columnCount; i++) {
            if (offset >= length) {
                throw QwpParseException.headerTooShort();
            }

            // Parse column name length (varint)
            QwpVarint.decode(address + offset, limit, decodeResult);
            offset += decodeResult.bytesRead;

            // Empty column names (length 0) are allowed for designated timestamp columns.
            if (decodeResult.value < 0 || decodeResult.value > MAX_COLUMN_NAME_LENGTH) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INVALID_COLUMN_NAME,
                        "invalid column name length at column " + i + ": " + decodeResult.value
                );
            }
            int nameLenInt = (int) decodeResult.value;
            if (offset + nameLenInt + 1 > length) {
                throw QwpParseException.headerTooShort();
            }

            // Read column name bytes
            byte[] nameBytes = new byte[nameLenInt];
            for (int j = 0; j < nameLenInt; j++) {
                nameBytes[j] = Unsafe.getByte(address + offset + j);
            }
            String columnName = new String(nameBytes, StandardCharsets.UTF_8);
            offset += nameLenInt;

            // Read column type
            byte typeCode = Unsafe.getByte(address + offset);
            offset++;

            QwpColumnDef colDef = new QwpColumnDef(columnName, typeCode);
            colDef.validate();
            columns.add(colDef);
        }

        result.setSchema(new QwpSchema(columns), offset);
    }

    private static ParseResult parseColumnsFromArray(byte[] buf, int bufOffset, int length, int columnCount, int offset) throws QwpParseException {
        ObjList<QwpColumnDef> columns = new ObjList<>(columnCount);
        QwpVarint.DecodeResult decodeResult = new QwpVarint.DecodeResult();
        int limit = bufOffset + length; // Absolute end position

        for (int i = 0; i < columnCount; i++) {
            if (offset >= length) {
                throw QwpParseException.headerTooShort();
            }

            // Parse column name length (varint)
            QwpVarint.decode(buf, bufOffset + offset, limit, decodeResult);
            offset += decodeResult.bytesRead;

            if (decodeResult.value < 0 || decodeResult.value > MAX_COLUMN_NAME_LENGTH) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INVALID_COLUMN_NAME,
                        "invalid column name length at column " + i + ": " + decodeResult.value
                );
            }
            int nameLenInt = (int) decodeResult.value;
            if (offset + nameLenInt + 1 > length) {
                throw QwpParseException.headerTooShort();
            }

            // Read column name
            String columnName = new String(buf, bufOffset + offset, nameLenInt, StandardCharsets.UTF_8);
            offset += nameLenInt;

            // Read column type
            byte typeCode = buf[bufOffset + offset];
            offset++;

            QwpColumnDef colDef = new QwpColumnDef(columnName, typeCode);
            colDef.validate();
            columns.add(colDef);
        }

        return ParseResult.of(new QwpSchema(columns), offset);
    }

    /**
     * Result of parsing a schema section.
     * <p>
     * This class is mutable and reusable to avoid allocations on hot paths.
     * Use {@link #setSchema} to populate, and {@link #clear} to reset for reuse.
     */
    public static final class ParseResult implements Mutable {
        public final QwpVarint.DecodeResult decodeResult = new QwpVarint.DecodeResult();
        public int bytesConsumed;
        public QwpSchema schema;

        // Factory method for convenience (allocates - use in tests)
        public static ParseResult of(QwpSchema schema, int bytesConsumed) {
            ParseResult result = new ParseResult();
            result.setSchema(schema, bytesConsumed);
            return result;
        }

        @Override
        public void clear() {
            this.schema = null;
            this.bytesConsumed = 0;
        }

        public void setSchema(QwpSchema schema, int bytesConsumed) {
            this.schema = schema;
            this.bytesConsumed = bytesConsumed;
        }
    }
}
