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

package io.questdb.cutlass.http.ilpv4;

import io.questdb.std.Unsafe;

import java.nio.charset.StandardCharsets;

import static io.questdb.cutlass.http.ilpv4.IlpV4Constants.*;

/**
 * Parser for ILP v4 table headers.
 * <p>
 * Table header structure:
 * <pre>
 * ┌─────────────────────────────────────────────────────────────┐
 * │ Table name length: varint                                   │
 * │ Table name: UTF-8 bytes                                     │
 * │ Row count: varint                                           │
 * │ Column count: varint                                        │
 * └─────────────────────────────────────────────────────────────┘
 * </pre>
 * <p>
 * This class is mutable and reusable. Call {@link #reset()} before parsing a new header.
 */
public class IlpV4TableHeader {

    private final IlpV4Varint.DecodeResult decodeResult = new IlpV4Varint.DecodeResult();
    private String tableName;
    private long rowCount;
    private int columnCount;
    private int bytesConsumed;

    /**
     * Parses a table header from direct memory.
     *
     * @param address memory address
     * @param length  available bytes
     * @throws IlpV4ParseException if parsing fails
     */
    public void parse(long address, int length) throws IlpV4ParseException {
        int offset = 0;
        long limit = address + length; // Absolute end address

        // Parse table name length
        IlpV4Varint.decode(address + offset, limit, decodeResult);
        offset += decodeResult.bytesRead;

        int nameLenInt = (int) decodeResult.value;
        if (nameLenInt <= 0) {
            throw IlpV4ParseException.create(
                    IlpV4ParseException.ErrorCode.INVALID_TABLE_NAME,
                    "empty table name"
            );
        }
        if (nameLenInt > MAX_TABLE_NAME_LENGTH) {
            throw IlpV4ParseException.create(
                    IlpV4ParseException.ErrorCode.INVALID_TABLE_NAME,
                    "table name too long: " + nameLenInt + " bytes (max " + MAX_TABLE_NAME_LENGTH + ")"
            );
        }
        if (offset + nameLenInt > length) {
            throw IlpV4ParseException.headerTooShort();
        }

        // Read table name bytes
        byte[] nameBytes = new byte[nameLenInt];
        for (int i = 0; i < nameLenInt; i++) {
            nameBytes[i] = Unsafe.getUnsafe().getByte(address + offset + i);
        }
        this.tableName = new String(nameBytes, StandardCharsets.UTF_8);
        offset += nameLenInt;

        // Parse row count
        if (offset >= length) {
            throw IlpV4ParseException.headerTooShort();
        }
        IlpV4Varint.decode(address + offset, limit, decodeResult);
        this.rowCount = decodeResult.value;
        offset += decodeResult.bytesRead;

        // Parse column count
        if (offset >= length) {
            throw IlpV4ParseException.headerTooShort();
        }
        IlpV4Varint.decode(address + offset, limit, decodeResult);
        long colCount = decodeResult.value;
        if (colCount > MAX_COLUMNS_PER_TABLE) {
            throw IlpV4ParseException.create(
                    IlpV4ParseException.ErrorCode.COLUMN_COUNT_EXCEEDED,
                    "column count exceeds maximum: " + colCount + " (max " + MAX_COLUMNS_PER_TABLE + ")"
            );
        }
        this.columnCount = (int) colCount;
        offset += decodeResult.bytesRead;

        this.bytesConsumed = offset;
    }

    /**
     * Parses a table header from a byte array.
     *
     * @param buf       buffer
     * @param bufOffset starting offset
     * @param length    available bytes
     * @throws IlpV4ParseException if parsing fails
     */
    public void parse(byte[] buf, int bufOffset, int length) throws IlpV4ParseException {
        int offset = 0;
        int limit = bufOffset + length; // Absolute position of end of data

        // Parse table name length
        IlpV4Varint.decode(buf, bufOffset + offset, limit, decodeResult);
        offset += decodeResult.bytesRead;

        int nameLenInt = (int) decodeResult.value;
        if (nameLenInt <= 0) {
            throw IlpV4ParseException.create(
                    IlpV4ParseException.ErrorCode.INVALID_TABLE_NAME,
                    "empty table name"
            );
        }
        if (nameLenInt > MAX_TABLE_NAME_LENGTH) {
            throw IlpV4ParseException.create(
                    IlpV4ParseException.ErrorCode.INVALID_TABLE_NAME,
                    "table name too long: " + nameLenInt + " bytes (max " + MAX_TABLE_NAME_LENGTH + ")"
            );
        }
        if (offset + nameLenInt > length) {
            throw IlpV4ParseException.headerTooShort();
        }

        // Read table name
        this.tableName = new String(buf, bufOffset + offset, nameLenInt, StandardCharsets.UTF_8);
        offset += nameLenInt;

        // Parse row count
        if (offset >= length) {
            throw IlpV4ParseException.headerTooShort();
        }
        IlpV4Varint.decode(buf, bufOffset + offset, limit, decodeResult);
        this.rowCount = decodeResult.value;
        offset += decodeResult.bytesRead;

        // Parse column count
        if (offset >= length) {
            throw IlpV4ParseException.headerTooShort();
        }
        IlpV4Varint.decode(buf, bufOffset + offset, limit, decodeResult);
        long colCount = decodeResult.value;
        if (colCount > MAX_COLUMNS_PER_TABLE) {
            throw IlpV4ParseException.create(
                    IlpV4ParseException.ErrorCode.COLUMN_COUNT_EXCEEDED,
                    "column count exceeds maximum: " + colCount + " (max " + MAX_COLUMNS_PER_TABLE + ")"
            );
        }
        this.columnCount = (int) colCount;
        offset += decodeResult.bytesRead;

        this.bytesConsumed = offset;
    }

    /**
     * Encodes this table header to direct memory.
     *
     * @param address destination address
     * @return address after encoded header
     */
    public long encode(long address) {
        byte[] nameBytes = tableName.getBytes(StandardCharsets.UTF_8);

        long pos = IlpV4Varint.encode(address, nameBytes.length);
        for (byte b : nameBytes) {
            Unsafe.getUnsafe().putByte(pos++, b);
        }

        pos = IlpV4Varint.encode(pos, rowCount);
        pos = IlpV4Varint.encode(pos, columnCount);

        return pos;
    }

    /**
     * Encodes this table header to a byte array.
     *
     * @param buf    destination buffer
     * @param offset starting offset
     * @return offset after encoded header
     */
    public int encode(byte[] buf, int offset) {
        byte[] nameBytes = tableName.getBytes(StandardCharsets.UTF_8);

        offset = IlpV4Varint.encode(buf, offset, nameBytes.length);
        System.arraycopy(nameBytes, 0, buf, offset, nameBytes.length);
        offset += nameBytes.length;

        offset = IlpV4Varint.encode(buf, offset, rowCount);
        offset = IlpV4Varint.encode(buf, offset, columnCount);

        return offset;
    }

    /**
     * Computes the encoded size in bytes for this header.
     *
     * @return encoded size
     */
    public int encodedSize() {
        byte[] nameBytes = tableName.getBytes(StandardCharsets.UTF_8);
        return IlpV4Varint.encodedLength(nameBytes.length) +
                nameBytes.length +
                IlpV4Varint.encodedLength(rowCount) +
                IlpV4Varint.encodedLength(columnCount);
    }

    /**
     * Resets this header for reuse.
     */
    public void reset() {
        tableName = null;
        rowCount = 0;
        columnCount = 0;
        bytesConsumed = 0;
    }

    // ==================== Getters and Setters ====================

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public long getRowCount() {
        return rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public void setColumnCount(int columnCount) {
        this.columnCount = columnCount;
    }

    /**
     * Gets the number of bytes consumed during parsing.
     */
    public int getBytesConsumed() {
        return bytesConsumed;
    }

    @Override
    public String toString() {
        return "IlpV4TableHeader{" +
                "tableName='" + tableName + '\'' +
                ", rowCount=" + rowCount +
                ", columnCount=" + columnCount +
                '}';
    }
}
