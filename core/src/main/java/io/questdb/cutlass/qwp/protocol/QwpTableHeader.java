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

import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8s;

import java.nio.charset.StandardCharsets;

import static io.questdb.cutlass.qwp.protocol.QwpConstants.*;

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
 * <p>
 * For zero-allocation parsing from direct memory, use {@link #getTableNameUtf8()}.
 * The {@link #getTableName()} method allocates a String on-demand when needed.
 */
public class QwpTableHeader {

    private final QwpVarint.DecodeResult decodeResult = new QwpVarint.DecodeResult();
    private final DirectUtf8String tableNameUtf8 = new DirectUtf8String();
    private String tableNameStr;  // Lazily allocated when getTableName() is called
    private long rowCount;
    private int columnCount;
    private int bytesConsumed;

    /**
     * Parses a table header from direct memory.
     *
     * @param address memory address
     * @param length  available bytes
     * @throws QwpParseException if parsing fails
     */
    public void parse(long address, int length) throws QwpParseException {
        int offset = 0;
        long limit = address + length; // Absolute end address

        // Parse table name length
        QwpVarint.decode(address + offset, limit, decodeResult);
        offset += decodeResult.bytesRead;

        int nameLenInt = (int) decodeResult.value;
        if (nameLenInt <= 0) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INVALID_TABLE_NAME,
                    "empty table name"
            );
        }
        if (nameLenInt > MAX_TABLE_NAME_LENGTH) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INVALID_TABLE_NAME,
                    "table name too long: " + nameLenInt + " bytes (max " + MAX_TABLE_NAME_LENGTH + ")"
            );
        }
        if (offset + nameLenInt > length) {
            throw QwpParseException.headerTooShort();
        }

        // Set up flyweight pointing to table name bytes in wire memory (zero allocation)
        this.tableNameUtf8.of(address + offset, address + offset + nameLenInt);
        this.tableNameStr = null;  // Clear cached String, will be lazily allocated if needed
        offset += nameLenInt;

        // Parse row count
        if (offset >= length) {
            throw QwpParseException.headerTooShort();
        }
        QwpVarint.decode(address + offset, limit, decodeResult);
        this.rowCount = decodeResult.value;
        offset += decodeResult.bytesRead;

        // Parse column count
        if (offset >= length) {
            throw QwpParseException.headerTooShort();
        }
        QwpVarint.decode(address + offset, limit, decodeResult);
        long colCount = decodeResult.value;
        if (colCount > MAX_COLUMNS_PER_TABLE) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.COLUMN_COUNT_EXCEEDED,
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
        long pos;
        if (tableNameUtf8.size() > 0) {
            // Copy directly from flyweight (avoids String allocation)
            int nameLen = tableNameUtf8.size();
            pos = QwpVarint.encode(address, nameLen);
            Unsafe.getUnsafe().copyMemory(tableNameUtf8.ptr(), pos, nameLen);
            pos += nameLen;
        } else {
            byte[] nameBytes = getTableName().getBytes(StandardCharsets.UTF_8);
            pos = QwpVarint.encode(address, nameBytes.length);
            for (byte b : nameBytes) {
                Unsafe.getUnsafe().putByte(pos++, b);
            }
        }

        pos = QwpVarint.encode(pos, rowCount);
        pos = QwpVarint.encode(pos, columnCount);

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
        if (tableNameUtf8.size() > 0) {
            // Copy directly from flyweight
            int nameLen = tableNameUtf8.size();
            offset = QwpVarint.encode(buf, offset, nameLen);
            for (int i = 0; i < nameLen; i++) {
                buf[offset + i] = Unsafe.getUnsafe().getByte(tableNameUtf8.ptr() + i);
            }
            offset += nameLen;
        } else {
            byte[] nameBytes = getTableName().getBytes(StandardCharsets.UTF_8);
            offset = QwpVarint.encode(buf, offset, nameBytes.length);
            System.arraycopy(nameBytes, 0, buf, offset, nameBytes.length);
            offset += nameBytes.length;
        }

        offset = QwpVarint.encode(buf, offset, rowCount);
        offset = QwpVarint.encode(buf, offset, columnCount);

        return offset;
    }

    /**
     * Computes the encoded size in bytes for this header.
     *
     * @return encoded size
     */
    public int encodedSize() {
        int nameLen;
        if (tableNameUtf8.size() > 0) {
            nameLen = tableNameUtf8.size();
        } else {
            nameLen = getTableName().getBytes(StandardCharsets.UTF_8).length;
        }
        return QwpVarint.encodedLength(nameLen) +
                nameLen +
                QwpVarint.encodedLength(rowCount) +
                QwpVarint.encodedLength(columnCount);
    }

    /**
     * Resets this header for reuse.
     */
    public void reset() {
        tableNameUtf8.clear();
        tableNameStr = null;
        rowCount = 0;
        columnCount = 0;
        bytesConsumed = 0;
    }

    // ==================== Getters and Setters ====================

    /**
     * Returns the table name as a UTF-8 sequence (zero allocation).
     * <p>
     * The returned sequence points directly to the wire memory and is valid
     * until the next call to {@link #parse} or {@link #reset}.
     */
    public DirectUtf8Sequence getTableNameUtf8() {
        return tableNameUtf8;
    }

    /**
     * Returns the table name as a String.
     * <p>
     * This method allocates on first call after parsing from direct memory.
     * For zero-allocation access, use {@link #getTableNameUtf8()}.
     */
    public String getTableName() {
        if (tableNameStr == null && tableNameUtf8.size() > 0) {
            tableNameStr = Utf8s.stringFromUtf8Bytes(tableNameUtf8.ptr(), tableNameUtf8.ptr() + tableNameUtf8.size());
        }
        return tableNameStr;
    }

    public void setTableName(String tableName) {
        this.tableNameStr = tableName;
        this.tableNameUtf8.clear();  // Not valid when set from String
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
        return "QwpTableHeader{" +
                "tableName='" + getTableName() + '\'' +
                ", rowCount=" + rowCount +
                ", columnCount=" + columnCount +
                '}';
    }
}
