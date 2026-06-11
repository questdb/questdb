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

import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8s;

/**
 * Parser for QWP v1 table headers.
 * <p>
 * Table header structure:
 * <pre>
 * ┌─────────────────────────────┐
 * │ Table name length: varint   │
 * │ Table name: UTF-8 bytes     │
 * │ Row count: varint           │
 * │ Column count: varint        │
 * └─────────────────────────────┘
 * </pre>
 * <p>
 * This class is mutable and reusable. Call {@link #reset()} before parsing a new header.
 * <p>
 * For zero-allocation parsing from direct memory, use {@link #getTableNameUtf8()}.
 * The {@link #getTableName()} method allocates a String on-demand when needed.
 */
public class QwpTableHeader {

    private final QwpVarint.DecodeResult decodeResult = new QwpVarint.DecodeResult();
    private final int maxRowCount;
    private final DirectUtf8String tableNameUtf8 = new DirectUtf8String();
    private int bytesConsumed;
    private int columnCount;
    private long rowCount;
    private String tableNameStr;  // Lazily allocated when getTableName() is called

    public QwpTableHeader() {
        this(QwpConstants.DEFAULT_MAX_ROWS_PER_TABLE);
    }

    public QwpTableHeader(int maxRowCount) {
        if (maxRowCount < 1 || maxRowCount > QwpConstants.DEFAULT_MAX_ROWS_PER_TABLE) {
            throw new IllegalArgumentException("maxRowCount must be between 1 and " + QwpConstants.DEFAULT_MAX_ROWS_PER_TABLE);
        }
        this.maxRowCount = maxRowCount;
    }

    /**
     * Gets the number of bytes consumed during parsing.
     */
    public int getBytesConsumed() {
        return bytesConsumed;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public long getRowCount() {
        return rowCount;
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

        // Validate on long before casting to int to prevent truncation
        // from bypassing the range check
        if (decodeResult.value <= 0) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INVALID_TABLE_NAME,
                    "empty table name"
            );
        }
        if (decodeResult.value > QwpConstants.MAX_TABLE_NAME_LENGTH) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INVALID_TABLE_NAME,
                    "table name too long: " + decodeResult.value + " bytes (max " + QwpConstants.MAX_TABLE_NAME_LENGTH + ")"
            );
        }
        int nameLenInt = (int) decodeResult.value;
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
        if (rowCount > maxRowCount) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.ROW_COUNT_EXCEEDED,
                    "row count exceeds maximum: " + rowCount + " (max " + maxRowCount + ')'
            );
        }
        offset += decodeResult.bytesRead;

        // Parse column count
        if (offset >= length) {
            throw QwpParseException.headerTooShort();
        }
        QwpVarint.decode(address + offset, limit, decodeResult);
        long colCount = decodeResult.value;
        if (colCount > QwpConstants.MAX_COLUMNS_PER_TABLE) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.COLUMN_COUNT_EXCEEDED,
                    "column count exceeds maximum: " + colCount + " (max " + QwpConstants.MAX_COLUMNS_PER_TABLE + ")"
            );
        }
        this.columnCount = (int) colCount;
        offset += decodeResult.bytesRead;

        this.bytesConsumed = offset;
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

    @Override
    public String toString() {
        return "QwpTableHeader{" +
                "tableName='" + getTableName() + '\'' +
                ", rowCount=" + rowCount +
                ", columnCount=" + columnCount +
                '}';
    }
}
