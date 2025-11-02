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

package io.questdb.cutlass.binary;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.std.BinarySequence;
import io.questdb.std.Long256;
import io.questdb.std.Mutable;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

import java.nio.charset.StandardCharsets;

/**
 * Streaming Columnar Binary Serializer - Re-entrant State Machine
 * <p>
 * This serializer implements a state machine that can be interrupted at any point
 * when the output buffer fills up and resumed later from the exact same position.
 * <p>
 * Key Design Principles:
 * - Fully re-entrant: Can be called multiple times with different buffers
 * - State preservation: All state is maintained across calls
 * - No blocking: Throws NoSpaceLeftInResponseBufferException when buffer full
 * - Columnar format: Data organized by column within row groups
 * - Streaming: Handles unknown data sizes via row groups
 * - Variable-length optimization: Strings/binary written without per-value length prefixes
 * <p>
 * Usage Pattern:
 * <pre>
 * serializer.of(metadata, cursor, rowGroupSize);
 *
 * while (!serializer.isComplete()) {
 *     try {
 *         sink.of(bufferAddress, bufferSize);
 *         serializer.serialize(sink);
 *     } catch (NoSpaceLeftInResponseBufferException e) {
 *         // Flush buffer to network
 *         flushBuffer(sink.position());
 *         // Continue serializing with next buffer
 *     }
 * }
 * </pre>
 * <p>
 * Binary Format: See SCBF_FORMAT.md for detailed specification
 * <p>
 * State Machine States:
 * 1. HEADER: Write file magic number, version, column count
 * 2. SCHEMA_COLUMN_NAME: Write column name (one column at a time)
 * 3. SCHEMA_COLUMN_TYPE: Write column type
 * 4. SCHEMA_COLUMN_METADATA: Write type-specific metadata
 * 5. ROW_GROUP_START: Begin new row group, read rows into memory
 * 6. ROW_GROUP_HEADER: Write row count for this group
 * 7. COLUMN_NULL_BITMAP_LENGTH: Write null bitmap length
 * 8. COLUMN_NULL_BITMAP_DATA: Write null bitmap bytes
 * 9. COLUMN_OFFSETS_LENGTH: Write offsets array length (variable-length types)
 * 10. COLUMN_OFFSETS_DATA: Write offset values
 * 11. COLUMN_DATA_LENGTH: Write column data length
 * 12. COLUMN_DATA_FIXED: Write fixed-size column data
 * 13. COLUMN_DATA_VARIABLE: Write variable-length column data without prefixes
 * 14. END_MARKER: Write end-of-stream marker
 * 15. COMPLETE: Serialization finished
 */
public class StreamingColumnarSerializer implements Mutable {

    private static final int END_MARKER = -1;
    private static final byte[] MAGIC = "SCBF".getBytes(StandardCharsets.US_ASCII);
    private static final short VERSION = 1;
    // Reusable Utf8Sink adapter
    private final BinaryDataSinkUtf8Adapter utf8Adapter = new BinaryDataSinkUtf8Adapter();
    private boolean complete;
    // Schema serialization state
    private int currentColumn;
    // Column data serialization state
    private int currentColumnInGroup;
    private RecordCursor cursor;
    // Input data
    private RecordMetadata metadata;
    private Record record;
    private int rowsInCurrentGroup;
    // State machine
    private State state = State.UNINITIALIZED;
    private int stateProgress;

    public StreamingColumnarSerializer() {
    }

    @Override
    public void clear() {
        metadata = null;
        cursor = null;
        record = null;
        state = State.UNINITIALIZED;
        stateProgress = 0;
        currentColumn = 0;
        // Keep utf8Buf allocated for reuse
        rowsInCurrentGroup = 0;
        currentColumnInGroup = 0;
        complete = false;
    }

    /**
     * Check if serialization is complete.
     *
     * @return true if all data has been serialized
     */
    public boolean isComplete() {
        return complete;
    }

    /**
     * Initialize serializer with metadata, cursor, and row group size.
     *
     * @param metadata column metadata
     * @param cursor   record cursor to serialize
     */
    public void of(RecordMetadata metadata, RecordCursor cursor) {
        this.metadata = metadata;
        this.cursor = cursor;
        this.record = cursor.getRecord();
        this.state = State.HEADER;
        this.stateProgress = 0;
        this.currentColumn = 0;
        this.complete = false;
    }

    /**
     * Serialize data to the provided sink.
     * This method is re-entrant and can be called multiple times.
     *
     * @param sink binary data sink
     * @throws NoSpaceLeftInResponseBufferException when sink buffer is full
     */
    public void serialize(BinaryDataSink sink) {
        // Configure the adapter with the current sink (in case serializer is reused)
        utf8Adapter.of(sink);

        while (!complete) {
            switch (state) {
                case HEADER:
                    serializeHeader(sink);
                    break;
                case SCHEMA_TYPES:
                    serializeSchemaTypes(sink);
                    break;
                case SCHEMA_NAMES:
                    serializeSchemaNames(sink);
                    break;
                case ROW_GROUP_START:
                    startRowGroup();
                    break;
                case ROW_GROUP_HEADER:
                    serializeRowGroupHeader(sink);
                    break;
                case COLUMN_DATA:
                    serializeColumnData(sink);
                    break;
                case END_MARKER:
                    serializeEndMarker(sink);
                    break;
                default:
                    throw new IllegalStateException("Unknown state: " + state);
            }
        }
    }

    // ===== State Serialization Methods =====

    private void serializeColumnData(BinaryDataSink sink) {
        // For row group size 1, serialize all columns: bitmap (1 byte) + offsets (8 bytes if variable) + data
        // Loop through columns, writing as many as fit in buffer
        // Uses bookmark/rollback to ensure atomicity - either entire column written or nothing

        while (currentColumnInGroup < metadata.getColumnCount()) {
            int bookmark = sink.bookmark();

            try {
                int columnType = metadata.getColumnType(currentColumnInGroup);
                int tag = ColumnType.tagOf(columnType);

                // Null bitmap length can be calculated as (rowCount + 7) / 8, no need to write it
                switch (tag) {
                    case ColumnType.BOOLEAN -> {
                        // Null bitmap: boolean has no null
                        sink.putByte((byte) 0x00);
                        // Data
                        sink.putByte((byte) (record.getBool(currentColumnInGroup) ? 1 : 0));
                    }
                    case ColumnType.BYTE -> {
                        byte value = record.getByte(currentColumnInGroup);
                        // Null bitmap
                        sink.putByte(value == 0 ? (byte) 0x01 : (byte) 0x00);
                        // Data
                        sink.putByte(value);
                    }
                    case ColumnType.GEOBYTE -> {
                        byte value = record.getGeoByte(currentColumnInGroup);
                        // Null bitmap
                        sink.putByte(value == 0 ? (byte) 0x01 : (byte) 0x00);
                        // Data
                        sink.putByte(value);
                    }
                    case ColumnType.SHORT -> {
                        short value = record.getShort(currentColumnInGroup);
                        // Null bitmap
                        sink.putByte(value == 0 ? (byte) 0x01 : (byte) 0x00);
                        // Data
                        sink.putShort(value);
                    }
                    case ColumnType.CHAR -> {
                        char value = record.getChar(currentColumnInGroup);
                        // Null bitmap
                        sink.putByte(value == 0 ? (byte) 0x01 : (byte) 0x00);
                        // Data
                        sink.putShort((short) value);
                    }
                    case ColumnType.GEOSHORT -> {
                        short value = record.getGeoShort(currentColumnInGroup);
                        // Null bitmap
                        sink.putByte(value == 0 ? (byte) 0x01 : (byte) 0x00);
                        // Data
                        sink.putShort(value);
                    }
                    case ColumnType.INT -> {
                        int value = record.getInt(currentColumnInGroup);
                        // Null bitmap
                        sink.putByte(value == Integer.MIN_VALUE ? (byte) 0x01 : (byte) 0x00);
                        // Data
                        sink.putInt(value);
                    }
                    case ColumnType.IPv4 -> {
                        int value = record.getIPv4(currentColumnInGroup);
                        // Null bitmap
                        sink.putByte(value == Integer.MIN_VALUE ? (byte) 0x01 : (byte) 0x00);
                        // Data
                        sink.putInt(value);
                    }
                    case ColumnType.GEOINT -> {
                        int value = record.getGeoInt(currentColumnInGroup);
                        // Null bitmap
                        sink.putByte(value == Integer.MIN_VALUE ? (byte) 0x01 : (byte) 0x00);
                        // Data
                        sink.putInt(value);
                    }
                    case ColumnType.LONG, ColumnType.DATE -> {
                        long value = record.getLong(currentColumnInGroup);
                        // Null bitmap
                        sink.putByte(value == Long.MIN_VALUE ? (byte) 0x01 : (byte) 0x00);
                        // Data
                        sink.putLong(value);
                    }
                    case ColumnType.TIMESTAMP -> {
                        long value = record.getTimestamp(currentColumnInGroup);
                        // Null bitmap
                        sink.putByte(value == Long.MIN_VALUE ? (byte) 0x01 : (byte) 0x00);
                        // Data
                        sink.putLong(value);
                    }
                    case ColumnType.GEOLONG -> {
                        long value = record.getGeoLong(currentColumnInGroup);
                        // Null bitmap
                        sink.putByte(value == Long.MIN_VALUE ? (byte) 0x01 : (byte) 0x00);
                        // Data
                        sink.putLong(value);
                    }
                    case ColumnType.FLOAT -> {
                        float value = record.getFloat(currentColumnInGroup);
                        // Null bitmap
                        sink.putByte(Float.isNaN(value) ? (byte) 0x01 : (byte) 0x00);
                        // Data
                        sink.putFloat(value);
                    }
                    case ColumnType.DOUBLE -> {
                        double value = record.getDouble(currentColumnInGroup);
                        // Null bitmap
                        sink.putByte(Double.isNaN(value) ? (byte) 0x01 : (byte) 0x00);
                        // Data
                        sink.putDouble(value);
                    }
                    case ColumnType.LONG256 -> {
                        Long256 value = record.getLong256A(currentColumnInGroup);
                        // Null bitmap
                        sink.putByte(value == null ? (byte) 0x01 : (byte) 0x00);
                        // Data
                        if (value != null) {
                            sink.putLong(value.getLong0());
                            sink.putLong(value.getLong1());
                            sink.putLong(value.getLong2());
                            sink.putLong(value.getLong3());
                        } else {
                            sink.putLong(0);
                            sink.putLong(0);
                            sink.putLong(0);
                            sink.putLong(0);
                        }
                    }
                    case ColumnType.UUID, ColumnType.LONG128 -> {
                        long lo = record.getLong128Lo(currentColumnInGroup);
                        long hi = record.getLong128Hi(currentColumnInGroup);
                        // Null bitmap
                        sink.putByte(lo == Long.MIN_VALUE ? (byte) 0x01 : (byte) 0x00);
                        // Data
                        sink.putLong128(lo, hi);
                    }
                    case ColumnType.STRING -> {
                        CharSequence value = record.getStrA(currentColumnInGroup);
                        // Null bitmap
                        sink.putByte(value == null ? (byte) 0x01 : (byte) 0x00);
                        // Offsets length is (rowCount+1)*4, no need to write it
                        // Calculate value length
                        int valueLength = value == null ? 0 : Utf8s.utf8Bytes(value);
                        // Offsets: [0, valueLength]
                        sink.putInt(0);
                        sink.putInt(valueLength);
                        // Data
                        if (value != null) {
                            boolean fullyWritten = Utf8s.encodeUtf16WithLimit(utf8Adapter, value, Integer.MAX_VALUE);
                            if (!fullyWritten) {
                                // String didn't fit, throw exception (will be caught and rolled back)
                                throw NoSpaceLeftInResponseBufferException.instance(valueLength);
                            }
                        }
                    }
                    case ColumnType.SYMBOL -> {
                        CharSequence value = record.getSymA(currentColumnInGroup);
                        // Null bitmap
                        sink.putByte(value == null ? (byte) 0x01 : (byte) 0x00);
                        // Offsets length is (rowCount+1)*4, no need to write it
                        // Calculate value length
                        int valueLength = value == null ? 0 : Utf8s.utf8Bytes(value);
                        // Offsets: [0, valueLength]
                        sink.putInt(0);
                        sink.putInt(valueLength);
                        // Data
                        if (value != null) {
                            boolean fullyWritten = Utf8s.encodeUtf16WithLimit(utf8Adapter, value, Integer.MAX_VALUE);
                            if (!fullyWritten) {
                                // String didn't fit, throw exception (will be caught and rolled back)
                                throw NoSpaceLeftInResponseBufferException.instance(valueLength);
                            }
                        }
                    }
                    case ColumnType.VARCHAR -> {
                        Utf8Sequence value = record.getVarcharA(currentColumnInGroup);
                        // Null bitmap
                        sink.putByte(value == null ? (byte) 0x01 : (byte) 0x00);
                        // Offsets length is (rowCount+1)*4, no need to write it
                        // Calculate value length
                        int valueLength = value == null ? 0 : value.size();
                        // Offsets: [0, valueLength]
                        sink.putInt(0);
                        sink.putInt(valueLength);
                        // Data
                        if (value != null) {
                            int size = value.size();
                            for (int i = 0; i < size; i++) {
                                sink.putByte(value.byteAt(i));
                            }
                        }
                    }
                    case ColumnType.BINARY -> {
                        BinarySequence value = record.getBin(currentColumnInGroup);
                        // Null bitmap
                        sink.putByte(value == null ? (byte) 0x01 : (byte) 0x00);
                        // Offsets length is (rowCount+1)*4, no need to write it
                        // Calculate value length
                        int valueLength = value == null ? 0 : (int) value.length();
                        // Offsets: [0, valueLength]
                        sink.putInt(0);
                        sink.putInt(valueLength);
                        // Data
                        if (value != null) {
                            long len = value.length();
                            for (long i = 0; i < len; i++) {
                                sink.putByte(value.byteAt(i));
                            }
                        }
                    }
                }

                // Column written successfully, move to next
                currentColumnInGroup++;
            } catch (NoSpaceLeftInResponseBufferException e) {
                // Not enough space to write entire column, rollback and rethrow
                sink.rollback(bookmark);
                throw e;
            }
        }

        // All columns in row group written, move to next row group
        currentColumnInGroup = 0;
        state = State.ROW_GROUP_START;
    }

    private void serializeEndMarker(BinaryDataSink sink) {
        sink.putInt(END_MARKER);
        complete = true;
        state = State.COMPLETE;
    }

    private void serializeHeader(BinaryDataSink sink) {
        // Write magic number (4 bytes)
        while (stateProgress < MAGIC.length) {
            sink.putByte(MAGIC[stateProgress]);
            stateProgress++;
        }

        // Write version (2 bytes)
        if (stateProgress == MAGIC.length) {
            sink.putShort(VERSION);
            stateProgress++;
        }

        // Write column count (4 bytes)
        if (stateProgress == MAGIC.length + 1) {
            sink.putInt(metadata.getColumnCount());
            stateProgress = 0;
            currentColumn = 0;
            state = State.SCHEMA_TYPES;
        }
    }

    private void serializeRowGroupHeader(BinaryDataSink sink) {
        sink.putInt(rowsInCurrentGroup);
        currentColumnInGroup = 0;
        state = State.COLUMN_DATA;
    }

    private void serializeSchemaNames(BinaryDataSink sink) {
        // Write all column names
        while (currentColumn < metadata.getColumnCount()) {
            String columnName = metadata.getColumnName(currentColumn);

            // Calculate UTF-8 byte length
            int nameLength = Utf8s.utf8Bytes(columnName);

            // Bookmark position in case name doesn't fit
            int bookmark = sink.bookmark();

            try {
                // Write name length (4 bytes)
                sink.putInt(nameLength);

                // Encode UTF-8 directly to sink
                boolean fullyWritten = Utf8s.encodeUtf16WithLimit(utf8Adapter, columnName, nameLength);

                if (!fullyWritten) {
                    // Name didn't fit, rollback and throw
                    sink.rollback(bookmark);
                    throw NoSpaceLeftInResponseBufferException.instance(nameLength + 4);
                }

                // Name written successfully, move to next column
                currentColumn++;
            } catch (NoSpaceLeftInResponseBufferException e) {
                // Rollback and rethrow to allow buffer flush
                sink.rollback(bookmark);
                throw e;
            }
        }

        // All names written, move to row groups
        currentColumn = 0;
        state = State.ROW_GROUP_START;
    }

    private void serializeSchemaTypes(BinaryDataSink sink) {
        // Write all column types (just the int, no metadata needed)
        // The columnType int already encodes everything: type, precision, bits, etc.
        // Uses bookmark/rollback for atomicity

        while (currentColumn < metadata.getColumnCount()) {
            int bookmark = sink.bookmark();

            try {
                int columnType = metadata.getColumnType(currentColumn);
                sink.putInt(columnType);
                currentColumn++;
            } catch (NoSpaceLeftInResponseBufferException e) {
                // Not enough space, rollback and rethrow
                sink.rollback(bookmark);
                throw e;
            }
        }

        // All types written, move to names
        currentColumn = 0;
        state = State.SCHEMA_NAMES;
    }

    private void startRowGroup() {
        // Get next row from cursor - this is our row group of size 1
        // Following the pattern from TestUtils.printSql():
        //   final Record record = cursor.getRecord();
        //   while (cursor.hasNext()) { ... read all columns from record ... }

        if (cursor.hasNext()) {
            // We have one row to serialize (record is now positioned at this row)
            rowsInCurrentGroup = 1;
            currentColumnInGroup = 0;
            state = State.ROW_GROUP_HEADER;
        } else {
            // No more data, write end marker
            rowsInCurrentGroup = 0;
            state = State.END_MARKER;
        }
    }

    // ===== State Machine Enum =====

    private enum State {
        UNINITIALIZED,
        HEADER,                      // Magic, version, column count
        SCHEMA_TYPES,                // All column types + metadata
        SCHEMA_NAMES,                // All column names (optional for reader)
        ROW_GROUP_START,
        ROW_GROUP_HEADER,
        COLUMN_DATA,                 // Null bitmap + offsets (if variable) + data
        END_MARKER,
        COMPLETE
    }
}
