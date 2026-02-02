/*******************************************************************************
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

package io.questdb.cutlass.ilpv4.client;

import io.questdb.cutlass.ilpv4.protocol.*;

import io.questdb.cutlass.ilpv4.protocol.IlpV4ColumnDef;
import io.questdb.cutlass.ilpv4.protocol.IlpV4GorillaEncoder;

import io.questdb.cutlass.ilpv4.protocol.IlpV4TimestampDecoder;
import io.questdb.std.QuietCloseable;

import static io.questdb.cutlass.ilpv4.protocol.IlpV4Constants.*;

/**
 * Encodes ILP v4 messages for WebSocket transport.
 * <p>
 * This encoder can write to either an internal {@link NativeBufferWriter} (default)
 * or an external {@link IlpBufferWriter} such as {@link io.questdb.cutlass.http.client.WebSocketSendBuffer}.
 * <p>
 * When using an external buffer, the encoder writes directly to it without intermediate copies,
 * enabling zero-copy WebSocket frame construction.
 * <p>
 * Usage with external buffer (zero-copy):
 * <pre>
 * WebSocketSendBuffer buf = client.getSendBuffer();
 * buf.beginBinaryFrame();
 * encoder.setBuffer(buf);
 * encoder.encode(tableData, false);
 * FrameInfo frame = buf.endBinaryFrame();
 * client.sendFrame(frame);
 * </pre>
 */
public class IlpV4WebSocketEncoder implements QuietCloseable {

    private NativeBufferWriter ownedBuffer;
    private IlpBufferWriter buffer;
    private final IlpV4GorillaEncoder gorillaEncoder = new IlpV4GorillaEncoder();
    private byte flags;

    public IlpV4WebSocketEncoder() {
        this.ownedBuffer = new NativeBufferWriter();
        this.buffer = ownedBuffer;
        this.flags = 0;
    }

    public IlpV4WebSocketEncoder(int bufferSize) {
        this.ownedBuffer = new NativeBufferWriter(bufferSize);
        this.buffer = ownedBuffer;
        this.flags = 0;
    }

    /**
     * Returns the underlying buffer.
     * <p>
     * If an external buffer was set via {@link #setBuffer(IlpBufferWriter)},
     * that buffer is returned. Otherwise, returns the internal buffer.
     */
    public IlpBufferWriter getBuffer() {
        return buffer;
    }

    /**
     * Sets an external buffer for encoding.
     * <p>
     * When set, the encoder writes directly to this buffer instead of its internal buffer.
     * The caller is responsible for managing the external buffer's lifecycle.
     * <p>
     * Pass {@code null} to revert to using the internal buffer.
     *
     * @param externalBuffer the external buffer to use, or null to use internal buffer
     */
    public void setBuffer(IlpBufferWriter externalBuffer) {
        this.buffer = externalBuffer != null ? externalBuffer : ownedBuffer;
    }

    /**
     * Returns true if currently using an external buffer.
     */
    public boolean isUsingExternalBuffer() {
        return buffer != ownedBuffer;
    }

    /**
     * Resets the encoder for a new message.
     * <p>
     * If using an external buffer, this only resets the internal state (flags).
     * The external buffer's reset is the caller's responsibility.
     * If using the internal buffer, resets both the buffer and internal state.
     */
    public void reset() {
        if (!isUsingExternalBuffer()) {
            buffer.reset();
        }
    }

    /**
     * Sets whether Gorilla timestamp encoding is enabled.
     */
    public void setGorillaEnabled(boolean enabled) {
        if (enabled) {
            flags |= FLAG_GORILLA;
        } else {
            flags &= ~FLAG_GORILLA;
        }
    }

    /**
     * Returns true if Gorilla encoding is enabled.
     */
    public boolean isGorillaEnabled() {
        return (flags & FLAG_GORILLA) != 0;
    }

    /**
     * Encodes a complete ILP v4 message from a table buffer.
     *
     * @param tableBuffer  the table buffer containing row data
     * @param useSchemaRef whether to use schema reference mode
     * @return the number of bytes written
     */
    public int encode(IlpV4TableBuffer tableBuffer, boolean useSchemaRef) {
        buffer.reset();

        // Write message header with placeholder for payload length
        writeHeader(1, 0);
        int payloadStart = buffer.getPosition();

        // Encode table data
        encodeTable(tableBuffer, useSchemaRef);

        // Patch payload length
        int payloadLength = buffer.getPosition() - payloadStart;
        buffer.patchInt(8, payloadLength);

        return buffer.getPosition();
    }

    /**
     * Encodes a complete ILP v4 message with delta symbol dictionary encoding.
     * <p>
     * This method sends only new symbols (delta) since the last confirmed watermark,
     * and uses global symbol IDs instead of per-column local indices.
     *
     * @param tableBuffer     the table buffer containing row data
     * @param globalDict      the global symbol dictionary
     * @param confirmedMaxId  the highest symbol ID the server has confirmed (from ConnectionSymbolState)
     * @param batchMaxId      the highest symbol ID used in this batch
     * @param useSchemaRef    whether to use schema reference mode
     * @return the number of bytes written
     */
    public int encodeWithDeltaDict(
            IlpV4TableBuffer tableBuffer,
            GlobalSymbolDictionary globalDict,
            int confirmedMaxId,
            int batchMaxId,
            boolean useSchemaRef
    ) {
        buffer.reset();

        // Calculate delta range
        int deltaStart = confirmedMaxId + 1;
        int deltaCount = Math.max(0, batchMaxId - confirmedMaxId);

        // Set delta dictionary flag
        byte savedFlags = flags;
        flags |= FLAG_DELTA_SYMBOL_DICT;

        // Write message header with placeholder for payload length
        writeHeader(1, 0);
        int payloadStart = buffer.getPosition();

        // Write symbol delta section (before tables)
        buffer.putVarint(deltaStart);
        buffer.putVarint(deltaCount);
        for (int id = deltaStart; id < deltaStart + deltaCount; id++) {
            String symbol = globalDict.getSymbol(id);
            buffer.putString(symbol);
        }

        // Encode table data (symbol columns will use global IDs)
        encodeTableWithGlobalSymbols(tableBuffer, useSchemaRef);

        // Patch payload length
        int payloadLength = buffer.getPosition() - payloadStart;
        buffer.patchInt(8, payloadLength);

        // Restore flags
        flags = savedFlags;

        return buffer.getPosition();
    }

    /**
     * Sets the delta symbol dictionary flag.
     */
    public void setDeltaSymbolDictEnabled(boolean enabled) {
        if (enabled) {
            flags |= FLAG_DELTA_SYMBOL_DICT;
        } else {
            flags &= ~FLAG_DELTA_SYMBOL_DICT;
        }
    }

    /**
     * Returns true if delta symbol dictionary encoding is enabled.
     */
    public boolean isDeltaSymbolDictEnabled() {
        return (flags & FLAG_DELTA_SYMBOL_DICT) != 0;
    }

    /**
     * Writes the ILP v4 message header.
     *
     * @param tableCount    number of tables in the message
     * @param payloadLength payload length (can be 0 if patched later)
     */
    public void writeHeader(int tableCount, int payloadLength) {
        // Magic "ILP4"
        buffer.putByte((byte) 'I');
        buffer.putByte((byte) 'L');
        buffer.putByte((byte) 'P');
        buffer.putByte((byte) '4');

        // Version
        buffer.putByte(VERSION_1);

        // Flags
        buffer.putByte(flags);

        // Table count (uint16, little-endian)
        buffer.putShort((short) tableCount);

        // Payload length (uint32, little-endian)
        buffer.putInt(payloadLength);
    }

    /**
     * Encodes a single table from the buffer.
     */
    private void encodeTable(IlpV4TableBuffer tableBuffer, boolean useSchemaRef) {
        IlpV4ColumnDef[] columnDefs = tableBuffer.getColumnDefs();
        int rowCount = tableBuffer.getRowCount();

        if (useSchemaRef) {
            writeTableHeaderWithSchemaRef(
                    tableBuffer.getTableName(),
                    rowCount,
                    tableBuffer.getSchemaHash(),
                    columnDefs.length
            );
        } else {
            writeTableHeaderWithSchema(tableBuffer.getTableName(), rowCount, columnDefs);
        }

        // Write each column's data
        boolean useGorilla = isGorillaEnabled();
        for (int i = 0; i < tableBuffer.getColumnCount(); i++) {
            IlpV4TableBuffer.ColumnBuffer col = tableBuffer.getColumn(i);
            IlpV4ColumnDef colDef = columnDefs[i];
            encodeColumn(col, colDef, rowCount, useGorilla);
        }
    }

    /**
     * Encodes a single table from the buffer using global symbol IDs.
     * This is used with delta dictionary encoding.
     */
    private void encodeTableWithGlobalSymbols(IlpV4TableBuffer tableBuffer, boolean useSchemaRef) {
        IlpV4ColumnDef[] columnDefs = tableBuffer.getColumnDefs();
        int rowCount = tableBuffer.getRowCount();

        if (useSchemaRef) {
            writeTableHeaderWithSchemaRef(
                    tableBuffer.getTableName(),
                    rowCount,
                    tableBuffer.getSchemaHash(),
                    columnDefs.length
            );
        } else {
            writeTableHeaderWithSchema(tableBuffer.getTableName(), rowCount, columnDefs);
        }

        // Write each column's data
        boolean useGorilla = isGorillaEnabled();
        for (int i = 0; i < tableBuffer.getColumnCount(); i++) {
            IlpV4TableBuffer.ColumnBuffer col = tableBuffer.getColumn(i);
            IlpV4ColumnDef colDef = columnDefs[i];
            encodeColumnWithGlobalSymbols(col, colDef, rowCount, useGorilla);
        }
    }

    /**
     * Writes a table header with full schema.
     */
    private void writeTableHeaderWithSchema(String tableName, int rowCount, IlpV4ColumnDef[] columns) {
        // Table name
        buffer.putString(tableName);

        // Row count (varint)
        buffer.putVarint(rowCount);

        // Column count (varint)
        buffer.putVarint(columns.length);

        // Schema mode: full schema (0x00)
        buffer.putByte(SCHEMA_MODE_FULL);

        // Column definitions (name + type for each)
        for (IlpV4ColumnDef col : columns) {
            buffer.putString(col.getName());
            buffer.putByte(col.getWireTypeCode());
        }
    }

    /**
     * Writes a table header with schema reference.
     */
    private void writeTableHeaderWithSchemaRef(String tableName, int rowCount, long schemaHash, int columnCount) {
        // Table name
        buffer.putString(tableName);

        // Row count (varint)
        buffer.putVarint(rowCount);

        // Column count (varint)
        buffer.putVarint(columnCount);

        // Schema mode: reference (0x01)
        buffer.putByte(SCHEMA_MODE_REFERENCE);

        // Schema hash (8 bytes)
        buffer.putLong(schemaHash);
    }

    /**
     * Encodes a single column.
     */
    private void encodeColumn(IlpV4TableBuffer.ColumnBuffer col, IlpV4ColumnDef colDef, int rowCount, boolean useGorilla) {
        int valueCount = col.getValueCount();

        // Write null bitmap if column is nullable
        if (colDef.isNullable()) {
            writeNullBitmapPacked(col.getNullBitmapPacked(), rowCount);
        }

        // Write column data based on type
        switch (col.getType()) {
            case TYPE_BOOLEAN:
                writeBooleanColumn(col.getBooleanValues(), valueCount);
                break;
            case TYPE_BYTE:
                writeByteColumn(col.getByteValues(), valueCount);
                break;
            case TYPE_SHORT:
            case TYPE_CHAR:
                writeShortColumn(col.getShortValues(), valueCount);
                break;
            case TYPE_INT:
                writeIntColumn(col.getIntValues(), valueCount);
                break;
            case TYPE_LONG:
                writeLongColumn(col.getLongValues(), valueCount);
                break;
            case TYPE_FLOAT:
                writeFloatColumn(col.getFloatValues(), valueCount);
                break;
            case TYPE_DOUBLE:
                writeDoubleColumn(col.getDoubleValues(), valueCount);
                break;
            case TYPE_TIMESTAMP:
            case TYPE_TIMESTAMP_NANOS:
                writeTimestampColumn(col.getLongValues(), valueCount, useGorilla);
                break;
            case TYPE_DATE:
                writeLongColumn(col.getLongValues(), valueCount);
                break;
            case TYPE_STRING:
            case TYPE_VARCHAR:
                writeStringColumn(col.getStringValues(), valueCount);
                break;
            case TYPE_SYMBOL:
                writeSymbolColumn(col, valueCount);
                break;
            case TYPE_UUID:
                writeUuidColumn(col.getUuidHigh(), col.getUuidLow(), valueCount);
                break;
            case TYPE_LONG256:
                writeLong256Column(col.getLong256Values(), valueCount);
                break;
            case TYPE_DOUBLE_ARRAY:
                writeDoubleArrayColumn(col, valueCount);
                break;
            case TYPE_LONG_ARRAY:
                writeLongArrayColumn(col, valueCount);
                break;
            case TYPE_DECIMAL64:
                writeDecimal64Column(col.getDecimalScale(), col.getDecimal64Values(), valueCount);
                break;
            case TYPE_DECIMAL128:
                writeDecimal128Column(col.getDecimalScale(), col.getDecimal128High(), col.getDecimal128Low(), valueCount);
                break;
            case TYPE_DECIMAL256:
                writeDecimal256Column(col.getDecimalScale(),
                        col.getDecimal256Hh(), col.getDecimal256Hl(),
                        col.getDecimal256Lh(), col.getDecimal256Ll(), valueCount);
                break;
            default:
                throw new IllegalStateException("Unknown column type: " + col.getType());
        }
    }

    /**
     * Encodes a single column using global symbol IDs for SYMBOL type.
     * All other column types are encoded the same as encodeColumn.
     */
    private void encodeColumnWithGlobalSymbols(IlpV4TableBuffer.ColumnBuffer col, IlpV4ColumnDef colDef, int rowCount, boolean useGorilla) {
        int valueCount = col.getValueCount();

        // Write null bitmap if column is nullable
        if (colDef.isNullable()) {
            writeNullBitmapPacked(col.getNullBitmapPacked(), rowCount);
        }

        // For symbol columns, use global IDs; for all others, use standard encoding
        if (col.getType() == TYPE_SYMBOL) {
            writeSymbolColumnWithGlobalIds(col, valueCount);
        } else {
            // Write column data based on type (same as encodeColumn)
            switch (col.getType()) {
                case TYPE_BOOLEAN:
                    writeBooleanColumn(col.getBooleanValues(), valueCount);
                    break;
                case TYPE_BYTE:
                    writeByteColumn(col.getByteValues(), valueCount);
                    break;
                case TYPE_SHORT:
                case TYPE_CHAR:
                    writeShortColumn(col.getShortValues(), valueCount);
                    break;
                case TYPE_INT:
                    writeIntColumn(col.getIntValues(), valueCount);
                    break;
                case TYPE_LONG:
                    writeLongColumn(col.getLongValues(), valueCount);
                    break;
                case TYPE_FLOAT:
                    writeFloatColumn(col.getFloatValues(), valueCount);
                    break;
                case TYPE_DOUBLE:
                    writeDoubleColumn(col.getDoubleValues(), valueCount);
                    break;
                case TYPE_TIMESTAMP:
                case TYPE_TIMESTAMP_NANOS:
                    writeTimestampColumn(col.getLongValues(), valueCount, useGorilla);
                    break;
                case TYPE_DATE:
                    writeLongColumn(col.getLongValues(), valueCount);
                    break;
                case TYPE_STRING:
                case TYPE_VARCHAR:
                    writeStringColumn(col.getStringValues(), valueCount);
                    break;
                case TYPE_UUID:
                    writeUuidColumn(col.getUuidHigh(), col.getUuidLow(), valueCount);
                    break;
                case TYPE_LONG256:
                    writeLong256Column(col.getLong256Values(), valueCount);
                    break;
                case TYPE_DOUBLE_ARRAY:
                    writeDoubleArrayColumn(col, valueCount);
                    break;
                case TYPE_LONG_ARRAY:
                    writeLongArrayColumn(col, valueCount);
                    break;
                case TYPE_DECIMAL64:
                    writeDecimal64Column(col.getDecimalScale(), col.getDecimal64Values(), valueCount);
                    break;
                case TYPE_DECIMAL128:
                    writeDecimal128Column(col.getDecimalScale(), col.getDecimal128High(), col.getDecimal128Low(), valueCount);
                    break;
                case TYPE_DECIMAL256:
                    writeDecimal256Column(col.getDecimalScale(),
                            col.getDecimal256Hh(), col.getDecimal256Hl(),
                            col.getDecimal256Lh(), col.getDecimal256Ll(), valueCount);
                    break;
                default:
                    throw new IllegalStateException("Unknown column type: " + col.getType());
            }
        }
    }

    /**
     * Writes a null bitmap from bit-packed long array.
     */
    private void writeNullBitmapPacked(long[] nullsPacked, int count) {
        int bitmapSize = (count + 7) / 8;

        for (int byteIdx = 0; byteIdx < bitmapSize; byteIdx++) {
            int longIndex = byteIdx >>> 3;
            int byteInLong = byteIdx & 7;
            byte b = (byte) ((nullsPacked[longIndex] >>> (byteInLong * 8)) & 0xFF);
            buffer.putByte(b);
        }
    }

    /**
     * Writes boolean column data (bit-packed).
     */
    private void writeBooleanColumn(boolean[] values, int count) {
        int packedSize = (count + 7) / 8;

        for (int i = 0; i < packedSize; i++) {
            byte b = 0;
            for (int bit = 0; bit < 8; bit++) {
                int idx = i * 8 + bit;
                if (idx < count && values[idx]) {
                    b |= (1 << bit);
                }
            }
            buffer.putByte(b);
        }
    }

    private void writeByteColumn(byte[] values, int count) {
        for (int i = 0; i < count; i++) {
            buffer.putByte(values[i]);
        }
    }

    private void writeShortColumn(short[] values, int count) {
        for (int i = 0; i < count; i++) {
            buffer.putShort(values[i]);
        }
    }

    private void writeIntColumn(int[] values, int count) {
        for (int i = 0; i < count; i++) {
            buffer.putInt(values[i]);
        }
    }

    private void writeLongColumn(long[] values, int count) {
        for (int i = 0; i < count; i++) {
            buffer.putLong(values[i]);
        }
    }

    private void writeFloatColumn(float[] values, int count) {
        for (int i = 0; i < count; i++) {
            buffer.putFloat(values[i]);
        }
    }

    private void writeDoubleColumn(double[] values, int count) {
        for (int i = 0; i < count; i++) {
            buffer.putDouble(values[i]);
        }
    }

    /**
     * Writes a timestamp column with optional Gorilla compression.
     * <p>
     * When Gorilla encoding is enabled and applicable (3+ timestamps with
     * delta-of-deltas fitting in 32-bit range), uses delta-of-delta compression.
     * Otherwise, falls back to uncompressed encoding.
     */
    private void writeTimestampColumn(long[] values, int count, boolean useGorilla) {
        if (useGorilla && count > 2 && IlpV4GorillaEncoder.canUseGorilla(values, count)) {
            // Write Gorilla encoding flag
            buffer.putByte(IlpV4TimestampDecoder.ENCODING_GORILLA);

            // Calculate size needed and ensure buffer has capacity
            int encodedSize = IlpV4GorillaEncoder.calculateEncodedSize(values, count);
            buffer.ensureCapacity(encodedSize);

            // Encode timestamps to buffer
            int bytesWritten = gorillaEncoder.encodeTimestamps(
                    buffer.getBufferPtr() + buffer.getPosition(),
                    buffer.getCapacity() - buffer.getPosition(),
                    values,
                    count
            );
            buffer.skip(bytesWritten);
        } else {
            // Write uncompressed
            if (useGorilla) {
                buffer.putByte(IlpV4TimestampDecoder.ENCODING_UNCOMPRESSED);
            }
            writeLongColumn(values, count);
        }
    }

    /**
     * Writes a string column with offset array.
     */
    private void writeStringColumn(String[] strings, int count) {
        // Calculate total data length
        int totalDataLen = 0;
        for (int i = 0; i < count; i++) {
            if (strings[i] != null) {
                totalDataLen += IlpBufferWriter.utf8Length(strings[i]);
            }
        }

        // Write offset array
        int runningOffset = 0;
        buffer.putInt(0);
        for (int i = 0; i < count; i++) {
            if (strings[i] != null) {
                runningOffset += IlpBufferWriter.utf8Length(strings[i]);
            }
            buffer.putInt(runningOffset);
        }

        // Write string data
        for (int i = 0; i < count; i++) {
            if (strings[i] != null) {
                buffer.putUtf8(strings[i]);
            }
        }
    }

    /**
     * Writes a symbol column with dictionary.
     * Format:
     * - Dictionary length (varint)
     * - Dictionary entries (length-prefixed UTF-8 strings)
     * - Symbol indices (varints, one per value)
     */
    private void writeSymbolColumn(IlpV4TableBuffer.ColumnBuffer col, int count) {
        // Get symbol data from column buffer
        int[] symbolIndices = col.getSymbolIndices();
        String[] dictionary = col.getSymbolDictionary();

        // Write dictionary
        buffer.putVarint(dictionary.length);
        for (String symbol : dictionary) {
            buffer.putString(symbol);
        }

        // Write symbol indices (one per non-null value)
        for (int i = 0; i < count; i++) {
            buffer.putVarint(symbolIndices[i]);
        }
    }

    /**
     * Writes a symbol column using global IDs (for delta dictionary mode).
     * Format:
     * - Global symbol IDs (varints, one per value)
     * <p>
     * The dictionary is not included here because it's written at the message level
     * in delta format.
     */
    private void writeSymbolColumnWithGlobalIds(IlpV4TableBuffer.ColumnBuffer col, int count) {
        int[] globalIds = col.getGlobalSymbolIds();
        if (globalIds == null) {
            // Fall back to local indices if no global IDs stored
            int[] symbolIndices = col.getSymbolIndices();
            for (int i = 0; i < count; i++) {
                buffer.putVarint(symbolIndices[i]);
            }
        } else {
            // Write global symbol IDs
            for (int i = 0; i < count; i++) {
                buffer.putVarint(globalIds[i]);
            }
        }
    }

    private void writeUuidColumn(long[] highBits, long[] lowBits, int count) {
        // Little-endian: lo first, then hi
        for (int i = 0; i < count; i++) {
            buffer.putLong(lowBits[i]);
            buffer.putLong(highBits[i]);
        }
    }

    private void writeLong256Column(long[] values, int count) {
        // Flat array: 4 longs per value, little-endian (least significant first)
        // values layout: [long0, long1, long2, long3] per row
        for (int i = 0; i < count * 4; i++) {
            buffer.putLong(values[i]);
        }
    }

    private void writeDoubleArrayColumn(IlpV4TableBuffer.ColumnBuffer col, int count) {
        byte[] dims = col.getArrayDims();
        int[] shapes = col.getArrayShapes();
        double[] data = col.getDoubleArrayData();

        int shapeIdx = 0;
        int dataIdx = 0;
        for (int row = 0; row < count; row++) {
            int nDims = dims[row];
            buffer.putByte((byte) nDims);

            int elemCount = 1;
            for (int d = 0; d < nDims; d++) {
                int dimLen = shapes[shapeIdx++];
                buffer.putInt(dimLen);
                elemCount *= dimLen;
            }

            for (int e = 0; e < elemCount; e++) {
                buffer.putDouble(data[dataIdx++]);
            }
        }
    }

    private void writeLongArrayColumn(IlpV4TableBuffer.ColumnBuffer col, int count) {
        byte[] dims = col.getArrayDims();
        int[] shapes = col.getArrayShapes();
        long[] data = col.getLongArrayData();

        int shapeIdx = 0;
        int dataIdx = 0;
        for (int row = 0; row < count; row++) {
            int nDims = dims[row];
            buffer.putByte((byte) nDims);

            int elemCount = 1;
            for (int d = 0; d < nDims; d++) {
                int dimLen = shapes[shapeIdx++];
                buffer.putInt(dimLen);
                elemCount *= dimLen;
            }

            for (int e = 0; e < elemCount; e++) {
                buffer.putLong(data[dataIdx++]);
            }
        }
    }

    private void writeDecimal64Column(byte scale, long[] values, int count) {
        buffer.putByte(scale);
        for (int i = 0; i < count; i++) {
            buffer.putLongBE(values[i]);
        }
    }

    private void writeDecimal128Column(byte scale, long[] high, long[] low, int count) {
        buffer.putByte(scale);
        for (int i = 0; i < count; i++) {
            buffer.putLongBE(high[i]);
            buffer.putLongBE(low[i]);
        }
    }

    private void writeDecimal256Column(byte scale, long[] hh, long[] hl, long[] lh, long[] ll, int count) {
        buffer.putByte(scale);
        for (int i = 0; i < count; i++) {
            buffer.putLongBE(hh[i]);
            buffer.putLongBE(hl[i]);
            buffer.putLongBE(lh[i]);
            buffer.putLongBE(ll[i]);
        }
    }

    @Override
    public void close() {
        if (ownedBuffer != null) {
            ownedBuffer.close();
            ownedBuffer = null;
        }
    }
}
