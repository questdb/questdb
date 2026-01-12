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

import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;

import static io.questdb.cutlass.http.ilpv4.IlpV4Constants.*;

/**
 * Encodes ILP v4 messages for sending.
 * <p>
 * This encoder produces the binary wire format defined in the ILP v4 specification.
 * It handles message headers, table blocks, schemas, and column data.
 * <p>
 * The encoder writes directly to an {@link HttpClient.Request} buffer.
 */
public class IlpV4MessageEncoder implements Closeable {

    private static final int GORILLA_BUFFER_SIZE = 64 * 1024; // 64KB for Gorilla encoding

    private HttpClient.Request request;
    private byte flags;
    // Pooled BitWriter to avoid allocation on every timestamp column
    private final IlpV4BitWriter bitWriter = new IlpV4BitWriter();
    // Temporary buffer for Gorilla encoding
    private long gorillaBuffer;
    private int gorillaBufferCapacity;

    /**
     * Creates an encoder that writes to the provided request.
     *
     * @param request the HTTP request to write to
     */
    public IlpV4MessageEncoder(HttpClient.Request request) {
        this.request = request;
        this.flags = 0;
    }

    /**
     * Sets the HTTP request for this encoder.
     * <p>
     * This allows reusing the encoder with different requests (e.g., for HTTP where
     * each request has its own buffer).
     *
     * @param request the HTTP request to write to
     */
    public void setRequest(HttpClient.Request request) {
        this.request = request;
    }

    /**
     * Returns the HTTP request.
     */
    public HttpClient.Request getRequest() {
        return request;
    }

    /**
     * Resets the encoder for a new message.
     */
    public void reset() {
        flags = 0;
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
     * Writes the message header.
     *
     * @param tableCount    number of tables in the message
     * @param payloadLength total payload length (not including header)
     */
    public void writeHeader(int tableCount, int payloadLength) {
        // Magic "ILP4"
        request.putByte((byte) 'I');
        request.putByte((byte) 'L');
        request.putByte((byte) 'P');
        request.putByte((byte) '4');

        // Version
        request.putByte(VERSION_1);

        // Flags
        request.putByte(flags);

        // Table count (uint16)
        request.putShort((short) tableCount);

        // Payload length (uint32)
        request.putInt(payloadLength);
    }

    /**
     * Updates the payload length in the header.
     * <p>
     * This should be called after encoding the message body if the
     * payload length was not known when writeHeader was called.
     *
     * @param payloadLength the actual payload length
     */
    public void updatePayloadLength(int payloadLength) {
        // Payload length is at offset 8 in the header (from content start)
        Unsafe.getUnsafe().putInt(request.getContentStart() + 8, payloadLength);
    }

    /**
     * Writes a table header with full schema.
     *
     * @param tableName  table name
     * @param rowCount   number of rows
     * @param columns    column definitions
     */
    public void writeTableHeaderWithSchema(String tableName, int rowCount, IlpV4ColumnDef[] columns) {
        // Table name
        writeString(tableName);

        // Row count (varint)
        writeVarint(rowCount);

        // Column count (varint) - BEFORE schema mode!
        writeVarint(columns.length);

        // Schema mode: full schema (0x00)
        writeByte(SCHEMA_MODE_FULL);

        // Column definitions (name + type for each)
        for (IlpV4ColumnDef col : columns) {
            writeString(col.getName());
            writeByte(col.getWireTypeCode());
        }
    }

    /**
     * Writes a table header with schema reference.
     *
     * @param tableName   table name
     * @param rowCount    number of rows
     * @param schemaHash  the schema hash
     * @param columnCount number of columns (must match cached schema)
     */
    public void writeTableHeaderWithSchemaRef(String tableName, int rowCount, long schemaHash, int columnCount) {
        // Table name
        writeString(tableName);

        // Row count (varint)
        writeVarint(rowCount);

        // Column count (varint) - BEFORE schema mode!
        writeVarint(columnCount);

        // Schema mode: reference (0x01)
        writeByte(SCHEMA_MODE_REFERENCE);

        // Schema hash (8 bytes)
        writeLong(schemaHash);
    }

    /**
     * Writes a null bitmap.
     *
     * @param nulls  boolean array where true = null
     * @param count  number of values
     */
    public void writeNullBitmap(boolean[] nulls, int count) {
        int bitmapSize = (count + 7) / 8;

        for (int i = 0; i < bitmapSize; i++) {
            byte b = 0;
            for (int bit = 0; bit < 8; bit++) {
                int idx = i * 8 + bit;
                if (idx < count && nulls[idx]) {
                    b |= (1 << bit);
                }
            }
            request.putByte(b);
        }
    }

    /**
     * Writes a null bitmap from bit-packed long array.
     *
     * @param nullsPacked  bit-packed null bitmap
     * @param count        number of values (rows)
     */
    public void writeNullBitmapPacked(long[] nullsPacked, int count) {
        int bitmapSize = (count + 7) / 8;

        for (int byteIdx = 0; byteIdx < bitmapSize; byteIdx++) {
            int longIndex = byteIdx >>> 3;  // byteIdx / 8
            int byteInLong = byteIdx & 7;   // byteIdx % 8
            byte b = (byte) ((nullsPacked[longIndex] >>> (byteInLong * 8)) & 0xFF);
            request.putByte(b);
        }
    }

    /**
     * Writes boolean column data (bit-packed).
     */
    public void writeBooleanColumn(boolean[] values, int count) {
        int packedSize = (count + 7) / 8;

        for (int i = 0; i < packedSize; i++) {
            byte b = 0;
            for (int bit = 0; bit < 8; bit++) {
                int idx = i * 8 + bit;
                if (idx < count && values[idx]) {
                    b |= (1 << bit);
                }
            }
            request.putByte(b);
        }
    }

    /**
     * Writes a byte column.
     */
    public void writeByteColumn(byte[] values, int count) {
        for (int i = 0; i < count; i++) {
            request.putByte(values[i]);
        }
    }

    /**
     * Writes a short column.
     */
    public void writeShortColumn(short[] values, int count) {
        for (int i = 0; i < count; i++) {
            request.putShort(values[i]);
        }
    }

    /**
     * Writes an int column.
     */
    public void writeIntColumn(int[] values, int count) {
        for (int i = 0; i < count; i++) {
            request.putInt(values[i]);
        }
    }

    /**
     * Writes a long column.
     */
    public void writeLongColumn(long[] values, int count) {
        for (int i = 0; i < count; i++) {
            request.putLong(values[i]);
        }
    }

    /**
     * Writes a float column.
     */
    public void writeFloatColumn(float[] values, int count) {
        for (int i = 0; i < count; i++) {
            request.putFloat(values[i]);
        }
    }

    /**
     * Writes a double column.
     */
    public void writeDoubleColumn(double[] values, int count) {
        for (int i = 0; i < count; i++) {
            request.putDouble(values[i]);
        }
    }

    /**
     * Writes a timestamp column with optional Gorilla encoding.
     */
    public void writeTimestampColumn(long[] values, boolean[] nulls, int rowCount, int valueCount, boolean useGorilla) {
        if (useGorilla && valueCount > 2 && canUseGorilla(values, valueCount)) {
            // Use Gorilla encoding
            writeByte(IlpV4TimestampDecoder.ENCODING_GORILLA);

            // Write first timestamp
            request.putLong(values[0]);

            if (valueCount == 1) {
                return;
            }

            // Write second timestamp
            request.putLong(values[1]);

            if (valueCount == 2) {
                return;
            }

            // Encode remaining timestamps using delta-of-delta into temporary buffer
            ensureGorillaBuffer();
            bitWriter.reset(gorillaBuffer, gorillaBufferCapacity);

            long prevTimestamp = values[1];
            long prevDelta = values[1] - values[0];

            for (int i = 2; i < valueCount; i++) {
                long delta = values[i] - prevTimestamp;
                long deltaOfDelta = delta - prevDelta;

                encodeDoD(bitWriter, deltaOfDelta);

                prevDelta = delta;
                prevTimestamp = values[i];
            }

            // Flush and copy to request
            int bytesWritten = bitWriter.finish();
            request.putBlockOfBytes(gorillaBuffer, bytesWritten);
        } else {
            if (useGorilla) {
                writeByte(IlpV4TimestampDecoder.ENCODING_UNCOMPRESSED);
            }
            writeLongColumn(values, valueCount);
        }
    }

    /**
     * Ensures the Gorilla buffer is allocated.
     */
    private void ensureGorillaBuffer() {
        if (gorillaBuffer == 0) {
            gorillaBufferCapacity = GORILLA_BUFFER_SIZE;
            gorillaBuffer = Unsafe.malloc(gorillaBufferCapacity, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Checks if Gorilla encoding can be safely used for the given timestamps.
     */
    private boolean canUseGorilla(long[] values, int count) {
        if (count < 3) {
            return true;
        }

        long prevDelta = values[1] - values[0];
        for (int i = 2; i < count; i++) {
            long delta = values[i] - values[i - 1];
            long deltaOfDelta = delta - prevDelta;
            if (deltaOfDelta < Integer.MIN_VALUE || deltaOfDelta > Integer.MAX_VALUE) {
                return false;
            }
            prevDelta = delta;
        }
        return true;
    }

    /**
     * Encodes a delta-of-delta value to the bit writer.
     */
    private static void encodeDoD(IlpV4BitWriter writer, long deltaOfDelta) {
        if (deltaOfDelta == 0) {
            writer.writeBit(0);
        } else if (deltaOfDelta >= -63 && deltaOfDelta <= 64) {
            writer.writeBit(1);
            writer.writeBit(0);
            writer.writeSigned(deltaOfDelta, 7);
        } else if (deltaOfDelta >= -255 && deltaOfDelta <= 256) {
            writer.writeBit(1);
            writer.writeBit(1);
            writer.writeBit(0);
            writer.writeSigned(deltaOfDelta, 9);
        } else if (deltaOfDelta >= -2047 && deltaOfDelta <= 2048) {
            writer.writeBit(1);
            writer.writeBit(1);
            writer.writeBit(1);
            writer.writeBit(0);
            writer.writeSigned(deltaOfDelta, 12);
        } else {
            writer.writeBit(1);
            writer.writeBit(1);
            writer.writeBit(1);
            writer.writeBit(1);
            writer.writeSigned(deltaOfDelta, 32);
        }
    }

    /**
     * Writes a string column with offset array.
     */
    public void writeStringColumn(String[] strings, int count) {
        // Calculate total data length
        int totalDataLen = 0;
        for (int i = 0; i < count; i++) {
            if (strings[i] != null) {
                totalDataLen += utf8Length(strings[i]);
            }
        }

        // Write offset array
        int runningOffset = 0;
        request.putInt(0);
        for (int i = 0; i < count; i++) {
            if (strings[i] != null) {
                runningOffset += utf8Length(strings[i]);
            }
            request.putInt(runningOffset);
        }

        // Write string data
        for (int i = 0; i < count; i++) {
            if (strings[i] != null) {
                encodeUtf8Direct(strings[i]);
            }
        }
    }

    /**
     * Calculates the UTF-8 encoded length of a string.
     */
    private static int utf8Length(String s) {
        int len = 0;
        for (int i = 0, n = s.length(); i < n; i++) {
            char c = s.charAt(i);
            if (c < 0x80) {
                len++;
            } else if (c < 0x800) {
                len += 2;
            } else if (c >= 0xD800 && c <= 0xDBFF && i + 1 < n) {
                i++;
                len += 4;
            } else {
                len += 3;
            }
        }
        return len;
    }

    /**
     * Encodes a string as UTF-8 directly to the request.
     */
    private void encodeUtf8Direct(String s) {
        for (int i = 0, n = s.length(); i < n; i++) {
            char c = s.charAt(i);
            if (c < 0x80) {
                request.putByte((byte) c);
            } else if (c < 0x800) {
                request.putByte((byte) (0xC0 | (c >> 6)));
                request.putByte((byte) (0x80 | (c & 0x3F)));
            } else if (c >= 0xD800 && c <= 0xDBFF && i + 1 < n) {
                char c2 = s.charAt(++i);
                int codePoint = 0x10000 + ((c - 0xD800) << 10) + (c2 - 0xDC00);
                request.putByte((byte) (0xF0 | (codePoint >> 18)));
                request.putByte((byte) (0x80 | ((codePoint >> 12) & 0x3F)));
                request.putByte((byte) (0x80 | ((codePoint >> 6) & 0x3F)));
                request.putByte((byte) (0x80 | (codePoint & 0x3F)));
            } else {
                request.putByte((byte) (0xE0 | (c >> 12)));
                request.putByte((byte) (0x80 | ((c >> 6) & 0x3F)));
                request.putByte((byte) (0x80 | (c & 0x3F)));
            }
        }
    }

    /**
     * Writes a symbol column with dictionary.
     */
    public void writeSymbolColumn(int[] symbolIndices, String[] dictionary, int count) {
        // Write dictionary
        writeVarint(dictionary.length);
        for (String symbol : dictionary) {
            writeString(symbol);
        }

        // Write symbol indices
        for (int i = 0; i < count; i++) {
            writeVarint(symbolIndices[i]);
        }
    }

    /**
     * Writes UUID column data (big-endian).
     */
    public void writeUuidColumn(long[] highBits, long[] lowBits, int count) {
        for (int i = 0; i < count; i++) {
            request.putLongBE(highBits[i]);
            request.putLongBE(lowBits[i]);
        }
    }

    /**
     * Writes a double array column.
     * Each row has: [nDims (1B)] [dim1..dimN (4B each)] [flattened values (8B each, LE)]
     *
     * @param dims        nDims per row
     * @param shapes      flattened shape data (all dimensions concatenated)
     * @param data        flattened double values
     * @param valueCount  number of non-null values
     * @param shapeOffset number of shape ints used
     * @param dataOffset  number of data elements used
     */
    public void writeDoubleArrayColumn(byte[] dims, int[] shapes, double[] data,
                                       int valueCount, int shapeOffset, int dataOffset) {
        // Write each row's array
        int shapeIdx = 0;
        int dataIdx = 0;
        for (int row = 0; row < valueCount; row++) {
            int nDims = dims[row];
            request.putByte((byte) nDims);

            int elemCount = 1;
            for (int d = 0; d < nDims; d++) {
                int dimLen = shapes[shapeIdx++];
                request.putInt(dimLen);
                elemCount *= dimLen;
            }

            for (int e = 0; e < elemCount; e++) {
                request.putDouble(data[dataIdx++]);
            }
        }
    }

    /**
     * Writes a long array column.
     * Each row has: [nDims (1B)] [dim1..dimN (4B each)] [flattened values (8B each, LE)]
     *
     * @param dims        nDims per row
     * @param shapes      flattened shape data (all dimensions concatenated)
     * @param data        flattened long values
     * @param valueCount  number of non-null values
     * @param shapeOffset number of shape ints used
     * @param dataOffset  number of data elements used
     */
    public void writeLongArrayColumn(byte[] dims, int[] shapes, long[] data,
                                     int valueCount, int shapeOffset, int dataOffset) {
        // Write each row's array
        int shapeIdx = 0;
        int dataIdx = 0;
        for (int row = 0; row < valueCount; row++) {
            int nDims = dims[row];
            request.putByte((byte) nDims);

            int elemCount = 1;
            for (int d = 0; d < nDims; d++) {
                int dimLen = shapes[shapeIdx++];
                request.putInt(dimLen);
                elemCount *= dimLen;
            }

            for (int e = 0; e < elemCount; e++) {
                request.putLong(data[dataIdx++]);
            }
        }
    }

    // ==================== Decimal column encoding ====================

    /**
     * Writes a Decimal64 column.
     * <p>
     * Wire format: [scale (1B)] + [values (8B each, big-endian)]
     *
     * @param scale the decimal scale (shared by all values)
     * @param values the unscaled 64-bit values
     * @param valueCount number of values to write
     */
    public void writeDecimal64Column(byte scale, long[] values, int valueCount) {
        // Write scale (shared for entire column)
        request.putByte(scale);

        // Write values (big-endian for decimal compatibility)
        for (int i = 0; i < valueCount; i++) {
            request.putLongBE(values[i]);
        }
    }

    /**
     * Writes a Decimal128 column.
     * <p>
     * Wire format: [scale (1B)] + [values (16B each: high then low, big-endian)]
     *
     * @param scale the decimal scale (shared by all values)
     * @param high the high 64 bits of each value
     * @param low the low 64 bits of each value
     * @param valueCount number of values to write
     */
    public void writeDecimal128Column(byte scale, long[] high, long[] low, int valueCount) {
        // Write scale (shared for entire column)
        request.putByte(scale);

        // Write values (big-endian: high then low)
        for (int i = 0; i < valueCount; i++) {
            request.putLongBE(high[i]);
            request.putLongBE(low[i]);
        }
    }

    /**
     * Writes a Decimal256 column.
     * <p>
     * Wire format: [scale (1B)] + [values (32B each: hh, hl, lh, ll, big-endian)]
     *
     * @param scale the decimal scale (shared by all values)
     * @param hh bits 255-192 of each value
     * @param hl bits 191-128 of each value
     * @param lh bits 127-64 of each value
     * @param ll bits 63-0 of each value
     * @param valueCount number of values to write
     */
    public void writeDecimal256Column(byte scale, long[] hh, long[] hl, long[] lh, long[] ll, int valueCount) {
        // Write scale (shared for entire column)
        request.putByte(scale);

        // Write values (big-endian: hh, hl, lh, ll)
        for (int i = 0; i < valueCount; i++) {
            request.putLongBE(hh[i]);
            request.putLongBE(hl[i]);
            request.putLongBE(lh[i]);
            request.putLongBE(ll[i]);
        }
    }

    /**
     * Writes a single byte.
     */
    public void writeByte(byte value) {
        request.putByte(value);
    }

    /**
     * Writes a short (little-endian).
     */
    public void writeShort(short value) {
        request.putShort(value);
    }

    /**
     * Writes an int (little-endian).
     */
    public void writeInt(int value) {
        request.putInt(value);
    }

    /**
     * Writes a long (little-endian).
     */
    public void writeLong(long value) {
        request.putLong(value);
    }

    /**
     * Writes a long in big-endian order.
     */
    public void writeLongBigEndian(long value) {
        request.putLongBE(value);
    }

    /**
     * Writes a string as length-prefixed UTF-8.
     */
    public void writeString(String value) {
        if (value == null || value.isEmpty()) {
            writeVarint(0);
            return;
        }

        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        writeVarint(bytes.length);
        for (byte b : bytes) {
            request.putByte(b);
        }
    }

    /**
     * Writes a varint (unsigned LEB128).
     */
    public void writeVarint(long value) {
        while (value > 0x7F) {
            request.putByte((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        request.putByte((byte) value);
    }

    /**
     * Returns the current position (bytes written to content).
     */
    public int getPosition() {
        return request.getContentLength();
    }

    /**
     * Returns the buffer address at content start.
     */
    public long getBufferAddress() {
        return request.getContentStart();
    }

    @Override
    public void close() {
        if (gorillaBuffer != 0) {
            Unsafe.free(gorillaBuffer, gorillaBufferCapacity, MemoryTag.NATIVE_DEFAULT);
            gorillaBuffer = 0;
        }
    }
}