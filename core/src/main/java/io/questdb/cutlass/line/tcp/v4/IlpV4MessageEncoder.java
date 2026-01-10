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

package io.questdb.cutlass.line.tcp.v4;

import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;

import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.*;

/**
 * Encodes ILP v4 messages for sending.
 * <p>
 * This encoder produces the binary wire format defined in the ILP v4 specification.
 * It handles message headers, table blocks, schemas, and column data.
 * <p>
 * The encoder writes to an {@link IlpV4OutputSink}, which can be backed by either
 * a native memory buffer (for TCP) or an HTTP client request buffer (for HTTP).
 */
public class IlpV4MessageEncoder implements Closeable {

    private static final int GORILLA_BUFFER_SIZE = 64 * 1024; // 64KB for Gorilla encoding

    private IlpV4OutputSink sink;
    private IlpV4NativeBufferSink ownedSink; // Only set if we own the sink
    private byte flags;
    // Pooled BitWriter to avoid allocation on every timestamp column
    private final IlpV4BitWriter bitWriter = new IlpV4BitWriter();
    // Temporary buffer for Gorilla encoding (used when sink doesn't support direct writes)
    private long gorillaBuffer;
    private int gorillaBufferCapacity;

    /**
     * Creates an encoder with its own native buffer.
     */
    public IlpV4MessageEncoder() {
        this(64 * 1024); // 64KB initial buffer
    }

    /**
     * Creates an encoder with its own native buffer of the specified initial capacity.
     */
    public IlpV4MessageEncoder(int initialCapacity) {
        this.ownedSink = new IlpV4NativeBufferSink(initialCapacity);
        this.sink = this.ownedSink;
        this.flags = 0;
    }

    /**
     * Creates an encoder that writes to the provided sink.
     *
     * @param sink the output sink to write to
     */
    public IlpV4MessageEncoder(IlpV4OutputSink sink) {
        this.sink = sink;
        this.ownedSink = null;
        this.flags = 0;
    }

    /**
     * Sets the output sink for this encoder.
     * <p>
     * This allows reusing the encoder with different sinks (e.g., for HTTP where
     * each request has its own buffer).
     *
     * @param sink the output sink to write to
     */
    public void setSink(IlpV4OutputSink sink) {
        this.sink = sink;
    }

    /**
     * Returns the output sink.
     */
    public IlpV4OutputSink getSink() {
        return sink;
    }

    /**
     * Resets the encoder for a new message.
     */
    public void reset() {
        if (ownedSink != null) {
            ownedSink.reset();
        }
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
        sink.ensureCapacity(HEADER_SIZE);

        // Magic "ILP4"
        sink.putByte((byte) 'I');
        sink.putByte((byte) 'L');
        sink.putByte((byte) 'P');
        sink.putByte((byte) '4');

        // Version
        sink.putByte(VERSION_1);

        // Flags
        sink.putByte(flags);

        // Table count (uint16)
        sink.putShort((short) tableCount);

        // Payload length (uint32)
        sink.putInt(payloadLength);
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
        // Payload length is at offset 8 in the header
        Unsafe.getUnsafe().putInt(sink.addressAt(8), payloadLength);
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
        sink.ensureCapacity(bitmapSize);

        for (int i = 0; i < bitmapSize; i++) {
            byte b = 0;
            for (int bit = 0; bit < 8; bit++) {
                int idx = i * 8 + bit;
                if (idx < count && nulls[idx]) {
                    b |= (1 << bit);
                }
            }
            sink.putByte(b);
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
        sink.ensureCapacity(bitmapSize);

        for (int byteIdx = 0; byteIdx < bitmapSize; byteIdx++) {
            int longIndex = byteIdx >>> 3;  // byteIdx / 8
            int byteInLong = byteIdx & 7;   // byteIdx % 8
            byte b = (byte) ((nullsPacked[longIndex] >>> (byteInLong * 8)) & 0xFF);
            sink.putByte(b);
        }
    }

    /**
     * Writes boolean column data (bit-packed).
     */
    public void writeBooleanColumn(boolean[] values, int count) {
        int packedSize = (count + 7) / 8;
        sink.ensureCapacity(packedSize);

        for (int i = 0; i < packedSize; i++) {
            byte b = 0;
            for (int bit = 0; bit < 8; bit++) {
                int idx = i * 8 + bit;
                if (idx < count && values[idx]) {
                    b |= (1 << bit);
                }
            }
            sink.putByte(b);
        }
    }

    /**
     * Writes a byte column.
     */
    public void writeByteColumn(byte[] values, int count) {
        sink.ensureCapacity(count);
        for (int i = 0; i < count; i++) {
            sink.putByte(values[i]);
        }
    }

    /**
     * Writes a short column.
     */
    public void writeShortColumn(short[] values, int count) {
        sink.ensureCapacity(count * 2);
        for (int i = 0; i < count; i++) {
            sink.putShort(values[i]);
        }
    }

    /**
     * Writes an int column.
     */
    public void writeIntColumn(int[] values, int count) {
        sink.ensureCapacity(count * 4);
        for (int i = 0; i < count; i++) {
            sink.putInt(values[i]);
        }
    }

    /**
     * Writes a long column.
     */
    public void writeLongColumn(long[] values, int count) {
        sink.ensureCapacity(count * 8);
        for (int i = 0; i < count; i++) {
            sink.putLong(values[i]);
        }
    }

    /**
     * Writes a float column.
     */
    public void writeFloatColumn(float[] values, int count) {
        sink.ensureCapacity(count * 4);
        for (int i = 0; i < count; i++) {
            sink.putFloat(values[i]);
        }
    }

    /**
     * Writes a double column.
     */
    public void writeDoubleColumn(double[] values, int count) {
        sink.ensureCapacity(count * 8);
        for (int i = 0; i < count; i++) {
            sink.putDouble(values[i]);
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
            sink.putLong(values[0]);

            if (valueCount == 1) {
                return;
            }

            // Write second timestamp
            sink.putLong(values[1]);

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

            // Flush and copy to sink
            int bytesWritten = bitWriter.finish();
            sink.putBytes(gorillaBuffer, bytesWritten);
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
        // Calculate total size needed for offset array
        int offsetArraySize = (count + 1) * 4;

        // Calculate total data length
        int totalDataLen = 0;
        for (int i = 0; i < count; i++) {
            if (strings[i] != null) {
                totalDataLen += utf8Length(strings[i]);
            }
        }

        sink.ensureCapacity(offsetArraySize + totalDataLen);

        // Write offset array
        int runningOffset = 0;
        sink.putInt(0);
        for (int i = 0; i < count; i++) {
            if (strings[i] != null) {
                runningOffset += utf8Length(strings[i]);
            }
            sink.putInt(runningOffset);
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
     * Encodes a string as UTF-8 directly to the sink.
     */
    private void encodeUtf8Direct(String s) {
        for (int i = 0, n = s.length(); i < n; i++) {
            char c = s.charAt(i);
            if (c < 0x80) {
                sink.putByte((byte) c);
            } else if (c < 0x800) {
                sink.putByte((byte) (0xC0 | (c >> 6)));
                sink.putByte((byte) (0x80 | (c & 0x3F)));
            } else if (c >= 0xD800 && c <= 0xDBFF && i + 1 < n) {
                char c2 = s.charAt(++i);
                int codePoint = 0x10000 + ((c - 0xD800) << 10) + (c2 - 0xDC00);
                sink.putByte((byte) (0xF0 | (codePoint >> 18)));
                sink.putByte((byte) (0x80 | ((codePoint >> 12) & 0x3F)));
                sink.putByte((byte) (0x80 | ((codePoint >> 6) & 0x3F)));
                sink.putByte((byte) (0x80 | (codePoint & 0x3F)));
            } else {
                sink.putByte((byte) (0xE0 | (c >> 12)));
                sink.putByte((byte) (0x80 | ((c >> 6) & 0x3F)));
                sink.putByte((byte) (0x80 | (c & 0x3F)));
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
        sink.ensureCapacity(count * 16);
        for (int i = 0; i < count; i++) {
            sink.putLongBE(highBits[i]);
            sink.putLongBE(lowBits[i]);
        }
    }

    /**
     * Writes a single byte.
     */
    public void writeByte(byte value) {
        sink.putByte(value);
    }

    /**
     * Writes a short (little-endian).
     */
    public void writeShort(short value) {
        sink.putShort(value);
    }

    /**
     * Writes an int (little-endian).
     */
    public void writeInt(int value) {
        sink.putInt(value);
    }

    /**
     * Writes a long (little-endian).
     */
    public void writeLong(long value) {
        sink.putLong(value);
    }

    /**
     * Writes a long in big-endian order.
     */
    public void writeLongBigEndian(long value) {
        sink.putLongBE(value);
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
        sink.ensureCapacity(bytes.length);
        for (byte b : bytes) {
            sink.putByte(b);
        }
    }

    /**
     * Writes a varint (unsigned LEB128).
     */
    public void writeVarint(long value) {
        sink.ensureCapacity(10);
        while (value > 0x7F) {
            sink.putByte((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        sink.putByte((byte) value);
    }

    /**
     * Returns the current position (bytes written).
     */
    public int getPosition() {
        return sink.position();
    }

    /**
     * Sets the position.
     */
    public void setPosition(int pos) {
        sink.position(pos);
    }

    /**
     * Returns the buffer capacity.
     */
    public int getCapacity() {
        return sink.capacity();
    }

    /**
     * Returns the buffer address (for backward compatibility).
     */
    public long getBufferAddress() {
        return sink.addressAt(0);
    }

    /**
     * Copies the encoded data to a byte array.
     */
    public byte[] toByteArray() {
        if (sink instanceof IlpV4NativeBufferSink) {
            return ((IlpV4NativeBufferSink) sink).toByteArray();
        }
        throw new UnsupportedOperationException("toByteArray() only supported for native buffer sink");
    }

    @Override
    public void close() {
        if (ownedSink != null) {
            ownedSink.close();
            ownedSink = null;
            sink = null;
        }
        if (gorillaBuffer != 0) {
            Unsafe.free(gorillaBuffer, gorillaBufferCapacity, MemoryTag.NATIVE_DEFAULT);
            gorillaBuffer = 0;
        }
    }
}
