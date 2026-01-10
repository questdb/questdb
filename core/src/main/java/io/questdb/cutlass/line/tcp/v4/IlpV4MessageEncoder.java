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

import java.nio.charset.StandardCharsets;

import static io.questdb.cutlass.line.tcp.v4.IlpV4Constants.*;

/**
 * Encodes ILP v4 messages for sending.
 * <p>
 * This encoder produces the binary wire format defined in the ILP v4 specification.
 * It handles message headers, table blocks, schemas, and column data.
 */
public class IlpV4MessageEncoder {

    private long bufferAddress;
    private int bufferCapacity;
    private int position;
    private byte flags;

    public IlpV4MessageEncoder() {
        this(64 * 1024); // 64KB initial buffer
    }

    public IlpV4MessageEncoder(int initialCapacity) {
        this.bufferCapacity = initialCapacity;
        this.bufferAddress = Unsafe.malloc(bufferCapacity, MemoryTag.NATIVE_DEFAULT);
        this.position = 0;
        this.flags = 0;
    }

    /**
     * Resets the encoder for a new message.
     */
    public void reset() {
        position = 0;
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
        ensureCapacity(HEADER_SIZE);

        // Magic "ILP4"
        Unsafe.getUnsafe().putByte(bufferAddress + position, (byte) 'I');
        Unsafe.getUnsafe().putByte(bufferAddress + position + 1, (byte) 'L');
        Unsafe.getUnsafe().putByte(bufferAddress + position + 2, (byte) 'P');
        Unsafe.getUnsafe().putByte(bufferAddress + position + 3, (byte) '4');

        // Version
        Unsafe.getUnsafe().putByte(bufferAddress + position + 4, VERSION_1);

        // Flags
        Unsafe.getUnsafe().putByte(bufferAddress + position + 5, flags);

        // Table count (uint16)
        Unsafe.getUnsafe().putShort(bufferAddress + position + 6, (short) tableCount);

        // Payload length (uint32)
        Unsafe.getUnsafe().putInt(bufferAddress + position + 8, payloadLength);

        position += HEADER_SIZE;
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
        Unsafe.getUnsafe().putInt(bufferAddress + 8, payloadLength);
    }

    /**
     * Writes a table header with full schema.
     * <p>
     * Format (per ILP v4 spec):
     * <pre>
     * TABLE HEADER:
     *   Table name length: varint
     *   Table name: UTF-8 bytes
     *   Row count: varint
     *   Column count: varint
     *
     * SCHEMA SECTION:
     *   Schema mode: 0x00 (full)
     *   For each column:
     *     Column name length: varint
     *     Column name: UTF-8 bytes
     *     Column type: byte
     * </pre>
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
     * <p>
     * Format (per ILP v4 spec):
     * <pre>
     * TABLE HEADER:
     *   Table name length: varint
     *   Table name: UTF-8 bytes
     *   Row count: varint
     *   Column count: varint  (placeholder, will be looked up from cache)
     *
     * SCHEMA SECTION:
     *   Schema mode: 0x01 (reference)
     *   Schema hash: 8 bytes
     * </pre>
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
        ensureCapacity(bitmapSize);

        for (int i = 0; i < bitmapSize; i++) {
            byte b = 0;
            for (int bit = 0; bit < 8; bit++) {
                int idx = i * 8 + bit;
                if (idx < count && nulls[idx]) {
                    b |= (1 << bit);
                }
            }
            Unsafe.getUnsafe().putByte(bufferAddress + position + i, b);
        }
        position += bitmapSize;
    }

    /**
     * Writes boolean column data (bit-packed).
     *
     * @param values boolean values
     * @param count  number of values
     */
    public void writeBooleanColumn(boolean[] values, int count) {
        int packedSize = (count + 7) / 8;
        ensureCapacity(packedSize);

        for (int i = 0; i < packedSize; i++) {
            byte b = 0;
            for (int bit = 0; bit < 8; bit++) {
                int idx = i * 8 + bit;
                if (idx < count && values[idx]) {
                    b |= (1 << bit);
                }
            }
            Unsafe.getUnsafe().putByte(bufferAddress + position + i, b);
        }
        position += packedSize;
    }

    /**
     * Writes a byte column.
     */
    public void writeByteColumn(byte[] values, int count) {
        ensureCapacity(count);
        for (int i = 0; i < count; i++) {
            Unsafe.getUnsafe().putByte(bufferAddress + position + i, values[i]);
        }
        position += count;
    }

    /**
     * Writes a short column.
     */
    public void writeShortColumn(short[] values, int count) {
        ensureCapacity(count * 2);
        for (int i = 0; i < count; i++) {
            Unsafe.getUnsafe().putShort(bufferAddress + position + i * 2, values[i]);
        }
        position += count * 2;
    }

    /**
     * Writes an int column.
     */
    public void writeIntColumn(int[] values, int count) {
        ensureCapacity(count * 4);
        for (int i = 0; i < count; i++) {
            Unsafe.getUnsafe().putInt(bufferAddress + position + i * 4, values[i]);
        }
        position += count * 4;
    }

    /**
     * Writes a long column.
     */
    public void writeLongColumn(long[] values, int count) {
        ensureCapacity(count * 8);
        for (int i = 0; i < count; i++) {
            Unsafe.getUnsafe().putLong(bufferAddress + position + i * 8, values[i]);
        }
        position += count * 8;
    }

    /**
     * Writes a float column.
     */
    public void writeFloatColumn(float[] values, int count) {
        ensureCapacity(count * 4);
        for (int i = 0; i < count; i++) {
            Unsafe.getUnsafe().putFloat(bufferAddress + position + i * 4, values[i]);
        }
        position += count * 4;
    }

    /**
     * Writes a double column.
     */
    public void writeDoubleColumn(double[] values, int count) {
        ensureCapacity(count * 8);
        for (int i = 0; i < count; i++) {
            Unsafe.getUnsafe().putDouble(bufferAddress + position + i * 8, values[i]);
        }
        position += count * 8;
    }

    /**
     * Writes a timestamp column with optional Gorilla encoding.
     * <p>
     * When Gorilla is enabled, writes Gorilla-encoded format with encoding flag.
     * When Gorilla is disabled, writes raw long values (no encoding flag).
     * <p>
     * Note: null bitmap is written by caller (IlpV4TableBuffer.encode) for nullable columns.
     *
     * @param values     timestamp values (packed, only non-null values)
     * @param nulls      null indicators (can be null if not nullable)
     * @param rowCount   total number of rows (for null bitmap reference)
     * @param valueCount number of actual non-null values
     * @param useGorilla whether to use Gorilla encoding
     */
    public void writeTimestampColumn(long[] values, boolean[] nulls, int rowCount, int valueCount, boolean useGorilla) {
        if (useGorilla && valueCount > 2 && canUseGorilla(values, valueCount)) {
            // Use Gorilla encoding
            // Write encoding flag
            writeByte(IlpV4TimestampDecoder.ENCODING_GORILLA);

            // Write first timestamp
            ensureCapacity(8);
            Unsafe.getUnsafe().putLong(bufferAddress + position, values[0]);
            position += 8;

            if (valueCount == 1) {
                return;
            }

            // Write second timestamp
            ensureCapacity(8);
            Unsafe.getUnsafe().putLong(bufferAddress + position, values[1]);
            position += 8;

            if (valueCount == 2) {
                return;
            }

            // Encode remaining timestamps using delta-of-delta
            IlpV4BitWriter bitWriter = new IlpV4BitWriter();
            bitWriter.reset(bufferAddress + position, bufferCapacity - position);

            long prevTimestamp = values[1];
            long prevDelta = values[1] - values[0];

            for (int i = 2; i < valueCount; i++) {
                long delta = values[i] - prevTimestamp;
                long deltaOfDelta = delta - prevDelta;

                encodeDoD(bitWriter, deltaOfDelta);

                prevDelta = delta;
                prevTimestamp = values[i];
            }

            // Flush and update position
            int bytesWritten = bitWriter.finish();
            position += bytesWritten;
        } else {
            // Use raw long array (no encoding flag when Gorilla is enabled, but data doesn't fit)
            // Or when Gorilla is disabled
            if (useGorilla) {
                // Gorilla enabled but can't use it - write uncompressed with encoding flag
                writeByte(IlpV4TimestampDecoder.ENCODING_UNCOMPRESSED);
            }
            writeLongColumn(values, valueCount);
        }
    }

    /**
     * Checks if Gorilla encoding can be safely used for the given timestamps.
     * Gorilla encoding can overflow if delta-of-delta values don't fit in 32 bits.
     *
     * @param values timestamp values
     * @param count  number of values
     * @return true if Gorilla encoding is safe
     */
    private boolean canUseGorilla(long[] values, int count) {
        if (count < 3) {
            return true; // No delta-of-delta encoding needed
        }

        long prevDelta = values[1] - values[0];
        for (int i = 2; i < count; i++) {
            long delta = values[i] - values[i - 1];
            long deltaOfDelta = delta - prevDelta;
            // Check if deltaOfDelta fits in 32-bit signed integer
            if (deltaOfDelta < Integer.MIN_VALUE || deltaOfDelta > Integer.MAX_VALUE) {
                return false;
            }
            prevDelta = delta;
        }
        return true;
    }

    /**
     * Encodes a delta-of-delta value to the bit writer.
     * <p>
     * Format matches the Gorilla decoder expectations:
     * - '0'    -> DoD is 0
     * - '10'   -> 7-bit signed value
     * - '110'  -> 9-bit signed value
     * - '1110' -> 12-bit signed value
     * - '1111' -> 32-bit signed value
     */
    private static void encodeDoD(IlpV4BitWriter writer, long deltaOfDelta) {
        if (deltaOfDelta == 0) {
            // '0' = DoD is 0
            writer.writeBit(0);
        } else if (deltaOfDelta >= -63 && deltaOfDelta <= 64) {
            // '10' prefix
            writer.writeBit(1);
            writer.writeBit(0);
            writer.writeSigned(deltaOfDelta, 7);
        } else if (deltaOfDelta >= -255 && deltaOfDelta <= 256) {
            // '110' prefix
            writer.writeBit(1);
            writer.writeBit(1);
            writer.writeBit(0);
            writer.writeSigned(deltaOfDelta, 9);
        } else if (deltaOfDelta >= -2047 && deltaOfDelta <= 2048) {
            // '1110' prefix
            writer.writeBit(1);
            writer.writeBit(1);
            writer.writeBit(1);
            writer.writeBit(0);
            writer.writeSigned(deltaOfDelta, 12);
        } else {
            // '1111' prefix + 32-bit signed
            writer.writeBit(1);
            writer.writeBit(1);
            writer.writeBit(1);
            writer.writeBit(1);
            writer.writeSigned(deltaOfDelta, 32);
        }
    }

    /**
     * Writes a string column with offset array.
     * <p>
     * Format:
     * <pre>
     * Offset array: (count + 1) * uint32 (4 bytes each)
     *   offset[0] = 0
     *   offset[i+1] = end position of string[i]
     * String data: concatenated UTF-8 bytes
     * </pre>
     *
     * @param strings string values
     * @param count   number of values
     */
    public void writeStringColumn(String[] strings, int count) {
        // Calculate total data size and build offsets
        int[] offsets = new int[count + 1];
        offsets[0] = 0;
        int totalDataLen = 0;
        byte[][] encodedStrings = new byte[count][];

        for (int i = 0; i < count; i++) {
            if (strings[i] != null) {
                encodedStrings[i] = strings[i].getBytes(StandardCharsets.UTF_8);
                totalDataLen += encodedStrings[i].length;
            } else {
                encodedStrings[i] = new byte[0];
            }
            offsets[i + 1] = totalDataLen;
        }

        // Write offset array (fixed uint32, NOT varints!)
        ensureCapacity((count + 1) * 4 + totalDataLen);
        for (int i = 0; i <= count; i++) {
            Unsafe.getUnsafe().putInt(bufferAddress + position, offsets[i]);
            position += 4;
        }

        // Write string data
        for (int i = 0; i < count; i++) {
            for (byte b : encodedStrings[i]) {
                Unsafe.getUnsafe().putByte(bufferAddress + position, b);
                position++;
            }
        }
    }

    /**
     * Writes a symbol column with dictionary.
     *
     * @param symbolIndices indices into dictionary
     * @param dictionary    unique symbol values
     * @param count         number of values
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
        ensureCapacity(count * 16);
        for (int i = 0; i < count; i++) {
            // UUID is big-endian
            writeLongBigEndian(highBits[i]);
            writeLongBigEndian(lowBits[i]);
        }
    }

    /**
     * Writes a single byte.
     */
    public void writeByte(byte value) {
        ensureCapacity(1);
        Unsafe.getUnsafe().putByte(bufferAddress + position, value);
        position++;
    }

    /**
     * Writes a short (little-endian).
     */
    public void writeShort(short value) {
        ensureCapacity(2);
        Unsafe.getUnsafe().putShort(bufferAddress + position, value);
        position += 2;
    }

    /**
     * Writes an int (little-endian).
     */
    public void writeInt(int value) {
        ensureCapacity(4);
        Unsafe.getUnsafe().putInt(bufferAddress + position, value);
        position += 4;
    }

    /**
     * Writes a long (little-endian).
     */
    public void writeLong(long value) {
        ensureCapacity(8);
        Unsafe.getUnsafe().putLong(bufferAddress + position, value);
        position += 8;
    }

    /**
     * Writes a long in big-endian order.
     */
    public void writeLongBigEndian(long value) {
        ensureCapacity(8);
        for (int i = 7; i >= 0; i--) {
            Unsafe.getUnsafe().putByte(bufferAddress + position + (7 - i), (byte) (value >> (i * 8)));
        }
        position += 8;
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
        ensureCapacity(bytes.length);
        for (byte b : bytes) {
            Unsafe.getUnsafe().putByte(bufferAddress + position, b);
            position++;
        }
    }

    /**
     * Writes a varint (unsigned LEB128).
     */
    public void writeVarint(long value) {
        ensureCapacity(10); // Max varint size
        while (value > 0x7F) {
            Unsafe.getUnsafe().putByte(bufferAddress + position, (byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
            position++;
        }
        Unsafe.getUnsafe().putByte(bufferAddress + position, (byte) value);
        position++;
    }

    /**
     * Returns the current buffer address.
     */
    public long getBufferAddress() {
        return bufferAddress;
    }

    /**
     * Returns the current position (bytes written).
     */
    public int getPosition() {
        return position;
    }

    /**
     * Sets the position.
     */
    public void setPosition(int pos) {
        this.position = pos;
    }

    /**
     * Returns the buffer capacity.
     */
    public int getCapacity() {
        return bufferCapacity;
    }

    /**
     * Copies the encoded data to a byte array.
     */
    public byte[] toByteArray() {
        byte[] result = new byte[position];
        for (int i = 0; i < position; i++) {
            result[i] = Unsafe.getUnsafe().getByte(bufferAddress + i);
        }
        return result;
    }

    /**
     * Ensures the buffer has enough capacity.
     */
    private void ensureCapacity(int required) {
        if (position + required > bufferCapacity) {
            int newCapacity = Math.max(bufferCapacity * 2, position + required);
            long newAddress = Unsafe.malloc(newCapacity, MemoryTag.NATIVE_DEFAULT);
            Unsafe.getUnsafe().copyMemory(bufferAddress, newAddress, position);
            Unsafe.free(bufferAddress, bufferCapacity, MemoryTag.NATIVE_DEFAULT);
            bufferAddress = newAddress;
            bufferCapacity = newCapacity;
        }
    }

    /**
     * Closes the encoder and frees memory.
     */
    public void close() {
        if (bufferAddress != 0) {
            Unsafe.free(bufferAddress, bufferCapacity, MemoryTag.NATIVE_DEFAULT);
            bufferAddress = 0;
        }
    }
}
