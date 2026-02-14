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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Decoder for SYMBOL columns in ILP v4 format.
 * <p>
 * Symbols use dictionary encoding:
 * <pre>
 * [Null bitmap if nullable]: ceil(rowCount / 8) bytes
 * Dictionary size: varint
 * For each dictionary entry:
 *   String length: varint
 *   String data: UTF-8 bytes
 * Value array: rowCount * varint (dictionary indices)
 *   Index 0 = first dictionary entry
 *   Index -1 (max varint) = NULL (alternative to bitmap)
 * </pre>
 */
public final class QwpSymbolDecoder implements QwpColumnDecoder {

    public static final QwpSymbolDecoder INSTANCE = new QwpSymbolDecoder();

    /**
     * Special value indicating null symbol (when not using bitmap).
     * Encoded as max varint value (all 1s in continuation bits).
     * We use Long.MAX_VALUE as a sentinel since unsigned varint can't actually be negative.
     */
    public static final long NULL_SYMBOL_INDEX = Long.MAX_VALUE;

    private final QwpVarint.DecodeResult decodeResult = new QwpVarint.DecodeResult();

    private QwpSymbolDecoder() {
    }

    @Override
    public int decode(long sourceAddress, int sourceLength, int rowCount, boolean nullable, ColumnSink sink) throws QwpParseException {
        if (rowCount == 0) {
            return 0;
        }

        int offset = 0;
        long limit = sourceAddress + sourceLength;

        // Parse null bitmap if nullable
        long nullBitmapAddress = 0;
        if (nullable) {
            int nullBitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            if (offset + nullBitmapSize > sourceLength) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                        "insufficient data for null bitmap"
                );
            }
            nullBitmapAddress = sourceAddress + offset;
            offset += nullBitmapSize;
        }

        // Parse dictionary size
        QwpVarint.decode(sourceAddress + offset, limit, decodeResult);
        int dictSize = (int) decodeResult.value;
        offset += decodeResult.bytesRead;

        if (dictSize < 0) {
            throw QwpParseException.create(
                    QwpParseException.ErrorCode.INVALID_DICTIONARY_INDEX,
                    "negative dictionary size: " + dictSize
            );
        }

        // Parse dictionary entries
        String[] dictionary = new String[dictSize];
        for (int i = 0; i < dictSize; i++) {
            // Parse string length
            QwpVarint.decode(sourceAddress + offset, limit, decodeResult);
            int stringLength = (int) decodeResult.value;
            offset += decodeResult.bytesRead;

            if (stringLength < 0) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INVALID_DICTIONARY_INDEX,
                        "negative string length in dictionary at index " + i
                );
            }

            // Parse string data
            if (offset + stringLength > sourceLength) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INSUFFICIENT_DATA,
                        "insufficient data for dictionary string at index " + i
                );
            }

            byte[] bytes = new byte[stringLength];
            for (int j = 0; j < stringLength; j++) {
                bytes[j] = Unsafe.getUnsafe().getByte(sourceAddress + offset + j);
            }
            dictionary[i] = new String(bytes, StandardCharsets.UTF_8);
            offset += stringLength;
        }

        // Parse value array (dictionary indices) - only for non-null values
        SymbolColumnSink symbolSink = (SymbolColumnSink) sink;
        for (int i = 0; i < rowCount; i++) {
            // Check null bitmap first - null values don't have an index
            if (nullable && QwpNullBitmap.isNull(nullBitmapAddress, i)) {
                sink.putNull(i);
                continue;
            }

            // Parse symbol index for non-null row
            QwpVarint.decode(sourceAddress + offset, limit, decodeResult);
            long index = decodeResult.value;
            offset += decodeResult.bytesRead;

            // Check for null index (max unsigned value treated as null)
            if (index == NULL_SYMBOL_INDEX || index < 0) {
                sink.putNull(i);
            } else if (index >= dictSize) {
                throw QwpParseException.create(
                        QwpParseException.ErrorCode.INVALID_DICTIONARY_INDEX,
                        "dictionary index out of bounds: " + index + " >= " + dictSize
                );
            } else {
                symbolSink.putSymbol(i, (int) index, dictionary[(int) index]);
            }
        }

        return offset;
    }

    @Override
    public int expectedSize(int rowCount, boolean nullable) {
        // Variable size - this is minimum (empty dictionary, 1-byte indices)
        int size = 0;
        if (nullable) {
            size += QwpNullBitmap.sizeInBytes(rowCount);
        }
        size += 1; // dictionary size (varint, at least 1 byte)
        size += rowCount; // indices (varint, at least 1 byte each)
        return size;
    }

    /**
     * Extended sink interface for symbol columns.
     */
    public interface SymbolColumnSink extends ColumnSink {
        /**
         * Called for each decoded symbol value.
         *
         * @param rowIndex    row index
         * @param dictIndex   dictionary index
         * @param symbolValue resolved symbol string value
         */
        void putSymbol(int rowIndex, int dictIndex, String symbolValue);
    }

    /**
     * Simple array-based sink for testing.
     */
    public static class ArraySymbolSink implements SymbolColumnSink {
        private final String[] values;
        private final int[] indices;
        private final boolean[] nulls;

        public ArraySymbolSink(int rowCount) {
            this.values = new String[rowCount];
            this.indices = new int[rowCount];
            this.nulls = new boolean[rowCount];
            java.util.Arrays.fill(indices, -1);
        }

        @Override
        public void putSymbol(int rowIndex, int dictIndex, String symbolValue) {
            values[rowIndex] = symbolValue;
            indices[rowIndex] = dictIndex;
        }

        @Override
        public void putNull(int rowIndex) {
            nulls[rowIndex] = true;
            indices[rowIndex] = -1;
        }

        public String getValue(int rowIndex) {
            return values[rowIndex];
        }

        public int getIndex(int rowIndex) {
            return indices[rowIndex];
        }

        public boolean isNull(int rowIndex) {
            return nulls[rowIndex];
        }

        // Unused methods from ColumnSink interface
        @Override public void putByte(int rowIndex, byte value) {}
        @Override public void putShort(int rowIndex, short value) {}
        @Override public void putInt(int rowIndex, int value) {}
        @Override public void putLong(int rowIndex, long value) {}
        @Override public void putFloat(int rowIndex, float value) {}
        @Override public void putDouble(int rowIndex, double value) {}
        @Override public void putBoolean(int rowIndex, boolean value) {}
        @Override public void putUuid(int rowIndex, long hi, long lo) {}
        @Override public void putLong256(int rowIndex, long l0, long l1, long l2, long l3) {}
    }

    // ==================== Static Encoding Methods ====================

    /**
     * Encodes symbol values to direct memory.
     * Only non-null values have indices written.
     *
     * @param destAddress destination address
     * @param values      symbol values to encode
     * @param nulls       null flags (can be null if not nullable)
     * @return address after encoded data
     */
    public static long encode(long destAddress, String[] values, boolean[] nulls) {
        int rowCount = values.length;
        boolean nullable = nulls != null;
        long pos = destAddress;

        // Write null bitmap if nullable
        if (nullable) {
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            QwpNullBitmap.fillNoneNull(pos, rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (nulls[i]) {
                    QwpNullBitmap.setNull(pos, i);
                }
            }
            pos += bitmapSize;
        }

        // Build dictionary (only from non-null values)
        List<String> dictList = new ArrayList<>();
        int[] dictIndices = new int[rowCount];
        for (int i = 0; i < rowCount; i++) {
            if (nullable && nulls[i]) {
                dictIndices[i] = -1;
                continue;
            }
            if (values[i] != null) {
                int idx = dictList.indexOf(values[i]);
                if (idx < 0) {
                    idx = dictList.size();
                    dictList.add(values[i]);
                }
                dictIndices[i] = idx;
            } else {
                dictIndices[i] = -1;
            }
        }

        // Write dictionary size
        pos = QwpVarint.encode(pos, dictList.size());

        // Write dictionary entries
        for (String entry : dictList) {
            byte[] bytes = entry.getBytes(StandardCharsets.UTF_8);
            pos = QwpVarint.encode(pos, bytes.length);
            for (byte b : bytes) {
                Unsafe.getUnsafe().putByte(pos++, b);
            }
        }

        // Write value indices only for non-null values
        for (int i = 0; i < rowCount; i++) {
            if (nullable && nulls[i]) continue;
            if (dictIndices[i] < 0) {
                // Null value (not from bitmap) - write max varint value
                pos = QwpVarint.encode(pos, NULL_SYMBOL_INDEX);
            } else {
                pos = QwpVarint.encode(pos, dictIndices[i]);
            }
        }

        return pos;
    }

    /**
     * Encodes symbol values to a byte array.
     * Only non-null values have indices written.
     *
     * @param buf    destination buffer
     * @param offset starting offset
     * @param values symbol values to encode
     * @param nulls  null flags (can be null if not nullable)
     * @return offset after encoded data
     */
    public static int encode(byte[] buf, int offset, String[] values, boolean[] nulls) {
        int rowCount = values.length;
        boolean nullable = nulls != null;

        // Write null bitmap if nullable
        if (nullable) {
            int bitmapSize = QwpNullBitmap.sizeInBytes(rowCount);
            QwpNullBitmap.fillNoneNull(buf, offset, rowCount);
            for (int i = 0; i < rowCount; i++) {
                if (nulls[i]) {
                    QwpNullBitmap.setNull(buf, offset, i);
                }
            }
            offset += bitmapSize;
        }

        // Build dictionary (only from non-null values)
        List<String> dictList = new ArrayList<>();
        int[] dictIndices = new int[rowCount];
        for (int i = 0; i < rowCount; i++) {
            if (nullable && nulls[i]) {
                dictIndices[i] = -1;
                continue;
            }
            if (values[i] != null) {
                int idx = dictList.indexOf(values[i]);
                if (idx < 0) {
                    idx = dictList.size();
                    dictList.add(values[i]);
                }
                dictIndices[i] = idx;
            } else {
                dictIndices[i] = -1;
            }
        }

        // Write dictionary size
        offset = QwpVarint.encode(buf, offset, dictList.size());

        // Write dictionary entries
        for (String entry : dictList) {
            byte[] bytes = entry.getBytes(StandardCharsets.UTF_8);
            offset = QwpVarint.encode(buf, offset, bytes.length);
            System.arraycopy(bytes, 0, buf, offset, bytes.length);
            offset += bytes.length;
        }

        // Write value indices only for non-null values
        for (int i = 0; i < rowCount; i++) {
            if (nullable && nulls[i]) continue;
            if (dictIndices[i] < 0) {
                offset = QwpVarint.encode(buf, offset, NULL_SYMBOL_INDEX);
            } else {
                offset = QwpVarint.encode(buf, offset, dictIndices[i]);
            }
        }

        return offset;
    }

    /**
     * Calculates the maximum encoded size for symbol values.
     * Actual size may be smaller due to dictionary deduplication.
     *
     * @param values symbol values
     * @param nulls  null flags (can be null if not nullable)
     * @return maximum encoded size in bytes
     */
    public static int maxEncodedSize(String[] values, boolean[] nulls) {
        int rowCount = values.length;
        boolean nullable = nulls != null;
        int size = 0;

        if (nullable) {
            size += QwpNullBitmap.sizeInBytes(rowCount);
        }

        // Dictionary: worst case is all unique values
        size += 5; // dictionary size varint (max 5 bytes)
        for (String value : values) {
            if (value != null) {
                size += 5; // string length varint
                size += value.getBytes(StandardCharsets.UTF_8).length;
            }
        }

        // Value indices: worst case is 10 bytes per varint (for null markers)
        size += rowCount * 10;

        return size;
    }
}
