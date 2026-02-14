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

/**
 * Interface for ILP v4 column decoders.
 * <p>
 * Different column types require different decoding strategies:
 * <ul>
 *   <li>Fixed-width types: direct memory read with null bitmap</li>
 *   <li>Variable-width types: offset array + data section</li>
 *   <li>Boolean: bit-packed values</li>
 *   <li>Timestamp: optional Gorilla delta-of-delta encoding</li>
 * </ul>
 */
public interface QwpColumnDecoder {

    /**
     * Decodes column data from the source buffer.
     *
     * @param sourceAddress address of column data (after header/schema)
     * @param sourceLength  available bytes
     * @param rowCount      number of rows to decode
     * @param nullable      whether the column is nullable
     * @param sink          destination for decoded values
     * @return number of bytes consumed from source
     * @throws QwpParseException if decoding fails
     */
    int decode(long sourceAddress, int sourceLength, int rowCount, boolean nullable, ColumnSink sink) throws QwpParseException;

    /**
     * Calculates the expected size in bytes for encoding this column type.
     *
     * @param rowCount  number of rows
     * @param nullable  whether column is nullable
     * @return expected byte size
     */
    int expectedSize(int rowCount, boolean nullable);

    /**
     * Sink interface for decoded column values.
     * Implementations write to QuestDB memory structures.
     */
    interface ColumnSink {
        /**
         * Called for each decoded byte value.
         */
        void putByte(int rowIndex, byte value);

        /**
         * Called for each decoded short value.
         */
        void putShort(int rowIndex, short value);

        /**
         * Called for each decoded int value.
         */
        void putInt(int rowIndex, int value);

        /**
         * Called for each decoded long value.
         */
        void putLong(int rowIndex, long value);

        /**
         * Called for each decoded float value.
         */
        void putFloat(int rowIndex, float value);

        /**
         * Called for each decoded double value.
         */
        void putDouble(int rowIndex, double value);

        /**
         * Called for each decoded boolean value.
         */
        void putBoolean(int rowIndex, boolean value);

        /**
         * Called for each null value.
         */
        void putNull(int rowIndex);

        /**
         * Called for UUID values (128-bit).
         */
        void putUuid(int rowIndex, long hi, long lo);

        /**
         * Called for LONG256 values (256-bit).
         */
        void putLong256(int rowIndex, long l0, long l1, long l2, long l3);
    }

    /**
     * Simple array-based sink for testing.
     */
    class ArrayColumnSink implements ColumnSink {
        private final Object[] values;
        private final boolean[] nulls;

        public ArrayColumnSink(int rowCount) {
            this.values = new Object[rowCount];
            this.nulls = new boolean[rowCount];
        }

        @Override
        public void putByte(int rowIndex, byte value) {
            values[rowIndex] = value;
        }

        @Override
        public void putShort(int rowIndex, short value) {
            values[rowIndex] = value;
        }

        @Override
        public void putInt(int rowIndex, int value) {
            values[rowIndex] = value;
        }

        @Override
        public void putLong(int rowIndex, long value) {
            values[rowIndex] = value;
        }

        @Override
        public void putFloat(int rowIndex, float value) {
            values[rowIndex] = value;
        }

        @Override
        public void putDouble(int rowIndex, double value) {
            values[rowIndex] = value;
        }

        @Override
        public void putBoolean(int rowIndex, boolean value) {
            values[rowIndex] = value;
        }

        @Override
        public void putNull(int rowIndex) {
            nulls[rowIndex] = true;
        }

        @Override
        public void putUuid(int rowIndex, long hi, long lo) {
            values[rowIndex] = new long[]{hi, lo};
        }

        @Override
        public void putLong256(int rowIndex, long l0, long l1, long l2, long l3) {
            values[rowIndex] = new long[]{l0, l1, l2, l3};
        }

        public Object getValue(int rowIndex) {
            return values[rowIndex];
        }

        public boolean isNull(int rowIndex) {
            return nulls[rowIndex];
        }

        public int size() {
            return values.length;
        }
    }
}
