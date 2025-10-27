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

package io.questdb.test.griffin.engine.functions;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.BinarySequence;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Interval;
import io.questdb.std.Long256;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

public class GroupByFunctionTest {
    private static final GroupByFunction function = new GroupByFunction() {
        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
        }

        @Override
        public ArrayView getArray(Record rec) {
            return null;
        }

        @Override
        public BinarySequence getBin(Record rec) {
            return null;
        }

        @Override
        public long getBinLen(Record rec) {
            return 0;
        }

        @Override
        public boolean getBool(Record rec) {
            return false;
        }

        @Override
        public byte getByte(Record rec) {
            return 0;
        }

        @Override
        public char getChar(Record rec) {
            return 0;
        }

        @Override
        public long getDate(Record rec) {
            return 0;
        }

        @Override
        public void getDecimal128(Record rec, Decimal128 sink) {
        }

        @Override
        public short getDecimal16(Record rec) {
            return 0;
        }

        @Override
        public void getDecimal256(Record rec, Decimal256 sink) {
        }

        @Override
        public int getDecimal32(Record rec) {
            return 0;
        }

        @Override
        public long getDecimal64(Record rec) {
            return 0;
        }

        @Override
        public byte getDecimal8(Record rec) {
            return 0;
        }

        @Override
        public double getDouble(Record rec) {
            return 0;
        }

        @Override
        public float getFloat(Record rec) {
            return 0;
        }

        @Override
        public byte getGeoByte(Record rec) {
            return 0;
        }

        @Override
        public int getGeoInt(Record rec) {
            return 0;
        }

        @Override
        public long getGeoLong(Record rec) {
            return 0;
        }

        @Override
        public short getGeoShort(Record rec) {
            return 0;
        }

        @Override
        public int getIPv4(Record rec) {
            return 0;
        }

        @Override
        public int getInt(Record rec) {
            return 0;
        }

        @Override
        public @NotNull Interval getInterval(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLong(Record rec) {
            return 0;
        }

        @Override
        public long getLong128Hi(Record rec) {
            return 0;
        }

        @Override
        public long getLong128Lo(Record rec) {
            return 0;
        }

        @Override
        public void getLong256(Record rec, CharSink<?> sink) {
        }

        @Override
        public Long256 getLong256A(Record rec) {
            return null;
        }

        @Override
        public Long256 getLong256B(Record rec) {
            return null;
        }

        @Override
        public RecordCursorFactory getRecordCursorFactory() {
            return null;
        }

        @Override
        public short getShort(Record rec) {
            return 0;
        }

        @Override
        public CharSequence getStrA(Record rec) {
            return null;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return null;
        }

        @Override
        public int getStrLen(Record rec) {
            return 0;
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            return null;
        }

        @Override
        public CharSequence getSymbolB(Record rec) {
            return null;
        }

        @Override
        public long getTimestamp(Record rec) {
            return 0;
        }

        @Override
        public int getType() {
            return 0;
        }

        @Override
        public int getValueIndex() {
            return 0;
        }

        @Override
        public Utf8Sequence getVarcharA(Record rec) {
            return null;
        }

        @Override
        public Utf8Sequence getVarcharB(Record rec) {
            return null;
        }

        @Override
        public int getVarcharSize(Record rec) {
            return 0;
        }

        @Override
        public void initValueIndex(int valueIndex) {
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
        }

        @Override
        public void setNull(MapValue mapValue) {
        }
    };

    @Test(expected = UnsupportedOperationException.class)
    public void testSetByte() {
        function.setByte(null, (byte) 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetDouble() {
        function.setDouble(null, 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetFloat() {
        function.setFloat(null, 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetInt() {
        function.setInt(null, 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetLong() {
        function.setLong(null, 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetShort() {
        function.setShort(null, (short) 0);
    }
}
