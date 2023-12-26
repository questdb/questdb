/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.cairo;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.CharSinkBase;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.engine.TestBinarySequence;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

public class RecordSinkFactoryTest extends AbstractCairoTest {

    @Test
    public void testColumnKeysAllSupportedTypes() {
        testColumnKeysAllSupportedTypes(false);
    }

    @Test
    public void testColumnKeysSkewIndex() {
        ArrayColumnTypes columnTypes = allArrayColumnTypes();
        int skew = 42;
        IntList skewIndex = new IntList();
        for (int i = 0, n = columnTypes.getColumnCount(); i < n; i++) {
            skewIndex.add(i + skew);
        }

        IntList expectedGetIndexes = new IntList();
        IntList expectedGetTypes = new IntList();
        IntList expectedPutTypes = new IntList();
        ListColumnFilter columnFilter = new ListColumnFilter();
        for (int i = 0, n = columnTypes.getColumnCount(); i < n; i++) {
            columnFilter.add(i + 1);
        }

        prepareExpectedIndexesAndTypes(columnTypes, expectedGetIndexes, expectedGetTypes, expectedPutTypes, skew, false);

        TestRecord testRecord = new TestRecord();
        TestRecordSink testRecordSink = new TestRecordSink();

        RecordSink sink = RecordSinkFactory.getInstance(new BytecodeAssembler(), columnTypes, columnFilter, null, false, skewIndex);
        sink.copy(testRecord, testRecordSink);

        Assert.assertEquals(expectedGetIndexes, testRecord.recordedIndexes);
        Assert.assertEquals(expectedGetTypes, testRecord.recordedTypes);
        Assert.assertEquals(expectedPutTypes, testRecordSink.recordedTypes);
    }

    @Test
    public void testColumnKeysSymAsString() {
        testColumnKeysAllSupportedTypes(false);
    }

    @Test
    public void testFunctionKeysAllSupportedTypes() {
        ArrayColumnTypes columnTypes = new ArrayColumnTypes();
        ListColumnFilter columnFilter = new ListColumnFilter();

        ObjList<Function> keyFunctions = allKeyFunctionTypes();

        IntList expectedPutTypes = new IntList();
        for (int i = 0, n = keyFunctions.size(); i < n; i++) {
            TestFunction func = (TestFunction) keyFunctions.getQuick(i);
            prepareExpectedPutType(func.type, expectedPutTypes, true);
        }

        TestRecord testRecord = new TestRecord();
        TestRecordSink testRecordSink = new TestRecordSink();

        RecordSink sink = RecordSinkFactory.getInstance(new BytecodeAssembler(), columnTypes, columnFilter, keyFunctions, false);
        sink.copy(testRecord, testRecordSink);

        for (int i = 0, n = keyFunctions.size(); i < n; i++) {
            TestFunction func = (TestFunction) keyFunctions.getQuick(i);
            if (func.type == ColumnType.LONG128 || func.type == ColumnType.UUID) {
                // LONG128 and UUID are accessed via two get calls, for low and high parts
                Assert.assertEquals(2, func.callCount);
            } else {
                Assert.assertEquals(1, func.callCount);
            }
        }
        Assert.assertEquals(expectedPutTypes, testRecordSink.recordedTypes);
    }

    @NotNull
    private static ArrayColumnTypes allArrayColumnTypes() {
        ArrayColumnTypes columnTypes = new ArrayColumnTypes();
        columnTypes.add(ColumnType.INT);
        columnTypes.add(ColumnType.IPv4);
        columnTypes.add(ColumnType.SYMBOL);
        columnTypes.add(ColumnType.LONG);
        columnTypes.add(ColumnType.DATE);
        columnTypes.add(ColumnType.TIMESTAMP);
        columnTypes.add(ColumnType.BYTE);
        columnTypes.add(ColumnType.SHORT);
        columnTypes.add(ColumnType.CHAR);
        columnTypes.add(ColumnType.BOOLEAN);
        columnTypes.add(ColumnType.FLOAT);
        columnTypes.add(ColumnType.DOUBLE);
        columnTypes.add(ColumnType.STRING);
        columnTypes.add(ColumnType.BINARY);
        columnTypes.add(ColumnType.LONG256);
        columnTypes.add(ColumnType.GEOBYTE);
        columnTypes.add(ColumnType.GEOSHORT);
        columnTypes.add(ColumnType.GEOINT);
        columnTypes.add(ColumnType.GEOLONG);
        columnTypes.add(ColumnType.LONG128);
        columnTypes.add(ColumnType.UUID);
        return columnTypes;
    }

    @NotNull
    private static ObjList<Function> allKeyFunctionTypes() {
        ObjList<Function> keyFunctions = new ObjList<>();
        keyFunctions.add(new TestFunction(ColumnType.INT));
        keyFunctions.add(new TestFunction(ColumnType.IPv4));
        keyFunctions.add(new TestFunction(ColumnType.SYMBOL));
        keyFunctions.add(new TestFunction(ColumnType.LONG));
        keyFunctions.add(new TestFunction(ColumnType.DATE));
        keyFunctions.add(new TestFunction(ColumnType.TIMESTAMP));
        keyFunctions.add(new TestFunction(ColumnType.BYTE));
        keyFunctions.add(new TestFunction(ColumnType.SHORT));
        keyFunctions.add(new TestFunction(ColumnType.CHAR));
        keyFunctions.add(new TestFunction(ColumnType.BOOLEAN));
        keyFunctions.add(new TestFunction(ColumnType.FLOAT));
        keyFunctions.add(new TestFunction(ColumnType.DOUBLE));
        keyFunctions.add(new TestFunction(ColumnType.STRING));
        keyFunctions.add(new TestFunction(ColumnType.BINARY));
        keyFunctions.add(new TestFunction(ColumnType.LONG256));
        keyFunctions.add(new TestFunction(ColumnType.GEOBYTE));
        keyFunctions.add(new TestFunction(ColumnType.GEOSHORT));
        keyFunctions.add(new TestFunction(ColumnType.GEOINT));
        keyFunctions.add(new TestFunction(ColumnType.GEOLONG));
        keyFunctions.add(new TestFunction(ColumnType.LONG128));
        keyFunctions.add(new TestFunction(ColumnType.UUID));
        return keyFunctions;
    }

    private static void prepareExpectedIndexesAndTypes(
            ArrayColumnTypes columnTypes,
            IntList expectedGetIndexes,
            IntList expectedGetTypes,
            IntList expectedPutTypes,
            int indexSkew,
            boolean symAsString
    ) {
        for (int i = 0, n = columnTypes.getColumnCount(); i < n; i++) {
            int type = columnTypes.getColumnType(i);
            prepareExpectedPutType(type, expectedPutTypes, symAsString);
            switch (ColumnType.tagOf(type)) {
                case ColumnType.LONG128:
                case ColumnType.UUID:
                    // LONG128 and UUID are accessed via two get calls, for low and high parts
                    expectedGetIndexes.add(i + indexSkew);
                    expectedGetIndexes.add(i + indexSkew);
                    expectedGetTypes.add(ColumnType.LONG128);
                    expectedGetTypes.add(ColumnType.LONG128);
                    break;
                case ColumnType.SYMBOL:
                    expectedGetIndexes.add(i + indexSkew);
                    expectedGetTypes.add(ColumnType.INT);
                    break;
                case ColumnType.IPv4:
                    expectedGetIndexes.add(i + indexSkew);
                    expectedGetTypes.add(ColumnType.IPv4);
                    break;
                case ColumnType.GEOBYTE:
                    expectedGetIndexes.add(i + indexSkew);
                    expectedGetTypes.add(ColumnType.GEOBYTE);
                    break;
                case ColumnType.GEOSHORT:
                    expectedGetIndexes.add(i + indexSkew);
                    expectedGetTypes.add(ColumnType.GEOSHORT);
                    break;
                case ColumnType.GEOINT:
                    expectedGetIndexes.add(i + indexSkew);
                    expectedGetTypes.add(ColumnType.GEOINT);
                    break;
                case ColumnType.GEOLONG:
                    expectedGetIndexes.add(i + indexSkew);
                    expectedGetTypes.add(ColumnType.GEOLONG);
                    break;
                default:
                    expectedGetIndexes.add(i + indexSkew);
                    expectedGetTypes.add(type);
            }
        }
    }

    private static void prepareExpectedPutType(int type, IntList expectedPutTypes, boolean symAsString) {
        switch (ColumnType.tagOf(type)) {
            case ColumnType.LONG128:
            case ColumnType.UUID:
                // LONG128 and UUID are accessed via two get calls, for low and high parts
                expectedPutTypes.add(ColumnType.LONG128);
                break;
            case ColumnType.SYMBOL:
                if (symAsString) {
                    expectedPutTypes.add(ColumnType.STRING);
                } else {
                    expectedPutTypes.add(ColumnType.INT);
                }
                break;
            case ColumnType.IPv4:
            case ColumnType.GEOINT:
                expectedPutTypes.add(ColumnType.INT);
                break;
            case ColumnType.GEOBYTE:
                expectedPutTypes.add(ColumnType.BYTE);
                break;
            case ColumnType.GEOSHORT:
                expectedPutTypes.add(ColumnType.SHORT);
                break;
            case ColumnType.GEOLONG:
                expectedPutTypes.add(ColumnType.LONG);
                break;
            default:
                expectedPutTypes.add(type);
        }
    }

    private static void testColumnKeysAllSupportedTypes(boolean symAsString) {
        ArrayColumnTypes columnTypes = allArrayColumnTypes();

        IntList expectedGetIndexes = new IntList();
        IntList expectedGetTypes = new IntList();
        IntList expectedPutTypes = new IntList();
        ListColumnFilter columnFilter = new ListColumnFilter();
        for (int i = 0, n = columnTypes.getColumnCount(); i < n; i++) {
            columnFilter.add(i + 1);
        }

        prepareExpectedIndexesAndTypes(columnTypes, expectedGetIndexes, expectedGetTypes, expectedPutTypes, 0, symAsString);

        TestRecord testRecord = new TestRecord();
        TestRecordSink testRecordSink = new TestRecordSink();

        RecordSink sink = RecordSinkFactory.getInstance(new BytecodeAssembler(), columnTypes, columnFilter, null, symAsString);
        sink.copy(testRecord, testRecordSink);

        Assert.assertEquals(expectedGetIndexes, testRecord.recordedIndexes);
        Assert.assertEquals(expectedGetTypes, testRecord.recordedTypes);
        Assert.assertEquals(expectedPutTypes, testRecordSink.recordedTypes);
    }

    private static class TestFunction implements Function {
        final int type;
        int callCount;

        private TestFunction(int type) {
            this.type = type;
        }

        @Override
        public int getArrayLength() {
            throw new UnsupportedOperationException();
        }

        @Override
        public BinarySequence getBin(Record rec) {
            Assert.assertEquals(ColumnType.BINARY, type);
            callCount++;
            return new TestBinarySequence().of(new byte[]{1, 2, 3});
        }

        @Override
        public long getBinLen(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean getBool(Record rec) {
            Assert.assertEquals(ColumnType.BOOLEAN, type);
            callCount++;
            return true;
        }

        @Override
        public byte getByte(Record rec) {
            Assert.assertEquals(ColumnType.BYTE, type);
            callCount++;
            return 1;
        }

        @Override
        public char getChar(Record rec) {
            Assert.assertEquals(ColumnType.CHAR, type);
            callCount++;
            return 'a';
        }

        @Override
        public long getDate(Record rec) {
            Assert.assertEquals(ColumnType.DATE, type);
            callCount++;
            return 1;
        }

        @Override
        public double getDouble(Record rec) {
            Assert.assertEquals(ColumnType.DOUBLE, type);
            callCount++;
            return 1;
        }

        @Override
        public float getFloat(Record rec) {
            Assert.assertEquals(ColumnType.FLOAT, type);
            callCount++;
            return 1;
        }

        @Override
        public byte getGeoByte(Record rec) {
            Assert.assertEquals(ColumnType.GEOBYTE, type);
            callCount++;
            return 1;
        }

        @Override
        public int getGeoInt(Record rec) {
            Assert.assertEquals(ColumnType.GEOINT, type);
            callCount++;
            return 1;
        }

        @Override
        public long getGeoLong(Record rec) {
            Assert.assertEquals(ColumnType.GEOLONG, type);
            callCount++;
            return 1;
        }

        @Override
        public short getGeoShort(Record rec) {
            Assert.assertEquals(ColumnType.GEOSHORT, type);
            callCount++;
            return 1;
        }

        @Override
        public int getIPv4(Record rec) {
            Assert.assertEquals(ColumnType.IPv4, type);
            callCount++;
            return 1;
        }

        @Override
        public int getInt(Record rec) {
            Assert.assertEquals(ColumnType.INT, type);
            callCount++;
            return 1;
        }

        @Override
        public long getLong(Record rec) {
            Assert.assertEquals(ColumnType.LONG, type);
            callCount++;
            return 1;
        }

        @Override
        public long getLong128Hi(Record rec) {
            if (type != ColumnType.LONG128 && type != ColumnType.UUID) {
                Assert.fail("LONG128 or UUID expected, was " + ColumnType.nameOf(type));
            }
            callCount++;
            return 1;
        }

        @Override
        public long getLong128Lo(Record rec) {
            if (type != ColumnType.LONG128 && type != ColumnType.UUID) {
                Assert.fail("LONG128 or UUID expected, was " + ColumnType.nameOf(type));
            }
            callCount++;
            return 1;
        }

        @Override
        public void getLong256(Record rec, CharSinkBase<?> sink) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Long256 getLong256A(Record rec) {
            Assert.assertEquals(ColumnType.LONG256, type);
            callCount++;
            Long256Impl long256 = new Long256Impl();
            long256.setAll(1, 1, 1, 1);
            return long256;
        }

        @Override
        public Long256 getLong256B(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Record getRecord(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RecordCursorFactory getRecordCursorFactory() {
            throw new UnsupportedOperationException();
        }

        @Override
        public short getShort(Record rec) {
            Assert.assertEquals(ColumnType.SHORT, type);
            callCount++;
            return 1;
        }

        @Override
        public CharSequence getStr(Record rec) {
            Assert.assertEquals(ColumnType.STRING, type);
            callCount++;
            return "abc";
        }

        @Override
        public CharSequence getStr(Record rec, int arrayIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void getStr(Record rec, CharSink sink) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void getStr(Record rec, CharSink sink, int arrayIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CharSequence getStrB(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CharSequence getStrB(Record rec, int arrayIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getStrLen(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getStrLen(Record rec, int arrayIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            Assert.assertEquals(ColumnType.SYMBOL, type);
            callCount++;
            return "abc";
        }

        @Override
        public CharSequence getSymbolB(Record rec) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getTimestamp(Record rec) {
            Assert.assertEquals(ColumnType.TIMESTAMP, type);
            callCount++;
            return 1;
        }

        @Override
        public int getType() {
            return type;
        }
    }

    private static class TestRecord implements Record {
        final IntList recordedIndexes = new IntList();
        final IntList recordedTypes = new IntList();

        @Override
        public BinarySequence getBin(int col) {
            recordedIndexes.add(col);
            recordedTypes.add(ColumnType.BINARY);
            return new TestBinarySequence().of(new byte[]{1, 2, 3});
        }

        @Override
        public boolean getBool(int col) {
            recordedIndexes.add(col);
            recordedTypes.add(ColumnType.BOOLEAN);
            return true;
        }

        @Override
        public byte getByte(int col) {
            recordedIndexes.add(col);
            recordedTypes.add(ColumnType.BYTE);
            return 1;
        }

        @Override
        public char getChar(int col) {
            recordedIndexes.add(col);
            recordedTypes.add(ColumnType.CHAR);
            return 'a';
        }

        @Override
        public long getDate(int col) {
            recordedIndexes.add(col);
            recordedTypes.add(ColumnType.DATE);
            return 1;
        }

        @Override
        public double getDouble(int col) {
            recordedIndexes.add(col);
            recordedTypes.add(ColumnType.DOUBLE);
            return 1;
        }

        @Override
        public float getFloat(int col) {
            recordedIndexes.add(col);
            recordedTypes.add(ColumnType.FLOAT);
            return 1;
        }

        @Override
        public byte getGeoByte(int col) {
            recordedIndexes.add(col);
            recordedTypes.add(ColumnType.GEOBYTE);
            return 1;
        }

        @Override
        public int getGeoInt(int col) {
            recordedIndexes.add(col);
            recordedTypes.add(ColumnType.GEOINT);
            return 1;
        }

        @Override
        public long getGeoLong(int col) {
            recordedIndexes.add(col);
            recordedTypes.add(ColumnType.GEOLONG);
            return 1;
        }

        @Override
        public short getGeoShort(int col) {
            recordedIndexes.add(col);
            recordedTypes.add(ColumnType.GEOSHORT);
            return 1;
        }

        @Override
        public int getIPv4(int col) {
            recordedIndexes.add(col);
            recordedTypes.add(ColumnType.IPv4);
            return 1;
        }

        @Override
        public int getInt(int col) {
            recordedIndexes.add(col);
            recordedTypes.add(ColumnType.INT);
            return 1;
        }

        @Override
        public long getLong(int col) {
            recordedIndexes.add(col);
            recordedTypes.add(ColumnType.LONG);
            return 1;
        }

        @Override
        public long getLong128Hi(int col) {
            recordedIndexes.add(col);
            recordedTypes.add(ColumnType.LONG128);
            return 1;
        }

        @Override
        public long getLong128Lo(int col) {
            recordedIndexes.add(col);
            recordedTypes.add(ColumnType.LONG128);
            return 1;
        }

        @Override
        public Long256 getLong256A(int col) {
            recordedIndexes.add(col);
            recordedTypes.add(ColumnType.LONG256);
            Long256Impl long256 = new Long256Impl();
            long256.setAll(1, 1, 1, 1);
            return long256;
        }

        @Override
        public short getShort(int col) {
            recordedIndexes.add(col);
            recordedTypes.add(ColumnType.SHORT);
            return 1;
        }

        @Override
        public CharSequence getStr(int col) {
            recordedIndexes.add(col);
            recordedTypes.add(ColumnType.STRING);
            return "abc";
        }

        @Override
        public CharSequence getSym(int col) {
            recordedIndexes.add(col);
            recordedTypes.add(ColumnType.SYMBOL);
            return "abc";
        }

        @Override
        public long getTimestamp(int col) {
            recordedIndexes.add(col);
            recordedTypes.add(ColumnType.TIMESTAMP);
            return 1;
        }
    }

    private static class TestRecordSink implements RecordSinkSPI {
        final IntList recordedTypes = new IntList();

        @Override
        public void putBin(BinarySequence value) {
            recordedTypes.add(ColumnType.BINARY);
        }

        @Override
        public void putBool(boolean value) {
            recordedTypes.add(ColumnType.BOOLEAN);
        }

        @Override
        public void putByte(byte value) {
            recordedTypes.add(ColumnType.BYTE);
        }

        @Override
        public void putChar(char value) {
            recordedTypes.add(ColumnType.CHAR);
        }

        @Override
        public void putDate(long value) {
            recordedTypes.add(ColumnType.DATE);
        }

        @Override
        public void putDouble(double value) {
            recordedTypes.add(ColumnType.DOUBLE);
        }

        @Override
        public void putFloat(float value) {
            recordedTypes.add(ColumnType.FLOAT);
        }

        @Override
        public void putInt(int value) {
            recordedTypes.add(ColumnType.INT);
        }

        @Override
        public void putLong(long value) {
            recordedTypes.add(ColumnType.LONG);
        }

        @Override
        public void putLong128(long lo, long hi) {
            recordedTypes.add(ColumnType.LONG128);
        }

        @Override
        public void putLong256(Long256 value) {
            recordedTypes.add(ColumnType.LONG256);
        }

        @Override
        public void putRecord(Record value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putShort(short value) {
            recordedTypes.add(ColumnType.SHORT);
        }

        @Override
        public void putStr(CharSequence value) {
            recordedTypes.add(ColumnType.STRING);
        }

        @Override
        public void putStr(CharSequence value, int lo, int hi) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putTimestamp(long value) {
            recordedTypes.add(ColumnType.TIMESTAMP);
        }

        @Override
        public void skip(int bytes) {
            recordedTypes.add(ColumnType.UNDEFINED);
        }
    }
}