/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.ql;

import com.questdb.ex.ParserException;
import com.questdb.parser.sql.QueryCompiler;
import com.questdb.std.DirectInputStream;
import com.questdb.std.LongList;
import com.questdb.std.Unsafe;
import com.questdb.std.ex.JournalException;
import com.questdb.store.JournalWriter;
import com.questdb.store.Record;
import com.questdb.store.RecordCursor;
import com.questdb.test.tools.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;


public class RecordListTest extends AbstractTest {
    private final QueryCompiler compiler = new QueryCompiler();

    @Test
    public void testAllFieldTypesField() throws JournalException, IOException, ParserException {
        writeAndReadRecords(getFactory().writer(AllFieldTypes.class), 1000, 64 * 1024,
                new RecordGenerator<AllFieldTypes>() {
                    @Override
                    public void assertRecord(Record value, int i) throws IOException {
                        AllFieldTypes expected = generate(i);
                        int col = 0;
                        String failedMsg = "Record " + i;
                        Assert.assertEquals(failedMsg, expected.aBool, value.getBool(col++));
                        Assert.assertEquals(failedMsg, expected.aString, value.getFlyweightStr(col++).toString());
                        Assert.assertEquals(failedMsg, expected.aByte, value.getByte(col++));
                        Assert.assertEquals(failedMsg, expected.aShort, value.getShort(col++));
                        Assert.assertEquals(failedMsg, expected.anInt, value.getInt(col++));
                        DirectInputStream binCol = value.getBin(col++);
                        byte[] expectedBin = expected.aBinary.array();
                        Assert.assertEquals(failedMsg, expectedBin.length, binCol.size());
                        for (int j = 0; j < expectedBin.length; j++) {
                            Assert.assertEquals(failedMsg + " byte " + j, expectedBin[j], (byte) binCol.read());
                        }
                        Assert.assertEquals(failedMsg, expected.aLong, value.getLong(col++));
                        Assert.assertEquals(failedMsg, expected.aDouble, value.getDouble(col++), 0.0001);
                        Assert.assertEquals(failedMsg, expected.aFloat, value.getFloat(col), 0.0001);
                    }

                    @Override
                    public AllFieldTypes generate(int i) {
                        AllFieldTypes af = new AllFieldTypes();
                        byte[] bin = new byte[i];
                        for (int j = 0; j < i; j++) {
                            bin[j] = (byte) (j % 255);
                        }
                        af.aBinary = ByteBuffer.wrap(bin);
                        af.aBool = i % 2 == 0;
                        af.aByte = (byte) (i % 255);
                        af.aDouble = i * Math.PI;
                        af.aFloat = (float) (Math.PI / i);
                        af.aLong = i * 2;
                        af.anInt = i;
                        af.aShort = (short) (i / 2);
                        StringBuilder sb = new StringBuilder(i);
                        for (int j = 0; j < i; j++) {
                            sb.append((char) j);
                        }
                        af.aString = sb.toString();
                        return af;
                    }
                });
    }

    @Test
    public void testBlankList() throws Exception {
        writeAndReadRecords(getFactory().writer(LongValue.class), 0, 450,
                new RecordGenerator<LongValue>() {
                    @Override
                    public void assertRecord(Record value, int i) {
                    }

                    @Override
                    public LongValue generate(int i) {
                        return new LongValue(i);
                    }
                });
    }

    @Test
    public void testCopyBinToAddress() throws JournalException, IOException, ParserException {
        final int pageLen = 1024 * 1024;
        writeAndReadRecords(getFactory().writer(Binary.class), 1, pageLen,
                new RecordGenerator<Binary>() {

                    @Override
                    public void assertRecord(Record value, int i) {
                        DirectInputStream binCol = value.getBin(0);
                        Binary expected = generate(i);
                        Assert.assertEquals(expected.aBinary.remaining(), binCol.size());
                        assertEquals(expected.aBinary, binCol);
                    }

                    @Override
                    public Binary generate(int i) {
                        Binary af = new Binary();
                        byte[] bin = new byte[1024 * 1024 - 15];
                        for (int j = 0; j < bin.length; j++) {
                            bin[j] = (byte) 'A';
                        }
                        af.aBinary = ByteBuffer.wrap(bin);
                        return af;
                    }
                });
    }

    @Test
    public void testSaveBinOverPageEdge() throws JournalException, IOException, ParserException {
        final int pageLen = 100;
        writeAndReadRecords(getFactory().writer(Binary.class), 1, pageLen,
                new RecordGenerator<Binary>() {

                    @Override
                    public void assertRecord(Record value, int i) throws IOException {
                        DirectInputStream binCol = value.getBin(0);
                        Binary expected = generate(i);
                        byte[] expectedBin = expected.aBinary.array();
                        Assert.assertEquals(expected.aBinary.remaining(), binCol.size());
                        for (int j = 0; j < expectedBin.length; j++) {
                            Assert.assertEquals(expectedBin[j], (byte) binCol.read());
                        }
                    }

                    @Override
                    public Binary generate(int i) {
                        Binary af = new Binary();
                        byte[] bin = new byte[pageLen];
                        for (int j = 0; j < bin.length; j++) {
                            bin[j] = (byte) (j % 255);
                        }
                        af.aBinary = ByteBuffer.wrap(bin);
                        return af;
                    }
                });
    }

    @Test
    public void testSaveLongField() throws JournalException, IOException, ParserException {
        writeAndReadRecords(getFactory().writer(LongValue.class), 100, 450,
                new RecordGenerator<LongValue>() {

                    @Override
                    public void assertRecord(Record value, int i) {
                        Assert.assertEquals((long) i, value.getLong(0));
                    }

                    @Override
                    public LongValue generate(int i) {
                        return new LongValue(i);
                    }
                });
    }

    @Test
    public void testSaveNullBinAndStrings() throws JournalException, IOException, ParserException {
        final int pageLen = 100;
        writeAndReadRecords(getFactory().writer(StringLongBinary.class), 3, pageLen,
                new RecordGenerator<StringLongBinary>() {

                    @Override
                    public void assertRecord(Record value, int i) {
                        StringLongBinary expected = generate(i);

                        CharSequence str = value.getFlyweightStr(0);
                        if (expected.aString != null || str != null) {
                            Assert.assertEquals(expected.aString, str.toString());
                        }

                        Assert.assertEquals(expected.aLong, value.getLong(1));

                        DirectInputStream binCol = value.getBin(2);
                        if (expected.aBinary != null && binCol != null) {
                            byte[] expectedBin = expected.aBinary.array();
                            Assert.assertEquals(expectedBin.length, binCol.size());
                        }
                    }

                    @Override
                    public StringLongBinary generate(int i) {
                        StringLongBinary af = new StringLongBinary();
                        af.aLong = i;
                        af.aString = i == 0 ? "A" : null;
                        af.aBinary = i == 1 ? ByteBuffer.wrap(new byte[2]) : null;
                        return af;
                    }
                });
    }

    private static void assertEquals(ByteBuffer expected, DirectInputStream actual) {
        int sz = (int) actual.size();
        long address = Unsafe.malloc(sz);
        try {
            long p = address;
            actual.copyTo(address, 0, sz);
            for (long i = 0; i < sz; i++) {
                Assert.assertEquals(expected.get(), Unsafe.getUnsafe().getByte(p++));
            }
        } finally {
            Unsafe.free(address, sz);
        }
    }

    private <T> void writeAndReadRecords(JournalWriter<T> journal, int count, int pageSize, RecordGenerator<T> generator) throws IOException, JournalException, ParserException {
        try {
            for (int i = 0; i < count; i++) {
                journal.append(generator.generate(i));
            }
            journal.commit();

            try (RecordList records = new RecordList(journal.getMetadata(), pageSize)) {
                LongList offsets = new LongList();

                try (RecordSource rs = compiler.compile(getFactory(), journal.getLocation().getName())) {
                    long o = -1;
                    RecordCursor cursor = rs.prepareCursor(getFactory());
                    try {
                        for (Record rec : cursor) {
                            offsets.add(o = records.append(rec, o));
                        }
                    } finally {
                        cursor.releaseCursor();
                    }
                }

                int i = 0;
                records.toTop();

                while (records.hasNext()) {
                    generator.assertRecord(records.next(), i++);
                }
            }
        } finally {
            journal.close();
        }
    }

    private interface RecordGenerator<T> {
        void assertRecord(Record value, int i) throws IOException;

        T generate(int i);
    }

    private static class LongValue {
        long value;

        public LongValue() {
        }

        LongValue(long val) {
            value = val;
        }
    }

    private static class AllFieldTypes {
        boolean aBool;
        String aString;
        byte aByte;
        short aShort;
        int anInt;
        ByteBuffer aBinary;
        long aLong;
        double aDouble;
        float aFloat;
    }

    private static class Binary {
        ByteBuffer aBinary;
    }

    private static class StringLongBinary {
        String aString;
        long aLong;
        ByteBuffer aBinary;
    }
}