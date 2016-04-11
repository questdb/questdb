/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.ql.impl;

import com.nfsdb.JournalWriter;
import com.nfsdb.ex.JournalConfigurationException;
import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.ex.ParserException;
import com.nfsdb.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.misc.Files;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.impl.join.hash.MemoryRecordAccessor;
import com.nfsdb.ql.parser.QueryCompiler;
import com.nfsdb.std.DirectInputStream;
import com.nfsdb.std.LongList;
import com.nfsdb.store.MemoryPages;
import com.nfsdb.test.tools.JournalTestFactory;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;


public class MemoryRecordAccessorTest {
    @Rule
    public final JournalTestFactory factory;
    private final QueryCompiler compiler;

    public MemoryRecordAccessorTest() {
        try {
            this.factory = new JournalTestFactory(
                    new JournalConfigurationBuilder().build(Files.makeTempDir())
            );
            this.compiler = new QueryCompiler();
        } catch (JournalConfigurationException e) {
            throw new JournalRuntimeException(e);
        }
    }

    @Test
    public void testAllFieldTypesField() throws JournalException, IOException, ParserException {
        writeAndReadRecords(factory.writer(AllFieldTypes.class), 1000, 64 * 1024,
                new RecordGenerator<AllFieldTypes>() {

                    @Override
                    public void assertRecord(Record value, int i) throws IOException {
                        AllFieldTypes expected = generate(i);
                        int col = 0;
                        String failedMsg = "Record " + i;
                        Assert.assertEquals(failedMsg, expected.aBool, value.getBool(col++));
                        Assert.assertEquals(failedMsg, expected.aString, value.getStr(col++).toString());
                        Assert.assertEquals(failedMsg, expected.aByte, value.get(col++));
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
    public void testCopyBinToAddress() throws JournalException, IOException, ParserException {
        final int pageLen = 1024 * 1024;
        writeAndReadRecords(factory.writer(Binary.class), 1, pageLen,
                new RecordGenerator<Binary>() {

                    @Override
                    public void assertRecord(Record value, int i) throws IOException {
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
        writeAndReadRecords(factory.writer(Binary.class), 1, pageLen,
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
        writeAndReadRecords(factory.writer(LongValue.class), 100, 450,
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
        writeAndReadRecords(factory.writer(StringLongBinary.class), 3, pageLen,
                new RecordGenerator<StringLongBinary>() {

                    @Override
                    public void assertRecord(Record value, int i) throws IOException {
                        StringLongBinary expected = generate(i);

                        CharSequence str = value.getStr(0);
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
        long address = Unsafe.getUnsafe().allocateMemory(sz);
        long p = address;
        actual.copyTo(address, 0, sz);
        for (long i = 0; i < sz; i++) {
            Assert.assertEquals(expected.get(), Unsafe.getUnsafe().getByte(p++));
        }
        Unsafe.getUnsafe().freeMemory(address);
    }

    private <T> void writeAndReadRecords(JournalWriter<T> journal, int count, int pageSize, RecordGenerator<T> generator) throws IOException, JournalException, ParserException {
        for (int i = 0; i < count; i++) {
            journal.append(generator.generate(i));
        }
        journal.commit();

        try (MemoryPages buffer = new MemoryPages(pageSize)) {
            MemoryRecordAccessor a = new MemoryRecordAccessor(journal.getMetadata(), buffer);
            LongList offsets = new LongList();

            for (Record rec : compiler.compile(factory, journal.getLocation().getName())) {
                offsets.add(a.append(rec));
            }

            for (int i = 0; i < count; i++) {
                a.of(offsets.getQuick(i));
                generator.assertRecord(a, i);
            }
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