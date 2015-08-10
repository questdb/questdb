/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.ql.collections;

import com.nfsdb.JournalWriter;
import com.nfsdb.collections.DirectInputStream;
import com.nfsdb.exceptions.JournalConfigurationException;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.ql.Compiler;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordSource;
import com.nfsdb.ql.parser.ParserException;
import com.nfsdb.test.tools.JournalTestFactory;
import com.nfsdb.utils.Files;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class DirectRecordTest {
    @Rule
    public final JournalTestFactory factory;
    public final Compiler compiler;

    public DirectRecordTest() {
        try {
            this.factory = new JournalTestFactory(
                    new JournalConfigurationBuilder() {{
                    }}.build(Files.makeTempDir())
            );
            this.compiler = new Compiler(factory);
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
                        assertEquals(failedMsg, expected.aBool, value.getBool(col++));
                        assertEquals(failedMsg, expected.aString, value.getStr(col++).toString());
                        assertEquals(failedMsg, expected.aByte, value.get(col++));
                        assertEquals(failedMsg, expected.aShort, value.getShort(col++));
                        assertEquals(failedMsg, expected.anInt, value.getInt(col++));
                        DirectInputStream binCol = value.getBin(col++);
                        byte[] expectedBin = expected.aBinary.array();
                        assertEquals(failedMsg, expectedBin.length, binCol.getLength());
                        for (int j = 0; j < expectedBin.length; j++) {
                            assertEquals(failedMsg + " byte " + j, expectedBin[j], (byte) binCol.read());
                        }
                        assertEquals(failedMsg, expected.aLong, value.getLong(col++));
                        assertEquals(failedMsg, expected.aDouble, value.getDouble(col++), 0.0001);
                        assertEquals(failedMsg, expected.aFloat, value.getFloat(col), 0.0001);
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
    public void testSaveBinOverPageEdge() throws JournalException, IOException, ParserException {
        final int pageLen = 100;
        writeAndReadRecords(factory.writer(Binary.class), 1, pageLen,
                new RecordGenerator<Binary>() {

                    @Override
                    public void assertRecord(Record value, int i) throws IOException {
                        DirectInputStream binCol = value.getBin(0);
                        Binary expected = generate(i);
                        byte[] expectedBin = expected.aBinary.array();
                        assertEquals(expectedBin.length, binCol.getLength());
                        for (int j = 0; j < expectedBin.length; j++) {
                            assertEquals(expectedBin[j], (byte) binCol.read());
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
                        assertEquals((long) i, value.getLong(0));
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
                            assertEquals(expected.aString, str.toString());
                        }

                        assertEquals(expected.aLong, value.getLong(1));

                        DirectInputStream binCol = value.getBin(2);
                        if (expected.aBinary != null && binCol != null) {
                            byte[] expectedBin = expected.aBinary.array();
                            assertEquals(expectedBin.length, binCol.getLength());
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

    public <T> void writeAndReadRecords(JournalWriter<T> longJournal, int count, int pageSize, RecordGenerator<T> generator) throws IOException, JournalException, ParserException {
        for (int i = 0; i < count; i++) {
            longJournal.append(generator.generate(i));
        }
        longJournal.commit();

        RecordSource<? extends Record> rows = compiler.compileSource(longJournal.getLocation().getName());
        try (DirectPagedBuffer buffer = new DirectPagedBuffer(pageSize)) {
            DirectRecord dr = new DirectRecord(longJournal.getMetadata(), buffer);
            List<Long> offsets = new ArrayList<>();
            for (Record rec : rows.prepareCursor(factory)) {
                offsets.add(dr.write(rec));
            }

            for (int i = 0; i < count; i++) {
                Long ost = offsets.get(i);
                dr.init(ost);
                generator.assertRecord(dr, i);
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

        public long getValue() {
            return value;
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