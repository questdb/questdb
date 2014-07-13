/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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
 */

package com.nfsdb.thrift;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.test.tools.JournalTestFactory;
import com.nfsdb.journal.utils.Dates;
import com.nfsdb.thrift.model.Quote;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TProtocol;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.BitSet;

public class ThriftNullsTest {

    @Rule
    public final JournalTestFactory factory = new JournalTestFactory("/nfsdb-thrift.xml", new ThriftNullsAdaptorFactory());

    @Test
    public void testByteBitField() throws Exception {
        ThriftNullsAdaptor<ByteFieldSample> adaptor = new ThriftNullsAdaptor<>(ByteFieldSample.class);
        BitSet source = new BitSet();
        source.set(0);
        source.set(2);
        source.set(3);

        ByteFieldSample sample = new ByteFieldSample();
        adaptor.setNulls(sample, source);
        Assert.assertEquals("1", Integer.toBinaryString(sample.__isset_bitfield));
    }

    @Test
    public void testShortBitField() throws Exception {
        ThriftNullsAdaptor<ShortFieldSample> adaptor = new ThriftNullsAdaptor<>(ShortFieldSample.class);
        BitSet source = new BitSet();
        source.set(0);
        source.set(2);
        source.set(1);

        ShortFieldSample sample = new ShortFieldSample();
        adaptor.setNulls(sample, source);
        Assert.assertEquals("10", Integer.toBinaryString(sample.__isset_bitfield));
    }

    @Test
    public void testIntBitField() throws Exception {
        ThriftNullsAdaptor<IntFieldSample> adaptor = new ThriftNullsAdaptor<>(IntFieldSample.class);
        BitSet source = new BitSet();
        source.set(0);
        source.set(2);
        source.set(3);

        IntFieldSample sample = new IntFieldSample();
        adaptor.setNulls(sample, source);
        Assert.assertEquals("1", Integer.toBinaryString(sample.__isset_bitfield));
    }

    @Test
    public void testLongBitField() throws Exception {
        ThriftNullsAdaptor<LongFieldSample> adaptor = new ThriftNullsAdaptor<>(LongFieldSample.class);
        BitSet source = new BitSet();
        source.set(0);
        source.set(2);
        source.set(3);

        LongFieldSample sample = new LongFieldSample();
        adaptor.setNulls(sample, source);
        Assert.assertEquals("1", Integer.toBinaryString(sample.__isset_bitfield));
    }

    @Test
    public void testBitSet() throws Exception {
        ThriftNullsAdaptor<BitSetSample> adaptor = new ThriftNullsAdaptor<>(BitSetSample.class);
        BitSet source = new BitSet();
        source.set(0);
        source.set(2);
        source.set(3);

        BitSetSample sample = new BitSetSample();
        adaptor.setNulls(sample, source);
        Assert.assertEquals("{0}", sample.__isset_bit_vector.toString());
    }

    @Test
    public void testGetByteBitField() throws Exception {
        ThriftNullsAdaptor<ByteFieldSample> adaptor = new ThriftNullsAdaptor<>(ByteFieldSample.class);
        BitSet dst = new BitSet();

        ByteFieldSample sample = new ByteFieldSample();
        sample.__isset_bitfield = 2;
        adaptor.getNulls(sample, dst);
        Assert.assertEquals("{1}", dst.toString());
    }

    @Test
    public void testGetShortBitField() throws Exception {
        ThriftNullsAdaptor<ShortFieldSample> adaptor = new ThriftNullsAdaptor<>(ShortFieldSample.class);
        BitSet dst = new BitSet();

        ShortFieldSample sample = new ShortFieldSample();
        sample.__isset_bitfield = 2;
        adaptor.getNulls(sample, dst);
        Assert.assertEquals("{1}", dst.toString());
    }

    @Test
    public void testGetIntBitField() throws Exception {
        ThriftNullsAdaptor<IntFieldSample> adaptor = new ThriftNullsAdaptor<>(IntFieldSample.class);
        BitSet dst = new BitSet();

        IntFieldSample sample = new IntFieldSample();
        sample.__isset_bitfield = 2;
        adaptor.getNulls(sample, dst);
        Assert.assertEquals("{1}", dst.toString());
    }

    @Test
    public void testGetLongBitField() throws Exception {
        ThriftNullsAdaptor<LongFieldSample> adaptor = new ThriftNullsAdaptor<>(LongFieldSample.class);
        BitSet dst = new BitSet();

        LongFieldSample sample = new LongFieldSample();
        sample.__isset_bitfield = 2;
        adaptor.getNulls(sample, dst);
        Assert.assertEquals("{1}", dst.toString());
    }

    @Test
    public void testGetBitSet() throws Exception {
        ThriftNullsAdaptor<BitSetSample> adaptor = new ThriftNullsAdaptor<>(BitSetSample.class);
        BitSet dst = new BitSet();

        BitSetSample sample = new BitSetSample();
        sample.__isset_bit_vector.set(1);
        adaptor.getNulls(sample, dst);
        Assert.assertEquals("{1}", dst.toString());
    }

    @Test
    public void testFirstSymbolNull() throws JournalException {

        JournalWriter<Quote> w = factory.writer(Quote.class, "quote", 1000);
        long timestamp = Dates.toMillis("2013-10-05T10:00:00.000Z");
        Quote q = new Quote();
        for (int i = 0; i < 3; i++) {
            w.clearObject(q);
            if (i == 0) {
                q.setAsk(123);
            }

            q.setTimestamp(timestamp);
            w.append(q);
        }

        w.commit();
        w.close();

        Journal<Quote> r = factory.reader(Quote.class, "quote");
        q = r.read(0);
        Quote q1 = r.read(1);

        Assert.assertNull(q.getSym());
        Assert.assertTrue(q.isSetAsk());

        Assert.assertFalse(q1.isSetAsk());
    }

    private static abstract class AbstractSample implements TBase {
        @Override
        public void read(TProtocol tProtocol) throws TException {

        }

        @Override
        public void write(TProtocol tProtocol) throws TException {

        }

        @Override
        public TFieldIdEnum fieldForId(int i) {
            return null;
        }

        @Override
        public boolean isSet(TFieldIdEnum tFieldIdEnum) {
            return false;
        }

        @Override
        public Object getFieldValue(TFieldIdEnum tFieldIdEnum) {
            return null;
        }

        @Override
        public void setFieldValue(TFieldIdEnum tFieldIdEnum, Object o) {

        }

        @Override
        public TBase deepCopy() {
            return null;
        }


        @Override
        public int compareTo(Object o) {
            return this == o ? 0 : -1;
        }
    }

    @SuppressWarnings("unused")
    private static class ByteFieldSample extends AbstractSample {
        private byte __isset_bitfield = 16;
        private String s1;
        private int a;
        private String s2;
        private long b;

        @Override
        public void clear() {
            __isset_bitfield = 0;
            s1 = null;
            a = 0;
            s2 = null;
            b = 0;
        }
    }

    @SuppressWarnings("unused")
    private static class ShortFieldSample extends AbstractSample {
        private short __isset_bitfield = 0;
        private String s1;
        private int a;
        private String s2;
        private long b;

        @Override
        public void clear() {
            __isset_bitfield = 0;
            s1 = null;
            a = 0;
            s2 = null;
            b = 0;
        }
    }

    @SuppressWarnings("unused")
    private static class IntFieldSample extends AbstractSample {
        private int __isset_bitfield = 0;
        private String s1;
        private int a;
        private String s2;
        private long b;

        @Override
        public void clear() {
            __isset_bitfield = 0;
            s1 = null;
            a = 0;
            s2 = null;
            b = 0;
        }
    }

    @SuppressWarnings("unused")
    private static class LongFieldSample extends AbstractSample {
        private int __isset_bitfield = 16;
        private String s1;
        private int a;
        private String s2;
        private long b;

        @Override
        public void clear() {
            __isset_bitfield = 0;
            s1 = null;
            a = 0;
            s2 = null;
            b = 0;
        }
    }

    @SuppressWarnings("unused")
    private static class BitSetSample extends AbstractSample {
        private final BitSet __isset_bit_vector = new BitSet(2);
        private String s1;
        private int a;
        private String s2;
        private long b;

        @Override
        public void clear() {
            __isset_bit_vector.clear();
            s1 = null;
            a = 0;
            s2 = null;
            b = 0;
        }
    }
}
