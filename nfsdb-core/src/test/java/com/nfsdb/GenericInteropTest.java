/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb;

import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalMetadataException;
import com.nfsdb.factory.configuration.JournalMetadataBuilder;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.lang.cst.impl.qry.JournalRecord;
import com.nfsdb.lang.cst.impl.qry.JournalRecordSource;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

public class GenericInteropTest extends AbstractTest {

    @Test
    public void testGenericAll() throws Exception {
        JournalWriter writer = makeGenericWriter();
        JournalEntryWriter w = writer.entryWriter();

        w.putSym(0, "EURUSD");
        w.putDate(1, 19999);
        w.putDouble(2, 1.24);
        w.putDouble(3, 1.25);
        w.putInt(4, 10000);
        w.putInt(5, 12000);
        w.putInt(6, 1);
        w.putStr(7, "OK");
        w.putStr(8, "system");
        w.putStr(9, "EURUSD:GLOBAL");
        w.putBool(10, true);
        w.putNull(11);
        w.putLong(12, 13141516);
        w.putShort(13, (short) 25000);
        w.append();

        w = writer.entryWriter();

        w.putSym(0, "EURUSD");
        w.putDate(1, 20000);
        w.putDouble(2, 1.23);
        w.putDouble(3, 1.26);
        w.putInt(4, 11000);
        w.putInt(5, 13000);
        w.putInt(6, 2);
        w.putStr(7, "STALE");
        w.putStr(8, "system");
        w.putStr(9, "EURUSD:GLOBAL");
        w.putBool(10, false);
        w.putNull(11);
        w.putLong(12, 23242526);
        w.putShort(13, (short) 30000);
        w.append();

        writer.commit();

        Journal reader = factory.reader("test");
        JournalRecordSource src = reader.rows();
        JournalRecord e;

        Assert.assertTrue(src.hasNext());
        Assert.assertNotNull(e = src.next());

        Assert.assertEquals("EURUSD", e.getSym(0));
        Assert.assertEquals(19999, e.getDate(1));
        Assert.assertEquals(1.24, e.getDouble(2), 0.000001);
        Assert.assertEquals(1.25, e.getDouble(3), 0.000001);
        Assert.assertEquals(10000, e.getInt(4));
        Assert.assertEquals(12000, e.getInt(5));
        Assert.assertEquals(1, e.get(6));
        Assert.assertEquals("OK", e.getStr(7));
        Assert.assertEquals("system", e.getStr(8));
        Assert.assertEquals("EURUSD:GLOBAL", e.getStr(9));
        Assert.assertTrue(e.getBool(10));
        Assert.assertNull(e.getStr(11));
        Assert.assertEquals(13141516, e.getLong(12));
        Assert.assertEquals(25000, e.getShort(13));

        Assert.assertTrue(src.hasNext());
        Assert.assertNotNull(e = src.next());

        Assert.assertEquals("EURUSD", e.getSym(0));
        Assert.assertEquals(20000, e.getDate(1));
        Assert.assertEquals(1.23, e.getDouble(2), 0.000001);
        Assert.assertEquals(1.26, e.getDouble(3), 0.000001);
        Assert.assertEquals(11000, e.getInt(4));
        Assert.assertEquals(13000, e.getInt(5));
        Assert.assertEquals(2, e.get(6));
        Assert.assertEquals("STALE", e.getStr(7));
        Assert.assertEquals("system", e.getStr(8));
        Assert.assertEquals("EURUSD:GLOBAL", e.getStr(9));
        Assert.assertFalse(e.getBool(10));
        Assert.assertNull(e.getStr(11));
        Assert.assertEquals(23242526, e.getLong(12));
        Assert.assertEquals(30000, e.getShort(13));

        Assert.assertFalse(src.hasNext());
    }

    @Test
    public void testGenericWriteObjectRead() throws Exception {
        JournalWriter writer = makeGenericWriter();

        JournalEntryWriter w = writer.entryWriter();

        w.putSym(0, "EURUSD");
        w.putDate(1, 19999);
        w.putDouble(2, 1.24);
        w.putDouble(3, 1.25);
        w.putInt(4, 10000);
        w.putInt(5, 12000);
        w.putInt(6, 1);
        w.putStr(7, "OK");
        w.putStr(8, "system");
        w.putStr(9, "EURUSD:GLOBAL");
        w.putBool(10, true);
        w.putNull(11);
        w.putLong(12, 1234567);
        w.putShort(13, (short) 11000);
        w.append();

        writer.commit();

        Journal<Data> reader = factory.reader(Data.class, "test");

        Iterator<Data> src = reader.bufferedIterator();
        Assert.assertTrue(src.hasNext());
        Data d;
        Assert.assertNotNull(d = src.next());

        Assert.assertEquals("EURUSD", d.sym);
        Assert.assertEquals(19999, d.created);
        Assert.assertEquals(1.24, d.bid, 0.000001);
        Assert.assertEquals(1.25, d.ask, 0.000001);
        Assert.assertEquals(10000, d.bidSize);
        Assert.assertEquals(12000, d.askSize);
        Assert.assertEquals(1, d.id);
        Assert.assertEquals("OK", d.status);
        Assert.assertEquals("system", d.user);
        Assert.assertEquals("EURUSD:GLOBAL", d.rateId);
        Assert.assertTrue(d.active);
        Assert.assertNull(d.nullable);
        Assert.assertEquals(1234567, d.ticks);
        Assert.assertEquals(11000, d.modulo);
    }

    @Test
    public void testObjectWriteGenericRead() throws Exception {
        JournalWriter<Data> writer = factory.writer(new JournalMetadataBuilder<Data>(Data.class) {{
            $sym("sym").index();
            $int("id").index();
            $str("rateId").index();
            location("test");
        }});

        Data d = new Data();
        d.sym = "GBPUSD";
        d.created = 30000;
        d.bid = 0.65;
        d.ask = 0.66;
        d.bidSize = 1000;
        d.askSize = 1100;
        d.id = 1;
        d.status = "OK";
        d.user = "system";
        d.rateId = "GBPUSD:GLOBAL";
        d.active = true;
        d.nullable = null;
        d.ticks = 12345678;
        d.modulo = 425;

        writer.append(d);
        writer.commit();

        Journal reader = factory.reader("test");
        JournalRecordSource src = reader.rows();
        JournalRecord e;

        Assert.assertTrue(src.hasNext());
        Assert.assertNotNull(e = src.next());

        Assert.assertEquals("GBPUSD", e.getSym(0));
        Assert.assertEquals(30000, e.getDate(1));
        Assert.assertEquals(0.65, e.getDouble(2), 0.000001);
        Assert.assertEquals(0.66, e.getDouble(3), 0.000001);
        Assert.assertEquals(1000, e.getInt(4));
        Assert.assertEquals(1100, e.getInt(5));
        Assert.assertEquals(1, e.get(6));
        Assert.assertEquals("OK", e.getStr(7));
        Assert.assertEquals("system", e.getStr(8));
        Assert.assertEquals("GBPUSD:GLOBAL", e.getStr(9));
        Assert.assertTrue(e.getBool(10));
        Assert.assertNull(e.getStr(11));
        Assert.assertEquals(12345678, e.getLong(12));
        Assert.assertEquals(425, e.getShort(13));

        Assert.assertFalse(src.hasNext());
    }

    @Test
    public void testObjectGenericObjectWriteSequence() throws Exception {
        JournalWriter<Data> writer = factory.writer(new JournalMetadataBuilder<Data>(Data.class) {{
            $date("created");
            $sym("sym").index();
            $int("id").index();
            $str("rateId").index();
            location("test");
        }});

        Data d = new Data();
        d.sym = "GBPUSD";
        d.created = 30000;
        d.bid = 0.65;
        d.ask = 0.66;
        d.bidSize = 1000;
        d.askSize = 1100;
        d.id = 1;
        d.status = "OK";
        d.user = "system";
        d.rateId = "GBPUSD:GLOBAL";
        d.active = true;
        d.nullable = null;
        d.ticks = 12345678;
        d.modulo = 425;

        writer.append(d);
        writer.commit();

        writer.close();

        JournalWriter writer2 = makeGenericWriter();
        JournalEntryWriter w = writer2.entryWriter();

        w.putSym(0, "EURUSD");
        w.putDate(1, 19999);
        w.putDouble(2, 1.24);
        w.putDouble(3, 1.25);
        w.putInt(4, 10000);
        w.putInt(5, 12000);
        w.putInt(6, 2);
        w.putStr(7, "OK");
        w.putStr(8, "system");
        w.putStr(9, "EURUSD:GLOBAL");
        w.putBool(10, true);
        w.putNull(11);
        w.putLong(12, 1234567);
        w.putShort(13, (short) 11000);
        w.append();

        writer2.commit();

        writer2.close();

        writer = factory.writer(Data.class, "test");

        d.sym = "HKDUSD";
        d.created = 40000;
        d.bid = 2.88;
        d.ask = 2.89;
        d.bidSize = 1000;
        d.askSize = 1100;
        d.id = 3;
        d.status = "OK";
        d.user = "system";
        d.rateId = "HKDUSD:GLOBAL";
        d.active = true;
        d.nullable = null;
        d.ticks = 989931;
        d.modulo = 398;

        writer.append(d);
        writer.commit();

        Journal<Data> reader = factory.reader(Data.class, "test");
        String expected = "GBPUSD\t1970-01-01T00:00:30.000Z\t0.65\t0.66\t1000\t1100\t1\tOK\tsystem\tGBPUSD:GLOBAL\ttrue\tnull\t12345678\t425\n" +
                "EURUSD\t1970-01-01T00:00:19.999Z\t1.24\t1.25\t10000\t12000\t2\tOK\tsystem\tEURUSD:GLOBAL\ttrue\tnull\t1234567\t11000\n" +
                "HKDUSD\t1970-01-01T00:00:40.000Z\t2.88\t2.89\t1000\t1100\t3\tOK\tsystem\tHKDUSD:GLOBAL\ttrue\tnull\t989931\t398\n";
        TestUtils.assertEquals(expected, reader.bufferedIterator());
    }

    @Test
    public void testGenericStructureMismatch() throws Exception {
        JournalWriter writer = makeGenericWriter();
        JournalEntryWriter w = writer.entryWriter();

        w.putSym(0, "EURUSD");
        w.putDate(1, 19999);
        w.putDouble(2, 1.24);
        w.putDouble(3, 1.25);
        w.putInt(4, 10000);
        w.putInt(5, 12000);
        w.putInt(6, 1);
        w.putStr(7, "OK");
        w.putStr(8, "system");
        w.putStr(9, "EURUSD:GLOBAL");
        w.putBool(10, true);
        w.putNull(11);
        w.putLong(12, 13141516);
        w.putShort(13, (short) 25000);
        w.append();
        writer.commit();

        writer.close();

        try {
            factory.writer(new JournalStructure("test") {{
                $str("sym");
                $date("created");
            }});
            Assert.fail("Expected exception");
        } catch (JournalMetadataException ignore) {
            // expected
        }
    }

    @Test
    public void testPartialObjectWriter() throws Exception {
        JournalWriter writer = makeGenericWriter();
        writer.close();

        try {
            factory.writer(Partial.class, "test");
            Assert.fail("Expected exception");
        } catch (JournalException ignore) {
            // ignore exception
        }
    }

    @Test
    public void testPartialObjectReader() throws Exception {
        JournalWriter writer = makeGenericWriter();

        JournalEntryWriter w = writer.entryWriter();

        w.putSym(0, "EURUSD");
        w.putDate(1, 19999);
        w.putDouble(2, 1.24);
        w.putDouble(3, 1.25);
        w.putInt(4, 10000);
        w.putInt(5, 12000);
        w.putInt(6, 1);
        w.putStr(7, "OK");
        w.putStr(8, "system");
        w.putStr(9, "EURUSD:GLOBAL");
        w.putBool(10, true);
        w.putNull(11);
        w.putLong(12, 13141516);
        w.putShort(13, (short) 25000);
        w.append();
        writer.commit();

        writer.close();


        Journal<Partial> reader = factory.reader(Partial.class, "test");

        String expected = "EURUSD\t1970-01-01T00:00:19.999Z\t1.24\t1.25\t10000\t12000";

        TestUtils.assertEquals(expected, reader.bufferedIterator());
    }

    private JournalWriter makeGenericWriter() throws JournalException {
        return factory.writer(new JournalStructure("test") {{
            $sym("sym").index();
            $date("created");
            $double("bid");
            $double("ask");
            $int("bidSize");
            $int("askSize");
            $int("id").index();
            $str("status");
            $str("user");
            $str("rateId").index();
            $bool("active");
            $str("nullable");
            $long("ticks");
            $short("modulo");
        }});
    }

    public static class Data {
        private String sym;
        private long created;
        private double bid;
        private double ask;
        private int bidSize;
        private int askSize;
        private int id;
        private String status;
        private String user;
        private String rateId;
        private boolean active;
        private String nullable;
        private long ticks;
        private short modulo;
    }

    public static class Partial {
        private String sym;
        private long created;
        private double bid;
        private double ask;
        private int bidSize;
        private int askSize;
    }

    public static class WrongType {
        private String sym;
        private long created;
        private int bid;
        private double ask;
        private int bidSize;
        private int askSize;
    }
}
