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

package com.questdb;

import com.questdb.ex.JournalMetadataException;
import com.questdb.ql.RecordSource;
import com.questdb.std.ex.JournalException;
import com.questdb.store.*;
import com.questdb.store.factory.configuration.JournalMetadataBuilder;
import com.questdb.store.factory.configuration.JournalStructure;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;

public class GenericInteropTest extends AbstractTest {

    @Test
    public void testGenericAll() throws Exception {
        try (JournalWriter writer = makeGenericWriter()) {
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
        }

        try (RecordSource rs = compile("test")) {
            RecordCursor cursor = rs.prepareCursor(getFactory());

            try {
                Record e;

                Assert.assertTrue(cursor.hasNext());
                Assert.assertNotNull(e = cursor.next());

                Assert.assertEquals("EURUSD", e.getSym(0));
                Assert.assertEquals(19999, e.getDate(1));
                Assert.assertEquals(1.24, e.getDouble(2), 0.000001);
                Assert.assertEquals(1.25, e.getDouble(3), 0.000001);
                Assert.assertEquals(10000, e.getInt(4));
                Assert.assertEquals(12000, e.getInt(5));
                Assert.assertEquals(1, e.getByte(6));
                TestUtils.assertEquals("OK", e.getFlyweightStr(7));
                TestUtils.assertEquals("system", e.getFlyweightStr(8));
                TestUtils.assertEquals("EURUSD:GLOBAL", e.getFlyweightStr(9));
                Assert.assertTrue(e.getBool(10));
                Assert.assertNull(e.getFlyweightStr(11));
                Assert.assertEquals(13141516, e.getLong(12));
                Assert.assertEquals(25000, e.getShort(13));

                Assert.assertTrue(cursor.hasNext());
                Assert.assertNotNull(e = cursor.next());

                Assert.assertEquals("EURUSD", e.getSym(0));
                Assert.assertEquals(20000, e.getDate(1));
                Assert.assertEquals(1.23, e.getDouble(2), 0.000001);
                Assert.assertEquals(1.26, e.getDouble(3), 0.000001);
                Assert.assertEquals(11000, e.getInt(4));
                Assert.assertEquals(13000, e.getInt(5));
                Assert.assertEquals(2, e.getByte(6));
                TestUtils.assertEquals("STALE", e.getFlyweightStr(7));
                TestUtils.assertEquals("system", e.getFlyweightStr(8));
                TestUtils.assertEquals("EURUSD:GLOBAL", e.getFlyweightStr(9));
                Assert.assertFalse(e.getBool(10));
                Assert.assertNull(e.getFlyweightStr(11));
                Assert.assertEquals(23242526, e.getLong(12));
                Assert.assertEquals(30000, e.getShort(13));

                Assert.assertFalse(cursor.hasNext());
            } finally {
                cursor.releaseCursor();
            }
        }
    }

    @Test
    public void testGenericStructureMismatch() throws Exception {
        try (JournalWriter writer = makeGenericWriter()) {
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
        }

        try {
            getFactory().writer(new JournalStructure("test") {{
                $str("sym");
                $date("created");
            }});
            Assert.fail("Expected exception");
        } catch (JournalMetadataException ignore) {
            // expected
        }
    }

    @Test
    public void testGenericWriteObjectRead() throws Exception {
        try (JournalWriter writer = makeGenericWriter()) {

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
        }

        try (Journal<Data> reader = getFactory().reader(Data.class, "test")) {

            Iterator<Data> src = JournalIterators.bufferedIterator(reader);
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
    }

    @Test
    public void testInvalidColumnName() throws Exception {

        File location = null;

        try (JournalWriter w = getFactory().writer(new JournalStructure("test") {{
            $int("id").index();
            $str("status?\0x");
        }})) {
            location = w.getLocation();
            w.entryWriter();
        } catch (JournalException ignore) {
            //ignore
        }

        Assert.assertNotNull(location);

        Files.deleteOrException(location);

        try (JournalWriter w = getFactory().writer(new JournalStructure("test") {{
            $int("id").index();
            $str("status");
        }})) {
            w.entryWriter();
        }
    }

    @Test
    public void testObjectGenericObjectWriteSequence() throws Exception {
        JournalWriter<Data> writer = getFactory().writer(new JournalMetadataBuilder<Data>(Data.class, "test") {{
            $date("created");
            $sym("sym").index();
            $int("id").index();
            $str("rateId").index();
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

        try (JournalWriter writer2 = makeGenericWriter()) {
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
        }

        writer = getFactory().writer(Data.class, "test");

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

        try (Journal<Data> reader = getFactory().reader(Data.class, "test")) {
            String expected = "Data{sym='GBPUSD', created=30000, bid=0.65, ask=0.66, bidSize=1000, askSize=1100, id=1, status='OK', user='system', rateId='GBPUSD:GLOBAL', active=true, nullable='null', ticks=12345678, modulo=425}\n" +
                    "Data{sym='EURUSD', created=19999, bid=1.24, ask=1.25, bidSize=10000, askSize=12000, id=2, status='OK', user='system', rateId='EURUSD:GLOBAL', active=true, nullable='null', ticks=1234567, modulo=11000}\n" +
                    "Data{sym='HKDUSD', created=40000, bid=2.88, ask=2.89, bidSize=1000, askSize=1100, id=3, status='OK', user='system', rateId='HKDUSD:GLOBAL', active=true, nullable='null', ticks=989931, modulo=398}\n";

            StringBuilder builder = new StringBuilder();
            for (Data data : JournalIterators.bufferedIterator(reader)) {
                builder.append(data).append('\n');
            }
            TestUtils.assertEquals(expected, builder);
        }

        writer.close();
    }

    @Test
    public void testObjectWriteGenericRead() throws Exception {
        try (JournalWriter<Data> writer = getFactory().writer(new JournalMetadataBuilder<Data>(Data.class, "test") {{
            $sym("sym").index();
            $int("id").index();
            $str("rateId").index();
        }})) {

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
        }

        try (RecordSource rs = compile("test")) {
            RecordCursor cursor = rs.prepareCursor(getFactory());

            try {
                Record e;

                Assert.assertTrue(cursor.hasNext());
                Assert.assertNotNull(e = cursor.next());

                Assert.assertEquals("GBPUSD", e.getSym(0));
                Assert.assertEquals(30000, e.getDate(1));
                Assert.assertEquals(0.65, e.getDouble(2), 0.000001);
                Assert.assertEquals(0.66, e.getDouble(3), 0.000001);
                Assert.assertEquals(1000, e.getInt(4));
                Assert.assertEquals(1100, e.getInt(5));
                Assert.assertEquals(1, e.getByte(6));
                TestUtils.assertEquals("OK", e.getFlyweightStr(7));
                TestUtils.assertEquals("system", e.getFlyweightStr(8));
                TestUtils.assertEquals("GBPUSD:GLOBAL", e.getFlyweightStr(9));
                Assert.assertTrue(e.getBool(10));
                Assert.assertNull(e.getFlyweightStr(11));
                Assert.assertEquals(12345678, e.getLong(12));
                Assert.assertEquals(425, e.getShort(13));

                Assert.assertFalse(cursor.hasNext());
            } finally {
                cursor.releaseCursor();
            }
        }
    }

    @Test
    public void testPartialObjectReader() throws Exception {
        try (JournalWriter writer = makeGenericWriter()) {

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
        }


        try (Journal<Partial> reader = getFactory().reader(Partial.class, "test")) {

            String expected = "Partial{sym='EURUSD', created=19999, bid=1.24, ask=1.25, bidSize=10000, askSize=12000}";

            StringBuilder builder = new StringBuilder();
            for (Partial p : JournalIterators.bufferedIterator(reader)) {
                builder.append(p);
            }
            TestUtils.assertEquals(expected, builder);
        }
    }

    @Test
    public void testPartialObjectWriter() throws Exception {
        makeGenericWriter().close();
        try {
            getFactory().writer(Partial.class, "test");
            Assert.fail("Expected exception");
        } catch (JournalException ignore) {
            // ignore exception
        }
    }

    private JournalWriter makeGenericWriter() throws JournalException {
        return getFactory().writer(new JournalStructure("test") {{
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

        @Override
        public String toString() {
            return "Data{" +
                    "sym='" + sym + '\'' +
                    ", created=" + created +
                    ", bid=" + bid +
                    ", ask=" + ask +
                    ", bidSize=" + bidSize +
                    ", askSize=" + askSize +
                    ", id=" + id +
                    ", status='" + status + '\'' +
                    ", user='" + user + '\'' +
                    ", rateId='" + rateId + '\'' +
                    ", active=" + active +
                    ", nullable='" + nullable + '\'' +
                    ", ticks=" + ticks +
                    ", modulo=" + modulo +
                    '}';
        }
    }

    @SuppressWarnings("CanBeFinal")
    public static class Partial {
        private String sym;
        private long created;
        private double bid;
        private double ask;
        private int bidSize;
        private int askSize;

        @Override
        public String toString() {
            return "Partial{" +
                    "sym='" + sym + '\'' +
                    ", created=" + created +
                    ", bid=" + bid +
                    ", ask=" + ask +
                    ", bidSize=" + bidSize +
                    ", askSize=" + askSize +
                    '}';
        }
    }
}
