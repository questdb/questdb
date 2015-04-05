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

package com.nfsdb.test.tools;

import com.nfsdb.Journal;
import com.nfsdb.JournalEntryWriter;
import com.nfsdb.JournalWriter;
import com.nfsdb.Partition;
import com.nfsdb.collections.DirectLongList;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.model.Quote;
import com.nfsdb.model.TestEntity;
import com.nfsdb.printer.JournalPrinter;
import com.nfsdb.printer.appender.AssertingAppender;
import com.nfsdb.printer.converter.DateConverter;
import com.nfsdb.query.ResultSet;
import com.nfsdb.query.iterator.JournalIterator;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.storage.KVIndex;
import com.nfsdb.storage.SymbolTable;
import com.nfsdb.utils.*;
import org.junit.Assert;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public final class TestUtils {

    private TestUtils() {
    }

    public static void assertCounter(AtomicInteger counter, int value, long timeout, TimeUnit unit) {
        long t = System.currentTimeMillis();
        while (counter.get() < value && (System.currentTimeMillis() - t) < unit.toMillis(timeout)) {
            LockSupport.parkNanos(10000L);
        }

        Assert.assertEquals(value, counter.get());
    }

    public static <T> void assertDataEquals(Journal<T> expected, Journal<T> actual) throws JournalException {
        Assert.assertEquals(expected.size(), actual.size());
        ResultSet<T> er = expected.query().all().asResultSet();
        ResultSet<T> ar = actual.query().all().asResultSet();
        for (int i = 0; i < er.size(); i++) {
            Assert.assertEquals(er.read(i), ar.read(i));
        }
    }

    public static <T> void assertEquals(String expected, ResultSet<T> rs) throws IOException {
        assertEquals(expected, rs.bufferedIterator());
    }

    public static <T> void assertEquals(String expected, JournalIterator<T> actual) throws IOException {
        try (JournalPrinter p = new JournalPrinter()) {
            p.setAppender(new AssertingAppender(expected));
            configure(p, actual.getJournal().getMetadata());
            out(p, actual);
        }
    }

    public static <T> void assertEquals(Journal<T> expected, Journal<T> actual) throws JournalException {
        Assert.assertEquals(expected.size(), actual.size());
        Assert.assertEquals(expected.getPartitionCount(), actual.getPartitionCount());
        // check if SymbolIndexes are the same

        ArrayList<Integer> colKeyCount = new ArrayList<>();

        for (int k = 0; k < expected.getMetadata().getColumnCount(); k++) {
            SymbolTable et = expected.getMetadata().getColumnMetadata(k).symbolTable;
            SymbolTable at = actual.getMetadata().getColumnMetadata(k).symbolTable;

            if (et == null && at == null) {
                continue;
            }

            if ((et == null) || (at == null)) {
                Assert.fail("SymbolTable mismatch");
            }

            Assert.assertEquals(et.size(), at.size());

            Lists.advance(colKeyCount, k);

            colKeyCount.set(k, et.size());

            for (int i = 0; i < et.size(); i++) {
                String ev = et.value(i);
                String av = at.value(i);
                Assert.assertEquals(ev, av);
                Assert.assertEquals(et.getQuick(ev), at.getQuick(av));
            }
        }

        DirectLongList ev = new DirectLongList();
        DirectLongList av = new DirectLongList();

        // check if partitions are the same
        for (int i = 0; i < expected.getPartitionCount(); i++) {

            Partition<T> ep = expected.getPartition(i, true);
            Partition<T> ap = actual.getPartition(i, true);

            // compare names
            Assert.assertEquals(ep.getName(), ap.getName());
            // compare sizes
            Assert.assertEquals(ep.size(), ap.size());
            // compare intervals

            if (ep != expected.getIrregularPartition() || ap != actual.getIrregularPartition()) {
                Assert.assertEquals("Interval mismatch. partition=" + i, ep.getInterval(), ap.getInterval());
            }

            for (int k = 0; k < expected.getMetadata().getColumnCount(); k++) {
                if (expected.getMetadata().getColumnMetadata(k).indexed) {
                    KVIndex ei = ep.getIndexForColumn(k);
                    KVIndex ai = ap.getIndexForColumn(k);

                    int count = colKeyCount.get(k);

                    for (int j = 0; j < count; j++) {
                        ev.reset();
                        av.reset();
                        ei.getValues(j, ev);
                        ai.getValues(j, av);

                        Assert.assertEquals("Values mismatch. partition=" + i + ",column=" + expected.getMetadata().getColumnMetadata(k).name + ", key=" + j + ": ", ev.size(), av.size());
                        for (int l = 0; l < ev.size(); l++) {
                            Assert.assertEquals(ev.get(l), av.get(l));
                        }
                    }
                }
            }

            for (int k = 0; k < ep.size(); k++) {
                Assert.assertEquals(ep.read(k), ap.read(k));
            }
        }
    }

    public static <T> void assertEquals(Iterator<T> expected, Iterator<T> actual) {
        while (true) {
            boolean expectedHasNext = expected.hasNext();
            boolean actualHasNext = actual.hasNext();

            Assert.assertEquals(expectedHasNext, actualHasNext);

            if (!expectedHasNext) {
                break;
            }
            Assert.assertEquals(expected.next(), actual.next());
        }
    }

    public static void assertEquals(File a, File b) throws IOException {
        try (RandomAccessFile rafA = new RandomAccessFile(a, "r")) {
            try (RandomAccessFile rafB = new RandomAccessFile(b, "r")) {
                try (FileChannel chA = rafA.getChannel()) {
                    try (FileChannel chB = rafB.getChannel()) {
                        Assert.assertEquals(chA.size(), chB.size());
                        ByteBuffer bufA = chA.map(FileChannel.MapMode.READ_ONLY, 0, chA.size());
                        try {
                            ByteBuffer bufB = chB.map(FileChannel.MapMode.READ_ONLY, 0, chB.size());
                            try {
                                long pa = ((DirectBuffer) bufA).address();
                                long pb = ((DirectBuffer) bufB).address();
                                long lim = pa + bufA.limit();

                                while (pa + 8 < lim) {
                                    if (Unsafe.getUnsafe().getLong(pa) != Unsafe.getUnsafe().getLong(pb)) {
                                        Assert.fail();
                                    }
                                    pa += 8;
                                    pb += 8;
                                }

                                while (pa < lim) {
                                    if (Unsafe.getUnsafe().getByte(pa++) != Unsafe.getUnsafe().getByte(pb++)) {
                                        Assert.fail();
                                    }
                                }

                            } finally {
                                ByteBuffers.release(bufB);
                            }

                        } finally {
                            ByteBuffers.release(bufA);
                        }
                    }
                }
            }

        }
    }

    public static void assertEquals(CharSequence expected, CharSequence actual) {
        Assert.assertEquals(expected.length(), actual.length());
        for (int i = 0; i < expected.length(); i++) {
            Assert.assertEquals(expected.charAt(i), actual.charAt(i));
        }
    }

    public static <T> void assertOrder(JournalIterator<T> rs) {
        ColumnMetadata meta = rs.getJournal().getMetadata().getTimestampMetadata();
        long max = 0;
        for (T obj : rs) {
            long timestamp = Unsafe.getUnsafe().getLong(obj, meta.offset);
            if (timestamp < max) {
                throw new AssertionError("Timestamp out of order. [ " + Dates.toString(timestamp) + " < " + Dates.toString(max) + "]");
            }
            max = timestamp;
        }
    }

    public static <T> void assertOrderDesc(JournalIterator<T> rs) {
        ColumnMetadata meta = rs.getJournal().getMetadata().getTimestampMetadata();
        long max = Long.MAX_VALUE;
        for (T obj : rs) {
            long timestamp = Unsafe.getUnsafe().getLong(obj, meta.offset);
            if (timestamp > max) {
                throw new AssertionError("Timestamp out of order. [ " + Dates.toString(timestamp) + " > " + Dates.toString(max) + "]");
            }
            max = timestamp;
        }
    }

    public static void compareSymbolTables(Journal a, Journal b) {
        for (int i = 0; i < a.getMetadata().getColumnCount(); i++) {
            SymbolTable m = a.getMetadata().getColumnMetadata(i).symbolTable;
            if (m != null) {
                SymbolTable s = b.getMetadata().getColumnMetadata(i).symbolTable;
                for (String value : m.values()) {
                    Assert.assertEquals(m.getQuick(value), s.getQuick(value));
                }
            }
        }
    }

    public static void configure(JournalPrinter p, JournalMetadata meta) {
        p.types(meta.getModelClass());

        for (int i = 0; i < meta.getColumnCount(); i++) {
            ColumnMetadata m = meta.getColumnMetadata(i);
            if (m.offset != 0) {
                JournalPrinter.Field f = p.f(m.name);
                if (m.type == ColumnType.DATE) {
                    f.c(new DateConverter(p));
                }
            }
        }
    }

    public static void generateQuoteData(JournalWriter<Quote> w, int count) throws JournalException {
        String symbols[] = {"AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L"};
        long timestamps[] = {Dates.parseDateTime("2013-09-04T10:00:00.000Z"), Dates.parseDateTime("2013-10-04T10:00:00.000Z"), Dates.parseDateTime("2013-11-04T10:00:00.000Z")};
        Quote q = new Quote();
        Rnd r = new Rnd();
        for (int i = 0; i < count; i++) {
            q.clear();
            q.setSym(symbols[Math.abs(r.nextInt() % (symbols.length))]);
            q.setAsk(Math.abs(r.nextDouble()));
            q.setBid(Math.abs(r.nextDouble()));
            q.setAskSize(Math.abs(r.nextInt()));
            q.setBidSize(Math.abs(r.nextInt()));
            q.setEx("LXE");
            q.setMode("Fast trading");
            q.setTimestamp(timestamps[i * timestamps.length / count]);
            w.append(q);
        }
        w.commit();
    }

    public static void generateQuoteData(JournalWriter<Quote> w, int count, long timetamp) throws JournalException {
        generateQuoteData(w, count, timetamp, 0);
    }

    public static void generateQuoteData(JournalWriter<Quote> w, int count, long timestamp, long increment) throws JournalException {
        String symbols[] = {"AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L"};
        Quote q = new Quote();
        Rnd r = new Rnd();

        for (int i = 0; i < count; i++) {
            q.clear();
            q.setSym(symbols[Math.abs(r.nextInt() % (symbols.length - 1))]);
            q.setAsk(Math.abs(r.nextDouble()));
            q.setBid(Math.abs(r.nextDouble()));
            q.setAskSize(Math.abs(r.nextInt()));
            q.setBidSize(Math.abs(r.nextInt()));
            q.setEx("LXE");
            q.setMode("Fast trading");
            q.setTimestamp(timestamp);
            timestamp += increment;
            w.append(q);
        }
    }

    public static void generateQuoteData(JournalWriter<Quote> w, int count, Interval interval) throws JournalException {
        generateQuoteData(w, count, interval, true);
    }

    public static void generateQuoteData(JournalWriter<Quote> w, int count, Interval interval, boolean commit) throws JournalException {

        long span = interval.getHi() - interval.getLo();
        long inc = span / count;
        long lo = interval.getLo();

        Rnd r = new Rnd();
        for (int i = 0; i < count; i++) {
            String symbols[] = {"AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L"};
            Quote q = new Quote();

            q.clear();
            q.setSym(symbols[Math.abs(r.nextInt() % (symbols.length - 1))]);
            q.setAsk(Math.abs(r.nextDouble()));
            q.setBid(Math.abs(r.nextDouble()));
            q.setAskSize(Math.abs(r.nextInt()));
            q.setBidSize(Math.abs(r.nextInt()));
            q.setEx("LXE");
            q.setMode("Fast trading");
            q.setTimestamp(lo);
            w.append(q);
            lo += inc;
        }
        if (commit) {
            w.commit();
        }
    }

    public static void generateQuoteDataGeneric(JournalWriter w, int count, long timestamp, long increment) throws JournalException {
        String symbols[] = {"AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L"};
        Rnd r = new Rnd();

        for (int i = 0; i < count; i++) {
            JournalEntryWriter ew = w.entryWriter(timestamp);
            ew.putSym(0, symbols[Math.abs(r.nextInt() % (symbols.length - 1))]);
            ew.putDouble(1, Math.abs(r.nextDouble()));
            ew.putDouble(2, Math.abs(r.nextDouble()));
            ew.putInt(3, Math.abs(r.nextInt()));
            ew.putInt(4, Math.abs(r.nextInt()));
            ew.putSym(5, "LXE");
            ew.putSym(6, "Fast trading");
            ew.append();
            timestamp += increment;
        }
    }

    public static void generateTestEntityData(JournalWriter<TestEntity> w, int count, long timetamp, int increment) throws JournalException {
        String symbols[] = {"AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L", null};
        Rnd r = new Rnd();
        for (int i = 0; i < count; i++) {
            TestEntity e = new TestEntity();
            e.setSym(symbols[Math.abs(r.nextInt() % (symbols.length))]);
            e.setAnInt(Math.abs(r.nextInt()));
            e.setADouble(Math.abs(r.nextDouble()));
            e.setBStr(UUID.randomUUID().toString());
            e.setDStr(UUID.randomUUID().toString());
            e.setDwStr(UUID.randomUUID().toString());
            e.setTimestamp(timetamp);
            timetamp += increment;
            w.append(e);
        }
        w.commit();
    }

    public static void generateTestEntityData(JournalWriter<TestEntity> w, int count) throws JournalException {
        generateTestEntityData(w, count, Dates.parseDateTime("2012-05-15T10:55:00.000Z"), count * 100);
    }

    @SuppressWarnings("unused")
    public static <T> void print(JournalIterator<T> iterator) throws IOException {
        try (JournalPrinter p = new JournalPrinter()) {
            configure(p, iterator.getJournal().getMetadata());
            out(p, iterator);
        }
    }

    private static <T> void out(JournalPrinter p, JournalIterator<T> iterator) throws IOException {
        for (T o : iterator) {
            p.out(o);
        }
    }
}
