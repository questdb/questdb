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

package com.nfsdb.journal.test.tools;

import com.nfsdb.journal.*;
import com.nfsdb.journal.collections.DirectLongList;
import com.nfsdb.journal.column.ColumnType;
import com.nfsdb.journal.column.SymbolTable;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.factory.configuration.ColumnMetadata;
import com.nfsdb.journal.factory.configuration.JournalMetadata;
import com.nfsdb.journal.index.KVIndex;
import com.nfsdb.journal.iterators.JournalIterator;
import com.nfsdb.journal.model.Quote;
import com.nfsdb.journal.model.TestEntity;
import com.nfsdb.journal.printer.JournalPrinter;
import com.nfsdb.journal.printer.appender.AssertingAppender;
import com.nfsdb.journal.printer.converter.DateConverter;
import com.nfsdb.journal.utils.Dates;
import com.nfsdb.journal.utils.Lists;
import com.nfsdb.journal.utils.Rnd;
import com.nfsdb.journal.utils.Unsafe;
import org.joda.time.Interval;
import org.junit.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.UUID;

public final class TestUtils {

    private TestUtils() {
    }

    public static void generateQuoteData(JournalWriter<Quote> w, int count) throws JournalException {
        String symbols[] = {"AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L"};
        long timestamps[] = {Dates.toMillis("2013-09-04T10:00:00.000Z"), Dates.toMillis("2013-10-04T10:00:00.000Z"), Dates.toMillis("2013-11-04T10:00:00.000Z")};
        Quote q = new Quote();
        Rnd r = new Rnd(System.currentTimeMillis(), System.currentTimeMillis());
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
        Rnd r = new Rnd(System.currentTimeMillis(), System.currentTimeMillis());

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

    public static void generateQuoteData(JournalWriter<Quote> w, int count, Interval interval) throws JournalException {
        generateQuoteData(w, count, interval, true);
    }

    public static void generateQuoteData(JournalWriter<Quote> w, int count, Interval interval, boolean commit) throws JournalException {

        long span = interval.getEndMillis() - interval.getStartMillis();
        long inc = span / count;
        long lo = interval.getStartMillis();

        for (int i = 0; i < count; i++) {
            String symbols[] = {"AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L"};
            Quote q = new Quote();
            Rnd r = new Rnd(System.currentTimeMillis(), System.currentTimeMillis());

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

    @SuppressWarnings("unused")
    public static <T> void print(JournalIterator<T> iterator) throws IOException {
        try (JournalPrinter p = new JournalPrinter()) {
            configure(p, iterator.getJournal().getMetadata());
            out(p, iterator);
        }
    }

    public static <T> void assertOrder(JournalIterator<T> rs) {
        ColumnMetadata meta = rs.getJournal().getMetadata().getTimestampColumnMetadata();
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
        ColumnMetadata meta = rs.getJournal().getMetadata().getTimestampColumnMetadata();
        long max = Long.MAX_VALUE;
        for (T obj : rs) {
            long timestamp = Unsafe.getUnsafe().getLong(obj, meta.offset);
            if (timestamp > max) {
                throw new AssertionError("Timestamp out of order. [ " + Dates.toString(timestamp) + " > " + Dates.toString(max) + "]");
            }
            max = timestamp;
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

    public static void generateQuoteData(int count, long timestamp, int increment) {
        String symbols[] = {"AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L"};
        String exchanges[] = {"LXE", "GR", "SK", "LN"};
        Rnd r = new Rnd(System.currentTimeMillis(), System.currentTimeMillis());
        for (int i = 0; i < count; i++) {
            Quote q = new Quote();
            q.setSym(symbols[Math.abs(r.nextInt() % (symbols.length - 1))]);
            q.setAsk(Math.abs(r.nextDouble()));
            q.setBid(Math.abs(r.nextDouble()));
            q.setAskSize(Math.abs(r.nextInt()));
            q.setBidSize(Math.abs(r.nextInt()));
            q.setEx(exchanges[Math.abs(r.nextInt() % (exchanges.length - 1))]);
            q.setMode("Fast trading");
            q.setTimestamp(timestamp);
            timestamp += increment;
            print(q);
        }
    }

    public static void generateTestEntityData(JournalWriter<TestEntity> w, int count, long timetamp, int increment) throws JournalException {
        String symbols[] = {"AGK.L", "BP.L", "TLW.L", "ABF.L", "LLOY.L", "BT-A.L", "WTB.L", "RRS.L", "ADM.L", "GKN.L", "HSBA.L", null};
        Rnd r = new Rnd(System.currentTimeMillis(), System.nanoTime());
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
        generateTestEntityData(w, count, Dates.toMillis("2012-05-15T10:55:00.000Z"), count * 100);
    }

    public static <T> void assertDataEquals(Journal<T> expected, Journal<T> actual) throws JournalException {
        Assert.assertEquals(expected.size(), actual.size());
        ResultSet<T> er = expected.query().all().asResultSet();
        ResultSet<T> ar = actual.query().all().asResultSet();
        for (int i = 0; i < er.size(); i++) {
            Assert.assertEquals(er.read(i), ar.read(i));
        }
    }

    public static <T> void assertEquals(Journal<T> expected, Journal<T> actual) throws JournalException {
        Assert.assertEquals(expected.size(), actual.size());
        Assert.assertEquals(expected.getPartitionCount(), actual.getPartitionCount());
        // check if SymbolIndexes are the same

        ArrayList<Integer> colKeyCount = new ArrayList<>();

        for (int k = 0; k < expected.getMetadata().getColumnCount(); k++) {
            SymbolTable et = expected.getColumnMetadata(k).symbolTable;
            SymbolTable at = actual.getColumnMetadata(k).symbolTable;

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
                if (expected.getColumnMetadata(k).meta.indexed) {
                    KVIndex ei = ep.getIndexForColumn(k);
                    KVIndex ai = ap.getIndexForColumn(k);

                    int count = colKeyCount.get(k);

                    for (int j = 0; j < count; j++) {
                        ev.reset();
                        av.reset();
                        ei.getValues(j, ev);
                        ai.getValues(j, av);

                        Assert.assertEquals("Values mismatch. partition=" + i + ",column=" + expected.getColumnMetadata(k).meta.name + ", key=" + j + ": ", ev.size(), av.size());
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

    private static <T> void out(JournalPrinter p, JournalIterator<T> iterator) throws IOException {
        for (T o : iterator) {
            p.out(o);
        }
    }

    private static void print(Quote q) {
        StringBuilder sb = new StringBuilder();
        sb.append("w.append(");
        sb.append("new Quote()");
        sb.append(".setSym(\"").append(q.getSym()).append("\")");
        sb.append(".setAsk(").append(q.getAsk()).append(")");
        sb.append(".setBid(").append(q.getBid()).append(")");
        sb.append(".setAskSize(").append(q.getAskSize()).append(")");
        sb.append(".setBidSize(").append(q.getBidSize()).append(")");
        sb.append(".setEx(\"").append(q.getEx()).append("\")");
        sb.append(".setMode(\"").append(q.getMode()).append("\")");
        sb.append(".setTimestamp(Dates.toMillis(\"").append(Dates.toString(q.getTimestamp())).append("\"))");
        sb.append(")");
        sb.append(";");
        System.out.println(sb);
    }
}
