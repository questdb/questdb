/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
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

package com.nfsdb.test.tools;

import com.nfsdb.Journal;
import com.nfsdb.JournalEntryWriter;
import com.nfsdb.JournalWriter;
import com.nfsdb.Partition;
import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.NumericException;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.iter.JournalIterator;
import com.nfsdb.misc.*;
import com.nfsdb.model.Quote;
import com.nfsdb.model.TestEntity;
import com.nfsdb.printer.JournalPrinter;
import com.nfsdb.printer.appender.AssertingAppender;
import com.nfsdb.printer.converter.DateConverter;
import com.nfsdb.ql.model.ExprNode;
import com.nfsdb.query.ResultSet;
import com.nfsdb.std.IntList;
import com.nfsdb.std.LongList;
import com.nfsdb.store.ColumnType;
import com.nfsdb.store.KVIndex;
import com.nfsdb.store.SymbolTable;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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

        IntList colKeyCount = new IntList();

        for (int k = 0; k < expected.getMetadata().getColumnCount(); k++) {
            SymbolTable et = expected.getMetadata().getColumn(k).symbolTable;
            SymbolTable at = actual.getMetadata().getColumn(k).symbolTable;

            if (et == null && at == null) {
                continue;
            }

            if ((et == null) || (at == null)) {
                Assert.fail("SymbolTable mismatch");
            }

            Assert.assertEquals(et.size(), at.size());
            colKeyCount.extendAndSet(k, et.size());

            for (int i = 0; i < et.size(); i++) {
                String ev = et.value(i);
                String av = at.value(i);
                Assert.assertEquals(ev, av);
                Assert.assertEquals(et.getQuick(ev), at.getQuick(av));
            }
        }

        LongList ev = new LongList();
        LongList av = new LongList();

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
                if (expected.getMetadata().getColumn(k).indexed) {
                    KVIndex ei = ep.getIndexForColumn(k);
                    KVIndex ai = ap.getIndexForColumn(k);

                    int count = colKeyCount.getQuick(k);

                    for (int j = 0; j < count; j++) {
                        ev.clear();
                        av.clear();
                        ei.getValues(j, ev);
                        ai.getValues(j, av);

                        Assert.assertEquals("Values mismatch. partition=" + i + ",column=" + expected.getMetadata().getColumn(k).name + ", key=" + j + ": ", ev.size(), av.size());
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
                                long pa = ByteBuffers.getAddress(bufA);
                                long pb = ByteBuffers.getAddress(bufB);
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
        if (expected == null && actual == null) {
            return;
        }

        if (expected != null && actual == null) {
            Assert.fail("Expected: \n`" + expected + "`but have NULL");
        }

        if (expected == null) {
            Assert.fail("Expected: NULL but have \n`" + actual + "`\n");
        }

        if (expected.length() != actual.length()) {
            Assert.fail("Expected: \n`" + expected + "`\n but have \n`" + actual + "`\n (length: " + expected.length() + " vs " + actual.length() + ")");
        }
        Assert.assertEquals(expected.length(), actual.length());
        for (int i = 0; i < expected.length(); i++) {
            if (expected.charAt(i) != actual.charAt(i)) {
                Assert.fail("At: " + i + ". Expected: `" + expected + "`, actual: `" + actual + '`');
            }
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
            SymbolTable m = a.getMetadata().getColumn(i).symbolTable;
            if (m != null) {
                SymbolTable s = b.getMetadata().getColumn(i).symbolTable;
                for (SymbolTable.Entry e : m.values()) {
                    Assert.assertEquals(m.getQuick(e.value), s.getQuick(e.value));
                }
            }
        }
    }

    public static void generateQuoteData(JournalWriter<Quote> w, int count) throws JournalException, NumericException {
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

    public static void generateTestEntityData(JournalWriter<TestEntity> w, int count) throws JournalException, NumericException {
        generateTestEntityData(w, count, Dates.parseDateTime("2012-05-15T10:55:00.000Z"), count * 100);
    }

    public static long toMemory(CharSequence sequence) {
        long lo = Unsafe.getUnsafe().allocateMemory(sequence.length() * 2);
        long ptr = lo;

        for (int i = 0; i < sequence.length(); i++) {
            Unsafe.getUnsafe().putByte(lo++, (byte) sequence.charAt(i));
        }

        return ptr;
    }

    public static String toRpn(ExprNode node) {
        switch (node.paramCount) {
            case 0:
                return node.token;
            case 1:
                return toRpn(node.rhs) + node.token;
            case 2:
                return toRpn(node.lhs) + toRpn(node.rhs) + node.token;
            default:
                String result = "";
                for (int i = 0; i < node.paramCount; i++) {
                    result = toRpn(node.args.getQuick(i)) + result;
                }
                return result + node.token;
        }
    }

    private static void configure(JournalPrinter p, JournalMetadata meta) {
        p.types(meta.getModelClass());

        for (int i = 0; i < meta.getColumnCount(); i++) {
            ColumnMetadata m = meta.getColumn(i);
            if (m.offset != 0) {
                JournalPrinter.Field f = p.f(m.name);
                if (m.type == ColumnType.DATE) {
                    f.c(new DateConverter(p));
                }
            }
        }
    }

    private static <T> void out(JournalPrinter p, JournalIterator<T> iterator) {
        for (T o : iterator) {
            p.out(o);
        }
    }
}
