/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.cairo.sql;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Deferred frame addresses ({@link PageFrameCursor#deferFrameAddresses()}): a table scan's native
 * frames are enumerated without their column addresses, and whichever thread first navigates to a
 * frame computes them ({@link PageFrameAddressCache#ensureFrameReady(int)}). The cache must then hold,
 * for every frame and column, exactly what the eager enumeration puts there: page and aux addresses
 * and sizes, frame sizes, formats and row id offsets, also with several threads filling the frames
 * at once. The data covers every column shape the address computation distinguishes: fixed-size,
 * var-size with and without data memory, column tops, columns a partition does not have, Parquet
 * partitions among native ones, interval scans and both scan directions, and frames of 1 row up to
 * whole partitions. Queries over the same data must also print the same with the switch on and off.
 */
public class PageFrameDeferredAddressTest extends AbstractCairoTest {

    @Test
    public void testAddressesMatchEagerFrames() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final String[] queries = {
                    "select * from t",
                    "select a, s, v, ts from t",
                    "select * from t where ts in '1970-01-02'",
                    "select * from t where ts between '1970-01-01T05:00' and '1970-01-03T07:00'",
                    "select v, arr, b, extra, extra_v from t",
            };
            int deferredSeen = 0;
            for (int frameRows : new int[]{1, 3, 64, 1_000, 1_000_000}) {
                sqlExecutionContext.changePageFrameSizes(1, frameRows);
                for (String query : queries) {
                    for (int order : new int[]{PartitionFrameCursorFactory.ORDER_ASC, PartitionFrameCursorFactory.ORDER_DESC}) {
                        deferredSeen += assertSameAddresses(query, order);
                    }
                }
            }
            Assert.assertTrue(deferredSeen > 0);
        });
    }

    @Test
    public void testQueriesSameWithSwitchOnAndOff() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final String[] queries = {
                    "select * from t where a % 3 = 0",
                    "select * from t where s = 'k1' and a > 10",
                    "select * from t where v like '%7%' limit 20",
                    "select * from t where a > 5 order by ts desc limit 15",
                    "select * from t where a > 5 limit -9",
                    "select s, count(), sum(a), max(v), min(extra), count(extra_v) from t order by s",
                    "select count(), sum(d), avg(extra) from t where a % 2 = 1",
                    "select s, last(v), first(arr) from t where d > 0.3 order by s",
                    "select * from t where ts in '1970-01-02' and d < 0.5",
                    "select a, d from t where d > 0.1 order by d desc limit 7",
            };
            for (int frameRows : new int[]{1, 7, 100, 1_000_000}) {
                sqlExecutionContext.changePageFrameSizes(1, frameRows);
                for (String query : queries) {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "false");
                    final String expected = print(query);
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "true");
                    TestUtils.assertEquals(query + " [frameRows=" + frameRows + ']', expected, print(query));
                }
            }
        });
    }

    // Returns the deferred frames seen.
    private int assertSameAddresses(String query, int order) throws Exception {
        try (
                RecordCursorFactory factory = select(query);
                PageFrameAddressCache eager = new PageFrameAddressCache();
                PageFrameAddressCache deferred = new PageFrameAddressCache()
        ) {
            Assert.assertTrue(factory.supportsPageFrameCursor());
            final int frameCount;
            try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, order)) {
                frameCount = fill(factory, cursor, eager);
                for (int f = 0; f < frameCount; f++) {
                    Assert.assertTrue(eager.isFrameReady(f));
                }
            }
            int deferredFrames = 0;
            try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, order)) {
                // a projection's cursor may not defer: its frames then come eager, and must still match
                cursor.deferFrameAddresses();
                Assert.assertEquals(frameCount, fill(factory, cursor, deferred));
                for (int f = 0; f < frameCount; f++) {
                    if (!deferred.isFrameReady(f)) {
                        deferredFrames++;
                        Assert.assertEquals(PartitionFormat.NATIVE, deferred.getFrameFormat(f));
                    }
                }
                // four threads fill the frames at once, each in its own order
                final int threads = 4;
                final CyclicBarrier barrier = new CyclicBarrier(threads);
                final AtomicReference<Throwable> failure = new AtomicReference<>();
                final Thread[] workers = new Thread[threads];
                for (int t = 0; t < threads; t++) {
                    final long seed = t;
                    workers[t] = new Thread(() -> {
                        try {
                            final Rnd rnd = new Rnd(seed, seed + 1);
                            barrier.await();
                            for (int i = 0; i < frameCount; i++) {
                                deferred.ensureFrameReady(seed % 2 == 0 ? i : rnd.nextInt(frameCount));
                            }
                            for (int i = 0; i < frameCount; i++) {
                                deferred.ensureFrameReady(i);
                            }
                        } catch (Throwable th) {
                            failure.set(th);
                        }
                    });
                    workers[t].start();
                }
                for (Thread worker : workers) {
                    worker.join();
                }
                if (failure.get() != null) {
                    throw new AssertionError(failure.get());
                }
                // while the cursor and its partitions are still open
                assertSameCache(query, eager, deferred, frameCount);
            }
            return deferredFrames;
        }
    }

    private void assertSameCache(String query, PageFrameAddressCache eager, PageFrameAddressCache deferred, int frameCount) {
        Assert.assertEquals(eager.getFrameCount(), deferred.getFrameCount());
        final int columnCount = eager.getColumnCount();
        Assert.assertEquals(columnCount, deferred.getColumnCount());
        for (int f = 0; f < frameCount; f++) {
            final String at = query + " frame " + f;
            Assert.assertTrue(at, deferred.isFrameReady(f));
            Assert.assertEquals(at, eager.getFrameFormat(f), deferred.getFrameFormat(f));
            Assert.assertEquals(at, eager.getFrameSize(f), deferred.getFrameSize(f));
            Assert.assertEquals(at, eager.getRowIdOffset(f), deferred.getRowIdOffset(f));
            if (eager.getFrameFormat(f) != PartitionFormat.NATIVE) {
                Assert.assertSame(at, eager.getParquetDecoder(f), deferred.getParquetDecoder(f));
                Assert.assertEquals(at, eager.getParquetRowGroup(f), deferred.getParquetRowGroup(f));
                Assert.assertEquals(at, eager.getParquetRowGroupLo(f), deferred.getParquetRowGroupLo(f));
                Assert.assertEquals(at, eager.getParquetRowGroupHi(f), deferred.getParquetRowGroupHi(f));
                continue;
            }
            final int offset = eager.toColumnOffset(f);
            for (int c = 0; c < columnCount; c++) {
                final String col = at + " column " + c;
                Assert.assertEquals(col + " page address", eager.getPageAddresses().get(offset + c), deferred.getPageAddresses().get(offset + c));
                Assert.assertEquals(col + " page size", eager.getPageSizes().get(offset + c), deferred.getPageSizes().get(offset + c));
                Assert.assertEquals(col + " aux address", eager.getAuxPageAddresses().get(offset + c), deferred.getAuxPageAddresses().get(offset + c));
                Assert.assertEquals(col + " aux size", eager.getAuxPageSizes().get(offset + c), deferred.getAuxPageSizes().get(offset + c));
            }
        }
    }

    private void createTable() throws Exception {
        execute("create table t (a int, s symbol, v varchar, st string, b binary, arr double[], d double, ts timestamp) timestamp(ts) partition by DAY");
        // day 1 and 2 without the extra columns
        execute("insert into t select x::int, 'k' || (x % 5), case when x % 4 = 0 then null when x % 4 = 1 then 'v' || x else 'a long varchar value ' || x end, "
                + "case when x % 3 = 0 then null else 'st' || x end, rnd_bin(1, 8, 2), case when x % 5 = 0 then null else array[x::double, 1.5] end, "
                + "rnd_double(), (x * 60_000_000L)::timestamp from long_sequence(2_000)");
        execute("alter table t add column extra int");
        execute("alter table t add column extra_v varchar");
        // the rest of day 2 and day 3: the new columns start mid-partition, a column top
        execute("insert into t select (2000 + x)::int, 'k' || (x % 5), 'v' || x, 'st' || x, rnd_bin(1, 8, 2), array[x::double], rnd_double(), "
                + "((2000 + x) * 60_000_000L)::timestamp, x::int, case when x % 2 = 0 then null else 'e' || x end from long_sequence(2_000)");
        // a short varchar column of inlined values only: no data memory
        execute("alter table t add column short_v varchar");
        execute("insert into t select (4000 + x)::int, 'k1', 'v', 'st', null, null, rnd_double(), ((4000 + x) * 60_000_000L)::timestamp, x::int, 'e', 'ab' "
                + "from long_sequence(500)");
        execute("alter table t convert partition to parquet list '1970-01-02'");
    }

    private int fill(RecordCursorFactory factory, PageFrameCursor cursor, PageFrameAddressCache addressCache) {
        addressCache.of(factory.getMetadata(), cursor.getColumnMapping(), cursor.isExternal());
        int frameCount = 0;
        PageFrame frame;
        while ((frame = cursor.next()) != null) {
            addressCache.add(frameCount++, frame);
        }
        return frameCount;
    }

    private String print(String query) throws Exception {
        final StringSink sink = new StringSink();
        try (RecordCursorFactory factory = select(query); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            io.questdb.cairo.CursorPrinter.println(cursor, factory.getMetadata(), sink, true, false);
        }
        return sink.toString();
    }
}
