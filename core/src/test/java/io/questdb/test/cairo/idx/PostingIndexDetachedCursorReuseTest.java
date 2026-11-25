/*******************************************************************************
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

package io.questdb.test.cairo.idx;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.EmptyRowCursor;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.idx.AbstractPostingIndexReader;
import io.questdb.cairo.idx.CoveringRowCursor;
import io.questdb.cairo.idx.PostingIndexBwdReader;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Detached posting cursors ({@code getDetachedCursor}) are reused through a per-reader pool
 * instead of being built on every call. Fixture: rows {@code [top, ROWS)} of the index hold key
 * {@code row % 4}; rows below {@code top} are the column top, the implicit NULLs of key 0.
 */
public class PostingIndexDetachedCursorReuseTest extends AbstractCairoTest {
    private static final int KEYS = 4;
    private static final int ROWS = 40;

    @Test
    public void testBwdDetachedCursorReused() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                writeIndex(path, "bwd_reuse", 0);
                try (PostingIndexBwdReader reader = bwdReader(path, "bwd_reuse", 0)) {
                    assertReuse(reader, 0, true);
                }
            }
        });
    }

    @Test
    public void testCloseAfterReaderCloseReleases() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                writeIndex(path, "late_close", 8);
                final PostingIndexFwdReader fwd = fwdReader(path, "late_close", 8);
                final PostingIndexBwdReader bwd = bwdReader(path, "late_close", 8);
                // a cursor closed while the readers are open waits in their pools, which close() drains
                fwd.getDetachedCursor(2, 0, Long.MAX_VALUE, null).close();
                bwd.getDetachedCursor(2, 0, Long.MAX_VALUE, null).close();
                Assert.assertEquals(1, fwd.getFreeDetachedCursorCount());
                Assert.assertEquals(1, bwd.getFreeDetachedCursorCount());
                final RowCursor fwdCursor = fwd.getDetachedCursor(1, 0, Long.MAX_VALUE, null);
                final RowCursor fwdNulls = fwd.getDetachedCursor(0, 0, Long.MAX_VALUE, null);
                final RowCursor bwdCursor = bwd.getDetachedCursor(1, 0, Long.MAX_VALUE, null);
                final RowCursor bwdNulls = bwd.getDetachedCursor(0, 0, Long.MAX_VALUE, null);
                assertRows(expected(1, 8, false), fwdCursor);
                assertRows(expected(1, 8, true), bwdCursor);
                fwd.close();
                bwd.close();
                // the readers are closed: the cursors free their buffers instead of pooling,
                // or assertMemoryLeak() fails
                fwdCursor.close();
                fwdNulls.close();
                bwdCursor.close();
                bwdNulls.close();
                Assert.assertEquals(0, fwd.getFreeDetachedCursorCount());
                Assert.assertEquals(0, bwd.getFreeDetachedCursorCount());
            }
        });
    }

    @Test
    public void testConcurrentCloseWhileReaderCloses() throws Exception {
        // each worker closes cursors the test's thread took, while that thread closes the readers:
        // a cursor pools before the readers drain their pools, or frees itself after
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                writeIndex(path, "race_close", 8);
                final int workers = 4;
                final int rounds = 200;
                for (int round = 0; round < rounds; round++) {
                    final PostingIndexFwdReader fwd = fwdReader(path, "race_close", 8);
                    final PostingIndexBwdReader bwd = bwdReader(path, "race_close", 8);
                    // a cursor in each pool before the race, which close() must drain
                    fwd.getDetachedCursor(3, 0, Long.MAX_VALUE, null).close();
                    bwd.getDetachedCursor(0, 0, Long.MAX_VALUE, null).close();
                    Assert.assertEquals(1, fwd.getFreeDetachedCursorCount());
                    Assert.assertEquals(1, bwd.getFreeDetachedCursorCount());
                    final ObjList<RowCursor[]> held = new ObjList<>();
                    for (int w = 0; w < workers; w++) {
                        held.add(new RowCursor[]{
                                fwd.getDetachedCursor(1 + w % 3, 0, Long.MAX_VALUE, null),
                                fwd.getDetachedCursor(0, 0, Long.MAX_VALUE, null),
                                bwd.getDetachedCursor(1 + w % 3, 0, Long.MAX_VALUE, null),
                                bwd.getDetachedCursor(0, 0, Long.MAX_VALUE, null)
                        });
                    }
                    final CyclicBarrier barrier = new CyclicBarrier(workers + 1);
                    final AtomicReference<Throwable> error = new AtomicReference<>();
                    final Thread[] threads = new Thread[workers];
                    for (int w = 0; w < workers; w++) {
                        final RowCursor[] cursors = held.getQuick(w);
                        threads[w] = new Thread(() -> {
                            try {
                                barrier.await();
                                for (RowCursor cursor : cursors) {
                                    cursor.close();
                                }
                            } catch (Throwable th) {
                                error.compareAndSet(null, th);
                            }
                        });
                        threads[w].start();
                    }
                    barrier.await();
                    fwd.close();
                    bwd.close();
                    for (Thread thread : threads) {
                        thread.join();
                    }
                    Assert.assertNull(error.get());
                    Assert.assertEquals("round " + round, 0, fwd.getFreeDetachedCursorCount());
                    Assert.assertEquals("round " + round, 0, bwd.getFreeDetachedCursorCount());
                }
            }
        });
    }

    @Test
    public void testCoveredCursorReused() throws Exception {
        // a reused cursor reads covered values when this call asks for them, whatever the
        // previous holder asked for; the covered value of row r is 1000 + r
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final long colBytes = (long) ROWS * Long.BYTES;
                final long colAddr = Unsafe.malloc(colBytes, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int r = 0; r < ROWS; r++) {
                        Unsafe.putLong(colAddr + (long) r * Long.BYTES, 1000L + r);
                    }
                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "covered_reuse", COLUMN_NAME_TXN_NONE)) {
                        writer.configureCovering(
                                new long[]{colAddr},
                                new long[]{0},
                                new int[]{3},
                                new int[]{1},
                                new int[]{ColumnType.LONG},
                                1
                        );
                        for (int r = 0; r < ROWS; r++) {
                            writer.add(r % KEYS, r);
                        }
                        writer.setMaxValue(ROWS - 1);
                        writer.commit();
                    }
                    final GenericRecordMetadata metadata = new GenericRecordMetadata();
                    metadata.add(new TableColumnMetadata("c0", ColumnType.LONG, IndexType.NONE, 0, false, null, 0, false));
                    metadata.add(new TableColumnMetadata("c1", ColumnType.LONG, IndexType.NONE, 0, false, null, 1, false));
                    final int[] required = {0};
                    try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                            configuration, path, "covered_reuse", COLUMN_NAME_TXN_NONE, -1, 0, metadata, new ColumnVersionReader(), 0)) {
                        final RowCursor first = reader.getDetachedCursor(1, 0, Long.MAX_VALUE, required);
                        assertCovered(expected(1, 0, false), first);
                        first.close();
                        final RowCursor rowIdsOnly = reader.getDetachedCursor(2, 0, Long.MAX_VALUE, null);
                        Assert.assertSame(first, rowIdsOnly);
                        assertRows(expected(2, 0, false), rowIdsOnly);
                        rowIdsOnly.close();
                        final RowCursor coveredAgain = reader.getDetachedCursor(3, 0, Long.MAX_VALUE, required);
                        Assert.assertSame(first, coveredAgain);
                        assertCovered(expected(3, 0, false), coveredAgain);
                        coveredAgain.close();
                        Assert.assertEquals(1, reader.getFreeDetachedCursorCount());
                    }
                } finally {
                    Unsafe.free(colAddr, colBytes, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testDoubleCloseCountsOnce() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                writeIndex(path, "double_close", 0);
                try (
                        PostingIndexFwdReader fwd = fwdReader(path, "double_close", 0);
                        PostingIndexBwdReader bwd = bwdReader(path, "double_close", 0)
                ) {
                    assertDoubleClose(fwd, false);
                    assertDoubleClose(bwd, true);
                }
            }
        });
    }

    @Test
    public void testFwdDetachedCursorReused() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                writeIndex(path, "fwd_reuse", 0);
                try (PostingIndexFwdReader reader = fwdReader(path, "fwd_reuse", 0)) {
                    assertReuse(reader, 0, false);
                }
            }
        });
    }

    @Test
    public void testNullCursorReusedInItsOwnPool() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                writeIndex(path, "null_reuse", 8);
                try (
                        PostingIndexFwdReader fwd = fwdReader(path, "null_reuse", 8);
                        PostingIndexBwdReader bwd = bwdReader(path, "null_reuse", 8)
                ) {
                    assertNullReuse(fwd, false);
                    assertNullReuse(bwd, true);
                }
            }
        });
    }

    @Test
    public void testFailedDecodeKeepsCursorReusable() throws Exception {
        // key 0 is rows 1 and 2, key 1 ten scattered rows, key 2 the rest: decoding key 1 grows the
        // block buffer key 0 left in the reused cursor, which fails under the memory limit; the
        // cursor, closed after the failure, goes back to the pool and serves key 1 in full
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int rows = 3000;
                final LongList key1 = new LongList();
                for (long r : new long[]{5, 17, 40, 41, 100, 333, 1000, 1500, 2222, 2999}) {
                    key1.add(r);
                }
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, "failed_decode", COLUMN_NAME_TXN_NONE)) {
                    for (int r = 0; r < rows; r++) {
                        writer.add(r == 1 || r == 2 ? 0 : (key1.indexOf(r) > -1 ? 1 : 2), r);
                    }
                    writer.setMaxValue(rows - 1);
                    writer.commit();
                }
                try (PostingIndexFwdReader reader = fwdReader(path, "failed_decode", 0)) {
                    final RowCursor small = reader.getDetachedCursor(0, 0, Long.MAX_VALUE, null);
                    final LongList key0 = new LongList();
                    key0.add(1);
                    key0.add(2);
                    assertRows(key0, small);
                    small.close();
                    Assert.assertEquals(1, reader.getFreeDetachedCursorCount());
                    final RowCursor reused = reader.getDetachedCursor(1, 0, Long.MAX_VALUE, null);
                    Assert.assertSame(small, reused);
                    Unsafe.setRssMemLimit(Unsafe.getRssMemUsed());
                    try {
                        reused.hasNext();
                        Assert.fail("the decode must fail to grow the block buffer");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "global RSS memory limit exceeded");
                    } finally {
                        Unsafe.setRssMemLimit(0);
                    }
                    reused.close();
                    Assert.assertEquals(1, reader.getFreeDetachedCursorCount());
                    final long news = AbstractPostingIndexReader.DETACHED_CURSOR_NEWS.get();
                    final RowCursor again = reader.getDetachedCursor(1, 0, Long.MAX_VALUE, null);
                    Assert.assertEquals(news, AbstractPostingIndexReader.DETACHED_CURSOR_NEWS.get());
                    Assert.assertSame(small, again);
                    assertRows(key1, again);
                    again.close();
                }
            }
        });
    }

    @Test
    public void testPoolKeepsAtMost128Cursors() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                writeIndex(path, "pool_cap", 0);
                try (
                        PostingIndexFwdReader fwd = fwdReader(path, "pool_cap", 0);
                        PostingIndexBwdReader bwd = bwdReader(path, "pool_cap", 0)
                ) {
                    assertPoolCap(fwd, false);
                    assertPoolCap(bwd, true);
                }
            }
        });
    }

    @Test
    public void testPooledCursorFreesLegacyRankDirectory() throws Exception {
        // a backward cursor over a legacy Elias-Fano blob, written without the rank trailer,
        // builds a rank directory of the blob; the pool keeps the cursor's block buffer but frees
        // that directory. A ranked blob builds none, so closing its cursor frees nothing.
        assertMemoryLeak(() -> {
            try (Path path = new Path().of(configuration.getDbRoot())) {
                for (boolean ranked : new boolean[]{false, true}) {
                    final String name = "rank_dir_" + ranked;
                    final int rows = 4000;
                    final boolean wasRanked = PostingIndexUtils.isEfRankTrailerEnabled;
                    PostingIndexUtils.isEfRankTrailerEnabled = ranked;
                    try (PostingIndexWriter writer = new PostingIndexWriter(configuration, PostingIndexUtils.ENCODING_EF)) {
                        writer.of(path, name, COLUMN_NAME_TXN_NONE, true);
                        writer.setNextTxnAtSeal(0L);
                        for (int r = 0; r < rows; r++) {
                            writer.add(r % KEYS, r);
                            if ((r + 1) % 1000 == 0) {
                                writer.setMaxValue(r);
                                writer.commit();
                            }
                        }
                    } finally {
                        PostingIndexUtils.isEfRankTrailerEnabled = wasRanked;
                    }
                    try (PostingIndexBwdReader reader = bwdReader(path, name, 0)) {
                        for (int run = 0; run < 2; run++) {
                            final RowCursor cursor = reader.getDetachedCursor(1, 0, Long.MAX_VALUE, null);
                            final LongList expected = new LongList();
                            for (int r = rows - KEYS + 1; r >= 0; r -= KEYS) {
                                expected.add(r);
                            }
                            assertRows(expected, cursor);
                            final long open = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_INDEX_READER);
                            cursor.close();
                            final long closed = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_INDEX_READER);
                            Assert.assertEquals(1, reader.getFreeDetachedCursorCount());
                            Assert.assertEquals("ranked: " + ranked + ", run: " + run, !ranked, closed < open);
                            Assert.assertTrue(closed <= open);
                        }
                    }
                }
            }
        });
    }

    private static void assertDoubleClose(AbstractPostingIndexReader reader, boolean desc) {
        final RowCursor cursor = reader.getDetachedCursor(1, 0, Long.MAX_VALUE, null);
        cursor.close();
        cursor.close();
        Assert.assertEquals(1, freeDetachedCursorCount(reader));
        final RowCursor a = reader.getDetachedCursor(1, 0, Long.MAX_VALUE, null);
        final RowCursor b = reader.getDetachedCursor(2, 0, Long.MAX_VALUE, null);
        Assert.assertSame(cursor, a);
        Assert.assertNotSame(cursor, b);
        assertRows(expected(1, 0, desc), a);
        assertRows(expected(2, 0, desc), b);
        a.close();
        b.close();
        Assert.assertEquals(2, freeDetachedCursorCount(reader));
    }

    private static void assertPoolCap(AbstractPostingIndexReader reader, boolean desc) {
        final int open = 129;
        final ObjList<RowCursor> cursors = new ObjList<>();
        long news = AbstractPostingIndexReader.DETACHED_CURSOR_NEWS.get();
        for (int i = 0; i < open; i++) {
            cursors.add(reader.getDetachedCursor(1, 0, Long.MAX_VALUE, null));
        }
        Assert.assertEquals(news + open, AbstractPostingIndexReader.DETACHED_CURSOR_NEWS.get());
        Misc.freeObjListAndClear(cursors);
        // the 129th close finds the pool full and frees its cursor
        Assert.assertEquals(128, freeDetachedCursorCount(reader));

        news = AbstractPostingIndexReader.DETACHED_CURSOR_NEWS.get();
        for (int i = 0; i < open; i++) {
            cursors.add(reader.getDetachedCursor(1 + i % 3, 0, Long.MAX_VALUE, null));
        }
        Assert.assertEquals(news + 1, AbstractPostingIndexReader.DETACHED_CURSOR_NEWS.get());
        for (int i = 0; i < open; i++) {
            assertRows(expected(1 + i % 3, 0, desc), cursors.getQuick(i));
        }
        Misc.freeObjListAndClear(cursors);
        Assert.assertEquals(128, freeDetachedCursorCount(reader));
    }

    private static void assertCovered(LongList expectedRows, RowCursor cursor) {
        final CoveringRowCursor covering = (CoveringRowCursor) cursor;
        for (int i = 0, n = expectedRows.size(); i < n; i++) {
            Assert.assertTrue(covering.hasNext());
            final long row = covering.next();
            Assert.assertEquals(expectedRows.getQuick(i), row);
            Assert.assertEquals(1000 + row, covering.getCoveredLong(0));
        }
        Assert.assertFalse(covering.hasNext());
    }

    private static void assertNullReuse(AbstractPostingIndexReader reader, boolean desc) {
        final long news = AbstractPostingIndexReader.DETACHED_CURSOR_NEWS.get();
        final RowCursor nulls = reader.getDetachedCursor(0, 0, Long.MAX_VALUE, null);
        assertRows(expected(0, 8, desc), nulls);
        nulls.close();
        // a plain key does not take the NULL-key cursor
        final RowCursor plain = reader.getDetachedCursor(1, 0, Long.MAX_VALUE, null);
        Assert.assertNotSame(nulls, plain);
        assertRows(expected(1, 8, desc), plain);
        plain.close();
        final RowCursor nullsAgain = reader.getDetachedCursor(0, 0, Long.MAX_VALUE, null);
        Assert.assertSame(nulls, nullsAgain);
        assertRows(expected(0, 8, desc), nullsAgain);
        final RowCursor plainAgain = reader.getDetachedCursor(3, 0, Long.MAX_VALUE, null);
        Assert.assertSame(plain, plainAgain);
        assertRows(expected(3, 8, desc), plainAgain);
        // the NULL-key cursor from row 5 on: the column top's rows 5, 6 and 7, then the index's,
        // relative to row 5
        final RowCursor nullsFrom5 = reader.getDetachedCursor(0, 5, Long.MAX_VALUE, null);
        Assert.assertNotSame(nullsAgain, nullsFrom5);
        final LongList from5 = new LongList();
        final LongList all = expected(0, 8, desc);
        for (int i = 0, n = all.size(); i < n; i++) {
            if (all.getQuick(i) >= 5) {
                // relative to the low row
                from5.add(all.getQuick(i) - 5);
            }
        }
        assertRows(from5, nullsFrom5);
        nullsFrom5.close();
        nullsAgain.close();
        plainAgain.close();
        Assert.assertEquals(news + 3, AbstractPostingIndexReader.DETACHED_CURSOR_NEWS.get());
        Assert.assertEquals(3, freeDetachedCursorCount(reader));
    }

    private static void assertReuse(AbstractPostingIndexReader reader, int top, boolean desc) {
        final long news = AbstractPostingIndexReader.DETACHED_CURSOR_NEWS.get();
        final RowCursor first = reader.getDetachedCursor(1, 0, Long.MAX_VALUE, null);
        assertRows(expected(1, top, desc), first);
        first.close();
        final RowCursor second = reader.getDetachedCursor(2, 0, Long.MAX_VALUE, null);
        Assert.assertSame(first, second);
        assertRows(expected(2, top, desc), second);
        second.close();
        // the last key, then the first
        final RowCursor last = reader.getDetachedCursor(KEYS - 1, 0, Long.MAX_VALUE, null);
        Assert.assertSame(first, last);
        assertRows(expected(KEYS - 1, top, desc), last);
        last.close();
        final RowCursor key0 = reader.getDetachedCursor(0, 0, Long.MAX_VALUE, null);
        Assert.assertSame(first, key0);
        assertRows(expected(0, top, desc), key0);
        key0.close();
        // a key the index does not have takes no cursor from the pool
        final RowCursor absent = reader.getDetachedCursor(KEYS, 0, Long.MAX_VALUE, null);
        Assert.assertSame(EmptyRowCursor.INSTANCE, absent);
        Assert.assertFalse(absent.hasNext());
        absent.close();
        // a row range: key 2's rows within [10, 30]
        final RowCursor ranged = reader.getDetachedCursor(2, 10, 30, null);
        Assert.assertSame(first, ranged);
        final LongList inRange = new LongList();
        final LongList all = expected(2, top, desc);
        for (int i = 0, n = all.size(); i < n; i++) {
            if (all.getQuick(i) >= 10 && all.getQuick(i) <= 30) {
                inRange.add(all.getQuick(i));
            }
        }
        // a ranged cursor's rows are relative to its low row
        for (int i = 0, n = inRange.size(); i < n; i++) {
            inRange.setQuick(i, inRange.getQuick(i) - 10);
        }
        assertRows(inRange, ranged);
        ranged.close();
        Assert.assertEquals(news + 1, AbstractPostingIndexReader.DETACHED_CURSOR_NEWS.get());
    }

    private static void assertRows(LongList expectedRows, RowCursor cursor) {
        final LongList actual = new LongList();
        while (cursor.hasNext()) {
            actual.add(cursor.next());
        }
        Assert.assertEquals(expectedRows.toString(), actual.toString());
    }

    private static PostingIndexBwdReader bwdReader(Path path, String name, long top) {
        return new PostingIndexBwdReader(configuration, path, name, COLUMN_NAME_TXN_NONE, -1, top, null, null, 0);
    }

    // the rows of a key, ascending or descending: rows below top are key 0's implicit NULLs
    private static LongList expected(int key, int top, boolean desc) {
        final LongList rows = new LongList();
        for (int r = 0; r < ROWS; r++) {
            if (r < top ? key == 0 : r % KEYS == key) {
                rows.add(r);
            }
        }
        if (desc) {
            rows.reverse();
        }
        return rows;
    }

    private static int freeDetachedCursorCount(AbstractPostingIndexReader reader) {
        return reader instanceof PostingIndexFwdReader fwd
                ? fwd.getFreeDetachedCursorCount()
                : ((PostingIndexBwdReader) reader).getFreeDetachedCursorCount();
    }

    private static PostingIndexFwdReader fwdReader(Path path, String name, long top) {
        return new PostingIndexFwdReader(configuration, path, name, COLUMN_NAME_TXN_NONE, -1, top);
    }

    private static void writeIndex(Path path, String name, int top) {
        try (PostingIndexWriter writer = new PostingIndexWriter(configuration, path, name, COLUMN_NAME_TXN_NONE)) {
            for (int r = top; r < ROWS; r++) {
                writer.add(r % KEYS, r);
            }
            writer.setMaxValue(ROWS - 1);
            writer.commit();
        }
    }
}
