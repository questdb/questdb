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

package io.questdb.test.griffin.engine.table;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.PartitionGeometry;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.table.KeyMajorPageFrameRecordCursor;
import io.questdb.griffin.engine.table.KeyMajorScanFactory;
import io.questdb.std.DirectLongList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Rows;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * A planned key-major walk ({@link KeyMajorPageFrameRecordCursor#enablePlannedWalk()}) against
 * the same walk collected: for one sequence of row limits, every {@code planKeyRows()} call must
 * return the status {@code collectKeyRows()} returns, and its plan, read back, the very row ids
 * collected. The warm-up rows a continuing task starts with ({@code planPrecedingRows()}) must be
 * the key's last rows collected. Plans are also read on another thread, while the walk goes on.
 * Data: many generations per partition's posting index (small and out-of-order commits), column
 * tops (the null prefix of key 0), dense and sparse keys, symbols the table lacks, and frames
 * from 1 row up to whole partitions.
 */
public class KeyMajorPlannedWalkTest extends AbstractCairoTest {
    private static final int[] WARMUPS = {1, 2, 4, 9};

    @Test
    public void testCompositePiecesRelocatedAndShifted() throws Exception {
        // Merge-append moves the pieces it merges to the files' tail, above their partition rows, and
        // leaves the pieces between them below theirs. The planned walk counts and seeks the posting
        // index in file rows, each frame at its own piece's shift.
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 128);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 1000);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_PIECE_THRESHOLD, 100_000);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 1);
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index type posting, a int, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("insert into t select rnd_symbol('A','B','C','D'), x::int, timestamp_sequence('2024-01-01', 43_200_000L) from long_sequence(4_000)");
            drainWalQueue();
            final Rnd rnd = TestUtils.generateRandom(LOG);
            for (int c = 0; c < 12; c++) {
                // backdated commits of a few rows each, every one into an earlier stretch of a day
                final long minute = rnd.nextLong(2 * 24 * 60);
                execute("insert into t select rnd_symbol('A','B','C','D','R'), (10_000 + x)::int, "
                        + "dateadd('m', " + minute + ", '2024-01-01')::timestamp + x from long_sequence(" + (1 + rnd.nextInt(5)) + ")");
                drainWalQueue();
            }
            assertHasShiftedPieces();
            final String[] queries = {
                    "select * from t where sym in ('A', 'C', 'R') order by sym",
                    "select * from t where sym in ('B', 'D') and ts in '2024-01-01' order by sym",
                    "select * from t where sym not in ('A') order by sym desc",
            };
            for (int i = 0; i < queries.length; i++) {
                // planned and collected walks agree, and the planned walk's rows are the query's
                Assert.assertTrue(assertWalks(queries[i], 21 + i));
                assertPlannedWalkMatchesFullSort(queries[i]);
            }
        });
    }

    @Test
    public void testCountAndIndexDisagreeFailsBothWays() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index type posting, a int, ts timestamp) timestamp(ts) partition by DAY");
            execute("insert into t select rnd_symbol('A','B','C'), x::int, (x * 60_000_000L)::timestamp from long_sequence(5000)");
            final String query = "select * from t where sym in ('A', 'C') order by sym";
            // fewer rows counted than the index holds: the surplus must fail the read, not vanish;
            // more rows counted than it holds: the read comes up short
            for (long skew : new long[]{-1, 1}) {
                for (long rowLimit : new long[]{Long.MAX_VALUE, 7}) {
                    try (
                            RecordCursorFactory factory = select(query);
                            RecordCursor cursor = keyMajorFactory(factory).getCursor(sqlExecutionContext);
                            DirectLongList rows = new DirectLongList(64, MemoryTag.NATIVE_DEFAULT)
                    ) {
                        final KeyMajorPageFrameRecordCursor walk = keyMajor(cursor);
                        Assert.assertTrue(walk.enablePlannedWalk());
                        KeyMajorPageFrameRecordCursor.setCountSkew(skew);
                        final LongList plan = new LongList();
                        try {
                            int status;
                            do {
                                plan.clear();
                                status = walk.planKeyRows(plan, rowLimit);
                                walk.readPlan(plan, rows);
                            } while (status != KeyMajorPageFrameRecordCursor.COLLECT_EXHAUSTED);
                            Assert.fail("skew " + skew + ", limit " + rowLimit + ": the walk read " + rows.size() + " rows without an error");
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "index walk found other rows than it counted");
                        } finally {
                            KeyMajorPageFrameRecordCursor.setCountSkew(0);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testColumnTopNullPrefix() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (a int, ts timestamp) timestamp(ts) partition by DAY");
            execute("insert into t select x::int, (x * 60_000_000L)::timestamp from long_sequence(1000)");
            execute("alter table t add column sym symbol index type posting");
            for (int c = 0; c < 6; c++) {
                execute("insert into t select x::int, ((1000 + " + c + " * 300 + x) * 60_000_000L)::timestamp, "
                        + "case when x % 3 = 0 then null else 'K' || (x % 5) end from long_sequence(300)");
            }
            // only partitions where the column exists, so every frame has a posting reader
            Assert.assertTrue(assertWalks("select * from t where sym in (null, 'K1', 'K3') and ts >= '1970-01-01T16:41' order by sym", 11));
            Assert.assertTrue(assertWalks("select * from t where sym in (null, 'NOPE') and ts >= '1970-01-01T16:41' order by sym", 12));
            // the whole first day: its posting reader starts with the column top's NULL rows
            Assert.assertTrue(assertWalks("select * from t where sym in (null, 'K2') order by sym", 13));
        });
    }

    @Test
    public void testManyGenerationsDenseAndSparseKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index type posting, a int, ts timestamp) timestamp(ts) partition by DAY");
            final Rnd rnd = TestUtils.generateRandom(LOG);
            for (int c = 0; c < 60; c++) {
                // dense keys A..E, a rare key R, NULLs; some commits out of order
                final long base = rnd.nextInt(4) == 0 ? rnd.nextLong(60 * 300) : c * 300L;
                execute("insert into t select case when x % 97 = 0 then 'R' when x % 13 = 0 then null else rnd_symbol('A','B','C','D','E') end, "
                        + "x::int, ((" + base + " + x) * 20_000_000L)::timestamp from long_sequence(" + (1 + rnd.nextInt(400)) + ")");
            }
            final String[] queries = {
                    "select * from t where sym in ('A', 'R', null, 'NOPE', 'E') order by sym",
                    "select * from t where sym in ('R') order by sym",
                    "select * from t where sym not in ('B', 'C') order by sym",
                    "select * from t where sym != 'A' order by sym desc",
                    "select * from t where sym in ('A', 'B', 'C', 'D', 'E', 'R') and ts in '1970-01-02' order by sym",
            };
            for (int frameRows : new int[]{1, 7, 64, 1_000_000}) {
                setProperty(PropertyKey.CAIRO_SQL_INDEX_SCAN_PAGE_FRAME_MAX_ROWS, frameRows);
                sqlExecutionContext.changePageFrameSizes(1, frameRows);
                int walks = 0;
                for (int q = 0; q < queries.length; q++) {
                    if (assertWalks(queries[q], rnd.nextLong())) {
                        walks++;
                    }
                }
                Assert.assertTrue(walks >= 3);
            }
        });
    }

    @Test
    public void testIndexWithoutRanksIsCountedAndSoughtByReading() throws Exception {
        // as for a legacy unranked encoding: counts and the rank seek come from reading the index
        KeyMajorPageFrameRecordCursor.setIndexRanksEnabled(false);
        try {
            testManyGenerationsDenseAndSparseKeys();
            execute("drop table t");
            testColumnTopNullPrefix();
        } finally {
            KeyMajorPageFrameRecordCursor.setIndexRanksEnabled(true);
        }
    }

    @Test
    public void testOneHugeKeyAndManyTinyKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (sym symbol index type posting, a int, ts timestamp) timestamp(ts) partition by HOUR");
            execute("insert into t select case when x % 4 != 0 then 'HUGE' else 'T' || (x % 2001) end, x::int, (x * 1_000_000L)::timestamp from long_sequence(40000)");
            setProperty(PropertyKey.CAIRO_SQL_INDEX_SCAN_PAGE_FRAME_MAX_ROWS, 512);
            sqlExecutionContext.changePageFrameSizes(1, 512);
            assertWalks("select * from t where sym not in ('T5') order by sym", 3);
            Assert.assertTrue(assertWalks("select * from t where sym in ('HUGE', 'T1', 'T2000', 'T4') order by sym", 4));
        });
    }

    private static void assertSameRows(String what, DirectLongList expected, long expectedLo, DirectLongList actual, long actualLo, long count) {
        for (long i = 0; i < count; i++) {
            if (expected.get(expectedLo + i) != actual.get(actualLo + i)) {
                Assert.fail(what + ": row " + i + " of " + count + ": expected " + expected.get(expectedLo + i) + " but was " + actual.get(actualLo + i));
            }
        }
    }

    // At least one piece of the first partition sits above its partition rows and one below.
    private void assertHasShiftedPieces() {
        try (TableReader reader = getReader("t")) {
            Assert.assertTrue(reader.getTxFile().isPartitionComposite(0));
            final PartitionGeometry geometry = reader.getGeometry();
            geometry.resolve(0);
            boolean isAbove = false;
            boolean isBelow = false;
            for (int i = 0, n = geometry.getPieceCount(0); i < n; i++) {
                final long shift = geometry.getPieceShift(0, i);
                isAbove |= shift > 0;
                isBelow |= shift < 0;
            }
            Assert.assertTrue("no relocated piece", isAbove && isBelow);
        }
    }

    // Walks the whole query through its plan and reads each planned row back through the cursor: the
    // rows must be those the query returns through a full sort, in its order. This checks the plan
    // against the table, where assertWalks() checks it against the collected walk.
    private void assertPlannedWalkMatchesFullSort(String query) throws Exception {
        final StringSink expected = new StringSink();
        printSql(query.replaceFirst("select ", "select /*+ no_index */ "), expected);
        final StringSink actual = new StringSink();
        actual.put("sym\ta\tts\n");
        try (
                RecordCursorFactory factory = select(query);
                RecordCursor cursor = keyMajorFactory(factory).getCursor(sqlExecutionContext);
                DirectLongList rows = new DirectLongList(64, MemoryTag.NATIVE_DEFAULT)
        ) {
            final KeyMajorPageFrameRecordCursor walk = keyMajor(cursor);
            Assert.assertTrue(query, walk.enablePlannedWalk());
            final LongList plan = new LongList();
            int status;
            do {
                plan.clear();
                status = walk.planKeyRows(plan, 97);
                walk.readPlan(plan, rows);
            } while (status != KeyMajorPageFrameRecordCursor.COLLECT_EXHAUSTED);
            final Record record = walk.getRecordB();
            for (long i = 0, n = rows.size(); i < n; i++) {
                // a walk's row id is (frame << 32 | frame row), see toFrameRowId(); recordAt() takes Rows' encoding
                final long rowId = rows.get(i);
                walk.recordAt(record, Rows.toRowID((int) (rowId >>> 32), rowId & 0xFFFF_FFFFL));
                final CharSequence sym = record.getSymA(0);
                actual.put(sym == null ? "" : sym).put('\t').put(record.getInt(1)).put('\t');
                MicrosFormatUtils.appendDateTimeUSec(actual, record.getTimestamp(2));
                actual.put('\n');
            }
        }
        TestUtils.assertEquals(query, expected, actual);
    }

    private static RecordCursorFactory keyMajorFactory(RecordCursorFactory factory) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof KeyMajorScanFactory) {
                return f;
            }
        }
        return null;
    }

    private static KeyMajorPageFrameRecordCursor keyMajor(RecordCursor cursor) {
        Assert.assertTrue("not a key-major cursor: " + cursor.getClass().getName(), cursor instanceof KeyMajorPageFrameRecordCursor);
        return (KeyMajorPageFrameRecordCursor) cursor;
    }

    // Collects the walk of the query twice, unplanned and planned, with the same random limits.
    private boolean assertWalks(String query, long seed) throws Exception {
        final Rnd rnd = new Rnd(seed, seed * 31 + 7);
        try (RecordCursorFactory probe = select(query)) {
            final RecordCursorFactory f = keyMajorFactory(probe);
            if (f == null) {
                return false;
            }
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                if (!(c instanceof KeyMajorPageFrameRecordCursor)) {
                    return false;
                }
            }
        }
        final ExecutorService reader = Executors.newSingleThreadExecutor();
        try (
                RecordCursorFactory factoryA = select(query);
                RecordCursorFactory factoryB = select(query);
                RecordCursor cursorA = keyMajorFactory(factoryA).getCursor(sqlExecutionContext);
                RecordCursor cursorB = keyMajorFactory(factoryB).getCursor(sqlExecutionContext);
                DirectLongList rowsA = new DirectLongList(64, MemoryTag.NATIVE_DEFAULT);
                DirectLongList rowsB = new DirectLongList(64, MemoryTag.NATIVE_DEFAULT);
                DirectLongList warm = new DirectLongList(64, MemoryTag.NATIVE_DEFAULT)
        ) {
            final KeyMajorPageFrameRecordCursor a = keyMajor(cursorA);
            final KeyMajorPageFrameRecordCursor b = keyMajor(cursorB);
            for (int pass = 0; pass < 2; pass++) {
                a.prepareFrames();
                Assert.assertTrue(query, b.enablePlannedWalk());
                Assert.assertTrue(b.isPlannedWalk());
                rowsA.clear();
                rowsB.clear();
                final LongList plan = new LongList();
                long keyStartA = 0;
                long calls = 0;
                while (true) {
                    final long limit = switch (rnd.nextInt(4)) {
                        case 0 -> rnd.nextInt(3);
                        case 1 -> 1 + rnd.nextInt(20);
                        case 2 -> 1 + rnd.nextInt(2_000);
                        default -> Long.MAX_VALUE;
                    };
                    final int keyIndex = a.getKeyIndex();
                    Assert.assertEquals(keyIndex, b.getKeyIndex());
                    final long loA = rowsA.size();
                    final int statusA = a.collectKeyRows(rowsA, limit);
                    final long loB = rowsB.size();
                    final int statusB;
                    final int how = rnd.nextInt(3);
                    if (how == 0) {
                        // collected through the plan on this thread
                        statusB = b.collectKeyRows(rowsB, limit);
                    } else {
                        plan.clear();
                        statusB = b.planKeyRows(plan, limit);
                        Assert.assertEquals(rowsA.size() - loA, KeyMajorPageFrameRecordCursor.planRowCount(plan, 0));
                        if (how == 1) {
                            b.readPlan(plan, rowsB);
                        } else {
                            // another thread reads the plan
                            final Future<?> f = reader.submit(() -> b.readPlan(plan, rowsB));
                            f.get();
                        }
                    }
                    calls++;
                    Assert.assertEquals(query + " call " + calls + " limit " + limit, statusA, statusB);
                    Assert.assertEquals(query + " call " + calls, rowsA.size() - loA, rowsB.size() - loB);
                    assertSameRows(query + " call " + calls, rowsA, loA, rowsB, loB, rowsA.size() - loA);
                    if (statusA == KeyMajorPageFrameRecordCursor.COLLECT_ROW_LIMIT) {
                        // a task continuing the key starts with the key's last rows
                        final int w = WARMUPS[rnd.nextInt(WARMUPS.length)];
                        plan.clear();
                        final long planned = b.planPrecedingRows(plan, w);
                        final long keyRows = rowsA.size() - keyStartA;
                        Assert.assertEquals(Math.min(w, keyRows), planned);
                        warm.clear();
                        b.readPlan(plan, warm);
                        Assert.assertEquals(planned, warm.size());
                        assertSameRows(query + " warm-up", rowsA, rowsA.size() - planned, warm, 0, planned);
                    } else {
                        keyStartA = rowsA.size();
                    }
                    if (statusA == KeyMajorPageFrameRecordCursor.COLLECT_EXHAUSTED) {
                        break;
                    }
                    if (statusA == KeyMajorPageFrameRecordCursor.COLLECT_KEY_END && rnd.nextInt(8) == 0 && a.getKeyIndex() < a.getKeyCount()) {
                        // a key read apart from the walk, whole frames at a time, then skipped
                        final long lo = rowsA.size();
                        final int nextKey = a.getKeyIndex();
                        int posA = 0;
                        int posB = 0;
                        final long chunk = 1 + rnd.nextInt(500);
                        final DirectLongList sideA = new DirectLongList(64, MemoryTag.NATIVE_DEFAULT);
                        final DirectLongList sideB = new DirectLongList(64, MemoryTag.NATIVE_DEFAULT);
                        try {
                            do {
                                posA = a.collectKeyFrames(nextKey, posA, sideA, chunk);
                                posB = b.collectKeyFrames(nextKey, posB, sideB, chunk);
                                Assert.assertEquals(posA, posB);
                                Assert.assertEquals(sideA.size(), sideB.size());
                            } while (posA > -1);
                            assertSameRows(query + " collectKeyFrames", sideA, 0, sideB, 0, sideA.size());
                        } finally {
                            sideA.close();
                            sideB.close();
                        }
                        a.skipKey();
                        b.skipKey();
                        Assert.assertEquals(lo, rowsA.size());
                        keyStartA = rowsA.size();
                    }
                }
                Assert.assertTrue(query, a.isWalkExhausted());
                Assert.assertTrue(query, b.isWalkExhausted());
                cursorA.toTop();
                cursorB.toTop();
                Assert.assertFalse(b.isPlannedWalk());
            }
        } finally {
            reader.shutdown();
        }
        return true;
    }
}
