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

package io.questdb.test.cairo.covering;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.idx.CoveringRowCursor;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.DirectLongList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

/**
 * A posting-index cursor opened without covered columns reads row ids only, so it must not pay
 * for, nor depend on, the sidecar bookkeeping a covering (INCLUDE) index carries: key-major walks
 * and plain index scans over an INCLUDE index read the same row ids as over the same index without
 * INCLUDE, through {@code hasNext()} and through the bulk {@link RowCursor#drainTo}, and a cursor
 * opened for covered columns still reads the right values. The tables are written in several
 * commits after the index was built, so partitions hold sealed and sparse generations, and one
 * column has a column top.
 */
public class CoveringIndexRowIdOnlyCursorTest extends AbstractCairoTest {
    private static final int KEYS = 37;
    private static final long ROWS = 60_000;

    @Test
    public void testKeyMajorQueriesMatchPlainIndex() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String[] queries = {
                    "select sym, v, ts from %s where sym in ('k1', 'k7', 'k30', 'absent') order by sym",
                    "select sym, v, ts from %s where sym in ('k3', 'k2') and ts >= '1970-01-02' order by sym desc",
                    "select sym, v - lag(v) over (partition by sym) d from %s where sym in ('k5', 'k6', 'k11') order by sym",
                    "select sym, v, ts from %s where sym = 'k9' and v %% 2 = 0",
            };
            for (String q : queries) {
                final String cov = String.format(q, "cov");
                final StringSink expected = new StringSink();
                final StringSink actual = new StringSink();
                printSql(String.format(q, "plain"), expected);
                printSql(cov, actual);
                TestUtils.assertEquals(cov, expected, actual);
                Assert.assertTrue(cov, expected.length() > 100);
            }
            final StringSink plan = new StringSink();
            printSql("explain " + String.format(queries[0], "cov"), plan);
            Assert.assertTrue(plan.toString(), plan.toString().contains("keyMajor: true"));
        });
    }

    @Test
    public void testRowIdOnlyCursorsMatchPlainIndex() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final Rnd rnd = TestUtils.generateRandom(LOG);
            try (TableReader cov = engine.getReader("cov"); TableReader plain = engine.getReader("plain")) {
                final int symIndex = cov.getMetadata().getColumnIndex("sym");
                Assert.assertEquals(cov.getPartitionCount(), plain.getPartitionCount());
                int coveredCursors = 0;
                for (int p = 0, n = cov.getPartitionCount(); p < n; p++) {
                    final long rows = cov.openPartition(p);
                    Assert.assertEquals(rows, plain.openPartition(p));
                    for (int key = 0; key <= KEYS + 1; key++) {
                        for (int f = 0; f < 6; f++) {
                            final long lo = f == 0 ? 0 : rnd.nextLong(rows);
                            final long hi = f == 0 ? rows - 1 : Math.min(rows - 1, lo + rnd.nextLong(rows));
                            for (int direction : new int[]{IndexReader.DIR_FORWARD, IndexReader.DIR_BACKWARD}) {
                                final IndexReader covReader = cov.getIndexReader(p, symIndex, direction);
                                final IndexReader plainReader = plain.getIndexReader(p, symIndex, direction);
                                final String label = "p=" + p + ", key=" + key + ", [" + lo + "," + hi + "], dir=" + direction;
                                final LongList expected = rows(plainReader.getCursor(key, lo, hi));
                                final RowCursor rowIdOnly = covReader.getCursor(key, lo, hi);
                                if (rowIdOnly instanceof CoveringRowCursor) {
                                    Assert.assertEquals(label, 0, cursorCoverCount(rowIdOnly));
                                }
                                assertRows(label + " hasNext", expected, rows(rowIdOnly));
                                if (direction == IndexReader.DIR_FORWARD) {
                                    assertRows(label + " drainTo", expected, drained(covReader.getCursor(key, lo, hi), rnd));
                                }
                                // a cursor opened for a covered column tracks the sidecars and reads the values
                                final RowCursor covered = covReader.getCursor(key, lo, hi, new int[]{0});
                                if (covered instanceof CoveringRowCursor crc) {
                                    Assert.assertTrue(label, cursorCoverCount(covered) > 0);
                                    coveredCursors++;
                                    final LongList values = new LongList();
                                    final LongList ids = new LongList();
                                    try {
                                        while (crc.hasNext()) {
                                            ids.add(crc.next());
                                            values.add(crc.getCoveredLong(0));
                                        }
                                    } finally {
                                        Misc.free(covered);
                                    }
                                    assertRows(label + " covered ids", expected, ids);
                                    for (int i = 0, m = ids.size(); i < m; i++) {
                                        // v = row number * 3 + 1; cursor row ids are relative to the frame's lo
                                        Assert.assertEquals(label + " covered value", expectedValue(cov, p, lo + ids.getQuick(i)), values.getQuick(i));
                                    }
                                } else {
                                    Misc.free(covered);
                                    Assert.assertEquals(label, 0, expected.size());
                                }
                            }
                        }
                    }
                }
                Assert.assertTrue(coveredCursors > 100);
            }
        });
    }

    private static void assertRows(String label, LongList expected, LongList actual) {
        Assert.assertEquals(label + " size", expected.size(), actual.size());
        for (int i = 0, n = expected.size(); i < n; i++) {
            Assert.assertEquals(label + " at " + i, expected.getQuick(i), actual.getQuick(i));
        }
    }

    private static int cursorCoverCount(RowCursor cursor) throws Exception {
        Class<?> clazz = cursor.getClass();
        while (clazz != null) {
            try {
                final Field field = clazz.getDeclaredField("cursorCoverCount");
                field.setAccessible(true);
                return field.getInt(cursor);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchFieldException("cursorCoverCount");
    }

    private static LongList drained(RowCursor cursor, Rnd rnd) {
        final LongList out = new LongList();
        try (DirectLongList sink = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT)) {
            while (true) {
                final long max = 1 + rnd.nextInt(rnd.nextBoolean() ? 5 : 700);
                final long n = cursor.drainTo(sink, 0, max);
                if (n < max) {
                    break;
                }
            }
            for (long i = 0, n = sink.size(); i < n; i++) {
                out.add(sink.get(i));
            }
        } finally {
            Misc.free(cursor);
        }
        return out;
    }

    private static long expectedValue(TableReader reader, int partition, long rowId) {
        // rows are 20_000 per day and the partitions hold whole days
        long first = 0;
        for (int p = 0; p < partition; p++) {
            first += reader.getPartitionRowCount(p);
        }
        return (first + rowId + 1) * 3 + 1;
    }

    private static LongList rows(RowCursor cursor) {
        final LongList out = new LongList();
        try {
            while (cursor.hasNext()) {
                out.add(cursor.next());
            }
        } finally {
            Misc.free(cursor);
        }
        return out;
    }

    private void createTables() throws Exception {
        for (String t : new String[]{"cov", "plain"}) {
            execute("create table " + t + " (ts timestamp, sym symbol, v long, s varchar) timestamp(ts) partition by day");
            // 3 days, 20_000 rows a day; one key missing from the second day
            insert(t, 1, 20_000);
            if (t.equals("cov")) {
                execute("alter table cov alter column sym add index type posting include (v, s)");
            } else {
                execute("alter table plain alter column sym add index type posting");
            }
            // further commits after the index was built: sparse generations on top of the sealed one
            for (long from = 20_001; from <= ROWS; from += 4_000) {
                insert(t, from, 4_000);
            }
        }
    }

    private void insert(String table, long from, long count) throws Exception {
        final String n = "(x + " + (from - 1) + ")";
        execute("insert into " + table + " select "
                + "((" + n + " - 1) * 4_320_000L)::timestamp, "
                + "case when " + n + " % 41 = 0 then null "
                + "when " + n + " % 13 = 0 and (" + n + " - 1) / 20_000 = 1 then 'k1' "
                + "when " + n + " % 13 = 0 then 'k' || (" + n + " % " + KEYS + ") "
                + "else 'k' || ((" + n + " * 7) % " + KEYS + ") end, "
                + n + " * 3 + 1, "
                + "'s' || " + n + " "
                + "from long_sequence(" + count + ")");
    }
}
