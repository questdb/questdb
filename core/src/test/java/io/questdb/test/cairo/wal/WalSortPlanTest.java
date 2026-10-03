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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.log.LogFactory;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.LogCapture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Applies blocks of WAL transactions written by several WAL writers. Transactions that are sorted and
 * do not overlap are copied in order, see TableWriterSegmentCopyInfo.buildSortPlan(), the result must be
 * the same as sorting all the rows.
 */
public class WalSortPlanTest extends AbstractCairoTest {
    private static final LogCapture capture = new LogCapture();
    private static final long START_TS = 1_704_067_200_000_000L; // 2024-01-01

    @Override
    @Before
    public void setUp() {
        LogFactory.enableGuaranteedLogging(TableWriter.class);
        super.setUp();
        capture.start();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        try {
            capture.stop();
            super.tearDown();
        } finally {
            LogFactory.disableGuaranteedLogging(TableWriter.class);
        }
    }

    @Test
    public void testLateOverlappingCommit() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final TableToken x = engine.verifyTableName("x");
            try (
                    WalWriter w1 = engine.getWalWriter(x);
                    WalWriter w2 = engine.getWalWriter(x);
                    TableWriter y = getWriter("y")
            ) {
                long ts = START_TS;
                for (int c = 0; c < 20; c++) {
                    writeRows(c % 2 == 0 ? w1 : w2, ts, 200, 1000);
                    writeRows(y, ts, 200, 1000);
                    ts += 200 * 1000;
                }
                // overlaps the 6th commit only, between its rows
                writeRows(w1, START_TS + 5 * 200 * 1000 + 500, 100, 1000);
                writeRows(y, START_TS + 5 * 200 * 1000 + 500, 100, 1000);
                y.commit();
            }
            drainWalQueue();
            capture.drain();
            capture.assertLoggedRE("sorted by plan \\[table=.*x.*, rows=4100, copiedRows=3800");
            assertSqlCursors("y", "x");
        });
    }

    @Test
    public void testNonOverlappingWritersAreCopied() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final TableToken x = engine.verifyTableName("x");
            try (
                    WalWriter w1 = engine.getWalWriter(x);
                    WalWriter w2 = engine.getWalWriter(x);
                    WalWriter w3 = engine.getWalWriter(x);
                    TableWriter y = getWriter("y")
            ) {
                final WalWriter[] walWriters = {w1, w2, w3};
                // writers take turns, every commit is sorted, commits are written out of time order
                final int[] slots = {3, 0, 7, 1, 2, 8, 5, 4, 6, 9, 11, 10};
                for (int c = 0; c < slots.length; c++) {
                    final long ts = START_TS + slots[c] * 300 * 1000L;
                    writeRows(walWriters[c % 3], ts, 300, 1000);
                    writeRows(y, ts, 300, 1000);
                }
                y.commit();
            }
            drainWalQueue();
            capture.drain();
            capture.assertLoggedRE("sorted by plan \\[table=.*x.*, rows=3600, copiedRows=3600, planItems=1");
            assertSqlCursors("y", "x");
        });
    }

    @Test
    public void testSortPlanDisabled() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_WAL_APPLY_SORT_PLAN_ENABLED, false);
        assertMemoryLeak(() -> {
            createTables();
            final TableToken x = engine.verifyTableName("x");
            try (
                    WalWriter w1 = engine.getWalWriter(x);
                    WalWriter w2 = engine.getWalWriter(x);
                    TableWriter y = getWriter("y")
            ) {
                for (int c = 0; c < 10; c++) {
                    final long ts = START_TS + (9 - c) * 300 * 1000L;
                    writeRows(c % 2 == 0 ? w1 : w2, ts, 300, 1000);
                    writeRows(y, ts, 300, 1000);
                }
                y.commit();
            }
            drainWalQueue();
            capture.drain();
            capture.assertNotLogged("sorted by plan");
            assertSqlCursors("y", "x");
        });
    }

    private static void createTables() throws Exception {
        final String columns = "(ts timestamp, i int, l long, d double, s symbol, str string, vc varchar, b boolean)";
        execute("create table x " + columns + " timestamp(ts) partition by hour wal");
        execute("create table y " + columns + " timestamp(ts) partition by hour bypass wal");
    }

    // Column values are derived from the timestamp, rows are the same whichever order they are written in
    private static void writeRows(TableWriterAPI writer, long startTs, int count, long step) {
        for (int r = 0; r < count; r++) {
            final long ts = startTs + r * step;
            final TableWriter.Row row = writer.newRow(ts);
            final long v = ts / step;
            row.putInt(1, (int) (v % 1000));
            row.putLong(2, v);
            row.putDouble(3, v / 3.0);
            row.putSym(4, "s" + (v % 7));
            if (v % 5 != 0) {
                row.putStr(5, "str" + v);
                row.putVarchar(6, new Utf8String(v % 3 == 0 ? "v" + v : "a longer varchar value " + v));
            }
            row.putBool(7, v % 2 == 0);
            row.append();
        }
        if (writer instanceof WalWriter walWriter) {
            walWriter.commit();
        }
    }
}
