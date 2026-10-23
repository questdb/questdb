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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableUtils;
import io.questdb.std.IntList;
import io.questdb.std.Rnd;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A deduplicating commit whose row ties the boundary of two parquet row groups merges the row groups
 * that share the timestamp as one run, decoded into one buffer. Each row group of the run keeps its
 * own column top: after an O3 merge rewrote an earlier row group, that group holds values and a later
 * one still holds its column top. A lazily converted BOOLEAN, BYTE, SHORT or CHAR column, which has no
 * NULL sentinel, must keep the values of the earlier group and the column-top rows of the later one.
 * <p>
 * Partition 2024-01-01 holds rows {@code v} 1..8 at hours 0, 1, 2, 3, 3, 5, 6, 7 in row groups of 4
 * rows, so row groups 0 and 1 share 03:00; 2024-01-02 holds one row. Column {@code b} gets a top over
 * the rows of row groups 0 and 1, an O3 row (v 50 at 01:30) merges into row group 0, then {@code b}
 * changes lazily to the target type. The rows read before the tie commit, through the decode of one
 * row group at a time, are the oracle: the tie commit adds its row and changes no stored row.
 */
public class O3ParquetCoalescedColumnTopTest extends AbstractCairoTest {
    private static final String DAY_ROWS = "(1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z'), "
            + "(3, '2024-01-01T02:00:00.000000Z'), (4, '2024-01-01T03:00:00.000000Z'), "
            + "(5, '2024-01-01T03:00:00.000000Z'), (6, '2024-01-01T05:00:00.000000Z'), "
            + "(7, '2024-01-01T06:00:00.000000Z'), (8, '2024-01-01T07:00:00.000000Z'), "
            + "(99, '2024-01-02T00:00:00.000000Z')";
    // {source type, literal of the value of v 50, target type, the value of v 50 in the target type}
    private static final String[][] LAZY_PAIRS = {
            {"BOOLEAN", "true", "INT", "1"},
            {"BOOLEAN", "true", "LONG", "1"},
            {"BOOLEAN", "true", "FLOAT", "1.0"},
            {"BOOLEAN", "true", "DOUBLE", "1.0"},
            {"BOOLEAN", "true", "DATE", "1970-01-01T00:00:00.001Z"},
            {"BOOLEAN", "true", "TIMESTAMP", "1970-01-01T00:00:00.000001Z"},
            {"BOOLEAN", "true", "VARCHAR", "true"},
            {"BOOLEAN", "true", "STRING", "true"},
            {"BYTE", "5", "INT", "5"},
            {"BYTE", "5", "LONG", "5"},
            {"BYTE", "5", "TIMESTAMP", "1970-01-01T00:00:00.000005Z"},
            {"BYTE", "5", "TIMESTAMP_NS", "1970-01-01T00:00:00.000000005Z"},
            {"BYTE", "5", "DATE", "1970-01-01T00:00:00.005Z"},
            {"BYTE", "5", "FLOAT", "5.0"},
            {"BYTE", "5", "DOUBLE", "5.0"},
            {"BYTE", "5", "DECIMAL(9,2)", "5.00"},
            {"BYTE", "5", "DECIMAL(38,4)", "5.0000"},
            {"BYTE", "5", "VARCHAR", "5"},
            {"BYTE", "5", "STRING", "5"},
            {"BYTE", "5", "SHORT", "5"},
            {"SHORT", "-300", "INT", "-300"},
            {"SHORT", "-300", "LONG", "-300"},
            {"SHORT", "-300", "TIMESTAMP", "1969-12-31T23:59:59.999700Z"},
            {"SHORT", "-300", "DATE", "1969-12-31T23:59:59.700Z"},
            {"SHORT", "-300", "FLOAT", "-300.0"},
            {"SHORT", "-300", "DOUBLE", "-300.0"},
            {"SHORT", "-300", "DECIMAL(9,2)", "-300.00"},
            {"SHORT", "-300", "DECIMAL(38,4)", "-300.0000"},
            {"SHORT", "-300", "VARCHAR", "-300"},
            {"SHORT", "-300", "STRING", "-300"},
            {"SHORT", "5", "BYTE", "5"},
            {"CHAR", "'x'", "VARCHAR", "x"},
            {"CHAR", "'x'", "STRING", "x"},
            // a source with a NULL sentinel, control
            {"INT", "5", "LONG", "5"},
            {"INT", "5", "VARCHAR", "5"},
            {"INT", "5", "STRING", "5"},
    };
    private static final String TIE_ROW = "(51, '2024-01-01T03:00:00.000000Z')";

    @Override
    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
    }

    @Test
    public void testConvertToNativeKeepsColumnTop() throws Exception {
        // the conversion to native converts one row group at a time: the column top of b, rows v 1..8 in
        // row groups 0 and 1 and v 99 of 2024-01-02, stays NULL and the rows with a value keep it
        assertMemoryLeak(() -> {
            for (String target : new String[]{"VARCHAR", "STRING"}) {
                final String t = "pt_" + target;
                execute("CREATE TABLE " + t + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO " + t + " (v, ts) VALUES " + DAY_ROWS);
                drainWalQueue();
                execute("ALTER TABLE " + t + " ADD COLUMN b BYTE");
                drainWalQueue();
                execute("INSERT INTO " + t + " (v, ts, b) VALUES (9, '2024-01-01T08:00:00.000000Z', 0), (10, '2024-01-01T09:00:00.000000Z', 10)");
                drainWalQueue();
                convertToParquet(t);
                execute("ALTER TABLE " + t + " ALTER COLUMN b TYPE " + target);
                drainWalQueue();
                execute("ALTER TABLE " + t + " CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                drainWalQueue();
                assertQuery("SELECT v, b FROM " + t + " WHERE ts < '2024-01-02'")
                        .noLeakCheck()
                        .returns("""
                                v\tb
                                1\t
                                2\t
                                3\t
                                4\t
                                5\t
                                6\t
                                7\t
                                8\t
                                9\t0
                                10\t10
                                """);
                assertQuery("SELECT count() FROM " + t + " WHERE b IS NULL").noLeakCheck().noRandomAccess().expectSize().returns("count\n9\n");
            }
        });
    }

    @Test
    public void testDedupKeyColumnTieReplacesStoredRow() throws Exception {
        // the converted column b is a dedup key. Row groups 03:00 x4 and 03:00,05,06,07 follow row group 0, all
        // three share 03:00 and hold the top of b; the O3 row at 01:30 merges into row group 0, where v 4 at
        // 03:00 now stores b = 0. The commit of (4, 03:00, b = 0) matches that row and replaces it: the row
        // count stays, v 4 takes s = 7
        assertMemoryLeak(() -> {
            for (String[] pair : new String[][]{{"INT", "0"}, {"VARCHAR", "'0'"}}) {
                final String t = "pt_" + pair[0];
                execute("CREATE TABLE " + t + " (v INT, s INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, v)");
                execute("INSERT INTO " + t + " (v, ts) VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z'), "
                        + "(3, '2024-01-01T02:00:00.000000Z'), (4, '2024-01-01T03:00:00.000000Z'), (5, '2024-01-01T03:00:00.000000Z'), "
                        + "(6, '2024-01-01T03:00:00.000000Z'), (7, '2024-01-01T03:00:00.000000Z'), (8, '2024-01-01T03:00:00.000000Z'), "
                        + "(9, '2024-01-01T03:00:00.000000Z'), (10, '2024-01-01T05:00:00.000000Z'), (11, '2024-01-01T06:00:00.000000Z'), "
                        + "(12, '2024-01-01T07:00:00.000000Z'), (99, '2024-01-02T00:00:00.000000Z')");
                drainWalQueue();
                convertToParquet(t);
                execute("ALTER TABLE " + t + " ADD COLUMN b BYTE");
                drainWalQueue();
                execute("INSERT INTO " + t + " (v, ts, b) VALUES (50, '2024-01-01T01:30:00.000000Z', 5)");
                drainWalQueue();
                execute("ALTER TABLE " + t + " DEDUP ENABLE UPSERT KEYS(ts, v, b)");
                execute("ALTER TABLE " + t + " ALTER COLUMN b TYPE " + pair[0]);
                drainWalQueue();
                assertQuery("SELECT v, b::INT b, s FROM " + t + " WHERE v IN (4, 50) ORDER BY v")
                        .noLeakCheck()
                        .returns("v\tb\ts\n4\t0\tnull\n50\t5\tnull\n");
                assertQuery("SELECT count() FROM " + t).noLeakCheck().noRandomAccess().expectSize().returns("count\n14\n");

                execute("INSERT INTO " + t + " (v, ts, b, s) VALUES (4, '2024-01-01T03:00:00.000000Z', " + pair[1] + ", 7)");
                drainWalQueue();
                assertQuery("SELECT v, b::INT b, s FROM " + t + " WHERE v IN (4, 50) ORDER BY v")
                        .noLeakCheck()
                        .returns("v\tb\ts\n4\t0\t7\n50\t5\tnull\n");
                assertQuery("SELECT count() FROM " + t).noLeakCheck().noRandomAccess().expectSize().returns("count\n14\n");
            }
        });
    }

    @Test
    public void testDedupSkipTieKeepsValues() throws Exception {
        // under DEDUP SKIP the first stage of the merge prepares only the key columns, the second stage
        // prepares b
        assertMemoryLeak(() -> {
            for (String target : new String[]{"INT", "VARCHAR"}) {
                final String t = "pt_" + target;
                createTopTable(t, " DEDUP SKIP KEYS(ts, v)", "BYTE", "5", target, true);
                assertQuery("SELECT b FROM " + t + " WHERE v = 50").noLeakCheck().returns("b\n5\n");
                assertTieKeepsRows(t, TIE_ROW);
                assertQuery("SELECT b FROM " + t + " WHERE v = 50").noLeakCheck().returns("b\n5\n");
            }
        });
    }

    @Test
    public void testDedupTieAcrossThreeRowGroupsKeepsValues() throws Exception {
        // row groups 00,01,02,03 | 03,03,03,03 | 03,05,06,07: the tie at 03:00 merges the three row groups as
        // one run, row group 0 holds values and the tops of row groups 1 and 2 join into one block
        assertMemoryLeak(() -> {
            for (String target : new String[]{"INT", "DOUBLE", "DECIMAL(9,2)", "VARCHAR"}) {
                final String t = "pt_" + target.replace('(', '_').replace(',', '_').replace(')', '_');
                execute("CREATE TABLE " + t + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, v)");
                execute("INSERT INTO " + t + " (v, ts) VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z'), "
                        + "(3, '2024-01-01T02:00:00.000000Z'), (4, '2024-01-01T03:00:00.000000Z'), (5, '2024-01-01T03:00:00.000000Z'), "
                        + "(6, '2024-01-01T03:00:00.000000Z'), (7, '2024-01-01T03:00:00.000000Z'), (8, '2024-01-01T03:00:00.000000Z'), "
                        + "(9, '2024-01-01T03:00:00.000000Z'), (10, '2024-01-01T05:00:00.000000Z'), (11, '2024-01-01T06:00:00.000000Z'), "
                        + "(12, '2024-01-01T07:00:00.000000Z'), (99, '2024-01-02T00:00:00.000000Z')");
                drainWalQueue();
                convertToParquet(t);
                execute("ALTER TABLE " + t + " ADD COLUMN b SHORT");
                drainWalQueue();
                execute("INSERT INTO " + t + " (v, ts, b) VALUES (50, '2024-01-01T01:30:00.000000Z', 5)");
                drainWalQueue();
                execute("ALTER TABLE " + t + " ALTER COLUMN b TYPE " + target);
                drainWalQueue();
                assertTieKeepsRows(t, TIE_ROW);
                assertQuery("SELECT v FROM " + t + " WHERE v = 50 AND b IS NULL").noLeakCheck().returns("v\n");
            }
        });
    }

    @Test
    public void testDedupTieAcrossTwoTopsKeepsValues() throws Exception {
        // row groups 00,01,02,03 | 03,04,05,06 | 06,07,08,09 with b added after the conversion: the O3 row
        // v 50 at 04:30 merges into row group 1 alone and ends its top, row groups 0 and 2 keep theirs. The
        // tie rows at 03:00 and 06:00 merge the three row groups as one run with two separate tops
        assertMemoryLeak(() -> {
            for (String target : new String[]{"INT", "DOUBLE", "DECIMAL(9,2)", "VARCHAR", "STRING"}) {
                final String t = "pt_" + target.replace('(', '_').replace(',', '_').replace(')', '_');
                execute("CREATE TABLE " + t + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, v)");
                execute("INSERT INTO " + t + " (v, ts) VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z'), "
                        + "(3, '2024-01-01T02:00:00.000000Z'), (4, '2024-01-01T03:00:00.000000Z'), (5, '2024-01-01T03:00:00.000000Z'), "
                        + "(6, '2024-01-01T04:00:00.000000Z'), (7, '2024-01-01T05:00:00.000000Z'), (8, '2024-01-01T06:00:00.000000Z'), "
                        + "(9, '2024-01-01T06:00:00.000000Z'), (10, '2024-01-01T07:00:00.000000Z'), (11, '2024-01-01T08:00:00.000000Z'), "
                        + "(12, '2024-01-01T09:00:00.000000Z'), (99, '2024-01-02T00:00:00.000000Z')");
                drainWalQueue();
                convertToParquet(t);
                execute("ALTER TABLE " + t + " ADD COLUMN b BYTE");
                drainWalQueue();
                execute("INSERT INTO " + t + " (v, ts, b) VALUES (50, '2024-01-01T04:30:00.000000Z', 5)");
                drainWalQueue();
                execute("ALTER TABLE " + t + " ALTER COLUMN b TYPE " + target);
                drainWalQueue();
                assertTieKeepsRows(t, "(51, '2024-01-01T03:00:00.000000Z'), (52, '2024-01-01T06:00:00.000000Z')");
                assertQuery("SELECT v FROM " + t + " WHERE v = 50 AND b IS NULL").noLeakCheck().returns("v\n");
            }
        });
    }

    @Test
    public void testDedupTieAfterTopRowGroupKeepsValues() throws Exception {
        // row groups 00,01,02,03 | 04,05,06,07 | 07,08,09,10 with b added after row 8: the tie at 07:00
        // merges row group 1, all top, with row group 2, which holds values. The top of the run is
        // one leading block. The table deduplicates only after the conversion: a deduplicating commit
        // of v 9 at 07:00 would merge the native partition and end the column top
        assertMemoryLeak(() -> {
            for (String target : new String[]{"INT", "DOUBLE", "VARCHAR", "STRING"}) {
                final String t = "pt_" + target;
                execute("CREATE TABLE " + t + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO " + t + " (v, ts) VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z'), "
                        + "(3, '2024-01-01T02:00:00.000000Z'), (4, '2024-01-01T03:00:00.000000Z'), (5, '2024-01-01T04:00:00.000000Z'), "
                        + "(6, '2024-01-01T05:00:00.000000Z'), (7, '2024-01-01T06:00:00.000000Z'), (8, '2024-01-01T07:00:00.000000Z'), "
                        + "(99, '2024-01-02T00:00:00.000000Z')");
                drainWalQueue();
                execute("ALTER TABLE " + t + " ADD COLUMN b BYTE");
                drainWalQueue();
                execute("INSERT INTO " + t + " (v, ts, b) VALUES (9, '2024-01-01T07:00:00.000000Z', 9), (10, '2024-01-01T08:00:00.000000Z', 0), "
                        + "(11, '2024-01-01T09:00:00.000000Z', 11), (12, '2024-01-01T10:00:00.000000Z', 12)");
                drainWalQueue();
                convertToParquet(t);
                execute("ALTER TABLE " + t + " DEDUP ENABLE UPSERT KEYS(ts, v)");
                execute("ALTER TABLE " + t + " ALTER COLUMN b TYPE " + target);
                drainWalQueue();
                // the top of b: v 1..8 and v 99 of 2024-01-02
                assertQuery("SELECT count() FROM " + t + " WHERE b IS NULL").noLeakCheck().noRandomAccess().expectSize().returns("count\n9\n");
                assertTieKeepsRows(t, "(51, '2024-01-01T07:00:00.000000Z')");
                assertQuery("SELECT v, b::INT b FROM " + t + " WHERE v BETWEEN 9 AND 12")
                        .noLeakCheck()
                        .returns("v\tb\n9\t9\n10\t0\n11\t11\n12\t12\n");
            }
        });
    }

    @Test
    public void testDedupTieColumnAbsentFromFileKeepsTop() throws Exception {
        // b is added after the conversion and no merge has written it to the parquet file yet: the tie merge
        // has no b to decode and writes the column top of every row
        assertMemoryLeak(() -> {
            for (String target : new String[]{"INT", "VARCHAR"}) {
                final String t = "pt_" + target;
                execute("CREATE TABLE " + t + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, v)");
                execute("INSERT INTO " + t + " (v, ts) VALUES " + DAY_ROWS);
                drainWalQueue();
                convertToParquet(t);
                execute("ALTER TABLE " + t + " ADD COLUMN b BYTE");
                execute("ALTER TABLE " + t + " ALTER COLUMN b TYPE " + target);
                drainWalQueue();
                assertTieKeepsRows(t, TIE_ROW);
                assertQuery("SELECT count() FROM " + t + " WHERE b IS NULL").noLeakCheck().noRandomAccess().expectSize().returns("count\n10\n");
            }
        });
    }

    @Test
    public void testDedupTieFailedCommitThenRetryKeepsValues() throws Exception {
        // the rewrite of the coalesced merge fails to open its parquet file: the WAL table suspends and keeps
        // the rows as they were, the retry after RESUME WAL keeps the values
        final AtomicBoolean isFailing = new AtomicBoolean();
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isFailing.get() && Utf8s.endsWithAscii(name, TableUtils.PARQUET_PARTITION_NAME)
                        && Utf8s.containsAscii(name, "pt")) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            createTopTable("pt", " DEDUP UPSERT KEYS(ts, v)", "BYTE", "5", "INT", true);
            execute("CREATE TABLE snap AS (SELECT v, b, ts FROM pt) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            isFailing.set(true);
            execute("INSERT INTO pt (v, ts) VALUES " + TIE_ROW);
            drainWalQueue();
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pt")));
            assertSqlCursors("SELECT v, b, ts FROM snap ORDER BY ts, v", "SELECT v, b, ts FROM pt ORDER BY ts, v");
            isFailing.set(false);
            execute("ALTER TABLE pt RESUME WAL");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pt")));
            execute("INSERT INTO snap (v, ts) VALUES " + TIE_ROW);
            assertSqlCursors("SELECT v, b, ts FROM snap ORDER BY ts, v", "SELECT v, b, ts FROM pt ORDER BY ts, v");
            assertQuery("SELECT b FROM pt WHERE v = 50").noLeakCheck().returns("b\n5\n");
        });
    }

    @Test
    public void testDedupTieKeepsValuesEveryLazyPair() throws Exception {
        // b is added after the conversion (null chunks of the absent column) or before it (a native column
        // top, rows 1..8, and v 9 with a value at 08:00)
        assertMemoryLeak(() -> {
            int tableIndex = 0;
            for (boolean isAddedAfterConvert : new boolean[]{true, false}) {
                for (String[] pair : LAZY_PAIRS) {
                    final String t = "pt" + tableIndex++;
                    createTopTable(t, " DEDUP UPSERT KEYS(ts, v)", pair[0], pair[1], pair[2], isAddedAfterConvert);
                    final String expected = "b\n" + pair[3] + "\n";
                    assertQuery("SELECT b FROM " + t + " WHERE v = 50").noLeakCheck().returns(expected);
                    assertTieKeepsRows(t, TIE_ROW);
                    assertQuery("SELECT b FROM " + t + " WHERE v = 50").noLeakCheck().returns(expected);
                }
            }
        });
    }

    @Test
    public void testDedupTieKeepsValuesFuzz() throws Exception {
        // random row counts per timestamp, the column added at a random point, random O3 rows with values and
        // tie commits at timestamps that several rows share, which a row group boundary may split; each tie
        // commit adds its row and changes no stored row
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final String[][] pairs = LAZY_PAIRS;
        assertMemoryLeak(() -> {
            for (int iteration = 0; iteration < 30; iteration++) {
                final String t = "pt" + iteration;
                final String[] pair = pairs[rnd.nextInt(pairs.length)];
                execute("CREATE TABLE " + t + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, v)");
                final int rowCount = 8 + rnd.nextInt(16);
                final boolean isAddedAfterConvert = rnd.nextBoolean();
                final int topRowCount = isAddedAfterConvert ? rowCount : 1 + rnd.nextInt(rowCount);
                final StringBuilder rows = new StringBuilder();
                final IntList sharedMinutes = new IntList();
                int minute = 0;
                for (int v = 1; v <= topRowCount; v++) {
                    // a run of equal timestamps every few rows, likely across a row group boundary
                    if (v > 1 && rnd.nextInt(3) == 0) {
                        sharedMinutes.add(minute);
                    } else {
                        minute += 10;
                    }
                    rows.append(v > 1 ? ", " : "").append('(').append(v).append(", '2024-01-01'::TIMESTAMP + ").append(minute).append(" * 60_000_000L)");
                }
                execute("INSERT INTO " + t + " (v, ts) VALUES " + rows + ", (99, '2024-01-02T00:00:00.000000Z')");
                drainWalQueue();
                if (isAddedAfterConvert) {
                    convertToParquet(t);
                }
                execute("ALTER TABLE " + t + " ADD COLUMN b " + pair[0]);
                drainWalQueue();
                if (!isAddedAfterConvert) {
                    rows.setLength(0);
                    for (int v = topRowCount + 1; v <= rowCount; v++) {
                        if (rnd.nextInt(3) == 0) {
                            sharedMinutes.add(minute);
                        } else {
                            minute += 10;
                        }
                        rows.append(v > topRowCount + 1 ? ", " : "").append('(').append(v).append(", '2024-01-01'::TIMESTAMP + ").append(minute)
                                .append(" * 60_000_000L, ").append(pair[1]).append(')');
                    }
                    if (rows.length() > 0) {
                        execute("INSERT INTO " + t + " (v, ts, b) VALUES " + rows);
                        drainWalQueue();
                    }
                    convertToParquet(t);
                }
                final int lastMinute = minute;
                for (int i = 0, n = 1 + rnd.nextInt(3); i < n; i++) {
                    execute("INSERT INTO " + t + " (v, ts, b) VALUES (" + (100 + i) + ", '2024-01-01'::TIMESTAMP + "
                            + (rnd.nextInt(lastMinute + 1) * 60_000_000L + 1) + "L, " + pair[1] + ")");
                    drainWalQueue();
                }
                execute("ALTER TABLE " + t + " ALTER COLUMN b TYPE " + pair[2]);
                drainWalQueue();
                for (int i = 0, n = 1 + rnd.nextInt(3); i < n; i++) {
                    final int tieMinute = sharedMinutes.size() > 0 ? sharedMinutes.getQuick(rnd.nextInt(sharedMinutes.size())) : 10 * rnd.nextInt(lastMinute / 10 + 1);
                    final String tieRow = "(" + (200 + i) + ", '2024-01-01'::TIMESTAMP + " + tieMinute * 60_000_000L + "L)";
                    assertTieKeepsRows(t, tieRow);
                }
                assertQuery("SELECT count() FROM " + t + " WHERE v BETWEEN 100 AND 102 AND b IS NULL")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n0\n");
            }
        });
    }

    @Test
    public void testDedupTieKeepsValuesServerDefaultRowGroupSize() throws Exception {
        // the server default row group size, 100_000 rows: 200_000 rows, two per timestamp from row 1,
        // rows 99_999 and 100_000 share 01:23:20 across the boundary of row groups 0 and 1
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE pt (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, v)");
            execute("INSERT INTO pt SELECT x, '2024-01-01T00:00:00.000000Z'::TIMESTAMP + (x / 2) * 100_000L FROM long_sequence(200_000)");
            execute("INSERT INTO pt (v, ts) VALUES (99, '2024-01-02T00:00:00.000000Z')");
            drainWalQueue();
            convertToParquet("pt");
            execute("ALTER TABLE pt ADD COLUMN b BYTE");
            drainWalQueue();
            execute("INSERT INTO pt (v, ts, b) VALUES (1_000_000, '2024-01-01T00:00:00.050000Z', 5)");
            drainWalQueue();
            execute("ALTER TABLE pt ALTER COLUMN b TYPE INT");
            drainWalQueue();
            assertQuery("SELECT v, ts FROM pt WHERE v IN (100_000, 100_001)")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("v\tts\n100000\t2024-01-01T01:23:20.000000Z\n100001\t2024-01-01T01:23:20.000000Z\n");
            assertTieKeepsRows("pt", "(2_000_000, '2024-01-01T01:23:20.000000Z')");
            assertQuery("SELECT b FROM pt WHERE v = 1_000_000").noLeakCheck().returns("b\n5\n");
        });
    }

    @Test
    public void testDedupTieOverTwoTopRowGroupsKeepsTop() throws Exception {
        // no O3 merge before the tie: row groups 0 and 1 hold only the top of b, which joins into one block
        assertMemoryLeak(() -> {
            for (String target : new String[]{"INT", "DECIMAL(9,2)", "VARCHAR"}) {
                final String t = "pt_" + target.replace('(', '_').replace(',', '_').replace(')', '_');
                execute("CREATE TABLE " + t + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, v)");
                execute("INSERT INTO " + t + " (v, ts) VALUES " + DAY_ROWS);
                drainWalQueue();
                execute("ALTER TABLE " + t + " ADD COLUMN b BYTE");
                drainWalQueue();
                execute("INSERT INTO " + t + " (v, ts, b) VALUES (9, '2024-01-01T08:00:00.000000Z', 9)");
                drainWalQueue();
                convertToParquet(t);
                execute("ALTER TABLE " + t + " ALTER COLUMN b TYPE " + target);
                drainWalQueue();
                assertTieKeepsRows(t, TIE_ROW);
            }
        });
    }

    @Test
    public void testDedupTieRepeatedKeepsValues() throws Exception {
        // a second tie commit at the same boundary merges the rows the first one wrote
        assertMemoryLeak(() -> {
            createTopTable("pt", " DEDUP UPSERT KEYS(ts, v)", "BYTE", "5", "VARCHAR", true);
            assertTieKeepsRows("pt", TIE_ROW);
            assertTieKeepsRows("pt", "(52, '2024-01-01T03:00:00.000000Z')");
            assertQuery("SELECT v, b FROM pt WHERE v IN (50, 51, 52)").noLeakCheck().returns("v\tb\n50\t5\n51\t\n52\t\n");
        });
    }

    @Test
    public void testDedupTieStatisticsDisabledKeepsValues() throws Exception {
        // without statistics a chunk has no null count, so neither the decoder nor the merge sees a column
        // top: the rows keep what the decode of one row group reads
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_STATISTICS_ENABLED, false);
        assertMemoryLeak(() -> {
            for (String target : new String[]{"INT", "VARCHAR"}) {
                final String t = "pt_" + target;
                createTopTable(t, " DEDUP UPSERT KEYS(ts, v)", "BYTE", "5", target, true);
                assertTieKeepsRows(t, TIE_ROW);
                assertQuery("SELECT b FROM " + t + " WHERE v = 50").noLeakCheck().returns("b\n5\n");
            }
        });
    }

    @Test
    public void testDedupTieValuesSurviveReopenAndConvertToNative() throws Exception {
        assertMemoryLeak(() -> {
            // {target type, literal of 0 in the target type}
            for (String[] target : new String[][]{{"INT", "0"}, {"VARCHAR", "'0'"}}) {
                final String t = "pt_" + target[0];
                createTopTable(t, " DEDUP UPSERT KEYS(ts, v)", "BYTE", "5", target[0], true);
                assertTieKeepsRows(t, TIE_ROW);
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                assertSnapshotRows(t);
                execute("ALTER TABLE " + t + " CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                drainWalQueue();
                assertQuery("SELECT isParquet FROM table_partitions('" + t + "') WHERE name = '2024-01-01'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("isParquet\nfalse\n");
                assertSnapshotRows(t);
                for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                    sqlExecutionContext.setJitMode(jitMode);
                    assertSqlCursors("SELECT v FROM snap WHERE b IS NULL ORDER BY v", "SELECT v FROM " + t + " WHERE b IS NULL ORDER BY v");
                    assertSqlCursors("SELECT v FROM snap WHERE b = " + target[1] + " ORDER BY v", "SELECT v FROM " + t + " WHERE b = " + target[1] + " ORDER BY v");
                }
                assertQuery("SELECT b FROM " + t + " WHERE v = 50").noLeakCheck().returns("b\n5\n");
            }
        });
    }

    private static void assertSnapshotRows(String table) throws Exception {
        assertSqlCursors("SELECT v, b, ts FROM snap ORDER BY ts, v", "SELECT v, b, ts FROM " + table + " ORDER BY ts, v");
    }

    // Commits the tie row into the table and into a snapshot of the rows read before the commit: the
    // table must hold the snapshot rows after the commit
    private static void assertTieKeepsRows(String table, String tieRow) throws Exception {
        execute("DROP TABLE IF EXISTS snap");
        execute("CREATE TABLE snap AS (SELECT v, b, ts FROM " + table + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO " + table + " (v, ts) VALUES " + tieRow);
        drainWalQueue();
        execute("INSERT INTO snap (v, ts) VALUES " + tieRow);
        assertSnapshotRows(table);
    }

    private static void convertToParquet(String table) throws Exception {
        execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
    }

    // b gets a top over rows v 1..8, added after the conversion to parquet or before it, with row v 9
    // at 08:00 holding a value. The O3 row v 50 at 01:30 merges into row group 0
    private static void createTopTable(
            String table,
            String dedup,
            String sourceType,
            String value,
            String targetType,
            boolean isAddedAfterConvert
    ) throws Exception {
        execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL" + dedup);
        execute("INSERT INTO " + table + " (v, ts) VALUES " + DAY_ROWS);
        drainWalQueue();
        if (isAddedAfterConvert) {
            convertToParquet(table);
        }
        execute("ALTER TABLE " + table + " ADD COLUMN b " + sourceType);
        drainWalQueue();
        if (!isAddedAfterConvert) {
            execute("INSERT INTO " + table + " (v, ts, b) VALUES (9, '2024-01-01T08:00:00.000000Z', " + value + ")");
            drainWalQueue();
            convertToParquet(table);
        }
        execute("INSERT INTO " + table + " (v, ts, b) VALUES (50, '2024-01-01T01:30:00.000000Z', " + value + ")");
        drainWalQueue();
        execute("ALTER TABLE " + table + " ALTER COLUMN b TYPE " + targetType);
        drainWalQueue();
    }
}
