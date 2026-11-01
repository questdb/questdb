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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
import org.junit.Test;

/**
 * An O3 merge into a parquet partition that lacks a BOOLEAN, BYTE, SHORT or CHAR column, added after the
 * conversion: the rewrite that the schema change forces writes the column into every row group, and when
 * the merge ends the column top, which spans the whole partition, the native merge materializes it as
 * 0/false. The parquet table {@code pt} must read the same as its native twin {@code tw}, which takes the
 * same SQL, also after a change of the column to a type with NULL.
 * <p>
 * Partition 2024-01-01 holds rows {@code v} 1..8 at hours 0..7 in row groups of 4 rows; 2024-01-02 holds row
 * 99, so 2024-01-01 is not the last partition.
 */
public class O3ParquetMissingColumnTopTest extends AbstractCairoTest {
    private static final String COLUMNS = "bo, bt, sh, ch";
    private static final String DAY_ROWS = "(1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z'), "
            + "(3, '2024-01-01T02:00:00.000000Z'), (4, '2024-01-01T03:00:00.000000Z'), "
            + "(5, '2024-01-01T04:00:00.000000Z'), (6, '2024-01-01T05:00:00.000000Z'), "
            + "(7, '2024-01-01T06:00:00.000000Z'), (8, '2024-01-01T07:00:00.000000Z')";
    private static final String O3_DAY = "2024-01-01";
    // the targets of bo BOOLEAN, bt BYTE, sh SHORT and ch CHAR, each with how it renders 0/false and the value
    // of the O3 row: true, 5, 5 and 'x'
    private static final String[][] BOOLEAN_TARGETS = {
            {"INT", "0", "1"},
            {"LONG", "0", "1"},
            {"DOUBLE", "0.0", "1.0"},
            {"DATE", "1970-01-01T00:00:00.000Z", "1970-01-01T00:00:00.001Z"},
            {"TIMESTAMP", "1970-01-01T00:00:00.000000Z", "1970-01-01T00:00:00.000001Z"},
            {"VARCHAR", "false", "true"},
            {"STRING", "false", "true"},
    };
    private static final String[][] CHAR_TARGETS = {
            // CHAR 0 is NULL: the text is NULL
            {"VARCHAR", "", "x"},
            {"STRING", "", "x"},
    };
    private static final String[][] INTEGER_TARGETS = {
            {"INT", "0", "5"},
            {"LONG", "0", "5"},
            {"TIMESTAMP", "1970-01-01T00:00:00.000000Z", "1970-01-01T00:00:00.000005Z"},
            {"DATE", "1970-01-01T00:00:00.000Z", "1970-01-01T00:00:00.005Z"},
            {"DOUBLE", "0.0", "5.0"},
            {"DECIMAL(9,2)", "0.00", "5.00"},
            {"VARCHAR", "0", "5"},
            {"STRING", "0", "5"},
    };

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // the native twin follows the column tops of the default O3 merge, see O3ParquetNoSentinelColumnTopTest
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, false);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
    }

    @Test
    public void testDedupTieOverEndedTopMatchesNative() throws Exception {
        // DEDUP UPSERT KEYS(ts, v), rows 4 and 5 share 03:00 across the boundary of row groups 0 and 1. b is added
        // after the conversion; the row at 01:30 merges into row group 0 and ends its top, row group 1 gets b with
        // 0. After the change to INT, the tie row at 03:00 merges row groups 0 and 1 as one run
        assertMemoryLeak(() -> {
            for (String t : tables()) {
                execute("CREATE TABLE " + t + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, v)");
                execute("INSERT INTO " + t + " (v, ts) VALUES " + DAY_ROWS.replace("2024-01-01T04:00", "2024-01-01T03:00")
                        + ", (99, '2024-01-02T00:00:00.000000Z')");
            }
            drainWalQueue();
            convertToParquet();
            for (String t : tables()) {
                execute("ALTER TABLE " + t + " ADD COLUMN b BYTE");
                execute("INSERT INTO " + t + " (v, ts, b) VALUES (50, '2024-01-01T01:30:00.000000Z', 5)");
                execute("ALTER TABLE " + t + " ALTER COLUMN b TYPE INT");
            }
            drainWalQueue();
            assertTwin();
            for (String t : tables()) {
                execute("INSERT INTO " + t + " (v, ts) VALUES (51, '2024-01-01T03:00:00.000000Z')");
            }
            drainWalQueue();
            assertTwin();
            assertQuery("SELECT v, b FROM pt WHERE ts IN '" + O3_DAY + "' ORDER BY ts, v")
                    .noLeakCheck()
                    .returns("""
                            v\tb
                            1\t0
                            2\t0
                            50\t5
                            3\t0
                            4\t0
                            5\t0
                            51\tnull
                            6\t0
                            7\t0
                            8\t0
                            """);
        });
    }

    @Test
    public void testEndedTopOfMissingColumnReadsZero() throws Exception {
        // bo, bt, sh and ch are added after the conversion; the row at 00:30 merges into row group 0 and ends their
        // top, all 8 rows: every row reads 0/false, as native materializes the top, before and after a change to a
        // type with NULL, read lazily from the parquet partition and after CONVERT TO NATIVE. With and without
        // statistics, WAL and BYPASS WAL
        for (boolean isStatisticsEnabled : new boolean[]{true, false}) {
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_STATISTICS_ENABLED, isStatisticsEnabled);
            assertMemoryLeak(() -> {
                for (String walMode : new String[]{"WAL", "BYPASS WAL"}) {
                    for (int i = 0; i < INTEGER_TARGETS.length; i++) {
                        final String[] boTarget = BOOLEAN_TARGETS[i % BOOLEAN_TARGETS.length];
                        final String[] btTarget = INTEGER_TARGETS[i];
                        final String[] shTarget = INTEGER_TARGETS[(i + 1) % INTEGER_TARGETS.length];
                        final String[] chTarget = CHAR_TARGETS[i % CHAR_TARGETS.length];
                        final String context = walMode + ", statistics " + isStatisticsEnabled + ", bo " + boTarget[0]
                                + ", bt " + btTarget[0] + ", sh " + shTarget[0] + ", ch " + chTarget[0];
                        try {
                            for (String t : tables()) {
                                execute("CREATE TABLE " + t + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walMode);
                                execute("INSERT INTO " + t + " (v, ts) VALUES " + DAY_ROWS + ", (99, '2024-01-02T00:00:00.000000Z')");
                            }
                            drainWalQueue();
                            convertToParquet();
                            for (String t : tables()) {
                                execute("ALTER TABLE " + t + " ADD COLUMN bo BOOLEAN");
                                execute("ALTER TABLE " + t + " ADD COLUMN bt BYTE");
                                execute("ALTER TABLE " + t + " ADD COLUMN sh SHORT");
                                execute("ALTER TABLE " + t + " ADD COLUMN ch CHAR");
                                execute("INSERT INTO " + t + " (v, ts, " + COLUMNS + ") VALUES (50, '2024-01-01T00:30:00.000000Z', true, 5, 5, 'x')");
                            }
                            drainWalQueue();
                            assertTwin();
                            assertDayRows(new String[]{"false", "0", "0", ""}, new String[]{"true", "5", "5", "x"});
                            for (String t : tables()) {
                                execute("ALTER TABLE " + t + " ALTER COLUMN bo TYPE " + boTarget[0]);
                                execute("ALTER TABLE " + t + " ALTER COLUMN bt TYPE " + btTarget[0]);
                                execute("ALTER TABLE " + t + " ALTER COLUMN sh TYPE " + shTarget[0]);
                                execute("ALTER TABLE " + t + " ALTER COLUMN ch TYPE " + chTarget[0]);
                            }
                            drainWalQueue();
                            final String[] zeros = {boTarget[1], btTarget[1], shTarget[1], chTarget[1]};
                            final String[] o3Values = {boTarget[2], btTarget[2], shTarget[2], chTarget[2]};
                            assertTwin();
                            assertDayRows(zeros, o3Values);
                            execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '" + O3_DAY + "'");
                            drainWalQueue();
                            assertTwin();
                            assertDayRows(zeros, o3Values);
                            for (String t : tables()) {
                                execute("DROP TABLE " + t);
                            }
                            drainWalQueue();
                        } catch (AssertionError e) {
                            throw new AssertionError(context, e);
                        }
                    }
                }
            });
        }
    }

    @Test
    public void testEndedTopOfMissingColumnPrunesByValue() throws Exception {
        // bo, bt and sh are added after the conversion; the row at 00:30 ends their top, all 8 rows. The rewrite
        // writes the copied row groups with a chunk of 0/false, whose _pm statistics (null count 0, min = max = 0)
        // the row group pruner reads: a filter on 0/false must keep those row groups, a filter on the O3 value may
        // prune them, and the counts match the native twin
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_STATISTICS_ENABLED, true);
        assertMemoryLeak(() -> {
            for (String t : tables()) {
                execute("CREATE TABLE " + t + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO " + t + " (v, ts) VALUES " + DAY_ROWS + ", (99, '2024-01-02T00:00:00.000000Z')");
            }
            drainWalQueue();
            convertToParquet();
            for (String t : tables()) {
                execute("ALTER TABLE " + t + " ADD COLUMN bo BOOLEAN");
                execute("ALTER TABLE " + t + " ADD COLUMN bt BYTE");
                execute("ALTER TABLE " + t + " ADD COLUMN sh SHORT");
                execute("INSERT INTO " + t + " (v, ts, bo, bt, sh) VALUES (50, '2024-01-01T00:30:00.000000Z', true, 5, 5)");
            }
            drainWalQueue();
            final String[][] filterCounts = {
                    {"bt = 0", "8"},
                    {"sh = 0", "8"},
                    {"bo = false", "8"},
                    {"bt = 5", "1"},
                    {"sh = 5", "1"},
                    {"bo = true", "1"},
            };
            for (String[] filterCount : filterCounts) {
                for (String t : tables()) {
                    assertQuery("SELECT count() FROM " + t + " WHERE ts IN '" + O3_DAY + "' AND " + filterCount[0])
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("count\n" + filterCount[1] + "\n");
                }
            }
        });
    }

    @Test
    public void testPresentTopKeptWhileMissingTopEnds() throws Exception {
        // p BYTE has top 2 in the parquet file, m BYTE is added after the conversion. The row at 02:30 merges into
        // row group 0 after row 3: the prefix of rows 1..3 keeps the top of p, and ends the top of m, all 8 rows
        assertMemoryLeak(() -> {
            for (String t : tables()) {
                execute("CREATE TABLE " + t + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO " + t + " (v, ts) VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-01T01:00:00.000000Z')");
                execute("ALTER TABLE " + t + " ADD COLUMN p BYTE");
                execute("INSERT INTO " + t + " (v, ts, p) VALUES (3, '2024-01-01T02:00:00.000000Z', 3), (4, '2024-01-01T03:00:00.000000Z', 4), "
                        + "(5, '2024-01-01T04:00:00.000000Z', 5), (6, '2024-01-01T05:00:00.000000Z', 6), "
                        + "(7, '2024-01-01T06:00:00.000000Z', 7), (8, '2024-01-01T07:00:00.000000Z', 8)");
                execute("INSERT INTO " + t + " (v, ts) VALUES (99, '2024-01-02T00:00:00.000000Z')");
            }
            drainWalQueue();
            convertToParquet();
            for (String t : tables()) {
                execute("ALTER TABLE " + t + " ADD COLUMN m BYTE");
                execute("INSERT INTO " + t + " (v, ts, p, m) VALUES (50, '2024-01-01T02:30:00.000000Z', 9, 9)");
                execute("ALTER TABLE " + t + " ALTER COLUMN p TYPE INT");
                execute("ALTER TABLE " + t + " ALTER COLUMN m TYPE INT");
            }
            drainWalQueue();
            assertTwin();
            assertQuery("SELECT v, p, m FROM pt WHERE ts IN '" + O3_DAY + "' ORDER BY ts")
                    .noLeakCheck()
                    .returns("""
                            v\tp\tm
                            1\tnull\t0
                            2\tnull\t0
                            3\t3\t0
                            50\t9\t9
                            4\t4\t0
                            5\t5\t0
                            6\t6\t0
                            7\t7\t0
                            8\t8\t0
                            """);
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            assertTwin();
        });
    }

    private static void assertTwin() throws Exception {
        assertSqlCursors("tw ORDER BY ts", "pt ORDER BY ts");
    }

    private static void convertToParquet() throws Exception {
        execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '" + O3_DAY + "'");
        drainWalQueue();
    }

    private static String[] tables() {
        return new String[]{"pt", "tw"};
    }

    // the rows of 2024-01-01 of pt: rows 1..8 hold the zeros, the O3 row at 00:30 the O3 values, of bo, bt, sh, ch
    private void assertDayRows(String[] zeros, String[] o3Values) throws Exception {
        final StringSink expected = new StringSink();
        expected.put("v\tbo\tbt\tsh\tch\n");
        for (int v = 1; v <= 8; v++) {
            expected.put(v).put('\t').put(String.join("\t", zeros)).put('\n');
            if (v == 1) {
                expected.put("50\t").put(String.join("\t", o3Values)).put('\n');
            }
        }
        assertQuery("SELECT v, " + COLUMNS + " FROM pt WHERE ts IN '" + O3_DAY + "' ORDER BY ts")
                .noLeakCheck()
                .returns(expected.toString());
    }
}
