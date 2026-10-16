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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An O3 commit into a parquet partition keeps the column top of a BOOLEAN, BYTE, SHORT or CHAR
 * column, which has no NULL sentinel and reads its top rows as NULL only through the column top
 * in the file and the NULL definition levels of its chunks. The parquet table {@code pt} goes
 * through the commit, its native twin {@code tw} takes the same SQL; the native rule decides which
 * tops survive: a top stays iff it ends inside the prefix of rows that keep their position, the
 * rows with {@code ts <= o3Lo - (dedup ? 1 : 0)}, or the commit appends (O3OpenColumnJob, O3PartitionJob).
 * <p>
 * Partition 2024-01-01 holds rows {@code v} 1..8 at hours 0..7, row group size 4, and columns
 * c1, c2, c3 of the tested type with top 2 (rows 1, 2). 2024-01-02 holds one row, so 2024-01-01
 * is not the last partition. After the O3 commit each case reads the column three ways: ALTER c1
 * TYPE SYMBOL (pre-pass over the footer decoder), a lazy ALTER of c2 read through the {@code _pm}
 * decoder, and CONVERT TO NATIVE then ALTER c3 TYPE SYMBOL.
 */
@RunWith(Parameterized.class)
public class O3ParquetNoSentinelColumnTopTest extends AbstractCairoTest {
    private static final String O3_DAY = "2024-01-01";
    private final boolean isWal;
    private final String type;

    public O3ParquetNoSentinelColumnTopTest(WalMode walMode, String type) {
        this.isWal = walMode == WalMode.WITH_WAL;
        this.type = type;
    }

    @Parameterized.Parameters(name = "{0}-{1}")
    public static Collection<Object[]> data() {
        final List<Object[]> params = new ArrayList<>();
        for (WalMode walMode : WalMode.values()) {
            for (String type : new String[]{"BOOLEAN", "BYTE", "SHORT", "CHAR"}) {
                params.add(new Object[]{walMode, type});
            }
        }
        return params;
    }

    @Test
    public void testAbsentColumnAppendRewriteKeepsTop() throws Exception {
        // c1..c3 are absent from the parquet file (added after the conversion, top -1): the commit
        // rewrites the file with the new schema; every old row reads NULL, the O3 row keeps its value
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            for (String t : tables()) {
                createBaseTable(t, "");
                execute("INSERT INTO " + t + " (v, ts) VALUES " + rowsNoValue(1, 8));
                execute("INSERT INTO " + t + " (v, ts) VALUES (99, '2024-01-02T00:00:00.000000Z')");
            }
            drainWalQueue();
            convertToParquet();
            for (String t : tables()) {
                addColumns(t);
            }
            drainWalQueue();
            insertO3("2024-01-01T23:00:00.000000Z");
            assertReaders(topRows(8, 8, -1, "2024-01-01T23:00:00.000000Z", true));
        });
    }

    @Test
    public void testAppendInPlaceKeepsTop() throws Exception {
        assertShape(4, "2024-01-01T23:00:00.000000Z", true);
    }

    @Test
    public void testAppendRewriteKeepsTop() throws Exception {
        // a single row group forces the rewrite
        assertShape(100, "2024-01-01T23:00:00.000000Z", true);
    }

    @Test
    public void testBoundaryO3AtTopRowKeepsTop() throws Exception {
        // a non-dedup row at the timestamp of the first row after the top goes after it: rows 1..3
        // keep their position, the top (2) is inside that prefix
        assertShape(4, "2024-01-01T02:00:00.000000Z", true);
    }

    @Test
    public void testBoundaryO3JustAfterTopRowKeepsTop() throws Exception {
        assertShape(4, "2024-01-01T02:00:00.000001Z", true);
    }

    @Test
    public void testBoundaryO3JustBeforeTopRowMaterializesTop() throws Exception {
        // the O3 row lands before row 3: the prefix is rows 1, 2, the top ends with it and the
        // native merge materializes the top rows as 0/false, as does the parquet merge
        assertShape(4, "2024-01-01T01:59:59.999999Z", false);
    }

    @Test
    public void testDedupSkipAllDroppedKeepsTop() throws Exception {
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP SKIP KEYS(ts)");
            // a duplicate of row 6: the dedup drops it, the file stays as it is
            for (String t : tables()) {
                execute("INSERT INTO " + t + " (v, ts) VALUES (6, '2024-01-01T05:00:00.000000Z')");
            }
            drainWalQueue();
            assertReaders(topRows(8, 2, -1, null, true));
        });
    }

    @Test
    public void testDedupSkipPartlyDroppedKeepsTop() throws Exception {
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP SKIP KEYS(ts)");
            // the duplicate of row 6 is dropped, the row at 23:00 is new: rows 1..5 keep their position
            for (String t : tables()) {
                execute("INSERT INTO " + t + " (v, ts, c1, c2, c3) VALUES (6, '2024-01-01T05:00:00.000000Z', "
                        + o3Literal() + ", " + o3Literal() + ", " + o3Literal() + "), "
                        + "(50, '2024-01-01T23:00:00.000000Z', " + o3Literal() + ", " + o3Literal() + ", " + o3Literal() + ")");
            }
            drainWalQueue();
            assertReaders(topRows(8, 2, -1, "2024-01-01T23:00:00.000000Z", true));
        });
    }

    @Test
    public void testDedupSkipRewriteDroppedFirstMergeKeepsTop() throws Exception {
        // ci changed type: the commit rewrites the partition through the row group materializer. The
        // duplicate of row 4 (row group 0) is dropped, the row at 05:30 merges into row group 1: the
        // output opens at the second merge and first copies row group 0, which keeps its top rows
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP SKIP KEYS(ts)");
            changeControlColumnType();
            for (String t : tables()) {
                execute("INSERT INTO " + t + " (v, ts, c1, c2, c3) VALUES "
                        + "(4, '2024-01-01T03:00:00.000000Z', " + o3Literal() + ", " + o3Literal() + ", " + o3Literal() + "), "
                        + "(50, '2024-01-01T05:30:00.000000Z', " + o3Literal() + ", " + o3Literal() + ", " + o3Literal() + ")");
            }
            drainWalQueue();
            assertReaders(topRows(8, 2, -1, "2024-01-01T05:30:00.000000Z", true));
        });
    }

    @Test
    public void testDedupSkipRewriteDroppedLaterMergeKeepsTop() throws Exception {
        // the row at 02:30 merges into row group 0 after the top and opens the rewrite, the duplicate
        // of row 6 (row group 1) is dropped: the rewrite copies row group 1 as it is
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP SKIP KEYS(ts)");
            changeControlColumnType();
            for (String t : tables()) {
                execute("INSERT INTO " + t + " (v, ts, c1, c2, c3) VALUES "
                        + "(50, '2024-01-01T02:30:00.000000Z', " + o3Literal() + ", " + o3Literal() + ", " + o3Literal() + "), "
                        + "(6, '2024-01-01T05:00:00.000000Z', " + o3Literal() + ", " + o3Literal() + ", " + o3Literal() + ")");
            }
            drainWalQueue();
            assertReaders(topRows(8, 2, -1, "2024-01-01T02:30:00.000000Z", true));
        });
    }

    @Test
    public void testDedupUpsertTieAfterTopKeepsTop() throws Exception {
        // UPSERT of row 4 (03:00): rows 1..3 keep their position, the top (2) is inside the prefix
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)");
            insertO3("2024-01-01T03:00:00.000000Z");
            assertReaders(topRows(8, 2, 4, null, true));
        });
    }

    @Test
    public void testDedupUpsertTieAtTopMaterializesTop() throws Exception {
        // UPSERT of row 3 (02:00): the dedup merge takes the equal row in, the prefix is rows 1, 2
        // and the top ends with it, so native and parquet materialize the top rows
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)");
            insertO3("2024-01-01T02:00:00.000000Z");
            assertReaders(topRows(8, 2, 3, null, false));
        });
    }

    @Test
    public void testFailedCommitThenRetryKeepsTop() throws Exception {
        // the in-place update fails to open data.parquet for writing: the commit fails and leaves the
        // file as it was, the retry keeps the top
        setInPlaceProperties(4);
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
            createShapeTables("");
            isFailing.set(true);
            final String o3Values = "(50, '2024-01-01T23:00:00.000000Z', " + o3Literal() + ", " + o3Literal() + ", " + o3Literal() + ")";
            if (isWal) {
                execute("INSERT INTO pt (v, ts, c1, c2, c3) VALUES " + o3Values);
                drainWalQueue();
                Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pt")));
                isFailing.set(false);
                execute("ALTER TABLE pt RESUME WAL");
                drainWalQueue();
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pt")));
            } else {
                try {
                    execute("INSERT INTO pt (v, ts, c1, c2, c3) VALUES " + o3Values);
                    Assert.fail("expected the commit to fail");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "bulk update failed and will be rolled back");
                }
                isFailing.set(false);
                assertReaders(topRows(8, 2, -1, null, true), false);
                execute("INSERT INTO pt (v, ts, c1, c2, c3) VALUES " + o3Values);
            }
            execute("INSERT INTO tw (v, ts, c1, c2, c3) VALUES " + o3Values);
            drainWalQueue();
            assertReaders(topRows(8, 2, -1, "2024-01-01T23:00:00.000000Z", true));
        });
    }

    @Test
    public void testFullTopAppendInPlaceKeepsTop() throws Exception {
        // c1..c3 were added while 2024-01-01 was the last partition: present in the parquet file with
        // a top of the whole partition. The commit updates in place; old rows read NULL, the O3 row
        // keeps its value.
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            for (String t : tables()) {
                createBaseTable(t, "");
                execute("INSERT INTO " + t + " (v, ts) VALUES " + rowsNoValue(1, 8));
                addColumns(t);
                execute("INSERT INTO " + t + " (v, ts) VALUES (99, '2024-01-02T00:00:00.000000Z')");
            }
            drainWalQueue();
            convertToParquet();
            insertO3("2024-01-01T23:00:00.000000Z");
            assertReaders(topRows(8, 8, -1, "2024-01-01T23:00:00.000000Z", true));
        });
    }

    @Test
    public void testLazyTextSourceAppendRewriteKeepsTop() throws Exception {
        // c1..c3 are VARCHAR with top 2 and a NULL in row 3, changed lazily to the tested type on the
        // parquet partition: the commit rewrites the partition through the row group materializer. The
        // top rows stay NULL; the real NULL of row 3 converts to 0/false, as the native conversion does.
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            for (String t : tables()) {
                createBaseTable(t, "");
                execute("INSERT INTO " + t + " (v, ts) VALUES " + rowsNoValue(1, 2));
                for (String c : new String[]{"c1", "c2", "c3"}) {
                    execute("ALTER TABLE " + t + " ADD COLUMN " + c + " VARCHAR");
                }
                execute("ALTER TABLE " + t + " ADD COLUMN ci INT");
                execute("ALTER TABLE " + t + " ADD COLUMN cv VARCHAR");
                final StringBuilder sql = new StringBuilder("INSERT INTO " + t + " (v, ts, c1, c2, c3) VALUES ");
                for (int v = 3; v <= 8; v++) {
                    if (v > 3) {
                        sql.append(", ");
                    }
                    final String text = v == 3 ? "NULL" : "'" + textOf(v) + "'";
                    sql.append('(').append(v).append(", '").append(tsOf(v)).append("', ")
                            .append(text).append(", ").append(text).append(", ").append(text).append(')');
                }
                execute(sql.toString());
                execute("INSERT INTO " + t + " (v, ts) VALUES (99, '2024-01-02T00:00:00.000000Z')");
            }
            drainWalQueue();
            convertToParquet();
            for (String t : tables()) {
                for (String c : new String[]{"c1", "c2", "c3"}) {
                    execute("ALTER TABLE " + t + " ALTER COLUMN " + c + " TYPE " + type);
                }
            }
            drainWalQueue();
            insertO3("2024-01-01T23:00:00.000000Z");
            final List<Row> expected = topRows(8, 2, -1, "2024-01-01T23:00:00.000000Z", true);
            // row 3 held a real NULL text: 0/false after the change
            expected.set(2, new Row(3, tsOf(3), Kind.ZERO));
            assertReaders(expected);
        });
    }

    @Test
    public void testMergeAfterTopInLaterRowGroupKeepsTop() throws Exception {
        assertShape(4, "2024-01-01T05:30:00.000000Z", true);
    }

    @Test
    public void testMergeAfterTopInTopRowGroupKeepsTop() throws Exception {
        // the O3 row lands in row group 0 after the top: rows 1..3 keep their position
        assertShape(4, "2024-01-01T02:30:00.000000Z", true);
    }

    @Test
    public void testMergeAtTopMaterializesTop() throws Exception {
        assertShape(4, "2024-01-01T01:30:00.000000Z", false);
    }

    @Test
    public void testMergeBeforeTopMaterializesTop() throws Exception {
        assertShape(4, "2024-01-01T00:30:00.000000Z", false);
    }

    @Test
    public void testMergeSplitKeepsTopInSecondRowGroup() throws Exception {
        // converted with row groups of 6 rows, top 5; with row groups of 4 the merge of a row at the
        // timestamp of row 6 (05:00) into row group 0 writes 7 rows in two row groups, of 4 and 3 rows.
        // A non-dedup row goes after the equal row: rows 1..6 keep their place, the second row group
        // starts at row 5, the last top row.
        setInPlaceProperties(6);
        assertMemoryLeak(() -> {
            createShapeTables("", 5);
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
            insertO3("2024-01-01T05:00:00.000000Z");
            assertReaders(topRows(8, 5, -1, "2024-01-01T05:00:00.000000Z", true));
        });
    }

    @Test
    public void testO3BetweenRowGroupsKeepsTop() throws Exception {
        // the row at 03:30 goes into a row group of its own between row groups 0 and 1
        assertShape(4, "2024-01-01T03:30:00.000000Z", true);
    }

    @Test
    public void testRepeatedAppendsAndReopenKeepTop() throws Exception {
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables("");
            insertO3("2024-01-01T22:00:00.000000Z");
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            insertO3("2024-01-01T23:00:00.000000Z");
            final List<Row> expected = topRows(8, 2, -1, "2024-01-01T22:00:00.000000Z", true);
            expected.add(new Row(50, "2024-01-01T23:00:00.000000Z", Kind.O3));
            assertReaders(expected);
        });
    }

    @Test
    public void testTwoPartitionsInOneCommitKeepTops() throws Exception {
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables("");
            for (String t : tables()) {
                execute("INSERT INTO " + t + " (v, ts) VALUES (100, '2024-01-03T00:00:00.000000Z')");
            }
            drainWalQueue();
            // 2024-01-02 holds row 99 with values, convert it too; its single row group goes through
            // the rewrite while 2024-01-01 updates in place
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-02'");
            drainWalQueue();
            for (String t : tables()) {
                execute("INSERT INTO " + t + " (v, ts, c1, c2, c3) VALUES "
                        + "(50, '2024-01-01T23:00:00.000000Z', " + o3Literal() + ", " + o3Literal() + ", " + o3Literal() + "), "
                        + "(51, '2024-01-02T23:00:00.000000Z', " + o3Literal() + ", " + o3Literal() + ", " + o3Literal() + ")");
            }
            drainWalQueue();
            final List<Row> expected = topRows(8, 2, -1, "2024-01-01T23:00:00.000000Z", true);
            assertReaders(expected, true, "2024-01-01", "2024-01-02");
        });
    }

    @Test
    public void testWideTopMergeAfterTopKeepsTop() throws Exception {
        // top 6 runs from row group 0 into row group 1; the O3 row lands in row group 1 after the top
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables("", 6);
            insertO3("2024-01-01T06:30:00.000000Z");
            assertReaders(topRows(8, 6, -1, "2024-01-01T06:30:00.000000Z", true));
        });
    }

    private static String tsOf(int v) {
        return String.format("2024-01-01T%02d:00:00.000000Z", v - 1);
    }

    private void addColumns(String table) throws Exception {
        for (String c : new String[]{"c1", "c2", "c3"}) {
            execute("ALTER TABLE " + table + " ADD COLUMN " + c + " " + type);
        }
        execute("ALTER TABLE " + table + " ADD COLUMN ci INT");
        execute("ALTER TABLE " + table + " ADD COLUMN cv VARCHAR");
        execute("ALTER TABLE " + table + " ADD COLUMN cg GEOHASH(1c)");
        execute("ALTER TABLE " + table + " ADD COLUMN cd DECIMAL(2,1)");
    }

    // c1 to SYMBOL and c2 lazily to the lazy target, then (pt) CONVERT TO NATIVE and c3 to SYMBOL;
    // pt must match tw and the expected rows after each step
    private void assertReaders(List<Row> expected) throws Exception {
        assertReaders(expected, true);
    }

    private void assertReaders(List<Row> expected, boolean isAltering) throws Exception {
        assertReaders(expected, isAltering, O3_DAY);
    }

    private void assertReaders(List<Row> expected, boolean isAltering, String... parquetDays) throws Exception {
        assertSqlCursors("tw ORDER BY ts", "pt ORDER BY ts");
        assertQuery("SELECT v, c1, c2, c3 FROM pt WHERE ts IN '" + O3_DAY + "' ORDER BY ts")
                .noLeakCheck()
                .returns(render(expected, false));
        if (!isAltering) {
            return;
        }
        for (String t : tables()) {
            execute("ALTER TABLE " + t + " ALTER COLUMN c1 TYPE SYMBOL");
            execute("ALTER TABLE " + t + " ALTER COLUMN c2 TYPE " + lazyTarget());
        }
        drainWalQueue();
        assertSqlCursors("tw ORDER BY ts", "pt ORDER BY ts");
        assertQuery("SELECT v, c1, c2, c3 FROM pt WHERE ts IN '" + O3_DAY + "' ORDER BY ts")
                .noLeakCheck()
                .returns(render(expected, true));
        // the filter on the lazily changed column, JIT on and off
        final String nullCount = "count\n" + countTop(expected) + "\n";
        for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
            sqlExecutionContext.setJitMode(jitMode);
            assertQuery("SELECT count() FROM pt WHERE ts IN '" + O3_DAY + "' AND c2 IS NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(nullCount);
        }
        for (String day : parquetDays) {
            execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '" + day + "'");
        }
        drainWalQueue();
        for (String t : tables()) {
            execute("ALTER TABLE " + t + " ALTER COLUMN c3 TYPE SYMBOL");
        }
        drainWalQueue();
        assertSqlCursors("tw ORDER BY ts", "pt ORDER BY ts");
        assertQuery("SELECT v, c1, c2, c3 FROM pt WHERE ts IN '" + O3_DAY + "' ORDER BY ts")
                .noLeakCheck()
                .returns(render(expected, true, true));
    }

    private void assertShape(int rowGroupSize, String o3Ts, boolean isTopKept) throws Exception {
        setInPlaceProperties(rowGroupSize);
        assertMemoryLeak(() -> {
            createShapeTables("");
            insertO3(o3Ts);
            assertReaders(topRows(8, 2, -1, o3Ts, isTopKept));
        });
    }

    // changes the type of control column ci on the parquet partition: the next O3 commit rewrites the
    // partition through the row group materializer
    private void changeControlColumnType() throws Exception {
        for (String t : tables()) {
            execute("ALTER TABLE " + t + " ALTER COLUMN ci TYPE LONG");
        }
        drainWalQueue();
    }

    private void convertToParquet() throws Exception {
        execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '" + O3_DAY + "'");
        drainWalQueue();
    }

    private long countTop(List<Row> rows) {
        long count = 0;
        for (Row row : rows) {
            // CHAR 0 converts to a NULL text, so a materialized CHAR top reads NULL as well
            if (row.kind == Kind.NULL || (row.kind == Kind.ZERO && "CHAR".equals(type))) {
                count++;
            }
        }
        return count;
    }

    private void createBaseTable(String table, String dedup) throws Exception {
        execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY "
                + (isWal ? "WAL" : "BYPASS WAL") + dedup);
    }

    private void createShapeTables(String dedup) throws Exception {
        createShapeTables(dedup, 2);
    }

    // rows 1..8, the columns added after `top` rows, 2024-01-01 converted to parquet
    private void createShapeTables(String dedup, int top) throws Exception {
        for (String t : tables()) {
            createBaseTable(t, dedup);
            execute("INSERT INTO " + t + " (v, ts) VALUES " + rowsNoValue(1, top));
            addColumns(t);
            final StringBuilder sql = new StringBuilder("INSERT INTO " + t + " (v, ts, c1, c2, c3, ci, cv, cg, cd) VALUES ");
            for (int v = top + 1; v <= 8; v++) {
                if (v > top + 1) {
                    sql.append(", ");
                }
                final String literal = literalOf(v);
                sql.append('(').append(v).append(", '").append(tsOf(v)).append("', ")
                        .append(literal).append(", ").append(literal).append(", ").append(literal)
                        .append(", ").append(v).append(", '").append(v).append("', #s, ").append(v % 10).append(".5m)");
            }
            execute(sql.toString());
            execute("INSERT INTO " + t + " (v, ts, c1, c2, c3) VALUES (99, '2024-01-02T00:00:00.000000Z', "
                    + literalOf(4) + ", " + literalOf(4) + ", " + literalOf(4) + ")");
        }
        drainWalQueue();
        convertToParquet();
    }

    private void insertO3(String ts) throws Exception {
        for (String t : tables()) {
            execute("INSERT INTO " + t + " (v, ts, c1, c2, c3, ci, cv) VALUES (50, '" + ts + "', "
                    + o3Literal() + ", " + o3Literal() + ", " + o3Literal() + ", 50, '50')");
        }
        drainWalQueue();
    }

    private String lazyTarget() {
        return switch (type) {
            case "BOOLEAN" -> "VARCHAR";
            case "BYTE" -> "INT";
            case "SHORT" -> "LONG";
            default -> "STRING";
        };
    }

    // SQL literal of the value of row v (3..8), the same in every case: 0/false right after the top,
    // the type's MIN and MAX, a 2-byte and the largest CHAR
    private String literalOf(int v) {
        return switch (type) {
            case "BOOLEAN" -> v % 2 == 0 ? "true" : "false";
            case "BYTE" -> textOf(v) + "::BYTE";
            case "SHORT" -> textOf(v) + "::SHORT";
            default -> "'" + textOf(v) + "'";
        };
    }

    private String o3Literal() {
        return switch (type) {
            case "BOOLEAN" -> "true";
            case "BYTE" -> "9::BYTE";
            case "SHORT" -> "999::SHORT";
            default -> "'q'";
        };
    }

    private String o3Text() {
        return switch (type) {
            case "BOOLEAN" -> "true";
            case "BYTE" -> "9";
            case "SHORT" -> "999";
            default -> "q";
        };
    }

    // renders the expected rows: c1, c2, c3 hold the tested type, or after the ALTERs c1 SYMBOL and c2
    // the lazy target; after CONVERT TO NATIVE c3 SYMBOL as well
    private String render(List<Row> rows, boolean isAltered) {
        return render(rows, isAltered, false);
    }

    private String render(List<Row> rows, boolean isAltered, boolean isC3Altered) {
        final StringSink sink = new StringSink();
        sink.put("v\tc1\tc2\tc3\n");
        for (Row row : rows) {
            final String text = switch (row.kind) {
                case NULL -> null;
                case ZERO -> zeroText();
                case O3 -> o3Text();
                case VALUE -> textOf(row.v);
            };
            sink.put(row.v).put('\t');
            sink.put(renderAs(text, isAltered ? "SYMBOL" : type)).put('\t');
            sink.put(renderAs(text, isAltered ? lazyTarget() : type)).put('\t');
            sink.put(renderAs(text, isC3Altered ? "SYMBOL" : type)).put('\n');
        }
        return sink.toString();
    }

    private String renderAs(String text, String columnType) {
        if (text == null) {
            // a NULL of a type without a sentinel cannot exist in a column of that type: before the ALTERs
            // the column reads its stored value, which the top rows do not have, see topRows()
            return switch (columnType) {
                case "INT", "LONG" -> "null";
                case "BOOLEAN" -> "false";
                case "BYTE", "SHORT" -> "0";
                default -> "";
            };
        }
        if ("CHAR".equals(type) && text.isEmpty()) {
            // CHAR 0 converts to a NULL text
            return "";
        }
        return text;
    }

    private String[] rowsNoValueArray(int lo, int hi) {
        final String[] rows = new String[hi - lo + 1];
        for (int v = lo; v <= hi; v++) {
            rows[v - lo] = "(" + v + ", '" + tsOf(v) + "')";
        }
        return rows;
    }

    private String rowsNoValue(int lo, int hi) {
        return String.join(", ", Arrays.asList(rowsNoValueArray(lo, hi)));
    }

    private String[] tables() {
        return new String[]{"pt", "tw"};
    }

    private void setInPlaceProperties(int rowGroupSize) {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, rowGroupSize);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
    }

    // text of the value of row v (3..8)
    private String textOf(int v) {
        final int i = v - 3;
        return switch (type) {
            case "BOOLEAN" -> v % 2 == 0 ? "true" : "false";
            case "BYTE" -> new String[]{"0", "-128", "127", "1", "-1", "42"}[i];
            case "SHORT" -> new String[]{"0", "-32768", "32767", "1", "-1", "1234"}[i];
            default -> new String[]{"a", "\uFFFF", "\u042F", "z", "0", "b"}[i];
        };
    }

    // rows 1..rowCount of 2024-01-01 with the columns added after `top` rows, the O3 row (if any) at
    // o3Ts, merged at its timestamp, and the O3 row that replaces row upsertRow (-1: none); the top rows read
    // NULL when isTopKept, 0/false otherwise
    private List<Row> topRows(int rowCount, int top, int upsertRow, String o3Ts, boolean isTopKept) {
        final List<Row> rows = new ArrayList<>();
        for (int v = 1; v <= rowCount; v++) {
            final Kind kind = v <= top ? (isTopKept ? Kind.NULL : Kind.ZERO) : Kind.VALUE;
            if (v == upsertRow) {
                rows.add(new Row(50, tsOf(v), Kind.O3));
            } else {
                rows.add(new Row(v, tsOf(v), kind));
            }
        }
        if (o3Ts != null) {
            int pos = 0;
            while (pos < rows.size() && rows.get(pos).ts.compareTo(o3Ts) <= 0) {
                pos++;
            }
            rows.add(pos, new Row(50, o3Ts, Kind.O3));
        }
        return rows;
    }

    private String zeroText() {
        return switch (type) {
            case "BOOLEAN" -> "false";
            case "BYTE", "SHORT" -> "0";
            default -> "";
        };
    }

    private enum Kind {
        NULL, ZERO, VALUE, O3
    }

    private record Row(int v, String ts, Kind kind) {
    }
}
