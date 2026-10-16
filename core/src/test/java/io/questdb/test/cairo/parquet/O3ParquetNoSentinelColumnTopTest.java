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
 * is not the last partition. After the O3 commit each case reads the column three ways, in this
 * order: a lazy ALTER of c2 to the lazy target, read through the {@code _pm} decoder while the
 * partition is still parquet; ALTER c1 TYPE SYMBOL (pre-pass over the footer decoder, which converts
 * the partition to native); and CONVERT TO NATIVE then ALTER c3 TYPE SYMBOL.
 * <p>
 * The lazy targets cover each way the {@code _pm} decoder turns top rows into NULL: through the
 * chunk's {@code null_count} (BOOLEAN to VARCHAR and INT, BYTE to VARCHAR and DOUBLE, SHORT to
 * STRING and DOUBLE) and through the definition levels (BYTE to INT, SHORT to LONG).
 */
@RunWith(Parameterized.class)
public class O3ParquetNoSentinelColumnTopTest extends AbstractCairoTest {
    // the absent columns of every type, with the SQL literal of the O3 row and whether IS NULL applies: the
    // types without NULL and the arrays, which IS NULL does not take, compare with tw only
    private static final String[][] EVERY_TYPE_COLUMNS = {
            {"cbool", "BOOLEAN", "true", "false"},
            {"cbyte", "BYTE", "9::BYTE", "false"},
            {"cshort", "SHORT", "999::SHORT", "false"},
            {"cchar", "CHAR", "'q'", "false"},
            {"cint", "INT", "7", "true"},
            {"clong", "LONG", "8", "true"},
            {"cfloat", "FLOAT", "1.5", "true"},
            {"cdouble", "DOUBLE", "3.0", "true"},
            {"cdate", "DATE", "'2024-01-01T00:00:00.000Z'::DATE", "true"},
            {"ctimestamp", "TIMESTAMP", "'2024-01-01T00:00:00.000000Z'::TIMESTAMP", "true"},
            {"cipv4", "IPv4", "'1.2.3.4'::IPv4", "true"},
            {"cuuid", "UUID", "'11111111-1111-1111-1111-111111111111'::UUID", "true"},
            {"clong256", "LONG256", "'0x01'::LONG256", "true"},
            {"cgeo1", "GEOHASH(1c)", "#s", "true"},
            {"cgeo4", "GEOHASH(4c)", "#sp05", "true"},
            {"cgeo8", "GEOHASH(8c)", "#sp052w92", "true"},
            {"cgeo12", "GEOHASH(12c)", "#sp052w92p1p8", "true"},
            {"cdec2", "DECIMAL(2,1)", "1.5m", "true"},
            {"cdec18", "DECIMAL(18,2)", "12.34m", "true"},
            {"cdec38", "DECIMAL(38,4)", "1.2345m", "true"},
            {"csym", "SYMBOL", "'sym'", "true"},
            {"cstring", "STRING", "'str'", "true"},
            {"cvarchar", "VARCHAR", "'42'", "true"},
            {"cbinary", "BINARY", "from_base64('Ymlu')", "true"},
            {"carr1", "DOUBLE[]", "ARRAY[1.0, 2.0]", "false"},
            {"carr2", "DOUBLE[][]", "ARRAY[[1.0], [2.0]]", "false"},
    };
    private static final String O3_DAY = "2024-01-01";
    private final boolean isWal;
    private final String lazyTarget;
    private final String type;

    public O3ParquetNoSentinelColumnTopTest(WalMode walMode, String type, String lazyTarget) {
        this.isWal = walMode == WalMode.WITH_WAL;
        this.type = type;
        this.lazyTarget = lazyTarget;
    }

    @Parameterized.Parameters(name = "{0}-{1}-{2}")
    public static Collection<Object[]> data() {
        final String[][] typeTargets = {
                {"BOOLEAN", "VARCHAR"},
                {"BOOLEAN", "INT"},
                {"BYTE", "VARCHAR"},
                {"BYTE", "DOUBLE"},
                {"BYTE", "INT"},
                {"SHORT", "STRING"},
                {"SHORT", "DOUBLE"},
                {"SHORT", "LONG"},
                {"CHAR", "STRING"},
        };
        final List<Object[]> params = new ArrayList<>();
        for (WalMode walMode : WalMode.values()) {
            for (String[] typeTarget : typeTargets) {
                params.add(new Object[]{walMode, typeTarget[0], typeTarget[1]});
            }
        }
        return params;
    }

    @Test
    public void testAbsentColumnAppendRewriteAfterDropColumnKeepsTop() throws Exception {
        // the rewrite drops column cx, present in the parquet file, and writes the null chunks of the
        // absent c1..c3 in the same commit
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createAbsentColumnTables("cx");
            for (String t : tables()) {
                execute("ALTER TABLE " + t + " DROP COLUMN cx");
            }
            drainWalQueue();
            insertO3("2024-01-01T23:00:00.000000Z");
            assertReaders(topRows(8, 8, -1, "2024-01-01T23:00:00.000000Z", true));
        });
    }

    @Test
    public void testAbsentColumnAppendRewriteKeepsTop() throws Exception {
        // c1..c3 are absent from the parquet file (added after the conversion, top -1): the commit
        // rewrites the file with the new schema; every old row reads NULL, the O3 row keeps its value
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createAbsentColumnTables("");
            insertO3("2024-01-01T23:00:00.000000Z");
            assertReaders(topRows(8, 8, -1, "2024-01-01T23:00:00.000000Z", true));
        });
    }

    @Test
    public void testAbsentColumnAppendRewriteThenAppendInPlace() throws Exception {
        // the second append updates the rewritten file in place and keeps the top of its null chunks,
        // also after the table reopens
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createAbsentColumnTables("");
            insertO3("2024-01-01T23:00:00.000000Z");
            insertO3("2024-01-01T23:30:00.000000Z");
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            final List<Row> expected = topRows(8, 8, -1, "2024-01-01T23:00:00.000000Z", true);
            expected.add(new Row(50, "2024-01-01T23:30:00.000000Z", Kind.O3));
            assertReaders(expected);
        });
    }

    @Test
    public void testAbsentColumnAppendRewriteThenMergeAfterLazyAlter() throws Exception {
        // after the append rewrite c2 changes lazily to the lazy target, then a row at 02:30 merges into
        // row group 0. The changed type re-encodes every row group of the partition, decoding the null
        // chunks through the _pm decoder. The merge ends the top (8) past the prefix of rows 1..3: c1, c3
        // read 0/false in rows 1..8, as native materializes them; c2 reads NULL there, as native reads the
        // top rows of a column changed to a type with NULL
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createAbsentColumnTables("");
            insertO3("2024-01-01T23:00:00.000000Z");
            for (String t : tables()) {
                execute("ALTER TABLE " + t + " ALTER COLUMN c2 TYPE " + lazyTarget);
            }
            drainWalQueue();
            final String o3c2 = renderAs(o3Text(), lazyTarget);
            final String o3c2Literal = "VARCHAR".equals(lazyTarget) || "STRING".equals(lazyTarget) ? "'" + o3c2 + "'" : o3c2;
            for (String t : tables()) {
                execute("INSERT INTO " + t + " (v, ts, c1, c2, c3, ci, cv) VALUES (50, '2024-01-01T02:30:00.000000Z', "
                        + o3Literal() + ", " + o3c2Literal + ", " + o3Literal() + ", 50, '50')");
            }
            drainWalQueue();
            assertSqlCursors("tw ORDER BY ts", "pt ORDER BY ts");
            final String zero = renderAs(zeroText(), type);
            final String o3 = renderAs(o3Text(), type);
            final StringSink expected = new StringSink();
            expected.put("v\tc1\tc2\tc3\n");
            for (int v = 1; v <= 8; v++) {
                expected.put(v).put('\t').put(zero).put('\t').put(renderAs(null, lazyTarget)).put('\t').put(zero).put('\n');
                if (v == 3) {
                    expected.put("50\t").put(o3).put('\t').put(o3c2).put('\t').put(o3).put('\n');
                }
            }
            expected.put("50\t").put(o3).put('\t').put(o3c2).put('\t').put(o3).put('\n');
            assertQuery("SELECT v, c1, c2, c3 FROM pt WHERE ts IN '" + O3_DAY + "' ORDER BY ts")
                    .noLeakCheck()
                    .returns(expected.toString());
            assertC2NullCount(O3_DAY, 8);
            execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '" + O3_DAY + "'");
            drainWalQueue();
            assertSqlCursors("tw ORDER BY ts", "pt ORDER BY ts");
        });
    }

    @Test
    public void testAbsentColumnsOfEveryTypeAppendRewriteReadNull() throws Exception {
        // 8 rows in two row groups of 4: the append rewrite copies both with a null chunk per column
        assertAbsentColumnsOfEveryTypeReadNull(8);
    }

    @Test
    public void testAbsentColumnsOfEveryTypeAppendRewriteThenMergeMatchNative() throws Exception {
        // 4 rows in one row group of 4: the append rewrite copies it with a null chunk per column and
        // writes the O3 row in a row group of its own. The row at 02:30 merges into row group 0 and
        // decodes every null chunk through the _pm decoder; the top (4) ends past the prefix of rows
        // 1..3 and is materialized: the types with NULL read NULL in rows 1..4, the others 0/false
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createEveryTypeTables(4);
            insertEveryTypeO3("2024-01-01T23:00:00.000000Z");
            insertEveryTypeO3("2024-01-01T02:30:00.000000Z");
            assertSqlCursors("tw ORDER BY ts", "pt ORDER BY ts");
            assertEveryTypeNullCounts(4, 2);
            assertQuery("SELECT v, cbool, cbyte, cshort, cchar FROM pt WHERE ts IN '" + O3_DAY + "' ORDER BY ts")
                    .noLeakCheck()
                    .returns("""
                            v\tcbool\tcbyte\tcshort\tcchar
                            1\tfalse\t0\t0\t
                            2\tfalse\t0\t0\t
                            3\tfalse\t0\t0\t
                            50\ttrue\t9\t999\tq
                            4\tfalse\t0\t0\t
                            50\ttrue\t9\t999\tq
                            """);
        });
    }

    @Test
    public void testAbsentColumnsOfEveryTypeAppendRewriteWithPartialRowGroupReadNull() throws Exception {
        // 9 rows in row groups of 4, 4 and 1
        assertAbsentColumnsOfEveryTypeReadNull(9);
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
    public void testDedupSkipDroppedAtMaxThenAppendKeepsTop() throws Exception {
        // top 7, the duplicate of row 8 (07:00) sits on the partition max: it still counts among the
        // O3 rows at or below the max, all dropped, and the row at 23:00 appends. A merge would keep
        // rows 1..7 in place, which ends the top with the prefix; the append keeps it
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP SKIP KEYS(ts)", 7);
            insertO3Rows(o3Row(8, "2024-01-01T07:00:00.000000Z"), o3Row(50, "2024-01-01T23:00:00.000000Z"));
            assertReaders(topRows(8, 7, -1, "2024-01-01T23:00:00.000000Z", true));
        });
    }

    @Test
    public void testDedupSkipDroppedInLastRowGroupThenAppendKeepsTop() throws Exception {
        // 10 rows in row groups of 4, 4 and 2, top 9: one MERGE of the last row group takes the
        // duplicate of row 10 (09:00), which the dedup drops, and the row at 23:00, which it keeps.
        // The merge writes rows, and its own duplicate is the only O3 row at or below the max
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP SKIP KEYS(ts)", 9, 10, -1);
            insertO3Rows(o3Row(10, "2024-01-01T09:00:00.000000Z"), o3Row(50, "2024-01-01T23:00:00.000000Z"));
            assertReaders(topRows(10, 9, -1, "2024-01-01T23:00:00.000000Z", true));
        });
    }

    @Test
    public void testDedupSkipDroppedThenAppendAbsentColumnKeepsTop() throws Exception {
        // c1..c3 are absent from the parquet file (top 8): the duplicate of row 4 is dropped, the row
        // at 23:00 rewrites the file with the new schema; every old row reads NULL
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createAbsentColumnTables("", " DEDUP SKIP KEYS(ts)");
            insertO3Rows(o3Row(4, "2024-01-01T03:00:00.000000Z"), o3Row(50, "2024-01-01T23:00:00.000000Z"));
            assertReaders(topRows(8, 8, -1, "2024-01-01T23:00:00.000000Z", true));
        });
    }

    @Test
    public void testDedupSkipDroppedThenAppendIntoPartialRowGroupKeepsTop() throws Exception {
        // 10 rows in row groups of 4, 4 and 2, top 9: the duplicate of row 6 is dropped, the row at
        // 23:00 merges into the partial last row group, past its max
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP SKIP KEYS(ts)", 9, 10, -1);
            insertO3Rows(o3Row(6, "2024-01-01T05:00:00.000000Z"), o3Row(50, "2024-01-01T23:00:00.000000Z"));
            assertReaders(topRows(10, 9, -1, "2024-01-01T23:00:00.000000Z", true));
        });
    }

    @Test
    public void testDedupSkipDroppedThenAppendKeepsTop() throws Exception {
        // top 6, the duplicate of row 4 (03:00) is dropped and the row at 23:00 lands after the partition:
        // native turns the commit into an append, which keeps the top, and so does the parquet update
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP SKIP KEYS(ts)", 6);
            insertO3Rows(o3Row(4, "2024-01-01T03:00:00.000000Z"), o3Row(50, "2024-01-01T23:00:00.000000Z"));
            assertReaders(topRows(8, 6, -1, "2024-01-01T23:00:00.000000Z", true));
        });
    }

    @Test
    public void testDedupSkipDroppedThenAppendRewriteKeepsTop() throws Exception {
        // ci changed type: the rewrite opens at the row group of the row at 23:00 and first copies row
        // groups 0 and 1 through the materializer, which keeps the top rows
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP SKIP KEYS(ts)", 6);
            changeControlColumnType();
            insertO3Rows(o3Row(4, "2024-01-01T03:00:00.000000Z"), o3Row(50, "2024-01-01T23:00:00.000000Z"));
            assertReaders(topRows(8, 6, -1, "2024-01-01T23:00:00.000000Z", true));
        });
    }

    @Test
    public void testDedupSkipDroppedThenAppendSurvivesReopen() throws Exception {
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP SKIP KEYS(ts)", 6);
            insertO3Rows(o3Row(4, "2024-01-01T03:00:00.000000Z"), o3Row(50, "2024-01-01T23:00:00.000000Z"));
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            insertO3("2024-01-01T23:30:00.000000Z");
            final List<Row> expected = topRows(8, 6, -1, "2024-01-01T23:00:00.000000Z", true);
            expected.add(new Row(50, "2024-01-01T23:30:00.000000Z", Kind.O3));
            assertReaders(expected);
        });
    }

    @Test
    public void testDedupSkipDroppedThenAppendTwoPartitionsKeepOwnTops() throws Exception {
        // 2024-01-01 and 2024-01-02 hold rows 1..8 with top 3 each. One commit drops a duplicate in
        // each: 2024-01-01 then appends at 23:00 and keeps its top, 2024-01-02 merges a row at 02:30
        // after the duplicate of row 2 and materializes its top, as native does
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            for (String t : tables()) {
                createBaseTable(t, " DEDUP SKIP KEYS(ts)");
                execute("INSERT INTO " + t + " (v, ts) VALUES " + rowsNoValue(1, 3) + ", "
                        + rowsNoValue(1, 3).replace(O3_DAY, "2024-01-02"));
                addColumns(t);
                final StringBuilder sql = new StringBuilder("INSERT INTO " + t + " (v, ts, c1, c2, c3) VALUES ");
                for (String day : new String[]{O3_DAY, "2024-01-02"}) {
                    for (int v = 4; v <= 8; v++) {
                        if (sql.charAt(sql.length() - 1) == ')') {
                            sql.append(", ");
                        }
                        final String literal = literalOf(v);
                        sql.append('(').append(v).append(", '").append(tsOf(v).replace(O3_DAY, day)).append("', ")
                                .append(literal).append(", ").append(literal).append(", ").append(literal).append(')');
                    }
                }
                execute(sql.toString());
                execute("INSERT INTO " + t + " (v, ts) VALUES (100, '2024-01-03T00:00:00.000000Z')");
            }
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-02'");
            drainWalQueue();
            insertO3Rows(
                    o3Row(4, "2024-01-01T03:00:00.000000Z"),
                    o3Row(50, "2024-01-01T23:00:00.000000Z"),
                    o3Row(2, "2024-01-02T01:00:00.000000Z"),
                    o3Row(50, "2024-01-02T02:30:00.000000Z")
            );
            final List<Row> expectedDay2 = topRows(8, 3, -1, null, false);
            expectedDay2.add(3, new Row(50, "2024-01-02T02:30:00.000000Z", Kind.O3));
            assertReaders(
                    true,
                    new String[]{"2024-01-01", "2024-01-02"},
                    List.of(topRows(8, 3, -1, "2024-01-01T23:00:00.000000Z", true), expectedDay2)
            );
        });
    }

    @Test
    public void testDedupSkipDroppedThenMergeMaterializesTop() throws Exception {
        // top 3, the duplicate of row 2 (01:00) is dropped, but the row at 02:30 merges into row group 0:
        // native merges with the prefix of row 1 and materializes the top, the row at 23:00 does not
        // turn the commit into an append. The top stays inside the merged row group
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP SKIP KEYS(ts)", 3);
            insertO3Rows(
                    o3Row(2, "2024-01-01T01:00:00.000000Z"),
                    o3Row(50, "2024-01-01T02:30:00.000000Z"),
                    o3Row(50, "2024-01-01T23:00:00.000000Z")
            );
            final List<Row> expected = topRows(8, 3, -1, "2024-01-01T02:30:00.000000Z", false);
            expected.add(new Row(50, "2024-01-01T23:00:00.000000Z", Kind.O3));
            assertReaders(expected);
        });
    }

    @Test
    public void testDedupSkipNewKeyAtStoredTimestampMaterializesTop() throws Exception {
        // KEYS(ts, v): the O3 row at 01:00 has another v than row 2, so the dedup keeps it and native
        // merges with the prefix of row 1, which materializes the top (3), although the row at 23:00
        // lands after the partition
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP SKIP KEYS(ts, v)", 3);
            insertO3Rows(o3Row(50, "2024-01-01T01:00:00.000000Z"), o3Row(50, "2024-01-01T23:00:00.000000Z"));
            final List<Row> expected = topRows(8, 3, -1, "2024-01-01T01:00:00.000000Z", false);
            expected.add(new Row(50, "2024-01-01T23:00:00.000000Z", Kind.O3));
            assertReaders(expected);
        });
    }

    @Test
    public void testDedupSkipNewRowJustAfterMaxKeepsTop() throws Exception {
        // the duplicate of row 4 is dropped, the new row lands one microsecond after the partition max
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP SKIP KEYS(ts)", 6);
            insertO3Rows(o3Row(4, "2024-01-01T03:00:00.000000Z"), o3Row(50, "2024-01-01T07:00:00.000001Z"));
            assertReaders(topRows(8, 6, -1, "2024-01-01T07:00:00.000001Z", true));
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
    public void testDedupSkipRewriteCopiesTwoRowGroupsKeepsTop() throws Exception {
        // 12 rows, top 6 runs from row group 0 into row group 1, ci changed type: the new row at 09:30
        // merges into row group 2 and opens the rewrite, which first copies row groups 0 and 1 through
        // the materializer. Row group 1 starts at row 5: rows 5, 6 keep the top, rows 7, 8 their values.
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP SKIP KEYS(ts)", 6, 12, -1);
            changeControlColumnType();
            insertO3("2024-01-01T09:30:00.000000Z");
            assertReaders(topRows(12, 6, -1, "2024-01-01T09:30:00.000000Z", true));
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
    public void testDuplicateTimestampsAtPrefixLineKeepTop() throws Exception {
        // rows 3 and 4 both at 02:00, top 3, row groups of 6: the prefix line falls inside row group 0.
        // A non-dedup row at 02:00 goes after both equal rows: rows 1..4 keep their position, and the
        // top (3) ends inside that prefix
        setInPlaceProperties(6);
        assertMemoryLeak(() -> {
            createShapeTables("", 3, 8, 4);
            insertO3("2024-01-01T02:00:00.000000Z");
            final List<Row> expected = topRows(8, 3, -1, null, true);
            expected.add(4, new Row(50, "2024-01-01T02:00:00.000000Z", Kind.O3));
            assertReaders(expected);
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
    public void testTwoPartitionsInOneCommitKeepOwnTops() throws Exception {
        // 2024-01-01 holds rows 1..8 before the columns exist (a top of the whole partition),
        // 2024-01-02 rows 1, 2 before them and rows 3..8 after (top 2). One commit appends to
        // 2024-01-01, which keeps its top, and merges before the top of 2024-01-02, which
        // materializes it: each partition keeps the tops below its own limit
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            for (String t : tables()) {
                createBaseTable(t, "");
                execute("INSERT INTO " + t + " (v, ts) VALUES " + rowsNoValue(1, 8) + ", "
                        + "(1, '2024-01-02T00:00:00.000000Z'), (2, '2024-01-02T01:00:00.000000Z')");
                addColumns(t);
                final StringBuilder sql = new StringBuilder("INSERT INTO " + t + " (v, ts, c1, c2, c3) VALUES ");
                for (int v = 3; v <= 8; v++) {
                    if (v > 3) {
                        sql.append(", ");
                    }
                    final String literal = literalOf(v);
                    sql.append('(').append(v).append(", '").append(tsOf(v).replace(O3_DAY, "2024-01-02")).append("', ")
                            .append(literal).append(", ").append(literal).append(", ").append(literal).append(')');
                }
                execute(sql.toString());
                execute("INSERT INTO " + t + " (v, ts) VALUES (100, '2024-01-03T00:00:00.000000Z')");
            }
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-02'");
            drainWalQueue();
            for (String t : tables()) {
                execute("INSERT INTO " + t + " (v, ts, c1, c2, c3) VALUES "
                        + "(50, '2024-01-01T23:00:00.000000Z', " + o3Literal() + ", " + o3Literal() + ", " + o3Literal() + "), "
                        + "(50, '2024-01-02T00:30:00.000000Z', " + o3Literal() + ", " + o3Literal() + ", " + o3Literal() + ")");
            }
            drainWalQueue();
            final List<Row> expectedDay2 = topRows(8, 2, -1, null, false);
            expectedDay2.add(1, new Row(50, "2024-01-02T00:30:00.000000Z", Kind.O3));
            assertReaders(
                    true,
                    new String[]{"2024-01-01", "2024-01-02"},
                    List.of(topRows(8, 8, -1, "2024-01-01T23:00:00.000000Z", true), expectedDay2)
            );
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

    // the columns of every type are added after the conversion of rowCount rows (row groups of 4); an
    // append rewrites the file with a null chunk per column and row group. Old rows read NULL, the O3
    // row its values, before and after a lazy change of the no-sentinel column of the tested type and
    // of INT, VARCHAR, DOUBLE and SYMBOL columns
    private void assertAbsentColumnsOfEveryTypeReadNull(int rowCount) throws Exception {
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createEveryTypeTables(rowCount);
            insertEveryTypeO3("2024-01-01T23:00:00.000000Z");
            assertSqlCursors("tw ORDER BY ts", "pt ORDER BY ts");
            assertEveryTypeNullCounts(rowCount, 1);

            final String noSentinelColumn = everyTypeColumnOf(type);
            final String[] lazyColumns = {noSentinelColumn, "cint", "cvarchar", "cdouble", "csym"};
            final String[] lazyTargets = {lazyTarget, "VARCHAR", "INT", "INT", "VARCHAR"};
            for (String t : tables()) {
                for (int i = 0; i < lazyColumns.length; i++) {
                    execute("ALTER TABLE " + t + " ALTER COLUMN " + lazyColumns[i] + " TYPE " + lazyTargets[i]);
                }
            }
            drainWalQueue();
            assertSqlCursors("tw ORDER BY ts", "pt ORDER BY ts");
            final StringSink expected = new StringSink();
            expected.put("v\t").put(String.join("\t", lazyColumns)).put('\n');
            for (int v = 1; v <= rowCount; v++) {
                expected.put(v).put('\t').put(renderAs(null, lazyTarget)).put("\t\tnull\tnull\t\n");
            }
            expected.put("50\t").put(renderAs(o3Text(), lazyTarget)).put("\t7\t42\t3\tsym\n");
            assertQuery("SELECT v, " + String.join(", ", lazyColumns) + " FROM pt WHERE ts IN '" + O3_DAY + "' ORDER BY ts")
                    .noLeakCheck()
                    .returns(expected.toString());
            for (String column : lazyColumns) {
                assertNullCounts(column, rowCount, 1);
            }
        });
    }

    private void assertC2NullCount(String day, long expectedCount) throws Exception {
        final String nullCount = "count\n" + expectedCount + "\n";
        for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
            sqlExecutionContext.setJitMode(jitMode);
            assertQuery("SELECT count() FROM pt WHERE ts IN '" + day + "' AND c2 IS NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(nullCount);
        }
    }

    private void assertDayRows(
            String[] days,
            List<List<Row>> expectedByDay,
            boolean isC1Altered,
            boolean isC2Altered,
            boolean isC3Altered
    ) throws Exception {
        for (int i = 0, n = expectedByDay.size(); i < n; i++) {
            assertQuery("SELECT v, c1, c2, c3 FROM pt WHERE ts IN '" + days[i] + "' ORDER BY ts")
                    .noLeakCheck()
                    .returns(render(expectedByDay.get(i), isC1Altered, isC2Altered, isC3Altered));
        }
    }

    private void assertEveryTypeNullCounts(long nullCount, long notNullCount) throws Exception {
        for (String[] column : EVERY_TYPE_COLUMNS) {
            if (Boolean.parseBoolean(column[3])) {
                assertNullCounts(column[0], nullCount, notNullCount);
            }
        }
    }

    // IS NULL and IS NOT NULL on a column of the parquet partition of pt
    private void assertNullCounts(String column, long nullCount, long notNullCount) throws Exception {
        assertQuery("SELECT count() FROM pt WHERE ts IN '" + O3_DAY + "' AND " + column + " IS NULL")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("count\n" + nullCount + "\n");
        assertQuery("SELECT count() FROM pt WHERE ts IN '" + O3_DAY + "' AND " + column + " IS NOT NULL")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("count\n" + notNullCount + "\n");
    }

    // c2 lazily to the lazy target, read through the _pm decoder while the partition is parquet, then
    // c1 to SYMBOL (converts the partition to native), then (pt) CONVERT TO NATIVE and c3 to SYMBOL;
    // pt must match tw and the expected rows after each step
    private void assertReaders(List<Row> expected) throws Exception {
        assertReaders(expected, true);
    }

    private void assertReaders(List<Row> expected, boolean isAltering) throws Exception {
        assertReaders(expected, isAltering, O3_DAY);
    }

    private void assertReaders(List<Row> expected, boolean isAltering, String... parquetDays) throws Exception {
        assertReaders(isAltering, parquetDays, List.of(expected));
    }

    // checks the rows of parquetDays[i] against expectedByDay[i], for the days that have expected rows
    private void assertReaders(boolean isAltering, String[] parquetDays, List<List<Row>> expectedByDay) throws Exception {
        assertSqlCursors("tw ORDER BY ts", "pt ORDER BY ts");
        assertDayRows(parquetDays, expectedByDay, false, false, false);
        if (!isAltering) {
            return;
        }
        for (String t : tables()) {
            execute("ALTER TABLE " + t + " ALTER COLUMN c2 TYPE " + lazyTarget);
        }
        drainWalQueue();
        assertSqlCursors("tw ORDER BY ts", "pt ORDER BY ts");
        assertDayRows(parquetDays, expectedByDay, false, true, false);
        // the filter on the lazily changed column, JIT on and off
        for (int i = 0, n = expectedByDay.size(); i < n; i++) {
            assertC2NullCount(parquetDays[i], countTop(expectedByDay.get(i)));
        }
        for (String t : tables()) {
            execute("ALTER TABLE " + t + " ALTER COLUMN c1 TYPE SYMBOL");
        }
        drainWalQueue();
        assertSqlCursors("tw ORDER BY ts", "pt ORDER BY ts");
        assertDayRows(parquetDays, expectedByDay, true, true, false);
        // the same filter once ALTER c1 TYPE SYMBOL has turned the partition native
        for (int i = 0, n = expectedByDay.size(); i < n; i++) {
            assertC2NullCount(parquetDays[i], countTop(expectedByDay.get(i)));
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
        assertDayRows(parquetDays, expectedByDay, true, true, true);
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

    private void createAbsentColumnTables(String extraColumn) throws Exception {
        createAbsentColumnTables(extraColumn, "");
    }

    // rows 1..8 of 2024-01-01 converted to parquet before c1..c3 exist: the columns are absent from the
    // parquet file; extraColumn (empty: none) is an INT column present in the file
    private void createAbsentColumnTables(String extraColumn, String dedup) throws Exception {
        for (String t : tables()) {
            createBaseTable(t, dedup);
            if (!extraColumn.isEmpty()) {
                execute("ALTER TABLE " + t + " ADD COLUMN " + extraColumn + " INT");
            }
            execute("INSERT INTO " + t + " (v, ts) VALUES " + rowsNoValue(1, 8));
            execute("INSERT INTO " + t + " (v, ts) VALUES (99, '2024-01-02T00:00:00.000000Z')");
        }
        drainWalQueue();
        convertToParquet();
        for (String t : tables()) {
            addColumns(t);
        }
        drainWalQueue();
    }

    private void createBaseTable(String table, String dedup) throws Exception {
        execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY "
                + (isWal ? "WAL" : "BYPASS WAL") + dedup);
    }

    // rows 1..rowCount of 2024-01-01 converted to parquet, then the columns of every type added
    private void createEveryTypeTables(int rowCount) throws Exception {
        for (String t : tables()) {
            createBaseTable(t, "");
            execute("INSERT INTO " + t + " (v, ts) VALUES " + rowsNoValue(1, rowCount));
            execute("INSERT INTO " + t + " (v, ts) VALUES (99, '2024-01-02T00:00:00.000000Z')");
        }
        drainWalQueue();
        convertToParquet();
        for (String t : tables()) {
            for (String[] column : EVERY_TYPE_COLUMNS) {
                execute("ALTER TABLE " + t + " ADD COLUMN " + column[0] + " " + column[1]);
            }
        }
        drainWalQueue();
    }

    private void createShapeTables(String dedup) throws Exception {
        createShapeTables(dedup, 2);
    }

    private void createShapeTables(String dedup, int top) throws Exception {
        createShapeTables(dedup, top, 8, -1);
    }

    // rows 1..rowCount, the columns added after `top` rows, 2024-01-01 converted to parquet; row
    // tiedRow (-1: none, else past the top) takes the timestamp of the row before it
    private void createShapeTables(String dedup, int top, int rowCount, int tiedRow) throws Exception {
        for (String t : tables()) {
            createBaseTable(t, dedup);
            execute("INSERT INTO " + t + " (v, ts) VALUES " + rowsNoValue(1, top));
            addColumns(t);
            final StringBuilder sql = new StringBuilder("INSERT INTO " + t + " (v, ts, c1, c2, c3, ci, cv, cg, cd) VALUES ");
            for (int v = top + 1; v <= rowCount; v++) {
                if (v > top + 1) {
                    sql.append(", ");
                }
                final String literal = literalOf(v);
                sql.append('(').append(v).append(", '").append(tsOf(v == tiedRow ? v - 1 : v)).append("', ")
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

    private String everyTypeColumnOf(String columnType) {
        for (String[] column : EVERY_TYPE_COLUMNS) {
            if (column[1].equals(columnType)) {
                return column[0];
            }
        }
        throw new AssertionError("no column of type " + columnType);
    }

    private void insertEveryTypeO3(String ts) throws Exception {
        final StringBuilder columns = new StringBuilder("v, ts");
        final StringBuilder values = new StringBuilder("50, '").append(ts).append('\'');
        for (String[] column : EVERY_TYPE_COLUMNS) {
            columns.append(", ").append(column[0]);
            values.append(", ").append(column[2]);
        }
        for (String t : tables()) {
            execute("INSERT INTO " + t + " (" + columns + ") VALUES (" + values + ")");
        }
        drainWalQueue();
    }

    private void insertO3(String ts) throws Exception {
        for (String t : tables()) {
            execute("INSERT INTO " + t + " (v, ts, c1, c2, c3, ci, cv) VALUES (50, '" + ts + "', "
                    + o3Literal() + ", " + o3Literal() + ", " + o3Literal() + ", 50, '50')");
        }
        drainWalQueue();
    }

    // inserts the rows, each from o3Row(), into pt and tw in one commit
    private void insertO3Rows(String... rows) throws Exception {
        for (String t : tables()) {
            execute("INSERT INTO " + t + " (v, ts, c1, c2, c3) VALUES " + String.join(", ", rows));
        }
        drainWalQueue();
    }

    // SQL literal of the value of row v (3..8, repeating from row 9), the same in every case: 0/false right after the top,
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

    // the VALUES tuple of a row with v at ts and the O3 value in c1..c3, see insertO3Rows()
    private String o3Row(int v, String ts) {
        return "(" + v + ", '" + ts + "', " + o3Literal() + ", " + o3Literal() + ", " + o3Literal() + ")";
    }

    private String o3Text() {
        return switch (type) {
            case "BOOLEAN" -> "true";
            case "BYTE" -> "9";
            case "SHORT" -> "999";
            default -> "q";
        };
    }

    // renders the expected rows: c1, c2, c3 hold the tested type, or after its ALTER c2 the lazy target,
    // c1 SYMBOL and c3 SYMBOL
    private String render(List<Row> rows, boolean isC1Altered, boolean isC2Altered, boolean isC3Altered) {
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
            sink.put(renderAs(text, isC1Altered ? "SYMBOL" : type)).put('\t');
            sink.put(renderAs(text, isC2Altered ? lazyTarget : type)).put('\t');
            sink.put(renderAs(text, isC3Altered ? "SYMBOL" : type)).put('\n');
        }
        return sink.toString();
    }

    private String renderAs(String text, String columnType) {
        if (text == null) {
            // a NULL of a type without a sentinel cannot exist in a column of that type: before the ALTERs
            // the column reads its stored value, which the top rows do not have, see topRows()
            return switch (columnType) {
                case "INT", "LONG", "DOUBLE" -> "null";
                case "BOOLEAN" -> "false";
                case "BYTE", "SHORT" -> "0";
                default -> "";
            };
        }
        if ("CHAR".equals(type) && text.isEmpty()) {
            // CHAR 0 converts to a NULL text
            return "";
        }
        if ("BOOLEAN".equals(type) && "INT".equals(columnType)) {
            return "true".equals(text) ? "1" : "0";
        }
        if ("DOUBLE".equals(columnType)) {
            return Double.toString(Double.parseDouble(text));
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

    // text of the value of row v (3..8, repeating from row 9)
    private String textOf(int v) {
        final int i = (v - 3) % 6;
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
