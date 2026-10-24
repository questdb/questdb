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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
    // the columns of the rows that rowOf() renders
    private static final String UPSERT_COLUMNS = "v, ts, c1, c2, c3, ci, cv, cg, cd";
    private final boolean isWal;
    private final String lazyTarget;
    private final String type;

    public O3ParquetNoSentinelColumnTopTest(WalMode walMode, String type, String lazyTarget) {
        this.isWal = walMode == WalMode.WITH_WAL;
        this.type = type;
        this.lazyTarget = lazyTarget;
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // The parquet rule follows the column tops the O3 merge of O3OpenColumnJob keeps, the merge that
        // production runs by default. Tests turn merge-append on, and its commits into the composite
        // partitions of the WAL twin tw read 0 in BOOLEAN, BYTE and SHORT rows where that merge keeps a
        // top, which reads NULL after a type change. Ask for the default merge.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, false);
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
    public void testDedupUpsertChangedCoalescedTieMaterializesTop() throws Exception {
        // KEYS(ts, v), top 4, row 5 shares the timestamp of row 4 (03:00) across the boundary of row groups 0 and
        // 1: the change of row 5 merges both row groups as one run, which forces a rewrite. The merge takes over
        // what the identity check decoded of the run, each column once, and materializes the top, as native does
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables("", 4, 8, 5);
            for (String t : tables()) {
                execute("ALTER TABLE " + t + " DEDUP ENABLE UPSERT KEYS(ts, v)");
            }
            drainWalQueue();
            assertMergeDecodedColumns(9, rowOf(5, tsOf(4), o3Literal(), "5", "'5'", "#s", "5.5m"));
            final List<Row> expected = topRows(8, 4, -1, null, false);
            expected.set(4, new Row(5, tsOf(4), Kind.O3));
            assertReaders(expected);
        });
    }

    @Test
    public void testDedupUpsertChangedColumnMaterializesTop() throws Exception {
        // top 6, row 7 resent with one column changed and a row at 23:00: native compares the changed column,
        // finds a difference and merges with the prefix of rows 1..6, which ends with the top: the top rows read
        // 0/false. The cases cover the comparer of INT v, INT, VARCHAR, GEOHASH(1c), DECIMAL(2,1) and the tested
        // type. Row 7 with v = 13 has the same values as row 7, see literalOf(). The tests that materialize a top
        // use a single row group of 8 rows: the merge writes every row of the top
        Assume.assumeTrue(isWal);
        setInPlaceProperties(8);
        final String[][] cases = {
                // the changed row 7, the row it reads as
                {rowOf(13, tsOf(7), literalOf(7), "7", "'7'", "#s", "7.5m"), "13", "VALUE"},
                {rowOf(7, tsOf(7), literalOf(7), "70", "'7'", "#s", "7.5m"), "7", "VALUE"},
                {rowOf(7, tsOf(7), literalOf(7), "7", "'70'", "#s", "7.5m"), "7", "VALUE"},
                {rowOf(7, tsOf(7), literalOf(7), "7", "'7'", "#u", "7.5m"), "7", "VALUE"},
                {rowOf(7, tsOf(7), literalOf(7), "7", "'7'", "#s", "8.5m"), "7", "VALUE"},
                {rowOf(7, tsOf(7), o3Literal(), "7", "'7'", "#s", "7.5m"), "7", "O3"},
        };
        assertMemoryLeak(() -> {
            for (String[] c : cases) {
                createShapeTables(" DEDUP UPSERT KEYS(ts)", 6);
                insertUpsertRows(c[0], newRow("2024-01-01T23:00:00.000000Z"));
                final List<Row> expected = topRows(8, 6, -1, "2024-01-01T23:00:00.000000Z", false);
                expected.set(6, new Row(Integer.parseInt(c[1]), tsOf(7), Kind.valueOf(c[2])));
                assertReaders(expected);
                dropTables();
            }
        });
    }

    @Test
    public void testDedupUpsertChangedEveryTypeInSecondRowGroupMatchesNative() throws Exception {
        // rows 1..8 hold a value in a column of every type, in row groups of 4, updated in place. Row 6 in row
        // group 1 changes: with KEYS(ts) every other column of the row turns NULL, the merge takes over every
        // column that the identity check decoded. With KEYS(ts, csym, cvarchar) every column but the keys turns
        // NULL, the same, or the VARCHAR key changes, which makes the row new: the merge takes over the keys that
        // the check decoded and decodes the other columns. The test does not depend on the parameters, it runs once
        Assume.assumeTrue(isWal && "BYTE".equals(type) && "INT".equals(lazyTarget));
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            assertEveryTypeRow6Upsert("ts", false);
            assertEveryTypeRow6Upsert("ts, csym, cvarchar", false);
            assertEveryTypeRow6Upsert("ts, csym, cvarchar", true);
        });
    }

    @Test
    public void testDedupUpsertChangedFailureThenRetryKeepsTop() throws Exception {
        // top 2, row 7 in row group 1 changes: the identity check leaves what it decoded of row group 1 to the
        // merge, then the commit fails before the merge: the in-place update or the rewrite (the parquet file lacks
        // the column added after the conversion) fails to open data.parquet, or its first write fails, the copy of
        // row group 0 in a rewrite. Nothing that the check decoded leaks. Once the table resumes, the commit applies
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        final AtomicBoolean isFailing = new AtomicBoolean();
        final AtomicBoolean isWriteFailure = new AtomicBoolean();
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (isFailing.get() && Utf8s.endsWithAscii(name, TableUtils.PARQUET_PARTITION_NAME)
                        && Utf8s.containsAscii(name, "pt~")) {
                    if (!isWriteFailure.get()) {
                        return -1;
                    }
                    // a read-only descriptor: the updater takes it, its first write fails
                    super.close(super.openRW(name, opts));
                    return super.openRONoCache(name);
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            for (boolean isRewrite : new boolean[]{false, true}) {
                for (boolean isWrite : new boolean[]{false, true}) {
                    createShapeTables(" DEDUP UPSERT KEYS(ts)");
                    if (isRewrite) {
                        for (String t : tables()) {
                            execute("ALTER TABLE " + t + " ADD COLUMN extra INT");
                        }
                        drainWalQueue();
                    }
                    final TableToken tableToken = engine.verifyTableName("pt");
                    final Set<String> partitionDirs = listPartitionDirs(tableToken);
                    isWriteFailure.set(isWrite);
                    isFailing.set(true);
                    insertUpsertRows(newRow(tsOf(7)));
                    Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
                    // a failed rewrite removes the directory it created
                    Assert.assertTrue(partitionDirs.containsAll(listPartitionDirs(tableToken)));
                    isFailing.set(false);
                    execute("ALTER TABLE pt RESUME WAL");
                    drainWalQueue();
                    Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
                    assertReaders(topRows(8, 2, 7, null, true));
                    dropTables();
                }
            }
        });
    }

    @Test
    public void testDedupUpsertChangedInLastRowGroupThenAppendKeepsTop() throws Exception {
        // 10 rows in row groups of 4, 4 and 2, top 2: one merge of the last row group takes the change of row 10
        // and the row at 23:00, past the partition max. The merge takes over what the identity check decoded, each
        // column once
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)", 2, 10, -1);
            assertMergeDecodedColumns(9, newRow(tsOf(10)), newRow("2024-01-01T23:00:00.000000Z"));
            assertReaders(topRows(10, 2, 10, "2024-01-01T23:00:00.000000Z", true));
        });
    }

    @Test
    public void testDedupUpsertChangedInSecondRowGroupAbsentColumnRewriteKeepsTop() throws Exception {
        // top 2, the parquet file lacks the column added after the conversion: the change of row 7 rewrites the
        // partition, which copies row group 0 with a null chunk for the column, then merges row group 1 from what
        // the identity check decoded, each column once
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)");
            for (String t : tables()) {
                execute("ALTER TABLE " + t + " ADD COLUMN extra INT");
            }
            drainWalQueue();
            assertMergeDecodedColumns(9, newRow(tsOf(7)));
            assertReaders(topRows(8, 2, 7, null, true));
        });
    }

    @Test
    public void testDedupUpsertChangedInSecondRowGroupMaterializedRewriteKeepsTop() throws Exception {
        // top 2, ci changed type: the change of row 7 rewrites the partition, which copies row group 0 through the
        // materializer. The materializer decodes into the buffers that hold the columns that the identity check
        // decoded past the keys, so the merge of row group 1 decodes it again
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)");
            changeControlColumnType();
            assertMergeDecodedColumns(hasNoSentinelType() ? 18 : 9, newRow(tsOf(7)));
            assertReaders(topRows(8, 2, 7, null, true));
        });
    }

    @Test
    public void testDedupUpsertChangedInSecondRowGroupKeepsTop() throws Exception {
        // top 2, row 7 in row group 1 changes: the in-place merge of row group 1 takes over what the identity check
        // decoded, each column once
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)");
            assertMergeDecodedColumns(9, newRow(tsOf(7)));
            assertReaders(topRows(8, 2, 7, null, true));
        });
    }

    @Test
    public void testDedupUpsertChangedInTwoPartitionsKeepsTop() throws Exception {
        // 2024-01-01 (rows 1..8, top 2) and 2024-01-02 (row 99) are parquet partitions. One commit changes row 7
        // and row 99: the merge of each partition takes over what its own identity check decoded, each column once
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)");
            for (String t : tables()) {
                execute("INSERT INTO " + t + " (v, ts) VALUES (100, '2024-01-03T00:00:00.000000Z')");
            }
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-02'");
            drainWalQueue();
            assertMergeDecodedColumns(18, newRow(tsOf(7)), newRow("2024-01-02T00:00:00.000000Z"));
            assertReaders(
                    true,
                    new String[]{O3_DAY, "2024-01-02"},
                    List.of(topRows(8, 2, 7, null, true), List.of(new Row(50, "2024-01-02T00:00:00.000000Z", Kind.O3)))
            );
        });
    }

    @Test
    public void testDedupUpsertCommitDuplicatesLastChangedMaterializesTop() throws Exception {
        // KEYS(ts): the commit holds row 4 as stored, then row 4 with v = 44. The dedup of the commit keeps
        // the last copy, which changes row 4: native merges with the prefix of rows 1..3 and materializes
        // the top (6)
        Assume.assumeTrue(isWal);
        setInPlaceProperties(8);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)", 6);
            insertUpsertRows(
                    storedRow(4, 6),
                    rowOf(44, tsOf(4), "NULL", "NULL", "NULL", "NULL", "NULL"),
                    newRow("2024-01-01T23:00:00.000000Z")
            );
            final List<Row> expected = topRows(8, 6, -1, "2024-01-01T23:00:00.000000Z", false);
            expected.set(3, new Row(44, tsOf(4), Kind.ZERO));
            assertReaders(expected);
        });
    }

    @Test
    public void testDedupUpsertCommitDuplicatesLastIdenticalKeepsTop() throws Exception {
        // KEYS(ts): the commit holds row 4 with v = 44, then row 4 as stored. The dedup of the commit keeps
        // the last copy, which replays row 4 unchanged: native appends the row at 23:00 and keeps the top
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)", 6);
            insertUpsertRows(
                    rowOf(44, tsOf(4), "NULL", "NULL", "NULL", "NULL", "NULL"),
                    storedRow(4, 6),
                    newRow("2024-01-01T23:00:00.000000Z")
            );
            assertReaders(topRows(8, 6, -1, "2024-01-01T23:00:00.000000Z", true));
        });
    }

    @Test
    public void testDedupUpsertIdenticalAtMaxThenAppendKeepsTop() throws Exception {
        // top 7, row 8 (07:00) sits on the partition max: its replay still counts among the O3 rows at or
        // below the max
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)", 7);
            insertUpsertRows(storedRow(8, 7), newRow("2024-01-01T23:00:00.000000Z"));
            assertReaders(topRows(8, 7, -1, "2024-01-01T23:00:00.000000Z", true));
        });
    }

    @Test
    public void testDedupUpsertIdenticalCoalescedTieThenAppendKeepsTop() throws Exception {
        // KEYS(ts, v), top 4, row 5 shares the timestamp of row 4 (03:00) across the boundary of row groups
        // 0 and 1: the replay of row 5 merges both row groups as one run, which forces a rewrite. The dedup
        // turns on after the rows exist: the row 5 insert would otherwise merge at 03:00 and materialize the top
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables("", 4, 8, 5);
            for (String t : tables()) {
                execute("ALTER TABLE " + t + " DEDUP ENABLE UPSERT KEYS(ts, v)");
            }
            drainWalQueue();
            insertUpsertRows(
                    rowOf(5, tsOf(4), literalOf(5), "5", "'5'", "#s", "5.5m"),
                    newRow("2024-01-01T23:00:00.000000Z")
            );
            assertReaders(topRows(8, 4, -1, "2024-01-01T23:00:00.000000Z", true));
        });
    }

    @Test
    public void testDedupUpsertIdenticalEveryTypeThenAppendKeepsTop() throws Exception {
        // rows 1..8 hold a value in a column of every type, BYTE w is added after the conversion (top 8). The
        // replay of row 6 with every value as stored is identical: native appends the row at 23:00 and keeps
        // the top of w. Then, per type, row 6 resent with NULL in place of the value of that column differs:
        // native merges and materializes the top of w. One row group holds the 8 rows, so that the merge writes
        // every row of the top. The test does not depend on the parameters, it runs once
        Assume.assumeTrue(isWal && "BYTE".equals(type) && "INT".equals(lazyTarget));
        setInPlaceProperties(8);
        assertMemoryLeak(() -> {
            createEveryTypeValueTables();
            insertEveryTypeRow6(null);
            assertByteColumnW(8);
            for (String[] column : EVERY_TYPE_COLUMNS) {
                dropTables();
                createEveryTypeValueTables();
                insertEveryTypeRow6(column[0]);
                assertByteColumnW(0);
            }
        });
    }

    @Test
    public void testDedupUpsertIdenticalInLastRowGroupThenAppendKeepsTop() throws Exception {
        // 10 rows in row groups of 4, 4 and 2, top 9: one MERGE of the last row group takes the replay of
        // row 10 and the row at 23:00, the compare covers the rows of the row group only
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)", 9, 10, -1);
            insertUpsertRows(storedRow(10, 9), newRow("2024-01-01T23:00:00.000000Z"));
            assertReaders(topRows(10, 9, -1, "2024-01-01T23:00:00.000000Z", true));
        });
    }

    @Test
    public void testDedupUpsertIdenticalInTwoRowGroupsThenAppendKeepsTop() throws Exception {
        // the replays of row 2 (row group 0) and row 6 (row group 1): two merges, both identical
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)", 6);
            insertUpsertRows(storedRow(2, 6), storedRow(6, 6), newRow("2024-01-01T23:00:00.000000Z"));
            assertReaders(topRows(8, 6, -1, "2024-01-01T23:00:00.000000Z", true));
        });
    }

    @Test
    public void testDedupUpsertIdenticalNullsEveryTypeThenAppendKeepsTop() throws Exception {
        // the columns of every type are absent from the parquet file (top 8): the replay of row 4 holds NULL
        // in each, as the null buffers of the absent columns do. Native appends the row at 23:00, every column
        // reads NULL in rows 1..8, also the types without NULL after a lazy change to INT. The test does not
        // depend on the parameters, it runs once
        Assume.assumeTrue(isWal && "BYTE".equals(type) && "INT".equals(lazyTarget));
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createEveryTypeTables(8, " DEDUP UPSERT KEYS(ts)");
            final StringBuilder columns = new StringBuilder("v, ts");
            final StringBuilder values = new StringBuilder("50, '2024-01-01T23:00:00.000000Z'");
            for (String[] column : EVERY_TYPE_COLUMNS) {
                columns.append(", ").append(column[0]);
                values.append(", ").append(column[2]);
            }
            for (String t : tables()) {
                execute("INSERT INTO " + t + " (" + columns + ") VALUES (4, '" + tsOf(4) + "'"
                        + ", NULL".repeat(EVERY_TYPE_COLUMNS.length) + "), (" + values + ")");
            }
            drainWalQueue();
            assertSqlCursors("tw ORDER BY ts", "pt ORDER BY ts");
            assertEveryTypeNullCounts(8, 1);
            for (String t : tables()) {
                for (String column : new String[]{"cbool", "cbyte", "cshort"}) {
                    execute("ALTER TABLE " + t + " ALTER COLUMN " + column + " TYPE INT");
                }
            }
            drainWalQueue();
            assertSqlCursors("tw ORDER BY ts", "pt ORDER BY ts");
            for (String column : new String[]{"cbool", "cbyte", "cshort"}) {
                assertNullCounts(column, 8, 1);
            }
        });
    }

    @Test
    public void testDedupUpsertIdenticalOnlyLeavesPartitionUnchanged() throws Exception {
        // the replay of row 4 into two row groups (update mode): native leaves the partition as it is
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)", 6);
            assertUpsertLeavesPartitionUnchanged(storedRow(4, 6));
            assertReaders(topRows(8, 6, -1, null, true));
        });
    }

    @Test
    public void testDedupUpsertIdenticalOnlySchemaRewriteLeavesPartitionUnchanged() throws Exception {
        // ci changed type, so a commit that writes rows rewrites the partition: the replay of row 4 writes none
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)", 6);
            changeControlColumnType();
            assertUpsertLeavesPartitionUnchanged(storedRow(4, 6));
            assertReaders(topRows(8, 6, -1, null, true));
        });
    }

    @Test
    public void testDedupUpsertIdenticalOnlySingleRowGroupLeavesPartitionUnchanged() throws Exception {
        // one row group of 8 rows: a commit that writes rows rewrites the file, the replay of row 4 writes none
        Assume.assumeTrue(isWal);
        setInPlaceProperties(8);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)", 6);
            assertUpsertLeavesPartitionUnchanged(storedRow(4, 6));
            assertReaders(topRows(8, 6, -1, null, true));
        });
    }

    @Test
    public void testDedupUpsertIdenticalPastTopThenAppendKeepsTop() throws Exception {
        // the replay of row 7, the first row after the top (6): a merge would keep rows 1..6 in place, which
        // ends the top with the prefix; the append keeps it
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)", 6);
            insertUpsertRows(storedRow(7, 6), newRow("2024-01-01T23:00:00.000000Z"));
            assertReaders(topRows(8, 6, -1, "2024-01-01T23:00:00.000000Z", true));
        });
    }

    @Test
    public void testDedupUpsertIdenticalReplayTwiceKeepsTop() throws Exception {
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)", 6);
            assertUpsertLeavesPartitionUnchanged(storedRow(4, 6));
            assertUpsertLeavesPartitionUnchanged(storedRow(4, 6), storedRow(7, 6));
            assertReaders(topRows(8, 6, -1, null, true));
        });
    }

    @Test
    public void testDedupUpsertIdenticalThenAppendAbsentColumnKeepsTop() throws Exception {
        // c1..c3 are absent from the parquet file (top 8): the replay of row 4 holds NULL in them, the row at
        // 23:00 rewrites the file with the new schema
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createAbsentColumnTables("", " DEDUP UPSERT KEYS(ts)");
            insertUpsertRows(storedRow(4, 8), newRow("2024-01-01T23:00:00.000000Z"));
            assertReaders(topRows(8, 8, -1, "2024-01-01T23:00:00.000000Z", true));
        });
    }

    @Test
    public void testDedupUpsertIdenticalThenAppendIndexedSymbol() throws Exception {
        // indexed SYMBOL s holds 'a' and 'b' in rows 7, 8: the replay of row 7 and the row at 23:00 with s = 'x'
        // append, the index finds the rows of each value, JIT on and off
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            for (String t : tables()) {
                createBaseTable(t, " DEDUP UPSERT KEYS(ts)");
                execute("INSERT INTO " + t + " (v, ts) VALUES " + rowsNoValue(1, 6));
                addColumns(t);
                execute("ALTER TABLE " + t + " ADD COLUMN s SYMBOL INDEX");
                execute("INSERT INTO " + t + " (v, ts, c1, c2, c3, s) VALUES "
                        + "(7, '" + tsOf(7) + "', " + literalOf(7) + ", " + literalOf(7) + ", " + literalOf(7) + ", 'a'), "
                        + "(8, '" + tsOf(8) + "', " + literalOf(8) + ", " + literalOf(8) + ", " + literalOf(8) + ", 'b')");
                execute("INSERT INTO " + t + " (v, ts) VALUES (99, '2024-01-02T00:00:00.000000Z')");
            }
            drainWalQueue();
            convertToParquet();
            for (String t : tables()) {
                execute("INSERT INTO " + t + " (v, ts, c1, c2, c3, s) VALUES "
                        + "(7, '" + tsOf(7) + "', " + literalOf(7) + ", " + literalOf(7) + ", " + literalOf(7) + ", 'a'), "
                        + "(50, '2024-01-01T23:00:00.000000Z', " + o3Literal() + ", " + o3Literal() + ", " + o3Literal() + ", 'x')");
            }
            drainWalQueue();
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(jitMode);
                for (String[] symbolRows : new String[][]{{"a", "7"}, {"b", "8"}, {"x", "50"}}) {
                    assertSqlCursors("tw WHERE s = '" + symbolRows[0] + "'", "pt WHERE s = '" + symbolRows[0] + "'");
                    assertQuery("SELECT v FROM pt WHERE s = '" + symbolRows[0] + "'")
                            .noLeakCheck()
                            .returns("v\n" + symbolRows[1] + "\n");
                }
            }
            assertReaders(topRows(8, 6, -1, "2024-01-01T23:00:00.000000Z", true));
        });
    }

    @Test
    public void testDedupUpsertIdenticalThenAppendKeepsTop() throws Exception {
        // top 6, the replay of row 4 (03:00) and a row at 23:00 after the partition: native finds the replayed
        // row identical and turns the commit into an append, which keeps the top, and so does the parquet update
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)", 6);
            insertUpsertRows(storedRow(4, 6), newRow("2024-01-01T23:00:00.000000Z"));
            assertReaders(topRows(8, 6, -1, "2024-01-01T23:00:00.000000Z", true));
        });
    }

    @Test
    public void testDedupUpsertIdenticalThenAppendRewriteKeepsTop() throws Exception {
        // ci changed type: the rewrite opens at the row group of the row at 23:00 and first copies row groups
        // 0 and 1 through the materializer, which keeps the top rows
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)", 6);
            changeControlColumnType();
            insertUpsertRows(storedRow(4, 6), newRow("2024-01-01T23:00:00.000000Z"));
            assertReaders(topRows(8, 6, -1, "2024-01-01T23:00:00.000000Z", true));
        });
    }

    @Test
    public void testDedupUpsertIdenticalThenAppendSurvivesReopen() throws Exception {
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)", 6);
            insertUpsertRows(storedRow(4, 6), newRow("2024-01-01T23:00:00.000000Z"));
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            insertO3("2024-01-01T23:30:00.000000Z");
            final List<Row> expected = topRows(8, 6, -1, "2024-01-01T23:00:00.000000Z", true);
            expected.add(new Row(50, "2024-01-01T23:30:00.000000Z", Kind.O3));
            assertReaders(expected);
        });
    }

    @Test
    public void testDedupUpsertIdenticalThenAppendTwoPartitionsKeepOwnTops() throws Exception {
        // 2024-01-01 and 2024-01-02 hold rows 1..8 with top 3 each. One commit replays row 2 of each:
        // 2024-01-01 then appends at 23:00 and keeps its top, 2024-01-02 also changes row 5, merges with the
        // prefix of row 1 and materializes its top, as native does
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            for (String t : tables()) {
                createBaseTable(t, " DEDUP UPSERT KEYS(ts)");
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
            insertUpsertRows(
                    storedRow(2, 3),
                    newRow("2024-01-01T23:00:00.000000Z"),
                    storedRow(2, 3).replace(O3_DAY, "2024-01-02"),
                    rowOf(5, "2024-01-02T04:00:00.000000Z", o3Literal(), "NULL", "NULL", "NULL", "NULL")
            );
            final List<Row> expectedDay2 = topRows(8, 3, -1, null, false);
            expectedDay2.set(4, new Row(5, "2024-01-02T04:00:00.000000Z", Kind.O3));
            assertReaders(
                    true,
                    new String[]{"2024-01-01", "2024-01-02"},
                    List.of(topRows(8, 3, -1, "2024-01-01T23:00:00.000000Z", true), expectedDay2)
            );
        });
    }

    @Test
    public void testDedupUpsertIdenticalThenChangedMaterializesTop() throws Exception {
        // the replay of row 2 passes the compare of row group 0, the change of row 7 fails the compare of row
        // group 1: native merges with the prefix of row 1
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)", 6);
            insertUpsertRows(storedRow(2, 6), newRow(tsOf(7)));
            assertReaders(topRows(8, 6, 7, null, false));
        });
    }

    @Test
    public void testDedupUpsertIdenticalThenGapRowMaterializesTop() throws Exception {
        // top 3, the new row at 03:30 goes into a row group of its own between row groups 0 and 1, a row at or
        // below the max that replaces nothing: native merges with the prefix of rows 1..3, which ends with the top
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)", 3);
            insertUpsertRows(storedRow(4, 3), newRow("2024-01-01T03:30:00.000000Z"));
            assertReaders(topRows(8, 3, -1, "2024-01-01T03:30:00.000000Z", false));
        });
    }

    @Test
    public void testDedupUpsertIdenticalThenNewRowJustAfterMaxKeepsTop() throws Exception {
        // the new row lands one microsecond after the partition max
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)", 6);
            insertUpsertRows(storedRow(4, 6), newRow("2024-01-01T07:00:00.000001Z"));
            assertReaders(topRows(8, 6, -1, "2024-01-01T07:00:00.000001Z", true));
        });
    }

    @Test
    public void testDedupUpsertIdenticalThenNewRowMaterializesTop() throws Exception {
        // the new row at 05:30 merges into row group 1: not every O3 row at or below the max is a duplicate,
        // native merges with the prefix of rows 1..3
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)", 6);
            insertUpsertRows(storedRow(4, 6), newRow("2024-01-01T05:30:00.000000Z"));
            assertReaders(topRows(8, 6, -1, "2024-01-01T05:30:00.000000Z", false));
        });
    }

    @Test
    public void testDedupUpsertIdenticalWithKeyColumnThenAppendKeepsTop() throws Exception {
        // KEYS(ts, v): the dedup compares the INT key, the identity check skips it
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts, v)", 6);
            insertUpsertRows(storedRow(4, 6), newRow("2024-01-01T23:00:00.000000Z"));
            assertReaders(topRows(8, 6, -1, "2024-01-01T23:00:00.000000Z", true));
        });
    }

    @Test
    public void testDedupUpsertIdentityCheckDecodedColumns() throws Exception {
        // the columns that the identity check of an UPSERT commit decodes, per commit, out of the 9 columns
        // of the parquet file. CHAR has no column without NULL in the table, the check does not run
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)", 6);
            final boolean isChecked = hasNoSentinelType();
            // a pure append: the check does not run
            assertIdentityCheckDecodedColumns(0, newRow("2024-01-01T23:00:00.000000Z"));
            // a new row in row group 1: the key stage decodes ts and finds no duplicate
            assertIdentityCheckDecodedColumns(isChecked ? 1 : 0, newRow("2024-01-01T05:30:00.000000Z"));
            // a new row in a row group of its own: the check stops before it decodes
            assertIdentityCheckDecodedColumns(0, newRow("2024-01-01T03:30:00.000000Z"));
            // a value update of row 7: the key stage passes, the value stage decodes the other 8 columns
            assertIdentityCheckDecodedColumns(
                    isChecked ? 9 : 0,
                    rowOf(7, tsOf(7), literalOf(7), "70", "'7'", "#s", "7.5m")
            );
            // the replay of row 2 in row group 0
            assertIdentityCheckDecodedColumns(isChecked ? 9 : 0, storedRow(2, 6));
        });
    }

    @Test
    public void testDedupUpsertMergeDecodedColumns() throws Exception {
        // the columns that the row group merges of an UPSERT commit decode, the identity check included, out of
        // the 9 columns of the parquet file: each column of each merged row group once. The merge of the first row
        // group takes over what the check decoded when the check stops there. After an identical row group, the
        // check stops at a later one, and the merge decodes both row groups again
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        final String changedRow7 = rowOf(7, tsOf(7), literalOf(7), "70", "'7'", "#s", "7.5m");
        final Object[][] cases = {
                // a change in row group 1
                {9L, new String[]{changedRow7}},
                // a new row in row group 1: the check decodes ts, the merge the other 8 columns
                {9L, new String[]{newRow("2024-01-01T05:30:00.000000Z")}},
                // a change in row group 1 and a row after the partition
                {9L, new String[]{changedRow7, newRow("2024-01-01T23:00:00.000000Z")}},
                // changes in row groups 0 and 1
                {18L, new String[]{rowOf(30, tsOf(3), "NULL", "NULL", "NULL", "NULL", "NULL"), changedRow7}},
                // the replay of row 2 in row group 0, then a change in row group 1
                {hasNoSentinelType() ? 36L : 18L, new String[]{storedRow(2, 6), changedRow7}},
                // the replay of row 2 in row group 0, then a new row in row group 1: the check decodes row group 0 and
                // the keys of row group 1
                {hasNoSentinelType() ? 28L : 18L, new String[]{storedRow(2, 6), newRow("2024-01-01T05:30:00.000000Z")}},
                // a new row in a row group of its own, which the check stops at before it decodes
                {0L, new String[]{newRow("2024-01-01T03:30:00.000000Z")}},
        };
        assertMemoryLeak(() -> {
            for (Object[] c : cases) {
                createShapeTables(" DEDUP UPSERT KEYS(ts)", 6);
                assertMergeDecodedColumns((long) c[0], (String[]) c[1]);
                dropTables();
            }
        });
    }

    @Test
    public void testDedupUpsertNanResentOverNullKeepsTop() throws Exception {
        // DOUBLE d and FLOAT f are NULL in row 7, the replay sends NaN, which is the same NULL: native appends
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            assertFloatReplay("NULL", "1.0", "NaN", "1.0", true);
            dropTables();
            assertFloatReplay("1.0", "NULL", "1.0", "NaN", true);
        });
    }

    @Test
    public void testDedupUpsertNegativeZeroResentAsZeroMaterializesTop() throws Exception {
        // DOUBLE d or FLOAT f holds -0.0 in row 7, the replay sends 0.0: native compares the bits, finds a
        // difference and merges with the prefix of rows 1..6
        Assume.assumeTrue(isWal);
        setInPlaceProperties(8);
        assertMemoryLeak(() -> {
            assertFloatReplay("-0.0", "1.0", "0.0", "1.0", false);
            dropTables();
            assertFloatReplay("1.0", "-0.0", "1.0", "0.0", false);
        });
    }

    @Test
    public void testDedupUpsertNewKeyAtStoredTimestampMaterializesTop() throws Exception {
        // KEYS(ts, v): the row at 03:00 has another v than row 4, so it is not a duplicate
        Assume.assumeTrue(isWal);
        setInPlaceProperties(8);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts, v)", 6);
            insertUpsertRows(newRow(tsOf(4)), newRow("2024-01-01T23:00:00.000000Z"));
            final List<Row> expected = topRows(8, 6, -1, tsOf(4), false);
            expected.add(new Row(50, "2024-01-01T23:00:00.000000Z", Kind.O3));
            assertReaders(expected);
        });
    }

    @Test
    public void testDedupUpsertNewRowInSecondRowGroupMaterializedRewriteKeepsTop() throws Exception {
        // top 2, ci changed type: the new row at 05:30 rewrites the partition, which copies row group 0 through the
        // materializer before the merge of row group 1. The merge takes over the keys that the identity check
        // decoded, which the materializer leaves alone, and decodes the other columns
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)");
            changeControlColumnType();
            assertMergeDecodedColumns(9, newRow("2024-01-01T05:30:00.000000Z"));
            assertReaders(topRows(8, 2, -1, "2024-01-01T05:30:00.000000Z", true));
        });
    }

    @Test
    public void testDedupUpsertNewRowInSecondRowGroupKeepsTop() throws Exception {
        // top 2, the new row at 05:30 merges into row group 1 in place: the merge takes over the keys that the
        // identity check decoded and decodes the other columns
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)");
            assertMergeDecodedColumns(9, newRow("2024-01-01T05:30:00.000000Z"));
            assertReaders(topRows(8, 2, -1, "2024-01-01T05:30:00.000000Z", true));
        });
    }

    @Test
    public void testDedupUpsertNonNullIntoTopRowMaterializesTop() throws Exception {
        // row 4 resent with a value in c1..c3, where its top reads NULL: native compares the value with NULL,
        // finds a difference and merges with the prefix of rows 1..3
        Assume.assumeTrue(isWal);
        setInPlaceProperties(8);
        assertMemoryLeak(() -> {
            createShapeTables(" DEDUP UPSERT KEYS(ts)", 6);
            insertUpsertRows(
                    rowOf(4, tsOf(4), o3Literal(), "NULL", "NULL", "NULL", "NULL"),
                    newRow("2024-01-01T23:00:00.000000Z")
            );
            final List<Row> expected = topRows(8, 6, -1, "2024-01-01T23:00:00.000000Z", false);
            expected.set(3, new Row(4, tsOf(4), Kind.O3));
            assertReaders(expected);
        });
    }

    @Test
    public void testDedupUpsertStoredDuplicatesChangedMaterializesTop() throws Exception {
        // rows 4 and 40 share 03:00, stored before the dedup is on. UPSERT maps both onto the resent row 40,
        // which changes row 4: native merges, both rows read v = 40 and the top (7) is materialized
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createStoredDuplicateTables(40);
            insertUpsertRows(
                    rowOf(40, tsOf(4), "NULL", "NULL", "NULL", "NULL", "NULL"),
                    newRow("2024-01-01T23:00:00.000000Z")
            );
            assertReaders(storedDuplicateRows(40, false));
        });
    }

    @Test
    public void testDedupUpsertStoredDuplicatesIdenticalKeepTop() throws Exception {
        // two copies of row 4 at 03:00, stored before the dedup is on, and the replay equals both: native
        // appends the row at 23:00 and keeps the top (7)
        Assume.assumeTrue(isWal);
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createStoredDuplicateTables(4);
            insertUpsertRows(storedRow(4, 6), newRow("2024-01-01T23:00:00.000000Z"));
            assertReaders(storedDuplicateRows(4, true));
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
    public void testNonDedupTieBeforeTopMaterializesTop() throws Exception {
        // rows 4..6 at 03:00 across row groups 0 and 1, top 4 inside row group 0; O3 rows at 02:00 and
        // 03:00: the prefix is rows 1..3, the top ends past it and materializes, as native. The row at
        // 03:00 goes after row 6, into row group 1
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables("", 4, 8, 5, 6);
            insertO3Rows(o3Row(50, "2024-01-01T02:00:00.000000Z"), o3Row(51, "2024-01-01T03:00:00.000000Z"));
            final List<Row> expected = topRows(8, 4, -1, null, false);
            expected.add(3, new Row(50, "2024-01-01T02:00:00.000000Z", Kind.O3));
            expected.add(7, new Row(51, "2024-01-01T03:00:00.000000Z", Kind.O3));
            assertReaders(expected);
        });
    }

    @Test
    public void testNonDedupTieInNextRowGroupKeepsTop() throws Exception {
        // rows 4..6 at 03:00 across row groups 0 and 1, top 5 ends inside row group 1; the O3 row at
        // 03:00 goes after row 6: rows 1..6 keep their position and the top stays, as native
        setInPlaceProperties(4);
        assertMemoryLeak(() -> {
            createShapeTables("", 5, 8, 5, 6);
            insertO3("2024-01-01T03:00:00.000000Z");
            final List<Row> expected = topRows(8, 5, -1, null, true);
            expected.add(6, new Row(50, "2024-01-01T03:00:00.000000Z", Kind.O3));
            assertReaders(expected);
        });
    }

    @Test
    public void testNonDedupTieInNextRowGroupRewriteKeepsTop() throws Exception {
        // the shape of testNonDedupTieInNextRowGroupKeepsTop, with unused-max-bytes -1 forcing a full
        // rewrite: the rewrite copies row group 0 and merges the O3 row at 03:00 into row group 1 after
        // row 6, so rows 1..5 keep their NULL top, as native
        setInPlaceProperties(4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, -1);
        assertMemoryLeak(() -> {
            createShapeTables("", 5, 8, 5, 6);
            insertO3("2024-01-01T03:00:00.000000Z");
            final List<Row> expected = topRows(8, 5, -1, null, true);
            expected.add(6, new Row(50, "2024-01-01T03:00:00.000000Z", Kind.O3));
            assertReaders(expected);
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

    private static long getIdentityCheckDecodedColumnCount() {
        try (TableWriter writer = getWriter("pt")) {
            return writer.getUpsertIdentityCheckDecodedColumnCount();
        }
    }

    private static long getMergeDecodedColumnCount() {
        try (TableWriter writer = getWriter("pt")) {
            return writer.getParquetMergeDecodedColumnCount();
        }
    }

    // names of the directories of the 2024-01-01 partition of the table, of every version that is on disk
    private static Set<String> listPartitionDirs(TableToken tableToken) {
        final String[] dirs = new File(root, tableToken.getDirName()).list((dir, name) -> name.startsWith(O3_DAY));
        Assert.assertNotNull(dirs);
        return new HashSet<>(Arrays.asList(dirs));
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

    // changes BYTE w of the tables made by createEveryTypeValueTables() lazily to INT, then asserts that pt
    // matches tw and that w reads NULL in nullCount rows of 2024-01-01
    private void assertByteColumnW(long nullCount) throws Exception {
        for (String t : tables()) {
            execute("ALTER TABLE " + t + " ALTER COLUMN w TYPE INT");
        }
        drainWalQueue();
        assertSqlCursors("tw ORDER BY ts", "pt ORDER BY ts");
        assertQuery("SELECT count() FROM pt WHERE ts IN '" + O3_DAY + "' AND w IS NULL")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("count\n" + nullCount + "\n");
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

    // DOUBLE d and FLOAT f exist from the start, rows 7, 8 hold values, d and f of row 7 as given; the commit
    // resends row 7 with the given d and f and adds a row at 23:00. The top (6) stays when isTopKept
    private void assertFloatReplay(String storedD, String storedF, String resentD, String resentF, boolean isTopKept) throws Exception {
        for (String t : tables()) {
            createBaseTable(t, " DEDUP UPSERT KEYS(ts)");
            execute("ALTER TABLE " + t + " ADD COLUMN d DOUBLE");
            execute("ALTER TABLE " + t + " ADD COLUMN f FLOAT");
            execute("INSERT INTO " + t + " (v, ts) VALUES " + rowsNoValue(1, 6));
            addColumns(t);
            execute("INSERT INTO " + t + " (v, ts, c1, c2, c3, d, f) VALUES "
                    + "(7, '" + tsOf(7) + "', " + literalOf(7) + ", " + literalOf(7) + ", " + literalOf(7) + ", " + storedD + ", " + storedF + "::FLOAT), "
                    + "(8, '" + tsOf(8) + "', " + literalOf(8) + ", " + literalOf(8) + ", " + literalOf(8) + ", 2.0, 2.0::FLOAT)");
            execute("INSERT INTO " + t + " (v, ts) VALUES (99, '2024-01-02T00:00:00.000000Z')");
        }
        drainWalQueue();
        convertToParquet();
        for (String t : tables()) {
            execute("INSERT INTO " + t + " (v, ts, c1, c2, c3, d, f) VALUES "
                    + "(7, '" + tsOf(7) + "', " + literalOf(7) + ", " + literalOf(7) + ", " + literalOf(7) + ", " + resentD + ", " + resentF + "::FLOAT), "
                    + "(50, '2024-01-01T23:00:00.000000Z', " + o3Literal() + ", " + o3Literal() + ", " + o3Literal() + ", 1.0, 1.0::FLOAT)");
        }
        drainWalQueue();
        assertReaders(topRows(8, 6, -1, "2024-01-01T23:00:00.000000Z", isTopKept));
    }

    // inserts the rows into pt and tw, then asserts that the identity check of the UPSERT commit into the parquet
    // partition of pt decoded the given number of columns
    private void assertIdentityCheckDecodedColumns(long expectedCount, String... rows) throws Exception {
        final long count = getIdentityCheckDecodedColumnCount();
        insertUpsertRows(rows);
        Assert.assertEquals(expectedCount, getIdentityCheckDecodedColumnCount() - count);
    }

    // inserts the rows into pt and tw, then asserts that the row group merges of the commit into the parquet
    // partitions of pt decoded the given number of columns, the UPSERT identity check included
    private void assertMergeDecodedColumns(long expectedCount, String... rows) throws Exception {
        final long count = getMergeDecodedColumnCount();
        insertUpsertRows(rows);
        Assert.assertEquals(expectedCount, getMergeDecodedColumnCount() - count);
    }

    // makes the tables of createEveryTypeValueTables() with the given dedup keys and resends row 6 in one commit:
    // as stored with a new VARCHAR key when isNewKey, else with NULL in every column but v and the keys. Asserts
    // that the merge decoded each of the 28 columns of the parquet file once and that pt matches tw
    private void assertEveryTypeRow6Upsert(String keys, boolean isNewKey) throws Exception {
        createEveryTypeValueTables(" DEDUP UPSERT KEYS(" + keys + ")");
        final StringBuilder columns = new StringBuilder("v, ts");
        final StringBuilder row6 = new StringBuilder("(6, '").append(tsOf(6)).append('\'');
        for (String[] column : EVERY_TYPE_COLUMNS) {
            columns.append(", ").append(column[0]);
            if (isNewKey) {
                row6.append(", ").append("cvarchar".equals(column[0]) ? "'43'" : column[2]);
            } else {
                row6.append(", ").append(keys.contains(column[0]) ? column[2] : "NULL");
            }
        }
        final long count = getMergeDecodedColumnCount();
        for (String t : tables()) {
            execute("INSERT INTO " + t + " (" + columns + ") VALUES " + row6 + ")");
        }
        drainWalQueue();
        // v, ts and the 26 columns of every type
        Assert.assertEquals(28, getMergeDecodedColumnCount() - count);
        assertSqlCursors("tw ORDER BY ts", "pt ORDER BY ts");
        assertQuery("SELECT count() FROM pt WHERE ts IN '" + O3_DAY + "'")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("count\n" + (isNewKey ? 9 : 8) + "\n");
        dropTables();
    }

    // inserts the rows into pt and tw, then asserts that the commit left the parquet partition of pt as it was,
    // as the native noop leaves tw: the row count, name txn, parquet size and column versions, the partition
    // directories, and no row written. With CHAR the identity check does not run and the merge writes the partition
    private void assertUpsertLeavesPartitionUnchanged(String... rows) throws Exception {
        final TableToken tableToken = engine.verifyTableName("pt");
        final String partitions = readTxnToString(tableToken, false, true, true, true);
        final Set<String> partitionDirs = listPartitionDirs(tableToken);
        final long writtenRows = engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();
        insertUpsertRows(rows);
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
        if (hasNoSentinelType()) {
            Assert.assertEquals(partitions, readTxnToString(tableToken, false, true, true, true));
            Assert.assertEquals(partitionDirs, listPartitionDirs(tableToken));
            Assert.assertEquals(writtenRows, engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows());
        } else {
            Assert.assertNotEquals(partitions, readTxnToString(tableToken, false, true, true, true));
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

    private void createEveryTypeTables(int rowCount) throws Exception {
        createEveryTypeTables(rowCount, "");
    }

    // rows 1..rowCount of 2024-01-01 converted to parquet, then the columns of every type added
    private void createEveryTypeTables(int rowCount, String dedup) throws Exception {
        for (String t : tables()) {
            createBaseTable(t, dedup);
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

    // DEDUP UPSERT KEYS(ts) tables with a column of every type, a value in each in rows 1..8 of 2024-01-01,
    // converted to parquet, then BYTE w added (top 8)
    private void createEveryTypeValueTables() throws Exception {
        createEveryTypeValueTables(" DEDUP UPSERT KEYS(ts)");
        for (String t : tables()) {
            execute("ALTER TABLE " + t + " ADD COLUMN w BYTE");
        }
        drainWalQueue();
    }

    // tables with the given dedup and a column of every type, a value in each in rows 1..8 of 2024-01-01,
    // converted to parquet
    private void createEveryTypeValueTables(String dedup) throws Exception {
        final StringBuilder columns = new StringBuilder("v INT, ts TIMESTAMP");
        final StringBuilder values = new StringBuilder();
        for (String[] column : EVERY_TYPE_COLUMNS) {
            columns.append(", ").append(column[0]).append(' ').append(column[1]);
            values.append(", ").append(column[2]);
        }
        for (String t : tables()) {
            execute("CREATE TABLE " + t + " (" + columns + ") TIMESTAMP(ts) PARTITION BY DAY WAL" + dedup);
            final StringBuilder sql = new StringBuilder("INSERT INTO " + t + " VALUES ");
            for (int v = 1; v <= 8; v++) {
                sql.append(v > 1 ? ", " : "").append('(').append(v).append(", '").append(tsOf(v)).append('\'').append(values).append(')');
            }
            execute(sql.toString());
            execute("INSERT INTO " + t + " (v, ts) VALUES (99, '2024-01-02T00:00:00.000000Z')");
        }
        drainWalQueue();
        convertToParquet();
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
        createShapeTables(dedup, top, rowCount, tiedRow, tiedRow);
    }

    // rows 1..rowCount, the columns added after `top` rows, 2024-01-01 converted to parquet; rows
    // tiedLo..tiedHi (-1: none) take the timestamp of row tiedLo - 1
    private void createShapeTables(String dedup, int top, int rowCount, int tiedLo, int tiedHi) throws Exception {
        for (String t : tables()) {
            createBaseTable(t, dedup);
            final StringBuilder topRows = new StringBuilder("INSERT INTO " + t + " (v, ts) VALUES ");
            for (int v = 1; v <= top; v++) {
                topRows.append(v > 1 ? ", " : "").append('(').append(v).append(", '")
                        .append(tsOf(v >= tiedLo && v <= tiedHi ? tiedLo - 1 : v)).append("')");
            }
            execute(topRows.toString());
            addColumns(t);
            final StringBuilder sql = new StringBuilder("INSERT INTO " + t + " (v, ts, c1, c2, c3, ci, cv, cg, cd) VALUES ");
            for (int v = top + 1; v <= rowCount; v++) {
                if (v > top + 1) {
                    sql.append(", ");
                }
                final String literal = literalOf(v);
                sql.append('(').append(v).append(", '").append(tsOf(v >= tiedLo && v <= tiedHi ? tiedLo - 1 : v)).append("', ")
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

    // rows 1..6 and a row with v = duplicateV at 03:00, the timestamp of row 4, before the columns exist (top 7),
    // rows 7, 8 with values, converted to parquet; the dedup UPSERT KEYS(ts) turns on after the conversion
    private void createStoredDuplicateTables(int duplicateV) throws Exception {
        for (String t : tables()) {
            createBaseTable(t, "");
            execute("INSERT INTO " + t + " (v, ts) VALUES " + rowsNoValue(1, 6) + ", (" + duplicateV + ", '" + tsOf(4) + "')");
            addColumns(t);
            execute("INSERT INTO " + t + " (" + UPSERT_COLUMNS + ") VALUES " + storedRow(7, 6) + ", " + storedRow(8, 6));
            execute("INSERT INTO " + t + " (v, ts) VALUES (99, '2024-01-02T00:00:00.000000Z')");
        }
        drainWalQueue();
        convertToParquet();
        for (String t : tables()) {
            execute("ALTER TABLE " + t + " DEDUP ENABLE UPSERT KEYS(ts)");
        }
        drainWalQueue();
    }

    private void dropTables() throws Exception {
        for (String t : tables()) {
            execute("DROP TABLE " + t);
        }
        drainWalQueue();
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

    // inserts row 6 as stored, or with NULL in nullColumn (null: none), and a row at 23:00 with w = 5 into the
    // tables made by createEveryTypeValueTables(), in one commit
    private void insertEveryTypeRow6(String nullColumn) throws Exception {
        final StringBuilder columns = new StringBuilder("v, ts, w");
        final StringBuilder row6 = new StringBuilder("(6, '").append(tsOf(6)).append("', NULL");
        final StringBuilder row50 = new StringBuilder("(50, '2024-01-01T23:00:00.000000Z', 5::BYTE");
        for (String[] column : EVERY_TYPE_COLUMNS) {
            columns.append(", ").append(column[0]);
            row6.append(", ").append(column[0].equals(nullColumn) ? "NULL" : column[2]);
            row50.append(", ").append(column[2]);
        }
        for (String t : tables()) {
            execute("INSERT INTO " + t + " (" + columns + ") VALUES " + row6 + "), " + row50 + ")");
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

    // inserts the rows, each from rowOf(), into pt and tw in one commit
    private void insertUpsertRows(String... rows) throws Exception {
        for (String t : tables()) {
            execute("INSERT INTO " + t + " (" + UPSERT_COLUMNS + ") VALUES " + String.join(", ", rows));
        }
        drainWalQueue();
    }

    // whether the tested type has no NULL value: BOOLEAN, BYTE and SHORT. CHAR 0 is NULL
    private boolean hasNoSentinelType() {
        return !"CHAR".equals(type);
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

    // the VALUES tuple of a new row at ts with v = 50 and the O3 value in c1..c3, see insertUpsertRows()
    private String newRow(String ts) {
        return rowOf(50, ts, o3Literal(), "50", "'50'", "#s", "5.5m");
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

    // the VALUES tuple of a row in UPSERT_COLUMNS, with the same value c in c1..c3
    private String rowOf(int v, String ts, String c, String ci, String cv, String cg, String cd) {
        return "(" + v + ", '" + ts + "', " + c + ", " + c + ", " + c + ", " + ci + ", " + cv + ", " + cg + ", " + cd + ")";
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

    // the rows of createStoredDuplicateTables() after a commit that sets v of both rows at 03:00 to duplicateV
    // and adds a row at 23:00; the top rows read NULL when isTopKept, 0/false otherwise
    private List<Row> storedDuplicateRows(int duplicateV, boolean isTopKept) {
        final Kind topKind = isTopKept ? Kind.NULL : Kind.ZERO;
        final List<Row> rows = new ArrayList<>();
        for (int v = 1; v <= 6; v++) {
            rows.add(new Row(v == 4 ? duplicateV : v, tsOf(v), topKind));
            if (v == 4) {
                rows.add(new Row(duplicateV, tsOf(4), topKind));
            }
        }
        rows.add(new Row(7, tsOf(7), Kind.VALUE));
        rows.add(new Row(8, tsOf(8), Kind.VALUE));
        rows.add(new Row(50, "2024-01-01T23:00:00.000000Z", Kind.O3));
        return rows;
    }

    // the VALUES tuple of row v of createShapeTables() with the values it stores: the columns are NULL in the
    // top rows
    private String storedRow(int v, int top) {
        if (v <= top) {
            return rowOf(v, tsOf(v), "NULL", "NULL", "NULL", "NULL", "NULL");
        }
        return rowOf(v, tsOf(v), literalOf(v), Integer.toString(v), "'" + v + "'", "#s", v % 10 + ".5m");
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
