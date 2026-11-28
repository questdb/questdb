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

package io.questdb.test.cutlass.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.parquet.CopyExportRequestJob;
import io.questdb.cutlass.parquet.ParquetExportMode;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

import static io.questdb.cutlass.parquet.ParquetExportMode.CURSOR_BASED;
import static io.questdb.cutlass.parquet.ParquetExportMode.DIRECT_PAGE_FRAME;
import static io.questdb.cutlass.parquet.ParquetExportMode.PAGE_FRAME_BACKED;

/**
 * COPY ... TO Parquet of sources the export reads through a query: a view named where COPY
 * takes a table, and queries with a NULL-typed column, such as a bare NULL or a CASE whose
 * branches are all NULL, which has no column type of its own. Expected rows come from the data
 * the tests insert.
 */
public class CopyExportSourceTest extends AbstractCairoTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        exportRoot = TestUtils.unchecked(() -> temp.newFolder("export").getAbsolutePath());
        inputRoot = exportRoot;
        staticOverrides.setProperty(PropertyKey.CAIRO_SQL_COPY_ROOT, exportRoot);
        staticOverrides.setProperty(PropertyKey.CAIRO_SQL_COPY_EXPORT_ROOT, exportRoot);
        AbstractCairoTest.setUpStatic();
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_SQL_COPY_EXPORT_ROOT, exportRoot);
    }

    @Test
    public void testCopyInvalidViewByName() throws Exception {
        // A view whose table is gone fails the export with the error a query over it reports.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE gone (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE VIEW v_gone AS (SELECT a, ts FROM gone)");
            execute("DROP TABLE gone");
            drainWalQueue();
            // As for any query over the view, the position is the table's offset in the view body,
            // 18, which COPY shifts by the offset of its source, 5, as it does for COPY (SELECT ...).
            assertExceptionNoLeakCheck("SELECT * FROM v_gone", 18, "table does not exist [table=gone]");
            assertExceptionNoLeakCheck("COPY v_gone TO 'v_gone' WITH FORMAT PARQUET", 5 + 18, "table does not exist [table=gone]");
            Assert.assertFalse(exportFileExists("v_gone"));
            // The failed export left nothing behind: the next one runs.
            execute("CREATE TABLE gone (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO gone VALUES (1, '2024-01-01T00:00:00.000000Z')");
            drainWalQueue();
            copy("COPY v_gone TO 'v_gone' WITH FORMAT PARQUET");
            assertParquetFile("v_gone", "ts", """
                    a\tts
                    1\t2024-01-01T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testCopyNullCaseColumnReusedExporter() throws Exception {
        // One export job keeps its exporter, and so its materializer, across exports: a NULL CASE
        // column, then a computed INT column at the same position, then the NULL CASE column
        // again through the same compiled COPY.
        assertMemoryLeak(() -> {
            createTable();
            final String nullCaseQuery = "SELECT CASE WHEN v > 0 THEN NULL END n, v FROM t";
            final String computedQuery = "SELECT v * 10 n, v FROM t";
            assertExportMode(nullCaseQuery, PAGE_FRAME_BACKED);
            assertExportMode(computedQuery, PAGE_FRAME_BACKED);
            final String expectedNullCase = """
                    n\tv
                    \t1
                    \t2
                    \t3
                    """;
            try (
                    CopyExportRequestJob job = new CopyExportRequestJob(engine);
                    RecordCursorFactory nullCaseCopy = select("COPY (" + nullCaseQuery + ") TO 'reuse' WITH FORMAT PARQUET");
                    RecordCursorFactory computedCopy = select("COPY (" + computedQuery + ") TO 'reuse' WITH FORMAT PARQUET")
            ) {
                runCopy(job, nullCaseCopy);
                assertParquetFile("reuse", null, expectedNullCase);
                assertNullStringColumn("reuse", "n", 3);

                runCopy(job, computedCopy);
                assertParquetFile("reuse", null, """
                        n\tv
                        10\t1
                        20\t2
                        30\t3
                        """);

                runCopy(job, nullCaseCopy);
                assertParquetFile("reuse", null, expectedNullCase);
                assertNullStringColumn("reuse", "n", 3);
            }
        });
    }

    @Test
    public void testCopyNullColumn() throws Exception {
        // A bare NULL has no column type; the export writes it as a STRING column of NULLs, the
        // type /exec reports for it.
        assertMemoryLeak(() -> {
            createTable();
            final String expected = """
                    sym\tn\tv\tts
                    a\t\t1\t2024-01-01T00:00:00.000000Z
                    b\t\t2\t2024-01-01T12:00:00.000000Z
                    c\t\t3\t2024-01-02T00:00:00.000000Z
                    """;
            copy("SELECT sym, NULL n, v, ts FROM t", PAGE_FRAME_BACKED, "null_hybrid", "");
            assertParquetFile("null_hybrid", "ts", expected);
            assertNullStringColumn("null_hybrid", "n", 3);

            copy("SELECT t.sym, NULL n, t.v, t.ts FROM t CROSS JOIN long_sequence(1)", CURSOR_BASED, "null_cursor", "");
            assertParquetFile("null_cursor", "ts", expected);
            assertNullStringColumn("null_cursor", "n", 3);

            // A sort that copies the rows of a join keeps no bytes for a NULL column: the export
            // must not read the column that follows it in its place.
            copy(
                    "SELECT n, s, n2, v FROM (SELECT NULL n, t.s, NULL n2, t.v FROM t CROSS JOIN long_sequence(1)) ORDER BY v DESC",
                    CURSOR_BASED,
                    "null_sorted",
                    ""
            );
            assertParquetFile("null_sorted", null, """
                    n\ts\tn2\tv
                    \tthree\t\t3
                    \ttwo\t\t2
                    \tone\t\t1
                    """);
            assertNullStringColumn("null_sorted", "n", 3);
            assertNullStringColumn("null_sorted", "n2", 3);

            copy("SELECT CASE WHEN v > 0 THEN NULL END n, s, count() c FROM t GROUP BY 1, 2 ORDER BY 2", CURSOR_BASED, "null_group_by", "");
            assertParquetFile("null_group_by", null, """
                    n\ts\tc
                    \tone\t1
                    \tthree\t1
                    \ttwo\t1
                    """);
            assertNullStringColumn("null_group_by", "n", 3);

            // Only NULL columns.
            copy("SELECT NULL n FROM t", PAGE_FRAME_BACKED, "null_only", "");
            assertParquetFile("null_only", null, "n\n\n\n\n");
            assertNullStringColumn("null_only", "n", 3);

            // A bloom filter over the NULL column.
            copy("SELECT sym, NULL n, v, ts FROM t", PAGE_FRAME_BACKED, "null_bloom", " BLOOM_FILTER_COLUMNS 'n'");
            assertParquetFile("null_bloom", "ts", expected);
            assertNullStringColumn("null_bloom", "n", 3);

            // Re-partitioning materializes the query into a temp table first.
            copy("COPY (SELECT sym, NULL n, v, ts FROM t) TO 'null_parts' WITH FORMAT PARQUET PARTITION_BY DAY");
            assertParquetFile("null_parts" + File.separator + "2024-01-01", "ts", """
                    sym\tn\tv\tts
                    a\t\t1\t2024-01-01T00:00:00.000000Z
                    b\t\t2\t2024-01-01T12:00:00.000000Z
                    """);
            assertParquetFile("null_parts" + File.separator + "2024-01-02", "ts", """
                    sym\tn\tv\tts
                    c\t\t3\t2024-01-02T00:00:00.000000Z
                    """);
            assertNullStringColumn("null_parts" + File.separator + "2024-01-01", "n", 2);
            assertNullStringColumn("null_parts" + File.separator + "2024-01-02", "n", 1);

            // A view with a NULL column, named in place of a table.
            execute("CREATE VIEW v_null AS (SELECT sym, NULL n, v, ts FROM t)");
            drainWalQueue();
            copy("COPY v_null TO 'null_view' WITH FORMAT PARQUET");
            assertParquetFile("null_view", "ts", expected);
            assertNullStringColumn("null_view", "n", 3);

            // A CASE whose branches are all NULL has the NULL type too, but unlike the bare NULL's
            // constant, its function implements no getter. Every export mode writes it as the same
            // STRING column of NULLs. Re-partitioning, through a temp table:
            copy("COPY (SELECT sym, CASE WHEN v > 0 THEN NULL END n, v, ts FROM t) TO 'null_case_parts' WITH FORMAT PARQUET PARTITION_BY DAY");
            assertParquetFile("null_case_parts" + File.separator + "2024-01-01", "ts", """
                    sym\tn\tv\tts
                    a\t\t1\t2024-01-01T00:00:00.000000Z
                    b\t\t2\t2024-01-01T12:00:00.000000Z
                    """);
            assertParquetFile("null_case_parts" + File.separator + "2024-01-02", "ts", """
                    sym\tn\tv\tts
                    c\t\t3\t2024-01-02T00:00:00.000000Z
                    """);
            assertNullStringColumn("null_case_parts" + File.separator + "2024-01-01", "n", 2);
            assertNullStringColumn("null_case_parts" + File.separator + "2024-01-02", "n", 1);

            // An empty table: no row reaches the column, and the file still types it STRING.
            execute("CREATE TABLE t_empty (sym SYMBOL, v INT, s VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            drainWalQueue();
            copy("SELECT CASE WHEN v > 0 THEN NULL END n, v FROM t_empty", PAGE_FRAME_BACKED, "null_case_empty", "");
            assertParquetFile("null_case_empty", null, "n\tv\n");
            try (RecordCursorFactory factory = select("SELECT n FROM " + readParquet("null_case_empty"))) {
                Assert.assertEquals("STRING", ColumnType.nameOf(factory.getMetadata().getColumnType(0)));
            }

            // The zero-copy export: v = 1 takes the implicit ELSE, v = 2 and 3 the THEN NULL.
            copy("SELECT sym, CASE WHEN v > 1 THEN NULL END n, v, ts FROM t", PAGE_FRAME_BACKED, "null_case_hybrid", "");
            assertParquetFile("null_case_hybrid", "ts", expected);
            assertNullStringColumn("null_case_hybrid", "n", 3);

            // NULL CASE columns around computed and pass-through ones: the switch form with ELSE
            // NULL, and a nested CASE. The computed INT keeps its values.
            copy(
                    "SELECT CASE sym WHEN 'a' THEN NULL ELSE NULL END n, v * 10 v10, s, CASE WHEN v > 1 THEN CASE WHEN v > 2 THEN NULL END END n2, ts FROM t",
                    PAGE_FRAME_BACKED,
                    "null_case_mixed",
                    ""
            );
            assertParquetFile("null_case_mixed", "ts", """
                    n\tv10\ts\tn2\tts
                    \t10\tone\t\t2024-01-01T00:00:00.000000Z
                    \t20\ttwo\t\t2024-01-01T12:00:00.000000Z
                    \t30\tthree\t\t2024-01-02T00:00:00.000000Z
                    """);
            assertNullStringColumn("null_case_mixed", "n", 3);
            assertNullStringColumn("null_case_mixed", "n2", 3);

            // Computed STRING and SYMBOL columns next to the NULL CASE keep their values; both
            // export as STRING.
            copy("SELECT CASE WHEN v > 0 THEN NULL END n, s::STRING s2, s::SYMBOL s3, v, ts FROM t", PAGE_FRAME_BACKED, "null_case_strings", "");
            assertParquetFile("null_case_strings", "ts", """
                    n\ts2\ts3\tv\tts
                    \tone\tone\t1\t2024-01-01T00:00:00.000000Z
                    \ttwo\ttwo\t2\t2024-01-01T12:00:00.000000Z
                    \tthree\tthree\t3\t2024-01-02T00:00:00.000000Z
                    """);
            assertNullStringColumn("null_case_strings", "n", 3);
            assertQuery("SELECT typeOf(s2) s2_type, typeOf(s3) s3_type, count() FROM " + readParquet("null_case_strings"))
                    .noLeakCheck()
                    .expectSize()
                    .returns("s2_type\ts3_type\tcount\nSTRING\tSTRING\t3\n");

            // A constant condition, and the switch form on VARCHAR and TIMESTAMP keys.
            copy(
                    "SELECT CASE WHEN true THEN NULL END n, CASE s WHEN 'one' THEN NULL END n2, CASE ts WHEN '2024-01-01' THEN NULL END n3, v FROM t",
                    PAGE_FRAME_BACKED,
                    "null_case_forms",
                    ""
            );
            assertParquetFile("null_case_forms", null, """
                    n\tn2\tn3\tv
                    \t\t\t1
                    \t\t\t2
                    \t\t\t3
                    """);
            assertNullStringColumn("null_case_forms", "n", 3);
            assertNullStringColumn("null_case_forms", "n2", 3);
            assertNullStringColumn("null_case_forms", "n3", 3);

            // A bind variable in the condition.
            bindVariableService.clear();
            bindVariableService.setInt(0, 1);
            copy("SELECT sym, CASE WHEN v > $1 THEN NULL END n, v, ts FROM t", PAGE_FRAME_BACKED, "null_case_bind", "");
            bindVariableService.clear();
            assertParquetFile("null_case_bind", "ts", expected);
            assertNullStringColumn("null_case_bind", "n", 3);

            // An interval that starts inside the first partition.
            copy("SELECT CASE WHEN v > 0 THEN NULL END n, v, ts FROM t WHERE ts >= '2024-01-01T12:00'", PAGE_FRAME_BACKED, "null_case_interval", "");
            assertParquetFile("null_case_interval", "ts", """
                    n\tv\tts
                    \t2\t2024-01-01T12:00:00.000000Z
                    \t3\t2024-01-02T00:00:00.000000Z
                    """);
            assertNullStringColumn("null_case_interval", "n", 2);

            // A subquery and a CTE.
            final String expectedNullCaseAndV = """
                    n\tv
                    \t1
                    \t2
                    \t3
                    """;
            copy("SELECT n, v FROM (SELECT CASE WHEN v > 0 THEN NULL END n, v FROM t)", PAGE_FRAME_BACKED, "null_case_subquery", "");
            assertParquetFile("null_case_subquery", null, expectedNullCaseAndV);
            assertNullStringColumn("null_case_subquery", "n", 3);
            copy("WITH q AS (SELECT CASE WHEN v > 0 THEN NULL END n, v FROM t) SELECT * FROM q", PAGE_FRAME_BACKED, "null_case_cte", "");
            assertParquetFile("null_case_cte", null, expectedNullCaseAndV);
            assertNullStringColumn("null_case_cte", "n", 3);

            // Only the NULL CASE column.
            copy("SELECT CASE WHEN v > 0 THEN NULL END n FROM t", PAGE_FRAME_BACKED, "null_case_only", "");
            assertParquetFile("null_case_only", null, "n\n\n\n\n");
            assertNullStringColumn("null_case_only", "n", 3);

            // A bloom filter over the NULL CASE column.
            copy("SELECT sym, CASE WHEN v > 1 THEN NULL END n, v, ts FROM t", PAGE_FRAME_BACKED, "null_case_bloom", " BLOOM_FILTER_COLUMNS 'n'");
            assertParquetFile("null_case_bloom", "ts", expected);
            assertNullStringColumn("null_case_bloom", "n", 3);

            // Single-row tables, one not partitioned and one without a designated timestamp.
            execute("CREATE TABLE tn (sym SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY NONE");
            execute("INSERT INTO tn VALUES ('a', 1, '2024-01-01T00:00:00.000000Z')");
            execute("CREATE TABLE tnots (sym SYMBOL, v INT)");
            execute("INSERT INTO tnots VALUES ('a', 1)");
            drainWalQueue();
            copy("SELECT sym, CASE WHEN v > 0 THEN NULL END n, v, ts FROM tn", PAGE_FRAME_BACKED, "null_case_not_partitioned", "");
            assertParquetFile("null_case_not_partitioned", "ts", """
                    sym\tn\tv\tts
                    a\t\t1\t2024-01-01T00:00:00.000000Z
                    """);
            assertNullStringColumn("null_case_not_partitioned", "n", 1);
            copy("SELECT sym, CASE WHEN v > 0 THEN NULL END n, v FROM tnots", PAGE_FRAME_BACKED, "null_case_no_timestamp", "");
            assertParquetFile("null_case_no_timestamp", null, """
                    sym\tn\tv
                    a\t\t1
                    """);
            assertNullStringColumn("null_case_no_timestamp", "n", 1);
            // ORDER BY ts DESC is left out: COPY writes the rows of such a projection in ascending
            // order, a separate bug.
        });
    }

    @Test
    public void testCopyNullColumnFilteredByIndexedSymbol() throws Exception {
        // A symbol IN list over a covering index keeps the export of a NULL CASE column zero-copy.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ti (sym SYMBOL INDEX TYPE POSTING INCLUDE (v, ts), v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO ti VALUES
                    ('a', 1, '2024-01-01T00:00:00.000000Z'),
                    ('b', 2, '2024-01-01T12:00:00.000000Z'),
                    ('c', 3, '2024-01-02T00:00:00.000000Z')
                    """);
            drainWalQueue();
            final String query = "SELECT sym, CASE WHEN v > 0 THEN NULL END n, v, ts FROM ti WHERE sym IN ('a', 'c')";
            assertQuery(query).noLeakCheck().assertsPlanContaining("CoveringIndex on: sym");
            copy(query, PAGE_FRAME_BACKED, "null_case_indexed", "");
            assertParquetFile("null_case_indexed", "ts", """
                    sym\tn\tv\tts
                    a\t\t1\t2024-01-01T00:00:00.000000Z
                    c\t\t3\t2024-01-02T00:00:00.000000Z
                    """);
            assertNullStringColumn("null_case_indexed", "n", 2);
        });
    }

    @Test
    public void testCopyNullColumnOnParquetPartition() throws Exception {
        // The zero-copy export decodes the row group of a Parquet partition, day 1, before it
        // writes the NULL CASE column; day 2 stays native.
        assertMemoryLeak(() -> {
            createTable();
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            assertQuery("SELECT name, isParquet FROM table_partitions('t')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            name\tisParquet
                            2024-01-01\ttrue
                            2024-01-02\tfalse
                            """);
            copy("SELECT sym, CASE WHEN v > 1 THEN NULL END n, v, ts FROM t", PAGE_FRAME_BACKED, "null_case_parquet", "");
            assertParquetFile("null_case_parquet", "ts", """
                    sym\tn\tv\tts
                    a\t\t1\t2024-01-01T00:00:00.000000Z
                    b\t\t2\t2024-01-01T12:00:00.000000Z
                    c\t\t3\t2024-01-02T00:00:00.000000Z
                    """);
            assertNullStringColumn("null_case_parquet", "n", 3);

            // The interval starts at row 1 of the Parquet day's row group.
            copy(
                    "SELECT sym, CASE WHEN v > 1 THEN NULL END n, v, ts FROM t WHERE ts >= '2024-01-01T12:00'",
                    PAGE_FRAME_BACKED,
                    "null_case_parquet_interval",
                    ""
            );
            assertParquetFile("null_case_parquet_interval", "ts", """
                    sym\tn\tv\tts
                    b\t\t2\t2024-01-01T12:00:00.000000Z
                    c\t\t3\t2024-01-02T00:00:00.000000Z
                    """);
            assertNullStringColumn("null_case_parquet_interval", "n", 2);
        });
    }

    @Test
    public void testCopyViewByName() throws Exception {
        // COPY <view> TO exports the rows the view returns, whichever way the export reads them.
        assertMemoryLeak(() -> {
            createTable();
            execute("CREATE VIEW v_scan AS (SELECT ts, v FROM t)");
            execute("CREATE VIEW v_computed AS (SELECT sym, v * 10 v10, ts FROM t)");
            execute("CREATE VIEW v_join AS (SELECT a.sym, b.v FROM t a CROSS JOIN t b WHERE b.v = 1)");
            drainWalQueue();

            assertExportMode("v_scan", DIRECT_PAGE_FRAME);
            copy("COPY v_scan TO 'v_scan' WITH FORMAT PARQUET");
            assertParquetFile("v_scan", "ts", """
                    ts\tv
                    2024-01-01T00:00:00.000000Z\t1
                    2024-01-01T12:00:00.000000Z\t2
                    2024-01-02T00:00:00.000000Z\t3
                    """);

            // The view's projection sits on top of the computing one, so the export reads rows.
            assertExportMode("v_computed", CURSOR_BASED);
            copy("COPY v_computed TO 'v_computed' WITH FORMAT PARQUET");
            assertParquetFile("v_computed", "ts", """
                    sym\tv10\tts
                    a\t10\t2024-01-01T00:00:00.000000Z
                    b\t20\t2024-01-01T12:00:00.000000Z
                    c\t30\t2024-01-02T00:00:00.000000Z
                    """);

            assertExportMode("v_join", CURSOR_BASED);
            copy("COPY v_join TO 'v_join' WITH FORMAT PARQUET");
            assertParquetFile("v_join", null, """
                    sym\tv
                    a\t1
                    b\t1
                    c\t1
                    """);

            // Re-partitioning, through a temp table.
            copy("COPY v_computed TO 'v_parts' WITH FORMAT PARQUET PARTITION_BY DAY");
            assertParquetFile("v_parts" + File.separator + "2024-01-01", "ts", """
                    sym\tv10\tts
                    a\t10\t2024-01-01T00:00:00.000000Z
                    b\t20\t2024-01-01T12:00:00.000000Z
                    """);
            assertParquetFile("v_parts" + File.separator + "2024-01-02", "ts", """
                    sym\tv10\tts
                    c\t30\t2024-01-02T00:00:00.000000Z
                    """);

            // Bloom filter columns name the view's columns.
            copy("COPY v_computed TO 'v_bloom' WITH FORMAT PARQUET BLOOM_FILTER_COLUMNS 'v10'");
            assertParquetFile("v_bloom", "ts", """
                    sym\tv10\tts
                    a\t10\t2024-01-01T00:00:00.000000Z
                    b\t20\t2024-01-01T12:00:00.000000Z
                    c\t30\t2024-01-02T00:00:00.000000Z
                    """);
            // The position points at the second name in the list.
            assertExceptionNoLeakCheck(
                    "COPY v_computed TO 'v_bloom_bad' WITH FORMAT PARQUET BLOOM_FILTER_COLUMNS 'v10,v'",
                    79,
                    "bloom_filter_columns contains non-existent column: v"
            );
            Assert.assertFalse(exportFileExists("v_bloom_bad"));
        });
    }

    @Test
    public void testCopyViewByNameKeepsIdentity() throws Exception {
        // Unquoted, "my view" reads table my under the alias view. COPY takes the name in three
        // quoting forms, and each must reach the view.
        assertMemoryLeak(() -> {
            createTable();
            execute("CREATE VIEW \"my view\" AS (SELECT sym, ts FROM t WHERE v = 2)");
            execute("CREATE TABLE my (decoy SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO my VALUES ('DECOY', '2024-01-01T00:00:00.000000Z')");
            drainWalQueue();
            final String expected = """
                    sym\tts
                    b\t2024-01-01T12:00:00.000000Z
                    """;
            copy("COPY 'my view' TO 'my_view_single' WITH FORMAT PARQUET");
            assertParquetFile("my_view_single", "ts", expected);
            copy("COPY \"my view\" TO 'my_view_double' WITH FORMAT PARQUET");
            assertParquetFile("my_view_double", "ts", expected);
            copy("COPY `my view` TO 'my_view_backtick' WITH FORMAT PARQUET");
            assertParquetFile("my_view_backtick", "ts", expected);
        });
    }

    @Test
    public void testCopyViewByNameReusedFactory() throws Exception {
        // One compiled COPY resolves its source on every execution: a view, the view after ALTER
        // VIEW, and a table that took the view's name.
        assertMemoryLeak(() -> {
            createTable();
            execute("CREATE VIEW v_reuse AS (SELECT sym, v FROM t WHERE v = 1)");
            drainWalQueue();
            try (
                    CopyExportRequestJob job = new CopyExportRequestJob(engine);
                    RecordCursorFactory factory = select("COPY v_reuse TO 'v_reuse' WITH FORMAT PARQUET")
            ) {
                runCopy(job, factory);
                assertParquetFile("v_reuse", null, """
                        sym\tv
                        a\t1
                        """);

                execute("ALTER VIEW v_reuse AS (SELECT v, sym FROM t WHERE v > 1)");
                drainWalQueue();
                runCopy(job, factory);
                assertParquetFile("v_reuse", null, """
                        v\tsym
                        2\tb
                        3\tc
                        """);

                execute("DROP VIEW v_reuse");
                execute("CREATE TABLE v_reuse (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
                execute("INSERT INTO v_reuse VALUES (42, '2024-01-03T00:00:00.000000Z')");
                drainWalQueue();
                runCopy(job, factory);
                assertParquetFile("v_reuse", "ts", """
                        x\tts
                        42\t2024-01-03T00:00:00.000000Z
                        """);
            }
        });
    }

    @Test
    public void testCopyViewByNameReusedFactoryRepartitions() throws Exception {
        // The same, with re-partitioning: the view, then a table of another partitioning.
        assertMemoryLeak(() -> {
            createTable();
            execute("CREATE VIEW v_parts AS (SELECT v, ts FROM t)");
            drainWalQueue();
            try (
                    CopyExportRequestJob job = new CopyExportRequestJob(engine);
                    RecordCursorFactory factory = select("COPY v_parts TO 'v_parts' WITH FORMAT PARQUET PARTITION_BY DAY")
            ) {
                runCopy(job, factory);
                assertParquetFile("v_parts" + File.separator + "2024-01-01", "ts", """
                        v\tts
                        1\t2024-01-01T00:00:00.000000Z
                        2\t2024-01-01T12:00:00.000000Z
                        """);
                assertParquetFile("v_parts" + File.separator + "2024-01-02", "ts", """
                        v\tts
                        3\t2024-01-02T00:00:00.000000Z
                        """);

                execute("DROP VIEW v_parts");
                execute("CREATE TABLE v_parts (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY MONTH");
                execute("INSERT INTO v_parts VALUES (1, '2024-01-03T00:00:00.000000Z'), (2, '2024-01-04T00:00:00.000000Z')");
                drainWalQueue();
                runCopy(job, factory);
                assertParquetFile("v_parts" + File.separator + "2024-01-03", "ts", """
                        x\tts
                        1\t2024-01-03T00:00:00.000000Z
                        """);
                assertParquetFile("v_parts" + File.separator + "2024-01-04", "ts", """
                        x\tts
                        2\t2024-01-04T00:00:00.000000Z
                        """);
            }
        });
    }

    private static void assertExportMode(String query, ParquetExportMode expectedMode) throws Exception {
        try (RecordCursorFactory factory = select(query)) {
            Assert.assertEquals(query, expectedMode, ParquetExportMode.determineExportMode(factory, false, sqlExecutionContext));
        }
    }

    private static void createTable() throws Exception {
        execute("CREATE TABLE t (sym SYMBOL, v INT, s VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO t VALUES
                ('a', 1, 'one', '2024-01-01T00:00:00.000000Z'),
                ('b', 2, 'two', '2024-01-01T12:00:00.000000Z'),
                ('c', 3, 'three', '2024-01-02T00:00:00.000000Z')
                """);
    }

    private static boolean exportFileExists(String fileName) {
        return new File(exportRoot, fileName + ".parquet").exists();
    }

    private static String readParquet(String fileName) {
        return "read_parquet('" + exportRoot + File.separator + fileName + ".parquet')";
    }

    private void assertExportFinished() throws Exception {
        assertQuery("SELECT status, message FROM \"sys.copy_export_log\" LIMIT -1")
                .noLeakCheck()
                .expectSize()
                .returns("status\tmessage\nfinished\t\n");
    }

    private void assertNullStringColumn(String fileName, String column, int rowCount) throws Exception {
        assertQuery("SELECT typeOf(" + column + ") type, count() FROM " + readParquet(fileName) + " WHERE " + column + " IS NULL")
                .noLeakCheck()
                .expectSize()
                .returns("type\tcount\nSTRING\t" + rowCount + "\n");
    }

    private void assertParquetFile(String fileName, @Nullable String timestamp, String expected) throws Exception {
        assertQuery("SELECT * FROM " + readParquet(fileName))
                .noLeakCheck()
                .expectSize()
                .timestamp(timestamp)
                .returns(expected);
    }

    private void copy(String copySql) throws Exception {
        try (
                CopyExportRequestJob job = new CopyExportRequestJob(engine);
                RecordCursorFactory factory = select(copySql)
        ) {
            runCopy(job, factory);
        }
    }

    private void copy(String query, ParquetExportMode expectedMode, String fileName, String options) throws Exception {
        assertExportMode(query, expectedMode);
        copy("COPY (" + query + ") TO '" + fileName + "' WITH FORMAT PARQUET" + options);
    }

    // Runs the COPY factory once: queues the export, runs it, and checks that it finished.
    private void runCopy(CopyExportRequestJob job, RecordCursorFactory factory) throws Exception {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertTrue(cursor.hasNext());
        }
        Assert.assertTrue("the queued export must run", job.run());
        drainWalQueue();
        assertExportFinished();
    }
}
