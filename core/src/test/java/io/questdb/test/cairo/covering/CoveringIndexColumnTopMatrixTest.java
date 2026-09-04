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

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The full matrix of covered reads over a partition whose indexed column carries a
 * column top. One test per case, named after the partition state, the partition
 * format, the filter and the execution path it takes.
 * <p>
 * Every table here is built by {@link #createMatrixTable}, which lays out the three
 * partition states side by side:
 *
 * <pre>
 *   2024-01-01  fully absent   sym does not exist in the partition (top == rowCount)
 *   2024-01-02  partial        sym arrives mid-partition           (0 &lt; top &lt; rowCount)
 *   2024-01-03  fully present  sym exists from the first row       (top == 0)
 * </pre>
 * <p>
 * A covered read of a partition that carries a top cannot come from the posting-index
 * sidecar: the chain holds no posting for a row below the top, so the sidecar holds no
 * value for it either. The covering factory answers such a partition from the
 * partition's own columns instead, which is what most of these cases pin. Two cases
 * are known gaps and are pinned as such, so that fixing either turns its test red
 * rather than leaving it silently asserting the old answer:
 * <ul>
 *     <li>a fully absent PARQUET partition still drops its rows from a NULL-key covered
 *     scan -- see {@link #testFullyAbsentParquetIsNullCoveredScanDropsRows};</li>
 *     <li>{@code SAMPLE BY} first/last throws over any partition that carries the
 *     indexed column -- see {@link #testSampleByFirstLastThrowsOnPartitionCarryingTheColumn}.</li>
 * </ul>
 */
public class CoveringIndexColumnTopMatrixTest extends AbstractCairoTest {

    @Test
    public void testFullyAbsentEqualsCoveredScan() throws Exception {
        // Matrix row 5. A partition that predates the indexed column can hold no row for
        // a non-NULL key, so it contributes nothing and the scan is not asked to read it.
        assertMemoryLeak(() -> {
            createMatrixTable("m_abs_eq");
            assertCoveredAndReference(
                    "SELECT ts, sym, val FROM m_abs_eq WHERE sym = 'A' AND ts IN '2024-01-01' ORDER BY ts",
                    "ts",
                    "ts\tsym\tval\n"
            );
        });
    }

    @Test
    public void testFullyAbsentEqualsSampleByFirstLast() throws Exception {
        // Matrix row 3. `other` sits outside the INCLUDE list, so codegen routes this to
        // SampleByFirstLast, which asks the partition's index reader for an
        // IndexFrameCursor. Only the stand-in null reader answers that, with an empty
        // frame cursor. Handing this partition a posting reader instead -- as an earlier
        // shape of this fix did -- turns the empty result into an
        // UnsupportedOperationException.
        assertMemoryLeak(() -> {
            createMatrixTable("m_abs_sbfl_eq");
            assertQuery("""
                    SELECT ts, sym, first(other) fo, last(other) lo FROM m_abs_sbfl_eq
                    WHERE sym = 'A' AND ts IN '2024-01-01'
                    SAMPLE BY 1h ALIGN TO FIRST OBSERVATION
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .timestamp("ts")
                    .withPlanContaining("SampleByFirstLast")
                    .returns("ts\tsym\tfo\tlo\n");
        });
    }

    @Test
    public void testFullyAbsentIsNullCoveredScan() throws Exception {
        // Matrix row 1. Every row of the partition matches the NULL key and must come
        // back with its INCLUDE value, read from val.d.
        assertMemoryLeak(() -> {
            createMatrixTable("m_abs_null");
            assertCoveredAndReference(
                    "SELECT ts, sym, val FROM m_abs_null WHERE sym = null AND ts IN '2024-01-01' ORDER BY ts",
                    "ts",
                    """
                            ts\tsym\tval
                            2024-01-01T00:00:00.000000Z\t\t10.0
                            2024-01-01T01:00:00.000000Z\t\t20.0
                            """
            );
        });
    }

    @Test
    public void testFullyAbsentIsNullSampleByFirstLast() throws Exception {
        // Matrix row 2. The twin of testFullyAbsentEqualsSampleByFirstLast for the NULL
        // key, where the stand-in reader's empty frame cursor is the only thing standing
        // between this query and a throw.
        assertMemoryLeak(() -> {
            createMatrixTable("m_abs_sbfl_null");
            assertQuery("""
                    SELECT ts, sym, first(other) fo, last(other) lo FROM m_abs_sbfl_null
                    WHERE sym = null AND ts IN '2024-01-01'
                    SAMPLE BY 1h ALIGN TO FIRST OBSERVATION
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .timestamp("ts")
                    .withPlanContaining("SampleByFirstLast")
                    .returns("""
                            ts\tsym\tfo\tlo
                            2024-01-01T00:00:00.000000Z\t\t100.0\t100.0
                            2024-01-01T01:00:00.000000Z\t\t200.0\t200.0
                            """);
        });
    }

    @Test
    public void testFullyAbsentLatestOn() throws Exception {
        // Matrix row 4. LATEST ON drives the BACKWARD reader through getCursor(), not
        // through page frames, so it is a separate path from every scan above. The last
        // NULL row of the table is the explicit NULL in the partial partition, and its
        // INCLUDE value has to come back with it.
        assertMemoryLeak(() -> {
            createMatrixTable("m_abs_latest");
            assertCoveredAndReference(
                    "SELECT sym, val FROM m_abs_latest WHERE sym = null LATEST ON ts PARTITION BY sym",
                    null,
                    """
                            sym\tval
                            \t60.0
                            """
            );
            assertCoveredAndReference(
                    "SELECT sym, val FROM m_abs_latest WHERE sym = 'A' LATEST ON ts PARTITION BY sym",
                    null,
                    """
                            sym\tval
                            A\t70.0
                            """
            );
        });
    }

    @Test
    public void testFullyAbsentParquetIsNullCoveredScanDropsRows() throws Exception {
        // Matrix row 6, a KNOWN GAP pinned as it stands. A Parquet partition keeps its
        // columns inside data.parquet, so the reader maps no native column memory for
        // them and there is nothing for the covered read to fall back TO. The partition
        // is skipped and its rows disappear from a NULL-key covered scan -- which is
        // exactly what it did before the fall-back existed. Decoding the rows out of
        // data.parquet is what a real fix takes; doing it turns this test red.
        assertMemoryLeak(() -> {
            createMatrixTable("m_pq_abs");
            execute("ALTER TABLE m_pq_abs CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            engine.releaseAllReaders();

            // Iterated by hand rather than through assertQuery: the covered cursor still
            // reports size() == 2 for this partition (its size() counts index matches,
            // which the drop does not touch), so size and iteration disagree and the
            // builder's size cross-check would fail on the gap itself.
            final String sql = "SELECT ts, sym, val FROM m_pq_abs WHERE sym = null AND ts IN '2024-01-01' ORDER BY ts";
            TestUtils.assertContains(
                    "the covering factory must still be the one that drops them",
                    getPlanSink(sql).getSink(),
                    "CoveringIndex"
            );
            try (RecordCursorFactory factory = select(sql)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    int rows = 0;
                    while (cursor.hasNext()) {
                        rows++;
                    }
                    Assert.assertEquals("the parquet partition contributes no row", 0, rows);
                    Assert.assertEquals("size() still counts the index matches it does not return", 2, cursor.size());
                }
            }

            // The plain index scan over the same partition still returns them, which is
            // what makes this a gap rather than a property of the data.
            assertQuery("""
                    SELECT /*+ no_covering */ ts, sym, val FROM m_pq_abs
                    WHERE sym = null AND ts IN '2024-01-01' ORDER BY ts
                    """)
                    .noLeakCheck()
                    .sizeMayVary()
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tval
                            2024-01-01T00:00:00.000000Z\t\t10.0
                            2024-01-01T01:00:00.000000Z\t\t20.0
                            """);
        });
    }

    @Test
    public void testFullyPresentCoveredScan() throws Exception {
        // Matrix row 11. A partition the indexed column has covered from its first row
        // has no top at all, so it stays on the sidecar -- both formats, both keys.
        assertMemoryLeak(() -> {
            createMatrixTable("m_present");
            createMatrixTable("m_present_pq");
            execute("ALTER TABLE m_present_pq CONVERT PARTITION TO PARQUET LIST '2024-01-03'");
            engine.releaseAllReaders();

            for (String table : new String[]{"m_present", "m_present_pq"}) {
                assertCoveredAndReference(
                        "SELECT ts, sym, val FROM " + table + " WHERE sym = 'A' AND ts IN '2024-01-03' ORDER BY ts",
                        "ts",
                        """
                                ts\tsym\tval
                                2024-01-03T00:00:00.000000Z\tA\t70.0
                                """
                );
                assertCoveredAndReference(
                        "SELECT ts, sym, val FROM " + table + " WHERE sym = null AND ts IN '2024-01-03' ORDER BY ts",
                        "ts",
                        "ts\tsym\tval\n"
                );
            }
        });
    }

    @Test
    public void testNonCoveringIndexUnchanged() throws Exception {
        // Matrix row 12. Neither a BITMAP index nor a POSTING index without an INCLUDE
        // list carries values, so neither goes anywhere near the covered path. Both must
        // answer every partition state exactly as the covering index does.
        assertMemoryLeak(() -> {
            createMatrixTable("m_cov");
            createBaseTable("m_bitmap");
            execute("ALTER TABLE m_bitmap ALTER COLUMN sym ADD INDEX CAPACITY 32");
            createBaseTable("m_posting");
            execute("ALTER TABLE m_posting ALTER COLUMN sym ADD INDEX TYPE POSTING");
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            final String expectedNull = """
                    ts\tsym\tval
                    2024-01-01T00:00:00.000000Z\t\t10.0
                    2024-01-01T01:00:00.000000Z\t\t20.0
                    2024-01-02T00:00:00.000000Z\t\t30.0
                    2024-01-02T01:00:00.000000Z\t\t40.0
                    2024-01-02T03:00:00.000000Z\t\t60.0
                    """;
            final String expectedA = """
                    ts\tsym\tval
                    2024-01-02T02:00:00.000000Z\tA\t50.0
                    2024-01-03T00:00:00.000000Z\tA\t70.0
                    """;
            for (String table : new String[]{"m_cov", "m_bitmap", "m_posting"}) {
                // Only the covering factory gives up random access; the two plain index
                // scans keep it, so the flag follows the table.
                final boolean isCovering = "m_cov".equals(table);
                assertMatrixRows(table, "null", isCovering, expectedNull);
                assertMatrixRows(table, "'A'", isCovering, expectedA);
            }
        });
    }

    @Test
    public void testPartialEqualsCoveredScan() throws Exception {
        // Matrix row 9. A non-NULL key matches only rows at or above the top, which the
        // chain does carry postings for. The answer was already right before the
        // fall-back; this pins that the fall-back did not change it.
        assertMemoryLeak(() -> {
            createMatrixTable("m_part_eq");
            createMatrixTable("m_part_eq_pq");
            execute("ALTER TABLE m_part_eq_pq CONVERT PARTITION TO PARQUET LIST '2024-01-02'");
            engine.releaseAllReaders();

            for (String table : new String[]{"m_part_eq", "m_part_eq_pq"}) {
                assertCoveredAndReference(
                        "SELECT ts, sym, val FROM " + table + " WHERE sym = 'A' AND ts IN '2024-01-02' ORDER BY ts",
                        "ts",
                        """
                                ts\tsym\tval
                                2024-01-02T02:00:00.000000Z\tA\t50.0
                                """
                );
            }
        });
    }

    @Test
    public void testPartialIsNullAcrossColumnTop() throws Exception {
        // Matrix rows 7 and 8 together, which is how a NULL-key scan meets them: rows 0
        // and 1 sit BELOW the top and carry an implicit NULL the chain has no posting
        // for, row 3 sits ABOVE it and carries an explicit NULL that the chain does hold
        // a posting for. Both must come back, in row order, each with its own INCLUDE
        // value and its own timestamp.
        assertMemoryLeak(() -> {
            createMatrixTable("m_part_null");
            createMatrixTable("m_part_null_pq");
            execute("ALTER TABLE m_part_null_pq CONVERT PARTITION TO PARQUET LIST '2024-01-02'");
            engine.releaseAllReaders();

            for (String table : new String[]{"m_part_null", "m_part_null_pq"}) {
                assertCoveredAndReference(
                        "SELECT ts, sym, val FROM " + table + " WHERE sym = null AND ts IN '2024-01-02' ORDER BY ts",
                        "ts",
                        """
                                ts\tsym\tval
                                2024-01-02T00:00:00.000000Z\t\t30.0
                                2024-01-02T01:00:00.000000Z\t\t40.0
                                2024-01-02T03:00:00.000000Z\t\t60.0
                                """
                );
            }
        });
    }

    @Test
    public void testSampleByFirstLastThrowsOnPartitionCarryingTheColumn() throws Exception {
        // Matrix row 10, a KNOWN GAP pinned as it stands and unrelated to the covered
        // path. SampleByFirstLast needs an IndexFrameCursor, and no posting reader
        // implements one; SqlCodeGenerator never gates the factory on the index type, so
        // any partition that actually carries a POSTING index reaches the default throw.
        // Implementing getFrameCursor on the posting readers -- or gating the factory on
        // IndexType.BITMAP -- turns this test red. It fires for both keys and for both
        // partition states that carry the column.
        assertMemoryLeak(() -> {
            createMatrixTable("m_sbfl_throw");
            for (String day : new String[]{"2024-01-02", "2024-01-03"}) {
                for (String key : new String[]{"null", "'A'"}) {
                    assertThrowsUnsupported("""
                            SELECT ts, sym, first(other) fo, last(other) lo FROM m_sbfl_throw
                            WHERE sym = %s AND ts IN '%s'
                            SAMPLE BY 1h ALIGN TO FIRST OBSERVATION
                            """.formatted(key, day));
                }
            }
        });
    }

    /**
     * Asserts the covered scan returns {@code expected} AND agrees with the plain index
     * scan over the same predicate. The reference is the stronger half: it reads the
     * same rows through the base columns without the covering factory, so a covered
     * value that drifts from the column it was copied from fails here even if
     * {@code expected} was written to match the drift.
     */
    /**
     * Runs the whole-table scan for one key against one of the three index kinds. Only
     * the covering factory gives up random access, so the flag follows the table.
     */
    private void assertMatrixRows(String table, String key, boolean isCovering, String expected) throws Exception {
        QueryAssertion assertion = assertQuery("SELECT ts, sym, val FROM " + table + " WHERE sym = " + key + " ORDER BY ts")
                .noLeakCheck()
                .sizeMayVary()
                .timestamp("ts");
        if (isCovering) {
            assertion = assertion.noRandomAccess();
        }
        assertion.returns(expected);
    }

    private void assertCoveredAndReference(String sql, String designatedTimestamp, String expected) throws Exception {
        assertQuery(sql)
                .noLeakCheck()
                .noRandomAccess()
                .sizeMayVary()
                .timestamp(designatedTimestamp)
                .withPlanContaining("CoveringIndex")
                .returns(expected);
        assertSqlCursors(sql, sql.replace("SELECT ", "SELECT /*+ no_covering */ "));
    }

    private static void assertThrowsUnsupported(String sql) throws Exception {
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            //noinspection StatementWithEmptyBody
            while (cursor.hasNext()) {
                // drain; the throw lands on the first frame
            }
            Assert.fail("expected UnsupportedOperationException from SAMPLE BY first/last over a POSTING index");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    /**
     * The matrix data, without the index. {@code val} is the INCLUDE column and
     * {@code other} deliberately is not, so a query over {@code other} cannot be served
     * by the covering factory and reaches SampleByFirstLast instead.
     */
    private static void createBaseTable(String name) throws Exception {
        execute("CREATE TABLE " + name + " (ts TIMESTAMP, val DOUBLE, other DOUBLE)" +
                " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        // 2024-01-01 in full, and the first two rows of 2024-01-02, land before sym exists.
        execute("""
                INSERT INTO %s VALUES
                ('2024-01-01T00:00:00', 10.0, 100.0),
                ('2024-01-01T01:00:00', 20.0, 200.0),
                ('2024-01-02T00:00:00', 30.0, 300.0),
                ('2024-01-02T01:00:00', 40.0, 400.0)
                """.formatted(name));
        // ADD COLUMN records a column top only on the partition that is open for append,
        // so 2024-01-02 gets top == 2 while 2024-01-01 gets no record at all and reports
        // its whole row count as the top.
        execute("ALTER TABLE " + name + " ADD COLUMN sym SYMBOL");
        // The rest of 2024-01-02 sits at or above that top: one real key, and one
        // EXPLICIT NULL the posting chain does carry an entry for. 2024-01-03 is written
        // entirely after sym exists, so it has no top.
        execute("""
                INSERT INTO %s VALUES
                ('2024-01-02T02:00:00', 50.0, 500.0, 'A'),
                ('2024-01-02T03:00:00', 60.0, 600.0, NULL),
                ('2024-01-03T00:00:00', 70.0, 700.0, 'A'),
                ('2024-01-03T01:00:00', 80.0, 800.0, 'B')
                """.formatted(name));
    }

    private static void createMatrixTable(String name) throws Exception {
        createBaseTable(name);
        execute("ALTER TABLE " + name + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (val)");
        engine.releaseAllWriters();
        engine.releaseAllReaders();
    }
}
