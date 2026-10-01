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
 * value for it either. The covering factory hands such an open to its backup -- the plan
 * the query would have got under {@code /*+ no_covering *}{@code /} -- and most of these
 * cases pin which of the two runs and that the two agree.
 * <p>
 * Every case answers correctly on a posting index. Three departures are pinned as such, so
 * that changing any of them turns its test red rather than leaving it silently asserting the
 * old answer:
 * <ul>
 *     <li>{@link #testFullyPresentCoveredScan} takes the backup for the NULL key even
 *     though the queried partition carries no top, because the check is whole-table.</li>
 *     <li>{@code SAMPLE BY} first/last gives up its index-backed factory for any posting
 *     index, not only where a frame cursor would actually be needed -- see
 *     {@link #testSampleByFirstLastOnPartitionCarryingTheColumn}.</li>
 *     <li>{@link #testSampleByFirstLastOverBitmapDropsNullPrefix} is a real bug, and the
 *     only case here that answers wrongly. It is pre-existing, reachable only through a
 *     BITMAP index, and pinned so the case above knows not to use one as its oracle.</li>
 * </ul>
 */
public class CoveringIndexColumnTopMatrixTest extends AbstractCairoTest {

    /**
     * Which plan a query runs: the covering one, the backup the covering factory defers to for a
     * NULL key over a partition carrying a column top, or a plain index scan on a table that has
     * no covering index at all. The three declare different factory properties.
     */
    private enum PlanKind {BACKUP, COVERING, PLAIN}

    @Test
    public void testFullyAbsentEqualsCoveredScan() throws Exception {
        // Matrix row 5. A partition that predates the indexed column can hold no row for
        // a non-NULL key, so it contributes nothing and the scan is not asked to read it.
        assertMemoryLeak(() -> {
            createMatrixTable("m_abs_eq");
            assertCoveredAndReference(
                    "SELECT ts, sym, val FROM m_abs_eq WHERE sym = 'A' AND ts IN '2024-01-01' ORDER BY ts",
                    "ts",
                    true,
                    "ts\tsym\tval\n"
            );
        });
    }

    @Test
    public void testFullyAbsentEqualsSampleByFirstLast() throws Exception {
        // Matrix row 3. `other` sits outside the INCLUDE list, so no covering factory is
        // built and the query reaches the SAMPLE BY paths. It used to reach
        // SampleByFirstLast and survive only because a partition with no index files gets
        // the stand-in null reader, the one non-bitmap reader that answers
        // getFrameCursor(). Now that SqlCodeGenerator picks that factory only for a BITMAP
        // index, this runs the ordinary group-by, which needs no frame cursor at all. The
        // answer is the same: no row of this partition carries sym, so none matches 'A'.
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
                    .withPlanContaining("Sample By")
                    .withPlanNotContaining("SampleByFirstLast")
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
                    false,
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
        // key. Every row of the partition matches, and the ordinary group-by reads their
        // `other` values through the row cursor.
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
                    .withPlanContaining("Sample By")
                    .withPlanNotContaining("SampleByFirstLast")
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
                    false,
                    """
                            sym\tval
                            \t60.0
                            """
            );
            assertCoveredAndReference(
                    "SELECT sym, val FROM m_abs_latest WHERE sym = 'A' LATEST ON ts PARTITION BY sym",
                    null,
                    true,
                    """
                            sym\tval
                            A\t70.0
                            """
            );
        });
    }

    @Test
    public void testFullyAbsentParquetIsNullCoveredScan() throws Exception {
        // Matrix row 6, the Parquet twin of testFullyAbsentIsNullCoveredScan. This was a known
        // gap while the covering factory tried to answer such a partition itself: a Parquet
        // partition keeps its columns inside data.parquet, so the reader maps no native column
        // memory and there was nothing to read. Deferring the whole query to the plain plan
        // instead needs no Parquet handling at all -- that plan already decodes Parquet -- so
        // the rows come back, and the covered scan agrees with the plain one.
        assertMemoryLeak(() -> {
            createMatrixTable("m_pq_abs");
            execute("ALTER TABLE m_pq_abs CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            engine.releaseAllReaders();

            assertCoveredAndReference(
                    "SELECT ts, sym, val FROM m_pq_abs WHERE sym = null AND ts IN '2024-01-01' ORDER BY ts",
                    "ts",
                    false,
                    """
                            ts\tsym\tval
                            2024-01-01T00:00:00.000000Z\t\t10.0
                            2024-01-01T01:00:00.000000Z\t\t20.0
                            """
            );
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
                        true,
                        """
                                ts\tsym\tval
                                2024-01-03T00:00:00.000000Z\tA\t70.0
                                """
                );
                assertCoveredAndReference(
                        "SELECT ts, sym, val FROM " + table + " WHERE sym = null AND ts IN '2024-01-03' ORDER BY ts",
                        "ts",
                        false,
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
                // Only the covering factory gives up random access; the two plain index scans
                // keep it. On the covering table the NULL key defers to the backup, which is a
                // plain scan wearing the covering factory's declared properties.
                final boolean isCoveringTable = "m_cov".equals(table);
                assertMatrixRows(table, "null", isCoveringTable ? PlanKind.BACKUP : PlanKind.PLAIN, expectedNull);
                assertMatrixRows(table, "'A'", isCoveringTable ? PlanKind.COVERING : PlanKind.PLAIN, expectedA);
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
                        true,
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
                        false,
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
    public void testSampleByFirstLastOnPartitionCarryingTheColumn() throws Exception {
        // Matrix row 10, a KNOWN GAP now closed. SampleByFirstLast walks the index through
        // IndexReader.getFrameCursor(), which hands out a raw address into a contiguous run
        // of row ids. Only the BITMAP reader can do that; a posting reader stores row ids
        // encoded, implements no frame cursor, and inherited the interface default that
        // throws UnsupportedOperationException on the first frame. The query compiled and
        // then died, for both keys and both partition states that carry the column.
        // SqlCodeGenerator now picks that factory only for a BITMAP index, so a posting key
        // runs the ordinary SAMPLE BY group-by, which reads the same rows through the row
        // cursor.
        // <p>
        // The oracle is the UNINDEXED twin, not the BITMAP one. See
        // testSampleByFirstLastOverBitmapDropsNullPrefix: the index-backed factory reads only
        // index frames, and no index carries an entry for a row below the column top, so the
        // bitmap fast path drops the null prefix. Comparing against it would pin that bug as
        // if it were the right answer.
        assertMemoryLeak(() -> {
            createMatrixTable("m_sbfl_covering");
            createBaseTable("m_sbfl_posting");
            execute("ALTER TABLE m_sbfl_posting ALTER COLUMN sym ADD INDEX TYPE POSTING");
            createBaseTable("m_sbfl_plain");
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            for (String day : new String[]{"2024-01-02", "2024-01-03"}) {
                for (String key : new String[]{"null", "'A'"}) {
                    final String reference = sampleByFirstLastSql("m_sbfl_plain", key, day);
                    assertSqlCursors(sampleByFirstLastSql("m_sbfl_covering", key, day), reference);
                    assertSqlCursors(sampleByFirstLastSql("m_sbfl_posting", key, day), reference);
                }
            }

            // The routing itself: neither posting kind reaches the index-backed factory.
            for (String table : new String[]{"m_sbfl_covering", "m_sbfl_posting"}) {
                assertQuery(sampleByFirstLastSql(table, "'A'", "2024-01-03"))
                        .noLeakCheck()
                        .assertsPlanNotContaining("SampleByFirstLast");
            }
        });
    }

    @Test
    public void testSampleByFirstLastOverBitmapDropsNullPrefix() throws Exception {
        // A SEPARATE KNOWN GAP, pre-existing and untouched, pinned here because matrix row 10
        // has to know not to use a BITMAP index as its oracle. SampleByFirstLast reads row ids
        // out of index frames and nothing else, and no index -- bitmap or posting -- holds an
        // entry for a row below the indexed column's top. So the NULL key over a partition
        // that carries a top comes back missing its null prefix, while every other plan
        // returns those rows. Only BITMAP still reaches that factory, which is why only
        // BITMAP still shows the loss. Fixing it turns this test red.
        assertMemoryLeak(() -> {
            createBaseTable("m_sbfl_bmp");
            execute("ALTER TABLE m_sbfl_bmp ALTER COLUMN sym ADD INDEX");
            createBaseTable("m_sbfl_none");
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            // 2024-01-02 carries a top of 2: rows 00:00 and 01:00 predate sym and match the
            // NULL key implicitly, 03:00 is an explicit NULL above the top.
            assertQuery(sampleByFirstLastSql("m_sbfl_bmp", "null", "2024-01-02"))
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .timestamp("ts")
                    .withPlanContaining("SampleByFirstLast")
                    .returns("""
                            ts\tsym\tfo\tlo
                            2024-01-02T03:00:00.000000Z\t\t600.0\t600.0
                            """);
            // The same query with no index at all returns the two dropped rows as well.
            assertQuery(sampleByFirstLastSql("m_sbfl_none", "null", "2024-01-02"))
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .timestamp("ts")
                    .withPlanNotContaining("SampleByFirstLast")
                    .returns("""
                            ts\tsym\tfo\tlo
                            2024-01-02T00:00:00.000000Z\t\t300.0\t300.0
                            2024-01-02T01:00:00.000000Z\t\t400.0\t400.0
                            2024-01-02T03:00:00.000000Z\t\t600.0\t600.0
                            """);
        });
    }

    private static String sampleByFirstLastSql(String table, String key, String day) {
        return """
                SELECT ts, sym, first(other) fo, last(other) lo FROM %s
                WHERE sym = %s AND ts IN '%s'
                SAMPLE BY 1h ALIGN TO FIRST OBSERVATION
                """.formatted(table, key, day);
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
    private void assertMatrixRows(String table, String key, PlanKind kind, String expected) throws Exception {
        QueryAssertion assertion = assertQuery("SELECT ts, sym, val FROM " + table + " WHERE sym = " + key + " ORDER BY ts")
                .noLeakCheck()
                .sizeMayVary()
                .timestamp("ts");
        if (kind != PlanKind.PLAIN) {
            // Both the covering plan and the backup it defers to declare no random access.
            assertion = assertion.noRandomAccess();
        }
        if (kind == PlanKind.BACKUP) {
            assertion = assertion.skipRandomAccessProbe();
        }
        assertion.returns(expected);
    }

    /**
     * Asserts the rows, and cross-checks them against the same query read through the plain
     * index scan. The answer must be the same whichever plan runs.
     */
    /**
     * Asserts the rows, which plan runs, and that the two plans agree.
     * <p>
     * Every table here is built by ADDing the indexed column, so every partition carries a top
     * for it. A NULL key therefore has no posting below the top and no sidecar entry to decode,
     * and the factory serves the query from its backup -- the plan {@code /*+ no_covering *}
     * {@code /} would have produced. A non-NULL key stays on the covering plan. The two report
     * The plan is a compile-time artefact, so it names the covering node either way; what it
     * does show is whether a backup was BUILT ({@code backup: true}), which is the compile-time
     * half of the decision. That the backup actually RAN is what the rows prove: the covering
     * plan would answer these with NULL INCLUDE values, having no sidecar entry to decode.
     */
    private void assertCoveredAndReference(
            String sql,
            String designatedTimestamp,
            boolean isCoveringPlan,
            String expected
    ) throws Exception {
        QueryAssertion assertion = assertQuery(sql)
                .noLeakCheck()
                .noRandomAccess()
                .sizeMayVary()
                .timestamp(designatedTimestamp)
                .withPlanContaining(isCoveringPlan ? "CoveringIndex" : "CoveringIndex backup: true");
        if (!isCoveringPlan) {
            // The backup is a PageFrameRecordCursorFactory, which declares no random access for
            // this query shape but hands out a cursor that implements getRecordB() anyway. Pin
            // the declaration and skip the probe.
            assertion = assertion.skipRandomAccessProbe();
        } else {
            assertion = assertion.withPlanNotContaining("backup");
        }
        assertion.returns(expected);
        assertSqlCursors(sql, sql.replace("SELECT ", "SELECT /*+ no_covering */ "));
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
