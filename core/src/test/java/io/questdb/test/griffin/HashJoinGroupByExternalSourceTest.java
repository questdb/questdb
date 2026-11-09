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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.plan;

/**
 * Pins how the fused hash join GROUP BY treats an input that reads a file outside the database.
 * <p>
 * A factory that reads such a file reports {@link RecordCursorFactory#usesExternalDataSource()},
 * and a materialized view rejects a sub-query whose factory does. The interface's default answer
 * walks {@link RecordCursorFactory#getBaseFactory()} alone, and the fused factory returns its
 * probe input there, so it would not report a {@code read_parquet()} build. No fused plan takes
 * one, though: the planner fuses a GROUP BY only over a join of two tables, and
 * {@code read_parquet()} is a table function, so the query keeps the ordinary plan, whose join
 * reports both inputs. These tests pin that, with the fused GROUP BY enabled and
 * {@code read_parquet()} serving page frames, which is the input shape the fused plan reads. A
 * planner change that lets such an input into the fused plan fails them, until the fused factory
 * reports its build input too.
 */
public class HashJoinGroupByExternalSourceTest extends AbstractCairoTest {
    private static final String FUSED = "Hash Join Group By";

    @Override
    @Before
    public void setUp() {
        // A parallel read_parquet() serves page frames, as the fused plan's inputs must.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_READ_PARQUET_ENABLED, "true");
        super.setUp();
        inputRoot = root;
    }

    @Test
    public void testMatViewRejectsSubQueryOverReadParquetBuild() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE base (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            for (String join : new String[]{"JOIN", "LEFT JOIN"}) {
                final String subQuery = "SELECT count(*) FROM r " + join + " read_parquet('p.parquet') p ON r.id = p.id";
                final String sql = "CREATE MATERIALIZED VIEW mv AS SELECT ts, sum(v) s FROM base WHERE v > (" + subQuery + ") SAMPLE BY 1h";
                assertExceptionNoLeakCheck(
                        sql,
                        sql.indexOf(subQuery),
                        "non-deterministic function cannot be used in materialized view: sub-query"
                );
            }
            // The same sub-query over the table p passes the check, so the rejection above is the
            // external source's.
            execute("CREATE MATERIALIZED VIEW mv AS SELECT ts, sum(v) s FROM base WHERE v > (SELECT count(*) FROM r JOIN p ON r.id = p.id) SAMPLE BY 1h");
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_name, view_status FROM materialized_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            view_name\tview_status
                            mv\tvalid
                            """);
        });
    }

    @Test
    public void testReadParquetBuildKeepsOrdinaryPlan() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // r holds ids 1 to 6, and p holds ids 1, 2, 3 and 0 twice each, with w = 1 to 8: ids 1,
            // 2 and 3 of r match w = 1 and 5, 2 and 6, and 3 and 7, which sum to 24.
            final String inner = """
                    n\tm\ts
                    6\t6\t24.0
                    """;
            // Ids 4, 5 and 6 of r match no row of p, and keep one row each with a NULL w.
            final String left = """
                    n\tm\ts
                    9\t6\t24.0
                    """;
            // Over two tables the GROUP BY takes the fused plan, which reads no external source.
            assertPlanAndSource("SELECT count(*) n, count(p.w) m, sum(p.w) s FROM r JOIN p ON r.id = p.id", true, false, inner);
            assertPlanAndSource("SELECT count(*) n, count(p.w) m, sum(p.w) s FROM r LEFT JOIN p ON r.id = p.id", true, false, left);
            // A read_parquet() build keeps the ordinary plan, which reports the external source.
            assertPlanAndSource("SELECT count(*) n, count(p.w) m, sum(p.w) s FROM r JOIN read_parquet('p.parquet') p ON r.id = p.id",
                    false, true, inner);
            assertPlanAndSource("SELECT count(*) n, count(p.w) m, sum(p.w) s FROM r LEFT JOIN read_parquet('p.parquet') p ON r.id = p.id",
                    false, true, left);
            // So does a read_parquet() probe.
            assertPlanAndSource("SELECT count(*) n, count(p.w) m, sum(p.w) s FROM read_parquet('p.parquet') p JOIN r ON r.id = p.id",
                    false, true, inner);
            // And a read_parquet() input under a sub-query's projection, or behind a view.
            assertPlanAndSource("SELECT count(*) n, count(p.w) m, sum(p.w) s FROM r JOIN (SELECT id, w FROM read_parquet('p.parquet')) p ON r.id = p.id",
                    false, true, inner);
            execute("CREATE VIEW pv AS (SELECT id, w FROM read_parquet('p.parquet'))");
            drainWalAndViewQueues();
            assertPlanAndSource("SELECT count(*) n, count(p.w) m, sum(p.w) s FROM r JOIN pv p ON r.id = p.id", false, true, inner);
            assertPlanAndSource("SELECT count(*) n, count(p.w) m, sum(p.w) s FROM r LEFT JOIN pv p ON r.id = p.id", false, true, left);
        });
    }

    private void assertPlanAndSource(String sql, boolean isFused, boolean isExternal, String expected) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            final String plan = plan(factory, sqlExecutionContext);
            Assert.assertEquals(plan, isFused, plan.contains(FUSED));
            // The read_parquet() input is a ReadParquetPageFrameRecordCursorFactory, which serves
            // the page frames that the fused plan reads.
            Assert.assertEquals(plan, isExternal, plan.contains("parquet page frame scan"));
            Assert.assertEquals(sql, isExternal, factory.usesExternalDataSource());
        }
        // The test's own memory leak check covers the query, and the one of the assertion would clear
        // the engine, and with it the view's compiled state.
        assertQuery(sql).noLeakCheck().noRandomAccess().expectSize().returns(expected);
    }

    private static void createTables() throws Exception {
        execute("CREATE TABLE r AS (SELECT x::INT id FROM long_sequence(6))");
        execute("CREATE TABLE p AS (SELECT (x % 4)::INT id, x::DOUBLE w FROM long_sequence(8))");
        try (
                Path path = new Path();
                PartitionDescriptor descriptor = new PartitionDescriptor();
                TableReader reader = engine.getReader("p")
        ) {
            path.of(root).concat("p.parquet");
            engine.getConfiguration().getFilesFacade().remove(path.$());
            PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
            PartitionEncoder.encode(descriptor, path);
            Assert.assertTrue(Files.exists(path.$()));
        }
    }
}
