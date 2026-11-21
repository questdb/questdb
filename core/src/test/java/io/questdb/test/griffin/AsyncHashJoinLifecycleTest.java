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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * Compiled factories of the parallel hash join whose tables change between executions. A query
 * that the planner admitted because its build was native when it compiled, with an ON residual or a
 * post-join filter over the build's VARCHAR payload, runs again after the build, and then the probe,
 * converted to Parquet, and after both converted back. A stack of three joins runs again after its
 * later stages' tables were truncated, grew past the first join's other input, so that the first join
 * swaps, and changed their column lists, which makes the factory stale. Every execution returns the
 * rows of the ordinary plan compiled at that time, and leaves no slot, ring byte or reader behind.
 */
public class AsyncHashJoinLifecycleTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
        sqlExecutionContext.setParallelHashJoinEnabled(true);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 16);
    }

    @After
    public void restorePageFrameSizes() {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
    }

    @Test
    public void testStackedFactoryAcrossTruncateGrowthAndAlter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT (x % 13)::INT k, (x % 5)::LONG l, x v, timestamp_sequence('2024-01-01', 600_000_000) ts FROM long_sequence(200)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE d1 AS (SELECT (x % 11)::INT k, x w, (x % 4)::LONG l FROM long_sequence(60))");
            execute("CREATE TABLE d2 AS (SELECT (x % 5)::LONG l, x w2 FROM long_sequence(20))");
            execute("CREATE TABLE d3 AS (SELECT (x % 4)::LONG l, x w3 FROM long_sequence(12))");
            sqlExecutionContext.changePageFrameSizes(8, 8);
            // A first INNER join that swaps while d1 holds fewer rows than t, an INNER later stage and
            // a LEFT one, whose keys read d1's column: from the probe, or, swapped, from the first
            // stage's payload.
            final String sql = "SELECT d1.w, t.v, d2.w2, d3.w3 FROM d1 JOIN t ON d1.k = t.k JOIN d2 ON d1.l = d2.l LEFT JOIN d3 ON d1.l = d3.l";
            RecordCursorFactory factory = select(sql);
            try {
                assertExecution(factory, sql, 3, true);
                // An empty later stage of a LEFT join yields NULLs; an empty INNER one yields no row.
                execute("TRUNCATE TABLE d3");
                assertExecution(factory, sql, 3, true);
                TestUtils.assertContains(result(factory, sqlExecutionContext), "\tnull:LONG\n");
                execute("TRUNCATE TABLE d2");
                assertExecution(factory, sql, 3, true);
                Assert.assertEquals("", rows(result(factory, sqlExecutionContext)));
                execute("INSERT INTO d2 SELECT (x % 5)::LONG, -x FROM long_sequence(7)");
                execute("INSERT INTO d3 SELECT (x % 4)::LONG, -x FROM long_sequence(3)");
                assertExecution(factory, sql, 3, true);
                // d1 outgrows t: the first join hashes t from now on, without a new compile, and the
                // later keys read d1's column from the probe.
                execute("INSERT INTO d1 SELECT (x % 11)::INT, x + 1_000, (x % 4)::LONG FROM long_sequence(300)");
                assertExecution(factory, sql, 3, false);
                // A later stage's changed column list makes the cached factory stale, and a new compile
                // reads the same rows.
                for (String alter : new String[]{
                        "ALTER TABLE d3 ADD COLUMN extra INT",
                        "ALTER TABLE d2 ALTER COLUMN w2 TYPE INT",
                        "ALTER TABLE d3 DROP COLUMN extra",
                }) {
                    execute(alter);
                    try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                        Assert.fail("expected a stale factory after " + alter);
                    } catch (TableReferenceOutOfDateException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "cached query plan cannot be used because table schema has changed");
                    }
                    assertReleased(find(factory));
                    factory.close();
                    factory = null;
                    factory = select(sql);
                    assertExecution(factory, sql, 3, false);
                }
            } finally {
                if (factory != null) {
                    factory.close();
                }
            }
        });
    }

    @Test
    public void testStorageDriftBetweenExecutions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT (x % 13)::INT k, (x % 5)::LONG l, x v, ('p' || (x % 7))::VARCHAR pv, timestamp_sequence('2024-01-01', 600_000_000) ts FROM long_sequence(400)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE d AS (SELECT (x % 11)::INT k, x w, CASE WHEN x % 5 = 0 THEN NULL ELSE 'p' || (x % 9) END::VARCHAR vc, timestamp_sequence('2024-01-01', 900_000_000) dts FROM long_sequence(300)) TIMESTAMP(dts) PARTITION BY DAY");
            execute("CREATE TABLE e AS (SELECT (x % 5)::LONG l, x ew FROM long_sequence(20))");
            sqlExecutionContext.changePageFrameSizes(8, 8);
            // A residual and post-join filters over d's VARCHAR payload, which the probe tasks read,
            // over one stage and over a stack; the planner admits them while d is native.
            final String[] sqls = {
                    "SELECT t.k, t.v, d.w, d.vc FROM t LEFT JOIN d ON t.k = d.k AND d.vc > t.pv",
                    "SELECT t.k, t.v, d.w, d.vc FROM t JOIN d ON t.k = d.k WHERE d.vc < t.pv AND t.ts IN '2024-01-02'",
                    "SELECT t.k, t.v, d.w, d.vc FROM t LEFT JOIN d ON t.k = d.k WHERE d.vc IS NULL OR length(d.vc) > 1",
                    "SELECT t.k, t.v, d.w, e.ew FROM t LEFT JOIN d ON t.k = d.k AND d.vc > t.pv JOIN e ON t.l = e.l WHERE length(d.vc) > e.ew % 3",
            };
            for (String sql : sqls) {
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(sql, join);
                    assertStorageExecution(factory, sql, "native");
                    execute("ALTER TABLE d CONVERT PARTITION TO PARQUET WHERE dts >= 0");
                    assertStorageExecution(factory, sql, "d parquet");
                    // A new compile keeps the ordinary plan over the Parquet build, while the
                    // cached factory still runs the operator.
                    assertOrdinaryPlan(sql, sqlExecutionContext);
                    execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
                    assertStorageExecution(factory, sql, "t and d parquet");
                    execute("ALTER TABLE d CONVERT PARTITION TO NATIVE WHERE dts >= 0");
                    execute("ALTER TABLE t CONVERT PARTITION TO NATIVE WHERE ts >= 0");
                    assertStorageExecution(factory, sql, "native again");
                    assertParallelHashJoin(sql, sqlExecutionContext);
                }
            }
        });
    }

    private static void assertExecution(RecordCursorFactory factory, String sql, int stageCount, boolean isSwapped) throws SqlException {
        final AsyncHashJoinRecordCursorFactory join = find(factory);
        Assert.assertNotNull(sql, join);
        Assert.assertEquals(stageCount, join.getSpec().getStageCount());
        Assert.assertEquals(sql, ordinary(sql, sqlExecutionContext), result(factory, sqlExecutionContext));
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertEquals(sql, isSwapped, join.getAtom().isSwapped());
            cursor.hasNext();
        }
        assertReleased(join);
    }

    private static void assertReleased(AsyncHashJoinRecordCursorFactory join) {
        for (int k = 0, n = join.getSpec().getStageCount(); k < n; k++) {
            Assert.assertNull(join.getAtom().getStage(k).getFrozenBuild());
        }
        Assert.assertEquals(0, join.getAtom().getRing().getAllocatedBytes());
        Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
        Assert.assertEquals(0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
        Assert.assertEquals(0, engine.getBusyReaderCount());
    }

    // The cached factory, which runs the operator whatever the storage, returns the rows of the
    // ordinary plan compiled for the storage of the moment, and twice.
    private static void assertStorageExecution(RecordCursorFactory factory, String sql, String label) throws SqlException {
        final String expected = ordinary(sql, sqlExecutionContext);
        Assert.assertEquals(sql + " [" + label + "]", expected, result(factory, sqlExecutionContext));
        Assert.assertEquals(sql + " [" + label + "]", expected, result(factory, sqlExecutionContext));
        assertReleased(find(factory));
    }

}
