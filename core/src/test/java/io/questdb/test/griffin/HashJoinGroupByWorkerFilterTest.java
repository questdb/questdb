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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AsyncFilterContext;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByAtom;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.context;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.fused;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.ordinary;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.plan;

/**
 * A regex holds a {@link java.util.regex.Matcher}, so a build filter that uses one is not thread
 * safe. The planner compiles a copy of such a filter per worker, both the build scan's WHERE filter
 * and an outer join's ON condition on the build alone, and a build in rounds runs each frame through
 * the copy of the slot that took the frame. These tests run that build on real workers, where a
 * shared filter would race, and compare the rows with the ordinary plan's.
 */
public class HashJoinGroupByWorkerFilterTest extends AbstractCairoTest {
    // An INNER join builds b, the smaller input, and runs the WHERE regex as its scan's filter, over
    // b's VARCHAR and STRING columns or over its SYMBOL columns cast to those types.
    private static final String[] INNER_QUERIES = {
            "SELECT p.g, count() n, sum(b.w) w, sum(p.v) v FROM p JOIN b ON p.id = b.id WHERE b.name::VARCHAR ~ 'x.*' ORDER BY p.g",
            "SELECT count() n, sum(b.w) w FROM p JOIN b ON p.id = b.id WHERE b.name::VARCHAR ~ 'x.*' AND b.tag::STRING ~ 't[01]'",
            "SELECT p.g, count() n, sum(b.w) w, sum(p.v) v FROM p JOIN b ON p.id = b.id WHERE b.vname ~ 'x.*' ORDER BY p.g",
            "SELECT count() n, sum(b.w) w FROM p JOIN b ON p.id = b.id WHERE b.vname ~ 'x.*' AND b.stag ~ 't[01]'"
    };
    // b's ON condition and its scan's WHERE filter both use a regex, over b's VARCHAR and STRING
    // columns or over its SYMBOL columns cast to those types. The build runs both filters over its
    // page frames, so a variable-size column they read never reaches the row heap. A LEFT join
    // builds b, the input after it, and a RIGHT join, which the planner runs as a LEFT one, builds b too.
    private static final String[] OUTER_QUERIES = {
            """
                    SELECT p.g, count() n, count(b.id) m, sum(b.w) w, sum(p.v) v
                    FROM p LEFT JOIN (SELECT * FROM b WHERE tag::STRING ~ 't[01]') b ON p.id = b.id AND b.name::VARCHAR ~ 'x.*'
                    ORDER BY p.g
                    """,
            """
                    SELECT count() n, count(b.id) m, sum(b.w) w, sum(p.v) v
                    FROM p LEFT JOIN (SELECT * FROM b WHERE tag::STRING ~ 't[01]') b ON p.id = b.id AND b.name::VARCHAR ~ 'x.*'
                    """,
            """
                    SELECT p.g, count() n, count(b.id) m, sum(b.w) w
                    FROM (SELECT * FROM b WHERE tag::STRING ~ 't[01]') b RIGHT JOIN p ON p.id = b.id AND b.name::VARCHAR ~ 'x.*'
                    ORDER BY p.g
                    """,
            // A regex over the SYMBOL column itself keeps per-dictionary state in each copy.
            """
                    SELECT p.g, count() n, count(b.id) m, sum(b.w) w
                    FROM p LEFT JOIN (SELECT * FROM b WHERE tag::STRING ~ 't[01]') b ON p.id = b.id AND b.name ~ 'x.*'
                    ORDER BY p.g
                    """,
            // A composite key stages into the build's map, which partitions per round too.
            """
                    SELECT p.g, count() n, count(b.id) m, sum(b.w) w
                    FROM p LEFT JOIN (SELECT * FROM b WHERE tag::STRING ~ 't[01]') b
                        ON p.id = b.id AND p.g = b.g AND b.name::VARCHAR ~ 'x.*'
                    ORDER BY p.g
                    """,
            """
                    SELECT p.g, count() n, count(b.id) m, sum(b.w) w, sum(p.v) v
                    FROM p LEFT JOIN (SELECT * FROM b WHERE stag ~ 't[01]') b ON p.id = b.id AND b.vname ~ 'x.*'
                    ORDER BY p.g
                    """,
            """
                    SELECT p.g, count() n, count(b.id) m, sum(b.w) w
                    FROM (SELECT * FROM b WHERE stag ~ 't[01]') b RIGHT JOIN p ON p.id = b.id AND b.vname ~ 'x.*'
                    ORDER BY p.g
                    """,
            """
                    SELECT p.g, count() n, count(b.id) m, sum(b.w) w
                    FROM p LEFT JOIN (SELECT * FROM b WHERE stag ~ 't[01]') b
                        ON p.id = b.id AND p.g = b.g AND b.vname ~ 'x.*'
                    ORDER BY p.g
                    """
    };
    private static final int WORKER_COUNT = 4;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        HashJoinBuildMode.PARALLEL.applyProperties(node1.getConfigurationOverrides());
    }

    @Test
    public void testInnerJoinBuildRegexWithFiberWorkers() throws Exception {
        assertWorkerFilters(WorkerPoolMode.FIBER_HOST, INNER_QUERIES, false);
    }

    @Test
    public void testInnerJoinBuildRegexWithLegacyWorkers() throws Exception {
        assertWorkerFilters(WorkerPoolMode.LEGACY, INNER_QUERIES, false);
    }

    @Test
    public void testInnerJoinBuildRegexWithOwnerWorkStealing() throws Exception {
        assertWorkerFilters(null, INNER_QUERIES, false);
    }

    @Test
    public void testOuterJoinBuildRegexWithFiberWorkers() throws Exception {
        assertWorkerFilters(WorkerPoolMode.FIBER_HOST, OUTER_QUERIES, true);
    }

    @Test
    public void testOuterJoinBuildRegexWithLegacyWorkers() throws Exception {
        assertWorkerFilters(WorkerPoolMode.LEGACY, OUTER_QUERIES, true);
    }

    @Test
    public void testOuterJoinBuildRegexWithOwnerWorkStealing() throws Exception {
        assertWorkerFilters(null, OUTER_QUERIES, true);
    }

    // Each slot must filter through a function of its own: the owner's and every worker's are
    // distinct objects, none of them thread safe.
    private static void assertDistinctCopies(String label, Function owner, ObjList<Function> copies) {
        Assert.assertNotNull(label, owner);
        Assert.assertFalse(label, owner.isThreadSafe());
        Assert.assertNotNull(label, copies);
        Assert.assertEquals(label, WORKER_COUNT, copies.size());
        for (int i = 0; i < WORKER_COUNT; i++) {
            final Function copy = copies.getQuick(i);
            Assert.assertNotNull(label, copy);
            Assert.assertNotSame(label, owner, copy);
            Assert.assertSame(label, owner.getClass(), copy.getClass());
            for (int j = 0; j < i; j++) {
                Assert.assertNotSame(label, copies.getQuick(j), copy);
            }
        }
    }

    // The atom exposes neither the filter context of the build scan nor the ON filter copies.
    private static void assertWorkerFilterCopies(String label, AsyncHashJoinGroupByAtom atom, boolean isOuter) throws Exception {
        final AsyncFilterContext buildFilterContext = (AsyncFilterContext) field(atom, "buildFilterContext");
        final ObjList<Function> scanCopies = new ObjList<>();
        for (int i = 0; i < WORKER_COUNT; i++) {
            scanCopies.add(buildFilterContext.getFilter(i));
        }
        assertDistinctCopies(label, buildFilterContext.getFilter(-1), scanCopies);
        if (isOuter) {
            @SuppressWarnings("unchecked") final ObjList<Function> onCopies = (ObjList<Function>) field(atom, "workerBuildOnFilters");
            assertDistinctCopies(label, (Function) field(atom, "buildOnFilter"), onCopies);
        } else {
            Assert.assertNull(label, field(atom, "buildOnFilter"));
        }
    }

    // p holds 6_000 rows, b 2_400 over two days. One in three of b's names contains an x, and half of
    // its tags are t0 or t1, so each regex drops a different share of the build's rows. b holds each
    // name and tag twice: as a SYMBOL, and as a VARCHAR name and a STRING tag. One in eight of the
    // VARCHAR and STRING copies is NULL, which no regex matches.
    private static void createTables(CairoEngine db, SqlExecutionContext context) throws Exception {
        db.execute("CREATE TABLE p (id INT, g INT, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", context);
        db.execute("""
                CREATE TABLE b (id INT, g INT, name SYMBOL, tag SYMBOL, vname VARCHAR, stag STRING, w DOUBLE, ts TIMESTAMP)
                TIMESTAMP(ts) PARTITION BY DAY
                """, context);
        db.execute("""
                INSERT INTO p
                SELECT (x % 1_500)::INT, (x % 7)::INT, x * 0.5, timestamp_sequence('2020-01-01', 30_000_000L)
                FROM long_sequence(6_000)
                """, context);
        db.execute("""
                INSERT INTO b
                SELECT id, g, name, tag,
                    CASE WHEN x % 8 = 0 THEN NULL ELSE name END::VARCHAR,
                    CASE WHEN x % 8 = 0 THEN NULL ELSE tag END::STRING,
                    w, ts
                FROM (
                    SELECT x, (x % 1_200)::INT id, (x % 7)::INT g,
                        CASE WHEN x % 3 = 0 THEN 'x' ELSE 'y' END || (x % 11) name,
                        't' || (x % 4) tag, x * 0.25 w, timestamp_sequence('2020-01-01', 72_000_000L) ts
                    FROM long_sequence(2_400)
                )
                """, context);
    }

    private static Object field(Object target, String name) throws Exception {
        final Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private void assertWorkerFilters(@Nullable WorkerPoolMode mode, String[] queries, boolean isOuter) throws Exception {
        assertMemoryLeak(() -> {
            final TestWorkerPool pool = mode == null ? null : new TestWorkerPool(WORKER_COUNT, mode);
            TestUtils.execute(pool, (db, compiler, ignored) -> {
                createTables(db, ignored);
                try (SqlExecutionContextImpl context = context(db, WORKER_COUNT)) {
                    // Frames of up to 40 rows give b's build about 60 frame tasks per round.
                    context.changePageFrameSizes(40, 40);
                    for (String sql : queries) {
                        final String expected = ordinary(db, sql, context);
                        try (RecordCursorFactory factory = db.select(sql, context)) {
                            final String label = sql + '\n' + plan(factory, context);
                            Assert.assertTrue(label, label.contains("Hash Join Group By"));
                            Assert.assertTrue(label, label.contains("buildFilter: "));
                            Assert.assertEquals(label, isOuter, label.contains("buildOnFilter: "));
                            final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                            assertWorkerFilterCopies(label, atom, isOuter);
                            // Repeated executions give a race between the slots more chances to show.
                            for (int run = 0; run < 3; run++) {
                                try (RecordCursor cursor = factory.getCursor(context)) {
                                    final StringSink sink = new StringSink();
                                    CursorPrinter.println(cursor, factory.getMetadata(), sink, true, true);
                                    Assert.assertEquals(label, expected, sink.toString());
                                    Assert.assertTrue(label, atom.isBuiltInRounds());
                                    Assert.assertTrue(label, atom.getBuildPartitionCount() > 1);
                                }
                            }
                        }
                        Assert.assertNull(sql, context.getMemoryTracker());
                    }
                }
            }, configuration, LOG);
        });
    }
}
