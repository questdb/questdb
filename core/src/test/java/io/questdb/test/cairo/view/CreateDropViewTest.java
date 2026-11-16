/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cairo.view;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CreateDropViewTest extends AbstractViewTest {
    // an aggregate over a view that orders x by a column other than its designated timestamp, see
    // createOrderedViewBase(), whose plan holds native memory from the moment it is generated
    private static final String GROUP_BY_OVER_ORDERED_VIEW = "SELECT k, sum(a) s FROM ordered_x";
    // a window function over the same view
    private static final String WINDOW_OVER_ORDERED_VIEW = "SELECT ts, k, sum(a) OVER () s FROM ordered_x";
    // a window function over the columns of a PIVOT whose IN sub-query reads cats, see createPivotBase()
    private static final String WINDOW_OVER_PIVOT = """
            SELECT grp, sum(c1) OVER () s
            FROM (SELECT * FROM data PIVOT (sum(val) FOR cat IN (SELECT c FROM cats) GROUP BY grp))""";

    @Before
    @Override
    public void setUp() {
        super.setUp();
        // Compilers capture the retry limit at construction, while tests change the configuration.
        engine.getSqlCompilerPool().releaseAll();
    }

    @Test
    public void testCreateDropConcurrent() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table price (" +
                            "  sym varchar, price double, ts timestamp" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            execute("insert into price values('gbpusd', 1.320, now())");
            drainWalQueue();

            final int iterations = 100;
            final CyclicBarrier barrier = new CyclicBarrier(2);
            final AtomicInteger errorCounter = new AtomicInteger();
            final AtomicInteger createCounter = new AtomicInteger();

            final Thread creator = new Thread(() -> {
                try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                    barrier.await();
                    for (int i = 0; i < iterations; i++) {
                        // use test alloc() function to make sure that we always free the factory
                        execute(
                                "create view if not exists price_view as (" +
                                        "select sym, alloc(42), last(price) as price, ts from price sample by 1h" +
                                        ")",
                                executionContext
                        );
                        drainWalQueue();
                        createCounter.incrementAndGet();
                    }
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    errorCounter.incrementAndGet();
                } finally {
                    Path.clearThreadLocals();
                }
            }, "create-view-thread");
            creator.start();

            final Thread dropper = new Thread(() -> {
                try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                    barrier.await();
                    int knownCount;
                    int droppedAt = 0;
                    while ((knownCount = createCounter.get()) < iterations && errorCounter.get() == 0) {
                        if (knownCount > droppedAt) {
                            execute("drop view if exists price_view", executionContext);
                            droppedAt = createCounter.get();
                            drainWalQueue();
                        } else {
                            Os.sleep(1);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    errorCounter.incrementAndGet();
                } finally {
                    Path.clearThreadLocals();
                }
            }, "drop-view-thread");
            dropper.start();

            creator.join();
            dropper.join();

            assertEquals(0, errorCounter.get());
        });
    }

    @Test
    public void testCreateViewBasedOnTable() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4";
            createView(VIEW1, query1, TABLE1);

            final String query2 = "select ts, k2, max(v) as v_max from " + TABLE2 + " where v > 6";
            createView(VIEW2, query2, TABLE2);

            assertQuery(TABLE1)
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tk\tk2\tv
                            1970-01-01T00:00:00.000000Z\tk0\tk2_0\t0
                            1970-01-01T00:00:10.000000Z\tk1\tk2_1\t1
                            1970-01-01T00:00:20.000000Z\tk2\tk2_2\t2
                            1970-01-01T00:00:30.000000Z\tk3\tk2_3\t3
                            1970-01-01T00:00:40.000000Z\tk4\tk2_4\t4
                            1970-01-01T00:00:50.000000Z\tk5\tk2_5\t5
                            1970-01-01T00:01:00.000000Z\tk6\tk2_6\t6
                            1970-01-01T00:01:10.000000Z\tk7\tk2_7\t7
                            1970-01-01T00:01:20.000000Z\tk8\tk2_8\t8
                            """);

            assertQuery(TABLE2)
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tk\tk2\tv
                            1970-01-01T00:00:00.000000Z\tk0\tk2_0\t0
                            1970-01-01T00:00:10.000000Z\tk1\tk2_1\t1
                            1970-01-01T00:00:20.000000Z\tk2\tk2_2\t2
                            1970-01-01T00:00:30.000000Z\tk3\tk2_3\t3
                            1970-01-01T00:00:40.000000Z\tk4\tk2_4\t4
                            1970-01-01T00:00:50.000000Z\tk5\tk2_5\t5
                            1970-01-01T00:01:00.000000Z\tk6\tk2_6\t6
                            1970-01-01T00:01:10.000000Z\tk7\tk2_7\t7
                            1970-01-01T00:01:20.000000Z\tk8\tk2_8\t8
                            """);

            assertQuery(VIEW1)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts\tk\tv_max
                            1970-01-01T00:00:50.000000Z\tk5\t5
                            1970-01-01T00:01:00.000000Z\tk6\t6
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            """);

            assertQuery(VIEW2)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts\tk2\tv_max
                            1970-01-01T00:01:10.000000Z\tk2_7\t7
                            1970-01-01T00:01:20.000000Z\tk2_8\t8
                            """);
        });
    }

    @Test
    public void testCreateViewBasedOnView() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4";
            createView(VIEW1, query1, TABLE1);

            final String query2 = VIEW1 + " where v_max > 6";
            createView(VIEW2, query2, TABLE1, VIEW1);

            final String query3 = VIEW2 + " where v_max > 7";
            createView(VIEW3, query3, TABLE1, VIEW1, VIEW2);

            final String query4 = "select date_trunc('hour', ts), avg(v_max) as v_avg from " + VIEW1;
            createView(VIEW4, query4, TABLE1, VIEW1);

            assertQueryAndPlan(
                    """
                            ts\tk\tv_max
                            1970-01-01T00:00:50.000000Z\tk5\t5
                            1970-01-01T00:01:00.000000Z\tk6\t6
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            """,
                    VIEW1,
                    """
                            QUERY PLAN
                            Async Group By workers: 1
                              keys: [ts,k]
                              values: [max(v)]
                              filter: 4<v
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: table1
                            """,
                    VIEW1
            );

            assertQueryAndPlan(
                    """
                            ts\tk\tv_max
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            """,
                    VIEW2,
                    null,
                    true,
                    false,
                    """
                            QUERY PLAN
                            Filter filter: 6<v_max
                                Async Group By workers: 1
                                  keys: [ts,k]
                                  values: [max(v)]
                                  filter: 4<v
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: table1
                            """,
                    VIEW1, VIEW2
            );

            assertQueryAndPlan(
                    """
                            ts\tk\tv_max
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            """,
                    VIEW3,
                    null,
                    true,
                    false,
                    """
                            QUERY PLAN
                            Filter filter: (6<v_max and 7<v_max)
                                Async Group By workers: 1
                                  keys: [ts,k]
                                  values: [max(v)]
                                  filter: 4<v
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: table1
                            """,
                    VIEW1, VIEW2, VIEW3
            );

            assertQueryAndPlan(
                    """
                            date_trunc\tv_avg
                            1970-01-01T00:00:00.000000Z\t6.5
                            """,
                    VIEW4,
                    """
                            QUERY PLAN
                            GroupBy vectorized: false
                              keys: [date_trunc]
                              values: [avg(v_max)]
                                Async Group By workers: 1
                                  keys: [ts,k]
                                  values: [max(v)]
                                  filter: 4<v
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: table1
                            """,
                    VIEW1, VIEW4
            );
        });
    }

    @Test
    public void testCreateViewCompilesAgainWhenViewChangesBeforeMetadataCompile() throws Exception {
        assertMemoryLeak(() -> {
            createPivotBase();
            execute("CREATE VIEW iv AS (SELECT * FROM data PIVOT (sum(val) FOR cat IN ('c1') GROUP BY grp))");
            drainViewQueue();
            // iv gains a PIVOT IN sub-query as CREATE VIEW generates the plan of its SELECT: after the compile that
            // records the values of the PIVOT IN sub-queries, none, and before the compile of the SELECT as a query of
            // its own for the metadata of the view, which takes the values from it. That compile finds iv out of date,
            // so CREATE VIEW compiles both again: the second attempt runs the sub-query once, and the view has the
            // columns iv has now.
            final AtomicInteger generationCount = new AtomicInteger();
            final AtomicBoolean isAltering = new AtomicBoolean();
            SqlCompilerImpl.setViewFactoryGenerationBarrier(() -> {
                // ALTER VIEW generates the plan of its SELECT too
                if (!isAltering.get() && generationCount.getAndIncrement() == 0) {
                    isAltering.set(true);
                    try {
                        execute("ALTER VIEW iv AS (SELECT * FROM data PIVOT (sum(val) FOR cat IN (SELECT c FROM cats) GROUP BY grp))");
                    } catch (SqlException e) {
                        throw new AssertionError(e);
                    } finally {
                        isAltering.set(false);
                    }
                }
            });
            final ReadCountingSecurityContext securityContext = new ReadCountingSecurityContext("cats");
            try (SqlExecutionContext context = readCountingContext(securityContext)) {
                execute("CREATE VIEW v AS (SELECT *, sum(c1) OVER () s FROM iv)", context);
            } finally {
                SqlCompilerImpl.setViewFactoryGenerationBarrier(null);
            }
            assertEquals(2, generationCount.get());
            assertEquals(1, securityContext.takeReadCount());
            drainViewQueue();
            assertViewState("v");
            assertQuery("SELECT * FROM v ORDER BY grp")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            grp\tc1\tc2\ts
                            g1\t1\t2\t4.0
                            g2\t3\tnull\t4.0
                            """);
        });
    }

    @Test
    public void testCreateViewCompilesSelectOnceWhenReaderOrderChangesNoPlan() throws Exception {
        assertMemoryLeak(() -> {
            createPivotBase();
            // The order a reader of the view inherits changes no plan of a SELECT that orders its rows itself. CREATE
            // VIEW compiles such a SELECT once, and so runs its PIVOT IN sub-query once: a DROP TABLE after that
            // compile changes nothing CREATE VIEW reads.
            dropCatsAsPlanGenerates(() -> execute("CREATE VIEW v AS (" + WINDOW_OVER_PIVOT + " ORDER BY grp)"));
            assertNotNull(engine.getTableTokenIfExists("v"));
            assertDesignatedTimestamp("v", null);
        });
    }

    @Test
    public void testCreateViewDeniedAtOpenFreesItsPlans() throws Exception {
        assertMemoryLeak(() -> {
            createOrderedViewBase();
            // SELECT on ordered_x is revoked as the plan of the SELECT opens, after CREATE VIEW compiled the SELECT as
            // a query of its own for the metadata of the view.
            final String sql = "CREATE VIEW v AS (" + GROUP_BY_OVER_ORDERED_VIEW + ")";
            try (SqlExecutionContext context = contextOf(new RevokingSecurityContext("ordered_x"))) {
                assertExceptionNoLeakCheck(sql, "CREATE VIEW ".length(), "permission denied [view=ordered_x]", context);
            }
            assertNull(engine.getTableTokenIfExists("v"));

            execute(sql);
            drainViewQueue();
            assertViewState("v");
            assertGroupByOverOrderedViewRead();
        });
    }

    @Test
    public void testCreateViewFailsWhenItsSelectStopsCompiling() throws Exception {
        assertMemoryLeak(() -> {
            createPivotBase();
            // The window function makes CREATE VIEW compile the SELECT once more, as a query of its own, for the
            // metadata of the view, which compiles the PIVOT IN sub-query again, and takes its values from the first
            // compile. A DROP TABLE between the two compiles fails the second one, and CREATE VIEW reports where the
            // statement names the table.
            final String sql = "CREATE VIEW v AS (" + WINDOW_OVER_PIVOT + ")";
            dropCatsAsPlanGenerates(() -> assertExceptionNoLeakCheck(sql, sql.indexOf("cats"), "table does not exist [table=cats]"));
            assertNull(engine.getTableTokenIfExists("v"));

            execute("CREATE TABLE cats (c SYMBOL)");
            execute("INSERT INTO cats VALUES ('c1'), ('c2')");
            execute(sql);
            drainViewQueue();
            assertViewState("v");
            assertQuery("SELECT * FROM v ORDER BY grp")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            grp\ts
                            g1\t4.0
                            g2\t4.0
                            """);
        });
    }

    @Test
    public void testCreateViewRecordsDesignatedTimestampOfViewCompile() throws Exception {
        // The plan CREATE VIEW opens keeps the order a reader of the view inherits, such as the ORDER BY of a
        // sub-query under a window function, which the SELECT as a query of its own drops. The view records the
        // metadata of the latter, as the view compile that follows CREATE VIEW does, so the catalog stays as it is.
        assertMemoryLeak(() -> {
            for (String wal : new String[]{"WAL", "BYPASS WAL"}) {
                execute("CREATE TABLE x (ts TIMESTAMP, k SYMBOL, a LONG) TIMESTAMP(ts) PARTITION BY DAY " + wal);
                execute("INSERT INTO x VALUES (0, 'k1', 3), (3_600_000_000, 'k2', 1), (7_200_000_000, 'k1', 2)");
                drainWalQueue();
                execute("CREATE VIEW ordered_x AS (SELECT * FROM x ORDER BY a)");
                drainViewQueue();
                // the SELECT of a view and the designated timestamp of its plan as a query of its own
                final String[][] views = {
                        {"SELECT ts, k, sum(a) OVER () s FROM (SELECT * FROM x ORDER BY a)", "ts"},
                        {"SELECT ts, k, avg(a) OVER () s FROM (SELECT * FROM x ORDER BY a)", "ts"},
                        {"SELECT ts, k, count() OVER () s FROM (SELECT * FROM x ORDER BY a)", "ts"},
                        {"SELECT ts, k, max(a) OVER () s FROM (SELECT * FROM x ORDER BY a)", "ts"},
                        {"SELECT ts, k, sum(a) OVER (ORDER BY ts) s FROM (SELECT * FROM x ORDER BY a)", "ts"},
                        {"SELECT ts, k, sum(a) OVER () s FROM (SELECT * FROM x ORDER BY a) WHERE k = 'k1'", "ts"},
                        {"WITH o AS (SELECT * FROM x ORDER BY a) SELECT ts, k, sum(a) OVER () s FROM o", "ts"},
                        {WINDOW_OVER_ORDERED_VIEW, "ts"},
                        // the order a reader inherits changes no plan of these
                        {"SELECT * FROM x", "ts"},
                        {"SELECT ts, k, sum(a) OVER () s FROM (SELECT * FROM x ORDER BY a) ORDER BY ts", "ts"},
                        {"SELECT ts, k, row_number() OVER () n FROM (SELECT * FROM x ORDER BY a)", null},
                        {"SELECT * FROM (SELECT * FROM x ORDER BY a)", null},
                        // the order a reader inherits changes the plan, though not its timestamp
                        {"SELECT k, sum(a) s FROM x", null},
                };
                for (String[] view : views) {
                    assertTimestampOfQueryOfItsOwn(view[0], view[1]);
                    assertCreatedViewTimestamp("CREATE VIEW", view[0], view[1]);
                }
                // the statements that create a view of a name no view has yet
                for (String statement : new String[]{"CREATE VIEW IF NOT EXISTS", "CREATE OR REPLACE VIEW"}) {
                    assertCreatedViewTimestamp(statement, views[0][0], views[0][1]);
                }
                execute("DROP VIEW ordered_x");
                execute("DROP TABLE x");
                drainWalAndViewQueues();
            }
        });
    }

    @Test
    public void testCreateViewRetriesWhenTableChangesAtOpen() throws Exception {
        assertMemoryLeak(() -> {
            createOrderedViewBase();
            // The open of the plan of the SELECT finds x changed, after CREATE VIEW compiled the SELECT as a query of
            // its own for the metadata of the view. CREATE VIEW compiles both again.
            final AlteringSecurityContext securityContext = new AlteringSecurityContext(1);
            try (SqlExecutionContext context = contextOf(securityContext)) {
                execute("CREATE VIEW v AS (" + GROUP_BY_OVER_ORDERED_VIEW + ")", context);
            }
            assertEquals(1, securityContext.changeCount);
            drainViewQueue();
            assertViewState("v");
            assertGroupByOverOrderedViewRead();
        });
    }

    @Test
    public void testCreateViewRetriesWhenTableChangesAtOpenRunOut() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_MAX_RECOMPILE_ATTEMPTS, 0);
        assertMemoryLeak(() -> {
            createOrderedViewBase();
            final AlteringSecurityContext securityContext = new AlteringSecurityContext(1);
            try (SqlExecutionContext context = contextOf(securityContext)) {
                assertExceptionNoLeakCheck(
                        "CREATE VIEW v AS (" + GROUP_BY_OVER_ORDERED_VIEW + ")",
                        0,
                        "cached query plan cannot be used because table schema has changed [table=x",
                        context
                );
            }
            assertEquals(1, securityContext.changeCount);
            assertNull(engine.getTableTokenIfExists("v"));
        });
    }

    @Test
    public void testCreateViewRunsPivotInSubQueryOnce() throws Exception {
        assertMemoryLeak(() -> {
            createPivotBase();
            // a NULL value of the PIVOT IN sub-query names a column too
            execute("INSERT INTO data VALUES (3, 'g2', null, 4)");
            execute("INSERT INTO cats VALUES (null)");
            execute("CREATE TABLE vals (v LONG)");
            // values each group of data has a row of
            execute("INSERT INTO vals VALUES (1), (3)");
            // An aggregate or a window function at the top of the SELECT makes the plan CREATE VIEW opens keep the
            // order a reader requires, so CREATE VIEW compiles the SELECT once more, as a query of its own, for the
            // metadata of the view. That compile takes the values of the PIVOT IN sub-query from the first one.
            final String pivotOfCats = "SELECT * FROM data PIVOT (sum(val) FOR cat IN (SELECT c FROM cats) GROUP BY grp)";
            assertPivotInSubQueryRunsOnce("cats", "SELECT *, sum(c1) OVER () s FROM (" + pivotOfCats + ")", """
                    grp\tc1\tc2\tNULL\ts
                    g1\t1\t2\tnull\t4.0
                    g2\t3\tnull\t4\t4.0
                    """);
            assertPivotInSubQueryRunsOnce("cats", "SELECT count() n FROM (" + pivotOfCats + ")", """
                    n
                    2
                    """);
            assertPivotInSubQueryRunsOnce("cats", "SELECT grp, sum(c2) s FROM (" + pivotOfCats + ")", """
                    grp\ts
                    g1\t2
                    g2\tnull
                    """);
            assertPivotInSubQueryRunsOnce("cats", "WITH p AS (" + pivotOfCats + ") SELECT count() n FROM p", """
                    n
                    2
                    """);
            // the values of a LONG column name the columns as they print
            assertPivotInSubQueryRunsOnce(
                    "vals",
                    "SELECT *, count() OVER () n FROM (SELECT * FROM data PIVOT (sum(val) FOR val IN (SELECT v FROM vals) GROUP BY grp))",
                    """
                            grp\t1\t3\tn
                            g1\t1\tnull\t2
                            g2\tnull\t3\t2
                            """
            );
        });
    }

    @Test
    public void testDeclareCreateView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE VIEW foo AS (DECLARE @x := 1, @y := 2 SELECT @x + @y)");
            assertQuery("select view_name, view_sql, view_table_dir_name, invalidation_reason, view_status from views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status
                            foo\tDECLARE @x := 1, @y := 2 SELECT @x + @y\tfoo~1\t\tvalid
                            """);
            assertQuery("foo")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column
                            3
                            """);
        });
    }

    @Test
    public void testDropView() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4";
            createView(VIEW1, query1, TABLE1);

            final String query2 = "select ts, k2, max(v) as v_max from " + TABLE2 + " where v > 6";
            createView(VIEW2, query2, TABLE2);

            assertQuery(VIEW1)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts\tk\tv_max
                            1970-01-01T00:00:50.000000Z\tk5\t5
                            1970-01-01T00:01:00.000000Z\tk6\t6
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            """);

            assertQuery("select ts, v_max from " + VIEW2)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts\tv_max
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """);

            execute("DROP VIEW " + VIEW1);
            drainWalQueue();
            assertNull(getViewDefinition(VIEW1));

            execute("DROP VIEW IF EXISTS " + VIEW1);
            drainWalQueue();
            assertNull(getViewDefinition(VIEW1));

            execute("DROP VIEW IF EXISTS " + VIEW2);
            drainWalQueue();
            assertNull(getViewDefinition(VIEW2));
        });
    }

    // Asserts the designated timestamp, null for none, of the plan of the SELECT as a query of its own, which is the
    // plan the view compile records the metadata of.
    private static void assertTimestampOfQueryOfItsOwn(String sql, @Nullable String expectedTimestamp) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            assertEquals(expectedTimestamp, timestampOf(factory.getMetadata()));
        }
    }

    // The context of the principal the security context authorizes, which the security context consults.
    private static SqlExecutionContext contextOf(OpeningSecurityContext securityContext) {
        final SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1).with(
                securityContext,
                bindVariableService,
                null,
                -1,
                null
        );
        securityContext.executionContext = context;
        return context;
    }

    // x, and ordered_x, which orders the rows of x by a column other than its designated timestamp
    private static void createOrderedViewBase() throws SqlException {
        execute("CREATE TABLE x (ts TIMESTAMP, k SYMBOL, a LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO x VALUES (0, 'k1', 3), (3_600_000_000, 'k2', 1), (7_200_000_000, 'k1', 2)");
        execute("CREATE VIEW ordered_x AS (SELECT * FROM x ORDER BY a)");
        drainViewQueue();
    }

    // data, and cats, which lists the values of the column cat of data
    private static void createPivotBase() throws SqlException {
        execute("CREATE TABLE data (ts TIMESTAMP, grp SYMBOL, cat SYMBOL, val LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO data VALUES (0, 'g1', 'c1', 1), (1, 'g1', 'c2', 2), (2, 'g2', 'c1', 3)");
        execute("CREATE TABLE cats (c SYMBOL)");
        execute("INSERT INTO cats VALUES ('c1'), ('c2')");
    }

    // Runs the code, which runs CREATE VIEW, with a barrier that drops cats as CREATE VIEW generates the plan of its
    // SELECT, after the compile of the SELECT ran its PIVOT IN sub-query, see createPivotBase().
    private static void dropCatsAsPlanGenerates(TestUtils.LeakProneCode code) throws Exception {
        final AtomicBoolean isDropped = new AtomicBoolean();
        SqlCompilerImpl.setViewFactoryGenerationBarrier(() -> {
            if (isDropped.compareAndSet(false, true)) {
                try {
                    execute("DROP TABLE cats");
                } catch (SqlException e) {
                    throw new AssertionError(e);
                }
            }
        });
        try {
            code.run();
        } finally {
            SqlCompilerImpl.setViewFactoryGenerationBarrier(null);
        }
        assertTrue("the barrier dropped cats", isDropped.get());
    }

    // the name and type of each column of the metadata, and its designated timestamp
    private static String metadataOf(RecordMetadata metadata) {
        final StringSink sink = new StringSink();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            sink.put(metadata.getColumnName(i)).put(':').put(ColumnType.nameOf(metadata.getColumnType(i))).put('\n');
        }
        return sink.put("timestamp: ").put(timestampOf(metadata)).toString();
    }

    // the metadata of the view, see metadataOf()
    private static String metadataOfView(String viewName) {
        try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName(viewName))) {
            return metadataOf(metadata);
        }
    }

    // the context of a principal that may read everything, whose reads of a table the security context counts
    private static SqlExecutionContext readCountingContext(ReadCountingSecurityContext securityContext) {
        return new SqlExecutionContextImpl(engine, 1).with(securityContext, bindVariableService, null, -1, null);
    }

    private static @Nullable String timestampOf(RecordMetadata metadata) {
        final int timestampIndex = metadata.getTimestampIndex();
        return timestampIndex > -1 ? metadata.getColumnName(timestampIndex) : null;
    }

    // Creates the view v with the statement, and asserts its designated timestamp, null for none, before and after the
    // view compile that follows, then drops it.
    private void assertCreatedViewTimestamp(String statement, String viewSql, @Nullable String expectedTimestamp) throws Exception {
        execute(statement + " v AS (" + viewSql + ")");
        assertDesignatedTimestamp("v", expectedTimestamp);
        drainViewQueue();
        assertViewState("v");
        assertDesignatedTimestamp("v", expectedTimestamp);
        execute("DROP VIEW v");
    }

    // Asserts the designated timestamp of the view, null for none: that of its metadata, and the one tables() shows.
    private void assertDesignatedTimestamp(String viewName, @Nullable String expectedTimestamp) throws Exception {
        try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName(viewName))) {
            assertEquals(expectedTimestamp, timestampOf(metadata));
        }
        assertQuery("SELECT designatedTimestamp FROM tables() WHERE table_name = '" + viewName + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("designatedTimestamp\n" + (expectedTimestamp != null ? expectedTimestamp : "") + '\n');
    }

    // Asserts the rows of the view v of GROUP_BY_OVER_ORDERED_VIEW.
    private void assertGroupByOverOrderedViewRead() throws Exception {
        assertQuery("SELECT * FROM v ORDER BY k")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        k\ts
                        k1\t5
                        k2\t1
                        """);
    }

    // Creates the view v of the SELECT, whose PIVOT IN sub-query reads the table, with each statement that creates a
    // view, and asserts that the statement runs the sub-query once, that the view records the metadata of the SELECT
    // as a query of its own, before and after the view compile that follows, and that the view returns the rows,
    // ordered by their first column. Then ALTER VIEW and CREATE OR REPLACE VIEW of the view, which compile the SELECT
    // for the dependencies of the view only, run the sub-query once too.
    private void assertPivotInSubQueryRunsOnce(String tableName, String viewSql, String expectedRows) throws Exception {
        final String expectedMetadata;
        try (RecordCursorFactory factory = select(viewSql)) {
            expectedMetadata = metadataOf(factory.getMetadata());
        }
        final ReadCountingSecurityContext securityContext = new ReadCountingSecurityContext(tableName);
        try (SqlExecutionContext context = readCountingContext(securityContext)) {
            for (String statement : new String[]{"CREATE VIEW", "CREATE VIEW IF NOT EXISTS", "CREATE OR REPLACE VIEW"}) {
                final String sql = statement + " v AS (" + viewSql + ")";
                execute(sql, context);
                assertEquals(sql, 1, securityContext.takeReadCount());
                assertEquals(sql, expectedMetadata, metadataOfView("v"));
                drainViewQueue();
                assertViewState("v");
                assertEquals(sql, expectedMetadata, metadataOfView("v"));
                assertQuery("SELECT * FROM v ORDER BY 1")
                        .noLeakCheck()
                        .expectSize()
                        .returns(expectedRows);

                for (String replacingStatement : new String[]{"ALTER VIEW", "CREATE OR REPLACE VIEW"}) {
                    execute(replacingStatement + " v AS (" + viewSql + ")", context);
                    assertEquals(replacingStatement + " after " + sql, 1, securityContext.takeReadCount());
                    drainViewQueue();
                    assertViewState("v");
                }
                execute("DROP VIEW v");
            }
        }
    }

    // Adds a column to x as the plan of the SELECT of CREATE VIEW opens, the first changeLimit times, as an ALTER TABLE
    // between the compile of the plan and its open would.
    private static final class AlteringSecurityContext extends OpeningSecurityContext {
        private final int changeLimit;
        private int changeCount;

        private AlteringSecurityContext(int changeLimit) {
            this.changeLimit = changeLimit;
        }

        @Override
        void onOpen(ViewDefinition viewDefinition) {
            if (changeCount < changeLimit) {
                changeCount++;
                try {
                    execute("ALTER TABLE x ADD COLUMN extra" + changeCount + " INT");
                } catch (SqlException e) {
                    throw new AssertionError(e);
                }
            }
        }
    }

    // Grants everything, and runs onOpen() as the plan of the SELECT of CREATE VIEW checks SELECT on a view as it
    // opens. CREATE VIEW opens the plan as a metadata probe, which its compiles of the SELECT are not, see contextOf().
    private static abstract class OpeningSecurityContext extends AllowAllSecurityContext {
        private SqlExecutionContext executionContext;

        @Override
        public void authorizeSelect(ViewDefinition viewDefinition) {
            if (executionContext.isMetadataProbe()) {
                onOpen(viewDefinition);
            }
        }

        abstract void onOpen(ViewDefinition viewDefinition);
    }

    // Denies SELECT on the view of the name as the plan of the SELECT of CREATE VIEW opens, after the compile of the
    // SELECT checked it, as a REVOKE between the compile and the open would.
    private static final class RevokingSecurityContext extends OpeningSecurityContext {
        private final String viewName;

        private RevokingSecurityContext(String viewName) {
            this.viewName = viewName;
        }

        @Override
        void onOpen(ViewDefinition viewDefinition) {
            if (viewDefinition.getViewToken().getTableName().equals(viewName)) {
                throw CairoException.authorization().put("permission denied [view=").put(viewName).put(']');
            }
        }
    }
}
