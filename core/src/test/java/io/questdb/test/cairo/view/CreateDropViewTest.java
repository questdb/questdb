/*******************************************************************************
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

import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CreateDropViewTest extends AbstractViewTest {

    @Test
    public void testCreateConstantView() throws Exception {
        final String query1 = "select 42 as col";
        createView(VIEW1, query1);

        assertQueryNoLeakCheck(
                "col\n" +
                        "42\n",
                VIEW1
        );
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

            assertQueryNoLeakCheck(
                    "ts\tk\tk2\tv\n" +
                            "1970-01-01T00:00:00.000000Z\tk0\tk2_0\t0\n" +
                            "1970-01-01T00:00:10.000000Z\tk1\tk2_1\t1\n" +
                            "1970-01-01T00:00:20.000000Z\tk2\tk2_2\t2\n" +
                            "1970-01-01T00:00:30.000000Z\tk3\tk2_3\t3\n" +
                            "1970-01-01T00:00:40.000000Z\tk4\tk2_4\t4\n" +
                            "1970-01-01T00:00:50.000000Z\tk5\tk2_5\t5\n" +
                            "1970-01-01T00:01:00.000000Z\tk6\tk2_6\t6\n" +
                            "1970-01-01T00:01:10.000000Z\tk7\tk2_7\t7\n" +
                            "1970-01-01T00:01:20.000000Z\tk8\tk2_8\t8\n",
                    TABLE1,
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    "ts\tk\tk2\tv\n" +
                            "1970-01-01T00:00:00.000000Z\tk0\tk2_0\t0\n" +
                            "1970-01-01T00:00:10.000000Z\tk1\tk2_1\t1\n" +
                            "1970-01-01T00:00:20.000000Z\tk2\tk2_2\t2\n" +
                            "1970-01-01T00:00:30.000000Z\tk3\tk2_3\t3\n" +
                            "1970-01-01T00:00:40.000000Z\tk4\tk2_4\t4\n" +
                            "1970-01-01T00:00:50.000000Z\tk5\tk2_5\t5\n" +
                            "1970-01-01T00:01:00.000000Z\tk6\tk2_6\t6\n" +
                            "1970-01-01T00:01:10.000000Z\tk7\tk2_7\t7\n" +
                            "1970-01-01T00:01:20.000000Z\tk8\tk2_8\t8\n",
                    TABLE2,
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    "ts\tk\tv_max\n" +
                            "1970-01-01T00:00:50.000000Z\tk5\t5\n" +
                            "1970-01-01T00:01:00.000000Z\tk6\t6\n" +
                            "1970-01-01T00:01:10.000000Z\tk7\t7\n" +
                            "1970-01-01T00:01:20.000000Z\tk8\t8\n",
                    VIEW1
            );

            assertQueryNoLeakCheck(
                    "ts\tk2\tv_max\n" +
                            "1970-01-01T00:01:10.000000Z\tk2_7\t7\n" +
                            "1970-01-01T00:01:20.000000Z\tk2_8\t8\n",
                    VIEW2
            );
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
                    "ts\tk\tv_max\n" +
                            "1970-01-01T00:00:50.000000Z\tk5\t5\n" +
                            "1970-01-01T00:01:00.000000Z\tk6\t6\n" +
                            "1970-01-01T00:01:10.000000Z\tk7\t7\n" +
                            "1970-01-01T00:01:20.000000Z\tk8\t8\n",
                    VIEW1,
                    "QUERY PLAN\n" +
                            "Async Group By workers: 1\n" +
                            "  keys: [ts,k]\n" +
                            "  values: [max(v)]\n" +
                            "  filter: 4<v\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: table1\n"
            );

            assertQueryAndPlan(
                    "ts\tk\tv_max\n" +
                            "1970-01-01T00:01:10.000000Z\tk7\t7\n" +
                            "1970-01-01T00:01:20.000000Z\tk8\t8\n",
                    VIEW2,
                    null,
                    true,
                    false,
                    "QUERY PLAN\n" +
                            "Filter filter: 6<v_max\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [ts,k]\n" +
                            "      values: [max(v)]\n" +
                            "      filter: 4<v\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: table1\n"
            );

            assertQueryAndPlan(
                    "ts\tk\tv_max\n" +
                            "1970-01-01T00:01:20.000000Z\tk8\t8\n",
                    VIEW3,
                    null,
                    true,
                    false,
                    // can optimizer remove the redundant 6<v_max filter?
                    "QUERY PLAN\n" +
                            "Filter filter: (6<v_max and 7<v_max)\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [ts,k]\n" +
                            "      values: [max(v)]\n" +
                            "      filter: 4<v\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: table1\n"
            );

            assertQueryAndPlan(
                    "date_trunc\tv_avg\n" +
                            "1970-01-01T00:00:00.000000Z\t6.5\n",
                    VIEW4,
                    "QUERY PLAN\n" +
                            "GroupBy vectorized: false\n" +
                            "  keys: [date_trunc]\n" +
                            "  values: [avg(v_max)]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [ts,k]\n" +
                            "      values: [max(v)]\n" +
                            "      filter: 4<v\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: table1\n"
            );
        });
    }

    @Test
    public void testDeclareCreateView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE VIEW foo AS (DECLARE @x := 1, @y := 2 SELECT @x + @y)");
            assertSql(
                    "view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status\n" +
                            "foo\tDECLARE @x := 1, @y := 2 SELECT @x + @y\tfoo~1\t\tvalid\n",
                    "select view_name, view_sql, view_table_dir_name, invalidation_reason, view_status from views()"
            );
            assertSql(
                    "column\n" +
                            "3\n",
                    "foo"
            );
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

            assertQueryNoLeakCheck(
                    "ts\tk\tv_max\n" +
                            "1970-01-01T00:00:50.000000Z\tk5\t5\n" +
                            "1970-01-01T00:01:00.000000Z\tk6\t6\n" +
                            "1970-01-01T00:01:10.000000Z\tk7\t7\n" +
                            "1970-01-01T00:01:20.000000Z\tk8\t8\n",
                    VIEW1
            );

            assertQueryNoLeakCheck(
                    "ts\tv_max\n" +
                            "1970-01-01T00:01:10.000000Z\t7\n" +
                            "1970-01-01T00:01:20.000000Z\t8\n",
                    "select ts, v_max from " + VIEW2
            );

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

    @Test
    public void testShowCreateView() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            final String query = "select ts, v+v doubleV, avg(v) from " + TABLE1 + " sample by 30s";
            execute("create view test as (" + query + ")");
            assertQueryNoLeakCheck(
                    "ddl\n" +
                            "CREATE VIEW 'test' AS ( \n" +
                            "select ts, v+v doubleV, avg(v) from table1 sample by 30s\n" +
                            ");\n",
                    "show create view test",
                    null,
                    false
            );
        });
    }

    @Test
    public void testShowCreateViewFail() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);

            final String query = "select ts, v+v doubleV, avg(v) from " + TABLE1 + " sample by 30s";
            execute("create view test as (" + query + ")");

            assertExceptionNoLeakCheck(
                    "show create test",
                    12,
                    "expected 'TABLE' or 'VIEW' or 'MATERIALIZED VIEW'"
            );
        });
    }

    @Test
    public void testShowCreateViewFail2() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            assertExceptionNoLeakCheck(
                    "show create view " + TABLE1,
                    17,
                    "view name expected, got table name"
            );
        });
    }

    @Test
    public void testShowCreateViewFail3() throws Exception {
        assertException(
                "show create view 'test';",
                17,
                "view does not exist [view=test]"
        );
    }
}
