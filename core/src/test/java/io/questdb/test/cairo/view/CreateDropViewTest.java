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
                    """
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
                            """,
                    TABLE1,
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    """
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
                            """,
                    TABLE2,
                    "ts",
                    true,
                    true
            );

            assertQueryNoLeakCheck(
                    """
                            ts\tk\tv_max
                            1970-01-01T00:00:50.000000Z\tk5\t5
                            1970-01-01T00:01:00.000000Z\tk6\t6
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            """,
                    VIEW1
            );

            assertQueryNoLeakCheck(
                    """
                            ts\tk2\tv_max
                            1970-01-01T00:01:10.000000Z\tk2_7\t7
                            1970-01-01T00:01:20.000000Z\tk2_8\t8
                            """,
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
    public void testDeclareCreateView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE VIEW foo AS (DECLARE @x := 1, @y := 2 SELECT @x + @y)");
            assertSql(
                    """
                            view_name\tview_sql\tview_table_dir_name\tinvalidation_reason\tview_status
                            foo\tDECLARE @x := 1, @y := 2 SELECT @x + @y\tfoo~1\t\tvalid
                            """,
                    "select view_name, view_sql, view_table_dir_name, invalidation_reason, view_status from views()"
            );
            assertSql(
                    """
                            column
                            3
                            """,
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
                    """
                            ts\tk\tv_max
                            1970-01-01T00:00:50.000000Z\tk5\t5
                            1970-01-01T00:01:00.000000Z\tk6\t6
                            1970-01-01T00:01:10.000000Z\tk7\t7
                            1970-01-01T00:01:20.000000Z\tk8\t8
                            """,
                    VIEW1
            );

            assertQueryNoLeakCheck(
                    """
                            ts\tv_max
                            1970-01-01T00:01:10.000000Z\t7
                            1970-01-01T00:01:20.000000Z\t8
                            """,
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
}
