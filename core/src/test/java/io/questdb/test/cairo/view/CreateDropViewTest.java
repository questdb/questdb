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

            final int iterations = 50;
            final CyclicBarrier barrier = new CyclicBarrier(2);
            final AtomicInteger errorCounter = new AtomicInteger();
            final AtomicInteger createCounter = new AtomicInteger();

            final Thread creator = new Thread(() -> {
                try (SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)) {
                    barrier.await();
                    for (int i = 0; i < iterations; i++) {
                        // Use test alloc() function to make sure that we always free the factory.
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
            });
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
            });
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
            drainWalQueue();

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ");");
            drainWalQueue();
            assertViewDefinition(VIEW1, query1);
            assertViewDefinitionFile(VIEW1, query1);
            assertViewStateFile(VIEW1);

            final String query2 = "select ts, k2, max(v) as v_max from " + TABLE2 + " where v > 6";
            execute("CREATE VIEW " + VIEW2 + " AS (" + query2 + ");");
            drainWalQueue();
            assertViewDefinition(VIEW2, query2);
            assertViewDefinitionFile(VIEW2, query2);
            assertViewStateFile(VIEW2);

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
            drainWalQueue();

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ");");
            drainWalQueue();
            assertViewDefinition(VIEW1, query1);
            assertViewDefinitionFile(VIEW1, query1);
            assertViewStateFile(VIEW1);

            // TODO: the below queries make the test to fail with NPE, fix it
            //final String query2 = "select ts, v_max from " + VIEW1;
            //final String query2 = "select date_trunc('hour', ts), avg(v_max) as v_avg from " + VIEW1;

            // TODO: the below query fails, v_max > 7 filter is ignored, fix it
            //final String query2 = "select * from " + VIEW1 + " where v_max > 7";

            final String query2 = "select * from " + VIEW1;
            execute("CREATE VIEW " + VIEW2 + " AS (" + query2 + ");");
            drainWalQueue();
            assertViewDefinition(VIEW2, query2);
            assertViewDefinitionFile(VIEW2, query2);
            assertViewStateFile(VIEW2);

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
                            "1970-01-01T00:00:50.000000Z\tk5\t5\n" +
                            "1970-01-01T00:01:00.000000Z\tk6\t6\n" +
                            "1970-01-01T00:01:10.000000Z\tk7\t7\n" +
                            "1970-01-01T00:01:20.000000Z\tk8\t8\n",
                    VIEW2,
                    "QUERY PLAN\n" +
                            "Async Group By workers: 1\n" +
                            "  keys: [ts,k]\n" +
                            "  values: [max(v)]\n" +
                            "  filter: 4<v\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: table1\n"
            );
        });
    }

    @Test
    public void testDropView() throws Exception {
        assertMemoryLeak(() -> {
            createTable(TABLE1);
            createTable(TABLE2);
            drainWalQueue();

            final String query1 = "select ts, k, max(v) as v_max from " + TABLE1 + " where v > 4";
            execute("CREATE VIEW " + VIEW1 + " AS (" + query1 + ");");
            drainWalQueue();
            assertViewDefinition(VIEW1, query1);
            assertViewDefinitionFile(VIEW1, query1);
            assertViewStateFile(VIEW1);

            final String query2 = "select ts, k2, max(v) as v_max from " + TABLE2 + " where v > 6";
            execute("CREATE VIEW " + VIEW2 + " AS (" + query2 + ");");
            drainWalQueue();
            assertViewDefinition(VIEW2, query2);
            assertViewDefinitionFile(VIEW2, query2);
            assertViewStateFile(VIEW1);

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
}
