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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.mv.MaterializedViewRefreshJob;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;


public class MatViewReloadOnRestartTest extends AbstractBootstrapTest {

    public static TestServerMain startWithEnvVariables(String... envs) {
        assert envs.length % 2 == 0;

        Map<String, String> envMap = new HashMap<>();
        for (int i = 0; i < envs.length; i += 2) {
            envMap.put(envs[i], envs[i + 1]);
        }
        TestServerMain serverMain = new TestServerMain(newBootstrapWithEnvVariables(envMap)) {
            @Override
            protected void setupMatViewRefreshJob(
                    WorkerPool workerPool,
                    CairoEngine engine,
                    int sharedWorkerCount
            ) {
            }

            @Override
            protected void setupWalApplyJob(
                    WorkerPool workerPool,
                    CairoEngine engine,
                    int sharedWorkerCount
            ) {
            }
        };
        serverMain.start();
        return serverMain;
    }

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testMatViewsReloadOnServerStart() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain main1 = startWithEnvVariables(
                    PropertyKey.CAIRO_MAT_VIEW_ENABLED.getEnvVarName(), "true",
                    PropertyKey.DEV_MODE_ENABLED.getEnvVarName(), "true"
            )) {
                execute(main1, "create table base_price (" +
                        "sym varchar, price double, ts timestamp" +
                        ") timestamp(ts) partition by DAY WAL"
                );

                createMatView(main1, "price_1h", "select sym, last(price) as price, ts from base_price sample by 1h");

                execute(main1, "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                        ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                        ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                        ",('gbpusd', 1.321, '2024-09-10T13:02')"
                );
                drainWalQueue(main1.getEngine());

                MaterializedViewRefreshJob refreshJob = new MaterializedViewRefreshJob(main1.getEngine());
                refreshJob.run(0);
                drainWalQueue(main1.getEngine());

                assertSql(main1,
                        "sym\tprice\tts\n" +
                                "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                                "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                                "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n",
                        "price_1h order by ts, sym"
                );

                execute(main1, "insert into base_price values('gbpusd', 1.319, '2024-09-10T12:05')" +
                        ",('gbpusd', 1.325, '2024-09-10T13:03')"
                );
                drainWalQueue(main1.getEngine());

                refreshJob.run(0);
                drainWalQueue(main1.getEngine());

                String expected = "sym\tprice\tts\n" +
                        "gbpusd\t1.319\t2024-09-10T12:00:00.000000Z\n" +
                        "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                        "gbpusd\t1.325\t2024-09-10T13:00:00.000000Z\n";

                assertSql(main1, expected, "select sym, last(price) as price, ts from base_price sample by 1h order by ts, sym");
                assertSql(main1, expected, "price_1h order by ts, sym");
            }

            try (final TestServerMain main2 = startWithEnvVariables(
                    PropertyKey.CAIRO_MAT_VIEW_ENABLED.getEnvVarName(), "true",
                    PropertyKey.DEV_MODE_ENABLED.getEnvVarName(), "true"
            )) {

                String expected = "sym\tprice\tts\n" +
                        "gbpusd\t1.319\t2024-09-10T12:00:00.000000Z\n" +
                        "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                        "gbpusd\t1.325\t2024-09-10T13:00:00.000000Z\n";

                assertSql(main2, expected, "select sym, last(price) as price, ts from base_price sample by 1h order by ts, sym");
                assertSql(main2, expected, "price_1h order by ts, sym");

                execute(main2, "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:10')" +
                        ",('gbpusd', 1.327, '2024-09-10T13:03')"
                );

                drainWalQueue(main2.getEngine());

                MaterializedViewRefreshJob refreshJob = new MaterializedViewRefreshJob(main2.getEngine());
                refreshJob.run(0);
                drainWalQueue(main2.getEngine());

                expected = "sym\tprice\tts\n" +
                        "gbpusd\t1.32\t2024-09-10T12:00:00.000000Z\n" +
                        "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                        "gbpusd\t1.327\t2024-09-10T13:00:00.000000Z\n";

                assertSql(main2, expected, "select sym, last(price) as price, ts from base_price sample by 1h order by ts, sym");
                assertSql(main2, expected, "price_1h order by ts, sym");
            }
        });
    }

    @Test
    public void testMatViewsReloadOnServerStartMissedBaseTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain main1 = startWithEnvVariables(
                    PropertyKey.CAIRO_MAT_VIEW_ENABLED.getEnvVarName(), "true",
                    PropertyKey.DEV_MODE_ENABLED.getEnvVarName(), "true"
            )) {
                execute(main1, "create table base_price (" +
                        "sym varchar, price double, ts timestamp" +
                        ") timestamp(ts) partition by DAY WAL"
                );

                createMatView(main1, "price_1h", "select sym, last(price) as price, ts from base_price sample by 1h");

                execute(main1, "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                        ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                        ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                        ",('gbpusd', 1.321, '2024-09-10T13:02')"
                );
                drainWalQueue(main1.getEngine());

                MaterializedViewRefreshJob refreshJob = new MaterializedViewRefreshJob(main1.getEngine());
                refreshJob.run(0);
                drainWalQueue(main1.getEngine());

                assertSql(main1,
                        "sym\tprice\tts\n" +
                                "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                                "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                                "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n",
                        "price_1h order by ts, sym"
                );
                execute(main1, "drop table base_price");
            }

            try (final TestServerMain main2 = startWithEnvVariables(
                    PropertyKey.CAIRO_MAT_VIEW_ENABLED.getEnvVarName(), "true",
                    PropertyKey.DEV_MODE_ENABLED.getEnvVarName(), "true"
            )) {

                MaterializedViewRefreshJob refreshJob = new MaterializedViewRefreshJob(main2.getEngine());
                refreshJob.run(0);

                assertSql(main2,
                        "last_error\tlast_error_code\n" +
                                "table does not exist [table=base_price]\t-105\n",
                        "select last_error, last_error_code from views"
                );
            }
        });
    }

    private static void assertSql(TestServerMain serverMain, final String expected, final String sql) {
        serverMain.assertSql(sql, expected);
    }

    private static void createMatView(TestServerMain serverMain, final String viewName, final String viewSql) {
        execute(serverMain, "create materialized view " + viewName + " as (" + viewSql + ") partition by DAY");
    }

    private static void execute(TestServerMain serverMain, final String sql) {
        serverMain.ddl(sql);
    }

    private void dropMatView(TestServerMain serverMain, final String view) {
        execute(serverMain, "drop table " + view);
    }
}
