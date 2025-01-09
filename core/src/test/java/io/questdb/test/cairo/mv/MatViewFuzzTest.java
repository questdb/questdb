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
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.cairo.fuzz.AbstractFuzzTest;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class MatViewFuzzTest extends AbstractFuzzTest {

    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.CAIRO_MAT_VIEW_ENABLED, "true");
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
    }

    @Test
    public void test2LevelDependencyViewFuzz() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            String mvName = testName.getMethodName() + "_mv";
            String mv2Name = testName.getMethodName() + "_mv2";
            fuzzer.createInitialTable(tableName, true);
            Rnd rnd = fuzzer.generateRandom(LOG);

            String viewSql = "select min(c3), max(c3), ts from  " + tableName + " sample by 1h";
            createMatView(viewSql, mvName);

            String view2Sql = "select min(min), max(max), ts from  " + mvName + " sample by 2h";
            createMatView(view2Sql, mv2Name);

            AtomicBoolean stop = new AtomicBoolean();
            Thread refreshJob = startRefreshJob(stop, rnd);

            setFuzzParams(rnd, 0);

            ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(tableName, rnd);
            ObjList<ObjList<FuzzTransaction>> fuzzTransactions = new ObjList<>();
            fuzzTransactions.add(transactions);
            fuzzer.applyManyWalParallel(
                    fuzzTransactions,
                    rnd,
                    tableName,
                    false,
                    true
            );

            stop.set(true);
            refreshJob.join();
            drainWalQueue();

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertSqlCursors(
                        compiler,
                        sqlExecutionContext,
                        viewSql,
                        mvName,
                        LOG
                );

                TestUtils.assertSqlCursors(
                        compiler,
                        sqlExecutionContext,
                        view2Sql,
                        mv2Name,
                        LOG
                );
            }
        });
    }

    @Test
    public void testBaseTableCanHaveColumnsAdded() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = fuzzer.generateRandom(LOG);
            setFuzzParams(rnd, 0.2);
            setFuzzProperties(rnd);
            runMvFuzz(rnd, getTestName(), 1);
        });
    }

    @Test
    public void testManyTablesViewFuzz() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = fuzzer.generateRandom(LOG);
            setFuzzParams(rnd, 0);
            setFuzzProperties(rnd);
            runMvFuzz(rnd, getTestName(), 1 + rnd.nextInt(4));
        });
    }

    @Test
    public void testOneViewFuzz() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            String mvName = testName.getMethodName() + "_mv";
            fuzzer.createInitialTable(tableName, true);
            Rnd rnd = fuzzer.generateRandom(LOG);

            int mins = 1 + rnd.nextInt(300);
            String viewSql = "select min(c3), max(c3), ts from  " + tableName + " sample by " + mins + "m";
            createMatView(viewSql, mvName);

            AtomicBoolean stop = new AtomicBoolean();
            Thread refreshJob = startRefreshJob(stop, rnd);

            setFuzzParams(rnd, 0);

            ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(tableName, rnd);
            ObjList<ObjList<FuzzTransaction>> fuzzTransactions = new ObjList<>();
            fuzzTransactions.add(transactions);
            fuzzer.applyManyWalParallel(
                    fuzzTransactions,
                    rnd,
                    tableName,
                    false,
                    true
            );

            stop.set(true);
            refreshJob.join();
            drainWalQueue();

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertSqlCursors(
                        compiler,
                        sqlExecutionContext,
                        viewSql,
                        mvName,
                        LOG
                );
            }
        });
    }

    private static void createMatView(String viewSql, String mvName) throws SqlException {
        execute(
                "create materialized view " + mvName + " as ("
                        + viewSql
                        + ") partition by DAY"
        );
    }

    private ObjList<FuzzTransaction> createTransactionsAndMv(Rnd rnd, String tableNameBase, String viewSql) throws SqlException, NumericException {
        String tableNameMv = tableNameBase + "_mv";

        fuzzer.createInitialTable(tableNameBase, true);
        createMatView(viewSql, tableNameMv);

        ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(tableNameBase, rnd);

        // Release table writers to reduce memory pressure
        engine.releaseInactive();
        return transactions;
    }

    private void runMvFuzz(Rnd rnd, String testTableName, int tableCount) throws Exception {
        AtomicBoolean stop = new AtomicBoolean();
        ObjList<Thread> refreshJobs = new ObjList<>();
        int refreshJobCount = 1 + rnd.nextInt(4);

        for (int i = 0; i < refreshJobCount; i++) {
            refreshJobs.add(startRefreshJob(stop, rnd));
        }

        ObjList<ObjList<FuzzTransaction>> fuzzTransactions = new ObjList<>();
        ObjList<String> viewSqls = new ObjList<>();

        for (int i = 0; i < tableCount; i++) {
            String tableNameBase = testTableName + "_" + i;
            String viewSql = "select min(c3), max(c3), ts from  " + tableNameBase + " sample by 1h";
            ObjList<FuzzTransaction> transactions = createTransactionsAndMv(rnd, tableNameBase, viewSql);
            fuzzTransactions.add(transactions);
            viewSqls.add(viewSql);
        }

        // Can help to reduce memory consumption.
        engine.releaseInactive();
        fuzzer.applyManyWalParallel(fuzzTransactions, rnd, testTableName, true, true);

        stop.set(true);
        for (int i = 0; i < refreshJobCount; i++) {
            refreshJobs.getQuick(i).join();
        }

        drainWalQueue();
        fuzzer.checkNoSuspendedTables();

        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            for (int i = 0; i < tableCount; i++) {
                String viewSql = viewSqls.getQuick(i);
                String mvName = testTableName + "_" + i + "_mv";
                LOG.info().$("asserting view ").$(mvName).$(" against ").$(viewSql).$();
                TestUtils.assertSqlCursors(
                        compiler,
                        sqlExecutionContext,
                        viewSql,
                        mvName,
                        LOG
                );
            }
        }
    }

    private void setFuzzParams(Rnd rnd, double collAddProb) {
        fuzzer.setFuzzCounts(
                rnd.nextBoolean(),
                rnd.nextInt(2_000_000),
                rnd.nextInt(1000),
                rnd.nextInt(3),
                rnd.nextInt(5),
                rnd.nextInt(1000),
                rnd.nextInt(1_000_000),
                5 + rnd.nextInt(10)
        );

        // Easy, no column manipulations
        fuzzer.setFuzzProbabilities(
                0.0,
                0.0,
                0.0,
                0.0,
                collAddProb,
                0.0,
                0.0,
                0.0,
                1,
                0.0,
                0.0,
                0.0,
                0.0
        );
    }

    private Thread startRefreshJob(AtomicBoolean stop, Rnd outsideRnd) {
        Rnd rnd = new Rnd(outsideRnd.nextLong(), outsideRnd.nextLong());
        Thread th = new Thread(() -> {
            try {
                MatViewRefreshJob refreshJob = new MatViewRefreshJob(engine);
                while (!stop.get()) {
                    refreshJob.run(0);
                    Os.sleep(rnd.nextInt(1000));
                }

                // Run one final time before stopping
                try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                    do {
                        drainWalQueue(walApplyJob);
                    } while (refreshJob.run(0));
                }

            } catch (Throwable throwable) {
                LOG.error().$("Refresh job failed: ").$(throwable).$();
            } finally {
                Path.clearThreadLocals();
                LOG.info().$("Refresh job stopped").$();
            }
        });
        th.start();
        return th;
    }
}
