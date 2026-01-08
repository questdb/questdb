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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.view.ViewCompilerJob;
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
import io.questdb.test.sql.RandomSelectGenerator;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class ViewFuzzTest extends AbstractFuzzTest {
    private static final String[] timestampTypes = new String[]{"timestamp", "timestamp_ns"};

    @Test
    public void testBaseTableCanHaveColumnsAdded() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = fuzzer.generateRandom(LOG);
            setFuzzParams(rnd, 0.2);
            setFuzzProperties(rnd);
            runViewFuzz(rnd, getTestName(), 1, true);
        });
    }

    @Test
    public void testManyTablesCompilerJobRace() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = fuzzer.generateRandom(LOG);
            setFuzzParams(rnd, 1_000, 10_000, 0, 0.0);
            setFuzzProperties(rnd);
            runViewFuzz(rnd, getTestName(), 1 + rnd.nextInt(4), false);
        });
    }

    @Test
    public void testManyTablesView() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = fuzzer.generateRandom(LOG);
            setFuzzParams(rnd, 0);
            setFuzzProperties(rnd);
            runViewFuzz(rnd, getTestName(), 1 + rnd.nextInt(4), true);
        });
    }

    @Test
    public void testMultipleLevelDependencyViews() throws Exception {
//        final Rnd rnd = fuzzer.generateRandom(LOG, 1667075212736414L, 1767857790813L);
//        final Rnd rnd = fuzzer.generateRandom(LOG, 1663538805955362L, 1767854254406L);
//        final Rnd rnd = fuzzer.generateRandom(LOG, 86385347609413L, 1767862063016L);
//        final Rnd rnd = fuzzer.generateRandom(LOG, 180665875728L, 1767862750951L);
//        final Rnd rnd = fuzzer.generateRandom(LOG, 7858902015755L, 1767870429187L);
        final Rnd rnd = fuzzer.generateRandom(LOG);
        final RandomSelectGenerator selectGenerator = new RandomSelectGenerator(engine, rnd);

        final String tableName = testName.getMethodName();
        final String view1Name = testName.getMethodName() + "_v1";
        final String view2Name = testName.getMethodName() + "_v2";
        final String view3Name = testName.getMethodName() + "_v3";
        final String view4Name = testName.getMethodName() + "_v4";
        testViewFuzz(selectGenerator, rnd, tableName, view1Name, view2Name, view3Name, view4Name);
    }

    @Test
    public void testOneView() throws Exception {
        final Rnd rnd = fuzzer.generateRandom(LOG);
        final RandomSelectGenerator selectGenerator = new RandomSelectGenerator(engine, rnd);

        final String tableName = testName.getMethodName();
        final String viewName = testName.getMethodName() + "_v";
        testViewFuzz(selectGenerator, rnd, tableName, viewName);
    }

    private static void createView(String viewName, String viewSql) throws SqlException {
        execute("create view " + viewName + " as (" + viewSql + ")");
    }

    private ObjList<FuzzTransaction> createTransactionsAndView(
            Rnd rnd,
            String tableNameBase,
            String viewName,
            String viewSql
    ) throws SqlException, NumericException {
        fuzzer.createInitialTableWal(tableNameBase, timestampTypes[rnd.nextInt(10) % 2]);
        createView(viewName, viewSql);

        ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(tableNameBase, rnd);

        // Release table writers to reduce memory pressure
        engine.releaseInactive();
        return transactions;
    }

    private void runViewFuzz(Rnd rnd, String testTableName, int tableCount, boolean expectValidViews) throws Exception {
        final AtomicBoolean stop = new AtomicBoolean();
        final ObjList<Thread> viewCompilerJobs = new ObjList<>();
        final int viewCompilerJobCount = 1 + rnd.nextInt(4);

        for (int i = 0; i < viewCompilerJobCount; i++) {
            viewCompilerJobs.add(startViewCompilerJob(i, stop, rnd));
        }

        final ObjList<ObjList<FuzzTransaction>> fuzzTransactions = new ObjList<>();
        final ObjList<String> viewSqls = new ObjList<>();

        for (int i = 0; i < tableCount; i++) {
            String tableNameBase = testTableName + "_" + i;
            String tableNameView = tableNameBase + "_v";
            String viewSql = "select min(c3), max(c3), ts from " + tableNameBase + " sample by 1h";
            ObjList<FuzzTransaction> transactions = createTransactionsAndView(rnd, tableNameBase, tableNameView, viewSql);
            fuzzTransactions.add(transactions);
            viewSqls.add(viewSql);
        }

        // Can help to reduce memory consumption.
        engine.releaseInactive();
        fuzzer.applyManyWalParallel(fuzzTransactions, rnd, testTableName, true, true);

        stop.set(true);
        for (int i = 0; i < viewCompilerJobCount; i++) {
            viewCompilerJobs.getQuick(i).join();
        }

        drainWalQueue();
        fuzzer.checkNoSuspendedTables();

        drainViewQueue();
        fuzzer.checkNoSuspendedTables();

        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            for (int i = 0; i < tableCount; i++) {
                final String viewName = testTableName + "_" + i + "_v";
                final String viewSql = viewSqls.getQuick(i);
                LOG.info().$("asserting view ").$(viewName).$(" against ").$(viewSql).$();
                // Check that the view exists.
                assertSql(
                        """
                                count
                                1
                                """,
                        "select count() " +
                                "from views() " +
                                "where view_name = '" + viewName + "'"
                );
                if (expectValidViews) {
                    assertSql(
                            """
                                    count
                                    1
                                    """,
                            "select count() " +
                                    "from views() " +
                                    "where view_name = '" + viewName + "' and view_status <> 'invalid'"
                    );
                    TestUtils.assertSqlCursors(
                            compiler,
                            sqlExecutionContext,
                            viewSql,
                            viewName,
                            LOG
                    );
                }
            }
        }
    }

    private void setFuzzParams(Rnd rnd, double colAddProb) {
        setFuzzParams(rnd, 10_000, 100_000, colAddProb, 0);
    }

    private void setFuzzParams(Rnd rnd, int transactionCount, int initialRowCount, double colAddProb, double truncateProb) {
        fuzzer.setFuzzCounts(
                rnd.nextBoolean(),
                rnd.nextInt(transactionCount),
                rnd.nextInt(1000),
                rnd.nextInt(3),
                rnd.nextInt(5),
                rnd.nextInt(1000),
                rnd.nextInt(initialRowCount),
                5 + rnd.nextInt(10)
        );

        // Easy, no column manipulations
        fuzzer.setFuzzProbabilities(
                0.0,
                0.0,
                0.0,
                0.0,
                colAddProb,
                0.0,
                0.0,
                0.0,
                1,
                0.0,
                0.0,
                truncateProb,
                0.0,
                0.0,
                0.1,
                0.0
        );
    }

    private Thread startViewCompilerJob(int workerId, AtomicBoolean stop, Rnd outsideRnd) {
        final Rnd rnd = new Rnd(outsideRnd.nextLong(), outsideRnd.nextLong());
        final Thread th = new Thread(
                () -> {
                    try {
                        try (ViewCompilerJob job = new ViewCompilerJob(workerId, engine, 0)) {
                            while (!stop.get()) {
                                job.run(workerId);
                                Os.sleep(rnd.nextInt(50));
                            }

                            // Run one final time before stopping
                            try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                                do {
                                    drainWalQueue(walApplyJob, engine);
                                } while (job.run(workerId));
                            }
                        }
                    } catch (Throwable throwable) {
                        LOG.error().$("View compiler job failed: ").$(throwable).$();
                    } finally {
                        Path.clearThreadLocals();
                        LOG.info().$("View compiler job stopped").$();
                    }
                }, "view-compiler-job" + workerId
        );
        th.start();
        return th;
    }

    private void testViewFuzz(RandomSelectGenerator selectGenerator, Rnd rnd, String baseTableName, String... viewNames) throws Exception {
        long start = MicrosTimestampDriver.floor("2022-02-24T17");
        testViewFuzz(selectGenerator, rnd, baseTableName, start, viewNames);
    }

    private void testViewFuzz(RandomSelectGenerator selectGenerator, Rnd rnd, String baseTableName, long start, String... viewNames) throws Exception {
        assertMemoryLeak(() -> {
            fuzzer.createInitialTableWal(baseTableName, timestampTypes[rnd.nextInt(10) % 2]);
            selectGenerator.registerTable(baseTableName);

            final ObjList<String> viewSqls = new ObjList<>();
            try (ViewCompilerJob viewCompiler = new ViewCompilerJob(0, engine, 0)) {
                for (final String viewName : viewNames) {
                    final String viewSql = selectGenerator.generate();
                    viewSqls.add(viewSql);
                    createView(viewName, viewSql);
                    LOG.info().$("created view ").$(viewName).$(" as ").$(viewSql).$();
                    viewCompiler.run(0); // compile the view before registering it -> so if there is a dependant view it gets to see this view's metadata
                    selectGenerator.registerTable(viewName);
                }
            }

            AtomicBoolean stop = new AtomicBoolean();
            Thread viewCompilerJob = startViewCompilerJob(0, stop, rnd);

            setFuzzParams(rnd, 0);

            ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(baseTableName, rnd, start);
            ObjList<ObjList<FuzzTransaction>> fuzzTransactions = new ObjList<>();
            fuzzTransactions.add(transactions);
            fuzzer.applyManyWalParallel(
                    fuzzTransactions,
                    rnd,
                    baseTableName,
                    false,
                    true
            );

            stop.set(true);
            viewCompilerJob.join();
            drainWalQueue();

            // count row count in the original table
            try (RecordCursorFactory factory = select("select count() from " + baseTableName);
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                var record = cursor.getRecord();
                final long rowCount = record.getLong(0);
                LOG.info().$("original table row count: ").$(rowCount).$();
            }

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (int i = 0, n = viewNames.length; i < n; i++) {
                    final String viewName = viewNames[i];
                    final String viewSql = viewSqls.getQuick(i);

                    // Order by ALL non-binary columns to ensure deterministic ordering.
                    // Ordering by a single low-cardinality column can cause flaky failures
                    // when multiple rows have the same value.
                    StringBuilder orderByClause = new StringBuilder();
                    final TableToken viewToken = engine.getTableTokenIfExists(viewName);
                    try (TableMetadata metadata = engine.getTableMetadata(viewToken)) {
                        for (int j = 0; j < metadata.getColumnCount(); j++) {
                            if (metadata.getColumnType(j) != ColumnType.BINARY) {
                                if (!orderByClause.isEmpty()) {
                                    orderByClause.append(", ");
                                }
                                orderByClause.append(j + 1);
                            }
                        }
                    }
                    String orderBy = !orderByClause.isEmpty() ? " order by " + orderByClause : "";

                    LOG.info().$("asserting view ").$(viewName).$(" against ").$(viewSql).$(", order-by: ").$(orderBy).I$();
                    TestUtils.assertSqlCursors(
                            compiler,
                            sqlExecutionContext,
                            "(" + viewSql + ")" + orderBy,
                            "(" + viewName + ")" + orderBy,
                            LOG
                    );
                }
            }
        });
    }
}
