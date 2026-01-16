/*******************************************************************************
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

package io.questdb.test;

import io.questdb.Bootstrap;
import io.questdb.Metrics;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.cairo.view.ViewCompilerJob;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Zip;
import io.questdb.test.cutlass.http.HttpQueryTestBuilder;
import io.questdb.test.cutlass.http.HttpServerConfigurationBuilder;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.OrderWith;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("ClassEscapesDefinedScope")
@OrderWith(RandomOrder.class)
public class AbstractTest {
    public static final Set<QuietCloseable> CLOSEABLE = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();
    protected static final Log LOG = LogFactory.getLog(AbstractTest.class);
    protected static final AtomicInteger OFF_POOL_READER_ID = new AtomicInteger();
    protected static String root;
    @Rule
    public final TestName testName = new TestName();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        TestOs.init();
        Zip.init();
        // it is necessary to initialize logger before tests start
        // logger doesn't relinquish memory until JVM stops,
        // which causes memory leak detector to fail should logger be
        // created mid-test
        LOG.info().$("setup logger").$();
        root = temp.newFolder("dbRoot").getAbsolutePath();
    }

    @AfterClass
    public static void tearDownStatic() {
        TestUtils.removeTestPath(root);
    }

    @Before
    public void setUp() {
        LOG.info().$("Starting test ").$safe(getClass().getSimpleName()).$('#').$safe(testName.getMethodName()).$();
        TestUtils.createTestPath(root);
        Metrics.ENABLED.clear();
        SqlCodeGenerator.ALLOW_FUNCTION_MEMOIZATION = false;
    }

    @After
    public void tearDown() throws Exception {
        LOG.info().$("Finished test ").$safe(getClass().getSimpleName()).$('#').$safe(testName.getMethodName()).$();
        TestUtils.removeTestPath(root);
        OFF_POOL_READER_ID.set(0);
        SqlCodeGenerator.ALLOW_FUNCTION_MEMOIZATION = false;
    }

    protected static MatViewRefreshJob createMatViewRefreshJob(CairoEngine engine) {
        return new MatViewRefreshJob(0, engine, 1);
    }

    protected static ViewCompilerJob createViewCompilerJob(CairoEngine engine) {
        return new ViewCompilerJob(0, engine);
    }

    protected static ApplyWal2TableJob createWalApplyJob(CairoEngine engine) {
        return new ApplyWal2TableJob(engine, 0);
    }

    @SuppressWarnings("StatementWithEmptyBody")
    protected static void drainMatViewQueue(MatViewRefreshJob refreshJob) {
        while (refreshJob.run(0)) ;
    }

    protected static void drainMatViewQueue(CairoEngine engine) {
        try (MatViewRefreshJob refreshJob = createMatViewRefreshJob(engine)) {
            drainMatViewQueue(refreshJob);
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    protected static void drainMatViewTimerQueue(MatViewTimerJob timerJob) {
        while (timerJob.run(0)) ;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    protected static void drainViewQueue(ViewCompilerJob compilerJob) {
        while (compilerJob.run(0)) ;
    }

    protected static void drainViewQueue(CairoEngine engine) {
        try (ViewCompilerJob compilerJob = createViewCompilerJob(engine)) {
            drainViewQueue(compilerJob);
        }
    }

    protected static void drainWalAndMatViewQueues(CairoEngine engine) {
        drainWalQueue(engine);
        drainMatViewQueue(engine);
        drainWalQueue(engine);
    }

    protected static void drainWalAndMatViewQueues(MatViewRefreshJob refreshJob, CairoEngine engine) {
        drainWalQueue(engine);
        drainMatViewQueue(refreshJob);
        drainWalQueue(engine);
    }

    protected static void drainWalAndViewQueues(CairoEngine engine) {
        drainWalQueue(engine);
        drainViewQueue(engine);
    }

    protected static void drainWalQueue(CairoEngine engine) {
        try (ApplyWal2TableJob walApplyJob = createWalApplyJob(engine)) {
            drainWalQueue(walApplyJob, engine);
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    protected static void drainWalQueue(ApplyWal2TableJob walApplyJob, CairoEngine engine) {
        CheckWalTransactionsJob checkWalTransactionsJob = new CheckWalTransactionsJob(engine);
        while (walApplyJob.run(0)) ;
        if (checkWalTransactionsJob.run(0)) {
            while (walApplyJob.run(0)) ;
        }
    }

    protected static String[] getServerMainArgs(CharSequence root) {
        return Bootstrap.getServerMainArgs(root);
    }

    protected static String[] getServerMainArgs() {
        return getServerMainArgs(root);
    }

    protected static HttpQueryTestBuilder getSimpleTester() {
        return new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false);
    }

    protected static TableReader newOffPoolReader(CairoConfiguration configuration, CharSequence tableName, CairoEngine engine) {
        return new TableReader(OFF_POOL_READER_ID.getAndIncrement(), configuration, engine.verifyTableName(tableName), engine.getTxnScoreboardPool());
    }
}
