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

package io.questdb.test;

import io.questdb.Bootstrap;
import io.questdb.Metrics;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.WriterInvariantChecker;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.cairo.view.ViewCompilerJob;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rnd;
import io.questdb.std.Zip;
import io.questdb.test.cutlass.http.HttpQueryTestBuilder;
import io.questdb.test.cairo.Overrides;
import io.questdb.test.cutlass.http.HttpServerConfigurationBuilder;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.runner.OrderWith;
import org.junit.runners.model.Statement;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("ClassEscapesDefinedScope")
@OrderWith(RandomOrder.class)
public class AbstractTest {
    public static final Set<QuietCloseable> CLOSEABLE = Collections.newSetFromMap(new ConcurrentHashMap<>());
    /**
     * Records the thread the test class runs on. A test that runs on another thread runs under a timeout rule,
     * on a thread of its own that ends with the test.
     */
    @ClassRule
    public static final TestRule classThreadRule = (base, description) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            classThread = Thread.currentThread();
            base.evaluate();
        }
    };
    /**
     * Gives every fuzz test class ({@link Overrides#isFuzzTest}) a commit mode of its own, drawn from all modes
     * independently of {@code -Dquestdb.test.commit.mode}, so across runs each fuzz class meets every mode rather
     * than the two the suite sweeps. A class rule, so the mode is in place for {@code @BeforeClass} as well; the
     * configuration paths read it through {@link Overrides#getTestCommitMode()}, and a test that pins a mode still
     * wins. {@code -Dquestdb.test.fuzz.commit.mode} reproduces a run.
     */
    @ClassRule
    public static final TestRule fuzzCommitModeRule = (base, description) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            final Class<?> testClass = description.getTestClass();
            if (!Overrides.isFuzzTest(testClass)) {
                base.evaluate();
                return;
            }
            final String mode = Overrides.pickFuzzCommitMode(new Rnd(System.nanoTime(), System.currentTimeMillis()));
            Overrides.setFuzzCommitMode(mode);
            LOG.advisory().$("fuzz test commit mode [class=").$safe(testClass.getSimpleName())
                    .$(", mode=").$(mode)
                    .$(", reproduce=-Dquestdb.test.fuzz.commit.mode=").$(mode)
                    .I$();
            try {
                base.evaluate();
            } finally {
                Overrides.setFuzzCommitMode(null);
            }
        }
    };
    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();
    protected static final Log LOG = LogFactory.getLog(AbstractTest.class);
    protected static final AtomicInteger OFF_POOL_READER_ID = new AtomicInteger();
    private static final long TEST_THREAD_EXIT_GRACE_MILLIS = 2_000;
    protected static String root;
    private static volatile Thread classThread;
    private static volatile Thread reportedAbandonedThread;
    private static volatile Thread testThread;
    private static volatile String testThreadName;
    @Rule
    public final TestName testName = new TestName();
    /**
     * Fails the test on any writer invariant violation (debug.cairo.writer.invariant.check.enabled) reported while it
     * ran. A rule rather than a tearDown() assertion: it wraps every @Before and @After, so it also sees violations
     * reported while a subclass's tearDown closes its writers, and failing here skips none of that cleanup. Covers
     * tests that do not run inside assertMemoryLeak, whose LeakCheck does the same.
     */
    @Rule
    public final TestRule writerInvariantCheck = (base, _) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            final long violationsBefore = WriterInvariantChecker.getViolationCount();
            base.evaluate();
            final long violations = WriterInvariantChecker.getViolationCount() - violationsBefore;
            if (violations > 0) {
                Assert.fail("writer invariant violated " + violations + " time(s), last: "
                        + WriterInvariantChecker.getLastViolation());
            }
        }
    };

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
        failIfAbandonedTestThreadRuns();
        final Thread current = Thread.currentThread();
        if (current != classThread) {
            testThreadName = getClass().getSimpleName() + '#' + testName.getMethodName();
            testThread = current;
        }
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
        if (testThread == Thread.currentThread()) {
            testThread = null;
        }
    }

    /**
     * Builds a {@link QueryAssertion} for tests that have no ambient {@link CairoEngine} (e.g. those
     * that drive a {@link TestServerMain}). The caller MUST supply the engine and execution context
     * via {@link QueryAssertion#withEngine} / {@link QueryAssertion#withContext} before the terminal,
     * typically from {@code serverMain.getEngine()} / {@code serverMain.getSqlExecutionContext()}.
     * {@link AbstractCairoTest} overrides this with a variant bound to its own static engine.
     */
    protected QueryAssertion assertQuery(CharSequence query) {
        return new QueryAssertion(null, null, () -> {
        }, query);
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
        while (refreshJob.run()) ;
    }

    protected static void drainMatViewQueue(CairoEngine engine) {
        try (MatViewRefreshJob refreshJob = createMatViewRefreshJob(engine)) {
            drainMatViewQueue(refreshJob);
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    protected static void drainMatViewTimerQueue(MatViewTimerJob timerJob) {
        while (timerJob.run()) ;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    protected static void drainViewQueue(ViewCompilerJob compilerJob) {
        while (compilerJob.run()) ;
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
        while (walApplyJob.run()) ;
        if (checkWalTransactionsJob.run()) {
            while (walApplyJob.run()) ;
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

    /**
     * Fails the test while the thread of an earlier test still runs. A timeout rule fails a test that runs too
     * long but cannot stop its thread, which keeps writing under the shared test root and mapping memory, so every
     * later test in this JVM would report what it does as its own failure: a memory leak, a table that already
     * exists. Only the thread of a test that did not reach {@link #tearDown()} is suspect, and it gets a short grace
     * to exit.
     */
    private static void failIfAbandonedTestThreadRuns() {
        final Thread previous = testThread;
        if (previous == null || previous == Thread.currentThread()) {
            return;
        }
        if (previous != reportedAbandonedThread) {
            try {
                previous.join(TEST_THREAD_EXIT_GRACE_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (previous.isAlive()) {
            reportedAbandonedThread = previous;
            Assert.fail("test " + testThreadName + " timed out and its thread is still running; this test would share"
                    + " the test root, its tables and the memory accounting with it");
        }
        testThread = null;
    }
}
