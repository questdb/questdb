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

package io.questdb.compat;

import io.questdb.cairo.CairoEngine;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.util.concurrent.TimeUnit;

public class AbstractTest {
    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();
    protected static final Log LOG = LogFactory.getLog(AbstractTest.class);
    protected static String root;
    @Rule
    public final TestName testName = new TestName();
    protected final StringSink sink = new StringSink();

    public static void assertEventually(Runnable assertion) {
        assertEventually(assertion, 30);
    }

    public static void assertEventually(Runnable assertion, int timeoutSeconds) {
        long maxSleepingTimeMillis = 1000;
        long nextSleepingTimeMillis = 10;
        long startTime = System.nanoTime();
        long deadline = startTime + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        for (; ; ) {
            try {
                assertion.run();
                return;
            } catch (AssertionError error) {
                if (System.nanoTime() >= deadline) {
                    throw error;
                }
            }
            Os.sleep(nextSleepingTimeMillis);
            nextSleepingTimeMillis = Math.min(maxSleepingTimeMillis, nextSleepingTimeMillis << 1);
        }
    }

    public static void createTestPath() {
        final Path path = Path.getThreadLocal(root);
        if (Files.exists(path.$())) {
            return;
        }
        Files.mkdirs(path.of(root).slash(), 509);
    }

    public static Rnd generateRandom(Log log) {
        return generateRandom(log, System.nanoTime(), System.currentTimeMillis());
    }

    public static Rnd generateRandom(Log log, long s0, long s1) {
        if (log != null) {
            log.info().$("random seeds: ").$(s0).$("L, ").$(s1).$('L').$();
        }
        System.out.printf("random seeds: %dL, %dL%n", s0, s1);
        return new Rnd(s0, s1);
    }

    public static void removeTestPath() {
        final Path path = Path.getThreadLocal(root);
        FilesFacade ff = FilesFacadeImpl.INSTANCE;
        path.slash$();
        Assert.assertTrue("Test dir cleanup error", !ff.exists(path.$()) || Files.rmdir(path.slash(), true));
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // it is necessary to initialise logger before tests start
        // logger doesn't relinquish memory until JVM stops
        // which causes memory leak detector to fail should logger be
        // created mid-test
        LOG.info().$("setup logger").$();
        root = temp.newFolder("dbRoot").getAbsolutePath();
    }

    @AfterClass
    public static void tearDownStatic() {
        removeTestPath();
    }

    public void assertSql(CairoEngine engine, CharSequence sql, CharSequence expectedResult) throws SqlException {
        engine.print(sql, sink);
        if (!Chars.equals(sink, expectedResult)) {
            Assert.assertEquals(expectedResult, sink);
        }
    }

    @Before
    public void setUp() {
        LOG.info().$("Starting test ").$safe(getClass().getSimpleName()).$('#').$safe(testName.getMethodName()).$();
        createTestPath();
    }

    @After
    public void tearDown() {
        LOG.info().$("Finished test ").$safe(getClass().getSimpleName()).$('#').$safe(testName.getMethodName()).$();
        removeTestPath();
    }
}
