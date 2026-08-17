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

package io.questdb.test.tools;

import io.questdb.Metrics;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.Rnd;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class TestUtilsTest extends AbstractCairoTest {

    @Test
    public void testAssertAsciiCompliance() {
        TestUtils.assertAsciiCompliance(null);
        TestUtils.assertAsciiCompliance(new Utf8String(new byte[]{'a'}, true));
        TestUtils.assertAsciiCompliance(new Utf8String(new byte[]{'a'}, false));

        Assert.assertThrows(
                AssertionError.class,
                () -> TestUtils.assertAsciiCompliance(new Utf8String(new byte[]{(byte) 0xc3, (byte) 0xa9}, true))
        );
    }

    @Test
    public void testAssertReverseLinesEqual() {
        Assert.assertThrows(AssertionError.class, () -> {
            TestUtils.assertReverseLinesEqual(null, "123\n456\n789\n", "123\n456\n789\n");
            TestUtils.assertReverseLinesEqual(null, "1234\n56\n789\n", "789\n456\n123\n");
        });
        TestUtils.assertReverseLinesEqual(null, "123\n456\n789\n", "789\n456\n123\n");
        TestUtils.assertReverseLinesEqual(null, "1234\n56\n789\n", "789\n56\n1234\n");
        TestUtils.assertReverseLinesEqual(null, "1234\n", "1234\n");
    }

    @Test
    public void testOrderTolerantRecordComparison() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (x long, ts timestamp) timestamp(ts) partition by day");
            execute("create table y (x long, ts timestamp) timestamp(ts) partition by day");

            execute("insert into x values (1, '2022-02-24T00:00:01.000000Z')");
            execute("insert into x values (2, '2022-02-24T00:00:01.000000Z')");
            execute("insert into x values (3, '2022-02-24T00:00:02.000000Z')");

            execute("insert into y values (2, '2022-02-24T00:00:01.000000Z')");
            execute("insert into y values (1, '2022-02-24T00:00:01.000000Z')");
            execute("insert into y values (3, '2022-02-24T00:00:02.000000Z')");

            HashMap<String, Integer> mapX = new HashMap<>();
            HashMap<String, Integer> mapY = new HashMap<>();

            addAllRecordsToMap("x", mapX);
            addAllRecordsToMap("y", mapY);

            Assert.assertEquals(mapX, mapY);

            execute("insert into y values (2, '2022-02-24T00:00:01.000000Z')");

            // now the maps should be different since we've added an extra record

            mapY.clear();
            addAllRecordsToMap("y", mapY);

            Assert.assertNotEquals(mapX, mapY);
        });
    }

    @Test
    public void testWorkerPoolModeForTestIdentityIsStableAndDistributed() {
        Assert.assertSame(WorkerPoolMode.LEGACY, TestUtils.workerPoolModeForTestIdentity("A"));
        Assert.assertSame(WorkerPoolMode.FIBER_HOST, TestUtils.workerPoolModeForTestIdentity("B"));
    }

    @Test
    public void testWorkerPoolModeIsStableWithinTest() {
        final String override = System.getProperty(TestUtils.WORKER_POOL_MODE_PROPERTY);
        Assume.assumeTrue(override == null || override.isEmpty());
        Assert.assertSame(TestUtils.getWorkerPoolMode(), TestUtils.getWorkerPoolMode());
    }

    @Test
    public void testWorkerPoolModePropertyOverridesBothSelectors() {
        final String override = System.getProperty(TestUtils.WORKER_POOL_MODE_PROPERTY);
        Assume.assumeTrue(override != null && !override.isEmpty());
        final WorkerPoolMode expected = WorkerPoolMode.valueOf(override.trim().toUpperCase(Locale.ROOT));
        Assert.assertSame(expected, TestUtils.getWorkerPoolMode());
        Assert.assertSame(expected, TestUtils.getWorkerPoolMode(new Rnd(Long.MIN_VALUE, 0)));
        Assert.assertSame(expected, TestUtils.getWorkerPoolMode(new Rnd(0, 0)));
    }

    @Test
    public void testWorkerPoolModeSeededSelectionIsStable() {
        final String override = System.getProperty(TestUtils.WORKER_POOL_MODE_PROPERTY);
        Assume.assumeTrue(override == null || override.isEmpty());
        Assert.assertSame(WorkerPoolMode.FIBER_HOST, TestUtils.getWorkerPoolMode(new Rnd(Long.MIN_VALUE, 0)));
        Assert.assertSame(WorkerPoolMode.LEGACY, TestUtils.getWorkerPoolMode(new Rnd(0, 0)));
    }

    @Test
    public void testWorkerPoolStartLogsResolvedMode() throws Exception {
        assertMemoryLeak(() -> {
            final AtomicReference<Log> startLog = new AtomicReference<>();
            try (
                    TestWorkerPool pool = new TestWorkerPool(
                            "m10-start-log",
                            1,
                            Metrics.DISABLED,
                            WorkerPoolMode.LEGACY
                    ) {
                        @Override
                        public void start(Log log) {
                            startLog.set(log);
                            super.start(log);
                        }
                    }
            ) {
                pool.start();
                Assert.assertNotNull(startLog.get());
            }
        });
    }

    @Test
    public void testWorkerPoolTestIdentityIsRevokedInChildThread() throws Exception {
        final String override = System.getProperty(TestUtils.WORKER_POOL_MODE_PROPERTY);
        Assume.assumeTrue(override == null || override.isEmpty());

        final AtomicReference<Throwable> childError = new AtomicReference<>();
        final AtomicReference<WorkerPoolMode> firstMode = new AtomicReference<>();
        final AtomicReference<WorkerPoolMode> secondMode = new AtomicReference<>();
        final CountDownLatch firstSelectionComplete = new CountDownLatch(1);
        final CountDownLatch releaseSecondSelection = new CountDownLatch(1);
        TestUtils.setWorkerPoolTestIdentity("B");
        final Thread child = new Thread(() -> {
            try {
                firstMode.set(TestUtils.getWorkerPoolMode());
                firstSelectionComplete.countDown();
                if (!releaseSecondSelection.await(10, TimeUnit.SECONDS)) {
                    throw new AssertionError("timed out waiting for identity revocation");
                }
                secondMode.set(TestUtils.getWorkerPoolMode());
            } catch (Throwable th) {
                childError.set(th);
                firstSelectionComplete.countDown();
            }
        }, "worker-pool-mode-revocation-test");
        child.start();
        try {
            Assert.assertTrue(firstSelectionComplete.await(10, TimeUnit.SECONDS));
            Assert.assertSame(WorkerPoolMode.FIBER_HOST, firstMode.get());
            TestUtils.clearWorkerPoolTestIdentity();
            releaseSecondSelection.countDown();
            child.join(10_000L);
            Assert.assertFalse(child.isAlive());
            if (childError.get() != null) {
                throw new AssertionError("child selector failed", childError.get());
            }
            Assert.assertSame(WorkerPoolMode.LEGACY, secondMode.get());
        } finally {
            TestUtils.clearWorkerPoolTestIdentity();
            releaseSecondSelection.countDown();
            child.join(10_000L);
        }
    }

    private static void addAllRecordsToMap(String query, Map<String, Integer> map) throws SqlException {
        try (
                RecordCursorFactory factory = select(query);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            TestUtils.addAllRecordsToMap(sink, cursor, factory.getMetadata(), map);
        }
    }
}
