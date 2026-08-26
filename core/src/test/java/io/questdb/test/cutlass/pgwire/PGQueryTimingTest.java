/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|_| |_|____/
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

package io.questdb.test.cutlass.pgwire;

import io.questdb.PropertyKey;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.metrics.QueryTrace;
import io.questdb.mp.ConcurrentQueue;
import io.questdb.std.Os;
import io.questdb.std.datetime.MicrosecondClock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.atomic.AtomicLong;

public class PGQueryTimingTest extends BasePGTest {
    private static final long CLOCK_TICK_NANOS = 1_000L;
    private static final long POST_RESUME_ACTIVE_MICROS = 1_000_000L;
    private static final long POST_RESUME_ACTIVE_NANOS = POST_RESUME_ACTIVE_MICROS * CLOCK_TICK_NANOS;
    private static final int POST_RESUME_CALLBACK_COUNT = 10;

    @Before
    public void setupTracing() {
        node1.getConfigurationOverrides().setProperty(PropertyKey.QUERY_TRACING_ENABLED, true);
    }

    @Test
    public void testFetchAllSocketSuspensionCountsAsWait() throws Exception {
        final ControlledMicrosecondClock clock = new ControlledMicrosecondClock();
        testMicrosClock = clock;
        TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
            @Override
            public boolean onGet(io.questdb.cairo.sql.Record rec, int count) {
                // Each 450-byte row nearly fills the 512-byte response buffer, so this row
                // executes only after the first forced fragmented socket resume.
                if (count == POST_RESUME_CALLBACK_COUNT) {
                    clock.advanceMicros(POST_RESUME_ACTIVE_MICROS);
                }
                return true;
            }
        });
        try {
            assertWithPgServerExtendedBinaryOnly(
                    (connection, _, _, _) -> assertSocketSuspensionCountsAsWait(connection),
                    this::setupTracingWithFragmentedResponse
            );
        } finally {
            TestLatchedCounterFunctionFactory.reset(null);
            testMicrosClock = defaultMicrosecondClock;
        }
    }

    @Test
    public void testFragmentedSyncPortalSuspensionCountsAsWait() throws Exception {
        assertWithPgServerExtendedBinaryOnly(
                (connection, _, _, _) -> assertPortalSuspensionCountsAsWait(connection),
                this::setupTracingWithFragmentedSync
        );
    }

    @Test
    public void testRetainedPortalResumeCountsPostResumeActiveTime() throws Exception {
        final ControlledMicrosecondClock clock = new ControlledMicrosecondClock();
        testMicrosClock = clock;
        TestLatchedCounterFunctionFactory.reset(new TestLatchedCounterFunctionFactory.Callback() {
            @Override
            public boolean onGet(io.questdb.cairo.sql.Record rec, int count) {
                // Fetch size 10 puts this row on the second Execute, after the retained
                // portal must resume its timer.
                if (count == POST_RESUME_CALLBACK_COUNT + 1) {
                    clock.advanceMicros(POST_RESUME_ACTIVE_MICROS);
                }
                return true;
            }
        });
        try {
            assertWithPgServerExtendedBinaryOnly(
                    (connection, _, _, _) -> assertRetainedPortalResumeCountsPostResumeActiveTime(connection),
                    this::setupTracing
            );
        } finally {
            TestLatchedCounterFunctionFactory.reset(null);
            testMicrosClock = defaultMicrosecondClock;
        }
    }

    @Test
    public void testPortalSuspensionCountsAsWait() throws Exception {
        assertWithPgServer(
                CONN_AWARE_EXTENDED,
                (connection, _, _, _) -> assertPortalSuspensionCountsAsWait(connection),
                this::setupTracing
        );
    }

    private void assertSocketSuspensionCountsAsWait(Connection connection) throws Exception {
        final ConcurrentQueue<QueryTrace> queue = engine.getMessageBus().getQueryTraceQueue();
        drain(queue);
        final String query = """
                SELECT x, lpad('', 450, 'x') AS value
                FROM long_sequence(%d)
                WHERE test_latched_counter()""".formatted(POST_RESUME_CALLBACK_COUNT);
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            // Fetch-all has no retained portal, so the forced fragments are the only wait source.
            statement.setFetchSize(0);
            Assert.assertEquals(0, statement.getFetchSize());
            try (ResultSet resultSet = statement.executeQuery()) {
                int rows = 0;
                while (resultSet.next()) {
                    rows++;
                }
                Assert.assertEquals(POST_RESUME_CALLBACK_COUNT, rows);
            }
        }
        Assert.assertEquals(POST_RESUME_CALLBACK_COUNT, TestLatchedCounterFunctionFactory.getCount());
        final QueryTrace trace = pollTraceFor(queue, query);
        Assert.assertTrue(
                "expected socket wait >= " + CLOCK_TICK_NANOS + ", got " + trace.waitNanos,
                trace.waitNanos >= CLOCK_TICK_NANOS
        );
        Assert.assertTrue(
                "expected post-resume active time >= " + POST_RESUME_ACTIVE_NANOS
                        + " [executionNanos=" + trace.executionNanos + ", waitNanos=" + trace.waitNanos + ']',
                trace.executionNanos - trace.waitNanos >= POST_RESUME_ACTIVE_NANOS
        );
        Assert.assertTrue(trace.firstRowNanos >= 0);
        Assert.assertTrue(trace.firstRowNanos <= trace.executionNanos);
    }

    private void assertRetainedPortalResumeCountsPostResumeActiveTime(Connection connection) throws Exception {
        final ConcurrentQueue<QueryTrace> queue = engine.getMessageBus().getQueryTraceQueue();
        drain(queue);
        connection.setAutoCommit(false);
        final int rowCount = POST_RESUME_CALLBACK_COUNT * 2;
        final String query = "SELECT x FROM long_sequence(%d) WHERE test_latched_counter()".formatted(rowCount);
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setFetchSize(POST_RESUME_CALLBACK_COUNT);
            Assert.assertEquals(POST_RESUME_CALLBACK_COUNT, statement.getFetchSize());
            try (ResultSet resultSet = statement.executeQuery()) {
                int rows = 0;
                while (resultSet.next()) {
                    rows++;
                }
                Assert.assertEquals(rowCount, rows);
            }
        }
        connection.commit();
        Assert.assertEquals(rowCount, TestLatchedCounterFunctionFactory.getCount());
        final QueryTrace trace = pollTraceFor(queue, query);
        Assert.assertTrue(
                "expected controlled portal wait >= " + CLOCK_TICK_NANOS + ", got " + trace.waitNanos,
                trace.waitNanos >= CLOCK_TICK_NANOS
        );
        Assert.assertTrue(
                "expected post-resume active time >= " + POST_RESUME_ACTIVE_NANOS
                        + " [executionNanos=" + trace.executionNanos + ", waitNanos=" + trace.waitNanos + ']',
                trace.executionNanos - trace.waitNanos >= POST_RESUME_ACTIVE_NANOS
        );
        Assert.assertTrue(trace.firstRowNanos >= 0);
        Assert.assertTrue(trace.firstRowNanos <= trace.executionNanos);
    }

    private void assertPortalSuspensionCountsAsWait(Connection connection) throws Exception {
        execute("CREATE TABLE tab AS (SELECT x FROM long_sequence(100))");
        final ConcurrentQueue<QueryTrace> queue = engine.getMessageBus().getQueryTraceQueue();
        drain(queue);
        connection.setAutoCommit(false);
        final String query = "SELECT x FROM tab";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setFetchSize(10);
            try (ResultSet resultSet = statement.executeQuery()) {
                int rows = 0;
                while (resultSet.next()) {
                    if (++rows % 10 == 0) {
                        Os.sleep(20);
                    }
                }
                Assert.assertEquals(100, rows);
            }
        }
        connection.commit();
        final QueryTrace trace = pollTraceFor(queue, query);
        Assert.assertTrue("expected wait > 0, got " + trace.waitNanos, trace.waitNanos > 0);
        Assert.assertTrue("expected portal wait >= 100000000, got " + trace.waitNanos, trace.waitNanos >= 100_000_000L);
        Assert.assertTrue(trace.waitNanos <= trace.executionNanos);
        Assert.assertTrue(trace.firstRowNanos >= 0);
        Assert.assertTrue(trace.firstRowNanos <= trace.executionNanos);
    }

    private static void drain(ConcurrentQueue<QueryTrace> queue) {
        final QueryTrace trace = new QueryTrace();
        while (queue.tryDequeue(trace)) {
        }
    }

    private static QueryTrace pollTraceFor(ConcurrentQueue<QueryTrace> queue, String query) {
        final QueryTrace trace = new QueryTrace();
        final long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            while (queue.tryDequeue(trace)) {
                if (query.equals(trace.queryText)) {
                    return trace;
                }
            }
            Os.sleep(50);
        }
        Assert.fail("no trace for query: " + query);
        return null;
    }

    private void setupTracingWithFragmentedResponse() {
        setupTracing();
        sendBufferSize = 512;
        forceSendFragmentationChunkSize = 10;
    }

    private void setupTracingWithFragmentedSync() {
        setupTracing();
        // Force the retained portal's sync response through a pending flush.
        sendBufferSize = 256;
        forceSendFragmentationChunkSize = 64;
    }

    private static class ControlledMicrosecondClock implements MicrosecondClock {
        private final AtomicLong micros = new AtomicLong();

        public void advanceMicros(long micros) {
            this.micros.addAndGet(micros);
        }

        @Override
        public long getTicks() {
            return micros.getAndIncrement();
        }
    }
}
