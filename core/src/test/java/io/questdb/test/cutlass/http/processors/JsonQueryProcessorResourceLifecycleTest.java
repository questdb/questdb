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

package io.questdb.test.cutlass.http.processors;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.ex.RetryOperationException;
import io.questdb.cutlass.http.processors.JsonQueryProcessor;
import io.questdb.cutlass.http.processors.JsonQueryProcessorState;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.network.PlainSocketFactory;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class JsonQueryProcessorResourceLifecycleTest extends AbstractTest {

    @Test
    public void testMountAndUnmountAreIdempotent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration cairoConfiguration = new DefaultTestCairoConfiguration(root);
            final DefaultHttpServerConfiguration httpConfiguration =
                    new DefaultHttpServerConfiguration(cairoConfiguration);
            try (
                    TrackingCairoEngine engine = new TrackingCairoEngine(cairoConfiguration);
                    HttpConnectionContext context = new HttpConnectionContext(httpConfiguration, PlainSocketFactory.INSTANCE)
            ) {
                context.getOrCreateSqlExecutionContext(engine, 1);
                try (JsonQueryProcessorState state = new JsonQueryProcessorState(context, cairoConfiguration.getNanosecondClock(), null)) {
                    state.setSqlExecutionOwnerId(7);

                    state.mountSqlExecutionOwner();
                    Assert.assertEquals(0, engine.mountCount);

                    state.unmountSqlExecutionOwner();
                    state.unmountSqlExecutionOwner();
                    Assert.assertEquals(1, engine.unmountCount);

                    state.mountSqlExecutionOwner();
                    state.mountSqlExecutionOwner();
                    Assert.assertEquals(1, engine.mountCount);
                }
                Assert.assertEquals(1, engine.endCount);
            }
        });
    }

    @Test
    public void testParkAndResumeOrderTimerAroundOwner() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration cairoConfiguration = new DefaultTestCairoConfiguration(root);
            final DefaultHttpServerConfiguration httpConfiguration =
                    new DefaultHttpServerConfiguration(cairoConfiguration);
            try (
                    TrackingCairoEngine engine = new TrackingCairoEngine(cairoConfiguration);
                    HttpConnectionContext context = new HttpConnectionContext(httpConfiguration, PlainSocketFactory.INSTANCE)
            ) {
                context.getOrCreateSqlExecutionContext(engine, 1);
                try (JsonQueryProcessorState state = new JsonQueryProcessorState(context, cairoConfiguration.getNanosecondClock(), null)) {
                    state.setCursor(new TrackingRecordCursor(engine.events));
                    state.setSqlExecutionOwnerId(7);
                    state.startExecutionTimer();

                    state.parkSqlExecutionOwner();
                    Assert.assertEquals(List.of("cursor.suspend", "owner.unmount"), engine.events);

                    state.resumeSqlExecutionOwner();
                    Assert.assertEquals(
                            List.of("cursor.suspend", "owner.unmount", "cursor.resume", "owner.mount"),
                            engine.events
                    );
                }
            }
        });
    }

    @Test
    public void testResourceGroupBypassReset() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration cairoConfiguration = new DefaultTestCairoConfiguration(root);
            final DefaultHttpServerConfiguration httpConfiguration =
                    new DefaultHttpServerConfiguration(cairoConfiguration);
            try (
                    TrackingCairoEngine engine = new TrackingCairoEngine(cairoConfiguration);
                    HttpConnectionContext context = new HttpConnectionContext(httpConfiguration, PlainSocketFactory.INSTANCE)
            ) {
                final SqlExecutionContext executionContext = context.getOrCreateSqlExecutionContext(engine, 1);
                Assert.assertFalse(executionContext.isResourceGroupBypassed());
                executionContext.setResourceGroupBypassed(true);
                Assert.assertTrue(executionContext.isResourceGroupBypassed());
                executionContext.reset();
                Assert.assertFalse(executionContext.isResourceGroupBypassed());
            }
        });
    }

    @Test
    public void testResponseOnlyResumeDoesNotMountOwner() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration cairoConfiguration = new DefaultTestCairoConfiguration(root);
            final DefaultHttpServerConfiguration httpConfiguration =
                    new DefaultHttpServerConfiguration(cairoConfiguration);
            try (
                    TrackingCairoEngine engine = new TrackingCairoEngine(cairoConfiguration);
                    HttpConnectionContext context = new HttpConnectionContext(httpConfiguration, PlainSocketFactory.INSTANCE)
            ) {
                context.getOrCreateSqlExecutionContext(engine, 1);
                try (JsonQueryProcessorState state = new JsonQueryProcessorState(context, cairoConfiguration.getNanosecondClock(), null)) {
                    state.setSqlExecutionOwnerId(7);
                    state.startExecutionTimer();

                    state.parkSqlExecutionOwner();
                    state.resumeSqlExecutionOwner();

                    Assert.assertEquals(List.of("owner.unmount"), engine.events);
                    Assert.assertEquals(0, engine.mountCount);
                }
                Assert.assertEquals(List.of("owner.unmount", "owner.end"), engine.events);
            }
        });
    }

    @Test
    public void testRetryUnmountsOwnerBetweenAttempts() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration cairoConfiguration = new DefaultTestCairoConfiguration(root);
            final DefaultHttpServerConfiguration httpConfiguration =
                    new DefaultHttpServerConfiguration(cairoConfiguration);
            try (
                    TrackingCairoEngine engine = new TrackingCairoEngine(cairoConfiguration);
                    HttpConnectionContext context = new HttpConnectionContext(httpConfiguration, PlainSocketFactory.INSTANCE);
                    JsonQueryProcessor processor = new JsonQueryProcessor(
                            httpConfiguration.getJsonQueryProcessorConfiguration(),
                            engine,
                            1
                    )
            ) {
                context.getOrCreateSqlExecutionContext(engine, 1);
                try (JsonQueryProcessorState state = new JsonQueryProcessorState(context, cairoConfiguration.getNanosecondClock(), null)) {
                    state.setOperationFuture(new PendingOperationFuture());
                    state.setSqlExecutionOwnerId(11);
                    state.startExecutionTimer();

                    assertRetry(processor, state);
                    Assert.assertEquals(0, engine.mountCount);
                    Assert.assertEquals(1, engine.unmountCount);

                    assertRetry(processor, state);
                    Assert.assertEquals(1, engine.mountCount);
                    Assert.assertEquals(2, engine.unmountCount);
                }
                Assert.assertEquals(1, engine.endCount);
            }
        });
    }

    @Test
    public void testUnmanagedScopeStillEnds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration cairoConfiguration = new DefaultTestCairoConfiguration(root);
            final DefaultHttpServerConfiguration httpConfiguration =
                    new DefaultHttpServerConfiguration(cairoConfiguration);
            try (
                    TrackingCairoEngine engine = new TrackingCairoEngine(cairoConfiguration);
                    HttpConnectionContext context = new HttpConnectionContext(httpConfiguration, PlainSocketFactory.INSTANCE)
            ) {
                context.getOrCreateSqlExecutionContext(engine, 1);
                try (JsonQueryProcessorState state = new JsonQueryProcessorState(context, cairoConfiguration.getNanosecondClock(), null)) {
                    state.setSqlExecutionOwnerId(-1);
                }
                Assert.assertEquals(1, engine.endCount);
                Assert.assertEquals(-1, engine.endedOwnerId);
            }
        });
    }

    private static void assertRetry(JsonQueryProcessor processor, JsonQueryProcessorState state) throws Exception {
        try {
            processor.execute0(state);
            Assert.fail("retry was expected");
        } catch (RetryOperationException expected) {
            // expected
        }
    }

    private static final class PendingOperationFuture implements OperationFuture {
        @Override
        public void await() {
        }

        @Override
        public int await(long timeout) {
            return QUERY_NO_RESPONSE;
        }

        @Override
        public void close() {
        }

        @Override
        public long getAffectedRowsCount() {
            return 0;
        }

        @Override
        public int getStatus() {
            return QUERY_NO_RESPONSE;
        }
    }

    private static final class TrackingRecordCursor implements RecordCursor {
        private final List<String> events;

        private TrackingRecordCursor(List<String> events) {
            this.events = events;
        }

        @Override
        public void close() {
            events.add("cursor.close");
        }

        @Override
        public Record getRecord() {
            return null;
        }

        @Override
        public Record getRecordB() {
            return null;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
        }

        @Override
        public void resumeTimer() {
            events.add("cursor.resume");
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public void suspendTimer() {
            events.add("cursor.suspend");
        }

        @Override
        public void toTop() {
        }
    }

    private static final class TrackingCairoEngine extends CairoEngine {
        private int endCount;
        private long endedOwnerId = Long.MIN_VALUE;
        private final List<String> events = new ArrayList<>();
        private int mountCount;
        private int unmountCount;

        private TrackingCairoEngine(DefaultTestCairoConfiguration configuration) {
            super(configuration);
        }

        @Override
        public void endSqlExecution(long ownerId, SqlExecutionContext executionContext) {
            endCount++;
            endedOwnerId = ownerId;
            events.add("owner.end");
        }

        @Override
        public void mountSqlExecution(long ownerId, SqlExecutionContext executionContext) {
            mountCount++;
            events.add("owner.mount");
        }

        @Override
        public void unmountSqlExecution(long ownerId, SqlExecutionContext executionContext) {
            unmountCount++;
            events.add("owner.unmount");
        }
    }
}
