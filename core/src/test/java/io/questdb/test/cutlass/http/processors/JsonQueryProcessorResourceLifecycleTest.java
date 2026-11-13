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
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.http.ActiveConnectionTracker;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.ex.RetryOperationException;
import io.questdb.cutlass.http.processors.JsonQueryProcessor;
import io.questdb.cutlass.http.processors.JsonQueryProcessorState;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.AssociativeCache;
import io.questdb.std.Misc;
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
                    engine.nextOwnerId = 7;
                    state.beginSqlExecutionOwner("SELECT 1", context.getSqlExecutionContext(), CompiledQuery.SELECT);

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
    public void testOwnerEndFailureFreesFactoryInsteadOfCachingIt() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration cairoConfiguration = new DefaultTestCairoConfiguration(root);
            final DefaultHttpServerConfiguration httpConfiguration =
                    new DefaultHttpServerConfiguration(cairoConfiguration);
            try (TrackingCairoEngine engine = new TrackingCairoEngine(cairoConfiguration)) {
                final TrackingSelectCache cache = new TrackingSelectCache(engine.events);
                try (
                        cache;
                        HttpConnectionContext context = new HttpConnectionContext(
                                httpConfiguration,
                                PlainSocketFactory.INSTANCE,
                                cache,
                                ActiveConnectionTracker.NO_TRACKING
                        )
                ) {
                    context.getOrCreateSqlExecutionContext(engine, 1);
                    final TrackingRecordCursorFactory factory = new TrackingRecordCursorFactory(engine.events);
                    try (JsonQueryProcessorState state = new JsonQueryProcessorState(context, cairoConfiguration.getNanosecondClock(), null)) {
                        engine.nextOwnerId = 7;
                        engine.isEndFailing = true;
                        state.beginSqlExecutionOwner("SELECT 1", context.getSqlExecutionContext(), CompiledQuery.SELECT);
                        state.setCursor(new TrackingRecordCursor(engine.events));
                        state.setFactoryForTest(factory, true);
                        try {
                            state.clear();
                            Assert.fail("owner end failure was expected");
                        } catch (RuntimeException e) {
                            Assert.assertEquals("owner end failed", e.getMessage());
                        }
                        Assert.assertEquals(List.of("cursor.close", "owner.end", "factory.close"), engine.events);
                        Assert.assertNull(cache.factory);
                    }
                }
            }
        });
    }

    @Test
    public void testOwnerEndsBeforeFactoryIsCached() throws Exception {
        // Ending the owner releases the execution's resources, which include state of the
        // factory's functions. Another connection can take the factory from the shared select
        // cache as soon as it is there, so the owner has to end first.
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration cairoConfiguration = new DefaultTestCairoConfiguration(root);
            final DefaultHttpServerConfiguration httpConfiguration =
                    new DefaultHttpServerConfiguration(cairoConfiguration);
            try (TrackingCairoEngine engine = new TrackingCairoEngine(cairoConfiguration)) {
                final TrackingSelectCache cache = new TrackingSelectCache(engine.events);
                try (
                        cache;
                        HttpConnectionContext context = new HttpConnectionContext(
                                httpConfiguration,
                                PlainSocketFactory.INSTANCE,
                                cache,
                                ActiveConnectionTracker.NO_TRACKING
                        )
                ) {
                    context.getOrCreateSqlExecutionContext(engine, 1);
                    final TrackingRecordCursorFactory factory = new TrackingRecordCursorFactory(engine.events);
                    try (JsonQueryProcessorState state = new JsonQueryProcessorState(context, cairoConfiguration.getNanosecondClock(), null)) {
                        engine.nextOwnerId = 7;
                        state.beginSqlExecutionOwner("SELECT 1", context.getSqlExecutionContext(), CompiledQuery.SELECT);
                        state.setCursor(new TrackingRecordCursor(engine.events));
                        state.setFactoryForTest(factory, true);
                        state.clear();
                        Assert.assertEquals(List.of("cursor.close", "owner.end", "cache.put"), engine.events);
                        Assert.assertSame(factory, cache.factory);
                    }
                }
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
                    engine.nextOwnerId = 7;
                    state.beginSqlExecutionOwner("SELECT 1", context.getSqlExecutionContext(), CompiledQuery.SELECT);
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
                    engine.nextOwnerId = 7;
                    state.beginSqlExecutionOwner("SELECT 1", context.getSqlExecutionContext(), CompiledQuery.SELECT);
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
                    engine.nextOwnerId = 11;
                    state.beginSqlExecutionOwner("SELECT 1", context.getSqlExecutionContext(), CompiledQuery.SELECT);
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
                    engine.nextOwnerId = -1;
                    state.beginSqlExecutionOwner("SELECT 1", context.getSqlExecutionContext(), CompiledQuery.SELECT);
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

    private static final class TrackingRecordCursorFactory implements RecordCursorFactory {
        private final List<String> events;

        private TrackingRecordCursorFactory(List<String> events) {
            this.events = events;
        }

        @Override
        public void close() {
            events.add("factory.close");
        }

        @Override
        public RecordMetadata getMetadata() {
            return null;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }
    }

    private static final class TrackingSelectCache implements AssociativeCache<RecordCursorFactory> {
        private final List<String> events;
        private RecordCursorFactory factory;

        private TrackingSelectCache(List<String> events) {
            this.events = events;
        }

        @Override
        public int capacity() {
            return 1;
        }

        @Override
        public void clear() {
            factory = Misc.free(factory);
        }

        @Override
        public void close() {
            clear();
        }

        @Override
        public RecordCursorFactory poll(CharSequence key) {
            final RecordCursorFactory polled = factory;
            factory = null;
            return polled;
        }

        @Override
        public void put(CharSequence key, RecordCursorFactory value) {
            events.add("cache.put");
            Misc.free(factory);
            factory = value;
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
        private final List<String> events = new ArrayList<>();
        private int endCount;
        private long endedOwnerId = Long.MIN_VALUE;
        private boolean isEndFailing;
        private int mountCount;
        private long nextOwnerId;
        private int unmountCount;

        private TrackingCairoEngine(DefaultTestCairoConfiguration configuration) {
            super(configuration);
        }

        @Override
        public long beginSqlExecution(CharSequence query, SqlExecutionContext executionContext, short compiledQueryType) {
            return nextOwnerId;
        }

        @Override
        public void endSqlExecution(long ownerId, SqlExecutionContext executionContext) {
            endCount++;
            endedOwnerId = ownerId;
            events.add("owner.end");
            if (isEndFailing) {
                throw new RuntimeException("owner end failed");
            }
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
