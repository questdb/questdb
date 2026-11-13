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

package io.questdb.test.cutlass.qwp;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.qwp.server.egress.QwpEgressProcessorState;
import io.questdb.cutlass.qwp.server.egress.QwpEgressUpgradeProcessor;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.AssociativeCache;
import io.questdb.std.Misc;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class QwpEgressResourceLifecycleTest extends AbstractTest {

    @Test
    public void testEndWhileParkedDoesNotRemountOwner() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            try (
                    TrackingCairoEngine engine = new TrackingCairoEngine(configuration);
                    SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(engine, 1)
                            .with(AllowAllSecurityContext.INSTANCE);
                    QwpEgressProcessorState state = new QwpEgressProcessorState(configuration)
            ) {
                state.beginSqlExecutionOwner("SELECT 1", executionContext, CompiledQuery.SELECT);
                state.beginStreaming(
                        1,
                        null,
                        new TrackingRecordCursor(engine.events),
                        0,
                        0,
                        "SELECT 1",
                        CompiledQuery.SELECT,
                        false
                );
                state.parkSqlExecutionOwner();

                state.endStreaming();

                Assert.assertEquals(
                        List.of(
                                "owner.begin",
                                "cursor.suspend",
                                "owner.unmount",
                                "cursor.close",
                                "owner.end"
                        ),
                        engine.events
                );
            }
        });
    }

    @Test
    public void testOwnerEndFailureFreesFactoryInsteadOfCachingIt() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            try (
                    TrackingCairoEngine engine = new TrackingCairoEngine(configuration);
                    SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(engine, 1)
                            .with(AllowAllSecurityContext.INSTANCE);
                    QwpEgressProcessorState state = new QwpEgressProcessorState(configuration)
            ) {
                final TrackingSelectCache cache = new TrackingSelectCache(engine.events);
                try (cache) {
                    final TrackingRecordCursorFactory factory = new TrackingRecordCursorFactory();
                    engine.isEndFailing = true;
                    state.beginSqlExecutionOwner("SELECT 1", executionContext, CompiledQuery.SELECT);
                    state.beginStreaming(
                            1,
                            factory,
                            new TrackingRecordCursor(engine.events),
                            0,
                            0,
                            "SELECT 1",
                            CompiledQuery.SELECT,
                            true
                    );
                    state.cacheStreamingFactoryOnEnd(cache);
                    try {
                        state.endStreaming();
                        Assert.fail("owner end failure was expected");
                    } catch (RuntimeException e) {
                        Assert.assertEquals("owner end failed", e.getMessage());
                    }
                    Assert.assertEquals(List.of("owner.begin", "cursor.close", "owner.end"), engine.events);
                    Assert.assertNull(cache.factory);
                    Assert.assertTrue(factory.closed);
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
            final DefaultTestCairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            try (
                    TrackingCairoEngine engine = new TrackingCairoEngine(configuration);
                    SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(engine, 1)
                            .with(AllowAllSecurityContext.INSTANCE);
                    QwpEgressProcessorState state = new QwpEgressProcessorState(configuration)
            ) {
                final TrackingSelectCache cache = new TrackingSelectCache(engine.events);
                try (cache) {
                    final TrackingRecordCursorFactory factory = new TrackingRecordCursorFactory();
                    state.beginSqlExecutionOwner("SELECT 1", executionContext, CompiledQuery.SELECT);
                    state.beginStreaming(
                            1,
                            factory,
                            new TrackingRecordCursor(engine.events),
                            0,
                            0,
                            "SELECT 1",
                            CompiledQuery.SELECT,
                            true
                    );
                    state.cacheStreamingFactoryOnEnd(cache);
                    Assert.assertEquals(List.of("owner.begin"), engine.events);

                    state.endStreaming();
                    Assert.assertEquals(
                            List.of("owner.begin", "cursor.close", "owner.end", "cache.put"),
                            engine.events
                    );
                    Assert.assertSame(factory, cache.factory);
                    Assert.assertEquals("SELECT 1", cache.key);
                    Assert.assertFalse(factory.closed);
                }
            }
        });
    }

    @Test
    public void testParkResumeAndEndOrder() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            try (
                    TrackingCairoEngine engine = new TrackingCairoEngine(configuration);
                    SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(engine, 1)
                            .with(AllowAllSecurityContext.INSTANCE);
                    QwpEgressProcessorState state = new QwpEgressProcessorState(configuration)
            ) {
                state.beginSqlExecutionOwner("SELECT 1", executionContext, CompiledQuery.SELECT);
                state.beginStreaming(
                        1,
                        null,
                        new TrackingRecordCursor(engine.events),
                        0,
                        0,
                        "SELECT 1",
                        CompiledQuery.SELECT,
                        false
                );

                state.parkSqlExecutionOwner();
                Assert.assertEquals(
                        List.of("owner.begin", "cursor.suspend", "owner.unmount"),
                        engine.events
                );

                state.resumeSqlExecutionOwner();
                Assert.assertEquals(
                        List.of(
                                "owner.begin",
                                "cursor.suspend",
                                "owner.unmount",
                                "cursor.resume",
                                "owner.mount"
                        ),
                        engine.events
                );

                state.endStreaming();
                Assert.assertEquals(
                        List.of(
                                "owner.begin",
                                "cursor.suspend",
                                "owner.unmount",
                                "cursor.resume",
                                "owner.mount",
                                "cursor.close",
                                "owner.end"
                        ),
                        engine.events
                );
            }
        });
    }

    @Test
    public void testPseudoSelectFactoryIsNotCached() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            try (
                    TrackingCairoEngine engine = new TrackingCairoEngine(configuration);
                    QwpEgressUpgradeProcessor processor = new QwpEgressUpgradeProcessor(
                            engine,
                            new DefaultHttpServerConfiguration(configuration),
                            1
                    );
                    QwpEgressProcessorState state = new QwpEgressProcessorState(configuration)
            ) {
                final String sql = "COPY tab TO 'tab.csv'";
                final TrackingRecordCursorFactory factory = new TrackingRecordCursorFactory();
                state.beginStreaming(
                        1,
                        factory,
                        null,
                        0,
                        0,
                        sql,
                        CompiledQuery.PSEUDO_SELECT,
                        true
                );

                Assert.assertFalse(state.isStreamingFactoryCacheable());
                invokeCacheStreamingFactoryIfAvailable(processor, state);
                Assert.assertNull(selectCache(processor).poll(sql));

                state.endStreaming();
                Assert.assertTrue(factory.closed);
            }
        });
    }

    @Test
    public void testSelectFactoryIsCached() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            try (
                    TrackingCairoEngine engine = new TrackingCairoEngine(configuration);
                    QwpEgressUpgradeProcessor processor = new QwpEgressUpgradeProcessor(
                            engine,
                            new DefaultHttpServerConfiguration(configuration),
                            1
                    );
                    QwpEgressProcessorState state = new QwpEgressProcessorState(configuration)
            ) {
                final String sql = "SELECT 1";
                final TrackingRecordCursorFactory factory = new TrackingRecordCursorFactory();
                state.beginStreaming(
                        1,
                        factory,
                        null,
                        0,
                        0,
                        sql,
                        CompiledQuery.SELECT,
                        true
                );

                Assert.assertTrue(state.isStreamingFactoryCacheable());
                invokeCacheStreamingFactoryIfAvailable(processor, state);
                state.endStreaming();

                final RecordCursorFactory cachedFactory = selectCache(processor).poll(sql);
                Assert.assertSame(factory, cachedFactory);
                Assert.assertFalse(factory.closed);
                Misc.free(cachedFactory);
                Assert.assertTrue(factory.closed);
            }
        });
    }

    @Test
    public void testUnmanagedScopeStillEnds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            try (
                    TrackingCairoEngine engine = new TrackingCairoEngine(configuration, -1);
                    SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(engine, 1)
                            .with(AllowAllSecurityContext.INSTANCE);
                    QwpEgressProcessorState state = new QwpEgressProcessorState(configuration)
            ) {
                state.beginSqlExecutionOwner("SET x = 1", executionContext, CompiledQuery.SET);
                state.endSqlExecutionOwner();

                Assert.assertEquals(List.of("owner.begin", "owner.end"), engine.events);
                Assert.assertEquals(-1, engine.endedOwnerId);
            }
        });
    }

    private static void invokeCacheStreamingFactoryIfAvailable(
            QwpEgressUpgradeProcessor processor,
            QwpEgressProcessorState state
    ) throws Exception {
        final Method method = QwpEgressUpgradeProcessor.class.getDeclaredMethod(
                "cacheStreamingFactoryIfAvailable",
                QwpEgressProcessorState.class
        );
        method.setAccessible(true);
        method.invoke(processor, state);
    }

    @SuppressWarnings("unchecked")
    private static AssociativeCache<RecordCursorFactory> selectCache(QwpEgressUpgradeProcessor processor) throws Exception {
        final Field field = QwpEgressUpgradeProcessor.class.getDeclaredField("selectCache");
        field.setAccessible(true);
        return (AssociativeCache<RecordCursorFactory>) field.get(processor);
    }

    private static final class TrackingCairoEngine extends CairoEngine {
        private final List<String> events = new ArrayList<>();
        private final long ownerId;
        private long endedOwnerId = Long.MIN_VALUE;
        private boolean isEndFailing;

        private TrackingCairoEngine(DefaultTestCairoConfiguration configuration) {
            this(configuration, 19);
        }

        private TrackingCairoEngine(DefaultTestCairoConfiguration configuration, long ownerId) {
            super(configuration);
            this.ownerId = ownerId;
        }

        @Override
        public long beginSqlExecution(
                CharSequence query,
                SqlExecutionContext executionContext,
                short compiledQueryType
        ) {
            events.add("owner.begin");
            return ownerId;
        }

        @Override
        public void endSqlExecution(long ownerId, SqlExecutionContext executionContext) {
            endedOwnerId = ownerId;
            events.add("owner.end");
            if (isEndFailing) {
                throw new RuntimeException("owner end failed");
            }
        }

        @Override
        public void mountSqlExecution(long ownerId, SqlExecutionContext executionContext) {
            events.add("owner.mount");
        }

        @Override
        public void unmountSqlExecution(long ownerId, SqlExecutionContext executionContext) {
            events.add("owner.unmount");
        }
    }

    private static final class TrackingRecordCursorFactory implements RecordCursorFactory {
        private boolean closed;

        @Override
        public void close() {
            closed = true;
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

    private static final class TrackingSelectCache implements AssociativeCache<RecordCursorFactory> {
        private final List<String> events;
        private RecordCursorFactory factory;
        private String key;

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
            key = null;
        }

        @Override
        public void close() {
            clear();
        }

        @Override
        public RecordCursorFactory poll(CharSequence key) {
            final RecordCursorFactory polled = factory;
            factory = null;
            this.key = null;
            return polled;
        }

        @Override
        public void put(CharSequence key, RecordCursorFactory value) {
            events.add("cache.put");
            Misc.free(factory);
            factory = value;
            this.key = key.toString();
        }
    }
}
