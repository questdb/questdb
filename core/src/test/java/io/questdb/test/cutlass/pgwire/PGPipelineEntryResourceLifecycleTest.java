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

package io.questdb.test.cutlass.pgwire;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cutlass.pgwire.PGPipelineEntry;
import io.questdb.cutlass.pgwire.PGResponseSink;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.std.ObjObjHashMap;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class PGPipelineEntryResourceLifecycleTest extends AbstractTest {

    @Test
    public void testErrorSyncClosesSuspendedCursorBeforeRetryingResponse() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            try (
                    TrackingCairoEngine engine = new TrackingCairoEngine(configuration);
                    SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(engine, 1)
                            .with(AllowAllSecurityContext.INSTANCE);
                    PGPipelineEntry entry = new PGPipelineEntry(engine)
            ) {
                setCursor(entry, new TrackingRecordCursor(engine.events));
                invoke(
                        entry,
                        "beginSqlExecutionOwner",
                        new Class<?>[]{CharSequence.class, SqlExecutionContext.class, short.class},
                        "SELECT 1",
                        executionContext,
                        CompiledQuery.SELECT
                );
                invoke(entry, "unmountSqlExecutionOwnerAfterExecute", new Class<?>[0]);
                final Field stateSuspended = PGPipelineEntry.class.getDeclaredField("stateSuspended");
                stateSuspended.setAccessible(true);
                stateSuspended.setBoolean(entry, true);
                entry.getErrorMessageSink().put("admission refused");
                final AtomicBoolean isFirstWrite = new AtomicBoolean(true);
                // This sink injects buffer exhaustion only; lifecycle assertions do not
                // depend on any simulated client response or message bytes.
                final PGResponseSink sink = (PGResponseSink) Proxy.newProxyInstance(
                        PGResponseSink.class.getClassLoader(),
                        new Class<?>[]{PGResponseSink.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("put") && isFirstWrite.compareAndSet(true, false)) {
                                throw NoSpaceLeftInResponseBufferException.instance(1, 0, 4096);
                            }
                            if (method.getReturnType() == long.class) {
                                return method.getName().equals("getSendBufferSize") ? 4096L : 0L;
                            }
                            if (method.getReturnType() == boolean.class) {
                                return true;
                            }
                            return method.getReturnType().isInstance(proxy) ? proxy : null;
                        }
                );
                Assert.assertThrows(
                        NoSpaceLeftInResponseBufferException.class,
                        () -> entry.msgSync(executionContext, new ObjObjHashMap<>(), sink)
                );
                final List<String> retiredEvents = List.of(
                        "owner.begin", "cursor.suspend", "owner.unmount", "cursor.close", "owner.end"
                );
                Assert.assertEquals(retiredEvents, engine.events);
                Assert.assertFalse(entry.isSuspended());
                entry.msgSync(executionContext, new ObjObjHashMap<>(), sink);
                Assert.assertEquals(retiredEvents, engine.events);
                Assert.assertFalse(entry.isError());
            }
        });
    }

    @Test
    public void testExecuteToSyncGapCountsAsClientWait() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            try (
                    TrackingCairoEngine engine = new TrackingCairoEngine(configuration);
                    SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(engine, 1)
                            .with(AllowAllSecurityContext.INSTANCE)
            ) {
                try (PGPipelineEntry entry = new PGPipelineEntry(engine)) {
                    setCursor(entry, new TrackingRecordCursor(engine.events));
                    invoke(
                            entry,
                            "beginSqlExecutionOwner",
                            new Class<?>[]{CharSequence.class, SqlExecutionContext.class, short.class},
                            "SELECT 1",
                            executionContext,
                            CompiledQuery.SELECT
                    );
                    Assert.assertEquals(CompiledQuery.SELECT, engine.compiledQueryType);

                    invoke(entry, "unmountSqlExecutionOwnerAfterExecute", new Class<?>[0]);
                    Assert.assertEquals(
                            List.of("owner.begin", "cursor.suspend", "owner.unmount"),
                            engine.events
                    );

                    invoke(entry, "mountSqlExecutionOwnerForSync", new Class<?>[0]);
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
                }
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
    public void testFailedOwnerResumeStopsTimerAndDoesNotRemountForErrorSync() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            try (
                    TrackingCairoEngine engine = new TrackingCairoEngine(configuration);
                    SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(engine, 1)
                            .with(AllowAllSecurityContext.INSTANCE)
            ) {
                try (PGPipelineEntry entry = new PGPipelineEntry(engine)) {
                    setCursor(entry, new TrackingRecordCursor(engine.events));
                    invoke(
                            entry,
                            "beginSqlExecutionOwner",
                            new Class<?>[]{CharSequence.class, SqlExecutionContext.class, short.class},
                            "SELECT 1",
                            executionContext,
                            CompiledQuery.SELECT
                    );
                    invoke(entry, "unmountSqlExecutionOwnerAfterExecute", new Class<?>[0]);
                    engine.mountFailure = new IllegalStateException("owner mount failed");
                    final InvocationTargetException failure = Assert.assertThrows(
                            InvocationTargetException.class,
                            () -> invoke(entry, "resumeSqlExecutionOwner", new Class<?>[0])
                    );
                    Assert.assertSame(engine.mountFailure, failure.getCause());
                    final List<String> failedResumeEvents = List.of(
                            "owner.begin", "cursor.suspend", "owner.unmount",
                            "cursor.resume", "owner.mount", "cursor.suspend"
                    );
                    Assert.assertEquals(failedResumeEvents, engine.events);

                    entry.getErrorMessageSink().put(engine.mountFailure.getMessage());
                    invoke(entry, "mountSqlExecutionOwnerForSync", new Class<?>[0]);
                    Assert.assertEquals(failedResumeEvents, engine.events);
                }
                Assert.assertEquals(
                        List.of(
                                "owner.begin", "cursor.suspend", "owner.unmount",
                                "cursor.resume", "owner.mount", "cursor.suspend",
                                "cursor.close", "owner.end"
                        ),
                        engine.events
                );
            }
        });
    }

    @Test
    public void testManagedSuspendedCursorResumesOwner() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            try (
                    TrackingCairoEngine engine = new TrackingCairoEngine(configuration);
                    SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(engine, 1)
                            .with(AllowAllSecurityContext.INSTANCE)
            ) {
                try (PGPipelineEntry entry = new PGPipelineEntry(engine)) {
                    setCursor(entry, new TrackingRecordCursor(engine.events));
                    invoke(
                            entry,
                            "beginSqlExecutionOwner",
                            new Class<?>[]{CharSequence.class, SqlExecutionContext.class, short.class},
                            "SELECT 1",
                            executionContext,
                            CompiledQuery.SELECT
                    );
                    invoke(entry, "unmountSqlExecutionOwnerAfterExecute", new Class<?>[0]);

                    invoke(entry, "mountSqlExecutionOwnerForSync", new Class<?>[0]);

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
                }
            }
        });
    }

    @Test
    public void testNonCursorExecuteToSyncGapDoesNotSuspendTimer() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            try (
                    TrackingCairoEngine engine = new TrackingCairoEngine(configuration);
                    SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(engine, 1)
                            .with(AllowAllSecurityContext.INSTANCE)
            ) {
                try (PGPipelineEntry entry = new PGPipelineEntry(engine)) {
                    invoke(
                            entry,
                            "beginSqlExecutionOwner",
                            new Class<?>[]{CharSequence.class, SqlExecutionContext.class, short.class},
                            "SET x = 1",
                            executionContext,
                            CompiledQuery.SET
                    );
                    invoke(entry, "unmountSqlExecutionOwnerAfterExecute", new Class<?>[0]);
                    invoke(entry, "mountSqlExecutionOwnerForSync", new Class<?>[0]);
                    Assert.assertEquals(
                            List.of("owner.begin", "owner.unmount"),
                            engine.events
                    );
                }
                Assert.assertEquals(
                        List.of("owner.begin", "owner.unmount", "owner.end"),
                        engine.events
                );
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
                            .with(AllowAllSecurityContext.INSTANCE)
            ) {
                try (PGPipelineEntry entry = new PGPipelineEntry(engine)) {
                    invoke(
                            entry,
                            "beginSqlExecutionOwner",
                            new Class<?>[]{CharSequence.class, SqlExecutionContext.class, short.class},
                            "SET x = 1",
                            executionContext,
                            CompiledQuery.SET
                    );
                    invoke(entry, "mountSqlExecutionOwner", new Class<?>[0]);
                    invoke(entry, "unmountSqlExecutionOwner", new Class<?>[0]);
                }
                Assert.assertEquals(List.of("owner.begin", "owner.end"), engine.events);
                Assert.assertEquals(-1, engine.endedOwnerId);
            }
        });
    }

    @Test
    public void testUnmanagedSuspendedCursorResumesWithoutOwnerMount() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            try (
                    TrackingCairoEngine engine = new TrackingCairoEngine(configuration, -1);
                    SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(engine, 1)
                            .with(AllowAllSecurityContext.INSTANCE)
            ) {
                try (PGPipelineEntry entry = new PGPipelineEntry(engine)) {
                    setCursor(entry, new TrackingRecordCursor(engine.events));
                    invoke(
                            entry,
                            "beginSqlExecutionOwner",
                            new Class<?>[]{CharSequence.class, SqlExecutionContext.class, short.class},
                            "COPY tab TO 'tab.csv'",
                            executionContext,
                            CompiledQuery.PSEUDO_SELECT
                    );

                    invoke(entry, "mountSqlExecutionOwnerForSync", new Class<?>[0]);

                    Assert.assertEquals(List.of("owner.begin", "cursor.resume"), engine.events);
                }
            }
        });
    }

    private static void invoke(PGPipelineEntry entry, String methodName, Class<?>[] parameterTypes, Object... args) throws Exception {
        final Method method = PGPipelineEntry.class.getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        method.invoke(entry, args);
    }

    private static void setCursor(PGPipelineEntry entry, RecordCursor cursor) throws Exception {
        final Field field = PGPipelineEntry.class.getDeclaredField("cursor");
        field.setAccessible(true);
        field.set(entry, cursor);
    }

    private static final class TrackingCairoEngine extends CairoEngine {
        private short compiledQueryType;
        private long endedOwnerId = Long.MIN_VALUE;
        private final List<String> events = new ArrayList<>();
        private RuntimeException mountFailure;
        private final long ownerId;

        private TrackingCairoEngine(DefaultTestCairoConfiguration configuration) {
            this(configuration, 23);
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
            this.compiledQueryType = compiledQueryType;
            events.add("owner.begin");
            return ownerId;
        }

        @Override
        public void endSqlExecution(long ownerId, SqlExecutionContext executionContext) {
            endedOwnerId = ownerId;
            events.add("owner.end");
        }

        @Override
        public void mountSqlExecution(long ownerId, SqlExecutionContext executionContext) {
            events.add("owner.mount");
            if (mountFailure != null) {
                throw mountFailure;
            }
        }

        @Override
        public void unmountSqlExecution(long ownerId, SqlExecutionContext executionContext) {
            events.add("owner.unmount");
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
}
