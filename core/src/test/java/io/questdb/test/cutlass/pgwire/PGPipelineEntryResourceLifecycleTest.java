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
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class PGPipelineEntryResourceLifecycleTest extends AbstractTest {

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
    public void testManagedSuspendedCursorRestoresStatementBypass() throws Exception {
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
                    Assert.assertFalse(executionContext.isResourceGroupBypassed());
                    invoke(entry, "unmountSqlExecutionOwnerAfterExecute", new Class<?>[0]);

                    // Another protocol statement can reuse and mutate the connection context while
                    // this portal is suspended. Restoring from ownerId would be wrong here: a
                    // managed statement has a positive owner but must restore bypass=false.
                    executionContext.setResourceGroupBypassed(true);
                    invoke(entry, "mountSqlExecutionOwnerForSync", new Class<?>[0]);

                    Assert.assertFalse(executionContext.isResourceGroupBypassed());
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
                Assert.assertFalse(executionContext.isResourceGroupBypassed());
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
    public void testUnmanagedSuspendedCursorRestoresStatementBypass() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            try (
                    TrackingCairoEngine engine = new TrackingCairoEngine(configuration, -1, true);
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
                    Assert.assertTrue(executionContext.isResourceGroupBypassed());

                    // Another protocol statement may reset the shared connection context while this
                    // cursor is suspended. Its own statement classification must win when it resumes.
                    executionContext.setResourceGroupBypassed(false);
                    invoke(entry, "mountSqlExecutionOwnerForSync", new Class<?>[0]);

                    Assert.assertTrue(executionContext.isResourceGroupBypassed());
                    Assert.assertEquals(List.of("owner.begin", "cursor.resume"), engine.events);
                }
                Assert.assertFalse(executionContext.isResourceGroupBypassed());
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
        private final boolean bypassOnBegin;
        private long endedOwnerId = Long.MIN_VALUE;
        private final List<String> events = new ArrayList<>();
        private final long ownerId;

        private TrackingCairoEngine(DefaultTestCairoConfiguration configuration) {
            this(configuration, 23, false);
        }

        private TrackingCairoEngine(DefaultTestCairoConfiguration configuration, long ownerId) {
            this(configuration, ownerId, false);
        }

        private TrackingCairoEngine(
                DefaultTestCairoConfiguration configuration,
                long ownerId,
                boolean bypassOnBegin
        ) {
            super(configuration);
            this.ownerId = ownerId;
            this.bypassOnBegin = bypassOnBegin;
        }

        @Override
        public long beginSqlExecution(
                CharSequence query,
                SqlExecutionContext executionContext,
                short compiledQueryType
        ) {
            this.compiledQueryType = compiledQueryType;
            executionContext.setResourceGroupBypassed(bypassOnBegin);
            events.add("owner.begin");
            return ownerId;
        }

        @Override
        public void endSqlExecution(long ownerId, SqlExecutionContext executionContext) {
            endedOwnerId = ownerId;
            executionContext.setResourceGroupBypassed(false);
            events.add("owner.end");
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
