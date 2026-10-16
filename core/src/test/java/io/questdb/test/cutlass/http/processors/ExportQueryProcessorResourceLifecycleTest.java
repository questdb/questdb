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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.processors.ExportQueryProcessor;
import io.questdb.cutlass.http.processors.ExportQueryProcessorState;
import io.questdb.cutlass.parquet.CopyExportRequestTask;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.network.PlainSocketFactory;
import io.questdb.test.AbstractTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static io.questdb.griffin.model.ExportModel.COPY_FORMAT_PARQUET;

public class ExportQueryProcessorResourceLifecycleTest extends AbstractTest {

    @Test
    public void testParkResumeAndEndOrder() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration cairoConfiguration = new DefaultTestCairoConfiguration(root);
            final DefaultHttpServerConfiguration httpConfiguration =
                    new DefaultHttpServerConfiguration(cairoConfiguration);
            try (
                    TrackingCairoEngine engine = new TrackingCairoEngine(cairoConfiguration);
                    HttpConnectionContext context = new HttpConnectionContext(httpConfiguration, PlainSocketFactory.INSTANCE)
            ) {
                final SqlExecutionContext executionContext = context.getOrCreateSqlExecutionContext(engine, 1);
                try (ExportQueryProcessorState state = new ExportQueryProcessorState(context, null)) {
                    state.setTaskAndCursorForTest(new CopyExportRequestTask(), new TrackingRecordCursor(engine.events));
                    state.beginSqlExecutionOwner("SELECT 1", executionContext, CompiledQuery.SELECT);

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
    public void testResponseOnlyResumeDoesNotMountOwner() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final DefaultTestCairoConfiguration cairoConfiguration = new DefaultTestCairoConfiguration(root);
            final DefaultHttpServerConfiguration httpConfiguration =
                    new DefaultHttpServerConfiguration(cairoConfiguration);
            try (
                    TrackingCairoEngine engine = new TrackingCairoEngine(cairoConfiguration);
                    HttpConnectionContext context = new HttpConnectionContext(httpConfiguration, PlainSocketFactory.INSTANCE)
            ) {
                final SqlExecutionContext executionContext = context.getOrCreateSqlExecutionContext(engine, 1);
                try (ExportQueryProcessorState state = new ExportQueryProcessorState(context, null)) {
                    state.setTaskAndCursorForTest(new CopyExportRequestTask(), new TrackingRecordCursor(engine.events));
                    state.getExportModel().setFormat(COPY_FORMAT_PARQUET);
                    setQueryState(state, "QUERY_PARQUET_FILE_SEND_COMPLETE");
                    state.beginSqlExecutionOwner("SELECT 1", executionContext, CompiledQuery.SELECT);
                    state.parkSqlExecutionOwner();
                    state.resumeSqlExecutionOwner();

                    Assert.assertEquals(
                            List.of("owner.begin", "cursor.suspend", "owner.unmount"),
                            engine.events
                    );
                }
                Assert.assertEquals(
                        List.of("owner.begin", "cursor.suspend", "owner.unmount", "cursor.close", "owner.end"),
                        engine.events
                );
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
                    TrackingCairoEngine engine = new TrackingCairoEngine(cairoConfiguration, -1);
                    HttpConnectionContext context = new HttpConnectionContext(httpConfiguration, PlainSocketFactory.INSTANCE)
            ) {
                final SqlExecutionContext executionContext = context.getOrCreateSqlExecutionContext(engine, 1);
                try (ExportQueryProcessorState state = new ExportQueryProcessorState(context, null)) {
                    state.beginSqlExecutionOwner("SET x = 1", executionContext, CompiledQuery.SET);
                }
                Assert.assertEquals(List.of("owner.begin", "owner.end"), engine.events);
                Assert.assertEquals(-1, engine.endedOwnerId);
            }
        });
    }

    private static void setQueryState(ExportQueryProcessorState state, String stateConstantName) throws Exception {
        final Field constantField = ExportQueryProcessor.class.getDeclaredField(stateConstantName);
        constantField.setAccessible(true);
        final Field queryStateField = ExportQueryProcessorState.class.getDeclaredField("queryState");
        queryStateField.setAccessible(true);
        queryStateField.setInt(state, constantField.getInt(null));
    }

    private static final class TrackingCairoEngine extends CairoEngine {
        private long endedOwnerId = Long.MIN_VALUE;
        private final List<String> events = new ArrayList<>();
        private final long ownerId;

        private TrackingCairoEngine(DefaultTestCairoConfiguration configuration) {
            this(configuration, 17);
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
