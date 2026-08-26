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

package io.questdb.test.cutlass.http;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.http.processors.ExportQueryProcessorState;
import io.questdb.cutlass.parquet.CopyExportRequestTask;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.griffin.QueryRegistry;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ExportQueryProcessorStateTest extends AbstractCairoTest {

    @Test
    public void testClearClosesAndUnregistersCursorAfterTaskCleanupFailure() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "state_clear_failure";
            execute("CREATE TABLE " + tableName + " AS (SELECT x FROM long_sequence(1))");
            final QueryRegistry registry = engine.getQueryRegistry();
            final AtomicLong queryId = new AtomicLong(-1);
            registry.setListener((query, id, context) -> {
                if (tableName.contentEquals(query)) {
                    queryId.set(id);
                }
            });

            final RuntimeException cleanupFailure = new RuntimeException("expected task cleanup failure");
            final ExportQueryProcessorState state = new ExportQueryProcessorState(null, null);
            try (RecordCursorFactory factory = select(tableName)) {
                Assert.assertTrue(factory instanceof QueryProgress);
                final RecordCursor cursor = factory.getCursor(sqlExecutionContext);
                Assert.assertTrue(queryId.get() >= 0);
                Assert.assertNotNull(registry.getEntry(queryId.get()));
                state.setTaskAndCursorForTest(
                        new CopyExportRequestTask() {
                            private boolean isFailurePending = true;

                            @Override
                            public void clear() {
                                super.clear();
                                if (isFailurePending) {
                                    isFailurePending = false;
                                    Assert.assertNotNull("task must clear before query cursor closes", registry.getEntry(queryId.get()));
                                    throw cleanupFailure;
                                }
                            }
                        },
                        cursor
                );

                final RuntimeException thrown = Assert.assertThrows(RuntimeException.class, state::clear);
                Assert.assertSame(cleanupFailure, thrown);
                Assert.assertNull("cleanup failure must not strand query registration", registry.getEntry(queryId.get()));
                Assert.assertEquals("cleanup failure must not strand raw table reader", 0, engine.getBusyReaderCount());
            } finally {
                registry.setListener(null);
                state.close();
            }
        });
    }

    @Test
    public void testCloseReleasesInternalTaskResourcesBeforeUnregisteringCursor() throws Exception {
        assertMemoryLeak(() -> {
            final String tableName = "state_internal_close_failure";
            execute("CREATE TABLE " + tableName + " AS (SELECT x FROM long_sequence(1))");
            final QueryRegistry registry = engine.getQueryRegistry();
            final AtomicLong queryId = new AtomicLong(-1);
            registry.setListener((query, id, context) -> {
                if (tableName.contentEquals(query)) {
                    queryId.set(id);
                }
            });

            final MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(
                    sqlExecutionContext.getSecurityContext(),
                    42,
                    MemoryTrackerWorkload.QUERY
            );
            final ExportQueryProcessorState state = new ExportQueryProcessorState(null, null);
            final CopyExportRequestTask task = new CopyExportRequestTask();
            try (RecordCursorFactory factory = select(tableName)) {
                Assert.assertTrue(factory instanceof QueryProgress);
                final RecordCursor cursor = factory.getCursor(sqlExecutionContext);
                Assert.assertTrue(queryId.get() >= 0);
                Assert.assertNotNull(registry.getEntry(queryId.get()));

                task.setMemoryTracker(tracker);
                final RuntimeException firstStreamFailure = new RuntimeException("expected decode-column close failure");
                final RuntimeException secondStreamFailure = new RuntimeException("expected column-data close failure");
                final RuntimeException factoryFailure = new RuntimeException("expected select-factory close failure");
                final AtomicBoolean isDecodeTrackerAttached = new AtomicBoolean();
                final AtomicBoolean isDecodeOwnerRegistered = new AtomicBoolean();
                final AtomicBoolean isColumnDataTrackerAttached = new AtomicBoolean();
                final AtomicBoolean isColumnDataOwnerRegistered = new AtomicBoolean();
                final ThrowingDirectIntList decodeColumns = new ThrowingDirectIntList(
                        firstStreamFailure,
                        () -> recordTrackerOwnerState(
                                task,
                                tracker,
                                registry,
                                queryId.get(),
                                isDecodeTrackerAttached,
                                isDecodeOwnerRegistered
                        )
                );
                final ThrowingDirectLongList columnData = new ThrowingDirectLongList(
                        secondStreamFailure,
                        () -> recordTrackerOwnerState(
                                task,
                                tracker,
                                registry,
                                queryId.get(),
                                isColumnDataTrackerAttached,
                                isColumnDataOwnerRegistered
                        )
                );
                replaceCloseableField(task.getStreamPartitionParquetExporter(), "decodeColumns", decodeColumns);
                replaceCloseableField(task.getStreamPartitionParquetExporter(), "columnData", columnData);
                final ThrowingFactory selectFactory = new ThrowingFactory(factoryFailure);
                task.setSelectFactory(selectFactory);
                state.setTaskAndCursorForTest(task, cursor);

                final RuntimeException thrown = Assert.assertThrows(RuntimeException.class, state::close);
                Assert.assertSame(firstStreamFailure, thrown);
                Assert.assertEquals(1, decodeColumns.closeCount);
                Assert.assertEquals(1, columnData.closeCount);
                Assert.assertTrue("decode columns must close while tracker is attached", isDecodeTrackerAttached.get());
                Assert.assertTrue("decode columns must close before query unregister", isDecodeOwnerRegistered.get());
                Assert.assertTrue("column data must close while tracker is attached", isColumnDataTrackerAttached.get());
                Assert.assertTrue("column data must close before query unregister", isColumnDataOwnerRegistered.get());
                Assert.assertEquals(1, selectFactory.closeCount);
                Assert.assertNull("task must release its tracker reference before state closes the cursor", task.getMemoryTracker());
                Assert.assertEquals(2, thrown.getSuppressed().length);
                Assert.assertSame(secondStreamFailure, thrown.getSuppressed()[0]);
                Assert.assertSame(factoryFailure, thrown.getSuppressed()[1]);
                Assert.assertNull("state must unregister the query after internal task cleanup", registry.getEntry(queryId.get()));
                Assert.assertEquals("state must release the raw reader after internal task cleanup", 0, engine.getBusyReaderCount());
            } finally {
                registry.setListener(null);
                state.close();
                task.close();
                tracker.close();
            }
        });
    }

    @Test
    public void testReleaseEntryFailureDoesNotRetainCopyId() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException releaseFailure = new RuntimeException("expected release failure");
            final ThrowingCopyExportContext copyExportContext = new ThrowingCopyExportContext(engine, releaseFailure);
            final ExportQueryProcessorState state = new ExportQueryProcessorState(null, copyExportContext);
            try {
                final Field copyIdField = ExportQueryProcessorState.class.getDeclaredField("copyID");
                copyIdField.setAccessible(true);
                copyIdField.setLong(state, 42);

                final RuntimeException thrown = Assert.assertThrows(RuntimeException.class, state::clear);
                Assert.assertSame(releaseFailure, thrown);
                state.clear();
                Assert.assertEquals("failed release must not be retried by pooled state", 1, copyExportContext.releaseCount);
            } finally {
                state.close();
            }
        });
    }

    private static void recordTrackerOwnerState(
            CopyExportRequestTask task,
            MemoryTracker tracker,
            QueryRegistry registry,
            long queryId,
            AtomicBoolean isTrackerAttached,
            AtomicBoolean isOwnerRegistered
    ) {
        isTrackerAttached.set(task.getMemoryTracker() == tracker);
        isOwnerRegistered.set(registry.getEntry(queryId) != null);
    }

    private static void replaceCloseableField(Object target, String fieldName, AutoCloseable replacement) throws Exception {
        final Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        final AutoCloseable current = (AutoCloseable) field.get(target);
        current.close();
        field.set(target, replacement);
    }

    private static final class ThrowingCopyExportContext extends CopyExportContext {
        private final ExportTaskEntry entry = new ExportTaskEntry();
        private final RuntimeException failure;
        private int releaseCount;

        private ThrowingCopyExportContext(CairoEngine engine, RuntimeException failure) {
            super(engine);
            this.failure = failure;
        }

        @Override
        public ExportTaskEntry getEntry(long id) {
            return entry;
        }

        @Override
        public void releaseEntry(ExportTaskEntry entry) {
            releaseCount++;
            if (releaseCount == 1) {
                throw failure;
            }
        }
    }

    private static final class ThrowingDirectIntList extends DirectIntList {
        private int closeCount;
        private final RuntimeException failure;
        private final Runnable onClose;

        private ThrowingDirectIntList(RuntimeException failure, Runnable onClose) {
            super(2, MemoryTag.NATIVE_PARQUET_EXPORTER);
            this.failure = failure;
            this.onClose = onClose;
        }

        @Override
        public void close() {
            closeCount++;
            if (closeCount == 1) {
                onClose.run();
                try {
                    throw failure;
                } finally {
                    super.close();
                }
            }
            super.close();
        }
    }

    private static final class ThrowingDirectLongList extends DirectLongList {
        private int closeCount;
        private final RuntimeException failure;
        private final Runnable onClose;

        private ThrowingDirectLongList(RuntimeException failure, Runnable onClose) {
            super(2, MemoryTag.NATIVE_PARQUET_EXPORTER);
            this.failure = failure;
            this.onClose = onClose;
        }

        @Override
        public void close() {
            closeCount++;
            if (closeCount == 1) {
                onClose.run();
                try {
                    throw failure;
                } finally {
                    super.close();
                }
            }
            super.close();
        }
    }

    private static final class ThrowingFactory extends AbstractRecordCursorFactory {
        private int closeCount;
        private final RuntimeException failure;

        private ThrowingFactory(RuntimeException failure) {
            super(new GenericRecordMetadata());
            this.failure = failure;
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        @Override
        protected void _close() {
            closeCount++;
            throw failure;
        }
    }
}
