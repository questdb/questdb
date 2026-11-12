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

import io.questdb.PropertyKey;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cutlass.http.ActiveConnectionTracker;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpConnectionContext;
import io.questdb.cutlass.http.processors.ExportQueryProcessorState;
import io.questdb.cutlass.parquet.CopyExportRequestTask;
import io.questdb.cutlass.parquet.HTTPSerialParquetExporter;
import io.questdb.cutlass.parquet.HybridColumnMaterializer;
import io.questdb.cutlass.parquet.ParquetExportMode;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.griffin.QueryRegistry;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.metrics.QueryTrace;
import io.questdb.mp.ConcurrentQueue;
import io.questdb.network.PlainSocketFactory;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.NoOpAssociativeCache;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ExportQueryProcessorStateTest extends AbstractCairoTest {

    @Before
    public void enableQueryTracing() {
        node1.getConfigurationOverrides().setProperty(PropertyKey.QUERY_TRACING_ENABLED, true);
    }

    @Test
    public void testCachePutFailureReleasesFactory() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException failure = new RuntimeException("expected cache put failure");
            try (
                    HttpConnectionContext context = new HttpConnectionContext(
                            new DefaultHttpServerConfiguration(configuration),
                            PlainSocketFactory.INSTANCE,
                            new NoOpAssociativeCache<>() {
                                @Override
                                public void put(CharSequence key, RecordCursorFactory factory) {
                                    super.put(key, factory);
                                    throw failure;
                                }
                            },
                            ActiveConnectionTracker.NO_TRACKING
                    );
                    ExportQueryProcessorState state = new ExportQueryProcessorState(context, null)
            ) {
                final String sql = "SELECT regexp_replace(x::varchar, '^([0-9])$', '$1') FROM long_sequence(3)";
                state.setQueryFactoryForTest(select(sql), sql, true);
                Assert.assertSame(failure, Assert.assertThrows(RuntimeException.class, state::clear));
                Assert.assertFalse(state.isQueryCacheable());
                state.clear();
            }
        });
    }

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
    public void testClearDiscardsCacheableFactoryAfterCleanupFailure() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException failure = new RuntimeException("expected task cleanup failure");
            try (ExportQueryProcessorState state = new ExportQueryProcessorState(null, null)) {
                final String sql = "SELECT regexp_replace(x::varchar, '^([0-9])$', '$1') FROM long_sequence(3)";
                state.setQueryFactoryForTest(select(sql), sql, true);
                state.setTaskAndCursorForTest(new CopyExportRequestTask() {
                    private boolean hasFailed;

                    @Override
                    public void clear() {
                        super.clear();
                        if (!hasFailed) {
                            hasFailed = true;
                            throw failure;
                        }
                    }
                }, null);
                Assert.assertSame(failure, Assert.assertThrows(RuntimeException.class, state::clear));
                Assert.assertFalse(state.isQueryCacheable());
                state.clear();
            }
        });
    }

    @Test
    public void testClearReleasesMaterializerFunctionsBeforeUnregisteringQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE export_cast AS (SELECT ('k' || (x % 300))::varchar v, x l FROM long_sequence(1000))");
            try (RecordCursorFactory factory = select("SELECT v::symbol s, l FROM export_cast")) {
                Assert.assertTrue(factory instanceof QueryProgress);
                Assert.assertEquals(
                        ParquetExportMode.PAGE_FRAME_BACKED,
                        ParquetExportMode.determineExportMode(factory, false, sqlExecutionContext)
                );
                for (MaterializerReleasePath releasePath : MaterializerReleasePath.values()) {
                    assertReleasesMaterializerFunctionsBeforeUnregister(factory, releasePath);
                }
            }
        });
    }

    @Test
    public void testCloseAndNonCacheableClearReleaseFactories() throws Exception {
        assertMemoryLeak(() -> {
            for (boolean isClosing : new boolean[]{false, true}) {
                try (ExportQueryProcessorState state = new ExportQueryProcessorState(null, null)) {
                    final String sql = "SELECT regexp_replace(x::varchar, '^([0-9])$', '$1') FROM long_sequence(3)";
                    state.setQueryFactoryForTest(select(sql), sql, isClosing);
                    if (isClosing) {
                        state.close();
                    } else {
                        state.clear();
                    }
                    Assert.assertFalse(state.isQueryCacheable());
                }
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
    public void testDirectPageFrameCursorTimerNotificationsAreForwarded() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE direct_page_frame_timer AS (SELECT x FROM long_sequence(1))");
            final ConcurrentQueue<QueryTrace> queue = engine.getMessageBus().getQueryTraceQueue();
            drain(queue);
            setCurrentMicros(1_000);
            try (
                    RecordCursorFactory factory = select("direct_page_frame_timer");
                    ExportQueryProcessorState state = new ExportQueryProcessorState(null, null)
            ) {
                PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                setPrivateField(state, "pageFrameCursor", cursor);
                setPrivateField(state, "parquetExportMode", ParquetExportMode.DIRECT_PAGE_FRAME);

                setCurrentMicros(1_100);
                state.suspendCursorTimer();
                setCurrentMicros(1_300);
                state.resumeCursorTimer();
                setCurrentMicros(1_500);
            }

            final QueryTrace trace = new QueryTrace();
            Assert.assertTrue(queue.tryDequeue(trace));
            Assert.assertEquals("direct_page_frame_timer", trace.queryText);
            Assert.assertEquals(500_000L, trace.executionNanos);
            Assert.assertEquals(200_000L, trace.clientWaitNanos);
            Assert.assertFalse(queue.tryDequeue(new QueryTrace()));
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

    // A PAGE_FRAME_BACKED export initializes the projection's functions against the query's memory
    // tracker, and a cast to symbol charges its native dictionary to that tracker. Closing the
    // page-frame cursor unregisters the query and returns the tracker to the pool, so the
    // dictionary must be gone by then.
    private static void assertReleasesMaterializerFunctionsBeforeUnregister(
            RecordCursorFactory factory,
            MaterializerReleasePath releasePath
    ) throws Exception {
        final VirtualRecordCursorFactory virtualFactory = (VirtualRecordCursorFactory) ParquetExportMode.unwrapFactory(factory);
        final long nativeBaseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
        final ExportQueryProcessorState state = new ExportQueryProcessorState(null, null);
        try {
            final PageFrameCursor pageFrameCursor = ((QueryProgress) factory).getPageFrameCursorFrom(
                    virtualFactory.getBaseFactory(),
                    sqlExecutionContext,
                    PartitionFrameCursorFactory.ORDER_ASC
            );
            final UnregisterProbePageFrameCursor probe = new UnregisterProbePageFrameCursor(
                    pageFrameCursor,
                    sqlExecutionContext.getMemoryTracker()
            );
            final HybridColumnMaterializer materializer = state.getMaterializerForTest();
            final DirectLongList columnData = state.getMaterializerColumnDataForTest();
            final HTTPSerialParquetExporter exporter = state.getOrCreateSerialParquetExporterForTest(
                    engine,
                    (SqlExecutionContextImpl) sqlExecutionContext
            );
            if (releasePath == MaterializerReleasePath.STATE_CLEAR_STATE_OWNED) {
                state.setPageFrameCursorForTest(probe);
            } else {
                exporter.setupPageFrameBackedExport(probe, materializer, columnData);
            }

            materializer.setUpPageFrameBacked(virtualFactory, pageFrameCursor, sqlExecutionContext);
            PageFrame frame;
            while ((frame = pageFrameCursor.next()) != null) {
                materializer.buildColumnDataFromPageFrame(pageFrameCursor, frame, columnData);
            }
            Assert.assertTrue(
                    "the export must hold the cast-to-symbol dictionary in native memory",
                    Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS) > nativeBaseline
            );
            Assert.assertTrue("the dictionary must be charged to the query's tracker", probe.tracker.getUsed() > 0);

            if (releasePath == MaterializerReleasePath.EXPORTER_CLEAR) {
                exporter.clearExportResources();
            } else {
                state.clear();
            }
            Assert.assertTrue("the release must close the page-frame cursor", probe.isClosed);
            Assert.assertEquals(
                    "the dictionary must be released before the query unregisters and recycles its tracker",
                    0,
                    probe.trackerUsedAtClose
            );
            Assert.assertEquals(nativeBaseline, probe.nativeFuncMemAtClose);
            Assert.assertEquals(nativeBaseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
        } finally {
            state.close();
        }
    }

    private static void drain(ConcurrentQueue<QueryTrace> queue) {
        final QueryTrace trace = new QueryTrace();
        while (queue.tryDequeue(trace)) {
        }
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

    private static void setPrivateField(Object target, String fieldName, Object value) throws Exception {
        final Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    // Where an HTTP parquet export releases its materializer: the exporter clears its resources at
    // the end of process(), or the state clears with the page-frame cursor handed to the exporter
    // or still its own (the export failed before it started).
    private enum MaterializerReleasePath {
        EXPORTER_CLEAR,
        STATE_CLEAR_EXPORTER_OWNED,
        STATE_CLEAR_STATE_OWNED
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

    // Records the query's native function memory at the moment the page-frame cursor closes, which
    // is when the query unregisters and its memory tracker goes back to the pool.
    private static final class UnregisterProbePageFrameCursor implements PageFrameCursor {
        private final PageFrameCursor delegate;
        private final MemoryTracker tracker;
        private boolean isClosed;
        private long nativeFuncMemAtClose = -1;
        private long trackerUsedAtClose = -1;

        private UnregisterProbePageFrameCursor(PageFrameCursor delegate, MemoryTracker tracker) {
            this.delegate = delegate;
            this.tracker = tracker;
        }

        @Override
        public void calculateSize(RecordCursor.Counter counter) {
            delegate.calculateSize(counter);
        }

        @Override
        public void close() {
            if (!isClosed) {
                isClosed = true;
                trackerUsedAtClose = tracker.getUsed();
                nativeFuncMemAtClose = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            }
            delegate.close();
        }

        @Override
        public ColumnMapping getColumnMapping() {
            return delegate.getColumnMapping();
        }

        @Override
        public long getRemainingRowsInInterval() {
            return delegate.getRemainingRowsInInterval();
        }

        @Override
        public StaticSymbolTable getSymbolTable(int columnIndex) {
            return delegate.getSymbolTable(columnIndex);
        }

        @Override
        public boolean isExternal() {
            return delegate.isExternal();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return delegate.newSymbolTable(columnIndex);
        }

        @Override
        public @Nullable PageFrame next(long skipTarget) {
            return delegate.next(skipTarget);
        }

        @Override
        public long size() {
            return delegate.size();
        }

        @Override
        public boolean supportsSizeCalculation() {
            return delegate.supportsSizeCalculation();
        }

        @Override
        public void toTop() {
            delegate.toTop();
        }
    }
}
