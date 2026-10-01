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

package io.questdb.test.cutlass.parquet;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cutlass.parquet.CopyExportRequestTask;
import io.questdb.cutlass.parquet.ParquetExportMode;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.parquet.ParquetFileDecoder;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.std.DirectIntList;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Misc;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestMillisecondClock;
import io.questdb.test.tools.TestNetworkSqlExecutionCircuitBreaker;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

// A copy/export failing while the client has disconnected must classify as CANCELLED even when a
// recent probe has opened the connection-check throttle window; reverting classifyFailureStatus to
// the throttled checkIfTripped() reads the broken-connection case as FAILED.
public class CopyExportRequestTaskTest extends AbstractCairoTest {

    @Test
    public void testClassifyFailureStatusCancelledOnBrokenConnectionWithinThrottleWindow() throws Exception {
        assertMemoryLeak(() -> {
            TestMillisecondClock clock = new TestMillisecondClock(1_000);
            try (TestNetworkSqlExecutionCircuitBreaker breaker = newBreaker(clock)) {
                breaker.of(1);
                breaker.resetTimer();
                // A first probe against a live connection opens the throttle window.
                Assert.assertEquals(CopyExportRequestTask.Status.FAILED, CopyExportRequestTask.classifyFailureStatus(breaker));

                breaker.isConnectionBroken = true;
                // Inside the window the throttled check still reads the socket as alive, so the classifier
                // must bypass the throttle to attribute the failure to the disconnect.
                Assert.assertFalse(breaker.checkIfTripped());
                Assert.assertEquals(CopyExportRequestTask.Status.CANCELLED, CopyExportRequestTask.classifyFailureStatus(breaker));
            }
        });
    }

    @Test
    public void testClassifyFailureStatusCancelledOnCancellation() throws Exception {
        assertMemoryLeak(() -> {
            TestMillisecondClock clock = new TestMillisecondClock(1_000);
            try (TestNetworkSqlExecutionCircuitBreaker breaker = newBreaker(clock)) {
                breaker.of(1);
                breaker.resetTimer();
                breaker.setCancelledFlag(new AtomicBoolean(true));
                Assert.assertEquals(CopyExportRequestTask.Status.CANCELLED, CopyExportRequestTask.classifyFailureStatus(breaker));
            }
        });
    }

    @Test
    public void testClassifyFailureStatusCancelledOnTimeout() throws Exception {
        assertMemoryLeak(() -> {
            TestMillisecondClock clock = new TestMillisecondClock(1_000);
            try (TestNetworkSqlExecutionCircuitBreaker breaker = newBreaker(clock)) {
                breaker.of(1);
                breaker.resetTimer();
                Assert.assertEquals(CopyExportRequestTask.Status.FAILED, CopyExportRequestTask.classifyFailureStatus(breaker));

                clock.millis += 100_001;
                Assert.assertEquals(
                        "an export failing past query.timeout must classify as CANCELLED",
                        CopyExportRequestTask.Status.CANCELLED,
                        CopyExportRequestTask.classifyFailureStatus(breaker)
                );
            }
        });
    }

    @Test
    public void testClassifyFailureStatusFailedOnHealthyConnection() throws Exception {
        assertMemoryLeak(() -> {
            TestMillisecondClock clock = new TestMillisecondClock(1_000);
            try (TestNetworkSqlExecutionCircuitBreaker breaker = newBreaker(clock)) {
                breaker.of(1);
                breaker.resetTimer();
                Assert.assertEquals(CopyExportRequestTask.Status.FAILED, CopyExportRequestTask.classifyFailureStatus(breaker));
            }
        });
    }

    @Test
    public void testClearReleasesDecodeBuffersBeforeThrowingFactory() throws Exception {
        assertReleasesDecodeBuffersBeforeThrowingFactory(false);
    }

    @Test
    public void testCloseReleasesDecodeBuffersBeforeThrowingFactories() throws Exception {
        assertReleasesDecodeBuffersBeforeThrowingFactory(true);
    }

    private void assertReleasesDecodeBuffersBeforeThrowingFactory(boolean isClose) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select x id, timestamp_sequence(400000000000, 500) ts from long_sequence(10)) timestamp(ts) partition by day");

            final FilesFacade ff = configuration.getFilesFacade();
            final CopyExportContext copyExportContext = engine.getCopyExportContext();
            final CopyExportRequestTask task = new CopyExportRequestTask();
            CopyExportContext.ExportTaskEntry entry = null;
            long fd = -1;
            long addr = 0;
            long fileSize = 0;
            MemoryTracker tracker = null;
            Throwable primaryFailure = null;
            try {
                try (
                        Path path = new Path();
                        PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                        ParquetFileDecoder parquetFileDecoder = new ParquetFileDecoder();
                        DirectIntList columns = new DirectIntList(2, MemoryTag.NATIVE_PARQUET_EXPORTER)
                ) {
                    path.of(root).concat("x.parquet").$();
                    try (TableReader reader = engine.getReader("x")) {
                        PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                        PartitionEncoder.encode(partitionDescriptor, path);
                    }

                    fd = TableUtils.openRO(ff, path.$(), LOG);
                    fileSize = ff.length(fd);
                    addr = TableUtils.mapRO(ff, fd, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
                    parquetFileDecoder.of(addr, fileSize, MemoryTag.NATIVE_PARQUET_EXPORTER);
                    columns.add(0);
                    columns.add(ColumnType.LONG);

                    tracker = engine.getMemoryTrackerProvider().acquire(
                            sqlExecutionContext.getSecurityContext(),
                            42,
                            MemoryTrackerWorkload.QUERY
                    );
                    task.setMemoryTracker(tracker);
                    final RowGroupBuffers decodeBuffers = task.getStreamPartitionParquetExporter().getDecodeRowGroupBuffers();
                    decodeBuffers.reopen();
                    parquetFileDecoder.decodeRowGroup(decodeBuffers, columns, 0, 0, 10);
                    Assert.assertTrue("real Rust decode must charge the export tracker", tracker.getUsed() > 0);

                    final RuntimeException closeFailure = new RuntimeException("injected select factory close failure");
                    final ThrowingFactory selectFactory = new ThrowingFactory(closeFailure);
                    task.setSelectFactory(selectFactory);
                    final RuntimeException tempCloseFailure = new RuntimeException("injected temp factory close failure");
                    final GenericRecordMetadata tempTableMetadata = new GenericRecordMetadata();
                    tempTableMetadata.add(new TableColumnMetadata("id", ColumnType.LONG));
                    final ThrowingFactory tempTableFactory = new ThrowingFactory(tempCloseFailure, tempTableMetadata);
                    final CloseCountingPageFrameCursor pageFrameCursor = new CloseCountingPageFrameCursor();
                    if (isClose) {
                        entry = copyExportContext.assignExportEntry(
                                sqlExecutionContext.getSecurityContext(),
                                "COPY x TO STDOUT",
                                "",
                                null,
                                CopyExportContext.CopyTrigger.HTTP
                        );
                        task.of(
                                entry,
                                null,
                                "x",
                                "",
                                configuration.getParquetExportCompressionCodec(),
                                configuration.getParquetExportCompressionLevel(),
                                configuration.getParquetExportRowGroupSize(),
                                configuration.getParquetExportDataPageSize(),
                                configuration.isParquetExportStatisticsEnabled(),
                                configuration.getParquetExportVersion(),
                                configuration.isParquetExportRawArrayEncoding(),
                                sqlExecutionContext.getNowTimestampType(),
                                sqlExecutionContext.getNow(sqlExecutionContext.getNowTimestampType()),
                                false,
                                null,
                                tempTableFactory.getMetadata(),
                                null,
                                ParquetExportMode.TEMP_TABLE,
                                null,
                                null,
                                -1,
                                configuration.getParquetExportBloomFilterFpp(),
                                null
                        );
                        task.setUpStreamPartitionParquetExporter(
                                tempTableFactory,
                                pageFrameCursor,
                                tempTableFactory.getMetadata(),
                                false
                        );
                    }

                    final RuntimeException thrown = Assert.assertThrows(
                            RuntimeException.class,
                            isClose ? task::close : task::clear
                    );
                    Assert.assertSame("cleanup must preserve the first failure", closeFailure, thrown);
                    Assert.assertEquals("decode buffers must credit the tracker before it is recycled", 0, tracker.getUsed());
                    Assert.assertNull("task must not retain a tracker after cleanup", task.getMemoryTracker());
                    Assert.assertNull("throwing factory must be detached before close", task.getSelectFactory());
                    Assert.assertEquals(1, selectFactory.closeCount);
                    if (isClose) {
                        Assert.assertEquals(1, tempTableFactory.closeCount);
                        Assert.assertEquals("task must close the owned cursor after earlier failures", 1, pageFrameCursor.closeCount);
                        Assert.assertNull("task must detach the owned cursor before close", task.getPageFrameCursor());
                        Assert.assertEquals(1, thrown.getSuppressed().length);
                        Assert.assertSame(tempCloseFailure, thrown.getSuppressed()[0]);
                        task.close();
                        Assert.assertEquals("a second close must not retry the owned cursor", 1, pageFrameCursor.closeCount);
                    } else {
                        task.clear();
                    }
                    Assert.assertEquals("a second cleanup must not retry the failed factory", 1, selectFactory.closeCount);

                    tracker.close();
                    tracker = null;
                    try (MemoryTracker recycled = engine.getMemoryTrackerProvider().acquire(
                            sqlExecutionContext.getSecurityContext(),
                            43,
                            MemoryTrackerWorkload.QUERY
                    )) {
                        Assert.assertEquals("recycled tracker must start clean", 0, recycled.getUsed());
                    }
                }
            } catch (Exception | Error e) {
                primaryFailure = e;
                throw e;
            } finally {
                Throwable cleanupFailure = null;
                task.setSelectFactory(null);
                cleanupFailure = Misc.freeBestEffort(cleanupFailure, task);
                cleanupFailure = Misc.freeBestEffort(cleanupFailure, tracker);
                try {
                    ff.close(fd);
                } catch (Throwable th) {
                    cleanupFailure = Misc.foldCleanupFailure(cleanupFailure, th);
                }
                try {
                    ff.munmap(addr, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
                } catch (Throwable th) {
                    cleanupFailure = Misc.foldCleanupFailure(cleanupFailure, th);
                }
                if (entry != null) {
                    try {
                        copyExportContext.releaseEntry(entry);
                    } catch (Throwable th) {
                        cleanupFailure = Misc.foldCleanupFailure(cleanupFailure, th);
                    }
                }
                if (cleanupFailure != null) {
                    if (primaryFailure != null) {
                        Misc.foldCleanupFailure(primaryFailure, cleanupFailure);
                    } else {
                        CairoException.rethrowCleanupFailure(cleanupFailure);
                    }
                }
            }
        });
    }

    private static TestNetworkSqlExecutionCircuitBreaker newBreaker(MillisecondClock clock) {
        return TestNetworkSqlExecutionCircuitBreaker.create(engine, clock, 100, 100_000);
    }

    private static final class CloseCountingPageFrameCursor implements PageFrameCursor {
        private int closeCount;

        @Override
        public void calculateSize(RecordCursor.Counter counter) {
        }

        @Override
        public void close() {
            closeCount++;
        }

        @Override
        public ColumnMapping getColumnMapping() {
            return null;
        }

        @Override
        public long getRemainingRowsInInterval() {
            return 0;
        }

        @Override
        public StaticSymbolTable getSymbolTable(int columnIndex) {
            return null;
        }

        @Override
        public boolean isExternal() {
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return null;
        }

        @Override
        public PageFrame next(long skipTarget) {
            return null;
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public boolean supportsSizeCalculation() {
            return true;
        }

        @Override
        public void toTop() {
        }
    }

    private static final class ThrowingFactory extends AbstractRecordCursorFactory {
        private int closeCount;
        private final RuntimeException closeFailure;

        private ThrowingFactory(RuntimeException closeFailure) {
            this(closeFailure, new GenericRecordMetadata());
        }

        private ThrowingFactory(RuntimeException closeFailure, GenericRecordMetadata metadata) {
            super(metadata);
            this.closeFailure = closeFailure;
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
            throw closeFailure;
        }
    }
}
