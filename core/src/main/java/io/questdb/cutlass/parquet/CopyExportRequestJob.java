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

package io.questdb.cutlass.parquet;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.griffin.engine.ops.CreateTableOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

public class CopyExportRequestJob extends AbstractQueueConsumerJob<CopyExportRequestTask> implements Closeable {
    private static final Log LOG = LogFactory.getLog(CopyExportRequestJob.class);
    @TestOnly
    private final @Nullable Callable<Exception> callback;
    private final CopyExportContext copyContext;
    private final CairoEngine engine;
    private final StringSink fileName = new StringSink();
    private final @NotNull MicrosecondClock microsecondClock;
    private boolean isClosed;
    private boolean isRequestLoaded;
    private CopyExportRequestTask localTaskCopy;
    private SQLSerialParquetExporter serialExporter;

    public CopyExportRequestJob(final CairoEngine engine) {
        this(engine, null);
    }

    @TestOnly
    public CopyExportRequestJob(final CairoEngine engine, @Nullable Callable<Exception> callback) {
        this(engine, callback, null);
    }

    @TestOnly
    public CopyExportRequestJob(
            final CairoEngine engine,
            @Nullable Callable<Exception> callback,
            @Nullable Supplier<SQLSerialParquetExporter> exporterFactory
    ) {
        super(engine.getMessageBus().getCopyExportRequestQueue(), engine.getMessageBus().getCopyExportRequestSubSeq());
        this.callback = callback;
        this.copyContext = engine.getCopyExportContext();
        this.engine = engine;
        microsecondClock = engine.getConfiguration().getMicrosecondClock();
        localTaskCopy = new CopyExportRequestTask();
        try {
            serialExporter = exporterFactory != null
                    ? exporterFactory.get()
                    : new SQLSerialParquetExporter(engine);
        } catch (Throwable t) {
            final Throwable failure = Misc.freeBestEffort(t, localTaskCopy);
            localTaskCopy = null;
            CairoException.rethrowCleanupFailure(failure);
        }
    }

    @Override
    public io.questdb.mp.Job cloneInstance() {
        return new CopyExportRequestJob(engine);
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        isClosed = true;
        Throwable cleanupFailure = null;
        if (isRequestLoaded) {
            try {
                cancelLoadedRequest("copy export job closed");
            } catch (Throwable th) {
                cleanupFailure = th;
            }
        }
        while (true) {
            try {
                if (!cancelQueuedRequest("copy export job closed")) {
                    break;
                }
            } catch (Throwable th) {
                cleanupFailure = addCleanupFailure(cleanupFailure, th);
            }
        }
        final SQLSerialParquetExporter exporter = serialExporter;
        serialExporter = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, exporter);
        final CopyExportRequestTask task = localTaskCopy;
        localTaskCopy = null;
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, task);
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    @Override
    public void closeInstance() {
        try {
            close();
        } catch (Throwable th) {
            LOG.error().$("could not close copy export job [error=").$(th).I$();
        }
    }

    @Override
    protected boolean doRun(long cursor, WorkerContext workerContext) {
        try {
            final CopyExportRequestTask task = queue.get(cursor);
            transferRequest(task);
        } finally {
            subSeq.done(cursor);
        }

        processRequest(workerContext.carrierId());
        return true;
    }

    private static Throwable addCleanupFailure(@Nullable Throwable primary, Throwable failure) {
        if (primary == null) {
            return failure;
        }
        if (primary != failure) {
            primary.addSuppressed(failure);
        }
        return primary;
    }

    private void cancelLoadedRequest(CharSequence message) {
        if (!isRequestLoaded) {
            return;
        }
        try {
            copyContext.updateStatus(
                    CopyExportRequestTask.Phase.WAITING,
                    CopyExportRequestTask.Status.CANCELLED,
                    null,
                    Numbers.INT_NULL,
                    message,
                    -1,
                    localTaskCopy.getTableName(),
                    localTaskCopy.getCopyID()
            );
        } finally {
            releaseRequest();
        }
    }

    private void failLoadedRequest(CharSequence message) {
        if (!isRequestLoaded) {
            return;
        }
        try {
            copyContext.updateStatus(
                    CopyExportRequestTask.Phase.WAITING,
                    failureStatus(localTaskCopy.getCircuitBreaker()),
                    null,
                    Numbers.INT_NULL,
                    message,
                    -1,
                    localTaskCopy.getTableName(),
                    localTaskCopy.getCopyID()
            );
        } finally {
            releaseRequest();
        }
    }

    private CopyExportRequestTask.Status failureStatus(SqlExecutionCircuitBreaker circuitBreaker) {
        return CopyExportRequestTask.classifyFailureStatus(circuitBreaker);
    }

    private void processRequest(int carrierId) {
        final CopyExportContext.ExportTaskEntry entry = localTaskCopy.getEntry();
        final SqlExecutionCircuitBreaker circuitBreaker = localTaskCopy.getCircuitBreaker();
        CopyExportRequestTask.Phase phase = CopyExportRequestTask.Phase.WAITING;
        try {
            if (callback != null) {
                callback.call();
            }
            entry.setStartTime(microsecondClock.getTicks(), carrierId);
            if (circuitBreaker.checkIfTripped()) {
                LOG.errorW().$("copy was cancelled [copyId=").$hexPadded(localTaskCopy.getCopyID()).$(']').$();
                throw CopyExportException.instance(phase, -1).put("cancelled by user").setInterruption(true).setCancellation(true);
            }
            copyContext.updateStatus(
                    CopyExportRequestTask.Phase.WAITING,
                    CopyExportRequestTask.Status.FINISHED,
                    null,
                    Numbers.INT_NULL,
                    "",
                    0,
                    localTaskCopy.getTableName(),
                    localTaskCopy.getCopyID());
            final MemoryTracker memoryTracker = engine.getMemoryTrackerProvider().acquire(
                    localTaskCopy.getSecurityContext(),
                    localTaskCopy.getCopyID(),
                    MemoryTrackerWorkload.QUERY
            );
            localTaskCopy.setMemoryTracker(memoryTracker);
            serialExporter.of(localTaskCopy);
            phase = serialExporter.process();

            entry.setPhase(CopyExportRequestTask.Phase.SUCCESS);
            copyContext.updateStatus(
                    CopyExportRequestTask.Phase.SUCCESS,
                    CopyExportRequestTask.Status.FINISHED,
                    serialExporter.getExportPath(),
                    serialExporter.getNumOfFiles(),
                    null,
                    0,
                    localTaskCopy.getTableName(),
                    localTaskCopy.getCopyID()
            );
        } catch (CopyExportException e) {
            copyContext.updateStatus(
                    e.getPhase(),
                    failureStatus(circuitBreaker),
                    null,
                    Numbers.INT_NULL,
                    e.getFlyweightMessage(),
                    e.getErrno(),
                    localTaskCopy.getTableName(),
                    localTaskCopy.getCopyID()
            );
        } catch (Throwable e) {
            copyContext.updateStatus(
                    phase,
                    failureStatus(circuitBreaker),
                    null,
                    Numbers.INT_NULL,
                    e.getMessage(),
                    -1,
                    localTaskCopy.getTableName(),
                    localTaskCopy.getCopyID()
            );
        } finally {
            releaseRequest();
        }
    }

    private boolean cancelQueuedRequest(CharSequence message) {
        while (true) {
            final long cursor = subSeq.next();
            if (cursor == -1) {
                return false;
            }
            if (cursor > -1) {
                try {
                    transferRequest(queue.get(cursor));
                } finally {
                    subSeq.done(cursor);
                }
                cancelLoadedRequest(message);
                return true;
            }
            Os.pause();
        }
    }

    private void releaseRequest() {
        if (!isRequestLoaded) {
            return;
        }
        final CopyExportContext.ExportTaskEntry entry = localTaskCopy.getEntry();
        final MemoryTracker memoryTracker = localTaskCopy.getMemoryTracker();
        Throwable cleanupFailure = null;
        try {
            localTaskCopy.clear();
        } catch (Throwable th) {
            cleanupFailure = th;
        }
        final SQLSerialParquetExporter exporter = serialExporter;
        if (exporter != null) {
            try {
                exporter.clearMemoryTracker();
            } catch (Throwable th) {
                cleanupFailure = addCleanupFailure(cleanupFailure, th);
            }
        }
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, memoryTracker);
        try {
            copyContext.releaseEntry(entry);
        } catch (Throwable th) {
            cleanupFailure = addCleanupFailure(cleanupFailure, th);
        } finally {
            isRequestLoaded = false;
        }
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    private void transferRequest(CopyExportRequestTask task) {
        final CopyExportContext.ExportTaskEntry entry = task.getEntry();
        final long copyID = task.getCopyID();
        final String tableName = task.getTableName();
        CreateTableOperation createOp = null;
        RecordCursorFactory selectFactory = null;
        Throwable transferFailure = null;
        try {
            final CharSequence sourceFileName = task.getFileName();
            fileName.clear();
            if (sourceFileName != null) {
                fileName.put(sourceFileName);
            }
            selectFactory = task.getSelectFactory();
            task.setSelectFactory(null);
            createOp = task.getCreateOp();
            task.setCreateOp(null);
            localTaskCopy.of(
                    entry,
                    createOp,
                    tableName,
                    sourceFileName != null ? fileName : null,
                    task.getCompressionCodec(),
                    task.getCompressionLevel(),
                    task.getRowGroupSize(),
                    task.getDataPageSize(),
                    task.isStatisticsEnabled(),
                    task.getParquetVersion(),
                    task.isRawArrayEncoding(),
                    task.getNowTimestampType(),
                    task.getNow(),
                    task.isDescending(),
                    task.getPageFrameCursor(),
                    task.getMetadata(),
                    task.getWriteCallback(),
                    task.getExportMode(),
                    task.getSelectText(),
                    task.getBloomFilterColumns(),
                    task.getBloomFilterColumnsPosition(),
                    task.getBloomFilterFpp(),
                    task.getBindVariableService()
            );
            localTaskCopy.setSelectFactory(selectFactory);
            isRequestLoaded = true;
            createOp = null;
            selectFactory = null;
        } catch (Throwable th) {
            transferFailure = th;
        }
        try {
            task.clear();
        } catch (Throwable th) {
            transferFailure = addCleanupFailure(transferFailure, th);
        }
        if (transferFailure != null) {
            final CharSequence message = transferFailure.getMessage();
            if (isRequestLoaded) {
                try {
                    failLoadedRequest(message);
                } catch (Throwable cleanupFailure) {
                    transferFailure = addCleanupFailure(transferFailure, cleanupFailure);
                }
            } else {
                transferFailure = Misc.freeBestEffort(transferFailure, selectFactory);
                transferFailure = Misc.freeBestEffort(transferFailure, createOp);
                try {
                    copyContext.updateStatus(
                            CopyExportRequestTask.Phase.WAITING,
                            CopyExportRequestTask.Status.FAILED,
                            null,
                            Numbers.INT_NULL,
                            message,
                            -1,
                            tableName,
                            copyID
                    );
                } catch (Throwable statusFailure) {
                    transferFailure = addCleanupFailure(transferFailure, statusFailure);
                }
                try {
                    copyContext.releaseEntry(entry);
                } catch (Throwable cleanupFailure) {
                    transferFailure = addCleanupFailure(transferFailure, cleanupFailure);
                }
            }
            CairoException.rethrowCleanupFailure(transferFailure);
        }
    }
}
