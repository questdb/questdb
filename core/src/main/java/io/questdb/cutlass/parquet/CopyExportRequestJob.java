/*******************************************************************************
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
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.AbstractQueueConsumerJob;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.MicrosecondClock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.concurrent.Callable;

public class CopyExportRequestJob extends AbstractQueueConsumerJob<CopyExportRequestTask> implements Closeable {
    private static final Log LOG = LogFactory.getLog(CopyExportRequestJob.class);
    private final CopyExportContext copyContext;
    private final @NotNull MicrosecondClock microsecondClock;
    @TestOnly
    private @Nullable Callable<Exception> callback;
    private CopyExportRequestTask localTaskCopy;
    private SQLSerialParquetExporter serialExporter;

    public CopyExportRequestJob(final CairoEngine engine) {
        super(engine.getMessageBus().getCopyExportRequestQueue(), engine.getMessageBus().getCopyExportRequestSubSeq());
        microsecondClock = engine.getConfiguration().getMicrosecondClock();
        localTaskCopy = new CopyExportRequestTask();
        try {
            serialExporter = new SQLSerialParquetExporter(engine);
            copyContext = engine.getCopyExportContext();
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    @TestOnly
    public CopyExportRequestJob(final CairoEngine engine, @Nullable Callable<Exception> callback) {
        this(engine);
        this.callback = callback;
    }

    @Override
    public void close() {
        this.serialExporter = Misc.free(serialExporter);
        this.localTaskCopy = Misc.free(localTaskCopy);
    }

    @Override
    protected boolean doRun(int workerId, long cursor, RunStatus runStatus) {
        try {
            CopyExportRequestTask task = queue.get(cursor);
            localTaskCopy.of(
                    task.getEntry(),
                    task.getCreateOp(),
                    task.getTableName(),
                    // we are copying CharSequence from the queue, and releasing it
                    Chars.toString(task.getFileName()),
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
                    task.getBloomFilterColumns(),
                    task.getBloomFilterFpp()
            );
            task.clear();
        } finally {
            subSeq.done(cursor);
        }

        if (this.callback != null) {
            try {
                this.callback.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        CopyExportContext.ExportTaskEntry entry = localTaskCopy.getEntry();
        try {
            entry.setStartTime(microsecondClock.getTicks(), workerId);
            SqlExecutionCircuitBreaker circuitBreaker = localTaskCopy.getCircuitBreaker();
            CopyExportRequestTask.Phase phase = CopyExportRequestTask.Phase.WAITING;

            try {
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
                serialExporter.of(localTaskCopy);
                phase = serialExporter.process(); // throws CopyExportException

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
                        circuitBreaker.checkIfTripped() ? CopyExportRequestTask.Status.CANCELLED : CopyExportRequestTask.Status.FAILED,
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
                        circuitBreaker.checkIfTripped() ? CopyExportRequestTask.Status.CANCELLED : CopyExportRequestTask.Status.FAILED,
                        null,
                        Numbers.INT_NULL,
                        e.getMessage(),
                        -1,
                        localTaskCopy.getTableName(),
                        localTaskCopy.getCopyID()
                );
            } finally {
                localTaskCopy.clear();
            }
        } finally {
            copyContext.releaseEntry(entry);
        }
        return true;
    }

}
