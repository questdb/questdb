/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.CreateTableOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.RecordCursorFactory.SCAN_DIRECTION_BACKWARD;

public class HTTPSerialParquetExporter {
    private static final Log LOG = LogFactory.getLog(HTTPSerialParquetExporter.class);
    protected final CopyExportContext copyExportContext;
    protected final ExportProgressReporter insertSelectReporter = new ExportProgressReporter();
    protected final SqlExecutionContextImpl sqlExecutionContext;
    protected SqlExecutionCircuitBreaker circuitBreaker;
    protected CopyExportRequestTask task;

    public HTTPSerialParquetExporter(CairoEngine engine) {
        this.sqlExecutionContext = new SqlExecutionContextImpl(engine, 1);
        this.copyExportContext = engine.getCopyExportContext();
    }

    public void of(CopyExportRequestTask task) {
        this.task = task;
        this.circuitBreaker = task.getCircuitBreaker();
        sqlExecutionContext.with(task.getSecurityContext(), null, null, -1, circuitBreaker);
    }

    public CopyExportRequestTask.Phase process() throws Exception {
        TableToken tableToken = null;
        CopyExportContext.ExportTaskEntry entry = task.getEntry();
        final CairoEngine cairoEngine = sqlExecutionContext.getCairoEngine();
        RecordCursorFactory factory = null;
        CopyExportRequestTask.Phase phase = CopyExportRequestTask.Phase.NONE;
        CreateTableOperation createOp = null;
        sqlExecutionContext.setNowAndFixClock(task.getNow(), task.getNowTimestampType());

        try {
            createOp = task.getCreateOp();
            if (createOp != null && task.getPageFrameCursor() == null) {
                insertSelectReporter.of(circuitBreaker, entry, task.getCopyID(), task.getTableName());
                createOp.setCopyDataProgressReporter(insertSelectReporter);
                phase = CopyExportRequestTask.Phase.POPULATING_TEMP_TABLE;
                entry.setPhase(phase);
                copyExportContext.updateStatus(phase, CopyExportRequestTask.Status.STARTED, null, Numbers.INT_NULL, null, 0, task.getTableName(), task.getCopyID());
                LOG.info().$("starting to create temporary table and populate with data [id=").$hexPadded(task.getCopyID()).$(", table=").$(task.getTableName()).$(']').$();
                createOp.execute(sqlExecutionContext, null);
                tableToken = cairoEngine.verifyTableName(task.getTableName());
                LOG.info().$("completed creating temporary table and populating with data [id=").$hexPadded(task.getCopyID()).$(", table=").$(tableToken).$(']').$();
                copyExportContext.updateStatus(phase, CopyExportRequestTask.Status.FINISHED, null, Numbers.INT_NULL, null, 0, task.getTableName(), task.getCopyID());

                try (SqlCompiler compiler = cairoEngine.getSqlCompiler()) {
                    int timestampIndex = createOp.getTimestampIndex();
                    boolean descending = timestampIndex > -1 && createOp.getSelectSqlScanDirection() == SCAN_DIRECTION_BACKWARD;
                    CharSequence sql = task.getTableName();
                    if (descending) {
                        StringSink sink = Misc.getThreadLocalSink();
                        sink.put(sql).put(" order by ").put(createOp.getColumnName(timestampIndex)).put(" desc");
                        sql = sink;
                    }
                    // in security context that doesn't allow database modifications we would allow
                    // temp table creation and selection in this particular setting.
                    SecurityContext sec = sqlExecutionContext.getSecurityContext();
                    PageFrameCursor pageFrameCursor;
                    try {
                        sqlExecutionContext.with(AllowAllSecurityContext.INSTANCE);
                        CompiledQuery cc = compiler.compile(sql, sqlExecutionContext);
                        factory = cc.getRecordCursorFactory();
                        assert factory.supportsPageFrameCursor(); // simple temp table must support page frame cursor
                        pageFrameCursor = factory.getPageFrameCursor(sqlExecutionContext, ORDER_ASC);
                    } finally {
                        sqlExecutionContext.with(sec);
                    }
                    task.setUpStreamPartitionParquetExporter(factory, pageFrameCursor, factory.getMetadata(), descending);
                    factory = null; // transfer ownership to the task
                }
            }

            // start streaming export
            phase = CopyExportRequestTask.Phase.STREAM_SENDING_DATA;
            entry.setPhase(phase);
            processStreamExport();
        } catch (PeerIsSlowToReadException e) {
            createOp = null;
            throw e;
        } catch (SqlException e) {
            LOG.error().$("HTTP parquet export failed [id=").$hexPadded(task.getCopyID()).$(", msg=").$(e.getFlyweightMessage()).$(']').$();
            Misc.free(factory);
            copyExportContext.updateStatus(
                    phase,
                    circuitBreaker.checkIfTripped() ? CopyExportRequestTask.Status.CANCELLED : CopyExportRequestTask.Status.FAILED,
                    null,
                    Numbers.INT_NULL,
                    e.getFlyweightMessage(),
                    e.getErrorCode(),
                    task.getTableName(),
                    task.getCopyID()
            );
            throw e;
        } catch (CairoException e) {
            LOG.error().$("HTTP parquet export failed [id=").$hexPadded(task.getCopyID()).$(", msg=").$(e.getFlyweightMessage()).$(']').$();
            Misc.free(factory);
            copyExportContext.updateStatus(
                    phase,
                    circuitBreaker.checkIfTripped() ? CopyExportRequestTask.Status.CANCELLED : CopyExportRequestTask.Status.FAILED,
                    null,
                    Numbers.INT_NULL,
                    e.getFlyweightMessage(),
                    e.getErrno(),
                    task.getTableName(),
                    task.getCopyID()
            );
            throw e;
        } catch (Throwable e) {
            LOG.error().$("HTTP parquet export failed [id=").$hexPadded(task.getCopyID()).$(", msg=").$(e).$(']').$();
            Misc.free(factory);
            copyExportContext.updateStatus(
                    phase,
                    circuitBreaker.checkIfTripped() ? CopyExportRequestTask.Status.CANCELLED : CopyExportRequestTask.Status.FAILED,
                    null,
                    Numbers.INT_NULL,
                    e.getMessage(),
                    -1,
                    task.getTableName(),
                    task.getCopyID()
            );
            throw e;
        } finally {
            if (createOp != null) {
                phase = CopyExportRequestTask.Phase.DROPPING_TEMP_TABLE;
                entry.setPhase(phase);
                copyExportContext.updateStatus(phase, CopyExportRequestTask.Status.STARTED, null, Numbers.INT_NULL, null, 0, task.getTableName(), task.getCopyID());
                task.getStreamPartitionParquetExporter().freeOwnedPageFrameCursor();
                try {
                    if (tableToken == null) {
                        tableToken = cairoEngine.getTableTokenIfExists(task.getTableName());
                    }
                    if (tableToken != null) {
                        cairoEngine.dropTableOrViewOrMatView(Path.getThreadLocal(""), tableToken);
                    }
                    copyExportContext.updateStatus(phase, CopyExportRequestTask.Status.FINISHED, null, Numbers.INT_NULL, null, 0, task.getTableName(), task.getCopyID());
                } catch (CairoException e) {
                    // drop failure doesn't affect task continuation - log and proceed
                    LOG.error().$("fail to drop temporary table [id=").$hexPadded(task.getCopyID()).$(", table=").$(tableToken).$(", msg=").$(e.getFlyweightMessage()).$(']').$();
                    copyExportContext.updateStatus(phase, CopyExportRequestTask.Status.FAILED, null, Numbers.INT_NULL, null, 0, task.getTableName(), task.getCopyID());
                } catch (Throwable e) {
                    LOG.error().$("fail to drop temporary table [id=").$hexPadded(task.getCopyID()).$(", table=").$(tableToken).$(", msg=").$(e.getMessage()).$(']').$();
                    copyExportContext.updateStatus(phase, CopyExportRequestTask.Status.FAILED, null, Numbers.INT_NULL, null, 0, task.getTableName(), task.getCopyID());
                }
            }
        }
        phase = CopyExportRequestTask.Phase.SUCCESS;
        copyExportContext.updateStatus(
                phase,
                CopyExportRequestTask.Status.FINISHED,
                null,
                Numbers.INT_NULL,
                null,
                0,
                task.getTableName(),
                task.getCopyID()
        );
        entry.setPhase(CopyExportRequestTask.Phase.SUCCESS);
        return phase;
    }

    private void processStreamExport() throws Exception {
        PageFrameCursor pageFrameCursor = task.getPageFrameCursor();
        assert pageFrameCursor != null;
        CopyExportRequestTask.StreamPartitionParquetExporter exporter = task.getStreamPartitionParquetExporter();
        if (circuitBreaker.checkIfTripped()) {
            LOG.error().$("copy was cancelled [id=").$hexPadded(task.getCopyID()).$(']').$();
            throw CopyExportException.instance(CopyExportRequestTask.Phase.STREAM_SENDING_DATA, -1).put("cancelled by user").setInterruption(true).setCancellation(true);
        }
        if (exporter.onResume()) {
            LOG.info().$("stream export progress (resume) [id=").$hexPadded(task.getCopyID())
                    .$(", rowsInFrame=").$(exporter.getCurrentFrameRowCount())
                    .$(", exported totalRows=").$(exporter.getTotalRows())
                    .$(", partitionIndex=").$(exporter.getCurrentPartitionIndex())
                    .$(']').$();
        } else {
            copyExportContext.updateStatus(CopyExportRequestTask.Phase.STREAM_SENDING_DATA, CopyExportRequestTask.Status.STARTED, null, Numbers.INT_NULL, null, 0, task.getTableName(), task.getCopyID());
        }

        PageFrame frame;
        // Initialize with current value to avoid spurious release after resume from PeerIsSlowToReadException
        long previousRowsWritten = exporter.getRowsWrittenToRowGroups();
        while ((frame = pageFrameCursor.next()) != null) {
            if (circuitBreaker.checkIfTripped()) {
                LOG.error().$("copy was cancelled [id=").$hexPadded(task.getCopyID()).$(']').$();
                throw CopyExportException.instance(CopyExportRequestTask.Phase.STREAM_SENDING_DATA, -1).put("cancelled by user").setInterruption(true).setCancellation(true);
            }
            long rowsInFrame = frame.getPartitionHi() - frame.getPartitionLo();
            int partitionIndex = frame.getPartitionIndex();

            exporter.setCurrentPartitionIndex(partitionIndex, rowsInFrame);
            exporter.writePageFrame(pageFrameCursor, frame);

            // Release partitions only after Rust has written a row group.
            // This ensures partition column data is not released while Rust
            // still holds references in pending_partitions.
            long currentRowsWritten = exporter.getRowsWrittenToRowGroups();
            if (currentRowsWritten > previousRowsWritten) {
                pageFrameCursor.releaseOpenPartitions();
                previousRowsWritten = currentRowsWritten;
            }

            LOG.info().$("stream export progress [id=").$hexPadded(task.getCopyID())
                    .$(", rowsInFrame=").$(rowsInFrame)
                    .$(", exported totalRows=").$(exporter.getTotalRows())
                    .$(", partitionIndex=").$(partitionIndex)
                    .$(']').$();
        }

        long totalRows = exporter.getTotalRows();
        exporter.finishExport();
        copyExportContext.updateStatus(CopyExportRequestTask.Phase.STREAM_SENDING_DATA, CopyExportRequestTask.Status.FINISHED, null, Numbers.INT_NULL, null, 0, task.getTableName(), task.getCopyID());
        LOG.info().$("stream export completed [id=").$hexPadded(task.getCopyID())
                .$(", totalRows=").$(totalRows)
                .$(']').$();
    }
}
