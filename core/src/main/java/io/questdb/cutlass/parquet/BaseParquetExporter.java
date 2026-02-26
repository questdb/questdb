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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectLongList;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;

public abstract class BaseParquetExporter {
    private static final Log LOG = LogFactory.getLog(BaseParquetExporter.class);
    protected final CopyExportContext copyExportContext;
    protected final ExportProgressReporter insertSelectReporter = new ExportProgressReporter();
    protected final SqlExecutionContextImpl sqlExecutionContext;
    protected SqlExecutionCircuitBreaker circuitBreaker;
    protected CopyExportRequestTask task;

    protected BaseParquetExporter(CairoEngine engine) {
        this.sqlExecutionContext = new SqlExecutionContextImpl(engine, 1);
        this.copyExportContext = engine.getCopyExportContext();
    }

    public void of(CopyExportRequestTask task) {
        this.task = task;
        this.circuitBreaker = task.getCircuitBreaker();
        sqlExecutionContext.with(task.getSecurityContext(), null, null, -1, circuitBreaker);
    }

    protected void drainHybridFrames(
            CopyExportRequestTask.StreamPartitionParquetExporter exporter,
            HybridColumnMaterializer mat,
            DirectLongList columnData,
            PageFrameCursor pfc,
            RecordCursor cursor,
            long batchSize,
            CopyExportRequestTask.Phase phase
    ) throws Exception {
        long previousRowsWritten = exporter.getRowsWrittenToRowGroups();
        long inFlightRows = 0;
        for (; ; ) {
            long rowCount;
            if (pfc != null) {
                var frame = pfc.next();
                if (frame == null) break;
                rowCount = mat.buildColumnDataFromPageFrame(pfc, frame, columnData);
            } else {
                rowCount = mat.buildColumnDataFromCursor(cursor, columnData, batchSize);
                if (rowCount == 0) break;
            }

            if (circuitBreaker.checkIfTripped()) {
                throw CopyExportException.instance(phase, -1).put("cancelled by user").setInterruption(true).setCancellation(true);
            }
            exporter.writeHybridFrame(columnData, rowCount);
            inFlightRows += rowCount;

            long currentRowsWritten = exporter.getRowsWrittenToRowGroups();
            if (currentRowsWritten > previousRowsWritten) {
                inFlightRows -= (currentRowsWritten - previousRowsWritten);
                if (inFlightRows <= rowCount) {
                    mat.releasePinnedBuffers();
                }
                if (pfc != null) {
                    pfc.releaseOpenPartitions();
                }
                previousRowsWritten = currentRowsWritten;
            }
        }
        exporter.finishExport();
    }

    protected void dropTempTable(
            CopyExportContext.ExportTaskEntry entry,
            TableToken tableToken
    ) {
        CopyExportRequestTask.Phase phase = CopyExportRequestTask.Phase.DROPPING_TEMP_TABLE;
        entry.setPhase(phase);
        copyExportContext.updateStatus(phase, CopyExportRequestTask.Status.STARTED, null, Numbers.INT_NULL, null, 0, task.getTableName(), task.getCopyID());
        CairoEngine cairoEngine = sqlExecutionContext.getCairoEngine();
        try {
            if (tableToken == null) {
                tableToken = cairoEngine.getTableTokenIfExists(task.getTableName());
            }
            if (tableToken != null) {
                cairoEngine.dropTableOrViewOrMatView(Path.getThreadLocal(""), tableToken);
            }
            copyExportContext.updateStatus(phase, CopyExportRequestTask.Status.FINISHED, null, Numbers.INT_NULL, null, 0, task.getTableName(), task.getCopyID());
        } catch (CairoException e) {
            LOG.error().$("fail to drop temporary table [id=").$hexPadded(task.getCopyID()).$(", table=").$(tableToken).$(", msg=").$(e.getFlyweightMessage()).$(']').$();
            copyExportContext.updateStatus(phase, CopyExportRequestTask.Status.FAILED, null, Numbers.INT_NULL, null, 0, task.getTableName(), task.getCopyID());
        } catch (Throwable e) {
            LOG.error().$("fail to drop temporary table [id=").$hexPadded(task.getCopyID()).$(", table=").$(tableToken).$(", msg=").$(e.getMessage()).$(']').$();
            copyExportContext.updateStatus(phase, CopyExportRequestTask.Status.FAILED, null, Numbers.INT_NULL, null, 0, task.getTableName(), task.getCopyID());
        }
    }
}
