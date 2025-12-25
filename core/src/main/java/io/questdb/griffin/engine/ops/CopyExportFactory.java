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

package io.questdb.griffin.engine.ops;

import io.questdb.MessageBus;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cutlass.parquet.CopyExportRequestTask;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.SingleValueRecordCursor;
import io.questdb.griffin.model.ExportModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.std.Chars;
import io.questdb.std.GenericLexer;
import io.questdb.std.Numbers;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

import static io.questdb.std.GenericLexer.unquote;

/**
 * Executes COPY statement lazily, i.e. on record cursor initialization, to play
 * nicely with server-side statements in PG Wire and query caching in general.
 */
public class CopyExportFactory extends AbstractRecordCursorFactory {

    private static final Log LOG = LogFactory.getLog(CopyExportFactory.class);
    private final static GenericRecordMetadata METADATA = new GenericRecordMetadata();
    private final StringSink exportIdSink = new StringSink();
    private final CopyImportFactory.CopyRecord record = new CopyImportFactory.CopyRecord();
    private final SingleValueRecordCursor cursor = new SingleValueRecordCursor(record);
    private int compressionCodec;
    private int compressionLevel;
    private CopyExportContext copyContext;
    private int dataPageSize;
    private String fileName;
    private MessageBus messageBus;
    private int parquetVersion;
    private int partitionBy;
    private boolean rawArrayEncoding = false;
    private int rowGroupSize;
    private SecurityContext securityContext;
    private String selectText = null;
    private CharSequence sqlText;
    private boolean statisticsEnabled;
    private @Nullable String tableName = null;
    private int tableOrSelectTextPos = 0;

    public CopyExportFactory(
            CairoEngine engine,
            ExportModel model,
            SecurityContext securityContext,
            CharSequence sqlText
    ) throws SqlException {
        super(METADATA);
        this.of(engine.getMessageBus(), engine.getCopyExportContext(), model, securityContext, sqlText);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        CopyExportContext.ExportTaskEntry entry = copyContext.assignExportEntry(
                securityContext,
                this.tableName != null ? this.tableName : this.selectText,
                this.fileName,
                null,
                CopyExportContext.CopyTrigger.SQL
        );
        long copyID = entry.getId();
        try {
            CreateTableOperation createOp = null;
            if (this.tableName != null) {
                TableToken tableToken = executionContext.getTableTokenIfExists(tableName);
                if (tableToken == null) {
                    throw SqlException.tableDoesNotExist(tableOrSelectTextPos, tableName);
                }
                if (partitionBy != -1) {
                    try (TableMetadata meta = executionContext.getCairoEngine().getTableMetadata(tableToken)) {
                        int tablePartitionBy = meta.getPartitionBy();
                        if (tablePartitionBy != partitionBy) {
                            this.selectText = this.tableName;
                        }
                    }
                }
            }

            if (this.selectText != null) {
                // prepare to create a temp table
                exportIdSink.clear();
                exportIdSink.put("copy.");
                Numbers.appendHex(exportIdSink, copyID, true);
                this.tableName = exportIdSink.toString();
                createOp = copyContext.validateAndCreateParquetExportTableOp(
                        executionContext,
                        selectText,
                        partitionBy,
                        tableName,
                        sqlText.toString(),
                        tableOrSelectTextPos
                );
            }

            exportIdSink.clear();
            Numbers.appendHex(exportIdSink, copyID, true);
            record.setValue(exportIdSink);
            final RingQueue<CopyExportRequestTask> copyExportRequestQueue = messageBus.getCopyExportRequestQueue();
            final MPSequence copyRequestPubSeq = messageBus.getCopyExportRequestPubSeq();
            long processingCursor;

            copyContext.updateStatus(
                    CopyExportRequestTask.Phase.WAITING,
                    CopyExportRequestTask.Status.STARTED,
                    null,
                    Numbers.INT_NULL,
                    "queued",
                    0,
                    tableName,
                    entry.getId()
            );

            do {
                processingCursor = copyRequestPubSeq.next();
            } while (processingCursor == -2);

            if (processingCursor == -1) {
                throw SqlException.$(0, "unable to process the export request - export queue is full");
            }

            try {
                final CopyExportRequestTask task = copyExportRequestQueue.get(processingCursor);
                int nowTimestampType = executionContext.getNowTimestampType();
                long now = executionContext.getNow(nowTimestampType);
                task.of(
                        entry,
                        createOp,
                        tableName,
                        fileName,
                        compressionCodec,
                        compressionLevel,
                        rowGroupSize,
                        dataPageSize,
                        statisticsEnabled,
                        parquetVersion,
                        rawArrayEncoding,
                        nowTimestampType,
                        now,
                        false,
                        null,
                        null,
                        null
                );
            } finally {
                copyRequestPubSeq.done(processingCursor);
            }
            // Entry is now owned by the task
            entry = null;
            cursor.toTop();
            return cursor;
        } catch (SqlException | CairoException ex) {
            exportIdSink.clear();
            Numbers.appendHex(exportIdSink, copyID, true);
            LOG.errorW().$("copy failed [id=").$(exportIdSink).$(", message=").$(ex.getFlyweightMessage()).I$();
            throw ex;
        } catch (Throwable ex) {
            exportIdSink.clear();
            Numbers.appendHex(exportIdSink, copyID, true);
            LOG.errorW().$("copy failed [id=").$(exportIdSink).$(", message=").$(ex.getMessage()).I$();
            throw ex;
        } finally {
            if (entry != null) {
                copyContext.releaseEntry(entry);
            }
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Copy");
    }

    private void of(
            MessageBus messageBus,
            CopyExportContext exportContext,
            ExportModel model,
            SecurityContext securityContext,
            CharSequence sqlText
    ) throws SqlException {
        this.messageBus = messageBus;
        this.copyContext = exportContext;
        if (model.getTableName() != null) {
            this.tableName = unquote(model.getTableName()).toString();
            this.tableOrSelectTextPos = model.getTableNameExpr().position;
        } else {
            assert model.getSelectText() != null;
            this.tableOrSelectTextPos = model.getSelectTextStartPos();
        }

        final ExpressionNode fileNameExpr = model.getFileName();
        this.fileName = fileNameExpr != null ? Chars.toString(GenericLexer.assertNoDots(unquote(fileNameExpr.token), fileNameExpr.position)) : null;
        this.securityContext = securityContext;
        this.selectText = Chars.toString(model.getSelectText());
        this.partitionBy = model.getPartitionBy();
        this.compressionCodec = model.getCompressionCodec();
        this.compressionLevel = model.getCompressionLevel();
        this.rowGroupSize = model.getRowGroupSize();
        this.dataPageSize = model.getDataPageSize();
        this.statisticsEnabled = model.isStatisticsEnabled();
        this.parquetVersion = model.getParquetVersion();
        this.rawArrayEncoding = model.isRawArrayEncoding();
        this.sqlText = sqlText;
    }

    static {
        METADATA.add(new TableColumnMetadata("id", ColumnType.STRING));
    }
}
