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

package io.questdb.griffin.engine.ops;

import io.questdb.MessageBus;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RowExpiryUtil;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cutlass.parquet.CopyExportRequestTask;
import io.questdb.cutlass.parquet.ParquetExportMode;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.SingleValueRecordCursor;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.model.ExportModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.std.Chars;
import io.questdb.std.GenericLexer;
import io.questdb.std.Misc;
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
    private @Nullable CharSequence bloomFilterColumns;
    private int bloomFilterColumnsPosition = -1;
    private double bloomFilterFpp = Double.NaN;
    private int compressionCodec;
    private int compressionLevel;
    private CopyExportContext copyContext;
    private int dataPageSize;
    private @Nullable String explicitSelectText = null;
    private String fileName;
    private MessageBus messageBus;
    private int parquetVersion;
    private int partitionBy;
    private boolean rawArrayEncoding = false;
    private int rowGroupSize;
    private SecurityContext securityContext;
    private @Nullable String sourceTableName = null;
    private CharSequence sqlText;
    private boolean statisticsEnabled;
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
                this.sourceTableName != null ? this.sourceTableName : this.explicitSelectText,
                this.fileName,
                null,
                CopyExportContext.CopyTrigger.SQL
        );
        long copyID = entry.getId();
        RecordCursorFactory selectFactory = null;
        CreateTableOperationImpl createOp = null;
        // Every routing decision this execution makes lives in locals. The parsed COPY source stays in
        // sourceTableName / explicitSelectText, so a cached factory resolves the same source on every call.
        String resolvedSelectText = this.explicitSelectText;
        String taskTableName = this.sourceTableName;
        try {
            if (this.sourceTableName != null) {
                TableToken tableToken = executionContext.getTableTokenIfExists(sourceTableName);
                if (tableToken == null) {
                    throw SqlException.tableDoesNotExist(tableOrSelectTextPos, sourceTableName);
                }
                // A table that carries an EXPIRE ROWS policy must export through the SELECT path: the read
                // filter that hides expired rows lives in the parser, so a table-reader export would write
                // out rows the table itself no longer returns. Re-partitioning needs the SELECT path too.
                final boolean mayHaveExpiryPolicy = executionContext.getCairoEngine()
                        .getMetadataCache().mayTableHaveExpiryPolicy(tableToken);
                if (partitionBy != -1 || mayHaveExpiryPolicy) {
                    try (TableMetadata meta = executionContext.getCairoEngine().getTableMetadata(tableToken)) {
                        final int tablePartitionBy = meta.getPartitionBy();
                        final String expiryPredicate = meta.getExpiryPredicate();
                        final boolean isSelectPathRequired = (partitionBy != -1 && tablePartitionBy != partitionBy)
                                || (expiryPredicate != null && !expiryPredicate.isEmpty());
                        if (isSelectPathRequired) {
                            // The source name is a catalog identifier, and the compiler reads this value as SQL.
                            // Quoting it keeps the query pointing at the very table the token above resolved:
                            // a name with a space, a hyphen, a keyword or a leading digit is one identifier here,
                            // exactly as it is in the catalog. The COPY target itself arrives in any of three
                            // quoting forms, so the parsed token cannot serve as SQL text either.
                            resolvedSelectText = RowExpiryUtil.quoteIdentifier(sourceTableName);
                        }
                    }
                }
            }

            ParquetExportMode exportMode = ParquetExportMode.TABLE_READER;
            if (resolvedSelectText != null) {
                // Determine export mode before creating CreateTableOperation.
                // For non-TEMP_TABLE modes the CreateTableOperation (and its
                // extra compilation) is not needed.
                exportIdSink.clear();
                exportIdSink.put("copy.");
                Numbers.appendHex(exportIdSink, copyID, true);
                taskTableName = exportIdSink.toString();

                CairoEngine engine = executionContext.getCairoEngine();
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    CompiledQuery selectQuery = compiler.compile(resolvedSelectText, executionContext);
                    if (selectQuery.getType() != CompiledQuery.SELECT) {
                        selectQuery.closeAllButSelect();
                        throw SqlException.$(0, "Copy command only accepts SELECT queries");
                    }
                    RecordCursorFactory rcf = selectQuery.getRecordCursorFactory();
                    try {
                        int resolvedPartitionBy = partitionBy == -1 ? PartitionBy.NONE : partitionBy;
                        if (resolvedPartitionBy == PartitionBy.NONE) {
                            exportMode = ParquetExportMode.determineExportMode(rcf, false, executionContext);
                        } else {
                            // Re-partitioning always requires a temp table
                            exportMode = ParquetExportMode.TEMP_TABLE;
                        }
                        if (exportMode == ParquetExportMode.TEMP_TABLE) {
                            createOp = new CreateTableOperationImpl(
                                    Chars.toString(resolvedSelectText),
                                    taskTableName,
                                    resolvedPartitionBy,
                                    false,
                                    engine.getConfiguration().getDefaultSymbolCapacity(),
                                    sqlText.toString(),
                                    false
                            );
                            createOp.setTableKind(TableUtils.TABLE_KIND_TEMP_PARQUET_EXPORT);
                            createOp.setBatchSize(engine.getConfiguration().getParquetExportBatchSize());
                            createOp.validateAndUpdateMetadataFromSelect(
                                    rcf.getMetadata(), rcf.getScanDirection()
                            );
                            CopyExportRequestTask.validateBloomFilterColumns(
                                    bloomFilterColumns, rcf.getMetadata(), bloomFilterColumnsPosition - tableOrSelectTextPos
                            );
                        } else {
                            CopyExportRequestTask.validateBloomFilterColumns(
                                    bloomFilterColumns, rcf.getMetadata(), bloomFilterColumnsPosition - tableOrSelectTextPos
                            );
                            selectFactory = rcf;
                            rcf = null;
                        }
                    } finally {
                        Misc.free(rcf);
                    }
                } catch (SqlException ex) {
                    // Positions map back onto the COPY source token. A quoted source name is one character
                    // longer than a bare one, which the mapping ignores: the table is resolved before this
                    // compilation, so an ordinary source-name error never reaches here.
                    ex.setPosition(ex.getPosition() + tableOrSelectTextPos);
                    throw ex;
                } catch (CairoException ex) {
                    ex.position(tableOrSelectTextPos + ex.getPosition());
                    throw ex;
                }
            } else if (bloomFilterColumns != null && !bloomFilterColumns.isEmpty()) {
                TableToken token = executionContext.getTableTokenIfExists(sourceTableName);
                try (TableMetadata meta = executionContext.getCairoEngine().getTableMetadata(token)) {
                    CopyExportRequestTask.validateBloomFilterColumns(bloomFilterColumns, meta, bloomFilterColumnsPosition);
                }
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
                    taskTableName,
                    entry.getId()
            );

            int nowTimestampType = executionContext.getNowTimestampType();
            long now = executionContext.getNow(nowTimestampType);
            BindVariableService bindVariableSnapshot = BindVariableServiceImpl.snapshot(
                    executionContext.getBindVariableService(),
                    executionContext.getCairoEngine().getConfiguration()
            );

            do {
                processingCursor = copyRequestPubSeq.next();
            } while (processingCursor == -2);

            if (processingCursor == -1) {
                throw SqlException.$(0, "unable to process the export request - export queue is full");
            }

            try {
                final CopyExportRequestTask task = copyExportRequestQueue.get(processingCursor);
                task.of(
                        entry,
                        createOp,
                        taskTableName,
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
                        null,
                        exportMode,
                        resolvedSelectText,
                        bloomFilterColumns,
                        bloomFilterColumnsPosition,
                        bloomFilterFpp,
                        bindVariableSnapshot
                );
                task.setSelectFactory(selectFactory);
                selectFactory = null;
                createOp = null; // ownership transferred to queue task
            } finally {
                copyRequestPubSeq.done(processingCursor);
            }
            // Entry is now owned by the task
            entry = null;
            // The export task is already published (the background job is launched) at this point, so
            // keep this ack cursor on the no-op breaker: a CANCEL QUERY or a tripped deadline must not
            // suppress the export-id row while the data-movement operation keeps running.
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
            Misc.free(selectFactory);
            Misc.free(createOp);
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
            this.sourceTableName = unquote(model.getTableName()).toString();
            this.tableOrSelectTextPos = model.getTableNameExpr().position;
        } else {
            assert model.getSelectText() != null;
            this.tableOrSelectTextPos = model.getSelectTextStartPos();
        }

        final ExpressionNode fileNameExpr = model.getFileName();
        this.fileName = fileNameExpr != null ? Chars.toString(GenericLexer.assertNoDots(unquote(fileNameExpr.token), fileNameExpr.position)) : null;
        this.securityContext = securityContext;
        this.explicitSelectText = Chars.toString(model.getSelectText());
        this.partitionBy = model.getPartitionBy();
        this.compressionCodec = model.getCompressionCodec();
        this.compressionLevel = model.getCompressionLevel();
        this.rowGroupSize = model.getRowGroupSize();
        this.dataPageSize = model.getDataPageSize();
        this.statisticsEnabled = model.isStatisticsEnabled();
        this.parquetVersion = model.getParquetVersion();
        this.rawArrayEncoding = model.isRawArrayEncoding();
        CharSequence filterColumns = model.getBloomFilterColumns();
        if (filterColumns != null && !filterColumns.isEmpty()) {
            this.bloomFilterColumns = filterColumns.toString();
        }
        this.bloomFilterColumnsPosition = model.getBloomFilterColumnsPosition();
        this.bloomFilterFpp = model.getBloomFilterFpp();
        this.sqlText = sqlText;
    }

    static {
        METADATA.add(new TableColumnMetadata("id", ColumnType.STRING));
    }
}
