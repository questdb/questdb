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

package io.questdb.griffin.engine.ops;

import io.questdb.MessageBus;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.AtomicCountedCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.parquet.CopyExportRequestTask;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.cutlass.text.CopyImportContext;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.SingleValueRecordCursor;
import io.questdb.griffin.model.CopyModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.network.SuspendEvent;
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
    private int compressionCodec;
    private int compressionLevel;
    private CopyExportContext copyContext;
    private long copyID;
    private int dataPageSize;
    private StringSink exportIdSink = new StringSink();
    private String fileName;
    private MessageBus messageBus;
    private int parquetVersion;
    private int partitionBy;
    private CopyRecord record = new CopyRecord();
    private SingleValueRecordCursor cursor = new SingleValueRecordCursor(record);
    private int rowGroupSize;
    private @Nullable SecurityContext securityContext;
    private @Nullable String selectText = null;
    private int sizeLimit;
    private boolean statisticsEnabled;
    private @Nullable SuspendEvent suspendEvent = null;
    private @Nullable String tableName = null;


    public CopyExportFactory(
            MessageBus messageBus,
            CopyExportContext copyContext,
            CopyModel model,
            SecurityContext securityContext
    ) throws SqlException {
        super(METADATA);
        this.of(messageBus, copyContext, model, securityContext);
    }

    public CopyExportFactory(
            MessageBus messageBus,
            CopyExportContext copyContext,
            CopyModel model,
            SecurityContext securityContext,
            @Nullable SuspendEvent suspendEvent
    ) throws SqlException {
        super(METADATA);
        this.suspendEvent = suspendEvent;
        this.of(messageBus, copyContext, model, securityContext);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RingQueue<CopyExportRequestTask> copyExportRequestQueue = messageBus.getCopyExportRequestQueue();
        final MPSequence copyRequestPubSeq = messageBus.getCopyExportRequestPubSeq();
        final AtomicCountedCircuitBreaker circuitBreaker = copyContext.getCircuitBreaker();

        long activeCopyID = copyContext.getActiveExportID();
        if (activeCopyID == CopyImportContext.INACTIVE_COPY_ID) {
            long processingCursor = copyRequestPubSeq.next();
            if (processingCursor > -1) {
                final CopyExportRequestTask task = copyExportRequestQueue.get(processingCursor);
                circuitBreaker.reset();

                circuitBreaker.inc();

                try {
                    copyID = copyContext.assignActiveExportId(executionContext.getSecurityContext());


                    if (this.selectText != null) {
                        // need to create a temp table which we will use for the export

                        exportIdSink.clear();
                        exportIdSink.put("copy.");
                        Numbers.appendHex(exportIdSink, copyID, true);
                        this.tableName = exportIdSink.toString();

                        // we need a new execution context that uses our circuit breaker, so copy cancel will apply
                        // to the query
                        SqlExecutionContextImpl queryExecutionContext = new SqlExecutionContextImpl(executionContext.getCairoEngine(), 1);
                        assert securityContext != null;

                        // increment for query task
                        circuitBreaker.inc();
                        queryExecutionContext.with(securityContext, null, null, -1, circuitBreaker);
                        createTempTable(queryExecutionContext);
                    }

                    assert tableName != null;

                    // sanity check that the table exists.
                    // TOCTOU - but we don't pass the table token, just fail early
                    if (executionContext.getTableToken(tableName) == null) {
                        throw SqlException.tableDoesNotExist(0, tableName);
                    }

                    exportIdSink.clear();
                    Numbers.appendHex(exportIdSink, copyID, true);
                    record.setValue(exportIdSink);

                    task.of(
                            executionContext.getSecurityContext(),
                            copyID,
                            tableName,
                            fileName,
                            sizeLimit,
                            compressionCodec,
                            compressionLevel,
                            rowGroupSize,
                            dataPageSize,
                            statisticsEnabled,
                            parquetVersion,
                            suspendEvent
                    );

                    cursor.toTop();
                    return cursor;
                } catch (Throwable ex) {
                    exportIdSink.clear();
                    Numbers.appendHex(exportIdSink, copyID, true);
                    LOG.errorW().$("copy failed [id=").$(exportIdSink).$(", message=").$(ex.getMessage()).I$();
                    // cleanup temp table
                    if (Chars.startsWith(tableName, "copy.")) {
                        LOG.info().$("cleaning up temp table [table=").$(tableName).I$();
                        try {
                            executionContext.getCairoEngine().execute("DROP TABLE IF EXISTS '" + tableName + "';");
                        } catch (SqlException e) {
                            LOG.errorW().$("cleaning up temp table failed [table=").$(tableName).I$();
                            throw e;
                        }
                    }
                    // clear copy context
                    copyContext.clear();
                    throw ex;
                } finally {
                    copyRequestPubSeq.done(processingCursor);
                }
            } else {
                throw SqlException.$(0, "unable to process the export request - another export may be in progress");
            }
        } else {
            exportIdSink.clear();
            Numbers.appendHex(exportIdSink, activeCopyID, true);
            throw SqlException.$(0, "another export request is in progress ")
                    .put("[activeExportId=")
                    .put(exportIdSink)
                    .put(']');
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

    private void of(MessageBus messageBus,
                    CopyExportContext copyImportContext,
                    CopyModel model,
                    SecurityContext securityContext) throws SqlException {
        this.messageBus = messageBus;
        this.copyContext = copyImportContext;

        if (model.getTableName() != null) {
            this.tableName = GenericLexer.unquote(model.getTableName()).toString();

        } else {
            assert model.getSelectText() != null;
        }

        final ExpressionNode fileNameExpr = model.getFileName();
        this.fileName = fileNameExpr != null ? GenericLexer.assertNoDots(unquote(fileNameExpr.token), fileNameExpr.position).toString() : null;
        this.securityContext = securityContext;
        this.selectText = model.getSelectText();
        this.partitionBy = model.getPartitionBy();
        this.sizeLimit = model.getSizeLimit();
        this.compressionCodec = model.getCompressionCodec();
        this.compressionLevel = model.getCompressionLevel();
        this.rowGroupSize = model.getRowGroupSize();
        this.dataPageSize = model.getDataPageSize();
        this.statisticsEnabled = model.isStatisticsEnabled();
        this.parquetVersion = model.getParquetVersion();
    }

    void createTempTable(SqlExecutionContext executionContext) throws SqlException {
        // compile the select to get the metadata
        CairoEngine engine = executionContext.getCairoEngine();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            CompiledQuery selectQuery = compiler.compile(selectText, executionContext);
            try (RecordCursorFactory rcf = selectQuery.getRecordCursorFactory()) {
                RecordMetadata metadata = rcf.getMetadata();

                try (CreateTableOperationImpl impl = new CreateTableOperationImpl(
                        selectText,
                        tableName,
                        partitionBy,
                        executionContext.getCairoEngine().getConfiguration().getDefaultSymbolCapacity())) {
                    impl.validateAndUpdateMetadataFromSelect(metadata);
                    impl.setSelectText(null);
                    impl.execute(executionContext, null);
                }

                ((AtomicCountedCircuitBreaker) executionContext.getCircuitBreaker()).inc();

                exportIdSink.clear();
                exportIdSink.put("INSERT INTO '").put(tableName).put("' SELECT * FROM (").put(selectText).put(')');

                executionContext.getCairoEngine().execute(exportIdSink, executionContext);
            }
        }
    }


    private static class CopyRecord implements Record {
        private CharSequence value;

        @Override
        public CharSequence getStrA(int col) {
            return value;
        }

        @Override
        public CharSequence getStrB(int col) {
            // the sink is immutable
            return getStrA(col);
        }

        @Override
        public int getStrLen(int col) {
            return value.length();
        }

        public void setValue(CharSequence value) {
            this.value = value;
        }
    }

    static {
        METADATA.add(new TableColumnMetadata("id", ColumnType.STRING));
    }
}
