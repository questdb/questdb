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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cutlass.parquet.CopyExportRequestTask;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.cutlass.text.CopyImportContext;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.SingleValueRecordCursor;
import io.questdb.griffin.model.CopyModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.network.SuspendEvent;
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

    private final static GenericRecordMetadata METADATA = new GenericRecordMetadata();
    private int compressionCodec;
    private int compressionLevel;
    private CopyExportContext copyContext;
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
        final AtomicBooleanCircuitBreaker circuitBreaker = copyContext.getCircuitBreaker();

        long activeCopyID = copyContext.getActiveExportID();
        if (activeCopyID == CopyImportContext.INACTIVE_COPY_ID) {
            long processingCursor = copyRequestPubSeq.next();
            if (processingCursor > -1) {
                final CopyExportRequestTask task = copyExportRequestQueue.get(processingCursor);

                long copyID = copyContext.assignActiveExportId(executionContext.getSecurityContext());

                if (this.selectText != null) {
                    // need to create a temp table which we will use for the export
                    createTempTable(copyID, executionContext); //
                    exportIdSink.clear();
                    exportIdSink.put("copy.");
                    Numbers.appendHex(exportIdSink, copyID, true);
                    tableName = exportIdSink.toString();
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

                circuitBreaker.reset();
                copyRequestPubSeq.done(processingCursor);

                cursor.toTop();
                return cursor;
            } else {
                throw SqlException.$(0, "Unable to process the export request. Another export request may be in progress.");
            }
        } else {
            exportIdSink.clear();
            Numbers.appendHex(exportIdSink, activeCopyID, true);
            throw SqlException.$(0, "Another export request is in progress. ")
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

    void createTempTable(long copyID, SqlExecutionContext executionContext) throws SqlException {
        exportIdSink.put("CREATE TABLE 'copy.");
        Numbers.appendHex(exportIdSink, copyID, true);
        exportIdSink.put("' AS (").put(selectText).put(')');

        if (partitionBy != PartitionBy.NONE && partitionBy > -1) {
            exportIdSink.put(" PARTITION BY ").put(PartitionBy.toString(partitionBy));
        }
        exportIdSink.put(';');
        executionContext.getCairoEngine().execute(exportIdSink);
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
