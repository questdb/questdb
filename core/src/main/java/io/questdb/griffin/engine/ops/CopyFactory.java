/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cutlass.text.AtomicBooleanCircuitBreaker;
import io.questdb.cutlass.text.CopyContext;
import io.questdb.cutlass.text.CopyRequestTask;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.SingleValueRecordCursor;
import io.questdb.griffin.model.CopyModel;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.str.StringSink;

/**
 * Executes COPY statement lazily, i.e. on record cursor initialization, to play
 * nicely with server-side statements in PG Wire and query caching in general.
 */
public class CopyFactory extends AbstractRecordCursorFactory {

    private final static GenericRecordMetadata METADATA = new GenericRecordMetadata();
    private final int atomicity;
    private final CopyContext copyContext;
    private final byte delimiter;
    private final String fileName;
    private final boolean headerFlag;
    private final StringSink importIdSink = new StringSink();
    private final MessageBus messageBus;
    private final int partitionBy;
    private final CopyRecord record = new CopyRecord();
    private final SingleValueRecordCursor cursor = new SingleValueRecordCursor(record);
    private final String tableName;
    private final String timestampColumn;
    private final String timestampFormat;

    public CopyFactory(
            MessageBus messageBus,
            CopyContext copyContext,
            String tableName,
            String fileName,
            CopyModel model
    ) {
        super(METADATA);
        this.messageBus = messageBus;
        this.copyContext = copyContext;
        this.tableName = tableName;
        this.fileName = fileName;
        this.headerFlag = model.isHeader();
        this.timestampColumn = Chars.toString(model.getTimestampColumnName());
        this.timestampFormat = Chars.toString(model.getTimestampFormat());
        this.delimiter = model.getDelimiter();
        this.partitionBy = model.getPartitionBy();
        this.atomicity = model.getAtomicity();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RingQueue<CopyRequestTask> textImportRequestQueue = messageBus.getTextImportRequestQueue();
        final MPSequence copyRequestPubSeq = messageBus.getCopyRequestPubSeq();
        final AtomicBooleanCircuitBreaker circuitBreaker = copyContext.getCircuitBreaker();

        long activeCopyID = copyContext.getActiveCopyID();
        if (activeCopyID == CopyContext.INACTIVE_COPY_ID) {
            long processingCursor = copyRequestPubSeq.next();
            if (processingCursor > -1) {
                final CopyRequestTask task = textImportRequestQueue.get(processingCursor);

                long copyID = copyContext.assignActiveImportId(executionContext.getSecurityContext());
                task.of(
                        executionContext.getSecurityContext(),
                        copyID,
                        tableName,
                        fileName,
                        headerFlag,
                        timestampColumn,
                        delimiter,
                        timestampFormat,
                        partitionBy,
                        atomicity
                );

                circuitBreaker.reset();
                copyRequestPubSeq.done(processingCursor);

                importIdSink.clear();
                Numbers.appendHex(importIdSink, copyID, true);
                record.setValue(importIdSink);
                cursor.toTop();
                return cursor;
            } else {
                throw SqlException.$(0, "Unable to process the import request. Another import request may be in progress.");
            }
        }

        importIdSink.clear();
        Numbers.appendHex(importIdSink, activeCopyID, true);
        throw SqlException.$(0, "Another import request is in progress. ")
                .put("[activeImportId=")
                .put(importIdSink)
                .put(']');
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Copy");
    }

    private static class CopyRecord implements Record {
        private CharSequence value;

        @Override
        public CharSequence getStr(int col) {
            return value;
        }

        @Override
        public CharSequence getStrB(int col) {
            // the sink is immutable
            return getStr(col);
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
