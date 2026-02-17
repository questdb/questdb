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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.cutlass.text.CopyImportContext;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.SingleValueRecordCursor;
import io.questdb.std.Misc;

/**
 * Executes COPY CANCEL statement lazily, i.e. on record cursor initialization, to play
 * nicely with server-side statements in PG Wire and query caching in general.
 */
public class CopyCancelFactory extends AbstractRecordCursorFactory {

    private static final int EXPORT_STATUS_INDEX = 6;
    private static final int IMPORT_STATUS_INDEX = 5;
    private final static GenericRecordMetadata METADATA = new GenericRecordMetadata();
    private final long cancelCopyID;
    private final String cancelCopyIDStr;
    private final CopyExportContext copyExportContext;
    private final CopyImportContext copyImportContext;
    private final RecordCursorFactory exportBaseFactory;
    private final RecordCursorFactory importBaseFactory;
    private final CopyCancelRecord record = new CopyCancelRecord();
    private final SingleValueRecordCursor cursor = new SingleValueRecordCursor(record);
    private CharSequence status;

    public CopyCancelFactory(
            CopyImportContext copyImportContext,
            CopyExportContext copyExportContext,
            long cancelCopyID,
            String cancelCopyIDStr,
            RecordCursorFactory importBaseFactory,
            RecordCursorFactory exportBaseFactory
    ) {
        super(METADATA);
        this.copyImportContext = copyImportContext;
        this.copyExportContext = copyExportContext;
        this.cancelCopyID = cancelCopyID;
        this.cancelCopyIDStr = cancelCopyIDStr;
        this.importBaseFactory = importBaseFactory;
        this.exportBaseFactory = exportBaseFactory;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final long activeImportCopyID = copyImportContext.getActiveImportID();

        if (activeImportCopyID == cancelCopyID && cancelCopyID != CopyImportContext.INACTIVE_COPY_ID) {
            final AtomicBooleanCircuitBreaker circuitBreaker = copyImportContext.getCircuitBreaker();
            copyImportContext.getImportOriginatorSecurityContext().authorizeCopyCancel(executionContext.getSecurityContext());
            circuitBreaker.cancel();
            // Cancelled active import, probably :)
            // This action is async and there is no guarantee that target table does not exist
            // to determine if COPY has stopped the client has to wait for status table to
            // be updated.
            status = "cancelled";
        } else {
            if (copyExportContext.cancel(cancelCopyID, executionContext.getSecurityContext())) {
                status = "cancelled";
            } else {
                status = null;
                if (importBaseFactory != null) {
                    try (RecordCursor importCursor = importBaseFactory.getCursor(executionContext)) {
                        Record rec = importCursor.getRecord();
                        // should be one row
                        if (importCursor.hasNext()) {
                            status = rec.getSymA(IMPORT_STATUS_INDEX);
                        }
                    }
                }

                if (exportBaseFactory != null && status == null) {
                    try (RecordCursor exportCursor = exportBaseFactory.getCursor(executionContext)) {
                        Record rec = exportCursor.getRecord();
                        // should be one row
                        if (exportCursor.hasNext()) {
                            status = rec.getSymA(EXPORT_STATUS_INDEX);
                        }
                    }
                }

                if (status == null) {
                    status = "unknown";
                }
            }
        }
        cursor.toTop();
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Copy Cancel");
    }

    @Override
    protected void _close() {
        Misc.free(importBaseFactory);
        Misc.free(exportBaseFactory);
        super._close();
    }

    private class CopyCancelRecord implements Record {
        @Override
        public CharSequence getStrA(int col) {
            switch (col) {
                case 0:
                    return cancelCopyIDStr;
                case 1:
                    return status;
                default:
                    throw new UnsupportedOperationException();
            }
        }
    }

    static {
        METADATA.add(new TableColumnMetadata("id", ColumnType.STRING));
        METADATA.add(new TableColumnMetadata("status", ColumnType.STRING));
    }
}
