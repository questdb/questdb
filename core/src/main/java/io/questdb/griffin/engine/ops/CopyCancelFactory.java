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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.text.CopyContext;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.SingleValueRecordCursor;

/**
 * Executes COPY CANCEL statement lazily, i.e. on record cursor initialization, to play
 * nicely with server-side statements in PG Wire and query caching in general.
 */
public class CopyCancelFactory extends AbstractRecordCursorFactory {

    private final static GenericRecordMetadata METADATA = new GenericRecordMetadata();
    private static final int STATUS_INDEX = 5;
    private final RecordCursorFactory baseFactory;
    private final long cancelCopyID;
    private final String cancelCopyIDStr;
    private final CopyContext copyContext;
    private final CopyCancelRecord record = new CopyCancelRecord();
    private final SingleValueRecordCursor cursor = new SingleValueRecordCursor(record);
    private CharSequence status;

    public CopyCancelFactory(
            CopyContext copyContext,
            long cancelCopyID,
            String cancelCopyIDStr,
            RecordCursorFactory baseFactory
    ) {
        super(METADATA);
        this.copyContext = copyContext;
        this.cancelCopyID = cancelCopyID;
        this.cancelCopyIDStr = cancelCopyIDStr;
        this.baseFactory = baseFactory;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final AtomicBooleanCircuitBreaker circuitBreaker = copyContext.getCircuitBreaker();
        final long activeCopyID = copyContext.getActiveCopyID();

        if (activeCopyID == cancelCopyID && cancelCopyID != CopyContext.INACTIVE_COPY_ID) {
            copyContext.getOriginatorSecurityContext().authorizeCopyCancel(executionContext.getSecurityContext());
            circuitBreaker.cancel();
            // Cancelled active import, probably :)
            // This action is async and there is no guarantee that target table does not exist
            // to determine if COPY has stopped the client has to wait for status table to
            // be updated.
            status = "cancelled";
        } else {
            try (RecordCursor c = baseFactory.getCursor(executionContext)) {
                Record rec = c.getRecord();
                // should be one row
                if (c.hasNext()) {
                    status = rec.getSymA(STATUS_INDEX);
                } else {
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
        baseFactory.close();
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
