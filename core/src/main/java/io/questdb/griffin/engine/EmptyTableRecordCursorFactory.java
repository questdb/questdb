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

package io.questdb.griffin.engine;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

public class EmptyTableRecordCursorFactory extends AbstractRecordCursorFactory {
    private final ObjList<PartitionFrameCursorFactory> frames;
    private final TableToken tableToken;

    public EmptyTableRecordCursorFactory(RecordMetadata metadata) {
        this(metadata, null, null);
    }

    /**
     * Rows the generator proved empty in place of reads of tables: each cursor it opens checks, through the frames of
     * each read, that the context may read what the read references. Takes ownership of the frames, which may be null.
     */
    public EmptyTableRecordCursorFactory(RecordMetadata metadata, @Nullable ObjList<PartitionFrameCursorFactory> frames) {
        this(metadata, null, frames);
    }

    /**
     * The scan of the target of a sequenced UPDATE, which reads no rows: the apply job reads them. Each cursor it opens
     * checks the read through the frames, as {@link #EmptyTableRecordCursorFactory(RecordMetadata, ObjList)} does.
     */
    public EmptyTableRecordCursorFactory(RecordMetadata metadata, TableToken tableToken, @Nullable ObjList<PartitionFrameCursorFactory> frames) {
        super(metadata);
        this.tableToken = tableToken;
        this.frames = frames;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        for (int i = 0, n = frames == null ? 0 : frames.size(); i < n; i++) {
            frames.getQuick(i).authorizeSelect(executionContext);
        }
        // The cursor is empty, so it never iterates and never consults the circuit breaker on its own.
        // Honor cancellation once at open time, so a query that optimized down to an empty result still
        // observes a tripped breaker instead of silently ignoring it.
        executionContext.getCircuitBreaker().statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        return EmptyTableRandomRecordCursor.INSTANCE;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return this.tableToken == null || this.tableToken == tableToken;
    }

    @Override
    public void toPlan(PlanSink sink) {
        if (tableToken != null) {
            sink.type("on").val(": ").val(tableToken.getTableName());
        } else {
            sink.type("Empty table");
        }
    }

    @Override
    protected void _close() {
        Misc.freeObjList(frames);
        Misc.freeIfCloseable(getMetadata());
    }
}
