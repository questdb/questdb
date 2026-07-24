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

package io.questdb.griffin.engine.orderby;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.BitSet;
import io.questdb.std.IntList;
import io.questdb.std.Misc;

public class SortKeyMaterializingRecordCursorFactory extends AbstractRecordCursorFactory {
    private final BitSet materializedColumns = new BitSet();
    private RecordCursorFactory base;
    private SortKeyMaterializingRecordCursor cursor;

    public SortKeyMaterializingRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory base,
            IntList materializedColIndices,
            IntList materializedColTypes
    ) {
        super(metadata);
        assert base.recordCursorSupportsRandomAccess()
                : "SortKeyMaterializingRecordCursorFactory requires a base factory that supports random access";
        this.base = base;
        for (int i = 0, n = materializedColIndices.size(); i < n; i++) {
            materializedColumns.set(materializedColIndices.getQuick(i));
        }
        this.cursor = new SortKeyMaterializingRecordCursor(
                metadata.getColumnCount(),
                materializedColIndices,
                materializedColTypes,
                configuration.getSqlSortKeyMaxBytes()
        );
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            cursor.of(baseCursor, executionContext);
            return cursor;
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public boolean isColumnIntWidthStable(int columnIndex) {
        // MaterializedRecord splits per column: a sort key it materialised reads from its own
        // buffer slot, every other column reads the live base record straight through. A key slot
        // is strided by the column's own width, so a 4-byte INT key must keep the default true -
        // delegating there would make getLong() take 8 bytes off it. A pass-through column carries
        // whatever the base projection carries, so an overflowing INT expression must widen on
        // store exactly as it does without the sort.
        return materializedColumns.get(columnIndex) || base.isColumnIntWidthStable(columnIndex);
    }

    @Override
    public boolean isColumnRowStable(int columnIndex) {
        // Paired with isColumnIntWidthStable above, through the same split. A materialised key has
        // been copied into its slot, and reading stored bytes twice gives the same value.
        return materializedColumns.get(columnIndex) || base.isColumnRowStable(columnIndex);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Materialize sort keys");
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    @Override
    protected void _close() {
        final RecordCursorFactory base = this.base;
        this.base = null;
        final SortKeyMaterializingRecordCursor cursor = this.cursor;
        this.cursor = null;
        Throwable failure = Misc.freeBestEffort(null, base);
        try {
            cursor.freeBuffers();
        } catch (Throwable th) {
            if (failure == null) {
                failure = th;
            } else if (failure != th) {
                failure.addSuppressed(th);
            }
        }
        CairoException.rethrowCleanupFailure(failure);
    }
}
