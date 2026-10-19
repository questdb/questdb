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

package io.questdb.griffin.engine.union;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

public class MergeUnionAllRecordCursorFactory extends AbstractSetRecordCursorFactory {
    private ObjList<ObjList<Function>> castFunctions;
    private MergeUnionAllRecordCursor mergeCursor;
    private ObjList<RecordCursorFactory> sourceFactories;
    private IntList sourcePositions;
    @Nullable
    private final IntList symbolUnionColumns;
    private final boolean isAscending;
    private final int timestampIndex;

    MergeUnionAllRecordCursorFactory(
            RecordMetadata metadata,
            ObjList<RecordCursorFactory> sourceFactories,
            IntList sourcePositions,
            ObjList<ObjList<Function>> castFunctions,
            int timestampIndex,
            boolean isAscending,
            @Nullable IntList symbolUnionColumns
    ) {
        super(metadata);
        this.castFunctions = castFunctions;
        this.isAscending = isAscending;
        this.sourceFactories = sourceFactories;
        this.sourcePositions = sourcePositions;
        this.symbolUnionColumns = symbolUnionColumns;
        this.timestampIndex = timestampIndex;
    }

    @Override
    public boolean followedOrderByAdvice() {
        return true;
    }

    @Override
    public boolean fragmentedSymbolTables() {
        return true;
    }

    @Override
    public String getBaseColumnName(int index) {
        return sourceFactories.getQuick(0).getMetadata().getColumnName(index);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        try {
            // The generator leaves a nested merge without a cursor so that an outer merge can take its
            // branches apart instead of stacking cursors. Only a merge that reaches the outer merge
            // directly gets taken apart though: an opaque wrapper - a computed projection, a LIMIT, a
            // window - retains the nested merge and executes it as an ordinary branch, and execution is
            // then the first point that needs the cursor. Create it here rather than expecting every
            // wrapper to prepare its base, which the wrappers this factory did not introduce do not do.
            createCursor();
            for (int i = 0, n = sourceFactories.size(); i < n; i++) {
                RecordCursor sourceCursor = sourceFactories.getQuick(i).getCursor(executionContext);
                try {
                    mergeCursor.openSource(i, sourceCursor);
                    sourceCursor = null;
                    Function.initNc(castFunctions.getQuick(i), mergeCursor.getSourceCursor(i), executionContext, null);
                } finally {
                    Misc.free(sourceCursor);
                }
            }
            mergeCursor.of(executionContext);
            return mergeCursor;
        } catch (Throwable th) {
            Misc.free(mergeCursor);
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        return isAscending ? SCAN_DIRECTION_FORWARD : SCAN_DIRECTION_BACKWARD;
    }

    @Nullable
    public IntList getSymbolUnionColumns() {
        return symbolUnionColumns;
    }

    @Override
    public boolean isNonDeterministic() {
        for (int i = 0, n = sourceFactories.size(); i < n; i++) {
            if (sourceFactories.getQuick(i).isNonDeterministic()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isStableWithinExecution() {
        for (int i = 0, n = sourceFactories.size(); i < n; i++) {
            if (!sourceFactories.getQuick(i).isStableWithinExecution()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Creates the merge cursor when the factory is known to be executable, which is what the generator
     * does the moment it stops holding the factory open for flattening. Idempotent, and closes the
     * factory on failure because the generator has not yet handed it to an owner that would free it.
     * {@link #getCursor(SqlExecutionContext)} creates the cursor on its own, so calling this is an
     * optimisation, not a precondition.
     */
    public void prepareCursor() {
        try {
            createCursor();
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type(getOperation());
        sink.attr("order").val('[');
        sink.putBaseColumnName(timestampIndex);
        sink.val(isAscending ? " asc]" : " desc]");
        sink.attr("branches").val(sourceFactories.size());
        for (int i = 0, n = sourceFactories.size(); i < n; i++) {
            sink.child(sourceFactories.getQuick(i));
        }
    }

    @Override
    public boolean usesExternalDataSource() {
        for (int i = 0, n = sourceFactories.size(); i < n; i++) {
            if (sourceFactories.getQuick(i).usesExternalDataSource()) {
                return true;
            }
        }
        return false;
    }

    MergeUnionAllRecordCursorFactoryBuilder.OperandState detachOperands() {
        final MergeUnionAllRecordCursorFactoryBuilder.OperandState state =
                new MergeUnionAllRecordCursorFactoryBuilder.OperandState(
                        sourceFactories,
                        sourcePositions,
                        castFunctions
                );
        sourceFactories = null;
        sourcePositions = null;
        castFunctions = null;
        mergeCursor = Misc.free(mergeCursor);
        return state;
    }

    @Override
    protected void _close() {
        final MergeUnionAllRecordCursor cursor = mergeCursor;
        mergeCursor = null;
        final ObjList<RecordCursorFactory> factories = sourceFactories;
        sourceFactories = null;
        final ObjList<ObjList<Function>> functions = castFunctions;
        castFunctions = null;
        sourcePositions = null;

        Throwable failure = Misc.freeBestEffort(null, cursor);
        failure = Misc.freeObjListBestEffort(failure, factories);
        if (functions != null) {
            for (int i = 0, n = functions.size(); i < n; i++) {
                failure = Misc.freeObjListBestEffort(failure, functions.getQuick(i));
            }
        }
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    protected CharSequence getOperation() {
        return "Union All Merge";
    }

    private void createCursor() {
        if (mergeCursor == null) {
            mergeCursor = new MergeUnionAllRecordCursor(castFunctions, timestampIndex, isAscending);
        }
    }
}
