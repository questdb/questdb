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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Factory for parallel markout query execution.
 * <p>
 * This factory creates the infrastructure for parallelizing:
 * Master -> MarkoutHorizon (CROSS JOIN) -> ASOF JOIN -> GROUP BY
 */
public class AsyncMarkoutGroupByRecordCursorFactory extends AbstractRecordCursorFactory {
    private final AsyncMarkoutGroupByAtom atom;
    private final AsyncMarkoutGroupByRecordCursor cursor;
    private final RecordCursorFactory masterFactory;   // Base master table
    private final ObjList<RecordCursorFactory> pricesFactories;   // Per-slot prices factories for ASOF JOIN
    private final ObjList<Function> recordFunctions;   // Record functions (includes groupByFunctions)
    private final RecordCursorFactory sequenceFactory;  // Offset sequence

    public AsyncMarkoutGroupByRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull CairoEngine engine,
            @NotNull RecordMetadata metadata,
            @NotNull RecordCursorFactory masterFactory,
            @NotNull ObjList<RecordCursorFactory> pricesFactories,
            @NotNull RecordCursorFactory sequenceFactory,
            int masterTimestampColumnIndex,
            int sequenceColumnIndex,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            @NotNull ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            @NotNull ObjList<Function> recordFunctions,
            @NotNull ArrayColumnTypes keyTypes,
            @NotNull ArrayColumnTypes valueTypes,
            @Nullable ColumnTypes asofJoinKeyTypes,
            @Nullable RecordSink masterKeyCopier,
            @Nullable RecordSink pricesKeyCopier,
            int pricesTimestampIndex,
            int workerCount
    ) {
        super(metadata);

        this.masterFactory = masterFactory;
        this.pricesFactories = pricesFactories;
        this.recordFunctions = recordFunctions;
        this.sequenceFactory = sequenceFactory;

        BytecodeAssembler asm = new BytecodeAssembler();
        ObjList<RecordSink> perWorkerMapSinks;
        EntityColumnFilter sequenceColumnFilter = new EntityColumnFilter();
        EntityColumnFilter masterColumnFilter = new EntityColumnFilter();
        EntityColumnFilter keyColumnFilter = new EntityColumnFilter();

        try {
            // Create RecordSink for materializing sequence (offset) records
            sequenceColumnFilter.of(sequenceFactory.getMetadata().getColumnCount());
            RecordSink sequenceRecordSink = RecordSinkFactory.getInstance(
                    asm,
                    sequenceFactory.getMetadata(),
                    sequenceColumnFilter
            );

            // Create RecordSink for materializing master records
            masterColumnFilter.of(masterFactory.getMetadata().getColumnCount());
            RecordSink masterRecordSink = RecordSinkFactory.getInstance(
                    asm,
                    masterFactory.getMetadata(),
                    masterColumnFilter
            );

            // Create owner and per-worker map sinks for the aggregation map key
            keyColumnFilter.of(keyTypes.getColumnCount());
            RecordSink ownerMapSink = RecordSinkFactory.getInstance(asm, keyTypes, keyColumnFilter);
            perWorkerMapSinks = new ObjList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                perWorkerMapSinks.add(RecordSinkFactory.getInstance(asm, keyTypes, keyColumnFilter));
            }

            // Create the atom with all per-worker resources
            this.atom = new AsyncMarkoutGroupByAtom(
                    asm,
                    configuration,
                    pricesFactories,
                    sequenceFactory,
                    sequenceRecordSink,
                    masterRecordSink,
                    masterTimestampColumnIndex,
                    sequenceColumnIndex,
                    keyTypes,
                    valueTypes,
                    asofJoinKeyTypes,
                    masterKeyCopier,
                    pricesKeyCopier,
                    pricesTimestampIndex,
                    groupByFunctions,
                    perWorkerGroupByFunctions,
                    ownerMapSink,
                    perWorkerMapSinks,
                    workerCount
            );

            // Create the cursor
            this.cursor = new AsyncMarkoutGroupByRecordCursor(
                    configuration,
                    engine,
                    engine.getMessageBus(),
                    atom,
                    recordFunctions,
                    masterRecordSink,
                    masterFactory.getMetadata()
            );
        } catch (Throwable th) {
            Misc.free(masterFactory);
            Misc.freeObjList(pricesFactories);
            Misc.free(sequenceFactory);
            throw th;
        }
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor masterCursor = null;
        RecordCursor sequenceCursor = null;
        try {
            masterCursor = masterFactory.getCursor(executionContext);
            sequenceCursor = sequenceFactory.getCursor(executionContext);
            cursor.of(masterCursor, sequenceCursor, executionContext);
            return cursor;
        } catch (Throwable th) {
            Misc.free(masterCursor);
            Misc.free(sequenceCursor);
            throw th;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Async Markout GroupBy");
        sink.child(masterFactory);
        sink.child(sequenceFactory);
        if (pricesFactories.size() > 0) {
            sink.child(pricesFactories.getQuick(0));
        }
    }

    @Override
    protected void _close() {
        Misc.free(atom);
        Misc.free(cursor);
        Misc.free(masterFactory);
        Misc.freeObjList(pricesFactories);
        Misc.freeObjList(recordFunctions);  // groupByFunctions are included in recordFunctions
        Misc.free(sequenceFactory);
    }
}
