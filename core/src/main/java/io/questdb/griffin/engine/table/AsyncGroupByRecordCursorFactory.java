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

import io.questdb.MessageBus;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.map.Unordered2Map;
import io.questdb.cairo.map.Unordered4Map;
import io.questdb.cairo.map.Unordered8Map;
import io.questdb.cairo.map.UnorderedVarcharMap;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountLongConstGroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.aggr.Aggregator;
import io.questdb.griffin.engine.table.aggr.IntKeyAggregator;
import io.questdb.griffin.engine.table.aggr.LongKeyAggregator;
import io.questdb.griffin.engine.table.aggr.LongKeyCountAggregator;
import io.questdb.griffin.engine.table.aggr.ShortKeyAggregator;
import io.questdb.griffin.engine.table.aggr.VarcharKeyAggregator;
import io.questdb.griffin.engine.table.aggr.VarcharKeyCountAggregator;
import io.questdb.jit.CompiledFilter;
import io.questdb.mp.SCSequence;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;

public class AsyncGroupByRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final SCSequence collectSubSeq = new SCSequence();
    private final AsyncGroupByRecordCursor cursor;
    private final PageFrameSequence<AsyncGroupByAtom> frameSequence;
    private final ObjList<GroupByFunction> groupByFunctions;
    private final ObjList<Function> recordFunctions; // includes groupByFunctions
    private final int workerCount;

    public AsyncGroupByRecordCursorFactory(
            @Transient @NotNull BytecodeAssembler asm,
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull RecordCursorFactory base,
            @NotNull RecordMetadata groupByMetadata,
            @Transient @NotNull ListColumnFilter listColumnFilter,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            @NotNull ObjList<GroupByFunction> groupByFunctions,
            @Nullable ObjList<ObjList<GroupByFunction>> perWorkerGroupByFunctions,
            @NotNull ObjList<Function> keyFunctions,
            @Nullable ObjList<ObjList<Function>> perWorkerKeyFunctions,
            @NotNull ObjList<Function> recordFunctions,
            @Nullable CompiledFilter compiledFilter,
            @Nullable MemoryCARW bindVarMemory,
            @Nullable ObjList<Function> bindVarFunctions,
            @Nullable Function filter,
            @NotNull PageFrameReduceTaskFactory reduceTaskFactory,
            @Nullable ObjList<Function> perWorkerFilters,
            int workerCount
    ) {
        super(groupByMetadata);
        try {
            this.base = base;
            this.groupByFunctions = groupByFunctions;
            this.recordFunctions = recordFunctions;
            AsyncGroupByAtom atom = new AsyncGroupByAtom(
                    asm,
                    configuration,
                    base.getMetadata(),
                    keyTypes,
                    valueTypes,
                    listColumnFilter,
                    groupByFunctions,
                    perWorkerGroupByFunctions,
                    keyFunctions,
                    perWorkerKeyFunctions,
                    compiledFilter,
                    bindVarMemory,
                    bindVarFunctions,
                    filter,
                    perWorkerFilters,
                    workerCount
            );
            final Aggregator aggregator = createAggregator(listColumnFilter, keyTypes, groupByFunctions, atom);
            if (filter != null) {
                this.frameSequence = new PageFrameSequence<>(
                        configuration,
                        messageBus,
                        atom,
                        aggregator::filterAndAggregate,
                        reduceTaskFactory,
                        workerCount,
                        PageFrameReduceTask.TYPE_GROUP_BY
                );
            } else {
                this.frameSequence = new PageFrameSequence<>(
                        configuration,
                        messageBus,
                        atom,
                        aggregator::aggregate,
                        reduceTaskFactory,
                        workerCount,
                        PageFrameReduceTask.TYPE_GROUP_BY
                );
            }
            this.cursor = new AsyncGroupByRecordCursor(groupByFunctions, recordFunctions, messageBus);
            this.workerCount = workerCount;
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    @Override
    public PageFrameSequence<AsyncGroupByAtom> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
        return frameSequence.of(base, executionContext, collectSubSeq, order);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final int order = base.getScanDirection() == SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
        cursor.of(execute(executionContext, collectSubSeq, order), executionContext);
        return cursor;
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public boolean recordCursorSupportsLongTopK() {
        return true;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        if (usesCompiledFilter()) {
            sink.type("Async JIT Group By");
        } else {
            sink.type("Async Group By");
        }
        sink.meta("workers").val(workerCount);
        sink.optAttr("keys", GroupByRecordCursorFactory.getKeys(recordFunctions, getMetadata()));
        sink.optAttr("values", groupByFunctions, true);
        sink.optAttr("filter", frameSequence.getAtom(), true);
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return frameSequence.getAtom().getCompiledFilter() != null;
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    private Aggregator createAggregator(
            ListColumnFilter columnFilter,
            ArrayColumnTypes keyTypes,
            ObjList<GroupByFunction> groupByFunctions,
            AsyncGroupByAtom atom
    ) {
        if (
                atom.getMapClass() == Unordered2Map.class
                        && columnFilter.size() == 1
                        && keyTypes.getColumnCount() == 1
                        && keyTypes.getColumnType(0) == ColumnType.SHORT
        ) {
            final int index = (columnFilter.getColumnIndex(0) * columnFilter.getIndexFactor(0) - 1);
            return new ShortKeyAggregator(index);
        }
        if (
                atom.getMapClass() == Unordered4Map.class
                        && columnFilter.size() == 1
                        && keyTypes.getColumnCount() == 1
                        && keyTypes.getColumnType(0) == ColumnType.INT
        ) {
            final int index = (columnFilter.getColumnIndex(0) * columnFilter.getIndexFactor(0) - 1);
            // There is no IntKeyCountAggregator since such combination should be handled by the .vect factory.
            return new IntKeyAggregator(index);
        }
        if (
                atom.getMapClass() == Unordered8Map.class
                        && columnFilter.size() == 1
                        && keyTypes.getColumnCount() == 1
                        && keyTypes.getColumnType(0) == ColumnType.LONG
        ) {
            final int index = (columnFilter.getColumnIndex(0) * columnFilter.getIndexFactor(0) - 1);
            if (groupByFunctions.size() == 1 && groupByFunctions.getQuick(0) instanceof CountLongConstGroupByFunction) {
                return new LongKeyCountAggregator(index);
            }
            return new LongKeyAggregator(index);
        }
        if (
                atom.getMapClass() == UnorderedVarcharMap.class
                        && columnFilter.size() == 1
                        && keyTypes.getColumnCount() == 1
                        && keyTypes.getColumnType(0) == ColumnType.VARCHAR
        ) {
            final int index = (columnFilter.getColumnIndex(0) * columnFilter.getIndexFactor(0) - 1);
            if (groupByFunctions.size() == 1 && groupByFunctions.getQuick(0) instanceof CountLongConstGroupByFunction) {
                return new VarcharKeyCountAggregator(index);
            }
            return new VarcharKeyAggregator(index);
        }
        return GenericAggregator.INSTANCE;
    }

    @Override
    protected void _close() {
        Misc.free(base);
        Misc.free(cursor);
        Misc.free(frameSequence);
        Misc.freeObjList(recordFunctions); // groupByFunctions are included in recordFunctions
    }
}
