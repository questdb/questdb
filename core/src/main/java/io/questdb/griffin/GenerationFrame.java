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

package io.questdb.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.WindowJoinStep;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjObjHashMap;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

/**
 * State of one {@link SqlCodeGenerator#generate} call. Function instantiation can compile a
 * sub-query, which re-enters generation before the outer call finishes, so the generator keeps
 * one frame per nesting depth and passes it explicitly to the operator generators, which keep
 * no state across a nested generation.
 */
final class GenerationFrame implements Closeable, Mutable {
    final IntList columnReferenceCounts = new IntList();
    final IntervalExtractor intervals;
    final LongList latestPrefixes = new LongList();
    final IntervalExtractor overrideIntervals;
    final ObjList<RecordCursorFactory> parallelFilterFactories = new ObjList<>();
    final ObjList<BoundExpression> parallelFilterPredicates = new ObjList<>();
    final ObjList<Function> patternArguments = new ObjList<>(2);
    final ObjList<BoundExpression> patternConjuncts = new ObjList<>();
    final IntList patternPositions = new IntList(2);
    final OutputSchema projectionScope = new OutputSchema();
    final GenericRecordMetadata projectionScopeMetadata = new GenericRecordMetadata();
    final ParquetPushdownExtractor pushdown = new ParquetPushdownExtractor();
    final ResourceScope resources = new ResourceScope();
    final ObjList<RecordCursorFactory> setOperationHeads = new ObjList<>();
    final ObjList<LogicalPlan> setOperationPlans = new ObjList<>();
    final IntList sharedConsumerCounts = new IntList();
    final ObjList<JoinInput> sharedConsumerSources = new ObjList<>();
    final IntList sharedConsumerTotals = new IntList();
    final ObjList<RecordCursorFactory> sharedFactories = new ObjList<>();
    final ObjList<JoinInput> sharedSources = new ObjList<>();
    final ObjectPool<SortPlan> sorts = new ObjectPool<>(SortPlan.FACTORY, 4);
    final IntList symbolKeySlots = new IntList();
    final SymbolKeyExtractor symbols = new SymbolKeyExtractor();
    final ObjObjHashMap<IntList, ObjList<WindowFunction>> windowGroups = new ObjObjHashMap<>();
    final ObjList<TableColumnMetadata> windowOutputColumns = new ObjList<>();
    final WindowFactoryGenerator.WindowPartitionKeys windowPartitionKeys = new WindowFactoryGenerator.WindowPartitionKeys();
    private final ObjList<TableColumnMetadata> projectionSlotColumns = new ObjList<>();
    BoundExpressionRewriter expressionRewriter;
    FunctionInstantiator functionInstantiator;
    TableFunctionSources functionSources;
    boolean isJoinIntervalCapture;
    boolean isJoinSlaveInput;
    boolean isPatternNegated;
    WindowJoinStep joinIntervalStep;
    RuntimeIntrinsicIntervalModel joinIntervals;
    BoundExpression latestWithin;
    int patternIndex;
    RecordCursorFactory sharedHeadFactory;
    int sharedHeadId;
    LogicalPlan sharedHeadTarget;

    GenerationFrame(CairoConfiguration configuration, StringSink tmpSink, LongList tmpLongs) {
        this.intervals = new IntervalExtractor(configuration, tmpSink, tmpLongs);
        this.overrideIntervals = new IntervalExtractor(configuration, tmpSink, tmpLongs);
    }

    @Override
    public void clear() {
        Throwable failure = Misc.clearBestEffort(null, resources);
        failure = Misc.clearBestEffort(failure, intervals);
        parallelFilterFactories.clear();
        parallelFilterPredicates.clear();
        isJoinIntervalCapture = false;
        isJoinSlaveInput = false;
        joinIntervals = null;
        joinIntervalStep = null;
        latestPrefixes.clear();
        latestWithin = null;
        symbols.clear();
        sorts.clear();
        columnReferenceCounts.clear();
        sharedConsumerCounts.clear();
        sharedConsumerSources.clear();
        sharedConsumerTotals.clear();
        sharedFactories.clear();
        sharedSources.clear();
        setOperationHeads.clear();
        setOperationPlans.clear();
        projectionScopeMetadata.clear();
        symbolKeySlots.clear();
        windowGroups.clear();
        windowOutputColumns.clear();
        windowPartitionKeys.clear();
        sharedHeadFactory = null;
        sharedHeadTarget = null;
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    public void close() {
        clear();
    }

    Throwable closePrepared(Throwable primary) {
        return functionSources.closePrepared(functionInstantiator.closePrepared(primary));
    }

    int getReferenceCount(int columnId) {
        return columnId < columnReferenceCounts.size() ? columnReferenceCounts.getQuick(columnId) : 0;
    }

    /**
     * The scope column a projection reserves for its output column at this index.
     */
    TableColumnMetadata projectionSlotColumn(int index, int type) {
        TableColumnMetadata column = projectionSlotColumns.getQuiet(index);
        if (column == null || column.getColumnType() != type) {
            column = new TableColumnMetadata("$projection" + index, type, IndexType.NONE, 0, false, null);
            projectionSlotColumns.extendAndSet(index, column);
        }
        return column;
    }

    void setReferenceCount(int columnId, int count) {
        while (columnReferenceCounts.size() <= columnId) {
            columnReferenceCounts.add(0);
        }
        columnReferenceCounts.setQuick(columnId, count);
    }
}
