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


package io.questdb.griffin.codegen;

import io.questdb.ParanoiaState;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.FullPartitionFrameCursorFactory;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IntervalPartitionFrameCursorFactory;
import io.questdb.cairo.ProjectableRecordCursorFactory;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableAccessInfo;
import io.questdb.cairo.sql.TableDescriptor;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.griffin.BoundExpressionRewriter;
import io.questdb.griffin.FunctionInstantiator;
import io.questdb.griffin.InstantiatedIntervalBounds;
import io.questdb.griffin.IntervalExtractor;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.PlanTable;
import io.questdb.griffin.PlanTables;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SymbolKeyExtractor;
import io.questdb.griffin.TableFunctionSources;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.griffin.engine.lv.LiveViewRecordCursorFactory;
import io.questdb.griffin.engine.table.AdaptiveSymbolPatternRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.griffin.engine.table.DeferredSingleSymbolFilterPageFrameRecordCursorFactory;
import io.questdb.griffin.engine.table.DeferredSymbolIndexFilteredRowCursorFactory;
import io.questdb.griffin.engine.table.DeferredSymbolIndexRowCursorFactory;
import io.questdb.griffin.engine.table.FilterOnExcludedValuesRecordCursorFactory;
import io.questdb.griffin.engine.table.FilterOnSubQueryRecordCursorFactory;
import io.questdb.griffin.engine.table.FilterOnValuesRecordCursorFactory;
import io.questdb.griffin.engine.table.FilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.LatestBySubQueryRecordCursorFactory;
import io.questdb.griffin.engine.table.PageFrameRecordCursorFactory;
import io.questdb.griffin.engine.table.PageFrameRowCursorFactory;
import io.questdb.griffin.engine.table.PostingIndexDistinctRecordCursorFactory;
import io.questdb.griffin.engine.table.PushdownFilterExtractor;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.griffin.engine.table.SortedSymbolIndexRecordCursorFactory;
import io.questdb.griffin.engine.table.SymbolPatternIndexRecordCursorFactory;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BindVariableExpression;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.FunctionSourcePlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PhysicalProperties;
import io.questdb.griffin.plan.logical.PlanToFactoryRules;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SortDirection;
import io.questdb.griffin.plan.logical.SortKeys;
import io.questdb.griffin.plan.logical.WindowJoinStep;
import io.questdb.std.Chars;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * Builds table scans, index scans, function sources and LATEST BY over a table: the access path
 * {@link io.questdb.griffin.optimiser.SqlOptimiser#planAccessPaths access path planning} recorded on
 * the {@link ScanPlan}, over the interval model and Parquet pushdown of the current
 * {@link GenerationFrame}.
 */
final class ScanFactoryGenerator {
    private final CairoConfiguration configuration;
    private final BoundExpressionRewriter expressionRewriter;
    private final FilterFactoryGenerator filterGenerator;
    private final FunctionInstantiator functionInstantiator;
    private final TableFunctionSources functionSources;
    private final InstantiatedIntervalBounds intervalBounds;
    private final LatestByFactoryGenerator latestByGenerator;
    private final PlanTables planTables;
    private final PageFrameReduceTaskFactory reduceTaskFactory;

    ScanFactoryGenerator(
            CairoConfiguration configuration,
            FilterFactoryGenerator filterGenerator,
            LatestByFactoryGenerator latestByGenerator,
            FunctionInstantiator functionInstantiator,
            BoundExpressionRewriter expressionRewriter,
            TableFunctionSources functionSources,
            PageFrameReduceTaskFactory reduceTaskFactory,
            PlanTables planTables
    ) {
        this.configuration = configuration;
        this.filterGenerator = filterGenerator;
        this.latestByGenerator = latestByGenerator;
        this.functionInstantiator = functionInstantiator;
        this.expressionRewriter = expressionRewriter;
        this.functionSources = functionSources;
        this.intervalBounds = new InstantiatedIntervalBounds(functionInstantiator);
        this.reduceTaskFactory = reduceTaskFactory;
        this.planTables = planTables;
    }

    /**
     * The plain single-key index scan a covering factory falls back to: the same plan this
     * method's caller builds when {@code /*+ no_covering *}{@code /} is set, minus the filter.
     * The filter stays with the wrapper above the covering factory, which applies it to
     * whichever of the two delegates runs, so putting it here too would both double-filter and
     * double-own the function.
     * <p>
     * The returned factory OWNS {@code dfcFactory} and {@code symbolFunc}: the covering factory
     * shares both with it rather than duplicating them, and frees them through this backup.
     */
    private static RecordCursorFactory buildSingleSymbolIndexScan(
            CairoConfiguration configuration,
            RecordMetadata queryMeta,
            PartitionFrameCursorFactory dfcFactory,
            int keyColumnIndex,
            Function symbolFunc,
            int indexDirection,
            boolean followsOrderByAdvice,
            IntList columnIndexes,
            IntList columnSizeShifts
    ) {
        return new DeferredSingleSymbolFilterPageFrameRecordCursorFactory(
                configuration,
                keyColumnIndex,
                symbolFunc,
                new DeferredSymbolIndexRowCursorFactory(keyColumnIndex, symbolFunc, indexDirection),
                queryMeta,
                dfcFactory,
                followsOrderByAdvice,
                columnIndexes,
                columnSizeShifts,
                true
        );
    }

    private static boolean countSymbols(BoundExpression expression, IntList keyIds, IntList counts) {
        if (!(expression instanceof FunctionExpression call)) {
            return true;
        }
        if (call.isAnd()) {
            return countSymbols(call.argumentAt(0), keyIds, counts) && countSymbols(call.argumentAt(1), keyIds, counts);
        }
        if (call.isOr()) {
            return false;
        }
        switch (call.getName()) {
            case "=" -> {
                for (int i = 0; i < 2; i++) {
                    final int key = keyPosition(call.argumentAt(i), keyIds);
                    if (key >= 0) {
                        final BoundExpression value = call.argumentAt(1 - i);
                        if (!(value instanceof ConstantExpression) && !(value instanceof BindVariableExpression)) {
                            return false;
                        }
                        counts.increment(key, 1);
                    }
                }
            }
            case "in" -> {
                if (keyPosition(call.argumentAt(0), keyIds) >= 0) {
                    for (int i = 1, n = call.getArgumentCount(); i < n; i++) {
                        if (call.argumentAt(i) instanceof CursorExpression) {
                            return false;
                        }
                    }
                    counts.increment(keyPosition(call.argumentAt(0), keyIds), call.getArgumentCount() - 1);
                }
            }
            case "!=", "<>" -> {
                return keyPosition(call.argumentAt(0), keyIds) < 0 && keyPosition(call.argumentAt(1), keyIds) < 0;
            }
            case "not" -> {
                return call.argumentAt(0) instanceof FunctionExpression negated && "in".equals(negated.getName())
                        && keyPosition(negated.argumentAt(0), keyIds) < 0;
            }
            default -> {
            }
        }
        return true;
    }

    private static int frameOrder(ScanPlan scan) {
        return scan.getScanDirection() == SortDirection.DESCENDING ? PartitionFrameCursorFactory.ORDER_DESC : PartitionFrameCursorFactory.ORDER_ASC;
    }

    private static int indexDirection(ScanPlan scan) {
        return scan.getIndexDirection() == SortDirection.DESCENDING ? IndexReader.DIR_BACKWARD : IndexReader.DIR_FORWARD;
    }

    private static int keyPosition(BoundExpression expression, IntList keyIds) {
        return expression instanceof ColumnExpression column && column.isDirectReference()
                ? keyIds.indexOf(column.getColumnId(), 0, keyIds.size()) : -1;
    }

    private static PartitionFrameCursorFactory newFrames(ScanPlan scan, RuntimeIntrinsicIntervalModel intervalModel,
                                                         PlanTable table, int order) {
        final TableDescriptor descriptor = table.getTableDescriptor();
        final PartitionFrameCursorFactory frames = intervalModel == null
                ? new FullPartitionFrameCursorFactory(scan.getTableToken(), scan.getMetadataVersion(), descriptor, order,
                scan.getViewName(), scan.getViewPosition(), scan.isUpdate())
                : new IntervalPartitionFrameCursorFactory(scan.getTableToken(), scan.getMetadataVersion(), intervalModel,
                descriptor, order, scan.getViewName(), scan.getViewPosition(), scan.isUpdate());
        try {
            frames.setAuthorizedColumnIndexes(scan.getAuthorizedColumnIndexes());
            frames.setRowCountRange(scan.getMinRowCount(), scan.getMaxRowCount());
        } catch (Throwable th) {
            Misc.free(frames, th);
            throw th;
        }
        return frames;
    }

    private static int requestedDirection(ScanPlan scan) {
        final SortKeys requestedOrder = scan.getRequestedOrder();
        return SqlCodeGenerator.queryModelDirection(requestedOrder.isEmpty() ? SortDirection.ASCENDING : requestedOrder.getDirections().getQuick(0));
    }

    private static Record.CharSequenceFunction subqueryKeyGetter(int type) {
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.STRING -> Record.GET_STR;
            case ColumnType.SYMBOL -> Record.GET_SYM;
            default -> Record.GET_VARCHAR;
        };
    }

    /**
     * Estimates how many values of each key a residual admits: equalities and IN lists over a key
     * count their values, and anything that can admit arbitrary values leaves every key unbounded.
     */
    private static IntList symbolCounts(BoundExpression residual, IntList keyIds) {
        final IntList counts = new IntList(keyIds.size());
        counts.setAll(keyIds.size(), 0);
        if (!countSymbols(residual, keyIds, counts)) {
            counts.setAll(keyIds.size(), Integer.MAX_VALUE);
        }
        for (int i = 0, n = counts.size(); i < n; i++) {
            if (counts.getQuick(i) == 0) {
                counts.setQuick(i, Integer.MAX_VALUE);
            }
        }
        return counts;
    }

    private RuntimeIntrinsicIntervalModel buildIntervals(IntervalExtractor scanIntervals, PlanTable table) {
        return scanIntervals == null ? null : scanIntervals.build(table.getPartitionedBy());
    }

    private void configurePushdown(GenerationFrame frame, PartitionFrameCursorFactory frames, BoundExpression residual, ScanPlan scan,
                                   RecordMetadata metadata, PlanTable table, SqlExecutionContext executionContext) throws SqlException {
        if (residual == null || !executionContext.isParquetRowGroupPruningEnabled()) {
            return;
        }
        final long partitionTableVersion = table.getPartitionTableVersion();
        if (!table.hasParquetPartitions()) {
            frames.setPushdownFilterCondition(partitionTableVersion, null);
            return;
        }
        final ObjList<PushdownFilterExtractor.PushdownFilterCondition> conditions = frame.pushdown.extract(residual, scan.getOutput(),
                metadata, null, metadata, functionInstantiator, executionContext);
        if (conditions != null) {
            frames.setPushdownFilterCondition(partitionTableVersion, conditions);
        }
    }

    private RecordCursorFactory filterScan(GenerationFrame frame, RecordCursorFactory base, ScanPlan scan, BoundExpression residual,
                                           @Nullable PreparedFilter stolenFilter, SqlExecutionContext executionContext) throws SqlException {
        final Function filter;
        try {
            filter = functionInstantiator.instantiate(residual, scan.getOutput(), base.getMetadata(), executionContext);
        } catch (Throwable th) {
            Misc.free(base, th);
            throw th;
        }
        final boolean isParallel = scan.getResidualAlgorithm() == FilterPlan.Algorithm.PARALLEL;
        if (stolenFilter != null) {
            stolenFilter.of(residual, scan.getOutput(), filter, isParallel && FilterFactoryGenerator.isJitApplicable(scan.isUpdate(), executionContext));
            return base;
        }
        return filterGenerator.generate(frame, residual, scan.getOutput(), base, filter, frame.producerMark(), executionContext,
                scan.isUpdate(), isParallel, scan.getFilterLimit(), scan.isResidualPreTouched());
    }

    private RecordCursorFactory generateIndexedScan(
            GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext, IntervalExtractor scanIntervals,
            GenericRecordMetadata metadata, PlanTable table, IntList indexes, IntList shifts, @Nullable PreparedFilter stolenFilter
    ) throws SqlException {
        final BoundExpression residual = scan.getResidual();
        final int keyIndex = scan.getOutput().getColumnIndexById(scan.getIndexColumnId());
        final boolean isExcluded = scan.getAccessPath() == ScanPlan.AccessPath.EXCLUDED_SYMBOL_INDEX;
        final ObjList<BoundExpression> values = isExcluded ? scan.getExcludedKeys() : scan.getIndexKeys();
        final int keyCount = values.size();
        final ObjList<Function> keys = new ObjList<>(keyCount);
        RuntimeIntrinsicIntervalModel intervalModel = buildIntervals(scanIntervals, table);
        Function filter = null;
        PartitionFrameCursorFactory frames = null;
        final int indexDirection = indexDirection(scan);
        final boolean isCovering = scan.getIndexRead() == ScanPlan.IndexRead.COVERING;
        int[] coveringMapping = null;
        boolean isIntervalModelTransferred = false;
        try {
            filter = residual == null ? null : functionInstantiator.instantiate(residual, scan.getOutput(), metadata, executionContext);
            if (ParanoiaState.PLAN_PARANOIA_MODE && filter != null && filter.isConstant()) {
                throw new AssertionError("index access path residual folds to a constant");
            }
            instantiateKeys(values, keys, scan, metadata, executionContext);
            if (!isExcluded && isCovering) {
                coveringMapping = buildCoveringMapping(table, indexes.getQuick(keyIndex), indexes);
            }
            isIntervalModelTransferred = true;
            frames = newFrames(scan, intervalModel, table, frameOrder(scan));
            configurePushdown(frame, frames, residual, scan, metadata, table, executionContext);
        } catch (Throwable th) {
            Misc.free(frames, th);
            Misc.freeObjList(keys, th);
            Misc.free(filter, th);
            if (!isIntervalModelTransferred) {
                Misc.free(intervalModel, th);
            }
            throw th;
        }
        if (isExcluded) {
            return new FilterOnExcludedValuesRecordCursorFactory(configuration, metadata, frames, keys,
                    keyIndex, filter, scan.getIndexOrder(), requestedDirection(scan),
                    indexDirection, indexes, shifts, configuration.getMaxSymbolNotEqualsCount());
        }
        final Function coveredFilter = coveringMapping == null ? null : filter;
        final RecordCursorFactory factory;
        try {
            if (keyCount == 1) {
                factory = generateSingleSymbolIndexScan(metadata, frames, keyIndex, keys.getQuick(0),
                        coveringMapping == null ? filter : null, indexDirection, scan.isRequestedOrderDelivered(),
                        indexes, shifts, coveringMapping, scan);
            } else {
                factory = generateSymbolValuesIndexScan(metadata, frames, keys, keyIndex,
                        coveringMapping == null ? filter : null, scan.getIndexOrder(), requestedDirection(scan), indexDirection,
                        indexes, shifts, coveringMapping, scan);
            }
        } catch (Throwable th) {
            Misc.free(coveredFilter, th);
            throw th;
        }
        if (coveredFilter == null) {
            return factory;
        }
        if (stolenFilter != null) {
            stolenFilter.of(residual, scan.getOutput(), coveredFilter, false);
            return factory;
        }
        return filterGenerator.generateCovering(frame, residual, scan.getOutput(), (CoveringIndexRecordCursorFactory) factory, coveredFilter,
                executionContext, scan.getResidualAlgorithm() == FilterPlan.Algorithm.PARALLEL,
                scan.getCoveredFilterLimit(), scan.isResidualPreTouched());
    }

    private RecordCursorFactory generateLatestByScan(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext,
                                                     IntervalExtractor scanIntervals, LatestByPlan latest, GenericRecordMetadata metadata,
                                                     PlanTable table, IntList indexes,
                                                     IntList shifts) throws SqlException {
        final BoundExpression residual = scan.getResidual();
        final CursorExpression keySubquery = scan.getKeySubquery();
        final ObjList<Function> keys = new ObjList<>();
        final ObjList<Function> excludedKeys = new ObjList<>();
        Function filter = null;
        PartitionFrameCursorFactory frames = null;
        RecordCursorFactory subquery = null;
        Record.CharSequenceFunction keyGetter = null;
        IntList symbolCounts = null;
        try {
            filter = residual == null ? null : functionInstantiator.instantiate(residual, scan.getOutput(), metadata, executionContext);
            if (filter != null && scan.getIndexRead() == ScanPlan.IndexRead.NONE) {
                filter = filterGenerator.compileLatestBy(frame, filter, residual, scan.getOutput(), metadata, table, indexes,
                        scan.isUpdate(), executionContext);
            }
            instantiateKeys(scan.getIndexKeys(), keys, scan, metadata, executionContext);
            instantiateKeys(scan.getExcludedKeys(), excludedKeys, scan, metadata, executionContext);
            frames = newFrames(scan, buildIntervals(scanIntervals, table), table, frameOrder(scan));
            if (residual != null && (residual.getFunctionFlags() & (BoundExpression.CONSTANT | BoundExpression.RUNTIME_CONSTANT)) == 0) {
                configurePushdown(frame, frames, residual, scan, metadata, table, executionContext);
            }
            if (keySubquery != null) {
                subquery = functionInstantiator.generateSubquery(keySubquery, executionContext);
                keyGetter = subqueryKeyGetter(subquery.getMetadata().getColumnType(0));
            } else if (residual != null) {
                symbolCounts = symbolCounts(residual, latest.getKeyColumnIds());
            }
        } catch (Throwable th) {
            Misc.free(subquery, th);
            Misc.free(frames, th);
            Misc.freeObjList(excludedKeys, th);
            Misc.freeObjList(keys, th);
            Misc.free(filter, th);
            throw th;
        }
        if (keySubquery != null) {
            return new LatestBySubQueryRecordCursorFactory(configuration, metadata, frames,
                    scan.getOutput().getColumnIndexById(latest.getKeyColumnIds().getQuick(0)), subquery, filter,
                    scan.getIndexRead() != ScanPlan.IndexRead.NONE, keyGetter, indexes, shifts);
        }
        return latestByGenerator.generateLatestByScan(frame, scan, frames, metadata, table, indexes, shifts, latest.getKeyColumnIds(),
                filter, keys, excludedKeys, symbolCounts, executionContext);
    }

    private RecordCursorFactory generatePageFrameScan(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext,
                                                      IntervalExtractor scanIntervals, GenericRecordMetadata metadata,
                                                      PlanTable table, IntList indexes, IntList shifts,
                                                      @Nullable PreparedFilter stolenFilter) throws SqlException {
        final BoundExpression residual = scan.getTableToken().isLiveView() ? null : scan.getResidual();
        final int order = frameOrder(scan);
        final PartitionFrameCursorFactory frames = newFrames(scan, buildIntervals(scanIntervals, table), table, order);
        try {
            configurePushdown(frame, frames, residual, scan, metadata, table, executionContext);
        } catch (Throwable th) {
            Misc.free(frames, th);
            throw th;
        }
        final RecordCursorFactory factory = generateScan(frames, metadata, order, PlanToFactoryRules.isPageFrameAdviceFollowed(scan),
                indexes, shifts, scan.isRandomAccess());
        if (residual == null) {
            return factory;
        }
        return filterScan(frame, factory, scan, residual, stolenFilter, executionContext);
    }

    private RecordCursorFactory generatePostingDistinctScan(AggregatePlan aggregate, ScanPlan scan, IntervalExtractor scanIntervals,
                                                            PlanTable table) {
        final ColumnExpression key = (ColumnExpression) aggregate.getGroupingExpressions().getQuick(0);
        final int index = scan.getSourceColumnIndexes().getQuick(scan.getOutput().getColumnIndexById(key.getColumnId()));
        final GenericRecordMetadata metadata = new GenericRecordMetadata()
                .add(table.newColumnMetadata(index, Chars.toString(aggregate.getOutput().getColumnName(0))));
        final IntList indexes = new IntList();
        indexes.add(index);
        if (scanIntervals != null && table.getTimestampIndex() != index) {
            indexes.add(table.getTimestampIndex());
        }
        final PartitionFrameCursorFactory frames = newFrames(scan, buildIntervals(scanIntervals, table), table,
                PartitionFrameCursorFactory.ORDER_ASC);
        return new PostingIndexDistinctRecordCursorFactory(metadata, frames, index, 0, indexes);
    }

    private RecordCursorFactory generateReaderScan(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext,
                                                   IntervalExtractor scanIntervals, LatestByPlan latest, PlanTable table,
                                                   @Nullable PreparedFilter stolenFilter) throws SqlException {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        final IntList indexes = new IntList(scan.getOutput().getColumnCount());
        final IntList shifts = new IntList(scan.getOutput().getColumnCount());
        for (int i = 0, n = scan.getOutput().getColumnCount(); i < n; i++) {
            final int sourceIndex = scan.getSourceColumnIndexes().getQuick(i);
            indexes.add(sourceIndex);
            metadata.add(table.newColumnMetadata(sourceIndex, table.getColumnName(sourceIndex)));
            shifts.add(Numbers.msb(ColumnType.sizeOf(table.getColumnType(sourceIndex))));
        }
        metadata.setTimestampIndex(PhysicalProperties.timestampIndex(scan));
        final ScanPlan.AccessPath accessPath = scan.getAccessPath();
        if (accessPath == null) {
            throw new IllegalStateException("scan access path is not planned");
        }
        return switch (accessPath) {
            case EMPTY -> new EmptyTableRecordCursorFactory(metadata);
            case PAGE_FRAMES -> generatePageFrameScan(frame, scan, executionContext, scanIntervals, metadata, table,
                    indexes, shifts, stolenFilter);
            case SORTED_SYMBOL_INDEX -> {
                final PartitionFrameCursorFactory frames = newFrames(scan, buildIntervals(scanIntervals, table), table, frameOrder(scan));
                yield new SortedSymbolIndexRecordCursorFactory(configuration, metadata, frames,
                        scan.getOutput().getColumnIndexById(scan.getIndexColumnId()),
                        scan.getRequestedOrder().getDirections().getQuick(0) == SortDirection.ASCENDING,
                        indexDirection(scan), indexes, shifts);
            }
            case SYMBOL_INDEX, EXCLUDED_SYMBOL_INDEX ->
                    generateIndexedScan(frame, scan, executionContext, scanIntervals, metadata,
                            table, indexes, shifts, stolenFilter);
            case SYMBOL_SUBQUERY -> generateSubqueryScan(frame, scan, executionContext, scanIntervals, metadata, table,
                    indexes, shifts);
            case SYMBOL_PATTERN -> generateSymbolPatternIndex(frame, scan, executionContext, scanIntervals, metadata,
                    table, indexes, shifts, stolenFilter);
            case LATEST_BY_SUBQUERY, LATEST_BY_VALUE, LATEST_BY_VALUES, LATEST_BY_ALL_INDEXED, LATEST_BY_STATIC_SYMBOL,
                 LATEST_BY_SYMBOLS,
                 LATEST_BY_ALL -> generateLatestByScan(frame, scan, executionContext, scanIntervals, latest, metadata,
                    table, indexes, shifts);
            case UPDATE_STUB, POSTING_DISTINCT ->
                    throw new IllegalStateException("access path does not read the table through a reader scan");
        };
    }

    private RecordCursorFactory generateScan(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext, IntervalExtractor scanIntervals,
                                             LatestByPlan latest, @Nullable PreparedFilter stolenFilter) throws SqlException {
        final RecordCursorFactory factory = generateTableScan(frame, scan, executionContext, scanIntervals, latest, stolenFilter);
        if (!scan.getTableToken().isLiveView() || scan.isUpdate()) {
            return factory;
        }
        // The live-view wrapper pins the in-memory tier and routes rows by seam timestamp.
        return new LiveViewRecordCursorFactory(executionContext.getCairoEngine(), scan.getTableToken(), factory);
    }

    private RecordCursorFactory generateSubqueryScan(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext,
                                                     IntervalExtractor scanIntervals, GenericRecordMetadata metadata,
                                                     PlanTable table,
                                                     IntList indexes, IntList shifts) throws SqlException {
        final BoundExpression residual = scan.getResidual();
        PartitionFrameCursorFactory frames = null;
        Function filter = null;
        RecordCursorFactory subquery = null;
        final Record.CharSequenceFunction keyGetter;
        try {
            frames = newFrames(scan, buildIntervals(scanIntervals, table), table, frameOrder(scan));
            configurePushdown(frame, frames, residual, scan, metadata, table, executionContext);
            filter = residual == null ? null : functionInstantiator.instantiate(residual, scan.getOutput(), metadata, executionContext);
            subquery = functionInstantiator.generateSubquery(scan.getKeySubquery(), executionContext);
            keyGetter = subqueryKeyGetter(subquery.getMetadata().getColumnType(0));
        } catch (Throwable th) {
            Misc.free(subquery, th);
            Misc.free(filter, th);
            Misc.free(frames, th);
            throw th;
        }
        return new FilterOnSubQueryRecordCursorFactory(configuration, metadata, frames, subquery,
                scan.getOutput().getColumnIndexById(scan.getIndexColumnId()), filter, keyGetter, indexes, shifts);
    }

    private RecordCursorFactory generateSymbolPatternIndex(
            GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext, IntervalExtractor scanIntervals,
            GenericRecordMetadata metadata, PlanTable table, IntList indexes, IntList shifts, @Nullable PreparedFilter stolenFilter
    ) throws SqlException {
        final BoundExpression predicate = scan.getResidual();
        final FunctionExpression pattern = scan.getKeyPattern();
        final OutputSchema input = scan.getOutput();
        final boolean isCovering = scan.getIndexRead() == ScanPlan.IndexRead.COVERING;
        frame.expressions.clear();
        LogicalPlans.collectConjuncts(predicate, frame.expressions);
        BoundExpression residual = null;
        for (int i = 0, n = frame.expressions.size(); i < n; i++) {
            final BoundExpression conjunct = frame.expressions.getQuick(i);
            if (conjunct != pattern) {
                residual = expressionRewriter.combineConjunction(conjunct, residual, 0);
            }
        }
        if ("!~".equals(pattern.getName())) {
            Misc.free(functionInstantiator.instantiate(pattern, input, metadata, executionContext));
        }
        final AdaptiveSymbolPatternRecordCursorFactory.PreparedSymbolPatternFilter filter =
                filterGenerator.instantiatePatternFilter(frame, pattern, residual, input, metadata, executionContext);
        if (filter == null) {
            throw new AssertionError("symbol pattern provider declaration differs from the instantiated function");
        }
        ObjList<Function> workerFilters = null;
        Function limit = null;
        final BoundExpression limitCount = isCovering ? scan.getCoveredFilterLimit() : scan.getFilterLimit();
        final boolean isParallel = scan.getResidualAlgorithm() == FilterPlan.Algorithm.PARALLEL && stolenFilter == null;
        boolean isTransferred = false;
        try {
            if (isParallel) {
                workerFilters = filterGenerator.instantiatePatternWorkers(frame, pattern, residual, input, metadata, filter, executionContext);
                if (limitCount != null) {
                    limit = filterGenerator.instantiateLimit(frame, limitCount, input, executionContext);
                }
            }
            final PartitionFrameCursorFactory frames = newFrames(scan, buildIntervals(scanIntervals, table), table, frameOrder(scan));
            try {
                configurePushdown(frame, frames, predicate, scan, metadata, table, executionContext);
            } catch (Throwable th) {
                Misc.free(frames, th);
                throw th;
            }
            isTransferred = true;
            return generateSymbolPatternIndex(frames, metadata, table, indexes, shifts, filter, predicate, pattern, residual, input,
                    workerFilters, limit, limitCount == null ? 0 : limitCount.getPosition(), scan.getIndexOrder(), isCovering,
                    isParallel, scan.isResidualPreTouched(), stolenFilter, executionContext);
        } catch (Throwable th) {
            if (!isTransferred) {
                Misc.free(filter, th);
                Misc.freeObjList(workerFilters, th);
                Misc.free(limit, th);
            }
            throw th;
        }
    }

    private RecordCursorFactory generateTableScan(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext, IntervalExtractor scanIntervals,
                                                  LatestByPlan latest, @Nullable PreparedFilter stolenFilter) throws SqlException {
        if (scan.getAccessPath() == ScanPlan.AccessPath.UPDATE_STUB) {
            return generateUpdateStub(scan, executionContext);
        }
        final boolean isOverridden = executionContext.isOverriddenIntrinsics(scan.getTableToken());
        final WindowJoinStep step = scan.getJoinIntervalStep();
        if (!isOverridden && step == null) {
            return generateReaderScan(frame, scan, executionContext, scanIntervals, latest, planTables.of(scan), stolenFilter);
        }
        final int timestampType = scan.getNativeTimestampType();
        final IntervalExtractor rangeIntervals = frame.intervals;
        final RecordCursorFactory factory;
        try {
            if (scanIntervals == null) {
                rangeIntervals.of(timestampType);
            }
            if (isOverridden) {
                rangeIntervals.override(scan.getTableToken(), executionContext);
            }
            if (step != null) {
                final long hi = step.getIntervalHi(timestampType);
                final long lo = step.getIntervalLo(timestampType);
                rangeIntervals.merge(scan.getJoinIntervals(), lo, hi);
            }
            factory = generateReaderScan(frame, scan, executionContext, rangeIntervals, latest, planTables.of(scan), stolenFilter);
        } catch (Throwable th) {
            Misc.clear(rangeIntervals, th);
            throw th;
        }
        return SqlCodeGenerator.clearAfter(rangeIntervals, factory);
    }

    /**
     * Builds the scan of a sequenced UPDATE, which reads no rows and takes no reader. The UPDATE target's columns carry
     * the sequencer metadata the statement validates against, which the table's reader may not have caught up with;
     * the columns of any other scan carry its table's metadata.
     */
    private RecordCursorFactory generateUpdateStub(ScanPlan scan, SqlExecutionContext executionContext) throws SqlException {
        final OutputSchema output = scan.getOutput();
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        if (!scan.isUpdate()) {
            final PlanTable table = planTables.of(scan);
            for (int i = 0, n = output.getColumnCount(); i < n; i++) {
                final int index = scan.getSourceColumnIndexes().getQuick(i);
                metadata.add(table.newColumnMetadata(index, table.getColumnName(index)));
            }
            metadata.setTimestampIndex(PhysicalProperties.timestampIndex(scan));
            return new EmptyTableRecordCursorFactory(metadata);
        }
        final TableRecordMetadata tableMetadata = executionContext.getMetadataForWrite(scan.getTableToken(), scan.getMetadataVersion());
        final RecordCursorFactory factory;
        try {
            for (int i = 0, n = output.getColumnCount(); i < n; i++) {
                final int index = tableMetadata.getColumnIndex(output.getColumnName(i));
                metadata.add(TableColumnMetadata.copyOf(tableMetadata, index, tableMetadata.getColumnName(index)));
            }
            metadata.setTimestampIndex(PhysicalProperties.timestampIndex(scan));
            factory = new EmptyTableRecordCursorFactory(metadata, tableMetadata.getTableToken());
        } catch (Throwable th) {
            Misc.free(tableMetadata, th);
            throw th;
        }
        return SqlCodeGenerator.closeAfter(tableMetadata, factory);
    }

    private void instantiateKeys(ObjList<BoundExpression> values, ObjList<Function> keys, ScanPlan scan,
                                 RecordMetadata metadata, SqlExecutionContext executionContext) throws SqlException {
        keys.checkCapacity(keys.size() + values.size());
        for (int i = 0, n = values.size(); i < n; i++) {
            keys.add(SymbolKeyExtractor.instantiateValue(values.getQuick(i), scan.getOutput(), metadata, functionInstantiator, executionContext));
        }
    }

    /**
     * Maps each column a scan reads, by its position in {@code columnIndexes}, to its position among the columns the
     * covering index of the key column includes, or -1 for the key column.
     */
    static int[] buildCoveringMapping(TableAccessInfo table, int keyColumnIndex, IntList columnIndexes) {
        final int[] mapping = new int[columnIndexes.size()];
        for (int i = 0, n = columnIndexes.size(); i < n; i++) {
            final int columnIndex = columnIndexes.getQuick(i);
            mapping[i] = columnIndex == keyColumnIndex ? -1 : table.getCoveredPosition(keyColumnIndex, columnIndex);
        }
        return mapping;
    }

    void configurePushdown(GenerationFrame frame, FunctionSourcePlan source, RecordCursorFactory base, BoundExpression residual,
                           SqlExecutionContext executionContext) throws SqlException {
        final RecordCursorFactory target = base instanceof SelectedRecordCursorFactory ? base.getBaseFactory() : base;
        if (target.mayHaveParquetPartitions(executionContext) && executionContext.isParquetRowGroupPruningEnabled()) {
            target.setPushdownFilterCondition(frame.pushdown.extract(residual, source.getOutput(), base.getMetadata(),
                    target == base ? null : source.getSourceColumnIndexes(), target.getMetadata(), functionInstantiator, executionContext));
        }
    }

    /**
     * Builds the scan of a filter fused into it. With a {@code stolenFilter}, builds the scan without the filter, which
     * a parallel consumer steals, and prepares the filter over it there.
     */
    RecordCursorFactory generateFiltered(GenerationFrame frame, ScanPlan scan, BoundExpression predicate, @Nullable PreparedFilter stolenFilter,
                                         SqlExecutionContext executionContext) throws SqlException {
        final int timestampIndex = scan.getOutput().getColumnIndexById(scan.getNativeTimestampColumnId());
        final RecordCursorFactory factory;
        try {
            if (timestampIndex >= 0) {
                frame.intervals.extract(predicate, scan.getOutput().getColumnId(timestampIndex), scan.getOutput(), intervalBounds,
                        expressionRewriter, scan.getDepth(), executionContext);
            }
            final RecordCursorFactory scanFactory = generateScan(frame, scan, executionContext, frame.intervals, null, stolenFilter);
            final BoundExpression residual = scan.getResidual();
            // Rows the live view serves from memory bypass the wrapped scan, so only intervals go below it.
            factory = scan.getTableToken().isLiveView() && residual != null
                    ? filterScan(frame, scanFactory, scan, residual, stolenFilter, executionContext) : scanFactory;
        } catch (Throwable th) {
            Misc.clear(frame.intervals, th);
            throw th;
        }
        return SqlCodeGenerator.clearAfter(frame.intervals, factory);
    }

    RecordCursorFactory generateFunctionSource(FunctionSourcePlan plan, SqlExecutionContext executionContext) throws SqlException {
        final RecordCursorFactory base = functionSources.takeFactory(plan, executionContext);
        final GenericRecordMetadata metadata;
        final IntList mapping;
        try {
            final RecordMetadata baseMetadata = base.getMetadata();
            final IntList sourceIndexes = plan.getSourceColumnIndexes();
            final int timestampIndex = PhysicalProperties.timestampIndex(plan);
            boolean isIdentity = sourceIndexes.size() == baseMetadata.getColumnCount() && timestampIndex == baseMetadata.getTimestampIndex();
            for (int i = 0, n = sourceIndexes.size(); i < n; i++) {
                isIdentity &= sourceIndexes.getQuick(i) == i;
            }
            if (isIdentity) {
                return base;
            }
            metadata = new GenericRecordMetadata();
            mapping = new IntList(sourceIndexes);
            for (int i = 0, n = mapping.size(); i < n; i++) {
                metadata.add(baseMetadata.getColumnMetadata(mapping.getQuick(i)));
            }
            metadata.setTimestampIndex(timestampIndex);
            if (base instanceof ProjectableRecordCursorFactory projectable) {
                projectable.setQueryProjectedMetadata(metadata);
                return base;
            }
        } catch (Throwable th) {
            Misc.free(base, th);
            throw th;
        }
        return new SelectedRecordCursorFactory(metadata, mapping, base);
    }

    RecordCursorFactory generateLatestBy(GenerationFrame frame, LatestByPlan latest, ScanPlan scan, SqlExecutionContext executionContext) throws SqlException {
        if (!(latest.getChild() instanceof FilterPlan filter)) {
            return generateScan(frame, scan, executionContext, null, latest, null);
        }
        final RecordCursorFactory factory;
        try {
            if (scan.getOutput().getColumnIndexById(scan.getNativeTimestampColumnId()) >= 0) {
                frame.intervals.extract(filter.getPredicate(), scan.getNativeTimestampColumnId(), scan.getOutput(), intervalBounds,
                        expressionRewriter, scan.getDepth(), executionContext);
            }
            factory = generateScan(frame, scan, executionContext, frame.intervals, latest, null);
        } catch (Throwable th) {
            Misc.clear(frame.intervals, th);
            throw th;
        }
        return SqlCodeGenerator.clearAfter(frame.intervals, factory);
    }

    /**
     * Builds the DISTINCT of the aggregate's single key from the posting index its scan reads, see
     * {@link PlanToFactoryRules#postingDistinctScan}.
     */
    RecordCursorFactory generatePostingDistinct(GenerationFrame frame, AggregatePlan aggregate, ScanPlan scan, SqlExecutionContext executionContext)
            throws SqlException {
        final BoundExpression predicate = aggregate.getChild() instanceof FilterPlan filter ? filter.getPredicate() : null;
        final RecordCursorFactory factory;
        try {
            if (predicate != null) {
                frame.intervals.extract(predicate, scan.getNativeTimestampColumnId(), scan.getOutput(), intervalBounds,
                        expressionRewriter, scan.getDepth(), executionContext);
            }
            factory = generatePostingDistinctScan(aggregate, scan, predicate == null ? null : frame.intervals, planTables.of(scan));
        } catch (Throwable th) {
            Misc.clear(frame.intervals, th);
            throw th;
        }
        return SqlCodeGenerator.clearAfter(frame.intervals, factory);
    }

    RecordCursorFactory generateScan(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext) throws SqlException {
        return generateScan(frame, scan, executionContext, null, null, null);
    }

    /**
     * Consumes the frames on entry, including on failure.
     */
    RecordCursorFactory generateScan(
            PartitionFrameCursorFactory frames,
            RecordMetadata queryMetadata,
            int order,
            boolean isFollowingOrderByAdvice,
            IntList columnIndexes,
            IntList columnSizeShifts,
            boolean isRandomAccessSupported
    ) {
        return new PageFrameRecordCursorFactory(configuration, queryMetadata, frames, new PageFrameRowCursorFactory(order),
                isFollowingOrderByAdvice, null, true, columnIndexes, columnSizeShifts, isRandomAccessSupported, false);
    }

    /**
     * Consumes frames, key and filter on entry, including on failure; covering scans leave filtering to their caller.
     */
    RecordCursorFactory generateSingleSymbolIndexScan(
            RecordMetadata metadata,
            PartitionFrameCursorFactory frames,
            int keyIndex,
            Function key,
            @Nullable Function filter,
            int indexDirection,
            boolean followsOrderByAdvice,
            IntList columnIndexes,
            IntList columnSizeShifts,
            int @Nullable [] coveringMapping,
            ScanPlan scan
    ) {
        if (coveringMapping != null) {
            assert filter == null;
            final RecordCursorFactory backup = scan.hasCoveringBackup()
                    ? buildSingleSymbolIndexScan(configuration, metadata, frames, keyIndex,
                    key, indexDirection, followsOrderByAdvice, columnIndexes, columnSizeShifts)
                    : null;
            return new CoveringIndexRecordCursorFactory(metadata, frames, columnIndexes.getQuick(keyIndex),
                    key, columnIndexes, coveringMapping, null, false, null, null,
                    backup, scan.isCoveringBackupSuppressed());
        }
        if (filter == null) {
            return buildSingleSymbolIndexScan(configuration, metadata, frames, keyIndex,
                    key, indexDirection, followsOrderByAdvice, columnIndexes, columnSizeShifts);
        }
        return new PageFrameRecordCursorFactory(configuration, metadata, frames,
                new DeferredSymbolIndexFilteredRowCursorFactory(keyIndex, key, filter, indexDirection), followsOrderByAdvice,
                filter, false, columnIndexes, columnSizeShifts, true, false);
    }

    /**
     * Consumes the frames, the prepared filter, the worker filters and the limit on entry, including on failure. With
     * a {@code stolenFilter}, builds the factory a parallel consumer reads the frames of, the page-frame scan or the
     * covering adaptive one, and prepares the filter over it there; otherwise filters in parallel when
     * {@code isParallel}.
     */
    RecordCursorFactory generateSymbolPatternIndex(
            PartitionFrameCursorFactory frames,
            GenericRecordMetadata metadata,
            TableAccessInfo table,
            IntList columnIndexes,
            IntList columnSizeShifts,
            AdaptiveSymbolPatternRecordCursorFactory.PreparedSymbolPatternFilter filter,
            BoundExpression predicate,
            FunctionExpression pattern,
            @Nullable BoundExpression residual,
            OutputSchema input,
            @Nullable ObjList<Function> workerFilters,
            @Nullable Function limit,
            int limitPosition,
            ScanPlan.IndexOrder indexOrder,
            boolean isCovering,
            boolean isParallel,
            boolean isPreTouchEnabled,
            @Nullable PreparedFilter stolenFilter,
            SqlExecutionContext executionContext
    ) {
        if (stolenFilter != null && !isCovering) {
            final AdaptiveSymbolPatternRecordCursorFactory.NonOwningPartitionFrameCursorFactory stolenFrames =
                    new AdaptiveSymbolPatternRecordCursorFactory.NonOwningPartitionFrameCursorFactory(frames);
            stolenFrames.ofStolenFilter(filter, columnIndexes);
            final RecordCursorFactory leaf;
            try {
                leaf = new PageFrameRecordCursorFactory(configuration, metadata, stolenFrames,
                        new PageFrameRowCursorFactory(frames.getOrder()), false, null, true, columnIndexes, columnSizeShifts, true, false);
            } catch (Throwable th) {
                Misc.free(stolenFrames, th);
                Misc.free(filter, th);
                throw th;
            }
            stolenFilter.ofPattern(predicate, input, filter, pattern, residual);
            return leaf;
        }
        final IntHashSet filterColumnIndexes = new IntHashSet();
        FilterFactoryGenerator.collectColumnIndexes(predicate, input, filterColumnIndexes);
        final int keyColumnIndex = filter.getSymbolColumnIndex();
        final boolean isNegated = filter.isNegated();
        final IntList effectiveKeys = new IntList();
        final AdaptiveSymbolPatternRecordCursorFactory.NonOwningPartitionFrameCursorFactory sharedFrames =
                new AdaptiveSymbolPatternRecordCursorFactory.NonOwningPartitionFrameCursorFactory(frames);
        RecordCursorFactory coveringDelegate = null;
        RecordCursorFactory indexDelegate = null;
        RecordCursorFactory scanDelegate;
        boolean isSelfFiltering = false;
        try {
            indexDelegate = new SymbolPatternIndexRecordCursorFactory(configuration, metadata, sharedFrames, keyColumnIndex,
                    effectiveKeys, indexOrder, IndexReader.DIR_FORWARD, columnIndexes, columnSizeShifts);
            if (isCovering) {
                final int[] coveringMapping = buildCoveringMapping(table, columnIndexes.getQuick(keyColumnIndex), columnIndexes);
                coveringDelegate = new CoveringIndexRecordCursorFactory(metadata, sharedFrames, columnIndexes.getQuick(keyColumnIndex),
                        null, columnIndexes, coveringMapping, null, false, null, effectiveKeys, null, false);
            }
            scanDelegate = new PageFrameRecordCursorFactory(configuration, metadata, sharedFrames,
                    new PageFrameRowCursorFactory(frames.getOrder()), false, null, true, columnIndexes, columnSizeShifts, true, false);
            if (coveringDelegate == null && isParallel) {
                isSelfFiltering = true;
                scanDelegate = new AsyncFilteredRecordCursorFactory(executionContext.getCairoEngine(), configuration,
                        executionContext.getMessageBus(), scanDelegate, filter, filterColumnIndexes, reduceTaskFactory, workerFilters,
                        limit, limitPosition, executionContext.getSharedQueryWorkerCount(), isPreTouchEnabled);
            }
        } catch (Throwable th) {
            Misc.free(coveringDelegate, th);
            Misc.free(indexDelegate, th);
            Misc.free(frames, th);
            if (!isSelfFiltering) {
                Misc.freeObjList(workerFilters, th);
                Misc.free(limit, th);
                Misc.free(filter, th);
            }
            throw th;
        }
        final AdaptiveSymbolPatternRecordCursorFactory adaptive;
        try {
            adaptive = new AdaptiveSymbolPatternRecordCursorFactory(metadata, frames, sharedFrames, columnIndexes, effectiveKeys,
                    columnIndexes.getQuick(keyColumnIndex), isNegated, configuration.getSymbolPatternIndexThreshold(), filter,
                    isSelfFiltering, indexDelegate, coveringDelegate, scanDelegate);
        } catch (Throwable th) {
            if (!isSelfFiltering) {
                Misc.freeObjList(workerFilters, th);
                Misc.free(limit, th);
                Misc.free(filter, th);
            }
            throw th;
        }
        if (isSelfFiltering) {
            return adaptive;
        }
        if (stolenFilter != null) {
            stolenFilter.ofPattern(predicate, input, filter, pattern, residual);
            return adaptive;
        }
        if (!isParallel) {
            return new FilteredRecordCursorFactory(adaptive, filter);
        }
        return new AsyncFilteredRecordCursorFactory(executionContext.getCairoEngine(), configuration, executionContext.getMessageBus(),
                adaptive, filter, filterColumnIndexes, reduceTaskFactory, workerFilters, limit, limitPosition,
                executionContext.getSharedQueryWorkerCount(), isPreTouchEnabled);
    }

    /**
     * Consumes frames, key functions and filter, including on failure; the key list is borrowed.
     */
    RecordCursorFactory generateSymbolValuesIndexScan(
            RecordMetadata metadata,
            PartitionFrameCursorFactory frames,
            ObjList<Function> keys,
            int keyIndex,
            @Nullable Function filter,
            ScanPlan.IndexOrder indexOrder,
            int orderDirection,
            int indexDirection,
            IntList columnIndexes,
            IntList columnSizeShifts,
            int @Nullable [] coveringMapping,
            ScanPlan scan
    ) {
        if (coveringMapping == null) {
            return new FilterOnValuesRecordCursorFactory(configuration, metadata, frames, keys, keyIndex,
                    filter, indexOrder, orderDirection,
                    indexDirection, columnIndexes, columnSizeShifts);
        }
        assert filter == null;
        final int readerKeyIndex = columnIndexes.getQuick(keyIndex);
        final RecordCursorFactory backup = scan.hasCoveringBackup()
                ? new FilterOnValuesRecordCursorFactory(configuration, metadata, frames, keys, keyIndex,
                null, indexOrder, orderDirection,
                indexDirection, columnIndexes, columnSizeShifts)
                : null;
        return new CoveringIndexRecordCursorFactory(metadata, frames, readerKeyIndex,
                null, columnIndexes, coveringMapping, keys, false, null, null, backup, scan.isCoveringBackupSuppressed());
    }
}
