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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursorFactory;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TableAccessInfo;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.griffin.IntervalExtractor;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.OrderByMnemonic;
import io.questdb.griffin.PlanTables;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SymbolKeyExtractor;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.griffin.engine.functions.regex.MatchSymbolFunctionFactory;
import io.questdb.griffin.engine.functions.regex.SymbolKeySetProvider;
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
import io.questdb.griffin.engine.table.SymbolIndexFilteredRowCursorFactory;
import io.questdb.griffin.engine.table.SymbolIndexRowCursorFactory;
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
import io.questdb.griffin.plan.logical.GeneratedShapes;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PhysicalProperties;
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
    private final FilterFactoryGenerator filterGenerator;
    private final LatestByFactoryGenerator latestByGenerator;
    private final PageFrameReduceTaskFactory reduceTaskFactory;
    private final PlanTables planTables;

    ScanFactoryGenerator(
            CairoConfiguration configuration,
            FilterFactoryGenerator filterGenerator,
            LatestByFactoryGenerator latestByGenerator,
            PageFrameReduceTaskFactory reduceTaskFactory,
            PlanTables planTables
    ) {
        this.configuration = configuration;
        this.filterGenerator = filterGenerator;
        this.latestByGenerator = latestByGenerator;
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
            int symbolKey,
            Function symbolFunc,
            int indexDirection,
            boolean followsOrderByAdvice,
            IntList columnIndexes,
            IntList columnSizeShifts
    ) {
        final RowCursorFactory rcf = symbolKey == SymbolTable.VALUE_NOT_FOUND
                ? new DeferredSymbolIndexRowCursorFactory(keyColumnIndex, symbolFunc, indexDirection)
                : new SymbolIndexRowCursorFactory(keyColumnIndex, symbolKey, indexDirection, null);
        return new DeferredSingleSymbolFilterPageFrameRecordCursorFactory(
                configuration,
                keyColumnIndex,
                symbolFunc,
                rcf,
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

    private static boolean isNegated(FunctionExpression pattern) {
        return pattern.getArgumentCount() == 1 || "!~".equals(pattern.getName());
    }

    private static int keyPosition(BoundExpression expression, IntList keyIds) {
        return expression instanceof ColumnExpression column && column.isDirectReference()
                ? keyIds.indexOf(column.getColumnId(), 0, keyIds.size()) : -1;
    }

    private static PartitionFrameCursorFactory newFrames(ScanPlan scan, RuntimeIntrinsicIntervalModel intervalModel,
                                                         RecordMetadata readerMetadata, int order) {
        final PartitionFrameCursorFactory frames = intervalModel == null
                ? new FullPartitionFrameCursorFactory(scan.getTableToken(), scan.getMetadataVersion(), readerMetadata, order,
                scan.getViewName(), scan.getViewPosition(), scan.isUpdate())
                : new IntervalPartitionFrameCursorFactory(scan.getTableToken(), scan.getMetadataVersion(), intervalModel,
                readerMetadata.getTimestampIndex(), readerMetadata, order, scan.getViewName(), scan.getViewPosition(), scan.isUpdate());
        try {
            frames.setAuthorizedColumnIndexes(scan.getAuthorizedColumnIndexes());
        } catch (Throwable th) {
            Misc.free(frames, th);
            throw th;
        }
        return frames;
    }

    private static int orderMnemonic(ScanPlan scan) {
        return scan.isRowOrderRequired() ? OrderByMnemonic.ORDER_BY_REQUIRED : OrderByMnemonic.ORDER_BY_INVARIANT;
    }

    private static int requestedDirection(ScanPlan scan) {
        final SortKeys requestedOrder = scan.getRequestedOrder();
        return SqlCodeGenerator.queryModelDirection(requestedOrder.size() == 0 ? SortDirection.ASCENDING : requestedOrder.getDirections().getQuick(0));
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

    private RuntimeIntrinsicIntervalModel buildIntervals(IntervalExtractor scanIntervals, TableReader reader) {
        return scanIntervals == null ? null : scanIntervals.build(reader.getPartitionedBy());
    }

    private void configurePushdown(GenerationFrame frame, PartitionFrameCursorFactory frames, BoundExpression residual, ScanPlan scan,
                                   RecordMetadata metadata, IntList indexes, TableReader reader,
                                   SqlExecutionContext executionContext) throws SqlException {
        if (residual == null || !executionContext.isParquetRowGroupPruningEnabled()) {
            return;
        }
        final long partitionTableVersion = reader.getTxFile().getPartitionTableVersion();
        if (!reader.hasParquetPartitions()) {
            frames.setPushdownFilterCondition(partitionTableVersion, null);
            return;
        }
        final ObjList<PushdownFilterExtractor.PushdownFilterCondition> conditions = frame.pushdown.extract(residual, scan.getOutput(),
                metadata, indexes, reader.getMetadata(), frame.functionInstantiator, executionContext);
        if (conditions != null) {
            frames.setPushdownFilterCondition(partitionTableVersion, conditions);
        }
    }

    private RecordCursorFactory filterScan(GenerationFrame frame, RecordCursorFactory base, ScanPlan scan, BoundExpression residual,
                                           SqlExecutionContext executionContext) throws SqlException {
        final Function filter;
        try {
            filter = frame.functionInstantiator.instantiate(residual, scan.getOutput(), base.getMetadata(), executionContext);
        } catch (Throwable th) {
            Misc.free(base, th);
            throw th;
        }
        final boolean isParallel = scan.getResidualAlgorithm() == FilterPlan.Algorithm.PARALLEL;
        if (frame.stolenFilter != null) {
            frame.stolenFilter.of(residual, scan.getOutput(), filter, isParallel && (!scan.isUpdate() || executionContext.isWalApplication()));
            return base;
        }
        return filterGenerator.generate(frame, residual, scan.getOutput(), base, filter, frame.functionInstantiator, executionContext,
                scan.isUpdate(), isParallel, scan.getFilterLimit(), scan.hasHint(ScanPlan.HINT_PRE_TOUCH));
    }

    private RecordCursorFactory generateIndexedScan(
            GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext, IntervalExtractor scanIntervals,
            GenericRecordMetadata metadata, RecordMetadata readerMetadata, TableReader reader, IntList indexes, IntList shifts
    ) throws SqlException {
        final BoundExpression residual = scan.getResidual();
        final int orderByMnemonic = orderMnemonic(scan);
        final int keyIndex = scan.getOutput().getColumnIndexById(scan.getIndexColumnId());
        final int readerKeyIndex = indexes.getQuick(keyIndex);
        final boolean isExcluded = scan.getAccessPath() == ScanPlan.AccessPath.EXCLUDED_SYMBOL_INDEX;
        final ObjList<BoundExpression> values = isExcluded ? scan.getExcludedKeys() : scan.getIndexKeys();
        final int keyCount = values.size();
        final ObjList<Function> keys = new ObjList<>(keyCount);
        RuntimeIntrinsicIntervalModel intervalModel = buildIntervals(scanIntervals, reader);
        Function filter = null;
        PartitionFrameCursorFactory frames = null;
        final boolean isOrderByKey = scan.getIndexOrder() == ScanPlan.IndexOrder.KEY;
        final boolean isOrderByTimestamp = scan.getIndexOrder() == ScanPlan.IndexOrder.TIMESTAMP;
        final int indexDirection = indexDirection(scan);
        final boolean isCovering = scan.getIndexRead() == ScanPlan.IndexRead.COVERING;
        int symbolKey = SymbolTable.VALUE_NOT_FOUND;
        int[] coveringMapping = null;
        try {
            filter = residual == null ? null : frame.functionInstantiator.instantiate(residual, scan.getOutput(), metadata, executionContext);
            if (ParanoiaState.PLAN_PARANOIA_MODE && filter != null && filter.isConstant()) {
                throw new AssertionError("index access path residual folds to a constant");
            }
            instantiateKeys(frame, values, keys, scan, metadata, executionContext);
            if (!isExcluded) {
                final Function firstKey = keys.getQuick(0);
                symbolKey = keyCount > 1 || firstKey.isRuntimeConstant() ? SymbolTable.VALUE_NOT_FOUND
                        : reader.getSymbolMapReader(readerKeyIndex).keyOf(firstKey.getStrA(null));
                coveringMapping = isCovering ? buildCoveringMapping(reader, readerKeyIndex, indexes) : null;
            }
            final RuntimeIntrinsicIntervalModel frameIntervals = intervalModel;
            intervalModel = null;
            frames = newFrames(scan, frameIntervals, readerMetadata, frameOrder(scan));
            configurePushdown(frame, frames, residual, scan, metadata, indexes, reader, executionContext);
        } catch (Throwable th) {
            Misc.free(frames, th);
            Misc.freeObjList(keys, th);
            Misc.free(filter, th);
            Misc.free(intervalModel, th);
            throw th;
        }
        if (isExcluded) {
            return new FilterOnExcludedValuesRecordCursorFactory(configuration, metadata, frames, keys,
                    keyIndex, filter, orderByMnemonic, isOrderByKey, isOrderByTimestamp, requestedDirection(scan),
                    indexDirection, indexes, shifts, configuration.getMaxSymbolNotEqualsCount());
        }
        final Function coveredFilter = coveringMapping == null ? null : filter;
        final RecordCursorFactory factory;
        try {
            if (keyCount == 1) {
                factory = generateSingleSymbolIndexScan(metadata, frames, keyIndex, symbolKey, keys.getQuick(0),
                        coveringMapping == null ? filter : null, indexDirection, isOrderByKey || isOrderByTimestamp,
                        indexes, shifts, coveringMapping, scan.hasNullableKey(), scan.hasHint(ScanPlan.HINT_FORCE_USE_COVERING));
            } else {
                factory = generateSymbolValuesIndexScan(metadata, frames, keys, keyIndex, reader,
                        coveringMapping == null ? filter : null, orderByMnemonic, isOrderByKey, isOrderByTimestamp,
                        requestedDirection(scan), indexDirection, indexes, shifts, coveringMapping, scan.hasNullableKey(),
                        scan.hasHint(ScanPlan.HINT_FORCE_USE_COVERING));
            }
        } catch (Throwable th) {
            Misc.free(coveredFilter, th);
            throw th;
        }
        if (coveredFilter == null) {
            return factory;
        }
        if (frame.stolenFilter != null) {
            frame.stolenFilter.of(residual, scan.getOutput(), coveredFilter, false);
            return factory;
        }
        return filterGenerator.generateCovering(residual, scan.getOutput(), (CoveringIndexRecordCursorFactory) factory, coveredFilter,
                frame.functionInstantiator, executionContext, scan.getResidualAlgorithm() == FilterPlan.Algorithm.PARALLEL,
                scan.getCoveredFilterLimit(), scan.hasHint(ScanPlan.HINT_PRE_TOUCH));
    }

    private RecordCursorFactory generateLatestByScan(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext,
                                                     IntervalExtractor scanIntervals, LatestByPlan latest, GenericRecordMetadata metadata,
                                                     GenericRecordMetadata readerMetadata, TableReader reader, IntList indexes,
                                                     IntList shifts) throws SqlException {
        final IntList keyIndexes = new IntList(latest.getKeyColumnIds().size());
        for (int i = 0, n = latest.getKeyColumnIds().size(); i < n; i++) {
            keyIndexes.add(scan.getOutput().getColumnIndexById(latest.getKeyColumnIds().getQuick(i)));
        }
        frame.latestPrefixes.clear();
        if (scan.getWithin() != null) {
            GeneratedShapes.withinPrefixes(scan.getWithin(), scan.getOutput(), frame.latestPrefixes);
        }
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
            filter = residual == null ? null : frame.functionInstantiator.instantiate(residual, scan.getOutput(), metadata, executionContext);
            instantiateKeys(frame, scan.getIndexKeys(), keys, scan, metadata, executionContext);
            instantiateKeys(frame, scan.getExcludedKeys(), excludedKeys, scan, metadata, executionContext);
            frames = newFrames(scan, buildIntervals(scanIntervals, reader), readerMetadata, frameOrder(scan));
            if (residual != null && (residual.getFunctionFlags() & (BoundExpression.CONSTANT | BoundExpression.RUNTIME_CONSTANT)) == 0) {
                configurePushdown(frame, frames, residual, scan, metadata, indexes, reader, executionContext);
            }
            if (keySubquery != null) {
                subquery = frame.functionInstantiator.generateSubquery(keySubquery, executionContext);
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
            return new LatestBySubQueryRecordCursorFactory(configuration, metadata, frames, keyIndexes.getQuick(0), subquery, filter,
                    scan.getIndexRead() != ScanPlan.IndexRead.NONE, keyGetter, indexes, shifts);
        }
        return latestByGenerator.generateLatestByScan(frame, scan, frames, metadata, reader, indexes, shifts, keyIndexes,
                filter, keys, excludedKeys, frame.latestPrefixes, symbolCounts, executionContext);
    }

    private RecordCursorFactory generatePageFrameScan(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext,
                                                      IntervalExtractor scanIntervals, GenericRecordMetadata metadata, RecordMetadata readerMetadata,
                                                      TableReader reader, IntList indexes, IntList shifts) throws SqlException {
        final BoundExpression residual = scan.getTableToken().isLiveView() ? null : scan.getResidual();
        final int order = frameOrder(scan);
        final PartitionFrameCursorFactory frames = newFrames(scan, buildIntervals(scanIntervals, reader), readerMetadata, order);
        try {
            configurePushdown(frame, frames, residual, scan, metadata, indexes, reader, executionContext);
        } catch (Throwable th) {
            Misc.free(frames, th);
            throw th;
        }
        final RecordCursorFactory factory = generateScan(frames, metadata, order, order == PartitionFrameCursorFactory.ORDER_DESC,
                indexes, shifts, scan.isRandomAccess());
        if (residual == null) {
            return factory;
        }
        return filterScan(frame, factory, scan, residual, executionContext);
    }

    private RecordCursorFactory generatePostingDistinctScan(AggregatePlan aggregate, ScanPlan scan, IntervalExtractor scanIntervals,
                                                            TableReader reader) {
        final ColumnExpression key = (ColumnExpression) aggregate.getGroupingExpressions().getQuick(0);
        final TableReaderMetadata tableMetadata = reader.getMetadata();
        final int index = scan.getSourceColumnIndexes().getQuick(scan.getOutput().getColumnIndexById(key.getColumnId()));
        final TableColumnMetadata column = tableMetadata.getColumnMetadata(index);
        final GenericRecordMetadata metadata = new GenericRecordMetadata().add(new TableColumnMetadata(
                Chars.toString(aggregate.getOutput().getColumnName(0)), key.getDataType(), column.getIndexType(),
                column.getIndexValueBlockCapacity(), column.isSymbolTableStatic(), null, column.getWriterIndex(),
                false, 0, column.isSymbolCacheFlag(), column.getSymbolCapacity()));
        final IntList indexes = new IntList();
        indexes.add(index);
        if (scanIntervals != null && tableMetadata.getTimestampIndex() != index) {
            indexes.add(tableMetadata.getTimestampIndex());
        }
        final PartitionFrameCursorFactory frames = newFrames(scan, buildIntervals(scanIntervals, reader),
                GenericRecordMetadata.copyOfNew(tableMetadata), PartitionFrameCursorFactory.ORDER_ASC);
        return new PostingIndexDistinctRecordCursorFactory(metadata, frames, index, 0, indexes);
    }

    private RecordCursorFactory generateReaderScan(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext,
                                                   IntervalExtractor scanIntervals, LatestByPlan latest, TableReader reader)
            throws SqlException {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        final GenericRecordMetadata readerMetadata = new GenericRecordMetadata();
        final TableReaderMetadata tableMetadata = reader.getMetadata();
        for (int i = 0, n = tableMetadata.getColumnCount(); i < n; i++) {
            readerMetadata.add(SqlCodeGenerator.copyColumn(tableMetadata, i, tableMetadata.getColumnName(i)));
        }
        readerMetadata.setTimestampIndex(tableMetadata.getTimestampIndex());
        final IntList indexes = new IntList(scan.getOutput().getColumnCount());
        final IntList shifts = new IntList(scan.getOutput().getColumnCount());
        for (int i = 0, n = scan.getOutput().getColumnCount(); i < n; i++) {
            // UPDATE binds writer metadata, whose deleted-column slots are not
            // the dense reader positions. Resolve the final layout by bound name.
            final int sourceIndex = tableMetadata.getColumnIndex(scan.getOutput().getColumnName(i));
            indexes.add(sourceIndex);
            metadata.add(readerMetadata.getColumnMetadata(sourceIndex));
            shifts.add(Numbers.msb(ColumnType.sizeOf(readerMetadata.getColumnType(sourceIndex))));
        }
        metadata.setTimestampIndex(PhysicalProperties.timestampIndex(scan));
        final ScanPlan.AccessPath accessPath = scan.getAccessPath();
        if (accessPath == null) {
            throw new IllegalStateException("scan access path is not planned");
        }
        return switch (accessPath) {
            case EMPTY -> new EmptyTableRecordCursorFactory(metadata);
            case PAGE_FRAMES ->
                    generatePageFrameScan(frame, scan, executionContext, scanIntervals, metadata, readerMetadata, reader,
                            indexes, shifts);
            case SORTED_SYMBOL_INDEX -> {
                final PartitionFrameCursorFactory frames = newFrames(scan, buildIntervals(scanIntervals, reader), readerMetadata, frameOrder(scan));
                yield new SortedSymbolIndexRecordCursorFactory(configuration, metadata, frames,
                        scan.getOutput().getColumnIndexById(scan.getIndexColumnId()),
                        scan.getRequestedOrder().getDirections().getQuick(0) == SortDirection.ASCENDING,
                        indexDirection(scan), indexes, shifts);
            }
            case SYMBOL_INDEX, EXCLUDED_SYMBOL_INDEX ->
                    generateIndexedScan(frame, scan, executionContext, scanIntervals, metadata,
                            readerMetadata, reader, indexes, shifts);
            case SYMBOL_SUBQUERY ->
                    generateSubqueryScan(frame, scan, executionContext, scanIntervals, metadata, readerMetadata, reader,
                            indexes, shifts);
            case SYMBOL_PATTERN ->
                    generateSymbolPatternIndex(frame, scan, executionContext, scanIntervals, metadata, readerMetadata,
                            reader, indexes, shifts);
            case LATEST_BY_SUBQUERY, LATEST_BY_VALUE, LATEST_BY_VALUES, LATEST_BY_ALL_INDEXED, LATEST_BY_STATIC_SYMBOL,
                 LATEST_BY_SYMBOLS,
                 LATEST_BY_ALL ->
                    generateLatestByScan(frame, scan, executionContext, scanIntervals, latest, metadata, readerMetadata,
                            reader, indexes, shifts);
            case POSTING_DISTINCT, UPDATE_STUB ->
                    throw new IllegalStateException("access path does not read the table through a reader scan");
        };
    }

    private RecordCursorFactory generateScan(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext, IntervalExtractor scanIntervals,
                                             LatestByPlan latest) throws SqlException {
        final RecordCursorFactory factory = generateTableScan(frame, scan, executionContext, scanIntervals, latest);
        if (!scan.getTableToken().isLiveView() || scan.isUpdate()) {
            return factory;
        }
        // The live-view wrapper pins the in-memory tier and routes rows by seam timestamp.
        return new LiveViewRecordCursorFactory(executionContext.getCairoEngine(), scan.getTableToken(), factory);
    }

    private RecordCursorFactory generateSubqueryScan(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext,
                                                     IntervalExtractor scanIntervals, GenericRecordMetadata metadata,
                                                     GenericRecordMetadata readerMetadata, TableReader reader,
                                                     IntList indexes, IntList shifts) throws SqlException {
        final BoundExpression residual = scan.getResidual();
        PartitionFrameCursorFactory frames = null;
        Function filter = null;
        RecordCursorFactory subquery = null;
        final Record.CharSequenceFunction keyGetter;
        try {
            frames = newFrames(scan, buildIntervals(scanIntervals, reader), readerMetadata, frameOrder(scan));
            configurePushdown(frame, frames, residual, scan, metadata, indexes, reader, executionContext);
            filter = residual == null ? null : frame.functionInstantiator.instantiate(residual, scan.getOutput(), metadata, executionContext);
            subquery = frame.functionInstantiator.generateSubquery(scan.getKeySubquery(), executionContext);
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
            GenericRecordMetadata metadata, RecordMetadata readerMetadata, TableReader reader, IntList indexes, IntList shifts
    ) throws SqlException {
        final BoundExpression predicate = scan.getResidual();
        final FunctionExpression pattern = scan.getKeyPattern();
        final OutputSchema input = scan.getOutput();
        final SortKeys requestedOrder = scan.getRequestedOrder();
        final boolean isOrderByTimestampOnly = requestedOrder.size() == 1
                && requestedOrder.getColumnIds().getQuick(0) == scan.getNativeTimestampColumnId();
        final boolean isCovering = scan.getIndexRead() == ScanPlan.IndexRead.COVERING;
        frame.patternConjuncts.clear();
        LogicalPlans.collectConjuncts(predicate, frame.patternConjuncts);
        if ("!~".equals(pattern.getName())) {
            Misc.free(frame.functionInstantiator.instantiate(pattern, input, metadata, executionContext));
        }
        AdaptiveSymbolPatternRecordCursorFactory.PreparedSymbolPatternFilter filter = preparePatternFilter(frame, pattern, input, metadata, executionContext);
        if (filter == null) {
            throw new AssertionError("symbol pattern provider declaration differs from the instantiated function");
        }
        ObjList<Function> workerFilters = null;
        Function limit = null;
        final BoundExpression limitCount = scan.getCoveredFilterLimit();
        final PreparedFilter stolenFilter = frame.stolenFilter;
        final boolean isParallel = scan.getResidualAlgorithm() == FilterPlan.Algorithm.PARALLEL && stolenFilter == null;
        try {
            if (isParallel) {
                if (!filter.isThreadSafe()) {
                    final int workerCount = executionContext.getSharedQueryWorkerCount();
                    workerFilters = new ObjList<>(workerCount);
                    frame.functionInstantiator.beginWorkerClones();
                    try {
                        for (int i = 0; i < workerCount; i++) {
                            workerFilters.add(preparePatternFilter(frame, pattern, input, metadata, executionContext));
                        }
                    } finally {
                        frame.functionInstantiator.endWorkerClones();
                    }
                }
                if (limitCount != null) {
                    limit = frame.functionInstantiator.instantiate(limitCount, input, executionContext);
                }
            }
            final PartitionFrameCursorFactory frames = newFrames(scan, buildIntervals(scanIntervals, reader), readerMetadata, frameOrder(scan));
            try {
                configurePushdown(frame, frames, predicate, scan, metadata, indexes, reader, executionContext);
            } catch (Throwable th) {
                Misc.free(frames, th);
                throw th;
            }
            final AdaptiveSymbolPatternRecordCursorFactory.PreparedSymbolPatternFilter ownedFilter = filter;
            final ObjList<Function> ownedWorkers = workerFilters;
            final Function ownedLimit = limit;
            filter = null;
            workerFilters = null;
            limit = null;
            return generateSymbolPatternIndex(frames, metadata, reader, indexes, shifts, ownedFilter, predicate, input,
                    ownedWorkers, ownedLimit, limitCount == null ? 0 : limitCount.getPosition(), orderMnemonic(scan),
                    isOrderByTimestampOnly, isCovering, isParallel, scan.hasHint(ScanPlan.HINT_PRE_TOUCH), stolenFilter, executionContext);
        } catch (Throwable th) {
            Misc.free(filter, th);
            Misc.freeObjList(workerFilters, th);
            Misc.free(limit, th);
            throw th;
        }
    }

    private RecordCursorFactory generateTableScan(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext, IntervalExtractor scanIntervals,
                                                  LatestByPlan latest) throws SqlException {
        final boolean isOverridden = executionContext.isOverriddenIntrinsics(scan.getTableToken()) && !scan.isWalClientUpdate();
        final WindowJoinStep step = scan.getJoinIntervalStep();
        if (!isOverridden && step == null) {
            return generateTableScan0(frame, scan, executionContext, scanIntervals, latest);
        }
        final int timestampType = scan.getNativeTimestampType();
        final IntervalExtractor rangeIntervals = scanIntervals != null ? scanIntervals : frame.overrideIntervals;
        final RecordCursorFactory factory;
        try {
            if (scanIntervals == null) {
                frame.overrideIntervals.of(timestampType);
            }
            if (isOverridden) {
                rangeIntervals.override(scan.getTableToken(), executionContext);
            }
            if (step != null) {
                final long hi = step.getIntervalHi(timestampType);
                final long lo = step.getIntervalLo(timestampType);
                rangeIntervals.merge(scan.getJoinIntervals(), lo, hi);
            }
            factory = generateTableScan0(frame, scan, executionContext, rangeIntervals, latest);
        } catch (Throwable th) {
            Misc.clear(frame.overrideIntervals, th);
            throw th;
        }
        return SqlCodeGenerator.clearAfter(frame.overrideIntervals, factory);
    }

    private RecordCursorFactory generateTableScan0(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext, IntervalExtractor scanIntervals,
                                                   LatestByPlan latest) throws SqlException {
        if (scan.isWalClientUpdate()) {
            // Client-side WAL UPDATE validates against sequencer metadata. The data
            // reader may still have an older schema; rows are read only during WAL apply.
            final TableRecordMetadata tableMetadata = executionContext.getMetadataForWrite(scan.getTableToken(), scan.getMetadataVersion());
            final RecordCursorFactory factory;
            try {
                final GenericRecordMetadata metadata = new GenericRecordMetadata();
                for (int i = 0, n = scan.getOutput().getColumnCount(); i < n; i++) {
                    final int index = tableMetadata.getColumnIndex(scan.getOutput().getColumnName(i));
                    metadata.add(SqlCodeGenerator.copyColumn(tableMetadata, index, tableMetadata.getColumnName(index)));
                }
                metadata.setTimestampIndex(PhysicalProperties.timestampIndex(scan));
                factory = new EmptyTableRecordCursorFactory(metadata, tableMetadata.getTableToken());
            } catch (Throwable th) {
                Misc.free(tableMetadata, th);
                throw th;
            }
            return SqlCodeGenerator.closeAfter(tableMetadata, factory);
        }
        return generateReaderScan(frame, scan, executionContext, scanIntervals, latest, planTables.of(scan));
    }

    private void instantiateKeys(GenerationFrame frame, ObjList<BoundExpression> values, ObjList<Function> keys, ScanPlan scan,
                                 RecordMetadata metadata, SqlExecutionContext executionContext) throws SqlException {
        keys.checkCapacity(keys.size() + values.size());
        for (int i = 0, n = values.size(); i < n; i++) {
            keys.add(SymbolKeyExtractor.instantiateValue(values.getQuick(i), scan.getOutput(), metadata, frame.functionInstantiator, executionContext));
        }
    }

    private Function positivePattern(GenerationFrame frame, FunctionExpression pattern, OutputSchema input, RecordMetadata metadata,
                                     SqlExecutionContext executionContext) throws SqlException {
        if (!isNegated(pattern)) {
            return frame.functionInstantiator.instantiate(pattern, input, metadata, executionContext);
        }
        if (pattern.getArgumentCount() == 1) {
            return frame.functionInstantiator.instantiate(pattern.argumentAt(0), input, metadata, executionContext);
        }
        frame.patternArguments.clear();
        frame.patternPositions.clear();
        try {
            for (int i = 0; i < 2; i++) {
                frame.patternArguments.add(frame.functionInstantiator.instantiate(pattern.argumentAt(i), input, metadata, executionContext));
                frame.patternPositions.add(pattern.getArgumentPosition(i));
            }
            final Function match = MatchSymbolFunctionFactory.positivePatternFactory(pattern).newInstance(pattern.getPosition(),
                    frame.patternArguments, frame.patternPositions, configuration, executionContext);
            frame.patternArguments.clear();
            return match;
        } catch (Throwable th) {
            Misc.freeObjList(frame.patternArguments, th);
            throw th;
        } finally {
            frame.patternArguments.clear();
        }
    }

    /**
     * Returns null when the pattern does not compile to a symbol key-set provider.
     */
    private AdaptiveSymbolPatternRecordCursorFactory.PreparedSymbolPatternFilter preparePatternFilter(
            GenerationFrame frame, FunctionExpression pattern, OutputSchema input, RecordMetadata metadata, SqlExecutionContext executionContext
    ) throws SqlException {
        Function provider = null;
        Function residual = null;
        try {
            provider = positivePattern(frame, pattern, input, metadata, executionContext);
            if (!(provider instanceof SymbolKeySetProvider)) {
                Misc.free(provider);
                return null;
            }
            BoundExpression residualExpression = null;
            for (int i = 0, n = frame.patternConjuncts.size(); i < n; i++) {
                final BoundExpression conjunct = frame.patternConjuncts.getQuick(i);
                if (conjunct != pattern) {
                    residualExpression = frame.expressionRewriter.combineConjunction(conjunct, residualExpression, 0);
                }
            }
            if (residualExpression != null) {
                residual = frame.functionInstantiator.instantiate(residualExpression, input, metadata, executionContext);
            }
            final FunctionExpression positive = pattern.getArgumentCount() == 1 ? (FunctionExpression) pattern.argumentAt(0) : pattern;
            final int keyIndex = input.getColumnIndexById(((ColumnExpression) positive.argumentAt(0)).getColumnId());
            final AdaptiveSymbolPatternRecordCursorFactory.PreparedSymbolPatternFilter filter =
                    new AdaptiveSymbolPatternRecordCursorFactory.PreparedSymbolPatternFilter(provider, residual, isNegated(pattern), keyIndex, null, null);
            provider = null;
            residual = null;
            return filter;
        } catch (Throwable th) {
            Misc.free(provider, th);
            Misc.free(residual, th);
            throw th;
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
                    target == base ? null : source.getSourceColumnIndexes(), target.getMetadata(), frame.functionInstantiator, executionContext));
        }
    }

    RecordCursorFactory generateFiltered(GenerationFrame frame, ScanPlan scan, BoundExpression predicate, SqlExecutionContext executionContext)
            throws SqlException {
        final int timestampIndex = scan.getOutput().getColumnIndexById(scan.getNativeTimestampColumnId());
        final RecordCursorFactory factory;
        try {
            if (timestampIndex >= 0) {
                frame.intervals.extract(predicate, scan.getOutput().getColumnId(timestampIndex), scan.getOutput(), frame.intervalBounds,
                        frame.expressionRewriter, scan.getDepth(), executionContext);
            }
            final RecordCursorFactory scanFactory = generateScan(frame, scan, executionContext, frame.intervals, null);
            final BoundExpression residual = scan.getResidual();
            // Rows the live view serves from memory bypass the wrapped scan, so only intervals go below it.
            factory = scan.getTableToken().isLiveView() && residual != null ? filterScan(frame, scanFactory, scan, residual, executionContext) : scanFactory;
        } catch (Throwable th) {
            Misc.clear(frame.intervals, th);
            throw th;
        }
        return SqlCodeGenerator.clearAfter(frame.intervals, factory);
    }

    RecordCursorFactory generateFunctionSource(GenerationFrame frame, FunctionSourcePlan plan, SqlExecutionContext executionContext) throws SqlException {
        final RecordCursorFactory base = frame.functionSources.takeFactory(plan, executionContext);
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
        if (!(latest.getInput() instanceof FilterPlan filter)) {
            return generateScan(frame, scan, executionContext, null, latest);
        }
        final RecordCursorFactory factory;
        try {
            if (scan.getOutput().getColumnIndexById(scan.getNativeTimestampColumnId()) >= 0) {
                frame.intervals.extract(filter.getPredicate(), scan.getNativeTimestampColumnId(), scan.getOutput(), frame.intervalBounds,
                        frame.expressionRewriter, scan.getDepth(), executionContext);
            }
            factory = generateScan(frame, scan, executionContext, frame.intervals, latest);
        } catch (Throwable th) {
            Misc.clear(frame.intervals, th);
            throw th;
        } finally {
            frame.latestPrefixes.clear();
        }
        return SqlCodeGenerator.clearAfter(frame.intervals, factory);
    }

    /**
     * Builds the DISTINCT of the aggregate's single key from the posting index its scan reads, see
     * {@link LogicalPlans#postingDistinctScan}.
     */
    RecordCursorFactory generatePostingDistinct(GenerationFrame frame, AggregatePlan aggregate, SqlExecutionContext executionContext)
            throws SqlException {
        final ScanPlan scan = GeneratedShapes.postingDistinctScan(aggregate);
        final BoundExpression predicate = aggregate.getInput() instanceof FilterPlan filter ? filter.getPredicate() : null;
        final RecordCursorFactory factory;
        try {
            if (predicate != null) {
                frame.intervals.extract(predicate, scan.getNativeTimestampColumnId(), scan.getOutput(), frame.intervalBounds,
                        frame.expressionRewriter, scan.getDepth(), executionContext);
            }
            factory = generatePostingDistinctScan(aggregate, scan, predicate == null ? null : frame.intervals, planTables.of(scan));
        } catch (Throwable th) {
            Misc.clear(frame.intervals, th);
            throw th;
        }
        return SqlCodeGenerator.clearAfter(frame.intervals, factory);
    }

    RecordCursorFactory generateScan(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext) throws SqlException {
        return generateScan(frame, scan, executionContext, null, null);
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
            int symbolKey,
            Function key,
            @Nullable Function filter,
            int indexDirection,
            boolean followsOrderByAdvice,
            IntList columnIndexes,
            IntList columnSizeShifts,
            int @Nullable [] coveringMapping,
            boolean canKeyBeNull,
            boolean isBackupSuppressed
    ) {
        if (coveringMapping != null) {
            assert filter == null;
            final RecordCursorFactory backup = !isBackupSuppressed && canKeyBeNull
                    ? buildSingleSymbolIndexScan(configuration, metadata, frames, keyIndex, symbolKey,
                    key, indexDirection, followsOrderByAdvice, columnIndexes, columnSizeShifts)
                    : null;
            return new CoveringIndexRecordCursorFactory(metadata, frames, columnIndexes.getQuick(keyIndex),
                    symbolKey, key, columnIndexes, coveringMapping, null, null, false, null, null,
                    backup, true, backup == null && canKeyBeNull);
        }
        if (filter == null) {
            return buildSingleSymbolIndexScan(configuration, metadata, frames, keyIndex, symbolKey,
                    key, indexDirection, followsOrderByAdvice, columnIndexes, columnSizeShifts);
        }
        if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
            return new PageFrameRecordCursorFactory(configuration, metadata, frames,
                    new DeferredSymbolIndexFilteredRowCursorFactory(keyIndex, key, filter, indexDirection), followsOrderByAdvice,
                    filter, false, columnIndexes, columnSizeShifts, true, false);
        }
        final RecordCursorFactory result;
        try {
            result = new PageFrameRecordCursorFactory(configuration, metadata, frames,
                    new SymbolIndexFilteredRowCursorFactory(keyIndex, symbolKey, filter, indexDirection, null), followsOrderByAdvice,
                    filter, false, columnIndexes, columnSizeShifts, true, false);
        } catch (Throwable th) {
            Misc.free(key, th);
            throw th;
        }
        return SqlCodeGenerator.closeAfter(key, result);
    }

    /**
     * Builds the scan of a fused filter a parallel consumer steals, without the filter, and prepares the filter over
     * it in {@code target}.
     */
    RecordCursorFactory generateStolenFilter(GenerationFrame frame, ScanPlan scan, BoundExpression predicate, PreparedFilter target,
                                             SqlExecutionContext executionContext) throws SqlException {
        frame.stolenFilter = target;
        try {
            return generateFiltered(frame, scan, predicate, executionContext);
        } finally {
            frame.stolenFilter = null;
        }
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
            TableReader reader,
            IntList columnIndexes,
            IntList columnSizeShifts,
            AdaptiveSymbolPatternRecordCursorFactory.PreparedSymbolPatternFilter filter,
            BoundExpression predicate,
            OutputSchema input,
            @Nullable ObjList<Function> workerFilters,
            @Nullable Function limit,
            int limitPosition,
            int orderByMnemonic,
            boolean isOrderByTimestampOnly,
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
            stolenFilter.of(predicate, input, filter, false);
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
        RecordCursorFactory scanDelegate = null;
        boolean isSelfFiltering = false;
        try {
            indexDelegate = new SymbolPatternIndexRecordCursorFactory(configuration, metadata, sharedFrames, keyColumnIndex,
                    effectiveKeys, orderByMnemonic, isOrderByTimestampOnly, IndexReader.DIR_FORWARD, columnIndexes, columnSizeShifts);
            if (isCovering) {
                final int[] coveringMapping = buildCoveringMapping(reader, columnIndexes.getQuick(keyColumnIndex), columnIndexes);
                coveringDelegate = new CoveringIndexRecordCursorFactory(metadata, sharedFrames, columnIndexes.getQuick(keyColumnIndex),
                        SymbolTable.VALUE_NOT_FOUND, null, columnIndexes, coveringMapping, null, reader, false, null,
                        effectiveKeys, null, false, false);
            }
            scanDelegate = new PageFrameRecordCursorFactory(configuration, metadata, sharedFrames,
                    new PageFrameRowCursorFactory(frames.getOrder()), false, null, true, columnIndexes, columnSizeShifts, true, false);
            if (coveringDelegate == null && isParallel) {
                final RecordCursorFactory unfiltered = scanDelegate;
                final ObjList<Function> scanWorkerFilters = workerFilters;
                final Function scanLimit = limit;
                scanDelegate = null;
                workerFilters = null;
                limit = null;
                isSelfFiltering = true;
                scanDelegate = new AsyncFilteredRecordCursorFactory(executionContext.getCairoEngine(), configuration,
                        executionContext.getMessageBus(), unfiltered, filter, filterColumnIndexes, reduceTaskFactory, scanWorkerFilters,
                        scanLimit, limitPosition, executionContext.getSharedQueryWorkerCount(), isPreTouchEnabled);
            }
        } catch (Throwable th) {
            Misc.free(coveringDelegate, th);
            Misc.free(indexDelegate, th);
            Misc.free(scanDelegate, th);
            Misc.free(frames, th);
            Misc.freeObjList(workerFilters, th);
            Misc.free(limit, th);
            if (!isSelfFiltering) {
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
            Misc.freeObjList(workerFilters, th);
            Misc.free(limit, th);
            if (!isSelfFiltering) {
                Misc.free(filter, th);
            }
            throw th;
        }
        if (isSelfFiltering) {
            return adaptive;
        }
        if (stolenFilter != null) {
            stolenFilter.of(predicate, input, filter, false);
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
     * Consumes frames, key functions and filter, including on failure; the key list and reader are borrowed.
     */
    RecordCursorFactory generateSymbolValuesIndexScan(
            RecordMetadata metadata,
            PartitionFrameCursorFactory frames,
            ObjList<Function> keys,
            int keyIndex,
            TableReader reader,
            @Nullable Function filter,
            int orderByMnemonic,
            boolean isOrderByKey,
            boolean isOrderByTimestamp,
            int orderDirection,
            int indexDirection,
            IntList columnIndexes,
            IntList columnSizeShifts,
            int @Nullable [] coveringMapping,
            boolean hasNullableKey,
            boolean isBackupSuppressed
    ) {
        if (coveringMapping == null) {
            return new FilterOnValuesRecordCursorFactory(configuration, metadata, frames, keys, keyIndex,
                    reader, filter, orderByMnemonic, isOrderByKey, isOrderByTimestamp, orderDirection,
                    indexDirection, columnIndexes, columnSizeShifts);
        }
        assert filter == null;
        final int readerKeyIndex = columnIndexes.getQuick(keyIndex);
        final RecordCursorFactory backup = hasNullableKey && !isBackupSuppressed
                ? new FilterOnValuesRecordCursorFactory(configuration, metadata, frames, keys, keyIndex,
                reader, null, orderByMnemonic, isOrderByKey, isOrderByTimestamp, orderDirection,
                indexDirection, columnIndexes, columnSizeShifts)
                : null;
        return new CoveringIndexRecordCursorFactory(metadata, frames, readerKeyIndex,
                SymbolTable.VALUE_NOT_FOUND, null, columnIndexes, coveringMapping, keys, reader,
                false, null, null, backup, true, hasNullableKey && isBackupSuppressed);
    }
}
