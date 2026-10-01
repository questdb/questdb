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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.FullPartitionFrameCursorFactory;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.IntervalPartitionFrameCursorFactory;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.ProjectableRecordCursorFactory;
import io.questdb.cairo.SymbolMapReader;
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
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
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
import io.questdb.griffin.engine.table.PushdownFilterExtractor;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.griffin.engine.table.SortedSymbolIndexRecordCursorFactory;
import io.questdb.griffin.engine.table.SymbolIndexFilteredRowCursorFactory;
import io.questdb.griffin.engine.table.SymbolIndexRowCursorFactory;
import io.questdb.griffin.engine.table.SymbolPatternIndexRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowContextImpl;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.RuntimeIntervalModel;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.griffin.plan.logical.BindVariableExpression;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.FunctionSourcePlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.WindowJoinStep;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * Builds table scans, index scans, function sources and LATEST BY over a table, applying the
 * interval, symbol-key and Parquet pushdown extractors of the current {@link GenerationFrame}.
 */
final class ScanFactoryGenerator {
    private final CairoConfiguration configuration;
    private final OutputSchema emptySchema;
    private final FilterFactoryGenerator filterGenerator;
    private final LatestByFactoryGenerator latestByGenerator;
    private final MatchSymbolFunctionFactory matchSymbolFactory = new MatchSymbolFunctionFactory();
    private final PageFrameReduceTaskFactory reduceTaskFactory;

    ScanFactoryGenerator(
            CairoConfiguration configuration,
            FilterFactoryGenerator filterGenerator,
            LatestByFactoryGenerator latestByGenerator,
            OutputSchema emptySchema,
            PageFrameReduceTaskFactory reduceTaskFactory
    ) {
        this.configuration = configuration;
        this.filterGenerator = filterGenerator;
        this.latestByGenerator = latestByGenerator;
        this.emptySchema = emptySchema;
        this.reduceTaskFactory = reduceTaskFactory;
    }

    static boolean isWalClientUpdate(ScanPlan scan, SqlExecutionContext executionContext) {
        return scan.isUpdate() && !executionContext.isWalApplication()
                && executionContext.getCairoEngine().isWalTable(scan.getTableToken());
    }

    void configurePushdown(GenerationFrame frame, FunctionSourcePlan source, RecordCursorFactory base, BoundExpression residual,
                           SqlExecutionContext executionContext) throws SqlException {
        final RecordCursorFactory target = base instanceof SelectedRecordCursorFactory ? base.getBaseFactory() : base;
        if (target.mayHaveParquetPartitions(executionContext) && executionContext.isParquetRowGroupPruningEnabled()) {
            target.setPushdownFilterCondition(frame.pushdown.extract(residual, source.getOutput(), base.getMetadata(),
                    target == base ? null : source.getSourceColumnIndexes(), target.getMetadata(), frame.functionBinder, executionContext));
        }
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
                metadata, indexes, reader.getMetadata(), frame.functionBinder, executionContext);
        if (conditions != null) {
            frames.setPushdownFilterCondition(partitionTableVersion, conditions);
        }
    }

    int generateFiltered(
            GenerationFrame frame, ScanPlan scan, BoundExpression residual, int requiredOrderColumnId, int requiredScanDirection,
            SortPlan orderAdvice, LimitPlan limitAdvice, int orderByMnemonic, SqlExecutionContext executionContext
    ) throws SqlException {
        final int timestampIndex = scan.getOutput().getColumnIndexById(scan.getNativeTimestampColumnId());
        try {
            if (timestampIndex >= 0) {
                residual = frame.intervals.extract(residual, scan.getOutput().getColumnId(timestampIndex), scan.getOutput(), frame.functionBinder, executionContext);
            }
            residual = foldSelfComparisons(frame, residual);
            final int order = requiredOrderColumnId == scan.getOutput().getTimestampColumnId()
                    && requiredScanDirection == RecordCursorFactory.SCAN_DIRECTION_BACKWARD
                    ? PartitionFrameCursorFactory.ORDER_DESC : PartitionFrameCursorFactory.ORDER_ASC;
            if (scan.getTableToken().isLiveView()) {
                // Rows the live view serves from memory bypass the wrapped scan, so only intervals go below it.
                final int scanSlot = generateScan(frame, scan, executionContext, order, frame.intervals, null, null, null,
                        orderAdvice, limitAdvice, orderByMnemonic);
                return residual == null ? scanSlot
                        : filterScan(frame, scanSlot, scan, residual, order, orderAdvice, limitAdvice, executionContext);
            }
            return generateScan(frame, scan, executionContext, order, frame.intervals, null, residual, null, orderAdvice, limitAdvice, orderByMnemonic);
        } catch (Throwable th) {
            Misc.clear(frame.intervals, th);
            throw th;
        } finally {
            frame.symbols.clear();
            frame.intervals.clear();
        }
    }

    int generateFunctionSource(GenerationFrame frame, FunctionSourcePlan plan, SqlExecutionContext executionContext) throws SqlException {
        final int inputSlot = frame.resources.reserve();
        final RecordCursorFactory base = frame.functionSources.takeFactory(plan, executionContext);
        frame.resources.own(inputSlot, base);
        final RecordMetadata baseMetadata = base.getMetadata();
        final OutputSchema output = plan.getOutput();
        final IntList sourceIndexes = plan.getSourceColumnIndexes();
        boolean isIdentity = sourceIndexes.size() == baseMetadata.getColumnCount()
                && output.getTimestampIndex() == baseMetadata.getTimestampIndex();
        for (int i = 0, n = sourceIndexes.size(); i < n; i++) {
            isIdentity &= sourceIndexes.getQuick(i) == i;
        }
        if (isIdentity) {
            return inputSlot;
        }
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        final IntList mapping = new IntList(sourceIndexes);
        for (int i = 0, n = mapping.size(); i < n; i++) {
            metadata.add(baseMetadata.getColumnMetadata(mapping.getQuick(i)));
        }
        metadata.setTimestampIndex(output.getTimestampIndex());
        if (base instanceof ProjectableRecordCursorFactory projectable) {
            projectable.setQueryProjectedMetadata(metadata);
            return inputSlot;
        }
        final int slot = frame.resources.reserve();
        final RecordCursorFactory factory = new SelectedRecordCursorFactory(metadata, mapping, base);
        frame.resources.detach(inputSlot);
        frame.resources.own(slot, factory);
        return slot;
    }

    int generateLatestBy(GenerationFrame frame, LatestByPlan latest, ScanPlan scan, SqlExecutionContext executionContext) throws SqlException {
        if (!(latest.getInput() instanceof FilterPlan filter)) {
            return generateScan(frame, scan, executionContext, PartitionFrameCursorFactory.ORDER_DESC, null, latest, null, null);
        }
        try {
            final BoundExpression predicate = filter.getPredicate();
            if (configuration.useWithinLatestByOptimisation()) {
                collectWithin(frame, scan.getOutput(), predicate);
            }
            BoundExpression residual = scan.getOutput().getColumnIndexById(scan.getNativeTimestampColumnId()) < 0 ? predicate : frame.intervals.extract(predicate,
                    scan.getNativeTimestampColumnId(), scan.getOutput(), frame.functionBinder, executionContext);
            final int candidateColumnId = latest.getKeyColumnIds().size() == 1 ? latest.getKeyColumnIds().getQuick(0) : -1;
            residual = frame.symbols.extract(foldSelfComparisons(frame, residual), candidateColumnId, frame.functionBinder);
            return generateScan(frame, scan, executionContext, PartitionFrameCursorFactory.ORDER_DESC, frame.intervals, latest, residual, frame.symbols);
        } catch (Throwable th) {
            Misc.clear(frame.intervals, th);
            throw th;
        } finally {
            frame.latestPrefixes.clear();
            frame.latestWithin = null;
            frame.symbols.clear();
            frame.intervals.clear();
        }
    }

    int generateScan(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext, int order) throws SqlException {
        return generateScan(frame, scan, executionContext, order, null, null, null, null);
    }

    private int generateScan(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext, int order, IntervalExtractor scanIntervals,
                     LatestByPlan latest, BoundExpression latestResidual, SymbolKeyExtractor latestKeys) throws SqlException {
        return generateScan(frame, scan, executionContext, order, scanIntervals, latest, latestResidual, latestKeys, null, null, OrderByMnemonic.ORDER_BY_REQUIRED);
    }

    private int generateScan(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext, int order, IntervalExtractor scanIntervals,
                     LatestByPlan latest, BoundExpression latestResidual, SymbolKeyExtractor latestKeys,
                     SortPlan orderAdvice, LimitPlan limitAdvice, int orderByMnemonic) throws SqlException {
        final int slot = generateTableScan(frame, scan, executionContext, order, scanIntervals, latest, latestResidual, latestKeys,
                orderAdvice, limitAdvice, orderByMnemonic);
        if (!scan.getTableToken().isLiveView() || scan.isUpdate()) {
            return slot;
        }
        // The live-view wrapper pins the in-memory tier and routes rows by seam timestamp.
        final int liveSlot = frame.resources.reserve();
        final RecordCursorFactory base = (RecordCursorFactory) frame.resources.detach(slot);
        try {
            frame.resources.own(liveSlot, new LiveViewRecordCursorFactory(executionContext.getCairoEngine(), scan.getTableToken(), base));
        } catch (Throwable th) {
            Misc.free(base, th);
            throw th;
        }
        return liveSlot;
    }

    private static boolean addWithinPrefix(ConstantExpression prefix, int columnType, LongList prefixes) {
        try {
            GeoHashes.addNormalizedGeoPrefix(prefix.getLongValue(), prefix.getDataType(), columnType, prefixes);
            return true;
        } catch (NumericException e) {
            return false;
        }
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

    private static boolean isIndexedSymbolColumn(BoundExpression expression, OutputSchema input, RecordMetadata metadata) {
        return expression instanceof ColumnExpression column && column.isDirectReference() && ColumnType.isSymbol(column.getDataType())
                && metadata.isColumnIndexed(input.getColumnIndexById(column.getColumnId()));
    }

    private static boolean isIndexedSymbolPattern(BoundExpression expression, OutputSchema input, RecordMetadata metadata) {
        if (expression instanceof FunctionExpression call && call.getArgumentCount() == 2) {
            final String name = call.getName();
            return ("like".equals(name) || "ilike".equals(name) || "~".equals(name)) && isIndexedSymbolColumn(call.argumentAt(0), input, metadata);
        }
        return false;
    }

    private static boolean isLimitOrderPreserved(ScanPlan scan, int order, SortPlan orderAdvice) {
        return orderAdvice == null
                || orderAdvice.getColumnIds().size() == 1
                && orderAdvice.getColumnIds().getQuick(0) == scan.getNativeTimestampColumnId()
                && (orderAdvice.getDirections().getQuick(0) == QueryModel.ORDER_DIRECTION_DESCENDING)
                == (order == PartitionFrameCursorFactory.ORDER_DESC);
    }

    private static boolean isNegatedIndexedSymbolPattern(BoundExpression expression, OutputSchema input, RecordMetadata metadata) {
        if (!(expression instanceof FunctionExpression call)) {
            return false;
        }
        final String name = call.getName();
        return call.getArgumentCount() == 1 && "not".equals(name) && isIndexedSymbolPattern(call.argumentAt(0), input, metadata)
                || call.getArgumentCount() == 2 && "!~".equals(name) && isIndexedSymbolColumn(call.argumentAt(0), input, metadata);
    }

    private static int keyPosition(BoundExpression expression, IntList keyIds) {
        return expression instanceof ColumnExpression column && column.isDirectReference()
                ? keyIds.indexOf(column.getColumnId(), 0, keyIds.size()) : -1;
    }

    private static PartitionFrameCursorFactory newFrames(ScanPlan scan, RuntimeIntrinsicIntervalModel intervalModel,
                                                         RecordMetadata readerMetadata, int order) {
        return intervalModel == null
                ? new FullPartitionFrameCursorFactory(scan.getTableToken(), scan.getMetadataVersion(), readerMetadata, order,
                scan.getViewName(), scan.getViewPosition(), scan.isUpdate())
                : new IntervalPartitionFrameCursorFactory(scan.getTableToken(), scan.getMetadataVersion(), intervalModel,
                readerMetadata.getTimestampIndex(), readerMetadata, order, scan.getViewName(), scan.getViewPosition(), scan.isUpdate());
    }

    private static int sortedSymbolIndexKey(ScanPlan scan, RuntimeIntrinsicIntervalModel intervalModel, BoundExpression residual,
                                            RecordMetadata metadata, SortPlan orderAdvice, SqlExecutionContext executionContext) {
        if (residual != null || intervalModel == null || orderAdvice == null || executionContext.isTimestampRequired()
                || scan.hasHint(ScanPlan.HINT_NO_INDEX) || scan.isUpdate() || !intervalModel.allIntervalsHitOnePartition()) {
            return -1;
        }
        final int count = orderAdvice.getColumnIds().size();
        if (count < 1 || count > 2 || count == 2 && orderAdvice.getColumnIds().getQuick(1) != scan.getOutput().getTimestampColumnId()) {
            return -1;
        }
        final int index = scan.getOutput().getColumnIndexById(orderAdvice.getColumnIds().getQuick(0));
        return index >= 0 && metadata.getColumnIndexType(index) == IndexType.BITMAP ? index : -1;
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

    private RuntimeIntrinsicIntervalModel buildIntervals(GenerationFrame frame, IntervalExtractor scanIntervals, TableReader reader) {
        final RuntimeIntrinsicIntervalModel model = scanIntervals == null ? null : scanIntervals.build(reader.getPartitionedBy());
        if (model != null && frame.isJoinIntervalCapture) {
            frame.joinIntervals = model;
        }
        return model;
    }

    /**
     * Records the top-level within() conjunct and its GeoHash prefixes in the frame. Only the
     * indexed LATEST BY scan consumes them, matching the prefixes on the latest row of each key.
     */
    private void collectWithin(GenerationFrame frame, OutputSchema output, BoundExpression predicate) {
        if (!(predicate instanceof FunctionExpression call) || frame.latestWithin != null) {
            return;
        }
        if (call.getArgumentCount() == 2 && call.isAnd()) {
            collectWithin(frame, output, call.argumentAt(0));
            collectWithin(frame, output, call.argumentAt(1));
            return;
        }
        if (!"within".equals(call.getName()) || !(call.argumentAt(0) instanceof ColumnExpression column)) {
            return;
        }
        final int index = output.getColumnIndexById(column.getColumnId());
        if (index < 0) {
            return;
        }
        final LongList prefixes = frame.latestPrefixes;
        final int columnType = column.getDataType();
        prefixes.add(index);
        prefixes.add(columnType);
        for (int i = 1, n = call.getArgumentCount(); i < n; i++) {
            if (!(call.argumentAt(i) instanceof ConstantExpression prefix) || !addWithinPrefix(prefix, columnType, prefixes)) {
                prefixes.clear();
                return;
            }
        }
        frame.latestWithin = call;
    }

    private int filterScan(GenerationFrame frame, int scanSlot, ScanPlan scan, BoundExpression residual, int order, SortPlan orderAdvice,
                           LimitPlan limitAdvice, SqlExecutionContext executionContext) throws SqlException {
        final int slot = frame.resources.reserve();
        final int filterSlot = frame.resources.reserve();
        final RecordCursorFactory base = (RecordCursorFactory) frame.resources.resources.getQuick(scanSlot);
        final Function filter = frame.functionBinder.instantiate(residual, scan.getOutput(), base.getMetadata(), executionContext);
        frame.resources.own(filterSlot, filter);
        frame.resources.detach(scanSlot);
        frame.resources.detach(filterSlot);
        frame.resources.own(slot, filterGenerator.generate(frame, residual, scan.getOutput(), base, filter, frame.functionBinder, executionContext,
                scan.isUpdate(), isLimitOrderPreserved(scan, order, orderAdvice) ? limitAdvice : null, scan.hasHint(ScanPlan.HINT_PRE_TOUCH)));
        return slot;
    }

    private BoundExpression foldSelfComparisons(GenerationFrame frame, BoundExpression predicate) {
        if (!(predicate instanceof FunctionExpression call) || call.getArgumentCount() != 2) {
            return predicate;
        }
        if (call.isAnd()) {
            final BoundExpression left = foldSelfComparisons(frame, call.argumentAt(0));
            if (left != call.argumentAt(0) && left instanceof ConstantExpression) {
                return left;
            }
            final BoundExpression right = foldSelfComparisons(frame, call.argumentAt(1));
            return right != call.argumentAt(1) && right instanceof ConstantExpression ? right
                    : frame.functionBinder.replaceConjunction(call, left, right);
        }
        if (call.argumentAt(0) instanceof ColumnExpression left && call.argumentAt(1) instanceof ColumnExpression right
                && left.isDirectReference() && right.isDirectReference() && left.getColumnId() == right.getColumnId()) {
            switch (call.getName()) {
                case "=" -> {
                    return null;
                }
                case "!=", "<>", ">", "<" -> {
                    return frame.functionBinder.newFalseConstant(call.getPosition());
                }
                default -> {
                }
            }
        }
        return predicate;
    }

    private int generateIndexedScan(
            GenerationFrame frame, int slot, ScanPlan scan, SqlExecutionContext executionContext, int order, IntervalExtractor scanIntervals,
            BoundExpression residual, GenericRecordMetadata metadata, RecordMetadata readerMetadata, TableReader reader,
            IntList indexes, IntList shifts, SortPlan orderAdvice, LimitPlan limitAdvice, int orderByMnemonic
    ) throws SqlException {
        final int keyIndex = scan.getOutput().getColumnIndexById(frame.symbols.getColumnId());
        final int readerKeyIndex = indexes.getQuick(keyIndex);
        final int keyCount = frame.symbols.getValues().size();
        final int intervalSlot = frame.resources.reserve();
        final var intervalModel = buildIntervals(frame, scanIntervals, reader);
        if (intervalModel != null) {
            frame.resources.own(intervalSlot, intervalModel);
        }
        final boolean isSinglePartition = intervalModel == null ? reader.getPartitionedBy() == PartitionBy.NONE
                : intervalModel.allIntervalsHitOnePartition();
        final int orderCount = orderAdvice == null ? 0 : orderAdvice.getColumnIds().size();
        int indexDirection = IndexReader.DIR_FORWARD;
        boolean isOrderByKey = false;
        boolean isOrderByTimestamp = false;
        final int timestampId = scan.getOutput().getTimestampColumnId();
        if (isSinglePartition && !executionContext.isTimestampRequired() && orderCount > 0 && orderCount < 3
                && orderAdvice.getColumnIds().getQuick(0) == frame.symbols.getColumnId()) {
            metadata.setTimestampIndex(-1);
            if (orderCount == 1) {
                isOrderByKey = true;
            } else if (orderAdvice.getColumnIds().getQuick(1) == timestampId) {
                isOrderByKey = true;
                if (orderAdvice.getDirections().getQuick(1) == QueryModel.ORDER_DIRECTION_DESCENDING) {
                    indexDirection = IndexReader.DIR_BACKWARD;
                }
            }
        }
        if (!isOrderByKey && orderCount == 1 && orderAdvice.getColumnIds().getQuick(0) == timestampId) {
            final boolean isDescending = orderAdvice.getDirections().getQuick(0) == QueryModel.ORDER_DIRECTION_DESCENDING;
            isOrderByTimestamp = keyCount == 1 || !isDescending;
            if (isOrderByTimestamp && isDescending) {
                indexDirection = IndexReader.DIR_BACKWARD;
            }
        }
        final int filterSlot = frame.resources.reserve();
        Function filter = residual == null ? null : frame.functionBinder.instantiate(residual, scan.getOutput(), metadata, executionContext);
        if (filter != null) {
            frame.resources.own(filterSlot, filter);
            if (filter.isConstant()) {
                final boolean isTrue = filter.getBool(null);
                frame.resources.detach(filterSlot);
                Misc.free(filter);
                filter = null;
                if (!isTrue) {
                    if (intervalModel != null) {
                        Misc.free(frame.resources.detach(intervalSlot));
                    }
                    frame.resources.own(slot, new EmptyTableRecordCursorFactory(metadata));
                    return slot;
                }
            }
        }
        if (keyCount == 0) {
            final ObjList<Function> excludedKeys = new ObjList<>(frame.symbols.getExcludedValues().size());
            final IntList excludedSlots = frame.symbolKeySlots;
            excludedSlots.clear();
            instantiateKeys(frame, frame.symbols.getExcludedValues(), excludedKeys, excludedSlots, scan, metadata, executionContext);
            final int frameSlot = frame.resources.reserve();
            frame.resources.own(frameSlot, newFrames(scan, intervalModel, readerMetadata, order));
            configurePushdown(frame, (PartitionFrameCursorFactory) frame.resources.resources.getQuick(frameSlot), residual, scan, metadata, indexes, reader, executionContext);
            if (intervalModel != null) {
                frame.resources.detach(intervalSlot);
            }
            final PartitionFrameCursorFactory frames = (PartitionFrameCursorFactory) frame.resources.detach(frameSlot);
            for (int i = 0, n = excludedSlots.size(); i < n; i++) {
                frame.resources.detach(excludedSlots.getQuick(i));
            }
            if (filter != null) {
                frame.resources.detach(filterSlot);
            }
            frame.resources.own(slot, new FilterOnExcludedValuesRecordCursorFactory(configuration, metadata, frames, excludedKeys,
                    keyIndex, filter, orderByMnemonic, isOrderByKey, isOrderByTimestamp,
                    orderCount == 0 ? QueryModel.ORDER_DIRECTION_ASCENDING : orderAdvice.getDirections().getQuick(0),
                    indexDirection, indexes, shifts, configuration.getMaxSymbolNotEqualsCount()));
            return slot;
        }
        final ObjList<Function> keys = new ObjList<>(keyCount);
        final IntList keySlots = frame.symbolKeySlots;
        keySlots.clear();
        for (int i = 0; i < keyCount; i++) {
            final int keySlot = frame.resources.reserve();
            keySlots.add(keySlot);
            final Function key = SymbolKeyExtractor.instantiateValue(frame.symbols.getValues().getQuick(i), scan.getOutput(), metadata, frame.functionBinder, executionContext);
            frame.resources.own(keySlot, key);
            keys.add(key);
        }
        final Function firstKey = keys.getQuick(0);
        final int symbolKey = keyCount > 1 || firstKey.isRuntimeConstant() ? SymbolTable.VALUE_NOT_FOUND
                : reader.getSymbolMapReader(readerKeyIndex).keyOf(firstKey.getStrA(null));
        final int[] coveringMapping = executionContext.isCoveringIndexEnabled() && !scan.isUpdate() && (keyCount == 1 || !isOrderByKey)
                && !scan.hasHint(ScanPlan.HINT_NO_COVERING)
                ? buildCoveringIndexMapping(reader, readerKeyIndex, indexes, metadata) : null;
        final int frameSlot = frame.resources.reserve();
        final PartitionFrameCursorFactory frames = newFrames(scan, intervalModel, readerMetadata, order);
        frame.resources.own(frameSlot, frames);
        configurePushdown(frame, frames, residual, scan, metadata, indexes, reader, executionContext);
        if (intervalModel != null) {
            frame.resources.detach(intervalSlot);
        }
        frame.resources.detach(frameSlot);
        for (int i = 0; i < keyCount; i++) {
            frame.resources.detach(keySlots.getQuick(i));
        }
        if (filter != null && coveringMapping == null) {
            frame.resources.detach(filterSlot);
        }
        if (keyCount == 1) {
            frame.resources.own(slot, generateSingleSymbolIndexScan(metadata, frames, keyIndex, symbolKey, firstKey,
                    coveringMapping == null ? filter : null, indexDirection, isOrderByKey || isOrderByTimestamp,
                    indexes, shifts, coveringMapping, scan.hasHint(ScanPlan.HINT_FORCE_USE_COVERING)));
        } else {
            frame.resources.own(slot, generateSymbolValuesIndexScan(metadata, frames, keys, keyIndex, reader,
                    coveringMapping == null ? filter : null, orderByMnemonic, isOrderByKey, isOrderByTimestamp,
                    orderCount == 0 ? QueryModel.ORDER_DIRECTION_ASCENDING : orderAdvice.getDirections().getQuick(0),
                    indexDirection, indexes, shifts, coveringMapping, scan.hasHint(ScanPlan.HINT_FORCE_USE_COVERING)));
        }
        if (filter != null && coveringMapping != null) {
            final int filteredSlot = frame.resources.reserve();
            final CoveringIndexRecordCursorFactory base = (CoveringIndexRecordCursorFactory) frame.resources.detach(slot);
            frame.resources.detach(filterSlot);
            final boolean isLimitOrderPreserved = orderCount == 0 || orderCount == 1
                    && orderAdvice.getColumnIds().getQuick(0) == timestampId
                    && orderAdvice.getDirections().getQuick(0) == QueryModel.ORDER_DIRECTION_ASCENDING;
            frame.resources.own(filteredSlot, filterGenerator.generateCovering(residual, scan.getOutput(), base, filter,
                    frame.functionBinder, executionContext, isLimitOrderPreserved ? limitAdvice : null, scan.hasHint(ScanPlan.HINT_PRE_TOUCH)));
            return filteredSlot;
        }
        return slot;
    }

    private int generateSubqueryScan(GenerationFrame frame, int slot, ScanPlan scan, SqlExecutionContext executionContext, int order,
                                     IntervalExtractor scanIntervals, BoundExpression residual, CursorExpression keySubquery, GenericRecordMetadata metadata,
                                     GenericRecordMetadata readerMetadata, TableReader reader, IntList indexes, IntList shifts) throws SqlException {
        final int frameSlot = frame.resources.reserve();
        frame.resources.own(frameSlot, newFrames(scan, buildIntervals(frame, scanIntervals, reader),
                readerMetadata, order));
        final PartitionFrameCursorFactory frames = (PartitionFrameCursorFactory) frame.resources.resources.getQuick(frameSlot);
        configurePushdown(frame, frames, residual, scan, metadata, indexes, reader, executionContext);
        final int filterSlot = frame.resources.reserve();
        final Function filter = residual == null ? null : frame.functionBinder.instantiate(residual, scan.getOutput(), metadata, executionContext);
        if (filter != null) {
            frame.resources.own(filterSlot, filter);
        }
        final int subquerySlot = frame.resources.reserve();
        final RecordCursorFactory subquery = frame.functionBinder.generateSubquery(keySubquery, executionContext);
        frame.resources.own(subquerySlot, subquery);
        if (filter != null) {
            frame.resources.detach(filterSlot);
        }
        frame.resources.detach(frameSlot);
        frame.resources.detach(subquerySlot);
        frame.resources.own(slot, new FilterOnSubQueryRecordCursorFactory(configuration, metadata, frames, subquery,
                scan.getOutput().getColumnIndexById(frame.symbols.getColumnId()), filter,
                subqueryKeyGetter(subquery.getMetadata().getColumnType(0)), indexes, shifts));
        return slot;
    }

    /**
     * Returns false, owning nothing new, when no indexed SYMBOL pattern conjunct can drive the scan.
     */
    private boolean generateSymbolPatternIndex(
            GenerationFrame frame, int slot, ScanPlan scan, SqlExecutionContext executionContext, int order, IntervalExtractor scanIntervals,
            BoundExpression predicate, GenericRecordMetadata metadata, RecordMetadata readerMetadata, TableReader reader,
            IntList indexes, IntList shifts, SortPlan orderAdvice, LimitPlan limitAdvice, int orderByMnemonic
    ) throws SqlException {
        final boolean isOrderByTimestampOnly = orderAdvice != null && orderAdvice.getColumnIds().size() == 1
                && orderAdvice.getColumnIds().getQuick(0) == scan.getNativeTimestampColumnId();
        if (isOrderByTimestampOnly && limitAdvice == null
                && orderAdvice.getDirections().getQuick(0) == QueryModel.ORDER_DIRECTION_DESCENDING) {
            return false;
        }
        frame.patternConjuncts.clear();
        LogicalPlans.collectConjuncts(predicate, frame.patternConjuncts);
        final OutputSchema input = scan.getOutput();
        frame.patternIndex = -1;
        for (int i = 0, n = frame.patternConjuncts.size(); i < n && frame.patternIndex < 0; i++) {
            final BoundExpression conjunct = frame.patternConjuncts.getQuick(i);
            if (isIndexedSymbolPattern(conjunct, input, metadata)) {
                frame.patternIndex = i;
                frame.isPatternNegated = false;
            } else if (isNegatedIndexedSymbolPattern(conjunct, input, metadata)) {
                frame.patternIndex = i;
                frame.isPatternNegated = true;
            }
        }
        if (frame.patternIndex < 0) {
            return false;
        }
        if (limitAdvice != null && limitAdvice.getHi() == null) {
            final Function lo = frame.functionBinder.instantiate(limitAdvice.getLo(), emptySchema, executionContext);
            try {
                if (filterGenerator.mayBeNegativeLimit(lo, executionContext)) {
                    return false;
                }
            } finally {
                Misc.free(lo);
            }
        }
        final BoundExpression pattern = frame.patternConjuncts.getQuick(frame.patternIndex);
        if (pattern instanceof FunctionExpression call && "!~".equals(call.getName())) {
            Misc.free(frame.functionBinder.instantiate(pattern, input, metadata, executionContext));
        }
        AdaptiveSymbolPatternRecordCursorFactory.PreparedSymbolPatternFilter filter = preparePatternFilter(frame, input, metadata, executionContext);
        if (filter == null) {
            return false;
        }
        ObjList<Function> workerFilters = null;
        try {
            final boolean isCoveringAllowed = executionContext.isCoveringIndexEnabled() && !scan.hasHint(ScanPlan.HINT_NO_COVERING);
            final boolean hasCovering = symbolPatternCoveringMapping(reader, filter.getSymbolColumnIndex(), indexes,
                    metadata, frame.isPatternNegated, isCoveringAllowed) != null;
            if (!filter.isThreadSafe() && executionContext.isParallelFilterEnabled()) {
                if (!hasCovering) {
                    filter = Misc.free(filter);
                    return false;
                }
                final int workerCount = executionContext.getSharedQueryWorkerCount();
                workerFilters = new ObjList<>(workerCount);
                for (int i = 0; i < workerCount; i++) {
                    workerFilters.add(preparePatternFilter(frame, input, metadata, executionContext));
                }
            }
            final IntHashSet filterColumns = new IntHashSet();
            FilterFactoryGenerator.collectColumnIndexes(predicate, input, filterColumns);
            final PartitionFrameCursorFactory frames = newFrames(scan,
                    buildIntervals(frame, scanIntervals, reader), readerMetadata, order);
            try {
                configurePushdown(frame, frames, predicate, scan, metadata, indexes, reader, executionContext);
            } catch (Throwable th) {
                Misc.free(frames, th);
                throw th;
            }
            final AdaptiveSymbolPatternRecordCursorFactory.PreparedSymbolPatternFilter ownedFilter = filter;
            final ObjList<Function> ownedWorkers = workerFilters;
            filter = null;
            workerFilters = null;
            frame.resources.own(slot, generateSymbolPatternIndex(frames, metadata, reader, indexes, shifts, ownedFilter,
                    filterColumns, ownedWorkers, orderByMnemonic, isOrderByTimestampOnly, isCoveringAllowed,
                    scan.hasHint(ScanPlan.HINT_PRE_TOUCH), executionContext));
            return true;
        } catch (Throwable th) {
            Misc.free(filter, th);
            Misc.freeObjList(workerFilters, th);
            throw th;
        }
    }

    private int generateTableScan(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext, int order, IntervalExtractor scanIntervals,
                                  LatestByPlan latest, BoundExpression latestResidual, SymbolKeyExtractor latestKeys,
                                  SortPlan orderAdvice, LimitPlan limitAdvice, int orderByMnemonic) throws SqlException {
        final boolean isOverridden = executionContext.isOverriddenIntrinsics(scan.getTableToken()) && !isWalClientUpdate(scan, executionContext);
        final WindowJoinStep step = frame.joinIntervalStep;
        frame.joinIntervalStep = null;
        final boolean isJoinIntervalMerged = step != null && !isWalClientUpdate(scan, executionContext)
                && frame.joinIntervals.getTimestampDriver().getTimestampType() == scan.getNativeTimestampType();
        if (!isOverridden && !isJoinIntervalMerged) {
            return generateTableScan0(frame, scan, executionContext, order, scanIntervals, latest, latestResidual, latestKeys,
                    orderAdvice, limitAdvice, orderByMnemonic);
        }
        final int timestampType = scan.getNativeTimestampType();
        final IntervalExtractor rangeIntervals = scanIntervals != null ? scanIntervals : frame.overrideIntervals;
        try {
            if (scanIntervals == null) {
                frame.overrideIntervals.of(timestampType);
            }
            if (isOverridden) {
                rangeIntervals.override(scan.getTableToken(), executionContext);
            }
            if (isJoinIntervalMerged) {
                long hi = step.getHi();
                if (step.getHiTimeUnit() != 0) {
                    hi = WindowContextImpl.toTimestampUnits(timestampType, hi, step.getHiTimeUnit(), step.getHiPosition(), "end");
                }
                long lo = Numbers.LONG_NULL;
                if (!step.isIncludePrevailing()) {
                    lo = step.getLo();
                    if (step.getLoTimeUnit() != 0) {
                        lo = WindowContextImpl.toTimestampUnits(timestampType, lo, step.getLoTimeUnit(), step.getLoPosition(), "start");
                    }
                }
                rangeIntervals.merge((RuntimeIntervalModel) frame.joinIntervals, lo, hi);
            }
            return generateTableScan0(frame, scan, executionContext, order, rangeIntervals, latest, latestResidual, latestKeys,
                    orderAdvice, limitAdvice, orderByMnemonic);
        } catch (Throwable th) {
            Misc.clear(frame.overrideIntervals, th);
            throw th;
        } finally {
            frame.overrideIntervals.clear();
        }
    }

    private int generateTableScan0(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext, int order, IntervalExtractor scanIntervals,
                                   LatestByPlan latest, BoundExpression latestResidual, SymbolKeyExtractor latestKeys,
                                   SortPlan orderAdvice, LimitPlan limitAdvice, int orderByMnemonic) throws SqlException {
        final int slot = frame.resources.reserve();
        if (isWalClientUpdate(scan, executionContext)) {
            // Client-side WAL UPDATE validates against sequencer metadata. The data
            // reader may still have an older schema; rows are read only during WAL apply.
            try (TableRecordMetadata tableMetadata = executionContext.getMetadataForWrite(scan.getTableToken(), scan.getMetadataVersion())) {
                final GenericRecordMetadata metadata = new GenericRecordMetadata();
                for (int i = 0, n = scan.getOutput().getColumnCount(); i < n; i++) {
                    final int index = tableMetadata.getColumnIndex(scan.getOutput().getColumnName(i));
                    metadata.add(SqlCodeGenerator.copyColumn(tableMetadata, index, tableMetadata.getColumnName(index)));
                }
                metadata.setTimestampIndex(scan.getOutput().getTimestampIndex());
                frame.resources.own(slot, new EmptyTableRecordCursorFactory(metadata, tableMetadata.getTableToken()));
            }
        } else {
            // Validate the bound version before constructing independently owned metadata.
            try (TableReader reader = executionContext.getReader(scan.getTableToken(), scan.getMetadataVersion())) {
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
                metadata.setTimestampIndex(scan.getOutput().getTimestampIndex());
                if (scanIntervals != null && scanIntervals.isIntrinsicFalse() || latestKeys != null && latestKeys.isFalse()) {
                    frame.resources.own(slot, new EmptyTableRecordCursorFactory(metadata));
                    return slot;
                }
                if (latest != null) {
                    final IntList keyIndexes = new IntList(latest.getKeyColumnIds().size());
                    for (int i = 0, n = latest.getKeyColumnIds().size(); i < n; i++) {
                        keyIndexes.add(scan.getOutput().getColumnIndexById(latest.getKeyColumnIds().getQuick(i)));
                    }
                    final boolean isIndexedAllowed = scanIntervals == null || configuration.useWithinLatestByOptimisation();
                    if (latestResidual != null && latestResidual == frame.latestWithin
                            && (latestKeys == null || !latestKeys.hasKey() && latestKeys.getSubquery() == null)
                            && LatestByFactoryGenerator.isIndexedScan(metadata, keyIndexes, isIndexedAllowed && !scan.hasHint(ScanPlan.HINT_NO_INDEX))) {
                        latestResidual = null;
                    } else {
                        frame.latestPrefixes.clear();
                    }
                    final int filterSlot = frame.resources.reserve();
                    final Function filter = latestResidual == null ? null : frame.functionBinder.instantiate(latestResidual, scan.getOutput(), metadata, executionContext);
                    if (filter != null) {
                        frame.resources.own(filterSlot, filter);
                    }
                    final ObjList<Function> keys = new ObjList<>();
                    final ObjList<Function> excludedKeys = new ObjList<>();
                    final IntList keySlots = frame.symbolKeySlots;
                    keySlots.clear();
                    if (latestKeys != null) {
                        instantiateKeys(frame, latestKeys.getValues(), keys, keySlots, scan, metadata, executionContext);
                        instantiateKeys(frame, latestKeys.getExcludedValues(), excludedKeys, keySlots, scan, metadata, executionContext);
                    }
                    final int frameSlot = frame.resources.reserve();
                    frame.resources.own(frameSlot, newFrames(scan, buildIntervals(frame, scanIntervals, reader),
                            readerMetadata, PartitionFrameCursorFactory.ORDER_DESC));
                    final PartitionFrameCursorFactory frames = (PartitionFrameCursorFactory) frame.resources.resources.getQuick(frameSlot);
                    if (latestResidual != null && (latestResidual.getFunctionFlags() & (BoundExpression.CONSTANT | BoundExpression.RUNTIME_CONSTANT)) == 0) {
                        configurePushdown(frame, frames, latestResidual, scan, metadata, indexes, reader, executionContext);
                    }
                    final CursorExpression keySubquery = latestKeys == null ? null : latestKeys.getSubquery();
                    if (keySubquery != null) {
                        final int keyIndex = keyIndexes.getQuick(0);
                        final int subquerySlot = frame.resources.reserve();
                        final RecordCursorFactory subquery = frame.functionBinder.generateSubquery(keySubquery, executionContext);
                        frame.resources.own(subquerySlot, subquery);
                        if (filter != null) {
                            frame.resources.detach(filterSlot);
                        }
                        frame.resources.detach(frameSlot);
                        frame.resources.detach(subquerySlot);
                        frame.resources.own(slot, new LatestBySubQueryRecordCursorFactory(configuration, metadata, frames, keyIndex, subquery, filter,
                                !scan.hasHint(ScanPlan.HINT_NO_INDEX) && metadata.isColumnIndexed(keyIndex),
                                subqueryKeyGetter(subquery.getMetadata().getColumnType(0)), indexes, shifts));
                        return slot;
                    }
                    if (filter != null) {
                        frame.resources.detach(filterSlot);
                    }
                    for (int i = 0, n = keySlots.size(); i < n; i++) {
                        frame.resources.detach(keySlots.getQuick(i));
                    }
                    frame.resources.detach(frameSlot);
                    frame.resources.own(slot, latestByGenerator.generateLatestByScan(
                            frames, metadata, reader, indexes, shifts, keyIndexes,
                            isIndexedAllowed,
                            filter, keys, excludedKeys, frame.latestPrefixes,
                            latestResidual == null ? null : symbolCounts(latestResidual, latest.getKeyColumnIds()),
                            !scan.hasHint(ScanPlan.HINT_NO_INDEX),
                            executionContext.isCoveringIndexEnabled() && !scan.hasHint(ScanPlan.HINT_NO_COVERING),
                            scan.hasHint(ScanPlan.HINT_FORCE_USE_COVERING), executionContext
                    ));
                    return slot;
                }
                if (latestResidual != null && !executionContext.isLiveViewCompile() && !scan.hasHint(ScanPlan.HINT_NO_INDEX)) {
                    latestResidual = frame.symbols.extractIndexed(latestResidual, scan.getOutput(), metadata, reader, frame.functionBinder);
                    if (frame.symbols.isFalse()) {
                        frame.resources.own(slot, new EmptyTableRecordCursorFactory(metadata));
                        return slot;
                    }
                    final CursorExpression keySubquery = frame.symbols.getSubquery();
                    if (keySubquery != null) {
                        return generateSubqueryScan(frame, slot, scan, executionContext, order, scanIntervals, latestResidual, keySubquery,
                                metadata, readerMetadata, reader, indexes, shifts);
                    }
                    if (!frame.symbols.hasKey() && configuration.isSymbolPatternIndexEnabled() && !scan.isUpdate()
                            && !scan.hasHint(ScanPlan.HINT_NO_SYMBOL_PATTERN_INDEX)
                            && generateSymbolPatternIndex(frame, slot, scan, executionContext, order, scanIntervals, latestResidual,
                            metadata, readerMetadata, reader, indexes, shifts, orderAdvice, limitAdvice, orderByMnemonic)) {
                        return slot;
                    }
                    if (frame.symbols.hasKey()) {
                        if (frame.symbols.getValues().size() > 0 || reader.getSymbolMapReader(indexes.getQuick(
                                scan.getOutput().getColumnIndexById(frame.symbols.getColumnId()))).getSymbolCount() < configuration.getMaxSymbolNotEqualsCount()) {
                            return generateIndexedScan(frame, slot, scan, executionContext, order, scanIntervals, latestResidual,
                                    metadata, readerMetadata, reader, indexes, shifts, orderAdvice, limitAdvice, orderByMnemonic);
                        }
                        latestResidual = restoreExclusions(frame, latestResidual);
                    }
                }
                final int frameSlot = frame.resources.reserve();
                final var intervalModel = buildIntervals(frame, scanIntervals, reader);
                frame.resources.own(frameSlot, newFrames(scan, intervalModel, readerMetadata, order));
                final int sortedKeyIndex = sortedSymbolIndexKey(scan, intervalModel, latestResidual, metadata, orderAdvice, executionContext);
                if (sortedKeyIndex >= 0) {
                    final boolean isTimestampDescending = orderAdvice.getColumnIds().size() == 2
                            && orderAdvice.getDirections().getQuick(1) == QueryModel.ORDER_DIRECTION_DESCENDING;
                    metadata.setTimestampIndex(-1);
                    frame.resources.own(slot, new SortedSymbolIndexRecordCursorFactory(configuration, metadata,
                            (PartitionFrameCursorFactory) frame.resources.detach(frameSlot), sortedKeyIndex,
                            orderAdvice.getDirections().getQuick(0) == QueryModel.ORDER_DIRECTION_ASCENDING,
                            isTimestampDescending ? IndexReader.DIR_BACKWARD : IndexReader.DIR_FORWARD, indexes, shifts));
                    return slot;
                }
                configurePushdown(frame, (PartitionFrameCursorFactory) frame.resources.resources.getQuick(frameSlot), latestResidual, scan,
                        metadata, indexes, reader, executionContext);
                frame.resources.own(slot, generateScan((PartitionFrameCursorFactory) frame.resources.detach(frameSlot), metadata,
                        order, order == PartitionFrameCursorFactory.ORDER_DESC, indexes, shifts, scan.isRandomAccess()));
                if (latestResidual != null) {
                    final int filteredSlot = frame.resources.reserve();
                    final int filterSlot = frame.resources.reserve();
                    final Function filter = frame.functionBinder.instantiate(latestResidual, scan.getOutput(), metadata, executionContext);
                    frame.resources.own(filterSlot, filter);
                    final RecordCursorFactory base = (RecordCursorFactory) frame.resources.detach(slot);
                    frame.resources.detach(filterSlot);
                    frame.resources.own(filteredSlot, filterGenerator.generate(frame, latestResidual, scan.getOutput(), base, filter,
                            frame.functionBinder, executionContext, scan.isUpdate(), isLimitOrderPreserved(scan, order, orderAdvice) ? limitAdvice : null,
                            scan.hasHint(ScanPlan.HINT_PRE_TOUCH)));
                    return filteredSlot;
                }
            }
        }
        return slot;
    }

    private void instantiateKeys(GenerationFrame frame, ObjList<BoundExpression> values, ObjList<Function> keys, IntList slots, ScanPlan scan,
                                 RecordMetadata metadata, SqlExecutionContext executionContext) throws SqlException {
        for (int i = 0, n = values.size(); i < n; i++) {
            final int slot = frame.resources.reserve();
            slots.add(slot);
            final Function key = SymbolKeyExtractor.instantiateValue(values.getQuick(i), scan.getOutput(), metadata, frame.functionBinder, executionContext);
            frame.resources.own(slot, key);
            keys.add(key);
        }
    }

    private Function positivePattern(GenerationFrame frame, FunctionExpression pattern, OutputSchema input, RecordMetadata metadata,
                                     SqlExecutionContext executionContext) throws SqlException {
        if (!frame.isPatternNegated) {
            return frame.functionBinder.instantiate(pattern, input, metadata, executionContext);
        }
        if (pattern.getArgumentCount() == 1) {
            return frame.functionBinder.instantiate(pattern.argumentAt(0), input, metadata, executionContext);
        }
        frame.patternArguments.clear();
        frame.patternPositions.clear();
        try {
            for (int i = 0; i < 2; i++) {
                frame.patternArguments.add(frame.functionBinder.instantiate(pattern.argumentAt(i), input, metadata, executionContext));
                frame.patternPositions.add(pattern.getArgumentPosition(i));
            }
            final Function match = matchSymbolFactory.newInstance(pattern.getPosition(), frame.patternArguments, frame.patternPositions,
                    configuration, executionContext);
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
            GenerationFrame frame, OutputSchema input, RecordMetadata metadata, SqlExecutionContext executionContext
    ) throws SqlException {
        final FunctionExpression pattern = (FunctionExpression) frame.patternConjuncts.getQuick(frame.patternIndex);
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
                if (i != frame.patternIndex) {
                    residualExpression = frame.functionBinder.combineConjunction(frame.patternConjuncts.getQuick(i), residualExpression, 0);
                }
            }
            if (residualExpression != null) {
                residual = frame.functionBinder.instantiate(residualExpression, input, metadata, executionContext);
            }
            final FunctionExpression positive = pattern.getArgumentCount() == 1 ? (FunctionExpression) pattern.argumentAt(0) : pattern;
            final int keyIndex = input.getColumnIndexById(((ColumnExpression) positive.argumentAt(0)).getColumnId());
            final AdaptiveSymbolPatternRecordCursorFactory.PreparedSymbolPatternFilter filter =
                    new AdaptiveSymbolPatternRecordCursorFactory.PreparedSymbolPatternFilter(provider, residual, frame.isPatternNegated, keyIndex, null, null);
            provider = null;
            residual = null;
            return filter;
        } catch (Throwable th) {
            Misc.free(provider, th);
            Misc.free(residual, th);
            throw th;
        }
    }

    private BoundExpression restoreExclusions(GenerationFrame frame, BoundExpression residual) throws SqlException {
        final ObjList<BoundExpression> conjuncts = frame.symbols.getExcludedConjuncts();
        BoundExpression root = conjuncts.getQuick(0);
        for (int i = 1, n = conjuncts.size(); i < n; i++) {
            root = frame.functionBinder.combineConjunction(conjuncts.getQuick(i), root, 0);
        }
        return frame.functionBinder.combineConjunction(residual, root, 0);
    }

    /**
     * Checks if all selected columns can be served from the covering index
     * sidecar. Returns a mapping array (query col → include idx, or -1 for the
     * indexed symbol column) if fully covered, or null otherwise.
     */
    static int[] buildCoveringIndexMapping(
            TableReader reader,
            int keyReaderColIdx,
            IntList columnIndexes,
            RecordMetadata queryMeta
    ) {
        IntList coveringIndices = reader.getMetadata().getColumnMetadata(keyReaderColIdx).getCoveringColumnIndices();
        if (coveringIndices == null || coveringIndices.size() == 0) {
            return null;
        }
        int queryColCount = queryMeta.getColumnCount();
        int[] mapping = new int[queryColCount];
        for (int q = 0; q < queryColCount; q++) {
            int readerColIdx = columnIndexes.getQuick(q);
            if (readerColIdx == keyReaderColIdx) {
                mapping[q] = -1; // symbol column — value known from WHERE key
                continue;
            }
            int writerColIdx = reader.getMetadata().getWriterIndex(readerColIdx);
            int includeIdx = -1;
            for (int c = 0, cn = coveringIndices.size(); c < cn; c++) {
                if (coveringIndices.getQuick(c) == writerColIdx) {
                    includeIdx = c;
                    break;
                }
            }
            if (includeIdx < 0) {
                return null; // not covered — fallback to regular scan
            }
            mapping[q] = includeIdx;
        }
        return mapping;
    }

    static int[] symbolPatternCoveringMapping(TableReader reader, int keyColumnIndex, IntList columnIndexes, RecordMetadata metadata,
                                              boolean isNegated, boolean isCoveringAllowed) {
        return isNegated || !isCoveringAllowed ? null
                : buildCoveringIndexMapping(reader, columnIndexes.getQuick(keyColumnIndex), columnIndexes, metadata);
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

    /**
     * Whether any element of an IN-list key can resolve to NULL, and so make the scan ask for
     * the NULL key. See {@link #canKeyBeNull}: a literal {@code null} resolves here, a runtime
     * constant does not resolve until it is bound.
     */
    static boolean canAnyKeyBeNull(ObjList<Function> keyValueFuncs, SymbolMapReader symbolMapReader) {
        for (int i = 0, n = keyValueFuncs.size(); i < n; i++) {
            final Function f = keyValueFuncs.getQuick(i);
            if (f.isRuntimeConstant() || symbolMapReader.keyOf(f.getStrA(null)) == SymbolTable.VALUE_IS_NULL) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether a covering scan on this key may have to answer the NULL key, and therefore needs
     * a backup plan for partitions that carry a column top. True for a literal {@code null},
     * which resolves to {@code VALUE_IS_NULL} at compile time, and for a runtime constant,
     * whose value is not known until it is bound. A literal that names a real symbol -- or one
     * that names no symbol at all -- can never be NULL and needs nothing.
     */
    static boolean canKeyBeNull(int symbolKey, Function symbolFunc) {
        return symbolKey == SymbolTable.VALUE_IS_NULL || symbolFunc.isRuntimeConstant();
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
        try {
            return new PageFrameRecordCursorFactory(configuration, queryMetadata, frames, new PageFrameRowCursorFactory(order),
                    isFollowingOrderByAdvice, null, true, columnIndexes, columnSizeShifts, isRandomAccessSupported, false);
        } catch (Throwable th) {
            Misc.free(frames, th);
            throw th;
        }
    }

    /**
     * Consumes frames, key and filter on entry; covering scans leave filtering to their caller.
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
            boolean isBackupSuppressed
    ) {
        RecordCursorFactory backup = null;
        RecordCursorFactory result = null;
        RowCursorFactory rows = null;
        try {
            if (coveringMapping != null) {
                assert filter == null;
                final PartitionFrameCursorFactory sharedFrames = frames;
                final Function sharedKey = key;
                if (!isBackupSuppressed && canKeyBeNull(symbolKey, key)) {
                    backup = buildSingleSymbolIndexScan(configuration, metadata, frames, keyIndex, symbolKey,
                            key, indexDirection, followsOrderByAdvice, columnIndexes, columnSizeShifts);
                    frames = null;
                    key = null;
                }
                result = new CoveringIndexRecordCursorFactory(metadata, sharedFrames, columnIndexes.getQuick(keyIndex),
                        symbolKey, sharedKey, columnIndexes, coveringMapping, null, null, false, null, null,
                        backup, true, backup == null && canKeyBeNull(symbolKey, sharedKey));
                backup = null;
                frames = null;
                key = null;
                return result;
            }
            if (filter == null) {
                result = buildSingleSymbolIndexScan(configuration, metadata, frames, keyIndex, symbolKey,
                        key, indexDirection, followsOrderByAdvice, columnIndexes, columnSizeShifts);
                frames = null;
                key = null;
                return result;
            }
            if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                rows = new DeferredSymbolIndexFilteredRowCursorFactory(keyIndex, key, filter, indexDirection);
                key = null;
            } else {
                rows = new SymbolIndexFilteredRowCursorFactory(keyIndex, symbolKey, filter, indexDirection, null);
            }
            result = new PageFrameRecordCursorFactory(configuration, metadata, frames, rows, followsOrderByAdvice,
                    filter, false, columnIndexes, columnSizeShifts, true, false);
            rows = null;
            frames = null;
            filter = null;
            final Function resolvedKey = key;
            key = null;
            Misc.free(resolvedKey);
            return result;
        } catch (Throwable th) {
            Misc.free(result, th);
            Misc.free(backup, th);
            Misc.free(rows, th);
            Misc.free(frames, th);
            Misc.free(filter, th);
            Misc.free(key, th);
            throw th;
        }
    }

    /**
     * Consumes the frames, the prepared filter and the worker filters on entry, including on failure.
     */
    RecordCursorFactory generateSymbolPatternIndex(
            PartitionFrameCursorFactory frames,
            GenericRecordMetadata metadata,
            TableReader reader,
            IntList columnIndexes,
            IntList columnSizeShifts,
            AdaptiveSymbolPatternRecordCursorFactory.PreparedSymbolPatternFilter filter,
            IntHashSet filterColumnIndexes,
            @Nullable ObjList<Function> workerFilters,
            int orderByMnemonic,
            boolean isOrderByTimestampOnly,
            boolean isCoveringAllowed,
            boolean isPreTouchEnabled,
            SqlExecutionContext executionContext
    ) {
        RecordCursorFactory coveringDelegate = null;
        RecordCursorFactory indexDelegate = null;
        RecordCursorFactory scanDelegate = null;
        AdaptiveSymbolPatternRecordCursorFactory adaptive = null;
        boolean isFilterOwned = false;
        try {
            final int keyColumnIndex = filter.getSymbolColumnIndex();
            final boolean isNegated = filter.isNegated();
            final IntList effectiveKeys = new IntList();
            final AdaptiveSymbolPatternRecordCursorFactory.NonOwningPartitionFrameCursorFactory sharedFrames =
                    new AdaptiveSymbolPatternRecordCursorFactory.NonOwningPartitionFrameCursorFactory(frames);
            indexDelegate = new SymbolPatternIndexRecordCursorFactory(configuration, metadata, sharedFrames, keyColumnIndex,
                    effectiveKeys, orderByMnemonic, isOrderByTimestampOnly, IndexReader.DIR_FORWARD, columnIndexes, columnSizeShifts);
            final int[] coveringMapping = symbolPatternCoveringMapping(reader, keyColumnIndex, columnIndexes, metadata, isNegated, isCoveringAllowed);
            if (coveringMapping != null) {
                coveringDelegate = new CoveringIndexRecordCursorFactory(metadata, sharedFrames, columnIndexes.getQuick(keyColumnIndex),
                        SymbolTable.VALUE_NOT_FOUND, null, columnIndexes, coveringMapping, null, reader, false, null,
                        effectiveKeys, null, false, false);
            }
            scanDelegate = new PageFrameRecordCursorFactory(configuration, metadata, sharedFrames,
                    new PageFrameRowCursorFactory(frames.getOrder()), false, null, true, columnIndexes, columnSizeShifts, true, false);
            final boolean isParallel = executionContext.isParallelFilterEnabled();
            final boolean isSelfFiltering = coveringDelegate == null && isParallel && filter.isThreadSafe();
            if (isSelfFiltering) {
                scanDelegate = new AsyncFilteredRecordCursorFactory(executionContext.getCairoEngine(), configuration,
                        executionContext.getMessageBus(), scanDelegate, filter, filterColumnIndexes, reduceTaskFactory, null,
                        null, 0, executionContext.getSharedQueryWorkerCount(), isPreTouchEnabled);
                isFilterOwned = true;
            }
            adaptive = new AdaptiveSymbolPatternRecordCursorFactory(metadata, frames, sharedFrames, columnIndexes, effectiveKeys,
                    columnIndexes.getQuick(keyColumnIndex), isNegated, configuration.getSymbolPatternIndexThreshold(), filter,
                    isSelfFiltering, indexDelegate, coveringDelegate, scanDelegate);
            frames = null;
            indexDelegate = null;
            coveringDelegate = null;
            scanDelegate = null;
            if (isSelfFiltering) {
                Misc.freeObjList(workerFilters);
                return adaptive;
            }
            final RecordCursorFactory result = isParallel && adaptive.supportsPageFrameCursor()
                    ? new AsyncFilteredRecordCursorFactory(executionContext.getCairoEngine(), configuration, executionContext.getMessageBus(),
                    adaptive, filter, filterColumnIndexes, reduceTaskFactory, workerFilters, null, 0,
                    executionContext.getSharedQueryWorkerCount(), isPreTouchEnabled)
                    : new FilteredRecordCursorFactory(adaptive, filter);
            if (result instanceof FilteredRecordCursorFactory) {
                Misc.freeObjList(workerFilters);
            }
            return result;
        } catch (Throwable th) {
            Misc.free(coveringDelegate, th);
            Misc.free(indexDelegate, th);
            Misc.free(scanDelegate, th);
            Misc.free(adaptive, th);
            Misc.free(frames, th);
            Misc.freeObjList(workerFilters, th);
            if (!isFilterOwned) {
                Misc.free(filter, th);
            }
            throw th;
        }
    }

    /**
     * Consumes frames, key functions and filter; the key list and reader are borrowed.
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
            boolean isBackupSuppressed
    ) {
        RecordCursorFactory backup = null;
        try {
            if (coveringMapping == null) {
                return new FilterOnValuesRecordCursorFactory(configuration, metadata, frames, keys, keyIndex,
                        reader, filter, orderByMnemonic, isOrderByKey, isOrderByTimestamp, orderDirection,
                        indexDirection, columnIndexes, columnSizeShifts);
            }
            assert filter == null;
            final int readerKeyIndex = columnIndexes.getQuick(keyIndex);
            final boolean hasNullableKey = canAnyKeyBeNull(keys, reader.getSymbolMapReader(readerKeyIndex));
            final PartitionFrameCursorFactory sharedFrames = frames;
            final ObjList<Function> sharedKeys = keys;
            if (hasNullableKey && !isBackupSuppressed) {
                backup = new FilterOnValuesRecordCursorFactory(configuration, metadata, frames, keys, keyIndex,
                        reader, null, orderByMnemonic, isOrderByKey, isOrderByTimestamp, orderDirection,
                        indexDirection, columnIndexes, columnSizeShifts);
                frames = null;
                keys = null;
            }
            return new CoveringIndexRecordCursorFactory(metadata, sharedFrames, readerKeyIndex,
                    SymbolTable.VALUE_NOT_FOUND, null, columnIndexes, coveringMapping, sharedKeys, reader,
                    false, null, null, backup, true, hasNullableKey && isBackupSuppressed);
        } catch (Throwable th) {
            Misc.free(backup, th);
            Misc.free(frames, th);
            Misc.freeObjList(keys, th);
            Misc.free(filter, th);
            throw th;
        }
    }
}
