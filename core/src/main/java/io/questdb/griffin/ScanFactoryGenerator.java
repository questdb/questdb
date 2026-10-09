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
import io.questdb.cairo.TableToken;
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
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SortDirection;
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

    private static boolean addWithinPrefix(ConstantExpression prefix, int columnType, LongList prefixes) {
        try {
            GeoHashes.addNormalizedGeoPrefix(prefix.getLongValue(), prefix.getDataType(), columnType, prefixes);
            return true;
        } catch (NumericException e) {
            return false;
        }
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
                && (orderAdvice.getDirections().getQuick(0) == SortDirection.DESCENDING)
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
        final PartitionFrameCursorFactory frames = intervalModel == null
                ? new FullPartitionFrameCursorFactory(scan.getTableToken(), scan.getMetadataVersion(), readerMetadata, order,
                scan.getViewName(), scan.getViewPosition(), scan.isUpdate())
                : new IntervalPartitionFrameCursorFactory(scan.getTableToken(), scan.getMetadataVersion(), intervalModel,
                readerMetadata.getTimestampIndex(), readerMetadata, order, scan.getViewName(), scan.getViewPosition(), scan.isUpdate());
        frames.setAuthorizedColumnIndexes(scan.getAuthorizedColumnIndexes());
        return frames;
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

    private RecordCursorFactory filterScan(GenerationFrame frame, RecordCursorFactory base, ScanPlan scan, BoundExpression residual, int order,
                                           SortPlan orderAdvice, LimitPlan limitAdvice, SqlExecutionContext executionContext) throws SqlException {
        final Function filter;
        try {
            filter = frame.functionInstantiator.instantiate(residual, scan.getOutput(), base.getMetadata(), executionContext);
        } catch (Throwable th) {
            Misc.free(base, th);
            throw th;
        }
        return filterGenerator.generate(frame, residual, scan.getOutput(), base, filter, frame.functionInstantiator, executionContext,
                scan.isUpdate(), isLimitOrderPreserved(scan, order, orderAdvice) ? limitAdvice : null, scan.hasHint(ScanPlan.HINT_PRE_TOUCH));
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
                    : frame.expressionRewriter.replaceConjunction(call, left, right);
        }
        if (call.argumentAt(0) instanceof ColumnExpression left && call.argumentAt(1) instanceof ColumnExpression right
                && left.isDirectReference() && right.isDirectReference() && left.getColumnId() == right.getColumnId()) {
            switch (call.getName()) {
                case "=" -> {
                    return null;
                }
                case "!=", "<>", ">", "<" -> {
                    return frame.expressionRewriter.newFalseConstant(call.getPosition());
                }
                default -> {
                }
            }
        }
        return predicate;
    }

    private RecordCursorFactory generateIndexedScan(
            GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext, int order, IntervalExtractor scanIntervals,
            BoundExpression residual, GenericRecordMetadata metadata, RecordMetadata readerMetadata, TableReader reader,
            IntList indexes, IntList shifts, SortPlan orderAdvice, LimitPlan limitAdvice, int orderByMnemonic
    ) throws SqlException {
        final int keyIndex = scan.getOutput().getColumnIndexById(frame.symbols.getColumnId());
        final int readerKeyIndex = indexes.getQuick(keyIndex);
        final int keyCount = frame.symbols.getValues().size();
        final ObjList<Function> keys = new ObjList<>(keyCount == 0 ? frame.symbols.getExcludedValues().size() : keyCount);
        RuntimeIntrinsicIntervalModel intervalModel = buildIntervals(frame, scanIntervals, reader);
        Function filter = null;
        PartitionFrameCursorFactory frames = null;
        final int orderCount = orderAdvice == null ? 0 : orderAdvice.getColumnIds().size();
        int indexDirection = IndexReader.DIR_FORWARD;
        boolean isOrderByKey = false;
        boolean isOrderByTimestamp = false;
        final int timestampId = scan.getOutput().getTimestampColumnId();
        int symbolKey = SymbolTable.VALUE_NOT_FOUND;
        int[] coveringMapping = null;
        try {
            final boolean isSinglePartition = intervalModel == null ? reader.getPartitionedBy() == PartitionBy.NONE
                    : intervalModel.allIntervalsHitOnePartition();
            if (isSinglePartition && !executionContext.isTimestampRequired() && orderCount > 0 && orderCount < 3
                    && orderAdvice.getColumnIds().getQuick(0) == frame.symbols.getColumnId()) {
                metadata.setTimestampIndex(-1);
                if (orderCount == 1) {
                    isOrderByKey = true;
                } else if (orderAdvice.getColumnIds().getQuick(1) == timestampId) {
                    isOrderByKey = true;
                    if (orderAdvice.getDirections().getQuick(1) == SortDirection.DESCENDING) {
                        indexDirection = IndexReader.DIR_BACKWARD;
                    }
                }
            }
            if (!isOrderByKey && orderCount == 1 && orderAdvice.getColumnIds().getQuick(0) == timestampId) {
                final boolean isDescending = orderAdvice.getDirections().getQuick(0) == SortDirection.DESCENDING;
                isOrderByTimestamp = keyCount == 1 || !isDescending;
                if (isOrderByTimestamp && isDescending) {
                    indexDirection = IndexReader.DIR_BACKWARD;
                }
            }
            filter = residual == null ? null : frame.functionInstantiator.instantiate(residual, scan.getOutput(), metadata, executionContext);
            if (filter != null && filter.isConstant()) {
                final boolean isTrue = filter.getBool(null);
                final Function constant = filter;
                filter = null;
                Misc.free(constant);
                if (!isTrue) {
                    final RuntimeIntrinsicIntervalModel unused = intervalModel;
                    intervalModel = null;
                    Misc.free(unused);
                    return new EmptyTableRecordCursorFactory(metadata);
                }
            }
            if (keyCount == 0) {
                instantiateKeys(frame, frame.symbols.getExcludedValues(), keys, scan, metadata, executionContext);
            } else {
                instantiateKeys(frame, frame.symbols.getValues(), keys, scan, metadata, executionContext);
                final Function firstKey = keys.getQuick(0);
                symbolKey = keyCount > 1 || firstKey.isRuntimeConstant() ? SymbolTable.VALUE_NOT_FOUND
                        : reader.getSymbolMapReader(readerKeyIndex).keyOf(firstKey.getStrA(null));
                coveringMapping = executionContext.isCoveringIndexEnabled() && !scan.isUpdate() && (keyCount == 1 || !isOrderByKey)
                        && !scan.hasHint(ScanPlan.HINT_NO_COVERING)
                        ? buildCoveringIndexMapping(reader, readerKeyIndex, indexes, metadata) : null;
            }
            final RuntimeIntrinsicIntervalModel frameIntervals = intervalModel;
            intervalModel = null;
            frames = newFrames(scan, frameIntervals, readerMetadata, order);
            configurePushdown(frame, frames, residual, scan, metadata, indexes, reader, executionContext);
        } catch (Throwable th) {
            Misc.free(frames, th);
            Misc.freeObjList(keys, th);
            Misc.free(filter, th);
            Misc.free(intervalModel, th);
            throw th;
        }
        if (keyCount == 0) {
            return new FilterOnExcludedValuesRecordCursorFactory(configuration, metadata, frames, keys,
                    keyIndex, filter, orderByMnemonic, isOrderByKey, isOrderByTimestamp,
                    SqlCodeGenerator.queryModelDirection(orderCount == 0 ? SortDirection.ASCENDING : orderAdvice.getDirections().getQuick(0)),
                    indexDirection, indexes, shifts, configuration.getMaxSymbolNotEqualsCount());
        }
        final Function coveredFilter = coveringMapping == null ? null : filter;
        final RecordCursorFactory factory;
        try {
            if (keyCount == 1) {
                factory = generateSingleSymbolIndexScan(metadata, frames, keyIndex, symbolKey, keys.getQuick(0),
                        coveringMapping == null ? filter : null, indexDirection, isOrderByKey || isOrderByTimestamp,
                        indexes, shifts, coveringMapping, scan.hasHint(ScanPlan.HINT_FORCE_USE_COVERING));
            } else {
                factory = generateSymbolValuesIndexScan(metadata, frames, keys, keyIndex, reader,
                        coveringMapping == null ? filter : null, orderByMnemonic, isOrderByKey, isOrderByTimestamp,
                        SqlCodeGenerator.queryModelDirection(orderCount == 0 ? SortDirection.ASCENDING : orderAdvice.getDirections().getQuick(0)),
                        indexDirection, indexes, shifts, coveringMapping, scan.hasHint(ScanPlan.HINT_FORCE_USE_COVERING));
            }
        } catch (Throwable th) {
            Misc.free(coveredFilter, th);
            throw th;
        }
        if (coveredFilter == null) {
            return factory;
        }
        final boolean isLimitOrderPreserved = orderCount == 0 || orderCount == 1
                && orderAdvice.getColumnIds().getQuick(0) == timestampId
                && orderAdvice.getDirections().getQuick(0) == SortDirection.ASCENDING;
        return filterGenerator.generateCovering(residual, scan.getOutput(), (CoveringIndexRecordCursorFactory) factory, coveredFilter,
                frame.functionInstantiator, executionContext, isLimitOrderPreserved ? limitAdvice : null, scan.hasHint(ScanPlan.HINT_PRE_TOUCH));
    }

    private RecordCursorFactory generateScan(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext, int order, IntervalExtractor scanIntervals,
                                             LatestByPlan latest, BoundExpression latestResidual, SymbolKeyExtractor latestKeys) throws SqlException {
        return generateScan(frame, scan, executionContext, order, scanIntervals, latest, latestResidual, latestKeys, null, null, OrderByMnemonic.ORDER_BY_REQUIRED);
    }

    private RecordCursorFactory generateScan(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext, int order, IntervalExtractor scanIntervals,
                                             LatestByPlan latest, BoundExpression latestResidual, SymbolKeyExtractor latestKeys,
                                             SortPlan orderAdvice, LimitPlan limitAdvice, int orderByMnemonic) throws SqlException {
        final RecordCursorFactory factory = generateTableScan(frame, scan, executionContext, order, scanIntervals, latest, latestResidual, latestKeys,
                orderAdvice, limitAdvice, orderByMnemonic);
        if (!scan.getTableToken().isLiveView() || scan.isUpdate()) {
            return factory;
        }
        // The live-view wrapper pins the in-memory tier and routes rows by seam timestamp.
        return new LiveViewRecordCursorFactory(executionContext.getCairoEngine(), scan.getTableToken(), factory);
    }

    private RecordCursorFactory generateSubqueryScan(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext, int order,
                                                     IntervalExtractor scanIntervals, BoundExpression residual, CursorExpression keySubquery,
                                                     GenericRecordMetadata metadata, GenericRecordMetadata readerMetadata, TableReader reader,
                                                     IntList indexes, IntList shifts) throws SqlException {
        PartitionFrameCursorFactory frames = null;
        Function filter = null;
        RecordCursorFactory subquery = null;
        final Record.CharSequenceFunction keyGetter;
        try {
            frames = newFrames(scan, buildIntervals(frame, scanIntervals, reader), readerMetadata, order);
            configurePushdown(frame, frames, residual, scan, metadata, indexes, reader, executionContext);
            filter = residual == null ? null : frame.functionInstantiator.instantiate(residual, scan.getOutput(), metadata, executionContext);
            subquery = frame.functionInstantiator.generateSubquery(keySubquery, executionContext);
            keyGetter = subqueryKeyGetter(subquery.getMetadata().getColumnType(0));
        } catch (Throwable th) {
            Misc.free(subquery, th);
            Misc.free(filter, th);
            Misc.free(frames, th);
            throw th;
        }
        return new FilterOnSubQueryRecordCursorFactory(configuration, metadata, frames, subquery,
                scan.getOutput().getColumnIndexById(frame.symbols.getColumnId()), filter, keyGetter, indexes, shifts);
    }

    /**
     * Returns null, owning nothing new, when no indexed SYMBOL pattern conjunct can drive the scan.
     */
    private RecordCursorFactory generateSymbolPatternIndex(
            GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext, int order, IntervalExtractor scanIntervals,
            BoundExpression predicate, GenericRecordMetadata metadata, RecordMetadata readerMetadata, TableReader reader,
            IntList indexes, IntList shifts, SortPlan orderAdvice, LimitPlan limitAdvice, int orderByMnemonic
    ) throws SqlException {
        final boolean isOrderByTimestampOnly = orderAdvice != null && orderAdvice.getColumnIds().size() == 1
                && orderAdvice.getColumnIds().getQuick(0) == scan.getNativeTimestampColumnId();
        if (isOrderByTimestampOnly && limitAdvice == null
                && orderAdvice.getDirections().getQuick(0) == SortDirection.DESCENDING) {
            return null;
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
            return null;
        }
        if (limitAdvice != null && limitAdvice.getHi() == null) {
            final Function lo = frame.functionInstantiator.instantiate(limitAdvice.getLo(), emptySchema, executionContext);
            try {
                if (filterGenerator.mayBeNegativeLimit(lo, executionContext)) {
                    return null;
                }
            } finally {
                Misc.free(lo);
            }
        }
        final BoundExpression pattern = frame.patternConjuncts.getQuick(frame.patternIndex);
        if (pattern instanceof FunctionExpression call && "!~".equals(call.getName())) {
            Misc.free(frame.functionInstantiator.instantiate(pattern, input, metadata, executionContext));
        }
        AdaptiveSymbolPatternRecordCursorFactory.PreparedSymbolPatternFilter filter = preparePatternFilter(frame, input, metadata, executionContext);
        if (filter == null) {
            return null;
        }
        ObjList<Function> workerFilters = null;
        try {
            final boolean isCoveringAllowed = executionContext.isCoveringIndexEnabled() && !scan.hasHint(ScanPlan.HINT_NO_COVERING);
            final boolean hasCovering = symbolPatternCoveringMapping(reader, filter.getSymbolColumnIndex(), indexes,
                    metadata, frame.isPatternNegated, isCoveringAllowed) != null;
            if (!filter.isThreadSafe() && executionContext.isParallelFilterEnabled()) {
                if (!hasCovering) {
                    final AdaptiveSymbolPatternRecordCursorFactory.PreparedSymbolPatternFilter unused = filter;
                    filter = null;
                    Misc.free(unused);
                    return null;
                }
                final int workerCount = executionContext.getSharedQueryWorkerCount();
                workerFilters = new ObjList<>(workerCount);
                frame.functionInstantiator.beginWorkerClones();
                try {
                    for (int i = 0; i < workerCount; i++) {
                        workerFilters.add(preparePatternFilter(frame, input, metadata, executionContext));
                    }
                } finally {
                    frame.functionInstantiator.endWorkerClones();
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
            return generateSymbolPatternIndex(frames, metadata, reader, indexes, shifts, ownedFilter,
                    filterColumns, ownedWorkers, orderByMnemonic, isOrderByTimestampOnly, isCoveringAllowed,
                    scan.hasHint(ScanPlan.HINT_PRE_TOUCH), executionContext);
        } catch (Throwable th) {
            Misc.free(filter, th);
            Misc.freeObjList(workerFilters, th);
            throw th;
        }
    }

    private RecordCursorFactory generateTableScan(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext, int order, IntervalExtractor scanIntervals,
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
        final RecordCursorFactory factory;
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
            factory = generateTableScan0(frame, scan, executionContext, order, rangeIntervals, latest, latestResidual, latestKeys,
                    orderAdvice, limitAdvice, orderByMnemonic);
        } catch (Throwable th) {
            Misc.clear(frame.overrideIntervals, th);
            throw th;
        }
        return SqlCodeGenerator.clearAfter(frame.overrideIntervals, factory);
    }

    private RecordCursorFactory generateTableScan0(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext, int order, IntervalExtractor scanIntervals,
                                                   LatestByPlan latest, BoundExpression latestResidual, SymbolKeyExtractor latestKeys,
                                                   SortPlan orderAdvice, LimitPlan limitAdvice, int orderByMnemonic) throws SqlException {
        if (isWalClientUpdate(scan, executionContext)) {
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
                metadata.setTimestampIndex(scan.getOutput().getTimestampIndex());
                factory = new EmptyTableRecordCursorFactory(metadata, tableMetadata.getTableToken());
            } catch (Throwable th) {
                Misc.free(tableMetadata, th);
                throw th;
            }
            return SqlCodeGenerator.closeAfter(tableMetadata, factory);
        }
        // Validate the bound version before constructing independently owned metadata.
        final TableReader reader = getBoundReader(scan, executionContext);
        final RecordCursorFactory factory;
        try {
            factory = generateReaderScan(frame, scan, executionContext, order, scanIntervals, latest, latestResidual, latestKeys,
                    orderAdvice, limitAdvice, orderByMnemonic, reader);
        } catch (Throwable th) {
            Misc.free(reader, th);
            throw th;
        }
        return SqlCodeGenerator.closeAfter(reader, factory);
    }

    private RecordCursorFactory generateReaderScan(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext, int order,
                                                   IntervalExtractor scanIntervals, LatestByPlan latest, BoundExpression latestResidual,
                                                   SymbolKeyExtractor latestKeys, SortPlan orderAdvice, LimitPlan limitAdvice, int orderByMnemonic,
                                                   TableReader reader) throws SqlException {
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
            return new EmptyTableRecordCursorFactory(metadata);
        }
        if (latest != null) {
            return generateLatestByScan(frame, scan, executionContext, scanIntervals, latest, latestResidual, latestKeys,
                    metadata, readerMetadata, reader, indexes, shifts);
        }
        if (latestResidual != null && !executionContext.isLiveViewCompile() && !scan.hasHint(ScanPlan.HINT_NO_INDEX)) {
            latestResidual = frame.symbols.extractIndexed(latestResidual, scan.getOutput(), metadata, reader, frame.expressionRewriter);
            if (frame.symbols.isFalse()) {
                return new EmptyTableRecordCursorFactory(metadata);
            }
            final CursorExpression keySubquery = frame.symbols.getSubquery();
            if (keySubquery != null) {
                return generateSubqueryScan(frame, scan, executionContext, order, scanIntervals, latestResidual, keySubquery,
                        metadata, readerMetadata, reader, indexes, shifts);
            }
            if (!frame.symbols.hasKey() && configuration.isSymbolPatternIndexEnabled() && !scan.isUpdate()
                    && !scan.hasHint(ScanPlan.HINT_NO_SYMBOL_PATTERN_INDEX)) {
                final RecordCursorFactory patternScan = generateSymbolPatternIndex(frame, scan, executionContext, order, scanIntervals,
                        latestResidual, metadata, readerMetadata, reader, indexes, shifts, orderAdvice, limitAdvice, orderByMnemonic);
                if (patternScan != null) {
                    return patternScan;
                }
            }
            if (frame.symbols.hasKey()) {
                if (frame.symbols.getValues().size() > 0 || reader.getSymbolMapReader(indexes.getQuick(
                        scan.getOutput().getColumnIndexById(frame.symbols.getColumnId()))).getSymbolCount() < configuration.getMaxSymbolNotEqualsCount()) {
                    return generateIndexedScan(frame, scan, executionContext, order, scanIntervals, latestResidual,
                            metadata, readerMetadata, reader, indexes, shifts, orderAdvice, limitAdvice, orderByMnemonic);
                }
                latestResidual = restoreExclusions(frame, latestResidual);
            }
        }
        final RuntimeIntrinsicIntervalModel intervalModel = buildIntervals(frame, scanIntervals, reader);
        final PartitionFrameCursorFactory frames = newFrames(scan, intervalModel, readerMetadata, order);
        final int sortedKeyIndex;
        try {
            sortedKeyIndex = sortedSymbolIndexKey(scan, intervalModel, latestResidual, metadata, orderAdvice, executionContext);
            if (sortedKeyIndex < 0) {
                configurePushdown(frame, frames, latestResidual, scan, metadata, indexes, reader, executionContext);
            }
        } catch (Throwable th) {
            Misc.free(frames, th);
            throw th;
        }
        if (sortedKeyIndex >= 0) {
            final boolean isTimestampDescending = orderAdvice.getColumnIds().size() == 2
                    && orderAdvice.getDirections().getQuick(1) == SortDirection.DESCENDING;
            metadata.setTimestampIndex(-1);
            return new SortedSymbolIndexRecordCursorFactory(configuration, metadata, frames, sortedKeyIndex,
                    orderAdvice.getDirections().getQuick(0) == SortDirection.ASCENDING,
                    isTimestampDescending ? IndexReader.DIR_BACKWARD : IndexReader.DIR_FORWARD, indexes, shifts);
        }
        final RecordCursorFactory factory = generateScan(frames, metadata, order, order == PartitionFrameCursorFactory.ORDER_DESC,
                indexes, shifts, scan.isRandomAccess());
        if (latestResidual == null) {
            return factory;
        }
        final Function filter;
        try {
            filter = frame.functionInstantiator.instantiate(latestResidual, scan.getOutput(), metadata, executionContext);
        } catch (Throwable th) {
            Misc.free(factory, th);
            throw th;
        }
        return filterGenerator.generate(frame, latestResidual, scan.getOutput(), factory, filter, frame.functionInstantiator, executionContext,
                scan.isUpdate(), isLimitOrderPreserved(scan, order, orderAdvice) ? limitAdvice : null, scan.hasHint(ScanPlan.HINT_PRE_TOUCH));
    }

    private RecordCursorFactory generateLatestByScan(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext, IntervalExtractor scanIntervals,
                                                     LatestByPlan latest, BoundExpression latestResidual, SymbolKeyExtractor latestKeys,
                                                     GenericRecordMetadata metadata, GenericRecordMetadata readerMetadata, TableReader reader,
                                                     IntList indexes, IntList shifts) throws SqlException {
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
        final CursorExpression keySubquery = latestKeys == null ? null : latestKeys.getSubquery();
        final ObjList<Function> keys = new ObjList<>();
        final ObjList<Function> excludedKeys = new ObjList<>();
        Function filter = null;
        PartitionFrameCursorFactory frames = null;
        RecordCursorFactory subquery = null;
        Record.CharSequenceFunction keyGetter = null;
        IntList symbolCounts = null;
        try {
            filter = latestResidual == null ? null : frame.functionInstantiator.instantiate(latestResidual, scan.getOutput(), metadata, executionContext);
            if (latestKeys != null) {
                instantiateKeys(frame, latestKeys.getValues(), keys, scan, metadata, executionContext);
                instantiateKeys(frame, latestKeys.getExcludedValues(), excludedKeys, scan, metadata, executionContext);
            }
            frames = newFrames(scan, buildIntervals(frame, scanIntervals, reader), readerMetadata, PartitionFrameCursorFactory.ORDER_DESC);
            if (latestResidual != null && (latestResidual.getFunctionFlags() & (BoundExpression.CONSTANT | BoundExpression.RUNTIME_CONSTANT)) == 0) {
                configurePushdown(frame, frames, latestResidual, scan, metadata, indexes, reader, executionContext);
            }
            if (keySubquery != null) {
                subquery = frame.functionInstantiator.generateSubquery(keySubquery, executionContext);
                keyGetter = subqueryKeyGetter(subquery.getMetadata().getColumnType(0));
            } else if (latestResidual != null) {
                symbolCounts = symbolCounts(latestResidual, latest.getKeyColumnIds());
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
            final int keyIndex = keyIndexes.getQuick(0);
            return new LatestBySubQueryRecordCursorFactory(configuration, metadata, frames, keyIndex, subquery, filter,
                    !scan.hasHint(ScanPlan.HINT_NO_INDEX) && metadata.isColumnIndexed(keyIndex), keyGetter, indexes, shifts);
        }
        return latestByGenerator.generateLatestByScan(
                frame, frames, metadata, reader, indexes, shifts, keyIndexes,
                isIndexedAllowed,
                filter, keys, excludedKeys, frame.latestPrefixes,
                symbolCounts,
                !scan.hasHint(ScanPlan.HINT_NO_INDEX),
                executionContext.isCoveringIndexEnabled() && !scan.hasHint(ScanPlan.HINT_NO_COVERING),
                scan.hasHint(ScanPlan.HINT_FORCE_USE_COVERING), executionContext
        );
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
        if (!frame.isPatternNegated) {
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
                    residualExpression = frame.expressionRewriter.combineConjunction(frame.patternConjuncts.getQuick(i), residualExpression, 0);
                }
            }
            if (residualExpression != null) {
                residual = frame.functionInstantiator.instantiate(residualExpression, input, metadata, executionContext);
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
            root = frame.expressionRewriter.combineConjunction(conjuncts.getQuick(i), root, 0);
        }
        return frame.expressionRewriter.combineConjunction(residual, root, 0);
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
     * Opens the reader of the table the scan was bound to, found by identity rather than by name. A rename
     * after binding leaves the bound plan valid, and the factories keep the bound token, so cursor open
     * still rejects a name that has moved to another table.
     */
    static TableReader getBoundReader(ScanPlan scan, SqlExecutionContext executionContext) {
        final TableToken bound = scan.getTableToken();
        final TableToken current = executionContext.getCairoEngine().getUpdatedTableToken(bound);
        return executionContext.getReader(current != null ? current : bound, scan.getMetadataVersion());
    }

    static boolean isWalClientUpdate(ScanPlan scan, SqlExecutionContext executionContext) {
        return scan.isUpdate() && !executionContext.isWalApplication()
                && executionContext.getCairoEngine().isWalTable(scan.getTableToken());
    }

    static int[] symbolPatternCoveringMapping(TableReader reader, int keyColumnIndex, IntList columnIndexes, RecordMetadata metadata,
                                              boolean isNegated, boolean isCoveringAllowed) {
        return isNegated || !isCoveringAllowed ? null
                : buildCoveringIndexMapping(reader, columnIndexes.getQuick(keyColumnIndex), columnIndexes, metadata);
    }

    void configurePushdown(GenerationFrame frame, FunctionSourcePlan source, RecordCursorFactory base, BoundExpression residual,
                           SqlExecutionContext executionContext) throws SqlException {
        final RecordCursorFactory target = base instanceof SelectedRecordCursorFactory ? base.getBaseFactory() : base;
        if (target.mayHaveParquetPartitions(executionContext) && executionContext.isParquetRowGroupPruningEnabled()) {
            target.setPushdownFilterCondition(frame.pushdown.extract(residual, source.getOutput(), base.getMetadata(),
                    target == base ? null : source.getSourceColumnIndexes(), target.getMetadata(), frame.functionInstantiator, executionContext));
        }
    }

    RecordCursorFactory generateFiltered(
            GenerationFrame frame, ScanPlan scan, BoundExpression residual, int requiredOrderColumnId, int requiredScanDirection,
            SortPlan orderAdvice, LimitPlan limitAdvice, int orderByMnemonic, SqlExecutionContext executionContext
    ) throws SqlException {
        final int timestampIndex = scan.getOutput().getColumnIndexById(scan.getNativeTimestampColumnId());
        final RecordCursorFactory factory;
        try {
            if (timestampIndex >= 0) {
                residual = frame.intervals.extract(residual, scan.getOutput().getColumnId(timestampIndex), scan.getOutput(), frame.functionInstantiator, frame.expressionRewriter, executionContext);
            }
            residual = foldSelfComparisons(frame, residual);
            final int order = requiredOrderColumnId == scan.getOutput().getTimestampColumnId()
                    && requiredScanDirection == RecordCursorFactory.SCAN_DIRECTION_BACKWARD
                    ? PartitionFrameCursorFactory.ORDER_DESC : PartitionFrameCursorFactory.ORDER_ASC;
            if (scan.getTableToken().isLiveView()) {
                // Rows the live view serves from memory bypass the wrapped scan, so only intervals go below it.
                final RecordCursorFactory scanFactory = generateScan(frame, scan, executionContext, order, frame.intervals, null, null, null,
                        orderAdvice, limitAdvice, orderByMnemonic);
                factory = residual == null ? scanFactory
                        : filterScan(frame, scanFactory, scan, residual, order, orderAdvice, limitAdvice, executionContext);
            } else {
                factory = generateScan(frame, scan, executionContext, order, frame.intervals, null, residual, null, orderAdvice, limitAdvice, orderByMnemonic);
            }
        } catch (Throwable th) {
            Misc.clear(frame.intervals, th);
            throw th;
        } finally {
            frame.symbols.clear();
        }
        return SqlCodeGenerator.clearAfter(frame.intervals, factory);
    }

    RecordCursorFactory generateFunctionSource(GenerationFrame frame, FunctionSourcePlan plan, SqlExecutionContext executionContext) throws SqlException {
        final RecordCursorFactory base = frame.functionSources.takeFactory(plan, executionContext);
        final GenericRecordMetadata metadata;
        final IntList mapping;
        try {
            final RecordMetadata baseMetadata = base.getMetadata();
            final OutputSchema output = plan.getOutput();
            final IntList sourceIndexes = plan.getSourceColumnIndexes();
            boolean isIdentity = sourceIndexes.size() == baseMetadata.getColumnCount()
                    && output.getTimestampIndex() == baseMetadata.getTimestampIndex();
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
            metadata.setTimestampIndex(output.getTimestampIndex());
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
            return generateScan(frame, scan, executionContext, PartitionFrameCursorFactory.ORDER_DESC, null, latest, null, null);
        }
        final RecordCursorFactory factory;
        try {
            final BoundExpression predicate = filter.getPredicate();
            if (configuration.useWithinLatestByOptimisation()) {
                collectWithin(frame, scan.getOutput(), predicate);
            }
            BoundExpression residual = scan.getOutput().getColumnIndexById(scan.getNativeTimestampColumnId()) < 0 ? predicate : frame.intervals.extract(predicate,
                    scan.getNativeTimestampColumnId(), scan.getOutput(), frame.functionInstantiator, frame.expressionRewriter, executionContext);
            final int candidateColumnId = latest.getKeyColumnIds().size() == 1 ? latest.getKeyColumnIds().getQuick(0) : -1;
            residual = frame.symbols.extract(foldSelfComparisons(frame, residual), candidateColumnId, frame.expressionRewriter);
            factory = generateScan(frame, scan, executionContext, PartitionFrameCursorFactory.ORDER_DESC, frame.intervals, latest, residual, frame.symbols);
        } catch (Throwable th) {
            Misc.clear(frame.intervals, th);
            throw th;
        } finally {
            frame.latestPrefixes.clear();
            frame.latestWithin = null;
            frame.symbols.clear();
        }
        return SqlCodeGenerator.clearAfter(frame.intervals, factory);
    }

    RecordCursorFactory generateScan(GenerationFrame frame, ScanPlan scan, SqlExecutionContext executionContext, int order) throws SqlException {
        return generateScan(frame, scan, executionContext, order, null, null, null, null);
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
            boolean isBackupSuppressed
    ) {
        if (coveringMapping != null) {
            assert filter == null;
            final boolean canKeyBeNull = canKeyBeNull(symbolKey, key);
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
        final int keyColumnIndex = filter.getSymbolColumnIndex();
        final boolean isNegated = filter.isNegated();
        final IntList effectiveKeys = new IntList();
        final AdaptiveSymbolPatternRecordCursorFactory.NonOwningPartitionFrameCursorFactory sharedFrames =
                new AdaptiveSymbolPatternRecordCursorFactory.NonOwningPartitionFrameCursorFactory(frames);
        final boolean isParallel = executionContext.isParallelFilterEnabled();
        RecordCursorFactory coveringDelegate = null;
        RecordCursorFactory indexDelegate = null;
        RecordCursorFactory scanDelegate = null;
        boolean isSelfFiltering = false;
        try {
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
            if (coveringDelegate == null && isParallel && filter.isThreadSafe()) {
                final RecordCursorFactory unfiltered = scanDelegate;
                scanDelegate = null;
                isSelfFiltering = true;
                scanDelegate = new AsyncFilteredRecordCursorFactory(executionContext.getCairoEngine(), configuration,
                        executionContext.getMessageBus(), unfiltered, filter, filterColumnIndexes, reduceTaskFactory, null,
                        null, 0, executionContext.getSharedQueryWorkerCount(), isPreTouchEnabled);
            }
        } catch (Throwable th) {
            Misc.free(coveringDelegate, th);
            Misc.free(indexDelegate, th);
            Misc.free(scanDelegate, th);
            Misc.free(frames, th);
            Misc.freeObjList(workerFilters, th);
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
            if (!isSelfFiltering) {
                Misc.free(filter, th);
            }
            throw th;
        }
        if (isSelfFiltering || !isParallel || !adaptive.supportsPageFrameCursor()) {
            try {
                Misc.freeObjList(workerFilters);
            } catch (Throwable th) {
                Misc.free(adaptive, th);
                if (!isSelfFiltering) {
                    Misc.free(filter, th);
                }
                throw th;
            }
            return isSelfFiltering ? adaptive : new FilteredRecordCursorFactory(adaptive, filter);
        }
        return new AsyncFilteredRecordCursorFactory(executionContext.getCairoEngine(), configuration, executionContext.getMessageBus(),
                adaptive, filter, filterColumnIndexes, reduceTaskFactory, workerFilters, null, 0,
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
            boolean isBackupSuppressed
    ) {
        if (coveringMapping == null) {
            return new FilterOnValuesRecordCursorFactory(configuration, metadata, frames, keys, keyIndex,
                    reader, filter, orderByMnemonic, isOrderByKey, isOrderByTimestamp, orderDirection,
                    indexDirection, columnIndexes, columnSizeShifts);
        }
        assert filter == null;
        final int readerKeyIndex = columnIndexes.getQuick(keyIndex);
        final boolean hasNullableKey;
        try {
            hasNullableKey = canAnyKeyBeNull(keys, reader.getSymbolMapReader(readerKeyIndex));
        } catch (Throwable th) {
            Misc.free(frames, th);
            Misc.freeObjList(keys, th);
            throw th;
        }
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
