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

package io.questdb.griffin.optimiser;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.sql.TableAccessInfo;
import io.questdb.griffin.BoundExpressionRewriter.ConjunctTest;
import io.questdb.griffin.IntervalAnalysis;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SymbolKeyExtractor;
import io.questdb.griffin.engine.functions.regex.MatchSymbolFunctionFactory;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.DomainReadPlan;
import io.questdb.griffin.plan.logical.ExpressionVisitor;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PlanExpressionVisitor;
import io.questdb.griffin.plan.logical.PlanToFactoryRules;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SortDirection;
import io.questdb.griffin.plan.logical.SortKeys;
import io.questdb.griffin.plan.logical.Subquery;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowJoinStep;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;

/**
 * Decides how every scan of a query level, and of each sub-query it reads, reads its table, once
 * {@link OrderPlanning} has recorded what the consumers of the scan require, and records the decision on the
 * {@link ScanPlan}: the access path, the index it reads and how, the key values, sub-query or pattern that drive it,
 * the requested order it delivers, the residual it filters with and the intervals it relies on. It reads the access
 * facts of the table, the interval analysis of the predicate, the pattern declarations and the configuration; the code
 * generator builds exactly what it records.
 * <p>
 * A decision depends on where the generator builds the scan, which this pass follows as the generator walks the
 * plan: the generation depth of the query level, whether a consumer requires the designated timestamp, see
 * {@link OperatorPlanning#requiresInputTimestamp}, the intervals of a window join master that the scan of its
 * slave narrows to, and whether a HORIZON JOIN reads the time frames of the scan under the filter of its slave, see
 * {@link PlanToFactoryRules#temporalStolenFilter}, which keeps that scan on its page frames, with a constant false
 * residual where the predicate admits no row. Every scan gets an access path; a static error the generator raises
 * before it reads the decision, such as an invalid interval bound, fails planning with the same message and position.
 * Once it has planned the inputs of a node, the walk hands the node to {@link OperatorPlanning#planOperators} with the
 * timestamp requirement of its consumer, which rejects what the generator cannot build.
 */
final class AccessPathPlanning {
    private static final int MODEL_NONE = -1;
    private final ObjList<LongList> capturedIntervals = new ObjList<>();
    private final IntList capturedTypes;
    private final CairoConfiguration configuration;
    private final ObjList<BoundExpression> conjuncts;
    private final OptimiserContext context;
    private final ObjList<CursorExpression> cursors = new ObjList<>();
    private final ExpressionVisitor cursorFinder = this::findCursor;
    private final PlanExpressionVisitor subqueryCollector = new PlanExpressionVisitor() {
        @Override
        public int visitColumnId(int columnId, int position) {
            return columnId;
        }

        @Override
        public BoundExpression visitExpression(BoundExpression expression) {
            expression.walk(cursorFinder);
            return expression;
        }
    };
    private final IntervalAnalysis intervals;
    private final SymbolKeyExtractor keys;
    private final OperatorPlanning operatorPlanning;
    private final ObjList<Subquery> subqueries = new ObjList<>();
    private final IntList tableColumnIndexes;
    private final ConjunctTest unfoldedConjuncts = this::isUnfoldedConjunct;
    /**
     * The decorrelation domains with reads the walk has planned: the walk plans a domain where it first reaches the
     * domain or one of its reads, as the generator builds it there, see {@link DomainReadPlan}.
     */
    private final ObjList<AggregatePlan> walkedProducers = new ObjList<>();
    private int captureLevel = -1;
    private int depth;
    private int falseComparisonPosition;
    private ScanPlan horizonSlaveScan;
    private boolean isCapturing;
    private WindowJoinStep pendingStep;
    private TableAccessInfo table;

    AccessPathPlanning(
            CairoConfiguration configuration,
            OptimiserContext context,
            OperatorPlanning operatorPlanning,
            ObjList<BoundExpression> conjuncts,
            ObjList<BoundExpression> excludedConjuncts,
            IntList capturedTypes,
            IntList tableColumnIndexes,
            StringSink intervalText,
            LongList parsedIntervals
    ) {
        this.configuration = configuration;
        this.context = context;
        this.operatorPlanning = operatorPlanning;
        this.conjuncts = conjuncts;
        this.capturedTypes = capturedTypes;
        this.tableColumnIndexes = tableColumnIndexes;
        this.intervals = new IntervalAnalysis(configuration, intervalText, parsedIntervals);
        this.keys = new SymbolKeyExtractor(conjuncts, excludedConjuncts);
    }

    /**
     * The row count a filter over the scan may stop after, when the consumer's LIMIT is a plain count.
     */
    private static BoundExpression limitCount(ScanPlan scan) {
        return scan.getLimitHi() == null ? scan.getLimitLo() : null;
    }

    /**
     * The pattern a negated pattern conjunct negates, or the conjunct itself.
     */
    private static FunctionExpression positivePattern(FunctionExpression pattern) {
        return pattern.getArgumentCount() == 1 ? (FunctionExpression) pattern.argumentAt(0) : pattern;
    }

    /**
     * The order a read of a list of keys emits its rows in when it delivers no requested order: merged row by row in
     * table order when the consumers require the order, key by key within each page frame when an aggregate without
     * keys folds the rows, and merged frame by frame in table order otherwise.
     */
    private static ScanPlan.IndexOrder unrequestedIndexOrder(ScanPlan scan) {
        return switch (scan.getRowOrder()) {
            case REQUIRED -> ScanPlan.IndexOrder.TABLE;
            case ANY_SINGLE_GROUP -> ScanPlan.IndexOrder.KEY_PER_FRAME;
            case ANY -> ScanPlan.IndexOrder.TABLE_PER_FRAME;
        };
    }

    private void capture(boolean hasModel) {
        if (!isCapturing || !hasModel) {
            return;
        }
        final LongList captured = capturedIntervals.getQuick(captureLevel);
        captured.clear();
        if (intervals.isStatic()) {
            captured.add(intervals.getStaticIntervals());
        }
        capturedTypes.setQuick(captureLevel, intervals.getTimestampType());
    }

    /**
     * The first top-level within() conjunct whose GeoHash prefixes are all constants the column takes, or null.
     */
    private FunctionExpression collectWithin(BoundExpression predicate, OutputSchema output) {
        if (!(predicate instanceof FunctionExpression call)) {
            return null;
        }
        if (call.isAnd()) {
            final FunctionExpression left = collectWithin(call.argumentAt(0), output);
            return left != null ? left : collectWithin(call.argumentAt(1), output);
        }
        if (!"within".equals(call.getName()) || !(call.argumentAt(0) instanceof ColumnExpression column)
                || output.getColumnIndexById(column.getColumnId()) < 0) {
            return null;
        }
        return PlanToFactoryRules.hasConstantWithinPrefixes(call) ? call : null;
    }

    private int findCursor(BoundExpression expression) {
        if (expression instanceof CursorExpression cursor) {
            cursors.add(cursor);
            return TreeWalk.SKIP_CHILDREN;
        }
        return TreeWalk.CONTINUE;
    }

    /**
     * Folds the comparisons of a column with itself among the conjuncts of the predicate: one that holds drops out, and
     * one that fails turns the predicate into a constant false at the position of the first such comparison.
     */
    private BoundExpression foldSelfComparisons(BoundExpression predicate) throws SqlException {
        falseComparisonPosition = -1;
        final BoundExpression folded = context.getRewriter().retainConjuncts(predicate, unfoldedConjuncts);
        return falseComparisonPosition < 0 ? folded : context.getRewriter().newFalseConstant(falseComparisonPosition);
    }

    private boolean isIndexedSymbolColumn(BoundExpression expression, OutputSchema output) {
        return expression instanceof ColumnExpression column && column.isDirectReference() && ColumnType.isSymbol(column.getDataType())
                && table.isIndexed(tableColumnIndex(output, column.getColumnId()));
    }

    private boolean isIndexedSymbolPattern(BoundExpression expression, OutputSchema output) {
        if (expression instanceof FunctionExpression call && call.getArgumentCount() == 2) {
            final String name = call.getName();
            return ("like".equals(name) || "ilike".equals(name) || "~".equals(name)) && isIndexedSymbolColumn(call.argumentAt(0), output);
        }
        return false;
    }

    private boolean isNegatedIndexedSymbolPattern(BoundExpression expression, OutputSchema output) {
        if (!(expression instanceof FunctionExpression call)) {
            return false;
        }
        final String name = call.getName();
        return call.getArgumentCount() == 1 && "not".equals(name) && isIndexedSymbolPattern(call.argumentAt(0), output)
                || call.getArgumentCount() == 2 && "!~".equals(name) && isIndexedSymbolColumn(call.argumentAt(0), output);
    }

    private boolean isSymbolKeySetProviderDeclared(FunctionExpression pattern, OutputSchema output) {
        final FunctionExpression positive = positivePattern(pattern);
        final boolean isSymbolTableStatic = table.isSymbolTableStatic(tableColumnIndex(output, ((ColumnExpression) positive.argumentAt(0)).getColumnId()));
        return MatchSymbolFunctionFactory.positivePatternFactory(pattern).isSymbolKeySetProvider(positive.getArguments(), isSymbolTableStatic);
    }

    /**
     * Drops a comparison of a column with itself that holds, and records where the first one that fails is.
     */
    private boolean isUnfoldedConjunct(BoundExpression conjunct) {
        if (!(conjunct instanceof FunctionExpression call)) {
            return true;
        }
        return switch (LogicalPlans.selfComparison(call)) {
            case LogicalPlans.SELF_COMPARISON_TRUE -> false;
            case LogicalPlans.SELF_COMPARISON_FALSE -> {
                if (falseComparisonPosition < 0) {
                    falseComparisonPosition = call.getPosition();
                }
                yield true;
            }
            default -> true;
        };
    }

    private void planIndexed(ScanPlan scan, BoundExpression residual, boolean hasModel, boolean isTimestampRequired) {
        final OutputSchema output = scan.getOutput();
        final int keyId = keys.getColumnId();
        final int tableKeyIndex = tableColumnIndex(output, keyId);
        final int keyCount = keys.getValues().size();
        final boolean isSinglePartition = hasModel
                ? intervals.allIntervalsHitOnePartition(table.getPartitionedBy()) : table.getPartitionedBy() == PartitionBy.NONE;
        final SortKeys order = scan.getRequestedOrder();
        final int orderCount = order.size();
        final int timestampId = output.getTimestampColumnId();
        ScanPlan.IndexOrder indexOrder = ScanPlan.IndexOrder.NONE;
        SortDirection indexDirection = keyCount == 1 ? scan.getScanDirection() : SortDirection.ASCENDING;
        boolean isTimestampDropped = false;
        if (isSinglePartition && !isTimestampRequired && orderCount > 0 && orderCount < 3 && order.getColumnIds().getQuick(0) == keyId) {
            isTimestampDropped = true;
            if (orderCount == 1) {
                indexOrder = ScanPlan.IndexOrder.KEY;
            } else if (order.getColumnIds().getQuick(1) == timestampId) {
                indexOrder = ScanPlan.IndexOrder.KEY;
                indexDirection = order.getDirections().getQuick(1);
                scan.setScanDirection(indexDirection);
            }
        }
        if (indexOrder == ScanPlan.IndexOrder.NONE && orderCount == 1 && order.getColumnIds().getQuick(0) == timestampId) {
            final boolean isDescending = order.getDirections().getQuick(0) == SortDirection.DESCENDING;
            if (keyCount == 1 || !isDescending) {
                indexOrder = ScanPlan.IndexOrder.TIMESTAMP;
                indexDirection = order.getDirections().getQuick(0);
            }
        }
        if (indexOrder == ScanPlan.IndexOrder.NONE && keyCount != 1) {
            indexOrder = unrequestedIndexOrder(scan);
        }
        scan.setSinglePartition(isSinglePartition);
        scan.setTimestampDropped(isTimestampDropped);
        final int truth = LogicalPlans.constantTruth(residual);
        if (truth == 0) {
            scan.setAccessPath(ScanPlan.AccessPath.EMPTY, null);
            return;
        }
        final BoundExpression filter = truth == 1 ? null : residual;
        scan.setIndexOrder(indexOrder, indexDirection);
        if (keyCount == 0) {
            scan.getExcludedKeys().addAll(keys.getExcludedValues());
            scan.setIndex(keyId, ScanPlan.IndexRead.INDEX);
            scan.setAccessPath(ScanPlan.AccessPath.EXCLUDED_SYMBOL_INDEX, filter);
            return;
        }
        scan.getIndexKeys().addAll(keys.getValues());
        final boolean isCovering = context.getExecutionContext().isCoveringIndexEnabled() && !scan.isUpdate()
                && (keyCount == 1 || indexOrder != ScanPlan.IndexOrder.KEY) && !scan.hasHint(ScanPlan.HINT_NO_COVERING)
                && table.isCovering(tableKeyIndex, tableColumnIndexes);
        scan.setIndex(keyId, isCovering ? ScanPlan.IndexRead.COVERING : ScanPlan.IndexRead.INDEX);
        scan.setAccessPath(ScanPlan.AccessPath.SYMBOL_INDEX, filter);
    }

    private void planLatestBy(
            ScanPlan scan,
            LatestByPlan latest,
            BoundExpression residual,
            SymbolKeyExtractor latestKeys,
            FunctionExpression within,
            boolean hasExtractor
    ) {
        final OutputSchema output = scan.getOutput();
        final IntList keyIds = latest.getKeyColumnIds();
        final boolean hasKey = keyIds.size() > 0;
        final int keyId = hasKey ? keyIds.getQuick(0) : -1;
        final int tableKeyIndex = hasKey ? tableColumnIndex(output, keyId) : -1;
        final boolean isIndexAllowed = !scan.hasHint(ScanPlan.HINT_NO_INDEX);
        final boolean isIndexed = hasKey && isIndexAllowed && table.isIndexed(tableKeyIndex);
        final boolean isSingleKeyBitmap = (!hasExtractor || configuration.useWithinLatestByOptimisation()) && isIndexAllowed
                && keyIds.size() == 1 && table.getIndexType(tableKeyIndex) == IndexType.BITMAP;
        if (residual != null && residual == within && (latestKeys == null || !latestKeys.hasKey() && latestKeys.getSubquery() == null)
                && isSingleKeyBitmap) {
            residual = null;
            scan.setWithin(within);
        }
        final CursorExpression keySubquery = latestKeys == null ? null : latestKeys.getSubquery();
        if (keySubquery != null) {
            scan.setKeySubquery(keySubquery);
            scan.setIndex(keyId, isIndexed ? ScanPlan.IndexRead.INDEX : ScanPlan.IndexRead.NONE);
            scan.setAccessPath(ScanPlan.AccessPath.LATEST_BY_SUBQUERY, residual);
            return;
        }
        if (LogicalPlans.constantTruth(residual) == 0) {
            scan.setAccessPath(ScanPlan.AccessPath.EMPTY, null);
            return;
        }
        final int valueCount = latestKeys == null ? 0 : latestKeys.getValues().size();
        final int excludedCount = latestKeys == null ? 0 : latestKeys.getExcludedValues().size();
        if (valueCount > 0 || excludedCount > 0) {
            final boolean isCovered = context.getExecutionContext().isCoveringIndexEnabled() && !scan.hasHint(ScanPlan.HINT_NO_COVERING)
                    && table.isCovering(tableKeyIndex, tableColumnIndexes);
            scan.getIndexKeys().addAll(latestKeys.getValues());
            scan.getExcludedKeys().addAll(latestKeys.getExcludedValues());
            scan.setIndex(keyId, !isIndexed || excludedCount > 0 ? ScanPlan.IndexRead.NONE
                    : isCovered ? ScanPlan.IndexRead.COVERING : ScanPlan.IndexRead.INDEX);
            scan.setAccessPath(valueCount == 1 && excludedCount == 0 ? ScanPlan.AccessPath.LATEST_BY_VALUE : ScanPlan.AccessPath.LATEST_BY_VALUES,
                    residual);
            return;
        }
        if (residual == null && isSingleKeyBitmap) {
            scan.setIndex(keyId, ScanPlan.IndexRead.INDEX);
            scan.setAccessPath(ScanPlan.AccessPath.LATEST_BY_ALL_INDEXED, null);
            return;
        }
        if (keyIds.size() == 1 && ColumnType.isSymbol(table.getColumnType(tableKeyIndex)) && table.isSymbolTableStatic(tableKeyIndex)) {
            scan.setAccessPath(ScanPlan.AccessPath.LATEST_BY_STATIC_SYMBOL, residual);
            return;
        }
        boolean hasOnlySymbolKeys = hasKey;
        for (int i = 0, n = keyIds.size(); i < n; i++) {
            hasOnlySymbolKeys &= ColumnType.isSymbol(table.getColumnType(tableColumnIndex(output, keyIds.getQuick(i))));
        }
        scan.setAccessPath(hasOnlySymbolKeys ? ScanPlan.AccessPath.LATEST_BY_SYMBOLS : ScanPlan.AccessPath.LATEST_BY_ALL, residual);
    }

    /**
     * Plans the indexed SYMBOL pattern access path and returns true, or returns false, recording nothing, when no
     * pattern conjunct can drive the scan, or when a backward scan serves a descending timestamp order: the order has
     * no LIMIT, or a parallel top-K would implement its LIMIT over every page frame of the table.
     */
    private boolean planPattern(ScanPlan scan, BoundExpression residual) {
        if (scan.isNativeTimestampOrderRequested() && scan.getRequestedOrder().getDirections().getQuick(0) == SortDirection.DESCENDING
                && (scan.getLimitLo() == null || context.getExecutionContext().isParallelTopKEnabled()
                && OperatorPlanning.isTopKCount(scan.getLimitLo(), scan.getLimitHi()))) {
            return false;
        }
        final OutputSchema output = scan.getOutput();
        conjuncts.clear();
        LogicalPlans.collectConjuncts(residual, conjuncts);
        int patternIndex = -1;
        boolean isNegated = false;
        for (int i = 0, n = conjuncts.size(); i < n && patternIndex < 0; i++) {
            final BoundExpression conjunct = conjuncts.getQuick(i);
            if (isIndexedSymbolPattern(conjunct, output)) {
                patternIndex = i;
            } else if (isNegatedIndexedSymbolPattern(conjunct, output)) {
                patternIndex = i;
                isNegated = true;
            }
        }
        if (patternIndex < 0 || limitCount(scan) != null && LogicalPlans.mayBeNegativeLimit(scan.getLimitLo())) {
            return false;
        }
        final FunctionExpression pattern = (FunctionExpression) conjuncts.getQuick(patternIndex);
        if (!isSymbolKeySetProviderDeclared(pattern, output)) {
            return false;
        }
        final int keyId = ((ColumnExpression) positivePattern(pattern).argumentAt(0)).getColumnId();
        final boolean isCovering = context.getExecutionContext().isCoveringIndexEnabled() && !scan.hasHint(ScanPlan.HINT_NO_COVERING)
                && !isNegated && table.isCovering(tableColumnIndex(output, keyId), tableColumnIndexes);
        scan.setKeyPattern(pattern);
        scan.setIndex(keyId, isCovering ? ScanPlan.IndexRead.COVERING : ScanPlan.IndexRead.INDEX);
        scan.setIndexOrder(scan.isNativeTimestampOrderRequested() ? ScanPlan.IndexOrder.TABLE
                : unrequestedIndexOrder(scan), SortDirection.ASCENDING);
        scan.setAccessPath(ScanPlan.AccessPath.SYMBOL_PATTERN, residual);
        return true;
    }

    /**
     * Mirrors the generator's posting-index DISTINCT: a single SYMBOL key without aggregates over a scan of a posting
     * index, whose predicate, if any, the intervals implement whole and leave non-empty.
     */
    private boolean planPosting(AggregatePlan aggregate) throws SqlException {
        if (aggregate.getAggregates().size() != 0 || aggregate.getGroupingExpressions().size() != 1
                || !(aggregate.getGroupingExpressions().getQuick(0) instanceof ColumnExpression key)
                || key.getDataType() != ColumnType.SYMBOL || !context.getExecutionContext().isCoveringIndexEnabled()) {
            return false;
        }
        LogicalPlan source = aggregate.getChild();
        BoundExpression predicate = null;
        if (source instanceof FilterPlan filter) {
            predicate = filter.getPredicate();
            source = filter.getChild();
        }
        if (!(source instanceof ScanPlan scan) || scan.hasHint(ScanPlan.HINT_NO_COVERING) || scan.hasHint(ScanPlan.HINT_NO_INDEX)) {
            return false;
        }
        final OutputSchema output = scan.getOutput();
        final int timestampIndex = output.getColumnIndexById(scan.getNativeTimestampColumnId());
        if (predicate != null) {
            if (timestampIndex < 0) {
                return false;
            }
            intervals.analyse(predicate, scan.getNativeTimestampColumnId(), output.getColumnType(timestampIndex), depth, context.getRewriter());
            if (intervals.getResidual() != null) {
                return false;
            }
        }
        final TableAccessInfo table = context.getTableAccessInfo(scan);
        final int index = scan.getSourceColumnIndexes().getQuick(output.getColumnIndexById(key.getColumnId()));
        if (!IndexType.isPosting(table.getIndexType(index))
                || predicate != null && (!intervals.hasIntervalFilters() || intervals.isStatic() && intervals.getStaticIntervals().size() == 0)) {
            return false;
        }
        scan.clearAccessPath();
        scan.setDepth(depth);
        scan.setAccessPath(ScanPlan.AccessPath.POSTING_DISTINCT, null);
        scan.setIndex(key.getColumnId(), ScanPlan.IndexRead.INDEX);
        return true;
    }

    private void planScan(ScanPlan scan, BoundExpression predicate, LatestByPlan latest, boolean isTimestampRequired) throws SqlException {
        final WindowJoinStep step = pendingStep;
        pendingStep = null;
        scan.clearAccessPath();
        scan.setDepth(depth);
        table = null;
        try {
            planTableScan(scan, predicate, latest, step, isTimestampRequired);
        } finally {
            table = null;
        }
    }

    /**
     * Plans the sub-queries of a scan the generator builds with the operator over it, {@code input}, read directly or
     * through one filter.
     */
    private void planScanSubqueries(LogicalPlan input, ScanPlan scan) throws SqlException {
        if (input != scan) {
            planSubqueries(input);
        }
        planSubqueries(scan);
    }

    private void planSubqueries(LogicalPlan plan) throws SqlException {
        final int mark = cursors.size();
        plan.visitReads(subqueryCollector);
        for (int i = mark, n = cursors.size(); i < n; i++) {
            final Subquery subquery = cursors.getQuick(i).getSubquery();
            if (subqueries.indexOf(subquery) >= 0) {
                continue;
            }
            subqueries.add(subquery);
            final int savedDepth = depth;
            final boolean wasCapturing = isCapturing;
            final WindowJoinStep savedStep = pendingStep;
            final int enclosingFrameLo = operatorPlanning.enterFrame();
            depth = savedDepth + 1;
            isCapturing = false;
            pendingStep = null;
            try {
                operatorPlanning.countSharedConsumers(subquery.getRoot());
                walk(subquery.getRoot(), false);
            } finally {
                depth = savedDepth;
                isCapturing = wasCapturing;
                pendingStep = savedStep;
                operatorPlanning.exitFrame(enclosingFrameLo);
            }
        }
        cursors.setPos(mark);
    }

    /**
     * Mirrors the generator's scan of a table: the intervals and keys the predicate implies, then the access path they
     * and the table allow; raises the error the generator raises before it reads the decision.
     */
    private void planTableScan(
            ScanPlan scan,
            BoundExpression predicate,
            LatestByPlan latest,
            WindowJoinStep step,
            boolean isTimestampRequired
    ) throws SqlException {
        final SqlExecutionContext executionContext = context.getExecutionContext();
        final OutputSchema output = scan.getOutput();
        final int timestampType = scan.getNativeTimestampType();
        final boolean isWalClientUpdate = scan.isWalClientUpdate();
        final boolean isLiveView = scan.getTableToken().isLiveView();
        BoundExpression residual = null;
        SymbolKeyExtractor latestKeys = null;
        FunctionExpression within = null;
        boolean hasExtractor = predicate != null;
        if (predicate != null) {
            if (latest != null && configuration.useWithinLatestByOptimisation()) {
                within = collectWithin(predicate, output);
            }
            final int timestampIndex = output.getColumnIndexById(scan.getNativeTimestampColumnId());
            if (timestampIndex >= 0) {
                intervals.analyse(predicate, scan.getNativeTimestampColumnId(), output.getColumnType(timestampIndex), depth, context.getRewriter());
                residual = intervals.getResidual();
            } else {
                intervals.of(timestampType);
                residual = predicate;
            }
            residual = foldSelfComparisons(residual);
            if (latest != null) {
                final int candidateColumnId = latest.getKeyColumnIds().size() == 1 ? latest.getKeyColumnIds().getQuick(0) : -1;
                residual = keys.extract(residual, candidateColumnId, context.getRewriter());
                latestKeys = keys;
            }
        }
        final boolean isOverridden = executionContext.isOverriddenIntrinsics(scan.getTableToken()) && !isWalClientUpdate;
        final boolean isMerged = step != null && !isWalClientUpdate && capturedTypes.getQuick(captureLevel) == timestampType;
        if (isOverridden || isMerged) {
            if (!hasExtractor) {
                intervals.of(timestampType);
                hasExtractor = true;
            }
            if (isOverridden) {
                intervals.override(scan.getTableToken(), executionContext);
            }
            if (isMerged) {
                final long hi = step.getIntervalHi(timestampType);
                final long lo = step.getIntervalLo(timestampType);
                final LongList masterIntervals = capturedIntervals.getQuick(captureLevel);
                intervals.merge(masterIntervals, lo, hi);
                scan.setJoinIntervalStep(step);
                scan.getJoinIntervals().add(masterIntervals);
            }
        }
        if (isWalClientUpdate) {
            scan.setAccessPath(ScanPlan.AccessPath.UPDATE_STUB, null);
            return;
        }
        final boolean hasModel = hasExtractor && intervals.hasIntervalFilters();
        scan.setIntervalScan(hasModel);
        table = context.getTableAccessInfo(scan);
        tableColumnIndexes.clear();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            final int columnIndex = table.getColumnIndex(output.getColumnName(i));
            assert columnIndex > -1 : "bound column is missing from its table version";
            tableColumnIndexes.add(columnIndex);
        }
        final boolean isHorizonSlave = scan == horizonSlaveScan;
        if (hasExtractor && intervals.isIntrinsicFalse() || latestKeys != null && latestKeys.isFalse()) {
            if (isHorizonSlave) {
                scan.setAccessPath(ScanPlan.AccessPath.PAGE_FRAMES, context.getRewriter().newFalseConstant(scan.getPosition()));
            } else {
                scan.setAccessPath(ScanPlan.AccessPath.EMPTY, null);
            }
            return;
        }
        if (latest != null) {
            planLatestBy(scan, latest, residual, latestKeys, within, hasExtractor);
            capture(hasModel);
            return;
        }
        final BoundExpression liveViewResidual = residual;
        if (isLiveView) {
            residual = null;
        }
        if (isHorizonSlave) {
            scan.setAccessPath(ScanPlan.AccessPath.PAGE_FRAMES, isLiveView ? liveViewResidual : residual);
            capture(hasModel);
            return;
        }
        if (residual != null && !executionContext.isLiveViewCompile() && !scan.hasHint(ScanPlan.HINT_NO_INDEX)) {
            residual = keys.extractIndexed(residual, output, tableColumnIndexes, table, context.getRewriter());
            if (keys.isFalse()) {
                scan.setAccessPath(ScanPlan.AccessPath.EMPTY, null);
                return;
            }
            final CursorExpression keySubquery = keys.getSubquery();
            if (keySubquery != null) {
                scan.setKeySubquery(keySubquery);
                scan.setIndex(keys.getColumnId(), ScanPlan.IndexRead.INDEX);
                scan.setAccessPath(ScanPlan.AccessPath.SYMBOL_SUBQUERY, residual);
                capture(hasModel);
                return;
            }
            if (!keys.hasKey() && configuration.isSymbolPatternIndexEnabled() && !scan.isUpdate()
                    && !scan.hasHint(ScanPlan.HINT_NO_SYMBOL_PATTERN_INDEX) && planPattern(scan, residual)) {
                capture(hasModel);
                return;
            }
            if (keys.hasKey()) {
                if (keys.getValues().size() > 0 || table.getSymbolCount(tableColumnIndex(output, keys.getColumnId()))
                        < configuration.getMaxSymbolNotEqualsCount()) {
                    planIndexed(scan, residual, hasModel, isTimestampRequired);
                    capture(hasModel);
                    return;
                }
                residual = restoreExclusions(residual);
            }
        }
        final int sortedKeyIndex = sortedSymbolIndexKey(scan, residual, hasModel, isTimestampRequired);
        if (sortedKeyIndex >= 0) {
            scan.setSinglePartition(true);
            scan.setTimestampDropped(true);
            scan.setIndex(output.getColumnId(sortedKeyIndex), ScanPlan.IndexRead.INDEX);
            final SortDirection indexDirection = scan.getRequestedOrder().size() == 2
                    ? scan.getRequestedOrder().getDirections().getQuick(1) : SortDirection.ASCENDING;
            scan.setScanDirection(indexDirection);
            scan.setIndexOrder(ScanPlan.IndexOrder.KEY, indexDirection);
            scan.setAccessPath(ScanPlan.AccessPath.SORTED_SYMBOL_INDEX, isLiveView ? liveViewResidual : null);
        } else {
            scan.setAccessPath(ScanPlan.AccessPath.PAGE_FRAMES, isLiveView ? liveViewResidual : residual);
        }
        capture(hasModel);
    }

    private BoundExpression restoreExclusions(BoundExpression residual) throws SqlException {
        final ObjList<BoundExpression> excluded = keys.getExcludedConjuncts();
        BoundExpression root = excluded.getQuick(0);
        for (int i = 1, n = excluded.size(); i < n; i++) {
            root = context.getRewriter().combineConjunction(excluded.getQuick(i), root, 0);
        }
        return context.getRewriter().combineConjunction(residual, root, 0);
    }

    /**
     * The output index of the key a single-partition scan can read the requested order of from its bitmap index, or
     * -1.
     */
    private int sortedSymbolIndexKey(ScanPlan scan, BoundExpression residual, boolean hasModel, boolean isTimestampRequired) {
        final SortKeys order = scan.getRequestedOrder();
        if (residual != null || !hasModel || order.isEmpty() || isTimestampRequired || scan.hasHint(ScanPlan.HINT_NO_INDEX) || scan.isUpdate()
                || !intervals.allIntervalsHitOnePartition(table.getPartitionedBy())) {
            return -1;
        }
        final int count = order.getColumnIds().size();
        if (count > 2 || count == 2 && order.getColumnIds().getQuick(1) != scan.getOutput().getTimestampColumnId()) {
            return -1;
        }
        final int index = scan.getOutput().getColumnIndexById(order.getColumnIds().getQuick(0));
        return index >= 0 && table.getIndexType(tableColumnIndexes.getQuick(index)) == IndexType.BITMAP ? index : -1;
    }

    /**
     * The index in the table of the scan column {@code columnId}.
     */
    private int tableColumnIndex(OutputSchema output, int columnId) {
        return tableColumnIndexes.getQuick(output.getColumnIndexById(columnId));
    }

    /**
     * Plans the scans under the node, each input of which requires its designated timestamp as
     * {@link OperatorPlanning#requiresInputTimestamp} decides from {@code isTimestampRequired}, the requirement of the
     * node's consumer, then the operators of the node.
     */
    private void walk(LogicalPlan plan, boolean isTimestampRequired) throws SqlException {
        if (plan instanceof AggregatePlan producer && producer.getReaderCount() > 0) {
            if (walkedProducers.indexOf(producer) > -1) {
                return;
            }
            walkedProducers.add(producer);
        }
        switch (plan) {
            case ScanPlan scan -> planScan(scan, null, null, isTimestampRequired);
            // A client-side WAL UPDATE does not fuse its predicate, yet raises the bound errors WAL application raises.
            case FilterPlan filter when filter.getChild() instanceof ScanPlan scan -> {
                planScan(scan, filter.getPredicate(), null, operatorPlanning.requiresInputTimestamp(filter, 0, isTimestampRequired));
                planSubqueries(scan);
            }
            case LatestByPlan latest -> walkLatestBy(latest, isTimestampRequired);
            case AggregatePlan aggregate -> walkAggregate(aggregate, isTimestampRequired);
            case DomainReadPlan read -> walk(read.getProducer(), false);
            case JoinPlan join -> walkJoin(join, isTimestampRequired);
            case WindowJoinPlan windowJoin -> walkWindowJoin(windowJoin, isTimestampRequired);
            case HorizonJoinPlan horizon -> walkHorizon(horizon, isTimestampRequired);
            case SampleByPlan sample ->
                    walk(PlanToFactoryRules.sampleByBase(sample), operatorPlanning.requiresInputTimestamp(sample, 0, isTimestampRequired));
            default -> {
                for (int i = 0, n = plan.inputCount(); i < n; i++) {
                    walk(plan.inputAt(i), operatorPlanning.requiresInputTimestamp(plan, i, isTimestampRequired));
                }
            }
        }
        planSubqueries(plan);
        operatorPlanning.planOperators(plan, isTimestampRequired);
    }

    private void walkAggregate(AggregatePlan aggregate, boolean isTimestampRequired) throws SqlException {
        if (PlanToFactoryRules.aggregateFactory(aggregate) == PlanToFactoryRules.AggregateFactory.HORIZON) {
            final HorizonJoinPlan horizon = (HorizonJoinPlan) aggregate.getChild();
            final boolean isHorizonTimestampRequired = operatorPlanning.requiresInputTimestamp(aggregate, 0, isTimestampRequired);
            walkHorizon(horizon, isHorizonTimestampRequired);
            planSubqueries(horizon);
            return;
        }
        if (!planPosting(aggregate)) {
            walk(aggregate.getChild(), operatorPlanning.requiresInputTimestamp(aggregate, 0, isTimestampRequired));
            return;
        }
        planScanSubqueries(aggregate.getChild(), PlanToFactoryRules.postingDistinctScan(aggregate));
    }

    private void walkHorizon(HorizonJoinPlan horizon, boolean isTimestampRequired) throws SqlException {
        walk(horizon.getMaster(), operatorPlanning.requiresInputTimestamp(horizon, 0, isTimestampRequired));
        for (int i = 0, n = horizon.getSlaves().size(); i < n; i++) {
            final LogicalPlan slave = horizon.getSlaves().getQuick(i).getInput();
            final FilterPlan filter = PlanToFactoryRules.temporalStolenFilter(slave);
            final ScanPlan previous = horizonSlaveScan;
            horizonSlaveScan = filter != null ? PlanToFactoryRules.fusedScan(filter) : null;
            try {
                walk(slave, operatorPlanning.requiresInputTimestamp(horizon, i + 1, isTimestampRequired));
            } finally {
                horizonSlaveScan = previous;
            }
        }
    }

    private void walkJoin(JoinPlan join, boolean isTimestampRequired) throws SqlException {
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        final boolean wasCapturing = isCapturing;
        try {
            walk(ordered.getQuick(0).getInput(), operatorPlanning.requiresInputTimestamp(join, 0, isTimestampRequired));
            operatorPlanning.addGeneratedInput(ordered.getQuick(0));
            isCapturing = false;
            for (int i = 1, n = ordered.size(); i < n; i++) {
                final JoinInput step = ordered.getQuick(i);
                if (step.getInput() != null) {
                    walk(step.getInput(), operatorPlanning.requiresInputTimestamp(join, i, isTimestampRequired));
                    operatorPlanning.addGeneratedInput(step);
                }
            }
        } finally {
            isCapturing = wasCapturing;
        }
    }

    private void walkLatestBy(LatestByPlan latest, boolean isTimestampRequired) throws SqlException {
        final LogicalPlan input = latest.getChild();
        final ScanPlan scan = PlanToFactoryRules.latestByScan(latest);
        final boolean isInputTimestampRequired = operatorPlanning.requiresInputTimestamp(latest, 0, isTimestampRequired);
        if (scan != null) {
            planScan(scan, input instanceof FilterPlan filter ? filter.getPredicate() : null, latest, isInputTimestampRequired);
            planScanSubqueries(input, scan);
            return;
        }
        walk(PlanToFactoryRules.latestByBase(latest), isInputTimestampRequired);
    }

    private void walkWindowJoin(WindowJoinPlan windowJoin, boolean isTimestampRequired) throws SqlException {
        final boolean wasCapturing = isCapturing;
        final int level = ++captureLevel;
        if (capturedIntervals.size() == level) {
            capturedIntervals.add(new LongList());
        }
        capturedIntervals.getQuick(level).clear();
        capturedTypes.extendAndSet(level, MODEL_NONE);
        try {
            isCapturing = true;
            walk(windowJoin.getMaster(), operatorPlanning.requiresInputTimestamp(windowJoin, 0, isTimestampRequired));
            isCapturing = false;
            for (int i = 0, n = windowJoin.getSteps().size(); i < n; i++) {
                final WindowJoinStep step = windowJoin.getSteps().getQuick(i);
                pendingStep = capturedTypes.getQuick(level) != MODEL_NONE && step.isTableSource() && !step.isDynamic() ? step : null;
                walk(step.getSlave(), operatorPlanning.requiresInputTimestamp(windowJoin, i + 1, isTimestampRequired));
                pendingStep = null;
            }
        } finally {
            captureLevel--;
            isCapturing = wasCapturing;
            pendingStep = null;
        }
    }

    /**
     * The sub-queries the last {@link #plan} planned, in the order it reached them.
     */
    ObjList<Subquery> getSubqueries() {
        return subqueries;
    }

    /**
     * Plans the access path of every scan of a plan the generator builds at {@code depth}, whose consumer requires
     * its designated timestamp when {@code isTimestampRequired}, and the operators over them, which keep the order of
     * the rows an UPDATE writes back, see {@link OperatorPlanning#readUpdateOrder}; rejects what the generator cannot
     * build.
     */
    void plan(LogicalPlan root, int depth, boolean isTimestampRequired) throws SqlException {
        subqueries.clear();
        cursors.clear();
        walkedProducers.clear();
        captureLevel = -1;
        horizonSlaveScan = null;
        isCapturing = false;
        pendingStep = null;
        this.depth = depth;
        operatorPlanning.clear();
        try {
            operatorPlanning.countSharedConsumers(root);
            walk(root, isTimestampRequired);
            operatorPlanning.readUpdateOrder(root);
        } finally {
            operatorPlanning.settleMasterSides();
        }
    }
}
