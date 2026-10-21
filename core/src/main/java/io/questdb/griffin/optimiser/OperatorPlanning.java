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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableAccessInfo;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.engine.orderby.SortKeyEncoder;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.FillMode;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.HorizonJoinSlave;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PhysicalProperties;
import io.questdb.griffin.plan.logical.PlanToFactoryRules;
import io.questdb.griffin.plan.logical.PlanVisitor;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationKind;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortDirection;
import io.questdb.griffin.plan.logical.SortKeys;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowJoinStep;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

/**
 * Owns the order-sensitive physical operators. Once {@link AccessPathPlanning} has planned the inputs of a node, it
 * calls {@link #planOperators}, which decides from the {@link PhysicalProperties} of the inputs how the generator
 * implements the node and records it on the plan: the sort algorithm, the operator that applies a LIMIT, the windows
 * whose order the input delivers and the window factory, the SAMPLE BY factory and the order a fill reads its input
 * in, the merge of a UNION ALL, the algorithm and master side of each join step and how a keyed ASOF join scans the
 * time frames of its slave, the LATEST BY algorithm of a derived
 * input, the timestamp a projection over a window join drops and whether a filter runs in parallel. The walk asks
 * {@link #requiresInputTimestamp} which inputs of a node must designate their timestamp and, before it plans a query
 * level, counts the decorrelation domains that re-read each aggregate, see {@link #countSharedConsumers}; it decides
 * how each domain reads its input from the join inputs the generator builds before it, see {@link #planSharedRead}.
 * The generator builds exactly what it records. From the same properties it rejects a time series join, a SAMPLE BY and
 * an aggregate function whose input does not deliver the ascending designated timestamp order they require, and a
 * full-fat ASOF or LT join whose slave has a column the join cannot carry.
 */
final class OperatorPlanning implements Mutable {
    private static final PlanVisitor SHARED_CONSUMERS = plan -> {
        if (plan instanceof AggregatePlan aggregate && aggregate.getSharedSource() != null
                && sharedTarget(aggregate.getSharedSource().getInput()) instanceof AggregatePlan target) {
            target.setSharedConsumerCount(target.getSharedConsumerCount() + 1);
        }
        return TreeWalk.CONTINUE;
    };
    private static final PlanVisitor SHARED_CONSUMER_COUNTS = plan -> {
        if (plan instanceof AggregatePlan aggregate) {
            aggregate.setSharedConsumerCount(0);
        }
        return TreeWalk.CONTINUE;
    };
    private final CairoConfiguration configuration;
    private final OptimiserContext context;
    private final ObjList<JoinInput> generatedInputs = new ObjList<>();
    private final IntList readIds;
    private final IntList readOrder;
    private final ObjList<JoinInput> undecidedSteps;
    private int frameInputLo;
    private boolean isFullFatJoins;

    OperatorPlanning(CairoConfiguration configuration, OptimiserContext context, IntList readOrder, IntList readIds,
                     ObjList<JoinInput> undecidedSteps) {
        this.configuration = configuration;
        this.context = context;
        this.readOrder = readOrder;
        this.readIds = readIds;
        this.undecidedSteps = undecidedSteps;
    }

    @Override
    public void clear() {
        generatedInputs.clear();
        frameInputLo = 0;
        undecidedSteps.clear();
    }

    /**
     * What a factory that computes column {@code columnId} of the plan exposes of its symbol table: the one the plan
     * records for it, without an index.
     */
    private static SymbolKey computedSymbolKey(LogicalPlan plan, int columnId) {
        final OutputSchema output = plan.getOutput();
        return output.isSymbolTableStatic(output.getColumnIndexById(columnId)) ? SymbolKey.STATIC : SymbolKey.NONE;
    }

    private static boolean hasEarlyExit(ObjList<FunctionExpression> aggregates) {
        for (int i = 0, n = aggregates.size(); i < n; i++) {
            if ((aggregates.getQuick(i).getFunctionFlags() & BoundExpression.EARLY_EXIT) == 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * True when the factory the generator builds for the plan carries a filter: the parallel, serial or gating filter
     * over the predicate of a filter node or the residual of a scan, which a selection, a sort or LIMIT the input
     * implements, a single-input join and a null-extending window join pass through.
     */
    private static boolean hasFilter(LogicalPlan plan) {
        while (true) {
            switch (plan) {
                case ProjectPlan project when PlanToFactoryRules.projectionFactory(project) == PlanToFactoryRules.ProjectionFactory.SELECTION ->
                        plan = project.getChild();
                case SortPlan sort when sort.getAlgorithm() == SortPlan.Algorithm.INPUT_ORDER
                        || sort.getAlgorithm() == SortPlan.Algorithm.TIMESTAMP_DECLARATION -> plan = sort.getChild();
                case LimitPlan limit when limit.getApplication() == LimitPlan.Application.INPUT ->
                        plan = limit.getChild();
                case JoinPlan join when join.getOrderedInputs().size() == 1 ->
                        plan = join.getOrderedInputs().getQuick(0).getInput();
                case FilterPlan filter when !PlanToFactoryRules.isFusedFilter(filter) -> {
                    final BoundExpression predicate = filter.getPredicate();
                    if (!LogicalPlans.isConstant(predicate)) {
                        return true;
                    }
                    if (predicate instanceof ConstantExpression constant && constant.getLongValue() == 0) {
                        return false;
                    }
                    plan = filter.getChild();
                }
                case FilterPlan filter -> {
                    return hasScanFilter((ScanPlan) filter.getChild());
                }
                case ScanPlan scan -> {
                    return hasScanFilter(scan);
                }
                case WindowJoinPlan windowJoin -> {
                    if (!windowJoin.isMasterPassedThrough()) {
                        return false;
                    }
                    plan = windowJoin.getMaster();
                }
                default -> {
                    return false;
                }
            }
        }
    }

    private static boolean hasNoParallelism(ObjList<? extends BoundExpression> expressions) {
        for (int i = 0, n = expressions.size(); i < n; i++) {
            if ((expressions.getQuick(i).getFunctionFlags() & BoundExpression.NO_PARALLELISM) != 0) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasScanFilter(ScanPlan scan) {
        final BoundExpression residual = scan.getResidual();
        return switch (scan.getAccessPath()) {
            case PAGE_FRAMES -> residual != null && !LogicalPlans.isConstant(residual);
            case SYMBOL_INDEX -> scan.getIndexRead() == ScanPlan.IndexRead.COVERING && residual != null;
            case SYMBOL_PATTERN -> true;
            case null, default -> false;
        };
    }

    private static boolean isEncodedKey(WindowSpec spec, OutputSchema input) {
        final IntList keys = spec.getOrderByColumnIds();
        for (int i = 0, n = keys.size(); i < n; i++) {
            if (!SortKeyEncoder.isEncodable(ColumnType.tagOf(input.getColumnType(input.getColumnIndexById(keys.getQuick(i)))))) {
                return false;
            }
        }
        return true;
    }

    /**
     * True when every key of the SAMPLE BY is its timestamp or one SYMBOL column and every aggregate is first() or
     * last() of a non-array column, the shape a SAMPLE BY reads from the index of its symbol key.
     */
    private static boolean isFirstLastShape(SampleByPlan sample, int timestampIndex, int baseTimestampIndex, int symbolIndex) {
        final OutputSchema input = sample.getChild().getOutput();
        final ObjList<BoundExpression> keys = sample.getGroupingExpressions();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final ColumnExpression column = (ColumnExpression) keys.getQuick(i);
            final int index = input.getColumnIndexById(column.getColumnId());
            if (index == timestampIndex && index == baseTimestampIndex) {
                continue;
            }
            if (input.getColumnType(index) != ColumnType.SYMBOL || index != symbolIndex || !column.isDirectReference()) {
                return false;
            }
        }
        final ObjList<FunctionExpression> aggregates = sample.getAggregates();
        for (int i = 0, n = aggregates.size(); i < n; i++) {
            final FunctionExpression call = aggregates.getQuick(i);
            if (call.getArgumentCount() != 1 || !(call.argumentAt(0) instanceof ColumnExpression column)
                    || !column.isDirectReference() || ColumnType.isArray(column.getDataType())
                    || !SqlKeywords.isFirstKeyword(call.getName()) && !SqlKeywords.isLastKeyword(call.getName())) {
                return false;
            }
        }
        return true;
    }

    /**
     * True when the generator gates its input once on the predicate, a runtime constant.
     */
    private static boolean isGate(BoundExpression predicate) {
        return !LogicalPlans.isConstant(predicate) && !isFiltering(predicate);
    }

    /**
     * Whether a capability an algorithm choice reads holds; the plan must carry it.
     */
    private static boolean isKnownYes(PhysicalProperties.Capability capability) {
        if (capability == PhysicalProperties.Capability.UNKNOWN) {
            throw new IllegalStateException("physical capability is unknown at planning");
        }
        return capability == PhysicalProperties.Capability.YES;
    }

    /**
     * True when the generator runs the filter over the residual of a fused scan in parallel, given the parallel filter
     * is enabled: over page frames and within a pattern scan; over a covering index without a backup index scan,
     * unless a LIMIT that may be negative asks a multi-key covering index for the backward frames it cannot serve.
     */
    private static boolean isParallelResidual(ScanPlan scan) {
        return switch (scan.getAccessPath()) {
            case PAGE_FRAMES, SYMBOL_PATTERN -> true;
            case SYMBOL_INDEX -> {
                final BoundExpression limit = scan.getCoveredFilterLimit();
                yield !scan.hasCoveringBackup() && (limit == null || scan.getIndexKeys().size() == 1 || !LogicalPlans.mayBeNegativeLimit(limit));
            }
            default -> false;
        };
    }

    /**
     * The aggregate a decorrelation domain re-reads through its shared source, as the generator finds it: under the
     * column projections and the leading branches of the set operations of the source's input.
     */
    private static LogicalPlan sharedTarget(LogicalPlan source) {
        return LogicalPlans.unwrapColumnProjections(leadingBranch(source));
    }

    /**
     * Rejects the full-fat temporal join step whose slave has a column the join cannot carry, see
     * {@link PlanToFactoryRules#fullFatTemporalRejectedColumn}.
     */
    private static void validateFullFatTemporalSlave(JoinInput step) throws SqlException {
        final int rejectedIndex = PlanToFactoryRules.fullFatTemporalRejectedColumn(step);
        if (rejectedIndex >= 0) {
            throw SqlException.position(step.getPosition()).put("right side column '")
                    .put(PlanToFactoryRules.factoryColumnName(step.getInput(), rejectedIndex)).put("' is of unsupported type");
        }
    }

    /**
     * Rejects an aggregate function that requires the designated timestamp of its input as its second argument when
     * the argument is not that column, at {@code timestampIndex} of {@code input}, and one that requires ascending
     * designated timestamp order when the input does not deliver it.
     */
    private static void validateTimestampAggregates(ObjList<FunctionExpression> aggregates, OutputSchema input, int timestampIndex,
                                                    boolean isAscending) throws SqlException {
        for (int i = 0, n = aggregates.size(); i < n; i++) {
            final FunctionExpression call = aggregates.getQuick(i);
            final int flags = call.getFunctionFlags();
            if ((flags & BoundExpression.TIMESTAMP_ARGUMENT) != 0 && (timestampIndex < 0
                    || !(call.argumentAt(1) instanceof ColumnExpression column) || input.getColumnIndexById(column.getColumnId()) != timestampIndex)) {
                throw SqlException.$(call.getPosition(), call.getName()).put("() requires the table's designated timestamp as the second argument");
            }
            if ((flags & BoundExpression.ASCENDING_TIMESTAMP) != 0 && !isAscending) {
                throw SqlException.$(call.getPosition(), call.getName()).put("() requires the base query to provide ascending designated timestamp order");
            }
        }
    }

    /**
     * The direction a validation reads; the plan must carry it.
     */
    private static PhysicalProperties.ScanDirection validatedDirection(PhysicalProperties.ScanDirection direction) {
        if (direction == PhysicalProperties.ScanDirection.UNKNOWN) {
            throw new IllegalStateException("scan direction is unknown at planning");
        }
        return direction;
    }

    /**
     * The GROUP BY the generator builds over the base it reads: the vectorised one for an aggregate of that shape
     * over page frames of a table whose parquet partitions store no column in a converted type, which the vectorised
     * one cannot read, else the parallel one when every key and aggregate function supports parallelism and the base
     * exposes page frames or a filter the GROUP BY steals, unless a keyless aggregate stops early over an unfiltered
     * base; serial otherwise, as is a consumer that re-reads a shared source's cursor.
     */
    private AggregatePlan.Algorithm aggregateAlgorithm(AggregatePlan aggregate, LogicalPlan base) {
        final SqlExecutionContext executionContext = context.getExecutionContext();
        final JoinInput source = aggregate.getSharedSource();
        if (!executionContext.isParallelGroupByEnabled() || source != null
                && PhysicalProperties.supportsSharedCursors(source.getInput()) != PhysicalProperties.Capability.NO) {
            return AggregatePlan.Algorithm.SERIAL;
        }
        final boolean isPageFrameSource = isPageFrameSource(base);
        if (isPageFrameSource && PlanToFactoryRules.hasVectorShape(aggregate) && !hasParquetConvertedColumns(base)) {
            return AggregatePlan.Algorithm.VECTORISED;
        }
        final boolean isFilterStolen = !isPageFrameSource && isStolenFilter(base, FilterConsumer.FRAME_REDUCER);
        if (!isParallelGroupBy(aggregate, base) || !isPageFrameSource && !isFilterStolen) {
            return AggregatePlan.Algorithm.SERIAL;
        }
        return isFilterStolen ? AggregatePlan.Algorithm.PARALLEL_STOLEN_FILTER : AggregatePlan.Algorithm.PARALLEL;
    }

    /**
     * True when the page frames the plan serves come from a table whose parquet partitions store a column under a
     * converted type.
     */
    private boolean hasParquetConvertedColumns(LogicalPlan plan) {
        while (plan.inputCount() == 1) {
            plan = plan.inputAt(0);
        }
        return plan instanceof ScanPlan scan && context.getTableAccessInfo(scan).hasParquetConvertedColumns();
    }

    /**
     * How the generator reads the master of the HORIZON JOIN: in parallel over its page frames, or over the frames
     * under a filter it steals from the master, when the functions of its consumer support parallelism and a
     * projection over a master with random access stays {@link PlanToFactoryRules#isHorizonWithinSlotCap within the
     * slot cap}; by row id otherwise, which rejects a master without random access.
     */
    private HorizonJoinPlan.Algorithm horizonAlgorithm(HorizonJoinPlan horizon, boolean hasParallelFunctions) throws SqlException {
        final LogicalPlan master = horizon.getMaster();
        final boolean isRandomAccess = isRandomAccess(master);
        HorizonJoinPlan.Algorithm algorithm = HorizonJoinPlan.Algorithm.SERIAL;
        if (context.getExecutionContext().isParallelHorizonJoinEnabled() && hasParallelFunctions
                && (horizon.getConsumer() == HorizonJoinPlan.Consumer.AGGREGATE || !isRandomAccess || PlanToFactoryRules.isHorizonWithinSlotCap(horizon))) {
            if (isPageFrameSource(master)) {
                algorithm = HorizonJoinPlan.Algorithm.PARALLEL;
            } else if (isStolenFilter(master, FilterConsumer.FRAME_REDUCER)) {
                algorithm = HorizonJoinPlan.Algorithm.PARALLEL_STOLEN_FILTER;
            }
        }
        if (algorithm == HorizonJoinPlan.Algorithm.SERIAL && !isRandomAccess) {
            throw SqlException.$(horizon.getSlaves().getQuick(0).getPosition(), "left-hand side of HORIZON JOIN can only be a table with an optional filter");
        }
        return algorithm;
    }

    private boolean isAdviceFollowed(LogicalPlan plan) {
        return PhysicalProperties.followsOrderAdvice(plan) == PhysicalProperties.Capability.YES;
    }

    /**
     * Reads the order of the master of a join step, the first {@code inputCount} ordered inputs of the join, and
     * reports whether it emits its rows in ascending designated timestamp order.
     */
    private boolean isAscendingMasterRead(JoinPlan join, int inputCount) {
        readJoinOrder(join, inputCount, false, true);
        return PhysicalProperties.masterScanDirection(join, inputCount) == PhysicalProperties.ScanDirection.FORWARD;
    }

    /**
     * True when the SAMPLE BY reads the first and last values of its buckets from the index of its base, see
     * {@link #firstLastIndexScan}, the bitmap index of a table whose parquet partitions store no column under a
     * converted type.
     */
    private boolean isFirstLastIndexScan(SampleByPlan sample, LogicalPlan base) {
        final ScanPlan scan = firstLastIndexScan(sample, base);
        if (scan == null) {
            return false;
        }
        final TableAccessInfo table = context.getTableAccessInfo(scan);
        final OutputSchema output = scan.getOutput();
        return !table.hasParquetConvertedColumns()
                && IndexType.isBitmap(table.getIndexType(table.getColumnIndex(output.getColumnName(output.getColumnIndexById(scan.getIndexColumnId())))));
    }

    /**
     * True when the generator builds the factory of the join input before the node operator planning plans now, in the
     * same generation frame, see {@link #addGeneratedInput}.
     */
    private boolean isGenerated(JoinInput input) {
        return generatedInputs.indexOf(input) >= frameInputLo;
    }

    /**
     * True when the temporal join step at {@code inputIndex} of the join's ordered inputs matches on keys, see
     * {@link PlanToFactoryRules#isKeyedTemporalJoin}.
     */
    private boolean isKeyedTemporalStep(JoinPlan join, int inputIndex, JoinInput step) {
        return PlanToFactoryRules.isKeyedTemporalJoin(step, join.getOrderedInputs().getQuick(inputIndex - 1).getOutput(),
                PhysicalProperties.masterTimestampIndex(join, inputIndex), timestampIndex(step.getInput()));
    }

    private boolean isPageFrameSource(LogicalPlan plan) {
        return PhysicalProperties.supportsPageFrameCursor(plan) == PhysicalProperties.Capability.YES;
    }

    private boolean isRandomAccess(LogicalPlan plan) {
        return PhysicalProperties.supportsRandomAccess(plan) == PhysicalProperties.Capability.YES;
    }

    /**
     * True when the factories of the master of the join step at {@code inputIndex} and of its slave report one table,
     * see {@link PhysicalProperties#tableToken}.
     */
    private boolean isSelfJoin(JoinPlan join, int inputIndex) {
        final TableToken master = PhysicalProperties.masterTableToken(join, inputIndex);
        return master != null && master.equals(PhysicalProperties.tableToken(join.getOrderedInputs().getQuick(inputIndex).getInput()));
    }

    /**
     * True when the leading cross input is fixed and the unkeyed step can cache its slave rows: the loop rewinds it for every
     * master row, as every nested loop but a RIGHT join's does, the slave is a join, whose rewind repeats the join, and
     * every step of that join is {@link LogicalPlans#isStableJoinStep stable}, so every evaluation yields its rows.
     */
    private boolean isSlaveMaterialized(JoinPlan join, JoinInput step) {
        if (!join.isLeadingCrossJoinFixed() || step.getJoinType() == JoinKind.RIGHT_OUTER || step.getJoinType() == JoinKind.FULL_OUTER
                || !(step.getInput() instanceof JoinPlan slave)) {
            return false;
        }
        final boolean isParallelGroupByEnabled = context.getExecutionContext().isParallelGroupByEnabled();
        final ObjList<JoinInput> steps = slave.getOrderedInputs();
        for (int i = 0, n = steps.size(); i < n; i++) {
            if (!LogicalPlans.isStableJoinStep(steps.getQuick(i), isParallelGroupByEnabled, null)) {
                return false;
            }
        }
        return true;
    }

    /**
     * True when the consumer steals the filter the generator builds for the plan and reads the frames under it: a
     * parallel filter without a LIMIT, within a pattern scan or above it; the serial filter of a table scan, which a
     * top-K and a frame reducer steal, and of a covering index scan, which a frame reducer steals; and a gate over page frames,
     * which a temporal join steals when the factory under the filter serves time frames, as it must for any filter.
     */
    private boolean isStolenFilter(LogicalPlan plan, FilterConsumer consumer) {
        final FilterPlan filter = PlanToFactoryRules.stolenFilter(plan);
        if (filter == null) {
            return false;
        }
        final boolean isSerialScanFilterStolen = consumer == FilterConsumer.TOP_K || consumer == FilterConsumer.FRAME_REDUCER;
        final boolean isGateStolen = consumer == FilterConsumer.TEMPORAL_JOIN;
        final boolean isStolen;
        if (!PlanToFactoryRules.isFusedFilter(filter)) {
            final BoundExpression predicate = filter.getPredicate();
            isStolen = isPageFrameSource(filter.getChild())
                    && (isFiltering(predicate) ? filter.getAlgorithm() == FilterPlan.Algorithm.PARALLEL : isGateStolen && isGate(predicate));
        } else {
            final ScanPlan scan = (ScanPlan) filter.getChild();
            final BoundExpression residual = scan.getResidual();
            final boolean isLimited = PhysicalProperties.implementsLimit(filter) == PhysicalProperties.Capability.YES;
            final boolean isCovering = scan.getIndexRead() == ScanPlan.IndexRead.COVERING;
            final boolean isParallel = scan.getResidualAlgorithm() == FilterPlan.Algorithm.PARALLEL;
            isStolen = switch (scan.getAccessPath()) {
                case PAGE_FRAMES -> residual != null && (isFiltering(residual)
                        ? (isParallel || isSerialScanFilterStolen) && !isLimited : isGateStolen && isGate(residual));
                case SYMBOL_INDEX ->
                        isCovering && residual != null && isFiltering(residual) && !scan.hasCoveringBackup()
                                && (isParallel ? !isLimited : consumer == FilterConsumer.FRAME_REDUCER);
                case SYMBOL_PATTERN -> !isCovering ? isParallel : isParallel ? !isLimited : isSerialScanFilterStolen;
                case null, default -> false;
            };
        }
        return isStolen && (consumer != FilterConsumer.TEMPORAL_JOIN
                || PhysicalProperties.supportsLeafTimeFrameCursor(filter) == PhysicalProperties.Capability.YES);
    }

    /**
     * True when an ASOF join steals the filter of its slave, read directly or through the selection the generator
     * builds the slave from.
     */
    private boolean isStolenTemporalFilter(LogicalPlan slave) {
        final FilterPlan filter = PlanToFactoryRules.temporalStolenFilter(slave);
        return filter != null && isStolenFilter(filter, FilterConsumer.TEMPORAL_JOIN);
    }

    /**
     * Maps every input column of the decorrelation domain onto the column of its shared source it reads, by the pair
     * decorrelation recorded for it, else by name, and, when each one maps onto a column of its type, replaces the pairs
     * with one per input column, in input order; returns whether it did.
     */
    private boolean mapSharedColumns(AggregatePlan aggregate, JoinInput source) {
        final OutputSchema sourceOutput = source.getSourceOutput();
        final OutputSchema input = aggregate.getChild().getOutput();
        final IntList inputIds = aggregate.getSharedInputIds();
        final IntList sourceIds = aggregate.getSharedSourceIds();
        final IntList mappedIds = readIds;
        mappedIds.clear();
        for (int i = 0, n = input.getColumnCount(); i < n; i++) {
            final int pair = inputIds.indexOf(input.getColumnId(i), 0, inputIds.size());
            final int index = pair < 0 ? sourceOutput.getColumnIndexQuiet(input.getColumnName(i))
                    : sourceOutput.getColumnIndexById(sourceIds.getQuick(pair));
            if (index < 0 || sourceOutput.getColumnType(index) != input.getColumnType(i)) {
                return false;
            }
            mappedIds.add(sourceOutput.getColumnId(index));
        }
        inputIds.clear();
        for (int i = 0, n = input.getColumnCount(); i < n; i++) {
            inputIds.add(input.getColumnId(i));
        }
        sourceIds.clear();
        sourceIds.addAll(mappedIds);
        return true;
    }

    /**
     * Chooses the parallel top-K of a bounded sort, which reads the page frames under its input: it builds at most one
     * projection over itself, see {@link PlanToFactoryRules#parallelTopKProjection}, reads every sort key from the input of
     * that projection, and reads the page frames of that input, or steals its filter. When the projection's input is
     * another projection, the whole input must be a source of page frames.
     */
    private SortPlan.Algorithm parallelTopK(SortPlan sort) {
        final SqlExecutionContext executionContext = context.getExecutionContext();
        if (!executionContext.isParallelTopKEnabled()) {
            return SortPlan.Algorithm.LIMITED;
        }
        final LogicalPlan base = PlanToFactoryRules.generatedPlan(sort.getChild());
        final ProjectPlan projection = PlanToFactoryRules.parallelTopKProjection(sort);
        if (projection == null && PlanToFactoryRules.isPeelableProjection(base)) {
            return isPageFrameSource(base) ? SortPlan.Algorithm.PARALLEL_TOP_K : SortPlan.Algorithm.LIMITED;
        }
        final LogicalPlan source = projection != null ? PlanToFactoryRules.generatedPlan(projection.getChild()) : base;
        final SortPlan.Algorithm algorithm;
        if (isPageFrameSource(source)) {
            algorithm = SortPlan.Algorithm.PARALLEL_TOP_K;
        } else if (isStolenFilter(source, FilterConsumer.TOP_K)) {
            algorithm = SortPlan.Algorithm.PARALLEL_FILTERED_TOP_K;
        } else {
            return SortPlan.Algorithm.LIMITED;
        }
        if (projection == null || !LogicalPlans.isComputedProjection(projection)) {
            return algorithm;
        }
        final OutputSchema input = projection.getChild().getOutput();
        final OutputSchema sorted = sort.getChild().getOutput();
        for (int i = 0, n = sort.getColumnIds().size(); i < n; i++) {
            final int index = sorted.getColumnIndexById(sort.getColumnIds().getQuick(i));
            if (projection.hasUpdateConversions() || !(projection.getExpressions().getQuick(index) instanceof ColumnExpression column)
                    || input.getColumnIndexById(column.getColumnId()) < 0) {
                return SortPlan.Algorithm.LIMITED;
            }
        }
        return algorithm;
    }

    private void planAggregate(AggregatePlan aggregate) throws SqlException {
        final PlanToFactoryRules.AggregateFactory factory = PlanToFactoryRules.aggregateFactory(aggregate);
        if (factory == PlanToFactoryRules.AggregateFactory.HORIZON) {
            final HorizonJoinPlan horizon = (HorizonJoinPlan) aggregate.getChild();
            validateHorizonInputs(horizon);
            validateTimestampAggregates(aggregate.getAggregates(), horizon.getOutput(), timestampIndex(horizon.getMaster()), false);
            aggregate.setAlgorithm(switch (horizonAlgorithm(horizon, hasParallelFunctions(aggregate))) {
                case SERIAL -> AggregatePlan.Algorithm.SERIAL;
                case PARALLEL -> AggregatePlan.Algorithm.PARALLEL;
                case PARALLEL_STOLEN_FILTER -> AggregatePlan.Algorithm.PARALLEL_STOLEN_FILTER;
            });
            return;
        }
        if (factory != PlanToFactoryRules.AggregateFactory.GROUP_BY) {
            return;
        }
        final LogicalPlan base = PlanToFactoryRules.aggregateBase(aggregate);
        readOrder(base, false, true);
        if (LogicalPlans.hasTimestampAggregate(aggregate.getAggregates())) {
            final LogicalPlan input = LogicalPlans.skipRenames(aggregate.getChild());
            final int baseTimestampIndex = timestampIndex(base);
            final int timestampIndex = base != input ? PlanToFactoryRules.projectedTimestampIndex((ProjectPlan) input, baseTimestampIndex) : baseTimestampIndex;
            validateTimestampAggregates(aggregate.getAggregates(), aggregate.getChild().getOutput(), timestampIndex,
                    baseTimestampIndex == timestampIndex && scanDirection(base) == PhysicalProperties.ScanDirection.FORWARD);
        }
        aggregate.setAlgorithm(aggregateAlgorithm(aggregate, base));
    }

    /**
     * Decides how the fill reads the buckets of its input.
     */
    private void planFill(FillPlan fill) {
        fill.setAlgorithm(isSampleByRowsFill(fill) ? FillPlan.Algorithm.SAMPLE_BY_ROWS : FillPlan.Algorithm.SORTED);
    }

    /**
     * Decides whether the generator runs a filter in parallel: over the page frames of its input when the parallel
     * filter is enabled, recorded on the filter, or, for a filter fused into the scan under it, as
     * {@link #isParallelResidual} decides, recorded on the scan.
     */
    private void planFilter(FilterPlan filter) {
        final boolean isParallelEnabled = context.getExecutionContext().isParallelFilterEnabled();
        if (!PlanToFactoryRules.isFusedFilter(filter)) {
            filter.setAlgorithm(!isFiltering(filter.getPredicate()) ? null
                    : PlanToFactoryRules.isWindowKeepFlag(filter) ? FilterPlan.Algorithm.WINDOW_KEEP_FLAG
                      : isParallelEnabled && isKnownYes(PhysicalProperties.supportsPageFrameCursor(filter.getChild())) ? FilterPlan.Algorithm.PARALLEL : FilterPlan.Algorithm.SERIAL);
            return;
        }
        final ScanPlan scan = (ScanPlan) filter.getChild();
        scan.setResidualAlgorithm(!hasResidualFilterChoice(scan) ? null
                : isParallelEnabled && isParallelResidual(scan) ? FilterPlan.Algorithm.PARALLEL : FilterPlan.Algorithm.SERIAL);
    }

    /**
     * Decides how the generator reads the master of a HORIZON JOIN whose rows a projection reads; the projection
     * evaluates its expressions on the thread that reads the rows, so they never stop the join running in parallel.
     */
    private void planHorizon(HorizonJoinPlan horizon) throws SqlException {
        if (horizon.getConsumer() != HorizonJoinPlan.Consumer.PROJECTION) {
            throw new IllegalStateException("fused horizon join is planned by its aggregate");
        }
        validateHorizonInputs(horizon);
        horizon.setAlgorithm(horizonAlgorithm(horizon, true));
    }

    /**
     * Rejects a HORIZON JOIN input the join consumes the designated timestamp of when its factory designates none, an
     * input that does not deliver ascending designated timestamp order, and a slave whose rows the join cannot match
     * through time-frame cursors.
     */
    private void validateHorizonInputs(HorizonJoinPlan horizon) throws SqlException {
        final LogicalPlan master = horizon.getMaster();
        validateConsumedTimestamp(master);
        final ObjList<HorizonJoinSlave> slaves = horizon.getSlaves();
        for (int i = 0, n = slaves.size(); i < n; i++) {
            final HorizonJoinSlave slave = slaves.getQuick(i);
            validateConsumedTimestamp(slave.getInput());
            validateTimeSeriesOrders(master, slave.getInput(), slave.getPosition());
        }
        for (int i = 0, n = slaves.size(); i < n; i++) {
            final HorizonJoinSlave slave = slaves.getQuick(i);
            if (!isKnownYes(PhysicalProperties.supportsHorizonSlaveTimeFrameCursor(slave.getInput()))) {
                throw SqlException.$(slave.getPosition(), "right-hand side of HORIZON JOIN can only be a table with an optional filter");
            }
        }
    }

    private void planJoin(JoinPlan join) throws SqlException {
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        for (int i = 0, n = ordered.size(); i < n; i++) {
            final JoinInput step = ordered.getQuick(i);
            if (step.getInput() != null && LogicalPlans.consumesInputTimestamp(join, i)) {
                validateConsumedTimestamp(step.getInput());
            }
        }
        for (int i = 1, n = ordered.size(); i < n; i++) {
            final JoinInput step = ordered.getQuick(i);
            final LogicalPlan slave = step.getInput();
            if (step.getJoinType().isRelational()) {
                planRelationalStep(join, i, step);
                continue;
            }
            switch (step.getJoinType()) {
                case UNNEST -> step.setAlgorithm(JoinInput.Algorithm.UNNEST);
                case ASOF, LT -> {
                    validateTimeSeriesOrders(join, i, slave, step.getPosition());
                    final JoinInput.Algorithm algorithm = temporalAlgorithm(join, i, step);
                    if (algorithm == JoinInput.Algorithm.FULL_FAT_TEMPORAL) {
                        validateFullFatTemporalSlave(step);
                    }
                    step.setAlgorithm(algorithm);
                    step.setTimeFrameScan(algorithm == JoinInput.Algorithm.TEMPORAL_TIME_FRAME && step.getJoinType() == JoinKind.ASOF
                            && isKeyedTemporalStep(join, i, step) ? timeFrameScan(join, i, step) : null);
                }
                case SPLICE -> {
                    validateTimeSeriesOrders(join, i, slave, step.getPosition());
                    step.setAlgorithm(isFullFatJoins ? JoinInput.Algorithm.FULL_FAT_SPLICE : JoinInput.Algorithm.SPLICE);
                }
            }
        }
    }

    private void planLatestBy(LatestByPlan latest) {
        if (PlanToFactoryRules.latestByScan(latest) != null) {
            return;
        }
        final LogicalPlan input = latest.getChild();
        final LogicalPlan base = PlanToFactoryRules.latestByBase(latest);
        boolean isAscending = false;
        if (latest.isTimestampOrderInherited() && input.getOutput().getColumnIndexById(latest.getTimestampColumnId())
                == PhysicalProperties.timestampIndex(base)) {
            readOrder(base, false, true);
            isAscending = PhysicalProperties.scanDirection(base) == PhysicalProperties.ScanDirection.FORWARD;
        }
        latest.setAlgorithm(!isRandomAccess(base) ? LatestByPlan.Algorithm.MATERIALIZED
                : isAscending ? LatestByPlan.Algorithm.ASCENDING_LIGHT : LatestByPlan.Algorithm.LIGHT);
    }

    /**
     * Decides which operator applies the LIMIT and, over a sort, how the sort implements it.
     */
    private void planLimit(LimitPlan limit) {
        final LogicalPlan limited = limit.getChild();
        if (!LogicalPlans.hasSortUnderStableProjects(limited)) {
            limit.setApplication(isInputLimit(limited) ? LimitPlan.Application.INPUT
                    : PlanToFactoryRules.masterSuffixScan(limit) != null ? LimitPlan.Application.MASTER_SUFFIX : LimitPlan.Application.OPERATOR);
            return;
        }
        final SortPlan sort = (SortPlan) LogicalPlans.skipProjects(limited);
        final LogicalPlan input = sort.getChild();
        final int keyIndex = input.getOutput().getColumnIndexById(sort.getColumnIds().getQuick(0));
        final boolean isTimestampOrdered = isTimestampOrdered(sort);
        final boolean isOrderDelivered = isAdviceFollowed(input) && PlanToFactoryRules.hasAdvisedInput(input)
                || sort.getColumnIds().size() == 1 && isTimestampOrdered;
        if (isInputLimit(input)) {
            limit.setApplication(LimitPlan.Application.INPUT);
            sort.setAlgorithm(isOrderDelivered ? SortPlan.Algorithm.INPUT_ORDER : sortAlgorithm(input));
            return;
        }
        if (isOrderDelivered) {
            limit.setApplication(LimitPlan.Application.OPERATOR);
            sort.setAlgorithm(SortPlan.Algorithm.INPUT_ORDER);
            return;
        }
        final BoundExpression lo = limit.getLo();
        final BoundExpression hi = limit.getHi();
        if (!isRandomAccess(input) || lo instanceof ConstantExpression loConstant && hi instanceof ConstantExpression hiConstant
                && LogicalPlans.limitValue(loConstant) >= 0 && LogicalPlans.limitValue(hiConstant) < 0) {
            limit.setApplication(LimitPlan.Application.OPERATOR);
            sort.setAlgorithm(sortAlgorithm(input));
            return;
        }
        limit.setApplication(LimitPlan.Application.SORT);
        if (!isTopKLimit(limit, isTimestampOrdered)) {
            sort.setAlgorithm(isTimestampOrdered ? SortPlan.Algorithm.PRESORTED_LIMITED : SortPlan.Algorithm.LIMITED);
        } else if (sort.getColumnIds().size() == 1
                && PhysicalProperties.supportsLongTopK(input, keyIndex) == PhysicalProperties.Capability.YES) {
            sort.setAlgorithm(SortPlan.Algorithm.LONG_TOP_K);
        } else {
            sort.setAlgorithm(parallelTopK(sort));
        }
    }

    private void planRelationalStep(JoinPlan join, int inputIndex, JoinInput step) {
        final LogicalPlan slave = step.getInput();
        final int markoutIndex = join.getOrderedInputs().getQuick(inputIndex - 1).getOutput().getColumnIndexById(step.getMarkoutTimestampColumnId());
        if (markoutIndex >= 0 && markoutIndex == PhysicalProperties.masterTimestampIndex(join, inputIndex)
                && PhysicalProperties.masterSupportsRandomAccess(join, inputIndex) == PhysicalProperties.Capability.YES
                && PhysicalProperties.isLongSequence(slave) == PhysicalProperties.Capability.YES
                && isAscendingMasterRead(join, inputIndex)) {
            step.setAlgorithm(JoinInput.Algorithm.MARKOUT);
        } else if (step.getKeys().size() == 0) {
            step.setAlgorithm(isSlaveMaterialized(join, step) ? JoinInput.Algorithm.MATERIALIZED_NESTED_LOOP : JoinInput.Algorithm.NESTED_LOOP);
        } else {
            final boolean isSlaveRandomAccess = PlanToFactoryRules.isSlaveEmptied(step) || isRandomAccess(slave);
            step.setAlgorithm(isSlaveRandomAccess && !isFullFatJoins ? JoinInput.Algorithm.LIGHT_HASH : JoinInput.Algorithm.HASH);
            if (step.getMasterSide() == null) {
                if (PhysicalProperties.masterSupportsRandomAccess(join, inputIndex) == PhysicalProperties.Capability.NO) {
                    step.setMasterSide(JoinInput.MasterSide.FIXED);
                } else {
                    undecidedSteps.add(step);
                }
            }
        }
    }

    private void planSampleBy(SampleByPlan sample) throws SqlException {
        final LogicalPlan sampled = sample.getChild();
        if (sample.isTimestampRequired()) {
            validateConsumedTimestamp(sampled);
        }
        final LogicalPlan base = PlanToFactoryRules.sampleByBase(sample);
        readOrder(base, false, true);
        final int baseTimestampIndex = timestampIndex(base);
        final int timestampIndex = sample.isTimestampRequired() ? baseTimestampIndex : sampled.getOutput().getColumnIndexById(sample.getTimestampColumnId());
        if (timestampIndex < 0) {
            throw SqlException.$(sample.getPosition(), "base query does not provide designated TIMESTAMP column");
        }
        if (scanDirection(base) != PhysicalProperties.ScanDirection.FORWARD) {
            throw SqlException.$(sample.getPosition(), sample.isJoinInput()
                    ? "ASC order over TIMESTAMP column is required but not provided"
                    : "base query does not provide ASC order over designated TIMESTAMP column");
        }
        validateTimestampAggregates(sample.getAggregates(), sampled.getOutput(), timestampIndex, baseTimestampIndex == timestampIndex);
        sample.setAlgorithm(sampleByAlgorithm(sample, base));
    }

    /**
     * Decides whether a UNION ALL merges its branches in the requested timestamp order, sorting the right branch
     * into it when the branch emits its timestamp in the other direction.
     */
    private void planSetOperation(SetOperationPlan operation) {
        final SortDirection direction = operation.getRequestedOrderDirection();
        final int orderIndex = operation.getOutput().getColumnIndexById(operation.getRequestedOrderColumnId());
        if (operation.getOperation() != SetOperationKind.UNION_ALL || direction == null || orderIndex < 0) {
            operation.setMerge(false, null);
            return;
        }
        final PhysicalProperties.ScanDirection requested = PhysicalProperties.ScanDirection.of(direction);
        final LogicalPlan left = operation.getLeft();
        final LogicalPlan right = operation.getRight();
        final int rightTimestampIndex = PhysicalProperties.timestampIndex(right);
        PhysicalProperties.ScanDirection rightDirection = PhysicalProperties.scanDirection(right);
        SortPlan.Algorithm rightSort = null;
        if (operation.isTimestampOrderPushable(orderIndex) && !(LogicalPlans.skipProjects(right) instanceof SortPlan)
                && rightTimestampIndex == orderIndex) {
            readOrder(right, false, true);
            if (rightDirection != requested) {
                rightSort = sortAlgorithm(right);
                rightDirection = requested;
            }
        }
        boolean isMerged = false;
        if (orderIndex == PhysicalProperties.timestampIndex(left) && orderIndex == rightTimestampIndex
                && left.getOutput().getColumnType(orderIndex) == right.getOutput().getColumnType(orderIndex)) {
            readOrder(left, false, true);
            if (PhysicalProperties.scanDirection(left) == requested) {
                if (rightSort == null) {
                    readOrder(right, false, true);
                }
                isMerged = rightDirection == requested;
            }
        }
        operation.setMerge(isMerged, rightSort);
    }

    /**
     * Records the head of the set operation under the column projections of the shared source's input, a set operation
     * serving no shared cursor itself, as the source of the leading branch of the decorrelation domain's input, see
     * {@link #leadingBranch}: when the head serves shared cursors and the domain's input is a set operation too, whose
     * leading branch lays out the head's column types. Returns whether it did.
     */
    private boolean planSharedHead(AggregatePlan aggregate, JoinInput source) {
        if (!(LogicalPlans.unwrapColumnProjections(source.getInput()) instanceof SetOperationPlan operation)
                || PhysicalProperties.supportsSharedCursors(leadingBranch(operation)) != PhysicalProperties.Capability.YES
                || !(LogicalPlans.unwrapColumnProjections(aggregate.getChild()) instanceof SetOperationPlan)) {
            return false;
        }
        final OutputSchema head = leadingBranch(operation).getOutput();
        final LogicalPlan branch = leadingBranch(aggregate.getChild());
        final OutputSchema output = branch.getOutput();
        if (output.getColumnCount() != head.getColumnCount()) {
            return false;
        }
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            if (output.getColumnType(i) != head.getColumnType(i)) {
                return false;
            }
        }
        aggregate.setSharedHead(operation, branch);
        return true;
    }

    /**
     * Decides how the generator reads the input of a decorrelation domain that re-reads its shared source: through
     * the factory the generator builds for the source when that factory serves shared cursors and every input column
     * maps onto a source column, see {@link #mapSharedColumns}; else through the head of the source's set operation,
     * see {@link #planSharedHead}. The domain generates its input when the generator builds the source after it or in
     * another frame, see {@link #isGenerated}, or when it builds no GROUP BY or count over its input, or steals its
     * input's filter.
     */
    private void planSharedRead(AggregatePlan aggregate) {
        final JoinInput source = aggregate.getSharedSource();
        if (source == null) {
            return;
        }
        final PlanToFactoryRules.AggregateFactory factory = PlanToFactoryRules.aggregateFactory(aggregate);
        if (factory != PlanToFactoryRules.AggregateFactory.GROUP_BY && factory != PlanToFactoryRules.AggregateFactory.COUNT
                || aggregate.getAlgorithm() == AggregatePlan.Algorithm.PARALLEL_STOLEN_FILTER || !isGenerated(source)) {
            aggregate.setSharedRead(AggregatePlan.SharedRead.NONE);
        } else if (PhysicalProperties.supportsSharedCursors(source.getInput()) == PhysicalProperties.Capability.YES
                && mapSharedColumns(aggregate, source)) {
            aggregate.setSharedRead(AggregatePlan.SharedRead.SOURCE);
        } else if (!planSharedHead(aggregate, source)) {
            aggregate.setSharedRead(AggregatePlan.SharedRead.NONE);
        }
    }

    /**
     * Decides how the generator implements a sort without a LIMIT over it; the LIMIT over a bounded sort decides again.
     */
    private void planSort(SortPlan sort, boolean isTimestampRequired) {
        final LogicalPlan input = sort.getChild();
        final int keyIndex = input.getOutput().getColumnIndexById(sort.getColumnIds().getQuick(0));
        final int timestampIndex = PhysicalProperties.timestampIndex(input);
        readOrder(input, true, keyIndex == timestampIndex);
        final boolean isAdviceFollowed = isAdviceFollowed(input);
        if (isAdviceFollowed && PlanToFactoryRules.hasAdvisedInput(input)) {
            sort.setAlgorithm(SortPlan.Algorithm.INPUT_ORDER);
        } else if (isAdviceFollowed && sort.isMarkoutHorizon()) {
            final int sortedIndex = sort.getOutput().getTimestampIndex();
            sort.setAlgorithm(sortedIndex < 0 || sortedIndex == timestampIndex || !isTimestampRequired
                    ? SortPlan.Algorithm.INPUT_ORDER : SortPlan.Algorithm.TIMESTAMP_DECLARATION);
        } else if (sort.getColumnIds().size() == 1 && isTimestampOrdered(sort)) {
            sort.setAlgorithm(SortPlan.Algorithm.INPUT_ORDER);
        } else {
            sort.setAlgorithm(sortAlgorithm(input));
        }
    }

    private void planWindow(WindowPlan window) {
        final LogicalPlan input = window.getChild();
        final SortKeys queryOrder = window.getQueryOrder();
        final boolean isAdviceFollowed = isAdviceFollowed(input) && PlanToFactoryRules.hasAdvisedInput(input);
        final int timestampIndex = PhysicalProperties.timestampIndex(input);
        final PhysicalProperties.ScanDirection direction = PhysicalProperties.scanDirection(input);
        boolean isDirectionRead = false;
        for (int i = 0, n = window.getSpecs().size(); i < n; i++) {
            final WindowSpec spec = window.getSpecs().getQuick(i);
            final IntList order = spec.getOrderByColumnIds();
            final boolean isTimestampKey = order.size() == 1
                    && (queryOrder.size() < 2 || (window.getFunctions().getQuick(i).getFunctionFlags() & BoundExpression.ORDERED_INPUT) != 0)
                    && input.getOutput().getColumnIndexById(order.getQuick(0)) == timestampIndex;
            final boolean isOrderDelivered = isAdviceFollowed && spec.isQueryOrderPrefix(queryOrder)
                    || isTimestampKey && direction == PhysicalProperties.ScanDirection.of(spec.getOrderByDirections().getQuick(0));
            spec.setOrderDelivered(isOrderDelivered);
            isDirectionRead |= isTimestampKey || isOrderDelivered;
        }
        readOrder(input, true, isDirectionRead);
        window.setAlgorithm(windowAlgorithm(window));
    }

    /**
     * A step joins in parallel when it reads the page frames of the master, or steals the master's parallel filter,
     * its aggregate functions support parallelism and its slave serves time frames. A step whose filter is constant
     * false null-extends the master instead, keeping its page frames for the next step; a step that joins leaves
     * neither page frames nor a filter to the next.
     */
    private void planWindowJoin(WindowJoinPlan windowJoin) throws SqlException {
        final LogicalPlan master = windowJoin.getMaster();
        validateConsumedTimestamp(master);
        for (int i = 0, n = windowJoin.getSteps().size(); i < n; i++) {
            validateConsumedTimestamp(windowJoin.getSteps().getQuick(i).getSlave());
        }
        final SqlExecutionContext executionContext = context.getExecutionContext();
        final int masterTimestampIndex = timestampIndex(master);
        boolean hasPageFrames = isPageFrameSource(master);
        boolean isFilterStolen = !hasPageFrames && isStolenFilter(master, FilterConsumer.WINDOW_JOIN);
        for (int i = 0, n = windowJoin.getSteps().size(); i < n; i++) {
            final WindowJoinStep step = windowJoin.getSteps().getQuick(i);
            validateTimeSeriesOrders(master, step.getSlave(), step.getPosition());
            validateTimestampAggregates(step.getAggregates(), step.getScope(), masterTimestampIndex, false);
            if (step.isNullExtending()) {
                isFilterStolen = false;
                continue;
            }
            final boolean isParallel = executionContext.isParallelWindowJoinEnabled() && (hasPageFrames || isFilterStolen)
                    && isParallelWindowJoinStep(step);
            step.setAlgorithm(!isParallel ? WindowJoinStep.Algorithm.SERIAL
                    : isFilterStolen ? WindowJoinStep.Algorithm.PARALLEL_STOLEN_FILTER : WindowJoinStep.Algorithm.PARALLEL);
            hasPageFrames = false;
            isFilterStolen = false;
        }
    }

    /**
     * Reads the order of the factory of the first {@code inputCount} ordered inputs of the join, see
     * {@link #readOrder}.
     */
    private void readJoinOrder(JoinPlan join, int inputCount, boolean isAdviceRead, boolean isDirectionRead) {
        if (!isAdviceRead && !isDirectionRead) {
            return;
        }
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        if (inputCount == 1) {
            readOrder(ordered.getQuick(0).getInput(), isAdviceRead, isDirectionRead);
            return;
        }
        final JoinInput step = ordered.getQuick(inputCount - 1);
        final JoinKind joinType = step.getJoinType();
        final boolean isMasterNullExtended = joinType == JoinKind.RIGHT_OUTER || joinType == JoinKind.FULL_OUTER;
        BoundExpression filter = step.getPostJoinFilter();
        boolean isConstantFolded = joinType == JoinKind.UNNEST || filter instanceof ConstantExpression constant && constant.isLiteral();
        if (!joinType.isTemporal() && joinType != JoinKind.UNNEST && !isMasterNullExtended && joinType != JoinKind.LEFT_OUTER
                && step.getOnResidual() != null) {
            if (filter == null) {
                filter = step.getOnResidual();
                isConstantFolded = filter instanceof ConstantExpression constant && constant.isLiteral();
            } else {
                isConstantFolded = false;
            }
        }
        if (filter != null) {
            if (isConstantFolded && LogicalPlans.isConstant(filter)) {
                if (LogicalPlans.constantTruth(filter) == 0) {
                    return;
                }
            } else {
                isAdviceRead = false;
            }
        }
        switch (step.getAlgorithm()) {
            case MARKOUT -> readJoinOrder(join, inputCount - 1, false, isDirectionRead);
            case NESTED_LOOP, MATERIALIZED_NESTED_LOOP -> {
                if (isMasterNullExtended) {
                    readOrder(step.getInput(), isAdviceRead, false);
                    readJoinOrder(join, inputCount - 1, false, isDirectionRead);
                } else {
                    readJoinOrder(join, inputCount - 1, isAdviceRead, isDirectionRead);
                }
            }
            case HASH, LIGHT_HASH -> {
                if (isMasterNullExtended) {
                    return;
                }
                if (step.getMasterSide() == null) {
                    if (isAdviceRead && PhysicalProperties.masterFollowsOrderAdvice(join, inputCount - 1) == PhysicalProperties.Capability.YES
                            || isDirectionRead && PhysicalProperties.masterScanDirection(join, inputCount - 1) != PhysicalProperties.ScanDirection.OTHER) {
                        step.setMasterSide(JoinInput.MasterSide.FIXED);
                    }
                }
                readJoinOrder(join, inputCount - 1, isAdviceRead, isDirectionRead);
            }
            case null -> {
            }
            default -> readJoinOrder(join, inputCount - 1, isAdviceRead, isDirectionRead);
        }
    }

    /**
     * A consumer reads, from the factory of the plan, whether it follows the order advice of its scans and the
     * direction it emits rows in. Fixes the master side of every light INNER hash join the factory hands the question
     * down to whose master's answer depends on the master's order.
     */
    private void readOrder(LogicalPlan plan, boolean isAdviceRead, boolean isDirectionRead) {
        if (!isAdviceRead && !isDirectionRead) {
            return;
        }
        switch (plan) {
            case FilterPlan filter -> {
                if (PlanToFactoryRules.isFusedFilter(filter)) {
                    return;
                }
                if (LogicalPlans.isConstant(filter.getPredicate())) {
                    if (LogicalPlans.constantTruth(filter.getPredicate()) != 0) {
                        readOrder(filter.getChild(), isAdviceRead, isDirectionRead);
                    }
                } else {
                    readOrder(filter.getChild(), false, isDirectionRead);
                }
            }
            case ProjectPlan project -> readOrder(project.getChild(), isAdviceRead, isDirectionRead);
            case SortPlan sort -> {
                if (sort.getAlgorithm() == SortPlan.Algorithm.INPUT_ORDER || sort.getAlgorithm() == SortPlan.Algorithm.TIMESTAMP_DECLARATION) {
                    readOrder(sort.getChild(), isAdviceRead, isDirectionRead);
                }
            }
            case LimitPlan limit -> {
                if (limit.getApplication() != LimitPlan.Application.SORT) {
                    readOrder(limit.getChild(), isAdviceRead && limit.getApplication() == LimitPlan.Application.INPUT, isDirectionRead);
                }
            }
            case DistinctPlan distinct -> {
                final LogicalPlan input = distinct.getChild();
                if (isRandomAccess(input) && PhysicalProperties.timestampIndex(input) >= 0) {
                    readOrder(input, false, isDirectionRead);
                }
            }
            case WindowPlan window -> readOrder(window.getChild(), isAdviceRead, isDirectionRead);
            case WindowJoinPlan windowJoin -> readOrder(windowJoin.getMaster(), isAdviceRead, isDirectionRead);
            case JoinPlan join -> readJoinOrder(join, join.getOrderedInputs().size(), isAdviceRead, isDirectionRead);
            case SetOperationPlan operation -> {
                if (!operation.getOperation().isUnion()) {
                    readOrder(operation.getLeft(), false, isDirectionRead);
                }
            }
            default -> {
            }
        }
    }

    /**
     * The SAMPLE BY factory: the interpolating one for FILL(LINEAR), the one that reads first and last values from
     * the index of its base, see {@link #isFirstLastIndexScan}, or the SAMPLE BY cursor, without a fill or filling
     * with constants.
     */
    private SampleByPlan.Algorithm sampleByAlgorithm(SampleByPlan sample, LogicalPlan base) {
        if (isLinearFill(sample)) {
            return SampleByPlan.Algorithm.INTERPOLATE;
        }
        if (isFirstLastIndexScan(sample, base)) {
            return SampleByPlan.Algorithm.FIRST_LAST_INDEX;
        }
        return isFillNone(sample) ? SampleByPlan.Algorithm.FILL_NONE : SampleByPlan.Algorithm.FILL_VALUE;
    }

    private PhysicalProperties.ScanDirection scanDirection(LogicalPlan plan) {
        return validatedDirection(PhysicalProperties.scanDirection(plan));
    }

    /**
     * The slave column of the single symbol key of the ASOF join step at {@code inputIndex}, see {@link #symbolKey},
     * when the generator maps the master's key onto the slave's symbol table: the master column is a SYMBOL, STRING or
     * VARCHAR, but not the SYMBOL column at the slave column's position in a self-join, see {@link #isSelfJoin},
     * which already reads the slave's symbol table. {@link SymbolKey#NONE} otherwise.
     */
    private SymbolKey singleSymbolKey(JoinPlan join, int inputIndex, JoinInput step) {
        if (step.getKeys().size() != 1) {
            return SymbolKey.NONE;
        }
        final OutputSchema master = join.getOrderedInputs().getQuick(inputIndex - 1).getOutput();
        final OutputSchema slave = step.getInput().getOutput();
        final int slaveId = step.getKeys().getQuick(0).getSlaveColumnId();
        final int masterIndex = master.getColumnIndexById(step.getKeys().getQuick(0).getMasterColumnId());
        final int slaveIndex = slave.getColumnIndexById(slaveId);
        if (slave.getColumnType(slaveIndex) != ColumnType.SYMBOL) {
            return SymbolKey.NONE;
        }
        return switch (master.getColumnType(masterIndex)) {
            case ColumnType.SYMBOL -> masterIndex == slaveIndex && isSelfJoin(join, inputIndex)
                    ? SymbolKey.NONE : symbolKey(step.getInput(), slaveId);
            case ColumnType.STRING, ColumnType.VARCHAR, ColumnType.VARCHAR_SLICE -> symbolKey(step.getInput(), slaveId);
            default -> SymbolKey.NONE;
        };
    }

    private SortPlan.Algorithm sortAlgorithm(LogicalPlan input) {
        return isRandomAccess(input) ? SortPlan.Algorithm.LIGHT : SortPlan.Algorithm.MATERIALIZED;
    }

    /**
     * What the factory of the time-frame slave exposes of the symbol table and the index of its SYMBOL column
     * {@code columnId}: those of the table column of the scan the factories under it pass the column on from, through
     * filters, selections, sorts, LIMITs, single-input joins and the master columns of window joins; a column a factory
     * computes has the symbol table the plan records for it and no index.
     */
    private SymbolKey symbolKey(LogicalPlan plan, int columnId) {
        while (true) {
            switch (plan) {
                case ScanPlan scan -> {
                    final TableAccessInfo table = context.getTableAccessInfo(scan);
                    final OutputSchema output = scan.getOutput();
                    final int columnIndex = table.getColumnIndex(output.getColumnName(output.getColumnIndexById(columnId)));
                    return !table.isSymbolTableStatic(columnIndex) ? SymbolKey.NONE
                            : table.isIndexed(columnIndex) ? SymbolKey.INDEXED_STATIC : SymbolKey.STATIC;
                }
                case FilterPlan filter -> plan = filter.getChild();
                case SortPlan sort -> plan = sort.getChild();
                case LimitPlan limit -> plan = limit.getChild();
                case JoinPlan join when join.getOrderedInputs().size() == 1 ->
                        plan = join.getOrderedInputs().getQuick(0).getInput();
                case ProjectPlan project -> {
                    switch (PlanToFactoryRules.projectionFactory(project)) {
                        case SELECTION, WINDOW_JOIN -> {
                            columnId = ((ColumnExpression) project.getExpressions().getQuick(project.getOutput().getColumnIndexById(columnId))).getColumnId();
                            plan = project.getChild();
                        }
                        default -> {
                            return computedSymbolKey(project, columnId);
                        }
                    }
                }
                case WindowJoinPlan windowJoin -> {
                    if (windowJoin.getMaster().getOutput().getColumnIndexById(columnId) < 0) {
                        return computedSymbolKey(windowJoin, columnId);
                    }
                    plan = windowJoin.getMaster();
                }
                default -> {
                    return computedSymbolKey(plan, columnId);
                }
            }
        }
    }

    /**
     * The ASOF or LT join the generator builds: full-fat over a slave without random access, one that steals the
     * filter of its slave, one that reads the time frames of its slave unless the {@code asof_linear} hint asks for
     * a linear scan or an LT join is keyed, and a linear scan of the slave otherwise.
     */
    private JoinInput.Algorithm temporalAlgorithm(JoinPlan join, int inputIndex, JoinInput step) {
        final LogicalPlan slave = step.getInput();
        if (isFullFatJoins || !isRandomAccess(slave)) {
            return JoinInput.Algorithm.FULL_FAT_TEMPORAL;
        }
        if ((step.getHints() & JoinInput.HINT_ASOF_LINEAR) != 0) {
            return JoinInput.Algorithm.TEMPORAL;
        }
        if (step.getJoinType() == JoinKind.ASOF) {
            if (isStolenTemporalFilter(slave)) {
                return JoinInput.Algorithm.TEMPORAL_STOLEN_FILTER;
            }
        } else if (isKeyedTemporalStep(join, inputIndex, step)) {
            return JoinInput.Algorithm.TEMPORAL;
        }
        return isKnownYes(PhysicalProperties.supportsTimeFrameCursor(slave)) ? JoinInput.Algorithm.TEMPORAL_TIME_FRAME : JoinInput.Algorithm.TEMPORAL;
    }

    /**
     * How the keyed ASOF join step at {@code inputIndex}, which navigates the time frames of its slave, finds the slave
     * row of each master row: the dense scan the {@code asof_dense} hint asks for, of the single symbol key when the
     * step joins on one, see {@link #singleSymbolKey}; over a single symbol key, the index scan the {@code asof_index}
     * hint asks for when the slave's key column has an index, else the memoized scan the {@code asof_memoized} hints
     * ask for; the fast scan otherwise.
     */
    private JoinInput.TimeFrameScan timeFrameScan(JoinPlan join, int inputIndex, JoinInput step) {
        final int hints = step.getHints();
        final SymbolKey key = singleSymbolKey(join, inputIndex, step);
        if ((hints & JoinInput.HINT_ASOF_DENSE) != 0) {
            return key == SymbolKey.NONE ? JoinInput.TimeFrameScan.DENSE : JoinInput.TimeFrameScan.DENSE_SINGLE_SYMBOL;
        }
        if (key == SymbolKey.NONE) {
            return JoinInput.TimeFrameScan.FAST;
        }
        if ((hints & JoinInput.HINT_ASOF_INDEX) != 0 && key == SymbolKey.INDEXED_STATIC) {
            return JoinInput.TimeFrameScan.INDEXED;
        }
        if ((hints & JoinInput.HINT_ASOF_MEMOIZED_DRIVEBY) != 0) {
            return JoinInput.TimeFrameScan.MEMOIZED_DRIVEBY;
        }
        return (hints & JoinInput.HINT_ASOF_MEMOIZED) != 0 ? JoinInput.TimeFrameScan.MEMOIZED : JoinInput.TimeFrameScan.FAST;
    }

    private int timestampIndex(LogicalPlan plan) {
        final int timestampIndex = PhysicalProperties.timestampIndex(plan);
        if (timestampIndex == PhysicalProperties.UNKNOWN_TIMESTAMP) {
            throw new IllegalStateException("designated timestamp is unknown at planning");
        }
        return timestampIndex;
    }

    private void validateConsumedTimestamp(LogicalPlan input) throws SqlException {
        if (!(input instanceof ProjectPlan) || PhysicalProperties.timestampIndex(input) != -1) {
            return;
        }
        LogicalPlan source = input;
        do {
            source = source.inputAt(0);
        } while (source instanceof ProjectPlan);
        if (source instanceof LatestByPlan latest && latest.isTimestampOrderInherited()) {
            throw SqlException.$(latest.getPosition(), "TIMESTAMP column is required but not provided");
        }
    }

    private void validateSlaveOrder(LogicalPlan slave, int position) throws SqlException {
        readOrder(slave, false, true);
        if (scanDirection(slave) != PhysicalProperties.ScanDirection.FORWARD) {
            throw SqlException.$(position, "right side of time series join doesn't have ASC timestamp order");
        }
    }

    /**
     * Rejects a time series join step whose master, the first {@code inputCount} ordered inputs of the join,
     * designates no timestamp, which binding defers for a master ordered by a key that is not a timestamp, or whose
     * master or slave does not emit its rows in ascending designated timestamp order.
     */
    private void validateTimeSeriesOrders(JoinPlan join, int inputCount, LogicalPlan slave, int position) throws SqlException {
        if (LogicalPlans.joinTimestampColumnId(join, inputCount) < 0) {
            throw SqlException.$(position, "left side of time series join has no timestamp");
        }
        readJoinOrder(join, inputCount, false, true);
        if (validatedDirection(PhysicalProperties.masterScanDirection(join, inputCount)) != PhysicalProperties.ScanDirection.FORWARD) {
            throw SqlException.$(position, "left side of time series join doesn't have ASC timestamp order");
        }
        validateSlaveOrder(slave, position);
    }

    /**
     * Rejects a time series join step whose master designates no timestamp, as for the join above, or whose master or
     * slave does not emit its rows in ascending designated timestamp order.
     */
    private void validateTimeSeriesOrders(LogicalPlan master, LogicalPlan slave, int position) throws SqlException {
        if (timestampIndex(master) < 0) {
            throw SqlException.$(position, "left side of time series join has no timestamp");
        }
        readOrder(master, false, true);
        if (scanDirection(master) != PhysicalProperties.ScanDirection.FORWARD) {
            throw SqlException.$(position, "left side of time series join doesn't have ASC timestamp order");
        }
        validateSlaveOrder(slave, position);
    }

    /**
     * The window factory: the streaming one when every window function evaluates in one pass over rows its input
     * delivers in the window's order; else the light cached one when the configuration enables it and encoded sorts,
     * the input supports random access, every window that orders its own rows sorts by keys that encode and every
     * window result is fixed-width; else the cached one.
     */
    private WindowPlan.Algorithm windowAlgorithm(WindowPlan window) {
        for (int i = 0, n = window.getSpecs().size(); i < n; i++) {
            if (!isStreamedWindow(window.getSpecs().getQuick(i), window.getFunctions().getQuick(i))) {
                return configuration.isSqlWindowCachedLightEnabled() && configuration.isSqlOrderBySortEnabled()
                        && isKnownYes(PhysicalProperties.supportsRandomAccess(window.getChild())) && isCachedLightShape(window)
                        ? WindowPlan.Algorithm.CACHED_LIGHT : WindowPlan.Algorithm.CACHED;
            }
        }
        return WindowPlan.Algorithm.STREAMING;
    }

    /**
     * The table scan the SAMPLE BY, which nothing fills, reads the first and last values of its buckets from the index
     * of: under a fused filter or identity projections, the scan of the index of a single SYMBOL key without a
     * residual, which the SAMPLE BY keys by that symbol or by none; null when the SAMPLE BY reads no such scan.
     */
    static ScanPlan firstLastIndexScan(SampleByPlan sample, LogicalPlan base) {
        while (base instanceof ProjectPlan project && PlanToFactoryRules.isIdentityProjection(project)) {
            base = project.getChild();
        }
        if (base instanceof FilterPlan filter && PlanToFactoryRules.isFusedFilter(filter)) {
            base = filter.getChild();
        }
        if (sample.getFillModes().size() > 0 || sample.isFillInput() || !(base instanceof ScanPlan scan)
                || scan.getAccessPath() != ScanPlan.AccessPath.SYMBOL_INDEX || scan.getIndexKeys().size() != 1
                || scan.getResidual() != null || scan.getIndexRead() == ScanPlan.IndexRead.COVERING
                || scan.getTableToken().isLiveView() && !scan.isUpdate()) {
            return null;
        }
        final int baseTimestampIndex = PhysicalProperties.timestampIndex(scan);
        final int timestampIndex = sample.isTimestampRequired() ? baseTimestampIndex
                : sample.getChild().getOutput().getColumnIndexById(sample.getTimestampColumnId());
        return isFirstLastShape(sample, timestampIndex, baseTimestampIndex, scan.getOutput().getColumnIndexById(scan.getIndexColumnId()))
                ? scan : null;
    }

    /**
     * True when every key and aggregate function of the aggregate supports parallelism.
     */
    static boolean hasParallelFunctions(AggregatePlan aggregate) {
        return !hasNoParallelism(aggregate.getGroupingExpressions()) && !hasNoParallelism(aggregate.getAggregates());
    }

    /**
     * True when the generator builds a filter over the residual of the scan whose execution it chooses: a filter over
     * page frames, a fused live view's filter over its scan, the filter over a covering index and the pattern scan's
     * filter; an index scan applies its residual itself.
     */
    static boolean hasResidualFilterChoice(ScanPlan scan) {
        final BoundExpression residual = scan.getResidual();
        if (residual == null) {
            return false;
        }
        return switch (scan.getAccessPath()) {
            case PAGE_FRAMES, SORTED_SYMBOL_INDEX -> isFiltering(residual);
            case SYMBOL_INDEX -> scan.getIndexRead() == ScanPlan.IndexRead.COVERING;
            case SYMBOL_PATTERN -> true;
            case null, default -> false;
        };
    }

    /**
     * True when the light cached window reads its input: every window result is fixed-width and every window that
     * orders its own rows sorts by keys that encode.
     */
    static boolean isCachedLightShape(WindowPlan window) {
        final OutputSchema input = window.getChild().getOutput();
        for (int i = 0, n = window.getSpecs().size(); i < n; i++) {
            final WindowSpec spec = window.getSpecs().getQuick(i);
            if (ColumnType.isVarSize(window.getFunctions().getQuick(i).getDataType())
                    || spec.getOrderByColumnIds().size() > 0 && !spec.isOrderDelivered() && !isEncodedKey(spec, input)) {
                return false;
            }
        }
        return true;
    }

    /**
     * True when the SAMPLE BY fills no bucket: it names no FILL, or FILL(NONE).
     */
    static boolean isFillNone(SampleByPlan sample) {
        final ObjList<FillMode> fill = sample.getFillModes();
        return fill.size() == 0 || fill.size() == 1 && fill.getQuick(0) == FillMode.NONE;
    }

    /**
     * True when the generator builds a filter for the predicate, which it neither folds nor gates once.
     */
    static boolean isFiltering(BoundExpression predicate) {
        return !LogicalPlans.isConstant(predicate)
                && (predicate instanceof ColumnExpression || (predicate.getFunctionFlags() & BoundExpression.RUNTIME_CONSTANT) == 0);
    }

    /**
     * True when the factory of the plan applies the LIMIT over it: the access path of the table scan the plan
     * filters, under any projections, reads only the rows the LIMIT keeps.
     */
    static boolean isInputLimit(LogicalPlan plan) {
        return PlanToFactoryRules.hasNativeFilterInput(plan) && PhysicalProperties.implementsLimit(plan) == PhysicalProperties.Capability.YES;
    }

    /**
     * True when the SAMPLE BY interpolates its empty buckets: FILL(LINEAR).
     */
    static boolean isLinearFill(SampleByPlan sample) {
        final ObjList<FillMode> fill = sample.getFillModes();
        return fill.size() == 1 && fill.getQuick(0) == FillMode.LINEAR;
    }

    /**
     * True when the GROUP BY over the base runs in parallel once it reads page frames: every key and aggregate
     * function supports parallelism, and a keyless aggregate that stops early reads a filtered base.
     */
    static boolean isParallelGroupBy(AggregatePlan aggregate, LogicalPlan base) {
        return hasParallelFunctions(aggregate)
                && !(aggregate.getGroupingExpressions().size() == 0 && hasEarlyExit(aggregate.getAggregates()) && !hasFilter(base));
    }

    /**
     * True when the window join step runs in parallel once it reads the master's page frames: its aggregate functions
     * support parallelism and its slave serves time frames.
     */
    static boolean isParallelWindowJoinStep(WindowJoinStep step) {
        return !hasNoParallelism(step.getAggregates()) && isKnownYes(PhysicalProperties.supportsTimeFrameCursor(step.getSlave()));
    }

    /**
     * True when the fill reads the rows of a SAMPLE BY cursor that fills no bucket itself.
     */
    static boolean isSampleByRowsFill(FillPlan fill) {
        return fill.getChild() instanceof SampleByPlan sample && sample.getAlgorithm() == SampleByPlan.Algorithm.FILL_NONE;
    }

    /**
     * True when the window function evaluates in one pass over rows its input delivers in the window's order.
     */
    static boolean isStreamedWindow(WindowSpec spec, FunctionExpression function) {
        return (spec.getOrderByColumnIds().size() == 0 || spec.isOrderDelivered()) && (function.getFunctionFlags() & BoundExpression.MULTI_PASS) == 0;
    }

    /**
     * True when the input of the sort emits its rows in the order of the sort's first key: that key is the input's
     * designated timestamp, in the input's scan direction.
     */
    static boolean isTimestampOrdered(SortPlan sort) {
        final LogicalPlan input = sort.getChild();
        return input.getOutput().getColumnIndexById(sort.getColumnIds().getQuick(0)) == PhysicalProperties.timestampIndex(input)
                && PhysicalProperties.scanDirection(input) == PhysicalProperties.ScanDirection.of(sort.getDirections().getQuick(0));
    }

    /**
     * True when a LIMIT of {@code lo} and {@code hi} is a count a top-K keeps: a constant
     * {@code 0 < lo <= Integer.MAX_VALUE} without a hi.
     */
    static boolean isTopKCount(BoundExpression lo, BoundExpression hi) {
        if (hi != null || !(lo instanceof ConstantExpression count)) {
            return false;
        }
        final long value = LogicalPlans.limitValue(count);
        return value > 0 && value <= Integer.MAX_VALUE;
    }

    /**
     * True when a bounded sort under the LIMIT keeps only its first rows by a top-K: the LIMIT is a top-K count, see
     * {@link #isTopKCount}, over an input that does not deliver the sort's order.
     */
    static boolean isTopKLimit(LimitPlan limit, boolean isTimestampOrdered) {
        return !isTimestampOrdered && isTopKCount(limit.getLo(), limit.getHi());
    }

    /**
     * The leading branch of the set operations under the column projections of the plan, or the plan itself; the
     * generator builds the head of a set operation from it.
     */
    static LogicalPlan leadingBranch(LogicalPlan plan) {
        while (LogicalPlans.unwrapColumnProjections(plan) instanceof SetOperationPlan operation) {
            plan = operation.getLeft();
        }
        return plan;
    }

    /**
     * Records that the generator has built the factory of the join input once access path planning planned the input,
     * which a decorrelation domain the generator builds later in the same frame may then share, see
     * {@link #planSharedRead}.
     */
    void addGeneratedInput(JoinInput input) {
        generatedInputs.add(input);
    }

    /**
     * Records on every aggregate of the query level how many decorrelation domains of the level re-read it through
     * their shared source; the generator builds the consumers of each one and the aggregate shares its rows with them.
     * A statement without such a domain leaves every count at zero.
     */
    void countSharedConsumers(LogicalPlan root) {
        if (context.hasSharedSources()) {
            root.walkTopDown(SHARED_CONSUMER_COUNTS);
            root.walkTopDown(SHARED_CONSUMERS);
        }
    }

    /**
     * Starts the generation frame of a sub-query, which shares only the join inputs the generator builds in it; returns
     * the start of the enclosing frame, for {@link #exitFrame}.
     */
    int enterFrame() {
        final int enclosingLo = frameInputLo;
        frameInputLo = generatedInputs.size();
        return enclosingLo;
    }

    /**
     * Ends the generation frame {@link #enterFrame} started, returning to the enclosing frame starting at
     * {@code enclosingLo}.
     */
    void exitFrame(int enclosingLo) {
        generatedInputs.setPos(frameInputLo);
        frameInputLo = enclosingLo;
    }

    /**
     * Decides, once access path planning has planned every input of {@code plan}, how the generator implements the
     * order-sensitive operators of the node: whether a sort runs and how, which operator applies a LIMIT, whether a
     * window orders its rows, whether a UNION ALL merges its branches, how each join step joins and whether a filter
     * runs in parallel. Fixes the master side of every light INNER hash join whose order a decision reads, or whose
     * designated timestamp the consumer of the node requires: {@code isTimestampRequired}. Rejects an input the node consumes the designated timestamp of
     * when the input's factory designates none, see {@link #validateConsumedTimestamp}, and an input that does not
     * deliver the ascending designated timestamp order the node requires.
     */
    void planOperators(LogicalPlan plan, boolean isTimestampRequired) throws SqlException {
        switch (plan) {
            case FilterPlan filter -> planFilter(filter);
            case SortPlan sort -> planSort(sort, isTimestampRequired);
            case LimitPlan limit -> planLimit(limit);
            case WindowPlan window -> planWindow(window);
            case SetOperationPlan operation -> planSetOperation(operation);
            case JoinPlan join -> {
                planJoin(join);
                if (isTimestampRequired || join.hasExplicitTimestamp()) {
                    readJoinOrder(join, join.getOrderedInputs().size(), false, true);
                }
            }
            case WindowJoinPlan windowJoin -> planWindowJoin(windowJoin);
            case HorizonJoinPlan horizon -> planHorizon(horizon);
            case AggregatePlan aggregate -> {
                planAggregate(aggregate);
                planSharedRead(aggregate);
            }
            case SampleByPlan sample -> planSampleBy(sample);
            case FillPlan fill -> planFill(fill);
            case LatestByPlan latest -> planLatestBy(latest);
            case ProjectPlan project when PlanToFactoryRules.isWindowJoinTimestampDroppable(project)
                    && !PlanToFactoryRules.isWindowJoinTimestampKept(project, (WindowJoinPlan) project.getChild(), readOrder, readIds) ->
                    project.markTimestampDropped();
            default -> {
            }
        }
    }

    /**
     * True when the node requires the designated timestamp of its input at {@code inputIndex}, the ordered input of
     * a join: it consumes it, see {@link LogicalPlans#consumesInputTimestamp}, or its own consumer requires its timestamp,
     * {@code isTimestampRequired}, and its factory designates that input's, see
     * {@link PhysicalProperties#timestampSource}. A sort designates its own key, so it requires nothing of its input.
     */
    boolean requiresInputTimestamp(LogicalPlan plan, int inputIndex, boolean isTimestampRequired) {
        return LogicalPlans.consumesInputTimestamp(plan, inputIndex)
                || isTimestampRequired && PhysicalProperties.timestampSource(plan) == inputIndex;
    }

    void setFullFatJoins(boolean isFullFatJoins) {
        this.isFullFatJoins = isFullFatJoins;
    }

    /**
     * Lets the smaller input drive every light INNER hash join whose order no decision read since
     * {@link #planOperators} planned it.
     */
    void settleMasterSides() {
        for (int i = 0, n = undecidedSteps.size(); i < n; i++) {
            final JoinInput step = undecidedSteps.getQuick(i);
            if (step.getMasterSide() == null) {
                step.setMasterSide(JoinInput.MasterSide.SMALLER);
            }
        }
        undecidedSteps.clear();
    }

    /**
     * The parallel operator that steals the filter of its input. A {@link #FRAME_REDUCER} is a GROUP BY or a HORIZON
     * JOIN, aggregating or not, which reduce the page frames under the filter alike.
     */
    private enum FilterConsumer {
        FRAME_REDUCER, TEMPORAL_JOIN, TOP_K, WINDOW_JOIN
    }

    /**
     * What the factory of an ASOF join's slave exposes of the symbol table of its key column: none the generator maps
     * master symbol keys onto, a static symbol table, or a static symbol table and an index.
     */
    private enum SymbolKey {
        NONE, STATIC, INDEXED_STATIC
    }
}
