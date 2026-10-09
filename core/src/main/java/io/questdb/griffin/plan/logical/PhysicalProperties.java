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

package io.questdb.griffin.plan.logical;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.SetOperationCasts;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * Derives, on demand and from the plan alone, the physical properties of the factory codegen builds for a plan node:
 * whether its cursor supports random access, page frames, time frames, shared cursors and long top-K, whether it
 * applies a LIMIT itself, follows the order advice of its scans or reads long_sequence(), and the order it emits its
 * rows in. It reads the access paths and the operator decisions order planning records on the plan. A property is
 * {@link Capability#UNKNOWN} where codegen decides it from facts the plan does not carry.
 */
public final class PhysicalProperties {
    public static final int UNKNOWN_TIMESTAMP = -2;
    private static final int DIRECTION = 8;
    private static final int EMPTY = properties(Capability.YES, Capability.NO, Capability.NO, Capability.NO, ScanDirection.FORWARD, Capability.NO);
    private static final int LIMIT = 4;
    private static final int LONG_SEQUENCE = 10;
    private static final int ORDER_ADVICE = 6;
    private static final int PAGE_FRAMES = 2;
    private static final int RANDOM_ACCESS = 0;

    private PhysicalProperties() {
    }

    /**
     * Whether the factory follows the order advice of the scans under it, so it delivers the order they were asked for.
     */
    public static Capability followsOrderAdvice(LogicalPlan plan) {
        return capability(derive(plan, null), ORDER_ADVICE);
    }

    /**
     * Whether the factory applies the LIMIT of its consumer itself.
     */
    public static Capability implementsLimit(LogicalPlan plan) {
        return capability(derive(plan, null), LIMIT);
    }

    /**
     * Whether the factory, or a factory along its base chain, reads long_sequence().
     */
    public static Capability isLongSequence(LogicalPlan plan) {
        return capability(derive(plan, null), LONG_SEQUENCE);
    }

    /**
     * Whether the factory of the first {@code inputCount} ordered inputs of the join, the master of its next step,
     * follows the order advice of the scans under it.
     */
    public static Capability masterFollowsOrderAdvice(JoinPlan join, int inputCount) {
        return capability(join(join, inputCount, false), ORDER_ADVICE);
    }

    /**
     * The order the factory of the first {@code inputCount} ordered inputs of the join emits its rows in.
     */
    public static ScanDirection masterScanDirection(JoinPlan join, int inputCount) {
        return direction(join(join, inputCount, false));
    }

    /**
     * Whether the cursor of the factory of the first {@code inputCount} ordered inputs of the join supports random
     * access.
     */
    public static Capability masterSupportsRandomAccess(JoinPlan join, int inputCount) {
        return capability(join(join, inputCount, false), RANDOM_ACCESS);
    }

    /**
     * The designated timestamp of the factory of the first {@code inputCount} ordered inputs of the join.
     */
    public static int masterTimestampIndex(JoinPlan join, int inputCount) {
        return join(join, inputCount, true);
    }

    /**
     * The order the factory emits its rows in, by designated timestamp.
     */
    public static ScanDirection scanDirection(LogicalPlan plan) {
        return direction(derive(plan, null));
    }

    /**
     * Whether the factory under the filter the generator builds for the node serves time-frame cursors: the factory
     * of a non-fused filter's input, the page-frame scan of a fused one without its residual, or the page-frame scan
     * of a pattern scan that filters in parallel.
     */
    public static Capability supportsLeafTimeFrameCursor(FilterPlan filter) {
        if (!(filter.getInput() instanceof ScanPlan scan) || scan.isWalClientUpdate()) {
            return supportsTimeFrameCursor(filter.getInput(), null);
        }
        return switch (scan.getAccessPath()) {
            case PAGE_FRAMES -> pageFrameTimeFrame(scan, scan.isRandomAccess());
            case SYMBOL_PATTERN ->
                    scan.getIndexRead() == ScanPlan.IndexRead.COVERING ? Capability.NO : pageFrameTimeFrame(scan, true);
            case null -> Capability.UNKNOWN;
            default -> Capability.NO;
        };
    }

    /**
     * Whether the cursor of the factory serves a long top-K over the output column.
     */
    public static Capability supportsLongTopK(LogicalPlan plan, int columnIndex) {
        return supportsLongTopK(plan, null, columnIndex);
    }

    /**
     * Whether the factory supports page-frame cursors.
     */
    public static Capability supportsPageFrameCursor(LogicalPlan plan) {
        return capability(derive(plan, null), PAGE_FRAMES);
    }

    /**
     * Whether the cursor of the factory supports random access.
     */
    public static Capability supportsRandomAccess(LogicalPlan plan) {
        return capability(derive(plan, null), RANDOM_ACCESS);
    }

    /**
     * Whether the factory serves shared cursors, so several consumers read the rows it computes once.
     */
    public static Capability supportsSharedCursors(LogicalPlan plan) {
        return supportsSharedCursors(plan, null);
    }

    /**
     * Whether the factory serves time-frame cursors: a forward table scan over page frames with random access, a
     * designated timestamp and no filter, or a factory that passes such a scan through.
     */
    public static Capability supportsTimeFrameCursor(LogicalPlan plan) {
        return supportsTimeFrameCursor(plan, null);
    }

    /**
     * The index of the designated timestamp in the metadata of the factory, -1 for none, or {@link #UNKNOWN_TIMESTAMP}.
     */
    public static int timestampIndex(LogicalPlan plan) {
        return timestamp(plan, null, false);
    }

    /**
     * The input whose designated timestamp the factory designates as its own, when it designates one: input 0 of
     * an operator that keeps its input's, the master of a join or a window join, the left branch of a set operation;
     * -1 when it designates none, or a column of its own, like a sort key, a grouping key or a sampled timestamp.
     * Answers for the input the timestamp would come from while {@link #timestampIndex} is still unknown.
     */
    public static int timestampSource(LogicalPlan plan) {
        return timestamp(plan, null, true);
    }

    private static int aggregate(AggregatePlan aggregate) {
        final Capability isKeyed = Capability.of(aggregate.getGroupingExpressions().size() > 0);
        if (aggregate.getInput() instanceof HorizonJoinPlan horizon) {
            return computed(isKeyed, ScanDirection.FORWARD, capability(derive(horizon.getMaster(), null), LONG_SEQUENCE));
        }
        if (isPostingIndexDistinct(aggregate)) {
            return computed(Capability.NO, ScanDirection.FORWARD, Capability.NO);
        }
        final int input = derive(aggregate.getInput(), null);
        // A shared source the aggregate re-reads hides the base chain of the source.
        final Capability isLongSequence = aggregate.getSharedSource() == null ? capability(input, LONG_SEQUENCE)
                : and(Capability.UNKNOWN, capability(input, LONG_SEQUENCE));
        if (LogicalPlans.isCount(aggregate)) {
            return computed(Capability.NO, ScanDirection.FORWARD, isLongSequence);
        }
        final ScanDirection direction = switch (aggregate.getAlgorithm()) {
            case PARALLEL -> direction(input);
            case PARALLEL_STOLEN_FILTER -> stolenFilterDirection(aggregate.getInput());
            case VECTORISED, SERIAL -> ScanDirection.FORWARD;
            case null -> ScanDirection.UNKNOWN;
        };
        return computed(isKeyed, direction, isLongSequence);
    }

    /**
     * Whether the GROUP BY serves shared cursors: the vectorised and the keyed parallel one always do; the serial and
     * the keyless parallel one only when order planning counts a consumer that re-reads the aggregate.
     */
    private static Capability aggregateSharedCursors(AggregatePlan aggregate) {
        if (aggregate.getInput() instanceof HorizonJoinPlan || LogicalPlans.isCount(aggregate) || isPostingIndexDistinct(aggregate)) {
            return Capability.NO;
        }
        return switch (aggregate.getAlgorithm()) {
            case VECTORISED -> Capability.YES;
            case PARALLEL, PARALLEL_STOLEN_FILTER ->
                    Capability.of(aggregate.getGroupingExpressions().size() > 0 || aggregate.getSharedConsumerCount() > 0);
            case SERIAL -> Capability.of(aggregate.getSharedConsumerCount() > 0);
            case null -> Capability.UNKNOWN;
        };
    }

    /**
     * Count and posting-index factories designate no timestamp, nor does the vectorised GROUP BY; before order
     * planning records the algorithm, a single key over page frames may still pick the vectorised one.
     */
    private static int aggregateTimestampIndex(AggregatePlan aggregate) {
        final int timestampIndex = aggregate.getOutput().getTimestampIndex();
        if (aggregate.getInput() instanceof HorizonJoinPlan || timestampIndex < 0) {
            return timestampIndex;
        }
        if (LogicalPlans.isCount(aggregate) || isPostingIndexDistinct(aggregate)) {
            return -1;
        }
        if (aggregate.getAlgorithm() == null && aggregate.getGroupingExpressions().size() == 1
                && capability(derive(aggregate.getInput(), null), PAGE_FRAMES) != Capability.NO) {
            return UNKNOWN_TIMESTAMP;
        }
        return aggregate.getAlgorithm() == AggregatePlan.Algorithm.VECTORISED ? -1 : timestampIndex;
    }

    private static Capability and(Capability left, Capability right) {
        if (left == Capability.NO || right == Capability.NO) {
            return Capability.NO;
        }
        return left == Capability.YES && right == Capability.YES ? Capability.YES : Capability.UNKNOWN;
    }

    private static Capability capability(int properties, int shift) {
        return switch (properties >>> shift & 3) {
            case 0 -> Capability.NO;
            case 1 -> Capability.YES;
            default -> Capability.UNKNOWN;
        };
    }

    private static int choose(Capability condition, int whenYes, int whenNo) {
        return switch (condition) {
            case YES -> whenYes;
            case NO -> whenNo;
            case UNKNOWN -> merge(whenYes, whenNo);
        };
    }

    private static Capability choose(Capability condition, Capability whenYes, Capability whenNo) {
        return switch (condition) {
            case YES -> whenYes;
            case NO -> whenNo;
            case UNKNOWN -> whenYes == whenNo ? whenYes : Capability.UNKNOWN;
        };
    }

    private static ScanDirection choose(Capability condition, ScanDirection whenYes, ScanDirection whenNo) {
        return switch (condition) {
            case YES -> whenYes;
            case NO -> whenNo;
            case UNKNOWN -> whenYes == whenNo ? whenYes : ScanDirection.UNKNOWN;
        };
    }

    private static int chooseTimestamp(Capability condition, int whenYes, int whenNo) {
        return switch (condition) {
            case YES -> whenYes;
            case NO -> whenNo;
            case UNKNOWN -> whenYes == whenNo ? whenYes : UNKNOWN_TIMESTAMP;
        };
    }

    private static int computed(Capability isRandomAccess, ScanDirection direction, Capability isLongSequence) {
        return properties(isRandomAccess, Capability.NO, Capability.NO, Capability.NO, direction, isLongSequence);
    }

    /**
     * A factory that computes its rows, the way aggregates and LATEST BY do: no page frames, LIMIT or order advice.
     */
    /**
     * A covering index scan, whose backup index scan, when a key may be NULL, emits rows in {@code backupDirection}.
     */
    private static int covering(ScanPlan scan, ScanDirection backupDirection) {
        final boolean hasBackup = scan.hasCoveringBackup();
        final ScanDirection direction = hasBackup && backupDirection != ScanDirection.FORWARD ? ScanDirection.OTHER : ScanDirection.FORWARD;
        final BoundExpression residual = scan.getResidual();
        if (residual == null) {
            return properties(Capability.NO, Capability.of(!hasBackup), Capability.NO, Capability.NO, direction, Capability.NO);
        }
        final Capability isParallel = residualParallel(scan);
        return properties(isParallel, Capability.NO, and(isParallel, Capability.of(scan.getCoveredFilterLimit() != null)),
                Capability.NO, direction, Capability.NO);
    }

    private static int derive(LogicalPlan plan, @Nullable LimitPlan sortedLimit) {
        return switch (plan) {
            case ScanPlan scan -> scan(scan);
            case FunctionSourcePlan source -> source.getRecordName() != null
                    ? computed(Capability.of(source.isRandomAccess()), ScanDirection.FORWARD, Capability.NO)
                    : properties(Capability.of(source.isRandomAccess()), Capability.of(source.isPageFrameSupported()), Capability.NO,
                    Capability.NO, source.getScanDirection(), Capability.of(source.isLongSequence()));
            case FilterPlan filter -> filter(filter);
            case ProjectPlan project -> project(project, sortedLimit);
            case SortPlan sort -> sortedLimit == null ? sort(sort) : sortedLimit(sort, sortedLimit);
            case LimitPlan limit -> LogicalPlans.hasSortUnderStableProjects(limit.getInput())
                    ? derive(limit.getInput(), limit) : limited(derive(limit.getInput(), null));
            case DistinctPlan distinct -> distinct(distinct, derive(distinct.getInput(), null));
            case AggregatePlan aggregate -> aggregate(aggregate);
            case SampleByPlan sample -> computed(sampleByRandomAccess(sample), ScanDirection.FORWARD,
                    capability(derive(sample.getInput(), null), LONG_SEQUENCE));
            case FillPlan fill ->
                    computed(Capability.NO, ScanDirection.FORWARD, capability(derive(fill.getInput(), null), LONG_SEQUENCE));
            case LatestByPlan latest -> latestBy(latest);
            case WindowPlan window -> window(window);
            case JoinPlan join -> join(join, join.getOrderedInputs().size(), false);
            case WindowJoinPlan windowJoin -> windowJoin(windowJoin);
            case SetOperationPlan operation -> setOperation(operation);
            case HorizonJoinPlan _ -> unknown(ScanDirection.UNKNOWN, Capability.UNKNOWN);
        };
    }

    private static ScanDirection direction(int properties) {
        return switch (properties >>> DIRECTION & 3) {
            case 0 -> ScanDirection.FORWARD;
            case 1 -> ScanDirection.BACKWARD;
            case 2 -> ScanDirection.OTHER;
            default -> ScanDirection.UNKNOWN;
        };
    }

    private static int distinct(DistinctPlan distinct, int input) {
        final int timestampIndex = timestamp(distinct.getInput(), null, false);
        final Capability isTimeSeries = and(capability(input, RANDOM_ACCESS),
                timestampIndex == UNKNOWN_TIMESTAMP ? Capability.UNKNOWN : Capability.of(timestampIndex >= 0));
        return choose(isTimeSeries, computed(Capability.YES, direction(input), capability(input, LONG_SEQUENCE)),
                computed(Capability.NO, ScanDirection.FORWARD, capability(input, LONG_SEQUENCE)));
    }

    private static int filter(FilterPlan filter) {
        final BoundExpression predicate = filter.getPredicate();
        if (filter.getInput() instanceof ScanPlan scan && !scan.isWalClientUpdate()) {
            return scan(scan);
        }
        return filtered(predicate, derive(filter.getInput(), null), parallel(filter.getAlgorithm()), false, true);
    }

    /**
     * The filter codegen builds over its input: none for a constant it folds, a gate for a runtime constant, else the
     * parallel filter, which absorbs a LIMIT it is given, or the serial one, as {@code isParallel} records.
     */
    private static int filtered(BoundExpression predicate, int input, Capability isParallel, boolean hasLimit, boolean isConstantFolded) {
        if (isConstantFolded && predicate instanceof ConstantExpression constant) {
            return constant.getLongValue() != 0 ? input : EMPTY;
        }
        if (!(predicate instanceof ColumnExpression) && (predicate.getFunctionFlags() & BoundExpression.RUNTIME_CONSTANT) != 0) {
            return with(input, ORDER_ADVICE, Capability.NO);
        }
        return properties(choose(isParallel, Capability.YES, capability(input, RANDOM_ACCESS)), Capability.NO,
                and(isParallel, Capability.of(hasLimit)), Capability.NO, direction(input), capability(input, LONG_SEQUENCE));
    }

    /**
     * The timestamp index of a factory that designates input 0's timestamp, or, when {@code isSource}, that input:
     * -1 when the index says it designates none.
     */
    private static int forwarded(int timestampIndex, boolean isSource) {
        return !isSource ? timestampIndex : timestampIndex == -1 ? -1 : 0;
    }

    /**
     * True when the window join step joins: its filter is not constant false, which null-extends the master instead.
     */
    private static boolean isJoiningStep(WindowJoinStep step) {
        return !(step.getFilter() instanceof ConstantExpression constant) || constant.getLongValue() != 0;
    }

    private static Capability isLightLatestBy(LatestByPlan latest) {
        return latest.getAlgorithm() == null ? Capability.UNKNOWN : Capability.of(latest.getAlgorithm() != LatestByPlan.Algorithm.MATERIALIZED);
    }

    private static boolean isPostingIndexDistinct(AggregatePlan aggregate) {
        final LogicalPlan input = aggregate.getInput();
        return (input instanceof FilterPlan filter ? filter.getInput() : input) instanceof ScanPlan scan
                && scan.getAccessPath() == ScanPlan.AccessPath.POSTING_DISTINCT;
    }

    /**
     * Whether the generator builds no factory for the sort, or for the LIMIT over it: YES when the input delivers the
     * order and applies the LIMIT, NO when a sort or LIMIT operator runs, UNKNOWN before planning.
     */
    private static Capability isSortPassedThrough(SortPlan sort, @Nullable LimitPlan sortedLimit) {
        final SortPlan.Algorithm algorithm = sort.getAlgorithm();
        if (algorithm == null || sortedLimit != null && sortedLimit.getApplication() == null) {
            return Capability.UNKNOWN;
        }
        return Capability.of(sortedLimit == null ? algorithm == SortPlan.Algorithm.INPUT_ORDER || algorithm == SortPlan.Algorithm.TIMESTAMP_DECLARATION
                : sortedLimit.getApplication() == LimitPlan.Application.INPUT && algorithm == SortPlan.Algorithm.INPUT_ORDER);
    }

    /**
     * Folds the steps of the join, returning the properties of its factory, or the index of its designated timestamp.
     */
    private static int join(JoinPlan join, int inputCount, boolean isTimestampIndex) {
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        final JoinInput first = ordered.getQuick(0);
        int properties = derive(first.getInput(), null);
        int timestampIndex = timestamp(first.getInput(), null, false);
        for (int i = 1; i < inputCount; i++) {
            final JoinInput step = ordered.getQuick(i);
            final JoinKind joinType = step.getJoinType();
            final Capability isFollowingAdvice = capability(properties, ORDER_ADVICE);
            final ScanDirection direction = direction(properties);
            BoundExpression filter = step.getPostJoinFilter();
            switch (joinType) {
                case UNNEST, ASOF, LT -> properties = joined(isFollowingAdvice, direction);
                case SPLICE -> {
                    properties = joined(isFollowingAdvice, direction);
                    timestampIndex = -1;
                }
                default -> {
                    final Capability isMarkout = step.getAlgorithm() == null ? Capability.UNKNOWN
                            : Capability.of(step.getAlgorithm() == JoinInput.Algorithm.MARKOUT);
                    final boolean isMasterNullExtended = joinType == JoinKind.RIGHT_OUTER || joinType == JoinKind.FULL_OUTER;
                    final boolean isSwappable = step.getMasterSide() == JoinInput.MasterSide.SMALLER;
                    final boolean isKeyed = step.getMasterKeyColumnIds().size() > 0;
                    final Capability isJoinFollowingAdvice = isSwappable ? Capability.NO : isKeyed ? (isMasterNullExtended ? Capability.NO : isFollowingAdvice)
                                                                                           : isMasterNullExtended ? capability(derive(step.getInput(), null), ORDER_ADVICE) : isFollowingAdvice;
                    properties = joined(choose(isMarkout, Capability.YES, isJoinFollowingAdvice),
                            choose(isMarkout, direction, isKeyed && isMasterNullExtended || isSwappable ? ScanDirection.OTHER : direction));
                    timestampIndex = chooseTimestamp(isMarkout, -1, isMasterNullExtended || isSwappable ? -1 : timestampIndex);
                    // An outer join applies its ON residual itself; an inner one filters the matched pairs with it.
                    if (!isMasterNullExtended && joinType != JoinKind.LEFT_OUTER && step.getOnResidual() != null) {
                        if (filter == null) {
                            filter = step.getOnResidual();
                        } else {
                            // The conjunction of the ON residual and the post-join filter, which codegen never folds.
                            properties = filtered(filter, properties, Capability.NO, false, false);
                            filter = null;
                        }
                    }
                }
            }
            if (filter != null) {
                properties = filtered(filter, properties, Capability.NO, false,
                        joinType == JoinKind.UNNEST || filter instanceof ConstantExpression constant && constant.isLiteral());
            }
        }
        return isTimestampIndex ? timestampIndex : properties;
    }

    /**
     * A join step, which emits in the order of its master and supports neither random access nor page frames.
     */
    private static int joined(Capability isFollowingAdvice, ScanDirection direction) {
        return properties(Capability.NO, Capability.NO, Capability.NO, isFollowingAdvice, direction, Capability.NO);
    }

    /**
     * The direction a scan that reads the rows of several keys from the index emits them in: by row id only from
     * forward frames through a heap of the key cursors, which it builds unless row order does not matter.
     */
    private static ScanDirection keyListDirection(ScanPlan scan) {
        final boolean isHeap = scan.isRowOrderRequired() || scan.getIndexOrder() == ScanPlan.IndexOrder.TIMESTAMP;
        return isHeap && scan.getScanDirection() == SortDirection.ASCENDING ? ScanDirection.FORWARD : ScanDirection.OTHER;
    }

    private static int latestBy(LatestByPlan latest) {
        final LogicalPlan input = latest.getInput();
        final LogicalPlan source = input instanceof FilterPlan filter ? filter.getInput() : input;
        if (source instanceof ScanPlan scan) {
            return scan(scan);
        }
        final int properties = derive(LogicalPlans.isTimestampDeclarationOnly(input) ? input.inputAt(0) : input, null);
        return computed(isLightLatestBy(latest), ScanDirection.FORWARD, capability(properties, LONG_SEQUENCE));
    }

    /**
     * A LIMIT over a factory with the given properties.
     */
    private static int limited(int input) {
        return properties(capability(input, RANDOM_ACCESS), Capability.NO, Capability.YES, Capability.NO, direction(input),
                capability(input, LONG_SEQUENCE));
    }

    private static int merge(int left, int right) {
        int properties = left;
        for (int shift = RANDOM_ACCESS; shift <= LONG_SEQUENCE; shift += 2) {
            if (((left ^ right) >>> shift & 3) != 0) {
                final int unknown = shift == DIRECTION ? ScanDirection.UNKNOWN.ordinal() : Capability.UNKNOWN.ordinal();
                properties = properties & ~(3 << shift) | unknown << shift;
            }
        }
        return properties;
    }

    private static int pageFrames(ScanPlan scan) {
        final ScanDirection direction = ScanDirection.of(scan.getScanDirection());
        return properties(Capability.of(scan.isRandomAccess()), Capability.YES, Capability.NO, Capability.of(direction == ScanDirection.BACKWARD),
                direction, Capability.NO);
    }

    private static Capability pageFrameTimeFrame(ScanPlan scan, boolean isRandomAccess) {
        return Capability.of(isRandomAccess && scanTimestampIndex(scan) >= 0 && scan.getScanDirection() != SortDirection.DESCENDING);
    }

    /**
     * Whether a filter runs in parallel, as order planning records it.
     */
    private static Capability parallel(@Nullable FilterPlan.Algorithm algorithm) {
        return algorithm == null ? Capability.UNKNOWN : Capability.of(algorithm == FilterPlan.Algorithm.PARALLEL);
    }

    private static int project(ProjectPlan project, @Nullable LimitPlan sortedLimit) {
        final LogicalPlan input = project.getInput();
        if (sortedLimit == null) {
            if (input instanceof WindowPlan window && LogicalPlans.isWindowOutputProjection(project, window)) {
                return window(window);
            }
            if (input instanceof WindowJoinPlan windowJoin && LogicalPlans.isColumnOnlyProjection(project)) {
                return windowJoin(windowJoin);
            }
        }
        final int properties = derive(input, sortedLimit);
        if (!LogicalPlans.isComputedProjection(project)) {
            return properties;
        }
        return with(with(properties, RANDOM_ACCESS, and(capability(properties, RANDOM_ACCESS), projectionRandomAccess(project))),
                PAGE_FRAMES, Capability.NO);
    }

    /**
     * Whether the functions of a computed projection support random access: none is random or reads a function
     * without random access.
     */
    private static Capability projectionRandomAccess(ProjectPlan project) {
        final ObjList<BoundExpression> expressions = project.getExpressions();
        for (int i = 0, n = expressions.size(); i < n; i++) {
            if ((expressions.getQuick(i).getFunctionFlags() & (BoundExpression.RANDOM | BoundExpression.NO_RANDOM_ACCESS)) != 0) {
                return Capability.NO;
            }
        }
        return Capability.YES;
    }

    /**
     * A declaring projection designates the column it declares, the input's timestamp only when that column is it.
     */
    private static int projectionTimestamp(ProjectPlan project, @Nullable LimitPlan sortedLimit, boolean isSource) {
        final LogicalPlan input = project.getInput();
        if (sortedLimit == null && input instanceof WindowJoinPlan windowJoin && LogicalPlans.isColumnOnlyProjection(project)) {
            final int masterIndex = timestamp(windowJoin.getMaster(), null, false);
            return forwarded(masterIndex == UNKNOWN_TIMESTAMP ? UNKNOWN_TIMESTAMP
                    : LogicalPlans.windowJoinProjectionTimestampIndex(project, windowJoin.getOutput(), masterIndex, windowJoin.getOutput().getColumnCount()
                                                                                                                    - windowJoin.getSteps().getQuick(windowJoin.getSteps().size() - 1).getAggregates().size()), isSource);
        }
        if (isSource && project.hasTimestampDeclaration()) {
            final OutputSchema inputOutput = input.getOutput();
            return project.getExpressions().getQuick(project.getOutput().getTimestampIndex()) instanceof ColumnExpression declared
                    && declared.isDirectReference() && inputOutput.getTimestampIndex() >= 0 && declared.getColumnId() == inputOutput.getTimestampColumnId() ? 0 : -1;
        }
        final int inputIndex = timestamp(input, sortedLimit, false);
        return forwarded(inputIndex == UNKNOWN_TIMESTAMP ? UNKNOWN_TIMESTAMP : LogicalPlans.projectedTimestampIndex(project, inputIndex), isSource);
    }

    private static int properties(
            Capability isRandomAccess,
            Capability isPageFrameSupported,
            Capability isLimitImplemented,
            Capability isFollowingAdvice,
            ScanDirection direction,
            Capability isLongSequence
    ) {
        return isRandomAccess.ordinal() << RANDOM_ACCESS | isPageFrameSupported.ordinal() << PAGE_FRAMES
                | isLimitImplemented.ordinal() << LIMIT | isFollowingAdvice.ordinal() << ORDER_ADVICE
                | direction.ordinal() << DIRECTION | isLongSequence.ordinal() << LONG_SEQUENCE;
    }

    private static Capability residualParallel(ScanPlan scan) {
        return parallel(scan.getResidualAlgorithm());
    }

    /**
     * The factory codegen builds for the access path the scan records, under the residual filter it records.
     */
    /**
     * Only the interpolating SAMPLE BY factory supports random access.
     */
    private static Capability sampleByRandomAccess(SampleByPlan sample) {
        return sample.getAlgorithm() == null ? Capability.UNKNOWN : Capability.of(sample.getAlgorithm() == SampleByPlan.Algorithm.INTERPOLATE);
    }

    private static int scan(ScanPlan scan) {
        final ScanPlan.AccessPath accessPath = scan.getAccessPath();
        if (accessPath == null) {
            return unknown(ScanDirection.of(scan.getScanDirection()), Capability.NO);
        }
        final BoundExpression residual = scan.getResidual();
        final boolean isLiveView = scan.getTableToken().isLiveView();
        final ScanDirection frameDirection = ScanDirection.of(scan.getScanDirection());
        final int properties = switch (accessPath) {
            case EMPTY, UPDATE_STUB -> EMPTY;
            case PAGE_FRAMES -> residual == null || isLiveView ? pageFrames(scan)
                    : filtered(residual, pageFrames(scan), residualParallel(scan), scan.getFilterLimit() != null, true);
            case SORTED_SYMBOL_INDEX ->
                    properties(Capability.YES, Capability.NO, Capability.NO, Capability.YES, ScanDirection.FORWARD, Capability.NO);
            case SYMBOL_INDEX -> symbolIndex(scan);
            case EXCLUDED_SYMBOL_INDEX ->
                    properties(Capability.YES, Capability.NO, Capability.NO, Capability.of(scan.isRequestedOrderDelivered()),
                            keyListDirection(scan), Capability.NO);
            // The sub-query is the base of its factory.
            case SYMBOL_SUBQUERY ->
                    properties(Capability.YES, Capability.NO, Capability.NO, Capability.NO, ScanDirection.FORWARD,
                            capability(derive(scan.getKeySubquery().getSubquery().getRoot(), null), LONG_SEQUENCE));
            case SYMBOL_PATTERN -> symbolPattern(scan);
            case POSTING_DISTINCT -> computed(Capability.NO, ScanDirection.FORWARD, Capability.NO);
            case LATEST_BY_VALUE, LATEST_BY_VALUES -> scan.getIndexRead() == ScanPlan.IndexRead.COVERING
                    ? computed(Capability.NO, accessPath == ScanPlan.AccessPath.LATEST_BY_VALUE ? ScanDirection.FORWARD : ScanDirection.OTHER, Capability.NO)
                    : computed(Capability.YES, ScanDirection.FORWARD, Capability.NO);
            case LATEST_BY_SUBQUERY, LATEST_BY_ALL_INDEXED, LATEST_BY_STATIC_SYMBOL, LATEST_BY_SYMBOLS, LATEST_BY_ALL ->
                    computed(Capability.YES, ScanDirection.FORWARD, Capability.NO);
        };
        return isLiveView && residual != null ? filtered(residual, properties, residualParallel(scan), scan.getFilterLimit() != null, true) : properties;
    }

    /**
     * Whether a page-frame scan serves time frames: it reads whole frames forward with random access, designates a
     * timestamp and carries no residual, or one the generator folds to true.
     */
    private static Capability scanTimeFrame(ScanPlan scan) {
        if (scan.getAccessPath() == null) {
            return Capability.UNKNOWN;
        }
        if (scan.getAccessPath() != ScanPlan.AccessPath.PAGE_FRAMES) {
            return Capability.NO;
        }
        return wrapped(scan.getResidual(), pageFrameTimeFrame(scan, scan.isRandomAccess()));
    }

    private static int scanTimestampIndex(ScanPlan scan) {
        return scan.isTimestampDropped() ? -1 : scan.getOutput().getTimestampIndex();
    }

    private static int setOperation(SetOperationPlan operation) {
        final int left = derive(operation.getLeft(), null);
        final int union = computed(Capability.NO, ScanDirection.FORWARD, Capability.NO);
        return switch (operation.getOperation()) {
            case UNION_ALL ->
                    operation.isMerged() ? properties(Capability.NO, Capability.NO, Capability.NO, Capability.YES,
                            ScanDirection.of(operation.getRequestedOrderDirection()), Capability.NO) : union;
            case UNION -> union;
            default -> computed(capability(left, RANDOM_ACCESS), direction(left), Capability.NO);
        };
    }

    private static int sort(SortPlan sort) {
        final int input = derive(sort.getInput(), null);
        return switch (sort.getAlgorithm()) {
            case INPUT_ORDER, TIMESTAMP_DECLARATION -> input;
            case null -> unknown(ScanDirection.UNKNOWN, capability(input, LONG_SEQUENCE));
            default -> sorted(sort, input, Capability.NO);
        };
    }

    private static int sorted(SortPlan sort, int input, Capability isLimitImplemented) {
        return properties(Capability.YES, Capability.NO, isLimitImplemented, Capability.NO, ScanDirection.of(sort.getDirections().getQuick(0)), capability(input, LONG_SEQUENCE));
    }

    /**
     * A sort under the LIMIT over it: the input applies the LIMIT to its own order or to the sort, a LIMIT operator
     * reads the input's order or the full sort, or the sort keeps only the rows the LIMIT selects.
     */
    private static int sortedLimit(SortPlan sort, LimitPlan limit) {
        final int input = derive(sort.getInput(), null);
        final boolean isInputOrder = sort.getAlgorithm() == SortPlan.Algorithm.INPUT_ORDER;
        return switch (limit.getApplication()) {
            case INPUT -> isInputOrder ? input : sorted(sort, input, Capability.NO);
            case OPERATOR -> isInputOrder ? limited(input) : sorted(sort, input, Capability.YES);
            case SORT -> sorted(sort, input, Capability.YES);
            case null -> unknown(ScanDirection.UNKNOWN, capability(input, LONG_SEQUENCE));
        };
    }


    /**
     * The direction of the factory under the filter a parallel GROUP BY or window join steals from the plan: the
     * frames of a self-filtering pattern scan, else the direction the filter passes through from its input.
     */
    private static ScanDirection stolenFilterDirection(LogicalPlan plan) {
        LogicalPlan filtered = plan;
        while (true) {
            if (filtered instanceof ProjectPlan project && !LogicalPlans.isComputedProjection(project)) {
                filtered = project.getInput();
            } else if (filtered instanceof FilterPlan filter && !(filter.getInput() instanceof ScanPlan)
                    && filter.getPredicate() instanceof ConstantExpression constant && constant.getLongValue() != 0) {
                filtered = filter.getInput();
            } else {
                break;
            }
        }
        if (filtered instanceof FilterPlan filter && filter.getInput() instanceof ScanPlan scan
                && scan.getAccessPath() == ScanPlan.AccessPath.SYMBOL_PATTERN && scan.getIndexRead() != ScanPlan.IndexRead.COVERING) {
            return ScanDirection.of(scan.getScanDirection());
        }
        return direction(derive(plan, null));
    }

    private static Capability supportsLongTopK(LogicalPlan plan, @Nullable LimitPlan sortedLimit, int columnIndex) {
        if (columnIndex < 0 || columnIndex >= plan.getOutput().getColumnCount()) {
            return Capability.UNKNOWN;
        }
        return switch (plan) {
            case AggregatePlan aggregate -> {
                if (aggregate.getInput() instanceof HorizonJoinPlan || aggregate.getGroupingExpressions().size() == 0) {
                    yield Capability.NO;
                }
                final int type = aggregate.getOutput().getColumnType(columnIndex);
                final Capability isLong = Capability.of(type == ColumnType.LONG || ColumnType.isTimestamp(type));
                yield isPostingIndexDistinct(aggregate) ? Capability.NO : isLong;
            }
            case ProjectPlan project -> {
                final LogicalPlan input = project.getInput();
                if (sortedLimit == null && input instanceof WindowPlan window && LogicalPlans.isWindowOutputProjection(project, window)) {
                    yield Capability.NO;
                }
                if (sortedLimit == null && input instanceof WindowJoinPlan windowJoin && LogicalPlans.isColumnOnlyProjection(project)) {
                    yield windowJoinLongTopK(windowJoin, windowJoin.getOutput().getColumnIndexById(((ColumnExpression) project.getExpressions().getQuick(columnIndex)).getColumnId()));
                }
                if (!(project.getExpressions().getQuick(columnIndex) instanceof ColumnExpression column)) {
                    yield Capability.NO;
                }
                final int index = input.getOutput().getColumnIndexById(column.getColumnId());
                if (index < 0) {
                    yield Capability.UNKNOWN;
                }
                if (!LogicalPlans.isComputedProjection(project)) {
                    yield supportsLongTopK(input, sortedLimit, index);
                }
                final int type = input.getOutput().getColumnType(index);
                if (project.hasUpdateConversions()) {
                    yield Capability.UNKNOWN;
                }
                if (type != ColumnType.LONG && !ColumnType.isTimestamp(type)) {
                    yield Capability.NO;
                }
                yield and(capability(project(project, sortedLimit), RANDOM_ACCESS),
                        supportsLongTopK(input, sortedLimit, index));
            }
            case FilterPlan filter -> filter.getInput() instanceof ScanPlan scan && !scan.isWalClientUpdate()
                    || !LogicalPlans.isConstant(filter.getPredicate()) ? Capability.NO : supportsLongTopK(filter.getInput(), null, columnIndex);
            case SortPlan sort -> {
                final Capability isPassedThrough = isSortPassedThrough(sort, sortedLimit);
                yield isPassedThrough == Capability.YES ? supportsLongTopK(sort.getInput(), null, columnIndex) : isPassedThrough;
            }
            case LimitPlan limit -> LogicalPlans.hasSortUnderStableProjects(limit.getInput())
                    ? supportsLongTopK(limit.getInput(), limit, columnIndex) : Capability.NO;
            case WindowJoinPlan windowJoin -> windowJoinLongTopK(windowJoin, columnIndex);
            default -> Capability.NO;
        };
    }

    private static Capability supportsSharedCursors(LogicalPlan plan, @Nullable LimitPlan sortedLimit) {
        return switch (plan) {
            case AggregatePlan aggregate -> aggregateSharedCursors(aggregate);
            case FilterPlan filter ->
                    filter.getInput() instanceof ScanPlan scan && !scan.isWalClientUpdate() ? Capability.NO
                            : wrapped(filter.getPredicate(), supportsSharedCursors(filter.getInput(), null));
            case ProjectPlan project -> {
                final LogicalPlan input = project.getInput();
                yield LogicalPlans.isComputedProjection(project)
                        || sortedLimit == null && (input instanceof WindowPlan window && LogicalPlans.isWindowOutputProjection(project, window)
                        || input instanceof WindowJoinPlan && LogicalPlans.isColumnOnlyProjection(project))
                        ? Capability.NO : supportsSharedCursors(input, sortedLimit);
            }
            case SortPlan sort -> {
                final Capability isPassedThrough = isSortPassedThrough(sort, sortedLimit);
                yield isPassedThrough == Capability.YES ? supportsSharedCursors(sort.getInput(), null) : isPassedThrough;
            }
            case LimitPlan limit -> LogicalPlans.hasSortUnderStableProjects(limit.getInput())
                    ? supportsSharedCursors(limit.getInput(), limit)
                    : limit.getApplication() == LimitPlan.Application.INPUT ? supportsSharedCursors(limit.getInput(), null)
                      : limit.getApplication() == null ? Capability.UNKNOWN : Capability.NO;
            case JoinPlan join -> join.getOrderedInputs().size() == 1
                    ? supportsSharedCursors(join.getOrderedInputs().getQuick(0).getInput(), null) : Capability.NO;
            default -> Capability.NO;
        };
    }

    private static Capability supportsTimeFrameCursor(LogicalPlan plan, @Nullable LimitPlan sortedLimit) {
        return switch (plan) {
            case ScanPlan scan -> scanTimeFrame(scan);
            case FilterPlan filter ->
                    filter.getInput() instanceof ScanPlan scan && !scan.isWalClientUpdate() ? scanTimeFrame(scan)
                            : wrapped(filter.getPredicate(), supportsTimeFrameCursor(filter.getInput(), null));
            case ProjectPlan project -> {
                final LogicalPlan input = project.getInput();
                if (sortedLimit == null && input instanceof WindowJoinPlan windowJoin && LogicalPlans.isColumnOnlyProjection(project)) {
                    yield windowJoinTimeFrame(windowJoin);
                }
                yield LogicalPlans.isComputedProjection(project)
                        || sortedLimit == null && input instanceof WindowPlan window && LogicalPlans.isWindowOutputProjection(project, window)
                        ? Capability.NO : supportsTimeFrameCursor(input, sortedLimit);
            }
            case SortPlan sort -> {
                final Capability isPassedThrough = isSortPassedThrough(sort, sortedLimit);
                yield isPassedThrough == Capability.YES ? supportsTimeFrameCursor(sort.getInput(), null) : isPassedThrough;
            }
            case LimitPlan limit -> LogicalPlans.hasSortUnderStableProjects(limit.getInput())
                    ? supportsTimeFrameCursor(limit.getInput(), limit)
                    : limit.getApplication() == LimitPlan.Application.INPUT ? supportsTimeFrameCursor(limit.getInput(), null)
                      : limit.getApplication() == null ? Capability.UNKNOWN : Capability.NO;
            case JoinPlan join -> join.getOrderedInputs().size() == 1
                    ? supportsTimeFrameCursor(join.getOrderedInputs().getQuick(0).getInput(), null) : Capability.NO;
            case WindowJoinPlan windowJoin -> windowJoinTimeFrame(windowJoin);
            default -> Capability.NO;
        };
    }

    private static int symbolIndex(ScanPlan scan) {
        final boolean isSingleKey = scan.getIndexKeys().size() == 1;
        final ScanDirection direction = isSingleKey ? ScanDirection.of(scan.getScanDirection()) : keyListDirection(scan);
        if (scan.getIndexRead() == ScanPlan.IndexRead.COVERING) {
            return covering(scan, direction);
        }
        return properties(Capability.YES, Capability.NO, Capability.NO, Capability.of(scan.isRequestedOrderDelivered()), direction, Capability.NO);
    }

    /**
     * The adaptive pattern scan: its index route emits rows by row id only from forward frames through a heap of the
     * key cursors, a parallel filter applies the residual within it unless a covering route reads frames, and a
     * filter above it applies the residual otherwise.
     */
    private static int symbolPattern(ScanPlan scan) {
        final SortKeys order = scan.getRequestedOrder();
        final boolean isHeap = scan.isRowOrderRequired() || order.size() == 1 && order.getColumnIds().getQuick(0) == scan.getNativeTimestampColumnId();
        final ScanDirection direction = isHeap && scan.getScanDirection() == SortDirection.ASCENDING ? ScanDirection.FORWARD : ScanDirection.OTHER;
        final boolean isCovering = scan.getIndexRead() == ScanPlan.IndexRead.COVERING;
        final Capability isParallel = residualParallel(scan);
        return properties(choose(isParallel, Capability.YES, Capability.of(!isCovering)), Capability.NO,
                and(isParallel, Capability.of(isCovering && scan.getCoveredFilterLimit() != null)), Capability.NO, direction, Capability.NO);
    }

    /**
     * The designated timestamp of the factory, see {@link #timestampIndex}, or the input it comes from when
     * {@code isSource}, see {@link #timestampSource}. A sort designates its own first key, also when its input already
     * delivers that order and it builds nothing.
     */
    private static int timestamp(LogicalPlan plan, @Nullable LimitPlan sortedLimit, boolean isSource) {
        return switch (plan) {
            case ProjectPlan project -> projectionTimestamp(project, sortedLimit, isSource);
            case LimitPlan limit ->
                    forwarded(timestamp(limit.getInput(), LogicalPlans.hasSortUnderStableProjects(limit.getInput()) ? limit : null,
                            false), isSource);
            case SortPlan sort -> isSource ? -1 : switch (sort.getAlgorithm()) {
                case INPUT_ORDER -> timestamp(sort.getInput(), null, false);
                case null -> UNKNOWN_TIMESTAMP;
                default -> sort.getOutput().getTimestampIndex();
            };
            case FilterPlan filter -> {
                if (!(filter.getInput() instanceof ScanPlan scan) || scan.isWalClientUpdate()) {
                    yield forwarded(timestamp(filter.getInput(), null, false), isSource);
                }
                yield forwarded(scanTimestampIndex(scan), isSource);
            }
            case LatestByPlan latest -> {
                final LogicalPlan input = latest.getInput();
                if ((input instanceof FilterPlan filter ? filter.getInput() : input) instanceof ScanPlan) {
                    yield forwarded(plan.getOutput().getTimestampIndex(), isSource);
                }
                final LogicalPlan base = LogicalPlans.isTimestampDeclarationOnly(input) ? input.inputAt(0) : input;
                yield forwarded(chooseTimestamp(isLightLatestBy(latest), -1, timestamp(base, null, false)), isSource);
            }
            case SetOperationPlan operation -> {
                if (operation.getOperation() == SetOperationKind.UNION_ALL) {
                    yield forwarded(operation.isMerged() ? operation.getOutput().getColumnIndexById(operation.getRequestedOrderColumnId()) : -1, isSource);
                }
                yield operation.getOperation().isUnion() || SetOperationCasts.isCastRequired(operation) ? -1
                        : forwarded(timestamp(operation.getLeft(), null, false), isSource);
            }
            case WindowPlan window -> forwarded(timestamp(window.getInput(), null, false), isSource);
            case WindowJoinPlan windowJoin -> forwarded(timestamp(windowJoin.getMaster(), null, false), isSource);
            case JoinPlan join -> forwarded(join(join, join.getOrderedInputs().size(), true), isSource);
            case AggregatePlan aggregate -> isSource ? -1 : aggregateTimestampIndex(aggregate);
            case ScanPlan scan -> isSource ? -1 : scanTimestampIndex(scan);
            case DistinctPlan _, FillPlan _ -> forwarded(plan.getOutput().getTimestampIndex(), isSource);
            case SampleByPlan _, FunctionSourcePlan _, HorizonJoinPlan _ ->
                    isSource ? -1 : plan.getOutput().getTimestampIndex();
        };
    }

    private static int unknown(ScanDirection direction, Capability isLongSequence) {
        return properties(Capability.UNKNOWN, Capability.UNKNOWN, Capability.UNKNOWN, Capability.UNKNOWN, direction, isLongSequence);
    }

    private static int window(WindowPlan window) {
        final int input = derive(window.getInput(), null);
        return properties(windowRandomAccess(window), Capability.NO, Capability.NO, capability(input, ORDER_ADVICE),
                direction(input), capability(input, LONG_SEQUENCE));
    }

    /**
     * Folds the steps of the window join: a step whose filter is constant false null-extends its master, keeping the
     * master's properties, and any other step is a serial or parallel window join, of which only the serial one
     * follows the master's advice and exposes its base chain.
     */
    private static int windowJoin(WindowJoinPlan windowJoin) {
        if (windowJoin.isEmpty()) {
            return EMPTY;
        }
        int properties = derive(windowJoin.getMaster(), null);
        final ObjList<WindowJoinStep> steps = windowJoin.getSteps();
        for (int i = 0, n = steps.size(); i < n; i++) {
            final WindowJoinStep step = steps.getQuick(i);
            if (isJoiningStep(step)) {
                final WindowJoinStep.Algorithm algorithm = step.getAlgorithm();
                final Capability isSerial = algorithm == null ? Capability.UNKNOWN : Capability.of(algorithm == WindowJoinStep.Algorithm.SERIAL);
                final ScanDirection direction = algorithm == WindowJoinStep.Algorithm.PARALLEL_STOLEN_FILTER
                        ? stolenFilterDirection(windowJoin.getMaster()) : direction(properties);
                properties = properties(Capability.NO, Capability.NO, Capability.NO, and(isSerial, capability(properties, ORDER_ADVICE)),
                        direction, and(isSerial, capability(properties, LONG_SEQUENCE)));
            }
        }
        return properties;
    }

    /**
     * Whether the cursor of the window join serves a long top-K over the column at {@code columnIndex} of its
     * output: only a null-extended master does, as the master would.
     */
    private static Capability windowJoinLongTopK(WindowJoinPlan windowJoin, int columnIndex) {
        if (windowJoin.isEmpty()) {
            return Capability.NO;
        }
        final ObjList<WindowJoinStep> steps = windowJoin.getSteps();
        for (int i = 0, n = steps.size(); i < n; i++) {
            if (isJoiningStep(steps.getQuick(i))) {
                return Capability.NO;
            }
        }
        final LogicalPlan master = windowJoin.getMaster();
        return columnIndex < master.getOutput().getColumnCount() ? supportsLongTopK(master, null, columnIndex) : Capability.NO;
    }

    /**
     * Whether the window join null-extends its master through every step, passing the master's time frames through.
     */
    private static Capability windowJoinTimeFrame(WindowJoinPlan windowJoin) {
        if (windowJoin.isEmpty()) {
            return Capability.NO;
        }
        final ObjList<WindowJoinStep> steps = windowJoin.getSteps();
        for (int i = 0, n = steps.size(); i < n; i++) {
            if (isJoiningStep(steps.getQuick(i))) {
                return Capability.NO;
            }
        }
        return supportsTimeFrameCursor(windowJoin.getMaster(), null);
    }

    /**
     * Only the cached window factories support random access.
     */
    private static Capability windowRandomAccess(WindowPlan window) {
        return window.getAlgorithm() == null ? Capability.UNKNOWN : Capability.of(window.getAlgorithm() != WindowPlan.Algorithm.STREAMING);
    }

    private static int with(int properties, int shift, Capability value) {
        return properties & ~(3 << shift) | value.ordinal() << shift;
    }

    /**
     * The property of the factory the generator builds over an input with the property {@code input} for the
     * predicate: the input's when the predicate is absent or folds to true, none when it folds to false or when a
     * filter or gate wraps the input.
     */
    private static Capability wrapped(@Nullable BoundExpression predicate, Capability input) {
        return predicate == null || predicate instanceof ConstantExpression constant && constant.getLongValue() != 0 ? input : Capability.NO;
    }

    /**
     * A three-valued property: the plan may leave it {@link #UNKNOWN} to codegen.
     */
    public enum Capability {
        NO, YES, UNKNOWN;

        public static Capability of(boolean value) {
            return value ? YES : NO;
        }
    }

    /**
     * The order rows come in by designated timestamp, as a factory reports it, or {@link #UNKNOWN} to the plan.
     */
    public enum ScanDirection {
        FORWARD, BACKWARD, OTHER, UNKNOWN;

        /**
         * The direction rows sorted by the designated timestamp come in.
         */
        public static ScanDirection of(SortDirection direction) {
            return direction == SortDirection.DESCENDING ? BACKWARD : FORWARD;
        }

        /**
         * The direction a factory reports as one of the {@code RecordCursorFactory.SCAN_DIRECTION_*} values.
         */
        public static ScanDirection of(int scanDirection) {
            return switch (scanDirection) {
                case RecordCursorFactory.SCAN_DIRECTION_FORWARD -> FORWARD;
                case RecordCursorFactory.SCAN_DIRECTION_BACKWARD -> BACKWARD;
                default -> OTHER;
            };
        }

        /**
         * The {@code RecordCursorFactory.SCAN_DIRECTION_*} value of a known direction.
         */
        public int factoryDirection() {
            return switch (this) {
                case FORWARD -> RecordCursorFactory.SCAN_DIRECTION_FORWARD;
                case BACKWARD -> RecordCursorFactory.SCAN_DIRECTION_BACKWARD;
                case OTHER -> RecordCursorFactory.SCAN_DIRECTION_OTHER;
                case UNKNOWN -> throw new IllegalStateException("scan direction is unknown");
            };
        }
    }
}
