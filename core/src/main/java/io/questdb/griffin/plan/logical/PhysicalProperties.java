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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.engine.LimitBounds;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * Derives, on demand and from the plan alone, the physical properties of the factory codegen builds for a plan node:
 * whether its cursor supports random access, page frames, time frames, shared cursors and long top-K, whether it
 * applies a LIMIT itself, follows the order advice of its scans or reads long_sequence(), and the order it emits its
 * rows in, and the table it reports. It reads the access paths and the operator decisions operator planning records on
 * the plan. A property is {@link Capability#UNKNOWN} where codegen decides it from facts the plan does not carry. It
 * also finds the table and the LIMITs that bound the rows the factory returns, see {@link #rowBoundScan}.
 */
public final class PhysicalProperties {
    public static final int UNKNOWN_TIMESTAMP = -2;
    private static final int DIRECTION = 8;
    private static final int LIMIT = 4;
    private static final int LONG_SEQUENCE = 10;
    private static final int ORDER_ADVICE = 6;
    private static final int PAGE_FRAMES = 2;
    private static final int RANDOM_ACCESS = 0;
    private static final int EMPTY = properties(Capability.YES, Capability.NO, Capability.NO, Capability.NO, ScanDirection.FORWARD, Capability.NO);

    private PhysicalProperties() {
    }

    /**
     * Whether the factory follows the order advice of the scans under it, so it delivers the order they were asked for.
     */
    public static Capability followsOrderAdvice(LogicalPlan plan) {
        return capability(derive(plan, null), ORDER_ADVICE);
    }

    /**
     * Whether the LIMITs whose count only execution knows along the inputs that bound the rows of {@code other}, see
     * {@link #rowBoundScan}, bound the rows of the plan too, as a LIMIT of the same bind variable along its inputs does;
     * true when no such LIMIT bounds the rows of {@code other}. A row bound of the plan no larger than that of
     * {@code other}, see {@link #rowBoundLimit}, then stays no larger for every count execution reads.
     */
    public static boolean hasRuntimeLimitsOf(LogicalPlan plan, LogicalPlan other) {
        for (LogicalPlan node = other; node != null; node = boundingInput(node)) {
            if (node instanceof LimitPlan limit && isRuntimeLimit(limit) && !hasRuntimeLimit(plan, limit)) {
                return false;
            }
        }
        return true;
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
        return capability(join(join, inputCount), ORDER_ADVICE);
    }

    /**
     * The order the factory of the first {@code inputCount} ordered inputs of the join emits its rows in.
     */
    public static ScanDirection masterScanDirection(JoinPlan join, int inputCount) {
        return direction(join(join, inputCount));
    }

    /**
     * Whether the cursor of the factory of the first {@code inputCount} ordered inputs of the join supports random
     * access.
     */
    public static Capability masterSupportsRandomAccess(JoinPlan join, int inputCount) {
        return capability(join(join, inputCount), RANDOM_ACCESS);
    }

    /**
     * The table the factory of the first {@code inputCount} ordered inputs of the join reports, see
     * {@link #tableToken}: the first input's alone, as the factory of a join reports none.
     */
    public static TableToken masterTableToken(JoinPlan join, int inputCount) {
        return inputCount == 1 ? tableToken(join.getOrderedInputs().getQuick(0).getInput()) : null;
    }

    /**
     * The designated timestamp of the factory of the first {@code inputCount} ordered inputs of the join.
     */
    public static int masterTimestampIndex(JoinPlan join, int inputCount) {
        return joinTimestamp(join, inputCount);
    }

    /**
     * The fewest rows a LIMIT of constant bounds allows along the inputs that bound the rows of the plan, see
     * {@link #rowBoundScan} and {@link #limitCount}; {@link Long#MAX_VALUE} for none. A LIMIT whose count only
     * execution knows may allow fewer, see {@link #hasRuntimeLimitsOf}.
     */
    public static long rowBoundLimit(LogicalPlan plan) {
        long limit = Long.MAX_VALUE;
        for (LogicalPlan node = plan; node != null; node = boundingInput(node)) {
            if (node instanceof LimitPlan limitPlan) {
                final long count = limitCount(limitPlan);
                if (count > -1) {
                    limit = Math.min(limit, count);
                }
            }
        }
        return limit;
    }

    /**
     * The scan whose table's row count bounds the rows the factory returns, null when no table does: a table scan
     * returns at most the rows of its table, and a filter, a projection, a keyed GROUP BY, a DISTINCT, a sort, a window,
     * a LIMIT and a decorrelation domain read at most the rows of their input. A scan of a live view returns rows its
     * table does not hold yet, so it bounds none.
     */
    public static ScanPlan rowBoundScan(LogicalPlan plan) {
        LogicalPlan node = plan;
        while (node != null && !(node instanceof ScanPlan)) {
            node = boundingInput(node);
        }
        return node instanceof ScanPlan scan && !scan.getTableToken().isLiveView() ? scan : null;
    }

    /**
     * The order the factory emits its rows in, by designated timestamp.
     */
    public static ScanDirection scanDirection(LogicalPlan plan) {
        return direction(derive(plan, null));
    }

    /**
     * Whether the factory a HORIZON JOIN matches the rows of its slave in serves time-frame cursors: the factory under
     * the filter the join steals from the slave, see {@link PlanToFactoryRules#horizonStolenFilter}, else the slave's.
     */
    public static Capability supportsHorizonSlaveTimeFrameCursor(LogicalPlan slave) {
        final FilterPlan filter = PlanToFactoryRules.horizonStolenFilter(slave);
        return filter != null ? supportsLeafTimeFrameCursor(filter) : supportsTimeFrameCursor(slave, null);
    }

    /**
     * Whether the factory under the filter the generator builds for the node serves time-frame cursors: the factory
     * of a non-fused filter's input, the page-frame scan of a fused one without its residual, or the page-frame scan
     * of a pattern scan that filters in parallel.
     */
    public static Capability supportsLeafTimeFrameCursor(FilterPlan filter) {
        final ScanPlan scan = PlanToFactoryRules.fusedScan(filter);
        if (scan == null) {
            return supportsTimeFrameCursor(filter.getChild(), null);
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
     * The table the factory reports, see {@link RecordCursorFactory#getTableToken()}: the table of a scan whose
     * factory reads the table's page frames, which the operators that keep the scan's cursor, or read its page
     * frames, report on; null for any other factory. Every access path and operator choice it reads is planned.
     */
    public static TableToken tableToken(LogicalPlan plan) {
        return switch (plan) {
            case ScanPlan scan -> scanTableToken(scan);
            case FilterPlan filter -> {
                final ScanPlan scan = PlanToFactoryRules.fusedScan(filter);
                yield scan != null ? scanTableToken(scan)
                        : filteredTableToken(filter.getPredicate(), filter.getAlgorithm(), tableToken(filter.getChild()));
            }
            case ProjectPlan project -> switch (PlanToFactoryRules.projectionFactory(project)) {
                case WINDOW, VIRTUAL -> null;
                case WINDOW_JOIN -> windowJoinTableToken((WindowJoinPlan) project.getChild());
                case SELECTION ->
                        PlanToFactoryRules.isIdentityProjection(project) ? tableToken(project.getChild()) : null;
            };
            case SortPlan sort ->
                    sort.getAlgorithm() == SortPlan.Algorithm.INPUT_ORDER ? tableToken(sort.getChild()) : null;
            case LimitPlan limit -> {
                if (LogicalPlans.hasSortUnderStableProjects(limit.getChild())) {
                    yield sortedLimitTableToken(limit.getChild(), limit);
                }
                yield limit.getApplication() == LimitPlan.Application.INPUT ? tableToken(limit.getChild()) : null;
            }
            case JoinPlan join -> masterTableToken(join, join.getOrderedInputs().size());
            case WindowJoinPlan windowJoin -> windowJoinTableToken(windowJoin);
            case LatestByPlan latest -> {
                final ScanPlan scan = PlanToFactoryRules.latestByScan(latest);
                yield scan != null ? scanTableToken(scan) : null;
            }
            default -> null;
        };
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
        final PlanToFactoryRules.AggregateFactory factory = PlanToFactoryRules.aggregateFactory(aggregate);
        if (factory == PlanToFactoryRules.AggregateFactory.HORIZON) {
            return computed(isKeyed, ScanDirection.FORWARD, capability(derive(((HorizonJoinPlan) aggregate.getChild()).getMaster(), null), LONG_SEQUENCE));
        }
        if (factory == PlanToFactoryRules.AggregateFactory.POSTING_DISTINCT) {
            return computed(Capability.NO, ScanDirection.FORWARD, Capability.NO);
        }
        final int input = derive(aggregate.getChild(), null);
        // A shared source the aggregate re-reads hides the base chain of the source.
        final Capability isLongSequence = aggregate.getSharedSource() == null ? capability(input, LONG_SEQUENCE)
                : and(Capability.UNKNOWN, capability(input, LONG_SEQUENCE));
        if (factory == PlanToFactoryRules.AggregateFactory.COUNT) {
            return computed(Capability.NO, ScanDirection.FORWARD, isLongSequence);
        }
        if (factory == PlanToFactoryRules.AggregateFactory.TIMESTAMP_ENDPOINT) {
            return computed(Capability.YES, ScanDirection.FORWARD, isLongSequence);
        }
        final ScanDirection direction = switch (aggregate.getAlgorithm()) {
            case PARALLEL -> direction(input);
            case PARALLEL_STOLEN_FILTER -> stolenFilterDirection(aggregate.getChild(), input);
            case HASH_JOIN -> ScanDirection.OTHER;
            case VECTORISED, SERIAL -> ScanDirection.FORWARD;
            case null -> ScanDirection.UNKNOWN;
        };
        return computed(isKeyed, direction, isLongSequence);
    }

    /**
     * Whether the GROUP BY serves shared cursors: the vectorised and the keyed parallel one always do; the serial and
     * the keyless parallel one only when other consumers read the aggregate, see {@link AggregatePlan#hasSharedConsumers()}.
     */
    private static Capability aggregateSharedCursors(AggregatePlan aggregate) {
        if (PlanToFactoryRules.aggregateFactory(aggregate) != PlanToFactoryRules.AggregateFactory.GROUP_BY) {
            return Capability.NO;
        }
        return switch (aggregate.getAlgorithm()) {
            case HASH_JOIN -> Capability.NO;
            case VECTORISED -> Capability.YES;
            case PARALLEL, PARALLEL_STOLEN_FILTER ->
                    Capability.of(aggregate.getGroupingExpressions().size() > 0 || aggregate.hasSharedConsumers());
            case SERIAL -> Capability.of(aggregate.hasSharedConsumers());
            case null -> Capability.UNKNOWN;
        };
    }

    /**
     * Count, timestamp endpoint and posting-index factories designate no timestamp, nor does the vectorised GROUP BY;
     * before order planning records the algorithm, a single key over page frames may still pick the vectorised one.
     */
    private static int aggregateTimestampIndex(AggregatePlan aggregate) {
        final int timestampIndex = aggregate.getOutput().getTimestampIndex();
        final PlanToFactoryRules.AggregateFactory factory = PlanToFactoryRules.aggregateFactory(aggregate);
        if (factory == PlanToFactoryRules.AggregateFactory.HORIZON || timestampIndex < 0) {
            return timestampIndex;
        }
        if (factory != PlanToFactoryRules.AggregateFactory.GROUP_BY) {
            return -1;
        }
        if (aggregate.getAlgorithm() == null && aggregate.getGroupingExpressions().size() == 1
                && capability(derive(aggregate.getChild(), null), PAGE_FRAMES) != Capability.NO) {
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

    /**
     * The input whose rows bound the rows of the plan, see {@link #rowBoundScan}; null when none does.
     */
    private static LogicalPlan boundingInput(LogicalPlan plan) {
        return switch (plan) {
            case FilterPlan filter -> filter.getChild();
            case ProjectPlan project -> project.getChild();
            case AggregatePlan aggregate -> aggregate.getGroupingExpressions().size() > 0 ? aggregate.getChild() : null;
            case DistinctPlan distinct -> distinct.getChild();
            case SortPlan sort -> sort.getChild();
            case WindowPlan window -> window.getChild();
            case DomainReadPlan read -> read.getProducer();
            case LimitPlan limit -> limit.getChild();
            default -> null;
        };
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

    /**
     * A factory that computes its rows, the way aggregates and LATEST BY do: no page frames, LIMIT or order advice.
     */
    private static int computed(Capability isRandomAccess, ScanDirection direction, Capability isLongSequence) {
        return properties(isRandomAccess, Capability.NO, Capability.NO, Capability.NO, direction, isLongSequence);
    }

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
            case LimitPlan limit -> LogicalPlans.hasSortUnderStableProjects(limit.getChild())
                    ? derive(limit.getChild(), limit) : limited(derive(limit.getChild(), null));
            case DistinctPlan distinct -> distinct(distinct, derive(distinct.getChild(), null));
            case AggregatePlan aggregate -> aggregate(aggregate);
            case DomainReadPlan read -> domainRead(read);
            case SampleByPlan sample -> computed(sampleByRandomAccess(sample), ScanDirection.FORWARD,
                    capability(derive(sample.getChild(), null), LONG_SEQUENCE));
            case FillPlan fill ->
                    computed(Capability.NO, ScanDirection.FORWARD, capability(derive(fill.getChild(), null), LONG_SEQUENCE));
            case LatestByPlan latest -> latestBy(latest);
            case WindowPlan window -> window(window);
            case JoinPlan join -> join(join, join.getOrderedInputs().size());
            case WindowJoinPlan windowJoin -> windowJoin(windowJoin);
            case SetOperationPlan operation -> setOperation(operation);
            case HorizonJoinPlan horizon -> computed(Capability.NO, scanDirection(horizon.getMaster()), Capability.NO);
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
        final int timestampIndex = timestamp(distinct.getChild(), null, false);
        final Capability isTimeSeries = and(capability(input, RANDOM_ACCESS),
                timestampIndex == UNKNOWN_TIMESTAMP ? Capability.UNKNOWN : Capability.of(timestampIndex >= 0));
        return choose(isTimeSeries, computed(Capability.YES, direction(input), capability(input, LONG_SEQUENCE)),
                computed(Capability.NO, ScanDirection.FORWARD, capability(input, LONG_SEQUENCE)));
    }

    /**
     * A domain read through a shared cursor of the producer's GROUP BY serves random access, scans forward and hides
     * the base chain; one that builds the producer's plan again reads it through a selection, which keeps the
     * producer's properties.
     */
    private static int domainRead(DomainReadPlan read) {
        return switch (read.getAlgorithm()) {
            case SHARED -> computed(Capability.YES, ScanDirection.FORWARD, Capability.NO);
            case REGENERATED -> derive(read.getProducer(), null);
            case null -> unknown(ScanDirection.UNKNOWN, Capability.UNKNOWN);
        };
    }

    private static int filter(FilterPlan filter) {
        final ScanPlan scan = PlanToFactoryRules.fusedScan(filter);
        if (scan != null) {
            return scan(scan);
        }
        return filtered(filter.getPredicate(), derive(filter.getChild(), null), parallel(filter.getAlgorithm()), false, true);
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
     * The table the filter codegen builds over an input that reports {@code input} reports: the input's for a constant
     * it folds to true, for the gate of a runtime constant and for the parallel filter, which keep the input's cursor
     * or read its page frames; none for a constant it folds to false and for the serial filter.
     */
    private static TableToken filteredTableToken(BoundExpression predicate, @Nullable FilterPlan.Algorithm algorithm, TableToken input) {
        if (predicate instanceof ConstantExpression constant) {
            return constant.getLongValue() != 0 ? input : null;
        }
        if (!(predicate instanceof ColumnExpression) && (predicate.getFunctionFlags() & BoundExpression.RUNTIME_CONSTANT) != 0) {
            return input;
        }
        return algorithm == FilterPlan.Algorithm.PARALLEL ? input : null;
    }

    /**
     * The timestamp index of a factory that designates input 0's timestamp, or, when {@code isSource}, that input:
     * -1 when the index says it designates none.
     */
    private static int forwarded(int timestampIndex, boolean isSource) {
        return !isSource ? timestampIndex : timestampIndex == -1 ? -1 : 0;
    }

    /**
     * Whether {@code limit} counts its rows by a bind variable, without a hi, and a LIMIT that counts them by the same
     * bind variable, without a hi, lies along the inputs that bound the rows of the plan, see {@link #rowBoundScan}.
     */
    private static boolean hasRuntimeLimit(LogicalPlan plan, LimitPlan limit) {
        if (limit.getHi() != null || !(limit.getLo() instanceof BindVariableExpression variable)) {
            return false;
        }
        for (LogicalPlan node = plan; node != null; node = boundingInput(node)) {
            if (node instanceof LimitPlan other && other.getHi() == null && other.getLo() instanceof BindVariableExpression otherVariable
                    && Chars.equals(variable.getName(), otherVariable.getName())) {
                return true;
            }
        }
        return false;
    }

    private static Capability isLightLatestBy(LatestByPlan latest) {
        return latest.getAlgorithm() == null ? Capability.UNKNOWN : Capability.of(latest.getAlgorithm() != LatestByPlan.Algorithm.MATERIALIZED);
    }

    /**
     * Whether the generator builds no factory for a LIMIT over no sort: YES when the input applies it, NO when a
     * LIMIT operator runs, UNKNOWN before planning.
     */
    private static Capability isLimitPassedThrough(LimitPlan limit) {
        final LimitPlan.Application application = limit.getApplication();
        return application == null ? Capability.UNKNOWN : Capability.of(application == LimitPlan.Application.INPUT);
    }

    private static boolean isRuntimeLimit(LimitPlan limit) {
        return !(limit.getLo() instanceof ConstantExpression) || limit.getHi() != null && !(limit.getHi() instanceof ConstantExpression);
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
     * Folds the steps of the join, returning the properties of its factory.
     */
    private static int join(JoinPlan join, int inputCount) {
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        int properties = derive(ordered.getQuick(0).getInput(), null);
        for (int i = 1; i < inputCount; i++) {
            final JoinInput step = ordered.getQuick(i);
            final JoinKind joinType = step.getJoinType();
            final Capability isFollowingAdvice = capability(properties, ORDER_ADVICE);
            final ScanDirection direction = direction(properties);
            BoundExpression filter = step.getPostJoinFilter();
            if (joinType.isRelational()) {
                final Capability isMarkout = markout(step);
                final boolean isMasterNullExtended = joinType.isMasterNulling();
                final boolean isSwappable = step.getMasterSide() == JoinInput.MasterSide.SMALLER;
                final boolean isKeyed = step.getKeys().size() > 0;
                final Capability isJoinFollowingAdvice = isSwappable ? Capability.NO : isKeyed ? (isMasterNullExtended ? Capability.NO : isFollowingAdvice)
                                                                                       : isMasterNullExtended ? capability(derive(step.getInput(), null), ORDER_ADVICE) : isFollowingAdvice;
                properties = joined(choose(isMarkout, Capability.YES, isJoinFollowingAdvice),
                        choose(isMarkout, direction, isKeyed && isMasterNullExtended || isSwappable ? ScanDirection.OTHER : direction));
                // An outer join applies its ON residual itself; an inner one filters the matched pairs with it.
                if (!isMasterNullExtended && !joinType.isSlaveNulling() && step.getOnResidual() != null) {
                    if (filter == null) {
                        filter = step.getOnResidual();
                    } else {
                        // The conjunction of the ON residual and the post-join filter, which codegen never folds.
                        properties = filtered(filter, properties, Capability.NO, false, false);
                        filter = null;
                    }
                }
            } else {
                properties = joined(isFollowingAdvice, direction);
            }
            if (filter != null) {
                properties = filtered(filter, properties, Capability.NO, false, PlanToFactoryRules.isPostJoinFilterFolded(step, filter));
            }
        }
        return properties;
    }

    /**
     * Folds the steps of the join, returning the index of the designated timestamp of its factory.
     */
    private static int joinTimestamp(JoinPlan join, int inputCount) {
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        int timestampIndex = timestamp(ordered.getQuick(0).getInput(), null, false);
        for (int i = 1; i < inputCount; i++) {
            final JoinInput step = ordered.getQuick(i);
            final JoinKind joinType = step.getJoinType();
            if (joinType.isRelational()) {
                timestampIndex = chooseTimestamp(markout(step), -1, PlanToFactoryRules.isMasterTimestampKept(join, i) ? timestampIndex : -1);
            } else if (joinType.isMasterNulling()) {
                timestampIndex = -1;
            }
        }
        return timestampIndex;
    }

    /**
     * A join step, which emits in the order of its master and supports neither random access nor page frames.
     */
    private static int joined(Capability isFollowingAdvice, ScanDirection direction) {
        return properties(Capability.NO, Capability.NO, Capability.NO, isFollowingAdvice, direction, Capability.NO);
    }

    /**
     * The direction a scan that reads the rows of several keys from the index emits them in: forward when it merges
     * the keys in table order over forward frames.
     */
    private static ScanDirection keyListDirection(ScanPlan scan) {
        return scan.getIndexOrder().isTableOrder() && scan.getScanDirection() == SortDirection.ASCENDING
                ? ScanDirection.FORWARD : ScanDirection.OTHER;
    }

    private static int latestBy(LatestByPlan latest) {
        final ScanPlan scan = PlanToFactoryRules.latestByScan(latest);
        if (scan != null) {
            return scan(scan);
        }
        final int properties = derive(PlanToFactoryRules.latestByBase(latest), null);
        return computed(isLightLatestBy(latest), ScanDirection.FORWARD, capability(properties, LONG_SEQUENCE));
    }

    /**
     * A LIMIT over a factory with the given properties.
     */
    private static int limited(int input) {
        return properties(capability(input, RANDOM_ACCESS), Capability.NO, Capability.YES, Capability.NO, direction(input),
                capability(input, LONG_SEQUENCE));
    }

    /**
     * The most rows a LIMIT returns, see {@link LimitBounds#rowCount}; -1 where only execution knows a bound
     * or a bound is NULL.
     */
    private static long limitCount(LimitPlan limit) {
        if (isRuntimeLimit(limit)) {
            return -1;
        }
        final long lo = LogicalPlans.limitValue((ConstantExpression) limit.getLo());
        final long hi = limit.getHi() == null ? 0 : LogicalPlans.limitValue((ConstantExpression) limit.getHi());
        return lo == Numbers.LONG_NULL || hi == Numbers.LONG_NULL ? -1 : LimitBounds.rowCount(lo, hi);
    }

    /**
     * Whether the join step runs the markout join, UNKNOWN before operator planning records its algorithm.
     */
    private static Capability markout(JoinInput step) {
        return step.getAlgorithm() == null ? Capability.UNKNOWN : Capability.of(step.getAlgorithm() == JoinInput.Algorithm.MARKOUT);
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

    /**
     * Time frames follow the partitions of the table's own designated timestamp, so a scan that designates another
     * column has none. A table without a designated timestamp has a single partition, which any timestamp follows.
     */
    private static Capability pageFrameTimeFrame(ScanPlan scan, boolean isRandomAccess) {
        return Capability.of(isRandomAccess && scanTimestampIndex(scan) >= 0 && scan.getScanDirection() != SortDirection.DESCENDING
                && (scan.getNativeTimestampColumnId() == -1 || scan.getOutput().getTimestampColumnId() == scan.getNativeTimestampColumnId()));
    }

    private static int pageFrames(ScanPlan scan) {
        final ScanDirection direction = ScanDirection.of(scan.getScanDirection());
        return properties(Capability.of(scan.isRandomAccess()), Capability.YES, Capability.NO,
                Capability.of(PlanToFactoryRules.isPageFrameAdviceFollowed(scan)), direction, Capability.NO);
    }

    /**
     * Whether a filter runs in parallel, as operator planning records it.
     */
    private static Capability parallel(@Nullable FilterPlan.Algorithm algorithm) {
        return algorithm == null ? Capability.UNKNOWN : Capability.of(algorithm == FilterPlan.Algorithm.PARALLEL);
    }

    private static int project(ProjectPlan project, @Nullable LimitPlan sortedLimit) {
        return switch (PlanToFactoryRules.projectionFactory(project)) {
            case WINDOW -> window((WindowPlan) project.getChild());
            case WINDOW_JOIN -> windowJoin((WindowJoinPlan) project.getChild());
            case VIRTUAL -> {
                final int properties = derive(project.getChild(), sortedLimit);
                yield with(with(properties, RANDOM_ACCESS, and(capability(properties, RANDOM_ACCESS), projectionRandomAccess(project))),
                        PAGE_FRAMES, Capability.NO);
            }
            case SELECTION -> derive(project.getChild(), sortedLimit);
        };
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
        final LogicalPlan input = project.getChild();
        final WindowJoinPlan windowJoin = PlanToFactoryRules.generatedWindowJoin(project);
        if (windowJoin != null) {
            final int masterIndex = timestamp(windowJoin.getMaster(), null, false);
            return forwarded(masterIndex == UNKNOWN_TIMESTAMP ? UNKNOWN_TIMESTAMP
                    : PlanToFactoryRules.windowJoinProjectionTimestampIndex(project, windowJoin.getOutput(), masterIndex, windowJoin.getOutput().getColumnCount()
                                                                                                                          - windowJoin.getSteps().getQuick(windowJoin.getSteps().size() - 1).getAggregates().size()), isSource);
        }
        if (isSource && project.hasTimestampDeclaration()) {
            final OutputSchema inputOutput = input.getOutput();
            return project.getExpressions().getQuick(project.getOutput().getTimestampIndex()) instanceof ColumnExpression declared
                    && declared.isDirectReference() && inputOutput.getTimestampIndex() >= 0 && declared.getColumnId() == inputOutput.getTimestampColumnId() ? 0 : -1;
        }
        final int inputIndex = timestamp(input, sortedLimit, false);
        return forwarded(inputIndex == UNKNOWN_TIMESTAMP ? UNKNOWN_TIMESTAMP : PlanToFactoryRules.projectedTimestampIndex(project, inputIndex), isSource);
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
     * Only the interpolating SAMPLE BY factory supports random access.
     */
    private static Capability sampleByRandomAccess(SampleByPlan sample) {
        return sample.getAlgorithm() == null ? Capability.UNKNOWN : Capability.of(sample.getAlgorithm() == SampleByPlan.Algorithm.INTERPOLATE);
    }

    /**
     * The factory codegen builds for the access path the scan records, under the residual filter it records.
     */
    private static int scan(ScanPlan scan) {
        final ScanPlan.AccessPath accessPath = scan.getAccessPath();
        if (accessPath == null) {
            return unknown(ScanDirection.of(scan.getScanDirection()), Capability.NO);
        }
        final BoundExpression residual = scan.getResidual();
        final boolean isLiveView = scan.getTableToken().isLiveView();
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
     * The table the factory of the scan reports, see {@link #tableToken}: the factory of its access path under the
     * residual filter it records, of which a covering index scan and a pattern scan that filters serially report
     * none; neither does the live-view wrapper nor the stub a client-side UPDATE reads.
     */
    private static TableToken scanTableToken(ScanPlan scan) {
        final TableToken table = scan.getTableToken();
        if (scan.isSequencedUpdate() || table.isLiveView() && !scan.isUpdate()) {
            return null;
        }
        return switch (scan.getAccessPath()) {
            case PAGE_FRAMES ->
                    scan.getResidual() == null ? table : filteredTableToken(scan.getResidual(), scan.getResidualAlgorithm(), table);
            case SORTED_SYMBOL_INDEX, EXCLUDED_SYMBOL_INDEX, SYMBOL_SUBQUERY, LATEST_BY_SUBQUERY, LATEST_BY_ALL_INDEXED,
                 LATEST_BY_STATIC_SYMBOL, LATEST_BY_SYMBOLS, LATEST_BY_ALL -> table;
            case SYMBOL_INDEX -> scan.getIndexRead() == ScanPlan.IndexRead.COVERING ? null : table;
            case LATEST_BY_VALUE, LATEST_BY_VALUES ->
                    scan.getKeySubquery() == null && scan.getIndexRead() == ScanPlan.IndexRead.COVERING ? null : table;
            case SYMBOL_PATTERN -> scan.getResidualAlgorithm() == FilterPlan.Algorithm.PARALLEL ? table : null;
            case EMPTY, UPDATE_STUB, POSTING_DISTINCT -> null;
            case null -> throw new IllegalStateException("scan access path is not planned");
        };
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
        final int input = derive(sort.getChild(), null);
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
        final int input = derive(sort.getChild(), null);
        final boolean isInputOrder = sort.getAlgorithm() == SortPlan.Algorithm.INPUT_ORDER;
        return switch (limit.getApplication()) {
            case INPUT -> isInputOrder ? input : sorted(sort, input, Capability.NO);
            case OPERATOR, MASTER_SUFFIX -> isInputOrder ? limited(input) : sorted(sort, input, Capability.YES);
            case SORT -> sorted(sort, input, Capability.YES);
            case null -> unknown(ScanDirection.UNKNOWN, capability(input, LONG_SEQUENCE));
        };
    }

    /**
     * The direction of the factory under the filter a parallel GROUP BY or window join steals from the plan: the
     * frames of a self-filtering pattern scan, else the direction of {@code properties}, the properties of the plan.
     */
    /**
     * The table the sorted LIMIT over the sort under {@code plan}, read through stable projections, reports: the
     * input's when the input delivers the order and applies the LIMIT, so no sort runs, and the parallel top-K's, which
     * reports the table of the page frames it reads unless it builds a projection over itself; none for a sort.
     */
    private static TableToken sortedLimitTableToken(LogicalPlan plan, LimitPlan limit) {
        if (plan instanceof ProjectPlan project) {
            final TableToken input = sortedLimitTableToken(project.getChild(), limit);
            return PlanToFactoryRules.isIdentityProjection(project) ? input : null;
        }
        final SortPlan sort = (SortPlan) plan;
        return switch (sort.getAlgorithm()) {
            case PARALLEL_TOP_K, PARALLEL_FILTERED_TOP_K -> {
                if (PlanToFactoryRules.parallelTopKProjection(sort) != null) {
                    yield null;
                }
                final LogicalPlan source = PlanToFactoryRules.parallelTopKSource(sort);
                yield sort.getAlgorithm() == SortPlan.Algorithm.PARALLEL_TOP_K ? tableToken(source)
                        : stolenFilterLeafTableToken(PlanToFactoryRules.stolenFilter(source));
            }
            case INPUT_ORDER ->
                    limit.getApplication() == LimitPlan.Application.INPUT ? tableToken(sort.getChild()) : null;
            case null, default -> null;
        };
    }

    private static ScanDirection stolenFilterDirection(LogicalPlan plan, int properties) {
        LogicalPlan filtered = plan;
        while (true) {
            if (filtered instanceof ProjectPlan project && !LogicalPlans.isComputedProjection(project)) {
                filtered = project.getChild();
            } else if (filtered instanceof FilterPlan filter && !(filter.getChild() instanceof ScanPlan)
                    && filter.getPredicate() instanceof ConstantExpression constant && constant.getLongValue() != 0) {
                filtered = filter.getChild();
            } else {
                break;
            }
        }
        if (filtered instanceof FilterPlan filter && filter.getChild() instanceof ScanPlan scan
                && scan.getAccessPath() == ScanPlan.AccessPath.SYMBOL_PATTERN && scan.getIndexRead() != ScanPlan.IndexRead.COVERING) {
            return ScanDirection.of(scan.getScanDirection());
        }
        return direction(properties);
    }

    /**
     * The table the factory under the filter a parallel consumer steals reports: the factory of a non-fused filter's
     * input, or the factory of a fused filter's access path without its residual, which the consumer applies.
     */
    private static TableToken stolenFilterLeafTableToken(FilterPlan filter) {
        final ScanPlan scan = PlanToFactoryRules.fusedScan(filter);
        if (scan == null) {
            return tableToken(filter.getChild());
        }
        final ScanPlan.AccessPath accessPath = scan.getAccessPath();
        if (accessPath != ScanPlan.AccessPath.PAGE_FRAMES && accessPath != ScanPlan.AccessPath.SYMBOL_PATTERN) {
            return scanTableToken(scan);
        }
        return scan.getTableToken().isLiveView() && !scan.isUpdate() ? null : scan.getTableToken();
    }

    private static Capability supportsLongTopK(LogicalPlan plan, @Nullable LimitPlan sortedLimit, int columnIndex) {
        if (columnIndex < 0 || columnIndex >= plan.getOutput().getColumnCount()) {
            return Capability.UNKNOWN;
        }
        return switch (plan) {
            case DomainReadPlan read -> switch (read.getAlgorithm()) {
                case SHARED -> Capability.NO;
                case REGENERATED -> supportsLongTopK(read.getProducer(), null,
                        read.getProducer().getOutput().getColumnIndexById(read.getProducerColumnIds().getQuick(columnIndex)));
                case null -> Capability.UNKNOWN;
            };
            case AggregatePlan aggregate -> {
                if (PlanToFactoryRules.aggregateFactory(aggregate) != PlanToFactoryRules.AggregateFactory.GROUP_BY
                        || aggregate.getAlgorithm() == AggregatePlan.Algorithm.HASH_JOIN
                        || aggregate.getGroupingExpressions().size() == 0) {
                    yield Capability.NO;
                }
                final int type = aggregate.getOutput().getColumnType(columnIndex);
                yield Capability.of(type == ColumnType.LONG || ColumnType.isTimestamp(type));
            }
            case ProjectPlan project -> {
                final LogicalPlan input = project.getChild();
                if (PlanToFactoryRules.generatedWindow(project) != null) {
                    yield Capability.NO;
                }
                final WindowJoinPlan windowJoin = PlanToFactoryRules.generatedWindowJoin(project);
                if (windowJoin != null) {
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
            case FilterPlan filter -> PlanToFactoryRules.isFusedFilter(filter)
                    || !LogicalPlans.isConstant(filter.getPredicate()) ? Capability.NO : supportsLongTopK(filter.getChild(), null, columnIndex);
            case SortPlan sort -> {
                final Capability isPassedThrough = isSortPassedThrough(sort, sortedLimit);
                yield isPassedThrough == Capability.YES ? supportsLongTopK(sort.getChild(), null, columnIndex) : isPassedThrough;
            }
            case LimitPlan limit -> LogicalPlans.hasSortUnderStableProjects(limit.getChild())
                    ? supportsLongTopK(limit.getChild(), limit, columnIndex) : Capability.NO;
            case WindowJoinPlan windowJoin -> windowJoinLongTopK(windowJoin, columnIndex);
            default -> Capability.NO;
        };
    }

    private static Capability supportsSharedCursors(LogicalPlan plan, @Nullable LimitPlan sortedLimit) {
        return switch (plan) {
            case AggregatePlan aggregate -> aggregateSharedCursors(aggregate);
            case DomainReadPlan read -> switch (read.getAlgorithm()) {
                case SHARED -> Capability.NO;
                case REGENERATED -> aggregateSharedCursors(read.getProducer());
                case null -> Capability.UNKNOWN;
            };
            case FilterPlan filter -> PlanToFactoryRules.isFusedFilter(filter) ? Capability.NO
                    : wrapped(filter.getPredicate(), supportsSharedCursors(filter.getChild(), null));
            case ProjectPlan project ->
                    PlanToFactoryRules.projectionFactory(project) != PlanToFactoryRules.ProjectionFactory.SELECTION
                            ? Capability.NO : supportsSharedCursors(project.getChild(), sortedLimit);
            case SortPlan sort -> {
                final Capability isPassedThrough = isSortPassedThrough(sort, sortedLimit);
                yield isPassedThrough == Capability.YES ? supportsSharedCursors(sort.getChild(), null) : isPassedThrough;
            }
            case LimitPlan limit -> {
                if (LogicalPlans.hasSortUnderStableProjects(limit.getChild())) {
                    yield supportsSharedCursors(limit.getChild(), limit);
                }
                final Capability isPassedThrough = isLimitPassedThrough(limit);
                yield isPassedThrough == Capability.YES ? supportsSharedCursors(limit.getChild(), null) : isPassedThrough;
            }
            case JoinPlan join -> join.getOrderedInputs().size() == 1
                    ? supportsSharedCursors(join.getOrderedInputs().getQuick(0).getInput(), null) : Capability.NO;
            default -> Capability.NO;
        };
    }

    private static Capability supportsTimeFrameCursor(LogicalPlan plan, @Nullable LimitPlan sortedLimit) {
        return switch (plan) {
            case ScanPlan scan -> scanTimeFrame(scan);
            case FilterPlan filter ->
                    PlanToFactoryRules.fusedScan(filter) instanceof ScanPlan scan ? scanTimeFrame(scan)
                            : wrapped(filter.getPredicate(), supportsTimeFrameCursor(filter.getChild(), null));
            case ProjectPlan project -> switch (PlanToFactoryRules.projectionFactory(project)) {
                case WINDOW, VIRTUAL -> Capability.NO;
                case WINDOW_JOIN -> windowJoinTimeFrame((WindowJoinPlan) project.getChild());
                case SELECTION -> supportsTimeFrameCursor(project.getChild(), sortedLimit);
            };
            case SortPlan sort -> {
                final Capability isPassedThrough = isSortPassedThrough(sort, sortedLimit);
                yield isPassedThrough == Capability.YES ? supportsTimeFrameCursor(sort.getChild(), null) : isPassedThrough;
            }
            case LimitPlan limit -> {
                if (LogicalPlans.hasSortUnderStableProjects(limit.getChild())) {
                    yield supportsTimeFrameCursor(limit.getChild(), limit);
                }
                final Capability isPassedThrough = isLimitPassedThrough(limit);
                yield isPassedThrough == Capability.YES ? supportsTimeFrameCursor(limit.getChild(), null) : isPassedThrough;
            }
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
     * The adaptive pattern scan: its index route emits rows in the order of its key list read, a parallel filter
     * applies the residual within it unless a covering route reads frames, and a filter above it applies the residual
     * otherwise.
     */
    private static int symbolPattern(ScanPlan scan) {
        final ScanDirection direction = keyListDirection(scan);
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
                    forwarded(timestamp(limit.getChild(), LogicalPlans.hasSortUnderStableProjects(limit.getChild()) ? limit : null,
                            false), isSource);
            case SortPlan sort -> isSource ? -1 : switch (sort.getAlgorithm()) {
                case INPUT_ORDER -> timestamp(sort.getChild(), null, false);
                case null -> UNKNOWN_TIMESTAMP;
                default -> sort.getOutput().getTimestampIndex();
            };
            case FilterPlan filter -> {
                final ScanPlan scan = PlanToFactoryRules.fusedScan(filter);
                if (scan == null) {
                    yield forwarded(timestamp(filter.getChild(), null, false), isSource);
                }
                yield forwarded(scanTimestampIndex(scan), isSource);
            }
            case LatestByPlan latest -> {
                if (PlanToFactoryRules.latestByScan(latest) != null) {
                    yield forwarded(plan.getOutput().getTimestampIndex(), isSource);
                }
                yield forwarded(chooseTimestamp(isLightLatestBy(latest), -1, timestamp(PlanToFactoryRules.latestByBase(latest), null, false)),
                        isSource);
            }
            case SetOperationPlan operation -> {
                if (operation.getOperation() == SetOperationKind.UNION_ALL) {
                    yield forwarded(operation.isMerged() ? operation.getOutput().getColumnIndexById(operation.getRequestedOrderColumnId()) : -1, isSource);
                }
                yield operation.getOperation().isUnion() || operation.isCastRequired() ? -1
                        : forwarded(timestamp(operation.getLeft(), null, false), isSource);
            }
            case WindowPlan window -> forwarded(timestamp(window.getChild(), null, false), isSource);
            case WindowJoinPlan windowJoin -> forwarded(timestamp(windowJoin.getMaster(), null, false), isSource);
            case JoinPlan join -> forwarded(joinTimestamp(join, join.getOrderedInputs().size()), isSource);
            case AggregatePlan aggregate -> isSource ? -1 : aggregateTimestampIndex(aggregate);
            case DomainReadPlan _ -> -1;
            case ScanPlan scan -> isSource ? -1 : scanTimestampIndex(scan);
            case DistinctPlan _, FillPlan _ -> forwarded(plan.getOutput().getTimestampIndex(), isSource);
            case HorizonJoinPlan horizon -> forwarded(timestamp(horizon.getMaster(), null, false), isSource);
            case SampleByPlan _, FunctionSourcePlan _ -> isSource ? -1 : plan.getOutput().getTimestampIndex();
        };
    }

    private static int unknown(ScanDirection direction, Capability isLongSequence) {
        return properties(Capability.UNKNOWN, Capability.UNKNOWN, Capability.UNKNOWN, Capability.UNKNOWN, direction, isLongSequence);
    }

    private static int window(WindowPlan window) {
        final int input = derive(window.getChild(), null);
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
        final int master = derive(windowJoin.getMaster(), null);
        int properties = master;
        final ObjList<WindowJoinStep> steps = windowJoin.getSteps();
        for (int i = 0, n = steps.size(); i < n; i++) {
            final WindowJoinStep step = steps.getQuick(i);
            if (!step.isNullExtending()) {
                final WindowJoinStep.Algorithm algorithm = step.getAlgorithm();
                final Capability isSerial = algorithm == null ? Capability.UNKNOWN : Capability.of(algorithm == WindowJoinStep.Algorithm.SERIAL);
                final ScanDirection direction = algorithm == WindowJoinStep.Algorithm.PARALLEL_STOLEN_FILTER
                        ? stolenFilterDirection(windowJoin.getMaster(), master) : direction(properties);
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
        final LogicalPlan master = windowJoin.getMaster();
        return windowJoin.isMasterPassedThrough() && columnIndex < master.getOutput().getColumnCount()
                ? supportsLongTopK(master, null, columnIndex) : Capability.NO;
    }

    /**
     * The table the window join factory reports: its master's when every step joins in parallel, reading the page
     * frames of the master, or of the factory under the master's filter the first step steals; none when a step
     * null-extends or joins serially, nor for an empty window join.
     */
    private static TableToken windowJoinTableToken(WindowJoinPlan windowJoin) {
        if (windowJoin.isEmpty()) {
            return null;
        }
        final ObjList<WindowJoinStep> steps = windowJoin.getSteps();
        final LogicalPlan master = windowJoin.getMaster();
        TableToken table = steps.size() > 0 && steps.getQuick(0).getAlgorithm() == WindowJoinStep.Algorithm.PARALLEL_STOLEN_FILTER
                ? stolenFilterLeafTableToken(PlanToFactoryRules.stolenFilter(master)) : tableToken(master);
        for (int i = 0, n = steps.size(); i < n; i++) {
            final WindowJoinStep step = steps.getQuick(i);
            if (step.isNullExtending() || step.getAlgorithm() == WindowJoinStep.Algorithm.SERIAL) {
                table = null;
            }
        }
        return table;
    }

    /**
     * Whether the window join null-extends its master through every step, passing the master's time frames through.
     */
    private static Capability windowJoinTimeFrame(WindowJoinPlan windowJoin) {
        return windowJoin.isMasterPassedThrough() ? supportsTimeFrameCursor(windowJoin.getMaster(), null) : Capability.NO;
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
