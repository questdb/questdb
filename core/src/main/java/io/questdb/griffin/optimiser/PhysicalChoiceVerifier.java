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

import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PhysicalProperties;
import io.questdb.griffin.plan.logical.PlanToFactoryRules;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationKind;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowJoinStep;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.IntList;
import org.jetbrains.annotations.Nullable;

/**
 * Checks the physical choices operator planning records on plan nodes against the rules the planner and the generator
 * decide them by, including that every input a choice reads back by row id derives random access. Each check returns
 * the {@link PlanVerifier} invariant the node violates, or null; under access path planning's output,
 * {@code isPlanned}, every choice the generator builds the node by is recorded.
 */
final class PhysicalChoiceVerifier {

    private PhysicalChoiceVerifier() {
    }

    private static boolean isBounded(SortPlan.Algorithm algorithm) {
        return algorithm == SortPlan.Algorithm.LIMITED || algorithm == SortPlan.Algorithm.PRESORTED_LIMITED || isTopK(algorithm);
    }

    /**
     * Whether the hints of the keyed ASOF step ask for the time-frame scan operator planning records: the dense scan
     * the {@code asof_dense} hint asks for, else the index or memoized scan of a single key their hints ask for, which
     * the memoized drive-by hint refines, or the fast scan.
     */
    private static boolean isHintedTimeFrameScan(JoinInput.TimeFrameScan scan, JoinInput step) {
        final int hints = step.getHints();
        final boolean isDense = (hints & JoinInput.HINT_ASOF_DENSE) != 0;
        final boolean isSingleKey = step.getKeys().size() == 1;
        return switch (scan) {
            case DENSE -> isDense;
            case DENSE_SINGLE_SYMBOL -> isDense && isSingleKey;
            case INDEXED -> !isDense && isSingleKey && (hints & JoinInput.HINT_ASOF_INDEX) != 0;
            case MEMOIZED ->
                    !isDense && isSingleKey && (hints & JoinInput.HINT_ASOF_MEMOIZED) != 0 && (hints & JoinInput.HINT_ASOF_MEMOIZED_DRIVEBY) == 0;
            case MEMOIZED_DRIVEBY -> !isDense && isSingleKey && (hints & JoinInput.HINT_ASOF_MEMOIZED_DRIVEBY) != 0;
            case FAST -> !isDense;
        };
    }

    private static boolean isLinear(JoinInput step) {
        return (step.getHints() & JoinInput.HINT_ASOF_LINEAR) != 0;
    }

    /**
     * Whether the window join step at {@code index} reads the page frames of the master: every step before it
     * null-extends the master, which keeps its page frames.
     */
    private static boolean isMasterPageFrameRead(WindowJoinPlan windowJoin, int index, boolean isPlanned) {
        for (int i = 0; i < index; i++) {
            if (!windowJoin.getSteps().getQuick(i).isNullExtending()) {
                return false;
            }
        }
        return !isUnsatisfied(PhysicalProperties.supportsPageFrameCursor(windowJoin.getMaster()), isPlanned);
    }

    /**
     * Whether the join step reads its master back by row id, which requires a master with random access.
     */
    private static boolean isMasterReadByRowId(JoinInput.Algorithm algorithm) {
        return algorithm == JoinInput.Algorithm.MARKOUT;
    }

    /**
     * Whether the parallel aggregate reads the frames its algorithm names, of its source, the master of a HORIZON JOIN
     * or the GROUP BY's base: the source's page frames, or the frames under the filter it steals from the source, as
     * {@link OperatorPlanning#hasParallelFunctions} and {@link OperatorPlanning#isParallelGroupBy} allow.
     */
    private static boolean isParallelAggregate(AggregatePlan aggregate, AggregatePlan.Algorithm algorithm, boolean isHorizon, boolean isPlanned) {
        final LogicalPlan source = isHorizon ? ((HorizonJoinPlan) aggregate.getChild()).getMaster() : PlanToFactoryRules.aggregateBase(aggregate);
        if (isHorizon ? !OperatorPlanning.hasParallelFunctions(aggregate) : !OperatorPlanning.isParallelGroupBy(aggregate, source)) {
            return false;
        }
        return algorithm == AggregatePlan.Algorithm.PARALLEL_STOLEN_FILTER ? PlanToFactoryRules.stolenFilter(source) != null
                : !isUnsatisfied(PhysicalProperties.supportsPageFrameCursor(source), isPlanned);
    }

    /**
     * Whether the sort reads its input back by row id, which requires an input with random access.
     */
    private static boolean isRowIdSort(@Nullable SortPlan.Algorithm algorithm) {
        return algorithm == SortPlan.Algorithm.LIGHT || algorithm == SortPlan.Algorithm.LIMITED
                || algorithm == SortPlan.Algorithm.PRESORTED_LIMITED || algorithm == SortPlan.Algorithm.LONG_TOP_K;
    }

    private static boolean isScanLimited(LogicalPlan plan, LimitPlan limit) {
        final ScanPlan scan = (ScanPlan) LogicalPlans.skipProjects(plan).inputAt(0);
        return scan.getLimitLo() == limit.getLo() && scan.getLimitHi() == limit.getHi();
    }

    /**
     * Whether the head of the shared source's set operation serves the leading branch of the domain's input the
     * domain records: the operation is the source's input under column projections, its head serves shared cursors,
     * and the branch, which leads the domain's set operation, lays out the head's column types.
     */
    private static boolean isSharedHeadServed(AggregatePlan aggregate, JoinInput source) {
        final SetOperationPlan operation = aggregate.getSharedOperation();
        final LogicalPlan branch = aggregate.getSharedHead();
        if (operation == null || operation != LogicalPlans.unwrapColumnProjections(source.getInput())
                || !(LogicalPlans.unwrapColumnProjections(aggregate.getChild()) instanceof SetOperationPlan)
                || branch != OperatorPlanning.leadingBranch(aggregate.getChild())) {
            return false;
        }
        final LogicalPlan head = OperatorPlanning.leadingBranch(operation);
        final OutputSchema output = branch.getOutput();
        final OutputSchema headOutput = head.getOutput();
        if (PhysicalProperties.supportsSharedCursors(head) != PhysicalProperties.Capability.YES || output.getColumnCount() != headOutput.getColumnCount()) {
            return false;
        }
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            if (output.getColumnType(i) != headOutput.getColumnType(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether the factory of the shared source serves the domain's input: it serves shared cursors and the pairs map
     * every input column, in input order, onto a source column of its type.
     */
    private static boolean isSharedSourceServed(AggregatePlan aggregate, JoinInput source) {
        final OutputSchema input = aggregate.getChild().getOutput();
        final OutputSchema sourceOutput = source.getSourceOutput();
        final IntList inputIds = aggregate.getSharedInputIds();
        final IntList sourceIds = aggregate.getSharedSourceIds();
        if (PhysicalProperties.supportsSharedCursors(source.getInput()) != PhysicalProperties.Capability.YES
                || inputIds.size() != input.getColumnCount() || sourceIds.size() != inputIds.size()) {
            return false;
        }
        for (int i = 0, n = input.getColumnCount(); i < n; i++) {
            final int index = sourceOutput.getColumnIndexById(sourceIds.getQuick(i));
            if (inputIds.getQuick(i) != input.getColumnId(i) || index < 0 || sourceOutput.getColumnType(index) != input.getColumnType(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether the join step reads its slave back by row id, which requires a slave with random access.
     */
    private static boolean isSlaveReadByRowId(JoinInput.Algorithm algorithm) {
        return algorithm == JoinInput.Algorithm.LIGHT_HASH || algorithm == JoinInput.Algorithm.TEMPORAL;
    }

    /**
     * Whether the ASOF join step steals the filter of its slave, whose factory under the filter serves time frames.
     */
    private static boolean isTimeFrameFilterStolen(JoinInput step, boolean isPlanned) {
        final FilterPlan filter = PlanToFactoryRules.temporalStolenFilter(step.getInput());
        return filter != null && !isUnsatisfied(PhysicalProperties.supportsLeafTimeFrameCursor(filter), isPlanned);
    }

    private static boolean isTopK(SortPlan.Algorithm algorithm) {
        return algorithm == SortPlan.Algorithm.LONG_TOP_K || algorithm == SortPlan.Algorithm.PARALLEL_TOP_K
                || algorithm == SortPlan.Algorithm.PARALLEL_FILTERED_TOP_K;
    }

    /**
     * Whether a derived capability fails an input requirement: it is absent, or, once every physical choice is
     * recorded, the plan cannot confirm it.
     */
    private static boolean isUnsatisfied(PhysicalProperties.Capability capability, boolean isPlanned) {
        return capability == PhysicalProperties.Capability.NO || isPlanned && capability == PhysicalProperties.Capability.UNKNOWN;
    }

    /**
     * Checks a HORIZON JOIN that reads its master by row id, which requires a master with random access.
     */
    private static String serialHorizon(HorizonJoinPlan horizon, boolean isPlanned) {
        return isUnsatisfied(PhysicalProperties.supportsRandomAccess(horizon.getMaster()), isPlanned) ? PlanVerifier.INPUT_RANDOM_ACCESS : null;
    }

    /**
     * Checks how a decorrelation domain reads its input: only a domain with a shared source records a read, which the
     * source's factory or the head of the source's set operation must serve.
     */
    private static String sharedRead(AggregatePlan aggregate, boolean isPlanned) {
        final JoinInput source = aggregate.getSharedSource();
        final AggregatePlan.SharedRead read = aggregate.getSharedRead();
        if (read == null) {
            return isPlanned && source != null ? PlanVerifier.CHOICE_UNPLANNED : null;
        }
        if (source == null) {
            return PlanVerifier.SHARED_READ;
        }
        final boolean isServed = switch (read) {
            case SOURCE -> isSharedSourceServed(aggregate, source);
            case SET_OPERATION_HEAD -> isSharedHeadServed(aggregate, source);
            case NONE -> true;
        };
        return isServed ? null : PlanVerifier.SHARED_READ;
    }

    static String aggregate(AggregatePlan aggregate, boolean isPlanned) {
        final String sharedRead = sharedRead(aggregate, isPlanned);
        if (sharedRead != null) {
            return sharedRead;
        }
        final AggregatePlan.Algorithm algorithm = aggregate.getAlgorithm();
        final PlanToFactoryRules.AggregateFactory factory = PlanToFactoryRules.aggregateFactory(aggregate);
        final boolean isHorizon = factory == PlanToFactoryRules.AggregateFactory.HORIZON;
        final boolean hasChoice = isHorizon || factory == PlanToFactoryRules.AggregateFactory.GROUP_BY;
        if (algorithm == null) {
            return isPlanned && hasChoice ? PlanVerifier.CHOICE_UNPLANNED : null;
        }
        final boolean isValid = hasChoice && switch (algorithm) {
            case VECTORISED -> !isHorizon && PlanToFactoryRules.hasVectorShape(aggregate);
            case PARALLEL, PARALLEL_STOLEN_FILTER -> isParallelAggregate(aggregate, algorithm, isHorizon, isPlanned);
            case SERIAL -> true;
        };
        if (!isValid) {
            return PlanVerifier.AGGREGATE_ALGORITHM;
        }
        return isHorizon && algorithm == AggregatePlan.Algorithm.SERIAL ? serialHorizon((HorizonJoinPlan) aggregate.getChild(), isPlanned) : null;
    }

    /**
     * Checks the slaves of a HORIZON JOIN, whose rows it matches through time-frame cursors once planned, and how it
     * reads its master: a fused aggregate records no choice on the join, a projection records the one the master
     * serves, see {@link OperatorPlanning}.
     */
    static String horizon(HorizonJoinPlan horizon, boolean isPlanned) {
        for (int i = 0, n = horizon.getSlaves().size(); isPlanned && i < n; i++) {
            if (isUnsatisfied(PhysicalProperties.supportsHorizonSlaveTimeFrameCursor(horizon.getSlaves().getQuick(i).getInput()), true)) {
                return PlanVerifier.HORIZON_SLAVE_TIME_FRAME;
            }
        }
        final HorizonJoinPlan.Algorithm algorithm = horizon.getAlgorithm();
        if (horizon.getConsumer() == HorizonJoinPlan.Consumer.AGGREGATE) {
            return algorithm == null ? null : PlanVerifier.HORIZON_ALGORITHM;
        }
        if (algorithm == null) {
            return isPlanned ? PlanVerifier.CHOICE_UNPLANNED : null;
        }
        final LogicalPlan master = horizon.getMaster();
        final boolean isValid = switch (algorithm) {
            case SERIAL -> true;
            case PARALLEL, PARALLEL_STOLEN_FILTER -> (PlanToFactoryRules.isHorizonWithinSlotCap(horizon)
                    || PhysicalProperties.supportsRandomAccess(master) != PhysicalProperties.Capability.YES)
                    && (algorithm == HorizonJoinPlan.Algorithm.PARALLEL
                    ? !isUnsatisfied(PhysicalProperties.supportsPageFrameCursor(master), isPlanned)
                    : PlanToFactoryRules.stolenFilter(master) != null);
        };
        if (!isValid) {
            return PlanVerifier.HORIZON_ALGORITHM;
        }
        return algorithm == HorizonJoinPlan.Algorithm.SERIAL ? serialHorizon(horizon, isPlanned) : null;
    }

    static String fill(FillPlan fill, boolean isPlanned) {
        final FillPlan.Algorithm algorithm = fill.getAlgorithm();
        if (algorithm == null) {
            return isPlanned ? PlanVerifier.CHOICE_UNPLANNED : null;
        }
        final int inputTimestampIndex = PhysicalProperties.timestampIndex(fill.getChild());
        return (algorithm == FillPlan.Algorithm.SAMPLE_BY_ROWS) != OperatorPlanning.isSampleByRowsFill(fill)
                || inputTimestampIndex != PhysicalProperties.UNKNOWN_TIMESTAMP
                && (algorithm == FillPlan.Algorithm.SORTED) == (inputTimestampIndex == fill.derivedTimestampIndex())
                ? PlanVerifier.FILL_ALGORITHM : null;
    }

    static String filter(FilterPlan filter, boolean isPlanned) {
        final boolean hasChoice = !PlanToFactoryRules.isFusedFilter(filter) && OperatorPlanning.isFiltering(filter.getPredicate());
        final FilterPlan.Algorithm algorithm = filter.getAlgorithm();
        if (algorithm == null) {
            return isPlanned && hasChoice ? PlanVerifier.CHOICE_UNPLANNED : null;
        }
        return !hasChoice || (algorithm == FilterPlan.Algorithm.WINDOW_KEEP_FLAG) != PlanToFactoryRules.isWindowKeepFlag(filter)
                ? PlanVerifier.FILTER_ALGORITHM : null;
    }

    /**
     * Checks the join step at ordered position {@code position}, -1 for a step of a join without an order.
     */
    static String joinStep(JoinPlan join, int position, JoinInput step, boolean isPlanned) {
        if (isPlanned && position > 0 && !step.isAlgorithmDecided()) {
            return PlanVerifier.CHOICE_UNPLANNED;
        }
        final JoinInput.Algorithm algorithm = step.getAlgorithm();
        if (algorithm == null) {
            return null;
        }
        final JoinKind joinType = step.getJoinType();
        final boolean isValid = switch (algorithm) {
            case UNNEST -> joinType == JoinKind.UNNEST;
            case TEMPORAL, FULL_FAT_TEMPORAL -> joinType == JoinKind.ASOF || joinType == JoinKind.LT;
            case TEMPORAL_TIME_FRAME ->
                    !isLinear(step) && (joinType == JoinKind.ASOF || joinType == JoinKind.LT && position > 0
                            && !PlanToFactoryRules.isKeyedTemporalJoin(step, join.getOrderedInputs().getQuick(position - 1).getOutput(),
                            PhysicalProperties.masterTimestampIndex(join, position), PhysicalProperties.timestampIndex(step.getInput())))
                            && !isUnsatisfied(PhysicalProperties.supportsTimeFrameCursor(step.getInput()), isPlanned);
            case TEMPORAL_STOLEN_FILTER ->
                    joinType == JoinKind.ASOF && !isLinear(step) && isTimeFrameFilterStolen(step, isPlanned);
            case SPLICE, FULL_FAT_SPLICE -> joinType == JoinKind.SPLICE;
            case MARKOUT -> joinType.isRelational() && step.getMarkoutTimestampColumnId() >= 0 && position > 0
                    && PhysicalProperties.masterScanDirection(join, position) == PhysicalProperties.ScanDirection.FORWARD;
            case NESTED_LOOP -> joinType.isRelational() && step.getKeys().size() == 0;
            case MATERIALIZED_NESTED_LOOP ->
                    join.isLeadingCrossJoinFixed() && joinType.isRelational() && joinType != JoinKind.RIGHT_OUTER && joinType != JoinKind.FULL_OUTER
                            && step.getKeys().size() == 0 && step.getInput() instanceof JoinPlan;
            case HASH, LIGHT_HASH -> joinType.isRelational() && step.getKeys().size() > 0;
        };
        if (!isValid) {
            return PlanVerifier.JOIN_ALGORITHM;
        }
        if (algorithm == JoinInput.Algorithm.FULL_FAT_TEMPORAL && PlanToFactoryRules.fullFatTemporalRejectedColumn(step) >= 0) {
            return PlanVerifier.JOIN_SLAVE_VALUE_TYPE;
        }
        final JoinInput.TimeFrameScan scan = step.getTimeFrameScan();
        final boolean hasScanChoice = algorithm == JoinInput.Algorithm.TEMPORAL_TIME_FRAME && joinType == JoinKind.ASOF && position > 0
                && PlanToFactoryRules.isKeyedTemporalJoin(step, join.getOrderedInputs().getQuick(position - 1).getOutput(),
                PhysicalProperties.masterTimestampIndex(join, position), PhysicalProperties.timestampIndex(step.getInput()));
        if (scan == null && isPlanned && hasScanChoice) {
            return PlanVerifier.CHOICE_UNPLANNED;
        }
        if (scan != null && (!hasScanChoice || !isHintedTimeFrameScan(scan, step))) {
            return PlanVerifier.JOIN_ALGORITHM;
        }
        return isSlaveReadByRowId(algorithm) && !PlanToFactoryRules.isSlaveEmptied(step)
                && isUnsatisfied(PhysicalProperties.supportsRandomAccess(step.getInput()), isPlanned)
                || isMasterReadByRowId(algorithm) && position > 0
                && isUnsatisfied(PhysicalProperties.masterSupportsRandomAccess(join, position), isPlanned)
                ? PlanVerifier.INPUT_RANDOM_ACCESS : null;
    }

    static String latestBy(LatestByPlan latest, boolean isPlanned) {
        final LatestByPlan.Algorithm algorithm = latest.getAlgorithm();
        final boolean isScanned = PlanToFactoryRules.latestByScan(latest) != null;
        if (algorithm != null && isScanned) {
            return PlanVerifier.LATEST_BY_ALGORITHM;
        }
        if (algorithm == null) {
            return isPlanned && !isScanned ? PlanVerifier.CHOICE_UNPLANNED : null;
        }
        return algorithm != LatestByPlan.Algorithm.MATERIALIZED
                && isUnsatisfied(PhysicalProperties.supportsRandomAccess(PlanToFactoryRules.latestByBase(latest)), isPlanned)
                ? PlanVerifier.INPUT_RANDOM_ACCESS : null;
    }

    static String limit(LimitPlan limit, boolean isPlanned) {
        final LimitPlan.Application application = limit.getApplication();
        if (application == null) {
            return isPlanned ? PlanVerifier.CHOICE_UNPLANNED : null;
        }
        final LogicalPlan limited = limit.getChild();
        final SortPlan sort = LogicalPlans.hasSortUnderStableProjects(limited) ? (SortPlan) LogicalPlans.skipProjects(limited) : null;
        final boolean isInput = OperatorPlanning.isInputLimit(sort != null ? sort.getChild() : limited);
        if ((application == LimitPlan.Application.INPUT) != isInput || isInput && !isScanLimited(sort != null ? sort.getChild() : limited, limit)
                || (application == LimitPlan.Application.MASTER_SUFFIX) != (PlanToFactoryRules.masterSuffixScan(limit) != null)) {
            return PlanVerifier.LIMIT_APPLICATION;
        }
        final SortPlan.Algorithm algorithm = sort != null ? sort.getAlgorithm() : null;
        if ((application == LimitPlan.Application.SORT) != isBounded(algorithm)) {
            return PlanVerifier.LIMIT_APPLICATION;
        }
        if (sort == null || application != LimitPlan.Application.SORT) {
            return null;
        }
        final boolean isTimestampOrdered = OperatorPlanning.isTimestampOrdered(sort);
        return (algorithm == SortPlan.Algorithm.PRESORTED_LIMITED) != isTimestampOrdered
                || isTopK(algorithm) && !OperatorPlanning.isTopKLimit(limit, isTimestampOrdered)
                ? PlanVerifier.LIMIT_APPLICATION : null;
    }

    static String sampleBy(SampleByPlan sample, boolean isPlanned) {
        final SampleByPlan.Algorithm algorithm = sample.getAlgorithm();
        if (algorithm == null) {
            return isPlanned ? PlanVerifier.CHOICE_UNPLANNED : null;
        }
        final boolean isValid = (algorithm == SampleByPlan.Algorithm.INTERPOLATE) == OperatorPlanning.isLinearFill(sample) && switch (algorithm) {
            case INTERPOLATE -> true;
            case FIRST_LAST_INDEX ->
                    OperatorPlanning.firstLastIndexScan(sample, PlanToFactoryRules.sampleByBase(sample)) != null;
            case FILL_NONE -> OperatorPlanning.isFillNone(sample);
            case FILL_VALUE -> !OperatorPlanning.isFillNone(sample);
        };
        return isValid ? null : PlanVerifier.SAMPLE_BY_ALGORITHM;
    }

    static String scan(ScanPlan scan, boolean isPlanned) {
        final ScanPlan.AccessPath accessPath = scan.getAccessPath();
        if (isPlanned && accessPath == null) {
            return PlanVerifier.ACCESS_PATH_UNPLANNED;
        }
        if (scan.getIndexRead() != ScanPlan.IndexRead.NONE && scan.getIndexColumnId() < 0
                || accessPath == null && (scan.getIndexColumnId() >= 0 || scan.getResidual() != null || scan.getIndexKeys().size() > 0)) {
            return PlanVerifier.ACCESS_PATH;
        }
        final boolean hasChoice = OperatorPlanning.hasResidualFilterChoice(scan);
        if (scan.getResidualAlgorithm() != null && !hasChoice) {
            return PlanVerifier.FILTER_ALGORITHM;
        }
        return isPlanned && hasChoice && scan.getResidualAlgorithm() == null ? PlanVerifier.CHOICE_UNPLANNED : null;
    }

    static String setOperation(SetOperationPlan operation, boolean isPlanned) {
        final SortPlan.Algorithm rightSort = operation.getRightBranchSort();
        if ((operation.isMerged() || rightSort != null) && operation.getOperation() != SetOperationKind.UNION_ALL
                || rightSort != null && rightSort != SortPlan.Algorithm.LIGHT && rightSort != SortPlan.Algorithm.MATERIALIZED) {
            return PlanVerifier.SET_OPERATION_MERGE;
        }
        return isRowIdSort(rightSort) && isUnsatisfied(PhysicalProperties.supportsRandomAccess(operation.getRight()), isPlanned)
                ? PlanVerifier.INPUT_RANDOM_ACCESS : null;
    }

    static String sort(SortPlan sort, boolean isPlanned) {
        final SortPlan.Algorithm algorithm = sort.getAlgorithm();
        if (algorithm == null) {
            return isPlanned ? PlanVerifier.CHOICE_UNPLANNED : null;
        }
        if (algorithm == SortPlan.Algorithm.TIMESTAMP_DECLARATION && !sort.isMarkoutHorizon()
                || isBounded(algorithm) && !sort.isLimited()
                || algorithm == SortPlan.Algorithm.PARALLEL_FILTERED_TOP_K && PlanToFactoryRules.stolenFilter(PlanToFactoryRules.parallelTopKSource(sort)) == null) {
            return PlanVerifier.SORT_ALGORITHM;
        }
        return isRowIdSort(algorithm) && isUnsatisfied(PhysicalProperties.supportsRandomAccess(sort.getChild()), isPlanned)
                ? PlanVerifier.INPUT_RANDOM_ACCESS : null;
    }

    static String window(WindowPlan window, boolean isPlanned) {
        final WindowPlan.Algorithm algorithm = window.getAlgorithm();
        if (algorithm == null) {
            return isPlanned ? PlanVerifier.CHOICE_UNPLANNED : null;
        }
        boolean isStreamed = true;
        for (int i = 0, n = window.getSpecs().size(); i < n; i++) {
            isStreamed &= OperatorPlanning.isStreamedWindow(window.getSpecs().getQuick(i), window.getFunctions().getQuick(i));
        }
        if ((algorithm == WindowPlan.Algorithm.STREAMING) != isStreamed
                || algorithm == WindowPlan.Algorithm.CACHED_LIGHT && !OperatorPlanning.isCachedLightShape(window)) {
            return PlanVerifier.WINDOW_ALGORITHM;
        }
        return algorithm == WindowPlan.Algorithm.CACHED_LIGHT && isUnsatisfied(PhysicalProperties.supportsRandomAccess(window.getChild()), isPlanned)
                ? PlanVerifier.INPUT_RANDOM_ACCESS : null;
    }

    static String windowJoinStep(WindowJoinPlan windowJoin, int index, WindowJoinStep step, boolean isPlanned) {
        final WindowJoinStep.Algorithm algorithm = step.getAlgorithm();
        if (algorithm == null) {
            return isPlanned && !step.isNullExtending() ? PlanVerifier.CHOICE_UNPLANNED : null;
        }
        final boolean isValid = !step.isNullExtending() && switch (algorithm) {
            case PARALLEL_STOLEN_FILTER ->
                    index == 0 && PlanToFactoryRules.stolenFilter(windowJoin.getMaster()) != null && OperatorPlanning.isParallelWindowJoinStep(step);
            case PARALLEL ->
                    isMasterPageFrameRead(windowJoin, index, isPlanned) && OperatorPlanning.isParallelWindowJoinStep(step);
            case SERIAL -> true;
        };
        return isValid ? null : PlanVerifier.WINDOW_JOIN_ALGORITHM;
    }
}
