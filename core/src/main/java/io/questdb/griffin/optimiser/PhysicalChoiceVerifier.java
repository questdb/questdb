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

/**
 * Checks the physical choices operator planning records on plan nodes against the rules the planner and the generator
 * decide them by. Each check returns the {@link PlanVerifier} invariant the node violates, or null; under access path
 * planning's output, {@code isPlanned}, every choice the generator builds the node by is recorded.
 */
final class PhysicalChoiceVerifier {

    private PhysicalChoiceVerifier() {
    }

    private static boolean isBounded(SortPlan.Algorithm algorithm) {
        return algorithm == SortPlan.Algorithm.LIMITED || algorithm == SortPlan.Algorithm.PRESORTED_LIMITED || isTopK(algorithm);
    }

    private static boolean isLinear(JoinInput step) {
        return (step.getHints() & JoinInput.HINT_ASOF_LINEAR) != 0;
    }

    private static boolean isScanLimited(LogicalPlan plan, LimitPlan limit) {
        final ScanPlan scan = (ScanPlan) LogicalPlans.skipProjects(plan).inputAt(0);
        return scan.getLimitLo() == limit.getLo() && scan.getLimitHi() == limit.getHi();
    }

    private static boolean isTopK(SortPlan.Algorithm algorithm) {
        return algorithm == SortPlan.Algorithm.LONG_TOP_K || algorithm == SortPlan.Algorithm.PARALLEL_TOP_K
                || algorithm == SortPlan.Algorithm.PARALLEL_FILTERED_TOP_K;
    }

    static String aggregate(AggregatePlan aggregate, boolean isPlanned) {
        final AggregatePlan.Algorithm algorithm = aggregate.getAlgorithm();
        final PlanToFactoryRules.AggregateFactory factory = PlanToFactoryRules.aggregateFactory(aggregate);
        final boolean isHorizon = factory == PlanToFactoryRules.AggregateFactory.HORIZON;
        final boolean hasChoice = isHorizon || factory == PlanToFactoryRules.AggregateFactory.GROUP_BY;
        if (algorithm == null) {
            return isPlanned && hasChoice ? PlanVerifier.CHOICE_UNPLANNED : null;
        }
        final boolean isValid = hasChoice && switch (algorithm) {
            case VECTORISED -> !isHorizon && PlanToFactoryRules.hasVectorShape(aggregate);
            case PARALLEL_STOLEN_FILTER ->
                    PlanToFactoryRules.stolenFilter(isHorizon ? ((HorizonJoinPlan) aggregate.getChild()).getMaster()
                            : PlanToFactoryRules.aggregateBase(aggregate)) != null;
            case PARALLEL, SERIAL -> true;
        };
        return isValid ? null : PlanVerifier.AGGREGATE_ALGORITHM;
    }

    static String fill(FillPlan fill, boolean isPlanned) {
        final FillPlan.Algorithm algorithm = fill.getAlgorithm();
        if (algorithm == null) {
            return isPlanned ? PlanVerifier.CHOICE_UNPLANNED : null;
        }
        return (algorithm == FillPlan.Algorithm.SAMPLE_BY_ROWS) != OperatorPlanning.isSampleByRowsFill(fill) ? PlanVerifier.FILL_ALGORITHM : null;
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
                            PhysicalProperties.masterTimestampIndex(join, position), PhysicalProperties.timestampIndex(step.getInput())));
            case TEMPORAL_STOLEN_FILTER ->
                    joinType == JoinKind.ASOF && !isLinear(step) && PlanToFactoryRules.temporalStolenFilter(step.getInput()) != null;
            case SPLICE, FULL_FAT_SPLICE -> joinType == JoinKind.SPLICE;
            case MARKOUT -> joinType.isRelational() && step.getMarkoutTimestampColumnId() >= 0;
            case NESTED_LOOP -> joinType.isRelational() && step.getMasterKeyColumnIds().size() == 0;
            case HASH, LIGHT_HASH -> joinType.isRelational() && step.getMasterKeyColumnIds().size() > 0;
        };
        return isValid ? null : PlanVerifier.JOIN_ALGORITHM;
    }

    static String latestBy(LatestByPlan latest, boolean isPlanned) {
        final boolean isScanned = PlanToFactoryRules.latestByScan(latest) != null;
        if (latest.getAlgorithm() != null && isScanned) {
            return PlanVerifier.LATEST_BY_ALGORITHM;
        }
        return isPlanned && !isScanned && latest.getAlgorithm() == null ? PlanVerifier.CHOICE_UNPLANNED : null;
    }

    static String limit(LimitPlan limit, boolean isPlanned) {
        final LimitPlan.Application application = limit.getApplication();
        if (application == null) {
            return isPlanned ? PlanVerifier.CHOICE_UNPLANNED : null;
        }
        final LogicalPlan limited = limit.getChild();
        final SortPlan sort = LogicalPlans.hasSortUnderStableProjects(limited) ? (SortPlan) LogicalPlans.skipProjects(limited) : null;
        final boolean isInput = OperatorPlanning.isInputLimit(sort != null ? sort.getChild() : limited);
        if ((application == LimitPlan.Application.INPUT) != isInput || isInput && !isScanLimited(sort != null ? sort.getChild() : limited, limit)) {
            return PlanVerifier.LIMIT_APPLICATION;
        }
        final SortPlan.Algorithm algorithm = sort != null ? sort.getAlgorithm() : null;
        if ((application == LimitPlan.Application.SORT) != isBounded(algorithm)) {
            return PlanVerifier.LIMIT_APPLICATION;
        }
        if (application != LimitPlan.Application.SORT) {
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
            case FIRST_LAST_INDEX -> sample.getFillMode() == SampleByPlan.FILL_NONE;
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

    static String setOperation(SetOperationPlan operation) {
        final SortPlan.Algorithm rightSort = operation.getRightBranchSort();
        return (operation.isMerged() || rightSort != null) && operation.getOperation() != SetOperationKind.UNION_ALL
                || rightSort != null && rightSort != SortPlan.Algorithm.LIGHT && rightSort != SortPlan.Algorithm.MATERIALIZED
                ? PlanVerifier.SET_OPERATION_MERGE : null;
    }

    static String sort(SortPlan sort, boolean isPlanned) {
        final SortPlan.Algorithm algorithm = sort.getAlgorithm();
        if (algorithm == null) {
            return isPlanned ? PlanVerifier.CHOICE_UNPLANNED : null;
        }
        return algorithm == SortPlan.Algorithm.TIMESTAMP_DECLARATION && !sort.isMarkoutHorizon()
                || isBounded(algorithm) && !sort.isLimited()
                || algorithm == SortPlan.Algorithm.PARALLEL_FILTERED_TOP_K && PlanToFactoryRules.stolenFilter(PlanToFactoryRules.parallelTopKSource(sort)) == null
                ? PlanVerifier.SORT_ALGORITHM : null;
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
        return (algorithm == WindowPlan.Algorithm.STREAMING) != isStreamed ? PlanVerifier.WINDOW_ALGORITHM : null;
    }

    static String windowJoinStep(WindowJoinPlan windowJoin, int index, WindowJoinStep step, boolean isPlanned) {
        final WindowJoinStep.Algorithm algorithm = step.getAlgorithm();
        if (algorithm == null) {
            return isPlanned && !step.isNullExtending() ? PlanVerifier.CHOICE_UNPLANNED : null;
        }
        return step.isNullExtending() || algorithm == WindowJoinStep.Algorithm.PARALLEL_STOLEN_FILTER
                && (index > 0 || PlanToFactoryRules.stolenFilter(windowJoin.getMaster()) == null)
                ? PlanVerifier.WINDOW_JOIN_ALGORITHM : null;
    }
}
