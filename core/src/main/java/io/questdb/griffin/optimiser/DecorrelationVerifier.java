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
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ForwardingPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.PlanVisitor;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

/**
 * Checks decorrelation's output against the join steps that do not commute, which {@link #record} finds before
 * decorrelation runs: each recorded step stays in the plan and keeps at least as many distinct equalities between its
 * own columns and other ON conjuncts, and, when its ON condition compared a column with an outer column, an equality
 * with one of its carriers, so decorrelation moved none of its ON condition out of it; every carrier is a column of
 * its step or of a step before it, and none reads a column that a step matching per outer row null-extends.
 */
final class DecorrelationVerifier {
    private final IntList counts;
    private final OnFacts facts;
    private final ObjList<JoinInput> reachedSteps;
    private final ObjList<JoinInput> recordedSteps = new ObjList<>();
    private final PlanVisitor stepRecorder = this::recordSteps;
    private String pass;
    private LogicalPlan root;
    private final PlanVisitor carrierChecker = this::checkCarriers;

    /**
     * Borrows {@code reachedSteps}, {@code pairs} and {@code counts} as temporaries; the counts hold from {@link #record}
     * to {@link #verify}.
     */
    DecorrelationVerifier(ObjList<JoinInput> reachedSteps, IntList pairs, IntList counts) {
        this.reachedSteps = reachedSteps;
        this.facts = new OnFacts(pairs);
        this.counts = counts;
    }

    /**
     * True when a step at an ordered position up to {@code through} null-extends the input at {@code input} and
     * matches per outer row: it null-extends its master and reads a carrier, or it keys on a carrier of its prefix.
     */
    private static boolean isNulledPerOuterRow(JoinPlan join, int input, int through) {
        final ObjList<JoinInput> steps = LogicalPlans.orderedSteps(join);
        for (int i = 1; i <= through; i++) {
            if (LogicalPlans.isNullingStep(join, steps.getQuick(i), steps.getQuick(input)) && isOuterDependent(steps.getQuick(i))) {
                return true;
            }
        }
        return false;
    }

    private static boolean isOuterDependent(JoinInput step) {
        final IntList carriers = step.getCarrierColumnIds();
        if (carriers.size() == 0) {
            return false;
        }
        if (step.getJoinType().isMasterNulling()) {
            return true;
        }
        for (int i = 0, n = carriers.size(); i < n; i++) {
            if (step.getSourceOutput().getColumnIndexById(carriers.getQuick(i)) < 0) {
                return true;
            }
        }
        return false;
    }

    private static int orderedSource(JoinPlan join, ObjList<JoinInput> steps, int columnId) {
        final int source = LogicalPlans.findJoinColumnSource(join, columnId);
        return source < 0 ? -1 : steps.indexOf(join.getInputs().getQuick(source));
    }

    private int checkCarriers(LogicalPlan plan) {
        if (plan instanceof JoinPlan join) {
            reachedSteps.addAll(join.getInputs());
            final ObjList<JoinInput> steps = LogicalPlans.orderedSteps(join);
            for (int p = 0, n = steps.size(); p < n; p++) {
                final JoinInput step = steps.getQuick(p);
                final IntList carriers = step.getCarrierColumnIds();
                for (int i = 0, m = carriers.size(); i < m; i++) {
                    final int columnId = carriers.getQuick(i);
                    if (step.getSourceOutput().getColumnIndexById(columnId) > -1) {
                        resolveCarrier(step.getInput(), columnId);
                        continue;
                    }
                    final int source = orderedSource(join, steps, columnId);
                    if (source < 0 || source >= p) {
                        throw PlanVerifier.failure(PlanVerifier.DECORRELATION_CARRIER_UNRESOLVED, columnId, null, join, pass, root);
                    }
                    if (isNulledPerOuterRow(join, source, p - 1)) {
                        throw PlanVerifier.failure(PlanVerifier.DECORRELATION_CARRIER, columnId, null, join, pass, root);
                    }
                    resolveCarrier(steps.getQuick(source).getInput(), columnId);
                }
            }
        }
        return TreeWalk.CONTINUE;
    }

    private void clear() {
        counts.clear();
        reachedSteps.clear();
        recordedSteps.clear();
    }

    private int recordSteps(LogicalPlan plan) {
        if (plan instanceof JoinPlan join) {
            for (int i = 0, n = join.getInputs().size(); i < n; i++) {
                final JoinInput step = join.getInputs().getQuick(i);
                if (!step.isDependent() && step.getJoinType().isBarrier()) {
                    facts.of(step);
                    recordedSteps.add(step);
                    counts.add(facts.pairCount());
                    counts.add(facts.outerPairCount);
                    counts.add(facts.otherCount);
                }
            }
        }
        return TreeWalk.CONTINUE;
    }

    /**
     * Follows a carrier column down to the join input that defines it, through column projections, forwarding
     * nodes, grouping keys and window pass-through columns; a computed column, such as the CASE a FULL join's carrier
     * is, ends the walk.
     */
    private void resolveCarrier(LogicalPlan plan, int columnId) {
        while (plan != null) {
            switch (plan) {
                case ProjectPlan project -> {
                    final int index = project.getOutput().getColumnIndexById(columnId);
                    if (index < 0 || !(project.getExpressions().getQuick(index) instanceof ColumnExpression column)) {
                        return;
                    }
                    columnId = column.getColumnId();
                    plan = project.getChild();
                }
                case ForwardingPlan forwarding -> plan = forwarding.inputAt(0);
                case AggregatePlan aggregate -> {
                    final int index = aggregate.getOutput().getColumnIndexById(columnId);
                    if (index < 0 || index >= aggregate.getGroupingExpressions().size()
                            || !(aggregate.getGroupingExpressions().getQuick(index) instanceof ColumnExpression column)) {
                        return;
                    }
                    columnId = column.getColumnId();
                    plan = aggregate.getChild();
                }
                case WindowPlan window -> {
                    if (window.getChild().getOutput().getColumnIndexById(columnId) < 0) {
                        return;
                    }
                    plan = window.getChild();
                }
                case JoinPlan join -> {
                    final ObjList<JoinInput> steps = LogicalPlans.orderedSteps(join);
                    final int source = orderedSource(join, steps, columnId);
                    if (source < 0) {
                        return;
                    }
                    if (isNulledPerOuterRow(join, source, steps.size() - 1)) {
                        throw PlanVerifier.failure(PlanVerifier.DECORRELATION_CARRIER, columnId, null, join, pass, root);
                    }
                    plan = steps.getQuick(source).getInput();
                }
                default -> {
                    return;
                }
            }
        }
    }

    /**
     * Records the ON condition facts of every join step of the plan that does not commute.
     */
    void record(LogicalPlan root) {
        clear();
        root.walkTopDown(stepRecorder);
    }

    /**
     * Verifies decorrelation's output against the steps {@link #record} found.
     */
    void verify(LogicalPlan root, String pass) {
        this.root = root;
        this.pass = pass;
        reachedSteps.clear();
        try {
            root.walkTopDown(carrierChecker);
            for (int i = 0, n = recordedSteps.size(); i < n; i++) {
                final JoinInput step = recordedSteps.getQuick(i);
                if (reachedSteps.indexOf(step) < 0) {
                    throw PlanVerifier.failure(PlanVerifier.DECORRELATION_STEP_LOST, -1, null, root, pass, root);
                }
                facts.of(step);
                final int countBase = 3 * i;
                if (facts.pairCount() < counts.getQuick(countBase) || facts.otherCount < counts.getQuick(countBase + 2)
                        || counts.getQuick(countBase + 1) > 0 && !facts.hasCarrierPair(step)) {
                    throw PlanVerifier.failure(PlanVerifier.JOIN_ON_KEPT, -1, null, root, pass, root);
                }
            }
        } finally {
            clear();
            facts.pairs.clear();
            this.root = null;
            this.pass = null;
        }
    }

    private static final class OnFacts {
        private final IntList pairs;
        private int otherCount;
        private int outerPairCount;

        private OnFacts(IntList pairs) {
            this.pairs = pairs;
        }

        private static boolean isColumnReference(BoundExpression expression) {
            return expression instanceof ColumnExpression column && !column.isCast() || expression instanceof OuterColumnExpression;
        }

        private void addConjuncts(BoundExpression predicate) {
            switch (predicate) {
                case null -> {
                    return;
                }
                case FunctionExpression call when call.isAnd() -> {
                    addConjuncts(call.argumentAt(0));
                    addConjuncts(call.argumentAt(1));
                    return;
                }
                case FunctionExpression call when call.getArgumentCount() == 2 && Chars.equals(call.getName(), '=') && isColumnReference(call.argumentAt(0)) && isColumnReference(call.argumentAt(1)) -> {
                    final boolean isLeftOuter = call.argumentAt(0) instanceof OuterColumnExpression;
                    final boolean isRightOuter = call.argumentAt(1) instanceof OuterColumnExpression;
                    if (isLeftOuter != isRightOuter) {
                        outerPairCount++;
                    } else if (!isLeftOuter) {
                        addPair(((ColumnExpression) call.argumentAt(0)).getColumnId(), ((ColumnExpression) call.argumentAt(1)).getColumnId());
                    }
                    return;
                }
                default -> {
                }
            }
            otherCount++;
        }

        private void addPair(int left, int right) {
            final int lo = Math.min(left, right);
            final int hi = Math.max(left, right);
            for (int i = 0, n = pairs.size(); i < n; i += 2) {
                if (pairs.getQuick(i) == lo && pairs.getQuick(i + 1) == hi) {
                    return;
                }
            }
            pairs.add(lo);
            pairs.add(hi);
        }

        private boolean hasCarrierPair(JoinInput step) {
            final IntList carriers = step.getCarrierColumnIds();
            for (int i = 0, n = pairs.size(); i < n; i++) {
                if (carriers.contains(pairs.getQuick(i))) {
                    return true;
                }
            }
            return false;
        }

        private void of(JoinInput step) {
            pairs.clear();
            otherCount = 0;
            outerPairCount = 0;
            for (int i = 0, n = step.getMasterKeyColumnIds().size(); i < n; i++) {
                addPair(step.getMasterKeyColumnIds().getQuick(i), step.getSlaveKeyColumnIds().getQuick(i));
            }
            addConjuncts(step.getOnResidual());
            addConjuncts(step.getKeyFilter());
        }

        private int pairCount() {
            return pairs.size() / 2;
        }
    }
}
