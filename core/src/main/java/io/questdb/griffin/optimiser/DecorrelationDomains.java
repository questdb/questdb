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

import io.questdb.griffin.BoundExpressionRewriter.ConjunctTest;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.PlanNodePools;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.UnnestSpec;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

/**
 * Builds decorrelation domains: the distinct values of outer columns, read from a copy of the master input or
 * prefix and shared with the master where the generator can re-read it. Crosses them into the inputs of block
 * sources. Records the steps decorrelation rewrote, whose columns a domain then reads from the master prefix.
 */
final class DecorrelationDomains implements Mutable {
    private final OptimiserContext context;
    private final DecorrelationContext ctx;
    private final ObjList<JoinInput> decorrelatedSteps;
    private final IntList domainOuterIds = new IntList();
    private final OuterColumnMapping mapping;
    private final PlanNodePools planNodes;
    private JoinInput domainStep;
    private final ConjunctTest nonDomainConjuncts = this::keepsNonDomainConjunct;

    /**
     * {@code decorrelatedSteps} is the optimiser's temporary step list, which the owner empties before decorrelation starts.
     */
    DecorrelationDomains(
            DecorrelationContext ctx,
            OuterColumnMapping mapping,
            OptimiserContext context,
            PlanNodePools planNodes,
            ObjList<JoinInput> decorrelatedSteps
    ) {
        this.ctx = ctx;
        this.mapping = mapping;
        this.context = context;
        this.planNodes = planNodes;
        this.decorrelatedSteps = decorrelatedSteps;
    }

    @Override
    public void clear() {
        domainOuterIds.clear();
        domainStep = null;
    }

    private boolean keepsNonDomainConjunct(BoundExpression conjunct) throws SqlException {
        if (!ctx.readsAnyOuter(conjunct, domainOuterIds)) {
            return true;
        }
        final BoundExpression moved = domainStep.getPostJoinFilter();
        domainStep.setPostJoinFilter(moved == null ? conjunct : context.getRewriter().combineConjunction(moved, conjunct, conjunct.getPosition()));
        return false;
    }

    /**
     * The master prefix of the first {@code inputCount} inputs as a join of the original inputs, for copying.
     */
    private JoinPlan prefix(int inputCount, int position) {
        final JoinPlan master = ctx.master();
        final JoinPlan prefix = planNodes.joins.next().of(position);
        for (int i = 0; i < inputCount; i++) {
            prefix.getInputs().add(master.getInputs().getQuick(i));
        }
        for (int i = 0, n = master.getOrderedInputs().size(); i < n; i++) {
            final JoinInput input = master.getOrderedInputs().getQuick(i);
            if (master.getInputs().indexOf(input) < inputCount) {
                prefix.getOrderedInputs().add(input);
            }
        }
        final OutputSchema output = prefix.getOutput();
        for (int i = 0; i < inputCount; i++) {
            final JoinInput input = master.getInputs().getQuick(i);
            final OutputSchema source = input.getSourceOutput();
            for (int c = 0, n = source.getColumnCount(); c < n; c++) {
                output.add(source.getColumnId(c), source.getColumnName(c), source.getColumnType(c), source.getMetadata(c),
                        source.isVisible(c), input.getBindingAlias());
            }
        }
        output.setTimestampColumnId(LogicalPlans.joinTimestampColumnId(prefix, prefix.getOrderedInputs().size()));
        return prefix;
    }

    private void shareMasterSource(AggregatePlan domain, JoinInput source) {
        final OutputSchema sourceOutput = source.getSourceOutput();
        final OutputSchema output = domain.getChild().getOutput();
        if (source.getUnnest() != null || sourceOutput.getColumnCount() != output.getColumnCount()) {
            return;
        }
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            if (sourceOutput.getColumnType(i) != output.getColumnType(i)
                    || !Chars.equalsIgnoreCase(sourceOutput.getColumnName(i), output.getColumnName(i))) {
                domain.getSharedInputIds().clear();
                domain.getSharedSourceIds().clear();
                return;
            }
            domain.getSharedInputIds().add(output.getColumnId(i));
            domain.getSharedSourceIds().add(sourceOutput.getColumnId(i));
        }
        domain.setSharedSource(source);
        planNodes.recordSharedSource();
    }

    void addDecorrelatedStep(JoinInput step) {
        decorrelatedSteps.add(step);
    }

    /**
     * A domain: the distinct values of the outer columns {@link #outerIds} holds, read from a copy of the master input
     * that defines them, or of the master prefix when they come from several inputs or from a decorrelated step, or
     * when a step of the prefix can NULL-extend them. The prefix then reaches the last such step. Maps each outer
     * column to its domain column.
     */
    AggregatePlan buildDomain(int position) {
        int firstInput = Integer.MAX_VALUE;
        int lastInput = -1;
        for (int i = 0, n = domainOuterIds.size(); i < n; i++) {
            final int input = ctx.masterInput(domainOuterIds.getQuick(i));
            firstInput = Math.min(firstInput, input);
            lastInput = Math.max(lastInput, input);
        }
        final JoinPlan master = ctx.master();
        int prefixCount = lastInput + 1;
        boolean isNulled = false;
        for (int i = 0, n = domainOuterIds.size(); i < n; i++) {
            final int input = ctx.masterInput(domainOuterIds.getQuick(i));
            for (int step = input, stepIndex = ctx.stepIndex(); step < stepIndex; step++) {
                if (LogicalPlans.isNullingStep(master, master.getInputs().getQuick(step), master.getInputs().getQuick(input))) {
                    isNulled = true;
                    prefixCount = Math.max(prefixCount, step + 1);
                }
            }
        }
        final JoinInput last = master.getInputs().getQuick(lastInput);
        final boolean isPrefix = isNulled || firstInput != lastInput || last.getInput() == null || isDecorrelated(last);
        final LogicalPlan original = isPrefix ? prefix(prefixCount, position) : last.getInput();
        final LogicalPlan source = ctx.copy(original);
        final AggregatePlan domain = planNodes.aggregates.next().of(source, position);
        domain.setExplicitGrouping(true);
        for (int i = 0, n = domainOuterIds.size(); i < n; i++) {
            final int outerId = domainOuterIds.getQuick(i);
            final int index = original.getOutput().getColumnIndexById(ctx.masterColumn(outerId));
            final int sourceId = source.getOutput().getColumnId(index);
            final int type = source.getOutput().getColumnType(index);
            domain.getGroupingExpressions().add(planNodes.columns.next().of(sourceId, type, position));
            final int columnId = context.newColumnId();
            domain.getOutput().add(columnId, ctx.outerRefName(outerId), type, false);
            mapping.push(outerId, columnId);
        }
        if (!isPrefix) {
            shareMasterSource(domain, last);
        }
        return domain;
    }

    void coverMappedMaster(int lo, int hi) {
        domainOuterIds.clear();
        final IntList outerIds = ctx.masterOuterIds();
        for (int i = 0, n = outerIds.size(); i < n; i++) {
            final int outerId = outerIds.getQuick(i);
            if (mapping.mappedColumn(outerId, lo, hi) > -1) {
                domainOuterIds.add(outerId);
            }
        }
    }

    /**
     * Makes the next domain cover each outer column of the mapping above {@code base}, in mapping order.
     */
    void coverMapping(int base) {
        domainOuterIds.clear();
        for (int i = base, n = mapping.mark(); i < n; i++) {
            domainOuterIds.add(mapping.outerIdAt(i));
        }
    }

    void coverOuter(int outerId) {
        if (!domainOuterIds.contains(outerId)) {
            domainOuterIds.add(outerId);
        }
    }

    JoinPlan crossDomain(LogicalPlan source, AggregatePlan domain, int position) {
        final JoinPlan join = planNodes.joins.next().of(position);
        join.addInput(planNodes.joinInputs.next().of(source, JoinKind.CROSS, null, position));
        join.addInput(planNodes.joinInputs.next().of(domain, JoinKind.CROSS, ctx.domainAlias(), position));
        join.getOutput().copyFrom(source.getOutput());
        join.getOutput().addMissingColumnsFrom(domain.getOutput());
        join.getOutput().setTimestampColumnId(LogicalPlans.joinTimestampColumnId(join, 2));
        return join;
    }

    /**
     * Crosses the domain into an input of the join and adds, hidden, the columns the join's output lacks. A standalone
     * UNNEST after the input then keeps its master's columns: the domain's and the one-row base's own, hidden.
     */
    JoinPlan crossInto(JoinPlan join, JoinInput input, AggregatePlan domain, int position) {
        final JoinPlan crossed = crossDomain(input.getInput(), domain, position);
        input.setInput(crossed);
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        for (int i = ordered.indexOf(input) + 1, n = ordered.size(); i < n; i++) {
            final UnnestSpec unnest = ordered.getQuick(i).getUnnest();
            if (unnest != null && unnest.isStandalone()) {
                unnest.keepMasterColumns();
            }
        }
        join.addMissingInputColumns();
        return crossed;
    }

    /**
     * Crosses a set-operation branch with a domain of the outer columns the other branch maps and this one
     * does not.
     */
    LogicalPlan crossMissing(LogicalPlan branch, int otherLo, int otherHi, int ownLo, int ownHi, int position) {
        domainOuterIds.clear();
        for (int i = otherLo; i < otherHi; i++) {
            final int outerId = mapping.outerIdAt(i);
            if (mapping.mappedColumn(outerId, ownLo, ownHi) < 0 && !domainOuterIds.contains(outerId)) {
                domainOuterIds.add(outerId);
            }
        }
        return domainOuterIds.size() == 0 ? branch : crossDomain(branch, buildDomain(position), position);
    }

    boolean hasDecorrelatedSteps() {
        return decorrelatedSteps.size() > 0;
    }

    /**
     * Joins the domain to the join as a CROSS step at {@code orderedIndex} of the execution order, last when the
     * index is the step count, and returns the step.
     */
    JoinInput insertDomainStep(JoinPlan join, AggregatePlan domain, int orderedIndex, int position) {
        final JoinInput step = planNodes.joinInputs.next().of(domain, JoinKind.CROSS, ctx.domainAlias(), position);
        join.getInputs().add(step);
        join.getOrderedInputs().insert(orderedIndex, 1, step);
        join.addMissingInputColumns();
        return step;
    }

    boolean isDecorrelated(JoinInput step) {
        return decorrelatedSteps.indexOf(step) > -1;
    }

    /**
     * Returns the conjuncts of the predicate that read none of {@link #outerIds}, moving the others to the domain
     * step, which joins after every input.
     */
    BoundExpression moveDomainConjuncts(BoundExpression predicate, JoinInput domainStep) throws SqlException {
        this.domainStep = domainStep;
        return context.getRewriter().retainConjuncts(predicate, nonDomainConjuncts);
    }

    /**
     * The outer columns the next domain covers, which the caller only reads.
     */
    IntList outerIds() {
        return domainOuterIds;
    }

    void resetOuterIds() {
        domainOuterIds.clear();
    }
}
