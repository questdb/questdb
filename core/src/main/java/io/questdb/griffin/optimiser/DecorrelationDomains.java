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

import io.questdb.griffin.CharacterStoreEntry;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

import static io.questdb.griffin.optimiser.DecorrelationContext.OUTER_REF_PREFIX;
import static io.questdb.griffin.optimiser.DecorrelationContext.appendMissingColumns;
import static io.questdb.griffin.optimiser.DecorrelationContext.pairIndex;

/**
 * Builds decorrelation domains: the distinct values of outer columns, read from a copy of the master input or
 * prefix and shared with the master where the generator can re-read it.
 */
final class DecorrelationDomains implements Mutable {
    final ObjList<JoinInput> decorrelatedSteps;
    final IntList domainEqualities = new IntList();
    final IntList domainOuterIds = new IntList();
    private final DecorrelationContext ctx;
    int domainSequence;

    /**
     * {@code decorrelatedSteps} is the optimiser's temporary step list, which the owner empties before decorrelation starts.
     */
    DecorrelationDomains(DecorrelationContext ctx, ObjList<JoinInput> decorrelatedSteps) {
        this.ctx = ctx;
        this.decorrelatedSteps = decorrelatedSteps;
    }

    @Override
    public void clear() {
        domainEqualities.clear();
        domainOuterIds.clear();
        domainSequence = 0;
    }

    private static void shareMasterSource(AggregatePlan domain, JoinInput source) {
        final OutputSchema sourceOutput = source.getSourceOutput();
        final OutputSchema output = domain.getInput().getOutput();
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
    }

    /**
     * The master prefix of the first {@code inputCount} inputs as a join of the original inputs, for copying.
     */
    private JoinPlan prefix(int inputCount, int position) {
        final JoinPlan prefix = ctx.planNodes.joins.next().of(position);
        for (int i = 0; i < inputCount; i++) {
            prefix.getInputs().add(ctx.master.getInputs().getQuick(i));
        }
        for (int i = 0, n = ctx.master.getOrderedInputs().size(); i < n; i++) {
            final JoinInput input = ctx.master.getOrderedInputs().getQuick(i);
            if (ctx.master.getInputs().indexOf(input) < inputCount) {
                prefix.getOrderedInputs().add(input);
            }
        }
        final OutputSchema output = prefix.getOutput();
        for (int i = 0; i < inputCount; i++) {
            final JoinInput input = ctx.master.getInputs().getQuick(i);
            final OutputSchema source = input.getSourceOutput();
            for (int c = 0, n = source.getColumnCount(); c < n; c++) {
                output.add(source.getColumnId(c), source.getColumnName(c), source.getColumnType(c), source.getMetadata(c),
                        source.isVisible(c), input.getBindingAlias());
            }
        }
        output.setTimestampIndex(output.getColumnIndexById(ctx.master.getOutput().getTimestampColumnId()));
        return prefix;
    }

    /**
     * A domain: the distinct values of the outer columns {@link #domainOuterIds} holds, read from a copy of
     * the master input that defines them, or of the master prefix when they come from several inputs or from a
     * decorrelated step, or when a step of the prefix can NULL-extend them. The prefix then reaches the last such
     * step. Maps each outer column to its domain column.
     */
    AggregatePlan buildDomain(int position) {
        int firstInput = Integer.MAX_VALUE;
        int lastInput = -1;
        for (int i = 0, n = domainOuterIds.size(); i < n; i++) {
            final int input = ctx.masterInput(domainOuterIds.getQuick(i));
            firstInput = Math.min(firstInput, input);
            lastInput = Math.max(lastInput, input);
        }
        int prefixCount = lastInput + 1;
        boolean isNulled = false;
        for (int i = 0, n = domainOuterIds.size(); i < n; i++) {
            final int input = ctx.masterInput(domainOuterIds.getQuick(i));
            for (int step = input; step < ctx.masterLimit; step++) {
                if (LogicalPlans.isNullingStep(ctx.master, ctx.master.getInputs().getQuick(step), ctx.master.getInputs().getQuick(input))) {
                    isNulled = true;
                    prefixCount = Math.max(prefixCount, step + 1);
                }
            }
        }
        final JoinInput last = ctx.master.getInputs().getQuick(lastInput);
        final boolean isPrefix = isNulled || firstInput != lastInput || last.getInput() == null || decorrelatedSteps.indexOf(last) > -1;
        final LogicalPlan original = isPrefix ? prefix(prefixCount, position) : last.getInput();
        final LogicalPlan source = ctx.copier.copy(original);
        final AggregatePlan domain = ctx.planNodes.aggregates.next().of(source, position);
        domain.setExplicitGrouping(true);
        for (int i = 0, n = domainOuterIds.size(); i < n; i++) {
            final int outerId = domainOuterIds.getQuick(i);
            final int index = original.getOutput().getColumnIndexById(ctx.masterColumn(outerId));
            final int sourceId = source.getOutput().getColumnId(index);
            final int type = source.getOutput().getColumnType(index);
            domain.getGroupingExpressions().add(ctx.planNodes.columns.next().of(sourceId, type, position));
            final int columnId = ctx.context.newColumnId();
            domain.getOutput().add(columnId, ctx.outerRefName(outerId), type, false);
            ctx.addMapping(outerId, columnId);
        }
        if (!isPrefix) {
            shareMasterSource(domain, last);
        }
        return domain;
    }

    /**
     * Collects the WHERE equalities between a source column and an outer column the domain will satisfy; they
     * become keys of the join with the domain.
     */
    void collectDomainEqualities(BoundExpression predicate, OutputSchema source) {
        if (!(predicate instanceof FunctionExpression call)) {
            return;
        }
        if (call.isAnd()) {
            collectDomainEqualities(call.argumentAt(0), source);
            collectDomainEqualities(call.argumentAt(1), source);
            return;
        }
        if (call.getArgumentCount() != 2 || !"=".equals(call.getName())) {
            return;
        }
        final BoundExpression left = call.argumentAt(0);
        final BoundExpression right = call.argumentAt(1);
        final ColumnExpression column = left instanceof ColumnExpression c ? c : right instanceof ColumnExpression c ? c : null;
        final OuterColumnExpression outer = left instanceof OuterColumnExpression o ? o : right instanceof OuterColumnExpression o ? o : null;
        if (column != null && outer != null && domainOuterIds.contains(outer.getColumnId()) && pairIndex(domainEqualities, outer.getColumnId()) < 0
                && source.getColumnIndexById(column.getColumnId()) > -1 && column.getDataType() == outer.getDataType()) {
            domainEqualities.add(outer.getColumnId());
            domainEqualities.add(column.getColumnId());
        }
    }

    JoinPlan crossDomain(LogicalPlan source, AggregatePlan domain, int position) {
        final JoinPlan join = ctx.planNodes.joins.next().of(position);
        join.getInputs().add(ctx.planNodes.joinInputs.next().of(source, JoinKind.CROSS, null, position));
        join.getInputs().add(ctx.planNodes.joinInputs.next().of(domain, JoinKind.CROSS, domainAlias(), position));
        join.getOrderedInputs().addAll(join.getInputs());
        join.getOutput().copyFrom(source.getOutput());
        appendMissingColumns(join.getOutput(), domain.getOutput());
        join.getOutput().setTimestampIndex(source.getOutput().getTimestampIndex());
        return join;
    }

    /**
     * Crosses a set-operation branch with a domain of the outer columns the other branch maps and this one
     * does not.
     */
    LogicalPlan crossMissing(LogicalPlan branch, int otherLo, int otherHi, int ownLo, int ownHi, int position) {
        domainOuterIds.clear();
        for (int i = otherLo; i < otherHi; i++) {
            final int outerId = ctx.mappedOuterIds.getQuick(i);
            if (ctx.mappedColumn(outerId, ownLo, ownHi) < 0 && !domainOuterIds.contains(outerId)) {
                domainOuterIds.add(outerId);
            }
        }
        return domainOuterIds.size() == 0 ? branch : crossDomain(branch, buildDomain(position), position);
    }

    CharSequence domainAlias() {
        final CharacterStoreEntry alias = ctx.characterStore.newEntry();
        alias.put(OUTER_REF_PREFIX).put(ctx.outerRefSequence);
        if (domainSequence++ > 0) {
            alias.put('_').put(domainSequence - 1);
        }
        return alias.toImmutable();
    }

    /**
     * Returns the conjuncts of the predicate that read none of {@link #domainOuterIds}, moving the others to the
     * domain step, which joins after every input.
     */
    BoundExpression moveDomainConjuncts(BoundExpression predicate, JoinInput domainStep) throws SqlException {
        if (predicate == null) {
            return null;
        }
        if (predicate instanceof FunctionExpression call && call.isAnd()) {
            final BoundExpression left = moveDomainConjuncts(call.argumentAt(0), domainStep);
            final BoundExpression right = moveDomainConjuncts(call.argumentAt(1), domainStep);
            if (left == null) {
                return right;
            }
            if (right == null) {
                return left;
            }
            return left == call.argumentAt(0) && right == call.argumentAt(1) ? call : ctx.context.getRewriter().replaceConjunction(call, left, right);
        }
        final int columnBase = ctx.tmpColumnIds.size();
        ctx.outerColumnReads.collect(predicate, ctx.tmpColumnIds);
        boolean isDomain = false;
        for (int i = columnBase, n = ctx.tmpColumnIds.size(); i < n && !isDomain; i++) {
            isDomain = domainOuterIds.contains(ctx.tmpColumnIds.getQuick(i));
        }
        ctx.tmpColumnIds.setPos(columnBase);
        if (!isDomain) {
            return predicate;
        }
        final BoundExpression moved = domainStep.getPostJoinFilter();
        domainStep.setPostJoinFilter(moved == null ? predicate : ctx.context.getRewriter().combineConjunction(moved, predicate, predicate.getPosition()));
        return null;
    }
}
