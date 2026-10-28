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
import io.questdb.griffin.plan.logical.DomainReadPlan;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.UnnestSpec;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

/**
 * Builds decorrelation domains: the distinct values of outer columns, read from a copy of the master input or
 * prefix and shared with the master where the generator can re-read it. A step builds each domain once; its further
 * consumers read it, see {@link DomainReadPlan}, until the step retires it, see {@link #retireProducers}. Crosses them
 * into the inputs of block sources and joins them to set-operation branches, see {@link #joinMissing}. Records the steps
 * decorrelation rewrote, whose columns a domain then reads from the master prefix.
 */
final class DecorrelationDomains implements Mutable {
    private final OptimiserContext context;
    private final DecorrelationContext ctx;
    private final ObjList<JoinInput> decorrelatedSteps;
    private final IntList domainOuterIds = new IntList();
    private final OuterColumnMapping mapping;
    private final IntList operationOuterIds = new IntList();
    private final PlanNodePools planNodes;
    private final ObjList<Producer> producers = new ObjList<>();
    private JoinInput domainStep;
    private int operationOuterBase;
    private int producerCount;
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
        operationOuterIds.clear();
        operationOuterBase = 0;
        domainStep = null;
        beginStep();
    }

    /**
     * The first domain the step built and did not retire that copies the master input or prefix {@code source}, see
     * {@link #sourceKey}, and covers the outer columns {@link #domainOuterIds} holds, in any order; null when none does.
     */
    private Producer findProducer(int source) {
        for (int i = 0; i < producerCount; i++) {
            final Producer producer = producers.getQuick(i);
            if (producer.source == source && producer.covers(domainOuterIds)) {
                return producer;
            }
        }
        return null;
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

    /**
     * A new domain of the outer columns {@link #outerIds} holds over a copy of the master input or prefix
     * {@code source}, see {@link #buildProducer}, which later domains of the step may read.
     */
    private AggregatePlan produce(int source, int position) {
        final JoinInput last = source < 0 ? null : ctx.master().getInputs().getQuick(source);
        final LogicalPlan original = last == null ? prefix(-source, position) : last.getInput();
        final LogicalPlan copy = ctx.copy(original);
        final AggregatePlan domain = planNodes.aggregates.next().of(copy, position);
        domain.setExplicitGrouping(true);
        for (int i = 0, n = domainOuterIds.size(); i < n; i++) {
            final int outerId = domainOuterIds.getQuick(i);
            final int index = original.getOutput().getColumnIndexById(ctx.masterColumn(outerId));
            final int sourceId = copy.getOutput().getColumnId(index);
            final int type = copy.getOutput().getColumnType(index);
            domain.getGroupingExpressions().add(planNodes.columns.next().of(sourceId, type, position));
            final int columnId = context.newColumnId();
            domain.getOutput().add(columnId, ctx.outerRefName(outerId), type, false);
            mapping.push(outerId, columnId);
        }
        if (last != null) {
            shareMasterSource(domain, last);
        }
        Producer producer = producers.getQuiet(producerCount);
        if (producer == null) {
            producer = new Producer();
            producers.extendAndSet(producerCount, producer);
        }
        producerCount++;
        producer.of(domain, source, domainOuterIds);
        return domain;
    }

    /**
     * A read of the producer's rows that maps each outer column {@link #outerIds} holds to a column of its own, paired
     * with the producer's column for the same outer column.
     */
    private DomainReadPlan read(Producer producer, int position) {
        final DomainReadPlan read = planNodes.domainReads.next().of(producer.domain, position);
        final OutputSchema producerOutput = producer.domain.getOutput();
        for (int i = 0, n = domainOuterIds.size(); i < n; i++) {
            final int outerId = domainOuterIds.getQuick(i);
            final int index = producer.outerIds.indexOf(outerId, 0, producer.outerIds.size());
            final int columnId = context.newColumnId();
            read.getOutput().add(columnId, ctx.outerRefName(outerId), producerOutput.getColumnType(index), false);
            read.getProducerColumnIds().add(producerOutput.getColumnId(index));
            mapping.push(outerId, columnId);
        }
        producer.domain.addReader();
        planNodes.recordSharedSource();
        return read;
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

    /**
     * The master input a domain of the outer columns {@link #outerIds} holds copies when one input defines them all,
     * else minus the input count of the master prefix it copies: when they come from several inputs or from a
     * decorrelated step, or when a step of the prefix can NULL-extend them. The prefix then reaches the last such step.
     */
    private int sourceKey() {
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
        return isNulled || firstInput != lastInput || last.getInput() == null || isDecorrelated(last) ? -prefixCount : lastInput;
    }

    void addDecorrelatedStep(JoinInput step) {
        decorrelatedSteps.add(step);
    }

    /**
     * Records the outer columns of the master that the set operation reads, which the domains of its branches cover,
     * see {@link #coverOperation} and {@link #joinMissing}, until {@link #endOperation}. Returns the mark of the
     * enclosing set operation for {@link #endOperation}.
     */
    int beginOperation(SetOperationPlan operation) {
        final int enclosingBase = operationOuterBase;
        operationOuterBase = operationOuterIds.size();
        ctx.collectMasterOuterIds(operation, operationOuterIds, operationOuterBase);
        return enclosingBase;
    }

    /**
     * Records the start of the decorrelation of the next step, whose domains read no domain of the step before.
     */
    void beginStep() {
        producerCount = 0;
    }

    /**
     * The domain of the outer columns {@link #outerIds} holds: a read of the domain the step built for them from the
     * same master input or prefix, see {@link #buildProducer}, else a new one. Maps each outer column to its column.
     */
    LogicalPlan buildDomain(int position) {
        final int source = sourceKey();
        final Producer producer = findProducer(source);
        return producer != null ? read(producer, position) : produce(source, position);
    }

    /**
     * A new domain: the distinct values of the outer columns {@link #outerIds} holds, read from a copy of the master
     * input that defines them, or of the master prefix when they come from several inputs or from a decorrelated step,
     * or when a step of the prefix can NULL-extend them, see {@link #sourceKey}. Maps each outer column to its domain
     * column. Later domains of the step over the same input and outer columns read it, see {@link #buildDomain}.
     */
    AggregatePlan buildProducer(int position) {
        return produce(sourceKey(), position);
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

    /**
     * Makes the next domain also cover each outer column the set operation reads, see {@link #beginOperation}.
     */
    void coverOperation() {
        for (int i = operationOuterBase, n = operationOuterIds.size(); i < n; i++) {
            coverOuter(operationOuterIds.getQuick(i));
        }
    }

    void coverOuter(int outerId) {
        if (!domainOuterIds.contains(outerId)) {
            domainOuterIds.add(outerId);
        }
    }

    JoinPlan crossDomain(LogicalPlan source, LogicalPlan domain, int position) {
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
    JoinPlan crossInto(JoinPlan join, JoinInput input, LogicalPlan domain, int position) {
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
     * Ends the set operation {@link #beginOperation} began, restoring the enclosing one from its mark.
     */
    void endOperation(int enclosingBase) {
        operationOuterIds.setPos(operationOuterBase);
        operationOuterBase = enclosingBase;
    }

    boolean hasDecorrelatedSteps() {
        return decorrelatedSteps.size() > 0;
    }

    /**
     * Joins the domain to the join as a CROSS step at {@code orderedIndex} of the execution order, last when the
     * index is the step count, and returns the step.
     */
    JoinInput insertDomainStep(JoinPlan join, LogicalPlan domain, int orderedIndex, int position) {
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
     * Joins a set-operation branch, when the mapping above {@code ownBase} lacks an outer column the operation reads,
     * see {@link #beginOperation}, to the domain of those columns, keyed on the columns the branch maps: each row of
     * the branch pairs only with the outer values that share its own. Maps each outer column to its domain column.
     */
    LogicalPlan joinMissing(LogicalPlan branch, int ownBase, int position) {
        final int ownEnd = mapping.mark();
        domainOuterIds.clear();
        for (int i = operationOuterBase, n = operationOuterIds.size(); i < n; i++) {
            final int outerId = operationOuterIds.getQuick(i);
            if (mapping.mappedColumn(outerId, ownBase, ownEnd) < 0) {
                domainOuterIds.add(outerId);
            }
        }
        if (domainOuterIds.size() == 0) {
            return branch;
        }
        final int keyBase = domainOuterIds.size();
        for (int i = ownBase; i < ownEnd; i++) {
            coverOuter(mapping.outerIdAt(i));
        }
        final LogicalPlan domain = buildDomain(position);
        final JoinPlan join = crossDomain(branch, domain, position);
        final JoinInput step = join.getInputs().getQuick(1);
        final OutputSchema branchOutput = branch.getOutput();
        final OutputSchema domainOutput = domain.getOutput();
        for (int i = keyBase, n = domainOuterIds.size(); i < n; i++) {
            final int outerId = domainOuterIds.getQuick(i);
            final int columnId = mapping.mappedColumn(outerId, ownBase, ownEnd);
            final int domainId = mapping.mappedColumn(outerId, ownEnd, mapping.mark());
            step.addKey(columnId, domainId, branchOutput.getColumnName(branchOutput.getColumnIndexById(columnId)),
                    domainOutput.getColumnName(domainOutput.getColumnIndexById(domainId)), position);
        }
        return join;
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

    /**
     * The number of domains the step built and did not retire, a mark for {@link #retireProducers}.
     */
    int producerMark() {
        return producerCount;
    }

    void resetOuterIds() {
        domainOuterIds.clear();
    }

    /**
     * Retires the domains the step built since {@code mark}, see {@link #producerMark}: their consumer releases them
     * before the step's later consumers read, so those build their own, see {@link #buildDomain}.
     */
    void retireProducers(int mark) {
        producerCount = mark;
    }

    /**
     * A domain a step built, with the master input or prefix it copies, see {@link #sourceKey}, and the outer columns
     * it covers, in the order of its columns.
     */
    private static final class Producer {
        private final IntList outerIds = new IntList();
        private AggregatePlan domain;
        private int source;

        /**
         * True when the domain covers the outer columns {@code ids} holds, as many and in any order.
         */
        boolean covers(IntList ids) {
            if (ids.size() != outerIds.size()) {
                return false;
            }
            for (int i = 0, n = ids.size(); i < n; i++) {
                if (outerIds.indexOf(ids.getQuick(i), 0, n) < 0 || ids.indexOf(outerIds.getQuick(i), 0, n) < 0) {
                    return false;
                }
            }
            return true;
        }

        void of(AggregatePlan domain, int source, IntList outerIds) {
            this.domain = domain;
            this.source = source;
            this.outerIds.clear();
            this.outerIds.addAll(outerIds);
        }
    }
}
