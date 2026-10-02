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

package io.questdb.griffin;

import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.FunctionSourcePlan;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.HorizonJoinSlave;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.UnnestSpec;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowJoinStep;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

/**
 * Copies bound plan subtrees and renames the columns plan nodes read. {@link #copy} gives every column the
 * subtree defines a fresh id, while references to columns defined outside it keep theirs; {@link #remap}
 * renames, in place, the columns and outer columns one node reads and forwards. Plan nodes come from the
 * statement's plan-node pools and expressions from the level's {@link FunctionBinder}, so copies live as long
 * as the original; a changed expression is a fresh description, unchanged ones are shared. A decorrelation
 * domain that shares a source inside a copied subtree shares that source's copy.
 */
final class LogicalPlanCopier implements Mutable {
    private final IntIntHashMap columnIds = new IntIntHashMap();
    private final ObjList<JoinInput> copiedInputs = new ObjList<>();
    private final ObjList<JoinInput> originalInputs = new ObjList<>();
    private final BindContext planNodes;
    private final ObjList<AggregatePlan> sharingCopies = new ObjList<>();
    private FunctionBinder functionBinder;
    private TableFunctionSources functionSources;
    private int nextColumnId;

    LogicalPlanCopier(BindContext planNodes) {
        this.planNodes = planNodes;
    }

    @Override
    public void clear() {
        columnIds.clear();
        copiedInputs.clear();
        originalInputs.clear();
        sharingCopies.clear();
        functionBinder = null;
        functionSources = null;
        nextColumnId = 0;
    }

    private static int remapColumnId(int columnId, IntIntHashMap columnIds) {
        final int remapped = columnId < 0 ? -1 : columnIds.get(columnId);
        return remapped < 0 ? columnId : remapped;
    }

    private static void remapColumnIds(IntList ids, IntIntHashMap columnIds) {
        for (int i = 0, n = ids.size(); i < n; i++) {
            ids.setQuick(i, remapColumnId(ids.getQuick(i), columnIds));
        }
    }

    private static void remapOutput(OutputSchema output, IntIntHashMap columnIds) {
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            output.setColumnId(i, remapColumnId(output.getColumnId(i), columnIds));
        }
    }

    private void assignColumnIds(OutputSchema output) {
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            final int columnId = output.getColumnId(i);
            if (columnIds.keyIndex(columnId) > -1) {
                columnIds.put(columnId, nextColumnId++);
            }
        }
    }

    private void assignColumnIds(LogicalPlan plan) {
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            assignColumnIds(plan.inputAt(i));
        }
        if (plan instanceof JoinPlan join) {
            for (int i = 0, n = join.getInputs().size(); i < n; i++) {
                final UnnestSpec unnest = join.getInputs().getQuick(i).getUnnest();
                if (unnest != null) {
                    assignColumnIds(unnest.getOutput());
                }
            }
        }
        assignColumnIds(plan.getOutput());
    }

    private void cloneAggregate(AggregatePlan aggregate, AggregatePlan clone) {
        clone.getAggregates().addAll(aggregate.getAggregates());
        clone.getGroupingExpressions().addAll(aggregate.getGroupingExpressions());
        clone.getSharedInputIds().addAll(aggregate.getSharedInputIds());
        clone.getSharedSourceIds().addAll(aggregate.getSharedSourceIds());
        clone.setConstantLeadingGroupBy(aggregate.hasConstantLeadingGroupBy());
        clone.setDirectTableInput(aggregate.hasDirectTableInput());
        clone.setExplicitGrouping(aggregate.hasExplicitGrouping());
        clone.setKeySpellingKept(aggregate.hasKeySpellingKept());
        clone.setSampleByBucket(aggregate.hasSampleByBucket());
        if (aggregate.getSharedSource() != null) {
            clone.setSharedSource(aggregate.getSharedSource());
            sharingCopies.add(clone);
        }
    }

    private FillPlan cloneFill(FillPlan fill) {
        final FillPlan clone = planNodes.fills.next().of(fill.getInput(), fill.getPosition());
        clone.getModes().addAll(fill.getModes());
        clone.getPositions().addAll(fill.getPositions());
        clone.getSourceColumnIds().addAll(fill.getSourceColumnIds());
        clone.getSourcePositions().addAll(fill.getSourcePositions());
        clone.getTargetColumnIds().addAll(fill.getTargetColumnIds());
        clone.getTokens().addAll(fill.getTokens());
        clone.getValues().addAll(fill.getValues());
        clone.setFrom(fill.getFrom());
        clone.setTo(fill.getTo());
        clone.setOffset(fill.getOffset());
        clone.setTimezone(fill.getTimezone());
        clone.setPeriod(fill.getPeriodToken(), fill.getPeriodPosition());
        clone.setTimestampColumnId(fill.getTimestampColumnId());
        return clone;
    }

    private HorizonJoinPlan cloneHorizonJoin(HorizonJoinPlan horizon) {
        final HorizonJoinPlan clone = planNodes.horizonJoinPlans.next().of(horizon.getMaster(), horizon.getMasterAlias(),
                horizon.getHorizonAlias(), horizon.getHorizonPosition(), horizon.getMode(), horizon.getPosition());
        clone.getOffsets().addAll(horizon.getOffsets());
        clone.getOffsetPositions().addAll(horizon.getOffsetPositions());
        for (int i = 0, n = horizon.getSlaves().size(); i < n; i++) {
            final HorizonJoinSlave slave = horizon.getSlaves().getQuick(i);
            final HorizonJoinSlave slaveClone = planNodes.horizonJoinSlaves.next().of(slave.getInput(), slave.getAlias(), slave.getPosition());
            slaveClone.getKeyPositions().addAll(slave.getKeyPositions());
            slaveClone.getMasterKeyColumnIds().addAll(slave.getMasterKeyColumnIds());
            slaveClone.getSlaveKeyColumnIds().addAll(slave.getSlaveKeyColumnIds());
            clone.getSlaves().add(slaveClone);
        }
        return clone;
    }

    private JoinPlan cloneJoin(JoinPlan join) {
        final JoinPlan clone = planNodes.joins.next().of(join.getPosition());
        for (int i = 0, n = join.getInputs().size(); i < n; i++) {
            clone.getInputs().add(cloneJoinInput(join.getInputs().getQuick(i)));
        }
        for (int i = 0, n = join.getOrderedInputs().size(); i < n; i++) {
            clone.getOrderedInputs().add(copiedInputs.getQuick(originalInputs.indexOf(join.getOrderedInputs().getQuick(i))));
        }
        clone.getFilterConjuncts().addAll(join.getFilterConjuncts());
        clone.getFilterConjunctOrigins().addAll(join.getFilterConjunctOrigins());
        clone.setExplicitTimestamp(join.hasExplicitTimestamp());
        return clone;
    }

    private JoinInput cloneJoinInput(JoinInput input) {
        final JoinInput clone = planNodes.joinInputs.next();
        if (input.getUnnest() != null) {
            clone.ofUnnest(cloneUnnest(input.getUnnest()), input.getBindingAlias(), input.getPosition());
        } else {
            clone.of(input.getInput(), input.getJoinType(), input.getBindingAlias(), input.getPosition());
        }
        clone.getOutput().copyFrom(input.getOutput());
        clone.getKeyPositions().addAll(input.getKeyPositions());
        clone.getMasterKeyColumnIds().addAll(input.getMasterKeyColumnIds());
        clone.getMasterKeyNames().addAll(input.getMasterKeyNames());
        clone.getSlaveKeyColumnIds().addAll(input.getSlaveKeyColumnIds());
        clone.getSlaveKeyNames().addAll(input.getSlaveKeyNames());
        clone.setHints(input.getHints());
        clone.setSubquery(input.isSubquery());
        clone.setKeyFilter(input.getKeyFilter());
        clone.setOnResidual(input.getOnResidual());
        clone.setPostJoinFilter(input.getPostJoinFilter());
        clone.setTolerance(input.getToleranceToken(), input.getTolerancePosition());
        clone.setUnsupportedOnExpression(input.getUnsupportedOnExpression(), input.getUnsupportedOnPosition());
        originalInputs.add(input);
        copiedInputs.add(clone);
        return clone;
    }

    private LogicalPlan cloneNode(LogicalPlan plan) {
        final LogicalPlan clone = switch (plan) {
            case ScanPlan scan -> cloneScan(scan);
            case FunctionSourcePlan source -> functionSources.copy(source);
            case FilterPlan filter -> planNodes.filters.next().of(filter.getInput(), filter.getPredicate(), filter.getPosition());
            case ProjectPlan project -> cloneProject(project);
            case SampleByPlan sampleBy -> cloneSampleBy(sampleBy);
            case AggregatePlan aggregate -> {
                final AggregatePlan aggregateClone = planNodes.aggregates.next().of(aggregate.getInput(), aggregate.getPosition());
                cloneAggregate(aggregate, aggregateClone);
                yield aggregateClone;
            }
            case FillPlan fill -> cloneFill(fill);
            case WindowPlan window -> cloneWindow(window);
            case DistinctPlan distinct -> planNodes.distincts.next().of(distinct.getInput(), distinct.getPosition());
            case LatestByPlan latest -> {
                final LatestByPlan latestClone = planNodes.latestByPlans.next().of(latest.getInput(), latest.getTimestampColumnId(), latest.getPosition());
                latestClone.getKeyColumnIds().addAll(latest.getKeyColumnIds());
                latestClone.setTimestampOrderInherited(latest.isTimestampOrderInherited());
                yield latestClone;
            }
            case JoinPlan join -> cloneJoin(join);
            case WindowJoinPlan windowJoin -> cloneWindowJoin(windowJoin);
            case HorizonJoinPlan horizon -> cloneHorizonJoin(horizon);
            case SetOperationPlan operation -> {
                final SetOperationPlan operationClone = planNodes.setOperations.next().of(operation.getLeft(), operation.getRight(),
                        operation.getOperation(), operation.getPosition(), operation.getRightPosition(), operation.isSymbolRestorationRequired());
                operationClone.getSymbolColumns().addAll(operation.getSymbolColumns());
                yield operationClone;
            }
            case SortPlan sort -> cloneSort(sort);
            case LimitPlan limit -> planNodes.limits.next().of(limit.getInput(), limit.getLo(), limit.getHi(), limit.getPosition());
            default -> throw new UnsupportedOperationException("plan copy: " + plan.getType());
        };
        clone.getOutput().copyFrom(plan.getOutput());
        return clone;
    }

    private ProjectPlan cloneProject(ProjectPlan project) {
        final ProjectPlan clone = planNodes.projects.next().of(project.getInput(), project.getPosition());
        clone.getExpressions().addAll(project.getExpressions());
        if (project.hasTimestampDeclaration()) {
            clone.markTimestampDeclaration();
        }
        return clone;
    }

    private SampleByPlan cloneSampleBy(SampleByPlan sampleBy) {
        final SampleByPlan clone = planNodes.sampleByPlans.next().of(sampleBy.getInput(), sampleBy.getPosition());
        cloneAggregate(sampleBy, clone);
        clone.getAggregateSql().addAll(sampleBy.getAggregateSql());
        clone.getFillPositions().addAll(sampleBy.getFillPositions());
        clone.getFillTokens().addAll(sampleBy.getFillTokens());
        clone.getFillValues().addAll(sampleBy.getFillValues());
        clone.setFillMode(sampleBy.getFillMode());
        clone.setFrom(sampleBy.getFrom());
        clone.setTo(sampleBy.getTo());
        clone.setOffset(sampleBy.getOffset());
        clone.setTimezone(sampleBy.getTimezone());
        clone.setPeriod(sampleBy.getPeriodToken(), sampleBy.getPeriod(), sampleBy.getPeriodPosition(),
                sampleBy.getPeriodUnit(), sampleBy.getPeriodUnitPosition());
        clone.setTimestampColumnId(sampleBy.getTimestampColumnId());
        clone.setJoinInput(sampleBy.isJoinInput());
        clone.setTimestampRequired(sampleBy.isTimestampRequired());
        return clone;
    }

    private ScanPlan cloneScan(ScanPlan scan) {
        final ScanPlan clone = planNodes.scans.next().of(scan.getTableToken(), scan.getMetadataVersion(), scan.getPosition(), scan.isUpdate());
        clone.getIndexedColumnIds().addAll(scan.getIndexedColumnIds());
        clone.getSourceColumnIndexes().addAll(scan.getSourceColumnIndexes());
        clone.setHints(scan.getHints());
        clone.setRandomAccess(scan.isRandomAccess());
        clone.setNativeTimestamp(scan.getNativeTimestampColumnId(), scan.getNativeTimestampType());
        clone.setView(scan.getViewName(), scan.getViewPosition());
        return clone;
    }

    private SortPlan cloneSort(SortPlan sort) {
        final SortPlan clone = planNodes.sorts.next().of(sort.getInput(), sort.getPosition());
        clone.getColumnIds().addAll(sort.getColumnIds());
        clone.getDirections().addAll(sort.getDirections());
        if (sort.hasAliasedKey()) {
            clone.markAliasedKey();
        }
        if (!sort.isLimited()) {
            clone.markUnlimited();
        }
        return clone;
    }

    private UnnestSpec cloneUnnest(UnnestSpec unnest) {
        final UnnestSpec clone = planNodes.unnestSpecs.next().of(unnest.isStandalone(), unnest.hasOrdinality());
        clone.getColumnAliases().addAll(unnest.getColumnAliases());
        clone.getExpressions().addAll(unnest.getExpressions());
        clone.getJsonColumnNames().addAll(unnest.getJsonColumnNames());
        clone.getJsonColumnTypes().addAll(unnest.getJsonColumnTypes());
        clone.getOutput().copyFrom(unnest.getOutput());
        return clone;
    }

    private WindowPlan cloneWindow(WindowPlan window) {
        final WindowPlan clone = planNodes.windowPlans.next().of(window.getInput(), window.getPosition());
        clone.getFunctions().addAll(window.getFunctions());
        for (int i = 0, n = window.getSpecs().size(); i < n; i++) {
            final WindowSpec spec = window.getSpecs().getQuick(i);
            final WindowSpec specClone = planNodes.windowSpecs.next().ofFrame(spec);
            specClone.getPartitionBy().addAll(spec.getPartitionBy());
            specClone.getOrderByColumnIds().addAll(spec.getOrderByColumnIds());
            specClone.getOrderByDirections().addAll(spec.getOrderByDirections());
            specClone.getOrderByNames().addAll(spec.getOrderByNames());
            specClone.getOrderByPositions().addAll(spec.getOrderByPositions());
            clone.getSpecs().add(specClone);
        }
        clone.getFunctionColumnIds().addAll(window.getFunctionColumnIds());
        if (window.isSelectOrdered()) {
            clone.markSelectOrdered();
        }
        return clone;
    }

    private WindowJoinPlan cloneWindowJoin(WindowJoinPlan windowJoin) {
        final WindowJoinPlan clone = planNodes.windowJoinPlans.next().of(windowJoin.getMaster(), windowJoin.getPosition());
        for (int i = 0, n = windowJoin.getSteps().size(); i < n; i++) {
            final WindowJoinStep step = windowJoin.getSteps().getQuick(i);
            final WindowJoinStep stepClone = planNodes.windowJoinSteps.next().of(step.getSlave(), step.getMasterAlias(), step.getSlaveAlias(),
                    step.isIncludePrevailing(), step.getPosition());
            stepClone.getAggregateColumnIds().addAll(step.getAggregateColumnIds());
            stepClone.getAggregates().addAll(step.getAggregates());
            stepClone.getMasterScope().copyFrom(step.getMasterScope());
            stepClone.getScope().copyFrom(step.getScope());
            stepClone.setFilter(step.getFilter());
            stepClone.setLo(step.getLo(), step.getLoExpression(), step.getLoSign(), step.getLoTimeUnit(), step.getLoPosition());
            stepClone.setHi(step.getHi(), step.getHiExpression(), step.getHiSign(), step.getHiTimeUnit(), step.getHiPosition());
            stepClone.setTableSource(step.isTableSource());
            clone.getSteps().add(stepClone);
        }
        clone.setEmpty(windowJoin.isEmpty());
        return clone;
    }

    private LogicalPlan copyTree(LogicalPlan plan) {
        final LogicalPlan copy = cloneNode(plan);
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            copy.replaceInput(i, copyTree(plan.inputAt(i)));
        }
        remap(copy, columnIds);
        return copy;
    }

    private BoundExpression remap(BoundExpression expression, IntIntHashMap columnIds) {
        return expression == null ? null : functionBinder.remapColumns(expression, columnIds);
    }

    private void remapAggregate(AggregatePlan aggregate, IntIntHashMap columnIds) {
        final ObjList<FunctionExpression> aggregates = aggregate.getAggregates();
        for (int i = 0, n = aggregates.size(); i < n; i++) {
            aggregates.setQuick(i, functionBinder.remapColumns(aggregates.getQuick(i), columnIds));
        }
        remapExpressions(aggregate.getGroupingExpressions(), columnIds);
        remapColumnIds(aggregate.getSharedInputIds(), columnIds);
        remapColumnIds(aggregate.getSharedSourceIds(), columnIds);
        if (aggregate instanceof SampleByPlan sampleBy) {
            sampleBy.setFrom(remap(sampleBy.getFrom(), columnIds));
            sampleBy.setTo(remap(sampleBy.getTo(), columnIds));
            sampleBy.setOffset(remap(sampleBy.getOffset(), columnIds));
            sampleBy.setTimezone(remap(sampleBy.getTimezone(), columnIds));
            sampleBy.setTimestampColumnId(remapColumnId(sampleBy.getTimestampColumnId(), columnIds));
        }
    }

    private void remapExpressions(ObjList<BoundExpression> expressions, IntIntHashMap columnIds) {
        for (int i = 0, n = expressions.size(); i < n; i++) {
            expressions.setQuick(i, functionBinder.remapColumns(expressions.getQuick(i), columnIds));
        }
    }

    private void remapFill(FillPlan fill, IntIntHashMap columnIds) {
        remapColumnIds(fill.getSourceColumnIds(), columnIds);
        remapColumnIds(fill.getTargetColumnIds(), columnIds);
        remapExpressions(fill.getValues(), columnIds);
        fill.setFrom(remap(fill.getFrom(), columnIds));
        fill.setTo(remap(fill.getTo(), columnIds));
        fill.setOffset(remap(fill.getOffset(), columnIds));
        fill.setTimezone(remap(fill.getTimezone(), columnIds));
        fill.setTimestampColumnId(remapColumnId(fill.getTimestampColumnId(), columnIds));
    }

    private void remapJoin(JoinPlan join, IntIntHashMap columnIds) {
        for (int i = 0, n = join.getInputs().size(); i < n; i++) {
            final JoinInput input = join.getInputs().getQuick(i);
            remapOutput(input.getOutput(), columnIds);
            remapColumnIds(input.getMasterKeyColumnIds(), columnIds);
            remapColumnIds(input.getSlaveKeyColumnIds(), columnIds);
            input.setKeyFilter(remap(input.getKeyFilter(), columnIds));
            input.setOnResidual(remap(input.getOnResidual(), columnIds));
            input.setPostJoinFilter(remap(input.getPostJoinFilter(), columnIds));
            final UnnestSpec unnest = input.getUnnest();
            if (unnest != null) {
                remapExpressions(unnest.getExpressions(), columnIds);
                remapOutput(unnest.getOutput(), columnIds);
            }
        }
        remapExpressions(join.getFilterConjuncts(), columnIds);
    }

    private void remapWindow(WindowPlan window, IntIntHashMap columnIds) {
        final ObjList<FunctionExpression> functions = window.getFunctions();
        for (int i = 0, n = functions.size(); i < n; i++) {
            functions.setQuick(i, functionBinder.remapColumns(functions.getQuick(i), columnIds));
            final WindowSpec spec = window.getSpecs().getQuick(i);
            remapExpressions(spec.getPartitionBy(), columnIds);
            remapColumnIds(spec.getOrderByColumnIds(), columnIds);
        }
        remapColumnIds(window.getFunctionColumnIds(), columnIds);
    }

    private void remapWindowJoin(WindowJoinPlan windowJoin, IntIntHashMap columnIds) {
        for (int i = 0, n = windowJoin.getSteps().size(); i < n; i++) {
            final WindowJoinStep step = windowJoin.getSteps().getQuick(i);
            remapColumnIds(step.getAggregateColumnIds(), columnIds);
            final ObjList<FunctionExpression> aggregates = step.getAggregates();
            for (int k = 0, m = aggregates.size(); k < m; k++) {
                aggregates.setQuick(k, functionBinder.remapColumns(aggregates.getQuick(k), columnIds));
            }
            remapOutput(step.getMasterScope(), columnIds);
            remapOutput(step.getScope(), columnIds);
            step.setFilter(remap(step.getFilter(), columnIds));
            step.setLo(step.getLo(), remap(step.getLoExpression(), columnIds), step.getLoSign(), step.getLoTimeUnit(), step.getLoPosition());
            step.setHi(step.getHi(), remap(step.getHiExpression(), columnIds), step.getHiSign(), step.getHiTimeUnit(), step.getHiPosition());
        }
    }

    private void shareCopiedSources() {
        for (int i = 0, n = sharingCopies.size(); i < n; i++) {
            final AggregatePlan copy = sharingCopies.getQuick(i);
            final int index = originalInputs.indexOf(copy.getSharedSource());
            if (index > -1) {
                copy.setSharedSource(copiedInputs.getQuick(index));
            }
        }
    }

    /**
     * Copies the subtree, numbering the columns it defines from the first id {@link #of} allows.
     */
    LogicalPlan copy(LogicalPlan plan) {
        columnIds.clear();
        copiedInputs.clear();
        originalInputs.clear();
        sharingCopies.clear();
        assignColumnIds(plan);
        final LogicalPlan copy = copyTree(plan);
        shareCopiedSources();
        return copy;
    }

    /**
     * The first column id no copy uses.
     */
    int getNextColumnId() {
        return nextColumnId;
    }

    /**
     * Copies plans of the level that {@code functionBinder} and {@code functionSources} bound, numbering
     * new columns from {@code nextColumnId}, the first id that level does not use.
     */
    LogicalPlanCopier of(FunctionBinder functionBinder, TableFunctionSources functionSources, int nextColumnId) {
        this.functionBinder = functionBinder;
        this.functionSources = functionSources;
        this.nextColumnId = nextColumnId;
        return this;
    }

    /**
     * Renames, in the node itself, every column and outer column it reads or forwards that the map holds;
     * the inputs are not visited. Columns the node defines keep their ids unless the map holds them.
     */
    void remap(LogicalPlan plan, IntIntHashMap columnIds) {
        switch (plan) {
            case ScanPlan scan -> {
                remapColumnIds(scan.getIndexedColumnIds(), columnIds);
                scan.setNativeTimestamp(remapColumnId(scan.getNativeTimestampColumnId(), columnIds), scan.getNativeTimestampType());
            }
            case FilterPlan filter -> filter.of(filter.getInput(), remap(filter.getPredicate(), columnIds), filter.getPosition());
            case ProjectPlan project -> remapExpressions(project.getExpressions(), columnIds);
            case AggregatePlan aggregate -> remapAggregate(aggregate, columnIds);
            case FillPlan fill -> remapFill(fill, columnIds);
            case WindowPlan window -> remapWindow(window, columnIds);
            case LatestByPlan latest -> {
                remapColumnIds(latest.getKeyColumnIds(), columnIds);
                latest.of(latest.getInput(), remapColumnId(latest.getTimestampColumnId(), columnIds), latest.getPosition());
            }
            case JoinPlan join -> remapJoin(join, columnIds);
            case WindowJoinPlan windowJoin -> remapWindowJoin(windowJoin, columnIds);
            case HorizonJoinPlan horizon -> {
                for (int i = 0, n = horizon.getSlaves().size(); i < n; i++) {
                    remapColumnIds(horizon.getSlaves().getQuick(i).getMasterKeyColumnIds(), columnIds);
                    remapColumnIds(horizon.getSlaves().getQuick(i).getSlaveKeyColumnIds(), columnIds);
                }
            }
            case SortPlan sort -> remapColumnIds(sort.getColumnIds(), columnIds);
            case LimitPlan limit -> limit.of(limit.getInput(), remap(limit.getLo(), columnIds), remap(limit.getHi(), columnIds), limit.getPosition());
            default -> {
            }
        }
        remapOutput(plan.getOutput(), columnIds);
    }
}
