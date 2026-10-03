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

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.FunctionSourcePlan;
import io.questdb.griffin.plan.logical.GroupingPlan;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowJoinStep;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

import static io.questdb.griffin.CorrelationKeys.hasOuterCondition;
import static io.questdb.griffin.DecorrelationContext.*;
import static io.questdb.griffin.ScalarCompensation.*;

/**
 * Rewrites every dependent join step (a LATERAL body reading the inputs before it through outer columns)
 * into an ordinary step. Runs before any other pass.
 * <p>
 * The body is processed block by block from its leaves; a block is a chain of single-input nodes with at
 * most one projection, over a source. An outer column a block reads is satisfied, in order of preference,
 * by a column a deeper block exposes for it, by an inner column its WHERE equates it to (when the block is
 * the body itself and reads no aggregate, or when every outer column of the block is equated), or by a
 * decorrelation domain: the distinct values of the outer columns, read from a copy of the master input or
 * prefix, joined to the block's source. The satisfying column flows up the body as a hidden column:
 * aggregates group by it, windows partition by it, a LIMIT becomes a per-key row number, LATEST ON a per-key
 * ranking, set-operation branches align on it. The step joins on outer column = exposed column.
 * <p>
 * A keyless aggregate yields one row even over no rows. For the body itself, or a body that wraps it in
 * null-propagating arithmetic, the step becomes a LEFT join and a projection over the join restores the
 * empty-input values (zero for counts), applies the body's LIMIT and, for a LEFT LATERAL with an ON condition,
 * that condition over the restored values; consumers of the join read the projection's renamed columns. In the
 * body of a LEFT LATERAL, a count joined without a condition joins LEFT to the column its outer columns map to
 * and the body's projection restores its values. A WHERE or ON that drops the empty row needs no restoring.
 * Any other keyless aggregate restores its empty values with a LEFT join from its own domain.
 */
final class DecorrelationPass implements Mutable {
    private final ScalarCompensation compensation;
    private final OptimiserContext context;
    private final DecorrelationContext ctx;
    private final DecorrelationDomains domains;
    private final CorrelationKeys keys;
    private final ObjList<BoundExpression> liftedConjuncts;
    private final IntList liftedSelectionIds = new IntList();
    private final ObjList<BoundExpression> liftedSelections = new ObjList<>();
    private final CorrelatedChainRewriter rewriter;
    private LogicalPlan branchTop;
    private JoinPlan pendingJoin;

    /**
     * Allocates plan nodes and expressions from the statement binder's pools; the other arguments are
     * scratch the optimiser lends to its passes.
     */
    DecorrelationPass(
            OptimiserContext context,
            BindContext planNodes,
            CharacterStore characterStore,
            ObjList<BoundExpression> callArguments,
            ObjList<BoundExpression> conjunctScratch,
            IntList indexScratch,
            IntList valueScratch,
            IntList keyScratch,
            ObjList<LogicalPlan> planScratch,
            OutputSchema schemaScratch,
            ObjList<JoinInput> stepScratch
    ) {
        this.context = context;
        liftedConjuncts = conjunctScratch;
        ctx = new DecorrelationContext(context, planNodes, characterStore, callArguments, indexScratch, valueScratch, keyScratch, planScratch,
                schemaScratch);
        domains = new DecorrelationDomains(context, ctx, stepScratch);
        keys = new CorrelationKeys(context, ctx);
        compensation = new ScalarCompensation(context, ctx, domains, conjunctScratch);
        rewriter = new CorrelatedChainRewriter(context, ctx, keys, compensation);
    }

    @Override
    public void clear() {
        liftedSelectionIds.clear();
        liftedSelections.clear();
        branchTop = null;
        pendingJoin = null;
        ctx.clear();
        domains.clear();
        keys.clear();
        compensation.clear();
    }

    /**
     * Conjuncts recorded as transitive facts never count a correlated one, and decorrelation has rewritten them.
     */
    private static void dropOuterConjuncts(JoinPlan join) {
        final ObjList<BoundExpression> conjuncts = join.getFilterConjuncts();
        for (int i = conjuncts.size() - 1; i > -1; i--) {
            if (LogicalPlans.hasOuterColumn(conjuncts.getQuick(i))) {
                conjuncts.remove(i);
                join.getFilterConjunctOrigins().removeIndex(i);
            }
        }
    }

    private static boolean hasDependentStep(JoinPlan join) {
        for (int i = 1, n = join.getInputs().size(); i < n; i++) {
            if (join.getInputs().getQuick(i).isDependent()) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasNullableOuterTypes(BoundExpression expression) {
        if (expression instanceof OuterColumnExpression outer) {
            final int type = outer.getDataType();
            return !ColumnType.isArray(type) && !ColumnType.isGeoHash(type) && !ColumnType.isCursor(type);
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (!hasNullableOuterTypes(call.argumentAt(i))) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean isChainNode(LogicalPlan node, boolean hasProject) {
        return switch (node) {
            case LimitPlan _, SortPlan _, DistinctPlan _, WindowPlan _, GroupingPlan _, FillPlan _, FilterPlan _,
                 LatestByPlan _ -> true;
            case ProjectPlan _ -> !hasProject;
            default -> false;
        };
    }

    private static boolean isLiftable(BoundExpression expression) {
        if (expression instanceof OuterColumnExpression) {
            return true;
        }
        if (expression instanceof FunctionExpression call) {
            if (call.isWindow() || call.isAggregate()) {
                return false;
            }
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (!isLiftable(call.argumentAt(i))) {
                    return false;
                }
            }
            return true;
        }
        return !(expression instanceof CursorExpression);
    }

    private static boolean isLiftableChainNode(LogicalPlan node) {
        return node instanceof ProjectPlan || node instanceof FilterPlan || node instanceof SortPlan;
    }

    /**
     * Projects a set-operation branch so it ends with one column for each outer column of {@link #domainOuterIds},
     * in that order; the branch's mapping lies in two ranges of the mapping stack.
     */
    private ProjectPlan alignBranch(LogicalPlan branch, int lo, int hi, int extraLo, int extraHi, int position) {
        final ProjectPlan project = ctx.planNodes.projects.next().of(branch, position);
        final OutputSchema input = branch.getOutput();
        for (int i = 0, n = input.getColumnCount(); i < n; i++) {
            final int columnId = input.getColumnId(i);
            if (ctx.mappedIndex(columnId, lo, hi) < 0 && ctx.mappedIndex(columnId, extraLo, extraHi) < 0) {
                project.getExpressions().add(ctx.planNodes.columns.next().of(columnId, input.getColumnType(i), position));
                project.getOutput().add(context.newColumnId(), input.getColumnName(i), input.getColumnType(i), input.getMetadata(i), input.isVisible(i));
            }
        }
        for (int k = 0, m = domains.domainOuterIds.size(); k < m; k++) {
            final int outerId = domains.domainOuterIds.getQuick(k);
            final int columnId = ctx.mappedColumn(outerId, lo, hi) > -1 ? ctx.mappedColumn(outerId, lo, hi) : ctx.mappedColumn(outerId, extraLo, extraHi);
            ctx.exposeColumn(project, input, columnId, ctx.outerRefName(outerId), position);
        }
        return project;
    }

    private void collectMasterOuterIds(LogicalPlan plan) {
        final int base = ctx.scratch.size();
        LogicalPlans.collectOuterColumnIds(plan, ctx.scratch);
        for (int i = base, n = ctx.scratch.size(); i < n; i++) {
            final int outerId = ctx.scratch.getQuick(i);
            if (ctx.masterInput(outerId) > -1 && !ctx.masterOuterIds.contains(outerId)) {
                ctx.masterOuterIds.add(outerId);
            }
        }
        ctx.scratch.setPos(base);
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            collectMasterOuterIds(plan.inputAt(i));
        }
    }

    /**
     * Rewrites one block of the body: the chain from {@code top} down to the block's source.
     */
    private LogicalPlan decorrelateBlock(LogicalPlan top, boolean isBodyTop, int base) throws SqlException {
        final int chainBase = ctx.chain.size();
        LogicalPlan node = top;
        boolean hasProject = false;
        while (isChainNode(node, hasProject)) {
            hasProject |= node instanceof ProjectPlan;
            ctx.chain.add(node);
            node = node.inputAt(0);
        }
        final boolean isBranch = top == branchTop;
        final int chainOuterBase = ctx.chainOuterIds.size();
        final int deferredBase = keys.deferredInputs.size();
        final int drivenBase = compensation.drivenInputs.size();
        final int uncompensatedBase = compensation.uncompensatedScalars.size();
        final ProjectPlan consumer = compensation.drivingConsumer(chainBase, hasProject);
        try {
            LogicalPlan source = decorrelateSource(node, base, consumer);
            for (int i = chainBase, n = ctx.chain.size(); i < n; i++) {
                LogicalPlans.collectOuterColumnIds(ctx.chain.getQuick(i), ctx.chainOuterIds);
            }
            if (source instanceof JoinPlan join) {
                LogicalPlans.collectOuterColumnIds(join, ctx.chainOuterIds);
            }
            for (int i = deferredBase, n = keys.deferredInputs.size(); i < n; i++) {
                ctx.chainOuterIds.add(keys.deferredOuterIds.getQuick(i));
            }
            source = satisfyOuterColumns(source, chainBase, chainOuterBase, isBodyTop, isBranch, base, keys.deferredInputs.size() > deferredBase);
            if (source instanceof JoinPlan join) {
                keys.keyDeferredInputs(join, deferredBase, base);
                compensation.compensateDrivenInputs(join, drivenBase, consumer);
                final int keyedBase = ctx.scratch.size();
                for (int i = 1, n = join.getInputs().size(); i < n; i++) {
                    if (hasOuterCondition(join.getInputs().getQuick(i))) {
                        ctx.scratch.add(i);
                    }
                }
                ctx.remapMapped(join, base);
                for (int i = keyedBase, n = ctx.scratch.size(); i < n; i++) {
                    keys.keyOuterConditions(join, join.getInputs().getQuick(ctx.scratch.getQuick(i)));
                }
                ctx.scratch.setPos(keyedBase);
            }
            return rewriter.rewriteChain(source, chainBase, base, isBodyTop || isBranch);
        } finally {
            ctx.chain.setPos(chainBase);
            ctx.chainOuterIds.setPos(chainOuterBase);
            keys.deferredInputs.setPos(deferredBase);
            keys.deferredOuterIds.setPos(deferredBase);
            keys.deferredColumnIds.setPos(deferredBase);
            compensation.drivenInputs.setPos(drivenBase);
            compensation.drivenScalars.setPos(drivenBase);
            compensation.uncompensatedScalars.setPos(uncompensatedBase);
        }
    }

    private LogicalPlan decorrelateBranch(LogicalPlan branch, int base) throws SqlException {
        final LogicalPlan previous = branchTop;
        branchTop = branch;
        try {
            return decorrelateBlock(branch, false, base);
        } finally {
            branchTop = previous;
        }
    }

    private LogicalPlan decorrelateJoin(JoinPlan join) throws SqlException {
        for (int i = 1, n = join.getInputs().size(); i < n; i++) {
            if (join.getInputs().getQuick(i).isDependent()) {
                decorrelateStep(join, i);
            }
        }
        if (compensation.compensatedIds.size() > 0) {
            compensation.hoistCompensatedConjuncts(join);
            pendingJoin = join;
        }
        return join;
    }

    private LogicalPlan decorrelateSetOperation(SetOperationPlan operation, int base) throws SqlException {
        final int leftBase = ctx.mappedOuterIds.size();
        operation.replaceInput(0, decorrelateBranch(operation.getLeft(), leftBase));
        final int rightBase = ctx.mappedOuterIds.size();
        operation.replaceInput(1, decorrelateBranch(operation.getRight(), rightBase));
        final int rightEnd = ctx.mappedOuterIds.size();
        operation.replaceInput(0, domains.crossMissing(operation.getLeft(), rightBase, rightEnd, leftBase, rightBase, operation.getPosition()));
        final int rightExtraBase = ctx.mappedOuterIds.size();
        operation.replaceInput(1, domains.crossMissing(operation.getRight(), leftBase, rightBase, rightBase, rightEnd, operation.getRightPosition()));
        final int extraEnd = ctx.mappedOuterIds.size();
        domains.domainOuterIds.clear();
        for (int i = 0, n = ctx.masterOuterIds.size(); i < n; i++) {
            final int outerId = ctx.masterOuterIds.getQuick(i);
            if (ctx.mappedColumn(outerId, leftBase, extraEnd) > -1) {
                domains.domainOuterIds.add(outerId);
            }
        }
        operation.replaceInput(0, alignBranch(operation.getLeft(), leftBase, rightBase, rightEnd, rightExtraBase, operation.getPosition()));
        operation.replaceInput(1, alignBranch(operation.getRight(), rightBase, rightEnd, rightExtraBase, extraEnd, operation.getRightPosition()));
        ctx.mappedOuterIds.setPos(base);
        ctx.mappedColumnIds.setPos(base);
        final OutputSchema output = operation.getOutput();
        final OutputSchema leftOutput = operation.getLeft().getOutput();
        for (int i = output.getColumnCount(), k = 0, n = leftOutput.getColumnCount(); i < n; i++, k++) {
            final int columnId = context.newColumnId();
            output.add(columnId, leftOutput.getColumnName(i), leftOutput.getColumnType(i), false);
            ctx.addMapping(domains.domainOuterIds.getQuick(k), columnId);
        }
        return operation;
    }

    /**
     * Lays out a window join over its decorrelated master: master columns, then the step aggregates, with each
     * step's scopes rebuilt over that layout.
     */
    private void alignWindowJoin(WindowJoinPlan windowJoin) {
        final OutputSchema output = windowJoin.getOutput();
        final OutputSchema master = windowJoin.getMaster().getOutput();
        ctx.alignColumns(output, master);
        int prefix = master.getColumnCount();
        for (int s = 0, m = windowJoin.getSteps().size(); s < m; s++) {
            final WindowJoinStep step = windowJoin.getSteps().getQuick(s);
            final OutputSchema masterScope = step.getMasterScope();
            masterScope.clear();
            for (int i = 0; i < prefix; i++) {
                addColumn(masterScope, output, i);
            }
            final OutputSchema scope = step.getScope();
            scope.copyFrom(masterScope);
            final OutputSchema slave = step.getSlave().getOutput();
            for (int i = 0, n = slave.getColumnCount(); i < n; i++) {
                scope.add(slave.getColumnId(i), slave.getColumnName(i), slave.getColumnType(i), slave.getMetadata(i), slave.isVisible(i),
                        step.getSlaveAlias());
                scope.setSymbolTableStatic(scope.getColumnCount() - 1, slave.isSymbolTableStatic(i));
            }
            prefix += step.getAggregateColumnIds().size();
        }
    }

    /**
     * Rewrites the source of a block, the plan below its chain, leaving the columns that satisfy the outer
     * columns it reads in the mapping above {@code base}.
     */
    private LogicalPlan decorrelateSource(LogicalPlan source, int base, ProjectPlan consumer) throws SqlException {
        if (!hasMasterOuterColumn(source)) {
            return source;
        }
        switch (source) {
            case JoinPlan join -> {
                for (int i = 0, n = join.getInputs().size(); i < n; i++) {
                    final JoinInput input = join.getInputs().getQuick(i);
                    if (input.getInput() != null && hasMasterOuterColumn(input.getInput())) {
                        final int inputBase = ctx.mappedOuterIds.size();
                        final ProjectPlan driven = i > 0 && consumer != null ? compensation.drivenScalar(join, input) : null;
                        if (driven != null) {
                            compensation.drivenScalars.add(driven);
                            compensation.drivenInputs.add(input);
                            compensation.uncompensatedScalars.add(driven);
                        } else if (input.getInput() instanceof ProjectPlan project && compensation.isScalarProjection(project)
                                && input.getJoinType() == JoinKind.INNER && rejectsZeroCount(input.getOnResidual(), project)) {
                            compensation.uncompensatedScalars.add(project);
                        }
                        input.setInput(decorrelateBlock(input.getInput(), false, inputBase));
                        if (driven != null && compensation.scalarAggregateBelow(driven) != null) {
                            compensation.exposeCarriers(driven);
                            input.setJoinType(JoinKind.LEFT_OUTER);
                        }
                        keys.joinMappedInput(input, inputBase, base);
                        if (i > 0 && (input.getJoinType() == JoinKind.LEFT_OUTER || input.getJoinType() == JoinKind.FULL_OUTER)) {
                            keys.deferMapping(input, inputBase);
                        }
                    }
                }
                rebuildJoinOutput(join);
                dropOuterConjuncts(join);
                return join;
            }
            case SetOperationPlan operation -> {
                return decorrelateSetOperation(operation, base);
            }
            case WindowJoinPlan windowJoin -> {
                windowJoin.replaceInput(0, decorrelateBlock(windowJoin.getMaster(), false, base));
                alignWindowJoin(windowJoin);
                return windowJoin;
            }
            case HorizonJoinPlan horizon -> {
                horizon.replaceInput(0, decorrelateBlock(horizon.getMaster(), false, base));
                ctx.alignColumns(horizon.getOutput(), horizon.getMaster().getOutput());
                return horizon;
            }
            default -> {
                return decorrelateBlock(source, false, base);
            }
        }
    }

    private void decorrelateStep(JoinPlan join, int index) throws SqlException {
        final JoinInput step = join.getInputs().getQuick(index);
        final JoinPlan previousMaster = ctx.master;
        final int previousLimit = ctx.masterLimit;
        final int uncompensatedBase = compensation.uncompensatedScalars.size();
        final int masterBase = ctx.masterOuterIds.size();
        ctx.master = join;
        ctx.masterLimit = index;
        ctx.outerAliases.clear();
        for (int i = 1, n = join.getInputs().size(); i < n; i++) {
            keys.collectOuterAliases(join.getInputs().getQuick(i).getPostJoinFilter(), join, index);
        }
        try {
            collectMasterOuterIds(step.getInput());
            step.setDependent(false);
            if (ctx.masterOuterIds.size() == masterBase) {
                return;
            }
            domains.decorrelatedSteps.add(step);
            do {
                ctx.outerRefSequence++;
            } while (ctx.hasOuterRefName(step.getInput()) || ctx.hasOuterRefName(join));
            domains.domainSequence = 0;
            compensation.scalarAggregate = null;
            liftedConjuncts.clear();
            liftedSelectionIds.clear();
            liftedSelections.clear();
            final LimitPlan limit = step.getInput() instanceof LimitPlan l ? l : null;
            final BoundExpression outerLimit = limit == null ? null : outerLimit(limit);
            final BoundExpression limitLo = limit == null ? null : limit.getLo();
            final BoundExpression limitHi = limit == null ? null : limit.getHi();
            final boolean isLeft = step.getJoinType() == JoinKind.LEFT_OUTER;
            final int conditionKeyCount = step.getMasterKeyColumnIds().size();
            final boolean isTrivialCondition = conditionKeyCount == 0 && isTrue(step.getOnResidual());
            final ProjectPlan rejecting = isLeft ? null : compensation.zeroRejectingScalar(step.getInput());
            final ProjectPlan wrapped = isLeft || !isTrivialCondition || rejecting != null ? null : compensation.wrappedScalar(step.getInput());
            if (rejecting != null || wrapped != null) {
                compensation.uncompensatedScalars.add(rejecting != null ? rejecting : wrapped);
            }
            final int base = ctx.mappedOuterIds.size();
            final LogicalPlan body = decorrelateBlock(step.getInput(), true, base);
            step.setInput(body);
            final ProjectPlan scalarBody = wrapped != null ? compensation.wrappedBody(body, wrapped) : compensation.scalarBody(body);
            if (scalarBody != null && wrapped == null) {
                if (outerLimit != null && compensation.hasZeroOnEmptyColumn(scalarBody)) {
                    throw SqlException.$(outerLimit.getPosition(), OUTER_LIMIT_OVER_COUNT);
                }
                compensation.exposeCarriers(scalarBody);
                compensation.forwardColumns(body, scalarBody);
            }
            final OutputSchema output = body.getOutput();
            for (int i = base, n = ctx.mappedOuterIds.size(); i < n; i++) {
                final int columnId = ctx.mappedColumnIds.getQuick(i);
                final int masterId = ctx.masterColumn(ctx.mappedOuterIds.getQuick(i));
                final int keyIndex = scalarBody == null ? step.getMasterKeyColumnIds().indexOf(masterId, 0, step.getMasterKeyColumnIds().size()) : -1;
                if (keyIndex > -1) {
                    keys.addKeyFilter(step, columnId, step.getSlaveKeyColumnIds().getQuick(keyIndex), output);
                } else {
                    JoinBinder.addJoinKey(step, masterId, columnId, ctx.masterColumnName(ctx.mappedOuterIds.getQuick(i)),
                            ctx.qualifiedName(step.getBindingAlias(), output.getColumnName(output.getColumnIndexById(columnId))), step.getPosition());
                }
            }
            if (liftedConjuncts.size() > 0 || liftedSelections.size() > 0) {
                liftAboveStep(join, step, body, masterBase);
            }
            rebuildJoinOutput(join);
            if (scalarBody != null) {
                final BoundExpression guard = limit == null ? null : compensation.limitGuard(ctx.masterExpression(limitLo), ctx.masterExpression(limitHi), join.getOutput());
                if (!isLeft && isTrivialCondition) {
                    step.setJoinType(JoinKind.LEFT_OUTER);
                    compensation.compensateStep(step, scalarBody, base, null, null);
                    if (guard != null) {
                        step.setPostJoinFilter(guard);
                    }
                } else {
                    compensation.compensateStep(step, scalarBody, base, guard, isLeft ? keys.stepCondition(join, step, conditionKeyCount) : null);
                }
            } else if (step.getJoinType() == JoinKind.CROSS && step.getMasterKeyColumnIds().size() > 0) {
                step.setJoinType(JoinKind.INNER);
            }
            ctx.mappedOuterIds.setPos(base);
            ctx.mappedColumnIds.setPos(base);
        } finally {
            ctx.masterOuterIds.setPos(masterBase);
            ctx.master = previousMaster;
            ctx.masterLimit = previousLimit;
            compensation.uncompensatedScalars.setPos(uncompensatedBase);
        }
    }

    private void exposeLiftedColumns(ProjectPlan project, BoundExpression expression) {
        final int columnBase = ctx.scratch.size();
        collectColumnIds(expression, ctx.scratch);
        final OutputSchema output = project.getOutput();
        for (int i = columnBase, n = ctx.scratch.size(); i < n; i++) {
            final int columnId = ctx.scratch.getQuick(i);
            if (ctx.substitution.keyIndex(columnId) < 0) {
                continue;
            }
            int exposed = -1;
            for (int k = 0, m = project.getExpressions().size(); k < m && exposed < 0; k++) {
                if (output.isVisible(k) && project.getExpressions().getQuick(k) instanceof ColumnExpression column && column.getColumnId() == columnId && !column.isCast()) {
                    exposed = output.getColumnId(k);
                }
            }
            if (exposed < 0) {
                final OutputSchema input = project.getInput().getOutput();
                final int type = input.getColumnType(input.getColumnIndexById(columnId));
                project.getExpressions().add(ctx.planNodes.columns.next().of(columnId, type, project.getPosition()));
                exposed = context.newColumnId();
                output.add(exposed, liftedName(output, input.getColumnName(input.getColumnIndexById(columnId))), type, false);
            }
            ctx.substitution.put(columnId, exposed);
        }
        ctx.scratch.setPos(columnBase);
    }

    private boolean hasDecorrelatedMasterReference(int chainOuterBase) {
        for (int i = chainOuterBase, n = ctx.chainOuterIds.size(); i < n; i++) {
            final int outerId = ctx.chainOuterIds.getQuick(i);
            if (ctx.masterOuterIds.contains(outerId) && domains.decorrelatedSteps.indexOf(ctx.master.getInputs().getQuick(ctx.masterInput(outerId))) > -1) {
                return true;
            }
        }
        return false;
    }

    private boolean hasMasterOuterColumn(LogicalPlan plan) {
        final int base = ctx.scratch.size();
        LogicalPlans.collectOuterColumnIds(plan, ctx.scratch);
        boolean isFound = false;
        for (int i = base, n = ctx.scratch.size(); i < n && !isFound; i++) {
            isFound = ctx.masterOuterIds.contains(ctx.scratch.getQuick(i));
        }
        ctx.scratch.setPos(base);
        if (isFound) {
            return true;
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            if (hasMasterOuterColumn(plan.inputAt(i))) {
                return true;
            }
        }
        return false;
    }

    private boolean isUnmappedOuter(BoundExpression expression, int base) {
        final int columnBase = ctx.scratch.size();
        LogicalPlans.collectOuterColumnIds(expression, ctx.scratch);
        boolean isFound = false;
        for (int i = columnBase, n = ctx.scratch.size(); i < n && !isFound; i++) {
            final int outerId = ctx.scratch.getQuick(i);
            isFound = ctx.masterOuterIds.contains(outerId) && ctx.mappedColumn(outerId, base, ctx.mappedOuterIds.size()) < 0;
        }
        ctx.scratch.setPos(columnBase);
        return isFound;
    }

    /**
     * Joins the lifted conjuncts to the step's ON and computes the lifted columns above the join, reading the
     * master's columns for the outer columns and the body's exposed columns for its own.
     */
    private void liftAboveStep(JoinPlan join, JoinInput step, LogicalPlan body, int masterBase) throws SqlException {
        LogicalPlan node = body;
        while (!(node instanceof ProjectPlan)) {
            node = node.inputAt(0);
        }
        final ProjectPlan project = (ProjectPlan) node;
        ctx.substitution.clear();
        for (int i = masterBase, n = ctx.masterOuterIds.size(); i < n; i++) {
            ctx.substitution.put(ctx.masterOuterIds.getQuick(i), ctx.masterColumn(ctx.masterOuterIds.getQuick(i)));
        }
        for (int i = 0, n = liftedConjuncts.size(); i < n; i++) {
            exposeLiftedColumns(project, liftedConjuncts.getQuick(i));
        }
        for (int i = 0, n = liftedSelections.size(); i < n; i++) {
            exposeLiftedColumns(project, liftedSelections.getQuick(i));
        }
        rebuildJoinOutput(join);
        for (int i = 0, n = liftedConjuncts.size(); i < n; i++) {
            final BoundExpression conjunct = context.getRewriter().remapColumns(liftedConjuncts.getQuick(i), ctx.substitution);
            step.setOnResidual(step.getOnResidual() == null ? conjunct : context.getRewriter().combineConjunction(step.getOnResidual(), conjunct, conjunct.getPosition()));
        }
        final OutputSchema output = project.getOutput();
        for (int i = 0, n = liftedSelections.size(); i < n; i++) {
            final int columnId = liftedSelectionIds.getQuick(i);
            final int index = output.getColumnIndexById(columnId);
            compensation.compensatedIds.add(columnId);
            compensation.compensatedNames.add(output.getColumnName(index));
            compensation.compensations.add(context.getRewriter().remapColumns(liftedSelections.getQuick(i), ctx.substitution));
            project.getExpressions().remove(index);
            output.remove(index);
            join.getOutput().remove(join.getOutput().getColumnIndexById(columnId));
        }
    }

    /**
     * Returns the conjuncts of the predicate that stay in the body, recording in {@link #liftedConjuncts} those
     * that read outer columns no equality satisfies.
     */
    private BoundExpression liftConjuncts(BoundExpression predicate, int base) {
        if (predicate == null) {
            return null;
        }
        if (predicate instanceof FunctionExpression call && call.isAnd()) {
            final BoundExpression left = liftConjuncts(call.argumentAt(0), base);
            final BoundExpression right = liftConjuncts(call.argumentAt(1), base);
            if (left == null) {
                return right;
            }
            if (right == null) {
                return left;
            }
            return left == call.argumentAt(0) && right == call.argumentAt(1) ? call : context.getRewriter().replaceConjunction(call, left, right);
        }
        if (isUnmappedOuter(predicate, base) && isLiftable(predicate)) {
            liftedConjuncts.add(predicate);
            return null;
        }
        return predicate;
    }

    /**
     * Moves the conjuncts of the body's WHERE and the computed columns of its projection that read outer
     * columns no equality satisfies above the step: the conjuncts join its ON, the columns a projection over
     * the join computes. The outer columns they read are then no longer the body's.
     */
    private void liftOuterTerms(FilterPlan filter, LogicalPlan source, int chainBase, int chainOuterBase, int base) {
        if (filter != null) {
            final BoundExpression remaining = liftConjuncts(filter.getPredicate(), base);
            filter.of(filter.getInput(), remaining != null ? remaining : ctx.planNodes.constants.next().ofBoolean(true, filter.getPosition()), filter.getPosition());
        }
        if (source instanceof JoinPlan join) {
            for (int i = 1, n = join.getInputs().size(); i < n; i++) {
                final JoinInput input = join.getInputs().getQuick(i);
                input.setPostJoinFilter(liftConjuncts(input.getPostJoinFilter(), base));
            }
        }
        for (int i = chainBase, n = ctx.chain.size(); i < n; i++) {
            if (ctx.chain.getQuick(i) instanceof ProjectPlan project) {
                final ObjList<BoundExpression> expressions = project.getExpressions();
                for (int k = 0, m = expressions.size(); k < m; k++) {
                    final BoundExpression expression = expressions.getQuick(k);
                    if (project.getOutput().isVisible(k) && !(expression instanceof OuterColumnExpression) && isUnmappedOuter(expression, base)
                            && isLiftable(expression) && hasNullableOuterTypes(expression)) {
                        liftedSelectionIds.add(project.getOutput().getColumnId(k));
                        liftedSelections.add(expression);
                        expressions.setQuick(k, ctx.planNodes.constants.next().ofNull(expression.getPosition()));
                    }
                }
            }
        }
        ctx.chainOuterIds.setPos(chainOuterBase);
        for (int i = chainBase, n = ctx.chain.size(); i < n; i++) {
            LogicalPlans.collectOuterColumnIds(ctx.chain.getQuick(i), ctx.chainOuterIds);
        }
        if (source instanceof JoinPlan join) {
            LogicalPlans.collectOuterColumnIds(join, ctx.chainOuterIds);
        }
    }

    private CharSequence liftedName(OutputSchema output, CharSequence name) {
        if (output.getColumnIndexQuiet(name) < 0) {
            boolean isTaken = false;
            for (int i = 0, n = output.getColumnCount(); i < n && !isTaken; i++) {
                isTaken = Chars.equalsIgnoreCase(output.getColumnName(i), name);
            }
            if (!isTaken) {
                return name;
            }
        }
        final CharacterStoreEntry entry = ctx.characterStore.newEntry();
        entry.put("__qdb_lifted_").put(ctx.carrierSequence++).put('_').put(name);
        return entry.toImmutable();
    }

    /**
     * Satisfies the outer columns the block chain reads and the mapping does not: by WHERE equalities when
     * the block allows it, otherwise by a domain joined to the block source.
     */
    private LogicalPlan satisfyOuterColumns(LogicalPlan source, int chainBase, int chainOuterBase, boolean isBodyTop, boolean isBranch, int base,
                                            boolean hasDeferred) throws SqlException {
        FilterPlan filter = null;
        boolean isLiftable = isBodyTop && !(source instanceof SetOperationPlan);
        for (int i = chainBase, n = ctx.chain.size(); i < n; i++) {
            final LogicalPlan node = ctx.chain.getQuick(i);
            if (node instanceof FilterPlan f && filter == null) {
                filter = f;
            }
            isLiftable &= isLiftableChainNode(node);
        }
        keys.droppedEqualities.clear();
        if (!isBranch) {
            if (filter != null) {
                keys.collectEqualities(filter.getPredicate(), filter.getInput().getOutput(), base);
            }
            if (source instanceof JoinPlan join) {
                for (int i = 1, n = join.getInputs().size(); i < n; i++) {
                    keys.collectEqualities(join.getInputs().getQuick(i).getPostJoinFilter(), join.getOutput(), base);
                }
            }
            if (isLiftable || keys.isEveryOuterColumnEquated(chainOuterBase, base)) {
                if (source instanceof JoinPlan join) {
                    for (int i = 1, n = join.getInputs().size(); i < n; i++) {
                        final JoinInput input = join.getInputs().getQuick(i);
                        input.setPostJoinFilter(keys.dropEqualities(input.getPostJoinFilter()));
                    }
                }
                for (int i = 0, n = keys.droppedEqualities.size(); i < n; i += 2) {
                    ctx.addMapping(keys.droppedEqualities.getQuick(i), keys.droppedEqualities.getQuick(i + 1));
                }
            } else {
                keys.droppedEqualities.clear();
            }
        }
        if (isLiftable && (ctx.mappedOuterIds.size() > base || hasDecorrelatedMasterReference(chainOuterBase))) {
            liftOuterTerms(filter, source, chainBase, chainOuterBase, base);
        }
        domains.domainOuterIds.clear();
        for (int i = chainOuterBase, n = ctx.chainOuterIds.size(); i < n; i++) {
            final int outerId = ctx.chainOuterIds.getQuick(i);
            if (ctx.masterOuterIds.contains(outerId) && ctx.mappedColumn(outerId, base, ctx.mappedOuterIds.size()) < 0 && !domains.domainOuterIds.contains(outerId)) {
                domains.domainOuterIds.add(outerId);
            }
        }
        if (domains.domainOuterIds.size() == 0) {
            return source;
        }
        domains.domainEqualities.clear();
        if (filter != null && !(source instanceof JoinPlan)) {
            domains.collectDomainEqualities(filter.getPredicate(), source.getOutput());
        }
        final AggregatePlan domain = domains.buildDomain(source.getPosition());
        if (!(source instanceof JoinPlan join)) {
            final JoinPlan crossed = domains.crossDomain(source, domain, source.getPosition());
            final JoinInput domainStep = crossed.getInputs().getQuick(1);
            final OutputSchema domainOutput = domain.getOutput();
            for (int i = 0, n = domains.domainEqualities.size(); i < n; i += 2) {
                final int outerId = domains.domainEqualities.getQuick(i);
                final int columnId = domains.domainEqualities.getQuick(i + 1);
                final int domainId = ctx.mappedColumn(outerId, base, ctx.mappedOuterIds.size());
                JoinBinder.addJoinKey(domainStep, columnId, domainId, source.getOutput().getColumnName(source.getOutput().getColumnIndexById(columnId)),
                        domainOutput.getColumnName(domainOutput.getColumnIndexById(domainId)), domainStep.getPosition());
                domainStep.setJoinType(JoinKind.INNER);
                keys.droppedEqualities.add(outerId);
                keys.droppedEqualities.add(columnId);
            }
            return crossed;
        }
        if (hasDeferred || JoinBinder.hasBarrierInput(join)) {
            final JoinInput first = join.getOrderedInputs().getQuick(0);
            first.setInput(domains.crossDomain(first.getInput(), domain, source.getPosition()));
        } else {
            final JoinInput step = ctx.planNodes.joinInputs.next().of(domain, JoinKind.CROSS, domains.domainAlias(), source.getPosition());
            for (int i = 1, n = join.getInputs().size(); i < n; i++) {
                final JoinInput input = join.getInputs().getQuick(i);
                input.setPostJoinFilter(domains.moveDomainConjuncts(input.getPostJoinFilter(), step));
                input.setOnResidual(domains.moveDomainConjuncts(input.getOnResidual(), step));
            }
            join.getInputs().add(step);
            join.getOrderedInputs().add(step);
        }
        rebuildJoinOutput(join);
        return join;
    }

    /**
     * Rewrites every dependent join step of the plan, innermost first. Consumers above a step whose values a
     * projection restores read the projection's columns.
     */
    LogicalPlan decorrelate(LogicalPlan plan) throws SqlException {
        LogicalPlan result = plan;
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            final LogicalPlan input = decorrelate(plan.inputAt(i));
            plan.replaceInput(i, input);
            if (input == pendingJoin) {
                pendingJoin = null;
                final LogicalPlan consumer = plan instanceof ProjectPlan project ? compensation.compensateConsumer(project) : null;
                if (consumer != null) {
                    result = consumer;
                } else {
                    plan.replaceInput(i, compensation.consumerProjection((JoinPlan) input));
                }
            }
        }
        if (compensation.consumerRemap.size() > 0) {
            ctx.copier.remap(plan, compensation.consumerRemap);
        }
        if (domains.decorrelatedSteps.size() > 0) {
            switch (plan) {
                case ProjectPlan _, GroupingPlan _, JoinPlan _, SetOperationPlan _, ScanPlan _,
                     FunctionSourcePlan _ -> {
                }
                case WindowJoinPlan windowJoin -> alignWindowJoin(windowJoin);
                default -> ctx.alignColumns(plan.getOutput(), plan.inputAt(0).getOutput());
            }
        }
        return plan instanceof JoinPlan join && hasDependentStep(join) ? decorrelateJoin(join) : result;
    }

}
