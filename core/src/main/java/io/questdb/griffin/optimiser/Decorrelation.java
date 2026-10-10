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

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.BoundExpressionRewriter.ConjunctTest;
import io.questdb.griffin.CharacterStore;
import io.questdb.griffin.CharacterStoreEntry;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.PlanNodePools;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.ExpressionVisitor;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
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
import io.questdb.griffin.plan.logical.PlanVisitor;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowJoinStep;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

import static io.questdb.griffin.optimiser.CorrelationKeys.hasOuterCondition;
import static io.questdb.griffin.optimiser.DecorrelationContext.*;
import static io.questdb.griffin.optimiser.ScalarCompensation.*;

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
final class Decorrelation implements OptimiserPass {
    private static final ExpressionVisitor NON_NULLABLE_OUTER_COLUMNS = expression -> expression instanceof OuterColumnExpression outer
            && (ColumnType.isArray(outer.getDataType()) || ColumnType.isGeoHash(outer.getDataType()) || ColumnType.isCursor(outer.getDataType()))
            ? TreeWalk.STOP : TreeWalk.CONTINUE;
    private static final ExpressionVisitor UNLIFTABLE_NODES = expression -> expression instanceof FunctionExpression call && (call.isWindow() || call.isAggregate())
            || expression instanceof CursorExpression ? TreeWalk.STOP : TreeWalk.CONTINUE;
    private final OuterJoinCarriers carriers;
    private final ScalarCompensation compensation;
    private final OptimiserContext context;
    private final DecorrelationContext ctx;
    private final DecorrelationDomains domains;
    private final CorrelationKeys keys;
    private final ObjList<BoundExpression> liftedConjuncts;
    private final ConjunctTest bodyConjuncts = this::keepsBodyConjunct;
    private final IntList liftedSelectionIds = new IntList();
    private final ObjList<BoundExpression> liftedSelections = new ObjList<>();
    private final CorrelatedChainRewriter rewriter;
    private LogicalPlan branchTop;
    private int liftBase;
    private JoinPlan pendingJoin;
    private final PlanVisitor masterOuterIds = this::collectNodeMasterOuterIds;
    private final PlanVisitor masterOuterReads = this::findMasterOuterColumn;

    /**
     * Allocates plan nodes and expressions from the statement binder's pools; the other arguments are
     * temporary lists the optimiser lends to its passes.
     */
    Decorrelation(
            OptimiserContext context,
            PlanNodePools planNodes,
            CharacterStore characterStore,
            ObjList<BoundExpression> tmpConjuncts,
            IntList tmpIndexes,
            IntList tmpValues,
            IntList tmpKeys,
            ObjList<LogicalPlan> tmpPlans,
            OutputSchema tmpSchema,
            ObjList<JoinInput> tmpSteps
    ) {
        this.context = context;
        liftedConjuncts = tmpConjuncts;
        ctx = new DecorrelationContext(context, planNodes, characterStore, tmpIndexes, tmpValues, tmpKeys, tmpPlans,
                tmpSchema);
        domains = new DecorrelationDomains(ctx, tmpSteps);
        keys = new CorrelationKeys(ctx);
        carriers = new OuterJoinCarriers(ctx, domains, keys);
        compensation = new ScalarCompensation(ctx, domains, tmpConjuncts);
        rewriter = new CorrelatedChainRewriter(ctx, keys, compensation);
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) throws SqlException {
        return decorrelate(plan);
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
        carriers.clear();
        compensation.clear();
    }

    @Override
    public String getName() {
        return "decorrelation";
    }

    /**
     * Empties the carriers decorrelation recorded on join steps for {@link PlanVerifier#verifyDecorrelation}.
     */
    public void releaseCarriers() {
        ctx.releaseCarriers();
    }

    @Override
    public boolean removesDependentSteps() {
        return true;
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
        return expression.walk(NON_NULLABLE_OUTER_COLUMNS);
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
        return expression.walk(UNLIFTABLE_NODES);
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
                masterScope.addColumnFrom(output, i);
            }
            final OutputSchema scope = step.getScope();
            scope.copyFrom(masterScope);
            scope.addColumnsFrom(step.getSlave().getOutput(), step.getSlaveAlias());
            prefix += step.getAggregateColumnIds().size();
        }
    }

    /**
     * Collects the outer columns the block reads above {@code chainOuterBase}: those its chain and source join
     * read, and those of its deferred nullable inputs, which the block satisfies itself.
     */
    private void collectChainOuterIds(LogicalPlan source, int chainBase, int chainOuterBase, int deferredBase) {
        ctx.chainOuterIds.setPos(chainOuterBase);
        for (int i = chainBase, n = ctx.chain.size(); i < n; i++) {
            ctx.outerColumnReads.collect(ctx.chain.getQuick(i), ctx.chainOuterIds);
        }
        if (source instanceof JoinPlan join) {
            ctx.outerColumnReads.collect(join, ctx.chainOuterIds);
        }
        for (int i = deferredBase, n = keys.deferredInputs.size(); i < n; i++) {
            ctx.chainOuterIds.add(keys.deferredOuterIds.getQuick(i));
        }
    }

    private void collectMasterOuterIds(LogicalPlan plan) {
        plan.walkTopDown(masterOuterIds);
    }

    private int collectNodeMasterOuterIds(LogicalPlan plan) {
        final int base = ctx.tmpColumnIds.size();
        ctx.outerColumnReads.collect(plan, ctx.tmpColumnIds);
        for (int i = base, n = ctx.tmpColumnIds.size(); i < n; i++) {
            final int outerId = ctx.tmpColumnIds.getQuick(i);
            if (ctx.masterInput(outerId) > -1 && !ctx.masterOuterIds.contains(outerId)) {
                ctx.masterOuterIds.add(outerId);
            }
        }
        ctx.tmpColumnIds.setPos(base);
        return TreeWalk.CONTINUE;
    }

    /**
     * Rewrites every dependent join step of the plan, innermost first. Consumers above a step whose values a
     * projection restores read the projection's columns.
     */
    private LogicalPlan decorrelate(LogicalPlan plan) throws SqlException {
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
            if (plan instanceof WindowJoinPlan windowJoin) {
                alignWindowJoin(windowJoin);
            } else if (plan instanceof WindowPlan window) {
                ctx.alignColumns(window.getOutput(), window.getInput().getOutput());
            }
        }
        return plan instanceof JoinPlan join && hasDependentStep(join) ? decorrelateJoin(join) : result;
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
            collectChainOuterIds(source, chainBase, chainOuterBase, deferredBase);
            source = satisfyOuterColumns(source, chainBase, chainOuterBase, isBodyTop, isBranch, base, deferredBase);
            if (source instanceof JoinPlan join) {
                keys.keyDeferredInputs(join, deferredBase, base);
                compensation.compensateDrivenInputs(join, drivenBase, consumer);
                final int keyedBase = ctx.tmpColumnIds.size();
                for (int i = 1, n = join.getInputs().size(); i < n; i++) {
                    if (hasOuterCondition(join.getInputs().getQuick(i))) {
                        ctx.tmpColumnIds.add(i);
                    }
                }
                ctx.remapMapped(join, base);
                for (int i = keyedBase, n = ctx.tmpColumnIds.size(); i < n; i++) {
                    keys.keyOuterConditions(join, join.getInputs().getQuick(ctx.tmpColumnIds.getQuick(i)));
                }
                ctx.tmpColumnIds.setPos(keyedBase);
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
     * Rewrites the source of a block, the plan below its chain, leaving the columns that satisfy the outer
     * columns it reads in the mapping above {@code base}.
     */
    private LogicalPlan decorrelateSource(LogicalPlan source, int base, ProjectPlan consumer) throws SqlException {
        if (!hasMasterOuterColumn(source)) {
            return source;
        }
        switch (source) {
            case JoinPlan join -> {
                final int lastMasterNulling = LogicalPlans.lastMasterNullingStep(join);
                for (int i = 0, n = join.getInputs().size(); i < n; i++) {
                    final JoinInput input = join.getInputs().getQuick(i);
                    if (input.getInput() != null && hasMasterOuterColumn(input.getInput())) {
                        final int inputBase = ctx.mappedOuterIds.size();
                        final ProjectPlan driven = i > 0 && consumer != null && LogicalPlans.orderedSteps(join).indexOf(input) > lastMasterNulling
                                ? compensation.drivenScalar(join, input) : null;
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
                        if (lastMasterNulling > -1) {
                            keys.deferMapping(input, inputBase);
                        } else {
                            keys.joinMappedInput(input, inputBase, base);
                            if (i > 0 && input.getJoinType() == JoinKind.LEFT_OUTER) {
                                keys.deferMapping(input, inputBase);
                            }
                        }
                    }
                }
                join.addMissingInputColumns();
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
                ctx.addCarrier(step, columnId);
                if (keyIndex > -1) {
                    keys.addKeyFilter(step, columnId, step.getSlaveKeyColumnIds().getQuick(keyIndex), output);
                } else {
                    step.addKey(masterId, columnId, ctx.masterColumnName(ctx.mappedOuterIds.getQuick(i)),
                            ctx.qualifiedName(step.getBindingAlias(), output.getColumnName(output.getColumnIndexById(columnId))), step.getPosition());
                }
            }
            if (liftedConjuncts.size() > 0 || liftedSelections.size() > 0) {
                liftAboveStep(join, step, body, masterBase);
            }
            join.addMissingInputColumns();
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
        final int columnBase = ctx.tmpColumnIds.size();
        ctx.collectColumnIds(expression, ctx.tmpColumnIds);
        final OutputSchema output = project.getOutput();
        for (int i = columnBase, n = ctx.tmpColumnIds.size(); i < n; i++) {
            final int columnId = ctx.tmpColumnIds.getQuick(i);
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
        ctx.tmpColumnIds.setPos(columnBase);
    }

    private int findMasterOuterColumn(LogicalPlan plan) {
        final int base = ctx.tmpColumnIds.size();
        ctx.outerColumnReads.collect(plan, ctx.tmpColumnIds);
        boolean isFound = false;
        for (int i = base, n = ctx.tmpColumnIds.size(); i < n && !isFound; i++) {
            isFound = ctx.masterOuterIds.contains(ctx.tmpColumnIds.getQuick(i));
        }
        ctx.tmpColumnIds.setPos(base);
        return isFound ? TreeWalk.STOP : TreeWalk.CONTINUE;
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
        return !plan.walkTopDown(masterOuterReads);
    }

    /**
     * Keeps a conjunct in the body unless it reads outer columns no equality satisfies, which it records in
     * {@link #liftedConjuncts} instead.
     */
    private boolean keepsBodyConjunct(BoundExpression conjunct) {
        if (ctx.readsUnmappedOuter(conjunct, liftBase) && isLiftable(conjunct)) {
            liftedConjuncts.add(conjunct);
            return false;
        }
        return true;
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
        join.addMissingInputColumns();
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

    private CharSequence liftedName(OutputSchema output, CharSequence name) {
        if (!output.hasColumnName(name)) {
            return name;
        }
        final CharacterStoreEntry entry = ctx.characterStore.newEntry();
        entry.put("__qdb_lifted_").put(ctx.carrierSequence++).put('_').put(name);
        return entry.toImmutable();
    }

    /**
     * Moves the conjuncts of the body's WHERE and the computed columns of its projection that read outer
     * columns no equality satisfies above the step: the conjuncts join its ON, the columns a projection over
     * the join computes. The outer columns they read are then no longer the body's.
     */
    private void liftOuterTerms(FilterPlan filter, LogicalPlan source, int chainBase, int chainOuterBase, int deferredBase, int base) throws SqlException {
        liftBase = base;
        if (filter != null) {
            final BoundExpression remaining = context.getRewriter().retainConjuncts(filter.getPredicate(), bodyConjuncts);
            filter.of(filter.getInput(), remaining != null ? remaining : ctx.planNodes.constants.next().ofBoolean(true, filter.getPosition()), filter.getPosition());
        }
        if (source instanceof JoinPlan join) {
            final int lastMasterNulling = LogicalPlans.lastMasterNullingStep(join);
            for (int i = 1, n = join.getInputs().size(); i < n; i++) {
                final JoinInput input = join.getInputs().getQuick(i);
                if (LogicalPlans.orderedSteps(join).indexOf(input) >= lastMasterNulling) {
                    input.setPostJoinFilter(context.getRewriter().retainConjuncts(input.getPostJoinFilter(), bodyConjuncts));
                }
            }
        }
        for (int i = chainBase, n = ctx.chain.size(); i < n; i++) {
            if (ctx.chain.getQuick(i) instanceof ProjectPlan project) {
                final ObjList<BoundExpression> expressions = project.getExpressions();
                for (int k = 0, m = expressions.size(); k < m; k++) {
                    final BoundExpression expression = expressions.getQuick(k);
                    if (project.getOutput().isVisible(k) && !(expression instanceof OuterColumnExpression) && ctx.readsUnmappedOuter(expression, base)
                            && isLiftable(expression) && hasNullableOuterTypes(expression)) {
                        liftedSelectionIds.add(project.getOutput().getColumnId(k));
                        liftedSelections.add(expression);
                        expressions.setQuick(k, ctx.planNodes.constants.next().ofNull(expression.getPosition()));
                    }
                }
            }
        }
        collectChainOuterIds(source, chainBase, chainOuterBase, deferredBase);
    }

    /**
     * Satisfies the outer columns the block chain reads and the mapping does not: by WHERE equalities when
     * the block allows it, otherwise by a domain joined to the block source.
     */
    private LogicalPlan satisfyOuterColumns(LogicalPlan source, int chainBase, int chainOuterBase, boolean isBodyTop, boolean isBranch, int base,
                                            int deferredBase) throws SqlException {
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
            final JoinPlan sourceJoin = source instanceof JoinPlan join ? join : null;
            if (filter != null) {
                keys.collectEqualities(filter.getPredicate(), filter.getInput().getOutput(), sourceJoin, base);
            }
            if (sourceJoin != null) {
                for (int i = 1, n = sourceJoin.getInputs().size(); i < n; i++) {
                    keys.collectEqualities(sourceJoin.getInputs().getQuick(i).getPostJoinFilter(), sourceJoin.getOutput(), sourceJoin, base);
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
            liftOuterTerms(filter, source, chainBase, chainOuterBase, deferredBase, base);
        }
        domains.domainOuterIds.clear();
        for (int i = chainOuterBase, n = ctx.chainOuterIds.size(); i < n; i++) {
            final int outerId = ctx.chainOuterIds.getQuick(i);
            if (ctx.masterOuterIds.contains(outerId) && ctx.mappedColumn(outerId, base, ctx.mappedOuterIds.size()) < 0 && !domains.domainOuterIds.contains(outerId)) {
                domains.domainOuterIds.add(outerId);
            }
        }
        if (source instanceof JoinPlan join && LogicalPlans.lastMasterNullingStep(join) > -1
                && (domains.domainOuterIds.size() > 0 || keys.deferredInputs.size() > deferredBase)) {
            return carriers.place(join, base, deferredBase, chainBase);
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
                domainStep.addKey(columnId, domainId, source.getOutput().getColumnName(source.getOutput().getColumnIndexById(columnId)),
                        domainOutput.getColumnName(domainOutput.getColumnIndexById(domainId)), domainStep.getPosition());
                domainStep.setJoinType(JoinKind.INNER);
                keys.droppedEqualities.add(outerId);
                keys.droppedEqualities.add(columnId);
            }
            return crossed;
        }
        if (keys.deferredInputs.size() > deferredBase || LogicalPlans.hasBarrierInput(join)) {
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
        join.addMissingInputColumns();
        return join;
    }

}
