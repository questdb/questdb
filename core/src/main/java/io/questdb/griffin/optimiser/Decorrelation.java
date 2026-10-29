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

import io.questdb.ParanoiaState;
import io.questdb.cairo.ColumnType;
import io.questdb.griffin.BoundExpressionRewriter.ConjunctTest;
import io.questdb.griffin.CharacterStore;
import io.questdb.griffin.CharacterStoreEntry;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.OuterColumnReads;
import io.questdb.griffin.PlanNodePools;
import io.questdb.griffin.SetOperationCasts;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.DomainReadPlan;
import io.questdb.griffin.plan.logical.ExpressionVisitor;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.FunctionSourcePlan;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.HorizonJoinSlave;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKey;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PlanToFactoryRules;
import io.questdb.griffin.plan.logical.PlanVisitor;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.griffin.plan.logical.UnaryPlan;
import io.questdb.griffin.plan.logical.UniqueColumnNames;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowJoinStep;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

import static io.questdb.griffin.optimiser.CorrelationKeys.hasOuterCondition;
import static io.questdb.griffin.optimiser.DecorrelationContext.isTrue;
import static io.questdb.griffin.optimiser.ScalarCompensation.*;

/**
 * Rewrites every dependent join step (a LATERAL body reading the inputs before it through outer columns)
 * into an ordinary step. Runs before any other pass.
 * <p>
 * The body is processed block by block from its leaves; a block is a chain of single-input nodes with at
 * most one projection, over a source. Below a filter the chain continues only through the nodes that commute with
 * it, so a node the filter's WHERE equalities do not reach starts a block of its own, as in the equivalent join;
 * {@link CorrelatedChainRewriter} holds the rule of each kind of chain node. An outer column a block reads is
 * satisfied, in order of preference,
 * by a column a deeper block exposes for it (that of the input the join orders first, when every step ordered before
 * that input that reads the outer column can join after it), by an inner column its WHERE equates it to (when the block is
 * the body itself and reads no aggregate, or when every outer column of the block is equated), or by a
 * decorrelation domain: the distinct values of the outer columns, read from a copy of the master input or
 * prefix, joined to the block's source. The satisfying column flows up the body as a hidden column:
 * aggregates group by it, windows partition by it, a LIMIT becomes a per-key row number (and row count, for a
 * negative constant bound), LATEST ON a per-key ranking, set-operation branches align on it. The step joins on
 * outer column = exposed column. A key may belong to no outer row, so the body's ranking only substitutes NULL
 * for a non-constant LIMIT bound, and the step rejects a negative one per outer row. A window or horizon join is a
 * block source whose master and slaves are blocks of their own: each slave joins the master on the columns that
 * satisfy the outer columns it reads, through a window join step filter or a horizon join key.
 * <p>
 * A keyless aggregate yields one row even over no rows. For the body itself, or a body that wraps it in
 * null-propagating arithmetic, the step becomes a LEFT join and a projection over the join restores the
 * empty-input values (zero for counts). A LEFT LATERAL applies the body's LIMIT and its ON condition over the
 * restored values; any other LATERAL filters the joined rows on them, or keeps its join when its ON or WHERE
 * drops the empty row. Either evaluates the LIMIT per outer row, which the body then does not rank. When a later
 * step null-extends the step, a row marker of the inputs before it keeps those rows NULL. Consumers of the join
 * read the projection's renamed columns; a later step that reads the body's values through its keys, ON condition
 * or own body precedes that projection, so the body then restores them itself. In the
 * body of a LEFT LATERAL, a count joined without a condition joins LEFT to the column its outer columns map to
 * and the body's projection restores its values. A WHERE or ON that drops the empty row needs no restoring.
 * Any other keyless aggregate restores its empty values with a LEFT join from its own domain, which for a
 * set-operation branch covers every outer column the operation reads.
 */
final class Decorrelation implements OptimiserPass {
    private static final String SOURCE_CORRELATION = "outer column reference in this source is not supported in a LATERAL sub-query";
    private static final ExpressionVisitor NON_NULLABLE_OUTER_COLUMNS = expression -> expression instanceof OuterColumnExpression outer
            && (ColumnType.isArray(outer.getDataType()) || ColumnType.isGeoHash(outer.getDataType()) || ColumnType.isCursor(outer.getDataType()))
            ? TreeWalk.STOP : TreeWalk.CONTINUE;
    private static final ExpressionVisitor UNLIFTABLE_NODES = expression -> expression instanceof FunctionExpression call && (call.isWindow() || call.isAggregate())
            || expression instanceof CursorExpression ? TreeWalk.STOP : TreeWalk.CONTINUE;
    private final PlanVisitor bodyOuterReads = this::findBodyOuterColumn;
    private final OuterJoinCarriers carriers;
    private final ObjList<LogicalPlan> chain;
    private final IntList chainOuterIds;
    private final OptimiserContext context;
    private final OuterColumnMapping mapping;
    private final OuterColumnReads outerColumnReads;
    private final PlanNodePools planNodes;
    private final IntIntHashMap substitution;
    private final IntList tmpColumnIds;
    private final DecorrelationCarriers verificationCarriers = ParanoiaState.PLAN_PARANOIA_MODE ? new DecorrelationCarriers() : null;
    private final ScalarCompensation compensation;
    private final DecorrelationContext ctx;
    private final DecorrelationDomains domains;
    private final CorrelationKeys keys;
    private final ObjList<BoundExpression> liftedConjuncts;
    private final IntList liftedSelectionIds;
    private final ObjList<BoundExpression> liftedSelections = new ObjList<>();
    private final IntList markedSelectionIds;
    private final PlanVisitor masterOuterReads = this::findMasterOuterColumn;
    private final CorrelatedChainRewriter rewriter;
    private LogicalPlan branchTop;
    private int liftBase;
    private final ConjunctTest bodyConjuncts = this::keepsBodyConjunct;
    private JoinPlan pendingJoin;
    private OutputSchema readBody;

    Decorrelation(
            OptimiserContext context,
            PlanNodePools planNodes,
            CharacterStore characterStore,
            ObjList<BoundExpression> tmpConjuncts,
            IntList tmpIndexes,
            IntList tmpValues,
            IntList tmpKeys,
            IntList tmpSlaveKeys,
            IntList tmpOrder,
            IntList tmpPending,
            IntList tmpPairs,
            ObjList<LogicalPlan> tmpPlans,
            OutputSchema tmpSchema,
            ObjList<JoinInput> tmpSteps,
            IntIntHashMap substitution,
            IntIntHashMap consumerRemap,
            UniqueColumnNames takenNames,
            OuterColumnReads outerColumnReads
    ) {
        liftedConjuncts = tmpConjuncts;
        liftedSelectionIds = tmpOrder;
        markedSelectionIds = tmpPending;
        this.context = context;
        this.planNodes = planNodes;
        this.chain = tmpPlans;
        this.chainOuterIds = new IntList();
        this.tmpColumnIds = tmpIndexes;
        this.substitution = substitution;
        this.outerColumnReads = outerColumnReads;
        mapping = new OuterColumnMapping(tmpValues, tmpKeys);
        ctx = new DecorrelationContext(context, planNodes, characterStore, mapping, chain, tmpIndexes, tmpSlaveKeys,
                tmpSchema, substitution, takenNames, outerColumnReads, verificationCarriers);
        domains = new DecorrelationDomains(ctx, mapping, context, planNodes, tmpSteps);
        final IntList ids = new IntList();
        keys = new CorrelationKeys(ctx, mapping, context, tmpPairs, ids);
        carriers = new OuterJoinCarriers(ctx, domains, keys, mapping, context, planNodes, substitution, ids);
        compensation = new ScalarCompensation(ctx, domains, mapping, context, planNodes, chain, tmpIndexes, substitution, outerColumnReads, consumerRemap);
        rewriter = new CorrelatedChainRewriter(ctx, keys, compensation, mapping, context, planNodes, chain, substitution);
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) throws SqlException {
        mapping.clear();
        ctx.endStep(0);
        tmpColumnIds.clear();
        return decorrelate(plan);
    }

    @Override
    public void clear() {
        chainOuterIds.clear();
        liftedSelections.clear();
        branchTop = null;
        pendingJoin = null;
        readBody = null;
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

    void verify(LogicalPlan plan, PlanVerifier verifier) {
        try {
            verifier.verifyDecorrelation(plan, getName(), verificationCarriers);
        } finally {
            verificationCarriers.clear();
        }
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

    private static boolean isChainNode(LogicalPlan node, boolean hasProject, boolean hasFilter) {
        return node instanceof UnaryPlan unary && !(hasProject && node instanceof ProjectPlan)
                && (!hasFilter || CorrelatedChainRewriter.isCommutingWithFilter(unary));
    }

    private static boolean isLiftable(BoundExpression expression) {
        return expression.walk(UNLIFTABLE_NODES);
    }

    private static boolean isLiftableChainNode(LogicalPlan node) {
        return node instanceof ProjectPlan || node instanceof FilterPlan || node instanceof SortPlan;
    }

    private void addUnmappedOuterId(int outerId, int lo, int hi) {
        if (ctx.isMasterOuter(outerId) && mapping.mappedColumn(outerId, lo, hi) < 0) {
            domains.coverOuter(outerId);
        }
    }

    /**
     * Projects a set-operation branch so it ends with one column for each outer column of
     * {@link DecorrelationDomains#outerIds}, in that order: the first column the mapping between {@code lo} and
     * {@code hi} maps it to.
     */
    private ProjectPlan alignBranch(LogicalPlan branch, int lo, int hi, int position) {
        final ProjectPlan project = planNodes.projects.next().of(branch, position);
        final OutputSchema input = branch.getOutput();
        for (int i = 0, n = input.getColumnCount(); i < n; i++) {
            final int columnId = input.getColumnId(i);
            if (mapping.mappedIndex(columnId, lo, hi) < 0) {
                project.getExpressions().add(planNodes.columns.next().of(columnId, input.getColumnType(i), position));
                project.getOutput().add(context.newColumnId(), input.getColumnName(i), input.getColumnType(i), input.getMetadata(i), input.isVisible(i));
            }
        }
        for (int k = 0, m = domains.outerIds().size(); k < m; k++) {
            final int outerId = domains.outerIds().getQuick(k);
            ctx.exposeColumn(project, input, mapping.mappedColumn(outerId, lo, hi), ctx.outerRefName(outerId), position);
        }
        return project;
    }

    /**
     * Collects the outer columns the block reads above {@code chainOuterBase}: those its chain and source join
     * read, and those of its deferred nullable inputs, which the block satisfies itself.
     */
    private void collectChainOuterIds(LogicalPlan source, int chainBase, int chainOuterBase, int deferredBase) {
        chainOuterIds.setPos(chainOuterBase);
        for (int i = chainBase, n = chain.size(); i < n; i++) {
            outerColumnReads.collect(chain.getQuick(i), chainOuterIds);
        }
        if (source instanceof JoinPlan join) {
            outerColumnReads.collect(join, chainOuterIds);
        }
        keys.collectDeferredOuterIds(deferredBase, chainOuterIds);
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
        compensation.remapConsumer(plan);
        if (domains.hasDecorrelatedSteps()) {
            ctx.realign(plan);
        }
        return plan instanceof JoinPlan join && hasDependentStep(join) ? decorrelateJoin(join) : result;
    }

    /**
     * Rewrites one block of the body: the chain from {@code top} down to the block's source. Below a filter, the
     * chain continues only through the nodes that commute with it.
     */
    private LogicalPlan decorrelateBlock(LogicalPlan top, boolean isBodyTop, int base) throws SqlException {
        final int chainBase = chain.size();
        LogicalPlan node = top;
        boolean hasProject = false;
        boolean hasFilter = false;
        while (isChainNode(node, hasProject, hasFilter)) {
            hasProject |= node instanceof ProjectPlan;
            hasFilter |= node instanceof FilterPlan;
            chain.add(node);
            node = node.inputAt(0);
        }
        final boolean isBranch = top == branchTop;
        final int chainOuterBase = chainOuterIds.size();
        final int deferredBase = keys.deferredMark();
        final int drivenBase = compensation.drivenMark();
        final int uncompensatedBase = compensation.uncompensatedMark();
        final ProjectPlan consumer = compensation.drivingConsumer(chainBase, hasProject);
        try {
            LogicalPlan source = decorrelateSource(node, base, consumer);
            collectChainOuterIds(source, chainBase, chainOuterBase, deferredBase);
            source = satisfyOuterColumns(source, chainBase, chainOuterBase, isBodyTop, isBranch, base, deferredBase);
            if (source instanceof JoinPlan join) {
                keys.keyDeferredInputs(join, deferredBase, base);
                compensation.compensateDrivenInputs(join, drivenBase, consumer);
                final int keyedBase = tmpColumnIds.size();
                for (int i = 1, n = join.getInputs().size(); i < n; i++) {
                    if (hasOuterCondition(join.getInputs().getQuick(i))) {
                        tmpColumnIds.add(i);
                    }
                }
                ctx.remapMapped(join, base);
                for (int i = keyedBase, n = tmpColumnIds.size(); i < n; i++) {
                    keys.keyOuterConditions(join, join.getInputs().getQuick(tmpColumnIds.getQuick(i)));
                }
                tmpColumnIds.setPos(keyedBase);
            }
            final boolean isUncompensated = isBodyTop && !isBodyReadLater(ctx.master(), ctx.master().getInputs().getQuick(ctx.stepIndex()));
            return rewriter.rewriteChain(source, chainBase, base, isUncompensated, isBranch);
        } finally {
            chain.setPos(chainBase);
            chainOuterIds.setPos(chainOuterBase);
            keys.truncateDeferred(deferredBase);
            compensation.truncateDriven(drivenBase, uncompensatedBase);
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
        if (compensation.hasCompensations()) {
            compensation.hoistCompensatedConjuncts(join);
            pendingJoin = join;
        }
        return join;
    }

    /**
     * Decorrelates each branch of the set operation and joins it, when it lacks an outer column the operation reads, to
     * the domain of those columns, keyed on its own, see {@link DecorrelationDomains#joinMissing}: the left branch before
     * the right one, which then reads that domain. Both end with one column per outer column, which the operation's
     * output carries hidden. When the operation releases its right branch before it reads the left one, see
     * {@link PlanToFactoryRules#isRightReleasedFirst}, the domains the right branch builds serve only that branch.
     */
    private LogicalPlan decorrelateSetOperation(SetOperationPlan operation, int base) throws SqlException {
        final int enclosingOperation = domains.beginOperation(operation);
        try {
            final int leftBase = mapping.mark();
            operation.replaceInput(0, decorrelateBranch(operation.getLeft(), leftBase));
            operation.replaceInput(0, domains.joinMissing(operation.getLeft(), leftBase, operation.getPosition()));
            final int rightBase = mapping.mark();
            final int rightProducerBase = domains.producerMark();
            operation.replaceInput(1, decorrelateBranch(operation.getRight(), rightBase));
            operation.replaceInput(1, domains.joinMissing(operation.getRight(), rightBase, operation.getRightPosition()));
            if (PlanToFactoryRules.isRightReleasedFirst(operation)) {
                domains.retireProducers(rightProducerBase);
            }
            final int rightEnd = mapping.mark();
            domains.coverMappedMaster(leftBase, rightEnd);
            operation.replaceInput(0, alignBranch(operation.getLeft(), leftBase, rightBase, operation.getPosition()));
            operation.replaceInput(1, alignBranch(operation.getRight(), rightBase, rightEnd, operation.getRightPosition()));
            mapping.reset(base);
            final OutputSchema output = operation.getOutput();
            final OutputSchema leftOutput = operation.getLeft().getOutput();
            SetOperationCasts.recordCasts(operation);
            for (int i = output.getColumnCount(), k = 0, n = leftOutput.getColumnCount(); i < n; i++, k++) {
                final int columnId = context.newColumnId();
                output.add(columnId, leftOutput.getColumnName(i), operation.getCastTypes().getQuick(i), false);
                mapping.push(domains.outerIds().getQuick(k), columnId);
            }
            return operation;
        } finally {
            domains.endOperation(enclosingOperation);
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
                final int lastMasterNulling = LogicalPlans.lastMasterNullingStep(join);
                for (int i = 0, n = join.getInputs().size(); i < n; i++) {
                    final JoinInput input = join.getInputs().getQuick(i);
                    if (input.getInput() != null && hasMasterOuterColumn(input.getInput())) {
                        final int inputBase = mapping.mark();
                        final ProjectPlan driven = i > 0 && consumer != null && LogicalPlans.orderedSteps(join).indexOf(input) > lastMasterNulling
                                ? compensation.drivenScalar(join, input) : null;
                        if (driven != null) {
                            compensation.addDriven(input, driven);
                        } else if (input.getInput() instanceof ProjectPlan project && compensation.isScalarProjection(project)
                                && input.getJoinType() == JoinKind.INNER && rejectsZeroCount(input.getOnResidual(), project)) {
                            compensation.keepUncompensated(project);
                        }
                        input.setInput(decorrelateBlock(input.getInput(), false, inputBase));
                        if (driven != null && compensation.scalarAggregateBelow(driven) != null) {
                            compensation.exposeCarriers(driven);
                            input.setJoinType(JoinKind.LEFT_OUTER);
                        }
                        if (lastMasterNulling > -1) {
                            keys.deferMapping(input, inputBase);
                        } else {
                            keys.joinMappedInput(join, input, inputBase, base);
                            if (i > 0 && input.getJoinType().isSlaveNulling()) {
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
            case WindowJoinPlan _, HorizonJoinPlan _ -> {
                decorrelateTemporalJoin(source, base);
                return source;
            }
            case UnaryPlan block -> {
                return decorrelateBlock(block, false, base);
            }
            case ScanPlan _, FunctionSourcePlan _, DomainReadPlan _ ->
                    throw SqlException.$(source.getPosition(), SOURCE_CORRELATION);
        }
    }

    private void decorrelateStep(JoinPlan join, int index) throws SqlException {
        final JoinInput step = join.getInputs().getQuick(index);
        final int uncompensatedBase = compensation.uncompensatedMark();
        final int scalarAggregateBase = compensation.scalarMark();
        final int masterBase = ctx.masterOuterIds().size();
        ctx.beginStep(join, index);
        domains.beginStep();
        for (int i = 1, n = join.getInputs().size(); i < n; i++) {
            keys.collectOuterAliases(join.getInputs().getQuick(i).getPostJoinFilter(), join, index);
        }
        try {
            ctx.collectMasterOuterIds(step.getInput());
            step.setDependent(false);
            if (ctx.masterOuterIds().size() == masterBase) {
                return;
            }
            domains.addDecorrelatedStep(step);
            ctx.pickOuterRefSequence(step.getInput(), join);
            compensation.beginStep();
            liftedConjuncts.clear();
            liftedSelectionIds.clear();
            liftedSelections.clear();
            markedSelectionIds.clear();
            final LimitPlan limit = step.getInput() instanceof LimitPlan l ? l : null;
            final BoundExpression limitLo = limit == null ? null : limit.getLo();
            final BoundExpression limitHi = limit == null ? null : limit.getHi();
            final boolean isLeft = step.getJoinType() == JoinKind.LEFT_OUTER;
            final int conditionKeyCount = step.getKeys().size();
            final boolean isTrivialCondition = conditionKeyCount == 0 && isTrue(step.getOnResidual());
            final ProjectPlan rejecting = isLeft ? null : compensation.zeroRejectingScalar(step.getInput());
            final ProjectPlan wrapped = isLeft || !isTrivialCondition || rejecting != null || isBodyReadLater(join, step) ? null
                    : compensation.wrappedScalar(step.getInput());
            if (rejecting != null || wrapped != null) {
                compensation.keepUncompensated(rejecting != null ? rejecting : wrapped);
            }
            final int base = mapping.mark();
            final LogicalPlan body = decorrelateBlock(step.getInput(), true, base);
            step.setInput(body);
            final ProjectPlan scalarBody = wrapped != null ? compensation.wrappedBody(body, wrapped) : compensation.scalarBody(body);
            if (scalarBody != null && wrapped == null) {
                compensation.exposeCarriers(scalarBody);
                ctx.realignAbove(body, scalarBody);
            }
            final OutputSchema output = body.getOutput();
            for (int i = base, n = mapping.mark(); i < n; i++) {
                final int columnId = mapping.columnIdAt(i);
                final int masterId = ctx.masterColumn(mapping.outerIdAt(i));
                final int keyIndex = scalarBody == null ? step.indexOfMasterKey(masterId) : -1;
                ctx.addCarrier(step, columnId);
                if (keyIndex > -1) {
                    context.addKeyFilter(step, keyIndex, columnId, output, step.getPosition());
                } else {
                    step.addKey(masterId, columnId, ctx.masterColumnName(mapping.outerIdAt(i)),
                            ctx.qualifiedName(step.getBindingAlias(), output.getColumnName(output.getColumnIndexById(columnId))), step.getPosition());
                }
            }
            if (liftedConjuncts.size() > 0 || liftedSelections.size() > 0) {
                liftAboveStep(join, step, body, masterBase);
            }
            join.addMissingInputColumns();
            if (scalarBody != null) {
                final BoundExpression guard = limit == null ? null : compensation.limitGuard(ctx.masterExpression(limitLo), ctx.masterExpression(limitHi), join.getOutput());
                if (isLeft) {
                    compensation.compensateStep(step, scalarBody, base, guard, keys.stepCondition(join, step, conditionKeyCount));
                } else {
                    if (!compensation.isEmptyRowRejected(step, scalarBody)) {
                        final BoundExpression condition = keys.stepCondition(join, step, conditionKeyCount);
                        step.setJoinType(JoinKind.LEFT_OUTER);
                        compensation.compensateStep(step, scalarBody, base, null, null);
                        filterStep(step, condition);
                    }
                    filterStep(step, guard);
                }
            }
            filterStep(step, compensation.takeBoundChecks());
            mapping.reset(base);
        } finally {
            ctx.endStep(masterBase);
            compensation.endStep(uncompensatedBase, scalarAggregateBase);
        }
    }

    /**
     * Rewrites a window or horizon join: its master as a block, then each slave that reads outer columns. A slave joins
     * the master on the columns that satisfy the outer columns it maps: the master's, or those of a domain crossed with
     * the master, which the join's own reads of outer columns read as well. A window join step filters on their
     * equality; a horizon join slave keys on them.
     */
    private void decorrelateTemporalJoin(LogicalPlan join, int base) throws SqlException {
        join.replaceInput(0, decorrelateBlock(join.inputAt(0), false, base));
        final int slaveBase = mapping.mark();
        for (int i = 1, n = join.inputCount(); i < n; i++) {
            if (hasMasterOuterColumn(join.inputAt(i))) {
                join.replaceInput(i, decorrelateBlock(join.inputAt(i), false, mapping.mark()));
            }
        }
        final int domainBase = mapping.mark();
        domains.resetOuterIds();
        for (int i = slaveBase; i < domainBase; i++) {
            addUnmappedOuterId(mapping.outerIdAt(i), base, slaveBase);
        }
        final int readBase = tmpColumnIds.size();
        outerColumnReads.collect(join, tmpColumnIds);
        for (int i = readBase, n = tmpColumnIds.size(); i < n; i++) {
            addUnmappedOuterId(tmpColumnIds.getQuick(i), base, slaveBase);
        }
        tmpColumnIds.setPos(readBase);
        final int position = join.getPosition();
        if (domains.outerIds().size() > 0) {
            join.replaceInput(0, domains.crossDomain(join.inputAt(0), domains.buildDomain(position), position));
        }
        ctx.realign(join);
        for (int i = slaveBase; i < domainBase; i++) {
            final int outerId = mapping.outerIdAt(i);
            final int slaveId = mapping.columnIdAt(i);
            final int masterId = mapping.mappedColumn(outerId, base, slaveBase) > -1 ? mapping.mappedColumn(outerId, base, slaveBase)
                    : mapping.mappedColumn(outerId, domainBase, mapping.mark());
            int s = 1;
            while (join.inputAt(s).getOutput().getColumnIndexById(slaveId) < 0) {
                s++;
            }
            switch (join) {
                case WindowJoinPlan windowJoin -> {
                    final WindowJoinStep step = windowJoin.getSteps().getQuick(s - 1);
                    final OutputSchema scope = step.getScope();
                    final BoundExpression equality = context.bindCall("=", position, ctx.column(scope, slaveId, position), ctx.column(scope, masterId, position), scope);
                    step.setFilter(step.getFilter() == null ? equality : context.getRewriter().combineConjunction(step.getFilter(), equality, position));
                }
                case HorizonJoinPlan horizon -> {
                    final HorizonJoinSlave slave = horizon.getSlaves().getQuick(s - 1);
                    slave.addKey(masterId, slaveId, slave.getPosition());
                }
                default -> throw new IllegalStateException("not a window or horizon join");
            }
        }
        for (int i = domainBase - 1; i >= slaveBase; i--) {
            mapping.removeAt(i);
        }
        ctx.remapMapped(join, base);
    }

    private void exposeLiftedColumns(ProjectPlan project, BoundExpression expression) {
        final int columnBase = tmpColumnIds.size();
        ctx.collectColumnIds(expression, tmpColumnIds);
        final OutputSchema output = project.getOutput();
        for (int i = columnBase, n = tmpColumnIds.size(); i < n; i++) {
            final int columnId = tmpColumnIds.getQuick(i);
            if (substitution.keyIndex(columnId) < 0) {
                continue;
            }
            int exposed = -1;
            for (int k = 0, m = project.getExpressions().size(); k < m && exposed < 0; k++) {
                if (output.isVisible(k) && project.getExpressions().getQuick(k) instanceof ColumnExpression column && column.getColumnId() == columnId && !column.isCast()) {
                    exposed = output.getColumnId(k);
                }
            }
            if (exposed < 0) {
                final OutputSchema input = project.getChild().getOutput();
                final int type = input.getColumnType(input.getColumnIndexById(columnId));
                project.getExpressions().add(planNodes.columns.next().of(columnId, type, project.getPosition()));
                exposed = context.newColumnId();
                output.add(exposed, liftedName(output, input.getColumnName(input.getColumnIndexById(columnId))), type, false);
            }
            substitution.put(columnId, exposed);
        }
        tmpColumnIds.setPos(columnBase);
    }

    /**
     * Adds the condition, unless it holds on every row, to the post-join filter of the step.
     */
    private void filterStep(JoinInput step, BoundExpression condition) throws SqlException {
        if (!isTrue(condition)) {
            step.setPostJoinFilter(context.getRewriter().combineConjunction(step.getPostJoinFilter(), condition, condition.getPosition()));
        }
    }

    private int findBodyOuterColumn(LogicalPlan plan) {
        final int base = tmpColumnIds.size();
        outerColumnReads.collect(plan, tmpColumnIds);
        boolean isFound = false;
        for (int i = base, n = tmpColumnIds.size(); i < n && !isFound; i++) {
            isFound = readBody.getColumnIndexById(tmpColumnIds.getQuick(i)) > -1;
        }
        tmpColumnIds.setPos(base);
        return isFound ? TreeWalk.STOP : TreeWalk.CONTINUE;
    }

    private int findMasterOuterColumn(LogicalPlan plan) {
        final int base = tmpColumnIds.size();
        outerColumnReads.collect(plan, tmpColumnIds);
        boolean isFound = false;
        for (int i = base, n = tmpColumnIds.size(); i < n && !isFound; i++) {
            isFound = ctx.isMasterOuter(tmpColumnIds.getQuick(i));
        }
        tmpColumnIds.setPos(base);
        return isFound ? TreeWalk.STOP : TreeWalk.CONTINUE;
    }

    private boolean hasDecorrelatedMasterReference(int chainOuterBase) {
        for (int i = chainOuterBase, n = chainOuterIds.size(); i < n; i++) {
            final int outerId = chainOuterIds.getQuick(i);
            if (ctx.isMasterOuter(outerId) && domains.isDecorrelated(ctx.master().getInputs().getQuick(ctx.masterInput(outerId)))) {
                return true;
            }
        }
        return false;
    }

    private boolean hasMasterOuterColumn(LogicalPlan plan) {
        return !plan.walkTopDown(masterOuterReads);
    }

    /**
     * True when a step after {@code step} reads a column of its body through its join keys, key filter or ON
     * condition, or as an outer column of its own body: such a read precedes the projection over the join, so the
     * body restores its own values over no rows.
     */
    private boolean isBodyReadLater(JoinPlan join, JoinInput step) {
        final OutputSchema body = step.getInput().getOutput();
        final ObjList<JoinInput> steps = LogicalPlans.orderedSteps(join);
        for (int i = steps.indexOf(step) + 1, n = steps.size(); i < n; i++) {
            final JoinInput input = steps.getQuick(i);
            final ObjList<JoinKey> stepKeys = input.getKeys();
            for (int k = 0, m = stepKeys.size(); k < m; k++) {
                if (body.getColumnIndexById(stepKeys.getQuick(k).getMasterColumnId()) > -1) {
                    return true;
                }
            }
            if (ctx.readsAnyColumn(input.getOnResidual(), body) || ctx.readsAnyColumn(input.getKeyFilter(), body)) {
                return true;
            }
            readBody = body;
            final boolean isRead = input.isDependent() && input.getInput() != null && !input.getInput().walkTopDown(bodyOuterReads);
            readBody = null;
            if (isRead) {
                return true;
            }
        }
        return false;
    }

    /**
     * True when every key of the join's steps that reads the column can join its step's ON condition instead:
     * the step is an inner or left join that keeps a key on columns other than the body's, or the step being
     * decorrelated, which decorrelation keys by the mapped columns.
     */
    private boolean isKeyReadMovable(int columnId, OutputSchema body, int base) {
        final ObjList<JoinInput> inputs = ctx.master().getInputs();
        for (int i = 1, n = inputs.size(); i < n; i++) {
            final JoinInput input = inputs.getQuick(i);
            if (!input.hasKeyOn(columnId)) {
                continue;
            }
            if (input.getJoinType() != JoinKind.INNER && input.getJoinType() != JoinKind.LEFT_OUTER || input.getKeyFilter() != null) {
                return false;
            }
            boolean hasOtherKey = i == ctx.stepIndex() && mapping.mark() > base;
            for (int k = 0, m = input.getKeys().size(); k < m && !hasOtherKey; k++) {
                final JoinKey key = input.getKeys().getQuick(k);
                hasOtherKey = body.getColumnIndexById(key.getMasterColumnId()) < 0 && body.getColumnIndexById(key.getSlaveColumnId()) < 0;
            }
            if (!hasOtherKey) {
                return false;
            }
        }
        return true;
    }

    /**
     * True when the projection column computes a value over outer columns no equality satisfies that the step
     * can compute above the join instead, with each join key that reads it joining its step's ON condition.
     */
    private boolean isLiftableSelection(ProjectPlan project, int index, int base) {
        final OutputSchema output = project.getOutput();
        final BoundExpression expression = project.getExpressions().getQuick(index);
        return output.isVisible(index) && !(expression instanceof OuterColumnExpression) && ctx.readsUnmappedOuter(expression, base)
                && isLiftable(expression) && hasNullableOuterTypes(expression) && isKeyReadMovable(output.getColumnId(index), output, base);
    }

    /**
     * True when the step's rows can be null-extended and the expression need not be NULL over a null-extended
     * row: it is not null-propagating arithmetic over a column of the projection's input.
     */
    private boolean isNonNullOverExtendedRow(ProjectPlan project, BoundExpression expression) {
        return (ctx.master().getInputs().getQuick(ctx.stepIndex()).getJoinType() == JoinKind.LEFT_OUTER || ctx.isStepNullExtendedLater())
                && !(isNullPropagating(expression) && ctx.readsAnyColumn(expression, project.getChild().getOutput()));
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
     * master's columns for the outer columns and the body's exposed columns for its own; the ON conditions of
     * the join read the lifted columns' values. A lifted column that is not NULL over a null-extended row reads
     * NULL where the body's row marker reads FALSE, which the step's own ON condition never sees.
     */
    private void liftAboveStep(JoinPlan join, JoinInput step, LogicalPlan body, int masterBase) throws SqlException {
        LogicalPlan node = body;
        while (!(node instanceof ProjectPlan project)) {
            node = node.inputAt(0);
        }
        substitution.clear();
        for (int i = masterBase, n = ctx.masterOuterIds().size(); i < n; i++) {
            substitution.put(ctx.masterOuterIds().getQuick(i), ctx.masterColumn(ctx.masterOuterIds().getQuick(i)));
        }
        for (int i = 0, n = liftedConjuncts.size(); i < n; i++) {
            exposeLiftedColumns(project, liftedConjuncts.getQuick(i));
        }
        for (int i = 0, n = liftedSelections.size(); i < n; i++) {
            exposeLiftedColumns(project, liftedSelections.getQuick(i));
        }
        final int markerId = markedSelectionIds.size() > 0 ? ctx.markRows(project, project.getPosition()) : -1;
        ctx.realignAbove(body, project);
        join.addMissingInputColumns();
        for (int i = 0, n = liftedConjuncts.size(); i < n; i++) {
            final BoundExpression conjunct = context.getRewriter().remapColumns(liftedConjuncts.getQuick(i), substitution);
            step.setOnResidual(step.getOnResidual() == null ? conjunct : context.getRewriter().combineConjunction(step.getOnResidual(), conjunct, conjunct.getPosition()));
        }
        final OutputSchema output = project.getOutput();
        for (int i = 0, n = liftedSelections.size(); i < n; i++) {
            final int columnId = liftedSelectionIds.getQuick(i);
            final int index = output.getColumnIndexById(columnId);
            final BoundExpression value = context.getRewriter().remapColumns(liftedSelections.getQuick(i), substitution);
            final int position = value.getPosition();
            final BoundExpression lifted = markedSelectionIds.contains(columnId)
                    ? ctx.caseWhen(ctx.column(join.getOutput(), markerId, position), value, planNodes.constants.next().ofNull(position), join.getOutput(), position)
                    : value;
            for (int s = 1, m = join.getInputs().size(); s < m; s++) {
                final JoinInput input = join.getInputs().getQuick(s);
                if (input.getOnResidual() != null) {
                    input.setOnResidual(context.getRewriter().substituteColumn(input.getOnResidual(), columnId, input == step ? value : lifted));
                }
            }
            compensation.addCompensation(columnId, output.getColumnName(index), lifted);
            project.getExpressions().remove(index);
            output.remove(index);
            join.getOutput().remove(join.getOutput().getColumnIndexById(columnId));
        }
        ctx.realignAbove(body, project);
    }

    /**
     * Moves the conjuncts of the body's WHERE and the computed columns of its projection that read outer
     * columns no equality satisfies above the step: the conjuncts join its ON, the columns a projection over
     * the join computes, and the join keys that read such a column join their steps' ON conditions. The outer
     * columns they read are then no longer the body's.
     */
    private void liftOuterTerms(FilterPlan filter, LogicalPlan source, int chainBase, int chainOuterBase, int deferredBase, int base) throws SqlException {
        liftBase = base;
        if (filter != null) {
            final BoundExpression remaining = context.getRewriter().retainConjuncts(filter.getPredicate(), bodyConjuncts);
            filter.of(filter.getChild(), remaining != null ? remaining : planNodes.constants.next().ofBoolean(true, filter.getPosition()), filter.getPosition());
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
        for (int i = chainBase, n = chain.size(); i < n; i++) {
            if (chain.getQuick(i) instanceof ProjectPlan project) {
                final ObjList<BoundExpression> expressions = project.getExpressions();
                for (int k = 0, m = expressions.size(); k < m; k++) {
                    if (isLiftableSelection(project, k, base)) {
                        final BoundExpression expression = expressions.getQuick(k);
                        final int columnId = project.getOutput().getColumnId(k);
                        keys.moveKeyReads(ctx.master(), columnId);
                        if (isNonNullOverExtendedRow(project, expression)) {
                            markedSelectionIds.add(columnId);
                        }
                        liftedSelectionIds.add(columnId);
                        liftedSelections.add(expression);
                        expressions.setQuick(k, planNodes.constants.next().ofNull(expression.getPosition()));
                    }
                }
            }
        }
        collectChainOuterIds(source, chainBase, chainOuterBase, deferredBase);
    }

    private CharSequence liftedName(OutputSchema output, CharSequence name) {
        if (!output.hasColumnName(name)) {
            return name;
        }
        final CharacterStoreEntry entry = ctx.sequencedName("__qdb_lifted_");
        entry.put('_').put(name);
        return entry.toImmutable();
    }

    /**
     * Satisfies the outer columns the block chain reads and the mapping does not: by WHERE equalities when
     * the block allows it, otherwise by a domain joined to the block source. A set-operation branch allows it only
     * when it restores its values over no rows, see {@link ScalarCompensation#isScalarChain}, from a domain that
     * covers every outer column the operation reads; so does the domain of any other branch that its WHERE
     * equalities key on each of those columns, see {@link DecorrelationDomains#coverOperation}.
     */
    private LogicalPlan satisfyOuterColumns(LogicalPlan source, int chainBase, int chainOuterBase, boolean isBodyTop, boolean isBranch, int base,
                                            int deferredBase) throws SqlException {
        FilterPlan filter = null;
        boolean isLiftable = isBodyTop && !(source instanceof SetOperationPlan);
        for (int i = chainBase, n = chain.size(); i < n; i++) {
            final LogicalPlan node = chain.getQuick(i);
            if (node instanceof FilterPlan f && filter == null) {
                filter = f;
            }
            isLiftable &= isLiftableChainNode(node);
        }
        keys.clearDroppedEqualities();
        final int equatedBase = mapping.mark();
        if (!isBranch || compensation.isScalarChain(chainBase)) {
            final JoinPlan sourceJoin = source instanceof JoinPlan join ? join : null;
            if (filter != null) {
                keys.collectEqualities(filter.getPredicate(), filter.getChild().getOutput(), sourceJoin, base);
            }
            if (sourceJoin != null) {
                for (int i = 1, n = sourceJoin.getInputs().size(); i < n; i++) {
                    keys.collectEqualities(sourceJoin.getInputs().getQuick(i).getPostJoinFilter(), sourceJoin.getOutput(), sourceJoin, base);
                }
            }
            if (isLiftable || keys.isEveryOuterColumnEquated(chainOuterIds, chainOuterBase, base)) {
                if (source instanceof JoinPlan join) {
                    for (int i = 1, n = join.getInputs().size(); i < n; i++) {
                        final JoinInput input = join.getInputs().getQuick(i);
                        input.setPostJoinFilter(keys.dropEqualities(input.getPostJoinFilter()));
                    }
                }
                for (int i = 0, n = keys.droppedEqualityCount(); i < n; i++) {
                    mapping.push(keys.droppedEqualityOuterId(i), keys.droppedEqualityColumnId(i));
                }
            } else {
                keys.clearDroppedEqualities();
            }
        }
        if (isLiftable && (mapping.mark() > base || hasDecorrelatedMasterReference(chainOuterBase))) {
            liftOuterTerms(filter, source, chainBase, chainOuterBase, deferredBase, base);
        }
        if (source instanceof JoinPlan join && LogicalPlans.lastMasterNullingStep(join) < 0 && keys.deferUncarriedMappings(join, base, equatedBase)) {
            collectChainOuterIds(source, chainBase, chainOuterBase, deferredBase);
        }
        domains.resetOuterIds();
        for (int i = chainOuterBase, n = chainOuterIds.size(); i < n; i++) {
            final int outerId = chainOuterIds.getQuick(i);
            if (ctx.isMasterOuter(outerId) && mapping.mappedColumn(outerId, base, mapping.mark()) < 0) {
                domains.coverOuter(outerId);
            }
        }
        if (source instanceof JoinPlan join && LogicalPlans.lastMasterNullingStep(join) > -1
                && (domains.outerIds().size() > 0 || keys.deferredMark() > deferredBase)) {
            return carriers.place(join, base, deferredBase);
        }
        if (domains.outerIds().size() == 0) {
            return source;
        }
        final int start = keys.droppedEqualityCount();
        if (filter != null && !(source instanceof JoinPlan)) {
            keys.collectEqualities(filter.getPredicate(), source.getOutput(), null, base);
        }
        if (isBranch && !(source instanceof JoinPlan) && keys.isEveryOuterColumnEquated(domains.outerIds(), 0, base)) {
            domains.coverOperation();
        }
        final LogicalPlan domain = domains.buildDomain(source.getPosition());
        if (!(source instanceof JoinPlan join)) {
            final JoinPlan crossed = domains.crossDomain(source, domain, source.getPosition());
            final JoinInput domainStep = crossed.getInputs().getQuick(1);
            final OutputSchema domainOutput = domain.getOutput();
            for (int i = start, n = keys.droppedEqualityCount(); i < n; i++) {
                final int columnId = keys.droppedEqualityColumnId(i);
                final int domainId = mapping.mappedColumn(keys.droppedEqualityOuterId(i), base, mapping.mark());
                domainStep.addKey(columnId, domainId, source.getOutput().getColumnName(source.getOutput().getColumnIndexById(columnId)),
                        domainOutput.getColumnName(domainOutput.getColumnIndexById(domainId)), domainStep.getPosition());
            }
            return crossed;
        }
        if (keys.deferredMark() > deferredBase || LogicalPlans.hasBarrierInput(join)) {
            domains.crossInto(join, join.getOrderedInputs().getQuick(0), domain, source.getPosition());
        } else {
            final int inputCount = join.getInputs().size();
            final JoinInput first = join.getOrderedInputs().getQuick(0);
            final JoinInput step = domains.insertDomainStep(join, domain, join.getOrderedInputs().size(), source.getPosition());
            for (int i = 0; i < inputCount; i++) {
                final JoinInput input = join.getInputs().getQuick(i);
                if (input != first) {
                    input.setPostJoinFilter(domains.moveDomainConjuncts(input.getPostJoinFilter(), step));
                    input.setOnResidual(domains.moveDomainConjuncts(input.getOnResidual(), step));
                }
            }
        }
        return join;
    }

}
