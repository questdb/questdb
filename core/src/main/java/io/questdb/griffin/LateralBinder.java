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
import io.questdb.griffin.BindContext.LateralScope;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.std.Chars;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

import static io.questdb.griffin.BindContext.copyCorrelatedAliases;
import static io.questdb.griffin.BindContext.findGroupingColumn;
import static io.questdb.griffin.BindContext.getColumnIndexQuiet;
import static io.questdb.griffin.BindContext.hasLiteral;
import static io.questdb.griffin.BindContext.isPlainColumnProjection;
import static io.questdb.griffin.BindContext.isTrivialCondition;
import static io.questdb.griffin.BindContext.isWildcard;
import static io.questdb.griffin.BindContext.joinColumnSource;
import static io.questdb.griffin.BindContext.sourceAlias;
import static io.questdb.griffin.AggregateBinder.hasAggregateReference;
import static io.questdb.griffin.SqlBinder.selectedLiteralIndex;
import static io.questdb.griffin.JoinBinder.addJoinKey;
import static io.questdb.griffin.OrderBinder.orderThroughProject;
import static io.questdb.griffin.OrderBinder.projectsSortColumns;

final class LateralBinder implements Mutable {
    static final String OUTER_REF_PREFIX = "__qdb_outer_ref__";
    private static final String COUNT_DRIVER_PREFIX = "__qdb_count_driver__";
    final IntList deferredCorrelationAliases = new IntList();
    final IntList deferredCorrelationInputs = new IntList();
    final IntList lateralCarrierIds = new IntList();
    final IntList lateralEqualityInputs = new IntList();
    final IntList lateralEqualityMasterIds = new IntList();
    final ObjList<CharSequence> lateralEqualityMasterNames = new ObjList<>();
    final IntList lateralEqualityPositions = new IntList();
    final IntList lateralEqualitySlaveIds = new IntList();
    final ObjList<CharSequence> lateralEqualitySlaveNames = new ObjList<>();
    final IntList lateralGuardInputs = new IntList();
    final IntList lateralLiftedInputs = new IntList();
    final IntList lateralWrappedCountColumns = new IntList();
    final IntList liftedSelectionColumns = new IntList();
    final ObjList<ExpressionNode> liftedSelectionTemplates = new ObjList<>();
    private final AggregateBinder aggregateBinder;
    private final SqlBinder binder;
    private final ObjList<ExpressionNode> compensationTemplates = new ObjList<>();
    private final BindContext ctx;
    private final IntList domainReferences = new IntList();
    private final IntList domainScopes = new IntList();
    private final ObjList<ExpressionNode> driverNodes = new ObjList<>();
    private final IntList driverReferences = new IntList();
    private final IntList eliminableReferences = new IntList();
    private final ObjList<ExpressionNode> lateralLiftedPredicates = new ObjList<>();
    private final ObjectPool<LateralScope> lateralScopePool = new ObjectPool<>(LateralScope.FACTORY, 2);
    private final ObjList<LateralScope> lateralScopeStash = new ObjList<>();
    private final ObjList<CharSequence> lateralTemplateNames = new ObjList<>();
    private final ObjList<ExpressionNode> lateralTemplates = new ObjList<>();
    private final IntList liftedIndexes = new IntList();
    private final ObjList<CharSequence> liftedNames = new ObjList<>();
    private final OrderBinder orderBinder;
    private final IntList outerReferences = new IntList();
    private final IntList outerReferenceScopes = new IntList();
    private final ObjList<CharSequence> substitutedNames = new ObjList<>();
    private final ObjList<CharSequence> substitutedOuterNames = new ObjList<>();
    private final ObjList<CharSequence> substitutedQualifiers = new ObjList<>();
    private final WindowBinder windowBinder;
    LogicalPlan correlationDomain;
    JoinInput countDriver;
    int countDriverLimit;
    QueryModel countDriverSource;
    CharSequence domainAlias;
    boolean isCountDriverQualified;
    boolean isLateralScalarBody;
    CharSequence lateralBodyAlias;
    QueryModel lateralBodyModel;
    ExpressionNode lateralHoistedWhere;
    QueryModel lateralHoistSource;
    ExpressionNode lateralScalarGuard;
    private int carrierSequence;
    private ExpressionNode lateralLifted;
    private int lateralOuterRefId = -1;
    private int outerReferenceSequence;

    LateralBinder(
            BindContext ctx,
            SqlBinder binder,
            WindowBinder windowBinder,
            OrderBinder orderBinder,
            AggregateBinder aggregateBinder
    ) {
        this.ctx = ctx;
        this.binder = binder;
        this.windowBinder = windowBinder;
        this.orderBinder = orderBinder;
        this.aggregateBinder = aggregateBinder;
    }

    @Override
    public void clear() {
        lateralScopePool.clear();
        lateralScopeStash.clear();
        outerReferenceSequence = 0;
        lateralOuterRefId = -1;
        carrierSequence = 0;
        driverReferences.clear();
        driverNodes.clear();
        lateralTemplateNames.clear();
        substitutedNames.clear();
        substitutedOuterNames.clear();
        substitutedQualifiers.clear();
        lateralTemplates.clear();
        compensationTemplates.clear();
        lateralLiftedPredicates.clear();
        liftedIndexes.clear();
        liftedNames.clear();
        lateralLifted = null;
        domainReferences.clear();
        domainScopes.clear();
        outerReferenceScopes.clear();
        outerReferences.clear();
        domainAlias = null;
        countDriver = null;
        countDriverLimit = 0;
        countDriverSource = null;
        isCountDriverQualified = false;
        lateralCarrierIds.clear();
        deferredCorrelationAliases.clear();
        deferredCorrelationInputs.clear();
        lateralEqualityInputs.clear();
        lateralEqualityMasterIds.clear();
        lateralEqualityMasterNames.clear();
        lateralEqualityPositions.clear();
        lateralEqualitySlaveIds.clear();
        lateralEqualitySlaveNames.clear();
        lateralGuardInputs.clear();
        lateralLiftedInputs.clear();
        liftedSelectionColumns.clear();
        liftedSelectionTemplates.clear();
        correlationDomain = null;
        lateralBodyAlias = null;
        lateralBodyModel = null;
        isLateralScalarBody = false;
        lateralHoistSource = null;
        lateralHoistedWhere = null;
        lateralWrappedCountColumns.clear();
        lateralScalarGuard = null;
    }

    private static int correlatedAlias(OutputSchema output, int index) {
        for (int i = 0, n = output.getCorrelatedAliasCount(); i < n; i++) {
            if (output.getCorrelatedAliasIndex(i) == index) {
                return i;
            }
        }
        return -1;
    }

    private static boolean hasSourceAlias(QueryModel source, CharSequence alias) {
        for (int i = 0, n = source.getJoinModels().size(); i < n; i++) {
            if (Chars.equalsIgnoreCaseNc(alias, sourceAlias(source.getJoinModels().getQuick(i)))) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasUnprojectedTerm(ExpressionNode node, QueryModel model) {
        if (node == null) {
            return false;
        }
        if (node.queryModel != null || node.windowExpression != null) {
            return true;
        }
        if (node.type == ExpressionNode.LITERAL) {
            return selectedLiteralIndex(node, model) < 0;
        }
        if (hasUnprojectedTerm(node.lhs, model) || hasUnprojectedTerm(node.rhs, model)) {
            return true;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (hasUnprojectedTerm(node.args.getQuick(i), model)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasZeroOnEmptyAggregate(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        if (SqlUtil.isZeroOnEmptyAggregate(node)) {
            return true;
        }
        if (hasZeroOnEmptyAggregate(node.lhs) || hasZeroOnEmptyAggregate(node.rhs)) {
            return true;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (hasZeroOnEmptyAggregate(node.args.getQuick(i))) {
                return true;
            }
        }
        return false;
    }

    private static boolean isDriverReference(ExpressionNode node, OutputSchema master, OutputSchema scope) {
        return Chars.indexOfLastUnquoted(node.token, '.') >= 0 && FunctionBinder.findColumn(node, master, null) >= 0
                && FunctionBinder.findColumn(node, scope, null) < 0;
    }

    private static boolean isLateralMasterInput(LateralScope lateral, int input) {
        return QueryModel.isLateralJoin(lateral.source.getJoinModels().getQuick(input).getJoinType());
    }

    private static boolean isNullPropagating(ExpressionNode node) {
        if (node.type == ExpressionNode.LITERAL) {
            return true;
        }
        return node.type == ExpressionNode.OPERATION && node.paramCount == 2
                && (Chars.equals(node.token, '+') || Chars.equals(node.token, '-')
                || Chars.equals(node.token, '*') || Chars.equals(node.token, '/'))
                && isNullPropagating(node.lhs) && isNullPropagating(node.rhs);
    }


    private static boolean isScalarCountBody(QueryModel body) {
        if (body == null || body.getUnionModel() != null || body.getLimitLo() != null || body.getLimitHi() != null || body.isDistinct()
                || body.getGroupBy().size() > 0 || body.getSampleBy() != null || body.getLatestBy().size() > 0) {
            return false;
        }
        final ObjList<QueryColumn> columns = body.getBottomUpColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            if (hasZeroOnEmptyAggregate(columns.getQuick(i).getAst())) {
                return true;
            }
        }
        return false;
    }

    private static int mergeOwner(int left, int right) {
        if (left == Integer.MAX_VALUE || left == right) {
            return right;
        }
        return right == Integer.MAX_VALUE ? left : -1;
    }

    private static int precedingCorrelatedColumn(JoinPlan join, CharSequence qualifier, CharSequence name, int inputIndex) {
        final OutputSchema target = join.getOutput();
        for (int i = 0, n = target.getCorrelatedAliasCount(); i < n; i++) {
            final CharSequence alias = target.getCorrelatedAliasName(i);
            final int index = target.getCorrelatedAliasIndex(i);
            if ((qualifier == null || Chars.equalsIgnoreCaseNc(qualifier, target.getCorrelatedAliasQualifier(i)))
                    && Chars.equalsIgnoreCase(alias, name)
                    && joinColumnSource(join, target.getColumnId(index)) < inputIndex) {
                return index;
            }
        }
        return -1;
    }

    private static void renameTemplateSelf(ExpressionNode node, CharSequence selfName, CharSequence token) {
        if (node == null) {
            return;
        }
        if (node.type == ExpressionNode.LITERAL && Chars.equalsIgnoreCase(node.token, selfName)) {
            node.token = token;
        }
        renameTemplateSelf(node.lhs, selfName, token);
        renameTemplateSelf(node.rhs, selfName, token);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            renameTemplateSelf(node.args.getQuick(i), selfName, token);
        }
    }

    private static int resolveLateralColumn(OutputSchema scope, CharSequence token) {
        final int dot = Chars.indexOfLastUnquoted(token, '.');
        if (dot < 0) {
            return getColumnIndexQuiet(scope, token);
        }
        final CharSequence column = GenericLexer.unquote(token.subSequence(dot + 1, token.length()));
        final int index = scope.getColumnIndexQuiet(token.subSequence(0, dot), column, 0, column.length());
        return index >= 0 ? index : Math.max(getColumnIndexQuiet(scope, token), -1);
    }

    private static void shareMasterSource(AggregatePlan domain, JoinInput source, OutputSchema output) {
        final OutputSchema sourceOutput = source.getSourceOutput();
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

    private static boolean substituteLiteral(QueryModel model, CharSequence from, CharSequence to) {
        boolean isSubstituted = false;
        for (; model != null; model = model.getNestedModel()) {
            isSubstituted |= substituteLiteral(model.getWhereClause(), from, to);
            final ObjList<QueryColumn> columns = model.getBottomUpColumns();
            for (int i = 0, n = columns.size(); i < n; i++) {
                isSubstituted |= substituteLiteral(columns.getQuick(i).getAst(), from, to);
            }
            final ObjList<QueryModel> joins = model.getJoinModels();
            for (int i = 1, n = joins.size(); i < n; i++) {
                isSubstituted |= substituteLiteral(joins.getQuick(i).getJoinCriteria(), from, to);
                isSubstituted |= substituteLiteral(joins.getQuick(i).getNestedModel(), from, to);
            }
            if (model.getUnionModel() != null) {
                isSubstituted |= substituteLiteral(model.getUnionModel(), from, to);
            }
        }
        return isSubstituted;
    }

    private static boolean substituteLiteral(ExpressionNode node, CharSequence from, CharSequence to) {
        if (node == null || node.queryModel != null) {
            return false;
        }
        if (node.type == ExpressionNode.LITERAL) {
            if (Chars.equalsIgnoreCase(node.token, from)) {
                node.token = to;
                return true;
            }
            return false;
        }
        boolean isSubstituted = substituteLiteral(node.lhs, from, to) | substituteLiteral(node.rhs, from, to);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            isSubstituted |= substituteLiteral(node.args.getQuick(i), from, to);
        }
        return isSubstituted;
    }

    private void addCorrelatedKey(AggregatePlan aggregate, OutputSchema input, int index, QueryModel model, CharSequence outerQualifier, CharSequence outerName) {
        if (findGroupingColumn(aggregate, input.getColumnId(index)) < 0) {
            ctx.groupingNodes.add(null);
            aggregate.getGroupingExpressions().add(ctx.columns.next().of(input.getColumnId(index), input.getColumnType(index), aggregate.getPosition()));
            final CharSequence name = outerRefName(input, outerQualifier, outerName, input.getColumnName(index));
            final boolean isTaken = model.getAliasToColumnMap().contains(name) || aggregate.getOutput().getColumnIndexQuiet(name) >= 0;
            aggregate.getOutput().add(ctx.nextColumnId, isTaken ? correlatedName() : name, input.getColumnType(index), input.getMetadata(index), false);
            ctx.nextColumnId++;
        }
    }

    private void addOuterReference(CharSequence qualifier, CharSequence name) {
        for (int d = ctx.lateralScopes.size() - 1; d >= 0; d--) {
            final int reference = ctx.lateralScopes.getQuick(d).master.getColumnIndexQuiet(qualifier, name, 0, name.length());
            if (reference >= 0) {
                for (int i = 0, n = outerReferences.size(); i < n; i++) {
                    if (outerReferences.getQuick(i) == reference && outerReferenceScopes.getQuick(i) == d) {
                        return;
                    }
                }
                outerReferenceScopes.add(d);
                outerReferences.add(reference);
                return;
            }
        }
    }

    private LogicalPlan bindDomain(int position, int sequence, SqlExecutionContext executionContext) throws SqlException {
        final CharacterStoreEntry aliasEntry = ctx.characterStore.newEntry();
        aliasEntry.put(OUTER_REF_PREFIX).put(sequence);
        final CharSequence alias = aliasEntry.toImmutable();
        final int base = domainReferences.size();
        domainReferences.addAll(outerReferences);
        domainScopes.addAll(outerReferenceScopes);
        try {
            LogicalPlan result = null;
            for (int i = base, n = domainReferences.size(); i < n; i++) {
                final int depth = domainScopes.getQuick(i);
                final LateralScope lateral = ctx.lateralScopes.getQuick(depth);
                int input = lateral.inputs.getQuick(domainReferences.getQuick(i));
                boolean isBound = false;
                boolean isMultiInput = false;
                for (int k = base; k < n; k++) {
                    if (domainScopes.getQuick(k) == depth) {
                        final int other = lateral.inputs.getQuick(domainReferences.getQuick(k));
                        isBound |= k < i && other == input;
                        isMultiInput |= other != input;
                    }
                }
                if (isBound || (isMultiInput && hasEarlierDomainScope(base, i, depth))) {
                    continue;
                }
                if (isMultiInput) {
                    for (int k = base; k < n; k++) {
                        if (domainScopes.getQuick(k) == depth) {
                            input = Math.max(input, lateral.inputs.getQuick(domainReferences.getQuick(k)));
                        }
                    }
                }
                final boolean isPrefix = isMultiInput || isLateralMasterInput(lateral, input);
                final LogicalPlan plan = bindMasterSource(lateral, depth, input, isPrefix, isMultiInput, executionContext);
                final ProjectPlan keys = isMultiInput || plan.getType() == LogicalPlan.Type.SET_OPERATION ? ctx.projects.next().of(plan, position) : null;
                final AggregatePlan domain = ctx.aggregates.next().of(keys != null ? keys : plan, position);
                domain.setExplicitGrouping(true);
                final OutputSchema output = plan.getOutput();
                if (!isMultiInput) {
                    shareMasterSource(domain, lateral.joinInputs.getQuick(input), output);
                }
                final OutputSchema master = lateral.master;
                for (int k = n - 1; k >= i; k--) {
                    final int reference = domainReferences.getQuick(k);
                    if (domainScopes.getQuick(k) != depth || (!isMultiInput && lateral.inputs.getQuick(reference) != input)) {
                        continue;
                    }
                    final int column = isPrefix
                            ? output.getColumnIndexQuiet(master.getColumnQualifier(reference), master.getColumnName(reference), 0, master.getColumnName(reference).length())
                            : lateral.positions.getQuick(reference);
                    domain.getGroupingExpressions().add(ctx.columns.next().of(output.getColumnId(column), output.getColumnType(column), position));
                    if (keys != null) {
                        keys.getExpressions().add(ctx.columns.next().of(output.getColumnId(column), output.getColumnType(column), position));
                        keys.getOutput().add(output.getColumnId(column), output.getColumnName(column), output.getColumnType(column), output.getMetadata(column), true);
                    }
                    final CharacterStoreEntry name = ctx.characterStore.newEntry();
                    name.put(alias).put('_').put(master.getColumnName(reference));
                    domain.getOutput().add(ctx.nextColumnId, name.toImmutable(), master.getColumnType(reference), master.getMetadata(reference), false);
                    ctx.nextColumnId++;
                }
                for (int k = i, index = domain.getOutput().getColumnCount(); k < n; k++) {
                    final int reference = domainReferences.getQuick(k);
                    if (domainScopes.getQuick(k) == depth && (isMultiInput || lateral.inputs.getQuick(reference) == input)) {
                        domain.getOutput().addCorrelatedAlias(master.getColumnQualifier(reference), master.getColumnName(reference), --index);
                    }
                }
                result = result == null ? domain : crossJoin(result, null, domain, position);
            }
            domainAlias = alias;
            return result;
        } finally {
            domainReferences.setPos(base);
            domainScopes.setPos(base);
        }
    }

    private LogicalPlan bindMasterSource(LateralScope lateral, int depth, int input, boolean isPrefix, boolean isMultiInput,
                                         SqlExecutionContext executionContext) throws SqlException {
        final int size = ctx.lateralScopes.size();
        final int base = lateralScopeStash.size();
        for (int i = depth; i < size; i++) {
            lateralScopeStash.add(ctx.lateralScopes.getQuick(i));
        }
        ctx.lateralScopes.setPos(depth);
        try {
            if (isPrefix) {
                return binder.joinBinder.bindJoins(lateral.model, lateral.source, isMultiInput ? masterSourceFilter(lateral.where, lateral, input) : null,
                        input + 1, executionContext);
            }
            return binder.bindSource(lateral.model, lateral.source.getJoinModels().getQuick(input), executionContext);
        } finally {
            for (int i = base, n = lateralScopeStash.size(); i < n; i++) {
                ctx.lateralScopes.add(lateralScopeStash.getQuick(i));
            }
            lateralScopeStash.setPos(base);
        }
    }

    private CharSequence carrierName(OutputSchema output) {
        final int sequence = carrierSequence++;
        for (int suffix = 0; ; suffix++) {
            final CharacterStoreEntry name = ctx.characterStore.newEntry();
            name.put("__qdb_count_carrier__").put(sequence);
            if (suffix > 0) {
                name.put(suffix);
            }
            final CharSequence carrier = name.toImmutable();
            if (output.getColumnIndexQuiet(carrier) < 0
                    && (lateralBodyModel == null || !lateralBodyModel.getAliasToColumnMap().contains(carrier))) {
                return carrier;
            }
        }
    }

    private ExpressionNode carrierTemplate(ExpressionNode node, AggregatePlan aggregate, ProjectPlan project, CharSequence qualifier) {
        if (node == null) {
            return null;
        }
        if (ctx.isAggregate(node)) {
            final int index = aggregate.getGroupingExpressions().size() + aggregateBinder.findAggregate(node);
            final OutputSchema output = aggregate.getOutput();
            final CharSequence carrier = carrierName(project.getOutput());
            project.getExpressions().add(ctx.columns.next().of(output.getColumnId(index), output.getColumnType(index), node.position));
            project.getOutput().add(ctx.nextColumnId++, carrier, output.getColumnType(index), true);
            ctx.wildcardExcludedIds.add(project.getOutput().getColumnId(project.getOutput().getColumnCount() - 1));
            final ExpressionNode reference = ctx.bindingExpressions.next().of(ExpressionNode.LITERAL,
                    qualifier == null ? carrier : ctx.qualifiedJoinName(qualifier, carrier), 0, node.position);
            return SqlUtil.isZeroOnEmptyAggregate(node) ? zeroCoalesce(reference) : reference;
        }
        final ExpressionNode copy = ctx.bindingExpressions.next().of(node.type, node.token, node.precedence, node.position);
        copy.paramCount = node.paramCount;
        copy.lhs = carrierTemplate(node.lhs, aggregate, project, qualifier);
        copy.rhs = carrierTemplate(node.rhs, aggregate, project, qualifier);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            copy.args.add(carrierTemplate(node.args.getQuick(i), aggregate, project, qualifier));
        }
        return copy;
    }

    private void collectBlockOuterReferences(QueryModel model, QueryModel source, OutputSchema scope, ExpressionNode where, CharSequence alias) {
        outerReferenceScopes.clear();
        outerReferences.clear();
        collectOuterReferences(where, scope, alias, null);
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            final QueryColumn column = model.getBottomUpColumns().getQuick(i);
            collectOuterReferences(column.getAst(), scope, alias, null);
            if (column instanceof WindowExpression window) {
                collectOuterReferences(window.getPartitionBy(), scope, alias, null);
                collectOuterReferences(window.getOrderBy(), scope, alias, null);
            }
        }
        collectOuterReferences(model.getGroupBy(), scope, alias, null);
        collectOuterReferences(source.getGroupBy(), scope, alias, null);
        collectOuterReferences(source.getOrderBy(), scope, alias, model);
        collectOuterReferences(source.getLatestBy(), scope, alias, null);
        for (int i = 0, n = model.getPivotGroupByColumns().size(); i < n; i++) {
            collectOuterReferences(model.getPivotGroupByColumns().getQuick(i).getAst(), scope, alias, null);
        }
        collectOuterReferences(model.getLimitLo(), scope, alias, null);
        collectOuterReferences(model.getLimitHi(), scope, alias, null);
        for (int i = 1, n = source.getJoinModels().size(); i < n; i++) {
            collectOuterReferences(source.getJoinModels().getQuick(i).getJoinCriteria(), scope, alias, null);
        }
    }

    private void collectDeferredOuterReferences(JoinPlan join, int deferredBase) {
        if (join == null) {
            return;
        }
        for (int i = deferredBase, n = deferredCorrelationInputs.size(); i < n; i++) {
            final int inputIndex = deferredCorrelationInputs.getQuick(i);
            final OutputSchema output = join.getInputs().getQuick(inputIndex).getSourceOutput();
            final int alias = deferredCorrelationAliases.getQuick(i);
            final CharSequence qualifier = output.getCorrelatedAliasQualifier(alias);
            final CharSequence name = output.getCorrelatedAliasName(alias);
            if (precedingCorrelatedColumn(join, qualifier, name, inputIndex) < 0) {
                addOuterReference(qualifier, name);
            }
        }
    }

    private void collectDriverEqualities(ExpressionNode where, OutputSchema master, OutputSchema scope) {
        if (where == null) {
            return;
        }
        if (SqlKeywords.isAndKeyword(where.token) && where.paramCount == 2) {
            collectDriverEqualities(where.lhs, master, scope);
            collectDriverEqualities(where.rhs, master, scope);
            return;
        }
        if (where.paramCount != 2 || !Chars.equals(where.token, '=')
                || where.lhs.type != ExpressionNode.LITERAL || where.rhs.type != ExpressionNode.LITERAL) {
            return;
        }
        if (isDriverReference(where.lhs, master, scope) && !isOuterLiteral(where.rhs, scope)) {
            driverNodes.add(where.lhs);
        } else if (isDriverReference(where.rhs, master, scope) && !isOuterLiteral(where.lhs, scope)) {
            driverNodes.add(where.rhs);
        }
    }

    private boolean collectDriverReferences(QueryModel model, OutputSchema master) {
        boolean isFound = false;
        for (; model != null; model = model.getNestedModel()) {
            isFound |= collectDriverReferences(model.getWhereClause(), master);
            final ObjList<QueryColumn> columns = model.getBottomUpColumns();
            for (int i = 0, n = columns.size(); i < n; i++) {
                isFound |= collectDriverReferences(columns.getQuick(i).getAst(), master);
            }
            final ObjList<QueryModel> joins = model.getJoinModels();
            for (int i = 1, n = joins.size(); i < n; i++) {
                isFound |= collectDriverReferences(joins.getQuick(i).getJoinCriteria(), master);
                isFound |= collectDriverReferences(joins.getQuick(i).getNestedModel(), master);
            }
            if (model.getUnionModel() != null) {
                isFound |= collectDriverReferences(model.getUnionModel(), master);
            }
        }
        return isFound;
    }

    private boolean collectDriverReferences(ExpressionNode node, OutputSchema master) {
        if (node == null || node.queryModel != null) {
            return false;
        }
        if (node.type == ExpressionNode.LITERAL) {
            final int reference = Chars.indexOfLastUnquoted(node.token, '.') < 0 ? -1 : FunctionBinder.findColumn(node, master, null);
            if (reference < 0) {
                return false;
            }
            if (!driverReferences.contains(reference)) {
                driverReferences.add(reference);
            }
            return true;
        }
        boolean isFound = collectDriverReferences(node.lhs, master) | collectDriverReferences(node.rhs, master);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            isFound |= collectDriverReferences(node.args.getQuick(i), master);
        }
        return isFound;
    }

    private void collectEliminableReference(ExpressionNode inner, ExpressionNode outer, OutputSchema scope, CharSequence alias) {
        if (FunctionBinder.findColumn(inner, scope, alias) < 0 || FunctionBinder.findColumn(outer, scope, alias) != -1) {
            return;
        }
        for (int d = ctx.lateralScopes.size() - 1; d >= 0; d--) {
            final int reference = FunctionBinder.findColumn(outer, ctx.lateralScopes.getQuick(d).master, null);
            if (reference >= 0) {
                for (int i = 0, n = outerReferences.size(); i < n; i++) {
                    if (outerReferences.getQuick(i) == reference && outerReferenceScopes.getQuick(i) == d
                            && !eliminableReferences.contains(i)) {
                        eliminableReferences.add(i);
                    }
                }
                return;
            }
        }
    }

    private void collectEliminableReferences(ExpressionNode node, OutputSchema scope, CharSequence alias) {
        if (node == null) {
            return;
        }
        if (SqlKeywords.isAndKeyword(node.token) && node.paramCount == 2) {
            collectEliminableReferences(node.lhs, scope, alias);
            collectEliminableReferences(node.rhs, scope, alias);
        } else if (node.paramCount == 2 && Chars.equals(node.token, '=')
                && node.lhs.type == ExpressionNode.LITERAL && node.rhs.type == ExpressionNode.LITERAL) {
            collectEliminableReference(node.lhs, node.rhs, scope, alias);
            collectEliminableReference(node.rhs, node.lhs, scope, alias);
        }
    }

    private void collectOuterReferences(ObjList<ExpressionNode> nodes, OutputSchema scope, CharSequence alias, QueryModel aliasModel) {
        for (int i = 0, n = nodes.size(); i < n; i++) {
            collectOuterReferences(nodes.getQuick(i), scope, alias, aliasModel);
        }
    }

    private void collectOuterReferences(ExpressionNode node, OutputSchema scope, CharSequence alias, QueryModel aliasModel) {
        if (node == null || node.queryModel != null) {
            return;
        }
        if (node.type == ExpressionNode.LITERAL) {
            if (FunctionBinder.findColumn(node, scope, alias) != -1
                    || aliasModel != null && aliasModel.getAliasToColumnMap().contains(node.token)) {
                return;
            }
            for (int d = ctx.lateralScopes.size() - 1; d >= 0; d--) {
                final int reference = FunctionBinder.findColumn(node, ctx.lateralScopes.getQuick(d).master, null);
                if (reference >= 0) {
                    for (int i = 0, n = outerReferences.size(); i < n; i++) {
                        if (outerReferences.getQuick(i) == reference && outerReferenceScopes.getQuick(i) == d) {
                            return;
                        }
                    }
                    outerReferenceScopes.add(d);
                    outerReferences.add(reference);
                    return;
                }
            }
            return;
        }
        collectOuterReferences(node.lhs, scope, alias, aliasModel);
        collectOuterReferences(node.rhs, scope, alias, aliasModel);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            collectOuterReferences(node.args.getQuick(i), scope, alias, aliasModel);
        }
        if (node.windowExpression != null) {
            collectOuterReferences(node.windowExpression.getPartitionBy(), scope, alias, aliasModel);
            collectOuterReferences(node.windowExpression.getOrderBy(), scope, alias, aliasModel);
        }
    }

    private boolean driveLateral(QueryModel body, OutputSchema scope, int position, SqlExecutionContext executionContext) throws SqlException {
        final OutputSchema master = ctx.lateralScopes.getLast().master;
        driverNodes.clear();
        for (QueryModel model = body; model != null; model = model.getNestedModel()) {
            collectDriverEqualities(model.getWhereClause(), master, scope);
        }
        if (driverNodes.size() == 0 || !(isScalarCountBody(body) || !hasAggregateChain(body)) || hasUndrivenReference(body, scope)) {
            return false;
        }
        if (countDriver == null) {
            driverReferences.clear();
            for (int i = 1; i < countDriverLimit; i++) {
                final QueryModel occurrence = countDriverSource.getJoinModels().getQuick(i);
                if (QueryModel.isLateralJoin(occurrence.getJoinType())) {
                    collectDriverReferences(occurrence.getNestedModel(), master);
                }
            }
            outerReferences.clear();
            outerReferenceScopes.clear();
            for (int i = 0, n = driverReferences.size(); i < n; i++) {
                outerReferences.add(driverReferences.getQuick(i));
                outerReferenceScopes.add(ctx.lateralScopes.size() - 1);
            }
            final LogicalPlan domain = bindDomain(position, outerRefId(), executionContext);
            countDriver = isCountDriverQualified
                    ? renameCountDriver(domain, position)
                    : ctx.joinInputs.next().of(domain, QueryModel.JOIN_CROSS, domainAlias, position);
        }
        final OutputSchema driverOutput = countDriver.getSourceOutput();
        final CharSequence driverQualifier = isCountDriverQualified ? countDriver.getBindingAlias() : null;
        for (int i = 0, n = driverNodes.size(); i < n; i++) {
            final ExpressionNode node = driverNodes.getQuick(i);
            final int reference = FunctionBinder.findColumn(node, master, null);
            final CharSequence qualifier = master.getColumnQualifier(reference);
            final CharSequence name = master.getColumnName(reference);
            final int column = driverOutput.getCorrelatedColumnIndexQuiet(qualifier, name, 0, name.length());
            node.token = ctx.qualifiedJoinName(driverQualifier, driverOutput.getColumnName(column));
            substitutedQualifiers.add(driverQualifier);
            substitutedNames.add(driverOutput.getColumnName(column));
            substitutedOuterNames.add(name);
        }
        return true;
    }

    private boolean eliminateOuterReference(ExpressionNode inner, ExpressionNode outer, OutputSchema scope, CharSequence alias) {
        final int index = FunctionBinder.findColumn(inner, scope, alias);
        if (index < 0 || FunctionBinder.findColumn(outer, scope, alias) != -1) {
            return false;
        }
        for (int d = ctx.lateralScopes.size() - 1; d >= 0; d--) {
            final OutputSchema master = ctx.lateralScopes.getQuick(d).master;
            final int reference = FunctionBinder.findColumn(outer, master, null);
            if (reference >= 0) {
                for (int i = 0, n = outerReferences.size(); i < n; i++) {
                    if (outerReferences.getQuick(i) == reference && outerReferenceScopes.getQuick(i) == d) {
                        scope.addCorrelatedAlias(master.getColumnQualifier(reference), master.getColumnName(reference), index);
                        outerReferences.removeIndex(i);
                        outerReferenceScopes.removeIndex(i);
                        return true;
                    }
                }
                return false;
            }
        }
        return false;
    }

    private ExpressionNode eliminateOuterReferences(ExpressionNode node, OutputSchema scope, CharSequence alias) {
        if (node == null) {
            return null;
        }
        if (SqlKeywords.isAndKeyword(node.token) && node.paramCount == 2) {
            final ExpressionNode left = eliminateOuterReferences(node.lhs, scope, alias);
            final ExpressionNode right = eliminateOuterReferences(node.rhs, scope, alias);
            if (left == null) {
                return right;
            }
            if (right == null) {
                return left;
            }
            node.lhs = left;
            node.rhs = right;
            return node;
        }
        if (node.paramCount != 2 || !Chars.equals(node.token, '=')
                || node.lhs.type != ExpressionNode.LITERAL || node.rhs.type != ExpressionNode.LITERAL) {
            return node;
        }
        return eliminateOuterReference(node.lhs, node.rhs, scope, alias) || eliminateOuterReference(node.rhs, node.lhs, scope, alias)
                ? null : node;
    }

    private boolean hasAggregateChain(QueryModel model) {
        for (; model != null; model = model.getNestedModel()) {
            if (model.getGroupBy().size() > 0 || model.getSampleBy() != null || model.isDistinct()) {
                return true;
            }
            final ObjList<QueryColumn> columns = model.getBottomUpColumns();
            for (int i = 0, n = columns.size(); i < n; i++) {
                if (ctx.hasAggregate(columns.getQuick(i).getAst())) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean hasCountReference(ExpressionNode node, QueryModel model) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.LITERAL) {
            return isCountColumn(node, model);
        }
        return hasCountReference(node.lhs, model) || hasCountReference(node.rhs, model);
    }

    private boolean hasEarlierDomainScope(int base, int index, int depth) {
        for (int k = base; k < index; k++) {
            if (domainScopes.getQuick(k) == depth) {
                return true;
            }
        }
        return false;
    }

    private boolean hasLateralCarrierReference(ExpressionNode node, OutputSchema scope) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.LITERAL) {
            return lateralCarrierIndex(node.token, scope) >= 0;
        }
        if (hasLateralCarrierReference(node.lhs, scope) || hasLateralCarrierReference(node.rhs, scope)) {
            return true;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (hasLateralCarrierReference(node.args.getQuick(i), scope)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasLateralOuterReference() {
        for (int i = 0, n = outerReferences.size(); i < n; i++) {
            final LateralScope lateral = ctx.lateralScopes.getQuick(outerReferenceScopes.getQuick(i));
            final int input = lateral.inputs.getQuick(outerReferences.getQuick(i));
            if (QueryModel.isLateralJoin(lateral.source.getJoinModels().getQuick(input).getJoinType())) {
                return true;
            }
        }
        return false;
    }

    private boolean hasNonNullableOuterLiteral(ExpressionNode node, OutputSchema scope, CharSequence alias) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.LITERAL) {
            if (FunctionBinder.findColumn(node, scope, alias) >= 0) {
                return false;
            }
            final OutputSchema master = ctx.lateralScopes.getLast().master;
            final int type = master.getColumnType(FunctionBinder.findColumn(node, master, null));
            return ColumnType.isArray(type) || ColumnType.isGeoHash(type) || ColumnType.isCursor(type);
        }
        if (hasNonNullableOuterLiteral(node.lhs, scope, alias) || hasNonNullableOuterLiteral(node.rhs, scope, alias)) {
            return true;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (hasNonNullableOuterLiteral(node.args.getQuick(i), scope, alias)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasUndrivenReference(QueryModel model, OutputSchema scope) {
        for (; model != null; model = model.getNestedModel()) {
            if (hasUndrivenReference(model.getWhereClause(), scope)
                    || hasUndrivenReference(model.getGroupBy(), scope) || hasUndrivenReference(model.getOrderBy(), scope)
                    || hasUndrivenReference(model.getLimitLo(), scope) || hasUndrivenReference(model.getLimitHi(), scope)) {
                return true;
            }
            final ObjList<QueryColumn> columns = model.getBottomUpColumns();
            for (int i = 0, n = columns.size(); i < n; i++) {
                if (hasUndrivenReference(columns.getQuick(i).getAst(), scope)) {
                    return true;
                }
            }
            final ObjList<QueryModel> joins = model.getJoinModels();
            for (int i = 1, n = joins.size(); i < n; i++) {
                if (hasUndrivenReference(joins.getQuick(i).getJoinCriteria(), scope) || hasUndrivenReference(joins.getQuick(i).getNestedModel(), scope)) {
                    return true;
                }
            }
            if (model.getUnionModel() != null && hasUndrivenReference(model.getUnionModel(), scope)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasUndrivenReference(ObjList<ExpressionNode> nodes, OutputSchema scope) {
        for (int i = 0, n = nodes.size(); i < n; i++) {
            if (hasUndrivenReference(nodes.getQuick(i), scope)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasUndrivenReference(ExpressionNode node, OutputSchema scope) {
        if (node == null) {
            return false;
        }
        if (node.queryModel != null) {
            return true;
        }
        if (node.type == ExpressionNode.LITERAL) {
            for (int i = 0, n = driverNodes.size(); i < n; i++) {
                if (driverNodes.getQuick(i) == node) {
                    return false;
                }
            }
            return isOuterLiteral(node, scope);
        }
        if (hasUndrivenReference(node.lhs, scope) || hasUndrivenReference(node.rhs, scope)) {
            return true;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (hasUndrivenReference(node.args.getQuick(i), scope)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasUnliftableTerm(ExpressionNode node, OutputSchema scope, CharSequence alias) {
        if (node == null) {
            return false;
        }
        if (node.queryModel != null || node.windowExpression != null) {
            return true;
        }
        if (node.type == ExpressionNode.LITERAL) {
            return FunctionBinder.findColumn(node, scope, alias) < 0
                    && FunctionBinder.findColumn(node, ctx.lateralScopes.getLast().master, null) < 0;
        }
        if (hasUnliftableTerm(node.lhs, scope, alias) || hasUnliftableTerm(node.rhs, scope, alias)) {
            return true;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (hasUnliftableTerm(node.args.getQuick(i), scope, alias)) {
                return true;
            }
        }
        return false;
    }

    private boolean isCountColumn(ExpressionNode node, QueryModel model) {
        if (node.type != ExpressionNode.LITERAL) {
            return false;
        }
        final int dot = Chars.indexOfLastUnquoted(node.token, '.');
        final CharSequence name = dot < 0 ? node.token : node.token.subSequence(dot + 1, node.token.length());
        final ObjList<QueryColumn> columns = model.getBottomUpColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn column = columns.getQuick(i);
            if (column.getAlias() != null && Chars.equalsIgnoreCase(column.getAlias(), name)) {
                return SqlUtil.isZeroOnEmptyAggregate(ctx.aggregateSelectExpressions.getQuick(i));
            }
        }
        return false;
    }

    private boolean isFullyEliminable(ExpressionNode where, OutputSchema scope, CharSequence alias) {
        eliminableReferences.clear();
        collectEliminableReferences(where, scope, alias);
        return eliminableReferences.size() == outerReferences.size();
    }

    private boolean isLiftableBody(QueryModel model, QueryModel source) {
        return model == lateralBodyModel && model.getUnionModel() == null && !model.isDistinct()
                && !aggregateBinder.hasAggregation(model, source) && !windowBinder.hasWindows(model, source)
                && model.getLimitLo() == null && model.getLimitHi() == null
                && source.getLatestBy().size() == 0 && source.getSampleBy() == null;
    }

    private boolean isLiftedNameTaken(CharSequence name, OutputSchema scope) {
        if (lateralBodyModel.getAliasToColumnMap().contains(name)) {
            return true;
        }
        for (int i = 0, n = liftedNames.size(); i < n; i++) {
            if (Chars.equalsIgnoreCase(liftedNames.getQuick(i), name)) {
                return true;
            }
        }
        for (int i = 0, n = scope.getCorrelatedAliasCount(); i < n; i++) {
            if (Chars.equalsIgnoreCase(scope.getColumnName(scope.getCorrelatedAliasIndex(i)), name)) {
                return true;
            }
        }
        return false;
    }

    private boolean isOuterLiteral(ExpressionNode node, OutputSchema scope) {
        if (FunctionBinder.findColumn(node, scope, null) >= 0) {
            return true;
        }
        for (int d = ctx.lateralScopes.size() - 1; d >= 0; d--) {
            if (FunctionBinder.findColumn(node, ctx.lateralScopes.getQuick(d).master, null) >= 0) {
                return true;
            }
        }
        return false;
    }

    private boolean isRejectingZeroCount(ExpressionNode node, QueryModel model) {
        if (node == null) {
            return false;
        }
        if (SqlKeywords.isAndKeyword(node.token) && node.paramCount == 2) {
            return isRejectingZeroCount(node.lhs, model) || isRejectingZeroCount(node.rhs, model);
        }
        if (node.paramCount != 2 || node.type != ExpressionNode.OPERATION) {
            return false;
        }
        final boolean isCountLeft = isCountColumn(node.lhs, model);
        if (!isCountLeft && !isCountColumn(node.rhs, model)) {
            return false;
        }
        final ExpressionNode constant = isCountLeft ? node.rhs : node.lhs;
        if (constant.type != ExpressionNode.CONSTANT) {
            return false;
        }
        final long value;
        try {
            value = Numbers.parseLong(constant.token);
        } catch (NumericException e) {
            return false;
        }
        final long lhs = isCountLeft ? 0 : value;
        final long rhs = isCountLeft ? value : 0;
        final CharSequence op = node.token;
        if (Chars.equals(op, '=')) {
            return lhs != rhs;
        }
        if (Chars.equals(op, "!=") || Chars.equals(op, "<>")) {
            return lhs == rhs;
        }
        if (Chars.equals(op, '<')) {
            return lhs >= rhs;
        }
        if (Chars.equals(op, "<=")) {
            return lhs > rhs;
        }
        if (Chars.equals(op, '>')) {
            return lhs <= rhs;
        }
        if (Chars.equals(op, ">=")) {
            return lhs < rhs;
        }
        return false;
    }

    private boolean isTotalCountFilter(ExpressionNode node, QueryModel model) {
        if (node == null) {
            return true;
        }
        if (SqlKeywords.isAndKeyword(node.token) && node.paramCount == 2) {
            return isTotalCountFilter(node.lhs, model) && isTotalCountFilter(node.rhs, model);
        }
        if (node.paramCount != 2 || node.type != ExpressionNode.OPERATION) {
            return false;
        }
        final boolean isCountLeft = isCountColumn(node.lhs, model);
        if (!isCountLeft && !isCountColumn(node.rhs, model)) {
            return false;
        }
        final ExpressionNode constant = isCountLeft ? node.rhs : node.lhs;
        if (constant.type != ExpressionNode.CONSTANT) {
            return false;
        }
        final long value;
        try {
            value = Numbers.parseLong(constant.token);
        } catch (NumericException e) {
            return false;
        }
        final CharSequence op = node.token;
        if (Chars.equals(op, "!=") || Chars.equals(op, "<>")) {
            return value < 0;
        }
        if (isCountLeft ? Chars.equals(op, ">=") : Chars.equals(op, "<=")) {
            return value <= 0;
        }
        if (isCountLeft ? Chars.equals(op, '>') : Chars.equals(op, '<')) {
            return value < 0;
        }
        return false;
    }

    private int lateralCarrierIndex(CharSequence token, OutputSchema scope) {
        final int index = resolveLateralColumn(scope, token);
        return index < 0 ? -1 : lateralCarrierIds.indexOf(scope.getColumnId(index), 0, lateralCarrierIds.size());
    }

    private ExpressionNode lateralLimitComparison(CharSequence operator, ExpressionNode limit, ExpressionNode rowNumber, int position) {
        final ExpressionNode guard = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, "__lateral_limit", 0, limit.position);
        guard.paramCount = 1;
        guard.rhs = limit;
        final ExpressionNode comparison = ctx.bindingExpressions.next().of(ExpressionNode.OPERATION, operator, 0, position);
        comparison.paramCount = 2;
        comparison.lhs = guard;
        comparison.rhs = rowNumber;
        return comparison;
    }

    private void liftLiterals(ExpressionNode node, OutputSchema scope, CharSequence alias) {
        if (node == null) {
            return;
        }
        if (node.type == ExpressionNode.LITERAL) {
            final int index = FunctionBinder.findColumn(node, scope, alias);
            if (index >= 0) {
                int lifted = liftedIndexes.indexOf(index, 0, liftedIndexes.size());
                if (lifted < 0) {
                    final CharSequence projected = projectedLiteralAlias(index, scope, alias);
                    if (projected != null) {
                        node.token = ctx.qualifiedJoinName(lateralBodyAlias, projected);
                        return;
                    }
                    CharSequence name = scope.getColumnName(index);
                    if (isLiftedNameTaken(name, scope)) {
                        final CharacterStoreEntry entry = ctx.characterStore.newEntry();
                        entry.put("__qdb_lifted_").put(liftedIndexes.size()).put('_').put(ctx.nextColumnId);
                        name = entry.toImmutable();
                    }
                    lifted = liftedIndexes.size();
                    liftedIndexes.add(index);
                    liftedNames.add(name);
                }
                node.token = ctx.qualifiedJoinName(lateralBodyAlias, liftedNames.getQuick(lifted));
            } else {
                final OutputSchema master = ctx.lateralScopes.getLast().master;
                final int reference = FunctionBinder.findColumn(node, master, null);
                node.token = ctx.qualifiedJoinName(master.getColumnQualifier(reference), master.getColumnName(reference));
            }
            return;
        }
        liftLiterals(node.lhs, scope, alias);
        liftLiterals(node.rhs, scope, alias);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            liftLiterals(node.args.getQuick(i), scope, alias);
        }
    }

    private ExpressionNode liftOuterConjuncts(ExpressionNode node, OutputSchema scope, CharSequence alias) {
        if (node == null) {
            return null;
        }
        if (SqlKeywords.isAndKeyword(node.token) && node.paramCount == 2) {
            final ExpressionNode left = liftOuterConjuncts(node.lhs, scope, alias);
            final ExpressionNode right = liftOuterConjuncts(node.rhs, scope, alias);
            if (left == null) {
                return right;
            }
            if (right == null) {
                return left;
            }
            node.lhs = left;
            node.rhs = right;
            return node;
        }
        outerReferenceScopes.clear();
        outerReferences.clear();
        collectOuterReferences(node, scope, alias, null);
        if (outerReferences.size() == 0 || hasUnliftableTerm(node, scope, alias)) {
            return node;
        }
        liftLiterals(node, scope, alias);
        if (lateralLifted == null) {
            lateralLifted = node;
        } else {
            final ExpressionNode and = ctx.bindingExpressions.next().of(ExpressionNode.OPERATION, "and", 0, node.position);
            and.paramCount = 2;
            and.lhs = lateralLifted;
            and.rhs = node;
            lateralLifted = and;
        }
        return null;
    }

    private void liftOuterSelections(QueryModel model, OutputSchema scope, CharSequence alias) {
        final ObjList<QueryColumn> columns = model.getBottomUpColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            if (isWildcard(columns.getQuick(i).getAst()) || columns.getQuick(i).getAlias() == null) {
                return;
            }
        }
        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn column = columns.getQuick(i);
            final ExpressionNode ast = column.getAst();
            if (ast.type == ExpressionNode.LITERAL || ctx.hasAggregate(ast)) {
                continue;
            }
            outerReferenceScopes.clear();
            outerReferences.clear();
            collectOuterReferences(ast, scope, alias, null);
            if (outerReferences.size() == 0 || hasUnliftableTerm(ast, scope, alias) || hasNonNullableOuterLiteral(ast, scope, alias)) {
                continue;
            }
            final ExpressionNode template = ExpressionNode.deepClone(ctx.bindingExpressions, ast);
            liftLiterals(template, scope, alias);
            liftedSelectionColumns.add(i);
            liftedSelectionTemplates.add(template);
            column.of(column.getAlias(), nullOuterLiterals(ExpressionNode.deepClone(ctx.bindingExpressions, ast), scope, alias),
                    column.isIncludeIntoWildcard(), column.getColumnType());
        }
    }

    private ExpressionNode masterSourceFilter(ExpressionNode where, LateralScope lateral, int input) {
        if (where == null) {
            return null;
        }
        if (SqlKeywords.isAndKeyword(where.token) && where.paramCount == 2) {
            final ExpressionNode left = masterSourceFilter(where.lhs, lateral, input);
            final ExpressionNode right = masterSourceFilter(where.rhs, lateral, input);
            if (left == null || right == null) {
                return left == null ? right : left;
            }
            final ExpressionNode and = ctx.bindingExpressions.next().of(ExpressionNode.OPERATION, "and", 0, where.position);
            and.paramCount = 2;
            and.lhs = left;
            and.rhs = right;
            return and;
        }
        final int owner = masterSourceOwner(where, lateral);
        return owner >= 0 && owner <= input ? ExpressionNode.deepClone(ctx.bindingExpressions, where) : null;
    }

    private int masterSourceOwner(ExpressionNode node, LateralScope lateral) {
        if (node == null || node.type == ExpressionNode.CONSTANT || node.type == ExpressionNode.BIND_VARIABLE) {
            return Integer.MAX_VALUE;
        }
        if (node.queryModel != null) {
            return -1;
        }
        if (node.type == ExpressionNode.LITERAL) {
            final int index = FunctionBinder.findColumn(node, lateral.master, null);
            return index < 0 ? -1 : lateral.inputs.getQuick(index);
        }
        int owner = mergeOwner(masterSourceOwner(node.lhs, lateral), masterSourceOwner(node.rhs, lateral));
        for (int i = 0, n = node.args.size(); i < n; i++) {
            owner = mergeOwner(owner, masterSourceOwner(node.args.getQuick(i), lateral));
        }
        return owner;
    }

    private ExpressionNode nullOuterLiterals(ExpressionNode node, OutputSchema scope, CharSequence alias) {
        if (node == null) {
            return null;
        }
        if (node.type == ExpressionNode.LITERAL) {
            if (FunctionBinder.findColumn(node, scope, alias) >= 0) {
                return node;
            }
            final OutputSchema master = ctx.lateralScopes.getLast().master;
            final ExpressionNode cast = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, "cast", 0, node.position);
            cast.paramCount = 2;
            cast.lhs = ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, "null", 0, node.position);
            cast.rhs = ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT,
                    ColumnType.nameOf(master.getColumnType(FunctionBinder.findColumn(node, master, null))), 0, node.position);
            return cast;
        }
        node.lhs = nullOuterLiterals(node.lhs, scope, alias);
        node.rhs = nullOuterLiterals(node.rhs, scope, alias);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            node.args.setQuick(i, nullOuterLiterals(node.args.getQuick(i), scope, alias));
        }
        return node;
    }

    private int outerReferenceOrdinal(CharSequence qualifier, CharSequence name) {
        for (int d = ctx.lateralScopes.size() - 1; d >= 0; d--) {
            final int reference = ctx.lateralScopes.getQuick(d).master.getColumnIndexQuiet(qualifier, name, 0, name.length());
            if (reference >= 0) {
                return reference;
            }
        }
        return -1;
    }

    private int outerRefId() {
        if (lateralOuterRefId < 0) {
            lateralOuterRefId = outerReferenceSequence++;
        }
        return lateralOuterRefId;
    }

    private CharSequence outerRefName(OutputSchema input, CharSequence qualifier, CharSequence outerName, CharSequence fallback) {
        if (outerName == null || ctx.lateralScopes.size() == 0 || Chars.startsWith(fallback, COUNT_DRIVER_PREFIX)) {
            return fallback;
        }
        final CharacterStoreEntry name = ctx.characterStore.newEntry();
        name.put(OUTER_REF_PREFIX).put(outerRefId()).put('_').put(substitutedOuterName(qualifier, outerName));
        final int reference = outerReferenceOrdinal(qualifier, outerName);
        int duplicates = 0;
        for (int i = 0, n = input.getCorrelatedAliasCount(); i < n; i++) {
            final CharSequence other = input.getCorrelatedAliasName(i);
            if (Chars.equalsIgnoreCase(other, outerName) && outerReferenceOrdinal(input.getCorrelatedAliasQualifier(i), other) < reference) {
                duplicates++;
            }
        }
        if (duplicates > 0) {
            name.put(duplicates);
        }
        return name.toImmutable();
    }

    private CharSequence projectedLiteralAlias(int index, OutputSchema scope, CharSequence alias) {
        final ObjList<QueryColumn> columns = lateralBodyModel.getBottomUpColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn column = columns.getQuick(i);
            if (column.getAst().type == ExpressionNode.LITERAL && column.getAlias() != null
                    && FunctionBinder.findColumn(column.getAst(), scope, alias) == index) {
                return column.getAlias();
            }
        }
        return null;
    }

    private void rejectNegativeLateralLimit(ExpressionNode limit, SqlExecutionContext executionContext) throws SqlException {
        if (limit != null && !hasLiteral(limit) && orderBinder.bindLimit(limit, executionContext) instanceof ConstantExpression constant
                && constant.getLongValue() < 0 && constant.getLongValue() != Numbers.LONG_NULL) {
            throw SqlException.$(limit.position, "negative LIMIT is not supported in a correlated lateral sub-query");
        }
    }

    private JoinInput renameCountDriver(LogicalPlan domain, int position) {
        CharSequence alias;
        do {
            final CharacterStoreEntry aliasEntry = ctx.characterStore.newEntry();
            aliasEntry.put(COUNT_DRIVER_PREFIX).put(outerReferenceSequence++);
            alias = aliasEntry.toImmutable();
        } while (hasSourceAlias(countDriverSource, alias));
        final OutputSchema output = domain.getOutput();
        final ProjectPlan project = ctx.projects.next().of(domain, position);
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            final CharacterStoreEntry name = ctx.characterStore.newEntry();
            name.put(alias).put('_').put(output.getCorrelatedAliasName(correlatedAlias(output, i)));
            project.getExpressions().add(ctx.columns.next().of(output.getColumnId(i), output.getColumnType(i), position));
            project.getOutput().add(ctx.nextColumnId++, name.toImmutable(), output.getColumnType(i), output.getMetadata(i), false);
        }
        copyCorrelatedAliases(output, project.getOutput(), output.getColumnCount());
        return ctx.joinInputs.next().of(project, QueryModel.JOIN_CROSS, alias, position);
    }

    private ExpressionNode replaceLateralCarriers(ExpressionNode node, OutputSchema scope) {
        if (node == null) {
            return null;
        }
        if (node.type == ExpressionNode.LITERAL) {
            final int carrier = lateralCarrierIndex(node.token, scope);
            if (carrier < 0) {
                return node;
            }
            final ExpressionNode template = ExpressionNode.deepClone(ctx.bindingExpressions, lateralTemplates.getQuick(carrier));
            renameTemplateSelf(template, lateralTemplateNames.getQuick(carrier), node.token);
            return template;
        }
        node.lhs = replaceLateralCarriers(node.lhs, scope);
        node.rhs = replaceLateralCarriers(node.rhs, scope);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            node.args.setQuick(i, replaceLateralCarriers(node.args.getQuick(i), scope));
        }
        return node;
    }

    private CharSequence substitutedOuterName(CharSequence qualifier, CharSequence name) {
        for (int i = 0, n = substitutedNames.size(); i < n; i++) {
            final CharSequence substituted = substitutedQualifiers.getQuick(i);
            if (Chars.equalsIgnoreCase(substitutedNames.getQuick(i), name) && (substituted == null || Chars.equalsIgnoreCaseNc(substituted, qualifier))) {
                return substitutedOuterNames.getQuick(i);
            }
        }
        return name;
    }

    private boolean substituteOuterEqualities(ExpressionNode where, OutputSchema master, QueryModel body) {
        if (where == null || body == null) {
            return false;
        }
        if (SqlKeywords.isAndKeyword(where.token) && where.paramCount == 2) {
            return substituteOuterEqualities(where.lhs, master, body) | substituteOuterEqualities(where.rhs, master, body);
        }
        if (where.paramCount != 2 || !Chars.equals(where.token, '=')
                || where.lhs.type != ExpressionNode.LITERAL || where.rhs.type != ExpressionNode.LITERAL) {
            return false;
        }
        return substituteOuterEquality(where.lhs, where.rhs, master, body) || substituteOuterEquality(where.rhs, where.lhs, master, body);
    }

    private boolean substituteOuterEquality(ExpressionNode inner, ExpressionNode outer, OutputSchema master, QueryModel body) {
        final int index = FunctionBinder.findColumn(inner, master, null);
        if (index < 0 || master.getColumnQualifier(index) == null || FunctionBinder.findColumn(outer, master, null) >= 0
                || Chars.indexOfLastUnquoted(outer.token, '.') < 0) {
            return false;
        }
        for (int d = ctx.lateralScopes.size() - 1; d >= 0; d--) {
            final OutputSchema scope = ctx.lateralScopes.getQuick(d).master;
            final int reference = FunctionBinder.findColumn(outer, scope, null);
            if (reference >= 0) {
                final CharSequence token = ctx.qualifiedJoinName(master.getColumnQualifier(index), master.getColumnName(index));
                if (!substituteLiteral(body, outer.token, token)) {
                    return false;
                }
                substitutedQualifiers.add(master.getColumnQualifier(index));
                substitutedNames.add(master.getColumnName(index));
                substitutedOuterNames.add(scope.getColumnName(reference));
                return true;
            }
        }
        return false;
    }

    static boolean hasCorrelatedColumns(OutputSchema output) {
        return output.getCorrelatedAliasCount() > 0;
    }

    static boolean hasOnlyCorrelatedKeys(AggregatePlan aggregate, OutputSchema input) {
        for (int i = 0, n = aggregate.getGroupingExpressions().size(); i < n; i++) {
            if (!(aggregate.getGroupingExpressions().getQuick(i) instanceof ColumnExpression column)) {
                return false;
            }
            final int index = input.getColumnIndexById(column.getColumnId());
            boolean isCorrelated = false;
            for (int k = 0, m = input.getCorrelatedAliasCount(); k < m && !isCorrelated; k++) {
                isCorrelated = input.getCorrelatedAliasIndex(k) == index;
            }
            if (!isCorrelated) {
                return false;
            }
        }
        return true;
    }

    static boolean isCorrelatedReference(ExpressionNode node, OutputSchema scope) {
        if (node.type != ExpressionNode.LITERAL) {
            return false;
        }
        final int dot = Chars.indexOfLastUnquoted(node.token, '.');
        if (dot < 0) {
            return false;
        }
        final CharSequence qualifier = GenericLexer.unquote(node.token.subSequence(0, dot));
        return scope.getCorrelatedColumnIndexQuiet(qualifier, node.token, dot + 1, node.token.length()) >= 0;
    }

    void addCorrelatedKeys(AggregatePlan aggregate, OutputSchema input, QueryModel model) {
        for (int i = 0, n = input.getCorrelatedAliasCount(); i < n; i++) {
            addCorrelatedKey(aggregate, input, input.getCorrelatedAliasIndex(i), model, input.getCorrelatedAliasQualifier(i), input.getCorrelatedAliasName(i));
        }
    }

    void addLateralTemplate(int columnId, ExpressionNode template, CharSequence selfName) {
        lateralCarrierIds.add(columnId);
        lateralTemplates.add(template);
        lateralTemplateNames.add(selfName);
    }

    LogicalPlan bindCorrelatedLimit(LogicalPlan input, QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        final ExpressionNode lo = model.getLimitLo();
        final ExpressionNode hi = model.getLimitHi();
        rejectNegativeLateralLimit(lo, executionContext);
        rejectNegativeLateralLimit(hi, executionContext);
        final int position = lo != null ? lo.position : hi.position;
        final ProjectPlan wrapper = input instanceof ProjectPlan project && project.getInput() instanceof SortPlan sort
                && isPlainColumnProjection(project) && !projectsSortColumns(project, sort) ? project : null;
        if (wrapper != null) {
            input = wrapper.getInput();
        }
        final OutputSchema output = input.getOutput();
        final OutputSchema correlated = wrapper == null ? output : wrapper.getOutput();
        final WindowSpec spec = ctx.windowSpecs.next().of(ctx.unboundedWindow);
        for (int i = 0, n = correlated.getCorrelatedAliasCount(); i < n; i++) {
            final int columnIndex = correlated.getCorrelatedAliasIndex(i);
            final int columnId = wrapper == null ? correlated.getColumnId(columnIndex)
                    : ((ColumnExpression) wrapper.getExpressions().getQuick(columnIndex)).getColumnId();
            boolean isPartitioned = false;
            for (int k = 0, m = spec.getPartitionBy().size(); k < m && !isPartitioned; k++) {
                isPartitioned = ((ColumnExpression) spec.getPartitionBy().getQuick(k)).getColumnId() == columnId;
            }
            if (!isPartitioned) {
                spec.getPartitionBy().add(ctx.columns.next().of(columnId, correlated.getColumnType(columnIndex), position));
            }
        }
        if (input instanceof SortPlan sort) {
            for (int i = 0, n = sort.getColumnIds().size(); i < n; i++) {
                final int columnId = sort.getColumnIds().getQuick(i);
                spec.getOrderByColumnIds().add(columnId);
                spec.getOrderByDirections().add(sort.getDirections().getQuick(i));
                spec.getOrderByPositions().add(position);
                spec.getOrderByNames().add(output.getColumnName(output.getColumnIndexById(columnId)));
            }
            input = sort.getInput();
        } else if (input instanceof ProjectPlan project && project.getInput() instanceof SortPlan sort
                && orderThroughProject(project, sort, spec, position)) {
            project.replaceInput(0, sort.getInput());
        }
        final ExpressionNode rowNumberCall = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, "row_number", 0, position);
        final FunctionExpression rowNumber = windowBinder.bindWindowFunction(rowNumberCall, spec, output, model.getNestedModel(), executionContext);
        final WindowPlan window = ctx.windowPlans.next().of(input, position);
        window.getOutput().copyFrom(output);
        final int rowNumberId = ctx.nextColumnId++;
        window.getFunctions().add(rowNumber);
        window.getSpecs().add(spec);
        window.getFunctionColumnIds().add(rowNumberId);
        window.getOutput().add(rowNumberId, "__lateral_rn", rowNumber.getDataType(), false);

        final ExpressionNode rowNumberRef = ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, "__lateral_rn", 0, position);
        ExpressionNode predicate = lateralLimitComparison(">=", hi == null ? lo : hi, rowNumberRef, position);
        if (hi != null && lo != null) {
            final ExpressionNode lower = lateralLimitComparison("<", lo, rowNumberRef, position);
            final ExpressionNode and = ctx.bindingExpressions.next().of(ExpressionNode.OPERATION, "and", 0, position);
            and.paramCount = 2;
            and.lhs = predicate;
            and.rhs = lower;
            predicate = and;
        }
        ctx.substitutionNodes.clear();
        ctx.substitutionColumns.clear();
        ctx.substitutionNodes.add(rowNumberRef);
        ctx.substitutionColumns.add(ctx.columns.next().of(rowNumberId, rowNumber.getDataType(), position));
        final BoundExpression bound = ctx.functionBinder.bind(predicate, window.getOutput(), null, ctx.substitutionNodes, ctx.substitutionColumns, executionContext);
        final FilterPlan filter = ctx.filters.next().of(window, bound, position);
        filter.getOutput().copyFrom(window.getOutput());
        ctx.stopTimestampIntrinsics(filter.getOutput());
        if (wrapper != null) {
            wrapper.replaceInput(0, filter);
            return wrapper;
        }
        return filter;
    }

    ExpressionNode bindCorrelation(QueryModel model, QueryModel source, OutputSchema scope, CharSequence alias, JoinPlan join,
                                           int deferredBase, ExpressionNode where, SqlExecutionContext executionContext) throws SqlException {
        collectBlockOuterReferences(model, source, scope, where, alias);
        collectDeferredOuterReferences(join, deferredBase);
        correlationDomain = null;
        if (outerReferences.size() == 0) {
            return where;
        }
        if (!ctx.isSetOperationBranch && (isLiftableBody(model, source) || isFullyEliminable(where, scope, alias))) {
            where = eliminateOuterReferences(where, scope, alias);
        }
        if (outerReferences.size() > 0 && (scope.getCorrelatedAliasCount() > 0 || hasLateralOuterReference())
                && isLiftableBody(model, source)) {
            where = liftOuterConjuncts(where, scope, alias);
            liftOuterSelections(model, scope, alias);
            collectBlockOuterReferences(model, source, scope, where, alias);
            collectDeferredOuterReferences(join, deferredBase);
        }
        if (outerReferences.size() > 0) {
            correlationDomain = bindDomain(source.getModelPosition(), outerRefId(), executionContext);
        }
        return where;
    }

    void bindDeferredCorrelations(JoinPlan join, int deferredBase) {
        final OutputSchema target = join.getOutput();
        for (int i = deferredBase, n = deferredCorrelationInputs.size(); i < n; i++) {
            final int inputIndex = deferredCorrelationInputs.getQuick(i);
            final JoinInput input = join.getInputs().getQuick(inputIndex);
            final OutputSchema output = input.getSourceOutput();
            final int alias = deferredCorrelationAliases.getQuick(i);
            final CharSequence name = output.getCorrelatedAliasName(alias);
            final CharSequence qualifier = output.getCorrelatedAliasQualifier(alias);
            final int master = precedingCorrelatedColumn(join, qualifier, name, inputIndex);
            final int column = output.getCorrelatedAliasIndex(alias);
            if (master < 0) {
                target.addCorrelatedAlias(qualifier, name, target.getColumnIndexById(output.getColumnId(column)));
                continue;
            }
            lateralEqualityInputs.add(inputIndex);
            lateralEqualityMasterIds.add(target.getColumnId(master));
            lateralEqualityMasterNames.add(ctx.qualifiedJoinName(target.getColumnQualifier(master), target.getColumnName(master)));
            lateralEqualitySlaveIds.add(output.getColumnId(column));
            lateralEqualitySlaveNames.add(ctx.qualifiedJoinName(input.getBindingAlias(), output.getColumnName(column)));
            lateralEqualityPositions.add(input.getPosition());
        }
    }

    LogicalPlan bindLateral(QueryModel model, QueryModel source, JoinPlan join, int index, CharSequence alias, ExpressionNode where,
                                    SqlExecutionContext executionContext) throws SqlException {
        final QueryModel occurrence = source.getJoinModels().getQuick(index);
        final LateralScope lateral = lateralScopePool.next().of(model, source, where);
        lateral.isLeft = occurrence.getJoinType() == QueryModel.JOIN_LATERAL_LEFT;
        for (int k = 0; k < index; k++) {
            final JoinInput input = join.getInputs().getQuick(k);
            final OutputSchema output = input.getSourceOutput();
            lateral.joinInputs.add(input);
            for (int c = 0, n = output.getColumnCount(); c < n; c++) {
                lateral.master.add(output.getColumnId(c), output.getColumnName(c), output.getColumnType(c),
                        output.getMetadata(c), output.isVisible(c), input.getBindingAlias());
                lateral.inputs.add(k);
                lateral.positions.add(c);
            }
        }
        if (ctx.lateralScopes.size() > 0 && substituteOuterEqualities(where, lateral.master, occurrence.getNestedModel())) {
            outerRefId();
        }
        if (countDriverSource != null && driveLateral(occurrence.getNestedModel(), join.getOutput(), occurrence.getJoinKeywordPosition(), executionContext)) {
            final OutputSchema driverOutput = countDriver.getSourceOutput();
            for (int c = 0, n = driverOutput.getColumnCount(); c < n; c++) {
                lateral.master.add(driverOutput.getColumnId(c), driverOutput.getColumnName(c), driverOutput.getColumnType(c),
                        driverOutput.getMetadata(c), true, isCountDriverQualified ? countDriver.getBindingAlias() : null);
                lateral.inputs.add(-1);
                lateral.positions.add(c);
            }
        }
        ctx.lateralScopes.add(lateral);
        final QueryModel previousBodyModel = lateralBodyModel;
        final CharSequence previousBodyAlias = lateralBodyAlias;
        final ExpressionNode previousLifted = lateralLifted;
        final int previousOuterRefId = lateralOuterRefId;
        lateralBodyModel = occurrence.getNestedModel();
        lateralBodyAlias = alias;
        lateralLifted = null;
        lateralOuterRefId = -1;
        isLateralScalarBody = false;
        final LogicalPlan body;
        try {
            body = binder.bindSource(model, occurrence, executionContext);
            if (lateralLifted != null) {
                lateralLiftedInputs.add(index);
                lateralLiftedPredicates.add(lateralLifted);
            }
        } finally {
            ctx.lateralScopes.remove(ctx.lateralScopes.size() - 1);
            lateralWrappedCountColumns.clear();
            lateralBodyModel = previousBodyModel;
            lateralBodyAlias = previousBodyAlias;
            lateralLifted = previousLifted;
            lateralOuterRefId = previousOuterRefId;
        }
        final OutputSchema output = body.getOutput();
        for (int i = 0, n = output.getCorrelatedAliasCount(); i < n; i++) {
            final CharSequence name = output.getCorrelatedAliasName(i);
            final int master = lateral.master.getColumnIndexQuiet(output.getCorrelatedAliasQualifier(i), name, 0, name.length());
            if (master < 0) {
                continue;
            }
            final int column = output.getCorrelatedAliasIndex(i);
            lateralEqualityInputs.add(index);
            lateralEqualityMasterIds.add(lateral.master.getColumnId(master));
            lateralEqualityMasterNames.add(ctx.qualifiedJoinName(lateral.master.getColumnQualifier(master), lateral.master.getColumnName(master)));
            lateralEqualitySlaveIds.add(output.getColumnId(column));
            lateralEqualitySlaveNames.add(ctx.qualifiedJoinName(alias, output.getColumnName(column)));
            lateralEqualityPositions.add(occurrence.getJoinKeywordPosition());
        }
        return body;
    }

    LogicalPlan compensateScalarAggregate(QueryModel model, AggregatePlan aggregate, ProjectPlan project,
                                                  SqlExecutionContext executionContext) throws SqlException {
        final OutputSchema output = project.getOutput();
        final int position = project.getPosition();
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            if (isWildcard(ctx.aggregateSelectExpressions.getQuick(i))) {
                return project;
            }
        }
        outerReferenceScopes.clear();
        outerReferences.clear();
        for (int i = 0, n = output.getCorrelatedAliasCount(); i < n; i++) {
            final CharSequence qualifier = output.getCorrelatedAliasQualifier(i);
            final CharSequence name = output.getCorrelatedAliasName(i);
            for (int d = ctx.lateralScopes.size() - 1; d >= 0; d--) {
                final int reference = ctx.lateralScopes.getQuick(d).master.getColumnIndexQuiet(qualifier, name, 0, name.length());
                if (reference >= 0) {
                    outerReferenceScopes.add(d);
                    outerReferences.add(reference);
                    break;
                }
            }
        }
        if (outerReferences.size() != output.getCorrelatedAliasCount()) {
            return project;
        }
        final int templateBase = compensationTemplates.size();
        try {
            for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
                final ExpressionNode expression = ctx.aggregateSelectExpressions.getQuick(i);
                compensationTemplates.add(hasZeroOnEmptyAggregate(expression) ? carrierTemplate(expression, aggregate, project, null) : null);
            }
            final LogicalPlan domain = bindDomain(position, outerReferenceSequence++, executionContext);
            final JoinPlan join = ctx.joins.next().of(position);
            final JoinInput master = ctx.joinInputs.next().of(domain, QueryModel.JOIN_CROSS, domainAlias, position);
            final JoinInput slave = ctx.joinInputs.next().of(project, QueryModel.JOIN_LEFT_OUTER, null, position);
            join.getInputs().add(master);
            join.getInputs().add(slave);
            join.getOrderedInputs().addAll(join.getInputs());
            binder.joinBinder.addJoinOutput(join, domain.getOutput(), domainAlias);
            for (int i = 0, n = output.getColumnCount(); i < n; i++) {
                join.getOutput().add(output.getColumnId(i), output.getColumnName(i), output.getColumnType(i), output.getMetadata(i), output.isVisible(i));
            }
            final OutputSchema domainOutput = domain.getOutput();
            for (int i = 0, n = output.getCorrelatedAliasCount(); i < n; i++) {
                final CharSequence name = output.getCorrelatedAliasName(i);
                final int domainIndex = domainOutput.getCorrelatedColumnIndexQuiet(output.getCorrelatedAliasQualifier(i), name, 0, name.length());
                final int index = output.getCorrelatedAliasIndex(i);
                addJoinKey(slave, domainOutput.getColumnId(domainIndex), output.getColumnId(index),
                        domainOutput.getColumnName(domainIndex), output.getColumnName(index), position);
            }
            final ProjectPlan compensated = ctx.projects.next().of(join, position);
            for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
                final ExpressionNode template = compensationTemplates.getQuick(templateBase + i);
                final BoundExpression bound = template == null
                        ? ctx.columns.next().of(output.getColumnId(i), output.getColumnType(i), position)
                        : ctx.functionBinder.bind(template, join.getOutput(), null, executionContext);
                compensated.getExpressions().add(bound);
                compensated.getOutput().add(ctx.nextColumnId++, output.getColumnName(i), bound.getDataType(), output.getMetadata(i), true);
            }
            exposeCorrelated(compensated, join.getOutput(), null);
            return compensated;
        } finally {
            compensationTemplates.setPos(templateBase);
        }
    }

    LogicalPlan correlateBranch(LogicalPlan branch, OutputSchema other, int position, SqlExecutionContext executionContext) throws SqlException {
        final OutputSchema output = branch.getOutput();
        outerReferenceScopes.clear();
        outerReferences.clear();
        for (int i = 0, n = other.getCorrelatedAliasCount(); i < n; i++) {
            final CharSequence name = other.getCorrelatedAliasName(i);
            if (output.getCorrelatedColumnIndexQuiet(other.getCorrelatedAliasQualifier(i), name, 0, name.length()) < 0) {
                addOuterReference(other.getCorrelatedAliasQualifier(i), name);
            }
        }
        if (outerReferences.size() == 0) {
            return branch;
        }
        final LogicalPlan domain = bindDomain(position, outerReferenceSequence++, executionContext);
        return crossJoin(branch, null, domain, position);
    }

    CharSequence correlatedName() {
        final CharacterStoreEntry name = ctx.characterStore.newEntry();
        name.put("__qdb_correlated_").put(ctx.nextColumnId);
        return name.toImmutable();
    }

    LogicalPlan correlateSource(QueryModel model, QueryModel source, CharSequence alias, LogicalPlan sourcePlan,
                                        ExpressionNode where, SqlExecutionContext executionContext) throws SqlException {
        ctx.correlatedWhere = bindCorrelation(model, source, sourcePlan.getOutput(), alias, null, 0, where, executionContext);
        if (correlationDomain == null) {
            return sourcePlan;
        }
        final int position = source.getModelPosition();
        final JoinPlan join = ctx.joins.next().of(position);
        join.getInputs().add(ctx.joinInputs.next().of(sourcePlan, QueryModel.JOIN_CROSS, sourceAlias(source), position));
        join.getInputs().add(ctx.joinInputs.next().of(correlationDomain, QueryModel.JOIN_CROSS, domainAlias, position));
        binder.joinBinder.addJoinOutput(join, sourcePlan.getOutput(), sourceAlias(source));
        binder.joinBinder.addJoinOutput(join, correlationDomain.getOutput(), domainAlias);
        correlationDomain = null;
        binder.joinBinder.bindJoinConditions(join, source, ctx.correlatedWhere, lateralEqualityInputs.size(), 0, executionContext);
        ctx.correlatedWhere = null;
        return join;
    }

    JoinPlan crossJoin(LogicalPlan left, CharSequence leftAlias, LogicalPlan right, int position) {
        final JoinPlan join = ctx.joins.next().of(position);
        join.getInputs().add(ctx.joinInputs.next().of(left, QueryModel.JOIN_CROSS, leftAlias, position));
        join.getInputs().add(ctx.joinInputs.next().of(right, QueryModel.JOIN_CROSS, null, position));
        join.getOrderedInputs().addAll(join.getInputs());
        binder.joinBinder.addJoinOutput(join, left.getOutput(), leftAlias);
        binder.joinBinder.addJoinOutput(join, right.getOutput(), null);
        join.getOutput().setTimestampIndex(left.getOutput().getTimestampIndex());
        return join;
    }

    void exposeCorrelated(ProjectPlan project, OutputSchema input, AggregatePlan aggregate) {
        if (aggregate == null) {
            for (int i = 0, n = input.getCorrelatedAliasCount(); i < n; i++) {
                exposeCorrelated(project, input, input.getCorrelatedAliasIndex(i), null, input.getCorrelatedAliasQualifier(i), input.getCorrelatedAliasName(i));
                project.getOutput().addCorrelatedAlias(input.getCorrelatedAliasQualifier(i), input.getCorrelatedAliasName(i),
                        project.getOutput().getColumnCount() - 1);
            }
            return;
        }
        final int base = project.getOutput().getColumnCount();
        for (int g = 0, gn = aggregate.getGroupingExpressions().size(); g < gn; g++) {
            for (int i = 0, n = input.getCorrelatedAliasCount(); i < n; i++) {
                if (findGroupingColumn(aggregate, input.getColumnId(input.getCorrelatedAliasIndex(i))) == g) {
                    exposeCorrelated(project, input, input.getCorrelatedAliasIndex(i), aggregate, input.getCorrelatedAliasQualifier(i), input.getCorrelatedAliasName(i));
                    break;
                }
            }
        }
        for (int i = 0, n = input.getCorrelatedAliasCount(); i < n; i++) {
            final int columnId = aggregate.getOutput().getColumnId(findGroupingColumn(aggregate, input.getColumnId(input.getCorrelatedAliasIndex(i))));
            for (int k = base; ; k++) {
                if (((ColumnExpression) project.getExpressions().getQuick(k)).getColumnId() == columnId) {
                    project.getOutput().addCorrelatedAlias(input.getCorrelatedAliasQualifier(i), input.getCorrelatedAliasName(i), k);
                    break;
                }
            }
        }
    }

    void exposeCorrelated(ProjectPlan project, OutputSchema input, int index, AggregatePlan aggregate, CharSequence outerQualifier, CharSequence outerName) {
        int columnId = input.getColumnId(index);
        CharSequence columnName = lateralLifted != null ? input.getColumnName(index)
                : outerRefName(input, outerQualifier, outerName, input.isVisible(index) ? null : input.getColumnName(index));
        if (aggregate != null) {
            final int grouping = findGroupingColumn(aggregate, columnId);
            columnId = aggregate.getOutput().getColumnId(grouping);
            columnName = aggregate.getOutput().getColumnName(grouping);
            if (grouping < ctx.groupingNodes.size() && ctx.groupingNodes.getQuick(grouping) != null && !aggregate.getOutput().isVisible(grouping)) {
                columnName = outerRefName(input, outerQualifier, outerName, columnName);
            }
        }
        for (int i = 0, n = project.getOutput().getColumnCount(); i < n && columnName != null; i++) {
            if (Chars.equalsIgnoreCase(columnName, project.getOutput().getColumnName(i))) {
                columnName = null;
            }
        }
        project.getExpressions().add(ctx.columns.next().of(columnId, input.getColumnType(index), project.getPosition()));
        project.getOutput().add(ctx.nextColumnId, columnName != null ? columnName : correlatedName(), input.getColumnType(index), input.getMetadata(index), false);
        ctx.nextColumnId++;
    }

    void exposeLifted(ProjectPlan project, OutputSchema input) {
        for (int i = 0, n = liftedIndexes.size(); i < n; i++) {
            final int index = liftedIndexes.getQuick(i);
            project.getExpressions().add(ctx.columns.next().of(input.getColumnId(index), input.getColumnType(index), project.getPosition()));
            project.getOutput().add(ctx.nextColumnId, liftedNames.getQuick(i), input.getColumnType(index), input.getMetadata(index), true);
            ctx.wildcardExcludedIds.add(ctx.nextColumnId++);
        }
        liftedIndexes.clear();
        liftedNames.clear();
    }

    /**
     * A LEFT LATERAL over a scalar body yields one row per outer row, whose values an empty body
     * compensates. Its ON therefore applies to the compensated values: every lateral output reads
     * NULL unless ON holds for them.
     */
    void guardLateralOutputs(OutputSchema lateral, CharSequence alias, ExpressionNode criteria, int templateStart, OutputSchema scope) {
        final int templateEnd = lateralCarrierIds.size();
        for (int i = 0, n = lateral.getColumnCount(); i < n; i++) {
            if (!lateral.isVisible(i) || lateralCarrierIds.indexOf(lateral.getColumnId(i), templateStart, templateEnd) >= 0) {
                continue;
            }
            final CharSequence self = ctx.qualifiedJoinName(alias, lateral.getColumnName(i));
            addLateralTemplate(lateral.getColumnId(i), ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, self, 0, criteria.position), self);
        }
        final ExpressionNode condition = replaceLateralCarriers(ExpressionNode.deepClone(ctx.bindingExpressions, criteria), scope);
        for (int i = templateStart, n = lateralCarrierIds.size(); i < n; i++) {
            final ExpressionNode template = lateralTemplates.getQuick(i);
            final ExpressionNode guarded = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, "case", 0, template.position);
            guarded.paramCount = 3;
            guarded.args.add(ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, "null", 0, template.position));
            guarded.args.add(template);
            guarded.args.add(ExpressionNode.deepClone(ctx.bindingExpressions, condition));
            lateralTemplates.setQuick(i, guarded);
        }
    }

    boolean hasLateralCarrierOrder(QueryModel source, OutputSchema scope) {
        for (int i = 0, n = source.getOrderBy().size(); i < n && lateralCarrierIds.size() > 0; i++) {
            final ExpressionNode order = source.getOrderBy().getQuick(i);
            if (order.type == ExpressionNode.LITERAL && lateralCarrierIndex(order.token, scope) >= 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Moves WHERE conjuncts over compensated lateral outputs above the projection, where they
     * read the projected values instead of evaluating the compensation again.
     */
    ExpressionNode hoistLateralCarrierTerms(ExpressionNode node, QueryModel model, OutputSchema scope) {
        if (node == null) {
            return null;
        }
        if (SqlKeywords.isAndKeyword(node.token) && node.paramCount == 2) {
            final ExpressionNode left = hoistLateralCarrierTerms(node.lhs, model, scope);
            final ExpressionNode right = hoistLateralCarrierTerms(node.rhs, model, scope);
            if (left == null) {
                return right;
            }
            if (right == null) {
                return left;
            }
            node.lhs = left;
            node.rhs = right;
            return node;
        }
        if (!hasLateralCarrierReference(node, scope) || hasUnprojectedTerm(node, model)) {
            return node;
        }
        if (lateralHoistedWhere == null) {
            lateralHoistedWhere = node;
        } else {
            final ExpressionNode and = ctx.bindingExpressions.next().of(ExpressionNode.OPERATION, "and", 0, node.position);
            and.paramCount = 2;
            and.lhs = lateralHoistedWhere;
            and.rhs = node;
            lateralHoistedWhere = and;
        }
        return null;
    }

    QueryModel innerLateralOccurrence(QueryModel model) {
        if (lateralBodyModel == null || model == lateralBodyModel || ctx.lateralScopes.size() == 0) {
            return null;
        }
        QueryModel layer = lateralBodyModel;
        while (layer != null && layer != model) {
            layer = layer.getNestedModel();
        }
        if (layer == null) {
            return null;
        }
        final ObjList<QueryModel> sources = ctx.lateralScopes.getLast().source.getJoinModels();
        for (int i = 1, n = sources.size(); i < n; i++) {
            final QueryModel occurrence = sources.getQuick(i);
            if (occurrence.getNestedModel() == lateralBodyModel) {
                return occurrence.getJoinType() == QueryModel.JOIN_LATERAL_LEFT ? null : occurrence;
            }
        }
        return null;
    }

    boolean isCorrelatedToInnermostLateral(OutputSchema output) {
        final OutputSchema master = ctx.lateralScopes.getLast().master;
        for (int i = 0, n = output.getCorrelatedAliasCount(); i < n; i++) {
            final CharSequence name = output.getCorrelatedAliasName(i);
            if (master.getColumnIndexQuiet(output.getCorrelatedAliasQualifier(i), name, 0, name.length()) < 0) {
                return false;
            }
        }
        return true;
    }

    boolean isCountDriverBlock(QueryModel source, int sourceLimit) {
        if (ctx.lateralScopes.size() == 0 || !ctx.lateralScopes.getLast().isLeft) {
            return false;
        }
        final OutputSchema master = ctx.lateralScopes.getLast().master;
        driverReferences.clear();
        boolean isBlock = false;
        isCountDriverQualified = false;
        for (int i = 1; i < sourceLimit; i++) {
            final QueryModel occurrence = source.getJoinModels().getQuick(i);
            if (QueryModel.isLateralJoin(occurrence.getJoinType()) && isScalarCountBody(occurrence.getNestedModel())
                    && collectDriverReferences(occurrence.getNestedModel(), master)) {
                isBlock = true;
                isCountDriverQualified |= occurrence.getJoinType() != QueryModel.JOIN_LATERAL_LEFT;
            }
        }
        return isBlock;
    }

    boolean isLateralWhereHoistable(QueryModel model, QueryModel source) {
        final ObjList<QueryColumn> columns = model.getBottomUpColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            if (isWildcard(columns.getQuick(i).getAst())) {
                return false;
            }
        }
        return !model.isDistinct() && !model.isUpdate() && source.getSampleBy() == null && source.getLatestBy().size() == 0
                && source.getSubsample() == null && !aggregateBinder.hasAggregation(model, source) && !windowBinder.hasWindows(model, source);
    }

    boolean isOuterOnly(ExpressionNode node, OutputSchema scope, CharSequence alias) {
        if (node == null) {
            return true;
        }
        if (node.type == ExpressionNode.LITERAL) {
            if (isCorrelatedReference(node, scope)) {
                return true;
            }
            if (FunctionBinder.findColumn(node, scope, alias) >= 0) {
                return false;
            }
            for (int d = ctx.lateralScopes.size() - 1; d >= 0; d--) {
                if (FunctionBinder.findColumn(node, ctx.lateralScopes.getQuick(d).master, null) >= 0) {
                    return true;
                }
            }
            return false;
        }
        if (node.queryModel != null || !isOuterOnly(node.lhs, scope, alias) || !isOuterOnly(node.rhs, scope, alias)) {
            return false;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (!isOuterOnly(node.args.getQuick(i), scope, alias)) {
                return false;
            }
        }
        return true;
    }

    boolean isWrappedScalarBody(QueryModel model) {
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            if (!ctx.isAggregate(ctx.aggregateSelectExpressions.getQuick(i))) {
                return false;
            }
        }
        for (QueryModel layer = lateralBodyModel; layer != model; layer = layer.getNestedModel()) {
            if (layer.getGroupBy().size() > 0 || layer.getUnionModel() != null || layer.isDistinct()
                    || layer.getLimitLo() != null || layer.getLimitHi() != null || layer.getSampleBy() != null
                    || layer.getLatestBy().size() > 0 || layer.getJoinModels().size() > 1
                    || (layer.getNestedModel() == model ? !isTotalCountFilter(layer.getWhereClause(), model) : layer.getWhereClause() != null)
                    || layer != lateralBodyModel && layer.getBottomUpColumns().size() > 0) {
                return false;
            }
        }
        final int base = lateralWrappedCountColumns.size();
        final ObjList<QueryColumn> columns = lateralBodyModel.getBottomUpColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final QueryColumn column = columns.getQuick(i);
            final ExpressionNode ast = column.getAst();
            if (column instanceof WindowExpression || isWildcard(ast) || !isNullPropagating(ast) || !hasAggregateReference(ast, model)) {
                lateralWrappedCountColumns.setPos(base);
                return false;
            }
            if (ast.type == ExpressionNode.LITERAL && isCountColumn(ast, model)) {
                lateralWrappedCountColumns.add(i);
            } else if (hasCountReference(ast, model)) {
                lateralWrappedCountColumns.setPos(base);
                return false;
            }
        }
        return true;
    }

    boolean isWrapperRejectingZeroCount(QueryModel model) {
        for (QueryModel layer = lateralBodyModel; layer != model; layer = layer.getNestedModel()) {
            if (layer.getNestedModel() == model && layer.getJoinModels().size() == 1
                    && isRejectingZeroCount(layer.getWhereClause(), model)) {
                return true;
            }
        }
        return false;
    }

    ExpressionNode lateralCompensatedColumn(OutputSchema scope, int index, int position) {
        final int carrier = lateralCarrierIds.size() == 0 ? -1
                : lateralCarrierIds.indexOf(scope.getColumnId(index), 0, lateralCarrierIds.size());
        if (carrier < 0) {
            return null;
        }
        final CharSequence qualifier = scope.getColumnQualifier(index);
        final CharacterStoreEntry token = ctx.characterStore.newEntry();
        if (qualifier != null) {
            token.put(qualifier).put('.');
        }
        token.put(scope.getColumnName(index));
        final ExpressionNode template = ExpressionNode.deepClone(ctx.bindingExpressions, lateralTemplates.getQuick(carrier));
        renameTemplateSelf(template, lateralTemplateNames.getQuick(carrier), token.toImmutable());
        template.position = position;
        return template;
    }

    ExpressionNode liftedCriteria(ExpressionNode criteria, int input) {
        if (lateralGuardInputs.indexOf(input, 0, lateralGuardInputs.size()) >= 0) {
            // The guarded outputs evaluate this ON over the compensated lateral values.
            criteria = null;
        }
        for (int i = 0, n = lateralLiftedInputs.size(); i < n; i++) {
            if (lateralLiftedInputs.getQuick(i) == input) {
                final ExpressionNode lifted = lateralLiftedPredicates.getQuick(i);
                if (criteria == null || isTrivialCondition(criteria)) {
                    return lifted;
                }
                final ExpressionNode and = ctx.bindingExpressions.next().of(ExpressionNode.OPERATION, "and", 0, lifted.position);
                and.paramCount = 2;
                and.lhs = criteria;
                and.rhs = lifted;
                return and;
            }
        }
        return criteria;
    }

    LogicalPlan orderCorrelatedColumns(LogicalPlan branch, OutputSchema template, int position) {
        final OutputSchema output = branch.getOutput();
        final ProjectPlan project = ctx.projects.next().of(branch, position);
        final OutputSchema projected = project.getOutput();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            boolean isCorrelated = false;
            for (int k = 0, m = output.getCorrelatedAliasCount(); k < m && !isCorrelated; k++) {
                isCorrelated = output.getCorrelatedAliasIndex(k) == i;
            }
            if (!isCorrelated) {
                project.getExpressions().add(ctx.columns.next().of(output.getColumnId(i), output.getColumnType(i), position));
                projected.add(ctx.nextColumnId++, output.getColumnName(i), output.getColumnType(i), output.getMetadata(i), output.isVisible(i));
            }
        }
        for (int i = 0, n = template.getCorrelatedAliasCount(); i < n; i++) {
            final CharSequence qualifier = template.getCorrelatedAliasQualifier(i);
            final CharSequence name = template.getCorrelatedAliasName(i);
            final int index = output.getCorrelatedColumnIndexQuiet(qualifier, name, 0, name.length());
            project.getExpressions().add(ctx.columns.next().of(output.getColumnId(index), output.getColumnType(index), position));
            projected.add(ctx.nextColumnId++, output.getColumnName(index), output.getColumnType(index), output.getMetadata(index), false);
            projected.addCorrelatedAlias(qualifier, name, projected.getColumnCount() - 1);
        }
        return project;
    }

    void registerScalarBodyTemplates(QueryModel model, AggregatePlan aggregate, ProjectPlan project) throws SqlException {
        isLateralScalarBody = true;
        boolean hasBareCount = false;
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n && !hasBareCount; i++) {
            hasBareCount = SqlUtil.isZeroOnEmptyAggregate(ctx.aggregateSelectExpressions.getQuick(i));
        }
        final ExpressionNode guard = scalarLimitGuard(model, !hasBareCount);
        lateralScalarGuard = guard;
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            if (isWildcard(ctx.aggregateSelectExpressions.getQuick(i))) {
                return;
            }
        }
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            final ExpressionNode expression = ctx.aggregateSelectExpressions.getQuick(i);
            if (!hasZeroOnEmptyAggregate(expression) && ctx.hasAggregate(expression)) {
                continue;
            }
            final CharSequence self = ctx.qualifiedJoinName(lateralBodyAlias, project.getOutput().getColumnName(i));
            ExpressionNode template = SqlUtil.isZeroOnEmptyAggregate(expression)
                    ? zeroCoalesce(ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, self, 0, expression.position))
                    : carrierTemplate(expression, aggregate, project, lateralBodyAlias);
            if (guard != null) {
                final ExpressionNode guarded = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, "case", 0, expression.position);
                guarded.paramCount = 3;
                guarded.args.add(ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, "null", 0, expression.position));
                guarded.args.add(template);
                guarded.args.add(ExpressionNode.deepClone(ctx.bindingExpressions, guard));
                template = guarded;
            }
            addLateralTemplate(project.getOutput().getColumnId(i), template, self);
        }
    }

    ExpressionNode scalarLimitGuard(QueryModel model, boolean isOuterReferenceAllowed) throws SqlException {
        final ExpressionNode lo = model.getLimitLo();
        final ExpressionNode hi = model.getLimitHi();
        if (lo == null && hi == null) {
            return null;
        }
        if (!isOuterReferenceAllowed && (hasLiteral(lo) || hasLiteral(hi))) {
            throw SqlException.$(hasLiteral(lo) ? lo.position : hi.position,
                    "LIMIT referencing an outer column is not supported over a scalar count in a correlated lateral sub-query; use a constant or bind variable");
        }
        final ExpressionNode one = ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, "1", 0, lo != null ? lo.position : hi.position);
        if (hi == null) {
            return lateralLimitComparison(">=", lo, one, lo.position);
        }
        final ExpressionNode upper = lateralLimitComparison(">=", hi, one, hi.position);
        if (lo == null) {
            return upper;
        }
        final ExpressionNode and = ctx.bindingExpressions.next().of(ExpressionNode.OPERATION, "and", 0, lo.position);
        and.paramCount = 2;
        and.lhs = lateralLimitComparison("<", lo, one, lo.position);
        and.rhs = upper;
        return and;
    }

    ExpressionNode substituteLateralCounts(ExpressionNode node, OutputSchema scope) {
        if (lateralCarrierIds.size() == 0 || !hasLateralCarrierReference(node, scope)) {
            return node;
        }
        return replaceLateralCarriers(ExpressionNode.deepClone(ctx.bindingExpressions, node), scope);
    }

    void truncateLateralEqualities(int size, int liftedSize) {
        lateralLiftedInputs.setPos(liftedSize);
        lateralLiftedPredicates.setPos(liftedSize);
        truncateLateralEqualities(size);
    }

    void truncateLateralEqualities(int size) {
        lateralEqualityInputs.setPos(size);
        lateralEqualityMasterIds.setPos(size);
        lateralEqualityMasterNames.setPos(size);
        lateralEqualityPositions.setPos(size);
        lateralEqualitySlaveIds.setPos(size);
        lateralEqualitySlaveNames.setPos(size);
    }

    void validateOuterJoinColumns(ExpressionNode expression, OutputSchema output) throws SqlException {
        if (expression == null) {
            return;
        }
        if (expression.type == ExpressionNode.LITERAL) {
            final CharSequence token = expression.token;
            final int dot = Chars.indexOfLastUnquoted(token, '.');
            if (dot > -1 && FunctionBinder.findColumn(expression, output, null) == -1
                    && !FunctionBinder.isUnknownQualifier(GenericLexer.unquote(token.subSequence(0, dot)), output, null)) {
                throw SqlException.position(expression.position).put("Invalid column: ").put(token, dot + 1, token.length());
            }
            return;
        }
        validateOuterJoinColumns(expression.lhs, output);
        validateOuterJoinColumns(expression.rhs, output);
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            validateOuterJoinColumns(expression.args.getQuick(i), output);
        }
    }

    ExpressionNode zeroCoalesce(ExpressionNode value) {
        final ExpressionNode coalesce = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, "coalesce", 0, value.position);
        coalesce.paramCount = 2;
        coalesce.lhs = value;
        coalesce.rhs = ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, "0", 0, value.position);
        return coalesce;
    }
}
