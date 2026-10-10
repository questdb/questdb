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

package io.questdb.griffin.bind;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.BoundExpressionRewriter;
import io.questdb.griffin.CharacterStore;
import io.questdb.griffin.CharacterStoreEntry;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.FunctionInstantiator;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.OuterColumnReads;
import io.questdb.griffin.PlanNodePools;
import io.questdb.griffin.PlanTables;
import io.questdb.griffin.PreparedFunctions;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.SubqueryCompiler;
import io.questdb.griffin.TableFunctionSources;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.ForwardingPlan;
import io.questdb.griffin.plan.logical.GroupingPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.std.Chars;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * The binding context the compiler's binder services share: the statement's pools and preparations, the expression
 * stage, the temporaries whose every use is a leaf step and the {@link BindScope} of the query binding now.
 */
public final class BindContext implements Mutable {
    final ObjectPool<ExpressionNode> bindingExpressions;
    final CharacterStore characterStore;
    final BoundExpressionRewriter expressionRewriter;
    final FunctionBinder functionBinder;
    final FunctionFactoryCache functionFactoryCache;
    final FunctionInstantiator functionInstantiator;
    final TableFunctionSources functionSources;
    final OuterColumnReads outerColumnReads = new OuterColumnReads();
    final PlanNodePools planNodes;
    final PlanTables planTables;
    final PreparedFunctions preparedFunctions;
    final ObjList<BoundExpression> tmpArguments;
    final IntList tmpOuterColumns;
    final IntList tmpPositions;
    final OutputSchema tmpScope;
    private final BindScopeStack scopes;
    /**
     * The query block being bound is a set-operation branch: the set operation binds the trailing
     * ORDER BY and LIMIT the parser places on its last branch, so no branch binds them itself. Every query level
     * resets it while it binds.
     */
    boolean isSetOperationBranch;

    /**
     * The context of the compiler's binder; the statement's preparations, the expression rewriter and the function
     * instantiator are the compiler's, and the rewriter and instantiator share the argument, position and scope
     * temporaries with this context.
     */
    public BindContext(
            SubqueryCompiler subqueryCompiler,
            FunctionParser functionParser,
            BindScopeStack scopes,
            ObjectPool<ExpressionNode> bindingExpressions,
            CharacterStore characterStore,
            PlanNodePools planNodes,
            PlanTables planTables,
            PreparedFunctions preparedFunctions,
            TableFunctionSources functionSources,
            BoundExpressionRewriter expressionRewriter,
            FunctionInstantiator functionInstantiator,
            ObjList<BoundExpression> tmpArguments,
            IntList tmpPositions,
            OutputSchema tmpScope,
            IntList tmpOuterColumns
    ) {
        this.scopes = scopes;
        this.bindingExpressions = bindingExpressions;
        this.characterStore = characterStore;
        this.planNodes = planNodes;
        this.planTables = planTables;
        this.preparedFunctions = preparedFunctions;
        this.functionSources = functionSources;
        this.expressionRewriter = expressionRewriter;
        this.functionInstantiator = functionInstantiator;
        this.tmpArguments = tmpArguments;
        this.tmpPositions = tmpPositions;
        this.tmpScope = tmpScope;
        this.tmpOuterColumns = tmpOuterColumns;
        this.functionFactoryCache = functionParser.getFunctionFactoryCache();
        this.functionBinder = new FunctionBinder(this, functionParser.getFunctionResolver(), subqueryCompiler);
    }

    @Override
    public void clear() {
        functionBinder.clear();
        tmpScope.clear();
        isSetOperationBranch = false;
    }

    private static boolean hasVisibleColumn(OutputSchema schema, CharSequence name) {
        for (int i = 0, n = schema.getColumnCount(); i < n; i++) {
            if (schema.isVisible(i) && Chars.equalsIgnoreCase(schema.getColumnName(i), name)) {
                return true;
            }
        }
        return false;
    }

    static ObjList<ExpressionNode> blockGroupBy(QueryModel model, QueryModel source) {
        return source.getGroupBy().size() > 0 && !source.isPivot() ? source.getGroupBy() : model.getGroupBy();
    }

    static int findGroupingColumn(GroupingPlan aggregate, int columnId) {
        for (int i = 0, n = aggregate.getGroupingExpressions().size(); i < n; i++) {
            if (aggregate.getGroupingExpressions().getQuick(i) instanceof ColumnExpression column && column.getColumnId() == columnId) {
                return i;
            }
        }
        return -1;
    }

    /**
     * The first column reference of the expression in operand order, or null when it has none.
     */
    static ExpressionNode firstColumnReference(ExpressionNode expression) {
        if (expression == null || expression.type == ExpressionNode.LITERAL) {
            return expression;
        }
        if (expression.paramCount < 3) {
            final ExpressionNode left = firstColumnReference(expression.lhs);
            return left != null ? left : firstColumnReference(expression.rhs);
        }
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            final ExpressionNode column = firstColumnReference(expression.args.getQuick(i));
            if (column != null) {
                return column;
            }
        }
        return null;
    }

    static int getColumnIndexQuiet(OutputSchema schema, CharSequence name) {
        final int index = schema.getColumnIndexQuiet(name);
        return index >= 0 || !SqlUtil.isQuoteProtectedAlias(name)
                ? index : schema.getColumnIndexQuiet(name, 1, name.length() - 1);
    }

    static boolean hasColumnReference(ExpressionNode expression) {
        return firstColumnReference(expression) != null;
    }

    static boolean hasComputedProjection(ProjectPlan project) {
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (!(project.getExpressions().getQuick(i) instanceof ColumnExpression)) {
                return true;
            }
        }
        return false;
    }

    static boolean isPlainColumnProjection(ProjectPlan project) {
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (!(project.getExpressions().getQuick(i) instanceof ColumnExpression column) || column.isCast()) {
                return false;
            }
        }
        return true;
    }

    static boolean isSameColumn(ExpressionNode left, ExpressionNode right, OutputSchema input, QueryModel source) {
        final int index = FunctionBinder.findColumn(left, input, sourceAlias(source));
        return index >= 0 && index == FunctionBinder.findColumn(right, input, sourceAlias(source));
    }

    static boolean isRowCount(ExpressionNode expression) {
        return expression.type == ExpressionNode.FUNCTION && expression.windowExpression == null
                && SqlKeywords.isCountKeyword(expression.token)
                && (expression.paramCount == 0 || expression.paramCount == 1
                && expression.rhs.type == ExpressionNode.CONSTANT && !SqlKeywords.isNullKeyword(expression.rhs.token));
    }

    static boolean isWildcardColumn(ExpressionNode expression, OutputSchema input, int index, CharSequence inputAlias) {
        if (!input.isVisible(index)) {
            return false;
        }
        final int dot = Chars.indexOfLastUnquoted(expression.token, '.');
        if (dot < 0) {
            return true;
        }
        final CharSequence qualifier = input.hasColumnQualifiers() ? input.getColumnQualifier(index) : inputAlias;
        return Chars.equalsIgnoreCase(GenericLexer.unquote(expression.token.subSequence(0, dot)), qualifier);
    }

    /**
     * The error of a WHERE or ON conjunct that binds to a value of a type other than BOOLEAN: the type mismatch of
     * the conjunction when other conjuncts of its WHERE clause or ON condition join the same filter, otherwise
     * "boolean expression expected". A column that the filter of its own table names with a qualifier reads
     * unqualified there, so its error has no source position (0).
     */
    static SqlException nonBooleanConjunct(ExpressionNode expression, int type, int position, boolean isTableFilter, boolean isFilterShared) {
        final boolean isUnqualified = isTableFilter && expression.type == ExpressionNode.LITERAL
                && Chars.indexOfLastUnquoted(expression.token, '.') > 0;
        final int errorPosition = isUnqualified ? 0 : position;
        return isFilterShared
                ? SqlException.$(errorPosition, "expression type mismatch, expected: BOOLEAN, actual: ").put(ColumnType.nameOf(type))
                : SqlException.$(errorPosition, "boolean expression expected");
    }

    static SqlException orderNotSelected(ExpressionNode order) {
        return SqlException.$(order.position, "ORDER BY expressions must appear in select list. Invalid column: ").put(order.token);
    }

    static CharSequence sourceAlias(QueryModel source) {
        final ExpressionNode name = source.getAlias() == null ? source.getTableNameExpr() : source.getAlias();
        return name == null ? null : GenericLexer.unquote(name.token);
    }

    static CharSequence unqualified(CharSequence token) {
        final int dot = Chars.indexOfLastUnquoted(token, '.');
        return dot < 0 ? token : token.subSequence(dot + 1, token.length());
    }

    void addProjection(ProjectPlan project, BoundExpression expression, OutputSchema nestedSchema, CharSequence requestedName, boolean isVisible) {
        final BindScope scope = scopes.current();
        project.getExpressions().add(expression);
        if (expression instanceof ColumnExpression column) {
            final OutputSchema input = project.getInput().getOutput();
            final int index = input.getColumnIndexById(column.getColumnId());
            project.getOutput().add(planNodes.nextColumnId(), createOutputName(requestedName), expression.getDataType(),
                    nestedSchema == null && index >= 0 ? input.getMetadata(index) : nestedSchema, isVisible);
            project.getOutput().setSymbolTableStatic(project.getOutput().getColumnCount() - 1, index >= 0 && input.isSymbolTableStatic(index));
            if (index >= 0 && input.isNameProtected(index) && Chars.equalsIgnoreCase(requestedName, input.getColumnName(index))) {
                project.getOutput().protectName(project.getOutput().getColumnCount() - 1);
            }
        } else {
            project.getOutput().add(planNodes.nextColumnId(), createOutputName(requestedName), expression.getDataType(), nestedSchema, isVisible);
        }
    }

    void addProjection(ProjectPlan project, OutputSchema input, int index, CharSequence requestedName, int position, boolean isSourceAliasReusable) {
        addProjection(project, planNodes.columns.next().of(input.getColumnId(index), input.getColumnType(index), position), input.getMetadata(index), requestedName, true);
        final int outputIndex = project.getExpressions().size() - 1;
        inheritTimestampBinding(input.getColumnId(index), project.getOutput().getColumnId(outputIndex));
        final BindScope scope = scopes.current();
        int aliasIndex = scope.sourceProjectionIndexes.getQuick(index);
        if (aliasIndex < 0 || !isSourceAliasReusable) {
            aliasIndex = outputIndex;
            scope.sourceProjectionIndexes.setQuick(index, aliasIndex);
        }
        scope.projectionAliasIndexes.add(aliasIndex);
    }

    int bindColumnIndex(ExpressionNode expression, OutputSchema metadata, QueryModel source) throws SqlException {
        return bindColumnIndex(expression, metadata, sourceAlias(source));
    }

    int bindColumnIndex(ExpressionNode expression, OutputSchema metadata, CharSequence scopeAlias) throws SqlException {
        if (expression.type != ExpressionNode.LITERAL) {
            throw new IllegalStateException("column reference is not a literal");
        }
        return FunctionBinder.resolveColumn(expression, metadata, scopeAlias);
    }

    void copyTimestampScope(OutputSchema output) {
        final BindScope scope = scopes.current();
        final IntHashSet intrinsicTimestampColumnIds = scope.intrinsicTimestampColumnIds;
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            final int id = output.getColumnId(i);
            if (intrinsicTimestampColumnIds.contains(id)) {
                scope.joinNativeTimestampIds.add(id);
            }
        }
    }

    CharSequence createOutputName(CharSequence requestedName) {
        final BindScope scope = scopes.current();
        return createOutputName(requestedName, scope.aliases, scope.aliasSequences);
    }

    CharSequence createOutputName(CharSequence requestedName, LowerCaseCharSequenceHashSet aliases,
                                  LowerCaseCharSequenceIntHashMap aliasSequences) {
        if (Chars.indexOf(requestedName, '.') >= 0 && !SqlUtil.isQuoteProtectedAlias(requestedName)) {
            final CharacterStoreEntry quoted = characterStore.newEntry();
            quoted.put('"').put(requestedName).put('"');
            requestedName = quoted.toImmutable();
        }
        final CharSequence alias = SqlUtil.createColumnAlias(characterStore, requestedName, -1, aliases, aliasSequences, false);
        aliases.add(alias);
        final CharacterStoreEntry name = characterStore.newEntry();
        if (SqlUtil.isQuoteProtectedAlias(alias)) {
            name.put(alias, 1, alias.length() - 1);
        } else {
            name.put(alias);
        }
        return name.toImmutable();
    }

    boolean hasAggregate(ExpressionNode expression) {
        if (expression == null) {
            return false;
        }
        if (isAggregate(expression)) {
            return true;
        }
        if (expression.paramCount < 3) {
            return hasAggregate(expression.lhs) || hasAggregate(expression.rhs);
        }
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            if (hasAggregate(expression.args.getQuick(i))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether the block aggregates: it groups, or a selected or ORDER BY expression holds an aggregate.
     */
    boolean hasAggregation(QueryModel model, QueryModel source) {
        if (blockGroupBy(model, source).size() > 0) {
            return true;
        }
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            if (hasAggregate(model.getBottomUpColumns().getQuick(i).getAst())) {
                return true;
            }
        }
        for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
            if (hasAggregate(source.getOrderBy().getQuick(i))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether an operator above the block's output binds the block's ORDER BY and LIMIT: the set operation of a
     * set-operation branch, or the SUBSAMPLE of the block.
     */
    boolean hasDeferredOrder(QueryModel source) {
        return isSetOperationBranch || source.getSubsample() != null;
    }

    /**
     * Projects every column of the input unchanged, under its own id and name, with the input's designated
     * timestamp.
     */
    ProjectPlan identityProjection(LogicalPlan input, int position) {
        final ProjectPlan project = planNodes.projects.next().of(input, position);
        final OutputSchema output = input.getOutput();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            project.getExpressions().add(planNodes.columns.next().of(output.getColumnId(i), output.getColumnType(i), position));
        }
        project.getOutput().copyFrom(output);
        return project;
    }

    void inheritTimestampBinding(int inputId, int outputId) {
        final BindScope scope = scopes.current();
        if (scope.intrinsicTimestampColumnIds.contains(inputId)) {
            scope.intrinsicTimestampColumnIds.add(outputId);
        }
        if (scope.ambiguousTimestampColumnIds.contains(inputId)) {
            scope.ambiguousTimestampColumnIds.add(outputId);
        }
    }

    boolean isAggregate(ExpressionNode expression) {
        return expression != null && expression.type == ExpressionNode.FUNCTION
                && expression.windowExpression == null && functionFactoryCache.isGroupBy(expression.token);
    }

    boolean isCursorCall(ExpressionNode node) {
        return node != null && node.type == ExpressionNode.FUNCTION && functionFactoryCache.isCursor(node.token);
    }

    void promoteNoArgFunctions(QueryModel model, OutputSchema scope, CharSequence alias) {
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            final ExpressionNode expression = model.getBottomUpColumns().getQuick(i).getAst();
            if (expression.type == ExpressionNode.LITERAL && !expression.isWildcard()
                    && FunctionBinder.findColumn(expression, scope, alias) == -1
                    && functionFactoryCache.isValidNoArgFunction(expression)) {
                expression.type = ExpressionNode.FUNCTION;
            }
        }
    }

    CharSequence qualifiedJoinName(CharSequence alias, CharSequence name) {
        if (alias == null) {
            return name;
        }
        final CharacterStoreEntry entry = characterStore.newEntry();
        entry.put(alias).put('.').put(name);
        return entry.toImmutable();
    }

    /**
     * Carries the designated timestamp of the source under column projections, filters and limits that leave it out,
     * as a hidden column, so that a temporal join or SAMPLE BY over them keeps its time.
     */
    void retainImplicitTimestamp(LogicalPlan plan) {
        if (plan.getOutput().getTimestampIndex() >= 0) {
            return;
        }
        if (plan instanceof ProjectPlan project) {
            for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
                if (!(project.getExpressions().getQuick(i) instanceof ColumnExpression)) {
                    return;
                }
            }
            retainImplicitTimestamp(project.getInput());
            final OutputSchema input = project.getInput().getOutput();
            final int timestampIndex = input.getTimestampIndex();
            if (timestampIndex >= 0) {
                final int columnId = planNodes.nextColumnId();
                project.getExpressions().add(planNodes.columns.next().of(input.getColumnId(timestampIndex),
                        input.getColumnType(timestampIndex), project.getPosition()));
                // An implicit timestamp belongs to the record layout, never the
                // enclosing query's SQL name scope or wildcard expansion.
                project.getOutput().add(columnId, "", input.getColumnType(timestampIndex), input.getMetadata(timestampIndex), false);
                project.getOutput().setTimestampIndex(project.getOutput().getColumnCount() - 1);
                inheritTimestampBinding(input.getColumnId(timestampIndex), columnId);
            }
        } else if (plan instanceof FilterPlan || plan instanceof LimitPlan) {
            retainImplicitTimestamp(plan.inputAt(0));
            ((ForwardingPlan) plan).deriveOutput();
        }
        // Other operators establish their own ordering or equality contract.
        // In particular, never enlarge a DISTINCT or set-operation tuple.
    }

    BindScope scope() {
        return scopes.current();
    }

    void stopTimestampIntrinsics(OutputSchema output) {
        final BindScope scope = scopes.current();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            scope.intrinsicTimestampColumnIds.remove(output.getColumnId(i));
            scope.ambiguousTimestampColumnIds.remove(output.getColumnId(i));
        }
    }

    void validateWildcard(ExpressionNode expression, OutputSchema input, QueryModel source) throws SqlException {
        final int dot = Chars.indexOfLastUnquoted(expression.token, '.');
        if (dot < 0) {
            return;
        }
        final CharSequence qualifier = GenericLexer.unquote(expression.token.subSequence(0, dot));
        if (input.hasColumnQualifier(qualifier)) {
            return;
        }
        // A valid source can expose no columns, so the output schema alone cannot prove absence.
        for (int i = 0, n = source.getJoinModels().size(); i < n; i++) {
            if (Chars.equalsIgnoreCaseNc(qualifier, sourceAlias(source.getJoinModels().getQuick(i)))) {
                return;
            }
        }
        throw SqlException.$(expression.position, "invalid table alias");
    }

    int wildcardExpansionCount(ExpressionNode expression, OutputSchema input, QueryModel source) {
        int count = 0;
        final CharSequence inputAlias = sourceAlias(source);
        for (int i = 0, n = input.getColumnCount(); i < n; i++) {
            if (isWildcardColumn(expression, input, i, inputAlias)) {
                count++;
            }
        }
        return count;
    }

    OutputSchema windowBindingScope(OutputSchema input) {
        final OutputSchema windowBindingSchema = scopes.current().windowBindingSchema;
        windowBindingSchema.clear();
        for (int i = 0, n = input.getColumnCount(); i < n; i++) {
            windowBindingSchema.addColumnAs(input, i, input.getColumnId(i), input.getColumnName(i), true, input.getColumnQualifier(i));
        }
        windowBindingSchema.setTimestampIndex(input.getTimestampIndex());
        return windowBindingSchema;
    }

    /**
     * Window calls and specs read the input column a computed column of the same name would shadow.
     */
    OutputSchema windowCallBindingScope(OutputSchema input) {
        final OutputSchema windowBindingSchema = scopes.current().windowBindingSchema;
        windowBindingSchema.clear();
        for (int i = 0, n = input.getColumnCount(); i < n; i++) {
            windowBindingSchema.addColumnAs(input, i, input.getColumnId(i), input.getColumnName(i),
                    input.isVisible(i) || !hasVisibleColumn(input, input.getColumnName(i)), input.getColumnQualifier(i));
        }
        windowBindingSchema.setTimestampIndex(input.getTimestampIndex());
        return windowBindingSchema;
    }
}
