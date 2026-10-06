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
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BindVariableExpression;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.DeferredErrorExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.GroupingPlan;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.HorizonJoinSlave;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.TypeExpression;
import io.questdb.griffin.plan.logical.UnnestSpec;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowJoinStep;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.std.Chars;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Per-binder binding state shared by {@link SqlBinder}, its area binders and the expression stage: the plan-node
 * pools, the column-id counter, the scope facts of the query block being bound and the temporary lists they share.
 */
final class BindContext implements Mutable {
    final ObjectPool<AggregatePlan> aggregates = new ObjectPool<>(AggregatePlan.FACTORY, 4);
    final LowerCaseCharSequenceIntHashMap aliasSequences = new LowerCaseCharSequenceIntHashMap();
    final LowerCaseCharSequenceHashSet aliases = new LowerCaseCharSequenceHashSet();
    final IntHashSet ambiguousTimestampColumnIds = new IntHashSet();
    final ObjList<BoundExpression> tmpArguments = new ObjList<>(2);
    final ObjectPool<ExpressionNode> bindingExpressions;
    final CharacterStore characterStore;
    final ObjectPool<ColumnExpression> columns = new ObjectPool<>(ColumnExpression.FACTORY, 16);
    final ObjectPool<ConstantExpression> constants = new ObjectPool<>(ConstantExpression.FACTORY, 4);
    final ObjList<ColumnExpression> cursorColumns = new ObjList<>();
    final ObjList<ExpressionNode> cursorNodes = new ObjList<>();
    /**
     * Projection columns, by column id, whose expression failed to bind; see {@link DeferredErrorExpression}.
     */
    final IntObjHashMap<DeferredErrorExpression> deferredColumns = new IntObjHashMap<>();
    final ObjectPool<DeferredErrorExpression> deferredErrors = new ObjectPool<>(DeferredErrorExpression.FACTORY, 2);
    final ObjectPool<DistinctPlan> distincts = new ObjectPool<>(DistinctPlan.FACTORY, 4);
    final BoundExpressionRewriter expressionRewriter;
    final ObjectPool<FillPlan> fills = new ObjectPool<>(FillPlan.FACTORY, 4);
    final ObjectPool<FilterPlan> filters = new ObjectPool<>(FilterPlan.FACTORY, 4);
    final FunctionBinder functionBinder;
    final FunctionFactoryCache functionFactoryCache;
    final FunctionInstantiator functionInstantiator;
    final TableFunctionSources functionSources;
    final ObjectPool<FunctionExpression> functions = new ObjectPool<>(FunctionExpression.FACTORY, 16);
    final ObjectPool<HorizonJoinPlan> horizonJoinPlans = new ObjectPool<>(HorizonJoinPlan.FACTORY, 2);
    final ObjectPool<HorizonJoinSlave> horizonJoinSlaves = new ObjectPool<>(HorizonJoinSlave.FACTORY, 2);
    final IntHashSet intrinsicTimestampColumnIds = new IntHashSet();
    final ObjectPool<JoinInput> joinInputs = new ObjectPool<>(JoinInput.FACTORY, 4);
    final IntHashSet joinNativeTimestampIds = new IntHashSet();
    final ObjectPool<JoinPlan> joins = new ObjectPool<>(JoinPlan.FACTORY, 2);
    final ObjectPool<LatestByPlan> latestByPlans = new ObjectPool<>(LatestByPlan.FACTORY, 4);
    final ObjectPool<LimitPlan> limits = new ObjectPool<>(LimitPlan.FACTORY, 4);
    final IntList tmpOuterColumns;
    final ObjectPool<OuterColumnExpression> outerColumns = new ObjectPool<>(OuterColumnExpression.FACTORY, 4);
    final ObjectPool<BindVariableExpression> parameters = new ObjectPool<>(BindVariableExpression.FACTORY, 8);
    final IntList tmpPositions = new IntList(2);
    final PreparedFunctions preparedFunctions = new PreparedFunctions();
    final IntList projectionAliasIndexes = new IntList();
    final ObjectPool<ProjectPlan> projects = new ObjectPool<>(ProjectPlan.FACTORY, 4);
    final ObjectPool<SampleByPlan> sampleByPlans = new ObjectPool<>(SampleByPlan.FACTORY, 4);
    final ObjectPool<ScanPlan> scans = new ObjectPool<>(ScanPlan.FACTORY, 4);
    final OutputSchema tmpScope = new OutputSchema();
    final ObjectPool<SetOperationPlan> setOperations = new ObjectPool<>(SetOperationPlan.FACTORY, 4);
    final ObjectPool<SortPlan> sorts = new ObjectPool<>(SortPlan.FACTORY, 4);
    final IntList sourceProjectionIndexes = new IntList();
    final ObjList<ColumnExpression> substitutionColumns = new ObjList<>();
    final ObjList<ExpressionNode> substitutionNodes = new ObjList<>();
    final IntHashSet translatingCopyIds = new IntHashSet();
    final ObjectPool<TypeExpression> types = new ObjectPool<>(TypeExpression.FACTORY, 8);
    final WindowExpression unboundedWindow = WindowExpression.FACTORY.newInstance();
    final ObjectPool<UnnestSpec> unnestSpecs = new ObjectPool<>(UnnestSpec.FACTORY, 4);
    final ObjectPool<WindowJoinPlan> windowJoinPlans = new ObjectPool<>(WindowJoinPlan.FACTORY, 2);
    final ObjectPool<WindowJoinStep> windowJoinSteps = new ObjectPool<>(WindowJoinStep.FACTORY, 2);
    final ObjectPool<WindowPlan> windowPlans = new ObjectPool<>(WindowPlan.FACTORY, 4);
    final ObjectPool<WindowSpec> windowSpecs = new ObjectPool<>(WindowSpec.FACTORY, 4);
    final ObjectPool<WindowExpression> windowSyntax = new ObjectPool<>(WindowExpression.FACTORY, 4);
    private final OutputSchema windowBindingSchema = new OutputSchema();
    LowerCaseCharSequenceObjHashMap<CharSequence> currentHints;
    boolean isInsideJoin;
    /**
     * The query block being bound is a set-operation branch: the set operation binds the trailing
     * ORDER BY and LIMIT the parser places on its last branch, so no branch binds them itself.
     */
    boolean isSetOperationBranch;
    /**
     * A sub-query failed to bind or generate since the last conjunct deferral read it; its error is never
     * deferred.
     */
    boolean isSubqueryFailed;
    int nextColumnId;

    BindContext(FunctionParser functionParser, ObjectPool<ExpressionNode> bindingExpressions, CharacterStore characterStore, SubqueryCompiler subqueries) {
        this.bindingExpressions = bindingExpressions;
        this.characterStore = characterStore;
        this.tmpOuterColumns = projectionAliasIndexes;
        this.functionFactoryCache = functionParser.getFunctionFactoryCache();
        this.expressionRewriter = new BoundExpressionRewriter(functionFactoryCache, columns, constants, functions, outerColumns,
                parameters, types, preparedFunctions, tmpArguments, tmpPositions);
        this.functionInstantiator = new FunctionInstantiator(functionParser, preparedFunctions, tmpScope, subqueries);
        this.functionBinder = new FunctionBinder(this, functionParser, subqueries);
        this.functionSources = new TableFunctionSources(functionParser);
    }

    @Override
    public void clear() {
        try {
            clearExpressions();
        } finally {
            functionSources.clear();
        }
        isInsideJoin = false;
        isSubqueryFailed = false;
        windowBindingSchema.clear();
        windowJoinPlans.clear();
        windowJoinSteps.clear();
        windowPlans.clear();
        windowSpecs.clear();
        windowSyntax.clear();
        aggregates.clear();
        aliases.clear();
        ambiguousTimestampColumnIds.clear();
        aliasSequences.clear();
        distincts.clear();
        filters.clear();
        fills.clear();
        currentHints = null;
        joinInputs.clear();
        joinNativeTimestampIds.clear();
        joins.clear();
        horizonJoinPlans.clear();
        horizonJoinSlaves.clear();
        intrinsicTimestampColumnIds.clear();
        latestByPlans.clear();
        limits.clear();
        projectionAliasIndexes.clear();
        projects.clear();
        sampleByPlans.clear();
        scans.clear();
        tmpScope.clear();
        setOperations.clear();
        sourceProjectionIndexes.clear();
        substitutionColumns.clear();
        substitutionNodes.clear();
        translatingCopyIds.clear();
        unnestSpecs.clear();
        sorts.clear();
        cursorColumns.clear();
        cursorNodes.clear();
        nextColumnId = 0;
        isSetOperationBranch = false;
    }

    /**
     * The id of the TIMESTAMP column a comparison compares with a constant or a call, which designated-timestamp
     * interval extraction evaluates, or -1.
     */
    private static int comparedTimestampId(ExpressionNode column, ExpressionNode value, OutputSchema scope, CharSequence alias) {
        if (column.type != ExpressionNode.LITERAL || value.type != ExpressionNode.CONSTANT && value.type != ExpressionNode.FUNCTION
                && value.type != ExpressionNode.OPERATION && value.type != ExpressionNode.BIND_VARIABLE) {
            return -1;
        }
        final int index = FunctionBinder.findColumn(column, scope, alias);
        return index >= 0 && ColumnType.isTimestamp(scope.getColumnType(index)) ? scope.getColumnId(index) : -1;
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

    static int getColumnIndexQuiet(OutputSchema schema, CharSequence name) {
        final int index = schema.getColumnIndexQuiet(name);
        // Preserve SqlUtil's compiler-alias lookup policy for a flat logical schema.
        return index >= 0 || !SqlUtil.isQuoteProtectedAlias(name)
                ? index : schema.getColumnIndexQuiet(name, 1, name.length() - 1);
    }

    static boolean hasColumnReference(ExpressionNode expression) {
        if (expression == null) {
            return false;
        }
        if (expression.type == ExpressionNode.LITERAL) {
            return true;
        }
        if (expression.paramCount < 3) {
            return hasColumnReference(expression.lhs) || hasColumnReference(expression.rhs);
        }
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            if (hasColumnReference(expression.args.getQuick(i))) {
                return true;
            }
        }
        return false;
    }

    static boolean hasComputedProjection(ProjectPlan project) {
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            if (!(project.getExpressions().getQuick(i) instanceof ColumnExpression)) {
                return true;
            }
        }
        return false;
    }

    static boolean hasLiteral(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.LITERAL) {
            return true;
        }
        if (hasLiteral(node.lhs) || hasLiteral(node.rhs)) {
            return true;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (hasLiteral(node.args.getQuick(i))) {
                return true;
            }
        }
        return false;
    }

    static boolean hasSubquery(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.QUERY) {
            return true;
        }
        if (node.paramCount < 3) {
            return hasSubquery(node.lhs) || hasSubquery(node.rhs);
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (hasSubquery(node.args.getQuick(i))) {
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

    static boolean isRowCount(ExpressionNode expression) {
        return expression.type == ExpressionNode.FUNCTION && expression.windowExpression == null
                && SqlKeywords.isCountKeyword(expression.token)
                && (expression.paramCount == 0 || expression.paramCount == 1
                && expression.rhs.type == ExpressionNode.CONSTANT && !SqlKeywords.isNullKeyword(expression.rhs.token));
    }

    static boolean isWildcard(ExpressionNode expression) {
        return expression.isWildcard();
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

    static int joinColumnSource(JoinPlan join, int columnId) {
        for (int i = 0, n = join.getInputs().size(); i < n; i++) {
            if (join.getInputs().getQuick(i).getSourceOutput().getColumnIndexById(columnId) >= 0) {
                return i;
            }
        }
        throw new IllegalStateException("join column is outside its inputs");
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
        project.getExpressions().add(expression);
        if (expression instanceof ColumnExpression column) {
            final OutputSchema input = project.getInput().getOutput();
            final int index = input.getColumnIndexById(column.getColumnId());
            final DeferredErrorExpression deferred = deferredColumns.get(column.getColumnId());
            if (deferred != null) {
                deferredColumns.put(nextColumnId, deferred);
            }
            project.getOutput().add(nextColumnId++, createOutputName(requestedName), expression.getDataType(),
                    nestedSchema == null && index >= 0 ? input.getMetadata(index) : nestedSchema, isVisible);
            project.getOutput().setSymbolTableStatic(project.getOutput().getColumnCount() - 1, index >= 0 && input.isSymbolTableStatic(index));
            if (index >= 0 && input.isNameProtected(index) && Chars.equalsIgnoreCase(requestedName, input.getColumnName(index))) {
                project.getOutput().protectName(project.getOutput().getColumnCount() - 1);
            }
        } else {
            if (expression instanceof DeferredErrorExpression deferred) {
                deferredColumns.put(nextColumnId, deferred);
            }
            project.getOutput().add(nextColumnId++, createOutputName(requestedName), expression.getDataType(), nestedSchema, isVisible);
        }
    }

    void addProjection(ProjectPlan project, OutputSchema input, int index, CharSequence requestedName, int position, boolean isSourceAliasReusable) {
        addProjection(project, columns.next().of(input.getColumnId(index), input.getColumnType(index), position), input.getMetadata(index), requestedName, true);
        final int outputIndex = project.getExpressions().size() - 1;
        inheritTimestampBinding(input.getColumnId(index), project.getOutput().getColumnId(outputIndex));
        int aliasIndex = sourceProjectionIndexes.getQuick(index);
        if (aliasIndex < 0 || !isSourceAliasReusable) {
            aliasIndex = outputIndex;
            sourceProjectionIndexes.setQuick(index, aliasIndex);
        }
        // OrderBinder.orderedTimestampIndex() resolves a source ORDER BY through these aliases.
        projectionAliasIndexes.add(aliasIndex);
    }

    int bindColumnIndex(ExpressionNode expression, OutputSchema metadata, QueryModel source) throws SqlException {
        return bindColumnIndex(expression, metadata, sourceAlias(source));
    }

    int bindColumnIndex(ExpressionNode expression, OutputSchema metadata, CharSequence scopeAlias) throws SqlException {
        final int index = bindForwardedColumnIndex(expression, metadata, scopeAlias);
        raiseDeferredColumn(metadata.getColumnId(index));
        return index;
    }

    /**
     * Binds a projection column. When the expression fails to bind with an error that code generation raises,
     * the column becomes a {@link DeferredErrorExpression}; an unresolved column reference of the expression
     * still fails at once, and so does an expression over a sub-query.
     */
    BoundExpression bindDeferrable(
            ExpressionNode expression,
            OutputSchema scope,
            CharSequence alias,
            int preferredType,
            SqlExecutionContext executionContext
    ) throws SqlException {
        try {
            return functionBinder.bind(expression, scope, alias, preferredType, executionContext);
        } catch (SqlException e) {
            return deferError(e, expression, scope, alias, null);
        }
    }

    /**
     * {@link #bindDeferrable} over caller-selected subtree replacements, which are borrowed only for this call.
     */
    BoundExpression bindDeferrable(
            ExpressionNode expression,
            OutputSchema scope,
            ObjList<ExpressionNode> replacementNodes,
            ObjList<ColumnExpression> replacementColumns,
            SqlExecutionContext executionContext
    ) throws SqlException {
        try {
            return functionBinder.bind(expression, scope, null, replacementNodes, replacementColumns, executionContext);
        } catch (SqlException e) {
            return deferError(e, expression, scope, null, replacementNodes);
        }
    }

    /**
     * Resolves a column that a projection forwards unchanged, which carries a deferred error along instead of
     * raising it.
     */
    int bindForwardedColumnIndex(ExpressionNode expression, OutputSchema metadata, CharSequence scopeAlias) throws SqlException {
        if (expression.type != ExpressionNode.LITERAL) {
            throw new IllegalStateException("column reference is not a literal");
        }
        return FunctionBinder.resolveColumn(expression, metadata, scopeAlias);
    }

    /**
     * Closes every prepared root nothing adopted and empties the description pools and temporary lists of the three
     * expression stages, which allocate from this context.
     */
    void clearExpressions() {
        try {
            preparedFunctions.clear();
        } finally {
            functionInstantiator.clear();
            functionBinder.clear();
            expressionRewriter.clear();
            columns.clear();
            constants.clear();
            deferredColumns.clear();
            deferredErrors.clear();
            functions.clear();
            outerColumns.clear();
            parameters.clear();
            types.clear();
        }
    }

    void copyTimestampScope(OutputSchema output) {
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            final int id = output.getColumnId(i);
            if (intrinsicTimestampColumnIds.contains(id)) {
                joinNativeTimestampIds.add(id);
            }
        }
    }

    CharSequence createOutputName(CharSequence requestedName) {
        return createOutputName(requestedName, aliases, aliasSequences);
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

    /**
     * The deferred form of a WHERE or ON conjunct that failed to bind with {@code e}, which generation raises
     * when it builds the filter. A column reference that does not resolve, or a sub-query that fails to bind
     * or generate, fails at once instead.
     */
    DeferredErrorExpression deferConjunct(
            SqlException e,
            ExpressionNode expression,
            OutputSchema scope,
            CharSequence alias,
            int joinInput
    ) throws SqlException {
        final boolean hasSubqueryFailure = isSubqueryFailed;
        isSubqueryFailed = false;
        if (hasSubqueryFailure && hasSubquery(expression)) {
            throw e;
        }
        validateColumnReferences(expression, scope, alias, null);
        int comparedColumnId = -1;
        if (expression.type == ExpressionNode.OPERATION && expression.paramCount == 2
                && FunctionBinder.isTemporalComparisonOperator(expression.token)) {
            comparedColumnId = comparedTimestampId(expression.lhs, expression.rhs, scope, alias);
            if (comparedColumnId < 0) {
                comparedColumnId = comparedTimestampId(expression.rhs, expression.lhs, scope, alias);
            }
        }
        return deferredErrors.next().ofConjunct(e, comparedColumnId, joinInput);
    }

    /**
     * The conjunct for an expression that bound to a value of a type other than BOOLEAN. A column that the
     * filter of its own table names with a qualifier reads unqualified there, so its error has no source
     * position (0).
     */
    DeferredErrorExpression nonBooleanConjunct(ExpressionNode expression, int type, int position, boolean isTableFilter, int joinInput) {
        final boolean isUnqualified = isTableFilter && expression.type == ExpressionNode.LITERAL
                && Chars.indexOfLastUnquoted(expression.token, '.') > 0;
        return deferredErrors.next().ofNonBoolean(type, isUnqualified ? 0 : position, joinInput);
    }

    /**
     * The deferred form of an expression that failed to bind with {@code e}. An expression over a sub-query,
     * or with a column reference that does not resolve, fails at once instead.
     */
    DeferredErrorExpression deferError(
            SqlException e,
            ExpressionNode expression,
            OutputSchema scope,
            CharSequence alias,
            ObjList<ExpressionNode> replacementNodes
    ) throws SqlException {
        if (hasSubquery(expression)) {
            throw e;
        }
        validateColumnReferences(expression, scope, alias, replacementNodes);
        return deferredErrors.next().of(e);
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

    void inheritTimestampBinding(int inputId, int outputId) {
        // Only the SQL literal-projection path calls this. In particular, an
        // identity CAST may bind to ColumnExpression but stays a pushdown
        // barrier; it must retain ordinary adaptive comparison semantics.
        if (intrinsicTimestampColumnIds.contains(inputId)) {
            intrinsicTimestampColumnIds.add(outputId);
        }
        if (ambiguousTimestampColumnIds.contains(inputId)) {
            ambiguousTimestampColumnIds.add(outputId);
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
            if (expression.type == ExpressionNode.LITERAL && !isWildcard(expression)
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
     * Anything that reads a column's value or type while binding raises the error the column deferred.
     */
    void raiseDeferredColumn(int columnId) throws SqlException {
        final DeferredErrorExpression deferred = deferredColumns.get(columnId);
        if (deferred != null) {
            throw deferred.raise();
        }
    }

    void stopTimestampIntrinsics(OutputSchema output) {
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            intrinsicTimestampColumnIds.remove(output.getColumnId(i));
            ambiguousTimestampColumnIds.remove(output.getColumnId(i));
        }
    }

    void validateColumnReferences(
            ExpressionNode node,
            OutputSchema scope,
            CharSequence alias,
            ObjList<ExpressionNode> replacementNodes
    ) throws SqlException {
        if (node == null || replacementNodes != null && replacementNodes.indexOf(node) >= 0) {
            return;
        }
        if (node.type == ExpressionNode.LITERAL) {
            if (FunctionBinder.findColumn(node, scope, alias) < 0 && !functionBinder.isOuterColumn(node, scope, alias)
                    && !functionFactoryCache.isValidNoArgFunction(node)) {
                FunctionBinder.resolveColumn(node, scope, alias);
            }
            return;
        }
        if (node.paramCount < 3) {
            validateColumnReferences(node.rhs, scope, alias, replacementNodes);
            validateColumnReferences(node.lhs, scope, alias, replacementNodes);
            return;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            validateColumnReferences(node.args.getQuick(i), scope, alias, replacementNodes);
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
        // Consult the immediate source aliases as well: a valid source can
        // expose no columns, so the output schema alone cannot prove absence.
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
        return windowBindingScope(input, false);
    }

    /**
     * Window calls and specs read the input column a computed column of the same name would shadow.
     */
    OutputSchema windowBindingScope(OutputSchema input, boolean isShadowingExcluded) {
        windowBindingSchema.clear();
        for (int i = 0, n = input.getColumnCount(); i < n; i++) {
            final boolean isReferenceable = !isShadowingExcluded || input.isVisible(i) || !hasVisibleColumn(input, input.getColumnName(i));
            windowBindingSchema.add(input.getColumnId(i), input.getColumnName(i), input.getColumnType(i),
                    input.getMetadata(i), isReferenceable, input.getColumnQualifier(i));
            windowBindingSchema.setSymbolTableStatic(i, input.isSymbolTableStatic(i));
        }
        windowBindingSchema.setTimestampIndex(input.getTimestampIndex());
        return windowBindingSchema;
    }
}
