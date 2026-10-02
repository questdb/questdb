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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.HorizonJoinSlave;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
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

final class BindContext implements Mutable {
    final ObjList<ExpressionNode> aggregateNodes = new ObjList<>();
    final ObjList<ExpressionNode> aggregateSelectExpressions = new ObjList<>();
    final ObjectPool<AggregatePlan> aggregates = new ObjectPool<>(AggregatePlan.FACTORY, 4);
    final LowerCaseCharSequenceIntHashMap aliasSequences = new LowerCaseCharSequenceIntHashMap();
    final LowerCaseCharSequenceHashSet aliases = new LowerCaseCharSequenceHashSet();
    final IntHashSet ambiguousTimestampColumnIds = new IntHashSet();
    final ObjectPool<ExpressionNode> bindingExpressions;
    final CharacterStore characterStore = new CharacterStore(1024, 16);
    final IntObjHashMap<ExpressionNode> columnSpellings = new IntObjHashMap<>();
    final ObjectPool<ColumnExpression> columns = new ObjectPool<>(ColumnExpression.FACTORY, 16);
    final CairoConfiguration configuration;
    final ObjectPool<ConstantExpression> constants = new ObjectPool<>(ConstantExpression.FACTORY, 4);
    final ObjList<ColumnExpression> cursorColumns = new ObjList<>();
    final ObjList<CharSequence> cursorNames = new ObjList<>();
    final ObjList<ExpressionNode> cursorNodes = new ObjList<>();
    final IntList cursorProjectionSources = new IntList();
    final IntList cursorSourceIndexes = new IntList();
    final ObjList<ExpressionNode> cursorSources = new ObjList<>();
    final ObjectPool<DistinctPlan> distincts = new ObjectPool<>(DistinctPlan.FACTORY, 4);
    final OutputSchema emptySchema;
    final ObjectPool<FillPlan> fills = new ObjectPool<>(FillPlan.FACTORY, 4);
    final ObjectPool<FilterPlan> filters = new ObjectPool<>(FilterPlan.FACTORY, 4);
    final FunctionBinder functionBinder;
    final FunctionFactoryCache functionFactoryCache;
    final FunctionParser functionParser;
    final TableFunctionSources functionSources;
    final ObjList<ExpressionNode> groupingNodes = new ObjList<>();
    final ObjectPool<HorizonJoinPlan> horizonJoinPlans = new ObjectPool<>(HorizonJoinPlan.FACTORY, 2);
    final ObjectPool<HorizonJoinSlave> horizonJoinSlaves = new ObjectPool<>(HorizonJoinSlave.FACTORY, 2);
    final IntHashSet intrinsicTimestampColumnIds = new IntHashSet();
    final ObjectPool<JoinInput> joinInputs = new ObjectPool<>(JoinInput.FACTORY, 4);
    final IntHashSet joinNativeTimestampIds = new IntHashSet();
    final ObjectPool<JoinPlan> joins = new ObjectPool<>(JoinPlan.FACTORY, 2);
    final ObjectPool<LatestByPlan> latestByPlans = new ObjectPool<>(LatestByPlan.FACTORY, 4);
    final ObjectPool<LimitPlan> limits = new ObjectPool<>(LimitPlan.FACTORY, 4);
    final ExpressionNode normalizedCount = ExpressionNode.FACTORY.newInstance();
    final IntList outerColumnScratch;
    final IntList projectionAliasIndexes = new IntList();
    final ObjectPool<ProjectPlan> projects = new ObjectPool<>(ProjectPlan.FACTORY, 4);
    final ObjectPool<SampleByPlan> sampleByPlans = new ObjectPool<>(SampleByPlan.FACTORY, 4);
    final ObjectPool<ScanPlan> scans = new ObjectPool<>(ScanPlan.FACTORY, 4);
    final OutputSchema scratchScope = new OutputSchema();
    final ObjectPool<SetOperationPlan> setOperations = new ObjectPool<>(SetOperationPlan.FACTORY, 4);
    final ObjectPool<SortPlan> sorts = new ObjectPool<>(SortPlan.FACTORY, 4);
    final IntList sourceProjectionIndexes = new IntList();
    final ObjList<ColumnExpression> substitutionColumns = new ObjList<>();
    final ObjList<ExpressionNode> substitutionNodes = new ObjList<>();
    final IntHashSet translatingCopyIds = new IntHashSet();
    final WindowExpression unboundedWindow = WindowExpression.FACTORY.newInstance();
    final ObjectPool<UnnestSpec> unnestSpecs = new ObjectPool<>(UnnestSpec.FACTORY, 4);
    final IntList wildcardExcludedIds = new IntList();
    final IntList windowAliasIds = new IntList();
    final OutputSchema windowBindingSchema = new OutputSchema();
    final ObjectPool<WindowJoinPlan> windowJoinPlans = new ObjectPool<>(WindowJoinPlan.FACTORY, 2);
    final ObjectPool<WindowJoinStep> windowJoinSteps = new ObjectPool<>(WindowJoinStep.FACTORY, 2);
    final ObjList<ExpressionNode> windowOrderExpressions = new ObjList<>();
    final ObjectPool<WindowPlan> windowPlans = new ObjectPool<>(WindowPlan.FACTORY, 4);
    final ObjList<ExpressionNode> windowSelectExpressions = new ObjList<>();
    final ObjectPool<WindowSpec> windowSpecs = new ObjectPool<>(WindowSpec.FACTORY, 4);
    final ObjectPool<WindowExpression> windowSyntax = new ObjectPool<>(WindowExpression.FACTORY, 4);
    LowerCaseCharSequenceObjHashMap<CharSequence> currentHints;
    SqlExecutionContext executionContext;
    boolean isInsideJoin;
    boolean isSetOperationBranch;
    int nextColumnId;

    BindContext(
            CairoConfiguration configuration,
            FunctionParser functionParser,
            ObjectPool<ExpressionNode> bindingExpressions,
            OutputSchema emptySchema
    ) {
        this.configuration = configuration;
        this.bindingExpressions = bindingExpressions;
        this.emptySchema = emptySchema;
        this.outerColumnScratch = projectionAliasIndexes;
        this.functionBinder = new FunctionBinder(functionParser, columns, constants, scratchScope);
        this.functionParser = functionParser;
        this.functionFactoryCache = functionParser.getFunctionFactoryCache();
        this.functionSources = new TableFunctionSources(functionParser);
    }

    @Override
    public void clear() {
        try {
            functionBinder.clear();
        } finally {
            functionSources.clear();
        }
        executionContext = null;
        isInsideJoin = false;
        windowAliasIds.clear();
        windowBindingSchema.clear();
        windowJoinPlans.clear();
        windowJoinSteps.clear();
        windowOrderExpressions.clear();
        windowPlans.clear();
        windowSelectExpressions.clear();
        windowSpecs.clear();
        windowSyntax.clear();
        aggregates.clear();
        aggregateNodes.clear();
        aggregateSelectExpressions.clear();
        aliases.clear();
        ambiguousTimestampColumnIds.clear();
        aliasSequences.clear();
        characterStore.clear();
        columnSpellings.clear();
        clearCursorColumns();
        distincts.clear();
        filters.clear();
        fills.clear();
        currentHints = null;
        joinInputs.clear();
        joinNativeTimestampIds.clear();
        joins.clear();
        groupingNodes.clear();
        horizonJoinPlans.clear();
        horizonJoinSlaves.clear();
        intrinsicTimestampColumnIds.clear();
        latestByPlans.clear();
        limits.clear();
        normalizedCount.clear();
        projectionAliasIndexes.clear();
        projects.clear();
        sampleByPlans.clear();
        scans.clear();
        scratchScope.clear();
        setOperations.clear();
        sourceProjectionIndexes.clear();
        substitutionColumns.clear();
        substitutionNodes.clear();
        translatingCopyIds.clear();
        unnestSpecs.clear();
        sorts.clear();
        nextColumnId = 0;
        isSetOperationBranch = false;
        wildcardExcludedIds.clear();
    }

    private static boolean hasVisibleColumn(OutputSchema schema, CharSequence name) {
        for (int i = 0, n = schema.getColumnCount(); i < n; i++) {
            if (schema.isVisible(i) && Chars.equalsIgnoreCase(schema.getColumnName(i), name)) {
                return true;
            }
        }
        return false;
    }

    private static boolean matchesWildcardColumn(ExpressionNode expression, OutputSchema input, int index, CharSequence inputAlias) {
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

    static ObjList<ExpressionNode> blockGroupBy(QueryModel model, QueryModel source) {
        return source.getGroupBy().size() > 0 && !source.isPivot() ? source.getGroupBy() : model.getGroupBy();
    }

    static int findGroupingColumn(AggregatePlan aggregate, int columnId) {
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
            project.getOutput().add(nextColumnId++, createOutputName(requestedName), expression.getDataType(),
                    nestedSchema == null && index >= 0 ? input.getMetadata(index) : nestedSchema, isVisible);
            project.getOutput().setSymbolTableStatic(project.getOutput().getColumnCount() - 1, index >= 0 && input.isSymbolTableStatic(index));
            if (index >= 0 && input.isNameProtected(index) && Chars.equalsIgnoreCase(requestedName, input.getColumnName(index))) {
                project.getOutput().protectName(project.getOutput().getColumnCount() - 1);
            }
        } else {
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
        if (expression.type != ExpressionNode.LITERAL) {
            throw new IllegalStateException("column reference is not a literal");
        }
        return FunctionBinder.resolveColumn(expression, metadata, scopeAlias);
    }

    void clearCursorColumns() {
        cursorColumns.clear();
        cursorNames.clear();
        cursorNodes.clear();
        cursorProjectionSources.clear();
        cursorSourceIndexes.clear();
        cursorSources.clear();
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

    boolean isWildcardColumn(ExpressionNode expression, OutputSchema input, int index, CharSequence inputAlias) {
        return matchesWildcardColumn(expression, input, index, inputAlias)
                && (wildcardExcludedIds.size() == 0 || !wildcardExcludedIds.contains(input.getColumnId(index)));
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

    void stopTimestampIntrinsics(OutputSchema output) {
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            intrinsicTimestampColumnIds.remove(output.getColumnId(i));
            ambiguousTimestampColumnIds.remove(output.getColumnId(i));
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
