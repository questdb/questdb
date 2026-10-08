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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkSPI;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.engine.window.LiveViewCheckpointFunctionCompiler;
import io.questdb.griffin.engine.window.LiveViewWindowDescription;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SortDirection;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.std.Chars;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

import static io.questdb.griffin.BindContext.*;

final class WindowBinder implements Mutable {
    private static final int MAX_WINDOW_NESTING_DEPTH = 8;

    /**
     * Binding resolves a window's type and validation only; the generator builds the partition sink.
     */
    private static final RecordSink UNUSED_PARTITION_SINK = new RecordSink() {
        @Override
        public void copy(Record r, RecordSinkSPI w) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setFunctions(ObjList<Function> keyFunctions) {
            throw new UnsupportedOperationException();
        }
    };

    private final VirtualRecord bindPartitionRecord = new VirtualRecord(new ObjList<>());
    private final BindContext ctx;
    private final FunctionParser functionParser;
    private final IntList windowGroupMembers = new IntList();
    private final IntHashSet windowInheritancePositions = new IntHashSet();
    private int windowLevelCount;

    WindowBinder(BindContext ctx, FunctionParser functionParser) {
        this.ctx = ctx;
        this.functionParser = functionParser;
    }

    @Override
    public void clear() {
        windowGroupMembers.clear();
        windowInheritancePositions.clear();
    }

    private static boolean hasSubQuery(ExpressionNode expression) {
        if (expression == null) {
            return false;
        }
        if (expression.type == ExpressionNode.QUERY || hasSubQuery(expression.lhs) || hasSubQuery(expression.rhs)) {
            return true;
        }
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            if (hasSubQuery(expression.args.getQuick(i))) {
                return true;
            }
        }
        return false;
    }

    private static void validateWindowNesting(ExpressionNode expression, int depth) throws SqlException {
        if (expression == null) {
            return;
        }
        if (expression.windowExpression != null) {
            if (depth > MAX_WINDOW_NESTING_DEPTH) {
                throw SqlException.$(expression.position, "too many levels of nested window functions [max=")
                        .put(MAX_WINDOW_NESTING_DEPTH).put(']');
            }
            depth++;
        }
        validateWindowNesting(expression.lhs, depth);
        validateWindowNesting(expression.rhs, depth);
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            validateWindowNesting(expression.args.getQuick(i), depth);
        }
    }

    private void addWindowCopies(ExpressionNode copy, ExpressionNode occurrence) {
        final BindScope scope = ctx.scope();
        if (copy == null || occurrence == null) {
            return;
        }
        if (copy.windowExpression != null) {
            scope.windowCopies.add(copy);
            scope.windowCopyOrigins.add(windowOccurrence(occurrence));
        }
        addWindowCopies(copy.lhs, occurrence.lhs);
        addWindowCopies(copy.rhs, occurrence.rhs);
        for (int i = 0, n = Math.min(copy.args.size(), occurrence.args.size()); i < n; i++) {
            addWindowCopies(copy.args.getQuick(i), occurrence.args.getQuick(i));
        }
    }

    private void addWindowInnerColumn(ProjectPlan project, OutputSchema input, int index) {
        final int columnId = input.getColumnId(index);
        final OutputSchema output = project.getOutput();
        if (output.getColumnIndexById(columnId) >= 0) {
            return;
        }
        ctx.scope().aliases.add(input.getColumnName(index));
        project.getExpressions().add(ctx.planNodes.columns.next().of(columnId, input.getColumnType(index), project.getPosition()));
        output.add(columnId, input.getColumnName(index), input.getColumnType(index),
                input.getMetadata(index), input.isVisible(index), input.getColumnQualifier(index));
        final int outputIndex = output.getColumnCount() - 1;
        output.setSymbolTableStatic(outputIndex, input.isSymbolTableStatic(index));
        if (index == input.getTimestampIndex()) {
            output.setTimestampIndex(outputIndex);
        }
    }

    private void addWindowInnerLiterals(ProjectPlan project, OutputSchema input, ExpressionNode node, QueryModel source, boolean isSpecIncluded) {
        if (node == null) {
            return;
        }
        if (node.type == ExpressionNode.LITERAL) {
            final int index = FunctionBinder.findColumn(node, input, sourceAlias(source));
            if (index >= 0) {
                addWindowInnerColumn(project, input, index);
            } else if (!isSpecIncluded) {
                addWindowSelfReference(project, node);
            }
            return;
        }
        final boolean isWindow = node.windowExpression != null;
        addWindowInnerLiterals(project, input, node.rhs, source, isSpecIncluded && !isWindow);
        addWindowInnerLiterals(project, input, node.lhs, source, isSpecIncluded && !isWindow);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            addWindowInnerLiterals(project, input, node.args.getQuick(i), source, isSpecIncluded && !isWindow);
        }
        if (isSpecIncluded && isWindow) {
            addWindowInnerLiterals(project, input, node.windowExpression.getPartitionBy(), source);
            addWindowInnerLiterals(project, input, node.windowExpression.getOrderBy(), source);
        }
    }

    private void addWindowInnerLiterals(ProjectPlan project, OutputSchema input, ObjList<ExpressionNode> nodes, QueryModel source) {
        for (int i = 0, n = nodes.size(); i < n; i++) {
            addWindowInnerLiterals(project, input, nodes.getQuick(i), source, true);
        }
    }

    private void addWindowSelfReference(ProjectPlan project, ExpressionNode node) {
        final BindScope scope = ctx.scope();
        if (Chars.indexOfLastUnquoted(node.token, '.') >= 0 || scope.windowSelfReferences.keyIndex(node.token) < 0) {
            return;
        }
        final OutputSchema output = project.getOutput();
        final CharSequence name = GenericLexer.unquote(node.token);
        int index = output.getColumnCount() - 1;
        while (index >= 0 && !Chars.equalsIgnoreCase(output.getColumnName(index), name)) {
            index--;
        }
        if (index < 0 || project.getExpressions().getQuick(index) instanceof ColumnExpression) {
            return;
        }
        final int id = scope.nextColumnId++;
        project.getExpressions().add(ctx.planNodes.columns.next().of(output.getColumnId(index), output.getColumnType(index), node.position));
        output.add(id, ctx.createOutputName(name), output.getColumnType(index), false);
        scope.windowSelfReferences.put(node.token, id);
    }

    private void addWindowTranslationColumn(ProjectPlan project, OutputSchema input, int index, CharSequence name) {
        project.getExpressions().add(ctx.planNodes.columns.next().of(input.getColumnId(index), input.getColumnType(index), project.getPosition()));
        project.getOutput().add(input.getColumnId(index), name, input.getColumnType(index), input.getMetadata(index), input.isVisible(index));
        final int outputIndex = project.getOutput().getColumnCount() - 1;
        project.getOutput().setSymbolTableStatic(outputIndex, input.isSymbolTableStatic(index));
        if (index == input.getTimestampIndex()) {
            project.getOutput().setTimestampIndex(outputIndex);
        }
    }

    private int appendWindowInputExpression(BoundExpression expression, CharSequence name, int position) {
        final BindScope scope = ctx.scope();
        final ProjectPlan project = ctx.planNodes.projects.next().of(scope.windowInput, position);
        final OutputSchema output = scope.windowInput.getOutput();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            project.getExpressions().add(ctx.planNodes.columns.next().of(output.getColumnId(i), output.getColumnType(i), position));
        }
        project.getOutput().copyFrom(output);
        final int id = scope.nextColumnId++;
        project.getExpressions().add(expression);
        project.getOutput().add(id, ctx.createOutputName(name), expression.getDataType(), false);
        scope.windowInput = project;
        return id;
    }

    private boolean areWindowsBound(ExpressionNode expression) {
        if (expression == null) {
            return true;
        }
        if (expression.windowExpression != null && ctx.scope().windowColumnIds.getQuick(findWindowNode(expression)) < 0) {
            return false;
        }
        if (!areWindowsBound(expression.lhs) || !areWindowsBound(expression.rhs)) {
            return false;
        }
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            if (!areWindowsBound(expression.args.getQuick(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * The expression bound over the window input when it evaluates a volatile function, otherwise null.
     */
    private BoundExpression bindVolatile(ExpressionNode expression, QueryModel source, SqlExecutionContext executionContext) {
        if (hasSubQuery(expression)) {
            return null;
        }
        try {
            final BoundExpression bound = ctx.functionBinder.bind(expression, ctx.windowBindingScope(ctx.scope().windowInput.getOutput()),
                    sourceAlias(source), ColumnType.STRING, executionContext);
            return LogicalPlans.isVolatile(bound) ? bound : null;
        } catch (SqlException e) {
            return null;
        }
    }

    /**
     * Computes, as a column under the next window level, an aliased expression that holds windows of
     * lower levels and a volatile function, once a window argument reads it; the argument and the
     * aliased column then read that one value.
     */
    private void bindVolatileAliasCopies(QueryModel model, QueryModel source, SqlExecutionContext executionContext) {
        final BindScope scope = ctx.scope();
        for (int i = 0, n = scope.windowAliasCopies.size(); i < n; i++) {
            final ExpressionNode copy = scope.windowAliasCopies.getQuick(i);
            final int column = scope.windowAliasCopyColumns.getQuick(i);
            if (scope.windowAliasIds.getQuick(column) < 0 && areWindowsBound(copy)) {
                final BoundExpression bound = bindVolatile(replaceWindowReferences(copy, true), source, executionContext);
                if (bound != null) {
                    scope.windowAliasIds.setQuick(column, appendWindowInputExpression(bound, model.getBottomUpColumns().getQuick(column).getName(), copy.position));
                }
            }
        }
    }

    /**
     * Evaluates the SELECT columns that hold no window function in a projection under the window plans,
     * together with the columns the window functions read, in the order they appear.
     */
    private ProjectPlan bindWindowInnerProjection(QueryModel model, QueryModel source, LogicalPlan input,
                                                  SqlExecutionContext executionContext) throws SqlException {
        final BindScope scope = ctx.scope();
        final OutputSchema output = input.getOutput();
        final ProjectPlan project = ctx.planNodes.projects.next().of(input, model.getModelPosition());
        final ObjList<QueryColumn> columns = model.getBottomUpColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final ExpressionNode ast = columns.getQuick(i).getAst();
            if (ast.isWildcard()) {
                for (int k = 0, count = output.getColumnCount(); k < count; k++) {
                    if (isWildcardColumn(ast, output, k, sourceAlias(source))) {
                        addWindowInnerColumn(project, output, k);
                    }
                }
            } else if (ast.type == ExpressionNode.LITERAL) {
                addWindowInnerColumn(project, output, ctx.bindColumnIndex(ast, output, source));
            } else if (isPureComputedColumn(ast)) {
                final BoundExpression bound = ctx.functionBinder.bind(ast, ctx.windowBindingScope(output), sourceAlias(source), ColumnType.STRING, executionContext);
                final int id = scope.nextColumnId++;
                project.getExpressions().add(bound);
                project.getOutput().add(id, ctx.createOutputName(columns.getQuick(i).getName()), bound.getDataType(), false);
                scope.windowAliasIds.setQuick(i, id);
            } else {
                addWindowInnerLiterals(project, output, ast, source, ast.windowExpression == null);
            }
        }
        for (int i = 0, n = columns.size(); i < n; i++) {
            final WindowExpression window = columns.getQuick(i).getAst().windowExpression;
            if (window != null) {
                addWindowInnerLiterals(project, output, window.getPartitionBy(), source);
                addWindowInnerLiterals(project, output, window.getOrderBy(), source);
            }
        }
        addWindowInnerLiterals(project, output, source.getOrderBy(), source);
        return project;
    }

    /**
     * Binds the partition and order expressions of a window. An order expression that is not a column keeps its
     * place in the spec unbound, and fails once the window call has bound: a window orders by input columns only.
     */
    private void bindWindowSpec(int windowIndex, WindowSpec spec, QueryModel source, SqlExecutionContext executionContext) throws SqlException {
        final BindScope scope = ctx.scope();
        final ExpressionNode node = scope.windowNodes.getQuick(windowIndex);
        final WindowExpression syntax = node.windowExpression;
        for (int k = 0, count = syntax.getPartitionBy().size(); k < count; k++) {
            final ExpressionNode partition = replaceWindowReferences(syntax.getPartitionBy().getQuick(k), true);
            if (ctx.isAggregate(partition)) {
                throw SqlException.$(node.position, "aggregate functions in partition by are not supported");
            }
            final int aggregatePosition = findAggregatePosition(partition);
            if (aggregatePosition >= 0) {
                throw SqlException.$(aggregatePosition, "Aggregate function cannot be passed as an argument");
            }
            spec.getPartitionBy().add(ctx.functionBinder.bind(partition, ctx.windowCallBindingScope(scope.windowInput.getOutput()), sourceAlias(source), executionContext));
        }
        for (int k = 0, count = syntax.getOrderBy().size(); k < count; k++) {
            final ExpressionNode order = replaceWindowReferences(syntax.getOrderBy().getQuick(k), true);
            if (order.type != ExpressionNode.LITERAL) {
                if (scope.windowOrderViolations.getQuick(windowIndex) == null) {
                    scope.windowOrderViolations.setQuick(windowIndex, order);
                }
                spec.getOrderByColumnIds().add(-1);
                spec.getOrderByDirections().add(SqlBinder.sortDirection(syntax.getOrderByDirection().getQuick(k)));
                spec.getOrderByPositions().add(order.position);
                spec.getOrderByNames().add(order.token);
                continue;
            }
            final BoundExpression bound = ctx.functionBinder.bind(order, ctx.windowCallBindingScope(scope.windowInput.getOutput()), sourceAlias(source), executionContext);
            // Only an outer column of a LATERAL body is not an input column of the window.
            final int id = bound instanceof ColumnExpression column ? column.getColumnId()
                    : appendWindowInputExpression(bound, "__window_order", order.position);
            final int index = scope.windowInput.getOutput().getColumnIndexById(id);
            spec.getOrderByColumnIds().add(id);
            spec.getOrderByDirections().add(SqlBinder.sortDirection(syntax.getOrderByDirection().getQuick(k)));
            spec.getOrderByPositions().add(order.position);
            spec.getOrderByNames().add(scope.windowInput.getOutput().getColumnName(index));
        }
    }

    private void clearWindowAliases() {
        final BindScope scope = ctx.scope();
        scope.windowAliasCopies.clear();
        scope.windowAliasCopyColumns.clear();
        scope.windowAliasReferences.clear();
        scope.windowAliasReferenceColumns.clear();
        scope.windowAliasResolutions.clear();
    }

    private void closeWindowGroup(int mark) {
        boolean hasMember = false;
        for (int i = mark, n = windowGroupMembers.size(); i < n && !hasMember; i++) {
            hasMember = windowGroupMembers.getQuick(i) >= 0;
        }
        if (hasMember) {
            windowLevelCount++;
            for (int i = mark, n = windowGroupMembers.size(); i < n; i++) {
                final int member = windowGroupMembers.getQuick(i);
                if (member >= 0) {
                    ctx.scope().windowLevels.setQuick(member, windowLevelCount);
                }
            }
        }
        windowGroupMembers.setPos(mark);
    }

    private void collectWindowNodes(ExpressionNode expression) throws SqlException {
        validateWindowNesting(expression, 0);
        collectWindowNodes0(expression);
    }

    private void collectWindowNodes0(ExpressionNode expression) {
        if (expression == null) {
            return;
        }
        if (expression.windowExpression != null && findWindowNode(expression) < 0) {
            ctx.scope().windowNodes.add(expression);
        }
        collectWindowNodes0(expression.lhs);
        collectWindowNodes0(expression.rhs);
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            collectWindowNodes0(expression.args.getQuick(i));
        }
    }

    /**
     * Copies a SELECT or ORDER BY expression for binding; each window call in the copy stays the
     * occurrence it was written as, so an alias reference and a repeated SELECT expression in
     * ORDER BY denote the same window value.
     */
    private ExpressionNode copyWindowSyntax(ExpressionNode syntax, ExpressionNode occurrence) {
        final ExpressionNode copy = ExpressionNode.deepClone(ctx.bindingExpressions, syntax);
        addWindowCopies(copy, occurrence);
        return copy;
    }

    private int findAggregatePosition(ExpressionNode expression) {
        if (expression == null) {
            return -1;
        }
        if (ctx.isAggregate(expression)) {
            return expression.position;
        }
        int position = findAggregatePosition(expression.lhs);
        if (position < 0) {
            position = findAggregatePosition(expression.rhs);
        }
        for (int i = 0, n = expression.args.size(); position < 0 && i < n; i++) {
            position = findAggregatePosition(expression.args.getQuick(i));
        }
        return position;
    }

    private int findWindowNode(ExpressionNode expression) {
        final BindScope scope = ctx.scope();
        final ExpressionNode occurrence = windowOccurrence(expression);
        for (int i = 0, n = scope.windowNodes.size(); i < n; i++) {
            if (windowOccurrence(scope.windowNodes.getQuick(i)) == occurrence) {
                return i;
            }
        }
        return -1;
    }

    private boolean hasPureComputedColumn(QueryModel model) {
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            if (isPureComputedColumn(model.getBottomUpColumns().getQuick(i).getAst())) {
                return true;
            }
        }
        return false;
    }

    private boolean isPureComputedColumn(ExpressionNode ast) {
        return ast.type != ExpressionNode.LITERAL && findWindowPosition(ast, true, false) < 0 && !ctx.hasAggregate(ast) && !ctx.isCursorCall(ast);
    }

    private WindowExpression lookupWindow(QueryModel model, CharSequence name) {
        while (model != null) {
            final WindowExpression window = model.getNamedWindows().get(name);
            if (window != null) {
                return window;
            }
            if (model.isNestedModelIsSubQuery()) {
                break;
            }
            model = model.getNestedModel();
        }
        return null;
    }

    private boolean nameWindowNode(ExpressionNode node, QueryModel model) {
        final BindScope scope = ctx.scope();
        if (node == null || node.windowExpression == null) {
            return false;
        }
        final int mark = windowGroupMembers.size();
        nameWindows(node.lhs, model);
        nameWindows(node.rhs, model);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            nameWindows(node.args.getQuick(i), model);
        }
        closeWindowGroup(mark);
        final int index = findWindowNode(node);
        if (scope.windowNames.getQuick(index) == null) {
            final CharSequence name = windowOutputName(scope.windowNodes.getQuick(index), model);
            scope.windowNames.setQuick(index, ctx.createOutputName(name));
            windowGroupMembers.add(index);
        } else if (scope.windowLevels.getQuick(index) == 0) {
            // an enclosing group still holds it; the innermost open group needs it first
            windowGroupMembers.setQuick(windowGroupMembers.indexOf(index, 0, windowGroupMembers.size()), -1);
            windowGroupMembers.add(index);
        }
        return true;
    }

    /**
     * Names window functions and assigns their levels: the windows nested directly in one window form
     * a level below it, levels stack in the order they complete, and the SELECT-list windows form the
     * top level. Returns the number of levels.
     */
    private int nameWindowNodes(QueryModel model) {
        final BindScope scope = ctx.scope();
        scope.windowNames.setAll(scope.windowNodes.size(), null);
        scope.windowLevels.setAll(scope.windowNodes.size(), 0);
        windowGroupMembers.clear();
        windowLevelCount = 0;
        for (int i = 0, n = scope.windowSelectExpressions.size(); i < n; i++) {
            nameWindows(scope.windowSelectExpressions.getQuick(i), model);
        }
        for (int i = 0, n = scope.windowOrderExpressions.size(); i < n; i++) {
            nameWindows(scope.windowOrderExpressions.getQuick(i), model);
        }
        closeWindowGroup(0);
        return windowLevelCount;
    }

    private void nameWindows(ExpressionNode node, QueryModel model) {
        if (node == null || nameWindowNode(node, model)) {
            return;
        }
        nameWindows(node.lhs, model);
        nameWindows(node.rhs, model);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            nameWindows(node.args.getQuick(i), model);
        }
    }

    /**
     * A window function argument may name an earlier computed column, which it reads through a
     * self-reference column of the inner projection.
     */
    private boolean referencesEarlierAlias(ExpressionNode node, ObjList<QueryColumn> columns, int columnIndex, OutputSchema source,
                                           boolean isWindowArgument) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.LITERAL) {
            if (Chars.indexOfLastUnquoted(node.token, '.') >= 0 || getColumnIndexQuiet(source, node.token) >= 0) {
                return false;
            }
            for (int i = 0; i < columnIndex; i++) {
                if (Chars.equalsIgnoreCase(columns.getQuick(i).getName(), GenericLexer.unquote(node.token))) {
                    return !isWindowArgument || !isPureComputedColumn(columns.getQuick(i).getAst());
                }
            }
            return false;
        }
        isWindowArgument |= node.windowExpression != null;
        if (referencesEarlierAlias(node.lhs, columns, columnIndex, source, isWindowArgument)
                || referencesEarlierAlias(node.rhs, columns, columnIndex, source, isWindowArgument)) {
            return true;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (referencesEarlierAlias(node.args.getQuick(i), columns, columnIndex, source, isWindowArgument)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Gives the columns under a window unique names: the window factory's metadata cannot hold the
     * duplicate names a join's qualified columns may carry.
     */
    private void renameQualifiedWindowInput(ProjectPlan project) {
        final BindScope scope = ctx.scope();
        if (!project.getInput().getOutput().hasColumnQualifiers()) {
            return;
        }
        scope.aliases.clear();
        scope.aliasSequences.clear();
        final OutputSchema output = project.getOutput();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            final CharSequence name = SqlUtil.createColumnAlias(ctx.characterStore, output.getColumnName(i), -1,
                    scope.aliases, scope.aliasSequences, false);
            scope.aliases.add(name);
            output.setColumnName(i, name, null);
        }
    }

    private ExpressionNode replaceWindowReferences(ExpressionNode expression, boolean isRootReplaced) {
        final BindScope scope = ctx.scope();
        if (expression == null) {
            return null;
        }
        for (int i = 0, n = scope.windowAliasCopies.size(); i < n; i++) {
            final int aliasId = scope.windowAliasIds.getQuick(scope.windowAliasCopyColumns.getQuick(i));
            if (scope.windowAliasCopies.getQuick(i) == expression && aliasId >= 0) {
                return windowColumnReference(aliasId, expression.position);
            }
        }
        for (int i = 0, n = scope.windowAliasResolutions.size(); i < n; i++) {
            if (scope.windowAliasReferences.getQuick(i) == expression) {
                return scope.windowAliasResolutions.getQuick(i);
            }
        }
        if (isRootReplaced && expression.windowExpression != null) {
            final int index = findWindowNode(expression);
            if (index >= 0 && scope.windowColumnIds.getQuick(index) >= 0) {
                return windowColumnReference(scope.windowColumnIds.getQuick(index), expression.position);
            }
        }
        final ExpressionNode copy = ctx.bindingExpressions.next().of(expression.type, expression.token, expression.precedence, expression.position);
        copy.paramCount = expression.paramCount;
        copy.queryModel = expression.queryModel;
        copy.isTimestampOrderInherited = expression.isTimestampOrderInherited;
        copy.windowExpression = expression.windowExpression;
        copy.lhs = replaceWindowReferences(expression.lhs, true);
        copy.rhs = replaceWindowReferences(expression.rhs, true);
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            copy.args.add(replaceWindowReferences(expression.args.getQuick(i), true));
        }
        return copy;
    }

    private WindowExpression resolveWindow(WindowExpression syntax, QueryModel model) throws SqlException {
        final WindowExpression result = syntax.deepClone(ctx.planNodes.windowSyntax, ctx.bindingExpressions);
        if (syntax.isNamedWindowReference()) {
            final WindowExpression named = lookupWindow(model, syntax.getWindowName());
            if (named == null) {
                throw SqlException.$(syntax.getWindowNamePosition(), "window '").put(syntax.getWindowName()).put("' is not defined");
            }
            result.copySpecFrom(resolveWindow(named, model), ctx.bindingExpressions);
            result.setResolvedWindow(syntax.getWindowName(), named.getAnchorKind() != WindowExpression.ANCHOR_KIND_NONE);
        }
        if (syntax.hasBaseWindow()) {
            final int position = syntax.getBaseWindowNamePosition();
            if (!windowInheritancePositions.add(position)) {
                throw SqlException.$(position, "circular window reference");
            }
            final WindowExpression parent = lookupWindow(model, syntax.getBaseWindowName());
            if (parent == null) {
                throw SqlException.$(position, "window '").put(syntax.getBaseWindowName()).put("' is not defined");
            }
            final WindowExpression base = resolveWindow(parent, model);
            if (result.getPartitionBy().size() == 0) {
                result.getPartitionBy().addAll(base.getPartitionBy());
            }
            if (result.getOrderBy().size() == 0) {
                result.getOrderBy().addAll(base.getOrderBy());
                result.getOrderByDirection().addAll(base.getOrderByDirection());
            }
            if (!result.isNonDefaultFrame()) {
                result.setFramingMode(base.getFramingMode());
                result.setRowsLoExpr(base.getRowsLoExpr(), base.getRowsLoExprPos());
                result.setRowsLoExprTimeUnit(base.getRowsLoExprTimeUnit());
                result.setRowsLoKind(base.getRowsLoKind(), base.getRowsLoKindPos());
                result.setRowsHiExpr(base.getRowsHiExpr(), base.getRowsHiExprPos());
                result.setRowsHiExprTimeUnit(base.getRowsHiExprTimeUnit());
                result.setRowsHiKind(base.getRowsHiKind(), base.getRowsHiKindPos());
                result.setExclusionKind(base.getExclusionKind(), base.getExclusionKindPos());
            }
            result.setBaseWindowName(null, 0);
            windowInheritancePositions.remove(position);
        }
        return result;
    }

    /**
     * A SELECT column naming an earlier column that holds a window reads that column's value when it
     * evaluates a volatile function, and otherwise computes the aliased expression again.
     */
    private void resolveWindowAliasReferences(QueryModel source, SqlExecutionContext executionContext) {
        final BindScope scope = ctx.scope();
        for (int i = 0, n = scope.windowAliasReferences.size(); i < n; i++) {
            final ExpressionNode reference = scope.windowAliasReferences.getQuick(i);
            final int column = scope.windowAliasReferenceColumns.getQuick(i);
            final int aliasId = scope.windowAliasIds.getQuick(column);
            if (aliasId >= 0) {
                scope.windowAliasResolutions.add(windowColumnReference(aliasId, reference.position));
            } else {
                final ExpressionNode aliased = replaceWindowReferences(scope.windowSelectExpressions.getQuick(column), true);
                scope.windowAliasResolutions.add(bindVolatile(aliased, source, executionContext) != null ? reference : aliased);
            }
        }
    }

    private ExpressionNode rewriteOrderWindowAliases(ExpressionNode expression, QueryModel model, QueryModel source,
                                                     SqlExecutionContext executionContext) throws SqlException {
        if (expression == null) {
            return null;
        }
        if (expression.windowExpression != null) {
            return rewriteWindowAliases(expression, model, source, false, executionContext);
        }
        expression.lhs = rewriteOrderWindowAliases(expression.lhs, model, source, executionContext);
        expression.rhs = rewriteOrderWindowAliases(expression.rhs, model, source, executionContext);
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            expression.args.setQuick(i, rewriteOrderWindowAliases(expression.args.getQuick(i), model, source, executionContext));
        }
        return expression;
    }

    private ExpressionNode rewriteWindowAliases(ExpressionNode expression, QueryModel model, QueryModel source,
                                                boolean isWindowArgument, SqlExecutionContext executionContext) throws SqlException {
        final BindScope scope = ctx.scope();
        if (expression == null) {
            return null;
        }
        final boolean isAlias = expression.type == ExpressionNode.LITERAL && !expression.isWildcard()
                && Chars.indexOfLastUnquoted(expression.token, '.') < 0 && getColumnIndexQuiet(scope.windowInput.getOutput(), expression.token) < 0;
        if (!isWindowArgument && !isAlias && expression.type == ExpressionNode.LITERAL && !expression.isWildcard()) {
            ctx.bindColumnIndex(expression, scope.windowInput.getOutput(), source);
        }
        if (isWindowArgument && expression.type == ExpressionNode.LITERAL) {
            final int index = scope.windowSelfReferences.keyIndex(expression.token);
            if (index < 0) {
                return windowColumnReference(scope.windowSelfReferences.valueAt(index), expression.position);
            }
        }
        if (isAlias) {
            for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
                final ExpressionNode selected = model.getBottomUpColumns().getQuick(i).getAst();
                if (scope.windowAliasIds.getQuick(i) >= 0 && selected.type == ExpressionNode.LITERAL && Chars.equalsIgnoreCase(selected.token, expression.token)) {
                    return windowColumnReference(scope.windowAliasIds.getQuick(i), expression.position);
                }
            }
            for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
                final QueryColumn column = model.getBottomUpColumns().getQuick(i);
                if (!Chars.equalsIgnoreCase(column.getName(), expression.token)) {
                    continue;
                }
                int id = scope.windowAliasIds.getQuick(i);
                if (id == -2) {
                    throw SqlException.invalidColumn(expression.position, expression.token);
                }
                if (id < 0 && !isWindowArgument && i < scope.windowSelectExpressions.size()
                        && findWindowPosition(column.getAst(), false, false) >= 0 && !ctx.hasAggregation(model, source)) {
                    scope.windowAliasReferences.add(expression);
                    scope.windowAliasReferenceColumns.add(i);
                    return expression;
                }
                if (id < 0) {
                    scope.windowAliasIds.setQuick(i, -2);
                    final ExpressionNode aliased = rewriteWindowAliases(copyWindowSyntax(column.getAst(), column.getAst()),
                            model, source, true, executionContext);
                    scope.windowAliasIds.setQuick(i, -1);
                    if (ctx.hasAggregate(aliased)) {
                        return aliased;
                    }
                    if (findWindowPosition(aliased, false, false) >= 0) {
                        scope.windowAliasCopies.add(aliased);
                        scope.windowAliasCopyColumns.add(i);
                        return aliased;
                    }
                    final BoundExpression bound = ctx.functionBinder.bind(aliased, ctx.windowBindingScope(scope.windowInput.getOutput()), sourceAlias(source), ColumnType.STRING, executionContext);
                    if (bound instanceof ColumnExpression ref) {
                        id = ref.getColumnId();
                    } else {
                        assert aliased != null;
                        id = appendWindowInputExpression(bound, column.getName(), aliased.position);
                    }
                    scope.windowAliasIds.setQuick(i, id);
                }
                return windowColumnReference(id, expression.position);
            }
            if (!isWindowArgument) {
                ctx.bindColumnIndex(expression, scope.windowInput.getOutput(), source);
            }
        }
        if (expression.windowExpression != null) {
            windowInheritancePositions.clear();
            final WindowExpression syntax = resolveWindow(expression.windowExpression, model);
            expression.windowExpression = syntax;
            validateWindowSpec(syntax);
            SqlUtil.normalizeWindowFrame(syntax, functionParser, executionContext);
            for (int i = 0, n = syntax.getPartitionBy().size(); i < n; i++) {
                syntax.getPartitionBy().setQuick(i, rewriteWindowAliases(syntax.getPartitionBy().getQuick(i), model, source, true, executionContext));
            }
            for (int i = 0, n = syntax.getOrderBy().size(); i < n; i++) {
                syntax.getOrderBy().setQuick(i, rewriteWindowAliases(syntax.getOrderBy().getQuick(i), model, source, true, executionContext));
            }
            isWindowArgument = true;
        }
        expression.lhs = rewriteWindowAliases(expression.lhs, model, source, isWindowArgument, executionContext);
        expression.rhs = rewriteWindowAliases(expression.rhs, model, source, isWindowArgument, executionContext);
        for (int i = 0, n = expression.args.size(); i < n; i++) {
            expression.args.setQuick(i, rewriteWindowAliases(expression.args.getQuick(i), model, source, isWindowArgument, executionContext));
        }
        return expression;
    }

    /**
     * The SELECT expression an ORDER BY expression repeats, or the ORDER BY expression itself.
     */
    private ExpressionNode selectedOrderExpression(QueryModel model, ExpressionNode order) {
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            final ExpressionNode selected = model.getBottomUpColumns().getQuick(i).getAst();
            if (ExpressionNode.compareNodesExact(order, selected)) {
                return selected;
            }
        }
        return order;
    }

    private void validateWindowClause(ExpressionNode expression, CharSequence name) throws SqlException {
        final int position = findWindowPosition(expression, true, false);
        if (position >= 0) {
            throw SqlException.$(position, "window function is not allowed in ").put(name).put(" clause");
        }
    }

    private void validateWindowSpec(WindowExpression spec) throws SqlException {
        for (int i = 0, n = spec.getPartitionBy().size(); i < n; i++) {
            final ExpressionNode expression = spec.getPartitionBy().getQuick(i);
            if (findWindowPosition(expression, false, false) >= 0) {
                throw SqlException.$(expression.position, "window function is not allowed in PARTITION BY clause");
            }
        }
        for (int i = 0, n = spec.getOrderBy().size(); i < n; i++) {
            final ExpressionNode expression = spec.getOrderBy().getQuick(i);
            if (findWindowPosition(expression, false, false) >= 0) {
                throw SqlException.$(expression.position, "window function is not allowed in ORDER BY clause of window specification");
            }
        }
    }

    private ExpressionNode windowColumnReference(int id, int position) {
        final OutputSchema output = ctx.scope().windowInput.getOutput();
        final int index = output.getColumnIndexById(id);
        if (index < 0) {
            throw new IllegalStateException("window dependency is outside its input");
        }
        return ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, output.getColumnName(index), 0, position);
    }

    /**
     * The window call, as written in the query, that a bound copy of it stands for.
     */
    private ExpressionNode windowOccurrence(ExpressionNode node) {
        final BindScope scope = ctx.scope();
        for (int i = 0, n = scope.windowCopies.size(); i < n; i++) {
            if (scope.windowCopies.getQuick(i) == node) {
                return scope.windowCopyOrigins.getQuick(i);
            }
        }
        return node;
    }

    /**
     * Names a window column after the SELECT alias it implements, or after its function.
     */
    private CharSequence windowOutputName(ExpressionNode node, QueryModel model) {
        final BindScope scope = ctx.scope();
        for (int i = 0, n = scope.windowSelectExpressions.size(); i < n; i++) {
            if (scope.windowSelectExpressions.getQuick(i) == node) {
                final CharSequence alias = model.getBottomUpColumns().getQuick(i).getName();
                return Chars.indexOf(alias, '.') < 0 ? alias : node.token;
            }
        }
        return node.token;
    }

    FunctionExpression bindWindowFunction(ExpressionNode expression, WindowSpec spec, OutputSchema input,
                                          QueryModel source, SqlExecutionContext executionContext) throws SqlException {
        try {
            final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            for (int i = 0, n = spec.getPartitionBy().size(); i < n; i++) {
                final BoundExpression partition = spec.getPartitionBy().getQuick(i);
                if (!LogicalPlans.hasOuterColumn(partition)) {
                    keyTypes.add(partition.getDataType());
                }
            }
            final boolean isPartitioned = keyTypes.getColumnCount() > 0;
            final int orderCount = spec.getOrderByColumnIds().size();
            final boolean isTimestampOrdered = orderCount == 1
                    && spec.getOrderByColumnIds().getQuick(0) == input.getTimestampColumnId();
            final int direction = isTimestampOrdered
                    ? spec.getOrderByDirections().getQuick(0) == SortDirection.ASCENDING ? RecordCursorFactory.SCAN_DIRECTION_FORWARD : RecordCursorFactory.SCAN_DIRECTION_BACKWARD
                    : RecordCursorFactory.SCAN_DIRECTION_OTHER;
            final int timestampIndex = input.getTimestampIndex();
            executionContext.configureWindowContext(isPartitioned ? bindPartitionRecord : null, isPartitioned ? UNUSED_PARTITION_SINK : null,
                    keyTypes, orderCount > 0,
                    direction, orderCount > 0 ? spec.getOrderByPositions().getQuick(0) : -1, false,
                    spec.getFramingMode(), spec.getRowsLo(), spec.getRowsLoExprTimeUnit(), spec.getRowsLoExprPos(), spec.getRowsLoKindPos(),
                    spec.getRowsHi(), spec.getRowsHiExprTimeUnit(), spec.getRowsHiExprPos(), spec.getRowsHiKindPos(),
                    spec.getExclusionKind(), spec.getExclusionKindPos(), timestampIndex,
                    timestampIndex < 0 ? ColumnType.UNDEFINED : input.getColumnType(timestampIndex), spec.isIgnoreNulls(), spec.getNullsDescPos());
            return ctx.functionBinder.bindWindow(expression, ctx.windowCallBindingScope(input), sourceAlias(source), executionContext);
        } finally {
            executionContext.clearWindowContext();
        }
    }

    LogicalPlan bindWindows(QueryModel model, QueryModel source, LogicalPlan input, SqlExecutionContext executionContext) throws SqlException {
        final BindScope bindScope = ctx.scope();
        bindScope.windowInput = input;
        bindScope.windowNodes.clear();
        bindScope.windowOrderExpressions.clear();
        bindScope.windowLevels.clear();
        bindScope.windowColumnIds.clear();
        clearWindowAliases();
        bindScope.windowCopies.clear();
        bindScope.windowCopyOrigins.clear();
        bindScope.windowSelectExpressions.clear();
        bindScope.windowAliasIds.setAll(model.getBottomUpColumns().size(), -1);
        bindScope.windowSelfReferences.clear();
        bindScope.aliases.clear();
        bindScope.aliasSequences.clear();
        ProjectPlan innerProject = null;
        if (hasPureComputedColumn(model) && model.getNamedWindows().size() == 0
                && bindScope.outerScopes.size() == 0 && !ctx.hasAggregation(model, source)
                && !hasProjectionReferences(model, input.getOutput())) {
            innerProject = bindWindowInnerProjection(model, source, input, executionContext);
            bindScope.windowInput = innerProject;
            bindScope.aliases.clear();
            bindScope.aliasSequences.clear();
        }
        for (int i = 0, n = bindScope.windowInput.getOutput().getColumnCount(); i < n; i++) {
            bindScope.aliases.add(bindScope.windowInput.getOutput().getColumnName(i));
        }
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            final ExpressionNode selected = model.getBottomUpColumns().getQuick(i).getAst();
            final ExpressionNode ast = copyWindowSyntax(selected, selected);
            if (ast.windowExpression != null && ast.type == ExpressionNode.FUNCTION && SqlKeywords.isCountKeyword(ast.token)
                    && ast.paramCount == 1 && ast.rhs.type == ExpressionNode.CONSTANT && !SqlKeywords.isNullKeyword(ast.rhs.token)) {
                ast.rhs = null;
                ast.paramCount = 0;
            }
            final ExpressionNode expression = bindScope.windowAliasIds.getQuick(i) >= 0 ? ast
                    : rewriteWindowAliases(ast, model, source, false, executionContext);
            bindScope.windowSelectExpressions.add(expression);
            collectWindowNodes(expression);
        }
        for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
            final ExpressionNode order = source.getOrderBy().getQuick(i);
            final ExpressionNode expression = rewriteOrderWindowAliases(copyWindowSyntax(order, selectedOrderExpression(model, order)),
                    model, source, executionContext);
            bindScope.windowOrderExpressions.add(expression);
            collectWindowNodes(expression);
        }
        final int levels = nameWindowNodes(model);
        WindowPlan bottomWindow = null;
        bindScope.windowColumnIds.setAll(bindScope.windowNodes.size(), -1);
        bindScope.windowOrderViolations.clear();
        bindScope.windowOrderViolations.setAll(bindScope.windowNodes.size(), null);
        for (int level = 1; level <= levels; level++) {
            bindVolatileAliasCopies(model, source, executionContext);
            bindScope.windowLevelSpecs.clear();
            for (int i = 0, n = bindScope.windowNodes.size(); i < n; i++) {
                if (bindScope.windowLevels.getQuick(i) != level) {
                    continue;
                }
                final ExpressionNode node = bindScope.windowNodes.getQuick(i);
                final WindowExpression syntax = node.windowExpression;
                final WindowSpec spec = ctx.planNodes.windowSpecs.next().of(syntax);
                bindScope.windowLevelSpecs.add(spec);
                if (executionContext.isLiveViewCompile()) {
                    spec.setLiveViewDescription(LiveViewWindowDescription.of(syntax));
                }
                bindWindowSpec(i, spec, source, executionContext);
            }
            final WindowPlan window = ctx.planNodes.windowPlans.next().of(bindScope.windowInput, model.getModelPosition());
            if (level == 1) {
                bottomWindow = window;
            }
            window.getOutput().copyFrom(bindScope.windowInput.getOutput());
            int specIndex = 0;
            for (int i = 0, n = bindScope.windowNodes.size(); i < n; i++) {
                if (bindScope.windowLevels.getQuick(i) != level) {
                    continue;
                }
                final ExpressionNode node = bindScope.windowNodes.getQuick(i);
                final WindowSpec spec = bindScope.windowLevelSpecs.getQuick(specIndex++);
                if (spec.getLiveViewDescription() != null) {
                    final OutputSchema scope = bindScope.windowInput.getOutput();
                    LiveViewCheckpointFunctionCompiler.validateRange(spec.getLiveViewDescription(), node.token,
                            spec.getOrderByColumnIds().size() == 1 && scope.getTimestampIndex() >= 0
                                    && spec.getOrderByColumnIds().getQuick(0) == scope.getTimestampColumnId()
                                    && spec.getOrderByDirections().getQuick(0) == SortDirection.ASCENDING);
                }
                final FunctionExpression function = bindWindowFunction(replaceWindowReferences(node, false), spec, bindScope.windowInput.getOutput(), source, executionContext);
                final ExpressionNode violation = bindScope.windowOrderViolations.getQuick(i);
                if (violation != null) {
                    throw SqlException.invalidColumn(violation.position, violation.token);
                }
                final int id = bindScope.nextColumnId++;
                window.getFunctions().add(function);
                window.getSpecs().add(spec);
                window.getFunctionColumnIds().add(id);
                window.getOutput().add(id, bindScope.windowNames.getQuick(i), function.getDataType(), false);
                bindScope.windowColumnIds.setQuick(i, id);
            }
            if (window.getFunctions().size() > 0) {
                bindScope.windowInput = window;
            } else if (level == 1) {
                bottomWindow = null;
            }
        }
        resolveWindowAliasReferences(source, executionContext);
        for (int i = 0, n = bindScope.windowSelectExpressions.size(); i < n; i++) {
            final ExpressionNode expression = bindScope.windowSelectExpressions.getQuick(i);
            bindScope.windowSelectExpressions.setQuick(i, bindScope.windowAliasIds.getQuick(i) >= 0
                    ? windowColumnReference(bindScope.windowAliasIds.getQuick(i), expression.position)
                    : replaceWindowReferences(expression, true));
        }
        for (int i = 0, n = bindScope.windowOrderExpressions.size(); i < n; i++) {
            bindScope.windowOrderExpressions.setQuick(i, replaceWindowReferences(bindScope.windowOrderExpressions.getQuick(i), true));
        }
        if (innerProject != null) {
            renameQualifiedWindowInput(innerProject);
        } else if (bottomWindow != null && bottomWindow.getInput() == input && input.getOutput().hasColumnQualifiers()) {
            final OutputSchema output = input.getOutput();
            final ProjectPlan project = ctx.planNodes.projects.next().of(input, bottomWindow.getPosition());
            for (int i = 0, n = output.getColumnCount(); i < n; i++) {
                addWindowTranslationColumn(project, output, i, output.getColumnName(i));
            }
            bottomWindow.replaceInput(0, project);
            renameQualifiedWindowInput(project);
        }
        final LogicalPlan result = bindScope.windowInput;
        bindScope.windowInput = null;
        ctx.stopTimestampIntrinsics(result.getOutput());
        return result;
    }

    int findAggregateOverWindowPosition(ExpressionNode expression) {
        if (expression == null) {
            return -1;
        }
        if (expression.type == ExpressionNode.FUNCTION && ctx.functionFactoryCache.isGroupBy(expression.token)) {
            return findWindowPosition(expression, false, false) >= 0 ? expression.position : -1;
        }
        int position = findAggregateOverWindowPosition(expression.lhs);
        if (position < 0) {
            position = findAggregateOverWindowPosition(expression.rhs);
        }
        for (int i = expression.args.size() - 1; position < 0 && i >= 0; i--) {
            position = findAggregateOverWindowPosition(expression.args.getQuick(i));
        }
        return position;
    }

    int findWindowPosition(ExpressionNode expression, boolean isPureNameIncluded, boolean isAggregateBarrier) {
        if (expression == null) {
            return -1;
        }
        if (expression.windowExpression != null || isPureNameIncluded && expression.type == ExpressionNode.FUNCTION
                && ctx.functionFactoryCache.isPureWindowFunction(expression.token)) {
            return expression.position;
        }
        if (isAggregateBarrier && ctx.isAggregate(expression)) {
            return -1;
        }
        int position = findWindowPosition(expression.lhs, isPureNameIncluded, isAggregateBarrier);
        if (position < 0) {
            position = findWindowPosition(expression.rhs, isPureNameIncluded, isAggregateBarrier);
        }
        for (int i = 0, n = expression.args.size(); position < 0 && i < n; i++) {
            position = findWindowPosition(expression.args.getQuick(i), isPureNameIncluded, isAggregateBarrier);
        }
        return position;
    }

    /**
     * Whether a SELECT column names an earlier column's alias that no source column shadows.
     * Such a reference reads the earlier column's value rather than evaluating it again.
     */
    boolean hasProjectionReferences(QueryModel model, OutputSchema source) {
        final ObjList<QueryColumn> columns = model.getBottomUpColumns();
        for (int i = 1, n = columns.size(); i < n; i++) {
            if (referencesEarlierAlias(columns.getQuick(i).getAst(), columns, i, source, false)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether a SELECT column over windows names an earlier column's alias; such a column reads the earlier
     * window function's value.
     */
    boolean hasWindowProjectionReferences(QueryModel model, OutputSchema source) {
        final ObjList<QueryColumn> columns = model.getBottomUpColumns();
        for (int i = 1, n = columns.size(); i < n; i++) {
            if (referencesEarlierAlias(ctx.scope().windowSelectExpressions.getQuick(i), columns, i, source, false)) {
                return true;
            }
        }
        return false;
    }

    boolean hasWindows(QueryModel model, QueryModel source) {
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            if (findWindowPosition(model.getBottomUpColumns().getQuick(i).getAst(), false, false) >= 0) {
                return true;
            }
        }
        for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
            if (findWindowPosition(source.getOrderBy().getQuick(i), false, false) >= 0) {
                return true;
            }
        }
        return false;
    }

    boolean isAggregationFree(QueryModel model, QueryModel source) {
        if (blockGroupBy(model, source).size() > 0) {
            return false;
        }
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            final ExpressionNode expression = model.getBottomUpColumns().getQuick(i).getAst();
            if (expression.windowExpression == null && ctx.hasAggregate(expression)) {
                return false;
            }
        }
        for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
            final ExpressionNode expression = source.getOrderBy().getQuick(i);
            if (expression.windowExpression == null && ctx.hasAggregate(expression)) {
                return false;
            }
        }
        return true;
    }

    void validateNamedWindows(QueryModel model) throws SqlException {
        final ObjList<CharSequence> names = model.getNamedWindows().keys();
        for (int i = 0, n = names.size(); i < n; i++) {
            windowInheritancePositions.clear();
            resolveWindow(model.getNamedWindows().get(names.getQuick(i)), model);
        }
    }

    /**
     * Rejects window functions an aggregating block cannot evaluate.
     */
    void validateWindowAggregation(QueryModel model, QueryModel source) throws SqlException {
        boolean hasTopLevelWindow = false;
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            hasTopLevelWindow |= validateWindowAggregation(model.getBottomUpColumns().getQuick(i).getAst());
        }
        for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
            hasTopLevelWindow |= validateWindowAggregation(source.getOrderBy().getQuick(i));
        }
        if (hasTopLevelWindow) {
            throw SqlException.$(0, "Window function is not allowed in context of aggregation. Use sub-query.");
        }
    }

    boolean validateWindowAggregation(ExpressionNode expression) throws SqlException {
        if (expression.windowExpression != null) {
            return true;
        }
        final int position = findWindowPosition(expression, true, true);
        if (position >= 0) {
            throw SqlException.$(position, "Window function is not allowed in context of aggregation. Use sub-query.");
        }
        if (!ctx.isAggregate(expression)) {
            final int aggregatePosition = findAggregateOverWindowPosition(expression);
            if (aggregatePosition >= 0) {
                throw SqlException.$(aggregatePosition, "Aggregate over window function cannot be combined with other terms. Use a sub-query.");
            }
        }
        return false;
    }

    void validateWindowClauses(QueryModel model) throws SqlException {
        validateWindowClause(model.getWhereClause(), "WHERE");
        validateWindowClause(model.getJoinCriteria(), "JOIN ON");
    }

    void validateWindowOrder(QueryModel model, QueryModel source) throws SqlException {
        for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
            final ExpressionNode expression = source.getOrderBy().getQuick(i);
            if (expression.type != ExpressionNode.FUNCTION
                    || expression.windowExpression == null && !ctx.functionFactoryCache.isPureWindowFunction(expression.token)) {
                continue;
            }
            boolean isSelected = false;
            for (int k = 0, count = model.getBottomUpColumns().size(); k < count; k++) {
                if (ExpressionNode.compareNodesExact(expression, model.getBottomUpColumns().getQuick(k).getAst())) {
                    isSelected = true;
                    break;
                }
            }
            if (!isSelected) {
                if (ctx.functionFactoryCache.isGroupBy(expression.token)) {
                    throw SqlException.$(expression.position, "Window function is not allowed in context of aggregation. Use sub-query.");
                }
                throw SqlException.emptyWindowContext(expression.position);
            }
        }
    }
}
