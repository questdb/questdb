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
import io.questdb.griffin.plan.logical.DeferredErrorExpression;
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
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
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
    final IntList windowAliasIds = new IntList();
    final ObjList<CharSequence> windowNames = new ObjList<>();
    final ObjList<ExpressionNode> windowOrderExpressions = new ObjList<>();
    /**
     * Per select column, the deferred error of a window call it holds that failed to bind, otherwise null.
     */
    final ObjList<DeferredErrorExpression> windowSelectErrors = new ObjList<>();
    final ObjList<ExpressionNode> windowSelectExpressions = new ObjList<>();
    private final VirtualRecord bindPartitionRecord = new VirtualRecord(new ObjList<>());
    private final BindContext ctx;
    private final FunctionParser functionParser;
    private final ObjList<ExpressionNode> windowAliasCopies = new ObjList<>();
    private final IntList windowAliasCopyColumns = new IntList();
    private final IntList windowAliasReferenceColumns = new IntList();
    private final ObjList<ExpressionNode> windowAliasReferences = new ObjList<>();
    private final ObjList<ExpressionNode> windowAliasResolutions = new ObjList<>();
    private final IntList windowColumnIds = new IntList();
    private final ObjList<DeferredErrorExpression> windowErrors = new ObjList<>();
    /**
     * Per window call, the first of its ORDER BY expressions that is not a column, otherwise null.
     */
    private final ObjList<ExpressionNode> windowOrderViolations = new ObjList<>();
    private final ObjList<ExpressionNode> windowCopies = new ObjList<>();
    private final ObjList<ExpressionNode> windowCopyOrigins = new ObjList<>();
    private final IntList windowGroupMembers = new IntList();
    private final IntHashSet windowInheritancePositions = new IntHashSet();
    private final IntList windowLevels = new IntList();
    private final ObjList<ExpressionNode> windowNodes = new ObjList<>();
    private final LowerCaseCharSequenceIntHashMap windowSelfReferences = new LowerCaseCharSequenceIntHashMap();
    private final LowerCaseCharSequenceHashSet windowTranslatedNames;
    private final LowerCaseCharSequenceIntHashMap windowTranslatedSequences;
    private LogicalPlan windowInput;
    private int windowLevelCount;

    WindowBinder(BindContext ctx, FunctionParser functionParser) {
        this.ctx = ctx;
        this.functionParser = functionParser;
        this.windowTranslatedNames = ctx.aliases;
        this.windowTranslatedSequences = ctx.aliasSequences;
    }

    @Override
    public void clear() {
        windowAliasIds.clear();
        windowOrderExpressions.clear();
        windowSelectErrors.clear();
        windowSelectExpressions.clear();
        windowErrors.clear();
        windowOrderViolations.clear();
        windowInput = null;
        windowSelfReferences.clear();
        windowColumnIds.clear();
        clearWindowAliases();
        windowCopies.clear();
        windowCopyOrigins.clear();
        windowInheritancePositions.clear();
        windowGroupMembers.clear();
        windowLevels.clear();
        windowNames.clear();
        windowNodes.clear();
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
        if (copy == null || occurrence == null) {
            return;
        }
        if (copy.windowExpression != null) {
            windowCopies.add(copy);
            windowCopyOrigins.add(windowOccurrence(occurrence));
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
        ctx.aliases.add(input.getColumnName(index));
        project.getExpressions().add(ctx.columns.next().of(columnId, input.getColumnType(index), project.getPosition()));
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
        if (Chars.indexOfLastUnquoted(node.token, '.') >= 0 || windowSelfReferences.keyIndex(node.token) < 0) {
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
        final int id = ctx.nextColumnId++;
        project.getExpressions().add(ctx.columns.next().of(output.getColumnId(index), output.getColumnType(index), node.position));
        output.add(id, ctx.createOutputName(name), output.getColumnType(index), false);
        windowSelfReferences.put(node.token, id);
    }

    private void addWindowTranslationColumn(ProjectPlan project, OutputSchema input, int index, CharSequence name) {
        project.getExpressions().add(ctx.columns.next().of(input.getColumnId(index), input.getColumnType(index), project.getPosition()));
        project.getOutput().add(input.getColumnId(index), name, input.getColumnType(index), input.getMetadata(index), input.isVisible(index));
        final int outputIndex = project.getOutput().getColumnCount() - 1;
        project.getOutput().setSymbolTableStatic(outputIndex, input.isSymbolTableStatic(index));
        if (index == input.getTimestampIndex()) {
            project.getOutput().setTimestampIndex(outputIndex);
        }
    }

    private int appendWindowInputExpression(BoundExpression expression, CharSequence name, int position) {
        final ProjectPlan project = ctx.projects.next().of(windowInput, position);
        final OutputSchema output = windowInput.getOutput();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            project.getExpressions().add(ctx.columns.next().of(output.getColumnId(i), output.getColumnType(i), position));
        }
        project.getOutput().copyFrom(output);
        final int id = ctx.nextColumnId++;
        project.getExpressions().add(expression);
        project.getOutput().add(id, ctx.createOutputName(name), expression.getDataType(), false);
        windowInput = project;
        return id;
    }

    private boolean areWindowsBound(ExpressionNode expression) {
        if (expression == null) {
            return true;
        }
        if (expression.windowExpression != null && windowColumnIds.getQuick(findWindowNode(expression)) < 0) {
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
            final BoundExpression bound = ctx.functionBinder.bind(expression, ctx.windowBindingScope(windowInput.getOutput()),
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
        for (int i = 0, n = windowAliasCopies.size(); i < n; i++) {
            final ExpressionNode copy = windowAliasCopies.getQuick(i);
            final int column = windowAliasCopyColumns.getQuick(i);
            if (windowAliasIds.getQuick(column) < 0 && areWindowsBound(copy)) {
                final BoundExpression bound = bindVolatile(replaceWindowReferences(copy, true), source, executionContext);
                if (bound != null) {
                    windowAliasIds.setQuick(column, appendWindowInputExpression(bound, model.getBottomUpColumns().getQuick(column).getName(), copy.position));
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
        final OutputSchema output = input.getOutput();
        final ProjectPlan project = ctx.projects.next().of(input, model.getModelPosition());
        final ObjList<QueryColumn> columns = model.getBottomUpColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final ExpressionNode ast = columns.getQuick(i).getAst();
            if (isWildcard(ast)) {
                for (int k = 0, count = output.getColumnCount(); k < count; k++) {
                    if (isWildcardColumn(ast, output, k, sourceAlias(source))) {
                        addWindowInnerColumn(project, output, k);
                    }
                }
            } else if (ast.type == ExpressionNode.LITERAL) {
                addWindowInnerColumn(project, output, ctx.bindColumnIndex(ast, output, source));
            } else if (isPureComputedColumn(ast)) {
                final BoundExpression bound = ctx.cursorNodes.size() > 0
                        ? ctx.functionBinder.bind(ast, ctx.windowBindingScope(output), sourceAlias(source), ColumnType.STRING, executionContext)
                        : ctx.bindDeferrable(ast, ctx.windowBindingScope(output), sourceAlias(source), ColumnType.STRING, executionContext);
                final int id = ctx.nextColumnId++;
                if (bound instanceof DeferredErrorExpression deferred) {
                    ctx.deferredColumns.put(id, deferred);
                }
                project.getExpressions().add(bound);
                project.getOutput().add(id, ctx.createOutputName(columns.getQuick(i).getName()), bound.getDataType(), false);
                windowAliasIds.setQuick(i, id);
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

    private void clearWindowAliases() {
        windowAliasCopies.clear();
        windowAliasCopyColumns.clear();
        windowAliasReferences.clear();
        windowAliasReferenceColumns.clear();
        windowAliasResolutions.clear();
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
                    windowLevels.setQuick(member, windowLevelCount);
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
            windowNodes.add(expression);
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

    private void deferWindow(int index, SqlException e, QueryModel source) throws SqlException {
        final ExpressionNode node = windowNodes.getQuick(index);
        final OutputSchema scope = ctx.windowBindingScope(windowInput.getOutput(), true);
        final WindowExpression syntax = node.windowExpression;
        for (int k = 0, n = syntax.getPartitionBy().size(); k < n; k++) {
            ctx.validateColumnReferences(syntax.getPartitionBy().getQuick(k), scope, sourceAlias(source), null);
        }
        for (int k = 0, n = syntax.getOrderBy().size(); k < n; k++) {
            ctx.validateColumnReferences(syntax.getOrderBy().getQuick(k), scope, sourceAlias(source), null);
        }
        windowErrors.setQuick(index, ctx.deferError(e, node, scope, sourceAlias(source), null));
    }

    /**
     * Gives each select column that holds a window call which failed to bind that call's error. A failed call
     * that anything but a select column reads raises its error at once.
     */
    private void deferWindowSelectErrors(int levels) throws SqlException {
        windowSelectErrors.setAll(windowSelectExpressions.size(), null);
        boolean hasError = false;
        for (int i = 0, n = windowErrors.size(); i < n; i++) {
            hasError |= windowErrors.getQuick(i) != null;
        }
        if (!hasError) {
            return;
        }
        for (int i = 0, n = windowOrderExpressions.size(); i < n; i++) {
            final DeferredErrorExpression deferred = findFailedWindow(windowOrderExpressions.getQuick(i));
            if (deferred != null) {
                throw deferred.raise();
            }
        }
        for (int i = 0, n = windowErrors.size(); i < n; i++) {
            final DeferredErrorExpression deferred = windowErrors.getQuick(i);
            if (deferred != null && (levels > 1 || windowAliasReferences.size() > 0 || windowAliasCopies.size() > 0)) {
                throw deferred.raise();
            }
        }
        for (int i = 0, n = windowSelectExpressions.size(); i < n; i++) {
            if (windowAliasIds.getQuick(i) < 0) {
                windowSelectErrors.setQuick(i, findFailedWindow(windowSelectExpressions.getQuick(i)));
            }
        }
    }

    private DeferredErrorExpression findFailedWindow(ExpressionNode expression) {
        if (expression == null) {
            return null;
        }
        if (expression.windowExpression != null) {
            final int index = findWindowNode(expression);
            if (index >= 0 && windowErrors.getQuick(index) != null) {
                return windowErrors.getQuick(index);
            }
        }
        DeferredErrorExpression deferred = findFailedWindow(expression.lhs);
        if (deferred == null) {
            deferred = findFailedWindow(expression.rhs);
        }
        for (int i = 0, n = expression.args.size(); deferred == null && i < n; i++) {
            deferred = findFailedWindow(expression.args.getQuick(i));
        }
        return deferred;
    }

    private int findWindowNode(ExpressionNode expression) {
        final ExpressionNode occurrence = windowOccurrence(expression);
        for (int i = 0, n = windowNodes.size(); i < n; i++) {
            if (windowOccurrence(windowNodes.getQuick(i)) == occurrence) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns the error of a partition or order expression that fails to bind, otherwise null; an aggregate in
     * PARTITION BY throws. An order expression that is not a column keeps its place in the spec unbound, and
     * fails once the window call has bound: a window orders by input columns only.
     */
    private SqlException bindWindowSpec(int windowIndex, WindowSpec spec, QueryModel source, SqlExecutionContext executionContext) throws SqlException {
        final ExpressionNode node = windowNodes.getQuick(windowIndex);
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
            try {
                spec.getPartitionBy().add(ctx.functionBinder.bind(partition, ctx.windowBindingScope(windowInput.getOutput(), true), sourceAlias(source), executionContext));
            } catch (SqlException e) {
                return e;
            }
        }
        for (int k = 0, count = syntax.getOrderBy().size(); k < count; k++) {
            final ExpressionNode order = replaceWindowReferences(syntax.getOrderBy().getQuick(k), true);
            if (order.type != ExpressionNode.LITERAL) {
                if (windowOrderViolations.getQuick(windowIndex) == null) {
                    windowOrderViolations.setQuick(windowIndex, order);
                }
                spec.getOrderByColumnIds().add(-1);
                spec.getOrderByDirections().add(SqlBinder.sortDirection(syntax.getOrderByDirection().getQuick(k)));
                spec.getOrderByPositions().add(order.position);
                spec.getOrderByNames().add(order.token);
                continue;
            }
            final BoundExpression bound;
            try {
                bound = ctx.functionBinder.bind(order, ctx.windowBindingScope(windowInput.getOutput(), true), sourceAlias(source), executionContext);
            } catch (SqlException e) {
                return e;
            }
            // Only an outer column of a LATERAL body is not an input column of the window.
            final int id = bound instanceof ColumnExpression column ? column.getColumnId()
                    : appendWindowInputExpression(bound, "__window_order", order.position);
            final int index = windowInput.getOutput().getColumnIndexById(id);
            spec.getOrderByColumnIds().add(id);
            spec.getOrderByDirections().add(SqlBinder.sortDirection(syntax.getOrderByDirection().getQuick(k)));
            spec.getOrderByPositions().add(order.position);
            spec.getOrderByNames().add(windowInput.getOutput().getColumnName(index));
        }
        return null;
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
        if (windowNames.getQuick(index) == null) {
            final CharSequence name = windowOutputName(windowNodes.getQuick(index), model);
            windowNames.setQuick(index, ctx.createOutputName(name));
            windowGroupMembers.add(index);
        } else if (windowLevels.getQuick(index) == 0) {
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
        windowNames.setAll(windowNodes.size(), null);
        windowLevels.setAll(windowNodes.size(), 0);
        windowGroupMembers.clear();
        windowLevelCount = 0;
        for (int i = 0, n = windowSelectExpressions.size(); i < n; i++) {
            nameWindows(windowSelectExpressions.getQuick(i), model);
        }
        for (int i = 0, n = windowOrderExpressions.size(); i < n; i++) {
            nameWindows(windowOrderExpressions.getQuick(i), model);
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
        if (!project.getInput().getOutput().hasColumnQualifiers()) {
            return;
        }
        windowTranslatedNames.clear();
        windowTranslatedSequences.clear();
        final OutputSchema output = project.getOutput();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            final CharSequence name = SqlUtil.createColumnAlias(ctx.characterStore, output.getColumnName(i), -1,
                    windowTranslatedNames, windowTranslatedSequences, false);
            windowTranslatedNames.add(name);
            output.setColumnName(i, name, null);
        }
    }

    private ExpressionNode replaceWindowReferences(ExpressionNode expression, boolean isRootReplaced) {
        if (expression == null) {
            return null;
        }
        for (int i = 0, n = windowAliasCopies.size(); i < n; i++) {
            final int aliasId = windowAliasIds.getQuick(windowAliasCopyColumns.getQuick(i));
            if (windowAliasCopies.getQuick(i) == expression && aliasId >= 0) {
                return windowColumnReference(aliasId, expression.position);
            }
        }
        for (int i = 0, n = windowAliasResolutions.size(); i < n; i++) {
            if (windowAliasReferences.getQuick(i) == expression) {
                return windowAliasResolutions.getQuick(i);
            }
        }
        if (isRootReplaced && expression.windowExpression != null) {
            final int index = findWindowNode(expression);
            if (index >= 0 && windowColumnIds.getQuick(index) >= 0) {
                return windowColumnReference(windowColumnIds.getQuick(index), expression.position);
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
        final WindowExpression result = syntax.deepClone(ctx.windowSyntax, ctx.bindingExpressions);
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
        for (int i = 0, n = windowAliasReferences.size(); i < n; i++) {
            final ExpressionNode reference = windowAliasReferences.getQuick(i);
            final int column = windowAliasReferenceColumns.getQuick(i);
            final int aliasId = windowAliasIds.getQuick(column);
            if (aliasId >= 0) {
                windowAliasResolutions.add(windowColumnReference(aliasId, reference.position));
            } else {
                final ExpressionNode aliased = replaceWindowReferences(windowSelectExpressions.getQuick(column), true);
                windowAliasResolutions.add(bindVolatile(aliased, source, executionContext) != null ? reference : aliased);
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
        if (expression == null) {
            return null;
        }
        final boolean isAlias = expression.type == ExpressionNode.LITERAL && !isWildcard(expression)
                && Chars.indexOfLastUnquoted(expression.token, '.') < 0 && getColumnIndexQuiet(windowInput.getOutput(), expression.token) < 0;
        if (!isWindowArgument && !isAlias && expression.type == ExpressionNode.LITERAL && !isWildcard(expression)) {
            ctx.bindColumnIndex(expression, windowInput.getOutput(), source);
        }
        if (isWindowArgument && expression.type == ExpressionNode.LITERAL) {
            final int index = windowSelfReferences.keyIndex(expression.token);
            if (index < 0) {
                return windowColumnReference(windowSelfReferences.valueAt(index), expression.position);
            }
        }
        if (isAlias) {
            for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
                final ExpressionNode selected = model.getBottomUpColumns().getQuick(i).getAst();
                if (windowAliasIds.getQuick(i) >= 0 && selected.type == ExpressionNode.LITERAL && Chars.equalsIgnoreCase(selected.token, expression.token)) {
                    return windowColumnReference(windowAliasIds.getQuick(i), expression.position);
                }
            }
            for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
                final QueryColumn column = model.getBottomUpColumns().getQuick(i);
                if (!Chars.equalsIgnoreCase(column.getName(), expression.token)) {
                    continue;
                }
                int id = windowAliasIds.getQuick(i);
                if (id == -2) {
                    throw SqlException.invalidColumn(expression.position, expression.token);
                }
                if (id < 0 && !isWindowArgument && i < windowSelectExpressions.size()
                        && findWindowPosition(column.getAst(), false, false) >= 0 && !ctx.hasAggregation(model, source)) {
                    windowAliasReferences.add(expression);
                    windowAliasReferenceColumns.add(i);
                    return expression;
                }
                if (id < 0) {
                    windowAliasIds.setQuick(i, -2);
                    final ExpressionNode aliased = rewriteWindowAliases(copyWindowSyntax(column.getAst(), column.getAst()),
                            model, source, true, executionContext);
                    windowAliasIds.setQuick(i, -1);
                    if (ctx.hasAggregate(aliased)) {
                        return aliased;
                    }
                    if (findWindowPosition(aliased, false, false) >= 0) {
                        windowAliasCopies.add(aliased);
                        windowAliasCopyColumns.add(i);
                        return aliased;
                    }
                    final BoundExpression bound = ctx.functionBinder.bind(aliased, ctx.windowBindingScope(windowInput.getOutput()), sourceAlias(source), ColumnType.STRING, executionContext);
                    id = bound instanceof ColumnExpression ref ? ref.getColumnId()
                            : appendWindowInputExpression(bound, column.getName(), aliased.position);
                    windowAliasIds.setQuick(i, id);
                }
                return windowColumnReference(id, expression.position);
            }
            if (!isWindowArgument) {
                ctx.bindColumnIndex(expression, windowInput.getOutput(), source);
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
        final OutputSchema output = windowInput.getOutput();
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
        for (int i = 0, n = windowCopies.size(); i < n; i++) {
            if (windowCopies.getQuick(i) == node) {
                return windowCopyOrigins.getQuick(i);
            }
        }
        return node;
    }

    /**
     * Names a window column after the SELECT alias it implements, or after its function.
     */
    private CharSequence windowOutputName(ExpressionNode node, QueryModel model) {
        for (int i = 0, n = windowSelectExpressions.size(); i < n; i++) {
            if (windowSelectExpressions.getQuick(i) == node) {
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
            return ctx.functionBinder.bindWindow(expression, ctx.windowBindingScope(input, true), sourceAlias(source), executionContext);
        } finally {
            executionContext.clearWindowContext();
        }
    }

    LogicalPlan bindWindows(QueryModel model, QueryModel source, LogicalPlan input, SqlExecutionContext executionContext) throws SqlException {
        windowInput = input;
        windowNodes.clear();
        windowOrderExpressions.clear();
        windowLevels.clear();
        windowColumnIds.clear();
        clearWindowAliases();
        windowCopies.clear();
        windowCopyOrigins.clear();
        windowSelectExpressions.clear();
        windowSelectErrors.clear();
        windowAliasIds.setAll(model.getBottomUpColumns().size(), -1);
        windowSelfReferences.clear();
        ctx.aliases.clear();
        ctx.aliasSequences.clear();
        ProjectPlan innerProject = null;
        if (hasPureComputedColumn(model) && model.getNamedWindows().size() == 0
                && !ctx.functionBinder.hasOuterScope() && !ctx.hasAggregation(model, source)
                && !hasProjectionReferences(model, input.getOutput())) {
            innerProject = bindWindowInnerProjection(model, source, input, executionContext);
            windowInput = innerProject;
            ctx.aliases.clear();
            ctx.aliasSequences.clear();
        }
        for (int i = 0, n = windowInput.getOutput().getColumnCount(); i < n; i++) {
            ctx.aliases.add(windowInput.getOutput().getColumnName(i));
        }
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            final ExpressionNode selected = model.getBottomUpColumns().getQuick(i).getAst();
            final ExpressionNode ast = copyWindowSyntax(selected, selected);
            if (ast.windowExpression != null && ast.type == ExpressionNode.FUNCTION && SqlKeywords.isCountKeyword(ast.token)
                    && ast.paramCount == 1 && ast.rhs.type == ExpressionNode.CONSTANT && !SqlKeywords.isNullKeyword(ast.rhs.token)) {
                ast.rhs = null;
                ast.paramCount = 0;
            }
            final ExpressionNode expression = windowAliasIds.getQuick(i) >= 0 ? ast
                    : rewriteWindowAliases(ast, model, source, false, executionContext);
            windowSelectExpressions.add(expression);
            collectWindowNodes(expression);
        }
        for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
            final ExpressionNode order = source.getOrderBy().getQuick(i);
            final ExpressionNode expression = rewriteOrderWindowAliases(copyWindowSyntax(order, selectedOrderExpression(model, order)),
                    model, source, executionContext);
            windowOrderExpressions.add(expression);
            collectWindowNodes(expression);
        }
        final int levels = nameWindowNodes(model);
        WindowPlan bottomWindow = null;
        windowColumnIds.setAll(windowNodes.size(), -1);
        windowErrors.clear();
        windowErrors.setAll(windowNodes.size(), null);
        windowOrderViolations.clear();
        windowOrderViolations.setAll(windowNodes.size(), null);
        final boolean isDeferrable = ctx.cursorNodes.size() == 0 && !ctx.hasAggregation(model, source);
        for (int level = 1; level <= levels; level++) {
            bindVolatileAliasCopies(model, source, executionContext);
            final int specStart = ctx.windowSpecs.getPos();
            for (int i = 0, n = windowNodes.size(); i < n; i++) {
                if (windowLevels.getQuick(i) != level) {
                    continue;
                }
                final ExpressionNode node = windowNodes.getQuick(i);
                final WindowExpression syntax = node.windowExpression;
                final WindowSpec spec = ctx.windowSpecs.next().of(syntax);
                if (executionContext.isLiveViewCompile()) {
                    spec.setLiveViewDescription(LiveViewWindowDescription.of(syntax));
                }
                final SqlException failure = bindWindowSpec(i, spec, source, executionContext);
                if (failure != null) {
                    if (!isDeferrable) {
                        throw failure;
                    }
                    deferWindow(i, failure, source);
                }
            }
            final WindowPlan window = ctx.windowPlans.next().of(windowInput, model.getModelPosition());
            if (level == 1) {
                bottomWindow = window;
            }
            window.getOutput().copyFrom(windowInput.getOutput());
            int specIndex = specStart;
            for (int i = 0, n = windowNodes.size(); i < n; i++) {
                if (windowLevels.getQuick(i) != level) {
                    continue;
                }
                final ExpressionNode node = windowNodes.getQuick(i);
                final WindowSpec spec = ctx.windowSpecs.peekQuick(specIndex++);
                if (windowErrors.getQuick(i) != null) {
                    continue;
                }
                if (spec.getLiveViewDescription() != null) {
                    final OutputSchema scope = windowInput.getOutput();
                    LiveViewCheckpointFunctionCompiler.validateRange(spec.getLiveViewDescription(), node.token,
                            spec.getOrderByColumnIds().size() == 1 && scope.getTimestampIndex() >= 0
                                    && spec.getOrderByColumnIds().getQuick(0) == scope.getTimestampColumnId()
                                    && spec.getOrderByDirections().getQuick(0) == SortDirection.ASCENDING);
                }
                final FunctionExpression function;
                try {
                    function = bindWindowFunction(replaceWindowReferences(node, false), spec, windowInput.getOutput(), source, executionContext);
                } catch (SqlException e) {
                    if (!isDeferrable) {
                        throw e;
                    }
                    deferWindow(i, e, source);
                    continue;
                }
                final ExpressionNode violation = windowOrderViolations.getQuick(i);
                if (violation != null) {
                    final SqlException e = SqlException.invalidColumn(violation.position, violation.token);
                    if (!isDeferrable) {
                        throw e;
                    }
                    deferWindow(i, e, source);
                    continue;
                }
                final int id = ctx.nextColumnId++;
                window.getFunctions().add(function);
                window.getSpecs().add(spec);
                window.getFunctionColumnIds().add(id);
                window.getOutput().add(id, windowNames.getQuick(i), function.getDataType(), false);
                windowColumnIds.setQuick(i, id);
            }
            if (window.getFunctions().size() > 0) {
                windowInput = window;
            } else if (level == 1) {
                bottomWindow = null;
            }
        }
        deferWindowSelectErrors(levels);
        resolveWindowAliasReferences(source, executionContext);
        for (int i = 0, n = windowSelectExpressions.size(); i < n; i++) {
            if (windowSelectErrors.getQuick(i) != null) {
                continue;
            }
            final ExpressionNode expression = windowSelectExpressions.getQuick(i);
            windowSelectExpressions.setQuick(i, windowAliasIds.getQuick(i) >= 0
                    ? windowColumnReference(windowAliasIds.getQuick(i), expression.position)
                    : replaceWindowReferences(expression, true));
        }
        for (int i = 0, n = windowOrderExpressions.size(); i < n; i++) {
            windowOrderExpressions.setQuick(i, replaceWindowReferences(windowOrderExpressions.getQuick(i), true));
        }
        if (innerProject != null) {
            renameQualifiedWindowInput(innerProject);
        } else if (bottomWindow != null && bottomWindow.getInput() == input && input.getOutput().hasColumnQualifiers()) {
            final OutputSchema output = input.getOutput();
            final ProjectPlan project = ctx.projects.next().of(input, bottomWindow.getPosition());
            for (int i = 0, n = output.getColumnCount(); i < n; i++) {
                addWindowTranslationColumn(project, output, i, output.getColumnName(i));
            }
            bottomWindow.replaceInput(0, project);
            renameQualifiedWindowInput(project);
        }
        final LogicalPlan result = windowInput;
        windowInput = null;
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
            if (referencesEarlierAlias(windowSelectExpressions.getQuick(i), columns, i, source, false)) {
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

    SqlException updateWindowException(QueryModel model) {
        final ObjList<QueryColumn> columns = model.getBottomUpColumns();
        boolean isGrouped = false;
        for (int i = 0, n = columns.size(); i < n && !isGrouped; i++) {
            final ExpressionNode expression = columns.getQuick(i).getAst();
            isGrouped = ctx.hasAggregate(expression)
                    || expression.windowExpression != null && ctx.functionFactoryCache.isGroupBy(expression.token);
        }
        if (isGrouped) {
            for (int i = 0, n = columns.size(); i < n; i++) {
                final ExpressionNode expression = columns.getQuick(i).getAst();
                final int position = expression.windowExpression != null ? expression.position : findWindowPosition(expression, false, true);
                if (position >= 0) {
                    return SqlException.$(position, "Window function is not allowed in context of aggregation. Use sub-query.");
                }
            }
        }
        for (int i = 0, n = columns.size(); i < n; i++) {
            final ExpressionNode expression = columns.getQuick(i).getAst();
            if (expression.windowExpression != null) {
                return SqlException.emptyWindowContext(expression.position);
            }
        }
        return AggregateBinder.updateAggregateException(model, ctx.functionFactoryCache);
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
