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
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.engine.window.LiveViewCheckpointFunctionCompiler;
import io.questdb.griffin.engine.window.LiveViewWindowDescription;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Chars;
import io.questdb.std.GenericLexer;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;


import static io.questdb.griffin.BindContext.blockGroupBy;
import static io.questdb.griffin.BindContext.getColumnIndexQuiet;
import static io.questdb.griffin.BindContext.isWildcard;
import static io.questdb.griffin.BindContext.sourceAlias;

final class WindowBinder implements Mutable {
    private static final int MAX_WINDOW_NESTING_DEPTH = 8;
    final ObjList<CharSequence> windowNames = new ObjList<>();
    private final AggregateBinder aggregateBinder;
    private final SqlBinder binder;
    private final BindContext ctx;
    private final BytecodeAssembler windowAssembler;
    private final EntityColumnFilter windowColumnFilter;
    private final IntList windowColumnIds = new IntList();
    private final IntList windowGroupMembers = new IntList();
    private final IntHashSet windowInheritancePositions = new IntHashSet();
    private final IntList windowLevels = new IntList();
    private final ObjList<ExpressionNode> windowNodes = new ObjList<>();
    private final LowerCaseCharSequenceIntHashMap windowSelfReferences = new LowerCaseCharSequenceIntHashMap();
    private final LowerCaseCharSequenceHashSet windowTranslatedNames;
    private final LowerCaseCharSequenceIntHashMap windowTranslatedSequences;
    private LogicalPlan windowInput;
    private int windowLevelCount;

    WindowBinder(
            BindContext ctx,
            SqlBinder binder,
            AggregateBinder aggregateBinder,
            BytecodeAssembler windowAssembler,
            EntityColumnFilter windowColumnFilter
    ) {
        this.ctx = ctx;
        this.binder = binder;
        this.aggregateBinder = aggregateBinder;
        this.windowAssembler = windowAssembler;
        this.windowColumnFilter = windowColumnFilter;
        this.windowTranslatedNames = ctx.aliases;
        this.windowTranslatedSequences = ctx.aliasSequences;
    }

    @Override
    public void clear() {
        windowInput = null;
        windowSelfReferences.clear();
        windowColumnIds.clear();
        windowInheritancePositions.clear();
        windowGroupMembers.clear();
        windowLevels.clear();
        windowNames.clear();
        windowNodes.clear();
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
                    if (ctx.isWildcardColumn(ast, output, k, sourceAlias(source))) {
                        addWindowInnerColumn(project, output, k);
                    }
                }
            } else if (ast.type == ExpressionNode.LITERAL) {
                addWindowInnerColumn(project, output, ctx.bindColumnIndex(ast, output, source));
            } else if (isPureComputedColumn(ast)) {
                final BoundExpression bound = ctx.functionBinder.bind(ast, ctx.windowBindingScope(output), sourceAlias(source),
                        ColumnType.STRING, executionContext);
                final int id = ctx.nextColumnId++;
                project.getExpressions().add(bound);
                project.getOutput().add(id, ctx.createOutputName(columns.getQuick(i).getName()), bound.getDataType(), false);
                ctx.windowAliasIds.setQuick(i, id);
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

    private void closeWindowGroup(int mark) {
        if (windowGroupMembers.size() > mark) {
            windowLevelCount++;
            for (int i = mark, n = windowGroupMembers.size(); i < n; i++) {
                windowLevels.setQuick(windowGroupMembers.getQuick(i), windowLevelCount);
            }
            windowGroupMembers.setPos(mark);
        }
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

    private int findWindowNode(ExpressionNode expression) {
        for (int i = 0, n = windowNodes.size(); i < n; i++) {
            if (ExpressionNode.compareNodesExact(windowNodes.getQuick(i), expression)) {
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
            windowGroupMembers.removeIndex(windowGroupMembers.indexOf(index, 0, windowGroupMembers.size()));
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
        for (int i = 0, n = ctx.windowSelectExpressions.size(); i < n; i++) {
            nameWindows(ctx.windowSelectExpressions.getQuick(i), model);
        }
        for (int i = 0, n = ctx.windowOrderExpressions.size(); i < n; i++) {
            nameWindows(ctx.windowOrderExpressions.getQuick(i), model);
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
                if (ctx.windowAliasIds.getQuick(i) >= 0 && selected.type == ExpressionNode.LITERAL && Chars.equalsIgnoreCase(selected.token, expression.token)) {
                    return windowColumnReference(ctx.windowAliasIds.getQuick(i), expression.position);
                }
            }
            for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
                final QueryColumn column = model.getBottomUpColumns().getQuick(i);
                if (!Chars.equalsIgnoreCase(column.getName(), expression.token)) {
                    continue;
                }
                int id = ctx.windowAliasIds.getQuick(i);
                if (id == -2) {
                    throw SqlException.invalidColumn(expression.position, expression.token);
                }
                if (id < 0) {
                    ctx.windowAliasIds.setQuick(i, -2);
                    final ExpressionNode aliased = rewriteWindowAliases(ExpressionNode.deepClone(ctx.bindingExpressions, column.getAst()),
                            model, source, true, executionContext);
                    ctx.windowAliasIds.setQuick(i, -1);
                    if (findWindowPosition(aliased, false, false) >= 0 || ctx.hasAggregate(aliased)) {
                        return aliased;
                    }
                    final BoundExpression bound = ctx.functionBinder.bind(aliased, ctx.windowBindingScope(windowInput.getOutput()), sourceAlias(source), ColumnType.STRING, executionContext);
                    id = bound instanceof ColumnExpression ref ? ref.getColumnId()
                            : appendWindowInputExpression(bound, column.getName(), aliased.position);
                    ctx.windowAliasIds.setQuick(i, id);
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
            SqlUtil.normalizeWindowFrame(syntax, ctx.functionParser, executionContext);
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

    /** Names a window column after the SELECT alias it implements, or after its function. */
    private CharSequence windowOutputName(ExpressionNode node, QueryModel model) {
        for (int i = 0, n = ctx.windowSelectExpressions.size(); i < n; i++) {
            if (ctx.windowSelectExpressions.getQuick(i) == node) {
                final CharSequence alias = model.getBottomUpColumns().getQuick(i).getName();
                return Chars.indexOf(alias, '.') < 0 ? alias : node.token;
            }
        }
        return node.token;
    }

    /**
     * A correlated LATEST ON ranks rows per key and outer row, since the native factory
     * requires a table scan that the correlation replaces with a join.
     */
    LogicalPlan bindLatestWindow(LogicalPlan input, LatestByPlan latest, QueryModel model, int position,
                                         SqlExecutionContext executionContext) throws SqlException {
        final OutputSchema output = input.getOutput();
        final WindowSpec spec = ctx.windowSpecs.next().of(ctx.unboundedWindow);
        for (int i = 0, n = latest.getKeyColumnIds().size(); i < n; i++) {
            final int columnId = latest.getKeyColumnIds().getQuick(i);
            spec.getPartitionBy().add(ctx.columns.next().of(columnId, output.getColumnType(output.getColumnIndexById(columnId)), position));
        }
        final int timestampId = latest.getTimestampColumnId();
        spec.getOrderByColumnIds().add(timestampId);
        spec.getOrderByDirections().add(QueryModel.ORDER_DIRECTION_DESCENDING);
        spec.getOrderByPositions().add(position);
        spec.getOrderByNames().add(output.getColumnName(output.getColumnIndexById(timestampId)));
        final ExpressionNode rowNumberCall = ctx.bindingExpressions.next().of(ExpressionNode.FUNCTION, "row_number", 0, position);
        final FunctionExpression rowNumber = bindWindowFunction(rowNumberCall, spec, output, model.getNestedModel(), executionContext);
        final WindowPlan window = ctx.windowPlans.next().of(input, position);
        window.getOutput().copyFrom(output);
        final int rowNumberId = ctx.nextColumnId++;
        window.getFunctions().add(rowNumber);
        window.getSpecs().add(spec);
        window.getFunctionColumnIds().add(rowNumberId);
        window.getOutput().add(rowNumberId, "_latest_rn", rowNumber.getDataType(), false);
        final ExpressionNode rowNumberRef = ctx.bindingExpressions.next().of(ExpressionNode.LITERAL, "_latest_rn", 0, position);
        final ExpressionNode predicate = ctx.bindingExpressions.next().of(ExpressionNode.OPERATION, "=", 0, position);
        predicate.paramCount = 2;
        predicate.lhs = rowNumberRef;
        predicate.rhs = ctx.bindingExpressions.next().of(ExpressionNode.CONSTANT, "1", 0, position);
        ctx.substitutionNodes.clear();
        ctx.substitutionColumns.clear();
        ctx.substitutionNodes.add(rowNumberRef);
        ctx.substitutionColumns.add(ctx.columns.next().of(rowNumberId, rowNumber.getDataType(), position));
        final BoundExpression bound = ctx.functionBinder.bind(predicate, window.getOutput(), null, ctx.substitutionNodes, ctx.substitutionColumns, executionContext);
        final FilterPlan filter = ctx.filters.next().of(window, bound, position);
        filter.getOutput().copyFrom(window.getOutput());
        ctx.stopTimestampIntrinsics(filter.getOutput());
        return filter;
    }

    FunctionExpression bindWindowFunction(ExpressionNode expression, WindowSpec spec, OutputSchema input,
                                                  QueryModel source, SqlExecutionContext executionContext) throws SqlException {
        ObjList<Function> partitionFunctions = null;
        try {
            final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            final int partitionCount = spec.getPartitionBy().size();
            if (partitionCount > 0) {
                partitionFunctions = new ObjList<>(partitionCount);
                for (int i = 0; i < partitionCount; i++) {
                    final Function function = ctx.functionBinder.instantiate(spec.getPartitionBy().getQuick(i), input, executionContext);
                    partitionFunctions.add(function);
                    keyTypes.add(function.getType());
                }
            }
            final VirtualRecord partitionRecord = partitionFunctions == null ? null : new VirtualRecord(partitionFunctions);
            windowColumnFilter.of(partitionCount);
            final RecordSink partitionSink = partitionFunctions == null ? null
                    : RecordSinkFactory.getInstance(ctx.configuration, windowAssembler, keyTypes, windowColumnFilter);
            final int orderCount = spec.getOrderByColumnIds().size();
            final boolean isTimestampOrdered = orderCount == 1
                    && spec.getOrderByColumnIds().getQuick(0) == input.getTimestampColumnId();
            final int direction = isTimestampOrdered
                    ? spec.getOrderByDirections().getQuick(0) == QueryModel.ORDER_DIRECTION_ASCENDING ? RecordCursorFactory.SCAN_DIRECTION_FORWARD : RecordCursorFactory.SCAN_DIRECTION_BACKWARD
                    : RecordCursorFactory.SCAN_DIRECTION_OTHER;
            final int timestampIndex = input.getTimestampIndex();
            executionContext.configureWindowContext(partitionRecord, partitionSink, keyTypes, orderCount > 0,
                    direction, orderCount > 0 ? spec.getOrderByPositions().getQuick(0) : -1, false,
                    spec.getFramingMode(), spec.getRowsLo(), spec.getRowsLoExprTimeUnit(), spec.getRowsLoExprPos(), spec.getRowsLoKindPos(),
                    spec.getRowsHi(), spec.getRowsHiExprTimeUnit(), spec.getRowsHiExprPos(), spec.getRowsHiKindPos(),
                    spec.getExclusionKind(), spec.getExclusionKindPos(), timestampIndex,
                    timestampIndex < 0 ? ColumnType.UNDEFINED : input.getColumnType(timestampIndex), spec.isIgnoreNulls(), spec.getNullsDescPos());
            final FunctionExpression function = ctx.functionBinder.bindWindow(expression, ctx.windowBindingScope(input, true), sourceAlias(source), executionContext);
            partitionFunctions = null;
            return function;
        } catch (Throwable th) {
            Misc.freeObjList(partitionFunctions, th);
            throw th;
        } finally {
            executionContext.clearWindowContext();
        }
    }

    LogicalPlan bindWindows(QueryModel model, QueryModel source, LogicalPlan input, SqlExecutionContext executionContext) throws SqlException {
        windowInput = input;
        windowNodes.clear();
        ctx.windowOrderExpressions.clear();
        windowLevels.clear();
        windowColumnIds.clear();
        ctx.windowSelectExpressions.clear();
        ctx.windowAliasIds.setAll(model.getBottomUpColumns().size(), -1);
        windowSelfReferences.clear();
        ctx.aliases.clear();
        ctx.aliasSequences.clear();
        ProjectPlan innerProject = null;
        if (hasPureComputedColumn(model) && model.getNamedWindows().size() == 0
                && ctx.lateralScopes.size() == 0 && !aggregateBinder.hasAggregation(model, source)
                && !binder.hasProjectionReferences(model, input.getOutput())) {
            innerProject = bindWindowInnerProjection(model, source, input, executionContext);
            windowInput = innerProject;
            ctx.aliases.clear();
            ctx.aliasSequences.clear();
        }
        for (int i = 0, n = windowInput.getOutput().getColumnCount(); i < n; i++) {
            ctx.aliases.add(windowInput.getOutput().getColumnName(i));
        }
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            final ExpressionNode ast = ExpressionNode.deepClone(ctx.bindingExpressions, model.getBottomUpColumns().getQuick(i).getAst());
            if (ast.windowExpression != null && ast.type == ExpressionNode.FUNCTION && SqlKeywords.isCountKeyword(ast.token)
                    && ast.paramCount == 1 && ast.rhs.type == ExpressionNode.CONSTANT && !SqlKeywords.isNullKeyword(ast.rhs.token)) {
                ast.rhs = null;
                ast.paramCount = 0;
            }
            final ExpressionNode expression = ctx.windowAliasIds.getQuick(i) >= 0 ? ast
                    : rewriteWindowAliases(ast, model, source, false, executionContext);
            ctx.windowSelectExpressions.add(expression);
            collectWindowNodes(expression);
        }
        for (int i = 0, n = source.getOrderBy().size(); i < n; i++) {
            final ExpressionNode expression = rewriteOrderWindowAliases(
                    ExpressionNode.deepClone(ctx.bindingExpressions, source.getOrderBy().getQuick(i)), model, source, executionContext);
            ctx.windowOrderExpressions.add(expression);
            collectWindowNodes(expression);
        }
        final int levels = nameWindowNodes(model);
        WindowPlan bottomWindow = null;
        windowColumnIds.setAll(windowNodes.size(), -1);
        for (int level = 1; level <= levels; level++) {
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
                final OutputSchema scope = windowInput.getOutput();
                for (int k = 0, count = scope.getCorrelatedAliasCount(); k < count; k++) {
                    final int index = scope.getCorrelatedAliasIndex(k);
                    spec.getPartitionBy().add(ctx.columns.next().of(scope.getColumnId(index), scope.getColumnType(index), node.position));
                }
                for (int k = 0, count = syntax.getPartitionBy().size(); k < count; k++) {
                    final ExpressionNode partition = replaceWindowReferences(syntax.getPartitionBy().getQuick(k), true);
                    if (LateralBinder.isCorrelatedReference(partition, scope)) {
                        continue;
                    }
                    if (ctx.isAggregate(partition)) {
                        throw SqlException.$(node.position, "aggregate functions in partition by are not supported");
                    }
                    final int aggregatePosition = aggregateBinder.findAggregatePosition(partition);
                    if (aggregatePosition >= 0) {
                        throw SqlException.$(aggregatePosition, "Aggregate function cannot be passed as an argument");
                    }
                    spec.getPartitionBy().add(ctx.functionBinder.bind(partition, ctx.windowBindingScope(windowInput.getOutput(), true), sourceAlias(source), executionContext));
                }
                for (int k = 0, count = syntax.getOrderBy().size(); k < count; k++) {
                    final ExpressionNode order = replaceWindowReferences(syntax.getOrderBy().getQuick(k), true);
                    if (ctx.hasAggregate(order)) {
                        throw SqlException.invalidColumn(order.position, order.token);
                    }
                    final BoundExpression bound = ctx.functionBinder.bind(order, ctx.windowBindingScope(windowInput.getOutput(), true), sourceAlias(source), executionContext);
                    final int id = bound instanceof ColumnExpression column ? column.getColumnId()
                            : appendWindowInputExpression(bound, "__window_order", order.position);
                    final int index = windowInput.getOutput().getColumnIndexById(id);
                    spec.getOrderByColumnIds().add(id);
                    spec.getOrderByDirections().add(syntax.getOrderByDirection().getQuick(k));
                    spec.getOrderByPositions().add(order.position);
                    spec.getOrderByNames().add(windowInput.getOutput().getColumnName(index));
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
                if (spec.getLiveViewDescription() != null) {
                    final OutputSchema scope = windowInput.getOutput();
                    LiveViewCheckpointFunctionCompiler.validateRange(spec.getLiveViewDescription(), node.token,
                            spec.getOrderByColumnIds().size() == 1 && scope.getTimestampIndex() >= 0
                                    && spec.getOrderByColumnIds().getQuick(0) == scope.getTimestampColumnId()
                                    && spec.getOrderByDirections().getQuick(0) == QueryModel.ORDER_DIRECTION_ASCENDING);
                }
                final FunctionExpression function = bindWindowFunction(replaceWindowReferences(node, false), spec,
                        windowInput.getOutput(), source, executionContext);
                final int id = ctx.nextColumnId++;
                window.getFunctions().add(function);
                window.getSpecs().add(spec);
                window.getFunctionColumnIds().add(id);
                window.getOutput().add(id, windowNames.getQuick(i), function.getDataType(), false);
                windowColumnIds.setQuick(i, id);
            }
            windowInput = window;
        }
        for (int i = 0, n = ctx.windowSelectExpressions.size(); i < n; i++) {
            final ExpressionNode expression = ctx.windowSelectExpressions.getQuick(i);
            ctx.windowSelectExpressions.setQuick(i, ctx.windowAliasIds.getQuick(i) >= 0
                    ? windowColumnReference(ctx.windowAliasIds.getQuick(i), expression.position)
                    : replaceWindowReferences(expression, true));
        }
        for (int i = 0, n = ctx.windowOrderExpressions.size(); i < n; i++) {
            ctx.windowOrderExpressions.setQuick(i, replaceWindowReferences(ctx.windowOrderExpressions.getQuick(i), true));
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

    boolean isPureComputedColumn(ExpressionNode ast) {
        return ast.type != ExpressionNode.LITERAL && findWindowPosition(ast, true, false) < 0 && !ctx.hasAggregate(ast) && !ctx.isCursorCall(ast);
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
        return aggregateBinder.updateAggregateException(model);
    }

    void validateNamedWindows(QueryModel model) throws SqlException {
        final ObjList<CharSequence> names = model.getNamedWindows().keys();
        for (int i = 0, n = names.size(); i < n; i++) {
            windowInheritancePositions.clear();
            resolveWindow(model.getNamedWindows().get(names.getQuick(i)), model);
        }
    }

    void validateWindowAggregation(QueryModel model, QueryModel source, boolean isOrderEnabled) throws SqlException {
        if (isAggregationFree(model, source) && !(model.isDistinct() && aggregateBinder.isDistinctRewritten(model, source, isOrderEnabled))) {
            return;
        }
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
