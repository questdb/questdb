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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

import static io.questdb.griffin.bind.BindContext.sourceAlias;

/**
 * Binds an UPDATE: the target table scan, the FROM join and the WHERE filter through the shared
 * {@link SqlBinder} steps, then the SET assignments as a {@link ProjectPlan} whose columns carry the
 * target column names and types.
 */
public final class UpdateBinder implements Mutable {
    private final SqlBinder binder;
    private final BindContext ctx;
    private final JoinBinder joinBinder;
    private final OrderBinder orderBinder;
    private final ObjList<CharSequence> tableColumnNames = new ObjList<>();
    private final IntList tableColumnTypes = new IntList();
    private final ObjList<CharSequence> targetNames = new ObjList<>();
    private final WindowBinder windowBinder;
    private long metadataVersion;
    private int tableId;
    private CharSequence tableName;
    private int tablePosition;
    private TableToken tableToken;
    private int timestampIndex = -1;

    UpdateBinder(BindContext ctx, SqlBinder binder, WindowBinder windowBinder, JoinBinder joinBinder, OrderBinder orderBinder) {
        this.ctx = ctx;
        this.binder = binder;
        this.windowBinder = windowBinder;
        this.joinBinder = joinBinder;
        this.orderBinder = orderBinder;
    }

    @Override
    public void clear() {
        tableColumnNames.clear();
        tableColumnTypes.clear();
        targetNames.clear();
        metadataVersion = 0;
        tableId = 0;
        tableName = null;
        tablePosition = 0;
        tableToken = null;
        timestampIndex = -1;
    }

    public long getMetadataVersion() {
        return metadataVersion;
    }

    public ObjList<CharSequence> getTableColumnNames() {
        return tableColumnNames;
    }

    public IntList getTableColumnTypes() {
        return tableColumnTypes;
    }

    public int getTableId() {
        return tableId;
    }

    public CharSequence getTableName() {
        return tableName;
    }

    public int getTablePosition() {
        return tablePosition;
    }

    public TableToken getTableToken() {
        return tableToken;
    }

    public ObjList<CharSequence> getTargetNames() {
        return targetNames;
    }

    private SqlException aggregateException(QueryModel model) {
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            final ExpressionNode value = model.getBottomUpColumns().getQuick(i).getAst();
            if (value.type == ExpressionNode.FUNCTION && ctx.functionFactoryCache.isGroupBy(value.token)) {
                return SqlException.$(value.position, "Unsupported function in SET clause");
            }
        }
        return SqlException.$(model.getModelPosition(), "Unsupported SQL complexity for the UPDATE statement");
    }

    private void bindAssignment(
            QueryColumn column, ProjectPlan project, OutputSchema output, QueryModel source, SqlExecutionContext executionContext
    ) throws SqlException {
        final BindScope scope = ctx.scope();
        final ExpressionNode expression = column.getAst();
        final CharSequence alias = sourceAlias(source);
        if (expression.type != ExpressionNode.LITERAL || ctx.functionBinder.isOuterColumn(expression, output, alias)) {
            final int targetIndex = getColumnIndex(column.getName());
            final BoundExpression bound = targetIndex >= 0
                    ? ctx.functionBinder.bindUpdateAssignment(expression, output, alias, tableColumnTypes.getQuick(targetIndex), executionContext)
                    : ctx.functionBinder.bind(expression, output, alias, ColumnType.STRING, executionContext);
            ctx.addProjection(project, bound, null, targetIndex >= 0 ? tableColumnNames.getQuick(targetIndex) : column.getName(), true);
            scope.projectionAliasIndexes.add(project.getExpressions().size() - 1);
            return;
        }
        final int index = ctx.bindColumnIndex(expression, output, source);
        CharSequence name = column.getAlias() != null ? column.getAlias() : output.getColumnName(index);
        final int targetIndex = getColumnIndex(name);
        if (targetIndex >= 0) {
            name = tableColumnNames.getQuick(targetIndex);
        }
        final int dot = Chars.indexOfLastUnquoted(expression.token, '.');
        final boolean isSourceAliasReusable = dot < 0 || Chars.equalsIgnoreCase(name, expression.token, dot + 1, expression.token.length());
        final boolean isTranslatingCopy = !isSourceAliasReusable && source.getJoinModels().size() == 1
                && scope.sourceProjectionIndexes.getQuick(index) < 0 && SqlBinder.isReferenced(project, output.getColumnId(index));
        ctx.addProjection(project, output, index, name, expression.position, isSourceAliasReusable);
        if (isTranslatingCopy) {
            scope.translatingCopyIds.add(project.getOutput().getColumnId(project.getOutput().getColumnCount() - 1));
        }
        if (targetIndex >= 0 && tableColumnTypes.getQuick(targetIndex) != output.getColumnType(index)) {
            project.getExpressions().setQuick(project.getExpressions().size() - 1, ctx.functionBinder.bind(expression, output, alias, executionContext));
        }
    }

    private int getColumnIndex(CharSequence name) {
        for (int i = 0, n = tableColumnNames.size(); i < n; i++) {
            if (Chars.equalsIgnoreCase(tableColumnNames.getQuick(i), name)
                    || SqlUtil.isQuoteProtectedAlias(name)
                    && Chars.equalsIgnoreCase(tableColumnNames.getQuick(i), name, 1, name.length() - 1)) {
                return i;
            }
        }
        return -1;
    }

    private void prepareAssignments(QueryModel model, ProjectPlan project, SqlExecutionContext executionContext) throws SqlException {
        final IntList targetTypes = project.getUpdateTargetTypes();
        final OutputSchema output = project.getOutput();
        final ObjList<QueryColumn> targets = model.getBottomUpColumns();
        targetNames.clear();
        boolean hasConversions = false;
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            final CharSequence target = output.getColumnName(i);
            final int targetPosition = i < targets.size() ? targets.getQuick(i).getAliasPosition() : 0;
            final int targetIndex = getColumnIndex(target);
            if (targetIndex < 0) {
                throw SqlException.invalidColumn(targetPosition, target);
            }
            if (targetIndex == timestampIndex) {
                throw SqlException.$(targetPosition, "Designated timestamp column cannot be updated");
            }
            final CharSequence name = tableColumnNames.getQuick(targetIndex);
            for (int k = 0, m = targetNames.size(); k < m; k++) {
                if (Chars.equalsIgnoreCase(targetNames.getQuick(k), name)) {
                    throw SqlException.$(targetPosition, "Duplicate column ").put(target).put(" in SET clause");
                }
            }
            targetNames.add(name);
            final int targetType = tableColumnTypes.getQuick(targetIndex);
            targetTypes.add(targetType);
            final BoundExpression expression = project.getExpressions().getQuick(i);
            if (targetType >= 0 && expression.getDataType() != targetType) {
                final Function function = ctx.functionBinder.prepareUpdateAssignment(expression, targetType, executionContext);
                output.setColumnType(i, LogicalPlans.updateColumnType(function.getType(), targetType));
                output.setSymbolTableStatic(i, function instanceof SymbolFunction symbol && symbol.isSymbolTableStatic());
                hasConversions = true;
            }
        }
        if (!hasConversions) {
            targetTypes.clear();
        }
    }

    private SqlException windowException(QueryModel model) {
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
                final int position = expression.windowExpression != null ? expression.position : windowBinder.findWindowPosition(expression, false, true);
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
        return aggregateException(model);
    }

    /**
     * Binds the select model of an UPDATE, whose nested model is the target table with the FROM join and
     * WHERE clause, into the projection of the assigned values.
     */
    LogicalPlan bind(QueryModel model, SqlExecutionContext executionContext) throws SqlException {
        final BindScope scope = ctx.scope();
        final QueryModel source = model.getNestedModel();
        SqlBinder.linkWindowExpressions(model);
        binder.validateBlockWindows(model, source);
        final ExpressionNode where = binder.copyWhereClause(source);
        final boolean hasJoin = source.getJoinModels().size() > 1;
        final LogicalPlan sourcePlan = hasJoin ? joinBinder.bindJoins(source, where, executionContext) : binder.bindSource(source, executionContext);
        final OutputSchema output = sourcePlan.getOutput();
        ctx.promoteNoArgFunctions(model, output, hasJoin ? null : sourceAlias(source));
        scope.aliases.clear();
        scope.aliasSequences.clear();
        scope.projectionAliasIndexes.clear();
        final LogicalPlan input = binder.bindWhere(where, sourcePlan, null, source, executionContext);
        windowBinder.validateWindowOrder(model, source);
        if (windowBinder.hasWindows(model, source)) {
            throw windowException(model);
        }
        if (!windowBinder.isAggregationFree(model, source)) {
            windowBinder.validateWindowAggregation(model, source);
        }
        if (ctx.hasAggregation(model, source)) {
            throw aggregateException(model);
        }
        final ProjectPlan project = ctx.planNodes.projects.next().of(input, model.getModelPosition());
        scope.sourceProjectionIndexes.setAll(output.getColumnCount(), -1);
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            bindAssignment(model.getBottomUpColumns().getQuick(i), project, output, source, executionContext);
        }
        binder.clearCursorColumns();
        prepareAssignments(model, project, executionContext);
        return orderBinder.designateTimestamp(project);
    }

    /**
     * Binds the UPDATE target table, opened for write, and records its identity and columns.
     */
    ScanPlan bindTarget(QueryModel source, ExpressionNode tableName, TableToken token, SqlExecutionContext executionContext) throws SqlException {
        if (token.isView()) {
            throw SqlException.position(tableName.position).put("cannot modify ").put(token.getType().keyword())
                    .put(" [view=").put(token.getTableName()).put(']');
        }
        try (TableRecordMetadata metadata = executionContext.getMetadataForWrite(token, source.getMetadataVersion())) {
            final ScanPlan scan = binder.bindScan(source, tableName, metadata);
            if (!executionContext.isWalApplication() && executionContext.getCairoEngine().isWalTable(scan.getTableToken())) {
                scan.markWalClientUpdate();
            } else {
                ctx.planTables.acquire(metadata.getTableToken(), metadata.getMetadataVersion(), tableName.position, executionContext);
            }
            this.tableName = tableName.token;
            tablePosition = tableName.position;
            tableToken = metadata.getTableToken();
            tableId = metadata.getTableId();
            metadataVersion = metadata.getMetadataVersion();
            timestampIndex = -1;
            tableColumnNames.clear();
            tableColumnTypes.clear();
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                final int type = metadata.getColumnType(i);
                if (type > 0) {
                    if (i == metadata.getTimestampIndex()) {
                        timestampIndex = tableColumnNames.size();
                    }
                    tableColumnTypes.add(type);
                    tableColumnNames.add(metadata.getColumnName(i));
                }
            }
            return scan;
        } catch (CairoException e) {
            if (e.isOutOfMemory() || e.isTableDoesNotExist()) {
                throw e;
            }
            throw SqlException.position(tableName.position).put(e);
        }
    }
}
