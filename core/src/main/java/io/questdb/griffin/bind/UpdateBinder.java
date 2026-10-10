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
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

import static io.questdb.griffin.bind.BindContext.getColumnIndexQuiet;
import static io.questdb.griffin.bind.BindContext.sourceAlias;

/**
 * Binds an UPDATE: the target table scan, the FROM join and the WHERE filter through the shared
 * {@link SqlBinder} steps, then the SET assignments as a {@link ProjectPlan} whose columns carry the
 * target column names and types.
 */
final class UpdateBinder implements Mutable {
    private final SqlBinder binder;
    private final BindContext ctx;
    private final OrderBinder orderBinder;
    private final UpdateTarget target;
    private final WindowBinder windowBinder;
    private ScanPlan targetScan;

    UpdateBinder(BindContext ctx, SqlBinder binder, WindowBinder windowBinder, OrderBinder orderBinder, UpdateTarget target) {
        this.ctx = ctx;
        this.binder = binder;
        this.windowBinder = windowBinder;
        this.orderBinder = orderBinder;
        this.target = target;
    }

    @Override
    public void clear() {
        target.clear();
        targetScan = null;
    }

    private static boolean isAssignable(int type, int targetType) {
        return type == targetType
                || type == ColumnType.IPv4 && (targetType == ColumnType.STRING || targetType == ColumnType.VARCHAR)
                || targetType == ColumnType.IPv4 && (type == ColumnType.STRING || type == ColumnType.VARCHAR)
                || ColumnType.isTimestamp(targetType) && ColumnType.isConvertibleFrom(type, targetType)
                || ColumnType.isSymbolOrString(targetType) && ColumnType.isConvertibleFrom(type, ColumnType.STRING)
                || targetType == ColumnType.VARCHAR && ColumnType.isConvertibleFrom(type, ColumnType.VARCHAR);
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

    /**
     * Binds one SET assignment as a select column named after the target column; a plain source column of another
     * type binds again as a value, for the conversion.
     */
    private void bindAssignment(QueryColumn column, ProjectPlan project, OutputSchema output, QueryModel source, SqlExecutionContext executionContext) throws SqlException {
        final OutputSchema targetOutput = targetScan.getOutput();
        final int targetIndex = getColumnIndexQuiet(targetOutput, column.getName());
        final int targetType = targetIndex < 0 ? ColumnType.UNDEFINED : targetOutput.getColumnType(targetIndex);
        final int index = binder.bindSelectColumn(project, output, output, source, column, column.getAst(), -1, -1, null, null,
                targetIndex < 0 ? column.getName() : targetOutput.getColumnName(targetIndex), targetType, executionContext);
        if (index >= 0 && targetIndex >= 0 && targetType != output.getColumnType(index)) {
            project.getExpressions().setQuick(project.getExpressions().size() - 1,
                    ctx.functionBinder.bind(column.getAst(), output, sourceAlias(source), executionContext));
        }
    }

    private void prepareAssignments(QueryModel model, ProjectPlan project, SqlExecutionContext executionContext) throws SqlException {
        final IntList targetTypes = project.getUpdateTargetTypes();
        final OutputSchema output = project.getOutput();
        final OutputSchema targetOutput = targetScan.getOutput();
        final ObjList<QueryColumn> targets = model.getBottomUpColumns();
        final ObjList<CharSequence> targetNames = target.getColumnNames();
        targetNames.clear();
        boolean hasConversions = false;
        for (int i = 0, n = project.getExpressions().size(); i < n; i++) {
            final CharSequence target = output.getColumnName(i);
            final int targetPosition = i < targets.size() ? targets.getQuick(i).getAliasPosition() : 0;
            final int targetIndex = getColumnIndexQuiet(targetOutput, target);
            if (targetIndex < 0) {
                throw SqlException.invalidColumn(targetPosition, target);
            }
            if (targetIndex == targetOutput.getTimestampIndex()) {
                throw SqlException.$(targetPosition, "Designated timestamp column cannot be updated");
            }
            targetNames.add(targetOutput.getColumnName(targetIndex));
            final int targetType = targetOutput.getColumnType(targetIndex);
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
            return;
        }
        for (int i = 0, n = targetTypes.size(); i < n; i++) {
            final int type = output.getColumnType(i);
            final int targetType = targetTypes.getQuick(i);
            if (!isAssignable(type, targetType)) {
                throw SqlException.inconvertibleTypes(targets.getQuick(i).getAst().position, type, "", targetType, output.getColumnName(i));
            }
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
        final LogicalPlan sourcePlan = binder.bindBlockSource(model, source, where, executionContext);
        final OutputSchema output = sourcePlan.getOutput();
        scope.resetAliases();
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
        binder.clearCursorColumns();
        for (int i = 0, n = model.getBottomUpColumns().size(); i < n; i++) {
            bindAssignment(model.getBottomUpColumns().getQuick(i), project, output, source, executionContext);
        }
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
            target.of(tableName.token, tableName.position, metadata.getTableToken(), metadata.getTableId(), metadata.getMetadataVersion());
            targetScan = scan;
            return scan;
        } catch (CairoException e) {
            if (e.isOutOfMemory() || e.isTableDoesNotExist()) {
                throw e;
            }
            throw SqlException.position(tableName.position).put(e);
        }
    }
}
