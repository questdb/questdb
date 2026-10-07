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
import io.questdb.cairo.ProjectableRecordCursorFactory;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.catalogue.AllTablesFunctionFactory;
import io.questdb.griffin.engine.functions.catalogue.ShowDateStyleCursorFactory;
import io.questdb.griffin.engine.functions.catalogue.ShowDefaultTransactionReadOnlyCursorFactory;
import io.questdb.griffin.engine.functions.catalogue.ShowMaxIdentifierLengthCursorFactory;
import io.questdb.griffin.engine.functions.catalogue.ShowParametersCursorFactory;
import io.questdb.griffin.engine.functions.catalogue.ShowSearchPathCursorFactory;
import io.questdb.griffin.engine.functions.catalogue.ShowServerVersionCursorFactory;
import io.questdb.griffin.engine.functions.catalogue.ShowServerVersionNumCursorFactory;
import io.questdb.griffin.engine.functions.catalogue.ShowStandardConformingStringsCursorFactory;
import io.questdb.griffin.engine.functions.catalogue.ShowTimeZoneFactory;
import io.questdb.griffin.engine.functions.catalogue.ShowTransactionIsolationLevelCursorFactory;
import io.questdb.griffin.engine.join.RecordAsAFieldRecordCursorFactory;
import io.questdb.griffin.engine.table.ShowColumnsRecordCursorFactory;
import io.questdb.griffin.engine.table.ShowPartitionsRecordCursorFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.plan.logical.FunctionSourcePlan;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.Path;

import java.io.Closeable;

/**
 * Owns prepared table-function roots independently of pooled logical descriptions.
 */
final class TableFunctionSources implements Closeable, Mutable {
    private final ObjList<ExpressionNode> expressions = new ObjList<>();
    private final FunctionParser parser;
    private final ObjectPool<FunctionSourcePlan> plans = new ObjectPool<>(FunctionSourcePlan.FACTORY, 4);
    private final ObjList<FunctionSourcePlan> prepared = new ObjList<>();
    private final ResourceScope resources = new ResourceScope();
    private final ObjList<QueryModel> showModels = new ObjList<>();
    private final IntList slots = new IntList();
    private SqlParserCallback callback;
    private Path path;

    TableFunctionSources(FunctionParser parser) {
        this.parser = parser;
    }

    @Override
    public void clear() {
        // Preparation slots cannot survive reset or refer to reused pooled nodes.
        for (int i = 0, n = prepared.size(); i < n; i++) {
            prepared.getQuick(i).clear();
        }
        prepared.clear();
        expressions.clear();
        showModels.clear();
        callback = null;
        slots.clear();
        plans.clear();
        resources.clear();
    }

    @Override
    public void close() {
        clear();
        path = Misc.free(path);
    }

    private static void describe(FunctionSourcePlan plan, RecordCursorFactory factory, int firstColumnId) {
        final RecordMetadata metadata = factory.getMetadata();
        final OutputSchema output = plan.getOutput();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (ColumnType.tagOf(metadata.getColumnType(i)) == ColumnType.RECORD) {
                throw new IllegalStateException("table function returned a RECORD column");
            }
            output.add(firstColumnId + i, metadata.getColumnName(i), metadata.getColumnType(i), true);
            output.setSymbolTableStatic(i, metadata.isSymbolTableStatic(i));
            plan.getSourceColumnIndexes().add(i);
        }
        output.setTimestampIndex(metadata.getTimestampIndex());
        plan.setSequenceStable(factory.isStableWithinExecution());
    }

    private static TableToken existingShowTable(QueryModel model, SqlExecutionContext executionContext, Path path) throws SqlException {
        final TableToken tableToken = executionContext.getTableTokenIfExists(model.getTableNameExpr().token);
        if (executionContext.getTableStatus(path, tableToken) != TableUtils.TABLE_EXISTS) {
            throw SqlException.tableDoesNotExist(model.getTableNameExpr().position, model.getTableNameExpr().token);
        }
        return tableToken;
    }

    private Path path() {
        if (path == null) {
            path = new Path(255, MemoryTag.NATIVE_SQL_COMPILER);
        }
        return path;
    }

    private RecordCursorFactory takeSourceFactory(FunctionSourcePlan plan, SqlExecutionContext executionContext) throws SqlException {
        final int index = prepared.indexOf(plan);
        if (index < 0) {
            throw new IllegalStateException("table-function source was not prepared");
        }
        final int slot = slots.getQuick(index);
        if (!resources.isOwned(slot)) {
            final QueryModel show = index < showModels.size() ? showModels.getQuick(index) : null;
            if (show != null) {
                return createShowFactory(show, executionContext, callback, path());
            }
            final Function function = parser.parseFunction(expressions.getQuick(index), AnyRecordMetadata.INSTANCE, executionContext);
            if (!(function instanceof CursorFunction)) {
                final SqlException exception = SqlException.$(plan.getPosition(), "function must return CURSOR");
                Misc.free(function, exception);
                throw exception;
            }
            return function.getRecordCursorFactory();
        }
        final Function function = (Function) resources.detach(slot);
        return function.getRecordCursorFactory();
    }

    static RecordCursorFactory createShowFactory(
            QueryModel model,
            SqlExecutionContext executionContext,
            SqlParserCallback sqlParserCallback,
            Path path
    ) throws SqlException {
        return switch (model.getShowKind()) {
            case QueryModel.SHOW_TABLES ->
                    new AllTablesFunctionFactory.AllTablesCursorFactory(executionContext.getCairoEngine().getConfiguration());
            case QueryModel.SHOW_COLUMNS -> new ShowColumnsRecordCursorFactory(
                    existingShowTable(model, executionContext, path), model.getTableNameExpr().position);
            case QueryModel.SHOW_PARTITIONS -> {
                final TableToken tableToken = existingShowTable(model, executionContext, path);
                final int timestampType;
                try (TableMetadata metadata = executionContext.getCairoEngine().getTableMetadata(tableToken)) {
                    timestampType = metadata.getTimestampType();
                }
                yield new ShowPartitionsRecordCursorFactory(tableToken, timestampType);
            }
            case QueryModel.SHOW_TRANSACTION, QueryModel.SHOW_TRANSACTION_ISOLATION_LEVEL ->
                    new ShowTransactionIsolationLevelCursorFactory();
            case QueryModel.SHOW_DEFAULT_TRANSACTION_READ_ONLY -> new ShowDefaultTransactionReadOnlyCursorFactory();
            case QueryModel.SHOW_MAX_IDENTIFIER_LENGTH -> new ShowMaxIdentifierLengthCursorFactory();
            case QueryModel.SHOW_STANDARD_CONFORMING_STRINGS -> new ShowStandardConformingStringsCursorFactory();
            case QueryModel.SHOW_SEARCH_PATH -> new ShowSearchPathCursorFactory();
            case QueryModel.SHOW_DATE_STYLE -> new ShowDateStyleCursorFactory();
            case QueryModel.SHOW_TIME_ZONE -> new ShowTimeZoneFactory();
            case QueryModel.SHOW_PARAMETERS -> new ShowParametersCursorFactory();
            case QueryModel.SHOW_SERVER_VERSION -> new ShowServerVersionCursorFactory();
            case QueryModel.SHOW_SERVER_VERSION_NUM -> new ShowServerVersionNumCursorFactory();
            case QueryModel.SHOW_CREATE_DATABASE ->
                    sqlParserCallback.generateShowCreateDatabaseFactory(model, executionContext, path);
            case QueryModel.SHOW_CREATE_TABLE ->
                    sqlParserCallback.generateShowCreateTableFactory(model, executionContext, path);
            case QueryModel.SHOW_CREATE_LIVE_VIEW ->
                    sqlParserCallback.generateShowCreateLiveViewFactory(model, executionContext, path);
            case QueryModel.SHOW_CREATE_MAT_VIEW ->
                    sqlParserCallback.generateShowCreateMatViewFactory(model, executionContext, path);
            case QueryModel.SHOW_CREATE_VIEW ->
                    sqlParserCallback.generateShowCreateViewFactory(model, executionContext, path);
            default -> sqlParserCallback.generateShowSqlFactory(model);
        };
    }

    FunctionSourcePlan bind(ExpressionNode expression, SqlExecutionContext executionContext, int firstColumnId) throws SqlException {
        assert expression.type == ExpressionNode.FUNCTION;
        final FunctionSourcePlan plan = plans.next().of(expression.position);
        final int slot = resources.reserve();
        prepared.add(plan);
        expressions.add(expression);
        slots.add(slot);
        try {
            final Function function = parser.parseFunction(expression, AnyRecordMetadata.INSTANCE, executionContext);
            resources.own(slot, function);
            if (!(function instanceof CursorFunction)) {
                throw SqlException.$(expression.position, "function must return CURSOR");
            }
            describe(plan, function.getRecordCursorFactory(), firstColumnId);
            plan.setProjectable(function.getRecordCursorFactory() instanceof ProjectableRecordCursorFactory);
            return plan;
        } catch (Throwable th) {
            resources.closeOwned(th);
            throw th;
        }
    }

    /**
     * Binds a SELECT-list cursor call as one RECORD column that carries each row of the call.
     */
    FunctionSourcePlan bindRecord(ExpressionNode expression, CharSequence name, SqlExecutionContext executionContext, int columnId) throws SqlException {
        assert expression.type == ExpressionNode.FUNCTION;
        final FunctionSourcePlan plan = plans.next().of(expression.position);
        final int slot = resources.reserve();
        prepared.add(plan);
        expressions.add(expression);
        slots.add(slot);
        try {
            final Function function = parser.parseFunction(expression, AnyRecordMetadata.INSTANCE, executionContext);
            resources.own(slot, function);
            if (!(function instanceof CursorFunction)) {
                throw SqlException.$(expression.position, "function must return CURSOR");
            }
            final RecordMetadata metadata = function.getRecordCursorFactory().getMetadata();
            plan.setSequenceStable(function.getRecordCursorFactory().isStableWithinExecution());
            final OutputSchema record = plan.getRecordSchema();
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                record.add(i, metadata.getColumnName(i), metadata.getColumnType(i), true);
            }
            plan.getOutput().add(columnId, name, ColumnType.RECORD, record, true);
            plan.getSourceColumnIndexes().add(0);
            plan.setRecordName(name);
            return plan;
        } catch (Throwable th) {
            resources.closeOwned(th);
            throw th;
        }
    }

    FunctionSourcePlan bindShow(QueryModel model, SqlExecutionContext executionContext, SqlParserCallback callback,
                                int firstColumnId) throws SqlException {
        final FunctionSourcePlan plan = plans.next().of(model.getModelPosition());
        final int slot = resources.reserve();
        prepared.add(plan);
        expressions.add(null);
        showModels.extendAndSet(prepared.size() - 1, model);
        slots.add(slot);
        this.callback = callback;
        try {
            resources.own(slot, new CursorFunction(createShowFactory(model, executionContext, callback, path())));
            parser.markCursorFunctionInstantiated();
            describe(plan, resources.function(slot).getRecordCursorFactory(), firstColumnId);
            return plan;
        } catch (Throwable th) {
            resources.closeOwned(th);
            throw th;
        }
    }

    Throwable closePrepared(Throwable primary) {
        return resources.closeOwned(primary);
    }

    /**
     * Copies a prepared source without its output, which the caller fills. The copy creates its own
     * function from the same call when generated.
     */
    FunctionSourcePlan copy(FunctionSourcePlan plan) {
        final int index = prepared.indexOf(plan);
        if (index < 0) {
            throw new IllegalStateException("table-function source was not prepared");
        }
        final FunctionSourcePlan copy = plans.next().of(plan.getPosition());
        copy.getRecordSchema().copyFrom(plan.getRecordSchema());
        copy.getSourceColumnIndexes().addAll(plan.getSourceColumnIndexes());
        copy.setProjectable(plan.isProjectable());
        copy.setSequenceStable(plan.isSequenceStable());
        copy.setRecordName(plan.getRecordName());
        prepared.add(copy);
        expressions.add(expressions.getQuick(index));
        if (index < showModels.size()) {
            showModels.extendAndSet(prepared.size() - 1, showModels.getQuick(index));
        }
        slots.add(resources.reserve());
        return copy;
    }

    RecordCursorFactory takeFactory(FunctionSourcePlan plan, SqlExecutionContext executionContext) throws SqlException {
        final RecordCursorFactory factory = takeSourceFactory(plan, executionContext);
        if (plan.getRecordName() == null) {
            return factory;
        }
        try {
            return new RecordAsAFieldRecordCursorFactory(factory, plan.getRecordName());
        } catch (Throwable th) {
            Misc.free(factory, th);
            throw th;
        }
    }
}
