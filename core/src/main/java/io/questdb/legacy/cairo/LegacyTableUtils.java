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


package io.questdb.legacy.cairo;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.view.ViewDefinition;
import io.questdb.griffin.AnyRecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.legacy.griffin.FunctionParser;
import io.questdb.legacy.griffin.model.ExpressionNode;
import io.questdb.legacy.griffin.model.IQueryModel;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;

/**
 * Master's methods of {@link io.questdb.cairo.TableUtils} that read the legacy query model.
 */
public final class LegacyTableUtils {

    private LegacyTableUtils() {
    }

    public static Function createCursorFunction(
            FunctionParser functionParser,
            @NotNull IQueryModel model,
            @NotNull SqlExecutionContext executionContext
    ) throws SqlException {
        final ExpressionNode tableNameExpr = model.getTableNameExpr();
        final SqlExecutionContext.TableFunctionView previousView = executionContext.getTableFunctionView();
        final ExpressionNode viewNameExpr = model.getViewNameExpr();
        if (viewNameExpr != null) {
            executionContext.setTableFunctionView(getTableFunctionView(viewNameExpr, executionContext));
        }
        final Function function;
        try {
            function = functionParser.parseFunction(tableNameExpr, AnyRecordMetadata.INSTANCE, executionContext);
        } finally {
            if (viewNameExpr != null) {
                executionContext.setTableFunctionView(previousView);
            }
        }
        if (!ColumnType.isCursor(function.getType())) {
            Misc.free(function);
            throw SqlException.$(tableNameExpr.position, "function must return CURSOR");
        }
        return function;
    }

    public static SqlExecutionContext.TableFunctionView getTableFunctionView(
            @NotNull ExpressionNode viewNameExpr,
            @NotNull SqlExecutionContext executionContext
    ) throws SqlException {
        if (viewNameExpr.tableFunctionView != null) {
            return viewNameExpr.tableFunctionView;
        }
        final TableToken viewToken = executionContext.getTableTokenIfExists(viewNameExpr.token);
        if (viewToken == null || !viewToken.isView()) {
            throw SqlException.viewDoesNotExist(viewNameExpr.position, viewNameExpr.token);
        }
        final ViewDefinition viewDefinition = executionContext.getCairoEngine().getViewGraph().getViewDefinition(viewToken);
        if (viewDefinition == null) {
            throw SqlException.viewDoesNotExist(viewNameExpr.position, viewNameExpr.token);
        }
        return new SqlExecutionContext.TableFunctionView(viewDefinition);
    }
}
