/*******************************************************************************
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

import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.ops.CreateMatViewOperationBuilder;
import io.questdb.griffin.engine.ops.CreateTableOperationBuilder;
import io.questdb.griffin.engine.ops.CreateTableOperationBuilderImpl;
import io.questdb.griffin.engine.ops.CreateViewOperationBuilder;
import io.questdb.griffin.engine.table.ShowCreateMatViewRecordCursorFactory;
import io.questdb.griffin.engine.table.ShowCreateTableRecordCursorFactory;
import io.questdb.griffin.engine.table.ShowCreateViewRecordCursorFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.GenericLexer;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.griffin.SqlKeywords.isTtlKeyword;

public interface SqlParserCallback {

    static @NotNull TableToken getMatViewToken(ExpressionNode tableNameExpr, SqlExecutionContext executionContext, Path path) throws SqlException {
        final TableToken viewToken = getTableToken(tableNameExpr, executionContext, path,
                SqlException.matViewDoesNotExist(tableNameExpr.position, tableNameExpr.token)
        );
        if (!viewToken.isMatView()) {
            throw SqlException.$(tableNameExpr.position, "materialized view name expected, got table name");
        }
        return viewToken;
    }

    static TableToken getTableToken(ExpressionNode tableNameExpr, SqlExecutionContext executionContext, Path path) throws SqlException {
        return getTableToken(tableNameExpr, executionContext, path,
                SqlException.tableDoesNotExist(tableNameExpr.position, tableNameExpr.token)
        );
    }

    static @NotNull TableToken getViewToken(ExpressionNode tableNameExpr, SqlExecutionContext executionContext, Path path) throws SqlException {
        final TableToken viewToken = getTableToken(tableNameExpr, executionContext, path,
                SqlException.viewDoesNotExist(tableNameExpr.position, tableNameExpr.token)
        );
        if (!viewToken.isView()) {
            throw SqlException.$(tableNameExpr.position, "view name expected, got table name");
        }
        return viewToken;
    }

    default RecordCursorFactory generateShowCreateMatViewFactory(QueryModel model, SqlExecutionContext executionContext, Path path) throws SqlException {
        final TableToken viewToken = getMatViewToken(model.getTableNameExpr(), executionContext, path);
        return new ShowCreateMatViewRecordCursorFactory(viewToken, model.getTableNameExpr().position);
    }

    default RecordCursorFactory generateShowCreateTableFactory(QueryModel model, SqlExecutionContext executionContext, Path path) throws SqlException {
        final TableToken tableToken = getTableToken(model.getTableNameExpr(), executionContext, path);
        return new ShowCreateTableRecordCursorFactory(tableToken, model.getTableNameExpr().position);
    }

    default RecordCursorFactory generateShowCreateViewFactory(QueryModel model, SqlExecutionContext executionContext, Path path) throws SqlException {
        final TableToken viewToken = getViewToken(model.getTableNameExpr(), executionContext, path);
        return new ShowCreateViewRecordCursorFactory(viewToken, model.getTableNameExpr().position);
    }

    default RecordCursorFactory generateShowSqlFactory(QueryModel model) {
        assert false;
        return null;
    }

    default CreateMatViewOperationBuilder parseCreateMatViewExt(
            GenericLexer lexer,
            SecurityContext securityContext,
            CreateMatViewOperationBuilder builder,
            @Nullable CharSequence tok
    ) throws SqlException {
        if (tok != null) {
            throw SqlException.unexpectedToken(lexer.lastTokenPosition(), tok);
        }
        return builder;
    }

    default CreateTableOperationBuilder parseCreateTableExt(
            GenericLexer lexer,
            SecurityContext securityContext,
            CreateTableOperationBuilder builder,
            @Nullable CharSequence tok
    ) throws SqlException {
        if (tok != null) {
            throw SqlException.unexpectedToken(lexer.lastTokenPosition(), tok);
        }
        return builder;
    }

    default CreateViewOperationBuilder parseCreateViewExt(
            GenericLexer lexer,
            SecurityContext securityContext,
            CreateViewOperationBuilder builder,
            @Nullable CharSequence tok
    ) throws SqlException {
        if (tok != null) {
            throw SqlException.unexpectedToken(lexer.lastTokenPosition(), tok);
        }
        return builder;
    }

    default int parseShowSql(
            GenericLexer lexer,
            QueryModel model,
            CharSequence tok,
            ObjectPool<ExpressionNode> expressionNodePool
    ) throws SqlException {
        return -1;
    }

    default CharSequence parseTtlSettings(
            GenericLexer lexer,
            CharSequence tok,
            int partitionBy,
            CreateTableOperationBuilderImpl builder,
            boolean isMatView
    ) throws SqlException {
        if (tok != null && isTtlKeyword(tok)) {
            final int ttlValuePos = lexer.getPosition();
            final int ttlHoursOrMonths = SqlParser.parseTtlHoursOrMonths(lexer);
            if (partitionBy != -1) {
                PartitionBy.validateTtlGranularity(partitionBy, ttlHoursOrMonths, ttlValuePos);
            }
            builder.setTtlHoursOrMonths(ttlHoursOrMonths);
            builder.setTtlPosition(ttlValuePos);
            tok = SqlUtil.fetchNext(lexer);
        }
        return tok;
    }

    private static TableToken getTableToken(ExpressionNode tableNameExpr, SqlExecutionContext executionContext, Path path, SqlException notExistsError) throws SqlException {
        final TableToken tableToken = executionContext.getTableTokenIfExists(tableNameExpr.token);
        if (executionContext.getTableStatus(path, tableToken) != TableUtils.TABLE_EXISTS) {
            throw notExistsError;
        }
        return tableToken;
    }
}
