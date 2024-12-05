/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.ops.CreateTableOperationBuilder;
import io.questdb.griffin.engine.table.ShowCreateTableRecordCursorFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.GenericLexer;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;

public interface SqlParserCallback {

    default RecordCursorFactory generateShowCreateTableFactory(QueryModel model, SqlExecutionContext executionContext, Path path) throws SqlException {
        TableToken tableToken = executionContext.getTableTokenIfExists(model.getTableNameExpr().token);
        if (executionContext.getTableStatus(path, tableToken) != TableUtils.TABLE_EXISTS) {
            throw SqlException.tableDoesNotExist(model.getTableNameExpr().position, model.getTableNameExpr().token);
        }
        return new ShowCreateTableRecordCursorFactory(tableToken, model.getTableNameExpr().position);
    }

    default RecordCursorFactory generateShowSqlFactory(QueryModel model) {
        assert false;
        return null;
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

    default int parseShowSql(
            GenericLexer lexer,
            QueryModel model,
            CharSequence tok,
            ObjectPool<ExpressionNode> expressionNodePool
    ) throws SqlException {
        return -1;
    }

}
