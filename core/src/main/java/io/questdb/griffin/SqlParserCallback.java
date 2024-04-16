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
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.model.CreateTableModel;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.GenericLexer;
import io.questdb.std.ObjectPool;
import org.jetbrains.annotations.Nullable;

public interface SqlParserCallback {

    default ExecutionModel createTableSuffix(
            GenericLexer lexer,
            SecurityContext securityContext,
            CreateTableModel model,
            @Nullable CharSequence tok
    ) throws SqlException {
        if (tok != null) {
            throw SqlException.unexpectedToken(lexer.lastTokenPosition(), tok);
        }
        return model;
    }

    default RecordCursorFactory generateShowSqlFactory(QueryModel model) throws SqlException {
        assert false;
        return null;
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
