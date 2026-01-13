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

package io.questdb.griffin.engine.ops;

import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.Chars;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

public class CreateViewOperationBuilderImpl implements CreateViewOperationBuilder, Mutable {
    private final CreateTableOperationBuilderImpl createTableOperationBuilder = new CreateTableOperationBuilderImpl();
    private final LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> dependencies = new LowerCaseCharSequenceObjHashMap<>();

    @Override
    public CreateViewOperation build(SqlCompiler compiler, SqlExecutionContext sqlExecutionContext, CharSequence sqlText) throws SqlException {
        final CreateTableOperationImpl createTableOperation = createTableOperationBuilder.build(compiler, sqlExecutionContext, sqlText);
        return new CreateViewOperationImpl(
                Chars.toString(sqlText),
                createTableOperation,
                dependencies
        );
    }

    @Override
    public void clear() {
        createTableOperationBuilder.clear();
        dependencies.clear();
    }

    public CreateTableOperationBuilderImpl getCreateTableOperationBuilder() {
        return createTableOperationBuilder;
    }

    @Override
    public LowerCaseCharSequenceObjHashMap<LowerCaseCharSequenceHashSet> getDependencies() {
        return dependencies;
    }

    @Override
    public QueryModel getQueryModel() {
        return createTableOperationBuilder.getQueryModel();
    }

    @Override
    public CharSequence getTableName() {
        return createTableOperationBuilder.getTableName();
    }

    @Override
    public ExpressionNode getTableNameExpr() {
        return createTableOperationBuilder.getTableNameExpr();
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("create view ");
        sink.put(createTableOperationBuilder.getTableName());
        sink.putAscii(" as (");
        if (createTableOperationBuilder.getQueryModel() != null) {
            createTableOperationBuilder.getQueryModel().toSink(sink);
        }
        sink.putAscii(')');
    }
}
