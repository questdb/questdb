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

package io.questdb.griffin.model;

import io.questdb.std.Mutable;
import io.questdb.std.ObjectFactory;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;

public class CompileViewModel implements ExecutionModel, Mutable, Sinkable {
    public static final ObjectFactory<CompileViewModel> FACTORY = CompileViewModel::new;
    private QueryModel queryModel;
    private ExpressionNode viewExpr;

    @Override
    public void clear() {
        viewExpr = null;
        queryModel = null;
    }

    @Override
    public int getModelType() {
        return ExecutionModel.COMPILE_VIEW;
    }

    @Override
    public QueryModel getQueryModel() {
        return queryModel;
    }

    @Override
    public CharSequence getTableName() {
        return viewExpr.token;
    }

    @Override
    public ExpressionNode getTableNameExpr() {
        return viewExpr;
    }

    public void setQueryModel(QueryModel queryModel) {
        this.queryModel = queryModel;
    }

    public void setTableNameExpr(ExpressionNode expr) {
        viewExpr = expr;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("compile view ");
        sink.put(viewExpr.token);
    }
}
