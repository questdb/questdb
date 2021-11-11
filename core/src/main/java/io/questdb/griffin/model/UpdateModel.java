/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.Sinkable;
import io.questdb.std.str.CharSink;

public class UpdateModel implements Mutable, ExecutionModel, QueryWithClauseModel, Sinkable {
    @Override
    public void addWithClause(CharSequence token, WithClauseModel wcm) {

    }

    @Override
    public WithClauseModel getWithClause(CharSequence token) {
        return null;
    }

    @Override
    public LowerCaseCharSequenceObjHashMap<WithClauseModel> getWithClauses() {
        return null;
    }

    public void addWithClauses(LowerCaseCharSequenceObjHashMap<WithClauseModel> withClauses) {

    }

    @Override
    public void clear() {

    }

    @Override
    public int getModelType() {
        return UPDATE;
    }

    public void setAlias(ExpressionNode updateTableAlias) {

    }

    public void setModelPosition(int position) {

    }

    public void setFromModel(QueryModel nestedModel) {
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put("update");
    }

    public void withSet(CharSequence col, ExpressionNode expr) {

    }

    public void withTableName(CharSequence tableName) {

    }
}
