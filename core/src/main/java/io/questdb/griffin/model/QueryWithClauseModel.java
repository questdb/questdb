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

public class QueryWithClauseModel implements Mutable {
    private final LowerCaseCharSequenceObjHashMap<WithClauseModel> withClauses = new LowerCaseCharSequenceObjHashMap<>();

    public void addWithClause(CharSequence name, WithClauseModel model) {
        withClauses.put(name, model);
    }

    public void addWithClauses(QueryWithClauseModel parentWithClauses) {
        withClauses.putAll(parentWithClauses.withClauses);
    }

    @Override
    public void clear() {
        withClauses.clear();
    }

    public WithClauseModel getWithClause(CharSequence name) {
        return withClauses.get(name);
    }
}
