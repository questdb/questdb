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

package io.questdb.griffin.engine.ops;

import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ExecutionModel;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.str.Sinkable;

public interface CreateTableOperationBuilder extends ExecutionModel, Sinkable {
    int COLUMN_FLAG_CACHED = 1;
    int COLUMN_FLAG_INDEXED = COLUMN_FLAG_CACHED << 1;
    int COLUMN_FLAG_DEDUP_KEY = COLUMN_FLAG_INDEXED << 1;

    CreateTableOperation build(
            SqlCompiler sqlCompiler,
            SqlExecutionContext executionContext,
            CharSequence sqlText
    ) throws SqlException;

    @Override
    default int getModelType() {
        return CREATE_TABLE;
    }

    void setSelectModel(QueryModel selectModel);
}
