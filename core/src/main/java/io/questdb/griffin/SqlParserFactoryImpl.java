/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.ObjectPool;

public class SqlParserFactoryImpl implements SqlParserFactory {
    public static final SqlParserFactory INSTANCE = new SqlParserFactoryImpl();

    @Override
    public SqlParser getInstance(
            CairoConfiguration configuration,
            SqlOptimiser optimiser,
            CharacterStore characterStore,
            ObjectPool<ExpressionNode> expressionNodePool,
            ObjectPool<QueryColumn> queryColumnPool,
            ObjectPool<QueryModel> queryModelPool,
            PostOrderTreeTraversalAlgo traversalAlgo
    ) {
        return new SqlParser(
                configuration,
                optimiser,
                characterStore,
                expressionNodePool,
                queryColumnPool,
                queryModelPool,
                traversalAlgo
        );
    }
}
