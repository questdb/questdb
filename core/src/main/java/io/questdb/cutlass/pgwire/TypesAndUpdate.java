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

package io.questdb.cutlass.pgwire;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.CompiledQueryImpl;
import io.questdb.griffin.UpdateStatement;
import io.questdb.std.WeakAutoClosableObjectPool;

public class TypesAndUpdate extends AbstractTypeContainer<TypesAndUpdate> {
    CompiledQueryImpl compiledQuery;

    public TypesAndUpdate(WeakAutoClosableObjectPool<TypesAndUpdate> parentPool, CairoEngine engine) {
        super(parentPool);
        compiledQuery = new CompiledQueryImpl(engine);
    }

    public CompiledQuery getCompiledQuery() {
        return compiledQuery;
    }

    public void of(UpdateStatement updateStatement, BindVariableService bindVariableService) {
        // Compiled query from SqlCompiler cannot be used
        // to store compiled statements because the instance re-used for every new compilation
        this.compiledQuery.ofUpdate(updateStatement);
        copyTypesFrom(bindVariableService);
    }
}
