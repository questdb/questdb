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

package io.questdb.cutlass.pgwire;

import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;

/**
 * Unlike other TypesAnd* classes, this one doesn't self-return to a pool. That's because
 * it's used for multithreaded calls to {@link io.questdb.std.ConcurrentAssociativeCache}.
 */
public class TypesAndSelect implements QuietCloseable {
    private final IntList types = new IntList();
    private RecordCursorFactory factory;

    public TypesAndSelect(RecordCursorFactory factory) {
        this.factory = factory;
    }

    @Override
    public void close() {
        factory = Misc.free(factory);
    }

    public void copyTypesFrom(BindVariableService bindVariableService) {
        AbstractTypeContainer.copyTypes(bindVariableService, types);
    }

    public void defineBindVariables(BindVariableService bindVariableService) throws SqlException {
        AbstractTypeContainer.defineBindVariables(types, bindVariableService);
    }

    public RecordCursorFactory getFactory() {
        return factory;
    }
}
