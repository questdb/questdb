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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.SqlException;
import io.questdb.std.AbstractSelfReturningObject;
import io.questdb.std.IntList;
import io.questdb.std.WeakSelfReturningObjectPool;

public abstract class AbstractTypeContainer<T extends AbstractTypeContainer<?>> extends AbstractSelfReturningObject<T> {
    private final IntList types = new IntList();
    private boolean closing;

    public AbstractTypeContainer(WeakSelfReturningObjectPool<T> parentPool) {
        super(parentPool);
    }

    @Override
    public void close() {
        if (!closing) {
            closing = true;
            super.close();
            types.clear();
            closing = false;
        }
    }

    public void defineBindVariables(BindVariableService bindVariableService) throws SqlException {
        defineBindVariables(types, bindVariableService);
    }

    static void copyTypes(BindVariableService fromBindVariableService, IntList toTypes) {
        for (int i = 0, n = fromBindVariableService.getIndexedVariableCount(); i < n; i++) {
            Function func = fromBindVariableService.getFunction(i);
            // For bind variable find in vararg parameters functions are not
            // created upfront. This is due to the type being unknown. On PG
            // wire bind variable type and value are provided *after* the compilation.
            if (func != null) {
                toTypes.add(func.getType());
            } else {
                toTypes.add(ColumnType.UNDEFINED);
            }
        }
    }

    static void defineBindVariables(IntList types, BindVariableService bindVariableService) throws SqlException {
        for (int i = 0, n = types.size(); i < n; i++) {
            bindVariableService.define(i, types.getQuick(i), 0);
        }
    }

    protected void copyTypesFrom(BindVariableService bindVariableService) {
        copyTypes(bindVariableService, types);
    }
}
