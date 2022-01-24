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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.SqlException;
import io.questdb.std.CleanClosable;
import io.questdb.std.IntList;
import io.questdb.std.WeakAutoClosableObjectPool;

public abstract class AbstractTypeContainer<T extends CleanClosable> implements CleanClosable {
    protected final IntList types = new IntList();
    protected final WeakAutoClosableObjectPool<T> parentPool;

    public AbstractTypeContainer(WeakAutoClosableObjectPool<T> parentPool) {
        this.parentPool = parentPool;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void close() {
        getTypes().clear();
        this.parentPool.push((T) this);
    }

    public void defineBindVariables(BindVariableService bindVariableService) throws SqlException {
        for (int i = 0, n = types.size(); i < n; i++) {
            bindVariableService.define(i, types.getQuick(i), 0);
        }
    }

    public IntList getTypes() {
        return types;
    }

    protected void copyTypesFrom(BindVariableService bindVariableService) {
        final int variableCount = bindVariableService.getIndexedVariableCount();
        getTypes().ensureCapacity(variableCount);
        for (int i = 0; i < variableCount; i++) {
            final Function f = bindVariableService.getFunction(i);
            if (f == null) {
                // Intrinsic parser may take over the bind variable corresponding
                // to the key column and remove it from the filter. In this case,
                // the bind variable function will be defined later.
                getTypes().set(i, ColumnType.UNDEFINED);
            } else {
                getTypes().set(i, f.getType());
            }
        }
    }
}
