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

package io.questdb.cairo.sql;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.SqlException;

/**
 * Weakly dimensional array function. Should be extended by any array function with the return type
 * dependent on an array argument, potentially a bind variable. Such functions will get their type
 * defined on query execution, once we get the bind variable values.
 */
public abstract class WeakDimsArrayFunction extends ArrayFunction {
    // May be set in case of bind var args + INSERT/UPDATE.
    protected int assignedType = ColumnType.UNDEFINED;
    protected int position;
    protected int type = ColumnType.UNDEFINED;

    /**
     * If a type was assigned to the function, it means that it has a bind variable argument
     * and, thus, a weak dimensional type, and it's used in an INSERT or UPDATE. In this case,
     * we have to double-check that the assigned (wanted) type matches the type derived from
     * the bind variable values.
     */
    @Override
    public void assignType(int type, BindVariableService bindVariableService) throws SqlException {
        this.assignedType = type;
    }

    @Override
    public int getType() {
        return assignedType != ColumnType.UNDEFINED ? assignedType : type;
    }

    /**
     * Compares the assigned (wanted) type with the actual type derived from the arguments.
     * Must be called in init() if the type was updated.
     */
    protected void validateAssignedType() throws SqlException {
        if (assignedType != ColumnType.UNDEFINED && assignedType != type) {
            throw SqlException.inconvertibleTypes(position, type, assignedType);
        }
    }
}
