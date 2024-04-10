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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface StealableFilterRecordCursorFactory {

    // to be used in combination with compiled filter
    @Nullable
    default ObjList<Function> getBindVarFunctions() {
        return null;
    }

    // to be used in combination with compiled filter
    @Nullable
    default MemoryCARW getBindVarMemory() {
        return null;
    }

    @Nullable
    default CompiledFilter getCompiledFilter() {
        return null;
    }

    @NotNull
    Function getFilter();

    /**
     * Closes everything but base factory and filter.
     */
    void halfClose();

    /**
     * Returns true if the factory stands for nothing more but a filter, so that
     * the above factory (e.g. a parallel GROUP BY one) can steal the filter.
     */
    boolean supportsFilterStealing();
}
