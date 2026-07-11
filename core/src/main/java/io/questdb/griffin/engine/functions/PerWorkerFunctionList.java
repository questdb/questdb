/*+*****************************************************************************
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

package io.questdb.griffin.engine.functions;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

import java.util.BitSet;

/**
 * An aligned per-worker function list that shares thread-safe owner functions and owns only the
 * marked worker-local clones. Callers use the list normally during execution and use the static
 * lifecycle helpers to avoid initializing, clearing, or closing shared owner references.
 */
public final class PerWorkerFunctionList<T extends Function> extends ObjList<T> {
    private final BitSet ownedFunctions = new BitSet();

    public PerWorkerFunctionList(int capacity) {
        super(capacity);
    }

    public void add(T function, boolean isOwned) {
        final int index = size();
        add(function);
        if (isOwned) {
            ownedFunctions.set(index);
        }
    }

    public static void clear(ObjList<? extends Function> functions) {
        if (functions instanceof PerWorkerFunctionList<?> perWorkerFunctions) {
            for (int i = perWorkerFunctions.ownedFunctions.nextSetBit(0); i > -1; i = perWorkerFunctions.ownedFunctions.nextSetBit(i + 1)) {
                Misc.clear(perWorkerFunctions.getQuick(i));
            }
        } else {
            Misc.clearObjList(functions);
        }
    }

    public static void close(ObjList<? extends Function> functions) {
        if (functions instanceof PerWorkerFunctionList<?> perWorkerFunctions) {
            for (int i = perWorkerFunctions.ownedFunctions.nextSetBit(0); i > -1; i = perWorkerFunctions.ownedFunctions.nextSetBit(i + 1)) {
                Misc.free(perWorkerFunctions.getQuick(i));
                perWorkerFunctions.setQuick(i, null);
            }
        } else {
            Misc.freeObjList(functions);
        }
    }

    public static void init(
            ObjList<? extends Function> functions,
            @Nullable ObjList<? extends Function> ownerFunctions,
            SymbolTableSource symbolTableSource,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (functions instanceof PerWorkerFunctionList<?> perWorkerFunctions) {
            for (int i = perWorkerFunctions.ownedFunctions.nextSetBit(0); i > -1; i = perWorkerFunctions.ownedFunctions.nextSetBit(i + 1)) {
                final Function function = perWorkerFunctions.getQuick(i);
                if (ownerFunctions != null) {
                    ownerFunctions.getQuick(i).offerStateTo(function);
                }
                function.init(symbolTableSource, executionContext);
            }
        } else {
            if (ownerFunctions != null) {
                for (int i = 0, n = functions.size(); i < n; i++) {
                    ownerFunctions.getQuick(i).offerStateTo(functions.getQuick(i));
                }
            }
            Function.init(functions, symbolTableSource, executionContext, null);
        }
    }

    public static boolean isOwned(ObjList<? extends Function> functions, int index) {
        return !(functions instanceof PerWorkerFunctionList<?> perWorkerFunctions) || perWorkerFunctions.ownedFunctions.get(index);
    }

}
