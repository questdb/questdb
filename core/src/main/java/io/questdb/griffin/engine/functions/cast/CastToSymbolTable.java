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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.ObjList;

/**
 * The key-to-value view a cast-to-symbol function hands out from {@code newSymbolTable()}, so a
 * worker thread can resolve keys the function has already assigned without touching the function.
 * It copies the values at construction and never mutates them again, which is what makes it safe
 * to read from another thread.
 * <p>
 * It is deliberately not a {@link io.questdb.cairo.sql.Function}. Every cast-to-symbol function
 * used to answer {@code newSymbolTable()} with a clone of itself, and that clone held - and on
 * close released - the very argument the live function was still reading. A caller that frees what
 * {@code newSymbolTable()} handed it therefore closed the projection's own argument: with a
 * resource-owning argument such as {@code json_extract}, every later read of the column came back
 * NULL. Handing out something that owns nothing removes that hazard rather than documenting it.
 * <p>
 * The view is a snapshot. A key the function assigns after the hand-out is not in it, which is the
 * same limit the clone carried.
 */
public final class CastToSymbolTable implements SymbolTable {
    private final ObjList<CharSequence> symbols = new ObjList<>();

    public CastToSymbolTable(ObjList<? extends CharSequence> symbols) {
        for (int i = 0, n = symbols.size(); i < n; i++) {
            this.symbols.add(symbols.getQuick(i));
        }
    }

    @Override
    public CharSequence valueBOf(int key) {
        return valueOf(key);
    }

    @Override
    public CharSequence valueOf(int symbolKey) {
        return symbols.getQuick(TableUtils.toIndexKey(symbolKey));
    }
}
