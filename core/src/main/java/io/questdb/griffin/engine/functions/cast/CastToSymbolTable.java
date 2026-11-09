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
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectString;
import org.jetbrains.annotations.Nullable;

/**
 * The key-to-value view a cast-to-symbol function hands out from {@code newSymbolTable()}.
 * <p>
 * It is deliberately not a {@link io.questdb.cairo.sql.Function}. Every cast-to-symbol function
 * used to answer {@code newSymbolTable()} with a clone of itself, and that clone held - and on
 * close released - the very argument the live function was still reading. A caller that frees what
 * {@code newSymbolTable()} handed it therefore closed the projection's own argument: with a
 * resource-owning argument such as {@code json_extract}, every later read of the column came back
 * NULL. Handing out something that owns nothing removes that hazard rather than documenting it.
 * <p>
 * The keys the function has assigned by the hand-out are copied to the heap, so the view answers
 * for them even after the function has released its native dictionary - when its cursor closes,
 * or after the function itself is closed - and from any thread. A key the function assigns after
 * the hand-out resolves through its live dictionary instead, into this view's own A/B flyweights,
 * the same way the view of {@link CastStrToSymbolFunctionFactory.Func} does. That serves a
 * consumer such as lag(), which takes the view before the scan mints any key. Like that view, it is
 * for serial reads only, and once the dictionary is released it answers null for such a key rather
 * than reading freed memory.
 */
public final class CastToSymbolTable implements SymbolTable {
    @Nullable
    private final CastToSymbolDictionary dictionary;
    private final ObjList<CharSequence> symbols = new ObjList<>();
    @Nullable
    private final DirectString viewA;
    @Nullable
    private final DirectString viewB;

    public CastToSymbolTable(ObjList<? extends CharSequence> symbols) {
        for (int i = 0, n = symbols.size(); i < n; i++) {
            this.symbols.add(symbols.getQuick(i));
        }
        this.dictionary = null;
        this.viewA = null;
        this.viewB = null;
    }

    CastToSymbolTable(CastToSymbolDictionary dictionary) {
        this.dictionary = dictionary;
        this.viewA = new DirectString();
        this.viewB = new DirectString();
        symbols.add(null);
        for (int key = 0, n = dictionary.size(); key < n; key++) {
            symbols.add(Chars.toString(dictionary.valueOf(key, viewA)));
        }
        viewA.clear();
    }

    @Override
    public CharSequence valueBOf(int key) {
        final int index = TableUtils.toIndexKey(key);
        if (index < symbols.size()) {
            return symbols.getQuick(index);
        }
        return dictionary != null ? dictionary.valueOf(key, viewB) : null;
    }

    @Override
    public CharSequence valueOf(int symbolKey) {
        final int index = TableUtils.toIndexKey(symbolKey);
        if (index < symbols.size()) {
            return symbols.getQuick(index);
        }
        return dictionary != null ? dictionary.valueOf(symbolKey, viewA) : null;
    }
}
