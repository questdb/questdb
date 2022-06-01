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

package io.questdb.cairo;

import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class SymbolMapDiff {
    public static final int END_OF_SYMBOL_DIFFS = -1;

    private final IntList keys = new IntList();
    private final ObjList<CharSequence> symbols = new ObjList<>();

    SymbolMapDiff() {
    }

    void add(CharSequence symbol, int key) {
        keys.add(key);
        symbols.add(symbol);
    }

    public int size() {
        assert keys.size() == symbols.size();
        return keys.size();
    }

    public int getKey(int index) {
        assert index > -1 && index < keys.size();
        return keys.get(index);
    }

    public CharSequence getSymbol(int index) {
        assert index > -1 && index < symbols.size();
        return symbols.get(index);
    }
}
