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

package io.questdb.griffin.engine.functions.regex;

import io.questdb.std.BitSet;
import io.questdb.std.IntList;

public final class SymbolKeySet {
    private static final long MAX_SPAN_PER_KEY = 32L;
    private final BitSet bits = new BitSet(64);
    private final IntList keys = new IntList();
    private int base;
    private int denseSpan;
    private boolean isDense;

    public void add(int key) {
        assert key >= 0 && (keys.size() == 0 || key > keys.getQuick(keys.size() - 1));
        isDense = false;
        keys.add(key);
    }

    public void build() {
        isDense = false;
        int size = keys.size();
        if (size > 0) {
            int min = keys.getQuick(0);
            long span = keys.getQuick(size - 1) - (long) min + 1;
            if (span <= MAX_SPAN_PER_KEY * size) {
                bits.clearBelow(denseSpan);
                base = min;
                for (int i = 0; i < size; i++) {
                    bits.set(keys.getQuick(i) - base);
                }
                denseSpan = (int) span;
                isDense = true;
            }
        }
    }

    public void clear() {
        isDense = false;
        keys.clear();
    }

    public boolean contains(int key) {
        if (key < 0) {
            return false;
        }
        return isDense ? key >= base && bits.get(key - base) : keys.binarySearchUniqueList(key) >= 0;
    }

    public void copyFrom(SymbolKeySet source) {
        if (source == this) {
            return;
        }
        keys.clear();
        keys.addAll(source.keys);
        isDense = source.isDense;
        if (isDense) {
            bits.clearBelow(denseSpan);
            bits.copyBelow(source.bits, source.denseSpan);
            base = source.base;
            denseSpan = source.denseSpan;
        }
    }

    public IntList getKeys() {
        return keys;
    }
}
