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

package io.questdb.cutlass.qwp.codec;

import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;

import java.util.Arrays;

/**
 * One SYMBOL column's native symbol key -> connection dictionary id map, for the lifetime of a
 * query. Symbol keys are dense, {@code 0 .. symbolCount - 1}, so a key below
 * {@link #getDenseMaxKeys()} is looked up in a plain array, sized on demand to the largest key
 * stored; any other key (a key past the cap, or a negative key other than NULL, which no caller
 * passes) goes to a hash map. The map only caches the ids {@link QwpEgressConnSymbolDict} assigns,
 * so the lookup structure has no effect on which id a key gets or when a key is first seen: the
 * connection dictionary's order and the batches' dictionary-budget splits are the same with any
 * cap, 0 included (hash map only).
 */
final class QwpSymbolKeyMap {
    static final int NO_ID = -1;
    private static final int[] EMPTY_DENSE = new int[0];
    private static final int MIN_DENSE_CAPACITY = 64;
    private final IntIntHashMap overflow = new IntIntHashMap();
    // keys stored in dense, so that clear() costs the keys the query used, not the array
    private final IntList storedDenseKeys = new IntList();
    // dense[key] is the key's id, or NO_ID; keys at or past dense.length have no id unless they are
    // at or past denseMaxKeys, where overflow holds them
    int[] dense = EMPTY_DENSE;
    private int denseMaxKeys;

    QwpSymbolKeyMap(int denseMaxKeys) {
        this.denseMaxKeys = Math.max(denseMaxKeys, 0);
    }

    void clear() {
        final int[] d = dense;
        for (int i = 0, n = storedDenseKeys.size(); i < n; i++) {
            d[storedDenseKeys.getQuick(i)] = NO_ID;
        }
        storedDenseKeys.clear();
        overflow.clear();
    }

    /**
     * @return the key's connection id, or {@link #NO_ID} when it has none yet
     */
    int get(int key) {
        final int[] d = dense;
        if (key >= 0 && key < d.length) {
            return d[key];
        }
        return getSlow(key);
    }

    int getDenseMaxKeys() {
        return denseMaxKeys;
    }

    /**
     * The id of a key that {@link #get} did not find in {@link #dense}: in the overflow map for a key
     * past the cap or negative, else none.
     */
    int getSlow(int key) {
        if (key >= 0 && key < denseMaxKeys) {
            return NO_ID;
        }
        final int index = overflow.keyIndex(key);
        return index < 0 ? overflow.valueAt(index) : NO_ID;
    }

    /**
     * Stores the id of a key that has none.
     */
    void put(int key, int connId) {
        assert connId >= 0;
        if (key >= 0 && key < denseMaxKeys) {
            int[] d = dense;
            if (key >= d.length) {
                d = grow(key);
            }
            assert d[key] == NO_ID;
            d[key] = connId;
            storedDenseKeys.add(key);
        } else {
            overflow.put(key, connId);
        }
    }

    /**
     * Changes the cap. Only between queries: the map must be empty.
     */
    void setDenseMaxKeys(int denseMaxKeys) {
        assert storedDenseKeys.size() == 0 && overflow.size() == 0;
        this.denseMaxKeys = Math.max(denseMaxKeys, 0);
        if (dense.length > this.denseMaxKeys) {
            dense = EMPTY_DENSE;
        }
    }

    private int[] grow(int key) {
        final int[] d = dense;
        final long wanted = Math.max(Numbers.ceilPow2((long) key + 1), MIN_DENSE_CAPACITY);
        final int capacity = (int) Math.min(wanted, denseMaxKeys);
        final int[] grown = Arrays.copyOf(d, capacity);
        Arrays.fill(grown, d.length, capacity, NO_ID);
        dense = grown;
        return grown;
    }
}
