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

package org.questdb;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.SymbolMapReaderImpl;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

final class SymbolMapCacheBenchmarkData {
    final int[] keys;
    final ObjList<String> values;

    SymbolMapCacheBenchmarkData(int count) {
        if (count < 2 || count > 65536 || Integer.bitCount(count) != 1) {
            throw new IllegalArgumentException("symbol count must be a power of two in [2, 65536]");
        }
        keys = new int[count];
        values = new ObjList<>(count);
        values.setPos(count);
        for (int i = 0; i < count; i++) {
            keys[i] = i;
        }
        Rnd rnd = new Rnd();
        for (int i = count - 1; i > 0; i--) {
            int j = rnd.nextInt(i + 1);
            int key = keys[i];
            keys[i] = keys[j];
            keys[j] = key;
        }
        for (int i = 0; i < count; i++) {
            int next = keys[(i + 1) & (count - 1)];
            // Two Latin-1 characters preserve compact Strings while encoding every key.
            values.setQuick(keys[i], "symbol" + (char) (next >>> 8) + (char) (next & 255));
        }
        boolean[] visited = new boolean[count];
        int key = keys[0];
        for (int i = 0; i < count; i++) {
            if (visited[key]) {
                throw new AssertionError("dependency chain repeats before visiting every key");
            }
            visited[key] = true;
            key = nextKey(values.getQuick(key));
        }
        if (key != keys[0]) {
            throw new AssertionError("dependency chain is not a cycle");
        }
    }

    static int cacheSize(SymbolMapReaderImpl reader) throws Exception {
        int size = reader.getCacheSize();
        try {
            size += (int) reader.getClass().getMethod("getLowCacheSize").invoke(reader);
        } catch (NoSuchMethodException ignored) {
            // The baseline has only the heap cache.
        }
        return size;
    }

    static boolean setBudget(CairoEngine engine, long limit) throws Exception {
        java.lang.reflect.Method accessor;
        try {
            accessor = engine.getClass().getMethod("getSymbolMapCachePool");
        } catch (NoSuchMethodException ignored) {
            return false;
        }
        Object pool = accessor.invoke(engine);
        Object budget = pool.getClass().getMethod("getBudget").invoke(pool);
        budget.getClass().getMethod("setLimit", long.class).invoke(budget, limit);
        return true;
    }

    static int nextKey(CharSequence value) {
        int length = value.length();
        return (value.charAt(length - 2) << 8) | value.charAt(length - 1);
    }
}
