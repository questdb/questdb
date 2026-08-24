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

package io.questdb.cairo.lv;

import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.ObjList;

import java.util.Arrays;

/**
 * Operation-scoped high-water pool for exact-width frozen key and scalar
 * images. Each width retains its simultaneous-use high-water count, independent
 * of the order in which later freezes encounter widths.
 */
final class LiveViewCheckpointByteArrayPool {
    private final IntObjHashMap<WidthPool> poolsByWidth = new IntObjHashMap<>();
    private int epoch;

    byte[] copy(MemoryR source, long offset, int length) {
        final byte[] out = next(length);
        for (int i = 0; i < length; i++) {
            out[i] = source.getByte(offset + i);
        }
        return out;
    }

    byte[] copy(byte[] source) {
        final byte[] out = next(source.length);
        System.arraycopy(source, 0, out, 0, source.length);
        return out;
    }

    byte[] next(int length) {
        WidthPool pool = poolsByWidth.get(length);
        if (pool == null) {
            pool = new WidthPool();
            poolsByWidth.put(length, pool);
        }
        if (pool.epoch != epoch) {
            pool.cursor = 0;
            pool.epoch = epoch;
        }
        if (pool.cursor == pool.arrays.size()) {
            final byte[] value = new byte[length];
            pool.arrays.add(value);
            pool.cursor++;
            return value;
        }
        final byte[] value = pool.arrays.getQuick(pool.cursor++);
        Arrays.fill(value, (byte) 0);
        return value;
    }

    void reset() {
        epoch++;
        if (epoch == 0) {
            epoch = 1;
            final Object[] widthPools = poolsByWidth.getValues();
            for (int i = 0, n = widthPools.length; i < n; i++) {
                final WidthPool pool = (WidthPool) widthPools[i];
                if (pool != null) {
                    pool.cursor = 0;
                    pool.epoch = epoch;
                }
            }
        }
    }

    private static final class WidthPool {
        private final ObjList<byte[]> arrays = new ObjList<>();
        private int cursor;
        private int epoch;
    }
}
