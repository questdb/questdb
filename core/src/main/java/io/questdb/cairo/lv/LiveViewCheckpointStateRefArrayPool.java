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

import io.questdb.std.IntObjHashMap;
import io.questdb.std.ObjList;

/**
 * Epoch-scoped high-water pool of exact-width state-reference arrays. One decode
 * of a leaf page needs one array per entry, and the widths repeat across entries,
 * so each width retains its own simultaneous-use high-water count independently
 * of the order the entries arrive in.
 * <p>
 * A borrowed array - and every reference inside it - stays valid until the next
 * {@link #reset()}. Callers that must outlive that copy the values out.
 */
final class LiveViewCheckpointStateRefArrayPool {

    static final LiveViewCheckpointStatePageRef[] EMPTY = new LiveViewCheckpointStatePageRef[0];
    private final IntObjHashMap<WidthPool> poolsByWidth = new IntObjHashMap<>();
    private int epoch;

    LiveViewCheckpointStatePageRef[] next(int width) {
        if (width == 0) {
            return EMPTY;
        }
        WidthPool pool = poolsByWidth.get(width);
        if (pool == null) {
            pool = new WidthPool();
            poolsByWidth.put(width, pool);
        }
        if (pool.epoch != epoch) {
            pool.cursor = 0;
            pool.epoch = epoch;
        }
        if (pool.cursor == pool.arrays.size()) {
            final LiveViewCheckpointStatePageRef[] value = new LiveViewCheckpointStatePageRef[width];
            for (int i = 0; i < width; i++) {
                value[i] = new LiveViewCheckpointStatePageRef();
            }
            pool.arrays.add(value);
            pool.cursor++;
            return value;
        }
        return pool.arrays.getQuick(pool.cursor++);
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
        private final ObjList<LiveViewCheckpointStatePageRef[]> arrays = new ObjList<>();
        private int cursor;
        private int epoch;
    }
}
