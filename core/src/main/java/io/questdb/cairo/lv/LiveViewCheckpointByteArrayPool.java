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
import org.jetbrains.annotations.TestOnly;

import java.util.Arrays;

/**
 * Operation-scoped high-water pool for exact-width decoded metadata images - the
 * identities, key schemas and manifests a root or directory reads once per root. Per-key
 * data never comes from it: partition keys are native, in a
 * {@link LiveViewCheckpointKeyArena}, and state payloads are native too, in a
 * {@link LiveViewCheckpointPayloadArena}. Each width retains its simultaneous-use
 * high-water count, independent of the order in which later reads encounter widths.
 * <p>
 * The pool never shrinks by itself. An owner that outlives the operations it
 * serves reads {@link #getRetainedBytes()} once an operation ends and calls
 * {@link #clear()} when one outlier operation left more behind than the owner is
 * willing to park.
 */
final class LiveViewCheckpointByteArrayPool {
    private final IntObjHashMap<WidthPool> poolsByWidth = new IntObjHashMap<>();
    private int epoch;
    private long retainedBytes;

    /**
     * Drops every pooled array, so the next operation allocates its images afresh.
     * Arrays an earlier operation handed out stay valid for whoever still holds them:
     * the pool only stops handing them out again.
     */
    void clear() {
        poolsByWidth.clear();
        retainedBytes = 0;
    }

    byte[] copy(byte[] source) {
        final byte[] out = next(source.length);
        System.arraycopy(source, 0, out, 0, source.length);
        return out;
    }

    /**
     * @return the image bytes of every array this pool holds, counted by walking its
     * widths rather than read from the count the pool keeps for itself
     */
    @TestOnly
    long countRetainedBytesForTest() {
        long bytes = 0;
        final Object[] widthPools = poolsByWidth.getValues();
        for (int i = 0, n = widthPools.length; i < n; i++) {
            final WidthPool pool = (WidthPool) widthPools[i];
            if (pool != null) {
                for (int j = 0, m = pool.arrays.size(); j < m; j++) {
                    bytes += pool.arrays.getQuick(j).length;
                }
            }
        }
        return bytes;
    }

    /**
     * @return the image bytes of every array this pool holds, excluding array headers
     */
    long getRetainedBytes() {
        return retainedBytes;
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
            retainedBytes += length;
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
