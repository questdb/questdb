/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cutlass.qwp.protocol;

import io.questdb.std.LongObjHashMap;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8s;

/**
 * Simple cache for ILP v4 schemas.
 * <p>
 * Keyed by combined hash of (tableName, schemaHash).
 * Single-threaded, zero-allocation on lookups.
 */
public class QwpSchemaCache {

    private final LongObjHashMap<QwpSchema> cache;
    private long hits;
    private long misses;

    public QwpSchemaCache() {
        this.cache = new LongObjHashMap<>();
    }

    public QwpSchemaCache(int initialCapacity) {
        this.cache = new LongObjHashMap<>(initialCapacity);
    }

    public void put(String tableName, QwpSchema schema) {
        long key = combineKey(tableName.hashCode(), schema.getSchemaHash());
        cache.put(key, schema);
    }

    public QwpSchema get(DirectUtf8Sequence tableName, long schemaHash) {
        long key = combineKey(Utf8s.hashCode(tableName), schemaHash);
        QwpSchema schema = cache.get(key);
        if (schema != null) {
            hits++;
        } else {
            misses++;
        }
        return schema;
    }

    public QwpSchema get(String tableName, long schemaHash) {
        long key = combineKey(tableName.hashCode(), schemaHash);
        QwpSchema schema = cache.get(key);
        if (schema != null) {
            hits++;
        } else {
            misses++;
        }
        return schema;
    }

    public void clear() {
        cache.clear();
        hits = 0;
        misses = 0;
    }

    public int size() {
        return cache.size();
    }

    public long getHits() {
        return hits;
    }

    public long getMisses() {
        return misses;
    }

    public double getHitRate() {
        long total = hits + misses;
        return total > 0 ? (double) hits / total : 0.0;
    }

    private static long combineKey(int tableNameHash, long schemaHash) {
        return ((long) tableNameHash << 32) ^ schemaHash;
    }
}
