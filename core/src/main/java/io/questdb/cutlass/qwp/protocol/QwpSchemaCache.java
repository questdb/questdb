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
 * Simple cache for QWP v1 schemas.
 * <p>
 * Keyed by combined hash of (tableName, schemaHash).
 * Single-threaded, zero-allocation on lookups.
 * <p>
 * Each entry stores the table name alongside the schema to guard against
 * hash collisions: two tables whose name hashes collide will not return
 * each other's schemas.
 */
public class QwpSchemaCache {

    private final LongObjHashMap<Entry> cache;
    private long hits;
    private long misses;

    public QwpSchemaCache() {
        this.cache = new LongObjHashMap<>();
    }

    public QwpSchemaCache(int initialCapacity) {
        this.cache = new LongObjHashMap<>(initialCapacity);
    }

    public void clear() {
        cache.clear();
        hits = 0;
        misses = 0;
    }

    public QwpSchema get(String tableName, long schemaHash) {
        long key = combineKey(tableName.hashCode(), schemaHash);
        Entry entry = cache.get(key);
        if (entry != null && entry.tableName.equals(tableName)) {
            hits++;
            return entry.schema;
        }
        misses++;
        return null;
    }

    public QwpSchema get(DirectUtf8Sequence tableName, long schemaHash) {
        long key = combineKey(Utf8s.hashCode(tableName), schemaHash);
        Entry entry = cache.get(key);
        if (entry != null && Utf8s.equalsUtf16(entry.tableName, tableName)) {
            hits++;
            return entry.schema;
        }
        misses++;
        return null;
    }

    public double getHitRate() {
        long total = hits + misses;
        return total > 0 ? (double) hits / total : 0.0;
    }

    public long getHits() {
        return hits;
    }

    public long getMisses() {
        return misses;
    }

    public void put(String tableName, QwpSchema schema) {
        long key = combineKey(tableName.hashCode(), schema.getSchemaHash());
        cache.put(key, new Entry(tableName, schema));
    }

    public int size() {
        return cache.size();
    }

    private static long combineKey(int tableNameHash, long schemaHash) {
        return ((long) tableNameHash << 32) ^ schemaHash;
    }

    private static class Entry {
        final QwpSchema schema;
        final String tableName;

        Entry(String tableName, QwpSchema schema) {
            this.tableName = tableName;
            this.schema = schema;
        }
    }
}
