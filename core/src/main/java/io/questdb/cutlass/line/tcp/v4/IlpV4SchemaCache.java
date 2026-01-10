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

package io.questdb.cutlass.line.tcp.v4;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe cache for ILP v4 schemas.
 * <p>
 * Schemas are cached by (tableName, schemaHash) tuple to allow
 * schema reference mode lookups.
 * <p>
 * The cache uses LRU eviction when the maximum size is exceeded.
 */
public class IlpV4SchemaCache {

    /**
     * Default maximum cache size.
     */
    public static final int DEFAULT_MAX_SIZE = 1000;

    /**
     * Cache key combining table name and schema hash.
     */
    private static final class CacheKey {
        private final String tableName;
        private final long schemaHash;

        CacheKey(String tableName, long schemaHash) {
            this.tableName = tableName;
            this.schemaHash = schemaHash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CacheKey cacheKey = (CacheKey) o;
            return schemaHash == cacheKey.schemaHash && tableName.equals(cacheKey.tableName);
        }

        @Override
        public int hashCode() {
            int result = tableName.hashCode();
            result = 31 * result + (int) (schemaHash ^ (schemaHash >>> 32));
            return result;
        }
    }

    /**
     * Cache entry with access tracking for LRU eviction.
     */
    private static final class CacheEntry {
        final IlpV4Schema schema;
        volatile long lastAccessTime;

        CacheEntry(IlpV4Schema schema) {
            this.schema = schema;
            this.lastAccessTime = System.nanoTime();
        }

        void touch() {
            lastAccessTime = System.nanoTime();
        }
    }

    private final ConcurrentHashMap<CacheKey, CacheEntry> cache;
    private final int maxSize;
    private final AtomicLong hits = new AtomicLong(0);
    private final AtomicLong misses = new AtomicLong(0);

    /**
     * Creates a schema cache with the default maximum size.
     */
    public IlpV4SchemaCache() {
        this(DEFAULT_MAX_SIZE);
    }

    /**
     * Creates a schema cache with a custom maximum size.
     *
     * @param maxSize maximum number of schemas to cache
     */
    public IlpV4SchemaCache(int maxSize) {
        this.maxSize = maxSize;
        this.cache = new ConcurrentHashMap<>(maxSize);
    }

    /**
     * Puts a schema into the cache.
     *
     * @param tableName the table name
     * @param schema    the schema to cache
     */
    public void put(String tableName, IlpV4Schema schema) {
        CacheKey key = new CacheKey(tableName, schema.getSchemaHash());

        // Evict if necessary before inserting
        if (cache.size() >= maxSize && !cache.containsKey(key)) {
            evictOldest();
        }

        cache.put(key, new CacheEntry(schema));
    }

    /**
     * Gets a schema from the cache.
     *
     * @param tableName  the table name
     * @param schemaHash the schema hash
     * @return the cached schema, or null if not found
     */
    public IlpV4Schema get(String tableName, long schemaHash) {
        CacheKey key = new CacheKey(tableName, schemaHash);
        CacheEntry entry = cache.get(key);

        if (entry != null) {
            entry.touch();
            hits.incrementAndGet();
            return entry.schema;
        }

        misses.incrementAndGet();
        return null;
    }

    /**
     * Checks if a schema is in the cache.
     *
     * @param tableName  the table name
     * @param schemaHash the schema hash
     * @return true if cached
     */
    public boolean contains(String tableName, long schemaHash) {
        return cache.containsKey(new CacheKey(tableName, schemaHash));
    }

    /**
     * Removes a schema from the cache.
     *
     * @param tableName  the table name
     * @param schemaHash the schema hash
     * @return the removed schema, or null if not found
     */
    public IlpV4Schema remove(String tableName, long schemaHash) {
        CacheEntry entry = cache.remove(new CacheKey(tableName, schemaHash));
        return entry != null ? entry.schema : null;
    }

    /**
     * Invalidates all schemas for a table.
     *
     * @param tableName the table name
     * @return number of schemas invalidated
     */
    public int invalidateTable(String tableName) {
        int count = 0;
        var iterator = cache.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            if (entry.getKey().tableName.equals(tableName)) {
                iterator.remove();
                count++;
            }
        }
        return count;
    }

    /**
     * Clears all cached schemas.
     */
    public void clear() {
        cache.clear();
        hits.set(0);
        misses.set(0);
    }

    /**
     * Gets the current cache size.
     */
    public int size() {
        return cache.size();
    }

    /**
     * Gets the maximum cache size.
     */
    public int getMaxSize() {
        return maxSize;
    }

    /**
     * Gets the cache hit count.
     */
    public long getHits() {
        return hits.get();
    }

    /**
     * Gets the cache miss count.
     */
    public long getMisses() {
        return misses.get();
    }

    /**
     * Gets the cache hit rate (0.0 to 1.0).
     *
     * @return hit rate, or 0.0 if no lookups have occurred
     */
    public double getHitRate() {
        long h = hits.get();
        long m = misses.get();
        long total = h + m;
        return total > 0 ? (double) h / total : 0.0;
    }

    /**
     * Evicts the oldest entry (by last access time).
     */
    private void evictOldest() {
        CacheKey oldestKey = null;
        long oldestTime = Long.MAX_VALUE;

        for (var entry : cache.entrySet()) {
            if (entry.getValue().lastAccessTime < oldestTime) {
                oldestTime = entry.getValue().lastAccessTime;
                oldestKey = entry.getKey();
            }
        }

        if (oldestKey != null) {
            cache.remove(oldestKey);
        }
    }

    @Override
    public String toString() {
        return "IlpV4SchemaCache{" +
                "size=" + size() +
                ", maxSize=" + maxSize +
                ", hits=" + hits.get() +
                ", misses=" + misses.get() +
                ", hitRate=" + String.format("%.2f%%", getHitRate() * 100) +
                '}';
    }
}
