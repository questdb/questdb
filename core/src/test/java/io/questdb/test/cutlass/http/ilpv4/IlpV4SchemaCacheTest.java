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

package io.questdb.test.cutlass.http.ilpv4;

import io.questdb.cutlass.ilpv4.protocol.IlpV4ColumnDef;
import io.questdb.cutlass.ilpv4.protocol.IlpV4Constants;
import io.questdb.cutlass.ilpv4.protocol.IlpV4Schema;
import io.questdb.cutlass.ilpv4.protocol.IlpV4SchemaCache;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class IlpV4SchemaCacheTest {

    // ==================== Basic Operations ====================

    @Test
    public void testCacheEmpty() {
        IlpV4SchemaCache cache = new IlpV4SchemaCache();
        Assert.assertEquals(0, cache.size());
        Assert.assertNull(cache.get("nonexistent", 12345L));
    }

    @Test
    public void testCachePut() {
        IlpV4SchemaCache cache = new IlpV4SchemaCache();
        IlpV4Schema schema = createTestSchema("col1", IlpV4Constants.TYPE_INT);

        cache.put("test_table", schema);

        Assert.assertEquals(1, cache.size());
    }

    @Test
    public void testCacheGet() {
        IlpV4SchemaCache cache = new IlpV4SchemaCache();
        IlpV4Schema schema = createTestSchema("col1", IlpV4Constants.TYPE_INT);

        cache.put("test_table", schema);
        IlpV4Schema retrieved = cache.get("test_table", schema.getSchemaHash());

        Assert.assertNotNull(retrieved);
        Assert.assertEquals(schema.getSchemaHash(), retrieved.getSchemaHash());
        Assert.assertEquals(schema.getColumnCount(), retrieved.getColumnCount());
    }

    @Test
    public void testCacheHit() {
        IlpV4SchemaCache cache = new IlpV4SchemaCache();
        IlpV4Schema schema = createTestSchema("col1", IlpV4Constants.TYPE_INT);

        cache.put("test_table", schema);

        Assert.assertEquals(0, cache.getHits());
        Assert.assertEquals(0, cache.getMisses());

        cache.get("test_table", schema.getSchemaHash()); // Hit

        Assert.assertEquals(1, cache.getHits());
        Assert.assertEquals(0, cache.getMisses());
    }

    @Test
    public void testCacheMiss() {
        IlpV4SchemaCache cache = new IlpV4SchemaCache();
        IlpV4Schema schema = createTestSchema("col1", IlpV4Constants.TYPE_INT);

        cache.put("test_table", schema);

        cache.get("test_table", 99999L); // Miss (wrong hash)
        cache.get("other_table", schema.getSchemaHash()); // Miss (wrong table)

        Assert.assertEquals(0, cache.getHits());
        Assert.assertEquals(2, cache.getMisses());
    }

    @Test
    public void testCacheByTableAndHash() {
        IlpV4SchemaCache cache = new IlpV4SchemaCache();

        // Create two different schemas
        IlpV4Schema schema1 = createTestSchema("col1", IlpV4Constants.TYPE_INT);
        IlpV4Schema schema2 = createTestSchema("col2", IlpV4Constants.TYPE_DOUBLE);

        // Put both for same table (different versions)
        cache.put("test_table", schema1);
        cache.put("test_table", schema2);

        Assert.assertEquals(2, cache.size());

        // Both should be retrievable
        Assert.assertNotNull(cache.get("test_table", schema1.getSchemaHash()));
        Assert.assertNotNull(cache.get("test_table", schema2.getSchemaHash()));
    }

    @Test
    public void testCacheContains() {
        IlpV4SchemaCache cache = new IlpV4SchemaCache();
        IlpV4Schema schema = createTestSchema("col1", IlpV4Constants.TYPE_INT);

        cache.put("test_table", schema);

        Assert.assertTrue(cache.contains("test_table", schema.getSchemaHash()));
        Assert.assertFalse(cache.contains("test_table", 99999L));
        Assert.assertFalse(cache.contains("other_table", schema.getSchemaHash()));
    }

    @Test
    public void testCacheRemove() {
        IlpV4SchemaCache cache = new IlpV4SchemaCache();
        IlpV4Schema schema = createTestSchema("col1", IlpV4Constants.TYPE_INT);

        cache.put("test_table", schema);
        Assert.assertEquals(1, cache.size());

        IlpV4Schema removed = cache.remove("test_table", schema.getSchemaHash());

        Assert.assertNotNull(removed);
        Assert.assertEquals(0, cache.size());
        Assert.assertNull(cache.get("test_table", schema.getSchemaHash()));
    }

    @Test
    public void testCacheRemoveNonExistent() {
        IlpV4SchemaCache cache = new IlpV4SchemaCache();

        IlpV4Schema removed = cache.remove("nonexistent", 12345L);

        Assert.assertNull(removed);
    }

    // ==================== Eviction Tests ====================

    @Test
    public void testCacheEviction() throws InterruptedException {
        IlpV4SchemaCache cache = new IlpV4SchemaCache(3); // Small cache

        // Add 4 schemas (should trigger eviction)
        for (int i = 0; i < 4; i++) {
            IlpV4Schema schema = createTestSchema("col" + i, IlpV4Constants.TYPE_INT);
            cache.put("table" + i, schema);
            Thread.sleep(10); // Ensure different access times
        }

        // Cache should not exceed max size
        Assert.assertTrue(cache.size() <= 3);
    }

    @Test
    public void testCacheEvictionLRU() throws InterruptedException {
        IlpV4SchemaCache cache = new IlpV4SchemaCache(3);

        IlpV4Schema schema0 = createTestSchema("col0", IlpV4Constants.TYPE_INT);
        IlpV4Schema schema1 = createTestSchema("col1", IlpV4Constants.TYPE_DOUBLE);
        IlpV4Schema schema2 = createTestSchema("col2", IlpV4Constants.TYPE_LONG);

        cache.put("table0", schema0);
        Thread.sleep(10);
        cache.put("table1", schema1);
        Thread.sleep(10);
        cache.put("table2", schema2);
        Thread.sleep(10);

        // Access table0 to make it "recently used"
        cache.get("table0", schema0.getSchemaHash());
        Thread.sleep(10);

        // Add new schema - should evict table1 (least recently used)
        IlpV4Schema schema3 = createTestSchema("col3", IlpV4Constants.TYPE_STRING);
        cache.put("table3", schema3);

        Assert.assertEquals(3, cache.size());
        Assert.assertNotNull(cache.get("table0", schema0.getSchemaHash())); // Still there
        Assert.assertNotNull(cache.get("table2", schema2.getSchemaHash())); // Still there
        Assert.assertNotNull(cache.get("table3", schema3.getSchemaHash())); // New entry
    }

    // ==================== Invalidation Tests ====================

    @Test
    public void testCacheInvalidation() {
        IlpV4SchemaCache cache = new IlpV4SchemaCache();

        IlpV4Schema schema1 = createTestSchema("col1", IlpV4Constants.TYPE_INT);
        IlpV4Schema schema2 = createTestSchema("col2", IlpV4Constants.TYPE_DOUBLE);

        cache.put("table1", schema1);
        cache.put("table1", schema2); // Different schema version for same table
        cache.put("table2", schema1);

        Assert.assertEquals(3, cache.size());

        int invalidated = cache.invalidateTable("table1");

        Assert.assertEquals(2, invalidated);
        Assert.assertEquals(1, cache.size());
        Assert.assertNull(cache.get("table1", schema1.getSchemaHash()));
        Assert.assertNull(cache.get("table1", schema2.getSchemaHash()));
        Assert.assertNotNull(cache.get("table2", schema1.getSchemaHash()));
    }

    @Test
    public void testCacheClear() {
        IlpV4SchemaCache cache = new IlpV4SchemaCache();

        for (int i = 0; i < 10; i++) {
            IlpV4Schema schema = createTestSchema("col" + i, IlpV4Constants.TYPE_INT);
            cache.put("table" + i, schema);
        }

        Assert.assertEquals(10, cache.size());

        cache.clear();

        Assert.assertEquals(0, cache.size());
        Assert.assertEquals(0, cache.getHits());
        Assert.assertEquals(0, cache.getMisses());
    }

    // ==================== Thread Safety Tests ====================

    @Test
    public void testCacheThreadSafety() throws InterruptedException {
        IlpV4SchemaCache cache = new IlpV4SchemaCache(100);
        int numThreads = 10;
        int operationsPerThread = 100;

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger errors = new AtomicInteger(0);

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < operationsPerThread; i++) {
                        String tableName = "table" + (threadId * 10 + (i % 10));
                        IlpV4Schema schema = createTestSchema("col" + i, IlpV4Constants.TYPE_INT);

                        // Mix of operations
                        cache.put(tableName, schema);
                        cache.get(tableName, schema.getSchemaHash());
                        cache.contains(tableName, schema.getSchemaHash());

                        if (i % 20 == 0) {
                            cache.remove(tableName, schema.getSchemaHash());
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        Assert.assertEquals(0, errors.get());
        // Just verify no exceptions - concurrent cache state is non-deterministic
    }

    @Test
    public void testCacheConcurrentReads() throws InterruptedException {
        IlpV4SchemaCache cache = new IlpV4SchemaCache();
        IlpV4Schema schema = createTestSchema("col1", IlpV4Constants.TYPE_INT);
        cache.put("test_table", schema);

        int numThreads = 10;
        int readsPerThread = 1000;

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int t = 0; t < numThreads; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < readsPerThread; i++) {
                        IlpV4Schema result = cache.get("test_table", schema.getSchemaHash());
                        if (result != null) {
                            successCount.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        // All reads should succeed
        Assert.assertEquals(numThreads * readsPerThread, successCount.get());
    }

    // ==================== Metrics Tests ====================

    @Test
    public void testCacheMetrics() {
        IlpV4SchemaCache cache = new IlpV4SchemaCache();
        IlpV4Schema schema = createTestSchema("col1", IlpV4Constants.TYPE_INT);

        cache.put("test_table", schema);

        // 3 hits
        cache.get("test_table", schema.getSchemaHash());
        cache.get("test_table", schema.getSchemaHash());
        cache.get("test_table", schema.getSchemaHash());

        // 2 misses
        cache.get("test_table", 99999L);
        cache.get("other_table", schema.getSchemaHash());

        Assert.assertEquals(3, cache.getHits());
        Assert.assertEquals(2, cache.getMisses());
        Assert.assertEquals(0.6, cache.getHitRate(), 0.001); // 3/5 = 0.6
    }

    @Test
    public void testCacheHitRateZeroLookups() {
        IlpV4SchemaCache cache = new IlpV4SchemaCache();
        Assert.assertEquals(0.0, cache.getHitRate(), 0.001);
    }

    @Test
    public void testCacheToString() {
        IlpV4SchemaCache cache = new IlpV4SchemaCache(100);
        IlpV4Schema schema = createTestSchema("col1", IlpV4Constants.TYPE_INT);
        cache.put("test", schema);
        cache.get("test", schema.getSchemaHash());
        cache.get("missing", 123L);

        String str = cache.toString();
        Assert.assertTrue(str.contains("size=1"));
        Assert.assertTrue(str.contains("maxSize=100"));
        Assert.assertTrue(str.contains("hits=1"));
        Assert.assertTrue(str.contains("misses=1"));
    }

    // ==================== Edge Cases ====================

    @Test
    public void testCacheDuplicatePut() {
        IlpV4SchemaCache cache = new IlpV4SchemaCache();
        IlpV4Schema schema = createTestSchema("col1", IlpV4Constants.TYPE_INT);

        cache.put("test_table", schema);
        cache.put("test_table", schema); // Same schema again

        Assert.assertEquals(1, cache.size());
    }

    @Test
    public void testCacheMaxSizeOne() throws InterruptedException {
        IlpV4SchemaCache cache = new IlpV4SchemaCache(1);

        IlpV4Schema schema1 = createTestSchema("col1", IlpV4Constants.TYPE_INT);
        IlpV4Schema schema2 = createTestSchema("col2", IlpV4Constants.TYPE_DOUBLE);

        cache.put("table1", schema1);
        Thread.sleep(10);
        cache.put("table2", schema2);

        Assert.assertEquals(1, cache.size());
    }

    @Test
    public void testDefaultMaxSize() {
        IlpV4SchemaCache cache = new IlpV4SchemaCache();
        Assert.assertEquals(IlpV4SchemaCache.DEFAULT_MAX_SIZE, cache.getMaxSize());
    }

    // ==================== Helper Methods ====================

    private IlpV4Schema createTestSchema(String columnName, byte typeCode) {
        IlpV4ColumnDef[] columns = {
                new IlpV4ColumnDef(columnName, typeCode)
        };
        return IlpV4Schema.create(columns);
    }
}
