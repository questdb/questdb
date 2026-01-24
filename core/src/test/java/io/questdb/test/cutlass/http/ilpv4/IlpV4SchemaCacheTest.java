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

public class IlpV4SchemaCacheTest {

    @Test
    public void testCacheEmpty() {
        IlpV4SchemaCache cache = new IlpV4SchemaCache();
        Assert.assertEquals(0, cache.size());
        Assert.assertNull(cache.get("nonexistent", 12345L));
    }

    @Test
    public void testCachePutAndGet() {
        IlpV4SchemaCache cache = new IlpV4SchemaCache();
        IlpV4Schema schema = createTestSchema("col1", IlpV4Constants.TYPE_INT);

        cache.put("test_table", schema);

        Assert.assertEquals(1, cache.size());
        IlpV4Schema retrieved = cache.get("test_table", schema.getSchemaHash());
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(schema.getSchemaHash(), retrieved.getSchemaHash());
    }

    @Test
    public void testCacheHit() {
        IlpV4SchemaCache cache = new IlpV4SchemaCache();
        IlpV4Schema schema = createTestSchema("col1", IlpV4Constants.TYPE_INT);

        cache.put("test_table", schema);

        Assert.assertEquals(0, cache.getHits());
        Assert.assertEquals(0, cache.getMisses());

        cache.get("test_table", schema.getSchemaHash());

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

        IlpV4Schema schema1 = createTestSchema("col1", IlpV4Constants.TYPE_INT);
        IlpV4Schema schema2 = createTestSchema("col2", IlpV4Constants.TYPE_DOUBLE);

        cache.put("test_table", schema1);
        cache.put("test_table", schema2);

        Assert.assertEquals(2, cache.size());
        Assert.assertNotNull(cache.get("test_table", schema1.getSchemaHash()));
        Assert.assertNotNull(cache.get("test_table", schema2.getSchemaHash()));
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
        Assert.assertEquals(0.6, cache.getHitRate(), 0.001);
    }

    @Test
    public void testCacheHitRateZeroLookups() {
        IlpV4SchemaCache cache = new IlpV4SchemaCache();
        Assert.assertEquals(0.0, cache.getHitRate(), 0.001);
    }

    @Test
    public void testCacheDuplicatePut() {
        IlpV4SchemaCache cache = new IlpV4SchemaCache();
        IlpV4Schema schema = createTestSchema("col1", IlpV4Constants.TYPE_INT);

        cache.put("test_table", schema);
        cache.put("test_table", schema);

        Assert.assertEquals(1, cache.size());
    }

    private IlpV4Schema createTestSchema(String columnName, byte typeCode) {
        IlpV4ColumnDef[] columns = {
                new IlpV4ColumnDef(columnName, typeCode)
        };
        return IlpV4Schema.create(columns);
    }
}
