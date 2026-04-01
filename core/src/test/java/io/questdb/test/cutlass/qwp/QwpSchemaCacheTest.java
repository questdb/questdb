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

package io.questdb.test.cutlass.qwp;

import io.questdb.cutlass.qwp.protocol.QwpColumnDef;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpSchema;
import io.questdb.cutlass.qwp.protocol.QwpSchemaCache;
import org.junit.Assert;
import org.junit.Test;

public class QwpSchemaCacheTest {

    @Test
    public void testCacheClear() {
        QwpSchemaCache cache = new QwpSchemaCache();

        for (int i = 0; i < 10; i++) {
            cache.put(i, createTestSchema("col" + i, QwpConstants.TYPE_INT));
        }

        Assert.assertEquals(10, cache.size());

        cache.clear();

        Assert.assertEquals(0, cache.size());
        Assert.assertEquals(0, cache.getHits());
        Assert.assertEquals(0, cache.getMisses());
    }

    @Test
    public void testCacheEmpty() {
        QwpSchemaCache cache = new QwpSchemaCache();
        Assert.assertEquals(0, cache.size());
        Assert.assertNull(cache.get(0));
    }

    @Test
    public void testCacheHit() {
        QwpSchemaCache cache = new QwpSchemaCache();
        QwpSchema schema = createTestSchema("col1", QwpConstants.TYPE_INT);

        cache.put(0, schema);

        Assert.assertEquals(0, cache.getHits());
        Assert.assertEquals(0, cache.getMisses());

        QwpSchema retrieved = cache.get(0);

        Assert.assertNotNull(retrieved);
        Assert.assertEquals(1, cache.getHits());
        Assert.assertEquals(0, cache.getMisses());
    }

    @Test
    public void testCacheHitRateZeroLookups() {
        QwpSchemaCache cache = new QwpSchemaCache();
        Assert.assertEquals(0.0, cache.getHitRate(), 0.001);
    }

    @Test
    public void testCacheMetrics() {
        QwpSchemaCache cache = new QwpSchemaCache();
        QwpSchema schema = createTestSchema("col1", QwpConstants.TYPE_INT);

        cache.put(0, schema);

        // 3 hits
        cache.get(0);
        cache.get(0);
        cache.get(0);

        // 2 misses
        cache.get(1);
        cache.get(99);

        Assert.assertEquals(3, cache.getHits());
        Assert.assertEquals(2, cache.getMisses());
        Assert.assertEquals(0.6, cache.getHitRate(), 0.001);
    }

    @Test
    public void testCacheMiss() {
        QwpSchemaCache cache = new QwpSchemaCache();
        cache.put(0, createTestSchema("col1", QwpConstants.TYPE_INT));

        cache.get(1);  // Miss (wrong ID)
        cache.get(99); // Miss (ID not present)

        Assert.assertEquals(0, cache.getHits());
        Assert.assertEquals(2, cache.getMisses());
    }

    @Test
    public void testCacheNegativeIdMiss() {
        QwpSchemaCache cache = new QwpSchemaCache();
        cache.put(0, createTestSchema("col1", QwpConstants.TYPE_INT));

        Assert.assertNull(cache.get(-1));
        Assert.assertEquals(1, cache.getMisses());
    }

    @Test
    public void testCachePutAndGet() {
        QwpSchemaCache cache = new QwpSchemaCache();
        QwpSchema schema = createTestSchema("col1", QwpConstants.TYPE_INT);

        cache.put(0, schema);

        Assert.assertEquals(1, cache.size());
        QwpSchema retrieved = cache.get(0);
        Assert.assertNotNull(retrieved);
        Assert.assertSame(schema, retrieved);
    }

    @Test
    public void testMultipleSchemas() {
        QwpSchemaCache cache = new QwpSchemaCache();

        QwpSchema schema0 = createTestSchema("col_a", QwpConstants.TYPE_INT);
        QwpSchema schema1 = createTestSchema("col_b", QwpConstants.TYPE_DOUBLE);
        QwpSchema schema2 = createTestSchema("col_c", QwpConstants.TYPE_STRING);

        cache.put(0, schema0);
        cache.put(1, schema1);
        cache.put(2, schema2);

        Assert.assertSame(schema0, cache.get(0));
        Assert.assertSame(schema1, cache.get(1));
        Assert.assertSame(schema2, cache.get(2));
    }

    @Test
    public void testOverwriteSchema() {
        QwpSchemaCache cache = new QwpSchemaCache();

        QwpSchema schema1 = createTestSchema("col_old", QwpConstants.TYPE_INT);
        QwpSchema schema2 = createTestSchema("col_new", QwpConstants.TYPE_DOUBLE);

        cache.put(0, schema1);
        cache.put(0, schema2);

        Assert.assertSame(schema2, cache.get(0));
    }

    private QwpSchema createTestSchema(String columnName, byte typeCode) {
        QwpColumnDef[] columns = {
                new QwpColumnDef(columnName, typeCode)
        };
        return QwpSchema.create(columns);
    }
}
