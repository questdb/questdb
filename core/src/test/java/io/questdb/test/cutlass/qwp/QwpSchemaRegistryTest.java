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
import io.questdb.cutlass.qwp.protocol.QwpParseException;
import io.questdb.cutlass.qwp.protocol.QwpSchema;
import io.questdb.cutlass.qwp.protocol.QwpSchemaRegistry;
import org.junit.Assert;
import org.junit.Test;

public class QwpSchemaRegistryTest {

    @Test
    public void testCacheClear() throws Exception {
        QwpSchemaRegistry registry = new QwpSchemaRegistry();

        for (int i = 0; i < 10; i++) {
            registry.put(i, createTestSchema("col" + i, QwpConstants.TYPE_INT));
        }

        Assert.assertEquals(10, registry.size());

        registry.clear();

        Assert.assertEquals(0, registry.size());
        Assert.assertEquals(0, registry.getHits());
        Assert.assertEquals(0, registry.getMisses());
    }

    @Test
    public void testCacheEmpty() {
        QwpSchemaRegistry registry = new QwpSchemaRegistry();
        Assert.assertEquals(0, registry.size());
        Assert.assertNull(registry.get(0));
    }

    @Test
    public void testCacheHit() throws Exception {
        QwpSchemaRegistry registry = new QwpSchemaRegistry();
        QwpSchema schema = createTestSchema("col1", QwpConstants.TYPE_INT);

        registry.put(0, schema);

        Assert.assertEquals(0, registry.getHits());
        Assert.assertEquals(0, registry.getMisses());

        QwpSchema retrieved = registry.get(0);

        Assert.assertNotNull(retrieved);
        Assert.assertEquals(1, registry.getHits());
        Assert.assertEquals(0, registry.getMisses());
    }

    @Test
    public void testCacheHitRateZeroLookups() {
        QwpSchemaRegistry registry = new QwpSchemaRegistry();
        Assert.assertEquals(0.0, registry.getHitRate(), 0.001);
    }

    @Test
    public void testCacheMetrics() throws Exception {
        QwpSchemaRegistry registry = new QwpSchemaRegistry();
        QwpSchema schema = createTestSchema("col1", QwpConstants.TYPE_INT);

        registry.put(0, schema);

        // 3 hits
        registry.get(0);
        registry.get(0);
        registry.get(0);

        // 2 misses
        registry.get(1);
        registry.get(99);

        Assert.assertEquals(3, registry.getHits());
        Assert.assertEquals(2, registry.getMisses());
        Assert.assertEquals(0.6, registry.getHitRate(), 0.001);
    }

    @Test
    public void testCacheMiss() throws Exception {
        QwpSchemaRegistry registry = new QwpSchemaRegistry();
        registry.put(0, createTestSchema("col1", QwpConstants.TYPE_INT));

        registry.get(1);  // Miss (wrong ID)
        registry.get(99); // Miss (ID not present)

        Assert.assertEquals(0, registry.getHits());
        Assert.assertEquals(2, registry.getMisses());
    }

    @Test
    public void testCacheNegativeIdMiss() throws Exception {
        QwpSchemaRegistry registry = new QwpSchemaRegistry();
        registry.put(0, createTestSchema("col1", QwpConstants.TYPE_INT));

        Assert.assertNull(registry.get(-1));
        Assert.assertEquals(1, registry.getMisses());
    }

    @Test
    public void testCachePutAndGet() throws Exception {
        QwpSchemaRegistry registry = new QwpSchemaRegistry();
        QwpSchema schema = createTestSchema("col1", QwpConstants.TYPE_INT);

        registry.put(0, schema);

        Assert.assertEquals(1, registry.size());
        QwpSchema retrieved = registry.get(0);
        Assert.assertNotNull(retrieved);
        Assert.assertSame(schema, retrieved);
    }

    @Test
    public void testMultipleSchemas() throws Exception {
        QwpSchemaRegistry registry = new QwpSchemaRegistry();

        QwpSchema schema0 = createTestSchema("col_a", QwpConstants.TYPE_INT);
        QwpSchema schema1 = createTestSchema("col_b", QwpConstants.TYPE_DOUBLE);
        QwpSchema schema2 = createTestSchema("col_c", QwpConstants.TYPE_VARCHAR);

        registry.put(0, schema0);
        registry.put(1, schema1);
        registry.put(2, schema2);

        Assert.assertSame(schema0, registry.get(0));
        Assert.assertSame(schema1, registry.get(1));
        Assert.assertSame(schema2, registry.get(2));
    }

    @Test
    public void testCacheClearResetsExpectedSchemaId() throws Exception {
        QwpSchemaRegistry registry = new QwpSchemaRegistry();

        registry.put(0, createTestSchema("col_old", QwpConstants.TYPE_INT));
        registry.clear();
        registry.put(0, createTestSchema("col_new", QwpConstants.TYPE_DOUBLE));

        Assert.assertNotNull(registry.get(0));
    }

    @Test
    public void testSchemaIdCanSkip() throws Exception {
        QwpSchemaRegistry registry = new QwpSchemaRegistry();
        registry.put(0, createTestSchema("col0", QwpConstants.TYPE_INT));

        // Gaps are allowed — the client may encode tables in hash map
        // iteration order, not schema ID assignment order
        registry.put(2, createTestSchema("col2", QwpConstants.TYPE_DOUBLE));
        Assert.assertNotNull(registry.get(0));
        Assert.assertNull(registry.get(1));
        Assert.assertNotNull(registry.get(2));
    }

    @Test
    public void testSchemaIdCanBeReRegistered() throws Exception {
        QwpSchemaRegistry registry = new QwpSchemaRegistry();
        registry.put(0, createTestSchema("col0", QwpConstants.TYPE_INT));
        registry.put(1, createTestSchema("col1", QwpConstants.TYPE_DOUBLE));

        // Re-registration replaces the cached schema
        QwpSchema updated = createTestSchema("col1_v2", QwpConstants.TYPE_LONG);
        registry.put(1, updated);
        Assert.assertEquals("col1_v2", registry.get(1).getColumns().getQuick(0).getName());
    }

    @Test
    public void testSchemaCountPerConnectionIsBounded() throws Exception {
        QwpSchemaRegistry registry = new QwpSchemaRegistry(2);
        registry.put(0, createTestSchema("col0", QwpConstants.TYPE_INT));
        registry.put(1, createTestSchema("col1", QwpConstants.TYPE_DOUBLE));

        try {
            registry.put(2, createTestSchema("col2", QwpConstants.TYPE_INT));
            Assert.fail("Expected QwpParseException");
        } catch (QwpParseException e) {
            Assert.assertEquals(QwpParseException.ErrorCode.INVALID_SCHEMA_ID, e.getErrorCode());
            Assert.assertTrue(e.getMessage().contains("maxSchemasPerConnection=2"));
        }
    }

    private QwpSchema createTestSchema(String columnName, byte typeCode) {
        QwpColumnDef[] columns = {
                new QwpColumnDef(columnName, typeCode)
        };
        return QwpSchema.create(columns);
    }
}
