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
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class QwpSchemaCacheTest {

    @Test
    public void testCacheByTableAndHash() {
        QwpSchemaCache cache = new QwpSchemaCache();

        QwpSchema schema1 = createTestSchema("col1", QwpConstants.TYPE_INT);
        QwpSchema schema2 = createTestSchema("col2", QwpConstants.TYPE_DOUBLE);

        cache.put(new Utf8String("test_table"), schema1);
        cache.put(new Utf8String("test_table"), schema2);

        Assert.assertEquals(2, cache.size());
        Assert.assertNotNull(cache.get("test_table", schema1.getSchemaHash()));
        Assert.assertNotNull(cache.get("test_table", schema2.getSchemaHash()));
    }

    @Test
    public void testCacheClear() {
        QwpSchemaCache cache = new QwpSchemaCache();

        for (int i = 0; i < 10; i++) {
            QwpSchema schema = createTestSchema("col" + i, QwpConstants.TYPE_INT);
            cache.put(new Utf8String("table" + i), schema);
        }

        Assert.assertEquals(10, cache.size());

        cache.clear();

        Assert.assertEquals(0, cache.size());
        Assert.assertEquals(0, cache.getHits());
        Assert.assertEquals(0, cache.getMisses());
    }

    @Test
    public void testCacheDuplicatePut() {
        QwpSchemaCache cache = new QwpSchemaCache();
        QwpSchema schema = createTestSchema("col1", QwpConstants.TYPE_INT);

        cache.put(new Utf8String("test_table"), schema);
        cache.put(new Utf8String("test_table"), schema);

        Assert.assertEquals(1, cache.size());
    }

    @Test
    public void testCacheEmpty() {
        QwpSchemaCache cache = new QwpSchemaCache();
        Assert.assertEquals(0, cache.size());
        Assert.assertNull(cache.get("nonexistent", 12345L));
    }

    @Test
    public void testCacheGetByDirectUtf8AfterPut() {
        // Production path: put() receives a DirectUtf8Sequence (from wire),
        // get() receives a DirectUtf8Sequence (from a later message).
        // Non-ASCII name verifies consistent UTF-8 hashing end-to-end.
        String tableName = "t\u00ebst_t\u00e4ble";
        QwpSchemaCache cache = new QwpSchemaCache();
        QwpSchema schema = createTestSchema("col1", QwpConstants.TYPE_INT);

        cache.put(new Utf8String(tableName), schema);

        byte[] utf8Bytes = tableName.getBytes(StandardCharsets.UTF_8);
        long address = Unsafe.malloc(utf8Bytes.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < utf8Bytes.length; i++) {
                Unsafe.getUnsafe().putByte(address + i, utf8Bytes[i]);
            }
            DirectUtf8String directName = new DirectUtf8String();
            directName.of(address, address + utf8Bytes.length);

            QwpSchema retrieved = cache.get(directName, schema.getSchemaHash());
            Assert.assertNotNull("schema lookup via DirectUtf8Sequence must hit after put", retrieved);
            Assert.assertEquals(schema.getSchemaHash(), retrieved.getSchemaHash());
        } finally {
            Unsafe.free(address, utf8Bytes.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testCacheHashCollisionDoesNotReturnWrongTable() {
        // "AaAa" and "BBBB" have identical Utf8s.hashCode() for ASCII
        Utf8String tableA = new Utf8String("AaAa");
        Utf8String tableB = new Utf8String("BBBB");
        Assert.assertEquals(Utf8s.hashCode(tableA), Utf8s.hashCode(tableB));

        QwpSchemaCache cache = new QwpSchemaCache();
        QwpSchema schemaA = createTestSchema("colA", QwpConstants.TYPE_INT);

        cache.put(tableA, schemaA);

        // Same schema hash, colliding table name hash — must NOT return tableA's schema
        Assert.assertNull(cache.get("BBBB", schemaA.getSchemaHash()));
        // tableA itself must still resolve
        Assert.assertNotNull(cache.get("AaAa", schemaA.getSchemaHash()));
    }

    @Test
    public void testCacheHit() {
        QwpSchemaCache cache = new QwpSchemaCache();
        QwpSchema schema = createTestSchema("col1", QwpConstants.TYPE_INT);

        cache.put(new Utf8String("test_table"), schema);

        Assert.assertEquals(0, cache.getHits());
        Assert.assertEquals(0, cache.getMisses());

        cache.get("test_table", schema.getSchemaHash());

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

        cache.put(new Utf8String("test_table"), schema);

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
    public void testCacheMiss() {
        QwpSchemaCache cache = new QwpSchemaCache();
        QwpSchema schema = createTestSchema("col1", QwpConstants.TYPE_INT);

        cache.put(new Utf8String("test_table"), schema);

        cache.get("test_table", 99999L); // Miss (wrong hash)
        cache.get("other_table", schema.getSchemaHash()); // Miss (wrong table)

        Assert.assertEquals(0, cache.getHits());
        Assert.assertEquals(2, cache.getMisses());
    }

    @Test
    public void testCachePutAndGet() {
        QwpSchemaCache cache = new QwpSchemaCache();
        QwpSchema schema = createTestSchema("col1", QwpConstants.TYPE_INT);

        cache.put(new Utf8String("test_table"), schema);

        Assert.assertEquals(1, cache.size());
        QwpSchema retrieved = cache.get("test_table", schema.getSchemaHash());
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(schema.getSchemaHash(), retrieved.getSchemaHash());
    }

    private QwpSchema createTestSchema(String columnName, byte typeCode) {
        QwpColumnDef[] columns = {
                new QwpColumnDef(columnName, typeCode)
        };
        return QwpSchema.create(columns);
    }
}
