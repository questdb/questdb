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

package io.questdb.test.cairo;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import org.junit.Assert;
import org.junit.Test;

public class TableColumnMetadataTest {
    @Test
    public void testHasIndex() {
        TableColumnMetadata metadata = new TableColumnMetadata("x", ColumnType.INT, IndexType.BITMAP, 0, true, null);
        Assert.assertEquals(0, metadata.getIndexValueBlockCapacity());
        Assert.assertTrue(metadata.isIndexed());
        Assert.assertTrue(metadata.isSymbolTableStatic());
    }

    @Test
    public void testNoIndex() {
        TableColumnMetadata metadata = new TableColumnMetadata("x", ColumnType.INT, IndexType.NONE, 0, false, null);
        Assert.assertEquals(0, metadata.getIndexValueBlockCapacity());
        Assert.assertFalse(metadata.isIndexed());
        Assert.assertFalse(metadata.isSymbolTableStatic());
    }

    @Test
    public void testPassThroughOfDropsTheStorageIdentity() {
        final TableColumnMetadata source = tableColumn();
        assertStorageIdentityDropped(TableColumnMetadata.passThroughOf(source, "renamed"));
        assertStorageIdentityDropped(TableColumnMetadata.passThroughOf(source, "retyped", ColumnType.STRING, false));
    }

    @Test
    public void testPassThroughOfKeepsWhatAReaderSees() {
        final TableColumnMetadata source = tableColumn();
        final TableColumnMetadata renamed = TableColumnMetadata.passThroughOf(source, "renamed");
        Assert.assertEquals("renamed", renamed.getColumnName());
        Assert.assertEquals(ColumnType.SYMBOL, renamed.getColumnType());
        Assert.assertEquals(IndexType.BITMAP, renamed.getIndexType());
        Assert.assertEquals(512, renamed.getIndexValueBlockCapacity());
        Assert.assertTrue(renamed.isSymbolTableStatic());
        Assert.assertEquals(0x0103, renamed.getParquetEncodingConfig());
        Assert.assertNotNull(renamed.getMetadata());
        Assert.assertEquals(1, renamed.getMetadata().getColumnCount());
        Assert.assertEquals("n", renamed.getMetadata().getColumnName(0));
        Assert.assertEquals(ColumnType.INT, renamed.getMetadata().getColumnType(0));

        final TableColumnMetadata retyped = TableColumnMetadata.passThroughOf(source, "retyped", ColumnType.STRING, false);
        Assert.assertEquals("retyped", retyped.getColumnName());
        Assert.assertEquals(ColumnType.STRING, retyped.getColumnType());
        Assert.assertEquals(IndexType.BITMAP, retyped.getIndexType());
        Assert.assertEquals(512, retyped.getIndexValueBlockCapacity());
        Assert.assertFalse(retyped.isSymbolTableStatic());
        Assert.assertEquals(0x0103, retyped.getParquetEncodingConfig());
        Assert.assertEquals("n", retyped.getMetadata().getColumnName(0));
    }

    private static void assertStorageIdentityDropped(TableColumnMetadata copy) {
        Assert.assertEquals(-1, copy.getWriterIndex());
        Assert.assertEquals(-1, copy.getOriginalWriterIndex());
        Assert.assertFalse(copy.isDedupKeyFlag());
        Assert.assertEquals(0, copy.getReplacingIndex());
        Assert.assertTrue(copy.isSymbolCacheFlag());
        Assert.assertEquals(0, copy.getSymbolCapacity());
    }

    private static TableColumnMetadata tableColumn() {
        final GenericRecordMetadata nested = new GenericRecordMetadata();
        nested.add(new TableColumnMetadata("n", ColumnType.INT));
        final TableColumnMetadata column = new TableColumnMetadata(
                "s", ColumnType.SYMBOL, IndexType.BITMAP, 512, true, nested, 7, true, 3, false, 2048, 5
        );
        column.setParquetEncodingConfig(0x0103);
        return column;
    }
}