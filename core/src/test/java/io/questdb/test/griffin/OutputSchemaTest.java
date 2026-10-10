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

package io.questdb.test.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.IntList;
import org.junit.Assert;
import org.junit.Test;

public class OutputSchemaTest {

    @Test
    public void testAddColumnFromKeepsEveryAttribute() {
        final OutputSchema nested = new OutputSchema();
        final OutputSchema source = new OutputSchema();
        source.add(7, "a", ColumnType.SYMBOL, nested, false, "t");
        source.setSymbolTableStatic(0, true);
        source.protectName(0);
        source.add(8, "ts", ColumnType.TIMESTAMP, true);
        source.setTimestampIndex(1);
        final OutputSchema target = new OutputSchema();
        target.add(1, "x", ColumnType.INT, true);
        target.addColumnFrom(source, 0);
        Assert.assertEquals(2, target.getColumnCount());
        Assert.assertEquals(7, target.getColumnId(1));
        Assert.assertEquals("a", target.getColumnName(1));
        Assert.assertEquals(ColumnType.SYMBOL, target.getColumnType(1));
        Assert.assertSame(nested, target.getMetadata(1));
        Assert.assertEquals("t", target.getColumnQualifier(1));
        Assert.assertFalse(target.isVisible(1));
        Assert.assertTrue(target.isSymbolTableStatic(1));
        Assert.assertTrue(target.isNameProtected(1));
        Assert.assertEquals(-1, target.getTimestampIndex());
        target.addColumnsFrom(source, "q");
        Assert.assertEquals(4, target.getColumnCount());
        Assert.assertEquals("q", target.getColumnQualifier(2));
        Assert.assertEquals("q", target.getColumnQualifier(3));
        // a join scope names its inputs' columns bare: the composed reference resolves
        Assert.assertFalse(target.isNameProtected(2));
        Assert.assertTrue(target.isSymbolTableStatic(2));
        Assert.assertTrue(target.isVisible(3));
        Assert.assertEquals(-1, target.getTimestampIndex());
        target.addColumnsFrom(source, null);
        Assert.assertEquals(6, target.getColumnCount());
        Assert.assertEquals("t", target.getColumnQualifier(4));
        Assert.assertFalse(target.isNameProtected(4));
        target.addColumnAs(source, 0, 9, "b", true, "r");
        Assert.assertEquals(9, target.getColumnId(6));
        Assert.assertEquals("b", target.getColumnName(6));
        Assert.assertEquals("r", target.getColumnQualifier(6));
        Assert.assertTrue(target.isVisible(6));
        Assert.assertTrue(target.isSymbolTableStatic(6));
        Assert.assertTrue(target.isNameProtected(6));
        Assert.assertSame(nested, target.getMetadata(6));
    }

    @Test
    public void testRetainReordersAndRelocatesTimestamp() {
        final OutputSchema schema = new OutputSchema();
        schema.add(1, "a", ColumnType.INT, true);
        schema.add(2, "ts", ColumnType.TIMESTAMP, true);
        schema.add(3, "b", ColumnType.LONG, null, false, "t");
        schema.protectName(2);
        schema.add(4, "c", ColumnType.DOUBLE, true);
        schema.setTimestampIndex(1);
        final IntList indexes = new IntList();
        indexes.add(2);
        indexes.add(1);
        schema.retain(indexes);
        Assert.assertEquals(2, schema.getColumnCount());
        Assert.assertEquals(3, schema.getColumnId(0));
        Assert.assertEquals("b", schema.getColumnName(0));
        Assert.assertEquals("t", schema.getColumnQualifier(0));
        Assert.assertFalse(schema.isVisible(0));
        Assert.assertTrue(schema.isNameProtected(0));
        Assert.assertEquals(2, schema.getColumnId(1));
        Assert.assertEquals(1, schema.getTimestampIndex());
        indexes.clear();
        indexes.add(0);
        schema.retain(indexes);
        Assert.assertEquals(1, schema.getColumnCount());
        Assert.assertEquals(3, schema.getColumnId(0));
        Assert.assertEquals(-1, schema.getTimestampIndex());
        indexes.clear();
        schema.retain(indexes);
        Assert.assertEquals(0, schema.getColumnCount());
        Assert.assertEquals(-1, schema.getTimestampIndex());
    }

    @Test
    public void testSetTimestampColumnId() {
        final OutputSchema schema = new OutputSchema();
        schema.add(1, "a", ColumnType.INT, true);
        schema.add(2, "ts", ColumnType.TIMESTAMP, true);
        schema.setTimestampColumnId(2);
        Assert.assertEquals(1, schema.getTimestampIndex());
        schema.setTimestampColumnId(9);
        Assert.assertEquals(-1, schema.getTimestampIndex());
        schema.setTimestampColumnId(-1);
        Assert.assertEquals(-1, schema.getTimestampIndex());
    }
}
