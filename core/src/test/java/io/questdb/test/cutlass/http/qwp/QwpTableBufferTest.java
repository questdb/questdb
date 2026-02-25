/*******************************************************************************
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

package io.questdb.test.cutlass.http.qwp;

import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpTableBuffer;
import org.junit.Assert;
import org.junit.Test;

public class QwpTableBufferTest {

    @Test
    public void testAddDoubleArrayNullOnNonNullableColumn() {
        QwpTableBuffer table = new QwpTableBuffer("test");
        QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("arr", QwpConstants.TYPE_DOUBLE_ARRAY, false);

        // Row 0: real array
        col.addDoubleArray(new double[]{1.0, 2.0});
        table.nextRow();

        // Row 1: null on non-nullable — must write empty array metadata
        col.addDoubleArray((double[]) null);
        table.nextRow();

        // Row 2: real array
        col.addDoubleArray(new double[]{3.0, 4.0});
        table.nextRow();

        Assert.assertEquals(3, table.getRowCount());
        Assert.assertEquals(3, col.getValueCount());
        Assert.assertEquals(col.getSize(), col.getValueCount());

        byte[] dims = col.getArrayDims();
        int[] shapes = col.getArrayShapes();
        Assert.assertEquals(1, dims[0]);
        Assert.assertEquals(2, shapes[0]);
        Assert.assertEquals(1, dims[1]);  // null row: 1D empty
        Assert.assertEquals(0, shapes[1]); // null row: 0 elements
        Assert.assertEquals(1, dims[2]);
        Assert.assertEquals(2, shapes[2]);
    }

    @Test
    public void testAddLongArrayNullOnNonNullableColumn() {
        QwpTableBuffer table = new QwpTableBuffer("test");
        QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("arr", QwpConstants.TYPE_LONG_ARRAY, false);

        // Row 0: real array
        col.addLongArray(new long[]{10, 20});
        table.nextRow();

        // Row 1: null on non-nullable — must write empty array metadata
        col.addLongArray((long[]) null);
        table.nextRow();

        // Row 2: real array
        col.addLongArray(new long[]{30, 40});
        table.nextRow();

        Assert.assertEquals(3, table.getRowCount());
        Assert.assertEquals(3, col.getValueCount());
        Assert.assertEquals(col.getSize(), col.getValueCount());

        byte[] dims = col.getArrayDims();
        int[] shapes = col.getArrayShapes();
        Assert.assertEquals(1, dims[0]);
        Assert.assertEquals(2, shapes[0]);
        Assert.assertEquals(1, dims[1]);  // null row: 1D empty
        Assert.assertEquals(0, shapes[1]); // null row: 0 elements
        Assert.assertEquals(1, dims[2]);
        Assert.assertEquals(2, shapes[2]);
    }

    @Test
    public void testAddSymbolNullOnNonNullableColumn() {
        QwpTableBuffer table = new QwpTableBuffer("test");
        QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("sym", QwpConstants.TYPE_SYMBOL, false);
        col.addSymbol("server1");
        table.nextRow();

        // Null on a non-nullable column must write a sentinel value,
        // keeping size and valueCount in sync
        col.addSymbol(null);
        table.nextRow();

        col.addSymbol("server2");
        table.nextRow();

        Assert.assertEquals(3, table.getRowCount());
        // For non-nullable columns, every row must have a physical value
        Assert.assertEquals(col.getSize(), col.getValueCount());
    }
}
