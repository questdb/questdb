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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.griffin.engine.functions.columns.ByteColumn;
import io.questdb.griffin.engine.functions.columns.GeoByteColumn;
import io.questdb.griffin.engine.functions.groupby.CountGeoHashGroupByFunctionByte;
import io.questdb.griffin.engine.functions.groupby.FirstByteGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.LastByteGroupByFunction;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import org.junit.Assert;
import org.junit.Test;

public class ByteGroupByFunctionBatchTest extends AbstractGroupByFunctionBatchTest {
    @Test
    public void testFirstByteBatch() {
        FirstByteGroupByFunction function = new FirstByteGroupByFunction(ByteColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateBytes((byte) 11, (byte) 22, (byte) 33);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(11, function.getByte(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testFirstByteBatchAccumulates() {
        FirstByteGroupByFunction function = new FirstByteGroupByFunction(ByteColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateBytes((byte) 11, (byte) 22);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateBytes((byte) 33, (byte) 44);
            function.computeBatch(value, ptr, 2, 2);

            Assert.assertEquals(11, function.getByte(value));
        }
    }

    @Test
    public void testFirstByteBatchEmpty() {
        FirstByteGroupByFunction function = new FirstByteGroupByFunction(ByteColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            function.computeBatch(value, 0, 0, 0);

            Assert.assertEquals(0, function.getByte(value));
        }
    }

    @Test
    public void testFirstByteSetEmpty() {
        FirstByteGroupByFunction function = new FirstByteGroupByFunction(ByteColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(0, function.getByte(value));
        }
    }

    @Test
    public void testLastByteBatch() {
        LastByteGroupByFunction function = new LastByteGroupByFunction(ByteColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateBytes((byte) 10, (byte) 20, (byte) 30);
            function.computeBatch(value, ptr, 3, 0);

            Assert.assertEquals(2, value.getLong(0));
            Assert.assertEquals(30, function.getByte(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastByteBatchSingle() {
        LastByteGroupByFunction function = new LastByteGroupByFunction(ByteColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateBytes((byte) 77);
            function.computeBatch(value, ptr, 1, 0);

            Assert.assertEquals(77, function.getByte(value));
        }
    }

    @Test
    public void testLastByteBatchAccumulates() {
        LastByteGroupByFunction function = new LastByteGroupByFunction(ByteColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateBytes((byte) 10, (byte) 20);
            function.computeBatch(value, ptr, 2, 0);

            ptr = allocateBytes((byte) 30, (byte) 40);
            function.computeBatch(value, ptr, 2, 2);

            Assert.assertEquals(40, function.getByte(value));
        }
    }

    @Test
    public void testLastByteSetEmpty() {
        LastByteGroupByFunction function = new LastByteGroupByFunction(ByteColumn.newInstance(COLUMN_INDEX));
        try (SimpleMapValue value = prepare(function)) {
            Assert.assertEquals(0, function.getByte(value));
        }
    }

    @Test
    public void testCountGeoHashByteBatchAccumulates() {
        int type = ColumnType.getGeoHashTypeWithBits(ColumnType.GEOBYTE_MAX_BITS);
        CountGeoHashGroupByFunctionByte function = new CountGeoHashGroupByFunctionByte(
                GeoByteColumn.newInstance(COLUMN_INDEX, type)
        );
        try (SimpleMapValue value = prepare(function)) {
            long ptr = allocateBytes((byte) 1, GeoHashes.BYTE_NULL, (byte) 2);
            function.computeBatch(value, ptr, 3, 0);

            ptr = allocateBytes((byte) 3, (byte) 4);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(4L, function.getLong(value));
        }
    }
}
