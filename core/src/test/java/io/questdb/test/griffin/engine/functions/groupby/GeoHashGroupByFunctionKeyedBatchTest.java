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
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.GeoByteColumn;
import io.questdb.griffin.engine.functions.columns.GeoIntColumn;
import io.questdb.griffin.engine.functions.columns.GeoLongColumn;
import io.questdb.griffin.engine.functions.columns.GeoShortColumn;
import io.questdb.griffin.engine.functions.groupby.CountGeoHashGroupByFunctionByte;
import io.questdb.griffin.engine.functions.groupby.CountGeoHashGroupByFunctionInt;
import io.questdb.griffin.engine.functions.groupby.CountGeoHashGroupByFunctionLong;
import io.questdb.griffin.engine.functions.groupby.CountGeoHashGroupByFunctionShort;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.allocArgBuffer;
import static io.questdb.test.griffin.engine.functions.groupby.KeyedBatchTestUtils.assertEquivalence;

/**
 * Asserts byte-for-byte equivalence of every {@code computeKeyedBatch} override
 * that reads from a {@code GEOHASH} column (byte, short, int, or long storage)
 * against the default implementation (loop over {@code computeFirst} /
 * {@code computeNext}). Covers both the direct-column fast path
 * ({@code argAddr != 0}) and the record-based slow path ({@code argAddr == 0}).
 * GEOHASH uses {@link GeoHashes#NULL} (= -1 across all widths) as the null
 * sentinel.
 */
public class GeoHashGroupByFunctionKeyedBatchTest {
    private static final int ARG_COLUMN_INDEX = 0;

    @Test
    public void testCountGeoByteFastPath() throws Exception {
        final int columnType = ColumnType.getGeoHashTypeWithBits(ColumnType.GEOBYTE_MAX_BITS);
        final byte[] values = {
                (byte) 0x12, GeoHashes.BYTE_NULL, (byte) 0x34, (byte) 0x56,
                (byte) 0x00, GeoHashes.BYTE_NULL, (byte) 0x7F, (byte) 0x01
        };
        TestUtils.assertMemoryLeak(() -> assertEquivalence(
                new CountGeoHashGroupByFunctionByte(GeoByteColumn.newInstance(ARG_COLUMN_INDEX, columnType)),
                true, Byte.BYTES, allocArgBuffer(values), values.length));
    }

    @Test
    public void testCountGeoByteSlowPath() throws Exception {
        final int columnType = ColumnType.getGeoHashTypeWithBits(ColumnType.GEOBYTE_MAX_BITS);
        final byte[] values = {
                (byte) 0x12, GeoHashes.BYTE_NULL, (byte) 0x34, (byte) 0x56,
                (byte) 0x00, GeoHashes.BYTE_NULL, (byte) 0x7F, (byte) 0x01
        };
        TestUtils.assertMemoryLeak(() -> assertEquivalence(
                new CountGeoHashGroupByFunctionByte(GeoByteColumn.newInstance(ARG_COLUMN_INDEX, columnType)),
                false, Byte.BYTES, allocArgBuffer(values), values.length));
    }

    @Test
    public void testCountGeoIntFastPath() throws Exception {
        final int columnType = ColumnType.getGeoHashTypeWithBits(ColumnType.GEOINT_MAX_BITS);
        final int[] values = {
                0x12345678, GeoHashes.INT_NULL, 0x7FFFFFFF, 0x0000_0000,
                0x1234_5678, GeoHashes.INT_NULL, 0x4000_0000, 0x0ABC_DEF0
        };
        TestUtils.assertMemoryLeak(() -> assertEquivalence(
                new CountGeoHashGroupByFunctionInt(GeoIntColumn.newInstance(ARG_COLUMN_INDEX, columnType)),
                true, Integer.BYTES, allocArgBuffer(values), (long) values.length * Integer.BYTES));
    }

    @Test
    public void testCountGeoIntSlowPath() throws Exception {
        final int columnType = ColumnType.getGeoHashTypeWithBits(ColumnType.GEOINT_MAX_BITS);
        final int[] values = {
                0x12345678, GeoHashes.INT_NULL, 0x7FFFFFFF, 0x0000_0000,
                0x1234_5678, GeoHashes.INT_NULL, 0x4000_0000, 0x0ABC_DEF0
        };
        TestUtils.assertMemoryLeak(() -> assertEquivalence(
                new CountGeoHashGroupByFunctionInt(GeoIntColumn.newInstance(ARG_COLUMN_INDEX, columnType)),
                false, Integer.BYTES, allocArgBuffer(values), (long) values.length * Integer.BYTES));
    }

    @Test
    public void testCountGeoLongFastPath() throws Exception {
        final int columnType = ColumnType.getGeoHashTypeWithBits(ColumnType.GEOLONG_MAX_BITS);
        final long[] values = {
                0x0123_4567_89AB_CDEFL, GeoHashes.NULL, 0x7FFF_FFFF_FFFF_FFFFL, 0x0L,
                0x1234_5678_9ABC_DEF0L, GeoHashes.NULL, 0x0FFF_FFFF_FFFF_FFFFL, 0x0A0B_0C0D_0E0F_1011L
        };
        TestUtils.assertMemoryLeak(() -> assertEquivalence(
                new CountGeoHashGroupByFunctionLong(GeoLongColumn.newInstance(ARG_COLUMN_INDEX, columnType)),
                true, Long.BYTES, allocArgBuffer(values), (long) values.length * Long.BYTES));
    }

    @Test
    public void testCountGeoLongSlowPath() throws Exception {
        final int columnType = ColumnType.getGeoHashTypeWithBits(ColumnType.GEOLONG_MAX_BITS);
        final long[] values = {
                0x0123_4567_89AB_CDEFL, GeoHashes.NULL, 0x7FFF_FFFF_FFFF_FFFFL, 0x0L,
                0x1234_5678_9ABC_DEF0L, GeoHashes.NULL, 0x0FFF_FFFF_FFFF_FFFFL, 0x0A0B_0C0D_0E0F_1011L
        };
        TestUtils.assertMemoryLeak(() -> assertEquivalence(
                new CountGeoHashGroupByFunctionLong(GeoLongColumn.newInstance(ARG_COLUMN_INDEX, columnType)),
                false, Long.BYTES, allocArgBuffer(values), (long) values.length * Long.BYTES));
    }

    @Test
    public void testCountGeoShortFastPath() throws Exception {
        final int columnType = ColumnType.getGeoHashTypeWithBits(ColumnType.GEOSHORT_MAX_BITS);
        final short[] values = {
                (short) 0x1234, GeoHashes.SHORT_NULL, (short) 0x7FFF, (short) 0x0001,
                (short) 0x0000, GeoHashes.SHORT_NULL, (short) 0x4000, (short) 0x0ABC
        };
        TestUtils.assertMemoryLeak(() -> assertEquivalence(
                new CountGeoHashGroupByFunctionShort(GeoShortColumn.newInstance(ARG_COLUMN_INDEX, columnType)),
                true, Short.BYTES, allocArgBuffer(values), (long) values.length * Short.BYTES));
    }

    @Test
    public void testCountGeoShortSlowPath() throws Exception {
        final int columnType = ColumnType.getGeoHashTypeWithBits(ColumnType.GEOSHORT_MAX_BITS);
        final short[] values = {
                (short) 0x1234, GeoHashes.SHORT_NULL, (short) 0x7FFF, (short) 0x0001,
                (short) 0x0000, GeoHashes.SHORT_NULL, (short) 0x4000, (short) 0x0ABC
        };
        TestUtils.assertMemoryLeak(() -> assertEquivalence(
                new CountGeoHashGroupByFunctionShort(GeoShortColumn.newInstance(ARG_COLUMN_INDEX, columnType)),
                false, Short.BYTES, allocArgBuffer(values), (long) values.length * Short.BYTES));
    }
}
