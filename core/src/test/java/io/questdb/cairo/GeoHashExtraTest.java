/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cairo;
import org.junit.Assert;
import org.junit.Test;

public class GeoHashExtraTest {
    @Test
    public void testBitsPrecision() {
        Assert.assertEquals(ColumnType.GEOHASH, ColumnType.tagOf(ColumnType.GEOHASH));
        Assert.assertEquals(0, GeoHashExtra.getBitsPrecision(ColumnType.GEOHASH));

        int geohashCol = GeoHashExtra.setBitsPrecision(ColumnType.GEOHASH, 42);
        Assert.assertEquals(ColumnType.GEOHASH, ColumnType.tagOf(geohashCol));
        Assert.assertEquals(42, GeoHashExtra.getBitsPrecision(geohashCol));
        geohashCol = GeoHashExtra.setBitsPrecision(geohashCol, 24);
        Assert.assertEquals(24, GeoHashExtra.getBitsPrecision(geohashCol));
    }

    @Test
    public void testStorageSize() {
        Assert.assertEquals(ColumnType.GEOHASH, ColumnType.tagOf(ColumnType.GEOHASH));
        Assert.assertEquals(0, GeoHashExtra.getBitsPrecision(ColumnType.GEOHASH));

        int geohashCol = GeoHashExtra.setBitsPrecision(ColumnType.GEOHASH, 42);
        Assert.assertEquals(ColumnType.GEOHASH, ColumnType.tagOf(geohashCol));
        Assert.assertEquals(64, GeoHashExtra.storageSizeInBits(geohashCol));
        Assert.assertEquals(3, GeoHashExtra.storageSizeInPow2(geohashCol));

        geohashCol = GeoHashExtra.setBitsPrecision(geohashCol, 24);
        Assert.assertEquals(32, GeoHashExtra.storageSizeInBits(geohashCol));
        Assert.assertEquals(2, GeoHashExtra.storageSizeInPow2(geohashCol));

        geohashCol = GeoHashExtra.setBitsPrecision(geohashCol, 13);
        Assert.assertEquals(16, GeoHashExtra.storageSizeInBits(geohashCol));
        Assert.assertEquals(1, GeoHashExtra.storageSizeInPow2(geohashCol));

        geohashCol = GeoHashExtra.setBitsPrecision(geohashCol, 7);
        Assert.assertEquals(8, GeoHashExtra.storageSizeInBits(geohashCol));
        Assert.assertEquals(0, GeoHashExtra.storageSizeInPow2(geohashCol));

        geohashCol = GeoHashExtra.setBitsPrecision(geohashCol, 0);
        Assert.assertEquals(64, GeoHashExtra.storageSizeInBits(geohashCol));
        Assert.assertEquals(3, GeoHashExtra.storageSizeInPow2(geohashCol));
    }

}
