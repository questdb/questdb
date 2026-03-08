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

package io.questdb.test.griffin.engine.functions.columns;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.engine.functions.columns.ColumnUtils;
import io.questdb.griffin.engine.functions.columns.GeoLongColumn;
import org.junit.Assert;
import org.junit.Test;

public class GeoLongColumnTest {

    @Test
    public void testNewInstanceReturnsCorrectTypeForCachedColumnIndexes() {
        for (int col = 0; col < 40; col++) {
            assertBits(ColumnType.GEOLONG_MIN_BITS, col);
            assertBits(ColumnType.GEOLONG_MAX_BITS, col);
        }
    }

    private static void assertBits(int bits, int column) {
        int type = ColumnType.getGeoHashTypeWithBits(bits);
        GeoLongColumn col = GeoLongColumn.newInstance(column, type);
        String desc = "col=" + column + ",bits=" + bits;

        Assert.assertEquals(desc, type, col.getType());
        Assert.assertEquals(desc, column, col.getColumnIndex());

        boolean isCached = GeoLongColumn.newInstance(column, type) == GeoLongColumn.newInstance(column, type);

        if (column < ColumnUtils.STATIC_COLUMN_COUNT) {
            Assert.assertTrue(isCached);
        } else {
            Assert.assertFalse(isCached);
        }
    }
}
