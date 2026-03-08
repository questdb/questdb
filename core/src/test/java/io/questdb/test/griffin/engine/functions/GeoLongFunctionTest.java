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

package io.questdb.test.griffin.engine.functions;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.engine.functions.GeoLongFunction;
import io.questdb.std.NumericException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class GeoLongFunctionTest extends AbstractCairoTest {
    private static final long hash;
    private static final GeoLongFunction function = new GeoLongFunction(
            ColumnType.getGeoHashTypeWithBits(40)
    ) {
        @Override
        public long getGeoLong(Record rec) {
            return hash;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }
    };

    @Test(expected = UnsupportedOperationException.class)
    public void testGetArray() {
        function.getArray(null);
    }

    @Test
    public void testSimple() {
        Assert.assertEquals(997599804458L, function.getGeoLong(null));
        Assert.assertEquals(40, ColumnType.getGeoHashBits(function.getType()));

        sink.clear();
        GeoHashes.appendBinary(function.getGeoLong(null), ColumnType.getGeoHashBits(function.getType()), sink);
        TestUtils.assertEquals("1110100001000101100101001111110000101010", sink);

        final int truncatedHash = (int) SqlUtil.implicitCastGeoHashAsGeoHash(
                function.getGeoLong(null),
                function.getType(),
                ColumnType.getGeoHashTypeWithBits(3)
        );
        sink.clear();
        GeoHashes.appendBinary(truncatedHash, 3, sink);
        TestUtils.assertEquals("111", sink);
    }

    static {
        try {
            hash = GeoHashes.fromString("x12t9z1b");
        } catch (NumericException e) {
            throw new RuntimeException();
        }
    }
}
