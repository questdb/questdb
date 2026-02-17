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
import io.questdb.griffin.engine.functions.GeoByteFunction;
import io.questdb.std.NumericException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class GeoByteFunctionTest extends AbstractCairoTest {
    private static final long hash;
    private static final GeoByteFunction function = new GeoByteFunction(
            ColumnType.getGeoHashTypeWithBits(5)
    ) {
        @Override
        public byte getGeoByte(Record rec) {
            return (byte) hash;
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

    @Test(expected = UnsupportedOperationException.class)
    public void testGetGeoInt() {
        function.getGeoInt(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetGeoLong() {
        function.getGeoLong(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetGeoShort() {
        function.getGeoShort(null);
    }

    @Test
    public void testSimple() {
        Assert.assertEquals(29, function.getGeoByte(null));
        Assert.assertEquals(5, ColumnType.getGeoHashBits(function.getType()));

        sink.clear();
        GeoHashes.appendBinary(function.getGeoByte(null), ColumnType.getGeoHashBits(function.getType()), sink);
        TestUtils.assertEquals("11101", sink);

        final int truncatedHash = (int) SqlUtil.implicitCastGeoHashAsGeoHash(
                function.getGeoByte(null),
                function.getType(),
                ColumnType.getGeoHashTypeWithBits(3)
        );
        sink.clear();
        GeoHashes.appendBinary(truncatedHash, 3, sink);
        TestUtils.assertEquals("111", sink);
    }

    static {
        try {
            hash = GeoHashes.fromString("x");
        } catch (NumericException e) {
            throw new RuntimeException();
        }
    }
}
