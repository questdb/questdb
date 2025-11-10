/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.griffin.engine.functions.GeoByteFunction;
import org.junit.Assert;
import org.junit.Test;

public class GeoHashFunctionTest {

    private static final GeoByteFunction nullFunction = new GeoByteFunction(ColumnType.GEOHASH) {
        @Override
        public byte getGeoByte(Record rec) {
            return GeoHashes.BYTE_NULL;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }
    };

    @Test(expected = UnsupportedOperationException.class)
    public void testChar() {
        Assert.assertEquals('a', nullFunction.getChar(null));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetArray() {
        nullFunction.getArray(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetBin() {
        nullFunction.getBin(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetBinLen() {
        nullFunction.getBinLen(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetBool() {
        nullFunction.getBool(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetByte() {
        nullFunction.getByte(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDate() {
        nullFunction.getDate(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal128() {
        nullFunction.getDecimal128(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal16() {
        nullFunction.getDecimal16(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal256() {
        nullFunction.getDecimal256(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal32() {
        nullFunction.getDecimal32(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal64() {
        nullFunction.getDecimal64(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal8() {
        nullFunction.getDecimal8(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDouble() {
        nullFunction.getDouble(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetFloat() {
        nullFunction.getFloat(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetIPv4() {
        nullFunction.getIPv4(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetInt() {
        nullFunction.getInt(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetRecordCursorFactory() {
        nullFunction.getRecordCursorFactory();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetShort() {
        nullFunction.getShort(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetStr() {
        nullFunction.getStrA(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetStrB() {
        nullFunction.getStrB(null);
    }

    @Test
    public void testGetStrIntoSink1() {
        Assert.assertEquals(GeoHashes.NULL, nullFunction.getGeoByte(null));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetStrLen() {
        nullFunction.getStrLen(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetSym() {
        nullFunction.getSymbol(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetSymB() {
        nullFunction.getSymbolB(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetTimestamp() {
        nullFunction.getTimestamp(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLong256() {
        nullFunction.getLong256(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLong256A() {
        nullFunction.getLong256A(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLong256B() {
        nullFunction.getLong256B(null);
    }
}