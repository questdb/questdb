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

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.CharFunction;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class CharFunctionTest {
    // assert that all type casts that are not possible will throw exception

    private final static byte expect = 4;
    private final static char value = '4';
    private static final CharFunction function = new CharFunction() {
        @Override
        public char getChar(Record rec) {
            return value;
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }
    };

    private static final CharFunction zeroFunc = new CharFunction() {
        @Override
        public char getChar(Record rec) {
            return 0;
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
    public void testGetBin() {
        function.getBin(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetBinLen() {
        function.getBinLen(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetBool() {
        function.getBool(null);
    }

    @Test
    public void testGetByte() {
        Assert.assertEquals(expect, function.getByte(null));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDate() {
        function.getDate(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal128() {
        function.getDecimal128(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal16() {
        function.getDecimal16(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal256() {
        function.getDecimal256(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal32() {
        function.getDecimal32(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal64() {
        function.getDecimal64(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal8() {
        function.getDecimal8(null);
    }

    @Test
    public void testGetDouble() {
        Assert.assertEquals(expect, function.getDouble(null), 0.0001);
    }

    @Test
    public void testGetFloat() {
        Assert.assertEquals(expect, function.getFloat(null), 0.0001);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetGeoByte() {
        function.getGeoByte(null);
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

    @Test(expected = UnsupportedOperationException.class)
    public void testGetIPv4() {
        function.getIPv4(null);
    }

    @Test
    public void testGetInt() {
        Assert.assertEquals(expect, function.getInt(null));
    }

    @Test
    public void testGetLong() {
        Assert.assertEquals(expect, function.getLong(null));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLong128Hi() {
        function.getLong128Hi(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLong128Lo() {
        function.getLong128Lo(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLong256() {
        function.getLong256(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLong256A() {
        function.getLong256A(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLong256B() {
        function.getLong256B(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetRecordCursorFactory() {
        function.getRecordCursorFactory();
    }

    @Test
    public void testGetShort() {
        Assert.assertEquals(expect, function.getShort(null));
    }

    @Test
    public void testGetStr() {
        TestUtils.assertEquals("4", function.getStrA(null));
    }

    @Test
    public void testGetStrB() {
        TestUtils.assertEquals("4", function.getStrB(null));
    }

    @Test
    public void testGetStrLen() {
        Assert.assertEquals(1, function.getStrLen(null));
    }

    @Test
    public void testGetStrZ() {
        Assert.assertNull(zeroFunc.getStrA(null));
    }

    @Test
    public void testGetStrZLen() {
        Assert.assertEquals(TableUtils.NULL_LEN, zeroFunc.getStrLen(null));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetSym() {
        function.getSymbol(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetSymbolB() {
        function.getSymbolB(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetTimestamp() {
        function.getTimestamp(null);
    }

    @Test
    public void testGetVarcharA() {
        Utf8Sequence value = function.getVarcharA(null);
        Assert.assertNotNull(value);
        TestUtils.assertEquals("4", value.toString());
    }

    @Test
    public void testGetVarcharB() {
        Utf8Sequence value = function.getVarcharB(null);
        Assert.assertNotNull(value);
        TestUtils.assertEquals("4", value.toString());
    }

    @Test
    public void testGetZeroVarchar() {
        Assert.assertNull(zeroFunc.getVarcharA(null));
        Assert.assertNull(zeroFunc.getVarcharB(null));
    }
}
