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

import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SymbolFunctionTest {
    // assert that all type casts that are not possible will throw exception

    private static final SymbolFunction function = new SymbolFunction() {
        @Override
        public int getInt(Record rec) {
            return 0;
        }

        @Override
        public CharSequence getSymbol(Record rec) {
            return "XYZ";
        }

        @Override
        public CharSequence getSymbolB(Record rec) {
            return "XYZ";
        }

        @Override
        public boolean isSymbolTableStatic() {
            return false;
        }

        @Override
        public CharSequence valueBOf(int key) {
            return "XYZ";
        }

        @Override
        public CharSequence valueOf(int symbolKey) {
            return "XYZ";
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

    @Test(expected = UnsupportedOperationException.class)
    public void testGetByte() {
        function.getByte(null);
    }

    @Test
    public void testGetChar() {
        Assert.assertEquals('X', function.getChar(null));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDate() {
        function.getDate(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDouble() {
        function.getDouble(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetFloat() {
        function.getFloat(null);
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

    @Test(expected = Exception.class)
    public void testGetInvalidTimestamp() {
        function.getTimestamp(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLong() {
        function.getLong(null);
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

    @Test(expected = UnsupportedOperationException.class)
    public void testGetShort() {
        function.getShort(null);
    }

    @Test
    public void testGetStr() {
        Assert.assertEquals("XYZ", function.getStrA(null));
    }

    @Test
    public void testGetStrB() {
        Assert.assertEquals("XYZ", function.getStrB(null));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetStrLen() {
        function.getStrLen(null);
    }

    @Test
    public void testGetVarcharA() {
        Utf8Sequence value = function.getVarcharA(null);
        Assert.assertNotNull(value);
        TestUtils.assertEquals("XYZ", value.toString());
    }

    @Test
    public void testGetVarcharB() {
        Utf8Sequence value = function.getVarcharB(null);
        Assert.assertNotNull(value);
        TestUtils.assertEquals("XYZ", value.toString());
    }

    @Test
    public void testTimestamp() {
        try (SymbolFunction symbolFunction = new SymbolFunction() {
            @Override
            public int getInt(Record rec) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CharSequence getSymbol(Record rec) {
                return "2024-04-09";
            }

            @Override
            public CharSequence getSymbolB(Record rec) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isSymbolTableStatic() {
                return false;
            }

            @Override
            public CharSequence valueBOf(int key) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CharSequence valueOf(int key) {
                throw new UnsupportedOperationException();
            }
        }) {
            Assert.assertEquals("2024-04-09T00:00:00.000Z", NanosTimestampDriver.INSTANCE.toMSecString(symbolFunction.getTimestamp(null)));
        }
    }
}
