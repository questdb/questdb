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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class BooleanFunctionTest {
    // assert that all type casts that are not possible will throw exception

    private static final BooleanFunction functionA = new BooleanFunction() {
        @Override
        public boolean getBool(Record rec) {
            return false;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }
    };

    private static final BooleanFunction functionB = new BooleanFunction() {
        @Override
        public boolean getBool(Record rec) {
            return true;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }
    };

    @Test(expected = UnsupportedOperationException.class)
    public void testGetArray() {
        functionA.getArray(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetBin() {
        functionA.getBin(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetBinLen() {
        functionA.getBinLen(null);
    }

    @Test
    public void testGetByte() {
        Assert.assertEquals(1, functionB.getByte(null));
        Assert.assertEquals(0, functionA.getByte(null));
    }

    @Test
    public void testGetChar() {
        Assert.assertEquals('F', functionA.getChar(null));
        final BooleanFunction function = new BooleanFunction() {
            @Override
            public boolean getBool(Record rec) {
                return true;
            }

            @Override
            public boolean isThreadSafe() {
                return true;
            }
        };
        Assert.assertEquals('T', function.getChar(null));
    }

    @Test
    public void testGetDate() {
        Assert.assertEquals(1, functionB.getDate(null));
        Assert.assertEquals(0, functionA.getDate(null));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal128() {
        functionA.getDecimal128(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal16() {
        functionA.getDecimal16(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal256() {
        functionA.getDecimal256(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal32() {
        functionA.getDecimal32(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal64() {
        functionA.getDecimal64(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDecimal8() {
        functionA.getDecimal8(null);
    }

    @Test
    public void testGetDouble() {
        Assert.assertEquals(1.0, functionB.getDouble(null), 0.000001);
        Assert.assertEquals(0.0, functionA.getDouble(null), 0.000001);
    }

    @Test
    public void testGetFloat() {
        Assert.assertEquals(1.0, functionB.getFloat(null), 0.000001);
        Assert.assertEquals(0.0, functionA.getFloat(null), 0.000001);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetGeoByte() {
        functionA.getGeoByte(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetGeoInt() {
        functionA.getGeoInt(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetGeoLong() {
        functionA.getGeoLong(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetGeoShort() {
        functionA.getGeoShort(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetIPv4() {
        Assert.assertEquals(1, functionB.getIPv4(null));
        Assert.assertEquals(0, functionA.getIPv4(null));
    }

    @Test
    public void testGetInt() {
        Assert.assertEquals(1, functionB.getInt(null));
        Assert.assertEquals(0, functionA.getInt(null));
    }

    @Test
    public void testGetLong() {
        Assert.assertEquals(1, functionB.getLong(null));
        Assert.assertEquals(0, functionA.getLong(null));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLong128Hi() {
        functionA.getLong128Hi(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLong128Lo() {
        functionA.getLong128Lo(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLong256() {
        functionA.getLong256(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLong256A() {
        functionA.getLong256A(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLong256B() {
        functionA.getLong256B(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetRecordCursorFactory() {
        functionA.getRecordCursorFactory();
    }

    @Test
    public void testGetShort() {
        Assert.assertEquals(1, functionB.getShort(null));
        Assert.assertEquals(0, functionA.getShort(null));
    }

    @Test
    public void testGetStr() {
        Assert.assertEquals("false", functionA.getStrA(null));
        Assert.assertEquals("true", functionB.getStrA(null));
    }

    @Test
    public void testGetStrLen() {
        Assert.assertEquals("false".length(), functionA.getStrLen(null));
        Assert.assertEquals("true".length(), functionB.getStrLen(null));
    }

    @Test
    public void testGetSym() {
        Assert.assertEquals("false", functionA.getSymbol(null));
        Assert.assertEquals("true", functionB.getSymbol(null));
    }

    @Test
    public void testGetSymB() {
        Assert.assertEquals("false", functionA.getSymbolB(null));
        Assert.assertEquals("true", functionB.getSymbolB(null));
    }

    @Test
    public void testGetTimestamp() {
        Assert.assertEquals(1, functionB.getTimestamp(null));
        Assert.assertEquals(0, functionA.getTimestamp(null));
    }

    @Test
    public void testGetVarcharA() {
        TestUtils.assertEquals("false", functionA.getVarcharA(null));
        TestUtils.assertEquals("true", functionB.getVarcharA(null));
    }

    @Test
    public void testGetVarcharB() {
        TestUtils.assertEquals("false", functionA.getVarcharB(null));
        TestUtils.assertEquals("true", functionB.getVarcharB(null));
    }
}
