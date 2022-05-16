/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.functions;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.str.StringSink;
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
        public boolean isReadThreadSafe() {
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
        public boolean isReadThreadSafe() {
            return true;
        }
    };

    @Test
    public void testChar() {
        Assert.assertEquals('F', functionA.getChar(null));
        final BooleanFunction function = new BooleanFunction() {
            @Override
            public boolean getBool(Record rec) {
                return true;
            }

            @Override
            public boolean isReadThreadSafe() {
                return true;
            }
        };
        Assert.assertEquals('T', function.getChar(null));
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
        Assert.assertEquals(1, functionA.getByte(null));
        Assert.assertEquals(0, functionB.getByte(null));
    }

    @Test
    public void testGetDate() {
        Assert.assertEquals(1, functionA.getDate(null));
        Assert.assertEquals(0, functionB.getDate(null));
    }

    @Test
    public void testGetDouble() {
        Assert.assertEquals(1.0, functionA.getDouble(null), 0.000001);
        Assert.assertEquals(0.0, functionB.getDouble(null), 0.000001);
    }

    @Test
    public void testGetFloat() {
        Assert.assertEquals(1.0, functionA.getFloat(null), 0.000001);
        Assert.assertEquals(0.0, functionB.getFloat(null), 0.000001);
    }

    @Test
    public void testGetInt() {
        Assert.assertEquals(1, functionA.getInt(null));
        Assert.assertEquals(0, functionB.getInt(null));
    }

    @Test
    public void testGetLong() {
        Assert.assertEquals(1, functionA.getLong(null));
        Assert.assertEquals(0, functionB.getLong(null));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetRecordCursorFactory() {
        functionA.getRecordCursorFactory();
    }

    @Test
    public void testGetShort() {
        Assert.assertEquals(1, functionA.getShort(null));
        Assert.assertEquals(0, functionB.getShort(null));
    }

    @Test
    public void testGetStr() {
        Assert.assertEquals("false", functionA.getStr(null));
        Assert.assertEquals("true", functionB.getStr(null));
    }

    @Test
    public void testGetStr2() {
        final StringSink sink = new StringSink();
        functionA.getStr(null, sink);
        TestUtils.assertEquals("false", sink);

        sink.clear();

        functionB.getStr(null, sink);
        TestUtils.assertEquals("true", sink);
    }

    @Test
    public void testGetStrB() {
        Assert.assertEquals("false", functionA.getStr(null));
        Assert.assertEquals("true", functionB.getStr(null));
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
        Assert.assertEquals(1, functionA.getTimestamp(null));
        Assert.assertEquals(0, functionB.getTimestamp(null));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLong256() {
        functionA.getLong256(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLong256A() {
        functionA.getLong256A(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLong256B() {
        functionA.getLong256B(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGeoLong() {
        functionA.getGeoLong(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGeoInt() {
        functionA.getGeoInt(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGeoByte() {
        functionA.getGeoByte(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGeoShort() {
        functionA.getGeoShort(null);
    }
}