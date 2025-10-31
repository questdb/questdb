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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.std.Numbers;
import io.questdb.test.TestTimestampType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

// asserts that all type casts that are not possible will throw exception
@RunWith(Parameterized.class)
public class TimestampFunctionTest {
    private final TimestampFunction function;
    private final TestTimestampType timestampType;

    public TimestampFunctionTest(TestTimestampType timestampType) {
        this.function = new TimestampFunction(timestampType.getTimestampType()) {
            @Override
            public long getTimestamp(Record rec) {
                return 145000L;
            }

            @Override
            public boolean isThreadSafe() {
                return true;
            }
        };
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

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

    @Test(expected = UnsupportedOperationException.class)
    public void testGetChar() {
        function.getChar(null);
    }

    @Test
    public void testGetDate() {
        Assert.assertEquals(timestampType.getDriver().toDate(145000), function.getDate(null));
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
        Assert.assertEquals(145000, function.getDouble(null), 0.1);
    }

    @Test
    public void testGetFloat() {
        Assert.assertEquals(145000, function.getFloat(null), 0.1);
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

    @Test(expected = UnsupportedOperationException.class)
    public void testGetInt() {
        function.getInt(null);
    }

    @Test
    public void testGetLong() {
        Assert.assertEquals(145000, function.getLong(null));
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

    @Test
    public void testGetNullDate() {
        final TimestampFunction function = new TimestampFunction(ColumnType.TIMESTAMP_MICRO) {
            @Override
            public long getTimestamp(Record rec) {
                return Numbers.LONG_NULL;
            }

            @Override
            public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            }

            @Override
            public boolean isThreadSafe() {
                return true;
            }
        };
        Assert.assertEquals(Numbers.LONG_NULL, function.getDate(null));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetRecordCursorFactory() {
        function.getRecordCursorFactory();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetShort() {
        function.getShort(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetStr() {
        function.getStrA(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetStrB() {
        function.getStrB(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetStrLen() {
        function.getStrLen(null);
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
    public void testGetVarcharA() {
        function.getVarcharA(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetVarcharB() {
        function.getVarcharB(null);
    }
}
