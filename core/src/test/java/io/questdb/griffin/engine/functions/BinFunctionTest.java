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

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Record;
import io.questdb.std.BinarySequence;
import org.junit.Test;

public class BinFunctionTest {
    // assert that all type casts that are not possible will throw exception

    private static final BinFunction function = new BinFunction() {
        @Override
        public BinarySequence getBin(Record rec) {
            return null;
        }

        @Override
        public long getBinLen(Record rec) {
            return TableUtils.NULL_LEN;
        }

        @Override
        public boolean isStateless() {
            return true;
        }
    };

    @Test(expected = UnsupportedOperationException.class)
    public void testChar() {
        function.getChar(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGeoByte() {
        function.getGeoByte(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGeoInt() {
        function.getGeoInt(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGeoLong() {
        function.getGeoLong(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGeoShort() {
        function.getGeoShort(null);
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
    public void testGetInt() {
        function.getInt(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLong() {
        function.getLong(null);
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
        function.getStr(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetStr2() {
        function.getStr(null, null);
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
    public void testGetTimestamp() {
        function.getTimestamp(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLong256() {
        function.getLong256(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLong256A() {
        function.getLong256A(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLong256B() {
        function.getLong256B(null);
    }
}