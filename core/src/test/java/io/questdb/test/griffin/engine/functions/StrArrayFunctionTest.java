/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.griffin.engine.functions.StrArrayFunction;
import io.questdb.std.str.CharSink;
import org.junit.Test;

public class StrArrayFunctionTest {
    // assert that all type casts that are not possible will throw exception

    private static final StrArrayFunction function = new StrArrayFunction() {
        @Override
        public int getArrayLength() {
            return 1;
        }

        @Override
        public CharSequence getStr(Record rec) {
            return "{hello}";
        }

        @Override
        public CharSequence getStr(Record rec, int arrayIndex) {
            return "hello";
        }

        @Override
        public void getStr(Record rec, CharSink sink) {
            sink.put(getStr(rec));
        }

        @Override
        public void getStr(Record rec, CharSink sink, int arrayIndex) {
            sink.put(getStr(rec, arrayIndex));
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return getStr(rec);
        }

        @Override
        public CharSequence getStrB(Record rec, int arrayIndex) {
            return getStr(rec, arrayIndex);
        }

        @Override
        public int getStrLen(Record rec) {
            return getStr(rec).length();
        }

        @Override
        public int getStrLen(Record rec, int arrayIndex) {
            return getStr(rec, arrayIndex).length();
        }

        @Override
        public boolean isReadThreadSafe() {
            return true;
        }
    };

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

    @Test(expected = UnsupportedOperationException.class)
    public void testGetDate() {
        function.getDate(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetIPv4() {
        function.getIPv4(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetRecordCursorFactory() {
        function.getRecordCursorFactory();
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