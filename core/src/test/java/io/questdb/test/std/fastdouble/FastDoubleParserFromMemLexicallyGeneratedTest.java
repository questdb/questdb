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

package io.questdb.test.std.fastdouble;

import io.questdb.std.MemoryTag;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;
import io.questdb.std.fastdouble.FastDoubleParser;
import io.questdb.std.str.Utf8s;

import static org.junit.Assert.assertEquals;

public class FastDoubleParserFromMemLexicallyGeneratedTest extends AbstractLexicallyGeneratedTest {

    @Override
    protected void testAgainstJdk(String str) {
        int len = str.length();
        long mem = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            Utf8s.strCpyAscii(str, mem);
            double expected = 0.0;
            boolean isExpectedToFail = false;
            try {
                expected = Double.parseDouble(str);
            } catch (NumberFormatException t) {
                isExpectedToFail = true;
            }

            double actual = 0;
            boolean actualFailed = false;
            try {
                actual = FastDoubleParser.parseDouble(mem, len, false);
            } catch (NumericException t) {
                actualFailed = true;
            }

            assertEquals(isExpectedToFail, actualFailed);
            if (!isExpectedToFail) {
                assertEquals("str=" + str, expected, actual, 0.001);
                assertEquals("longBits of " + expected,
                        Double.doubleToLongBits(expected),
                        Double.doubleToLongBits(actual)
                );
            }
        } finally {
            Unsafe.free(mem, len, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
