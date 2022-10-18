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

package io.questdb.std;

import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class StringSinkTest {
    @Test
    public void testUnprintable() {
        StringSink ss = new StringSink();
        ss.putAsPrintable("\u0101abcd\u1234def\u0012");

        TestUtils.assertEquals("āabcdሴdef\\u0012", ss.toString());
    }

    @Test
    public void testUnprintableNewLine() {
        StringSink ss = new StringSink();
        ss.putAsPrintable("\nasd+-~f\r\0 д");

        TestUtils.assertEquals("\\u000aasd+-~f\\u000d\\u0000 д", ss.toString());
    }
}
