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

package io.questdb.test.std.str;

import io.questdb.std.str.DirectString;
import io.questdb.std.str.DirectUtf16Sink;
import org.junit.Assert;
import org.junit.Test;

public class DirectStringTest {

    @Test
    public void testSubSequence() {
        final String string = "foobar";
        final int stringLen = string.length();
        try (DirectUtf16Sink utf16Sink = new DirectUtf16Sink(stringLen << 2)) {
            utf16Sink.put('f')
                    .put('o')
                    .put('o')
                    .put('b')
                    .put('a')
                    .put('r');
            final DirectString directString = new DirectString().of(utf16Sink.ptr(), utf16Sink.ptr() + utf16Sink.length());
            for (int i = 0; i < stringLen; ++i) {
                for (int j = i; j < stringLen; ++j) {
                    final CharSequence stringSubSequence = string.subSequence(i, j);
                    final CharSequence directStringSubSequence = directString.subSequence(i, j);
                    Assert.assertEquals(stringSubSequence.toString(), directStringSubSequence.toString());
                    Assert.assertEquals(stringSubSequence.length(), directStringSubSequence.length());
                }
            }
        }
    }
}
