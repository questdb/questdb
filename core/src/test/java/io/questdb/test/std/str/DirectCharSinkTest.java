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

import io.questdb.std.str.DirectUtf16Sink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class DirectCharSinkTest {

    @Test
    public void testResize() {
        final String expected = "string 0\n" +
                "string 1\n" +
                "string 2\n" +
                "string 3\n" +
                "string 4\n" +
                "string 5\n" +
                "string 6\n" +
                "string 7\n" +
                "string 8\n" +
                "string 9\n" +
                "string 10\n" +
                "string 11\n" +
                "string 12\n" +
                "string 13\n" +
                "string 14\n" +
                "string 15\n" +
                "string 16\n" +
                "string 17\n" +
                "string 18\n" +
                "string 19\n" +
                "string 20\n" +
                "string 21\n" +
                "string 22\n" +
                "string 23\n" +
                "string 24\n" +
                "string 25\n" +
                "string 26\n" +
                "string 27\n" +
                "string 28\n" +
                "string 29\n";

        final int initialCapacity = 16;
        try (DirectUtf16Sink sink = new DirectUtf16Sink(initialCapacity)) {
            for (int i = 0; i < 30; i++) {
                sink.put("string ").put(i).put('\n');
            }
            TestUtils.assertEquals(expected, sink);
            sink.clear();
            for (int i = 0; i < 30; i++) {
                sink.put("string ").put(i).put('\n');
            }
            TestUtils.assertEquals(expected, sink);

            Assert.assertTrue(sink.length() > 0);
            Assert.assertTrue(sink.getCapacity() >= sink.length());
            sink.resetCapacity();
            Assert.assertEquals(0, sink.length());
            Assert.assertEquals(initialCapacity, sink.getCapacity());
        }
    }
}
