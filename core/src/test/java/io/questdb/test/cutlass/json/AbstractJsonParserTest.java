/*+*****************************************************************************
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

package io.questdb.test.cutlass.json;

import io.questdb.cutlass.json.AbstractJsonParser;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class AbstractJsonParserTest extends AbstractTest {

    @Test
    public void testClearReusesCopyBuffer() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    CopyingParser parser = new CopyingParser();
                    DirectUtf8Sink sink = new DirectUtf8Sink(256)
            ) {
                sink.put("{\"key1\":\"value1\",\"key2\":\"a value that is longer than the rest\"}");
                parser.parse(sink);
                final long memUsed = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TEXT_PARSER_RSS);

                for (int i = 0; i < 10_000; i++) {
                    parser.clear();
                    parser.parse(sink);
                }

                // after clear() the copy buffer is refilled from the start, it does not grow with every parse
                Assert.assertEquals(memUsed, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_TEXT_PARSER_RSS));
                Assert.assertEquals("[key1,value1,key2,a value that is longer than the rest]", parser.copies.toString());
            }
        });
    }

    @Test
    public void testCopiesSurviveBufferGrowth() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    CopyingParser parser = new CopyingParser();
                    DirectUtf8Sink sink = new DirectUtf8Sink(256)
            ) {
                // a short document first, so that the buffer is small
                sink.put("{\"a\":\"b\"}");
                parser.parse(sink);
                Assert.assertEquals("[a,b]", parser.copies.toString());

                // the next document outgrows the buffer several times during a single parse,
                // the strings copied earlier in the same parse must survive the reallocation
                parser.clear();
                sink.clear();
                final StringBuilder expected = new StringBuilder("[");
                sink.put('{');
                for (int i = 0; i < 100; i++) {
                    if (i > 0) {
                        sink.put(',');
                        expected.append(',');
                    }
                    sink.put("\"key").put(i).put("\":\"value").put(i).put('"');
                    expected.append("key").append(i).append(",value").append(i);
                }
                sink.put('}');
                expected.append(']');
                parser.parse(sink);
                Assert.assertEquals(expected.toString(), parser.copies.toString());
            }
        });
    }

    private static class CopyingParser extends AbstractJsonParser {
        private final ObjList<CharSequence> copies = new ObjList<>();

        CopyingParser() {
            super(4);
        }

        @Override
        public void clear() {
            super.clear();
            copies.clear();
        }

        @Override
        public void onEvent(int code, CharSequence tag, int position) {
            if (tag != null) {
                copies.add(copy(tag));
            }
        }
    }
}
