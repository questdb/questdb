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

package io.questdb.test.std.str;

import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.StdoutSink;
import io.questdb.std.str.Utf8Sequence;
import org.junit.Test;

/**
 * Output of these tests need to be observed manually in console output.
 */
public class StdoutSinkTest {
    @Test
    public void testBasics() {
        try (StdoutSink out = new StdoutSink(); DirectUtf8Sink utf8 = new DirectUtf8Sink(4096)) {
            System.out.println("Expecting 'hello world' in console output.");
            out.put("hello world\n");
            out.flush();

            System.out.println("Expecting '234' in console output.");
            out.put("123456", 1, 4);
            out.put((byte) '\n');
            out.flush();

            // Stress auto-flushing logic in `put(long lo, long hi)`.
            System.out.println("Expecting `('a' * 100 + '\\n') * 10` followed by `('a' * 100 + '\\n') * 10` in console output.");
            for (int i = 0; i < 10; i++) {
                for (int j = 0; j < 100; ++j) {
                    utf8.put('a');
                }
                utf8.put('\n');
            }
            for (int i = 0; i < 10; i++) {
                for (int j = 0; j < 100; ++j) {
                    utf8.put('b');
                }
                utf8.put('\n');
            }
            out.put(utf8);
            out.flush();

            System.out.println("Expecting 'king crimson' in console output.");
            utf8.clear();
            utf8.put("king crimson\n");
            final Utf8Sequence seq = utf8;
            out.put(seq);
            out.flush();
        }
    }
}
