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

package io.questdb.test.std;

import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class RndTest extends AbstractTest {

    @Test
    public void testGeneratesDecodableUtf8() {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        Utf8StringSink utf8Sink = new Utf8StringSink();
        StringSink utf16Sink = new StringSink();
        for (int i = 0; i < 100; i++) {
            utf8Sink.clear();
            rnd.nextUtf8Str(rnd.nextInt(i + 1) + 1, utf8Sink);
            utf16Sink.clear();
            Assert.assertTrue("generation failed for " + i + " chars", Utf8s.utf8ToUtf16(utf8Sink, utf16Sink));
        }
    }
}
