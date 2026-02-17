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

package io.questdb.test.preferences;

import io.questdb.cairo.CairoException;
import io.questdb.cutlass.json.JsonException;
import io.questdb.preferences.PreferencesParser;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import static io.questdb.test.tools.TestUtils.assertContains;
import static io.questdb.test.tools.TestUtils.assertEquals;
import static org.junit.Assert.fail;

public class PreferencesParserTest extends AbstractCairoTest {

    @Test
    public void testFragmentedInputNotSupported() throws Exception {
        assertMemoryLeak(() -> {
            final CharSequenceObjHashMap<CharSequence> parserMap = new CharSequenceObjHashMap<>();
            try (PreferencesParser parser = new PreferencesParser(engine.getConfiguration(), parserMap)) {
                try (DirectUtf8Sink sink = new DirectUtf8Sink(1024)) {
                    sink.put("{\"key1\":\"value1\",");
                    parser.parse(sink);

                    sink.clear();
                    sink.put("\"key2\":\"va");
                    try {
                        parser.parse(sink);
                        fail();
                    } catch (JsonException e) {
                        assertEquals("JSON lexer cache is disabled", e.getFlyweightMessage());
                    }
                }
            }
        });
    }

    @Test
    public void testParser() throws Exception {
        assertMemoryLeak(() -> {
            final CharSequenceObjHashMap<CharSequence> parserMap = new CharSequenceObjHashMap<>();
            try (PreferencesParser parser = new PreferencesParser(engine.getConfiguration(), parserMap)) {
                try (DirectUtf8Sink sink = new DirectUtf8Sink(1024)) {
                    // positive case
                    sink.put("{\"key1\":\"value1\",\"key2\":\"value2\",\"key3\":\"value3\"}");
                    parser.parse(sink);

                    // nested object
                    assertError(
                            parser,
                            sink,
                            "{\"key1\":\"value1\",\"key2\":{\"key3\":\"value3\"}}",
                            "unexpected input format [code=1, state=1]"
                    );

                    // missing object start
                    assertError(
                            parser,
                            sink,
                            "\"key1\":\"value1\",\"key2\":\"value2\"}",
                            "Unexpected quote '\"'"
                    );

                    // extra closing '}'
                    assertError(
                            parser,
                            sink,
                            "{\"key1\":\"value1\",\"key2\":\"value2\"}}",
                            "Dangling }"
                    );
                }
            }
        });
    }

    private static void assertError(PreferencesParser parser, DirectUtf8Sink sink, String preferences, String errorMessage) {
        parser.clear();
        sink.clear();
        sink.put(preferences);
        try {
            parser.parse(sink);
            fail("Exception expected");
        } catch (CairoException | JsonException e) {
            assertContains(e.getFlyweightMessage(), errorMessage);
        }
    }
}
