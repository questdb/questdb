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

package io.questdb.test.log;

import io.questdb.log.LogAlertSocketWriter;
import io.questdb.log.LogError;
import io.questdb.log.TemplateParser;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class TemplateParserTest {
    private static final Map<String, String> ENV = new HashMap<>();
    private static final TemplateParser parser = new TemplateParser();
    private static final StringSink sink = new StringSink();

    @Test
    public void testChangeFileTimestamp() {
        parser.parseEnv("${date:y}", 0);
        parser.setDateValue(1637091363010000L); // time is always in micros
        TestUtils.assertEquals("2021", parser);
    }

    @Test
    public void testFailedParse() {
        assertFail("$", "Unexpected '$' at position 0");
        assertFail("$$", "Unexpected '$' at position 1");
        assertFail("${}", "Missing expression at position 2");
        int position = ENV.get("JSON_FILE").length() + 1;
        assertFail("$JSON_FILE$", "Unexpected '$' at position " + position);
        assertFail("$COCO$", "Undefined property: COCO");
        assertFail("$ COCO  $", "Undefined property:  COCO  ");
        assertFail("${COCO", "Missing '}' at position 6");
        assertFail("$JSON_FILE}", "Mismatched '{}' at position 10");
        assertFail("${date:}", "Missing expression at position 7");
        assertFail("${date:       }", "Missing expression at position 14");
        assertFail("/a/b/$DATABASE_ROOT/c", "Undefined property: DATABASE_ROOT/c");
    }

    @Test
    public void testKeyOffsets() {
        Map<String, String> props = new HashMap<>();
        props.put("tarzan", "T");
        props.put("jane", "J");
        parser.parse("${date:yyyy}{${tarzan}^$jane}", 0, props);
        TestUtils.assertEquals("1970{T^J}", parser);
        Assert.assertTrue(parser.getKeyOffset("date:") < 0);
        Assert.assertEquals(13, parser.getKeyOffset("tarzan"));
        Assert.assertEquals(23, parser.getKeyOffset("jane"));
    }

    @Test
    public void testParseTemplate() throws IOException {
        try (InputStream is = getClass().getResourceAsStream("/alert-manager-tpt-international.json")) {
            byte[] buff = new byte[1024];
            Assert.assertNotNull(is);
            int len = is.read(buff, 0, buff.length);
            String template = new String(buff, 0, len);
            parser.parseUtf8(template, 0, LogAlertSocketWriter.ALERT_PROPS);
            TestUtils.assertEquals(
                    "[\n" +
                            "  {\n" +
                            "    \"Status\": \"firing\",\n" +
                            "    \"Labels\": {\n" +
                            "      \"alertname\": \"உலகனைத்தும்\",\n" +
                            "      \"category\": \"воно мені не\",\n" +
                            "      \"severity\": \"łódź jeża lub osiem\",\n" +
                            "      \"orgid\": \"GLOBAL\",\n" +
                            "      \"service\": \"QuestDB\",\n" +
                            "      \"namespace\": \"GLOBAL\",\n" +
                            "      \"cluster\": \"GLOBAL\",\n" +
                            "      \"instance\": \"GLOBAL\",\n" +
                            "      \"我能吞下玻璃而不傷身體\": \"ππππππππππππππππππππ 01\"\n" +
                            "    },\n" +
                            "    \"Annotations\": {\n" +
                            "      \"description\": \"ERROR/GLOBAL/GLOBAL/GLOBAL/GLOBAL\",\n" +
                            "      \"message\": \"${ALERT_MESSAGE}\"\n" +
                            "    }\n" +
                            "  }\n" +
                            "]\n" +
                            "\n",
                    parser
            );
        }
    }

    @Test
    public void testSuccessfulParse() {
        assertParseEquals(
                "a/b/f/file.json",
                "a/b/f/file.json",
                "[a/b/f/file.json]"
        );
        assertParseEquals(
                "${date:   dd }",
                "01",
                "[01]"
        );
        assertParseEquals(
                "${date:   dd/MM/y }",
                "01/01/1970",
                "[01/01/1970]"
        );
        assertParseEquals(
                "${date:   MM/dd/y}",
                "01/01/1970",
                "[01/01/1970]"
        );
        assertParseEquals(
                "${date:yyyy-MM-dd HH:mm:ss}",
                "1970-01-01 00:00:00",
                "[1970-01-01 00:00:00]"
        );
        assertParseEquals(
                "${date:yyyy-MM-ddTHH:mm:ss}",
                "1970-01-01T00:00:00",
                "[1970-01-01T00:00:00]"
        );
        assertParseEquals(
                "$JSON_FILE",
                "file.json",
                "[file.json]");
        assertParseEquals(
                "${JSON_FILE}",
                "file.json",
                "[file.json]"
        );
        assertParseEquals(
                "a/b/f/$JSON_FILE",
                "a/b/f/file.json",
                "[a/b/f/,file.json]"
        );
        assertParseEquals(
                "a/b/f/${JSON_FILE}",
                "a/b/f/file.json",
                "[a/b/f/,file.json]"
        );
        assertParseEquals(
                "a/b/f/${date:y}/$JSON_FILE",
                "a/b/f/1970/file.json",
                "[a/b/f/,1970,/,file.json]"
        );
        assertParseEquals(
                "${DATABASE_ROOT}/a/b/f/${date:y}/$JSON_FILE",
                "c:/\\/\\/a/b/f/1970/file.json",
                "[c:/\\/\\,/a/b/f/,1970,/,file.json]");
        assertParseEquals(
                "{}",
                "{}",
                "[{}]"
        );
        assertParseEquals(
                "{$JSON_FILE}",
                "{file.json}",
                "[{,file.json,}]"
        );
        assertParseEquals(
                "{${DATABASE_ROOT}}",
                "{c:/\\/\\}",
                "[{,c:/\\/\\,}]"
        );
        assertParseEquals(
                "{$DATABASE_ROOT}",
                "{c:/\\/\\}",
                "[{,c:/\\/\\,}]"
        );

        Map<String, String> props = new HashMap<>();
        props.put("A", "alpha");
        props.put("B", "betha");
        props.put("C", "cetha");
        props.put("Z", "zetha");
        assertParseEquals(
                "${A}/${B}/${C}/${date:y}/$Z",
                "alpha/betha/cetha/1970/zetha",
                "[alpha,/,betha,/,cetha,/,1970,/,zetha]",
                props
        );
    }

    @Test
    public void testToString() {
        parser.parseEnv("calendar:${date:y}!", 0);
        parser.setDateValue(1637091363010000L); // time is always in micros
        String str = parser.toString();
        TestUtils.assertEquals("calendar:2021!", str);
    }

    private void assertFail(String location, String expected) {
        try {
            parser.parse(location, 0, ENV);
            Assert.fail();
        } catch (LogError t) {
            Assert.assertEquals(expected, t.getMessage());
        }
    }

    private void assertParseEquals(
            String location,
            String expected,
            String expectedLocation,
            Map<String, String> props
    ) {
        parser.parse(location, 0, props);
        sink.clear();
        sink.put(parser.getTemplateNodes());
        TestUtils.assertEquals(expected, parser);
        TestUtils.assertEquals(expectedLocation, sink);
    }

    private void assertParseEquals(String location, String expected, String expectedLocation) {
        assertParseEquals(location, expected, expectedLocation, ENV);
    }

    static {
        ENV.put("JSON_FILE", "file.json");
        ENV.put("DATABASE_ROOT", "c:/\\/\\");
    }
}
