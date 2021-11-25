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

package io.questdb.log;

import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class DollarExprTest {
    private static final Map<String, String> ENV = new HashMap<>();

    static {
        ENV.put("JSON_FILE", "file.json");
        ENV.put("DATABASE_ROOT", "c:/\\/\\");
    }

    private DollarExpr dollar$;
    private StringSink sink;

    @Before
    public void setUp() {
        dollar$ = new DollarExpr();
        sink = new StringSink();
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
                "   01 ",
                "[   01 ]"
        );
        assertParseEquals(
                "${date:   dd/MM/y }",
                "   01/01/1970 ",
                "[   01/01/1970 ]"
        );
        assertParseEquals(
                "${date:   MM/dd/y}",
                "   01/01/1970",
                "[   01/01/1970]"
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
        assertFail("/a/b/$DATABASE_ROOT/c", "Undefined property: DATABASE_ROOT/c");
    }

    @Test
    public void testChangeFileTimestamp() {
        dollar$.resolveEnv("${date:y}", 0);
        dollar$.setDateValue(1637091363010000L); // time always in micros
        Assert.assertEquals("2021", dollar$.toString());
    }

    @Test
    public void testKeyOffsets() {
        Map<String, String> props = new HashMap<>();
        props.put("tarzan", "T");
        props.put("jane", "J");
        dollar$.resolve("${date:yyyy}{${tarzan}^$jane}", 0, props);
        Assert.assertEquals("1970{T^J}", dollar$.toString());
        Assert.assertTrue(dollar$.getKeyOffset("date:") < 0);
        Assert.assertEquals(dollar$.getKeyOffset("tarzan"), 13);
        Assert.assertEquals(dollar$.getKeyOffset("jane"), 23);
    }

    private void assertParseEquals(String location, String expected, String expectedLocation) {
        assertParseEquals(location, expected, expectedLocation, ENV);
    }

    private void assertParseEquals(
            String location,
            String expected,
            String expectedLocation,
            Map<String, String> props
    ) {
        dollar$.resolve(location, 0, props);
        sink.clear();
        sink.put(dollar$.getLocationComponents());
        Assert.assertEquals(expected, dollar$.toString());
        Assert.assertEquals(expectedLocation, sink.toString());
    }

    private void assertFail(String location, String expected) {
        try {
            dollar$.resolve(location, 0, ENV);
            Assert.fail();
        } catch (LogError t) {
            Assert.assertEquals(expected, t.getMessage());
        }
    }
}
