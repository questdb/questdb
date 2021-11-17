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

import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

public class LocationParserTest {

    private LocationParser locationParser;
    private StringSink sink;

    @Before
    public void setUp() {
        locationParser = new LocationParser();
        sink = new StringSink();
        System.setProperty("JSON_FILE", "file.json");
        System.setProperty("DATABASE_ROOT", "c:/\\/\\");
    }

    @Test
    public void testSuccessfulParse() {
        assertParseEquals("a/b/f/file.json", "[a/b/f/file.json]");
        assertParseEquals("${date:   dd }", "[   01 ]");
        assertParseEquals("${date:   dd/MM/y }", "[   01/01/1970 ]");
        assertParseEquals("${date:   MM/dd/y}", "[   01/01/1970]");
        assertParseEquals("${date:yyyy-MM-dd HH:mm:ss}", "[1970-01-01 00:00:00]");
        assertParseEquals("${date:yyyy-MM-ddTHH:mm:ss}", "[1970-01-01T00:00:00]");
        assertParseEquals("$JSON_FILE", "[file.json]");
        assertParseEquals("a/b/f/$JSON_FILE", "[a/b/f/,file.json]");
        assertParseEquals("a/b/f/${date:y}/$JSON_FILE", "[a/b/f/,1970,/,file.json]");
        assertParseEquals("${DATABASE_ROOT}/a/b/f/${date:y}/$JSON_FILE", "[c:/\\/\\,/a/b/f/,1970,/,file.json]");

        Properties properties = new Properties();
        properties.put("A", "alpha");
        properties.put("B", "betha");
        properties.put("C", "cetha");
        properties.put("Z", "zetha");
        assertParseEquals("${A}/${B}/${C}/${date:y}/$Z", "[alpha,/,betha,/,cetha,/,1970,/,zetha]", properties);
    }

    @Test
    public void testFailedParse() {
        assertFail("$", "Unexpected '$' at position 0");
        assertFail("$$", "Unexpected '$' at position 1");
        int position = System.getProperty("JSON_FILE").length() + 1;
        assertFail("$JSON_FILE$", "Unexpected '$' at position " + position);
        assertFail("$COCO$", "Undefined property: COCO");
        assertFail("$ COCO  $", "Undefined property: COCO");
        assertFail("{}", "Missing '$' at position 0");
        assertFail("${COCO", "Missing '}' at position 6");
        assertFail("$COCO}", "Unexpected '}' at position 5");
        assertFail("${date:}", "Missing expression at position 7");
        //assertFail("${date: Ketchup}", "Unexpected '}' at position 5"); // TODO: the compiler (TimestampFormatCompiler) should explode, but it does not
    }

    @Test
    public void testChangeFileTimestamp() {
        try (Path path = new Path()) {
            locationParser.parse("${date:y}", 0);
            locationParser.setFileTimestamp(1637091363010000L); // time always in micros
            locationParser.buildFilePath(path);
            Assert.assertEquals("2021", path.toString());
        }
    }

    private void assertParseEquals(String location, String expected) {
        assertParseEquals(location, expected, System.getProperties());
    }

    private void assertParseEquals(String location, String expected, Properties properties) {
        try (Path path = new Path()) {
            locationParser.parse(location, 0, properties);
            locationParser.buildFilePath(path);
            sink.clear();
            locationParser.getLocationComponents().toSink(sink);
            Assert.assertEquals(expected, sink.toString());
        }
    }

    private void assertFail(String location, String expected) {
        try {
            locationParser.parse(location, 0);
            sink.clear();
            locationParser.getLocationComponents().toSink(sink);
            System.out.printf(">>%s%n", sink);
            Assert.fail();
        } catch (LogError t) {
            Assert.assertEquals(expected, t.getMessage());
        }
    }
}
