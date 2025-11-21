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

package io.questdb.test.griffin;

import io.questdb.griffin.JsonSink;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

public class JsonSinkTest {

    @Test
    public void testArray() {
        JsonSink json = new JsonSink();
        StringSink sink = new StringSink();
        json.of(sink, true)  // formatted
                .startObject()
                .key("items").startArray()
                .val("item1")
                .val("item2")
                .val("item3")
                .endArray()
                .endObject();

        String expected = "{\n" +
                "  \"items\": [\n" +
                "    \"item1\",\n" +
                "    \"item2\",\n" +
                "    \"item3\"\n" +
                "  ]\n" +
                "}";
        Assert.assertEquals(expected, sink.toString());
    }

    @Test
    public void testClear() {
        JsonSink json = new JsonSink();
        StringSink sink = new StringSink();
        json.of(sink, true)  // formatted
                .startObject()
                .key("test").val("value")
                .endObject();

        String expected1 = "{\n" +
                "  \"test\": \"value\"\n" +
                "}";
        Assert.assertEquals(expected1, sink.toString());

        json.clear().startObject()
                .key("new").val("data")
                .endObject();

        String expected2 = "{\n" +
                "  \"new\": \"data\"\n" +
                "}";
        Assert.assertEquals(expected2, sink.toString());
    }

    @Test
    public void testComplexStructure() {
        JsonSink json = new JsonSink();
        StringSink sink = new StringSink();
        json.of(sink, true)  // formatted
                .startObject()
                .key("users").startArray()
                .startObject()
                .key("id").val(1)
                .key("name").val("User1")
                .key("active").val(true)
                .endObject()
                .startObject()
                .key("id").val(2)
                .key("name").val("User2")
                .key("active").val(false)
                .endObject()
                .endArray()
                .endObject();

        String expected = "{\n" +
                "  \"users\": [\n" +
                "    {\n" +
                "      \"id\": 1,\n" +
                "      \"name\": \"User1\",\n" +
                "      \"active\": true\n" +
                "    },\n" +
                "    {\n" +
                "      \"id\": 2,\n" +
                "      \"name\": \"User2\",\n" +
                "      \"active\": false\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        Assert.assertEquals(expected, sink.toString());
    }

    @Test
    public void testEscaping() {
        JsonSink json = new JsonSink();
        StringSink sink = new StringSink();
        json.of(sink, true)  // formatted
                .startObject()
                .key("quoted").val("He said \"hello\"")
                .key("newline").val("line1\nline2")
                .key("tab").val("col1\tcol2")
                .endObject();

        String expected = "{\n" +
                "  \"quoted\": \"He said \\\"hello\\\"\",\n" +
                "  \"newline\": \"line1\\nline2\",\n" +
                "  \"tab\": \"col1\\tcol2\"\n" +
                "}";
        Assert.assertEquals(expected, sink.toString());
    }

    @Test
    public void testMixedTypes() {
        JsonSink json = new JsonSink();
        StringSink sink = new StringSink();
        json.of(sink, true)  // formatted
                .startObject()
                .key("string").val("text")
                .key("integer").val(42)
                .key("long").val(123456789L)
                .key("double").val(3.14)
                .key("float").val(2.71f)
                .key("boolean").val(true)
                .key("null_value").valNull()
                .endObject();

        String expected = "{\n" +
                "  \"string\": \"text\",\n" +
                "  \"integer\": 42,\n" +
                "  \"long\": 123456789,\n" +
                "  \"double\": 3.14,\n" +
                "  \"float\": 2.71,\n" +
                "  \"boolean\": true,\n" +
                "  \"null_value\": null\n" +
                "}";
        Assert.assertEquals(expected, sink.toString());
    }

    @Test
    public void testNestedObject() {
        JsonSink json = new JsonSink();
        StringSink sink = new StringSink();
        json.of(sink, true)  // formatted
                .startObject()
                .key("person").startObject()
                .key("name").val("Alice")
                .key("age").val(25)
                .endObject()
                .endObject();

        String expected = "{\n" +
                "  \"person\": {\n" +
                "    \"name\": \"Alice\",\n" +
                "    \"age\": 25\n" +
                "  }\n" +
                "}";
        Assert.assertEquals(expected, sink.toString());
    }

    @Test
    public void testNonFormattedJson() {
        JsonSink json = new JsonSink();
        StringSink sink = new StringSink();
        json.of(sink, false)  // non-formatted
                .startObject()
                .key("name").val("Alice")
                .key("age").val(30)
                .key("items").startArray()
                .val("a")
                .val("b")
                .endArray()
                .endObject();

        String expected = "{\"name\":\"Alice\",\"age\":30,\"items\":[\"a\",\"b\"]}";
        Assert.assertEquals(expected, sink.toString());
    }

    @Test
    public void testNonFormattedWithCustomSink() {
        JsonSink json = new JsonSink();
        StringSink customSink = new StringSink();
        json.of(customSink, false)  // non-formatted
                .startObject()
                .key("data").val("value")
                .endObject();

        String expected = "{\"data\":\"value\"}";
        Assert.assertEquals(expected, customSink.toString());
    }

    @Test
    public void testSimpleObject() {
        JsonSink json = new JsonSink();
        StringSink sink = new StringSink();
        json.of(sink, true)  // formatted
                .startObject()
                .key("name").val("John")
                .key("age").val(30)
                .endObject();

        String expected = "{\n" +
                "  \"name\": \"John\",\n" +
                "  \"age\": 30\n" +
                "}";
        Assert.assertEquals(expected, sink.toString());
    }

    @Test
    public void testWithCustomCharSink() {
        JsonSink json = new JsonSink();
        StringSink customSink = new StringSink();
        json.of(customSink, true)  // formatted
                .startObject()
                .key("message").val("Hello from custom sink")
                .endObject();

        String expected = "{\n" +
                "  \"message\": \"Hello from custom sink\"\n" +
                "}";
        Assert.assertEquals(expected, customSink.toString());
    }

    @Test
    public void testSubstringValue() {
        JsonSink json = new JsonSink();
        StringSink sink = new StringSink();
        json.of(sink, true)  // formatted
                .startObject()
                .key("filename").val("path/to/data.parquet", 8, 20)  // "data.parquet"
                .endObject();

        String expected = "{\n" +
                "  \"filename\": \"data.parquet\"\n" +
                "}";
        Assert.assertEquals(expected, sink.toString());
    }
}
