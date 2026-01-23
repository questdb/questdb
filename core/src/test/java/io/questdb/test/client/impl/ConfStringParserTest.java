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

package io.questdb.test.client.impl;

import io.questdb.client.impl.ConfStringParser;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public final class ConfStringParserTest {
    private static final StringSink sink = new StringSink();

    @Test
    public void testByteNotCharPosition() {
        String input = "p::n=静;:=42;";
        int pos = assertSchemaOk(input, "p");
        pos = assertNextKeyValueOk(input, pos, "n", "静");
        assertNextKeyError(input, pos, "key must be consist of alpha-numerical ascii characters and underscore, not ':' at position 7");
    }

    @Test
    public void testEmptyValue() {
        String config = "http::addr=;";
        int pos = assertSchemaOk(config, "http");
        pos = assertNextKeyValueOk(config, pos, "addr", "");
        assertNoNext(config, pos);
    }

    @Test
    public void testEqualsCharInValue() {
        String conf = "http::addr===localhost==;user==j=o=e=;pass==blo==ggs==;";
        int pos = assertSchemaOk(conf, "http");
        pos = assertNextKeyValueOk(conf, pos, "addr", "==localhost==");
        pos = assertNextKeyValueOk(conf, pos, "user", "=j=o=e=");
        pos = assertNextKeyValueOk(conf, pos, "pass", "=blo==ggs==");
        assertNoNext(conf, pos);
    }

    @Test
    public void testIncompleteKeyNoValue() {
        String config = "http::host";
        int pos = assertSchemaOk(config, "http");
        pos = assertNextKeyError(config, pos, "incomplete key-value pair before end of input at position 10");
        assertNoNext(config, pos);
    }

    @Test
    public void testInvalidCtrlCharsInValue() {
        int[] badChars = {0x0, 0x1, 0x2, 0x3, 0x4, 0x5,
                0x6, 0x7, 0x8, 0xb, 0xc, 0xe,
                0xf, 0x10, 0x11, 0x12, 0x13, 0x14,
                0x15, 0x16, 0x17, 0x18, 0x19, 0x1a,
                0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x7f, 0x80, 0x8a, 0x9f};
        for (int badChar : badChars) {
            String config = "http::addr=" + (char) badChar + ";";
            int pos = assertSchemaOk(config, "http");
            pos = assertNextKeyOk(config, pos, "addr");

            pos = ConfStringParser.value(config, pos, sink);
            Assert.assertTrue(pos < 0);
            TestUtils.assertContains(sink, "invalid character");
            TestUtils.assertContains(sink, "at position 11");
            assertNoNext(config, pos);
        }
    }

    @Test
    public void testKeyCannotBeEmpty() {
        String config = "http::=;";
        int pos = assertSchemaOk(config, "http");
        pos = assertNextKeyError(config, pos, "empty key");
        assertNoNext(config, pos);
    }

    @Test
    public void testKeyMustConsistsOfAlphanumericalsAndUnderscore() {
        String config = "https::ho st=localhost;";
        int pos = assertSchemaOk(config, "https");
        pos = assertNextKeyError(config, pos, "key must be consist of alpha-numerical ascii characters and underscore, not ' ' at position 9");
        assertNoNext(config, pos);

        config = "tcp::long_key=localhost;";
        pos = assertSchemaOk(config, "tcp");
        pos = assertNextKeyValueOk(config, pos, "long_key", "localhost");
        assertNoNext(config, pos);

        config = "tcp::1long_key=localhost;";
        pos = assertSchemaOk(config, "tcp");
        pos = assertNextKeyValueOk(config, pos, "1long_key", "localhost");
        assertNoNext(config, pos);

        config = "tcp::1long_key_=localhost;";
        pos = assertSchemaOk(config, "tcp");
        pos = assertNextKeyValueOk(config, pos, "1long_key_", "localhost");
        assertNoNext(config, pos);
    }

    @Test
    public void testKeyValue_simple() {
        String config = "http::addr=localhost;user=joe;pass=bloggs;auto_flush_rows=1000;";
        int pos = assertSchemaOk(config, "http");

        pos = assertNextKeyValueOk(config, pos, "addr", "localhost");
        pos = assertNextKeyValueOk(config, pos, "user", "joe");
        pos = assertNextKeyValueOk(config, pos, "pass", "bloggs");
        pos = assertNextKeyValueOk(config, pos, "auto_flush_rows", "1000");
        assertNoNext(config, pos);
    }

    @Test
    public void testKeysCaseSensitive() {
        String config = "http::addr=localhost;USER=joe;pAsS=bloggs;";

        int pos = assertSchemaOk("http::addr=localhost;USER=joe;pAsS=bloggs;", "http");
        assertHasNext(config, pos);
        pos = assertNextKeyValueOk(config, pos, "addr", "localhost");
        pos = assertNextKeyValueOk(config, pos, "USER", "joe");
        pos = assertNextKeyValueOk(config, pos, "pAsS", "bloggs");
        assertNoNext(config, pos);
    }

    @Test
    public void testMissingEquals() {
        String config = "http::addrlocalhost;foo=bar";
        int pos = assertSchemaOk("http::addrlocalhost;foo=bar", "http");
        pos = assertNextKeyError(config, pos, "incomplete key-value pair before end of input at position 19");
        assertNoNext(config, pos);
    }

    @Test
    public void testMissingTrailingSemicolon() {
        String config = "http::host=localhost;port=9000";
        int pos = assertSchemaOk(config, "http");

        assertHasNext(config, pos);
        pos = assertNextKeyValueOk(config, pos, "host", "localhost");
        pos = assertNextKeyOk(config, pos, "port");
        pos = assertNextValueOk(config, pos, "9000");
        assertNoNext(config, pos);
    }

    @Test
    public void testNoParamsWithColons() {
        String config = "http::";
        int pos = assertSchemaOk(config, "http");
        assertNoNext(config, pos);
    }

    @Test
    public void testNoParamsWithoutColons() {
        String config = "http";
        int pos = assertSchemaOk(config, "http");
        assertNoNext(config, pos);
    }

    @Test
    public void testSchemaCaseSensitive() {
        assertSchemaOk("http::addr=localhost;USER=joe;pAsS=bloggs;", "http");
        assertSchemaOk("HTTP::addr=localhost;USER=joe;pAsS=bloggs;", "HTTP");
        assertSchemaOk("HtTp::addr=localhost;USER=joe;pAsS=bloggs;", "HtTp");
    }

    @Test
    public void testSchemaParser() {
        assertSchemaError("::host=localhost;", "empty schema at position 0");
        assertSchemaError("ht tp::", "bad separator, expected ':' got ' ' at position 2");
        assertSchemaError("::", "empty schema at position 0");
        assertSchemaError("", "expected schema identifier, not an empty string at position 0");
        assertSchemaError("httpaddr=localhost;user=joe;pass=bloggs;auto_flush_rows=1000;", "bad separator, expected ':' got '=' at position 8");
        assertSchemaError("http:a::addr=localhost;user=joe;pass=bloggs;auto_flush_rows=1000;", "bad separator, expected '::' got ':a' at position 4");
        assertSchemaError("x;/host=localhost;", "bad separator, expected ':' got ';' at position 1");
        assertSchemaError("x:;host=localhost;", "bad separator, expected '::' got ':;' at position 1");
        assertSchemaError("http://localhost:9000;host=localhost;", "bad separator, expected '::' got ':/' at position 4");
        assertSchemaOk("http::;", "http");
        assertSchemaOk("HTTP::;", "HTTP");
        assertSchemaOk("http::addr=localhost;user=joe;pass=bloggs;auto_flush_rows=1000;", "http");
        assertSchemaOk("TCP::addr=localhost;user=joe;pass=bloggs;auto_flush_rows=1000;", "TCP");
    }

    @Test
    public void testSemicolonEscaping() {
        String config = "http::pass=bl;;oggs;;;";
        int pos = assertSchemaOk(config, "http");
        pos = assertNextKeyValueOk(config, pos, "pass", "bl;oggs;");
        assertNoNext(config, pos);

        config = "http::;;";
        pos = assertSchemaOk(config, "http");
        assertHasNext(config, pos);
        pos = assertNextKeyError(config, pos, "incomplete key-value pair before end of input at position 6");
        assertNoNext(config, pos);

        config = "http::foo=bar;;";
        pos = assertSchemaOk(config, "http");
        assertHasNext(config, pos);
        pos = assertNextKeyOk(config, pos, "foo");
        pos = assertNextValueOk(config, pos, "bar;");
        assertNoNext(config, pos);

        config = "https::foo=;;;;;";
        pos = assertSchemaOk(config, "https");
        assertHasNext(config, pos);
        pos = assertNextKeyValueOk(config, pos, "foo", ";;");
        assertNoNext(config, pos);
    }

    @Test
    public void testValuesCaseSensitive() {
        String config = "http::addr=localhost;user=JOE;pass=bLogGs;";
        int pos = assertSchemaOk(config, "http");
        pos = assertNextKeyValueOk(config, pos, "addr", "localhost");
        pos = assertNextKeyValueOk(config, pos, "user", "JOE");
        pos = assertNextKeyValueOk(config, pos, "pass", "bLogGs");
        assertNoNext(config, pos);
    }

    private static void assertHasNext(CharSequence input, int pos) {
        Assert.assertTrue(ConfStringParser.hasNext(input, pos));
    }

    private static int assertNextKeyError(CharSequence input, int pos, String expectedError) {
        Assert.assertTrue(ConfStringParser.hasNext(input, pos));
        pos = ConfStringParser.nextKey(input, pos, sink);
        Assert.assertTrue(pos < 0);
        TestUtils.assertEquals(expectedError, sink);
        return pos;
    }

    private static int assertNextKeyOk(CharSequence input, int pos, String expectedKey) {
        Assert.assertTrue(ConfStringParser.hasNext(input, pos));
        pos = ConfStringParser.nextKey(input, pos, sink);
        Assert.assertTrue(pos >= 0);
        TestUtils.assertEquals(expectedKey, sink);
        return pos;
    }

    private static int assertNextKeyValueOk(CharSequence input, int pos, String expectedKey, String expectedValue) {
        pos = assertNextKeyOk(input, pos, expectedKey);
        pos = assertNextValueOk(input, pos, expectedValue);
        return pos;
    }

    private static int assertNextValueError(CharSequence input, int pos, String expectedError) {
        pos = ConfStringParser.value(input, pos, sink);
        Assert.assertTrue(pos < 0);
        TestUtils.assertEquals(expectedError, sink);
        return pos;
    }

    private static int assertNextValueOk(CharSequence input, int pos, String expectedValue) {
        pos = ConfStringParser.value(input, pos, sink);
        Assert.assertTrue(pos >= 0);
        TestUtils.assertEquals(expectedValue, sink);
        return pos;
    }

    private static void assertNoNext(CharSequence input, int pos) {
        Assert.assertFalse(ConfStringParser.hasNext(input, pos));
    }

    private static void assertSchemaError(String configString, String expectedMessage) {
        int pos = ConfStringParser.of(configString, sink);
        Assert.assertTrue(pos < 0);
        TestUtils.assertEquals(expectedMessage, sink);
    }

    private static int assertSchemaOk(String configString, String expectedSchema) {
        int pos = ConfStringParser.of(configString, sink);
        Assert.assertTrue(pos >= 0);
        TestUtils.assertEquals(expectedSchema, sink);
        return pos;
    }
}
