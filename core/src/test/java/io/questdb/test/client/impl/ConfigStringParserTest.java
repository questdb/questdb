package io.questdb.test.client.impl;

import io.questdb.client.impl.ConfigStringParser;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public final class ConfigStringParserTest {
    private static final StringSink sink = new StringSink();

    @Before
    public void setUp() {
        sink.clear();
    }

    @Test
    public void testEmptyValue() {
        String config = "http::addr=;";
        int pos = assertSchemaOk(config, "http");
        pos = assertKeyValue(config, pos, "addr", "");
        assertNoNext(config, pos);
    }

    @Test
    public void testKeyCannotBeEmpty() {
        String config = "http::=;";
        int pos = assertSchemaOk(config, "http");

        assertHasNext(config, pos);

        Assert.assertTrue(ConfigStringParser.nextKey(config, pos, sink) < 0);
        TestUtils.assertEquals("empty key", sink);
    }

    @Test
    public void testKeyValue_simple() {
        String config = "http::addr=localhost;user=joe;pass=bloggs;auto_flush_rows=1000;";
        int pos = assertSchemaOk(config, "http");

        pos = assertKeyValue(config, pos, "addr", "localhost");
        pos = assertKeyValue(config, pos, "user", "joe");
        pos = assertKeyValue(config, pos, "pass", "bloggs");
        pos = assertKeyValue(config, pos, "auto_flush_rows", "1000");
        assertNoNext(config, pos);
    }

    @Test
    public void testKeysCaseInsensitive() {
        String config = "http::addr=localhost;USER=joe;pAsS=bloggs;";

        int pos = assertSchemaOk("http::addr=localhost;USER=joe;pAsS=bloggs;", "http");
        assertHasNext(config, pos);
        pos = assertKeyValue(config, pos, "addr", "localhost");
        pos = assertKeyValue(config, pos, "user", "joe");
        pos = assertKeyValue(config, pos, "pass", "bloggs");
        assertNoNext(config, pos);
    }

    @Test
    public void testMissingEquals() {
        String config = "http::addrlocalhost;foo=bar";
        int pos = assertSchemaOk("http::addrlocalhost;foo=bar", "http");

        pos = assertNextKeyError(config, pos, "missing '='");
        assertNoNext(config, pos);
    }

    @Test
    public void testMissingTrailingSemicolon() {
        String config = "http::addr=localhost";
        int pos = assertSchemaOk("http::addr=localhost", "http");

        assertHasNext(config, pos);
        pos = assertNextKey(config, pos, "addr");

        pos = assertNextValueError(config, pos, "missing trailing ';'");
        assertNoNext(config, pos);
    }

    @Test
    public void testSchemaParser() {
        assertSchemaError("ht tp::", "schema contains a whitespace");
        assertSchemaError("http::", "missing trailing ';'");
        assertSchemaError("::", "schema is empty");
        assertSchemaError("", "schema name must start with schema type, e.g. http::");
        assertSchemaError("httpaddr=localhost;user=joe;pass=bloggs;auto_flush_rows=1000;", "schema name must start with schema type, e.g. http::");
        assertSchemaError("http:a::addr=localhost;user=joe;pass=bloggs;auto_flush_rows=1000;", "schema name must start with schema type, e.g. http::");
        assertSchemaOk("http::;", "http");
        assertSchemaOk("HTTP::;", "http");
        assertSchemaOk("http::addr=localhost;user=joe;pass=bloggs;auto_flush_rows=1000;", "http");
        assertSchemaOk("TCP::addr=localhost;user=joe;pass=bloggs;auto_flush_rows=1000;", "tcp");
    }

    @Test
    public void testSemicolonEscaping() {
        String config = "http::pass=bl;;oggs;;;";
        int pos = assertSchemaOk(config, "http");
        assertKeyValue(config, pos, "pass", "bl;oggs;");

        config = "http::;;";
        pos = assertSchemaOk(config, "http");
        assertHasNext(config, pos);
        pos = assertNextKeyError(config, pos, "missing '='");
        assertNoNext(config, pos);

        config = "http::foo=bar;;";
        pos = assertSchemaOk(config, "http");
        assertHasNext(config, pos);
        pos = assertNextKey(config, pos, "foo");
        pos = assertNextValueError(config, pos, "missing trailing ';'");
        assertNoNext(config, pos);

        config = "https::foo=;;;;;";
        pos = assertSchemaOk(config, "https");
        assertHasNext(config, pos);
        pos = assertKeyValue(config, pos, "foo", ";;");
        assertNoNext(config, pos);
    }

    @Test
    public void testValuesCaseSensitive() {
        String config = "http::addr=localhost;user=JOE;pass=bLogGs;";
        int pos = assertSchemaOk(config, "http");
        pos = assertKeyValue(config, pos, "addr", "localhost");
        pos = assertKeyValue(config, pos, "user", "JOE");
        pos = assertKeyValue(config, pos, "pass", "bLogGs");
        assertNoNext(config, pos);
    }

    private static void assertHasNext(CharSequence input, int pos) {
        Assert.assertTrue(ConfigStringParser.hasNext(input, pos));
    }

    private static int assertKeyValue(CharSequence input, int pos, String expectedKey, String expectedValue) {
        pos = assertNextKey(input, pos, expectedKey);
        pos = assertNextValue(input, pos, expectedValue);
        return pos;
    }

    private static int assertNextKey(CharSequence input, int pos, String expectedKey) {
        Assert.assertTrue(ConfigStringParser.hasNext(input, pos));
        pos = ConfigStringParser.nextKey(input, pos, sink);

        Assert.assertTrue(pos >= 0);
        TestUtils.assertEquals(expectedKey, sink);

        return pos;
    }

    private static int assertNextKeyError(CharSequence input, int pos, String expectedError) {
        Assert.assertTrue(ConfigStringParser.hasNext(input, pos));
        pos = ConfigStringParser.nextKey(input, pos, sink);
        Assert.assertTrue(pos < 0);
        TestUtils.assertEquals(expectedError, sink);
        return pos;
    }

    private static int assertNextValue(CharSequence input, int pos, String expectedValue) {
        pos = ConfigStringParser.value(input, pos, sink);
        Assert.assertTrue(pos >= 0);
        TestUtils.assertEquals(expectedValue, sink);
        return pos;
    }

    private static int assertNextValueError(CharSequence input, int pos, String expectedError) {
        pos = ConfigStringParser.value(input, pos, sink);
        Assert.assertTrue(pos < 0);
        TestUtils.assertEquals(expectedError, sink);
        return pos;
    }

    private static void assertNoNext(CharSequence input, int pos) {
        Assert.assertFalse(ConfigStringParser.hasNext(input, pos));
    }

    private static void assertSchemaError(String configString, String expectedMessage) {
        int pos = ConfigStringParser.of(configString, sink);
        Assert.assertTrue(pos < 0);
        TestUtils.assertEquals(expectedMessage, sink);
    }

    private static int assertSchemaOk(String configString, String expectedSchema) {
        int pos = ConfigStringParser.of(configString, sink);
        Assert.assertTrue(pos >= 0);
        TestUtils.assertEquals(expectedSchema, sink);
        return pos;
    }
}
