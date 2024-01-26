package io.questdb.test.client.impl;

import io.questdb.client.impl.ConfigurationStringParser;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public final class ConfigurationStringParserTest {
    private static final StringSink sink = new StringSink();

    @Before
    public void setUp() {
        sink.clear();
    }

    @Test
    public void testEmptyValue() {
        ConfigurationStringParser parser = new ConfigurationStringParser();
        assertSchemaOk(parser, "http::addr=;", "http");

        Assert.assertTrue(parser.hasNext());
        assertKeyValue(parser, "addr", "");
        Assert.assertFalse(parser.hasNext());
    }

    @Test
    public void testKeyValue_simple() {
        ConfigurationStringParser parser = new ConfigurationStringParser();
        assertSchemaOk(parser, "http::addr=localhost;user=joe;pass=bloggs;auto_flush_rows=1000;", "http");

        Assert.assertTrue(parser.hasNext());
        assertKeyValue(parser, "addr", "localhost");
        assertKeyValue(parser, "user", "joe");
        assertKeyValue(parser, "pass", "bloggs");
        assertKeyValue(parser, "auto_flush_rows", "1000");
        Assert.assertFalse(parser.hasNext());
    }

    @Test
    public void testKeysCaseInsensitive() {
        ConfigurationStringParser parser = new ConfigurationStringParser();
        assertSchemaOk(parser, "http::addr=localhost;USER=joe;pAsS=bloggs;", "http");
        assertKeyValue(parser, "addr", "localhost");
        assertKeyValue(parser, "user", "joe");
        assertKeyValue(parser, "pass", "bloggs");
    }

    @Test
    public void testMissingEquals() {
        ConfigurationStringParser parser = new ConfigurationStringParser();
        assertSchemaOk(parser, "http::addrlocalhost;foo=bar", "http");

        Assert.assertTrue(parser.hasNext());
        Assert.assertFalse(parser.nextKey(sink));
        TestUtils.assertEquals("missing '='", sink);
    }

    @Test
    public void testMissingTrailingSemicolon() {
        ConfigurationStringParser parser = new ConfigurationStringParser();
        assertSchemaOk(parser, "http::addr=localhost", "http");

        Assert.assertTrue(parser.hasNext());
        assertNextKey(parser, "addr");

        Assert.assertFalse(parser.nextValue(sink));
        TestUtils.assertEquals("missing trailing ';'", sink);
    }

    @Test
    public void testSchemaParser() {
        ConfigurationStringParser parser = new ConfigurationStringParser();
        assertSchemaError(parser, "ht tp::", "schema contains a whitespace");
        assertSchemaError(parser, "http::", "missing trailing ';'");
        assertSchemaError(parser, "::", "schema is empty");
        assertSchemaError(parser, "", "schema name must start with schema type, e.g. http::");
        assertSchemaError(parser, "httpaddr=localhost;user=joe;pass=bloggs;auto_flush_rows=1000;", "schema name must start with schema type, e.g. http::");
        assertSchemaError(parser, "http:a::addr=localhost;user=joe;pass=bloggs;auto_flush_rows=1000;", "schema name must start with schema type, e.g. http::");
        assertSchemaOk(parser, "http::;", "http");
        assertSchemaOk(parser, "HTTP::;", "http");
        assertSchemaOk(parser, "http::addr=localhost;user=joe;pass=bloggs;auto_flush_rows=1000;", "http");
        assertSchemaOk(parser, "TCP::addr=localhost;user=joe;pass=bloggs;auto_flush_rows=1000;", "tcp");
    }

    @Test
    public void testValuesCaseSensitive() {
        ConfigurationStringParser parser = new ConfigurationStringParser();
        assertSchemaOk(parser, "http::addr=localhost;user=JOE;pass=bLogGs;", "http");
        assertKeyValue(parser, "addr", "localhost");
        assertKeyValue(parser, "user", "JOE");
        assertKeyValue(parser, "pass", "bLogGs");
    }

    private static void assertKeyValue(ConfigurationStringParser parser, String expectedKey, String expectedValue) {
        assertNextKey(parser, expectedKey);
        assertNextValue(parser, expectedValue);
    }

    private static void assertNextKey(ConfigurationStringParser parser, String expectedKey) {
        Assert.assertTrue(parser.hasNext());
        Assert.assertTrue(parser.nextKey(sink));
        TestUtils.assertEquals(expectedKey, sink);
    }

    private static void assertNextValue(ConfigurationStringParser parser, String expectedValue) {
        Assert.assertTrue(parser.nextValue(sink));
        TestUtils.assertEquals(expectedValue, sink);
    }

    private static void assertSchema(ConfigurationStringParser parser, String configString, boolean expectedReturn, String expectedOutput) {
        Assert.assertEquals(expectedReturn, parser.startFrom(configString, sink));
        Assert.assertEquals(expectedOutput, sink.toString());
    }

    private static void assertSchemaError(ConfigurationStringParser parser, String configString, String expectedMessage) {
        assertSchema(parser, configString, false, expectedMessage);
    }

    private static void assertSchemaOk(ConfigurationStringParser parser, String configString, String expectedSchema) {
        assertSchema(parser, configString, true, expectedSchema);
    }
}
