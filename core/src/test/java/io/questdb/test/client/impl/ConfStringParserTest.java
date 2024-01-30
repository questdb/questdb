package io.questdb.test.client.impl;

import io.questdb.client.impl.ConfStringParser;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public final class ConfStringParserTest {
    private static final StringSink sink = new StringSink();

    @Before
    public void setUp() {
        sink.clear();
    }

    @Test
    public void testByteNotCharPosition() {
        String input = "p::n=静;:=42;";
        int pos = assertSchemaOk(input, "p");
        pos = assertNextKeyValueOk(input, pos, "n", "静");
        assertNextKeyError(input, pos, "key must start with a letter, not ':' at position 7");
    }

    @Test
    public void testEmptyValue() {
        String config = "http::addr=;";
        int pos = assertSchemaOk(config, "http");
        pos = assertNextKeyValueOk(config, pos, "addr", "");
        assertNoNext(config, pos);
    }

    @Test
    public void testIncompleteKeyNoValue() {
        String config = "http::host";
        int pos = assertSchemaOk(config, "http");
        pos = assertNextKeyError(config, pos, "incomplete key-value pair before end of input at position 10");
        assertNoNext(config, pos);
    }

    @Test
    public void testKeyCannotBeEmpty() {
        String config = "http::=;";
        int pos = assertSchemaOk(config, "http");
        pos = assertNextKeyError(config, pos, "empty key");
        assertNoNext(config, pos);
    }

    @Test
    public void testKeyMustConsistsOfLettersOrDigits() {
        String config = "https::ho st=localhost;";
        int pos = assertSchemaOk(config, "https");
        pos = assertNextKeyError(config, pos, "key must be consist of letters and digits, not ' ' at position 9");
        assertNoNext(config, pos);
    }

    @Test
    public void testKeyMustNotStartWithDigits() {
        String config = "https::2host=localhost;";
        int pos = assertSchemaOk(config, "https");
        pos = assertNextKeyError(config, pos, "key must start with a letter, not '2' at position 7");
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
    public void testKeysCaseInsensitive() {
        String config = "http::addr=localhost;USER=joe;pAsS=bloggs;";

        int pos = assertSchemaOk("http::addr=localhost;USER=joe;pAsS=bloggs;", "http");
        assertHasNext(config, pos);
        pos = assertNextKeyValueOk(config, pos, "addr", "localhost");
        pos = assertNextKeyValueOk(config, pos, "user", "joe");
        pos = assertNextKeyValueOk(config, pos, "pass", "bloggs");
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
        pos = assertNextValueError(config, pos, "missing trailing semicolon at position 30");
        assertNoNext(config, pos);
    }

    @Test
    public void testSchemaParser() {
        assertSchemaError("ht tp::", "bad separator, expected ':' got ' ' at position 2");
        assertSchemaError("http::", "missing trailing semicolon at position 5");
        assertSchemaError("::", "schema is empty");
        assertSchemaError("", "schema name must start with schema type, e.g. http::");
        assertSchemaError("httpaddr=localhost;user=joe;pass=bloggs;auto_flush_rows=1000;", "bad separator, expected ':' got '=' at position 8");
        assertSchemaError("http:a::addr=localhost;user=joe;pass=bloggs;auto_flush_rows=1000;", "bad separator, expected '::' got ':a' at position 4");
        assertSchemaError("x;/host=localhost;", "bad separator, expected ':' got ';' at position 1");
        assertSchemaError("x:;host=localhost;", "bad separator, expected '::' got ':;' at position 1");
        assertSchemaError("http://localhost:9000;host=localhost;", "bad separator, expected '::' got ':/' at position 4");
        assertSchemaOk("http::;", "http");
        assertSchemaOk("HTTP::;", "http");
        assertSchemaOk("http::addr=localhost;user=joe;pass=bloggs;auto_flush_rows=1000;", "http");
        assertSchemaOk("TCP::addr=localhost;user=joe;pass=bloggs;auto_flush_rows=1000;", "tcp");
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
        pos = assertNextValueError(config, pos, "missing trailing semicolon at position 15");
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
