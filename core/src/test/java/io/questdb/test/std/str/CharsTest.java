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

package io.questdb.test.std.str;

import io.questdb.cairo.CairoException;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.ObjList;
import io.questdb.std.str.FileNameExtractorUtf8Sequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.griffin.engine.TestBinarySequence;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Random;

public class CharsTest {
    private static final FileNameExtractorUtf8Sequence extractor = new FileNameExtractorUtf8Sequence();

    @Test
    public void testBase64Decode() {
        String encoded = "+W8kK89c79Jb97CrQM3aGuPJE85fEdFoXwSsEWjU736IXm4v7+mZKiOL82uYhGaxmIYUUJh5/Xj44tX0NrD4lQ==";
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        Chars.base64Decode(encoded, buffer);
        buffer.flip();

        byte[] decode = Base64.getDecoder().decode(encoded);
        Assert.assertEquals(decode.length, buffer.remaining());
        for (byte b : decode) {
            Assert.assertEquals(b, buffer.get());
        }
    }

    @Test
    public void testBase64DecodeMiscLengths() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            sb.setLength(0);
            for (int j = 0; j < i; j++) {
                sb.append(j % 10);
            }
            String encoded = Base64.getEncoder().encodeToString(sb.toString().getBytes());

            ByteBuffer buffer = ByteBuffer.allocate(1024);
            Chars.base64Decode(encoded, buffer);
            buffer.flip();

            byte[] decode = Base64.getDecoder().decode(encoded);
            Assert.assertEquals(decode.length, buffer.remaining());
            for (byte b : decode) {
                Assert.assertEquals(b, buffer.get());
            }
        }
    }

    @Test
    public void testBase64DecodeUtf8() {
        String encoded = Base64.getEncoder().encodeToString("аз съм грут:गाजर का हलवा".getBytes());
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        Chars.base64Decode(encoded, buffer);
        buffer.flip();

        byte[] decode = Base64.getDecoder().decode(encoded);
        Assert.assertEquals(decode.length, buffer.remaining());
        for (byte b : decode) {
            Assert.assertEquals(b, buffer.get());
        }
    }

    @Test
    public void testBase64Encode() {
        final StringSink sink = new StringSink();
        final TestBinarySequence testBinarySequence = new TestBinarySequence();
        sink.clear();
        Chars.base64Encode(testBinarySequence.of("this is a test".getBytes()), 100, sink);
        Assert.assertEquals("dGhpcyBpcyBhIHRlc3Q=", sink.toString());
        sink.clear();
        Chars.base64Encode(testBinarySequence.of("this is a test".getBytes()), 4, sink);
        Assert.assertEquals("dGhpcw==", sink.toString());
        // ignore the null
        Chars.base64Encode(null, 4, sink);
        Assert.assertEquals("dGhpcw==", sink.toString());

        // random part
        Random rand = new Random(System.currentTimeMillis());
        int len = rand.nextInt(100) + 1;
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte) rand.nextInt(0xFF);
        }
        testBinarySequence.of(bytes);
        sink.clear();
        Chars.base64Encode(testBinarySequence, (int) testBinarySequence.length(), sink);
        byte[] decoded = Base64.getDecoder().decode(sink.toString());
        Assert.assertArrayEquals(bytes, decoded);
    }

    @Test
    public void testBase64UrlDecode_ByteBuffer() {
        String s = "this is a test";
        String encoded = Base64.getUrlEncoder().encodeToString(s.getBytes());
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        Chars.base64UrlDecode(encoded, buffer);
        buffer.flip();
        String s2 = new String(buffer.array(), buffer.position(), buffer.remaining());
        TestUtils.assertEquals(s, s2);

        // null is ignored
        buffer.clear();
        Chars.base64UrlDecode(null, buffer);
        Assert.assertEquals(0, buffer.position());

        // single char with no padding
        buffer.clear();
        Chars.base64UrlDecode(Base64.getUrlEncoder().encodeToString("a".getBytes(StandardCharsets.UTF_8)), buffer);
        buffer.flip();
        Assert.assertEquals(1, buffer.remaining());
        Assert.assertEquals('a', buffer.get());

        // empty string
        buffer.clear();
        Chars.base64UrlDecode("", buffer);
        buffer.flip();
        Assert.assertEquals(0, buffer.remaining());

        // single char is invalid
        buffer.clear();
        try {
            Chars.base64UrlDecode("a", buffer);
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "invalid base64 encoding");
        }

        // empty string with padding
        buffer.clear();
        Chars.base64UrlDecode("===", buffer);
        buffer.flip();
        Assert.assertEquals(0, buffer.remaining());

        // non-ascii in input
        buffer.clear();
        try {
            Chars.base64UrlDecode("a\u00A0", buffer);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "non-ascii character while decoding base64");
        }

        // ascii but not base64
        buffer.clear();
        try {
            Chars.base64UrlDecode("a\u0001", buffer);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "invalid base64 character [ch=\u0001]");
        }

        // random part
        Random rand = new Random(System.currentTimeMillis());
        int len = rand.nextInt(100) + 1;
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte) rand.nextInt(0xFF);
        }
        encoded = Base64.getUrlEncoder().encodeToString(bytes);
        buffer.clear();
        Chars.base64UrlDecode(encoded, buffer);
        buffer.flip();
        byte[] decoded = new byte[buffer.remaining()];
        buffer.get(decoded);
        Assert.assertArrayEquals(bytes, decoded);
    }

    @Test
    public void testBase64UrlDecode_Utf8Sink() {
        String s = "this is a test";
        String encoded = Base64.getUrlEncoder().encodeToString(s.getBytes());
        Utf8StringSink sink = new Utf8StringSink();
        Chars.base64UrlDecode(encoded, sink);
        TestUtils.assertEquals(s, sink);

        // null is ignored
        sink.clear();
        Chars.base64UrlDecode(null, sink);
        Assert.assertEquals(0, sink.size());

        // single char with no padding
        sink.clear();
        Chars.base64UrlDecode(Base64.getUrlEncoder().encodeToString("a".getBytes(StandardCharsets.UTF_8)), sink);
        Assert.assertEquals(1, sink.size());
        TestUtils.assertEquals("a", sink);

        // empty string
        sink.clear();
        Chars.base64UrlDecode("", sink);
        Assert.assertEquals(0, sink.size());

        // single char is invalid
        sink.clear();
        try {
            Chars.base64UrlDecode("a", sink);
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "invalid base64 encoding");
        }

        // empty string with padding
        sink.clear();
        Chars.base64UrlDecode("===", sink);
        Assert.assertEquals(0, sink.size());

        // non-ascii in input
        sink.clear();
        try {
            Chars.base64UrlDecode("a\u00A0", sink);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "non-ascii character while decoding base64");
        }

        // ascii but not base64
        sink.clear();
        try {
            Chars.base64UrlDecode("a\u0001", sink);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "invalid base64 character [ch=\u0001]");
        }
    }

    @Test
    public void testBase64UrlEncode() {
        final StringSink sink = new StringSink();
        final TestBinarySequence testBinarySequence = new TestBinarySequence();
        sink.clear();
        Chars.base64UrlEncode(testBinarySequence.of("this is a test".getBytes()), 100, sink);
        Assert.assertEquals("dGhpcyBpcyBhIHRlc3Q", sink.toString());
        sink.clear();
        Chars.base64UrlEncode(testBinarySequence.of("this is a test".getBytes()), 4, sink);
        Assert.assertEquals("dGhpcw", sink.toString());
        // ignore the null
        Chars.base64UrlEncode(null, 4, sink);
        Assert.assertEquals("dGhpcw", sink.toString());

        // random part
        Random rand = new Random(System.currentTimeMillis());
        int len = rand.nextInt(100) + 1;
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = (byte) rand.nextInt(0xFF);
        }
        testBinarySequence.of(bytes);
        sink.clear();
        Chars.base64UrlEncode(testBinarySequence, (int) testBinarySequence.length(), sink);
        byte[] decoded = Base64.getUrlDecoder().decode(sink.toString());
        Assert.assertArrayEquals(bytes, decoded);
    }

    @Test
    public void testContainsWordIgnoreCase() {
        // --- Positive Cases ---
        // Middle
        Assert.assertTrue(Chars.containsWordIgnoreCase("a b c", "b", ' '));
        Assert.assertTrue(Chars.containsWordIgnoreCase("alpha beta gamma", "beta", ' '));
        Assert.assertTrue(Chars.containsWordIgnoreCase("a,b,c", "b", ','));
        // Start
        Assert.assertTrue(Chars.containsWordIgnoreCase("b c d", "b", ' '));
        Assert.assertTrue(Chars.containsWordIgnoreCase("beta gamma", "beta", ' '));
        Assert.assertTrue(Chars.containsWordIgnoreCase("b,c,d", "b", ','));
        // End
        Assert.assertTrue(Chars.containsWordIgnoreCase("a b c", "c", ' '));
        Assert.assertTrue(Chars.containsWordIgnoreCase("alpha beta", "beta", ' '));
        Assert.assertTrue(Chars.containsWordIgnoreCase("a,b,c", "c", ','));
        // Single word sequence
        Assert.assertTrue(Chars.containsWordIgnoreCase("word", "word", ' '));
        Assert.assertTrue(Chars.containsWordIgnoreCase("word", "word", ','));
        // Multiple occurrences
        Assert.assertTrue(Chars.containsWordIgnoreCase("a b a", "a", ' '));
        Assert.assertTrue(Chars.containsWordIgnoreCase("beta alpha beta", "beta", ' '));
        Assert.assertTrue(Chars.containsWordIgnoreCase("a b,c d", "b,c", ' '));

        // --- Negative Cases ---
        // Term not present
        Assert.assertFalse(Chars.containsWordIgnoreCase("a b c", "d", ' '));
        Assert.assertFalse(Chars.containsWordIgnoreCase("alpha beta", "gamma", ' '));
        // Term is substring (start)
        Assert.assertFalse(Chars.containsWordIgnoreCase("abc d", "ab", ' '));
        Assert.assertFalse(Chars.containsWordIgnoreCase("alphabet soup", "alpha", ' '));
        // Term is substring (middle) - not preceded by separator
        Assert.assertFalse(Chars.containsWordIgnoreCase("xabc d", "abc", ' '));
        Assert.assertFalse(Chars.containsWordIgnoreCase("alphabet soup", "lphabe", ' '));
        // Term is substring (middle) - not followed by separator
        Assert.assertFalse(Chars.containsWordIgnoreCase("a bcd", "bc", ' '));
        Assert.assertFalse(Chars.containsWordIgnoreCase("alpha beta", "bet", ' '));
        // Term is substring (end)
        Assert.assertFalse(Chars.containsWordIgnoreCase("the alphabet", "bet", ' '));
        // Incorrect separator used in check
        Assert.assertFalse(Chars.containsWordIgnoreCase("a,b,c", "b", ' '));
        Assert.assertFalse(Chars.containsWordIgnoreCase("a b c", "b", ','));
        // Term matches but wrong separator before
        Assert.assertFalse(Chars.containsWordIgnoreCase("a,b c", "b", ' '));
        // Term matches but wrong separator after
        Assert.assertFalse(Chars.containsWordIgnoreCase("a b,c", "b", ' '));
        // Term contains separator char, but boundaries don't match separator
        Assert.assertFalse(Chars.containsWordIgnoreCase("a b,c d", "b,c", ',')); // Space before/after != ','

        // --- Edge Cases ---
        // Null inputs
        Assert.assertFalse(Chars.containsWordIgnoreCase(null, "a", ' '));
        Assert.assertFalse(Chars.containsWordIgnoreCase("a b c", null, ' '));
        Assert.assertFalse(Chars.containsWordIgnoreCase(null, null, ' '));
        // Empty inputs (Assuming empty term is not a word)
        Assert.assertFalse(Chars.containsWordIgnoreCase("", "a", ' '));
        Assert.assertFalse(Chars.containsWordIgnoreCase("a b c", "", ' '));
        Assert.assertFalse(Chars.containsWordIgnoreCase("", "", ' '));
        Assert.assertFalse(Chars.containsWordIgnoreCase(" ", "", ' '));
        // Sequence equals term
        Assert.assertTrue(Chars.containsWordIgnoreCase("abc", "abc", ' '));
        Assert.assertTrue(Chars.containsWordIgnoreCase("abc", "abc", ','));
        // Sequence contains only separators
        Assert.assertFalse(Chars.containsWordIgnoreCase("   ", "a", ' '));
        Assert.assertFalse(Chars.containsWordIgnoreCase(",,,", "a", ','));
        // Using StringBuilder (different CharSequence type)
        Assert.assertTrue(Chars.containsWordIgnoreCase(new StringBuilder("a b c"), "b", ' '));
        Assert.assertFalse(Chars.containsWordIgnoreCase(new StringBuilder("abc d"), "ab", ' '));
        Assert.assertFalse(Chars.containsWordIgnoreCase(new StringBuilder("a b c"), null, ' '));
        Assert.assertFalse(Chars.containsWordIgnoreCase(new StringBuilder("a b c"), "", ' '));
    }

    @Test
    public void testEmptyString() {
        TestUtils.assertEquals("", extractor.of(Utf8String.EMPTY));
    }

    @Test
    public void testEndsWith() {
        Assert.assertFalse(Chars.endsWith(null, null));
        Assert.assertFalse(Chars.endsWith("a", null));
        Assert.assertFalse(Chars.endsWith(null, "a"));
        Assert.assertFalse(Chars.endsWith("", "a"));
        Assert.assertTrue(Chars.endsWith("a", ""));
        Assert.assertFalse(Chars.endsWith("ab", "abc"));
        Assert.assertFalse(Chars.endsWith("abc", "x"));
        Assert.assertTrue(Chars.endsWith("abcd", "cd"));
    }

    @Test
    public void testEqualsLowerCaseAscii() {
        Assert.assertTrue(Chars.equalsLowerCaseAscii("foo bar baz", "foo bar baz", 0, 11));
        Assert.assertTrue(Chars.equalsLowerCaseAscii("foo bar baz", "FoO bAr BaZ", 0, 11));
        Assert.assertTrue(Chars.equalsLowerCaseAscii("foo bar", "foo bar baz", 0, 7));
        Assert.assertTrue(Chars.equalsLowerCaseAscii("foo bar", "bar foo bar baz", 4, 11));
        Assert.assertFalse(Chars.equalsLowerCaseAscii("foo bar baz", "foo bar", 0, 7));
        Assert.assertTrue(Chars.equalsLowerCaseAscii("foo_bar_baz", "foo_BAR_baz", 0, 11));
        Assert.assertFalse(Chars.equalsLowerCaseAscii("foo_bar_baz", "foo_foo_baz", 0, 11));
    }

    @Test
    public void testIPv4ToString() {
        Assert.assertEquals("255.255.255.255", TestUtils.ipv4ToString(0xffffffff));
        Assert.assertEquals("0.0.0.25", TestUtils.ipv4ToString(25));
    }

    @Test
    public void testIndexOf() {
        Assert.assertEquals(4, Chars.indexOf("foo bar baz", 0, 11, "bar"));
        Assert.assertEquals(4, Chars.indexOf("foo bar baz", 0, 11, "ba"));
        Assert.assertEquals(8, Chars.indexOf("foo bar baz", 6, 11, "ba"));
        Assert.assertEquals(1, Chars.indexOf("foo bar baz", 0, 7, "oo"));
        Assert.assertEquals(0, Chars.indexOf("foo bar baz", 2, 4, ""));
        Assert.assertEquals(-1, Chars.indexOf("foo bar baz", 0, 11, "BaR"));
        Assert.assertEquals(-1, Chars.indexOf("foo bar baz", 2, 4, "y"));
        Assert.assertEquals(-1, Chars.indexOf("", 0, 0, "oo"));
        Assert.assertEquals(-1, Chars.indexOf("", 0, 0, "y"));
    }

    @Test
    public void testIndexOfIgnoreCase() {
        Assert.assertEquals(4, Chars.indexOfIgnoreCase("foo bar baz", 0, 11, "bar"));
        Assert.assertEquals(4, Chars.indexOfIgnoreCase("foo bar baz", 0, 11, "BAR"));

        Assert.assertEquals(4, Chars.indexOfIgnoreCase("FOO BAR BAZ", 0, 11, "bar"));
        Assert.assertEquals(4, Chars.indexOfIgnoreCase("FOO BAR BAZ", 0, 11, "BAR"));

        Assert.assertEquals(4, Chars.indexOfIgnoreCase("foo bar baz", 0, 11, "ba"));
        Assert.assertEquals(4, Chars.indexOfIgnoreCase("foo bar baz", 0, 11, "BA"));

        Assert.assertEquals(8, Chars.indexOfIgnoreCase("foo BAr BAz", 6, 11, "ba"));
        Assert.assertEquals(8, Chars.indexOfIgnoreCase("foo BAr BAz", 6, 11, "BA"));

        Assert.assertEquals(1, Chars.indexOfIgnoreCase("foo bar baz", 0, 7, "oo"));
        Assert.assertEquals(1, Chars.indexOfIgnoreCase("foo bar baz", 0, 7, "OO"));

        Assert.assertEquals(0, Chars.indexOfIgnoreCase("foo bar baz", 2, 4, ""));
        Assert.assertEquals(-1, Chars.indexOfIgnoreCase("foo bar baz", 2, 4, "y"));
        Assert.assertEquals(-1, Chars.indexOfIgnoreCase("foo bar baz", 2, 4, "Y"));
        Assert.assertEquals(-1, Chars.indexOfIgnoreCase("", 0, 0, "oo"));
        Assert.assertEquals(-1, Chars.indexOfIgnoreCase("", 0, 0, "OO"));
        Assert.assertEquals(-1, Chars.indexOfIgnoreCase("", 0, 0, "y"));
        Assert.assertEquals(-1, Chars.indexOfIgnoreCase("", 0, 0, "y"));
    }

    @Test
    public void testIndexOfLowerCase() {
        Assert.assertEquals(4, Chars.indexOfLowerCase("foo bar baz", 0, 11, "bar"));
        Assert.assertEquals(4, Chars.indexOfLowerCase("FOO BAR BAZ", 0, 11, "bar"));
        Assert.assertEquals(4, Chars.indexOfLowerCase("foo bar baz", 0, 11, "ba"));
        Assert.assertEquals(8, Chars.indexOfLowerCase("foo BAr BAz", 6, 11, "ba"));
        Assert.assertEquals(1, Chars.indexOfLowerCase("foo bar baz", 0, 7, "oo"));
        Assert.assertEquals(0, Chars.indexOfLowerCase("foo bar baz", 2, 4, ""));
        Assert.assertEquals(-1, Chars.indexOfLowerCase("foo bar baz", 2, 4, "y"));
        Assert.assertEquals(-1, Chars.indexOfLowerCase("", 0, 0, "oo"));
        Assert.assertEquals(-1, Chars.indexOfLowerCase("", 0, 0, "y"));
    }

    @Test
    public void testIndexOfNonWhitespace() {
        String in = " a bbbbs   sss";
        Assert.assertEquals(-1, Chars.indexOfNonWhitespace(in, 0, in.length(), 0));

        Assert.assertEquals(7, Chars.indexOfNonWhitespace(in, 3, 11, -1));
        Assert.assertEquals(-1, Chars.indexOfNonWhitespace(in, 0, 1, -1));

        Assert.assertEquals(11, Chars.indexOfNonWhitespace(in, 8, in.length(), 1));
        Assert.assertEquals(-1, Chars.indexOfNonWhitespace(in, 8, 10, 1));
    }

    @Test
    public void testIsAscii() {
        Assert.assertTrue(Chars.isAscii(""));
        Assert.assertTrue(Chars.isAscii("foo bar baz"));
        Assert.assertFalse(Chars.isAscii("фу бар баз"));
    }

    @Test
    public void testIsBlank() {
        Assert.assertTrue(Chars.isBlank(null));
        Assert.assertTrue(Chars.isBlank(""));
        Assert.assertTrue(Chars.isBlank(" "));
        Assert.assertTrue(Chars.isBlank("      "));
        Assert.assertTrue(Chars.isBlank("\r\f\n\t"));

        Assert.assertFalse(Chars.isBlank("a"));
        Assert.assertFalse(Chars.isBlank("0"));
        Assert.assertFalse(Chars.isBlank("\\"));
        Assert.assertFalse(Chars.isBlank("\\r"));
        Assert.assertFalse(Chars.isBlank("ac/dc"));
    }

    @Test
    public void testIsNotQuoted() {
        Assert.assertFalse(Chars.isQuoted("'banana\""));
        Assert.assertFalse(Chars.isQuoted("banana\""));
        Assert.assertFalse(Chars.isQuoted("\"banana"));
        Assert.assertFalse(Chars.isQuoted("\"banana'"));
        Assert.assertFalse(Chars.isQuoted("'"));
        Assert.assertFalse(Chars.isQuoted("\""));
        Assert.assertFalse(Chars.isQuoted("banana"));
    }

    @Test
    public void testIsOnlyDecimals() {
        Assert.assertTrue(Chars.isOnlyDecimals("9876543210123456789"));
        Assert.assertFalse(Chars.isOnlyDecimals(""));
        Assert.assertFalse(Chars.isOnlyDecimals(" "));
        Assert.assertFalse(Chars.isOnlyDecimals("99 "));
        Assert.assertFalse(Chars.isOnlyDecimals("987654321a123456789"));
    }

    @Test
    public void testIsQuoted() {
        Assert.assertTrue(Chars.isQuoted("'banana'"));
        Assert.assertTrue(Chars.isQuoted("''"));
        Assert.assertTrue(Chars.isQuoted("\"banana\""));
        Assert.assertTrue(Chars.isQuoted("\"\""));
    }

    @Test
    public void testLastIndexOfDifferent() {
        Assert.assertEquals(-1, Chars.lastIndexOfDifferent("   ", 0, 3, ' '));
        Assert.assertEquals(0, Chars.lastIndexOfDifferent("s  ", 0, 3, ' '));
        Assert.assertEquals(0, Chars.lastIndexOfDifferent("s s ", 0, 2, ' '));
        Assert.assertEquals(-1, Chars.lastIndexOfDifferent("s  ", 1, 3, ' '));
    }

    @Test
    public void testNameFromPath() {
        StringBuilder name = new StringBuilder();
        name.append(Files.SEPARATOR).append("xyz").append(Files.SEPARATOR).append("dir1").append(Files.SEPARATOR).append("dir2").append(Files.SEPARATOR).append("this is my name");
        TestUtils.assertEquals("this is my name", extractor.of(new Utf8String(name)));
    }

    @Test
    public void testPathList() {
        assertThat("[abc,d1]", Chars.splitLpsz("abc d1"));
    }

    @Test
    public void testPathListLeadingSpaces() {
        assertThat("[abc,d1]", Chars.splitLpsz("   abc d1"));
    }

    @Test
    public void testPathListQuotedSpace() {
        assertThat("[abc,d1 cd,x]", Chars.splitLpsz("abc \"d1 cd\" x"));
    }

    @Test
    public void testPathListQuotedSpaceEmpty() {
        assertThat("[abc,x]", Chars.splitLpsz("abc \"\" x"));
    }

    @Test
    public void testPathListTrailingSpace() {
        assertThat("[abc,d1]", Chars.splitLpsz("abc d1    "));
    }

    @Test
    public void testPathListUnclosedQuote() {
        assertThat("[abc,c cd]", Chars.splitLpsz("abc \"c cd"));
    }

    @Test
    public void testPlainName() {
        TestUtils.assertEquals("xyz.txt", extractor.of(new Utf8String("xyz.txt")));
    }

    @Test
    public void testStartsWithLowerCase() {
        Assert.assertTrue(Chars.startsWithLowerCase("", ""));
        String[] positive = {"", "a", "ab", "abc"};
        for (String s : positive) {
            Assert.assertTrue(Chars.startsWithLowerCase("abc", s));
            Assert.assertTrue(Chars.startsWithLowerCase("ABC", s));
        }

        Assert.assertTrue(Chars.startsWithLowerCase("abcd", ""));
        Assert.assertFalse(Chars.startsWithLowerCase("", "abcd"));
        Assert.assertFalse(Chars.startsWithLowerCase("abc", "abcd"));
        Assert.assertFalse(Chars.startsWithLowerCase("ABC", "abcd"));
        // The pattern has to be lower-case.
        Assert.assertFalse(Chars.startsWithLowerCase("abc", "ABC"));
        Assert.assertFalse(Chars.startsWithLowerCase("ABC", "ABC"));
    }

    @Test
    public void testUnescape() {
        final StringSink sink = new StringSink();
        final char escapeChar = '\'';

        // double escape in the middle
        final String input1 = "prefix''suffix";
        final String expected1 = "prefix'suffix";
        sink.clear();
        Chars.unescape(input1, 0, input1.length(), escapeChar, sink);
        Assert.assertEquals(
                "Test Case 1 Failed: Double escape in middle",
                expected1,
                sink.toString()
        );

        // double escape at the start
        final String input2 = "''suffix";
        final String expected2 = "'suffix";
        sink.clear();
        Chars.unescape(input2, 0, input2.length(), escapeChar, sink);
        Assert.assertEquals(
                "Test Case 2 Failed: Double escape at start",
                expected2,
                sink.toString()
        );

        // double escape at the end
        final String input3 = "prefix''";
        final String expected3 = "prefix'";
        sink.clear();
        Chars.unescape(input3, 0, input3.length(), escapeChar, sink);
        Assert.assertEquals(
                "Test Case 3 Failed: Double escape at end",
                expected3,
                sink.toString()
        );

        // multiple double escapes
        final String input4 = "a''b''c''";
        final String expected4 = "a'b'c'";
        sink.clear();
        Chars.unescape(input4, 0, input4.length(), escapeChar, sink);
        Assert.assertEquals(
                "Test Case 4 Failed: Multiple double escapes",
                expected4,
                sink.toString()
        );

        // mix of single and double escapes
        final String input5 = "a'b''c'd";
        final String expected5 = "a'b'c'd";
        sink.clear();
        Chars.unescape(input5, 0, input5.length(), escapeChar, sink);
        Assert.assertEquals(
                "Test Case 5 Failed: Mixed single and double escapes",
                expected5,
                sink.toString()
        );

        // only a double escape
        final String input6 = "''";
        final String expected6 = "'";
        sink.clear();
        Chars.unescape(input6, 0, input6.length(), escapeChar, sink);
        Assert.assertEquals(
                "Test Case 6 Failed: Only double escape",
                expected6,
                sink.toString()
        );

        // no escapes
        final String input7 = "plain string";
        final String expected7 = "plain string";
        sink.clear();
        Chars.unescape(input7, 0, input7.length(), escapeChar, sink);
        Assert.assertEquals(
                "Test Case 7 Failed: No escapes",
                expected7,
                sink.toString()
        );

        // single escape not at end
        final String input8 = "single'escape";
        final String expected8 = "single'escape";
        sink.clear();
        Chars.unescape(input8, 0, input8.length(), escapeChar, sink);
        Assert.assertEquals(
                "Test Case 8 Failed: Single escape internal",
                expected8,
                sink.toString()
        );

        // single escape at end
        final String input9 = "single'";
        final String expected9 = "single'";
        sink.clear();
        Chars.unescape(input9, 0, input9.length(), escapeChar, sink);
        Assert.assertEquals(
                "Test Case 9 Failed: Single escape at end",
                expected9,
                sink.toString()
        );

        // empty input
        final String input10 = "";
        final String expected10 = "";
        sink.clear();
        Chars.unescape(input10, 0, input10.length(), escapeChar, sink);
        Assert.assertEquals(
                "Test Case 10 Failed: Empty input",
                expected10,
                sink.toString()
        );
    }

    private void assertThat(String expected, ObjList<Path> list) {
        Assert.assertEquals(expected, list.toString());
        for (int i = 0, n = list.size(); i < n; i++) {
            list.getQuick(i).close();
        }
    }
}
