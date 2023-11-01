/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.std.ObjList;
import io.questdb.std.str.*;
import io.questdb.test.griffin.engine.TestBinarySequence;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Random;
import java.util.function.BiConsumer;

public class CharsTest {
    private static final FileNameExtractorUtf8Sequence extractor = new FileNameExtractorUtf8Sequence();
    private static char separator;

    @BeforeClass
    public static void setUp() {
        separator = System.getProperty("file.separator").charAt(0);
    }

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
    public void testBase64DecodeByteSink() {
        base64DecodeByteSink(Base64.getDecoder(), Chars::base64Decode);
    }

    @Test
    public void testBase64DecodeByteSinkInvalidInput() {
        base64DecodeByteSinkInvalidInput(Chars::base64Decode);
    }

    @Test
    public void testBase64DecodeByteSinkMiscLengths() {
        base64DecodeByteSinkMiscLengths(Base64.getEncoder(), Base64.getDecoder(), Chars::base64Decode);
    }

    @Test
    public void testBase64DecodeByteSinkUtf8() {
        base64DecodeByteSinkUtf8(Base64.getEncoder(), Base64.getDecoder(), Chars::base64Decode);
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
        Assert.assertEquals(sink.toString(), "dGhpcyBpcyBhIHRlc3Q=");
        sink.clear();
        Chars.base64Encode(testBinarySequence.of("this is a test".getBytes()), 4, sink);
        Assert.assertEquals(sink.toString(), "dGhpcw==");
        // ignore the null
        Chars.base64Encode(null, 4, sink);
        Assert.assertEquals(sink.toString(), "dGhpcw==");

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
    public void testBase64UrlDecode() {
        String s = "this is a test";
        String encoded = Base64.getUrlEncoder().encodeToString(s.getBytes());
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        Chars.base64UrlDecode(encoded, buffer);
        buffer.flip();
        String s2 = new String(buffer.array(), buffer.position(), buffer.remaining());
        TestUtils.equals(s, s2);

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
    public void testBase64UrlDecodeByteSink() {
        base64DecodeByteSink(Base64.getUrlDecoder(), Chars::base64UrlDecode);
    }

    @Test
    public void testBase64UrlDecodeByteSinkInvalidInput() {
        base64DecodeByteSinkInvalidInput(Chars::base64UrlDecode);
    }

    @Test
    public void testBase64UrlDecodeByteSinkUtf8() {
        base64DecodeByteSinkUtf8(Base64.getUrlEncoder(), Base64.getUrlDecoder(), Chars::base64UrlDecode);
    }

    @Test
    public void testBase64UrlEncode() {
        final StringSink sink = new StringSink();
        final TestBinarySequence testBinarySequence = new TestBinarySequence();
        sink.clear();
        Chars.base64UrlEncode(testBinarySequence.of("this is a test".getBytes()), 100, sink);
        Assert.assertEquals(sink.toString(), "dGhpcyBpcyBhIHRlc3Q");
        sink.clear();
        Chars.base64UrlEncode(testBinarySequence.of("this is a test".getBytes()), 4, sink);
        Assert.assertEquals(sink.toString(), "dGhpcw");
        // ignore the null
        Chars.base64UrlEncode(null, 4, sink);
        Assert.assertEquals(sink.toString(), "dGhpcw");

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
    public void testBaseUrl64DecodeByteSinkMiscLengths() {
        base64DecodeByteSinkMiscLengths(Base64.getUrlEncoder(), Base64.getUrlDecoder(), Chars::base64UrlDecode);
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
    public void testNameFromPath() {
        StringBuilder name = new StringBuilder();
        name.append(separator).append("xyz").append(separator).append("dir1").append(separator).append("dir2").append(separator).append("this is my name");
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

    private static void base64DecodeByteSink(Base64.Decoder jdkDecoder, BiConsumer<CharSequence, DirectUtf8Sink> decoderUnderTest) {
        String encoded = "QmFzZTY0IGlzIGEgZ3JvdXAgb2Ygc2ltaWxhciBiaW5hcnktdG8tdGV4dA";
        try (DirectUtf8Sink sink = new DirectUtf8Sink(16)) {
            decoderUnderTest.accept(encoded, sink);

            byte[] decode = jdkDecoder.decode(encoded);
            Assert.assertEquals(decode.length, sink.size());
            for (int i = 0; i < decode.length; i++) {
                Assert.assertEquals(decode[i], sink.byteAt(i));
            }
        }
    }

    private static void base64DecodeByteSinkInvalidInput(BiConsumer<CharSequence, DirectUtf8Sink> decoder) {
        String encoded = "a";
        try (DirectUtf8Sink sink = new DirectUtf8Sink(16)) {
            decoder.accept(encoded, sink);
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "invalid base64 encoding");
        }
    }

    private static void base64DecodeByteSinkMiscLengths(Base64.Encoder jdkEncoder, Base64.Decoder jdkDecoder, BiConsumer<CharSequence, DirectUtf8Sink> decoderUnderTest) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            sb.setLength(0);
            for (int j = 0; j < i; j++) {
                sb.append(j % 10);
            }
            String encoded = jdkEncoder.encodeToString(sb.toString().getBytes());
            try (DirectUtf8Sink sink = new DirectUtf8Sink(16)) {
                decoderUnderTest.accept(encoded, sink);

                byte[] decode = jdkDecoder.decode(encoded);
                Assert.assertEquals(decode.length, sink.size());
                for (int j = 0; j < decode.length; j++) {
                    Assert.assertEquals(decode[j], sink.byteAt(j));
                }
            }
        }
    }

    private void assertThat(String expected, ObjList<Path> list) {
        Assert.assertEquals(expected, list.toString());
        for (int i = 0, n = list.size(); i < n; i++) {
            list.getQuick(i).close();
        }
    }

    private void base64DecodeByteSinkUtf8(Base64.Encoder jdkEncoder, Base64.Decoder jdkDecoder, BiConsumer<CharSequence, DirectUtf8Sink> decoderUnderTest) {
        String encoded = jdkEncoder.encodeToString("аз съм грут:गाजर का हलवा".getBytes());
        try (DirectUtf8Sink sink = new DirectUtf8Sink(16)) {
            decoderUnderTest.accept(encoded, sink);

            byte[] decode = jdkDecoder.decode(encoded);
            Assert.assertEquals(decode.length, sink.size());
            for (int i = 0; i < decode.length; i++) {
                Assert.assertEquals(decode[i], sink.byteAt(i));
            }
        }
    }
}
