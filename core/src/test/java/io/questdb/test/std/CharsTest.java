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

package io.questdb.test.std;

import io.questdb.cairo.CairoException;
import io.questdb.std.*;
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

public class CharsTest {
    private static final FileNameExtractorCharSequence extractor = new FileNameExtractorCharSequence();
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
        String encoded = "+W8kK89c79Jb97CrQM3aGuPJE85fEdFoXwSsEWjU736IXm4v7+mZKiOL82uYhGaxmIYUUJh5/Xj44tX0NrD4lQ==";
        try (DirectByteCharSink sink = new DirectByteCharSink(16)) {
            Chars.base64Decode(encoded, sink);

            byte[] decode = Base64.getDecoder().decode(encoded);
            Assert.assertEquals(decode.length, sink.length());
            for (int i = 0; i < decode.length; i++) {
                Assert.assertEquals(decode[i], sink.byteAt(i));
            }
        }
    }

    @Test
    public void testBase64DecodeByteSinkInvalidInput() {
        String encoded = "a";
        try (DirectByteCharSink sink = new DirectByteCharSink(16)) {
            Chars.base64Decode(encoded, sink);
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "invalid base64 encoding");
        }
    }

    @Test
    public void testBase64DecodeByteSinkMiscLengths() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            sb.setLength(0);
            for (int j = 0; j < i; j++) {
                sb.append(j % 10);
            }
            String encoded = Base64.getEncoder().encodeToString(sb.toString().getBytes());
            try (DirectByteCharSink sink = new DirectByteCharSink(16)) {
                Chars.base64Decode(encoded, sink);

                byte[] decode = Base64.getDecoder().decode(encoded);
                Assert.assertEquals(decode.length, sink.length());
                for (int j = 0; j < decode.length; j++) {
                    Assert.assertEquals(decode[j], sink.byteAt(j));
                }
            }
        }
    }

    @Test
    public void testBase64DecodeByteSinkUtf8() {
        String encoded = Base64.getEncoder().encodeToString("аз съм грут:गाजर का हलवा".getBytes());
        try (DirectByteCharSink sink = new DirectByteCharSink(16)) {
            Chars.base64Decode(encoded, sink);

            byte[] decode = Base64.getDecoder().decode(encoded);
            Assert.assertEquals(decode.length, sink.length());
            for (int i = 0; i < decode.length; i++) {
                Assert.assertEquals(decode[i], sink.byteAt(i));
            }
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
    public void testEmptyString() {
        TestUtils.assertEquals("", extractor.of(""));
    }

    @Test
    public void testEndsWith() {
        Assert.assertFalse(Chars.endsWith(null, null));
        Assert.assertFalse(Chars.endsWith("a", null));
        Assert.assertFalse(Chars.endsWith(null, "a"));
        Assert.assertFalse(Chars.endsWith("", "a"));
        Assert.assertFalse(Chars.endsWith("a", ""));
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
        TestUtils.assertEquals("this is my name", extractor.of(name));
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
        TestUtils.assertEquals("xyz.txt", extractor.of("xyz.txt"));
    }

    @Test
    public void testUtf8CharDecode() {
        long p = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        try {
            testUtf8Char("A", p, false); // 1 byte
            testUtf8Char("Ч", p, false); // 2 bytes
            testUtf8Char("∆", p, false); // 3 bytes
            testUtf8Char("\uD83D\uDE00\"", p, true); // fail, cannot store it as one char
        } finally {
            Unsafe.free(p, 8, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUtf8CharMalformedDecode() {
        long p = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        try {
            // empty
            Assert.assertEquals(0, Chars.utf8CharDecode(p, p));
            // one byte
            Unsafe.getUnsafe().putByte(p, (byte) 0xFF);
            Assert.assertEquals(0, Chars.utf8CharDecode(p, p + 1));
            Unsafe.getUnsafe().putByte(p, (byte) 0xC0);
            Assert.assertEquals(0, Chars.utf8CharDecode(p, p + 1));
            Unsafe.getUnsafe().putByte(p, (byte) 0x80);
            Assert.assertEquals(0, Chars.utf8CharDecode(p, p + 1));
            // two bytes
            Unsafe.getUnsafe().putByte(p, (byte) 0xC0);
            Unsafe.getUnsafe().putByte(p + 1, (byte) 0x80);
            Assert.assertEquals(0, Chars.utf8CharDecode(p, p + 2));

            Unsafe.getUnsafe().putByte(p, (byte) 0xC1);
            Unsafe.getUnsafe().putByte(p + 1, (byte) 0xBF);
            Assert.assertEquals(0, Chars.utf8CharDecode(p, p + 2));

            Unsafe.getUnsafe().putByte(p, (byte) 0xC2);
            Unsafe.getUnsafe().putByte(p + 1, (byte) 0x00);
            Assert.assertEquals(0, Chars.utf8CharDecode(p, p + 2));

            Unsafe.getUnsafe().putByte(p, (byte) 0xE0);
            Unsafe.getUnsafe().putByte(p + 1, (byte) 0x80);
            Unsafe.getUnsafe().putByte(p + 2, (byte) 0xC0);
            Assert.assertEquals(0, Chars.utf8CharDecode(p, p + 3));

            Unsafe.getUnsafe().putByte(p, (byte) 0xE0);
            Unsafe.getUnsafe().putByte(p + 1, (byte) 0xC0);
            Unsafe.getUnsafe().putByte(p + 2, (byte) 0xBF);
            Assert.assertEquals(0, Chars.utf8CharDecode(p, p + 3));

            Unsafe.getUnsafe().putByte(p, (byte) 0xE0);
            Unsafe.getUnsafe().putByte(p + 1, (byte) 0xA0);
            Unsafe.getUnsafe().putByte(p + 2, (byte) 0x7F);
            Assert.assertEquals(0, Chars.utf8CharDecode(p, p + 3));

            Unsafe.getUnsafe().putByte(p, (byte) 0xED);
            Unsafe.getUnsafe().putByte(p + 1, (byte) 0xAE);
            Unsafe.getUnsafe().putByte(p + 2, (byte) 0x80);
            Assert.assertEquals(0, Chars.utf8CharDecode(p, p + 3));

        } finally {
            Unsafe.free(p, 8, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUtf8Support() {

        StringBuilder expected = new StringBuilder();
        for (int i = 0; i < 0xD800; i++) {
            expected.append((char) i);
        }

        String in = expected.toString();
        long p = Unsafe.malloc(8 * 0xffff, MemoryTag.NATIVE_DEFAULT);
        try {
            byte[] bytes = in.getBytes(Files.UTF_8);
            for (int i = 0, n = bytes.length; i < n; i++) {
                Unsafe.getUnsafe().putByte(p + i, bytes[i]);
            }
            CharSink b = new StringSink();
            Chars.utf8toUtf16(p, p + bytes.length, b);
            TestUtils.assertEquals(in, b.toString());
        } finally {
            Unsafe.free(p, 8 * 0xffff, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUtf8SupportZ() {

        StringBuilder expected = new StringBuilder();
        for (int i = 1; i < 0xD800; i++) {
            expected.append((char) i);
        }

        String in = expected.toString();
        long p = Unsafe.malloc(8 * 0xffff, MemoryTag.NATIVE_DEFAULT);
        try {
            byte[] bytes = in.getBytes(Files.UTF_8);
            for (int i = 0, n = bytes.length; i < n; i++) {
                Unsafe.getUnsafe().putByte(p + i, bytes[i]);
            }
            Unsafe.getUnsafe().putByte(p + bytes.length, (byte) 0);
            CharSink b = new StringSink();
            Chars.utf8ToUtf16Z(p, b);
            TestUtils.assertEquals(in, b.toString());
        } finally {
            Unsafe.free(p, 8 * 0xffff, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void testUtf8Char(String x, long p, boolean failExpected) {
        byte[] bytes = x.getBytes(Files.UTF_8);
        for (int i = 0, n = Math.min(bytes.length, 8); i < n; i++) {
            Unsafe.getUnsafe().putByte(p + i, bytes[i]);
        }
        int res = Chars.utf8CharDecode(p, p + bytes.length);
        boolean eq = x.charAt(0) == (char) Numbers.decodeHighShort(res);
        Assert.assertTrue(failExpected != eq);
    }

    private void assertThat(String expected, ObjList<Path> list) {
        Assert.assertEquals(expected, list.toString());
        for (int i = 0, n = list.size(); i < n; i++) {
            list.getQuick(i).close();
        }
    }
}
