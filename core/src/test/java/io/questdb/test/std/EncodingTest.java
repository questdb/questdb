package io.questdb.test.std;

import io.questdb.cairo.CairoException;
import io.questdb.std.Encoding;
import io.questdb.std.str.StringSink;
import io.questdb.test.griffin.engine.TestBinarySequence;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Random;

public class EncodingTest {
    @Test
    public void testBase64Decode() {
        String encoded = "+W8kK89c79Jb97CrQM3aGuPJE85fEdFoXwSsEWjU736IXm4v7+mZKiOL82uYhGaxmIYUUJh5/Xj44tX0NrD4lQ==";
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        Encoding.base64Decode(encoded, buffer);
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
        Encoding.base64Encode(testBinarySequence.of("this is a test".getBytes()), 100, sink);
        Assert.assertEquals(sink.toString(), "dGhpcyBpcyBhIHRlc3Q=");
        sink.clear();
        Encoding.base64Encode(testBinarySequence.of("this is a test".getBytes()), 4, sink);
        Assert.assertEquals(sink.toString(), "dGhpcw==");
        // ignore the null
        Encoding.base64Encode(null, 4, sink);
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
        Encoding.base64Encode(testBinarySequence, (int) testBinarySequence.length(), sink);
        byte[] decoded = Base64.getDecoder().decode(sink.toString());
        Assert.assertArrayEquals(bytes, decoded);
    }

    @Test
    public void testBase64UrlDecode() {
        String s = "this is a test";
        String encoded = Base64.getUrlEncoder().encodeToString(s.getBytes());
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        Encoding.base64UrlDecode(encoded, buffer);
        buffer.flip();
        String s2 = new String(buffer.array(), buffer.position(), buffer.remaining());
        TestUtils.equals(s, s2);

        // null is ignored
        buffer.clear();
        Encoding.base64UrlDecode(null, buffer);
        Assert.assertEquals(0, buffer.position());

        // single char with no padding
        buffer.clear();
        Encoding.base64UrlDecode(Base64.getUrlEncoder().encodeToString("a".getBytes(StandardCharsets.UTF_8)), buffer);
        buffer.flip();
        Assert.assertEquals(1, buffer.remaining());
        Assert.assertEquals('a', buffer.get());

        // empty string
        buffer.clear();
        Encoding.base64UrlDecode("", buffer);
        buffer.flip();
        Assert.assertEquals(0, buffer.remaining());

        // single char is invalid
        buffer.clear();
        try {
            Encoding.base64UrlDecode("a", buffer);
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "invalid base64 encoding");
        }

        // empty string with padding
        buffer.clear();
        Encoding.base64UrlDecode("===", buffer);
        buffer.flip();
        Assert.assertEquals(0, buffer.remaining());

        // non-ascii in input
        buffer.clear();
        try {
            Encoding.base64UrlDecode("a\u00A0", buffer);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "non-ascii character while decoding base64");
        }

        // ascii but not base64
        buffer.clear();
        try {
            Encoding.base64UrlDecode("a\u0001", buffer);
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
        Encoding.base64UrlDecode(encoded, buffer);
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
        Encoding.base64UrlEncode(testBinarySequence.of("this is a test".getBytes()), 100, sink);
        Assert.assertEquals(sink.toString(), "dGhpcyBpcyBhIHRlc3Q");
        sink.clear();
        Encoding.base64UrlEncode(testBinarySequence.of("this is a test".getBytes()), 4, sink);
        Assert.assertEquals(sink.toString(), "dGhpcw");
        // ignore the null
        Encoding.base64UrlEncode(null, 4, sink);
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
        Encoding.base64UrlEncode(testBinarySequence, (int) testBinarySequence.length(), sink);
        byte[] decoded = Base64.getUrlDecoder().decode(sink.toString());
        Assert.assertArrayEquals(bytes, decoded);
    }
}
