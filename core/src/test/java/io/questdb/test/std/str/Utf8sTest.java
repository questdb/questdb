package io.questdb.test.std.str;

import io.questdb.std.*;
import io.questdb.std.str.*;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class Utf8sTest {

    @Test
    public void testContainsAscii() {
        Assert.assertTrue(Utf8s.containsAscii(new Utf8String("foo bar baz"), "bar"));
        Assert.assertFalse(Utf8s.containsAscii(new Utf8String("foo bar baz"), "buz"));
        Assert.assertTrue(Utf8s.containsAscii(new Utf8String("foo bar baz"), ""));
        Assert.assertFalse(Utf8s.containsAscii(Utf8String.EMPTY, "foobar"));
    }

    @Test
    public void testDoubleQuotedTextBySingleQuoteParsing() {
        StringSink query = new StringSink();

        String text = "select count(*) from \"\"file.csv\"\" abcd";
        Assert.assertTrue(copyToSinkWithTextUtil(query, text, false));

        Assert.assertEquals(text, query.toString());
    }

    @Test
    public void testDoubleQuotedTextParsing() {
        StringSink query = new StringSink();

        String text = "select count(*) from \"\"file.csv\"\" abcd";
        Assert.assertTrue(copyToSinkWithTextUtil(query, text, true));

        Assert.assertEquals(text.replace("\"\"", "\""), query.toString());
    }

    @Test
    public void testEndsWithAscii() {
        Assert.assertTrue(Utf8s.endsWithAscii(new Utf8String("foo bar baz"), "baz"));
        Assert.assertFalse(Utf8s.endsWithAscii(new Utf8String("foo bar baz"), "bar"));
        Assert.assertTrue(Utf8s.endsWithAscii(new Utf8String("foo bar baz"), ""));
        Assert.assertFalse(Utf8s.endsWithAscii(Utf8String.EMPTY, "foo"));
    }

    @Test
    public void testEndsWithAsciiChar() {
        Assert.assertTrue(Utf8s.endsWithAscii(new Utf8String("foo bar baz"), 'z'));
        Assert.assertFalse(Utf8s.endsWithAscii(new Utf8String("foo bar baz"), 'f'));
        Assert.assertFalse(Utf8s.endsWithAscii(new Utf8String("foo bar baz"), (char) 0));
        Assert.assertFalse(Utf8s.endsWithAscii(Utf8String.EMPTY, ' '));
    }

    @Test
    public void testEquals() {
        final DirectUtf8Sequence str1a = new GcUtf8String("test1");
        final DirectUtf8Sequence str1b = new GcUtf8String("test1");
        final DirectUtf8Sequence str2 = new GcUtf8String("test2");
        final DirectUtf8Sequence str3 = new GcUtf8String("a_longer_string");
        Assert.assertNotEquals(str1a.ptr(), str1b.ptr());

        Assert.assertTrue(Utf8s.equals(str1a, str1a));
        Assert.assertTrue(Utf8s.equals(str1a, str1b));
        Assert.assertFalse(Utf8s.equals(str1a, str2));
        Assert.assertFalse(Utf8s.equals(str2, str3));

        Assert.assertTrue(Utf8s.equals(str1a, (Utf8Sequence) str1a));
        Assert.assertTrue(Utf8s.equals(str1a, (Utf8Sequence) str1b));
        Assert.assertFalse(Utf8s.equals(str1a, (Utf8Sequence) str2));
        Assert.assertFalse(Utf8s.equals(str2, (Utf8Sequence) str3));

        final Utf8String onHeapStr1a = new Utf8String("test1".getBytes(StandardCharsets.UTF_8));
        final Utf8String onHeapStr1b = new Utf8String("test1".getBytes(StandardCharsets.UTF_8));
        final Utf8String onHeapStr2 = new Utf8String("test2".getBytes(StandardCharsets.UTF_8));
        final Utf8String onHeapStr3 = new Utf8String("a_longer_string".getBytes(StandardCharsets.UTF_8));

        Assert.assertTrue(Utf8s.equals(onHeapStr1a, onHeapStr1a));
        Assert.assertTrue(Utf8s.equals(onHeapStr1a, onHeapStr1b));
        Assert.assertFalse(Utf8s.equals(onHeapStr1a, onHeapStr2));
        Assert.assertFalse(Utf8s.equals(onHeapStr2, onHeapStr3));

        final DirectUtf8String directStr1a = new DirectUtf8String().of(str1a.lo(), str1a.hi());
        final DirectUtf8String directStr2 = new DirectUtf8String().of(str2.lo(), str2.hi());

        Assert.assertTrue(Utf8s.equals(directStr1a, onHeapStr1a));
        Assert.assertTrue(Utf8s.equals(directStr1a, onHeapStr1b));
        Assert.assertFalse(Utf8s.equals(directStr1a, onHeapStr2));
        Assert.assertFalse(Utf8s.equals(directStr2, onHeapStr3));

        Assert.assertTrue(Utf8s.equals(directStr1a, 0, 3, onHeapStr1a, 0, 3));
        Assert.assertFalse(Utf8s.equals(directStr1a, 0, 3, onHeapStr3, 0, 3));
    }

    @Test
    public void testEqualsAscii() {
        final Utf8String str = new Utf8String("test1");

        Assert.assertTrue(Utf8s.equalsAscii("test1", str));
        Assert.assertFalse(Utf8s.equalsAscii("test2", str));
        Assert.assertFalse(Utf8s.equalsAscii("a_longer_string", str));

        Assert.assertTrue(Utf8s.equalsAscii("test1", 0, 3, str, 0, 3));
        Assert.assertFalse(Utf8s.equalsAscii("a_longer_string", 0, 3, str, 0, 3));
    }

    @Test
    public void testEqualsIgnoreCaseAscii() {
        final Utf8String str = new Utf8String("test1");

        Assert.assertTrue(Utf8s.equalsIgnoreCaseAscii("test1", str));
        Assert.assertTrue(Utf8s.equalsIgnoreCaseAscii("TeSt1", str));
        Assert.assertTrue(Utf8s.equalsIgnoreCaseAscii("TEST1", str));
        Assert.assertFalse(Utf8s.equalsIgnoreCaseAscii("test2", str));
        Assert.assertFalse(Utf8s.equalsIgnoreCaseAscii("a_longer_string", str));

        Assert.assertTrue(Utf8s.equalsIgnoreCaseAscii(new Utf8String("test1"), str));
        Assert.assertTrue(Utf8s.equalsIgnoreCaseAscii(new Utf8String("TeSt1"), str));
        Assert.assertTrue(Utf8s.equalsIgnoreCaseAscii(new Utf8String("TEST1"), str));
        Assert.assertFalse(Utf8s.equalsIgnoreCaseAscii(new Utf8String("test2"), str));
        Assert.assertFalse(Utf8s.equalsIgnoreCaseAscii(new Utf8String("a_longer_string"), str));

        Assert.assertTrue(Utf8s.equalsIgnoreCaseAscii(new Utf8String("test1"), 0, 5, str, 0, 5));
        Assert.assertTrue(Utf8s.equalsIgnoreCaseAscii(new Utf8String("TeSt1"), 0, 5, str, 0, 5));
        Assert.assertTrue(Utf8s.equalsIgnoreCaseAscii(new Utf8String("TEST1"), 0, 5, str, 0, 5));
        Assert.assertFalse(Utf8s.equalsIgnoreCaseAscii(new Utf8String("test2"), 0, 5, str, 0, 5));
        Assert.assertTrue(Utf8s.equalsIgnoreCaseAscii(new Utf8String("test2"), 0, 4, str, 0, 4));
    }

    @Test
    public void testEqualsNcAscii() {
        final Utf8String str = new Utf8String("test1");

        Assert.assertTrue(Utf8s.equalsNcAscii("test1", str));
        Assert.assertFalse(Utf8s.equalsNcAscii("test2", str));
        Assert.assertFalse(Utf8s.equalsNcAscii("a_longer_string", str));

        Assert.assertFalse(Utf8s.equalsNcAscii("test1", null));
    }

    @Test
    public void testHashCode() {
        final int size = 64;
        StringSink charSink = new StringSink();
        Utf8StringSink utf8Sink = new Utf8StringSink();
        for (int i = 0; i < size; i++) {
            charSink.putAscii('A');
            utf8Sink.putAscii('A');

            Assert.assertEquals(Chars.hashCode(charSink), Utf8s.hashCode(utf8Sink));

            if (i > 0) {
                Assert.assertEquals(Chars.hashCode(charSink, 0, i - 1), Utf8s.hashCode(utf8Sink, 0, i - 1));
            }
        }
    }

    @Test
    public void testIndexOfAscii() {
        Assert.assertEquals(1, Utf8s.indexOfAscii(new Utf8String("foo bar baz"), 0, 7, "oo"));
        Assert.assertEquals(1, Utf8s.indexOfAscii(new Utf8String("foo bar baz"), 0, 7, "oo", -1));
        Assert.assertEquals(-1, Utf8s.indexOfAscii(new Utf8String("foo bar baz"), 2, 4, "y"));
        Assert.assertEquals(-1, Utf8s.indexOfAscii(Utf8String.EMPTY, 0, 0, "byz"));
    }

    @Test
    public void testIndexOfAsciiChar() {
        Assert.assertEquals(1, Utf8s.indexOfAscii(new Utf8String("foo bar baz"), 'o'));
        Assert.assertEquals(-1, Utf8s.indexOfAscii(new Utf8String("foo bar baz"), 'y'));
        Assert.assertEquals(-1, Utf8s.indexOfAscii(Utf8String.EMPTY, 'y'));

        Assert.assertEquals(2, Utf8s.indexOfAscii(new Utf8String("foo bar baz"), 2, 'o'));
        Assert.assertEquals(-1, Utf8s.indexOfAscii(new Utf8String("foo bar baz"), 2, 'y'));
        Assert.assertEquals(-1, Utf8s.indexOfAscii(Utf8String.EMPTY, 0, 'y'));

        Assert.assertEquals(2, Utf8s.indexOfAscii(new Utf8String("foo bar baz"), 2, 4, 'o'));
        Assert.assertEquals(2, Utf8s.indexOfAscii(new Utf8String("foo bar baz"), 0, 4, 'o', -1));
        Assert.assertEquals(-1, Utf8s.indexOfAscii(new Utf8String("foo bar baz"), 2, 4, 'y'));
        Assert.assertEquals(-1, Utf8s.indexOfAscii(Utf8String.EMPTY, 0, 0, 'y'));
    }

    @Test
    public void testLastIndexOfAscii() {
        Assert.assertEquals(2, Utf8s.lastIndexOfAscii(new Utf8String("foo bar baz"), 'o'));
        Assert.assertEquals(10, Utf8s.lastIndexOfAscii(new Utf8String("foo bar baz"), 'z'));
        Assert.assertEquals(-1, Utf8s.lastIndexOfAscii(new Utf8String("foo bar baz"), 'y'));
        Assert.assertEquals(-1, Utf8s.lastIndexOfAscii(Utf8String.EMPTY, 'y'));
    }

    @Test
    public void testLowerCaseAsciiHashCode() {
        final int size = 64;
        StringSink charSink = new StringSink();
        Utf8StringSink utf8Sink = new Utf8StringSink();
        for (int i = 0; i < size; i++) {
            charSink.putAscii('a');
            utf8Sink.putAscii('A');

            Assert.assertEquals(Chars.hashCode(charSink), Utf8s.lowerCaseAsciiHashCode(utf8Sink));

            if (i > 0) {
                Assert.assertEquals(Chars.hashCode(charSink, 0, i - 1), Utf8s.lowerCaseAsciiHashCode(utf8Sink, 0, i - 1));
            }
        }
    }

    @Test
    public void testQuotedTextParsing() {
        StringSink query = new StringSink();

        String text = "select count(*) from \"file.csv\" abcd";
        Assert.assertTrue(copyToSinkWithTextUtil(query, text, false));

        Assert.assertEquals(text, query.toString());
    }

    @Test
    public void testStartsWith() {
        Assert.assertTrue(Utf8s.startsWith(new Utf8String("фу бар баз"), new Utf8String("фу")));
        Assert.assertFalse(Utf8s.startsWith(new Utf8String("фу бар баз"), new Utf8String("бар")));
        Assert.assertTrue(Utf8s.startsWith(new Utf8String("фу бар баз"), Utf8String.EMPTY));
        Assert.assertFalse(Utf8s.startsWith(Utf8String.EMPTY, new Utf8String("фу-фу-фу")));
    }

    @Test
    public void testStartsWithAscii() {
        Assert.assertTrue(Utf8s.startsWithAscii(new Utf8String("foo bar baz"), "foo"));
        Assert.assertFalse(Utf8s.startsWithAscii(new Utf8String("foo bar baz"), "bar"));
        Assert.assertTrue(Utf8s.startsWithAscii(new Utf8String("foo bar baz"), ""));
        Assert.assertFalse(Utf8s.startsWithAscii(Utf8String.EMPTY, "foo"));
    }

    @Test
    public void testStrCpy() {
        final int size = 32;
        long mem = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        DirectUtf8String str = new DirectUtf8String();
        str.of(mem, mem + size);
        try {
            Utf8StringSink sink = new Utf8StringSink();
            sink.repeat("a", size);
            Utf8s.strCpy(sink, size, mem);

            TestUtils.assertEquals(sink, str);

            // overwrite the sink contents
            sink.clear();
            sink.repeat("b", size);
            sink.clear();

            Utf8s.strCpy(mem, mem + size, sink);

            TestUtils.assertEquals(sink, str);
        } finally {
            Unsafe.free(mem, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testStrCpyAscii() {
        final int size = 32;
        long mem = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        DirectUtf8String actualString = new DirectUtf8String();
        actualString.of(mem, mem + size);
        try {
            StringSink expectedSink = new StringSink();
            expectedSink.repeat("a", size);
            expectedSink.putAscii("foobar"); // this should get ignored by Utf8s.strCpyAscii()
            Utf8s.strCpyAscii(expectedSink.toString().toCharArray(), 0, size, mem);

            expectedSink.clear(size);
            TestUtils.assertEquals(expectedSink, actualString);

            expectedSink.clear();
            expectedSink.repeat("b", size);
            Utf8s.strCpyAscii(expectedSink, mem);

            expectedSink.clear(size);
            TestUtils.assertEquals(expectedSink, actualString);

            actualString.of(mem, mem + (size / 2));

            expectedSink.clear();
            expectedSink.repeat("c", size);
            Utf8s.strCpyAscii(expectedSink, size / 2, mem);

            expectedSink.clear(size / 2);
            TestUtils.assertEquals(expectedSink, actualString);

            expectedSink.clear();
            expectedSink.repeat("d", size);
            Utf8s.strCpyAscii(expectedSink, 0, size / 2, mem);

            expectedSink.clear(size / 2);
            TestUtils.assertEquals(expectedSink, actualString);
        } finally {
            Unsafe.free(mem, size, MemoryTag.NATIVE_DEFAULT);
        }
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
            Assert.assertEquals(0, Utf8s.utf8CharDecode(p, p));
            // one byte
            Unsafe.getUnsafe().putByte(p, (byte) 0xFF);
            Assert.assertEquals(0, Utf8s.utf8CharDecode(p, p + 1));
            Unsafe.getUnsafe().putByte(p, (byte) 0xC0);
            Assert.assertEquals(0, Utf8s.utf8CharDecode(p, p + 1));
            Unsafe.getUnsafe().putByte(p, (byte) 0x80);
            Assert.assertEquals(0, Utf8s.utf8CharDecode(p, p + 1));
            // two bytes
            Unsafe.getUnsafe().putByte(p, (byte) 0xC0);
            Unsafe.getUnsafe().putByte(p + 1, (byte) 0x80);
            Assert.assertEquals(0, Utf8s.utf8CharDecode(p, p + 2));

            Unsafe.getUnsafe().putByte(p, (byte) 0xC1);
            Unsafe.getUnsafe().putByte(p + 1, (byte) 0xBF);
            Assert.assertEquals(0, Utf8s.utf8CharDecode(p, p + 2));

            Unsafe.getUnsafe().putByte(p, (byte) 0xC2);
            Unsafe.getUnsafe().putByte(p + 1, (byte) 0x00);
            Assert.assertEquals(0, Utf8s.utf8CharDecode(p, p + 2));

            Unsafe.getUnsafe().putByte(p, (byte) 0xE0);
            Unsafe.getUnsafe().putByte(p + 1, (byte) 0x80);
            Unsafe.getUnsafe().putByte(p + 2, (byte) 0xC0);
            Assert.assertEquals(0, Utf8s.utf8CharDecode(p, p + 3));

            Unsafe.getUnsafe().putByte(p, (byte) 0xE0);
            Unsafe.getUnsafe().putByte(p + 1, (byte) 0xC0);
            Unsafe.getUnsafe().putByte(p + 2, (byte) 0xBF);
            Assert.assertEquals(0, Utf8s.utf8CharDecode(p, p + 3));

            Unsafe.getUnsafe().putByte(p, (byte) 0xE0);
            Unsafe.getUnsafe().putByte(p + 1, (byte) 0xA0);
            Unsafe.getUnsafe().putByte(p + 2, (byte) 0x7F);
            Assert.assertEquals(0, Utf8s.utf8CharDecode(p, p + 3));

            Unsafe.getUnsafe().putByte(p, (byte) 0xED);
            Unsafe.getUnsafe().putByte(p + 1, (byte) 0xAE);
            Unsafe.getUnsafe().putByte(p + 2, (byte) 0x80);
            Assert.assertEquals(0, Utf8s.utf8CharDecode(p, p + 3));
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
            Utf8s.utf8ToUtf16(p, p + bytes.length, b);
            TestUtils.assertEquals(in, b.toString());
        } finally {
            Unsafe.free(p, 8 * 0xffff, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUtf8SupportZ() {
        final int nChars = 128;
        final StringBuilder expected = new StringBuilder();
        for (int i = 0; i < nChars; i++) {
            expected.append(i);
        }

        String in = expected.toString();
        byte[] bytes = in.getBytes(StandardCharsets.UTF_8);
        final int nBytes = bytes.length + 1; // +1 byte for the NULL terminator

        long mem = Unsafe.malloc(nBytes, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < nBytes - 1; i++) {
                Unsafe.getUnsafe().putByte(mem + i, bytes[i]);
            }
            Unsafe.getUnsafe().putByte(mem + nBytes - 1, (byte) 0);

            StringSink b = new StringSink();
            Utf8s.utf8ToUtf16Z(mem, b);
            TestUtils.assertEquals(in, b.toString());
        } finally {
            Unsafe.free(mem, nBytes, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testUtf8toUtf16() {
        StringSink utf16Sink = new StringSink();
        String empty = "";
        String ascii = "abc";
        String cyrillic = "абв";
        String chinese = "你好";
        String emoji = "😀";
        String mixed = "abcабв你好😀";
        String[] strings = {empty, ascii, cyrillic, chinese, emoji, mixed};
        byte[] terminators = {':', '-', ' ', '\0'};
        try (DirectUtf8Sink utf8Sink = new DirectUtf8Sink(4)) {
            for (String left : strings) {
                for (String right : strings) {
                    for (byte terminator : terminators) {
                        // test with terminator (left + terminator + right)
                        String input = left + (char) terminator + right;
                        int expectedUtf8ByteRead = left.getBytes(StandardCharsets.UTF_8).length;
                        assertUtf8ToUtf16WithTerminator(utf8Sink, utf16Sink, input, left, terminator, expectedUtf8ByteRead);
                    }
                    for (byte terminator : terminators) {
                        // test without terminator (left + right)
                        String input = left + right;
                        int expectedUtf8ByteRead = input.getBytes(StandardCharsets.UTF_8).length;
                        assertUtf8ToUtf16WithTerminator(utf8Sink, utf16Sink, input, input, terminator, expectedUtf8ByteRead);
                    }
                }
            }
        }
    }

    private static void assertUtf8ToUtf16WithTerminator(
            DirectUtf8Sink utf8Sink,
            StringSink utf16Sink,
            String inputString,
            String expectedDecodedString,
            byte terminator,
            int expectedUtf8ByteRead
    ) {
        utf8Sink.clear();
        utf16Sink.clear();

        utf8Sink.put(inputString);
        int n = Utf8s.utf8ToUtf16(utf8Sink, utf16Sink, terminator);
        Assert.assertEquals(inputString, expectedUtf8ByteRead, n);
        TestUtils.assertEquals(expectedDecodedString, utf16Sink);

        Assert.assertEquals(inputString, Utf8s.stringFromUtf8Bytes(utf8Sink));
        Assert.assertEquals(inputString, Utf8s.stringFromUtf8Bytes(utf8Sink.lo(), utf8Sink.hi()));

        Assert.assertEquals(inputString, utf8Sink.toString());
    }

    private static void testUtf8Char(String x, long p, boolean failExpected) {
        byte[] bytes = x.getBytes(Files.UTF_8);
        for (int i = 0, n = Math.min(bytes.length, 8); i < n; i++) {
            Unsafe.getUnsafe().putByte(p + i, bytes[i]);
        }
        int res = Utf8s.utf8CharDecode(p, p + bytes.length);
        boolean eq = x.charAt(0) == (char) Numbers.decodeHighShort(res);
        Assert.assertTrue(failExpected != eq);
    }

    private boolean copyToSinkWithTextUtil(StringSink query, String text, boolean doubleQuoteParse) {
        byte[] bytes = text.getBytes(Files.UTF_8);
        long ptr = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < bytes.length; i++) {
            Unsafe.getUnsafe().putByte(ptr + i, bytes[i]);
        }

        boolean res;
        if (doubleQuoteParse) {
            res = Utf8s.utf8ToUtf16EscConsecutiveQuotes(ptr, ptr + bytes.length, query);
        } else {
            res = Utf8s.utf8ToUtf16(ptr, ptr + bytes.length, query);
        }
        Unsafe.free(ptr, bytes.length, MemoryTag.NATIVE_DEFAULT);
        return res;
    }
}
