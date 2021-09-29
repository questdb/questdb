/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.std;

import io.questdb.std.str.CharSink;
import io.questdb.std.str.FileNameExtractorCharSequence;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class CharsTest {
    private static final FileNameExtractorCharSequence extractor = new FileNameExtractorCharSequence();
    private static char separator;

    @BeforeClass
    public static void setUp() {
        separator = System.getProperty("file.separator").charAt(0);
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
    public void testIsOnlyDecimals() {
        Assert.assertTrue(Chars.isOnlyDecimals("9876543210123456789"));
        Assert.assertFalse(Chars.isOnlyDecimals(""));
        Assert.assertFalse(Chars.isOnlyDecimals(" "));
        Assert.assertFalse(Chars.isOnlyDecimals("99 "));
        Assert.assertFalse(Chars.isOnlyDecimals("987654321a123456789"));
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
    public void testUtf8Support() {

        StringBuilder expected = new StringBuilder();
        for (int i = 0; i < 0xD800; i++) {
            expected.append((char) i);
        }

        String in = expected.toString();
        long p = Unsafe.malloc(8 * 0xffff, MemoryTag.NATIVE_DEFAULT);
        try {
            byte[] bytes = in.getBytes(StandardCharsets.UTF_8);
            for (int i = 0, n = bytes.length; i < n; i++) {
                Unsafe.getUnsafe().putByte(p + i, bytes[i]);
            }
            CharSink b = new StringSink();
            Chars.utf8Decode(p, p + bytes.length, b);
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
            byte[] bytes = in.getBytes(StandardCharsets.UTF_8);
            for (int i = 0, n = bytes.length; i < n; i++) {
                Unsafe.getUnsafe().putByte(p + i, bytes[i]);
            }
            Unsafe.getUnsafe().putByte(p + bytes.length, (byte) 0);
            CharSink b = new StringSink();
            Chars.utf8DecodeZ(p, b);
            TestUtils.assertEquals(in, b.toString());
        } finally {
            Unsafe.free(p, 8 * 0xffff, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void assertThat(String expected, ObjList<Path> list) {
        Assert.assertEquals(expected, list.toString());
        for (int i = 0, n = list.size(); i < n; i++) {
            list.getQuick(i).close();
        }
    }
}
