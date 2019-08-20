/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.std;

import com.questdb.std.str.*;
import com.questdb.test.tools.TestUtils;
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
    public void testConcat() {
        ConcatCharSequence concat = new ConcatCharSequence();
        concat.add("this");
        concat.add(" is ");
        concat.add("");
        concat.add("working");

        TestUtils.assertEquals("this is working", concat);
        Assert.assertEquals('w', concat.charAt(8));
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
        long p = Unsafe.malloc(8 * 0xffff);
        try {
            byte[] bytes = in.getBytes(StandardCharsets.UTF_8);
            for (int i = 0, n = bytes.length; i < n; i++) {
                Unsafe.getUnsafe().putByte(p + i, bytes[i]);
            }
            CharSink b = new StringSink();
            Chars.utf8Decode(p, p + bytes.length, b);
            TestUtils.assertEquals(in, b.toString());
        } finally {
            Unsafe.free(p, 8 * 0xffff);
        }
    }

    @Test
    public void testUtf8SupportZ() {

        StringBuilder expected = new StringBuilder();
        for (int i = 1; i < 0xD800; i++) {
            expected.append((char) i);
        }

        String in = expected.toString();
        long p = Unsafe.malloc(8 * 0xffff);
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
            Unsafe.free(p, 8 * 0xffff);
        }
    }

    private void assertThat(String expected, ObjList<Path> list) {
        Assert.assertEquals(expected, list.toString());
        for (int i = 0, n = list.size(); i < n; i++) {
            list.getQuick(i).close();
        }
    }
}
