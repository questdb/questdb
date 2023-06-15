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

import io.questdb.std.str.Utf8Native;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import org.junit.Assert;
import org.junit.Test;

import java.util.stream.Collectors;

public class Utf8StringTest {

    @Test
    public void testEncoding() {
        // Ascii chars
        final Utf8String s1 = new Utf8String("hello");
        Assert.assertEquals(5, s1.length());
        Assert.assertEquals(5, s1.size());
        Assert.assertEquals('h', s1.charAt(0));
        Assert.assertEquals('e', s1.charAt(1));
        Assert.assertEquals('l', s1.charAt(2));
        Assert.assertEquals('l', s1.charAt(3));
        Assert.assertEquals('o', s1.charAt(4));
        Assert.assertEquals((byte) 'h', s1.byteAt(0));
        Assert.assertEquals((byte) 'e', s1.byteAt(1));
        Assert.assertEquals((byte) 'l', s1.byteAt(2));
        Assert.assertEquals((byte) 'l', s1.byteAt(3));
        Assert.assertEquals((byte) 'o', s1.byteAt(4));

        // Nul stability.
        final Utf8String s2 = new Utf8String("hello\u0000world");
        Assert.assertEquals(11, s2.length());
        Assert.assertEquals(11, s2.size());
        Assert.assertEquals('\0', s2.charAt(5));
        Assert.assertEquals((byte) '\0', s2.byteAt(5));

        // Code points in the latin-1 range
        final Utf8String s3 = new Utf8String("àèìòù");
        Assert.assertEquals(5, s3.length());
        Assert.assertEquals(10, s3.size());

        // >>> "àèìòù".encode('utf-8')
        // b'\xc3\xa0\xc3\xa8\xc3\xac\xc3\xb2\xc3\xb9'
        Assert.assertEquals('à', s3.charAt(0));
        Assert.assertEquals('è', s3.charAt(1));
        Assert.assertEquals('ì', s3.charAt(2));
        Assert.assertEquals('ò', s3.charAt(3));
        Assert.assertEquals('ù', s3.charAt(4));
        Assert.assertEquals((byte)0xc3, s3.byteAt(0));
        Assert.assertEquals((byte)0xa0, s3.byteAt(1));
        Assert.assertEquals((byte)0xc3, s3.byteAt(2));
        Assert.assertEquals((byte)0xa8, s3.byteAt(3));
        Assert.assertEquals((byte)0xc3, s3.byteAt(4));
        Assert.assertEquals((byte)0xac, s3.byteAt(5));
        Assert.assertEquals((byte)0xc3, s3.byteAt(6));
        Assert.assertEquals((byte)0xb2, s3.byteAt(7));
        Assert.assertEquals((byte)0xc3, s3.byteAt(8));
        Assert.assertEquals((byte)0xb9, s3.byteAt(9));

        // Code points that are in the UTF-16 range without surrogate pairs.
        final Utf8String s4 = new Utf8String("ðãµ¶");
        Assert.assertEquals(4, s4.length());
        Assert.assertEquals(8, s4.size());
        Assert.assertEquals('ð', s4.charAt(0));
        Assert.assertEquals('ã', s4.charAt(1));
        Assert.assertEquals('µ', s4.charAt(2));
        Assert.assertEquals('¶', s4.charAt(3));

        // >>> "ðãµ¶".encode('utf-8')
        // b'\xc3\xb0\xc3\xa3\xc2\xb5\xc2\xb6'
        Assert.assertEquals((byte)0xc3, s4.byteAt(0));
        Assert.assertEquals((byte)0xb0, s4.byteAt(1));
        Assert.assertEquals((byte)0xc3, s4.byteAt(2));
        Assert.assertEquals((byte)0xa3, s4.byteAt(3));
        Assert.assertEquals((byte)0xc2, s4.byteAt(4));
        Assert.assertEquals((byte)0xb5, s4.byteAt(5));
        Assert.assertEquals((byte)0xc2, s4.byteAt(6));
        Assert.assertEquals((byte)0xb6, s4.byteAt(7));

        // Code points that require surrogate pairs in UTF-16.
        final Utf8String s5 = new Utf8String("🧊🦞");
        Assert.assertEquals(4, s5.length());
        Assert.assertEquals(8, s5.size());

        // ['0xd83e', '0xddca', '0xd83e', '0xdd9e']
        Assert.assertEquals(0xd83e, s5.charAt(0));
        Assert.assertEquals(0xddca, s5.charAt(1));
        Assert.assertEquals(0xd83e, s5.charAt(2));
        Assert.assertEquals(0xdd9e, s5.charAt(3));

        final int[] chars = s5.chars().toArray();
        Assert.assertEquals(0xd83e, chars[0]);
        Assert.assertEquals(0xddca, chars[1]);
        Assert.assertEquals(0xd83e, chars[2]);
        Assert.assertEquals(0xdd9e, chars[3]);

        final int[] codePoints = s5.codePoints().toArray();
        Assert.assertEquals(0x1f9ca, codePoints[0]);
        Assert.assertEquals(0x1f99e, codePoints[1]);

        // >>> "🧊🦞".encode('utf-8')
        // b'\xf0\x9f\xa7\x8a\xf0\x9f\xa6\x9e'
        Assert.assertEquals((byte)0xf0, s5.byteAt(0));
        Assert.assertEquals((byte)0x9f, s5.byteAt(1));
        Assert.assertEquals((byte)0xa7, s5.byteAt(2));
        Assert.assertEquals((byte)0x8a, s5.byteAt(3));
        Assert.assertEquals((byte)0xf0, s5.byteAt(4));
        Assert.assertEquals((byte)0x9f, s5.byteAt(5));
        Assert.assertEquals((byte)0xa6, s5.byteAt(6));
        Assert.assertEquals((byte)0x9e, s5.byteAt(7));
    }

    @Test
    public void testUtf8Sequence() {
        final Utf8Sequence s1 = new Utf8String("");
        Assert.assertEquals(0, s1.size());

        final Utf8Sequence s2 = new Utf8String("hello");
        Assert.assertEquals(5, s2.size());
        Assert.assertEquals('h', s2.byteAt(0));
        Assert.assertEquals('e', s2.byteAt(1));
        Assert.assertEquals('l', s2.byteAt(2));
        Assert.assertEquals('l', s2.byteAt(3));
        Assert.assertEquals('o', s2.byteAt(4));
    }

    @Test
    public void testUtf8Native() {
        final Utf8Native s1 = new Utf8String("");
        Assert.assertNotEquals(0, s1.ptr());
        Assert.assertNotEquals(0, s1.lo());
        Assert.assertNotEquals(0, s1.hi());
        Assert.assertEquals(s1.lo(), s1.hi());

        final Utf8Native s2 = new Utf8String("hi");
        Assert.assertEquals(2, s2.size());
        Assert.assertNotEquals(0, s2.ptr());
        Assert.assertNotEquals(0, s2.lo());
        Assert.assertNotEquals(0, s2.hi());
        Assert.assertEquals(s2.lo() + 2, s2.hi());
        Assert.assertEquals((byte)'h', s2.byteAt(0));
        Assert.assertEquals((byte)'i', s2.byteAt(1));
    }

    @Test
    public void testSubsequence() {
        final Utf8String s1 = new Utf8String("hello");
        final CharSequence s2 = s1.subSequence(1, 3);
        Assert.assertEquals(2, s2.length());
        Assert.assertEquals('e', s2.charAt(0));
        Assert.assertEquals('l', s2.charAt(1));
    }

    @Test
    public void testToString() {
        final Utf8String s1 = new Utf8String("hello");
        Assert.assertEquals("hello", s1.toString());
    }

    @Test
    public void testHashCode() {
        final String src = "hello";
        final Utf8String s1 = new Utf8String(src);
        Assert.assertEquals(src.hashCode(), s1.hashCode());
    }

    @Test
    public void testEquals() {
        // Null test
        final Utf8String s1 = new Utf8String("hi");
        final boolean nullIdentity = s1.equals(null);
        Assert.assertFalse(nullIdentity);

        // Self test
        final Utf8String s2 = new Utf8String("hi");
        final boolean identity = s1.equals(s1);
        Assert.assertTrue(identity);

        final String src = "hello";
        final Utf8String s3 = new Utf8String(src);
        Assert.assertEquals(s3, src);

        // Test pointer identity
        final Utf8Native nat1 = new Utf8Native() {
            @Override
            public long ptr() {
                return s3.ptr();
            }

            @Override
            public int size() {
                return s3.size();
            }
        };
        Assert.assertEquals(s3, nat1);

        // Test Utf8Sequence equality
        final Utf8Sequence seq1 = new Utf8Sequence() {
            @Override
            public int size() {
                return s3.size();
            }

            @Override
            public byte byteAt(int index) {
                return s3.byteAt(index);
            }
        };
        Assert.assertEquals(s3, seq1);

        Assert.assertNotEquals(s1, seq1);

        Assert.assertNotEquals(new Utf8String("hellO"), seq1);
    }
}
