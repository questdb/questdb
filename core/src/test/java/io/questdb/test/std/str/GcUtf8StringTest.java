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

import io.questdb.std.str.GcUtf8String;
import io.questdb.std.str.Utf8Sequence;
import org.junit.Assert;
import org.junit.Test;

public class GcUtf8StringTest {

    @Test
    public void testEncoding() {
        // Ascii chars
        final GcUtf8String s1 = new GcUtf8String("hello");
        Assert.assertEquals(5, s1.size());
        Assert.assertEquals((byte) 'h', s1.byteAt(0));
        Assert.assertEquals((byte) 'e', s1.byteAt(1));
        Assert.assertEquals((byte) 'l', s1.byteAt(2));
        Assert.assertEquals((byte) 'l', s1.byteAt(3));
        Assert.assertEquals((byte) 'o', s1.byteAt(4));

        // Nul stability.
        final GcUtf8String s2 = new GcUtf8String("hello\u0000world");
        Assert.assertEquals(11, s2.size());
        Assert.assertEquals((byte) '\0', s2.byteAt(5));

        // Code points in the latin-1 range
        final GcUtf8String s3 = new GcUtf8String("àèìòù");
        Assert.assertEquals(10, s3.size());

        // >>> "àèìòù".encode('utf-8')
        // b'\xc3\xa0\xc3\xa8\xc3\xac\xc3\xb2\xc3\xb9'
        Assert.assertEquals((byte) 0xc3, s3.byteAt(0));
        Assert.assertEquals((byte) 0xa0, s3.byteAt(1));
        Assert.assertEquals((byte) 0xc3, s3.byteAt(2));
        Assert.assertEquals((byte) 0xa8, s3.byteAt(3));
        Assert.assertEquals((byte) 0xc3, s3.byteAt(4));
        Assert.assertEquals((byte) 0xac, s3.byteAt(5));
        Assert.assertEquals((byte) 0xc3, s3.byteAt(6));
        Assert.assertEquals((byte) 0xb2, s3.byteAt(7));
        Assert.assertEquals((byte) 0xc3, s3.byteAt(8));
        Assert.assertEquals((byte) 0xb9, s3.byteAt(9));

        // Code points that are in the UTF-16 range without surrogate pairs.
        final GcUtf8String s4 = new GcUtf8String("ðãµ¶");
        Assert.assertEquals(8, s4.size());

        // >>> "ðãµ¶".encode('utf-8')
        // b'\xc3\xb0\xc3\xa3\xc2\xb5\xc2\xb6'
        Assert.assertEquals((byte) 0xc3, s4.byteAt(0));
        Assert.assertEquals((byte) 0xb0, s4.byteAt(1));
        Assert.assertEquals((byte) 0xc3, s4.byteAt(2));
        Assert.assertEquals((byte) 0xa3, s4.byteAt(3));
        Assert.assertEquals((byte) 0xc2, s4.byteAt(4));
        Assert.assertEquals((byte) 0xb5, s4.byteAt(5));
        Assert.assertEquals((byte) 0xc2, s4.byteAt(6));
        Assert.assertEquals((byte) 0xb6, s4.byteAt(7));

        // Code points that require surrogate pairs in UTF-16.
        final GcUtf8String s5 = new GcUtf8String("🧊🦞");
        Assert.assertEquals(8, s5.size());

        // >>> "🧊🦞".encode('utf-8')
        // b'\xf0\x9f\xa7\x8a\xf0\x9f\xa6\x9e'
        Assert.assertEquals((byte) 0xf0, s5.byteAt(0));
        Assert.assertEquals((byte) 0x9f, s5.byteAt(1));
        Assert.assertEquals((byte) 0xa7, s5.byteAt(2));
        Assert.assertEquals((byte) 0x8a, s5.byteAt(3));
        Assert.assertEquals((byte) 0xf0, s5.byteAt(4));
        Assert.assertEquals((byte) 0x9f, s5.byteAt(5));
        Assert.assertEquals((byte) 0xa6, s5.byteAt(6));
        Assert.assertEquals((byte) 0x9e, s5.byteAt(7));
    }

    // String builder used to avoid string identity.
    @SuppressWarnings({"StringEquality", "StringBufferReplaceableByString"})
    @Test
    public void testEquals() {
        // Null test
        final GcUtf8String s1 = new GcUtf8String("hi");
        //noinspection ConstantValue,SimplifiableAssertion
        Assert.assertFalse(s1.equals(null));

        // Self test
        // noinspection SimplifiableAssertion,EqualsWithItself
        Assert.assertTrue(s1.equals(s1));

        // Two obj equality
        final StringBuilder sb = new StringBuilder();
        sb.append('h');
        sb.append('i');
        final String src1 = sb.toString();
        final boolean srcNotIdentical = s1.toString() != src1;
        Assert.assertTrue(srcNotIdentical);
        final GcUtf8String s2 = new GcUtf8String(src1);
        Assert.assertEquals(s1, s2);

        final String src2 = "hello";
        final GcUtf8String s3 = new GcUtf8String(src2);
        Assert.assertNotEquals(src2, s3);

        // Test Utf8String vs Utf8String equality
        final GcUtf8String s4 = new GcUtf8String(src2);
        Assert.assertEquals(s3, s4);

        Assert.assertNotEquals(new GcUtf8String("hellO"), s3);
    }

    @Test
    public void testHashCode() {
        final String src = "hello";
        final GcUtf8String s1 = new GcUtf8String(src);
        Assert.assertEquals(src.hashCode(), s1.hashCode());
    }

    @Test
    public void testToString() {
        final GcUtf8String s1 = new GcUtf8String("hello");
        Assert.assertEquals("hello", s1.toString());

        final String src = "abc";
        final GcUtf8String s2 = new GcUtf8String(src);
        @SuppressWarnings("StringEquality") final boolean identity = src == s2.toString();
        Assert.assertTrue(identity);
    }

    @Test
    public void testUtf8Native() {
        final GcUtf8String s1 = new GcUtf8String("");
        Assert.assertNotEquals(0, s1.ptr());
        Assert.assertNotEquals(0, s1.lo());
        Assert.assertNotEquals(0, s1.hi());
        Assert.assertEquals(s1.lo(), s1.hi());

        final GcUtf8String s2 = new GcUtf8String("hi");
        Assert.assertEquals(2, s2.size());
        Assert.assertNotEquals(0, s2.ptr());
        Assert.assertNotEquals(0, s2.lo());
        Assert.assertNotEquals(0, s2.hi());
        Assert.assertEquals(s2.lo() + 2, s2.hi());
        Assert.assertEquals((byte) 'h', s2.byteAt(0));
        Assert.assertEquals((byte) 'i', s2.byteAt(1));
    }

    @Test
    public void testUtf8Sequence() {
        final Utf8Sequence s1 = new GcUtf8String("");
        Assert.assertEquals(0, s1.size());

        final Utf8Sequence s2 = new GcUtf8String("hello");
        Assert.assertEquals(5, s2.size());
        Assert.assertEquals('h', s2.byteAt(0));
        Assert.assertEquals('e', s2.byteAt(1));
        Assert.assertEquals('l', s2.byteAt(2));
        Assert.assertEquals('l', s2.byteAt(3));
        Assert.assertEquals('o', s2.byteAt(4));
    }
}
