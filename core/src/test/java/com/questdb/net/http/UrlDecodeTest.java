/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.net.http;

import com.questdb.std.CharSequenceObjHashMap;
import com.questdb.std.Misc;
import com.questdb.std.ObjectPool;
import com.questdb.std.Unsafe;
import com.questdb.std.str.DirectByteCharSequence;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class UrlDecodeTest {

    private final ObjectPool<DirectByteCharSequence> pool = new ObjectPool<>(DirectByteCharSequence.FACTORY, 16);
    private final CharSequenceObjHashMap<CharSequence> map = new CharSequenceObjHashMap<>();

    @Before
    public void setUp() {
        pool.clear();
        map.clear();
    }

    @Test
    public void testDuplicateAmp() {
        String v = "x=a&&y==b";
        long p = TestUtils.toMemory(v);
        try {
            Misc.urlDecode(p, p + v.length(), map, pool);
            TestUtils.assertEquals("a", map.get("x"));
            TestUtils.assertEquals("b", map.get("y"));
        } finally {
            Unsafe.free(p, v.length());
        }
    }

    @Test
    public void testSimple() {
        String v = "x=a&y=b";
        long p = TestUtils.toMemory(v);
        try {
            Misc.urlDecode(p, p + v.length(), map, pool);
            TestUtils.assertEquals("a", map.get("x"));
            TestUtils.assertEquals("b", map.get("y"));
        } finally {
            Unsafe.free(p, v.length());
        }
    }

    @Test
    public void testSingleQuote() {
        String v = "x=%27a%27&y==b";
        long p = TestUtils.toMemory(v);
        try {
            int o = Misc.urlDecode(p, p + v.length(), map, pool);
            DirectByteCharSequence cs = new DirectByteCharSequence().of(p, p + v.length() - o);
            TestUtils.assertEquals("x='a'&y==b", cs);
            TestUtils.assertEquals("'a'", map.get("x"));
            TestUtils.assertEquals("b", map.get("y"));
        } finally {
            Unsafe.free(p, v.length());
        }
    }

    @Test
    public void testTrailingEmpty() {
        String v = "x=a&y=b&z=";
        long p = TestUtils.toMemory(v);
        try {
            Misc.urlDecode(p, p + v.length(), map, pool);
            TestUtils.assertEquals("a", map.get("x"));
            TestUtils.assertEquals("b", map.get("y"));
            Assert.assertNull(map.get("z"));
        } finally {
            Unsafe.free(p, v.length());
        }
    }

    @Test
    public void testTrailingNull() {
        String v = "x=a&y=b&";
        long p = TestUtils.toMemory(v);
        try {
            Misc.urlDecode(p, p + v.length(), map, pool);
            TestUtils.assertEquals("a", map.get("x"));
            TestUtils.assertEquals("b", map.get("y"));
        } finally {
            Unsafe.free(p, v.length());
        }
    }

    @Test
    public void testURLDec() {
        String v = "x=a&y=b+c%26&z=ab%20ba&w=2";
        long p = TestUtils.toMemory(v);
        try {
            Misc.urlDecode(p, p + v.length(), map, pool);
            TestUtils.assertEquals("a", map.get("x"));
            TestUtils.assertEquals("b c&", map.get("y"));
            TestUtils.assertEquals("ab ba", map.get("z"));
            TestUtils.assertEquals("2", map.get("w"));
        } finally {
            Unsafe.free(p, v.length());
        }
    }

    @Test
    public void testURLDecSpace() {
        String v = "x=a&y=b+c&z=123";
        long p = TestUtils.toMemory(v);
        try {
            Misc.urlDecode(p, p + v.length(), map, pool);
            TestUtils.assertEquals("a", map.get("x"));
            TestUtils.assertEquals("b c", map.get("y"));
            TestUtils.assertEquals("123", map.get("z"));
        } finally {
            Unsafe.free(p, v.length());
        }
    }

    @Test
    public void testURLDecTrailingSpace() {
        String v = "x=a&y=b+c";
        long p = TestUtils.toMemory(v);
        try {
            Misc.urlDecode(p, p + v.length(), map, pool);
            TestUtils.assertEquals("a", map.get("x"));
            TestUtils.assertEquals("b c", map.get("y"));
        } finally {
            Unsafe.free(p, v.length());
        }
    }

}