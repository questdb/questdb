/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.net.http;

import com.nfsdb.misc.Misc;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.std.CharSequenceObjHashMap;
import com.nfsdb.std.DirectByteCharSequence;
import com.nfsdb.std.ObjectPool;
import com.nfsdb.test.tools.TestUtils;
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
    public void testDuplicateAmp() throws Exception {
        String v = "x=a&&y==b";
        long p = TestUtils.toMemory(v);
        try {
            Misc.urlDecode(p, p + v.length(), map, pool);
            TestUtils.assertEquals("a", map.get("x"));
            TestUtils.assertEquals("b", map.get("y"));
        } finally {
            Unsafe.getUnsafe().freeMemory(p);
        }
    }

    @Test
    public void testSimple() throws Exception {
        String v = "x=a&y=b";
        long p = TestUtils.toMemory(v);
        try {
            Misc.urlDecode(p, p + v.length(), map, pool);
            TestUtils.assertEquals("a", map.get("x"));
            TestUtils.assertEquals("b", map.get("y"));
        } finally {
            Unsafe.getUnsafe().freeMemory(p);
        }
    }

    @Test
    public void testTrailingEmpty() throws Exception {
        String v = "x=a&y=b&z=";
        long p = TestUtils.toMemory(v);
        try {
            Misc.urlDecode(p, p + v.length(), map, pool);
            TestUtils.assertEquals("a", map.get("x"));
            TestUtils.assertEquals("b", map.get("y"));
            Assert.assertNull(map.get("z"));
        } finally {
            Unsafe.getUnsafe().freeMemory(p);
        }
    }

    @Test
    public void testTrailingNull() throws Exception {
        String v = "x=a&y=b&";
        long p = TestUtils.toMemory(v);
        try {
            Misc.urlDecode(p, p + v.length(), map, pool);
            TestUtils.assertEquals("a", map.get("x"));
            TestUtils.assertEquals("b", map.get("y"));
        } finally {
            Unsafe.getUnsafe().freeMemory(p);
        }
    }

    @Test
    public void testURLDec() throws Exception {
        String v = "x=a&y=b+c%26&z=ab%20ba&w=2";
        long p = TestUtils.toMemory(v);
        try {
            Misc.urlDecode(p, p + v.length(), map, pool);
            TestUtils.assertEquals("a", map.get("x"));
            TestUtils.assertEquals("b c&", map.get("y"));
            TestUtils.assertEquals("ab ba", map.get("z"));
            TestUtils.assertEquals("2", map.get("w"));
        } finally {
            Unsafe.getUnsafe().freeMemory(p);
        }
    }

    @Test
    public void testURLDecSpace() throws Exception {
        String v = "x=a&y=b+c&z=123";
        long p = TestUtils.toMemory(v);
        try {
            Misc.urlDecode(p, p + v.length(), map, pool);
            TestUtils.assertEquals("a", map.get("x"));
            TestUtils.assertEquals("b c", map.get("y"));
            TestUtils.assertEquals("123", map.get("z"));
        } finally {
            Unsafe.getUnsafe().freeMemory(p);
        }
    }

    @Test
    public void testURLDecTrailingSpace() throws Exception {
        String v = "x=a&y=b+c";
        long p = TestUtils.toMemory(v);
        try {
            Misc.urlDecode(p, p + v.length(), map, pool);
            TestUtils.assertEquals("a", map.get("x"));
            TestUtils.assertEquals("b c", map.get("y"));
        } finally {
            Unsafe.getUnsafe().freeMemory(p);
        }
    }

}