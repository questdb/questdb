/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.http;

import com.nfsdb.collections.CharSequenceObjHashMap;
import com.nfsdb.collections.DirectByteCharSequence;
import com.nfsdb.collections.ObjectPool;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class UrlDecodeTest {

    private final ObjectPool<DirectByteCharSequence> pool = new ObjectPool<>(DirectByteCharSequence.FACTORY, 16);
    private final CharSequenceObjHashMap<CharSequence> map = new CharSequenceObjHashMap<>();

    @Before
    public void setUp() throws Exception {
        pool.clear();
        map.clear();
    }

    @Test
    public void testDuplicateAmp() throws Exception {
        parse("x=a&&y==b");
        TestUtils.assertEquals("a", map.get("x"));
        TestUtils.assertEquals("b", map.get("y"));
    }

    @Test
    public void testSimple() throws Exception {
        parse("x=a&y=b");
        TestUtils.assertEquals("a", map.get("x"));
        TestUtils.assertEquals("b", map.get("y"));
    }

    @Test
    public void testTrailingEmpty() throws Exception {
        parse("x=a&y=b&z=");
        TestUtils.assertEquals("a", map.get("x"));
        TestUtils.assertEquals("b", map.get("y"));
        Assert.assertNull(map.get("z"));
    }

    @Test
    public void testTrailingNull() throws Exception {
        parse("x=a&y=b&");
        TestUtils.assertEquals("a", map.get("x"));
        TestUtils.assertEquals("b", map.get("y"));
    }

    @Test
    public void testURLDec() throws Exception {
        parse("x=a&y=b+c%26&z=ab%20ba&w=2");
        TestUtils.assertEquals("a", map.get("x"));
        TestUtils.assertEquals("b c&", map.get("y"));
        TestUtils.assertEquals("ab ba", map.get("z"));
        TestUtils.assertEquals("2", map.get("w"));
    }

    @Test
    public void testURLDecSpace() throws Exception {
        parse("x=a&y=b+c&z=123");
        TestUtils.assertEquals("a", map.get("x"));
        TestUtils.assertEquals("b c", map.get("y"));
        TestUtils.assertEquals("123", map.get("z"));
    }

    @Test
    public void testURLDecTrailingSpace() throws Exception {
        String qry = "x=a&y=b+c";
        parse(qry);
        TestUtils.assertEquals("a", map.get("x"));
        TestUtils.assertEquals("b c", map.get("y"));
    }

    private void parse(String s) {
        long p = TestUtils.toMemory(s);
        try {
            Request.urlDecode(p, p + s.length(), map, pool);
        } finally {
            Unsafe.getUnsafe().freeMemory(p);
        }
    }
}