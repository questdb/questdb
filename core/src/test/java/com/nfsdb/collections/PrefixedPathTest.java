/*
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
 */

package com.nfsdb.collections;

import com.nfsdb.misc.Os;
import org.junit.Assert;
import org.junit.Test;

public class PrefixedPathTest {
    @Test
    public void testBorderlineChild() throws Exception {
        try (PrefixedPath path = new PrefixedPath("/home/xterm/public", 12)) {
            Assert.assertEquals(transform("/home/xterm/public/xyz/123456789/abcd"), path.of("xyz/123456789/abcd").toString());
        }
    }

    @Test
    public void testLargeChild() throws Exception {
        try (PrefixedPath path = new PrefixedPath("/home/xterm/public", 24)) {
            Assert.assertEquals(transform("/home/xterm/public/xyz/123456789/abcdef"), path.of("xyz/123456789/abcdef").toString());
        }
    }

    @Test
    public void testReuse() throws Exception {
        try (PrefixedPath path = new PrefixedPath("/home/xterm/public", 12)) {
            Assert.assertEquals(transform("/home/xterm/public/xyz"), path.of("xyz").toString());
            Assert.assertEquals(transform("/home/xterm/public/xyz"), path.of("xyz").toString());
            Assert.assertEquals(transform("/home/xterm/public/xyz"), path.of("xyz").toString());
        }
    }

    @Test
    public void testSimpleNoSlash() throws Exception {
        try (PrefixedPath path = new PrefixedPath("/home/xterm/public")) {
            Assert.assertEquals(transform("/home/xterm/public/"), path.toString());
        }
    }

    @Test
    public void testSimpleSlash() throws Exception {
        try (PrefixedPath path = new PrefixedPath("/home/xterm/public/")) {
            Assert.assertEquals(transform("/home/xterm/public/"), path.toString());
        }
    }

    @Test
    public void testSmallChild() throws Exception {
        try (PrefixedPath path = new PrefixedPath("/home/xterm/public")) {
            Assert.assertEquals(transform("/home/xterm/public/xyz"), path.of("xyz").toString());
        }
    }

    private static String transform(final String s) {
        return Os.type == Os.WINDOWS ? s.replaceAll("/", "\\\\") : s;
    }
}