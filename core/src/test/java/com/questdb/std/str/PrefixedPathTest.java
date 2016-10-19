/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
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
 ******************************************************************************/

package com.questdb.std.str;

import com.questdb.misc.Os;
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