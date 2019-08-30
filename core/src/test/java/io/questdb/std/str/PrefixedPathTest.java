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

package io.questdb.std.str;

import io.questdb.std.Os;
import org.junit.Assert;
import org.junit.Test;

public class PrefixedPathTest {
    private static String transform(final String s) {
        return Os.type == Os.WINDOWS ? s.replaceAll("/", "\\\\") : s;
    }

    @Test
    public void testBorderlineChild() {
        try (PrefixedPath path = new PrefixedPath("/home/xterm/public", 12)) {
            assertThat(path, "/home/xterm/public/xyz/123456789/abcd", "xyz/123456789/abcd");
        }
    }

    @Test
    public void testLargeChild() {
        try (PrefixedPath path = new PrefixedPath("/home/xterm/public", 24)) {
            assertThat(path, "/home/xterm/public/xyz/123456789/abcdef", "xyz/123456789/abcdef");
        }
    }

    @Test
    public void testReuse() {
        try (PrefixedPath path = new PrefixedPath("/home/xterm/public", 12)) {
            assertThat(path, "/home/xterm/public/xyz", "xyz");
            assertThat(path, "/home/xterm/public/xyz", "xyz");
            assertThat(path, "/home/xterm/public/xyz", "xyz");
        }
    }

    @Test
    public void testSimpleNoSlash() {
        try (PrefixedPath path = new PrefixedPath("/home/xterm/public")) {
            Assert.assertEquals(transform("/home/xterm/public/"), path.$().toString());
        }
    }

    @Test
    public void testSimpleSlash() {
        try (PrefixedPath path = new PrefixedPath("/home/xterm/public/")) {
            Assert.assertEquals(transform("/home/xterm/public/"), path.$().toString());
        }
    }

    @Test
    public void testSmallChild() {
        try (PrefixedPath path = new PrefixedPath("/home/xterm/public")) {
            assertThat(path, "/home/xterm/public/xyz", "xyz");
        }
    }

    private void assertThat(PrefixedPath path, String expected, CharSequence concat) {
        Assert.assertEquals(transform(expected), path.rewind().concat(concat).$().toString());
    }
}