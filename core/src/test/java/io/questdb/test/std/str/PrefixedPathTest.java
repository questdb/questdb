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

import io.questdb.std.Os;
import io.questdb.std.str.PrefixedPath;
import org.junit.Assert;
import org.junit.Test;

public class PrefixedPathTest {
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
            path.$();
            Assert.assertEquals(transform("/home/xterm/public/"), path.toString());
        }
    }

    @Test
    public void testSimpleSlash() {
        try (PrefixedPath path = new PrefixedPath("/home/xterm/public/")) {
            path.$();
            Assert.assertEquals(transform("/home/xterm/public/"), path.toString());
        }
    }

    @Test
    public void testSmallChild() {
        try (PrefixedPath path = new PrefixedPath("/home/xterm/public")) {
            assertThat(path, "/home/xterm/public/xyz", "xyz");
        }
    }

    private static String transform(final String s) {
        return Os.isWindows() ? s.replaceAll("/", "\\\\") : s;
    }

    private void assertThat(PrefixedPath path, String expected, CharSequence concat) {
        path.rewind().concat(concat).$();
        Assert.assertEquals(transform(expected), path.toString());
    }
}