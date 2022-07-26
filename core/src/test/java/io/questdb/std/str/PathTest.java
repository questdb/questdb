/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.std.str;

import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

public class PathTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();
    private final Path path = new Path();
    private final char separator = System.getProperty("file.separator").charAt(0);

    @Test
    public void testConcatNoSlash() {
        TestUtils.assertEquals("xyz" + separator + "123", path.of("xyz").concat("123").$());
    }

    @Test
    public void testConcatWithSlash() {
        TestUtils.assertEquals("xyz" + separator + "123", path.of("xyz/").concat("123").$());
    }

    @Test
    public void testLpszConcat() {
        try (Path p1 = new Path()) {
            p1.of("abc").concat("123").$();
            try (Path p = new Path()) {
                p.of("/xyz/").concat(p1.address()).$();
                Assert.assertEquals(separator + "xyz" + separator + "abc" + separator + "123", p.toString());
            }
        }
    }

    @Test
    public void testOverflow() {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < 256; i++) {
            b.append('9');
        }

        try (Path p = new Path()) {
            TestUtils.assertEquals("9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999" + System.getProperty("file.separator") + "xyz",
                    p.of(b).concat("xyz").$());
        }
    }

    @Test
    public void testPathThreadLocalDoesNotAllocateOnRelease() {
        final long count = Unsafe.getMallocCount();
        Path.clearThreadLocals();
        Assert.assertEquals(count, Unsafe.getMallocCount());
    }

    @Test
    public void testPathOfPathUtf8() {
        Os.init();

        path.of("пути неисповедимы");
        Path path2 = new Path();
        path2.of(path);
        TestUtils.assertEquals(path, path2);

        // Reduce
        path.of("пути");
        path2.of(path);
        TestUtils.assertEquals(path, path2);

        // Extend
        path.of(Chars.repeat("пути неисповедимы", 50)).$();
        path2.of(path);
        TestUtils.assertEquals(path, path2);

        // Clear
        path.of("").$();
        path2.of(path);
        TestUtils.assertEquals(path, path2);

        // Destination closed
        path.of("1").$();
        path2.close();
        path2.of(path);
        TestUtils.assertEquals(path, path2);

        // Self copy
        path2.of(path2);
        TestUtils.assertEquals(path, path2);
    }

    @Test
    public void testSeekZ() {
        try (Path path = new Path()) {
            path.of("12345656788990").$();

            Assert.assertEquals(14, path.length());

            String inject = "hello\0";
            Chars.asciiStrCpy(inject, 0, inject.length(), path.address());

            Assert.assertSame(path, path.seekZ());
            TestUtils.assertEquals("hello", path);

            path.chop$().concat("next");
            TestUtils.assertEquals("hello" + Files.SEPARATOR + "next", path);
        }
    }

    @Test
    public void testSimple() {
        TestUtils.assertEquals("xyz", path.of("xyz").$());
    }

    @Test
    public void testZeroEnd() throws Exception {
        File dir = temp.newFolder("a", "b", "c");
        File f = new File(dir, "f.txt");
        Assert.assertTrue(f.createNewFile());

        Assert.assertTrue(Files.exists(path.of(temp.getRoot().getAbsolutePath()).concat("a").concat("b").concat("c").concat("f.txt").$()));
    }

    @Test
    public void testParent() {
        try (
                Path path = new Path();
                Path expected = new Path()
        ) {
            expected.trimTo(0).concat("A").concat("B").concat("C").$();
            path.of(expected).concat("D").$();
            Assert.assertEquals(expected.toString(), path.parent$().toString());
            path.of(expected).concat("D").slash$();
            Assert.assertEquals(expected.toString(), path.parent$().toString());
        }
    }
}