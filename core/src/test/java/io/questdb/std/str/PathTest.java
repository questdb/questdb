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

import io.questdb.std.Files;
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
}