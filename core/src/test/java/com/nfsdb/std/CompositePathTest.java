/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

package com.nfsdb.std;

import com.nfsdb.misc.Files;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

public class CompositePathTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();
    private final CompositePath path = new CompositePath();
    private final char separator = System.getProperty("file.separator").charAt(0);

    @Test
    public void testConcatNoSlash() throws Exception {
        TestUtils.assertEquals("xyz" + separator + "123", path.of("xyz").concat("123").$());
    }

    @Test
    public void testConcatWithSlash() throws Exception {
        TestUtils.assertEquals("xyz" + separator + "123", path.of("xyz/").concat("123").$());
    }

    @Test
    public void testOverflow() throws Exception {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < 256; i++) {
            b.append('9');
        }

        try (CompositePath p = new CompositePath()) {
            TestUtils.assertEquals("9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999/xyz",
                    p.of(b).concat("xyz").$());
        }
    }

    @Test
    public void testSimple() throws Exception {
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