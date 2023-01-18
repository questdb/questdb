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

package io.questdb;

import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class AllowedVolumePathsTests {

    private final static Rnd rnd = new Rnd();
    private final static StringSink sink = new StringSink();
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();
    private AllowedVolumePaths allowedVolumePaths;
    private Path path;


    @Before
    public void setUp() {
        path = new Path();
        allowedVolumePaths = new AllowedVolumePaths();
    }

    @After
    public void tearDown() {
        Misc.free(path);
    }

    @Test
    public void testNotValidPath0() {
        try {
            allowedVolumePaths.of(" alias      ", path);
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), "invalid syntax, missing path declaration at offset 1");
        }
    }

    @Test
    public void testNotValidPath1() {
        try {
            allowedVolumePaths.of(" alias  ->    ", path);
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), "empty value at offset 14");
        }
    }

    @Test
    public void testNotValidPath10() {
        try {
            allowedVolumePaths.of(" 'oscar  -> ''   ", path);
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), "missing closing quote at offset 6");
        }
    }

    @Test
    public void testNotValidPath11() {
        try {
            allowedVolumePaths.of(" oscar'  -> ''   ", path);
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), "missing opening quote at offset 1");
        }
    }

    @Test
    public void testNotValidPath12() {
        try {
            allowedVolumePaths.of(" 'oscar'  -> 'path   ", path);
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), "missing closing quote at offset 17");
        }
    }

    @Test
    public void testNotValidPath13() {
        try {
            allowedVolumePaths.of(" 'oscar'  -> path'   ", path);
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), "missing opening quote at offset 13");
        }
    }

    @Test
    public void testNotValidPath2() {
        try {
            allowedVolumePaths.of("   ->    ", path);
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), "empty value at offset 3");
        }
    }

    @Test
    public void testNotValidPath3() {
        try {
            allowedVolumePaths.of(" 'lola'  -> '  '   ", path);
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), "inaccessible volume [path=  ]");
        }
    }

    @Test
    public void testNotValidPath4() {
        try {
            allowedVolumePaths.of(" 'lola'  -> ''   ", path);
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), "empty value at offset 13");
        }
    }

    @Test
    public void testNotValidPath5() {
        try {
            allowedVolumePaths.of(" ''  -> '  '   ", path);
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), "empty value at offset 2");
        }
    }

    @Test
    public void testNotValidPath6() throws IOException {
        File volume = temp.newFolder("limpopo");
        try {
            allowedVolumePaths.of("volume 1 -> " + volume + ", volume 2 -> null", path);
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), "inaccessible volume [path=null]");
        } finally {
            volume.delete();
        }
    }

    @Test
    public void testNotValidPath7() throws IOException {
        File volume = temp.newFolder("limpopo");
        String volumePath = volume.getAbsolutePath();
        try {
            allowedVolumePaths.of("volume 1 -> " + volumePath + ", " + volumePath, path);
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), "invalid syntax, missing path declaration at offset " + (volumePath.length() + 14));
        } finally {
            volume.delete();
        }
    }

    @Test
    public void testNotValidPath8() throws IOException {
        File volume = temp.newFolder("limpopo");
        String volumePath = volume.getAbsolutePath();
        try {
            allowedVolumePaths.of("volume 1 -> " + volumePath + ", " + volumePath + ", ", path);
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), "invalid syntax, missing alias declaration at offset " + (volumePath.length() + 13));
        } finally {
            volume.delete();
        }
    }

    @Test
    public void testNotValidPath9() throws IOException {
        File volume = temp.newFolder("limpopo");
        String volumePath = volume.getAbsolutePath();
        try {
            allowedVolumePaths.of("volume 1 -> " + volumePath + ", volume 2 -> potato, ", path);
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), "inaccessible volume [path=potato]");
        } finally {
            volume.delete();
        }
    }

    @Test
    public void testValidEmptyPath() throws Exception {
        allowedVolumePaths.of("       ", path);
        Assert.assertNull(allowedVolumePaths.resolveAlias(""));
        Assert.assertNull(allowedVolumePaths.resolveAlias("       "));
    }

    @Test
    public void testValidSinglePath() throws Exception {
        String alias = "volumeA";
        File volume = temp.newFolder(alias);
        try {
            String volumePath = volume.getAbsolutePath();
            for (int i = 0; i < 100; i++) {
                sink.clear();
                loadVolumePath(alias, volumePath);
                allowedVolumePaths.of(sink.toString(), path);
                Assert.assertTrue(allowedVolumePaths.isValidVolumeAlias("volumeA"));
                Assert.assertTrue(allowedVolumePaths.isValidVolumeAlias("VOLUMEA"));
                Assert.assertTrue(allowedVolumePaths.isValidVolumeAlias("volumea"));
                Assert.assertEquals(volumePath, allowedVolumePaths.resolveAlias(alias));
            }
        } finally {
            volume.delete();
        }
    }

    @Test
    public void testValidVariousPaths() throws Exception {
        String alias0 = "volumeA";
        String alias1 = "驥驥驥驥";
        File volume0 = temp.newFolder(alias0);
        File volume1 = temp.newFolder(alias1);
        try {
            String volumePath0 = volume0.getAbsolutePath();
            String volumePath1 = volume1.getAbsolutePath();
            for (int i = 0; i < 100; i++) {
                sink.clear();
                loadVolumePath(alias0, volumePath0);
                sink.put(',');
                loadVolumePath(alias1, volumePath1);
                allowedVolumePaths.of(sink.toString(), path);
                Assert.assertEquals(volumePath0, allowedVolumePaths.resolveAlias(alias0));
                Assert.assertEquals(volumePath1, allowedVolumePaths.resolveAlias(alias1));
            }
        } finally {
            volume0.delete();
            volume1.delete();
        }
    }

    private void loadVolumePath(String alias, String volumePath) {
        randWhiteSpace();
        sink.put(alias);
        randWhiteSpace();
        sink.put("->");
        randWhiteSpace();
        sink.put(volumePath);
        randWhiteSpace();
    }

    private void randWhiteSpace() {
        for (int i = 0, n = Math.abs(rnd.nextInt()) % 4; i < n; i++) {
            sink.put(' ');
        }
    }
}
