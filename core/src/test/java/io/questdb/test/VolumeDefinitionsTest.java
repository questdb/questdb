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

package io.questdb.test;

import io.questdb.ServerConfigurationException;
import io.questdb.VolumeDefinitions;
import io.questdb.std.Files;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

public class VolumeDefinitionsTest {

    private static final Rnd rnd = new Rnd();
    private static final StringSink sink = new StringSink();
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();
    private Path path;
    private VolumeDefinitions volumeDefinitions;

    @Before
    public void setUp() {
        path = new Path();
        volumeDefinitions = new VolumeDefinitions();
    }

    @After
    public void tearDown() {
        Misc.free(path);
    }

    @Test
    public void testNotValidDefinitions0() {
        assertFail(" alias      ", "invalid syntax, dangling alias [alias=alias, offset=1]");
        assertFail(" alias  ->    ", "invalid syntax, missing volume path at offset 14");
        assertFail(" 'oscar  -> ''   ", "missing closing quote at offset 6");
        assertFail(" oscar'  -> ''   ", "missing opening quote at offset 1");
        assertFail(" 'oscar'  -> 'path   ", "missing closing quote at offset 17");
        assertFail(" 'oscar'  -> path'   ", "missing opening quote at offset 13");
        assertFail("   ->    ", "empty value at offset 3");
        assertFail(" 'lola'  -> '  '   ", "inaccessible volume [path=  ]");
        assertFail(" 'lola'  -> ''  ", "invalid syntax, missing volume path at offset 13");
        assertFail(" ''  -> '  '   ", "empty value at offset 2");
        assertFail(" 'peter'  -> -> '  '   ", "invalid syntax, missing volume path at offset 12");
        assertFail(",", "invalid syntax, missing alias at offset 0");
    }

    @Test
    public void testNotValidDefinitions1() {
        withVolume("limpopo", volume -> {
            String volumePath = volume.getAbsolutePath();
            assertFail("volume 1 -> " + volumePath + ", volume 2 -> null", "inaccessible volume [path=null]");
            assertFail("volume 1 -> " + volumePath + ", volume 2  ", "invalid syntax, dangling alias [alias=volume 2, offset=" + (volumePath.length() + 14) + ']');
            assertFail("volume 1 -> " + volumePath + ", volume 2  ->", "invalid syntax, missing volume path at offset " + (volumePath.length() + 26));
            assertFail("volume 1 -> " + volumePath + ", volume 2 -> " + volumePath + ",", "invalid syntax, dangling separator [sep=',']");
            assertFail("volume 1 -> " + volumePath + ", volume 2 -> " + volumePath + " ,", "invalid syntax, dangling separator [sep=',']");
            assertFail("volume 1 -> " + volumePath + ", volume 2 -> " + volumePath + ", ", "invalid syntax, dangling separator [sep=',']");
            assertFail("volume 1 -> " + volumePath + ", volume 1 -> " + volumePath + ", ", "duplicate alias [alias=volume 1]");
        });
    }

    @Test
    public void testNotValidDefinitions2() {
        withVolume("neptune", volume0 ->
                withVolume("mars", volume1 -> {
                            String rootPath = volume1.getAbsolutePath();
                            String expected = "standard volume cannot have an alias [alias=main, root=" + rootPath + ']';
                            assertFail("volume 1 -> " + volume0.getAbsolutePath() + ", main -> " + rootPath + ", ", expected, rootPath);
                        }
                )
        );
    }

    @Test
    public void testValidEmptyDefinition() throws Exception {
        volumeDefinitions.of("       ", path, "");
        Assert.assertNull(volumeDefinitions.resolveAlias(""));
        Assert.assertNull(volumeDefinitions.resolveAlias("       "));
        volumeDefinitions.forEach((alias, volumePath) -> Assert.fail());
    }

    @Test
    public void testValidSingleVolume() {
        String alias0 = "volumeA";
        withVolume(alias0, volume0 -> {
            String volumePath0 = volume0.getAbsolutePath();
            sink.clear();
            appendVolumeDefinition(alias0, volumePath0);
            parseVolumeDefinitions();
            Assert.assertNotNull(volumeDefinitions.resolveAlias("volumeA"));
            Assert.assertNotNull(volumeDefinitions.resolveAlias("VOLUMEA"));
            Assert.assertNotNull(volumeDefinitions.resolveAlias("volumea"));
            Assert.assertEquals(volumePath0, volumeDefinitions.resolveAlias(alias0));
        });
    }

    @Test
    public void testValidVariousVolumes() {
        String alias0 = "volumeA";
        String alias1 = "驥驥驥驥";
        withVolume(alias0, volume0 -> withVolume(alias1, volume1 -> {
            String volumePath0 = volume0.getAbsolutePath();
            String volumePath1 = volume1.getAbsolutePath();
            sink.clear();
            appendVolumeDefinition(alias0, volumePath0);
            sink.put(',');
            appendVolumeDefinition(alias1, volumePath1);
            parseVolumeDefinitions();
            Assert.assertEquals(volumePath0, volumeDefinitions.resolveAlias(alias0));
            Assert.assertEquals(volumePath1, volumeDefinitions.resolveAlias(alias1));
        }));
    }

    private void appendVolumeDefinition(String alias, String volumePath) {
        randWhiteSpace();
        sink.put(alias);
        randWhiteSpace();
        sink.put("->");
        randWhiteSpace();
        sink.put(volumePath);
        randSlash();
        randWhiteSpace();
    }

    private void assertFail(String text, String expectedErrorMsg) {
        assertFail(text, expectedErrorMsg, "");
    }

    private void assertFail(String text, String expectedErrorMsg, String root) {
        try {
            volumeDefinitions.of(text, path, root);
            Assert.fail();
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), expectedErrorMsg);
        }
    }

    private void parseVolumeDefinitions() {
        try {
            volumeDefinitions.of(sink.toString(), path, "");
        } catch (ServerConfigurationException unexpected) {
            Assert.fail(unexpected.getMessage());
        }
    }

    private void randSlash() {
        if (rnd.nextDouble() < 0.33) {
            sink.put(Files.SEPARATOR);
        }
    }

    private void randWhiteSpace() {
        for (int i = 0, n = Math.abs(rnd.nextInt()) % 4; i < n; i++) {
            sink.put(' ');
        }
    }

    private void withVolume(String volumeName, Consumer<File> volumeConsumer) {
        File volume = null;
        try {
            volume = temp.newFolder(volumeName);
            volumeConsumer.accept(volume);
        } catch (IOException unexpected) {
            Assert.fail(unexpected.getMessage());
        } finally {
            if (volume != null) {
                Assert.assertTrue(volume.delete());
            }
        }
    }
}
