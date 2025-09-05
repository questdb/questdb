/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cairo;

import io.questdb.ServerMain;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DataID;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.Uuid;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class DataIDTest extends AbstractBootstrapTest {
    @Test
    public void testDataIDSurvivesRestarts() throws Exception {
        TestUtils.assertMemoryLeak(() -> {

            // Start.
            Uuid initDataId;
            try (ServerMain serverMain = startWithEnvVariables()) {
                serverMain.start();
                initDataId = serverMain.getEngine().getDataID().get();
                Assert.assertTrue(serverMain.getEngine().getDataID().isInitialized());
            }

            // Restart 1.
            Uuid restartUuid1;
            try (ServerMain serverMain = startWithEnvVariables()) {
                serverMain.start();
                restartUuid1 = serverMain.getEngine().getDataID().get();
                Assert.assertTrue(serverMain.getEngine().getDataID().isInitialized());
            }

            Assert.assertEquals(initDataId.getLo(), restartUuid1.getLo());
            Assert.assertEquals(initDataId.getHi(), restartUuid1.getHi());

            // Restart 2.
            Uuid restartUuid2;
            try (ServerMain serverMain = startWithEnvVariables()) {
                serverMain.start();
                restartUuid2 = serverMain.getEngine().getDataID().get();
                Assert.assertTrue(serverMain.getEngine().getDataID().isInitialized());
            }

            Assert.assertEquals(initDataId.getLo(), restartUuid2.getLo());
            Assert.assertEquals(initDataId.getHi(), restartUuid2.getHi());
        });
    }

    @Test
    public void testOpenDataID() throws Exception {
        assertMemoryLeak(() -> {
            final java.io.File tmpDbRoot = new java.io.File(temp.newFolder(".testOpenDataID.installRoot"), "db");
            Assert.assertTrue(tmpDbRoot.mkdirs());
            final CairoConfiguration config = new DefaultTestCairoConfiguration(tmpDbRoot.getAbsolutePath());
            final DataID id = DataID.open(config);
            Assert.assertNotNull(id);
            Assert.assertFalse(id.isInitialized());
            Assert.assertEquals(Numbers.LONG_NULL, id.getLo());
            Assert.assertEquals(Numbers.LONG_NULL, id.getHi());

            final Rnd rnd = new Rnd(config.getMicrosecondClock().getTicks(), config.getMillisecondClock().getTicks());
            final Uuid currentId = new Uuid();
            currentId.of(rnd.nextLong(), rnd.nextLong());
            id.set(currentId.getLo(), currentId.getHi());
            Assert.assertTrue(id.isInitialized());
            Assert.assertEquals(id.getLo(), currentId.getLo());
            Assert.assertEquals(id.getHi(), currentId.getHi());

            Assert.assertEquals(id.get().getLo(), id.getLo());
            Assert.assertEquals(id.get().getHi(), id.getHi());

            final DataID updatedId = DataID.open(config);
            Assert.assertTrue(updatedId.isInitialized());
            Assert.assertEquals(updatedId.getLo(), currentId.getLo());
            Assert.assertEquals(updatedId.getHi(), currentId.getHi());
        });
    }

    /**
     * This test checks that we write the data as per the standard RFC 4122 big endian binary representation.
     */
    @Test
    public void testSpecificValue() throws Exception {
        final Uuid specific = new Uuid();
        specific.of("14cec117-b3f0-487f-83af-55e3b9acf4da");
        final byte[] expected = {
                (byte) 20, (byte) -50, (byte) -63, (byte) 23,
                (byte) -77, (byte) -16, (byte) 72, (byte) 127,
                (byte) -125, (byte) -81, (byte) 85, (byte) -29,
                (byte) -71, (byte) -84, (byte) -12, (byte) -38
        };

        assertMemoryLeak(() -> {
            final java.io.File tmpDbRoot = new java.io.File(temp.newFolder(".testSpecificValue.installRoot"), "db");
            Assert.assertTrue(tmpDbRoot.mkdirs());
            final CairoConfiguration config = new DefaultTestCairoConfiguration(tmpDbRoot.getAbsolutePath());
            final DataID id = DataID.open(config);
            Assert.assertFalse(id.isInitialized());
            id.set(specific.getLo(), specific.getHi());

            final java.io.File dataIdFile = new java.io.File(tmpDbRoot, DataID.FILENAME);
            final byte[] actual = java.nio.file.Files.readAllBytes(dataIdFile.toPath());

            Assert.assertArrayEquals(expected, actual);
        });
    }
}