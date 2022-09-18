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

import io.questdb.test.tools.TestUtils;
import org.junit.*;


public class ServerMainTest extends AbstractBootstrapTest {

    @Test
    @Ignore("this is fubar, not sure why")
    public void testServerMainStartNotCalled() throws Exception {
        createDummyConfiguration();
        try (final ServerMain serverMain = new ServerMain("-d", root.toString())) {
            Assert.assertNotNull(serverMain.getConfiguration());
            Assert.assertNotNull(serverMain.getCairoEngine());
            Assert.assertNotNull(serverMain.getWorkerPoolManager());
            Assert.assertFalse(serverMain.hasStarted());
            Assert.assertFalse(serverMain.hasBeenClosed());
        } catch (IllegalStateException err) {
            TestUtils.assertContains("start was not called at all", err.getMessage());
        }
    }

    @Test
    @Ignore("this is fubar, not sure why")
    public void testServerMainStart() throws Exception {
        createDummyConfiguration();
        try (final ServerMain serverMain = new ServerMain("-d", root.toString())) {
            Assert.assertNotNull(serverMain.getConfiguration());
            Assert.assertNotNull(serverMain.getCairoEngine());
            Assert.assertNotNull(serverMain.getWorkerPoolManager());
            Assert.assertFalse(serverMain.hasStarted());
            Assert.assertFalse(serverMain.hasBeenClosed());
            serverMain.start(false);
        }
    }
}
