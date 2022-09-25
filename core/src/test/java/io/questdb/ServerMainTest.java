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

import io.questdb.std.Os;
import org.junit.*;


public class ServerMainTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        try {
            createDummyConfiguration();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testServerMainStart() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain("-d", root.toString())) {
                Assert.assertNotNull(serverMain.getConfiguration());
                Assert.assertNotNull(serverMain.getCairoEngine());
                Assert.assertNotNull(serverMain.getWorkerPoolManager());
                Assert.assertFalse(serverMain.hasStarted());
                Assert.assertFalse(serverMain.hasBeenClosed());
                serverMain.start();
                Os.sleep(1000L); // do some work with the server. Also, allow some time for
                // the workers to finish what they need to do on start
            }
        });
    }
}
