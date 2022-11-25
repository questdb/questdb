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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

public class ServerMainTest extends AbstractBootstrapTest {

    // log is needed to greedily allocate logger infra and
    // exclude it from leak detector
    @SuppressWarnings("unused")
    private static final Log LOG = LogFactory.getLog(ServerMainTest.class);

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractBootstrapTest.setUpStatic();
        try {
            createDummyConfiguration();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void setUp() {
        try (Path path = new Path().of(root).concat("db")) {
            int plen = path.length();
            Files.remove(path.concat("sys.column_versions_purge_log.lock").$());
            Files.remove(path.trimTo(plen).concat("telemetry_config.lock").$());
        }
    }

    @Test
    public void testServerMainNoReStart() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION)) {
                serverMain.start();
                serverMain.start(); // <== no effect
                serverMain.close();
                try {
                    serverMain.getCairoEngine();
                } catch (IllegalStateException ex) {
                    TestUtils.assertContains("close was called", ex.getMessage());
                }
                try {
                    serverMain.getWorkerPoolManager();
                } catch (IllegalStateException ex) {
                    TestUtils.assertContains("close was called", ex.getMessage());
                }
                serverMain.start(); // <== no effect
                serverMain.close(); // <== no effect
                serverMain.start(); // <== no effect
            }
        });
    }

    @Test
    public void testServerMainNoStart() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain ignore = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION)) {
                Os.pause();
            }
        });
    }

    @Test
    public void testServerMainPgWire() throws Exception {
        try (final ServerMain serverMain = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION)) {
            serverMain.start();
            try (Connection ignored = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES)) {
                Os.pause();
            }
        }
    }

    @Test
    public void testServerMainStart() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain("-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION)) {
                Assert.assertNotNull(serverMain.getConfiguration());
                Assert.assertNotNull(serverMain.getCairoEngine());
                Assert.assertNotNull(serverMain.getWorkerPoolManager());
                Assert.assertFalse(serverMain.hasStarted());
                Assert.assertFalse(serverMain.hasBeenClosed());
                serverMain.start();
            }
        });
    }

    @Test
    public void testServerMainStartHttpDisabled() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Map<String, String> env = new HashMap<>(System.getenv());
            env.put("QDB_HTTP_ENABLED", "false");
            Bootstrap bootstrap = new Bootstrap(null, env, "-d", root.toString(), Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION);
            try (final ServerMain serverMain = new ServerMain(bootstrap)) {
                Assert.assertFalse(serverMain.getConfiguration().getHttpServerConfiguration().isEnabled());
                serverMain.start();
            }
        });
    }
}
