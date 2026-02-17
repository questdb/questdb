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

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.std.Files;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

public class ServerMainIlpAuthTest extends AbstractBootstrapTest {
    private static final String AUTH_CONF_PATH = "conf/auth.conf";

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration(PropertyKey.LINE_TCP_AUTH_DB_PATH.getPropertyPath() + "=" + AUTH_CONF_PATH));
        TestUtils.unchecked(ServerMainIlpAuthTest::createIlpConfiguration);
        dbPath.parent().$();
    }

    @Test
    public void testServerMainStartIlpAuthEnabled() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                Assert.assertTrue(serverMain.getConfiguration().getLineTcpReceiverConfiguration().isEnabled());
                Assert.assertEquals(AUTH_CONF_PATH, serverMain.getConfiguration().getLineTcpReceiverConfiguration().getAuthDB());
                serverMain.start();
            }
        });
    }

    private static void createIlpConfiguration() throws FileNotFoundException, UnsupportedEncodingException {
        final String confPath = root + Files.SEPARATOR + AUTH_CONF_PATH;
        try (PrintWriter writer = new PrintWriter(confPath, CHARSET)) {
            writer.println("testUser1	ec-p-256-sha256	AKfkxOBlqBN8uDfTxu2Oo6iNsOPBnXkEH4gt44tBJKCY	AL7WVjoH-IfeX_CXo5G1xXKp_PqHUrdo3xeRyDuWNbBX");
        }
    }
}
