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

package io.questdb.test;

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.client.Sender;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ServerMainHttpAuthTest extends AbstractBootstrapTest {
    private static final String PASSWORD = "quest";
    private static final String USER = "admin";

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration(
                PropertyKey.HTTP_USER.getPropertyPath() + "=" + USER,
                PropertyKey.HTTP_PASSWORD.getPropertyPath() + "=" + PASSWORD)
        );
        dbPath.parent().$();
    }

    @Test
    public void testBadPassword() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();

                try (Sender sender = Sender.fromConfig("http::addr=localhost:" + HTTP_PORT + ";user=admin;password=notquest;")) {
                    sender.table("x").longColumn("i", 42).atNow();
                    sender.flush();
                } catch (LineSenderException e) {
                    TestUtils.assertContains(e.getMessage(), "Unauthorized");
                }
            }
        });
    }

    @Test
    public void testConfigurationLoaded() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {

                // no need to start the server, just check that the configuration is loaded
                Assert.assertEquals(USER, serverMain.getConfiguration().getHttpServerConfiguration().getUsername());
                Assert.assertEquals(PASSWORD, serverMain.getConfiguration().getHttpServerConfiguration().getPassword());
            }
        });
    }

    @Test
    public void testMissingAuth() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();

                try (Sender sender = Sender.fromConfig("http::addr=localhost:" + HTTP_PORT + ";")) {
                    sender.table("x").longColumn("i", 42).atNow();
                    sender.flush();
                } catch (LineSenderException e) {
                    TestUtils.assertContains(e.getMessage(), "Unauthorized");
                }
            }
        });
    }

    @Test
    public void testSuccessfulAuth() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();

                try (Sender sender = Sender.fromConfig("http::addr=localhost:" + HTTP_PORT + ";user=admin;password=quest;")) {
                    sender.table("x").longColumn("i", 42).atNow();
                    sender.flush();
                }
            }
        });
    }
}
