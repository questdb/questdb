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

import io.questdb.*;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.FilesFacadeImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

public class DynamicPropServerConfigurationTest extends AbstractTest {

    private SOCountDownLatch latch;
    private File serverConf;

    @Test
    public void TestPgWireCredentialsReloadByDeletingProp() throws Exception {
        assertMemoryLeak(() -> {
            try (FileWriter w = new FileWriter(serverConf)) {
                w.write("pg.user=steven\n");
                w.write("pg.password=sklar\n");
            }

            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                try (Connection conn = getConnection("steven", "sklar")) {
                    Assert.assertFalse(conn.isClosed());
                }

                // Overwrite file to remove props
                try (FileWriter w = new FileWriter(serverConf, false)) {
                    w.write("\n");
                }

                latch.await();

                try (Connection conn = getConnection("admin", "quest")) {
                    Assert.assertFalse(conn.isClosed());
                }
            }
        });
    }

    @Test
    public void TestPgWireCredentialsReloadWithChangedProp() throws Exception {
        assertMemoryLeak(() -> {
            try (FileWriter w = new FileWriter(serverConf)) {
                w.write("pg.user=steven\n");
                w.write("pg.password=sklar\n");
            }

            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                try (Connection conn = getConnection("steven", "sklar")) {
                    Assert.assertFalse(conn.isClosed());
                }

                try (FileWriter w = new FileWriter(serverConf)) {
                    w.write("pg.user=nevets\n");
                    w.write("pg.password=ralks\n");
                }

                latch.await();

                try (Connection conn = getConnection("nevets", "ralks")) {
                    Assert.assertFalse(conn.isClosed());
                }
                Assert.assertThrows(PSQLException.class, () -> getConnection("admin", "quest"));
            }
        });
    }

    @Test
    public void TestPgWireCredentialsReloadWithChangedPropAfterRecreatedFile() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                try (Connection conn = getConnection("admin", "quest")) {
                    Assert.assertFalse(conn.isClosed());
                }

                Assert.assertTrue(serverConf.delete());
                Assert.assertTrue(serverConf.createNewFile());

                try (FileWriter w = new FileWriter(serverConf)) {
                    w.write("pg.user=steven\n");
                    w.write("pg.password=sklar\n");
                }

                latch.await();

                try (Connection conn = getConnection("steven", "sklar")) {
                    Assert.assertFalse(conn.isClosed());
                }

                Assert.assertThrows(PSQLException.class, () -> getConnection("admin", "quest"));
            }
        });

    }

    @Test
    public void TestPgWireCredentialsReloadWithNewProp() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                try (Connection conn = getConnection("admin", "quest")) {
                    Assert.assertFalse(conn.isClosed());
                }

                try (FileWriter w = new FileWriter(serverConf)) {
                    w.write("pg.user=steven\n");
                    w.write("pg.password=sklar\n");
                }

                latch.await();

                try (Connection conn = getConnection("steven", "sklar")) {
                    Assert.assertFalse(conn.isClosed());
                }

                Assert.assertThrows(PSQLException.class, () -> getConnection("admin", "quest"));

            }
        });
    }

    public Bootstrap getBootstrap() {
        return new Bootstrap(
                getBootstrapConfig(),
                Bootstrap.getServerMainArgs(root)
        );
    }

    public BootstrapConfiguration getBootstrapConfig() {
        return new DefaultBootstrapConfiguration() {
            @Override
            public ServerConfiguration getServerConfiguration(Bootstrap bootstrap) {
                try {
                    return new DynamicPropServerConfiguration(
                            bootstrap.getRootDirectory(),
                            bootstrap.loadProperties(),
                            getEnv(),
                            bootstrap.getLog(),
                            bootstrap.getBuildInformation(),
                            FilesFacadeImpl.INSTANCE,
                            bootstrap.getMicrosecondClock(),
                            FactoryProviderFactoryImpl.INSTANCE,
                            true,
                            () -> latch.countDown()
                    );
                } catch (Exception exc) {
                    Assert.fail(exc.getMessage());
                    return null;
                }
            }
        };
    }

    @Before
    public void setUp() {
        latch = new SOCountDownLatch(1);
        Path serverConfPath = Paths.get(temp.getRoot().getAbsolutePath(), "dbRoot", "conf", "server.conf");
        try {
            Files.createDirectories(serverConfPath.getParent());
            serverConf = serverConfPath.toFile();
            Assert.assertTrue(serverConf.createNewFile());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertTrue(serverConf.exists());
    }

    @Test
    public void testPgNamedStatementLimitReloadWithChangedProp() throws Exception {
        assertMemoryLeak(() -> {
            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                int namedStatementLimit = serverMain.getConfiguration().getPGWireConfiguration().getNamedStatementLimit();
                Assert.assertEquals(10_000, namedStatementLimit);

                try (FileWriter w = new FileWriter(serverConf)) {
                    w.write("pg.named.statement.limit=10\n");
                }

                latch.await();

                namedStatementLimit = serverMain.getConfiguration().getPGWireConfiguration().getNamedStatementLimit();
                Assert.assertEquals(10, namedStatementLimit);
            }
        });
    }

    @Test
    public void testReloadDisabled() throws Exception {
        assertMemoryLeak(() -> {
            try (FileWriter w = new FileWriter(serverConf)) {
                w.write("pg.user=steven\n");
                w.write("pg.password=sklar\n");
                w.write("config.reload.enabled=false\n");
            }

            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                try (Connection conn = getConnection("steven", "sklar")) {
                    Assert.assertFalse(conn.isClosed());
                }

                // Overwrite file to remove props
                try (FileWriter w = new FileWriter(serverConf, false)) {
                    w.write("\n");
                }

                Assert.assertFalse(latch.await(TimeUnit.SECONDS.toNanos(1)));

                try (Connection conn = getConnection("steven", "sklar")) {
                    Assert.assertFalse(conn.isClosed());
                }
            }
        });
    }

    @Test
    public void testRemovalOfUnsupportedPropertyWontTriggerReload() throws Exception {
        assertMemoryLeak(() -> {
            try (FileWriter w = new FileWriter(serverConf)) {
                w.write("pg.user=steven\n");
                w.write("pg.password=sklar\n");
                w.write("cairo.legacy.string.column.type.default=true\n"); // non-default value
            }

            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                Assert.assertTrue(serverMain.getConfiguration().getLineTcpReceiverConfiguration().isUseLegacyStringDefault());

                // remove unsupported property
                try (FileWriter w = new FileWriter(serverConf, false)) {
                    w.write("pg.user=steven\n");
                    w.write("pg.password=sklar\n");
                }

                Assert.assertFalse(latch.await(TimeUnit.SECONDS.toNanos(1)));
            }
        });
    }

    @Test
    public void testRemovedUnsupportedPropertyWontReturnToDefault() throws Exception {
        assertMemoryLeak(() -> {
            try (FileWriter w = new FileWriter(serverConf)) {
                w.write("pg.user=steven\n");
                w.write("pg.password=sklar\n");
                w.write("cairo.legacy.string.column.type.default=true\n"); // non-default value
            }

            try (ServerMain serverMain = new ServerMain(getBootstrap())) {
                serverMain.start();

                Assert.assertTrue(serverMain.getConfiguration().getLineTcpReceiverConfiguration().isUseLegacyStringDefault());

                // remove unsupported property and change some supported property
                try (FileWriter w = new FileWriter(serverConf, false)) {
                    w.write("pg.user=steven\n");
                    w.write("pg.password=foo\n");
                }

                latch.await();

                // unsupported property should stay as it was before reload
                Assert.assertTrue(serverMain.getConfiguration().getLineTcpReceiverConfiguration().isUseLegacyStringDefault());
            }
        });
    }

    private static Connection getConnection(String user, String pass) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", pass);
        final String url = String.format("jdbc:postgresql://127.0.0.1:%d/qdb", 8812);
        return DriverManager.getConnection(url, properties);
    }
}
