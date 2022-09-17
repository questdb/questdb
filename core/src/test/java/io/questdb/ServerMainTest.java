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

import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;


public class ServerMainTest extends AbstractBootstrapTest {

    @Test
    public void testServerMain() throws Exception {
        BootstrapTest.createDummyConfiguration();
        try (ServerMain serverMain = new ServerMain("-d", root.toString())) {
            Assert.assertNotNull(serverMain.getConfiguration());
            Assert.assertNotNull(serverMain.getWorkerPoolManager());
            Assert.assertFalse(serverMain.hasStarted());
            serverMain.start();
            Assert.assertTrue(serverMain.hasStarted());

            Properties properties = new Properties();
            properties.setProperty("user", "admin");
            properties.setProperty("password", "quest");
            properties.setProperty("sslmode", "disable");
            properties.setProperty("binaryTransfer", "true");
            final String url = "jdbc:postgresql://127.0.0.1:8822/qdb";
            final SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(serverMain.getCairoEngine(), 1)
                    .with(
                            AllowAllCairoSecurityContext.INSTANCE,
                            null,
                            null,
                            -1,
                            null);

            try (Connection connection = DriverManager.getConnection(url, properties)) {
                connection.prepareStatement("create table xyz(a int)").execute();
                try (TableWriter ignored = serverMain.getCairoEngine().getWriter(
                        sqlExecutionContext.getCairoSecurityContext(), "xyz", "testing"
                )) {
                    connection.prepareStatement("drop table xyz").execute();
                    Assert.fail();
                } catch (SQLException e) {
                    TestUtils.assertContains(e.getMessage(), "Could not lock 'xyz'");
                    Assert.assertEquals("00000", e.getSQLState());
                }
            }
        }
    }
}
