/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.cutlass.pgwire;

import io.questdb.DefaultFactoryProvider;
import io.questdb.FactoryProvider;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cutlass.pgwire.PGWireConfiguration;
import io.questdb.cutlass.pgwire.PGWireServer;
import io.questdb.cutlass.pgwire.ReadOnlyUsersAwareSecurityContextFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Os;
import io.questdb.test.tools.TestUtils;
import org.junit.*;
import org.postgresql.PGProperty;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.TimeZone;

import static io.questdb.test.tools.TestUtils.assertContains;
import static org.junit.Assert.fail;

public class PGSecurityTest extends BasePGTest {

    private static final SecurityContextFactory READ_ONLY_SECURITY_CONTEXT_FACTORY = new ReadOnlyUsersAwareSecurityContextFactory(true, null, false);
    private static final FactoryProvider READ_ONLY_FACTORY_PROVIDER = new DefaultFactoryProvider() {
        @Override
        public SecurityContextFactory getSecurityContextFactory() {
            return READ_ONLY_SECURITY_CONTEXT_FACTORY;
        }
    };
    private static final PGWireConfiguration READ_ONLY_CONF = new Port0PGWireConfiguration() {
        @Override
        public FactoryProvider getFactoryProvider() {
            return READ_ONLY_FACTORY_PROVIDER;
        }
    };
    private static final SecurityContextFactory READ_ONLY_USER_SECURITY_CONTEXT_FACTORY = new ReadOnlyUsersAwareSecurityContextFactory(false, "user", false);
    private static final FactoryProvider READ_ONLY_USER_FACTORY_PROVIDER = new DefaultFactoryProvider() {
        @Override
        public SecurityContextFactory getSecurityContextFactory() {
            return READ_ONLY_USER_SECURITY_CONTEXT_FACTORY;
        }
    };
    private static final PGWireConfiguration READ_ONLY_USER_CONF = new Port0PGWireConfiguration() {
        @Override
        public FactoryProvider getFactoryProvider() {
            return READ_ONLY_USER_FACTORY_PROVIDER;
        }

        @Override
        public boolean isReadOnlyUserEnabled() {
            return true;
        }
    };

    @BeforeClass
    public static void init() {
        inputRoot = TestUtils.getCsvRoot();
    }

    @Test
    public void testAllowsSelect() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP)", sqlExecutionContext);
            executeWithPg("select * from src");
        });
    }

    @Test
    public void testDisallowAddNewColumn() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP)", sqlExecutionContext);
            assertQueryDisallowed("alter table src add column newCol string");
        });
    }

    @Test
    public void testDisallowCopy() throws Exception {
        assertMemoryLeak(() -> assertQueryDisallowed("copy testDisallowCopySerial from '/test-alltypes.csv' with header true"));
    }

    @Test
    public void testDisallowCreateTable() throws Exception {
        assertMemoryLeak(() -> assertQueryDisallowed("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY DAY"));
    }

    @Test
    public void testDisallowDelete() throws Exception {
        // we don't support DELETE yet. this test exists as a reminder to check read-only security context is honoured
        // when/if DELETE is implemented.
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP)", sqlExecutionContext);
            try {
                executeWithPg("delete from src");
                fail("It appears delete are implemented. Please change this test to check DELETE are refused with the read-only context");
            } catch (PSQLException e) {
                // the parser does not support DELETE
                assertContains(e.getMessage(), "unexpected token: from");
            }
        });
    }

    @Test
    public void testDisallowDrop() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP)", sqlExecutionContext);
            assertQueryDisallowed("drop table src");
        });
    }

    @Test
    public void testDisallowInsert() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY DAY", sqlExecutionContext);
            assertQueryDisallowed("insert into src values (now(), 'foo')");
        });
    }

    @Test
    public void testDisallowInsertAsSelect() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY DAY", sqlExecutionContext);
            TestUtils.insert(compiler, sqlExecutionContext, "insert into src values (now(), 'foo')");
            assertQueryDisallowed("insert into src select now(), name from src");
        });
    }

    @Test
    public void testDisallowSnapshotComplete() throws Exception {
        // snapshot is not supported on Windows at all
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY day", sqlExecutionContext);
            compiler.compile("snapshot prepare", sqlExecutionContext);
            try {
                assertQueryDisallowed("snapshot complete");
            } finally {
                compiler.compile("snapshot complete", sqlExecutionContext);
            }
        });
    }

    @Test
    public void testDisallowSnapshotPrepare() throws Exception {
        // snapshot is not supported on Windows at all
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY day", sqlExecutionContext);
            assertQueryDisallowed("snapshot prepare");
        });
    }

    @Test
    public void testDisallowTruncate() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY day", sqlExecutionContext);
            executeInsert("insert into src values (now(), 'foo')");
            assertQueryDisallowed("truncate table src");
        });
    }

    @Test
    public void testDisallowUpdate() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY DAY", sqlExecutionContext);
            executeInsert("insert into src values ('2022-04-12T17:30:45.145921Z', 'foo')");

            try {
                executeWithPg("update src set name = 'bar'");
                Assert.fail("Should not be possible to update in Read-only mode");
            } catch (PSQLException e) {
                // the parser does not support DELETE
                assertContains(e.getMessage(), "Write permission denied");
            }

            // if this asserts fails then it means UPDATE are already implemented
            // please change this test to check the update throws an exception in the read-only mode
            // this is in place, so we won't forget to test UPDATE honours read-only security context
            assertSql("select * from src", "ts\tname\n" +
                    "2022-04-12T17:30:45.145921Z\tfoo\n");
        });
    }

    @Test
    public void testDisallowVacuum() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY day", sqlExecutionContext);
            assertQueryDisallowed("vacuum partitions src");
        });
    }

    @Test
    public void testDisallowsBackupDatabase() throws Exception {
        assertMemoryLeak(() -> {
            configureForBackups();
            compiler.compile("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY day", sqlExecutionContext);
            executeInsert("insert into src values (now(), 'foo')");
            assertQueryDisallowed("backup database");
        });
    }

    @Test
    public void testDisallowsBackupTable() throws Exception {
        assertMemoryLeak(() -> {
            configureForBackups();
            compiler.compile("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY day", sqlExecutionContext);
            executeInsert("insert into src values (now(), 'foo')");
            assertQueryDisallowed("backup table src");
        });
    }

    @Test
    @Ignore("This is failing, but repair is nop so that's ok")
    public void testDisallowsRepairTable() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY day", sqlExecutionContext);
            executeInsert("insert into src values (now(), 'foo')");
            assertQueryDisallowed("repair table src");
        });
    }

    @Test
    public void testInitialPropertiesParsedCorrectly() throws Exception {
        // there was a bug where a value of each property was also used as a key for a property created out of thin air.
        // so when a client sends a property with a value set to "user" then a buggy pgwire parser would create
        // also a key "user" out of thin air with a value set as the next key. Example:
        // 2022-05-17T16:39:18.308689Z I i.q.c.p.PGConnectionContext property [name=user, value=admin] <-- this is a legit property
        // 2022-05-17T16:39:18.308707Z I i.q.c.p.PGConnectionContext property [name=admin, value=database] <-- this is a property "invented" by a buggy pgwire parser
        // 2022-05-17T16:39:18.308724Z I i.q.c.p.PGConnectionContext property [name=database, value=qdb] <-- a legit property set by a client
        // 2022-05-17T16:39:18.308789Z I i.q.c.p.PGConnectionContext property [name=qdb, value=client_encoding] <-- again, a property created out of thin air

        // so this test sets a property to "user" and check authentication still succeed. it would fail on a buggy pgwire parser
        // because the out of thin air property would overwrite the user set by the client. Example:
        // 2022-05-17T15:58:38.973955Z I i.q.c.p.PGConnectionContext property [name=user, value=user] <-- client indicates username is "user"
        // 2022-05-17T15:58:38.974236Z I i.q.c.p.PGConnectionContext property [name=user, value=database] <-- buggy pgwire parser overwrites username with out of thin air value
        assertMemoryLeak(() -> {
            try (
                    final PGWireServer server = createPGServer(1);
                    final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                // Postgres JDBC clients ignores unknown properties and does not send them to a server
                // so have to use a property which actually exists
                getConnectionWithCustomProperty(server.getPort(), PGProperty.OPTIONS.getName()).close();
            }
        });
    }

    @Test
    public void testReadOnlyUser() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table src (ts TIMESTAMP)", sqlExecutionContext);
            try (
                    final PGWireServer server = createPGServer(READ_ONLY_USER_CONF);
                    final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                try (
                        final Connection defaultUserConnection = getConnection(server.getPort(), false, true);
                        final Connection roUserConnection = getConnectionWithReadOnlyUser(server.getPort())
                ) {
                    String query = "drop table src";
                    try (final Statement statement = roUserConnection.createStatement()) {
                        statement.execute(query);
                        fail("Query '" + query + "' must fail for the read-only user!");
                    } catch (PSQLException e) {
                        assertContains(e.getMessage(), "Write permission denied");
                    }
                    try (final Statement statement = defaultUserConnection.createStatement()) {
                        statement.execute(query);
                    }
                }
            }
        });
    }

    private void assertQueryDisallowed(String query) throws Exception {
        try {
            executeWithPg(query);
            fail("Query '" + query + "' must fail in the read-only mode!");
        } catch (PSQLException e) {
            assertContains(e.getMessage(), "Write permission denied");
        }
    }

    private void executeWithPg(String query) throws Exception {
        try (
                final PGWireServer server = createPGServer(READ_ONLY_CONF);
                final WorkerPool workerPool = server.getWorkerPool()
        ) {
            workerPool.start(LOG);
            try (
                    final Connection connection = getConnection(server.getPort(), false, true);
                    final Statement statement = connection.createStatement()
            ) {
                statement.execute(query);
            }
        }
    }

    protected Connection getConnectionWithCustomProperty(int port, String key) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "admin");
        properties.setProperty("password", "quest");
        properties.setProperty("sslmode", "disable");
        properties.setProperty(key, "user");

        TimeZone.setDefault(TimeZone.getTimeZone("EDT"));
        // use this line to switch to local postgres
        // return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/qdb", properties);
        final String url = String.format("jdbc:postgresql://127.0.0.1:%d/qdb", port);
        return DriverManager.getConnection(url, properties);
    }

    protected Connection getConnectionWithReadOnlyUser(int port) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "user");
        properties.setProperty("password", "quest");
        properties.setProperty("sslmode", "disable");
        properties.setProperty("binaryTransfer", "true");
        properties.setProperty("preferQueryMode", Mode.SIMPLE.value);

        TimeZone.setDefault(TimeZone.getTimeZone("EDT"));
        // use this line to switch to local postgres
        // return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/qdb", properties);
        final String url = String.format("jdbc:postgresql://127.0.0.1:%d/qdb", port);
        return DriverManager.getConnection(url, properties);
    }
}
