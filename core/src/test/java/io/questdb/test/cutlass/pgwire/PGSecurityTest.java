/*+*****************************************************************************
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

package io.questdb.test.cutlass.pgwire;

import io.questdb.DefaultFactoryProvider;
import io.questdb.FactoryProvider;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.security.SecurityContextFactory;
import io.questdb.cutlass.pgwire.PGConfiguration;
import io.questdb.cutlass.pgwire.PGServer;
import io.questdb.cutlass.pgwire.ReadOnlyUsersAwareSecurityContextFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Os;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.postgresql.PGProperty;
import org.postgresql.util.PSQLException;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HexFormat;
import java.util.Properties;
import java.util.TimeZone;

import static io.questdb.test.tools.TestUtils.assertContains;

public class PGSecurityTest extends BasePGTest {

    private static final PGConfiguration ENTITY_DISABLED_CONF = new Port0PGConfiguration() {
        @Override
        public FactoryProvider getFactoryProvider() {
            return new DefaultFactoryProvider() {
                @Override
                public @NotNull SecurityContextFactory getSecurityContextFactory() {
                    return (principalContext, _) -> new EntityDisabledSecurityContext(principalContext.getPrincipal());
                }
            };
        }
    };
    private static final SecurityContextFactory READ_ONLY_SECURITY_CONTEXT_FACTORY = new ReadOnlyUsersAwareSecurityContextFactory(true, null, false);
    private static final FactoryProvider READ_ONLY_FACTORY_PROVIDER = new DefaultFactoryProvider() {
        @Override
        public @NotNull SecurityContextFactory getSecurityContextFactory() {
            return READ_ONLY_SECURITY_CONTEXT_FACTORY;
        }
    };
    private static final PGConfiguration READ_ONLY_CONF = new Port0PGConfiguration() {
        @Override
        public FactoryProvider getFactoryProvider() {
            return READ_ONLY_FACTORY_PROVIDER;
        }
    };
    private static final SecurityContextFactory READ_ONLY_USER_SECURITY_CONTEXT_FACTORY = new ReadOnlyUsersAwareSecurityContextFactory(false, "user", false);
    private static final FactoryProvider READ_ONLY_USER_FACTORY_PROVIDER = new DefaultFactoryProvider() {
        @Override
        public @NotNull SecurityContextFactory getSecurityContextFactory() {
            return READ_ONLY_USER_SECURITY_CONTEXT_FACTORY;
        }
    };
    private static final PGConfiguration READ_ONLY_USER_CONF = new Port0PGConfiguration() {
        @Override
        public FactoryProvider getFactoryProvider() {
            return READ_ONLY_USER_FACTORY_PROVIDER;
        }

        @Override
        public boolean isReadOnlyUserEnabled() {
            return true;
        }
    };
    // EntityDisabledSecurityContext fails checkEntityEnabled() while a test sets this flag
    private static volatile boolean isEntityDisabled;

    @BeforeClass
    public static void init() {
        inputRoot = TestUtils.getCsvRoot();
    }

    @Test
    public void testAllowDumpThreadStacks() throws Exception {
        assertMemoryLeak(() -> executeWithPg("select dump_thread_stacks();"));
    }

    @Test
    public void testAllowsSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts TIMESTAMP)");
            executeWithPg("select * from src");
        });
    }

    @Test
    public void testCurrentUserReflectsConfiguredPrincipal() throws Exception {
        // current_user() must report the authenticated pgwire user, not the hardcoded "admin" default
        assertMemoryLeak(() -> {
            try (
                    final PGServer server = createPGServer(READ_ONLY_USER_CONF);
                    final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                try (
                        final Connection defaultUserConnection = getConnection(server.getPort(), false, true);
                        final Connection roUserConnection = getConnectionWithReadOnlyUser(server.getPort())
                ) {
                    // the read-only user "user" gets a read-only context that still reports its own name
                    assertCurrentUser(roUserConnection, "user");
                    // the default admin user maps to the shared singleton and reports the default name
                    assertCurrentUser(defaultUserConnection, "admin");
                }
            }
        });
    }

    @Test
    public void testDisallowAddNewColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts TIMESTAMP)");
            assertQueryDisallowed("alter table src add column newCol string");
        });
    }

    @Test
    public void testDisallowCopy() throws Exception {
        execute("create table testDisallowCopySerial (l long)");
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
            execute("create table src (ts TIMESTAMP)");
            try {
                executeWithPg("delete from src");
                assertExceptionNoLeakCheck("It appears delete are implemented. Please change this test to check DELETE are refused with the read-only context");
            } catch (PSQLException e) {
                // the parser does not support DELETE
                assertContains(e.getMessage(), "unexpected token [from]");
            }
        });
    }

    @Test
    public void testDisallowDrop() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts TIMESTAMP)");
            assertQueryDisallowed("drop table src");
        });
    }

    @Test
    public void testDisallowInsert() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY DAY");
            assertQueryDisallowed("insert into src values (now(), 'foo')");
        });
    }

    @Test
    public void testDisallowInsertAsSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY DAY");
            execute("insert into src values (now(), 'foo')");
            assertQueryDisallowed("insert into src select now(), name from src");
        });
    }

    @Test
    public void testDisallowSnapshotComplete() throws Exception {
        // snapshot is not supported on Windows at all
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            execute("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY day");
            execute("checkpoint create");
            try {
                assertQueryDisallowed("checkpoint release");
            } finally {
                execute("checkpoint release");
            }
        });
    }

    @Test
    public void testDisallowSnapshotPrepare() throws Exception {
        // snapshot is not supported on Windows at all
        assertMemoryLeak(() -> {
            execute("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY day");
            assertQueryDisallowed("checkpoint create");
        });
    }

    @Test
    public void testDisallowTruncate() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY day");
            execute("insert into src values (now(), 'foo')");
            assertQueryDisallowed("truncate table src");
        });
    }

    @Test
    public void testDisallowUpdate() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY DAY");
            execute("insert into src values ('2022-04-12T17:30:45.145921Z', 'foo')");

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
            assertQuery("select * from src")
                    .noLeakCheck()
                    .returnsOnce("""
                            ts\tname
                            2022-04-12T17:30:45.145921Z\tfoo
                            """);
        });
    }

    @Test
    public void testDisallowVacuum() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY day");
            assertQueryDisallowed("vacuum partitions src");
        });
    }

    @Test
    @Ignore("This is failing, but repair is nop so that's ok")
    public void testDisallowsRepairTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts TIMESTAMP, name string) timestamp(ts) PARTITION BY day");
            execute("insert into src values (now(), 'foo')");
            assertQueryDisallowed("repair table src");
        });
    }

    @Test
    public void testEntityDisabledMidSessionFlushSendsErrorOnce() throws Exception {
        assertEntityDisabledConversation((out, in) -> {
            out.write(pgMessage('H'));
            Assert.assertEquals("E[entity is disabled]", readMessageSummary(in));
            // the batch stays failed until Sync, and Sync does not repeat the error
            out.write(pgMessage('S'));
            Assert.assertEquals("Z(I)", readReplySummary(in));
            isEntityDisabled = false;
            out.write(pgQuery("SELECT 3"));
            Assert.assertEquals("T D C Z(I)", readReplySummary(in));
        });
    }

    @Test
    public void testEntityDisabledMidSessionExtendedQuerySendsErrorOnce() throws Exception {
        // P s 'SELECT 1'; S | disabled: P; B; E; S | B p s; E p; S | D S s; S | E ''; S | C S s; S
        // | enabled: B '' s; E ''; S
        // Parse, Bind, Describe, Execute and Close fail while the entity is disabled, each batch
        // answers one error and one ReadyForQuery, and nothing of a failed batch runs.
        assertEntityDisabledConversation((out, in) -> {
            isEntityDisabled = false;
            out.write(pgMessages(pgParse("s", "SELECT 1"), pgMessage('S')));
            Assert.assertEquals("1 Z(I)", readReplySummary(in));
            isEntityDisabled = true;
            out.write(pgMessages(pgParse("", "SELECT 2"), pgBind("", ""), pgExecute("", 0), pgMessage('S')));
            Assert.assertEquals("E[entity is disabled] Z(I)", readReplySummary(in));
            out.write(pgMessages(pgBind("p", "s"), pgExecute("p", 0), pgMessage('S')));
            Assert.assertEquals("E[entity is disabled] Z(I)", readReplySummary(in));
            out.write(pgMessages(pgDescribe('S', "s"), pgMessage('S')));
            Assert.assertEquals("E[entity is disabled] Z(I)", readReplySummary(in));
            out.write(pgMessages(pgExecute("", 0), pgMessage('S')));
            Assert.assertEquals("E[entity is disabled] Z(I)", readReplySummary(in));
            out.write(pgMessages(pgClose('S', "s"), pgMessage('S')));
            Assert.assertEquals("E[entity is disabled] Z(I)", readReplySummary(in));
            isEntityDisabled = false;
            // the failed Close left s in place
            out.write(pgMessages(pgBind("", "s"), pgExecute("", 0), pgMessage('S')));
            Assert.assertEquals("2 D C Z(I)", readReplySummary(in));
        });
    }

    @Test
    public void testEntityDisabledMidSessionLoneSyncReplies() throws Exception {
        assertEntityDisabledConversation((out, in) -> {
            out.write(pgMessage('S'));
            Assert.assertEquals("E[entity is disabled] Z(I)", readReplySummary(in));
            isEntityDisabled = false;
            out.write(pgQuery("SELECT 3"));
            Assert.assertEquals("T D C Z(I)", readReplySummary(in));
        });
    }

    @Test
    public void testEntityDisabledMidSessionSimpleQueryReplies() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    final PGServer server = createPGServer(ENTITY_DISABLED_CONF);
                    final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                try (final Connection connection = getConnection(Mode.SIMPLE, server.getPort(), false)) {
                    connection.setNetworkTimeout(Runnable::run, 5_000);
                    assertSelectReturns(connection, 1);
                    isEntityDisabled = true;
                    try (final Statement statement = connection.createStatement()) {
                        statement.executeQuery("SELECT 2");
                        Assert.fail("the query must fail while the entity is disabled");
                    } catch (PSQLException e) {
                        assertContains(e.getMessage(), "entity is disabled");
                    } finally {
                        isEntityDisabled = false;
                    }
                    // the connection is still in sync with the client
                    assertSelectReturns(connection, 3);
                }
            } finally {
                isEntityDisabled = false;
            }
        });
    }

    @Test
    public void testEntityDisabledMidSessionTerminateCloses() throws Exception {
        assertEntityDisabledConversation((out, in) -> {
            out.write(pgMessage('X'));
            Assert.assertEquals(-1, in.read());
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
        assertWithPgServer(CONN_AWARE_ALL, (_, _, _, port) -> getConnectionWithCustomProperty(port, PGProperty.OPTIONS.getName()).close());
    }

    @Test
    public void testReadOnlyUser() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts TIMESTAMP)");
            try (
                    final PGServer server = createPGServer(READ_ONLY_USER_CONF);
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
                        assertExceptionNoLeakCheck("Query '" + query + "' must fail for the read-only user!");
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

    @Test
    public void testSecurityContextFactoryThrowsCairoException() throws Exception {
        final PGConfiguration conf = new Port0PGConfiguration() {
            @Override
            public FactoryProvider getFactoryProvider() {
                return new DefaultFactoryProvider() {
                    @Override
                    public @NotNull SecurityContextFactory getSecurityContextFactory() {
                        return (_, _) -> {
                            throw CairoException.nonCritical().put("test security context error");
                        };
                    }
                };
            }
        };

        assertMemoryLeak(() -> {
            try (
                    final PGServer server = createPGServer(conf);
                    final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                try {
                    getConnection(server.getPort(), false, true);
                    Assert.fail("Connection should have been denied");
                } catch (PSQLException e) {
                    assertContains(e.getMessage(), "test security context error");
                }
            }
        });
    }

    private static void assertCurrentUser(Connection connection, String expectedUser) throws SQLException {
        try (
                final Statement statement = connection.createStatement();
                // current_user() and session_user() must both reflect the authenticated user
                final ResultSet rs = statement.executeQuery("SELECT current_user(), session_user()")
        ) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(expectedUser, rs.getString(1));
            Assert.assertEquals(expectedUser, rs.getString(2));
            Assert.assertFalse(rs.next());
        }
    }

    private static void assertSelectReturns(Connection connection, int value) throws SQLException {
        try (
                final Statement statement = connection.createStatement();
                final ResultSet rs = statement.executeQuery("SELECT " + value)
        ) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(value, rs.getInt(1));
            Assert.assertFalse(rs.next());
        }
    }

    private static byte[] pgMessage(char type) {
        return new byte[]{(byte) type, 0, 0, 0, 4};
    }

    private static byte[] pgQuery(String sql) {
        final byte[] text = sql.getBytes(StandardCharsets.UTF_8);
        final int length = Integer.BYTES + text.length + 1;
        final byte[] message = new byte[1 + length];
        message[0] = 'Q';
        message[1] = (byte) (length >>> 24);
        message[2] = (byte) (length >>> 16);
        message[3] = (byte) (length >>> 8);
        message[4] = (byte) length;
        System.arraycopy(text, 0, message, 5, text.length);
        return message;
    }

    // Reads one server message and names it: the message type, the message of an
    // ErrorResponse and the transaction status of ReadyForQuery, e.g. "E[...]" or "Z(I)".
    private static String readMessageSummary(DataInputStream in) throws IOException {
        final int type = in.readUnsignedByte();
        final byte[] body = in.readNBytes(in.readInt() - Integer.BYTES);
        final StringBuilder summary = new StringBuilder().append((char) type);
        switch (type) {
            case 'E' -> {
                summary.append('[');
                for (int i = 0; body[i] != 0; ) {
                    int end = i + 1;
                    while (body[end] != 0) {
                        end++;
                    }
                    if (body[i] == 'M') {
                        summary.append(new String(body, i + 1, end - i - 1, StandardCharsets.UTF_8));
                    }
                    i = end + 1;
                }
                summary.append(']');
            }
            case 'Z' -> summary.append('(').append((char) body[0]).append(')');
            default -> {
            }
        }
        return summary.toString();
    }

    // Reads server messages up to and including ReadyForQuery, see readMessageSummary()
    private static String readReplySummary(DataInputStream in) throws IOException {
        final StringBuilder summary = new StringBuilder();
        String message;
        do {
            message = readMessageSummary(in);
            if (!summary.isEmpty()) {
                summary.append(' ');
            }
            summary.append(message);
        } while (message.charAt(0) != 'Z');
        return summary.toString();
    }

    // Logs in to a server whose security context fails checkEntityEnabled() on demand,
    // disables the entity and runs the conversation
    private void assertEntityDisabledConversation(EntityDisabledConversation conversation) throws Exception {
        assertMemoryLeak(() -> {
            try (
                    final PGServer server = createPGServer(ENTITY_DISABLED_CONF);
                    final WorkerPool workerPool = server.getWorkerPool();
                    final Socket socket = new Socket("127.0.0.1", server.getPort())
            ) {
                workerPool.start(LOG);
                socket.setSoTimeout(5_000);
                final OutputStream out = socket.getOutputStream();
                final DataInputStream in = new DataInputStream(socket.getInputStream());
                // StartupMessage for admin, then the password quest
                out.write(HexFormat.of().parseHex("0000003600030000757365720061646d696e0064617461626173650071646200636c69656e745f656e636f64696e6700555446380000"));
                Assert.assertEquals("R", readMessageSummary(in));
                out.write(HexFormat.of().parseHex("700000000a717565737400"));
                assertContains(readReplySummary(in), "Z(I)");
                isEntityDisabled = true;
                conversation.run(out, in);
            } finally {
                isEntityDisabled = false;
            }
        });
    }

    private void assertQueryDisallowed(String query) throws Exception {
        try {
            executeWithPg(query);
            Assert.fail("Query '" + query + "' must fail in the read-only mode!");
        } catch (PSQLException e) {
            assertContains(e.getMessage(), "permission denied");
        }
    }

    private void executeWithPg(String query) throws Exception {
        try (
                final PGServer server = createPGServer(READ_ONLY_CONF);
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

    @FunctionalInterface
    private interface EntityDisabledConversation {
        void run(OutputStream out, DataInputStream in) throws Exception;
    }

    private static class EntityDisabledSecurityContext extends AllowAllSecurityContext {
        EntityDisabledSecurityContext(CharSequence principal) {
            super(false, principal);
        }

        @Override
        public void checkEntityEnabled() {
            if (isEntityDisabled) {
                throw CairoException.nonCritical().put("entity is disabled");
            }
        }

        @Override
        protected SecurityContext newPrincipalContext(CharSequence principal) {
            return new EntityDisabledSecurityContext(principal);
        }
    }
}
