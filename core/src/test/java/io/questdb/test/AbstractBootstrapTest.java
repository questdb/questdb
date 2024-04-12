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

import io.questdb.Bootstrap;
import io.questdb.PropBootstrapConfiguration;
import io.questdb.PropServerConfiguration;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Files;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.postgresql.util.PSQLException;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static io.questdb.PropertyKey.*;

public abstract class AbstractBootstrapTest extends AbstractTest {
    protected static final String CHARSET = "UTF8";
    protected static final int HTTP_MIN_PORT = 9011;
    protected static final int HTTP_PORT = 9010;
    protected static final int ILP_BUFFER_SIZE = 4 * 1024;
    protected static final int ILP_PORT = 9009;
    protected static final Properties PG_CONNECTION_PROPERTIES = new Properties();
    protected static final int PG_PORT = 8822;
    protected static final String PG_CONNECTION_URI = getPgConnectionUri(PG_PORT);
    protected static int ILP_WORKER_COUNT = 1;
    protected static Path auxPath;
    protected static Path dbPath;
    protected static int dbPathLen;
    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(20 * 60 * 1000, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractTest.setUpStatic();
        TestUtils.unchecked(() -> {
            dbPath = new Path().of(root).concat(PropServerConfiguration.DB_DIRECTORY).$();
            dbPathLen = dbPath.size();
            auxPath = new Path();
            dbPath.trimTo(dbPathLen).$();
        });
    }

    public static TestServerMain startWithEnvVariables(String... envs) {
        assert envs.length % 2 == 0;

        Map<String, String> envMap = new HashMap<>();
        for (int i = 0; i < envs.length; i += 2) {
            envMap.put(envs[i], envs[i + 1]);
        }
        TestServerMain serverMain = new TestServerMain(newBootstrapWithEnvVariables(envMap));
        serverMain.start();
        return serverMain;
    }

    @AfterClass
    public static void tearDownStatic() {
        dbPath = Misc.free(dbPath);
        auxPath = Misc.free(auxPath);
        AbstractTest.tearDownStatic();
    }

    @NotNull
    private static Bootstrap newBootstrapWithEnvVariables(Map<String, String> envs) {
        Map<String, String> env = new HashMap<>(System.getenv());

        env.putAll(envs);
        return new Bootstrap(
                new PropBootstrapConfiguration() {
                    @Override
                    public Map<String, String> getEnv() {
                        return env;
                    }
                },
                getServerMainArgs()
        );
    }

    protected static void assertQueryFails(
            String username,
            String password,
            int port,
            String queryText,
            String exceptionMessageMustContain,
            String assertionFailureMessage
    ) throws SQLException {
        try (Connection conn = getConnection(username, password, port)) {
            conn.createStatement().execute(queryText);
            Assert.fail(assertionFailureMessage);
        } catch (PSQLException e) {
            TestUtils.assertContains(e.getMessage(), exceptionMessageMustContain);
        }
    }

    protected static void assertQuerySucceeds(
            String username,
            String password,
            int port,
            String queryText,
            String assertionFailureMessage
    ) throws SQLException {
        try (Connection conn = getConnection(username, password, port)) {
            conn.createStatement().execute(queryText);
        } catch (PSQLException e) {
            throw new AssertionError(assertionFailureMessage, e);
        }
    }

    protected static void createDummyConfiguration(String... extra) throws Exception {
        createDummyConfigurationInRoot(root, extra);
    }

    protected static void createDummyConfiguration(
            int httpPort,
            int httpMinPort,
            int pgPort,
            int ilpPort,
            String root,
            String... extra
    ) throws Exception {
        final String confPath = root + Files.SEPARATOR + "conf";
        TestUtils.createTestPath(confPath);
        String file = confPath + Files.SEPARATOR + "server.conf";
        try (PrintWriter writer = new PrintWriter(file, CHARSET)) {
            // enable all services, but UDP; it has to be enabled per test
            writer.println(HTTP_ENABLED + "=true");
            writer.println(HTTP_MIN_ENABLED + "=true");
            writer.println(PG_ENABLED + "=true");
            writer.println(LINE_TCP_ENABLED + "=true");

            // disable services
            writer.println(HTTP_QUERY_CACHE_ENABLED + "=false");
            writer.println(PG_SELECT_CACHE_ENABLED + "=false");
            writer.println(PG_INSERT_CACHE_ENABLED + "=false");
            writer.println(PG_UPDATE_CACHE_ENABLED + "=false");
            writer.println(CAIRO_WAL_ENABLED_DEFAULT + "=false");
            writer.println(METRICS_ENABLED + "=false");
            writer.println(TELEMETRY_ENABLED + "=false");
            writer.println(TELEMETRY_DISABLE_COMPLETELY + "=true");

            // configure endpoints
            writer.println(HTTP_BIND_TO + "=0.0.0.0:" + httpPort);
            writer.println(HTTP_MIN_NET_BIND_TO + "=0.0.0.0:" + httpMinPort);
            writer.println(PG_NET_BIND_TO + "=0.0.0.0:" + pgPort);
            writer.println(LINE_TCP_NET_BIND_TO + "=0.0.0.0:" + ilpPort);
            writer.println(LINE_UDP_BIND_TO + "=0.0.0.0:" + ilpPort);
            writer.println(LINE_UDP_RECEIVE_BUFFER_SIZE + "=" + ILP_BUFFER_SIZE);
            writer.println(HTTP_FROZEN_CLOCK + "=true");

            // configure worker pools
            writer.println(SHARED_WORKER_COUNT + "=2");
            writer.println(HTTP_WORKER_COUNT + "=1");
            writer.println(HTTP_MIN_WORKER_COUNT + "=1");
            writer.println(PG_WORKER_COUNT + "=1");
            writer.println(LINE_TCP_WRITER_WORKER_COUNT + "=1");
            writer.println(LINE_TCP_IO_WORKER_COUNT + "=" + ILP_WORKER_COUNT);

            // extra
            if (extra != null) {
                for (String s : extra) {
                    // '\' is interpreted by java.util.Properties as an escape character
                    // and we don't want that for Windows paths.
                    writer.println(s.replace("\\", "\\\\"));
                }
            }
        }

        // mime types
        file = confPath + Files.SEPARATOR + "mime.types";
        try (PrintWriter writer = new PrintWriter(file, CHARSET)) {
            writer.println("");
        }

        // note: nice try, but this is not used!
        // at this point LogFactory has been initialized already
        // logs
        file = confPath + Files.SEPARATOR + "log.conf";
        System.setProperty("out", file);
        try (PrintWriter writer = new PrintWriter(file, CHARSET)) {
            writer.println("writers=stdout");
            writer.println("w.stdout.class=io.questdb.log.LogConsoleWriter");
            writer.println("w.stdout.level=INFO");
        }
    }

    protected static void createDummyConfigurationInRoot(String root, String... extra) throws Exception {
        createDummyConfiguration(HTTP_PORT, HTTP_MIN_PORT, PG_PORT, ILP_PORT, root, extra);
    }

    protected static void drainWalQueue(CairoEngine engine) {
        try (final ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 1, 1)) {
            walApplyJob.drain(0);
            new CheckWalTransactionsJob(engine).run(0);
            // run once again as there might be notifications to handle now
            walApplyJob.drain(0);
        }
    }

    static void dropTable(SqlCompiler compiler, SqlExecutionContext context, TableToken tableToken) throws Exception {
        compiler.compile("DROP TABLE '" + tableToken.getTableName() + '\'', context);
    }

    static String[] extendArgsWith(String[] args, String... moreArgs) {
        int argsLen = args.length;
        int extLen = moreArgs.length;
        int size = argsLen + extLen;
        if (size < 1) {
            Assert.fail("what are you trying to do?");
        }
        String[] extendedArgs = new String[size];
        System.arraycopy(args, 0, extendedArgs, 0, argsLen);
        System.arraycopy(moreArgs, 0, extendedArgs, argsLen, extLen);
        return extendedArgs;
    }

    protected static Connection getConnection(String username, String password, int port) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", username);
        properties.setProperty("password", password);
        final String url = String.format("jdbc:postgresql://127.0.0.1:%d/qdb", port);
        return DriverManager.getConnection(url, properties);
    }

    protected static String getPgConnectionUri(int pgPort) {
        return "jdbc:postgresql://127.0.0.1:" + pgPort + "/qdb";
    }

    void assertFail(String message, String... args) {
        try {
            new Bootstrap(extendArgsWith(args, Bootstrap.SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION));
            Assert.fail();
        } catch (Bootstrap.BootstrapException thr) {
            TestUtils.assertContains(thr.getMessage(), message);
        }
    }

    static {
        PG_CONNECTION_PROPERTIES.setProperty("user", "admin");
        PG_CONNECTION_PROPERTIES.setProperty("password", "quest");
        PG_CONNECTION_PROPERTIES.setProperty("sslmode", "disable");
        PG_CONNECTION_PROPERTIES.setProperty("binaryTransfer", "true");
    }
}
