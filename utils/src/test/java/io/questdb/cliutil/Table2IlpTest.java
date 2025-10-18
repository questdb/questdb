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

package io.questdb.cliutil;

import io.questdb.DefaultServerConfiguration;
import io.questdb.ServerConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cutlass.http.DefaultHttpServerConfiguration;
import io.questdb.cutlass.http.HttpCookieHandler;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.cutlass.http.HttpHeaderParserFactory;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.http.HttpSessionStore;
import io.questdb.cutlass.http.processors.JsonQueryProcessor;
import io.questdb.cutlass.http.processors.LineHttpProcessorImpl;
import io.questdb.cutlass.line.tcp.DefaultLineTcpReceiverConfiguration;
import io.questdb.cutlass.line.tcp.LineTcpReceiver;
import io.questdb.cutlass.pgwire.DefaultPGCircuitBreakerRegistry;
import io.questdb.cutlass.pgwire.DefaultPGConfiguration;
import io.questdb.cutlass.pgwire.PGConfiguration;
import io.questdb.cutlass.pgwire.PGServer;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Table2IlpTest {
    private static final int HTTP_PORT = 9900;
    private static final int ILP_PORT = 9909;
    private static final Log LOG = LogFactory.getLog(Table2IlpTest.class);
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();
    protected static CharSequence root;
    private static DefaultCairoConfiguration configuration;
    private static CairoEngine engine;
    private static HttpServer httpServer;
    private static PGServer pgServer;
    private static LineTcpReceiver receiver;
    private static SqlExecutionContextImpl sqlExecutionContext;
    private static WorkerPool workerPool;

    public static void assertEventually(Runnable assertion, int timeoutSeconds) {
        long maxSleepingTimeMillis = 1000;
        long nextSleepingTimeMillis = 10;
        long startTime = System.nanoTime();
        long deadline = startTime + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        for (; ; ) {
            try {
                assertion.run();
                return;
            } catch (AssertionError error) {
                if (System.nanoTime() >= deadline) {
                    throw error;
                }
            }
            Os.sleep(nextSleepingTimeMillis);
            nextSleepingTimeMillis = Math.min(maxSleepingTimeMillis, nextSleepingTimeMillis << 1);
        }
    }

    public static void createTestPath(CharSequence root) {
        try (Path path = new Path().of(root)) {
            if (Files.exists(path.$())) {
                return;
            }
            Files.mkdirs(path.of(root).slash(), 509);
        }
    }

    public static void removeTestPath(CharSequence root) {
        Path path = Path.getThreadLocal(root);
        Files.rmdir(path.slash(), true);
    }

    public static void setCairoStatic() {
        // it is necessary to initialise logger before tests start
        // logger doesn't relinquish memory until JVM stops
        // which causes memory leak detector to fail should logger be
        // created mid-test
        try {
            root = temp.newFolder("dbRoot").getAbsolutePath();
        } catch (IOException e) {
            throw new ExceptionInInitializerError();
        }
        configuration = new DefaultCairoConfiguration(root);
        engine = new CairoEngine(configuration);
    }

    @BeforeClass
    public static void setUpStatic() throws SqlException {
        setCairoStatic();
        BindVariableServiceImpl bindVariableService = new BindVariableServiceImpl(configuration);
        sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
                .with(
                        engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        bindVariableService,
                        null,
                        -1,
                        null);
        bindVariableService.clear();
        final PGConfiguration conf = new DefaultPGConfiguration() {
            @Override
            public int getSendBufferSize() {
                return 512;
            }

            @Override
            public int getWorkerCount() {
                return 3;
            }
        };

        DefaultPGCircuitBreakerRegistry registry = new DefaultPGCircuitBreakerRegistry(conf, engine.getConfiguration());

        workerPool = new WorkerPool(conf);
        pgServer = new PGServer(conf, engine, workerPool, registry, () -> new SqlExecutionContextImpl(engine, 0));

        receiver = new LineTcpReceiver(new DefaultLineTcpReceiverConfiguration(configuration) {
            @Override
            public int getBindPort() {
                return ILP_PORT;
            }

            @Override
            public long getMaintenanceInterval() {
                return 25;
            }

            @Override
            public long getWriterIdleTimeout() {
                return 500;
            }
        }, engine, workerPool, workerPool);
        WorkerPoolUtils.setupWriterJobs(workerPool, engine);

        httpServer = createHttpServer(
                new DefaultServerConfiguration(root) {
                    @Override
                    public HttpFullFatServerConfiguration getHttpServerConfiguration() {
                        return new DefaultHttpServerConfiguration(configuration) {
                            @Override
                            public int getBindPort() {
                                return HTTP_PORT;
                            }
                        };
                    }
                },
                engine,
                workerPool
        );

        workerPool.start(LOG);
    }

    @AfterClass
    public static void tearDownStatic() {
        sqlExecutionContext.close();
        workerPool.halt();
        receiver.close();
        pgServer.close();
        engine.close();
        httpServer.close();
    }

    @Test
    public void copyAllColumnTypes() throws SqlException, InterruptedException {
        String tableNameSrc = "src";
        createTable(tableNameSrc, 40_000, false);

        String tableNameDst = "dst";
        createTable(tableNameDst, 1, false);
        engine.execute("truncate table " + tableNameDst, sqlExecutionContext);

        addColumn(tableNameSrc, tableNameDst, "nullint", "int");
        addColumn(tableNameSrc, tableNameDst, "nulllong", "long");
        addColumn(tableNameSrc, tableNameDst, "nullts", "timestamp");
        addColumn(tableNameSrc, tableNameDst, "nullstr", "string");
        addColumn(tableNameSrc, tableNameDst, "nulluuid", "uuid");

        Table2Ilp.Table2IlpParams params = Table2Ilp.Table2IlpParams.parse(
                new String[]{
                        "-s", tableNameSrc,
                        "-d", tableNameDst,
                        "-sc", "jdbc:postgresql://localhost:8812/qdb?ssl=false&user=admin&password=quest",
                        "-dc", "localhost:" + ILP_PORT,
                        "-sym", "sym_col,sym_col_2",
                        "-sts", "ts",
                }
        );
        CountDownLatch done = setUpWaitTableWriterRelease(tableNameDst);

        new Table2IlpCopier().copyTable(params);
        done.await();

        assertEventually(() -> {
            try {
                TestUtils.assertEquals(engine, sqlExecutionContext, tableNameSrc, tableNameDst);
            } catch (SqlException e) {
                throw new RuntimeException(e);
            }
        }, 60);
    }

    @Test
    public void copyAllColumnTypesHttp() throws SqlException {
        String tableNameSrc = "src";
        createTable(tableNameSrc, 40_000, false);

        String tableNameDst = "dst";
        createTable(tableNameDst, 1, true);
        engine.execute("truncate table " + tableNameDst, sqlExecutionContext);

        addColumn(tableNameSrc, tableNameDst, "nullint", "int");
        addColumn(tableNameSrc, tableNameDst, "nulllong", "long");
        addColumn(tableNameSrc, tableNameDst, "nullts", "timestamp");
        addColumn(tableNameSrc, tableNameDst, "nullstr", "string");
        addColumn(tableNameSrc, tableNameDst, "nulluuid", "uuid");

        Table2Ilp.Table2IlpParams params = Table2Ilp.Table2IlpParams.parse(
                new String[]{
                        "-s", tableNameSrc,
                        "-d", tableNameDst,
                        "-sc", "jdbc:postgresql://localhost:8812/qdb?ssl=false&user=admin&password=quest",
                        "-dilp", "http::addr=localhost:" + HTTP_PORT + ";auto_flush_rows=1000;",
                        "-sym", "sym_col,sym_col_2",
                        "-sts", "ts",
                }
        );
        new Table2IlpCopier().copyTable(params);

        ApplyWal2TableJob job = new ApplyWal2TableJob(engine, 0);
        job.run(0);
        TestUtils.assertEquals(engine, sqlExecutionContext, tableNameSrc, tableNameDst);
    }

    @Test
    public void copyWithOffset() throws SqlException, InterruptedException {
        String tableNameSrc = "src";
        createTable(tableNameSrc, 20000, false);

        String tableNameDst = "dst";
        createTable(tableNameDst, 1, false);
        engine.execute("truncate table " + tableNameDst, sqlExecutionContext);

        addColumn(tableNameSrc, tableNameDst, "nullint", "int");
        addColumn(tableNameSrc, tableNameDst, "nulllong", "long");

        String sourceQuery = tableNameSrc + " LIMIT 189, 10568";
        Table2Ilp.Table2IlpParams params = Table2Ilp.Table2IlpParams.parse(
                new String[]{
                        "-s", sourceQuery,
                        "-d", tableNameDst,
                        "-sc", "jdbc:postgresql://localhost:8812/qdb",
                        "-dc", "localhost:" + ILP_PORT,
                        "-sym", "sym_col,sym_col_2",
                        "-sts", "ts",
                }
        );
        CountDownLatch done = setUpWaitTableWriterRelease(tableNameDst);

        long rowsSent = new Table2IlpCopier().copyTable(params);
        Assert.assertEquals(10568 - 189, rowsSent);
        done.await();

        assertEventually(() -> {
            try {
                TestUtils.assertEquals(engine, sqlExecutionContext, sourceQuery, tableNameDst);
            } catch (SqlException e) {
                throw new RuntimeException(e);
            }
        }, 60);
    }

    @Before
    public void setUp() {
        createTestPath(root);
        engine.getTableIdGenerator().open();
        engine.getTableIdGenerator().reset();
        engine.reloadTableNames();
    }

    @After
    public void tearDown() {
        engine.getTableIdGenerator().close();
        engine.clear();
        removeTestPath(root);
    }

    @Test
    public void testCommandInvalidTimestampColumn() {
        Table2Ilp.Table2IlpParams params = Table2Ilp.Table2IlpParams.parse(
                new String[]{
                        "-s", "a",
                        "-d", "b",
                        "-sc", "jdbc:postgresql://localhost:8812/qdb?ssl=false&user=admin&password=quest",
                        "-dc", " localhost : " + ILP_PORT,
                        "-sts", "-ts",
                        "-dtls"
                }
        );

        Assert.assertFalse(params.isValid());
    }

    @Test
    public void testCommandLineHttp() {
        Table2Ilp.Table2IlpParams params = Table2Ilp.Table2IlpParams.parse(
                new String[]{
                        "-s", "a",
                        "-d", "b",
                        "-sc", "jdbc:postgresql://localhost:8812/qdb?ssl=false&user=admin&password=quest",
                        "-dilp", " http::addr=localhost:" + HTTP_PORT + ";auto_flush_rows=1000;",
                        "-sts", "ts",
                        "-dtls"
                }
        );

        Assert.assertTrue(params.isValid());
        Assert.assertTrue(params.enableDestinationTls());
        Assert.assertEquals("http::addr=localhost:" + HTTP_PORT + ";auto_flush_rows=1000;", params.getDestinationIlpConnection());
    }

    @Test
    public void testCommandLineIlpAuthToken() {
        Table2Ilp.Table2IlpParams params = Table2Ilp.Table2IlpParams.parse(
                new String[]{
                        "-s", "a",
                        "-d", "b",
                        "-sc", "jdbc:postgresql://localhost:8812/qdb?ssl=false&user=admin&password=quest",
                        "-dc", "localhost:" + ILP_PORT,
                        "-sts", "ts",
                        "-dtls",
                        "-dauth", "name:token"
                }
        );

        Assert.assertTrue(params.isValid());
        Assert.assertEquals("name", params.getDestinationAuthKey());
        Assert.assertEquals("token", params.getDestinationAuthToken());
    }

    @Test
    public void testCommandLineIlpTls() {
        Table2Ilp.Table2IlpParams params = Table2Ilp.Table2IlpParams.parse(
                new String[]{
                        "-s", "a",
                        "-d", "b",
                        "-sc", "jdbc:postgresql://localhost:8812/qdb?ssl=false&user=admin&password=quest",
                        "-dc", " localhost : " + ILP_PORT,
                        "-sts", "ts",
                        "-dtls"
                }
        );

        Assert.assertTrue(params.isValid());
        Assert.assertTrue(params.enableDestinationTls());
        Assert.assertEquals("localhost", params.getDestinationIlpHost());
        Assert.assertEquals(ILP_PORT, params.getDestinationIlpPort());
    }

    @Test
    public void testCommandLineInvalidNoDestination() {
        Table2Ilp.Table2IlpParams params = Table2Ilp.Table2IlpParams.parse(
                new String[]{
                        "-s", "a",
                        "-sc", "jdbc:postgresql://localhost:8812/qdb?ssl=false&user=admin&password=quest",
                        "-dc", "localhost:" + ILP_PORT,
                        "-sts", "ts"
                }
        );

        Assert.assertFalse(params.isValid());
    }

    @Test
    public void testCommandLineInvalidNoDestinationConnection() {
        Table2Ilp.Table2IlpParams params = Table2Ilp.Table2IlpParams.parse(
                new String[]{
                        "-s", "a",
                        "-d", "b",
                        "-sc", "jdbc:postgresql://localhost:8812/qdb?ssl=false&user=admin&password=quest",
                        "-sts", "ts"
                }
        );

        Assert.assertFalse(params.isValid());
    }

    @Test
    public void testCommandLineInvalidNoSource() {
        Table2Ilp.Table2IlpParams params = Table2Ilp.Table2IlpParams.parse(
                new String[]{
                        "-d", "b",
                        "-sc", "jdbc:postgresql://localhost:8812/qdb?ssl=false&user=admin&password=quest",
                        "-dc", "localhost:" + ILP_PORT,
                }
        );

        Assert.assertFalse(params.isValid());
    }

    @Test
    public void testCommandLineInvalidNoSourceConnection() {
        Table2Ilp.Table2IlpParams params = Table2Ilp.Table2IlpParams.parse(
                new String[]{
                        "-s", "a",
                        "-d", "b",
                        "-dc", "localhost:" + ILP_PORT,
                        "-sts", "ts"
                }
        );

        Assert.assertFalse(params.isValid());
    }

    @Test
    public void testCommandLineNoSymbols() {
        Table2Ilp.Table2IlpParams params = Table2Ilp.Table2IlpParams.parse(
                new String[]{
                        "-s", "a",
                        "-d", "b",
                        "-sc", "jdbc:postgresql://localhost:8812/qdb?ssl=false&user=admin&password=quest",
                        "-dc", "localhost:" + ILP_PORT,
                }
        );

        Assert.assertTrue(params.isValid());
        Assert.assertNotNull(params.getSymbols());
        Assert.assertEquals(0, params.getSymbols().length);
    }

    private static void addColumn(String tableNameSrc, String tableNameDst, String name, String type) throws SqlException {
        engine.execute("alter table " + tableNameSrc + " add column " + name + " " + type, sqlExecutionContext);
        engine.execute("alter table " + tableNameDst + " add column " + name + " " + type, sqlExecutionContext);
    }

    private static HttpServer createHttpServer(
            ServerConfiguration serverConfiguration,
            CairoEngine cairoEngine,
            WorkerPool workerPool
    ) {
        final HttpFullFatServerConfiguration httpServerConfiguration = serverConfiguration.getHttpServerConfiguration();
        if (!httpServerConfiguration.isEnabled()) {
            return null;
        }

        final HttpSessionStore sessionStore = serverConfiguration.getFactoryProvider().getHttpSessionStore();
        final HttpCookieHandler cookieHandler = serverConfiguration.getFactoryProvider().getHttpCookieHandler();
        final HttpHeaderParserFactory headerParserFactory = serverConfiguration.getFactoryProvider().getHttpHeaderParserFactory();
        final HttpServer server = new HttpServer(
                httpServerConfiguration,
                workerPool,
                serverConfiguration.getFactoryProvider().getHttpSocketFactory(),
                cookieHandler,
                sessionStore,
                headerParserFactory
        );
        HttpServer.HttpRequestHandlerBuilder jsonQueryProcessorBuilder = () -> new JsonQueryProcessor(
                httpServerConfiguration.getJsonQueryProcessorConfiguration(),
                cairoEngine,
                1
        );

        HttpServer.HttpRequestHandlerBuilder ilpV2WriteProcessorBuilder = () -> new LineHttpProcessorImpl(
                cairoEngine,
                httpServerConfiguration
        );

        HttpServer.addDefaultEndpoints(
                server,
                serverConfiguration,
                cairoEngine,
                1,
                jsonQueryProcessorBuilder,
                ilpV2WriteProcessorBuilder
        );
        return server;
    }

    private static void createTable(String tableName, int rows, boolean isWAL) throws SqlException {
        engine.execute(
                "create table " + tableName + " as (select" +
                        " cast(x as int) kk, " +
                        " rnd_int() a," +
                        " rnd_boolean() b," +
                        " rnd_str('dasd', 'asdfasd asdfa', null, 'asdfa\" , asdfa') c," +
                        " rnd_double(2) d," +
                        " rnd_float(2) e," +
                        " rnd_short(10,1024) f," +
                        " rnd_date(to_date('2015', 'yyyy'), to_date('2016', 'yyyy'), 2) g," +
                        " rnd_symbol('a', 'b', 'c', null) sym_col," +
                        " rnd_symbol(2, 2, 3, 5) sym_col_2," +
                        " rnd_long() j," +
                        " timestamp_sequence(0, 1000000000) k," +
                        " rnd_byte(2,50) l," +
                        " timestamp_sequence('2022-02-24T04', 500000) ts," +
                        " rnd_geohash(5) geo8," +
                        // " rnd_geohash(11) geo16," + -- non char geo hashes cannot be sent in ILP, SELECT can convert them to string as a workaround
                        " rnd_str(5,16,2) n" +
                        " from long_sequence(" + rows + ")) timestamp(ts) partition by DAY " + (isWAL ? "WAL" : "BYPASS WAL"),
                sqlExecutionContext
        );
    }

    private static CountDownLatch setUpWaitTableWriterRelease(String tableNameDst) {
        CountDownLatch done = new CountDownLatch(1);
        engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
            if (PoolListener.isWalOrWriter(factoryType) && event == PoolListener.EV_RETURN) {
                if (Chars.equals(tableNameDst, name.getTableName())) {
                    done.countDown();
                }
            }
        });
        return done;
    }
}
