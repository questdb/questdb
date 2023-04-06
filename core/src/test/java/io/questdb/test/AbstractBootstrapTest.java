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

package io.questdb.test;

import io.questdb.Bootstrap;
import io.questdb.PropServerConfiguration;
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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.PrintWriter;
import java.util.Properties;

public abstract class AbstractBootstrapTest extends AbstractTest {
    protected static final String CHARSET = "UTF8";
    protected static final int HTTP_MIN_PORT = 9011;
    protected static final int HTTP_PORT = 9010;
    protected static final int ILP_BUFFER_SIZE = 4 * 1024;
    protected static final int ILP_PORT = 9009;
    protected static final Properties PG_CONNECTION_PROPERTIES = new Properties();
    protected static final int PG_PORT = 8822;
    protected static final String PG_CONNECTION_URI = getPgConnectionUri(PG_PORT);
    protected static Path auxPath;
    protected static Path dbPath;
    protected static int dbPathLen;
    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractTest.setUpStatic();
        TestUtils.unchecked(() -> {
            dbPath = new Path().of(root).concat(PropServerConfiguration.DB_DIRECTORY).$();
            dbPathLen = dbPath.length();
            auxPath = new Path();
            dbPath.trimTo(dbPathLen).$();
        });
    }

    @AfterClass
    public static void tearDownStatic() throws Exception {
        dbPath = Misc.free(dbPath);
        auxPath = Misc.free(auxPath);
        AbstractTest.tearDownStatic();
    }

    protected static void createDummyConfiguration(
            int httpPort,
            int httpMinPort,
            int pgPort,
            int ilpPort,
            String... extra) throws Exception {
        final String confPath = root + Files.SEPARATOR + "conf";
        TestUtils.createTestPath(confPath);
        String file = confPath + Files.SEPARATOR + "server.conf";
        try (PrintWriter writer = new PrintWriter(file, CHARSET)) {

            // enable services
            writer.println("http.enabled=true");
            writer.println("http.min.enabled=true");
            writer.println("pg.enabled=true");
            writer.println("line.tcp.enabled=true");
            writer.println("line.udp.enabled=true");

            // disable services
            writer.println("http.query.cache.enabled=false");
            writer.println("pg.select.cache.enabled=false");
            writer.println("pg.insert.cache.enabled=false");
            writer.println("pg.update.cache.enabled=false");
            writer.println("cairo.wal.enabled.default=false");
            writer.println("metrics.enabled=false");
            writer.println("telemetry.enabled=false");

            // configure end points
            writer.println("http.bind.to=0.0.0.0:" + httpPort);
            writer.println("http.min.net.bind.to=0.0.0.0:" + httpMinPort);
            writer.println("pg.net.bind.to=0.0.0.0:" + pgPort);
            writer.println("line.tcp.net.bind.to=0.0.0.0:" + ilpPort);
            writer.println("line.udp.bind.to=0.0.0.0:" + ilpPort);
            writer.println("line.udp.receive.buffer.size=" + ILP_BUFFER_SIZE);

            // configure worker pools
            writer.println("shared.worker.count=2");
            writer.println("http.worker.count=1");
            writer.println("http.min.worker.count=1");
            writer.println("pg.worker.count=1");
            writer.println("line.tcp.writer.worker.count=1");
            writer.println("line.tcp.io.worker.count=1");

            // extra
            if (extra != null) {
                for (String s : extra) {
                    writer.println(s);
                }
            }
        }

        // mime types
        file = confPath + Files.SEPARATOR + "mime.types";
        try (PrintWriter writer = new PrintWriter(file, CHARSET)) {
            writer.println("");
        }

        // logs
        file = confPath + Files.SEPARATOR + "log.conf";
        System.setProperty("out", file);
        try (PrintWriter writer = new PrintWriter(file, CHARSET)) {
            writer.println("writers=stdout");
            writer.println("w.stdout.class=io.questdb.log.LogConsoleWriter");
            writer.println("w.stdout.level=INFO");
        }
    }

    protected static void createDummyConfiguration(String... extra) throws Exception {
        createDummyConfiguration(HTTP_PORT, HTTP_MIN_PORT, PG_PORT, ILP_PORT, extra);
    }

    protected static void drainWalQueue(CairoEngine engine) {
        try (final ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 1, 1, null)) {
            walApplyJob.drain(0);
            new CheckWalTransactionsJob(engine).run(0);
            // run once again as there might be notifications to handle now
            walApplyJob.drain(0);
        }
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

    static void dropTable(
            SqlCompiler compiler,
            SqlExecutionContext context,
            TableToken tableToken
    ) throws Exception {
        compiler.compile("DROP TABLE " + tableToken.getTableName(), context);
    }

    static {
        PG_CONNECTION_PROPERTIES.setProperty("user", "admin");
        PG_CONNECTION_PROPERTIES.setProperty("password", "quest");
        PG_CONNECTION_PROPERTIES.setProperty("sslmode", "disable");
        PG_CONNECTION_PROPERTIES.setProperty("binaryTransfer", "true");
    }
}
