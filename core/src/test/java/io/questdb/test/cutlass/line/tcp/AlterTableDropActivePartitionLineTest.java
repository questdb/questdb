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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cutlass.line.AbstractLineTcpSender;
import io.questdb.cutlass.line.LineTcpSenderV2;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.network.Net;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class AlterTableDropActivePartitionLineTest extends AbstractBootstrapTest {

    private static final Log LOG = LogFactory.getLog(AlterTableDropActivePartitionLineTest.class);
    private static final String[] colour = {
            "Yellow",
            "Blue",
            "Green",
            "Red",
            "Gray",
            "Orange",
            "Black",
            "White",
            "Pink",
            "Brown",
            "Purple",
    };
    private static final String[] country = {
            "Ukraine",
            "Poland",
            "Lithuania",
            "USA",
            "Germany",
            "Czechia",
            "England",
            "Spain",
            "Singapore",
            "Taiwan",
            "Romania",
    };
    private final String tableName = "PurposelessTable";

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractBootstrapTest.setUpStatic();
        TestUtils.unchecked(() -> createDummyConfiguration());
    }

    @Test
    @Ignore
    public void testServerMainPgWireConcurrentlyWithLineTcpSender() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();

                final CairoEngine engine = serverMain.getEngine();

                // create table over PGWire
                try (
                        Connection connection = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                        PreparedStatement stmt = connection.prepareStatement(
                                "CREATE TABLE " + tableName + "( " +
                                        "favourite_colour SYMBOL INDEX CAPACITY 256, " +
                                        "country SYMBOL INDEX CAPACITY 256, " +
                                        "uniqueId LONG, " +
                                        "quantity INT, " +
                                        "ppu DOUBLE, " +
                                        "addressId STRING, " +
                                        "timestamp TIMESTAMP" +
                                        ") TIMESTAMP(timestamp) PARTITION BY DAY " +
                                        "WITH maxUncommittedRows=1000, o3MaxLag=200000us" // 200 millis
                        )
                ) {
                    LOG.info().$("creating table: ").$safe(tableName).$();
                    stmt.execute();
                }

                TableToken token = engine.verifyTableName(tableName);
                // set up a thread that will send ILP/TCP for today

                // today is deterministic
                final String activePartitionName = "2022-10-19";
                final AtomicLong timestampNano = new AtomicLong(MicrosFormatUtils.parseTimestamp(
                        activePartitionName + "T00:00:00.000000Z") * 1000L
                );

                final SOCountDownLatch ilpAgentHalted = new SOCountDownLatch(1);
                final AtomicBoolean ilpAgentKeepSending = new AtomicBoolean(true);
                final AtomicLong uniqueId = new AtomicLong(0L);

                final Thread ilpAgent = new Thread(() -> {
                    final Rnd rnd = new Rnd();
                    try (AbstractLineTcpSender sender = LineTcpSenderV2.newSender(Net.parseIPv4("127.0.0.1"), ILP_PORT, ILP_BUFFER_SIZE)) {
                        while (ilpAgentKeepSending.get()) {
                            for (int i = 0; i < 100; i++) {
                                addLine(sender, uniqueId, timestampNano, rnd);
                            }
                            sender.flush();
                        }
                        // send a few more
                        for (int i = 0, n = 50 + rnd.nextInt(100); i < n; i++) {
                            addLine(sender, uniqueId, timestampNano, rnd).flush();
                        }
                    } finally {
                        ilpAgentHalted.countDown();
                    }
                });

                // so that we know when the table writer is returned to the pool whence the ilpAgent is stopped
                final SOCountDownLatch tableWriterReturnedToPool = new SOCountDownLatch(1);
                engine.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                    if (name != null && Chars.equalsNc(tableName, name.getTableName())) {
                        if (PoolListener.isWalOrWriter(factoryType) && event == PoolListener.EV_RETURN) {
                            tableWriterReturnedToPool.countDown();
                        }
                    }
                });

                // start sending Lines
                ilpAgent.start();

                // give the ilpAgent some time
                while (uniqueId.get() < 500_000L) {
                    Os.pause();
                }

                // check table reader size
                long beforeDropSize;
                try (TableReader reader = engine.getReader(token)) {
                    beforeDropSize = reader.size();
                    Assert.assertTrue(beforeDropSize > 0L);
                }

                // drop active partition over PGWire
                try (
                        Connection connection = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                        PreparedStatement stmt = connection.prepareStatement("ALTER TABLE " + tableName + " DROP PARTITION LIST '" + activePartitionName + "'")
                ) {
                    LOG.info().$("dropping active partition").$();
                    stmt.execute();
                }

                // tell ilpAgent to stop (it sends a few more lines after processing this signal)
                // and wait until it has finished
                ilpAgentKeepSending.set(false);
                ilpAgentHalted.await();

                // check size
                try (TableReader reader = engine.getReader(token)) {
                    Assert.assertTrue(beforeDropSize > reader.size());
                }

                // wait for table writer to be returned to the pool
                tableWriterReturnedToPool.await();

                // drop active partition over PGWire
                try (
                        Connection connection = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES);
                        PreparedStatement stmt = connection.prepareStatement("ALTER TABLE " + tableName + " DROP PARTITION LIST '" + activePartitionName + "'")
                ) {
                    LOG.info().$("dropping active partition again").$();
                    stmt.execute();
                }

                // check size
                try (
                        SqlExecutionContext context = TestUtils.createSqlExecutionCtx(engine);
                        SqlCompiler compiler = engine.getSqlCompiler()
                ) {
                    TestUtils.assertSql(
                            compiler,
                            context,
                            "SELECT min(timestamp), max(timestamp), count() FROM " + tableName + " WHERE timestamp IN '" + activePartitionName + "'",
                            Misc.getThreadLocalSink(),
                            "min\tmax\tcount\n" +
                                    "\t\t0\n"
                    );
                }
            }
        });
    }

    private static String rndOf(Rnd rnd, String[] array) {
        return array[rnd.nextPositiveInt() % array.length];
    }

    private AbstractLineTcpSender addLine(AbstractLineTcpSender sender, AtomicLong uniqueId, AtomicLong timestampNano, Rnd rnd) {
        sender.metric(tableName)
                .tag("favourite_colour", rndOf(rnd, colour))
                .tag("country", rndOf(rnd, country))
                .field("uniqueId", uniqueId.getAndIncrement())
                .field("quantity", rnd.nextPositiveInt())
                .field("ppu", rnd.nextFloat())
                .field("addressId", rnd.nextString(50))
                .at(timestampNano.getAndAdd(1L + rnd.nextLong(100_000L)), ChronoUnit.NANOS);
        return sender;
    }
}
