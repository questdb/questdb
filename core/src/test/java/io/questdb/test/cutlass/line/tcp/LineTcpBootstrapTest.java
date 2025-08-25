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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.client.Sender;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

import static io.questdb.test.tools.TestUtils.assertEventually;

public class LineTcpBootstrapTest extends AbstractBootstrapTest {
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testAsciiFlagCorrectlySetForVarcharColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();
                CairoEngine engine = serverMain.getEngine();
                try (SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine)) {
                    engine.execute(
                            "CREATE TABLE 'betfairRunners' (\n" +
                                    "  id INT,\n" +
                                    "  runner VARCHAR,\n" +
                                    "  age BYTE,\n" +
                                    "  remarks SYMBOL CAPACITY 2048 CACHE INDEX CAPACITY 256,\n" +
                                    "  timestamp TIMESTAMP\n" +
                                    ") timestamp (timestamp) PARTITION BY MONTH WAL\n" +
                                    "DEDUP UPSERT KEYS(timestamp, id);",
                            sqlExecutionContext
                    );

                    try (Sender sender = Sender.fromConfig("http::addr=localhost:" + serverMain.getHttpServerPort() + ";")) {
                        for (int i = 0; i < 1; i++) {
                            sender.table("betfairRunners").symbol("remarks", "SAw,CkdRnUp&2").
                                    stringColumn("runner", "BallyMac Fifra")
                                    .longColumn("id", 548738)
                                    .longColumn("age", 58)
                                    .at(MicrosFormatUtils.parseTimestamp("2024-06-30T00:00:00Z"), ChronoUnit.MICROS);
                            sender.table("betfairRunners").symbol("remarks", "Fcd-Ck1").
                                    stringColumn("runner", "BallyMac Fifra")
                                    .longColumn("id", 548738)
                                    .longColumn("age", 58)
                                    .at(MicrosFormatUtils.parseTimestamp("2024-06-24T00:00:00Z"), ChronoUnit.MICROS);
                            sender.table("betfairRunners").symbol("remarks", "(R8) LdRnIn військові").
                                    stringColumn("runner", "BallyMac Fifra")
                                    .longColumn("id", 548738)
                                    .longColumn("age", 58)
                                    .at(MicrosFormatUtils.parseTimestamp("2024-06-17T00:00:00Z"), ChronoUnit.MICROS);
                            sender.flush();
                        }
                    }

                    // server main already runs Apply2Wal job in the background. We have to wait for the table to catchup
                    try (RecordCursorFactory waitFact = engine.select("wal_tables where writertxn <> sequencertxn;", sqlExecutionContext)) {
                        assertEventually(() -> {
                            try {
                                try (RecordCursor cursor = waitFact.getCursor(sqlExecutionContext)) {
                                    Assert.assertFalse(cursor.hasNext());
                                }
                            } catch (SqlException e) {
                                throw new RuntimeException(e);
                            }
                        });
                    }

                    try (
                            RecordCursorFactory factory = engine.select("select distinct runner from betfairRunners", sqlExecutionContext);
                            RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                    ) {
                        Assert.assertTrue(cursor.hasNext());
                        if (cursor.hasNext()) {
                            // more than 1 distinct result...
                            // i.e.
                            // runner
                            // varchar
                            // BallyMac Fifra
                            // BallyMac Fifra
                            // both are ascii but one of the above has isAscii set to false
                            // this messes up the hash calculation in OrderedMap when Distinct is executed
                            throw SqlException.$(0, "More than one result in record cursor. Should be one row after distinct query.");
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testDisconnectOnErrorWithWAL() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();
                CairoEngine engine = serverMain.getEngine();
                try (SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine)) {

                    engine.execute("create table x (ts timestamp, a int) timestamp(ts) partition by day wal", sqlExecutionContext);
                }

                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getBindPort();
                try (Sender sender = Sender.builder(Sender.Transport.TCP).address("localhost").port(port).build()) {
                    for (int i = 0; i < 1_000_000; i++) {
                        sender.table("x").stringColumn("a", "42").atNow();
                    }
                    Assert.fail();
                } catch (LineSenderException expected) {
                }
            }
        });
    }
}
