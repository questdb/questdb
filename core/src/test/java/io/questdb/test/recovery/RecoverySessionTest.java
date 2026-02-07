/*******************************************************************************
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

package io.questdb.test.recovery;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.recovery.BoundedTxnReader;
import io.questdb.recovery.ConsoleRenderer;
import io.questdb.recovery.RecoverySession;
import io.questdb.recovery.TableDiscoveryService;
import io.questdb.recovery.TxnStateService;
import io.questdb.std.FilesFacade;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;

public class RecoverySessionTest extends AbstractCairoTest {
    private static final FilesFacade FF = TestFilesFacadeImpl.INSTANCE;

    @Test
    public void testSessionTablesAndShowCommands() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithRows("session_tbl", 4);

            RecoverySession session = new RecoverySession(
                    configuration.getDbRoot(),
                    new TableDiscoveryService(FF),
                    new TxnStateService(new BoundedTxnReader(FF)),
                    new ConsoleRenderer()
            );

            ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
            ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
            try (
                    PrintStream out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);
                    PrintStream err = new PrintStream(errBytes, true, StandardCharsets.UTF_8)
            ) {
                int code = session.run(
                        new BufferedReader(new StringReader("tables\nshow session_tbl\nquit\n")),
                        out,
                        err
                );
                Assert.assertEquals(0, code);
            }

            String outText = outBytes.toString(StandardCharsets.UTF_8);
            String errText = errBytes.toString(StandardCharsets.UTF_8);
            Assert.assertTrue(outText.contains("session_tbl"));
            Assert.assertTrue(outText.contains("table: session_tbl"));
            Assert.assertTrue(outText.contains("partitions:"));
            Assert.assertEquals("", errText);
        });
    }

    private static void createTableWithRows(String tableName, int rowCount) throws SqlException {
        execute("create table " + tableName + " (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
        execute(
                "insert into "
                        + tableName
                        + " select rnd_symbol('AA', 'BB', 'CC'), timestamp_sequence('1970-01-01', "
                        + Micros.DAY_MICROS
                        + "L) from long_sequence("
                        + rowCount
                        + ")"
        );
        waitForAppliedRows(tableName, rowCount);
    }

    private static long getRowCount(String tableName) throws SqlException {
        try (RecordCursorFactory factory = select("select count() from " + tableName)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                return cursor.getRecord().getLong(0);
            }
        }
    }

    private static void waitForAppliedRows(String tableName, int expectedRows) throws SqlException {
        for (int i = 0; i < 20; i++) {
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            drainWalQueue(engine);
            if (getRowCount(tableName) == expectedRows) {
                return;
            }
        }
        Assert.assertEquals(expectedRows, getRowCount(tableName));
    }
}
