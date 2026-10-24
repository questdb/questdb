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


package io.questdb.test.cairo.wal;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalEventCursor;
import io.questdb.cairo.wal.WalEventReader;
import io.questdb.cairo.wal.WalTxnType;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class WalWriterSavepointTest extends AbstractCairoTest {

    @Test
    public void testCommitDropsSavepoint() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createTable();
            try (WalWriter w = engine.getWalWriter(token)) {
                append(w, 1, "2024-01-01T00:00:01");
                w.setSavepoint();
                w.commit();
                append(w, 2, "2024-01-01T00:00:02");
                // The savepoint belonged to the committed txn: roll back this one whole.
                w.rollbackToSavepoint();
                append(w, 3, "2024-01-01T00:00:03");
                w.commit();
            }
            drainWalQueue();
            assertQuery("select v, ts from t").noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary().returns("""
                    v\tts
                    1\t2024-01-01T00:00:01.000000Z
                    3\t2024-01-01T00:00:03.000000Z
                    """);
        });
    }

    @Test
    public void testRollbackToSavepointKeepsRowsAndTimestampBoundsBeforeIt() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createTable();
            try (WalWriter w = engine.getWalWriter(token)) {
                append(w, 1, "2024-01-01T00:00:10");
                append(w, 2, "2024-01-01T00:00:20");
                w.setSavepoint();
                // Widens both bounds and turns the txn out of order; all of it must go.
                append(w, 3, "2024-01-01T00:00:05");
                append(w, 4, "2024-01-01T00:00:30");
                w.rollbackToSavepoint();
                w.commit();
            }
            drainWalQueue();
            assertQuery("select v, ts from t").noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary().returns("""
                    v\tts
                    1\t2024-01-01T00:00:10.000000Z
                    2\t2024-01-01T00:00:20.000000Z
                    """);
            // The txn's event record carries the bounds and the out-of-order flag
            // the dropped rows had changed.
            try (Path path = new Path(); WalEventReader reader = new WalEventReader(configuration)) {
                path.of(configuration.getDbRoot()).concat(token).concat("wal1").slash().put(0);
                WalEventCursor cursor = reader.of(path, 0);
                Assert.assertEquals(WalTxnType.DATA, cursor.getType());
                WalEventCursor.DataInfo info = cursor.getDataInfo();
                Assert.assertEquals(ts("2024-01-01T00:00:10"), info.getMinTimestamp());
                Assert.assertEquals(ts("2024-01-01T00:00:20"), info.getMaxTimestamp());
                Assert.assertFalse(info.isOutOfOrder());
            }
        });
    }

    @Test
    public void testRollbackToSavepointWithoutOneRollsBackTheTxn() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createTable();
            try (WalWriter w = engine.getWalWriter(token)) {
                append(w, 1, "2024-01-01T00:00:01");
                w.rollbackToSavepoint();
                w.commit();
            }
            drainWalQueue();
            assertQuery("select count() from t").noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary().returns("count\n0\n");
        });
    }

    @Test
    public void testSavepointSurvivesColumnAddThatMovesUncommittedRows() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createTable();
            try (WalWriter w = engine.getWalWriter(token)) {
                append(w, 1, "2024-01-01T00:00:01");
                append(w, 2, "2024-01-01T00:00:02");
                w.setSavepoint();
                // Adding a column with rows uncommitted moves them to a new segment.
                addColumn(w, "x", ColumnType.LONG);
                TableWriter.Row row = w.newRow(ts("2024-01-01T00:00:03"));
                row.putLong(0, 3);
                row.putLong(2, 30);
                row.append();
                w.rollbackToSavepoint();
                // The writer keeps working after the restore.
                row = w.newRow(ts("2024-01-01T00:00:04"));
                row.putLong(0, 4);
                row.putLong(2, 40);
                row.append();
                w.commit();
            }
            drainWalQueue();
            assertQuery("select v, x, ts from t").noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary().returns("""
                    v\tx\tts
                    1\tnull\t2024-01-01T00:00:01.000000Z
                    2\tnull\t2024-01-01T00:00:02.000000Z
                    4\t40\t2024-01-01T00:00:04.000000Z
                    """);
        });
    }

    private static void append(WalWriter w, long v, String timestamp) throws Exception {
        TableWriter.Row row = w.newRow(ts(timestamp));
        row.putLong(0, v);
        row.append();
    }

    private static TableToken createTable() throws Exception {
        execute("create table t (v long, ts timestamp) timestamp(ts) partition by day wal");
        return engine.verifyTableName("t");
    }

    private static long ts(String timestamp) throws Exception {
        return io.questdb.std.datetime.microtime.MicrosFormatUtils.parseTimestamp(timestamp + ".000000Z");
    }
}
