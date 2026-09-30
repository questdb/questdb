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
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * A writer that adds a SYMBOL column itself while it holds uncommitted rows must not touch the records it
 * has already sequenced.
 * <p>
 * Rewriting the last event record is only for the NO_TXN path of a commit, where that record is the
 * commit's own, not yet sequenced, DATA record and has to gain the new column's null flag. A local ADD
 * COLUMN has no such record: its pending rows get theirs, flag included, when they are committed. The last
 * record of the segment is then someone else's already sequenced txn, and rewriting it as DATA replaced
 * that txn's content under its seqTxn.
 */
public class WalEventRewriteSequencedRecordTest extends AbstractCairoTest {

    @Test
    public void testLocalAddSymbolKeepsSequencedTruncate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (WalWriter other = getWalWriter("x")) {
                appendRow(other, 0, 100);
                other.commit();
                try (WalWriter writer = getWalWriter("x")) {
                    // The TRUNCATE is the only record of the writer's segment, so no rows precede the
                    // pending ones and the ADD COLUMN below does not roll them to a new segment.
                    writer.truncateSoft();
                    appendRow(writer, 1, 2);
                    writer.addColumn("s", ColumnType.SYMBOL, AllowAllSecurityContext.INSTANCE);
                    writer.commit();
                }
            }
            drainWalQueue();
            assertNotSuspended("x");
            assertQuery("SELECT v, s FROM x ORDER BY ts").expectSize().returns("v\ts\n2\t\n");
            assertQuery("SELECT count() FROM x WHERE s IS NULL").noRandomAccess().expectSize().returns("count\n1\n");
        });
    }

    @Test
    public void testLocalAddSymbolKeepsSequencedUpdate() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES (0, 100)");
            drainWalQueue();
            try (WalWriter writer = getWalWriter("x")) {
                // A fresh segment whose only record will be the UPDATE.
                writer.rollSegment();
            }
            execute("UPDATE x SET v = 5");
            try (WalWriter writer = getWalWriter("x")) {
                appendRow(writer, 1, 2);
                writer.addColumn("s", ColumnType.SYMBOL, AllowAllSecurityContext.INSTANCE);
                writer.commit();
            }
            drainWalQueue();
            assertNotSuspended("x");
            assertQuery("SELECT v, s FROM x ORDER BY ts").expectSize().returns("v\ts\n5\t\n2\t\n");
            assertQuery("SELECT count() FROM x WHERE s IS NULL").noRandomAccess().expectSize().returns("count\n2\n");
        });
    }

    private static void appendRow(WalWriter writer, long ts, long v) {
        final TableWriter.Row row = writer.newRow(ts);
        row.putLong(1, v);
        row.append();
    }

    private static void assertNotSuspended(String tableName) {
        final TableToken token = engine.verifyTableName(tableName);
        Assert.assertFalse(
                "table must not be suspended: " + engine.getTableSequencerAPI().getTxnTracker(token).getErrorMessage(),
                engine.getTableSequencerAPI().isSuspended(token)
        );
    }
}
