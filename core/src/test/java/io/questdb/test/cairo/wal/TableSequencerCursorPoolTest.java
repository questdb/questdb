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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.seq.TableMetadataChangeLog;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.cairo.wal.seq.TableSequencerCursorPool;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class TableSequencerCursorPoolTest extends AbstractCairoTest {

    @Test
    public void testPoolsOwnIndependentReusableCursors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cursor_pool (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("ALTER TABLE cursor_pool ADD COLUMN value LONG");

            final TableToken tableToken = engine.verifyTableName("cursor_pool");
            final TableSequencerAPI sequencerAPI = engine.getTableSequencerAPI();
            try (
                    TableSequencerCursorPool poolA = new TableSequencerCursorPool();
                    TableSequencerCursorPool poolB = new TableSequencerCursorPool()
            ) {
                final TransactionLogCursor transactionCursorA = sequencerAPI.getCursor(tableToken, 0, poolA);
                final TransactionLogCursor transactionCursorB = sequencerAPI.getCursor(tableToken, 0, poolB);
                Assert.assertNotSame(transactionCursorA, transactionCursorB);
                Assert.assertTrue(transactionCursorA.hasNext());
                Assert.assertTrue(transactionCursorB.hasNext());
                transactionCursorA.close();
                transactionCursorB.close();

                try (
                        TransactionLogCursor reusedCursorA = sequencerAPI.getCursor(tableToken, 0, poolA);
                        TransactionLogCursor reusedCursorB = sequencerAPI.getCursor(tableToken, 0, poolB)
                ) {
                    Assert.assertSame(transactionCursorA, reusedCursorA);
                    Assert.assertSame(transactionCursorB, reusedCursorB);
                }

                final TableMetadataChangeLog metadataCursorA =
                        sequencerAPI.getMetadataChangeLogSlow(tableToken, 0, poolA);
                final TableMetadataChangeLog metadataCursorB =
                        sequencerAPI.getMetadataChangeLogSlow(tableToken, 0, poolB);
                Assert.assertNotSame(metadataCursorA, metadataCursorB);
                Assert.assertTrue(metadataCursorA.hasNext());
                Assert.assertTrue(metadataCursorB.hasNext());
                metadataCursorA.close();
                metadataCursorB.close();

                try (TableMetadataChangeLog reusedMetadataCursor =
                             sequencerAPI.getMetadataChangeLogSlow(tableToken, 0, poolA)) {
                    Assert.assertSame(metadataCursorA, reusedMetadataCursor);
                }
            }
        });
    }
}
