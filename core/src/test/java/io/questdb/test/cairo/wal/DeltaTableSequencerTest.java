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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.wal.seq.TableSequencerCursorHolder;
import io.questdb.cairo.wal.seq.TableTransactionLog;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.cairo.wal.seq.TableTransactionLogV1;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.questdb.cairo.wal.WalUtils.*;

public class DeltaTableSequencerTest extends AbstractCairoTest {
    @BeforeClass
    public static void setUpStatic() throws Exception {
        engineFactory = configuration -> new CairoEngine(configuration) {
            @Override
            public boolean isDeltaTableSupported() {
                return true;
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testEmptyDeltaPartitionedJournal() throws Exception {
        testEmptyJournal(true, 16);
    }

    @Test
    public void testEmptyDeltaUnpartitionedJournal() throws Exception {
        testEmptyJournal(true, 0);
    }

    @Test
    public void testEmptyWalPartitionedJournal() throws Exception {
        testEmptyJournal(false, 16);
    }

    @Test
    public void testEmptyWalUnpartitionedJournal() throws Exception {
        testEmptyJournal(false, 0);
    }

    @Test
    public void testUnknownJournalFormatRejected() throws Exception {
        assertMemoryLeak(() -> {
            TableToken token = createDelta();
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                path.of(configuration.getDbRoot()).concat(token).concat(SEQ_DIR);
                int seqPathLen = path.size();
                mem.smallFile(configuration.getFilesFacade(), path.concat(TXNLOG_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
                mem.putInt(0, WAL_SEQUENCER_FORMAT_VERSION_V2 + 1);
                mem.close(false);
                path.trimTo(seqPathLen);
                Assert.assertThrows(UnsupportedOperationException.class, () -> TableTransactionLog.readMaxStructureVersion(configuration.getFilesFacade(), path));
                Assert.assertThrows(UnsupportedOperationException.class, () -> engine.getTableSequencerAPI().lastTxn(token));
            }
        });
    }

    private TableToken createDelta() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 0);
        execute("CREATE TABLE d (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY DELTA");
        TableToken token = engine.verifyTableName("d");
        Assert.assertTrue(engine.getTableSequencerAPI().releaseAll());
        return token;
    }

    private void testEmptyJournal(boolean isDelta, int partTransactionCount) throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, partTransactionCount);
            execute("CREATE TABLE d (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + (isDelta ? "DELTA" : "WAL"));
            TableToken token = engine.verifyTableName("d");
            int recordFormat = partTransactionCount == 0 ? WAL_SEQUENCER_FORMAT_VERSION_V1 : WAL_SEQUENCER_FORMAT_VERSION_V2;
            try (Path path = new Path(); MemoryCMARW mem = Vm.getCMARWInstance()) {
                path.of(configuration.getDbRoot()).concat(token).concat(SEQ_DIR);
                int seqPathLen = path.size();
                mem.smallFile(configuration.getFilesFacade(), path.concat(TXNLOG_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
                Assert.assertEquals(recordFormat, mem.getInt(0));
                Assert.assertEquals(0, mem.getLong(TableTransactionLogFile.MAX_TXN_OFFSET_64));
                Assert.assertEquals(partTransactionCount, mem.getInt(TableTransactionLogFile.HEADER_SEQ_PART_SIZE_32));
                if (partTransactionCount == 0) {
                    // Reserved header bytes are not a previous transaction when maxTxn is zero.
                    mem.putLong(TableTransactionLogFile.HEADER_SIZE - TableTransactionLogV1.RECORD_SIZE, 123456789L);
                }
                mem.close(false);
                path.trimTo(seqPathLen);
                Assert.assertEquals(0, TableTransactionLog.readMaxStructureVersion(configuration.getFilesFacade(), path));
                for (int i = 0; i < 2; i++) {
                    Assert.assertTrue(engine.getTableSequencerAPI().releaseAll());
                    Assert.assertEquals(0, engine.getTableSequencerAPI().lastTxn(token));
                    try (
                            TableSequencerCursorHolder holder = new TableSequencerCursorHolder();
                            TransactionLogCursor cursor = engine.getTableSequencerAPI().getCursor(token, 0, holder)
                    ) {
                        Assert.assertEquals(recordFormat, cursor.getVersion());
                        Assert.assertEquals(partTransactionCount, cursor.getPartitionSize());
                        Assert.assertEquals(0, cursor.getMaxTxn());
                        Assert.assertFalse(cursor.hasNext());
                        cursor.toTop();
                        Assert.assertFalse(cursor.hasNext());
                    }
                }
            }
        });
    }
}
