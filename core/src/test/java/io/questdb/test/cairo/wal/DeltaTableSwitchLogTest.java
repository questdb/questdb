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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableWriteMode;
import io.questdb.cairo.wal.DefaultWalDirectoryPolicy;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.cairo.wal.seq.TableTransactionLogV1;
import io.questdb.cairo.wal.seq.TableTransactionLogV2;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.std.FilesFacade;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.wal.WalUtils.*;
import static io.questdb.cairo.wal.seq.TableTransactionLogFile.*;

public class DeltaTableSwitchLogTest extends AbstractCairoTest {
    @Test
    public void testHeaderSyncFailureV1() throws Exception {
        testSwitch(false, 2);
    }

    @Test
    public void testHeaderSyncFailureV2() throws Exception {
        testSwitch(true, 2);
    }

    @Test
    public void testRecordSyncFailureV1() throws Exception {
        testSwitch(false, 1);
    }

    @Test
    public void testRecordSyncFailureV2() throws Exception {
        testSwitch(true, 1);
    }

    @Test
    public void testSwitchDurabilityV1() throws Exception {
        testSwitch(false, 0);
    }

    @Test
    public void testSwitchDurabilityV2() throws Exception {
        testSwitch(true, 0);
    }

    private static void assertPrefix(TransactionLogCursor cursor) {
        for (int i = 1; i <= 2; i++) {
            Assert.assertTrue(cursor.hasNext());
            Assert.assertEquals(i, cursor.getTxn());
            Assert.assertEquals(0, cursor.getStructureVersion());
            Assert.assertEquals(i, cursor.getWalId());
            Assert.assertEquals(i + 10, cursor.getSegmentId());
            Assert.assertEquals(i + 20, cursor.getSegmentTxn());
            Assert.assertEquals(i + 100, cursor.getCommitTimestamp());
        }
    }

    private static void assertSwitch(TransactionLogCursor cursor) {
        Assert.assertTrue(cursor.hasNext());
        Assert.assertEquals(3, cursor.getTxn());
        Assert.assertEquals(0, cursor.getStructureVersion());
        Assert.assertEquals(DELTA_TABLE_SWITCH_WAL_ID, cursor.getWalId());
        Assert.assertEquals(DELTA_TABLE_SWITCH_VERSION, cursor.getSegmentId());
        Assert.assertEquals(TableWriteMode.DELTA_RUNS, cursor.getSegmentTxn());
        Assert.assertEquals(123_456, cursor.getCommitTimestamp());
        Assert.assertFalse(cursor.hasNext());
    }

    private static TableTransactionLogFile newLog(CairoConfiguration config, boolean isPartitioned) {
        return isPartitioned
                ? new TableTransactionLogV2(config, 2, DefaultWalDirectoryPolicy.INSTANCE)
                : new TableTransactionLogV1(config);
    }

    private void testSwitch(boolean isPartitioned, int failAtSync) throws Exception {
        for (int commitMode : new int[]{CommitMode.NOSYNC, CommitMode.ASYNC, CommitMode.SYNC}) {
            assertMemoryLeak(() -> {
                SwitchFilesFacade ff = new SwitchFilesFacade(isPartitioned);
                CairoConfiguration config = new DefaultCairoConfiguration(root) {
                    @Override
                    public int getCommitMode() {
                        return commitMode;
                    }

                    @Override
                    public FilesFacade getFilesFacade() {
                        return ff;
                    }
                };
                try (Path path = new Path()) {
                    path.of(root).concat("journal" + commitMode);
                    Assert.assertEquals(0, ff.mkdir(path.$(), config.getMkDirMode()));
                    int pathLen = path.size();
                    try (TableTransactionLogFile log = newLog(config, isPartitioned)) {
                        log.create(path, 100);
                        log.open(path);
                        for (int i = 1; i <= 2; i++) {
                            log.addEntry(0, i, i + 10, i + 20, i + 100, i, i, 1);
                        }
                        ff.headerFd = ff.openRO(path.concat(TXNLOG_FILE_NAME).$());
                        Assert.assertTrue(ff.headerFd > 0);
                        path.trimTo(pathLen);
                        try {
                            ff.failAtSync = failAtSync;
                            ff.isArmed = true;
                            if (failAtSync == 0) {
                                Assert.assertEquals(3, log.addDeltaTableSwitchEntry(0, 123_456));
                                Assert.assertEquals(2, ff.syncCalls);
                            } else {
                                CairoException error = Assert.assertThrows(CairoException.class,
                                        () -> log.addDeltaTableSwitchEntry(0, 123_456));
                                TestUtils.assertContains(error.getFlyweightMessage(), "injected switch sync failure");
                                Assert.assertEquals(failAtSync, ff.syncCalls);
                            }
                            ff.isArmed = false;
                            // A record-sync failure cannot publish N. A header-sync failure is already
                            // observable as accepted and cannot be treated as an aborted conversion.
                            Assert.assertEquals(failAtSync == 1 ? 2 : 3, log.lastTxn());
                            Assert.assertEquals(failAtSync == 1 ? 2 : 3,
                                    ff.readNonNegativeLong(ff.headerFd, MAX_TXN_OFFSET_64));
                            Assert.assertEquals(isPartitioned ? WAL_SEQUENCER_FORMAT_VERSION_V2 : WAL_SEQUENCER_FORMAT_VERSION_V1,
                                    ff.readNonNegativeInt(ff.headerFd, 0));
                        } finally {
                            ff.isArmed = false;
                            ff.close(ff.headerFd);
                            ff.headerFd = -1;
                        }
                    }
                    try (TableTransactionLogFile reopened = newLog(config, isPartitioned)) {
                        Assert.assertEquals(0, reopened.open(path));
                        try (TransactionLogCursor cursor = reopened.getCursor(0, path)) {
                            assertPrefix(cursor);
                            if (failAtSync == 1) {
                                Assert.assertFalse(cursor.hasNext());
                            } else {
                                assertSwitch(cursor);
                            }
                        }
                        if (failAtSync == 1) {
                            // Reopening ignores the unaccepted record and reuses exactly transaction 3.
                            Assert.assertEquals(3, reopened.addDeltaTableSwitchEntry(0, 123_456));
                            try (TransactionLogCursor cursor = reopened.getCursor(0, path)) {
                                assertPrefix(cursor);
                                assertSwitch(cursor);
                            }
                        }
                    }
                }
            });
        }
    }

    private static class SwitchFilesFacade extends TestFilesFacadeImpl {
        private final boolean isPartitioned;
        private int failAtSync;
        private long firstSyncAddress;
        private long headerFd = -1;
        private boolean isArmed;
        private int syncCalls;

        private SwitchFilesFacade(boolean isPartitioned) {
            this.isPartitioned = isPartitioned;
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            if (isArmed) {
                Assert.assertFalse(async);
                syncCalls++;
                Assert.assertEquals(syncCalls == 1 ? 2 : 3, readNonNegativeLong(headerFd, MAX_TXN_OFFSET_64));
                if (syncCalls == 1) {
                    firstSyncAddress = addr;
                    // The third transaction starts a new V2 part, or follows two V1 records.
                    long recordAddress = addr + (isPartitioned ? 0 : HEADER_SIZE + 2 * TableTransactionLogV1.RECORD_SIZE);
                    Assert.assertEquals(DELTA_TABLE_SWITCH_WAL_ID, Unsafe.getUnsafe().getInt(recordAddress + TX_LOG_WAL_ID_OFFSET));
                    Assert.assertEquals(DELTA_TABLE_SWITCH_VERSION, Unsafe.getUnsafe().getInt(recordAddress + TX_LOG_SEGMENT_OFFSET));
                } else if (isPartitioned) {
                    Assert.assertNotEquals(firstSyncAddress, addr);
                } else {
                    Assert.assertEquals(firstSyncAddress, addr);
                }
                if (syncCalls == failAtSync) {
                    throw CairoException.critical(0).put("injected switch sync failure");
                }
            }
            super.msync(addr, len, async);
        }
    }
}
