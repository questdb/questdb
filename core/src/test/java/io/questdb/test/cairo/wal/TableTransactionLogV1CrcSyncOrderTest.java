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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.TableTransactionLogV1;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.std.FilesFacade;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.crash.CrashFaultFilesFacade;
import io.questdb.test.std.SyncAttributingFilesFacade;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.List;

/**
 * V1 submits the advisory CRC sidecar for asynchronous writeback before synchronously flushing the
 * txnlog. Missing sidecar entries read unverified; they do not require an extra synchronous flush.
 */
public class TableTransactionLogV1CrcSyncOrderTest extends AbstractCairoTest {

    @Test
    public void testCrcEntryPrecedesHeaderMaxTxnPublication() throws Exception {
        assertMemoryLeak(() -> {
            final SyncAttributingFilesFacade syncFf = new SyncAttributingFilesFacade();
            final CairoConfiguration cfg = syncConfig(syncFf);

            try (Path path = new Path()) {
                path.of(root).concat("v1seqcrc");
                syncFf.mkdir(path.$(), configuration.getMkDirMode());

                final TableTransactionLogV1 v1 = new TableTransactionLogV1(cfg);
                try {
                    v1.create(path, System.currentTimeMillis());
                    v1.open(path);
                    syncFf.clearCounters(); // ignore syncs during create/open

                    for (int i = 0; i < 5; i++) {
                        v1.addEntry(i, i + 1, i + 2, i + 3, System.currentTimeMillis(), 0L, 0L, 0L);
                    }

                    Assert.assertEquals(0, syncFf.msyncCount("/_txnlog.c", false));
                    Assert.assertEquals(5, syncFf.msyncCount("/_txnlog.c", true));
                    Assert.assertEquals(5, syncFf.msyncCount("/_txnlog", false));
                    assertCrcBeforeHeader(syncFf.barrierOrder());
                } finally {
                    v1.close();
                }
            }
        });
    }

    @Test
    public void testReusedTxnAfterCrashDoesNotRetainStaleChecksum() throws Exception {
        Assume.assumeFalse(Os.isWindows());
        assertMemoryLeak(() -> {
            final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
            try (Path path = new Path()) {
                path.of(root).concat("v1reusedcrc");
                Assert.assertEquals(0, ff.mkdir(path.$(), configuration.getMkDirMode()));
                final String seqDir = path.toString();
                try (TableTransactionLogV1 v1 = new TableTransactionLogV1(syncConfig(ff))) {
                    v1.create(path, 1);
                    v1.open(path);
                    v1.addEntry(0, 1, 0, 0, 1, 0, 0, 1);
                    ff.markDurableBaseline(seqDir);
                    // The OS may persist the next CRC before the txnlog publishes its transaction.
                    v1.beginMetadataChangeEntry(1, null, null, 2);
                    v1.endMetadataChangeEntry();
                    ff.markFileDurable(seqDir + "/" + WalUtils.TXNLOG_CRC_FILE_NAME);
                }
                ff.crash(seqDir);
                try (TableTransactionLogV1 v1 = new TableTransactionLogV1(syncConfig(ff))) {
                    v1.open(path);
                    Assert.assertEquals(1, v1.lastTxn());
                    Assert.assertEquals(2, v1.addEntry(0, 2, 0, 0, 3, 0, 0, 1));
                }
                ff.crash(seqDir);
                try (
                        TableTransactionLogV1 v1 = new TableTransactionLogV1(syncConfig(ff));
                        TransactionLogCursor cursor = v1.getCursor(0, path)
                ) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(1, cursor.getWalId());
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(2, cursor.getWalId());
                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    private static void assertCrcBeforeHeader(List<String> order) {
        int firstCrcIdx = -1;
        int firstHeaderIdx = -1;
        for (int i = 0; i < order.size(); i++) {
            final String p = order.get(i);
            // "_txnlog.c" does not end with "_txnlog", so these two never both match.
            final boolean isCrc = p.endsWith(WalUtils.TXNLOG_CRC_FILE_NAME);
            final boolean isHeader = p.endsWith(WalUtils.TXNLOG_FILE_NAME);
            if (isCrc && firstCrcIdx < 0) {
                firstCrcIdx = i;
            }
            if (isHeader && firstHeaderIdx < 0) {
                firstHeaderIdx = i;
            }
        }

        if (firstCrcIdx < 0 || firstHeaderIdx < 0) {
            final StringBuilder sb = new StringBuilder(
                    "Expected both the CRC sidecar and the txnlog header to be msync'd. Recorded order:\n"
            );
            for (int i = 0; i < order.size(); i++) {
                sb.append("  [").append(i).append("] ").append(order.get(i)).append('\n');
            }
            sb.append("firstCrcIdx=").append(firstCrcIdx).append(" firstHeaderIdx=").append(firstHeaderIdx);
            Assert.fail(sb.toString());
        }

        Assert.assertTrue(
                "the CRC sidecar writeback must be submitted before the txnlog flush"
                        + " (firstCrcIdx=" + firstCrcIdx + " firstHeaderIdx=" + firstHeaderIdx + ")",
                firstCrcIdx < firstHeaderIdx
        );
    }

    private CairoConfiguration syncConfig(FilesFacade syncFf) {
        return new CairoConfigurationWrapper(configuration) {
            @Override
            public int getCommitMode() {
                return CommitMode.SYNC;
            }

            @Override
            public FilesFacade getFilesFacade() {
                return syncFf;
            }
        };
    }
}
