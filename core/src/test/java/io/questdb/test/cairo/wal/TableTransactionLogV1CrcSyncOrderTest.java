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
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Pins the ordering invariant for V1's additive CRC sidecar: the CRC for a txn must reach the device
 * BEFORE the header advertises that txn.
 * <p>
 * Get this backwards and a crash in the window leaves a record the reader classifies as
 * absent-beyond-the-watermark -- torn -- which is a loud false alarm on an otherwise healthy table.
 * The safe direction costs nothing: a CRC with no txn behind it is simply never read.
 * <p>
 * Reuses {@link TableTransactionLogV2SyncOrderTest.SyncOrderFilesFacade}, which records msync order
 * by resolving the sync'd address back to the file that was mmap'd.
 */
public class TableTransactionLogV1CrcSyncOrderTest extends AbstractCairoTest {

    @Test
    public void testCrcEntryPrecedesHeaderMaxTxnPublication() throws Exception {
        assertMemoryLeak(() -> {
            final TableTransactionLogV2SyncOrderTest.SyncOrderFilesFacade syncFf =
                    new TableTransactionLogV2SyncOrderTest.SyncOrderFilesFacade();
            final CairoConfiguration cfg = syncConfig(syncFf);

            try (Path path = new Path()) {
                path.of(root).concat("v1seqcrc");
                syncFf.mkdir(path.$(), configuration.getMkDirMode());

                final TableTransactionLogV1 v1 = new TableTransactionLogV1(cfg);
                try {
                    v1.create(path, System.currentTimeMillis());
                    v1.open(path);
                    syncFf.resetSyncOrder(); // ignore syncs during create/open

                    for (int i = 0; i < 5; i++) {
                        v1.addEntry(i, i + 1, i + 2, i + 3, System.currentTimeMillis(), 0L, 0L, 0L);
                    }

                    assertCrcBeforeHeader(syncFf.getSyncOrder());
                } finally {
                    v1.close();
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
                "the CRC sidecar must be durable before the header advertises the txn"
                        + " (firstCrcIdx=" + firstCrcIdx + " firstHeaderIdx=" + firstHeaderIdx + ")",
                firstCrcIdx < firstHeaderIdx
        );
    }

    private CairoConfiguration syncConfig(TableTransactionLogV2SyncOrderTest.SyncOrderFilesFacade syncFf) {
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
