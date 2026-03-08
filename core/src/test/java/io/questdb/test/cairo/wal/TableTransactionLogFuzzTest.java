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

import io.questdb.cairo.MemorySerializer;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.cairo.wal.seq.TableTransactionLogV1;
import io.questdb.cairo.wal.seq.TableTransactionLogV2;
import io.questdb.cairo.wal.seq.TransactionLogCursor;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static io.questdb.cairo.wal.seq.TableTransactionLogFile.STRUCTURAL_CHANGE_WAL_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TableTransactionLogFuzzTest extends AbstractCairoTest {
    private final MemorySerializer voidSerializer = new MemorySerializer() {
        @Override
        public void fromSink(Object instance, MemoryCR memory, long offsetLo, long offsetHi) {
        }

        @Override
        public short getCommandType(Object instance) {
            return 0;
        }

        @Override
        public void toSink(Object obj, MemoryA sink) {
        }
    };

    @Test
    public void testAppendReadTransactions() throws Exception {
        FilesFacade ff = new TestFilesFacadeImpl();
        Rnd rnd = TestUtils.generateRandom(LOG);

        assertMemoryLeak(() -> {
            int chunkTransactionCount = 1 + (1 << rnd.nextInt(12)) + rnd.nextInt(10);

            try (Path path = new Path()) {
                path.of(root);

                path.of(root).concat("v1");
                ff.mkdir(path.$(), configuration.getMkDirMode());
                TableTransactionLogV1 v1 = new TableTransactionLogV1(configuration);
                v1.create(path.of(root).concat("v1"), 65897);
                v1.open(path);

                path.of(root).concat("v2");
                ff.mkdir(path.$(), configuration.getMkDirMode());
                TableTransactionLogV2 v2 = new TableTransactionLogV2(configuration, chunkTransactionCount);
                v2.create(path, 65897);
                v2.open(path);

                int txnCount = 2 + (int) (rnd.nextDouble() * 3.0 * chunkTransactionCount);

                int lastStrVersion = 0;
                IntList transactionIds = new IntList();
                for (int i = 0; i < txnCount; i++) {
                    int txnType = rnd.nextInt(2);
                    lastStrVersion += txnType;
                    transactionIds.add(txnType);
                    transactionIds.add(lastStrVersion);
                }

                for (int i = 0; i < txnCount; i++) {
                    int txnId = transactionIds.getQuick(2 * i);
                    int sv = transactionIds.getQuick(2 * i + 1);
                    writeTxn(txnId, sv, v1, i);
                    writeTxn(txnId, sv, v2, i);
                    if (rnd.nextBoolean()) {
                        // Reopen
                        long strVersion1 = v1.open(path.of(root).concat("v1"));
                        long strVersion2 = v2.open(path.of(root).concat("v2"));
                        assertEquals(strVersion1, strVersion2);

                        assertEquals(sv, strVersion1);
                        assertEquals(sv, strVersion2);
                    }
                }

                assertEquals(txnCount, v1.lastTxn());
                assertEquals(txnCount, v2.lastTxn());

                // Assert that the transactions are the same from random points
                int iterations = rnd.nextInt(50) + 1;
                for (int i = 0; i < iterations; i++) {
                    int txnLo = rnd.nextInt(txnCount - 1) + 1;
                    assertTxns(transactionIds, txnLo, path.of(root).concat("v1"), v1);
                    assertTxns(transactionIds, txnLo, path.of(root).concat("v2"), v2);
                }

                v1.close();
                v2.close();
            }
        });
    }

    private static void compareCursor(IntList transactionIds, int txnLo, TransactionLogCursor cursor) {
        int size = transactionIds.size() / 2;
        for (int i = txnLo; i < size; i++) {
            int txnId = transactionIds.getQuick(2 * i);
            int sv = transactionIds.getQuick(2 * i + 1);
            assertTrue(cursor.hasNext());

            if (txnId == 0) {
                assertEquals(sv, cursor.getStructureVersion());
                assertEquals(i, cursor.getWalId());
                assertEquals(i + 1, cursor.getSegmentId());
                assertEquals(i + 2, cursor.getSegmentTxn());
                assertEquals(i + 123, cursor.getCommitTimestamp());
            } else {
                assertEquals(sv, cursor.getStructureVersion());
                assertEquals(STRUCTURAL_CHANGE_WAL_ID, cursor.getWalId());
                assertEquals(-1, cursor.getSegmentId());
                assertEquals(-1, cursor.getSegmentTxn());
                assertEquals(i + 125, cursor.getCommitTimestamp());
            }
        }
    }

    private void assertTxns(
            IntList transactionIds,
            int txnLo,
            Path path,
            TableTransactionLogFile logFile
    ) {

        LOG.info().$("asserting transactions from ").$(txnLo).$(" to ").$(transactionIds.size())
                .$(" with in ").$(path).$();

        try (TransactionLogCursor cursor = logFile.getCursor(txnLo, path)) {
            compareCursor(transactionIds, txnLo, cursor);
            cursor.toTop();
            compareCursor(transactionIds, txnLo, cursor);
            int size = transactionIds.size() / 2;
            int mid = (size + txnLo) / 2;
            cursor.setPosition(mid);
            compareCursor(transactionIds, mid, cursor);
        }
    }

    private void writeTxn(int txnId, int structureVersion, TableTransactionLogFile txnLogFile, int i) {
        if (txnId == 0) {
            txnLogFile.addEntry(structureVersion, i, i + 1, i + 2, i + 123, 0, 0, 0);
        } else {
            txnLogFile.beginMetadataChangeEntry(structureVersion, voidSerializer, null, i + 125);
            txnLogFile.endMetadataChangeEntry();
        }
    }
}
