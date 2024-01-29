/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.wal;

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

public class TableTransactionLogFuzzTest extends AbstractCairoTest {
    private final MemorySerializer voidSerializer = new MemorySerializer() {
        @Override
        public void fromSink(Object instance, MemoryCR memory, long offsetLo, long offsetHi) {
        }

        @Override
        public void toSink(Object obj, MemoryA sink) {
        }
    };

    @Test
    public void testAppendReadTransactions() throws Exception {
        FilesFacade ff = new TestFilesFacadeImpl();
        Rnd rnd = TestUtils.generateRandom(LOG, 1428514848626375L, 1706034568367L);

        assertMemoryLeak(() -> {
            int chunkTransactionCount = 1 + rnd.nextInt(50000);

            try (Path path = new Path()) {
                path.of(root);

                path.of(root).concat("v1").$();
                ff.mkdir(path, configuration.getMkDirMode());
                TableTransactionLogV1 v1 = new TableTransactionLogV1(ff);
                v1.create(path.of(root).concat("v1"), 65897);
                v1.open(path);

                path.of(root).concat("v2").$();
                ff.mkdir(path, configuration.getMkDirMode());
                TableTransactionLogV2 v2 = new TableTransactionLogV2(ff, chunkTransactionCount, configuration.getMkDirMode());
                v2.create(path, 65897);
                v2.open(path);

                int txnCount = 1 + (int) (rnd.nextDouble() * 3.0 * chunkTransactionCount);

                IntList transactionIds = new IntList();
                for (int i = 0; i < txnCount; i++) {
                    transactionIds.add(rnd.nextInt(2));
                }
                for (int i = 0; i < txnCount; i++) {
                    int txnId = transactionIds.getQuick(i);
                    writTxn(txnId, v1, i);
                    writTxn(txnId, v2, i);
                }

                // Assert that the transactions are the same from random points
                int iterations = rnd.nextInt(50) + 1;
                for (int i = 0; i < iterations; i++) {
                    int txnLo = rnd.nextInt(txnCount - 1) + 1;
                    assertTxns(transactionIds, txnLo, path.of(root).concat("v1").$(), v1);
                    assertTxns(transactionIds, txnLo, path.of(root).concat("v2").$(), v2);
                }

                v1.close();
                v2.close();

                TableTransactionLogV2.clearThreadLocals();
            }
        });
    }

    private void assertTxns(IntList transactionIds, int txnLo, Path path, TableTransactionLogFile logFile) {

        LOG.info().$("asserting transactions from ").$(txnLo).$(" to ").$(transactionIds.size())
                .$(" with in ").$(path).$();

        try (TransactionLogCursor cursor = logFile.getCursor(txnLo, path)) {
            int size = transactionIds.size();
            for (int i = txnLo - 1; i < size; i++) {

                int txnId = transactionIds.getQuick(i);
                if (txnId == 0) {
                    assertEquals(0, cursor.getStructureVersion());
                    assertEquals(i, cursor.getWalId());
                    assertEquals(i + 1, cursor.getSegmentId());
                    assertEquals(i + 2, cursor.getSegmentTxn());
                    assertEquals(i + 123, cursor.getCommitTimestamp());
                } else {
                    assertEquals(i, cursor.getStructureVersion());
                    assertEquals(STRUCTURAL_CHANGE_WAL_ID, cursor.getWalId());
                    assertEquals(-1, cursor.getSegmentId());
                    assertEquals(-1, cursor.getSegmentTxn());
                    assertEquals(i + 125, cursor.getCommitTimestamp());
                }
                assertEquals(i < size - 1, cursor.hasNext());
            }
        }
    }

    private void writTxn(int txnId, TableTransactionLogFile txnLogFile, int i) {
        if (txnId == 0) {
            txnLogFile.addEntry(0, i, i + 1, i + 2, i + 123);
        } else {
            txnLogFile.beginMetadataChangeEntry(i, voidSerializer, null, i + 125);
            txnLogFile.endMetadataChangeEntry();
        }
    }
}
