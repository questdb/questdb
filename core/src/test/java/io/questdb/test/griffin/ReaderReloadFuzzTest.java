/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.griffin.wal.AbstractFuzzTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

public class ReaderReloadFuzzTest extends AbstractFuzzTest {
    @Test
    public void testReaderDoesNotReopenFilesFuzz() throws Exception {
        Rnd rnd = generateRandom(LOG);
        fuzzer.setFuzzProbabilities(
                0,
                0,
                0,
                0,
                0.05,
                0.05,
                0,
                0.2,
                0,
                0.5,
                0,
                0
        );

        // Basic load to keep the test lite, we just want to fuzz different transaction types, not intensive inserting
        fuzzer.setFuzzCounts(
                rnd.nextBoolean(),
                rnd.nextInt(2_000),
                rnd.nextInt(1000),
                rnd.nextInt(3),
                rnd.nextInt(5),
                rnd.nextInt(1000),
                rnd.nextInt(1_000_000),
                5 + rnd.nextInt(10)
        );

        setFuzzProperties(1, getRndO3PartitionSplit(rnd), getRndO3PartitionSplitMaxCount(rnd), 10 * Numbers.SIZE_1MB, 3);
        runFuzzWithWithReload(rnd);
    }

    protected void runFuzzWithWithReload(Rnd rnd) throws Exception {
        // Snapshot is not supported on Windows.
        Assume.assumeFalse(Os.isWindows());
        boolean testHardLinkCheckpoint = rnd.nextBoolean();

        AtomicLong openFileCount = new AtomicLong();
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.containsAscii(name, "2000-01-01") && Utf8s.containsAscii(name, "ts.d")) {
                    openFileCount.incrementAndGet();
                }
                return super.openRO(name);
            }
        };

        assertMemoryLeak(ff, () -> {
            int size = rnd.nextInt(16 * 1024 * 1024);
            node1.setProperty(PropertyKey.DEBUG_CAIRO_O3_COLUMN_MEMORY_SIZE, size);
            if (testHardLinkCheckpoint) {
                node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "100G");
            }

            String tableNameNonWal = testName.getMethodName() + "_non_wal";
            fuzzer.createInitialTable(tableNameNonWal, false, 100);
            String tableNameWal = testName.getMethodName();
            TableToken walTable = fuzzer.createInitialTable(tableNameWal, true, 100);

            insert("insert into " + tableNameWal + "(ts) values ('2000-01-01')");
            drainWalQueue();

            try (TableReader reader = engine.getReader(walTable)) {
                reader.openPartition(0);

                reader.goPassive();

                ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(tableNameNonWal, rnd);
                int snapshotIndex = 1 + rnd.nextInt(transactions.size() - 1);

                ObjList<FuzzTransaction> beforeSnapshot = new ObjList<>();
                beforeSnapshot.addAll(transactions, 0, snapshotIndex);
                ObjList<FuzzTransaction> afterSnapshot = new ObjList<>();
                afterSnapshot.addAll(transactions, snapshotIndex, transactions.size());

                fuzzer.applyToWal(beforeSnapshot, tableNameWal, rnd.nextInt(2) + 1, rnd);
                drainWalQueue();

                Assert.assertFalse("table suspended", engine.getTableSequencerAPI().isSuspended(walTable));
                long openFiles = openFileCount.get();

                reader.goActive();
                reader.openPartition(0);
                Assert.assertEquals("unaffected partition should not be reloaded, file open count should stay the same", openFiles, openFileCount.get());
            }
        });
    }
}
