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

package io.questdb.test.cairo;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.cairo.fuzz.AbstractFuzzTest;
import io.questdb.test.fuzz.FuzzValidateSymbolFilterOperation;
import io.questdb.test.fuzz.FuzzInsertOperation;
import io.questdb.test.fuzz.FuzzTransaction;
import org.junit.Test;

public class BitmapIndexFuzzTest extends AbstractFuzzTest {
    private static final long BASE_TIMESTAMP = 1704067200000000L; // 2024-01-01T00:00:00.000000Z
    private static final Log LOG = LogFactory.getLog(BitmapIndexFuzzTest.class);
    private static final int MAX_INITIAL_PARTITIONS = 10;
    private static final int MAX_ITERATIONS = 1_000;
    private static final int MAX_MAX_SYMBOL_ID = 200;
    private static final int MAX_ROWS_PER_COMMIT = 10;
    private static final int MAX_ROWS_PER_PARTITION = 30_000;
    private static final int MAX_TIME_DIFF_MINUTES = 10;
    private static final long MICROS_PER_HOUR = 3_600_000_000L;
    private static final long MICROS_PER_MINUTE = 60_000_000L;
    private static final int MIN_INITIAL_PARTITIONS = 2;
    private static final int MIN_ITERATIONS = 100;
    private static final int MIN_MAX_SYMBOL_ID = 50;
    private static final int MIN_ROWS_PER_COMMIT = 1;
    private static final int MIN_ROWS_PER_PARTITION = 1000;
    private static final int MIN_TIME_DIFF_MINUTES = 1;

    private static final String TABLE_NAME = "trades";

    @Test
    public void testIndexAccess() throws Exception {
        final Rnd rnd = generateRandom(LOG);

        final int testIterations = MIN_ITERATIONS + rnd.nextInt(MAX_ITERATIONS - MIN_ITERATIONS + 1);
        final int symbolCountMax = MIN_MAX_SYMBOL_ID + rnd.nextInt(MAX_MAX_SYMBOL_ID - MIN_MAX_SYMBOL_ID + 1);
        final int partitionCount = MIN_INITIAL_PARTITIONS + rnd.nextInt(MAX_INITIAL_PARTITIONS - MIN_INITIAL_PARTITIONS + 1);
        final int rowsPerCommit = MIN_ROWS_PER_COMMIT + rnd.nextInt(MAX_ROWS_PER_COMMIT - MIN_ROWS_PER_COMMIT + 1);
        final int maxTimeDiffMinutes = MIN_TIME_DIFF_MINUTES + rnd.nextInt(MAX_TIME_DIFF_MINUTES - MIN_TIME_DIFF_MINUTES + 1);

        LOG.info().$("Starting fuzz test with partitionCount=").$(partitionCount)
                .$(" symbolCountMax=").$(symbolCountMax)
                .$(" iterations=").$(testIterations)
                .$(" rowsPerCommit=").$(rowsPerCommit)
                .$(" maxTimeDiffMinutes=").$(maxTimeDiffMinutes)
                .$(" seeds: ").$(rnd.getSeed0()).$("L, ").$(rnd.getSeed1()).$("L").$();

        // aggressive partition splitting
        setFuzzProperties(
                1000000L,  // maxApplyTimePerTable
                1L,        // splitPartitionThreshold - aggressive splits
                10,        // o3PartitionSplitMaxCount
                100000L,   // walMaxLagSize
                10         // maxWalFdCache
        );

        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE " + TABLE_NAME + " (" +
                            "  symbol SYMBOL INDEX," +
                            "  ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY HOUR"
            );

            ObjList<FuzzTransaction> transactions = generateIndexedSymbolTransactions(
                    rnd,
                    testIterations,
                    symbolCountMax,
                    partitionCount,
                    rowsPerCommit,
                    maxTimeDiffMinutes
            );

            fuzzer.applyNonWal(transactions, TABLE_NAME, rnd);
        });
    }

    private ObjList<FuzzTransaction> generateIndexedSymbolTransactions(
            Rnd rnd,
            int iterations,
            int maxSymbolId,
            int partitionCount,
            int rowsPerCommit,
            int maxTimeDiffMinutes) {

        ObjList<FuzzTransaction> transactions = new ObjList<>();
        // Initial population transaction
        FuzzTransaction initTransaction = new FuzzTransaction();
        final int rowsPerPartition = MIN_ROWS_PER_PARTITION + rnd.nextInt(MAX_ROWS_PER_PARTITION - MIN_ROWS_PER_PARTITION + 1);

        LOG.info().$("Populating table with ").$(partitionCount).$(" partitions, ")
                .$(maxSymbolId).$(" max symbol ID and ")
                .$(rowsPerPartition).$(" rows per partition").$();
        String[] symbols = new String[maxSymbolId];

        for (int i = 0; i < maxSymbolId; i++) {
            symbols[i] = "SYM" + (i + 1);
        }

        // Populate initial data
        for (int partition = 0; partition < partitionCount; partition++) {
            long partitionTimestamp = BASE_TIMESTAMP + (partition * MICROS_PER_HOUR);

            for (int i = 0; i < rowsPerPartition; i++) {
                long timestampOffset = rnd.nextLong(MICROS_PER_HOUR);
                long timestamp = partitionTimestamp + timestampOffset;

                initTransaction.operationList.add(
                        new FuzzInsertOperation(
                                rnd.getSeed0(),
                                rnd.getSeed1(),
                                timestamp,
                                0.0, // notSet probability
                                0.0, // nullSet probability
                                0.0, // cancelRows probability
                                10,  // string length
                                symbols
                        )
                );
            }
        }
        transactions.add(initTransaction);

        // Generate test transactions with inserts followed by validation transactions
        long currentTimestamp = BASE_TIMESTAMP;
        for (int iter = 0; iter < iterations; iter++) {
            // Transaction 1: Insert batch of rows
            FuzzTransaction insertTransaction = new FuzzTransaction();
            for (int batch = 0; batch < rowsPerCommit; batch++) {
                // Create out-of-order timestamp
                long maxTimeDiffMicros = maxTimeDiffMinutes * MICROS_PER_MINUTE;
                long oooTimestamp = currentTimestamp - rnd.nextLong(maxTimeDiffMicros);
                insertTransaction.operationList.add(
                        new FuzzInsertOperation(
                                rnd.getSeed0(),
                                rnd.getSeed1(),
                                oooTimestamp,
                                0.0,
                                0.0,
                                0.0,
                                10,
                                symbols
                        )
                );
            }
            transactions.add(insertTransaction);

            // Transaction 2: Validation queries (in separate transaction after commit)
            FuzzTransaction validationTransaction = new FuzzTransaction();
            int queriesPerCommit = 3 + rnd.nextInt(8);
            for (int q = 0; q < queriesPerCommit; q++) {
                validationTransaction.operationList.add(
                        new FuzzValidateSymbolFilterOperation(symbols)
                );
            }
            transactions.add(validationTransaction);

            currentTimestamp += rnd.nextLong(MICROS_PER_MINUTE);
        }

        return transactions;
    }
}