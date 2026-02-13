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

package io.questdb.test.cairo.fuzz;

import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.griffin.SqlCompiler;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.std.datetime.microtime.Micros.DAY_MICROS;

/**
 * Fuzz test that exercises O3 merges on a single Parquet partition across
 * multiple commit rounds with varying row counts and timestamp ranges.
 * <p>
 * Creates a WAL table with a Parquet partition and a non-WAL oracle table,
 * then replays multiple rounds of random O3 transactions into both and
 * verifies they remain identical.
 */
public class O3ParquetMergeStrategyFuzzTest extends AbstractFuzzTest {

    @Test
    public void testMultiRoundO3OnParquetPartition() throws Exception {
        Rnd rnd = generateRandom(LOG, 612796798016125L, 1770992357965L);

        // Data-only fuzz: no schema changes, drops, or truncations.
        setFuzzProbabilities(
                0,      // cancelRow
                0,      // notSet
                0,      // null
                0,      // rollback
                0,      // colAdd
                0,      // colRemove
                0,      // colRename
                0,      // colTypeChange
                1.0,    // dataAdd
                0.5,    // equalTs
                0,      // partitionDrop
                0,      // truncate
                0,      // tableDrop
                0,      // setTtl
                0,      // replace
                0       // symbolAccess
        );

        int initialRowCount = 2000 + rnd.nextInt(5000);
        // O3 enabled, single partition, no IO failures.
        fuzzer.setFuzzCounts(
                true,           // isO3
                5000,           // fuzzRowCount
                30,             // transactionCount
                20,             // strLen
                10,             // symbolStrLenMax
                20,             // symbolCountMax
                initialRowCount,
                1,              // partitionCount
                -1,             // parallelWalCount (unused)
                0               // ioFailureCount
        );

        assertMemoryLeak(() -> {
            // Create WAL table (populates data_temp and adds standard columns).
            String walTable = getTestName();
            fuzzer.createInitialTableWal(walTable, initialRowCount);
            drainWalQueue();

            // Create non-WAL oracle from the same data.
            String oracleTable = walTable + "_oracle";
            fuzzer.createInitialTableNonWal(oracleTable, null);

            // Insert rows into the next day so that 2022-02-24 is no longer
            // the active (last) partition and can be converted to Parquet.
            execute("INSERT INTO " + walTable + "(ts) VALUES ('2022-02-25T00:00:00.000000Z'), ('2022-02-25T00:00:01.000000Z')");
            execute("INSERT INTO " + oracleTable + "(ts) VALUES ('2022-02-25T00:00:00.000000Z'), ('2022-02-25T00:00:01.000000Z')");
            drainWalQueue();

            // Convert the first partition (2022-02-24) to Parquet.
            final long partitionTs;
            StringSink partName = new StringSink();
            try (TableReader reader = engine.getReader(walTable)) {
                Assert.assertTrue("expected at least 2 partitions", reader.getPartitionCount() >= 2);
                partitionTs = reader.getPartitionTimestampByIndex(0);
                PartitionBy.setSinkForPartition(
                        partName,
                        reader.getMetadata().getTimestampType(),
                        reader.getPartitionedBy(),
                        partitionTs
                );
            }
            execute("ALTER TABLE " + walTable + " CONVERT PARTITION TO PARQUET LIST '" + partName + "'");
            drainWalQueue();

            // Widen the O3 timestamp range to the full partition day so that
            // generated data can land before, within, and after the initial rows.
            // Initial data starts at '2022-02-24' (see FuzzRunner.createInitialTable).
            long dayEnd = partitionTs + DAY_MICROS;

            int rounds = 3 + rnd.nextInt(8);
            LOG.info()
                    .$("starting fuzz: initialRowCount=").$(initialRowCount)
                    .$(", rounds=").$(rounds)
                    .$(", partitionTs=").$(partitionTs)
                    .$();

            for (int round = 0; round < rounds; round++) {
                // Pick a timestamp range pattern for this round.
                long start, end;
                int pattern = rnd.nextInt(5);

                switch (pattern) {
                    case 0 -> {
                        // Narrow range: small cluster of rows
                        long center = partitionTs + rnd.nextLong(DAY_MICROS);
                        long halfSpan = 1 + rnd.nextLong(Math.max(1, DAY_MICROS / 20));
                        start = Math.max(partitionTs, center - halfSpan);
                        end = Math.min(dayEnd, center + halfSpan);
                    }
                    case 1 -> {
                        // Full partition span
                        start = partitionTs;
                        end = dayEnd;
                    }
                    case 2 -> {
                        // First half of partition
                        start = partitionTs;
                        end = partitionTs + DAY_MICROS / 2;
                    }
                    case 3 -> {
                        // Second half of partition
                        start = partitionTs + DAY_MICROS / 2;
                        end = dayEnd;
                    }
                    default -> {
                        // Random sub-range
                        long a = partitionTs + rnd.nextLong(DAY_MICROS);
                        long b = partitionTs + rnd.nextLong(DAY_MICROS);
                        start = Math.min(a, b);
                        end = Math.max(a, b) + 1;
                    }
                }

                if (start >= end) {
                    end = start + 1;
                }

                LOG.info()
                        .$("round ").$(round)
                        .$(": pattern=").$(pattern)
                        .$(", start=").$(start)
                        .$(", end=").$(end)
                        .$();

                // Generate transactions from WAL table metadata.
                ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(walTable, rnd, start, end);
                try {
                    // Apply to non-WAL oracle.
                    fuzzer.applyNonWal(transactions, oracleTable, rnd);
                    // Apply to WAL table (writes via WalWriter, then drains WAL queue
                    // which triggers O3 merge with the Parquet partition).
                    fuzzer.applyWal(transactions, walTable, 1, rnd);
                } finally {
                    Misc.freeObjListAndClear(transactions);
                }
                drainWalQueue();
            }

            // Verify WAL+Parquet table matches the non-WAL oracle.
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, oracleTable, walTable, LOG);
            }
        });
    }
}
