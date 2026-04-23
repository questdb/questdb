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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.griffin.SqlCompiler;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.griffin.engine.table.parquet.ParquetVersion;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.cairo.fuzz.AbstractFuzzTest;
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
    public void testMultiRoundO3OnAllParquetPartitions() throws Exception {
        Rnd rnd = generateRandom(LOG);

        // Data-only fuzz: no schema changes, drops, or truncations.
        setFuzzProbabilities(
                0,      // cancelRow
                0,      // notSet
                0.1,    // null
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

        int rowGroupSize = 500 + rnd.nextInt(2000);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, rowGroupSize);

        int parquetVersion = rnd.nextBoolean() ? ParquetVersion.PARQUET_VERSION_V1 : ParquetVersion.PARQUET_VERSION_V2;
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_VERSION, parquetVersion);

        String compressionCodec = randomCompressionCodec(rnd);
        int compressionLevel = randomCompressionLevel(rnd, compressionCodec);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_COMPRESSION_CODEC, compressionCodec);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_COMPRESSION_LEVEL, compressionLevel);

        // Randomize rewrite thresholds so that rewrite mode is exercised with
        // high probability across multiple O3 rounds on small partitions.
        // ratio 0.1-0.5, absolute max 4KB-64KB (reachable with small row groups).
        double rewriteRatio = 0.1 + rnd.nextDouble() * 0.4;
        long rewriteMaxBytes = 4096 + rnd.nextLong(60_000);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, String.valueOf(rewriteRatio));
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, rewriteMaxBytes);

        int initialRowCount = 2000 + rnd.nextInt(5000);
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
            String walTable = getTestName();
            fuzzer.createInitialTableWal(walTable, initialRowCount);
            drainWalQueue();

            String oracleTable = walTable + "_oracle";
            fuzzer.createInitialTableNonWal(oracleTable, null);

            // Convert the last (and only) partition to Parquet — all partitions are now parquet.
            final long partitionTs;
            StringSink partName = new StringSink();
            try (TableReader reader = engine.getReader(walTable)) {
                Assert.assertEquals("expected exactly 1 partition", 1, reader.getPartitionCount());
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

            long dayEnd = partitionTs + DAY_MICROS;

            int rounds = 3 + rnd.nextInt(8);
            LOG.info()
                    .$("starting all-parquet fuzz: initialRowCount=").$(initialRowCount)
                    .$(", rounds=").$(rounds)
                    .$(", rowGroupSize=").$(rowGroupSize)
                    .$(", parquetVersion=V").$(parquetVersion)
                    .$(", compression=").$(compressionCodec)
                    .$(", compressionLevel=").$(compressionLevel)
                    .$(", rewriteRatio=").$(rewriteRatio)
                    .$(", rewriteMaxBytes=").$(rewriteMaxBytes)
                    .$(", partitionTs=").$(partitionTs)
                    .$();

            for (int round = 0; round < rounds; round++) {
                long start, end;
                int pattern = rnd.nextInt(5);

                switch (pattern) {
                    case 0 -> {
                        long center = partitionTs + rnd.nextLong(DAY_MICROS);
                        long halfSpan = 1 + rnd.nextLong(Math.max(1, DAY_MICROS / 20));
                        start = Math.max(partitionTs, center - halfSpan);
                        end = Math.min(dayEnd, center + halfSpan);
                    }
                    case 1 -> {
                        start = partitionTs;
                        end = dayEnd;
                    }
                    case 2 -> {
                        start = partitionTs;
                        end = partitionTs + DAY_MICROS / 2;
                    }
                    case 3 -> {
                        start = partitionTs + DAY_MICROS / 2;
                        end = dayEnd;
                    }
                    default -> {
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

                ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(walTable, rnd, start, end);
                try {
                    fuzzer.applyNonWal(transactions, oracleTable, rnd);
                    fuzzer.applyWal(transactions, walTable, 1, rnd);
                } finally {
                    Misc.freeObjListAndClear(transactions);
                }
                drainWalQueue();
            }

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, oracleTable, walTable, LOG);
            }

            assertRowGroupSizes(walTable, rowGroupSize);
            assertParquetMetaInvariants(walTable);
        });
    }

    @Test
    public void testMultiRoundO3OnParquetPartition() throws Exception {
        Rnd rnd = generateRandom(LOG);

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

        // Randomize the row group size so the test is likely to produce
        // multiple row groups across runs.
        int rowGroupSize = 500 + rnd.nextInt(2000);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, rowGroupSize);

        int parquetVersion = rnd.nextBoolean() ? ParquetVersion.PARQUET_VERSION_V1 : ParquetVersion.PARQUET_VERSION_V2;
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_VERSION, parquetVersion);

        String compressionCodec = randomCompressionCodec(rnd);
        int compressionLevel = randomCompressionLevel(rnd, compressionCodec);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_COMPRESSION_CODEC, compressionCodec);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_COMPRESSION_LEVEL, compressionLevel);

        // Randomize rewrite thresholds so that rewrite mode is exercised with
        // high probability across multiple O3 rounds on small partitions.
        double rewriteRatio = 0.1 + rnd.nextDouble() * 0.4;
        long rewriteMaxBytes = 4096 + rnd.nextLong(60_000);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, String.valueOf(rewriteRatio));
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, rewriteMaxBytes);

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
                    .$(", rowGroupSize=").$(rowGroupSize)
                    .$(", parquetVersion=V").$(parquetVersion)
                    .$(", compression=").$(compressionCodec)
                    .$(", compressionLevel=").$(compressionLevel)
                    .$(", rewriteRatio=").$(rewriteRatio)
                    .$(", rewriteMaxBytes=").$(rewriteMaxBytes)
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

            assertRowGroupSizes(walTable, rowGroupSize);
            assertParquetMetaInvariants(walTable);
        });
    }

    @Test
    public void testSmallInOrderAppendsProduceAtMostOneSmallRowGroup() throws Exception {
        Rnd rnd = generateRandom(LOG);

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
                0,      // equalTs
                0,      // partitionDrop
                0,      // truncate
                0,      // tableDrop
                0,      // setTtl
                0,      // replace
                0       // symbolAccess
        );

        int rowGroupSize = 500 + rnd.nextInt(2000);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, rowGroupSize);

        int parquetVersion = rnd.nextBoolean() ? ParquetVersion.PARQUET_VERSION_V1 : ParquetVersion.PARQUET_VERSION_V2;
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_VERSION, parquetVersion);

        String compressionCodec = randomCompressionCodec(rnd);
        int compressionLevel = randomCompressionLevel(rnd, compressionCodec);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_COMPRESSION_CODEC, compressionCodec);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_COMPRESSION_LEVEL, compressionLevel);

        int initialRowCount = 2000 + rnd.nextInt(5000);
        // Small in-order transactions: low row count per round, no O3 within each batch.
        // A single transaction per round avoids intra-round timestamp overlap.
        fuzzer.setFuzzCounts(
                false,          // isO3
                rowGroupSize / 8, // fuzzRowCount — small relative to RG size
                1,              // transactionCount
                20,             // strLen
                10,             // symbolStrLenMax
                20,             // symbolCountMax
                initialRowCount,
                1,              // partitionCount
                -1,             // parallelWalCount (unused)
                0               // ioFailureCount
        );

        assertMemoryLeak(() -> {
            String walTable = getTestName();
            fuzzer.createInitialTableWal(walTable, initialRowCount);
            drainWalQueue();

            String oracleTable = walTable + "_oracle";
            fuzzer.createInitialTableNonWal(oracleTable, null);

            // Insert rows into the next day so that 2022-02-24 is no longer
            // the active (last) partition and can be converted to Parquet.
            execute("INSERT INTO " + walTable + "(ts) VALUES ('2022-02-25T00:00:00.000000Z'), ('2022-02-25T00:00:01.000000Z')");
            execute("INSERT INTO " + oracleTable + "(ts) VALUES ('2022-02-25T00:00:00.000000Z'), ('2022-02-25T00:00:01.000000Z')");
            drainWalQueue();

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

            // Generate in-order transactions with timestamps after existing data.
            // Initial data covers roughly [00:00, 00:00 + initialRowCount seconds].
            // Each round uses a non-overlapping window advancing through the second
            // half of the day, so every batch appends after the previous one.
            long dayEnd = partitionTs + DAY_MICROS;
            int rounds = 5 + rnd.nextInt(6);
            long windowSize = (dayEnd - partitionTs - DAY_MICROS / 2) / rounds;

            LOG.info()
                    .$("starting in-order fuzz: initialRowCount=").$(initialRowCount)
                    .$(", rounds=").$(rounds)
                    .$(", rowGroupSize=").$(rowGroupSize)
                    .$(", parquetVersion=V").$(parquetVersion)
                    .$(", compression=").$(compressionCodec)
                    .$(", compressionLevel=").$(compressionLevel)
                    .$(", partitionTs=").$(partitionTs)
                    .$();

            for (int round = 0; round < rounds; round++) {
                long start = partitionTs + DAY_MICROS / 2 + round * windowSize;
                long end = start + windowSize;

                LOG.info()
                        .$("round ").$(round)
                        .$(", start=").$(start)
                        .$(", end=").$(end)
                        .$();

                ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(walTable, rnd, start, end);
                try {
                    fuzzer.applyNonWal(transactions, oracleTable, rnd);
                    fuzzer.applyWal(transactions, walTable, 1, rnd);
                } finally {
                    Misc.freeObjListAndClear(transactions);
                }
                drainWalQueue();
            }

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, oracleTable, walTable, LOG);
            }

            assertRowGroupSizes(walTable, rowGroupSize, true);
            assertParquetMetaInvariants(walTable);
        });
    }

    @Test
    public void testO3MergeWithIoFailures() throws Exception {
        Rnd rnd = generateRandom(LOG);

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

        int rowGroupSize = 500 + rnd.nextInt(2000);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, rowGroupSize);

        int parquetVersion = rnd.nextBoolean() ? ParquetVersion.PARQUET_VERSION_V1 : ParquetVersion.PARQUET_VERSION_V2;
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_VERSION, parquetVersion);

        String compressionCodec = randomCompressionCodec(rnd);
        int compressionLevel = randomCompressionLevel(rnd, compressionCodec);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_COMPRESSION_CODEC, compressionCodec);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_COMPRESSION_LEVEL, compressionLevel);

        double rewriteRatio = 0.1 + rnd.nextDouble() * 0.4;
        long rewriteMaxBytes = 4096 + rnd.nextLong(60_000);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, String.valueOf(rewriteRatio));
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, rewriteMaxBytes);

        int initialRowCount = 2000 + rnd.nextInt(5000);
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
                5 + rnd.nextInt(10) // ioFailureCount
        );

        assertMemoryLeak(() -> {
            String walTable = getTestName();
            fuzzer.createInitialTableWal(walTable, initialRowCount);
            drainWalQueue();

            String oracleTable = walTable + "_oracle";
            fuzzer.createInitialTableNonWal(oracleTable, null);

            execute("INSERT INTO " + walTable + "(ts) VALUES ('2022-02-25T00:00:00.000000Z'), ('2022-02-25T00:00:01.000000Z')");
            execute("INSERT INTO " + oracleTable + "(ts) VALUES ('2022-02-25T00:00:00.000000Z'), ('2022-02-25T00:00:01.000000Z')");
            drainWalQueue();

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

            long dayEnd = partitionTs + DAY_MICROS;

            int rounds = 3 + rnd.nextInt(8);
            LOG.info()
                    .$("starting io-failure fuzz: initialRowCount=").$(initialRowCount)
                    .$(", rounds=").$(rounds)
                    .$(", rowGroupSize=").$(rowGroupSize)
                    .$(", parquetVersion=V").$(parquetVersion)
                    .$(", compression=").$(compressionCodec)
                    .$(", compressionLevel=").$(compressionLevel)
                    .$(", rewriteRatio=").$(rewriteRatio)
                    .$(", rewriteMaxBytes=").$(rewriteMaxBytes)
                    .$(", partitionTs=").$(partitionTs)
                    .$();

            for (int round = 0; round < rounds; round++) {
                long start, end;
                int pattern = rnd.nextInt(5);

                switch (pattern) {
                    case 0 -> {
                        long center = partitionTs + rnd.nextLong(DAY_MICROS);
                        long halfSpan = 1 + rnd.nextLong(Math.max(1, DAY_MICROS / 20));
                        start = Math.max(partitionTs, center - halfSpan);
                        end = Math.min(dayEnd, center + halfSpan);
                    }
                    case 1 -> {
                        start = partitionTs;
                        end = dayEnd;
                    }
                    case 2 -> {
                        start = partitionTs;
                        end = partitionTs + DAY_MICROS / 2;
                    }
                    case 3 -> {
                        start = partitionTs + DAY_MICROS / 2;
                        end = dayEnd;
                    }
                    default -> {
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

                ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(walTable, rnd, start, end);
                try {
                    fuzzer.applyNonWal(transactions, oracleTable, rnd);
                    fuzzer.applyWal(transactions, walTable, 1, rnd);
                } finally {
                    Misc.freeObjListAndClear(transactions);
                }
                drainWalQueue();
            }

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, oracleTable, walTable, LOG);
            }

            assertRowGroupSizes(walTable, rowGroupSize);
            assertParquetMetaInvariants(walTable);
        });
    }

    @Test
    public void testO3MergeWithSchemaChanges() throws Exception {
        Rnd rnd = generateRandom(LOG);

        // Enable schema evolution: columns may be added, removed, or renamed
        // between O3 merge rounds. This exercises the parquet rewrite path
        // triggered by schema mismatch (hasMissingColumns, hasExtraColumns).
        setFuzzProbabilities(
                0,      // cancelRow
                0,      // notSet
                0.1,    // null
                0,      // rollback
                0.1,    // colAdd
                0.05,   // colRemove
                0.05,   // colRename
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

        int rowGroupSize = 500 + rnd.nextInt(2000);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, rowGroupSize);

        int parquetVersion = rnd.nextBoolean() ? ParquetVersion.PARQUET_VERSION_V1 : ParquetVersion.PARQUET_VERSION_V2;
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_VERSION, parquetVersion);

        String compressionCodec = randomCompressionCodec(rnd);
        int compressionLevel = randomCompressionLevel(rnd, compressionCodec);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_COMPRESSION_CODEC, compressionCodec);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_COMPRESSION_LEVEL, compressionLevel);

        double rewriteRatio = 0.1 + rnd.nextDouble() * 0.4;
        long rewriteMaxBytes = 4096 + rnd.nextLong(60_000);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, String.valueOf(rewriteRatio));
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, rewriteMaxBytes);

        int initialRowCount = 2000 + rnd.nextInt(5000);
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
            String walTable = getTestName();
            fuzzer.createInitialTableWal(walTable, initialRowCount);
            drainWalQueue();

            String oracleTable = walTable + "_oracle";
            fuzzer.createInitialTableNonWal(oracleTable, null);

            execute("INSERT INTO " + walTable + "(ts) VALUES ('2022-02-25T00:00:00.000000Z'), ('2022-02-25T00:00:01.000000Z')");
            execute("INSERT INTO " + oracleTable + "(ts) VALUES ('2022-02-25T00:00:00.000000Z'), ('2022-02-25T00:00:01.000000Z')");
            drainWalQueue();

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

            long dayEnd = partitionTs + DAY_MICROS;

            int rounds = 3 + rnd.nextInt(8);
            LOG.info()
                    .$("starting schema-change fuzz: initialRowCount=").$(initialRowCount)
                    .$(", rounds=").$(rounds)
                    .$(", rowGroupSize=").$(rowGroupSize)
                    .$(", parquetVersion=V").$(parquetVersion)
                    .$(", compression=").$(compressionCodec)
                    .$(", compressionLevel=").$(compressionLevel)
                    .$(", rewriteRatio=").$(rewriteRatio)
                    .$(", rewriteMaxBytes=").$(rewriteMaxBytes)
                    .$(", partitionTs=").$(partitionTs)
                    .$();

            for (int round = 0; round < rounds; round++) {
                long start, end;
                int pattern = rnd.nextInt(5);

                switch (pattern) {
                    case 0 -> {
                        long center = partitionTs + rnd.nextLong(DAY_MICROS);
                        long halfSpan = 1 + rnd.nextLong(Math.max(1, DAY_MICROS / 20));
                        start = Math.max(partitionTs, center - halfSpan);
                        end = Math.min(dayEnd, center + halfSpan);
                    }
                    case 1 -> {
                        start = partitionTs;
                        end = dayEnd;
                    }
                    case 2 -> {
                        start = partitionTs;
                        end = partitionTs + DAY_MICROS / 2;
                    }
                    case 3 -> {
                        start = partitionTs + DAY_MICROS / 2;
                        end = dayEnd;
                    }
                    default -> {
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

                ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(walTable, rnd, start, end);
                try {
                    fuzzer.applyNonWal(transactions, oracleTable, rnd);
                    fuzzer.applyWal(transactions, walTable, 1, rnd);
                } finally {
                    Misc.freeObjListAndClear(transactions);
                }
                drainWalQueue();
            }

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, oracleTable, walTable, LOG);
            }

            assertRowGroupSizes(walTable, rowGroupSize);
            assertParquetMetaInvariants(walTable);
        });
    }

    private static String randomCompressionCodec(Rnd rnd) {
        String[] codecs = {"uncompressed", "snappy", "gzip", "brotli", "zstd", "lz4_raw"};
        return codecs[rnd.nextInt(codecs.length)];
    }

    private static int randomCompressionLevel(Rnd rnd, String codec) {
        return switch (codec) {
            case "gzip" -> rnd.nextInt(11);       // 0-10
            case "brotli" -> rnd.nextInt(12);     // 0-11
            case "zstd" -> 1 + rnd.nextInt(22);   // 1-22
            default -> 0;
        };
    }

    private void assertParquetMetaInvariants(String tableName) {
        try (TableReader reader = engine.getReader(tableName)) {
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                if (reader.getPartitionFormat(i) != PartitionFormat.PARQUET) {
                    continue;
                }
                reader.openPartition(i);
                ParquetMetaFileReader parquetMeta = reader.getAndInitParquetMetaPartitionDecoder(i).metadata();

                // The _pm footer's parquet file size must equal the size committed in _txn
                // field 3 (the MVCC version token). Any divergence means the committed
                // _txn snapshot no longer resolves to a footer and stale readers break.
                long txParquetFileSize = reader.getTxFile().getPartitionParquetFileSize(i);
                long parquetMetaParquetFileSize = parquetMeta.getParquetFileSize();
                Assert.assertEquals(
                        "partition " + i + ": _pm parquet file size " + parquetMetaParquetFileSize
                                + " != _txn parquet file size " + txParquetFileSize,
                        txParquetFileSize,
                        parquetMetaParquetFileSize
                );

                // Sum of row group sizes recorded in _pm must equal the partition row count
                // recorded in _txn. Divergence indicates the partition and its metadata
                // drifted out of sync during an O3 merge.
                int rgCount = parquetMeta.getRowGroupCount();
                long parquetMetaRowSum = 0;
                for (int rg = 0; rg < rgCount; rg++) {
                    parquetMetaRowSum += parquetMeta.getRowGroupSize(rg);
                }
                long partitionRowCount = reader.getPartitionRowCountFromMetadata(i);
                Assert.assertEquals(
                        "partition " + i + ": _pm row group sum " + parquetMetaRowSum
                                + " != _txn partition row count " + partitionRowCount,
                        partitionRowCount,
                        parquetMetaRowSum
                );

                // _pm must have at least one column.
                Assert.assertTrue(
                        "partition " + i + ": _pm has 0 columns",
                        parquetMeta.getColumnCount() > 0
                );

                // Every column id in _pm must be a non-negative writer index.
                int parquetMetaColumnCount = parquetMeta.getColumnCount();
                for (int c = 0; c < parquetMetaColumnCount; c++) {
                    int columnId = parquetMeta.getColumnId(c);
                    Assert.assertTrue(
                            "partition " + i + ", _pm column " + c + ": column id "
                                    + columnId + " must be non-negative",
                            columnId >= 0
                    );
                }
            }
        }
    }

    private void assertRowGroupSizes(String tableName, int rowGroupSize) {
        assertRowGroupSizes(tableName, rowGroupSize, false);
    }

    private void assertRowGroupSizes(String tableName, int rowGroupSize, boolean assertAtMostOneSmallRg) {
        int smallRgThreshold = rowGroupSize / 4;
        try (TableReader reader = engine.getReader(tableName)) {
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                if (reader.getPartitionFormat(i) != PartitionFormat.PARQUET) {
                    continue;
                }
                reader.openPartition(i);
                ParquetMetaFileReader meta = reader.getAndInitParquetMetaPartitionDecoder(i).metadata();
                int rgCount = meta.getRowGroupCount();
                int smallRgCount = 0;
                for (int rg = 0; rg < rgCount; rg++) {
                    int rgSize = (int) meta.getRowGroupSize(rg);
                    Assert.assertTrue(
                            "row group " + rg + " has " + rgSize + " rows, exceeds 1.5x configured size " + rowGroupSize,
                            rgSize <= rowGroupSize + rowGroupSize / 2
                    );
                    if (rgSize < smallRgThreshold) {
                        smallRgCount++;
                    }
                }
                if (assertAtMostOneSmallRg) {
                    Assert.assertTrue(
                            "partition " + i + " has " + smallRgCount +
                                    " small row groups (< " + smallRgThreshold + " rows), expected at most 1",
                            smallRgCount <= 1
                    );
                }
            }
        }
    }
}
