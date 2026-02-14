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

package io.questdb.test.recovery;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.recovery.BoundedTxnReader;
import io.questdb.recovery.TxnPartitionState;
import io.questdb.recovery.TxnSerializer;
import io.questdb.recovery.TxnState;
import io.questdb.recovery.TxnStateBuilder;
import io.questdb.recovery.TxnSymbolState;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

public class TxnSerializerTest extends AbstractCairoTest {
    private static final FilesFacade FF = TestFilesFacadeImpl.INSTANCE;

    @Test
    public void testActiveSideMatchesTxWriter() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "ser_active_side";
            execute("create table " + tableName
                    + " (s1 symbol, s2 symbol, val long, ts timestamp) timestamp(ts) partition by DAY");
            execute("insert into " + tableName
                    + " select rnd_symbol('A','B','C'), rnd_symbol('X','Y','Z'), x,"
                    + " timestamp_sequence('1970-01-01', " + Micros.DAY_MICROS + "L) from long_sequence(7)");

            BoundedTxnReader reader = new BoundedTxnReader(FF);
            TxnState state;
            try (Path txnPath = new Path()) {
                txnPathOf(tableName, txnPath).$();
                state = reader.read(txnPath.$());

                try (Path serializedPath = new Path()) {
                    serializedPath.of(configuration.getDbRoot()).concat("active_side_test").$();
                    TxnSerializer.write(state, serializedPath.$(), FF);
                    assertActiveSideIdentical(txnPath.$(), serializedPath.$());
                }
            }
        });
    }

    @Test
    public void testEngineCanQueryAfterTxnRewrite() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "ser_engine_rewrite";
            execute("create table " + tableName
                    + " (sym symbol, val long, ts timestamp) timestamp(ts) partition by DAY");
            execute("insert into " + tableName
                    + " select rnd_symbol('A','B','C'), x,"
                    + " timestamp_sequence('2020-01-01', " + Micros.DAY_MICROS + "L) from long_sequence(5)");

            // capture expected results before rewrite
            long expectedCount;
            long expectedMin;
            long expectedMax;
            long expectedDistinctSym;
            try (var factory = select("select count(), min(ts), max(ts), count_distinct(sym) from " + tableName)) {
                try (var cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    var rec = cursor.getRecord();
                    expectedCount = rec.getLong(0);
                    expectedMin = rec.getTimestamp(1);
                    expectedMax = rec.getTimestamp(2);
                    expectedDistinctSym = rec.getLong(3);
                }
            }

            // read _txn, rewrite it in place
            BoundedTxnReader reader = new BoundedTxnReader(FF);
            try (Path txnPath = new Path()) {
                txnPathOf(tableName, txnPath).$();
                TxnState state = reader.read(txnPath.$());
                TxnSerializer.write(state, txnPath.$(), FF);
            }

            // force engine to re-read from disk
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            // verify same query returns the same results
            try (var factory = select("select count(), min(ts), max(ts), count_distinct(sym) from " + tableName)) {
                try (var cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue("cursor should have rows", cursor.hasNext());
                    var rec = cursor.getRecord();
                    Assert.assertEquals("count", expectedCount, rec.getLong(0));
                    Assert.assertEquals("min(ts)", expectedMin, rec.getTimestamp(1));
                    Assert.assertEquals("max(ts)", expectedMax, rec.getTimestamp(2));
                    Assert.assertEquals("count_distinct(sym)", expectedDistinctSym, rec.getLong(3));
                }
            }
        });
    }

    @Test
    public void testFileSizeMatchesFormula() throws Exception {
        assertMemoryLeak(() -> {
            int[][] configs = {
                    {0, 0},
                    {3, 0},
                    {0, 5},
                    {3, 5},
            };

            for (int[] config : configs) {
                int symbolCount = config[0];
                int partitionCount = config[1];
                String label = symbolCount + "sym_" + partitionCount + "part";

                TxnStateBuilder b = minimalBuilder()
                        .mapWriterCount(symbolCount)
                        .transientRowCount(partitionCount > 0 ? 10 : 0);
                for (int s = 0; s < symbolCount; s++) {
                    b.symbols().add(new TxnSymbolState(s, s + 1, s + 1));
                }
                for (int p = 0; p < partitionCount; p++) {
                    b.partitions().add(new TxnPartitionState(
                            p, (long) p * Micros.DAY_MICROS, p == partitionCount - 1 ? 10 : 100,
                            -1, 0, false, false, 0
                    ));
                }

                TxnState state = b.build();
                try (Path outPath = new Path()) {
                    outPath.of(configuration.getDbRoot()).concat("size_" + label).$();
                    TxnSerializer.write(state, outPath.$(), FF);

                    long fd = FF.openRO(outPath.$());
                    Assert.assertTrue("cannot open " + label, fd > -1);
                    try {
                        long actualSize = FF.length(fd);
                        int symbolsSize = symbolCount * Long.BYTES;
                        int partitionSegmentSize = partitionCount * TableUtils.LONGS_PER_TX_ATTACHED_PARTITION * Long.BYTES;
                        long logicalSize = TableUtils.TX_BASE_HEADER_SIZE
                                + TableUtils.calculateTxRecordSize(symbolsSize, partitionSegmentSize);
                        // file is page-aligned by MemoryCMARW truncation
                        long expectedSize = Files.ceilPageSize(logicalSize);
                        Assert.assertEquals(label + ": file size", expectedSize, actualSize);
                    } finally {
                        FF.close(fd);
                    }
                }
            }
        });
    }

    @Test
    public void testFuzzSelfRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();

            for (int iter = 0; iter < 1000; iter++) {
                long txn = Math.abs(rnd.nextLong()) + 1;
                long seqTxn = Math.abs(rnd.nextLong());
                int lagTxnCount = rnd.nextInt();
                int lagRowCount = rnd.nextInt();
                long lagMinTs = rnd.nextLong();
                long lagMaxTs = rnd.nextLong();

                TxnStateBuilder b = new TxnStateBuilder()
                        .baseVersion(rnd.nextLong())
                        .txn(txn)
                        .transientRowCount(Math.abs(rnd.nextLong()) % 1_000_000)
                        .fixedRowCount(Math.abs(rnd.nextLong()) % 10_000_000)
                        .minTimestamp(rnd.nextLong())
                        .maxTimestamp(rnd.nextLong())
                        .structureVersion(Math.abs(rnd.nextLong()))
                        .dataVersion(Math.abs(rnd.nextLong()))
                        .partitionTableVersion(Math.abs(rnd.nextLong()))
                        .columnVersion(Math.abs(rnd.nextLong()))
                        .truncateVersion(Math.abs(rnd.nextLong()))
                        .seqTxn(seqTxn)
                        .lagTxnCount(lagTxnCount)
                        .lagRowCount(lagRowCount)
                        .lagMinTimestamp(lagMinTs)
                        .lagMaxTimestamp(lagMaxTs)
                        .lagChecksum(TableUtils.calculateTxnLagChecksum(
                                txn, seqTxn, lagRowCount, lagMinTs, lagMaxTs, lagTxnCount
                        ));

                int symbolCount = rnd.nextInt(21);
                b.mapWriterCount(symbolCount);
                for (int s = 0; s < symbolCount; s++) {
                    b.symbols().add(new TxnSymbolState(s, rnd.nextInt(1000), rnd.nextInt(1000)));
                }

                int partitionCount = rnd.nextInt(101);
                for (int p = 0; p < partitionCount; p++) {
                    long rowCount = Math.abs(rnd.nextLong()) & ((1L << 44) - 1);
                    int squashCount = rnd.nextInt(0xFFFF + 1);
                    b.partitions().add(new TxnPartitionState(
                            p,
                            rnd.nextLong(),
                            rowCount,
                            rnd.nextLong(),
                            rnd.nextLong(),
                            rnd.nextBoolean(),
                            rnd.nextBoolean(),
                            squashCount
                    ));
                }

                TxnState state = b.build();
                assertSelfRoundTrip(state, "fuzz iteration " + iter);
            }
        });
    }

    @Test
    public void testModifiedNonWalTableReadableByTxReader() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "ser_mod_nonwal";
            execute("create table " + tableName
                    + " (s1 symbol, s2 symbol, val long, ts timestamp) timestamp(ts) partition by DAY");
            execute("insert into " + tableName
                    + " select rnd_symbol('A','B','C'), rnd_symbol('X','Y','Z'), x,"
                    + " timestamp_sequence('1970-01-01', " + Micros.DAY_MICROS + "L) from long_sequence(7)");

            BoundedTxnReader reader = new BoundedTxnReader(FF);
            TxnState original;
            try (Path txnPath = new Path()) {
                original = reader.read(txnPathOf(tableName, txnPath).$());
            }

            Assert.assertTrue("should have partitions", original.getPartitions().size() > 1);
            Assert.assertEquals("should have 2 symbol columns", 2, original.getSymbols().size());

            // modify: bump row counts, shift timestamps, change versions
            TxnStateBuilder b = copyToBuilder(original)
                    .fixedRowCount(original.getFixedRowCount() + 100)
                    .transientRowCount(original.getTransientRowCount() + 50)
                    .maxTimestamp(original.getMaxTimestamp() + Micros.DAY_MICROS)
                    .dataVersion(original.getDataVersion() + 1)
                    .columnVersion(original.getColumnVersion() + 1);

            TxnState modified = b.build();

            try (Path outPath = new Path()) {
                outPath.of(configuration.getDbRoot()).concat("mod_nonwal_txn").$();
                TxnSerializer.write(modified, outPath.$(), FF);

                try (TxReader txReader = new TxReader(FF)) {
                    txReader.ofRO(outPath.$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
                    Assert.assertTrue("TxReader should load modified file", txReader.unsafeLoadAll());
                    assertTxReaderMatchesState(modified, txReader, "modified non-WAL");
                }
            }
        });
    }

    @Test
    public void testModifiedVersionFieldsReadableByTxReader() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "ser_mod_versions";
            execute("create table " + tableName
                    + " (sym symbol, ts timestamp) timestamp(ts) partition by DAY");
            execute("insert into " + tableName
                    + " select rnd_symbol('AA','BB'), timestamp_sequence('1970-01-01', "
                    + Micros.DAY_MICROS + "L) from long_sequence(3)");

            BoundedTxnReader reader = new BoundedTxnReader(FF);
            TxnState original;
            try (Path txnPath = new Path()) {
                original = reader.read(txnPathOf(tableName, txnPath).$());
            }

            TxnStateBuilder b = copyToBuilder(original)
                    .structureVersion(42)
                    .dataVersion(17)
                    .partitionTableVersion(99)
                    .columnVersion(55)
                    .truncateVersion(3);

            TxnState modified = b.build();

            try (Path outPath = new Path()) {
                outPath.of(configuration.getDbRoot()).concat("mod_versions_txn").$();
                TxnSerializer.write(modified, outPath.$(), FF);

                try (TxReader txReader = new TxReader(FF)) {
                    txReader.ofRO(outPath.$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
                    Assert.assertTrue("TxReader should load modified file", txReader.unsafeLoadAll());
                    assertTxReaderMatchesState(modified, txReader, "modified versions");
                }
            }
        });
    }

    @Test
    public void testModifiedWalTableReadableByTxReader() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "ser_mod_wal";
            execute("create table " + tableName
                    + " (s1 symbol, s2 symbol, s3 symbol, val double, ts timestamp)"
                    + " timestamp(ts) partition by DAY WAL");
            try (WalWriter walWriter = getWalWriter(tableName)) {
                long ts = 0;
                for (int i = 0; i < 5; i++) {
                    TableWriter.Row row = walWriter.newRow(ts);
                    row.putSym(0, "S" + (i % 3));
                    row.putSym(1, "T" + (i % 2));
                    row.putSym(2, "U" + i);
                    row.putDouble(3, i * 1.5);
                    row.append();
                    ts += Micros.DAY_MICROS;
                }
                walWriter.commit();
            }
            waitForAppliedRows(tableName, 5);

            BoundedTxnReader reader = new BoundedTxnReader(FF);
            TxnState original;
            try (Path txnPath = new Path()) {
                original = reader.read(txnPathOf(tableName, txnPath).$());
            }

            Assert.assertEquals("should have 3 symbol columns", 3, original.getSymbols().size());
            Assert.assertTrue("should have partitions", original.getPartitions().size() > 1);

            // modify: change seqTxn, lag fields, bump truncateVersion
            long newSeqTxn = original.getSeqTxn() + 10;
            int newLagRowCount = 7;
            int newLagTxnCount = 3;
            long newLagMin = 1000L;
            long newLagMax = 2000L;
            TxnStateBuilder b = copyToBuilder(original)
                    .seqTxn(newSeqTxn)
                    .lagRowCount(newLagRowCount)
                    .lagTxnCount(newLagTxnCount)
                    .lagMinTimestamp(newLagMin)
                    .lagMaxTimestamp(newLagMax)
                    .lagChecksum(TableUtils.calculateTxnLagChecksum(
                            original.getTxn(), newSeqTxn, newLagRowCount, newLagMin, newLagMax, newLagTxnCount
                    ))
                    .truncateVersion(original.getTruncateVersion() + 1);

            TxnState modified = b.build();

            try (Path outPath = new Path()) {
                outPath.of(configuration.getDbRoot()).concat("mod_wal_txn").$();
                TxnSerializer.write(modified, outPath.$(), FF);

                try (TxReader txReader = new TxReader(FF)) {
                    txReader.ofRO(outPath.$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
                    Assert.assertTrue("TxReader should load modified file", txReader.unsafeLoadAll());
                    assertTxReaderMatchesState(modified, txReader, "modified WAL");
                }

                // also verify self round-trip of the modified state
                TxnState readBack = reader.read(outPath.$());
                assertTxnStatesEqual(modified, readBack, "modified WAL readBack");
            }
        });
    }

    @Test
    public void testOverwriteLargerFileWithSmallerState() throws Exception {
        assertMemoryLeak(() -> {
            // build large state: 10 symbols, 50 partitions
            TxnStateBuilder bigBuilder = minimalBuilder()
                    .mapWriterCount(10)
                    .transientRowCount(99);
            for (int s = 0; s < 10; s++) {
                bigBuilder.symbols().add(new TxnSymbolState(s, s + 1, s + 1));
            }
            for (int p = 0; p < 50; p++) {
                bigBuilder.partitions().add(new TxnPartitionState(
                        p, (long) p * Micros.DAY_MICROS, p == 49 ? 99 : 100,
                        -1, 0, false, false, 0
                ));
            }
            TxnState bigState = bigBuilder.build();

            try (Path outPath = new Path()) {
                outPath.of(configuration.getDbRoot()).concat("overwrite_test").$();

                // write the large state first
                TxnSerializer.write(bigState, outPath.$(), FF);
                long largeSize = fileLength(outPath.$());
                Assert.assertTrue("large file should have nonzero size", largeSize > 0);

                // compute expected logical sizes for comparison
                int largeSymbolsSize = 10 * Long.BYTES;
                int largePartSize = 50 * TableUtils.LONGS_PER_TX_ATTACHED_PARTITION * Long.BYTES;
                long largeLogical = TableUtils.TX_BASE_HEADER_SIZE
                        + TableUtils.calculateTxRecordSize(largeSymbolsSize, largePartSize);
                Assert.assertEquals("large file size", Files.ceilPageSize(largeLogical), largeSize);

                // build small state: 0 symbols, 0 partitions
                TxnState smallState = minimalBuilder()
                        .txn(2)
                        .baseVersion(4)
                        .lagChecksum(TableUtils.calculateTxnLagChecksum(
                                2, 0, 0, Long.MAX_VALUE, Long.MIN_VALUE, 0
                        ))
                        .build();

                // overwrite with the small state
                TxnSerializer.write(smallState, outPath.$(), FF);
                long smallSize = fileLength(outPath.$());
                long smallLogical = TableUtils.TX_BASE_HEADER_SIZE
                        + TableUtils.calculateTxRecordSize(0, 0);
                Assert.assertEquals("small file size", Files.ceilPageSize(smallLogical), smallSize);
                Assert.assertTrue("small logical size should be less than large logical size",
                        smallLogical < largeLogical);

                // read back and verify it matches the small state
                BoundedTxnReader reader = new BoundedTxnReader(FF);
                TxnState readBack = reader.read(outPath.$());
                assertTxnStatesEqual(smallState, readBack, "overwrite readBack");

                // self round-trip on the overwritten file
                assertSelfRoundTrip(smallState, "overwrite small");
            }
        });
    }

    @Test
    public void testProductionRoundTripByteIdentical() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "ser_prod_rt";
            createTableWithTxnPartitions(tableName, 5);

            BoundedTxnReader reader = new BoundedTxnReader(FF);
            TxnState original;
            try (Path txnPath = new Path()) {
                original = reader.read(txnPathOf(tableName, txnPath).$());
            }

            try (Path pathA = new Path(); Path pathB = new Path()) {
                pathA.of(configuration.getDbRoot()).concat("prod_rt_a").$();
                TxnSerializer.write(original, pathA.$(), FF);

                TxnState readBack = reader.read(pathA.$());

                pathB.of(configuration.getDbRoot()).concat("prod_rt_b").$();
                TxnSerializer.write(readBack, pathB.$(), FF);

                assertFilesIdentical(pathA.$(), pathB.$());
            }
        });
    }

    @Test
    public void testProductionTxReaderCanReadSerializedFile() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "ser_production";
            createTableWithTxnPartitions(tableName, 5);

            BoundedTxnReader reader = new BoundedTxnReader(FF);
            TxnState original;
            try (Path txnPath = new Path()) {
                original = reader.read(txnPathOf(tableName, txnPath).$());
            }

            try (Path outPath = new Path()) {
                outPath.of(configuration.getDbRoot()).concat("production_test_txn").$();
                TxnSerializer.write(original, outPath.$(), FF);

                try (TxReader txReader = new TxReader(FF)) {
                    txReader.ofRO(outPath.$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
                    Assert.assertTrue("TxReader should load serialized file", txReader.unsafeLoadAll());
                    assertTxReaderMatchesState(original, txReader, "production TxReader");
                }
            }
        });
    }

    @Test
    public void testSelfRoundTripAllFlags() throws Exception {
        assertMemoryLeak(() -> {
            long maxRowCount = (1L << 44) - 1;
            int maxSquashCount = 0xFFFF;

            TxnStateBuilder b = minimalBuilder().transientRowCount(maxRowCount);
            b.partitions().add(new TxnPartitionState(
                    0, 1000000L, maxRowCount, -1, 8192, true, true, maxSquashCount
            ));
            assertSelfRoundTrip(b.build(), "all flags");
        });
    }

    @Test
    public void testSelfRoundTripBothABSides() throws Exception {
        assertMemoryLeak(() -> {
            // Side A: even baseVersion
            TxnStateBuilder builderA = minimalBuilder()
                    .baseVersion(2)
                    .mapWriterCount(2)
                    .transientRowCount(50);
            builderA.symbols().add(new TxnSymbolState(0, 10, 10));
            builderA.symbols().add(new TxnSymbolState(1, 20, 20));
            builderA.partitions().add(new TxnPartitionState(
                    0, 0L, 100, -1, 0, false, false, 0
            ));
            builderA.partitions().add(new TxnPartitionState(
                    1, Micros.DAY_MICROS, 50, -1, 0, false, false, 0
            ));
            TxnState stateA = builderA.build();
            assertSelfRoundTrip(stateA, "side A");

            // Side B: odd baseVersion, same data
            TxnStateBuilder builderB = minimalBuilder()
                    .baseVersion(3)
                    .mapWriterCount(2)
                    .transientRowCount(50);
            builderB.symbols().add(new TxnSymbolState(0, 10, 10));
            builderB.symbols().add(new TxnSymbolState(1, 20, 20));
            builderB.partitions().add(new TxnPartitionState(
                    0, 0L, 100, -1, 0, false, false, 0
            ));
            builderB.partitions().add(new TxnPartitionState(
                    1, Micros.DAY_MICROS, 50, -1, 0, false, false, 0
            ));
            TxnState stateB = builderB.build();
            assertSelfRoundTrip(stateB, "side B");

            // write both to separate files
            try (Path pathA = new Path(); Path pathB = new Path()) {
                pathA.of(configuration.getDbRoot()).concat("ab_side_a").$();
                pathB.of(configuration.getDbRoot()).concat("ab_side_b").$();
                TxnSerializer.write(stateA, pathA.$(), FF);
                TxnSerializer.write(stateB, pathB.$(), FF);

                // same data ⇒ same file size
                long sizeA = fileLength(pathA.$());
                long sizeB = fileLength(pathB.$());
                Assert.assertEquals("A and B sides should have same file size", sizeA, sizeB);

                // different A/B header slots ⇒ files are NOT byte-identical
                long fdA = FF.openRO(pathA.$());
                Assert.assertTrue(fdA > -1);
                try {
                    long fdB = FF.openRO(pathB.$());
                    Assert.assertTrue(fdB > -1);
                    try {
                        long addrA = FF.mmap(fdA, sizeA, 0, Files.MAP_RO, MemoryTag.MMAP_DEFAULT);
                        long addrB = FF.mmap(fdB, sizeB, 0, Files.MAP_RO, MemoryTag.MMAP_DEFAULT);
                        try {
                            Assert.assertFalse(
                                    "A and B side files should differ in header bytes",
                                    Vect.memeq(addrA, addrB, sizeA)
                            );
                        } finally {
                            FF.munmap(addrA, sizeA, MemoryTag.MMAP_DEFAULT);
                            FF.munmap(addrB, sizeB, MemoryTag.MMAP_DEFAULT);
                        }
                    } finally {
                        FF.close(fdB);
                    }
                } finally {
                    FF.close(fdA);
                }
            }
        });
    }

    @Test
    public void testSelfRoundTripEmptyPartitions() throws Exception {
        assertMemoryLeak(() -> assertSelfRoundTrip(createMinimalState(), "empty partitions"));
    }

    @Test
    public void testSelfRoundTripLargePartition() throws Exception {
        assertMemoryLeak(() -> {
            long largeRowCount = (1L << 44) - 1;

            TxnStateBuilder b = minimalBuilder().transientRowCount(largeRowCount);
            b.partitions().add(new TxnPartitionState(
                    0, 1000000L, largeRowCount, -1, 0, false, false, 0
            ));
            assertSelfRoundTrip(b.build(), "large partition");
        });
    }

    @Test
    public void testSelfRoundTripMultiplePartitions() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "ser_multi";
            createTableWithTxnPartitions(tableName, 10);

            BoundedTxnReader reader = new BoundedTxnReader(FF);
            TxnState original;
            try (Path txnPath = new Path()) {
                original = reader.read(txnPathOf(tableName, txnPath).$());
            }

            assertSelfRoundTrip(original, "multiple partitions");
        });
    }

    @Test
    public void testSelfRoundTripParquetFlag() throws Exception {
        assertMemoryLeak(() -> {
            TxnStateBuilder b = minimalBuilder().transientRowCount(100);
            b.partitions().add(new TxnPartitionState(
                    0, 1000000L, 100, -1, 4096, true, false, 0
            ));
            assertSelfRoundTrip(b.build(), "parquet flag");
        });
    }

    @Test
    public void testSelfRoundTripReadOnlyFlag() throws Exception {
        assertMemoryLeak(() -> {
            TxnStateBuilder b = minimalBuilder().transientRowCount(100);
            b.partitions().add(new TxnPartitionState(
                    0, 1000000L, 100, -1, 0, false, true, 0
            ));
            assertSelfRoundTrip(b.build(), "readOnly flag");
        });
    }

    @Test
    public void testSelfRoundTripSinglePartition() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "ser_single";
            createNonWalTableWithRows(tableName, 1);

            BoundedTxnReader reader = new BoundedTxnReader(FF);
            TxnState original;
            try (Path txnPath = new Path()) {
                original = reader.read(txnPathOf(tableName, txnPath).$());
            }

            assertSelfRoundTrip(original, "single partition");
        });
    }

    @Test
    public void testSelfRoundTripSquashCount() throws Exception {
        assertMemoryLeak(() -> {
            TxnStateBuilder b = minimalBuilder().transientRowCount(500);
            b.partitions().add(new TxnPartitionState(
                    0, 1000000L, 500, -1, 0, false, false, 42
            ));
            assertSelfRoundTrip(b.build(), "squash count");
        });
    }

    @Test
    public void testSelfRoundTripWithSymbols() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "ser_symbols";
            execute("create table " + tableName
                    + " (s1 symbol, s2 symbol, s3 symbol, ts timestamp) timestamp(ts) partition by DAY");
            execute("insert into " + tableName
                    + " select rnd_symbol('A','B'), rnd_symbol('C','D'), rnd_symbol('E','F'),"
                    + " timestamp_sequence('1970-01-01', " + Micros.DAY_MICROS + "L) from long_sequence(5)");

            BoundedTxnReader reader = new BoundedTxnReader(FF);
            TxnState original;
            try (Path txnPath = new Path()) {
                original = reader.read(txnPathOf(tableName, txnPath).$());
            }

            Assert.assertTrue("should have symbols", original.getSymbols().size() > 0);
            assertSelfRoundTrip(original, "with symbols");
        });
    }

    @Test
    public void testNegativeMapWriterCountClampedToZero() throws Exception {
        assertMemoryLeak(() -> {
            TxnStateBuilder b = minimalBuilder().mapWriterCount(-1);
            TxnState state = b.build();

            try (Path outPath = new Path()) {
                outPath.of(configuration.getDbRoot()).concat("neg_mwc_test").$();
                TxnSerializer.write(state, outPath.$(), FF);

                BoundedTxnReader reader = new BoundedTxnReader(FF);
                TxnState readBack = reader.read(outPath.$());
                // Math.max(0, -1) = 0 in TxnSerializer.write
                Assert.assertEquals(0, readBack.getMapWriterCount());
                Assert.assertEquals(0, readBack.getSymbols().size());
            }
        });
    }

    @Test
    public void testPartitionWithAllFlagBitsSet() throws Exception {
        assertMemoryLeak(() -> {
            long maxRowCount = (1L << 44) - 1;
            int maxSquashCount = 0xFFFF;
            long txn = 2;

            // baseVersion must equal txn for TxReader.unsafeLoadAll() to succeed
            TxnStateBuilder b = minimalBuilder()
                    .baseVersion(txn)
                    .txn(txn)
                    .transientRowCount(maxRowCount)
                    .mapWriterCount(2)
                    .lagChecksum(TableUtils.calculateTxnLagChecksum(
                            txn, 0, 0, Long.MAX_VALUE, Long.MIN_VALUE, 0
                    ));
            b.symbols().add(new TxnSymbolState(0, 5, 5));
            b.symbols().add(new TxnSymbolState(1, 10, 10));
            b.partitions().add(new TxnPartitionState(
                    0, Long.MIN_VALUE, maxRowCount, Long.MAX_VALUE, Long.MAX_VALUE,
                    true, true, maxSquashCount
            ));

            TxnState state = b.build();
            assertSelfRoundTrip(state, "all flags maxed");

            // verify via TxReader too
            try (Path outPath = new Path()) {
                outPath.of(configuration.getDbRoot()).concat("all_flags_maxed").$();
                TxnSerializer.write(state, outPath.$(), FF);

                try (TxReader txReader = new TxReader(FF)) {
                    txReader.ofRO(outPath.$(), io.questdb.cairo.ColumnType.TIMESTAMP, io.questdb.cairo.PartitionBy.DAY);
                    Assert.assertTrue(txReader.unsafeLoadAll());
                    Assert.assertTrue(txReader.isPartitionParquet(0));
                    Assert.assertTrue(txReader.isPartitionReadOnly(0));
                    Assert.assertEquals(maxSquashCount, txReader.getPartitionSquashCount(0));
                }
            }
        });
    }

    private static void assertActiveSideIdentical(LPSZ production, LPSZ serialized) {
        long fdProd = FF.openRO(production);
        Assert.assertTrue("cannot open production file", fdProd > -1);
        try {
            long fdSer = FF.openRO(serialized);
            Assert.assertTrue("cannot open serialized file", fdSer > -1);
            try {
                long sizeProd = FF.length(fdProd);
                long sizeSer = FF.length(fdSer);
                Assert.assertTrue("production file too small", sizeProd >= TableUtils.TX_BASE_HEADER_SIZE);
                Assert.assertTrue("serialized file too small", sizeSer >= TableUtils.TX_BASE_HEADER_SIZE);

                long addrProd = FF.mmap(fdProd, sizeProd, 0, Files.MAP_RO, MemoryTag.MMAP_DEFAULT);
                Assert.assertNotEquals(-1, addrProd);
                try {
                    long addrSer = FF.mmap(fdSer, sizeSer, 0, Files.MAP_RO, MemoryTag.MMAP_DEFAULT);
                    Assert.assertNotEquals(-1, addrSer);
                    try {
                        // determine active side from version
                        long version = Unsafe.getUnsafe().getLong(addrProd + TableUtils.TX_BASE_OFFSET_VERSION_64);
                        Assert.assertEquals("version mismatch",
                                version, Unsafe.getUnsafe().getLong(addrSer + TableUtils.TX_BASE_OFFSET_VERSION_64));

                        boolean isA = (version & 1L) == 0L;
                        long baseOffsetSlot = isA ? TableUtils.TX_BASE_OFFSET_A_32 : TableUtils.TX_BASE_OFFSET_B_32;
                        long symbolsSizeSlot = isA ? TableUtils.TX_BASE_OFFSET_SYMBOLS_SIZE_A_32 : TableUtils.TX_BASE_OFFSET_SYMBOLS_SIZE_B_32;
                        long partitionsSizeSlot = isA ? TableUtils.TX_BASE_OFFSET_PARTITIONS_SIZE_A_32 : TableUtils.TX_BASE_OFFSET_PARTITIONS_SIZE_B_32;

                        int baseOffsetProd = Unsafe.getUnsafe().getInt(addrProd + baseOffsetSlot);
                        int symbolsSizeProd = Unsafe.getUnsafe().getInt(addrProd + symbolsSizeSlot);
                        int partitionsSizeProd = Unsafe.getUnsafe().getInt(addrProd + partitionsSizeSlot);
                        int recordSizeProd = TableUtils.calculateTxRecordSize(symbolsSizeProd, partitionsSizeProd);

                        int baseOffsetSer = Unsafe.getUnsafe().getInt(addrSer + baseOffsetSlot);
                        int symbolsSizeSer = Unsafe.getUnsafe().getInt(addrSer + symbolsSizeSlot);
                        int partitionsSizeSer = Unsafe.getUnsafe().getInt(addrSer + partitionsSizeSlot);
                        int recordSizeSer = TableUtils.calculateTxRecordSize(symbolsSizeSer, partitionsSizeSer);

                        Assert.assertEquals("record sizes differ", recordSizeProd, recordSizeSer);

                        // base offsets may differ (production file has room for both A+B records),
                        // so compare record content at each file's own base offset
                        int recordSize = recordSizeProd;
                        if (!Vect.memeq(addrProd + baseOffsetProd, addrSer + baseOffsetSer, recordSize)) {
                            for (int i = 0; i < recordSize; i++) {
                                byte a = Unsafe.getUnsafe().getByte(addrProd + baseOffsetProd + i);
                                byte b = Unsafe.getUnsafe().getByte(addrSer + baseOffsetSer + i);
                                if (a != b) {
                                    Assert.fail(String.format(
                                            "active side differs at record offset %d: production=0x%02X serialized=0x%02X (recordSize=%d)",
                                            i, a & 0xFF, b & 0xFF, recordSize
                                    ));
                                }
                            }
                        }
                    } finally {
                        FF.munmap(addrSer, sizeSer, MemoryTag.MMAP_DEFAULT);
                    }
                } finally {
                    FF.munmap(addrProd, sizeProd, MemoryTag.MMAP_DEFAULT);
                }
            } finally {
                FF.close(fdSer);
            }
        } finally {
            FF.close(fdProd);
        }
    }

    private static void assertFilesIdentical(LPSZ pathA, LPSZ pathB) {
        long fdA = FF.openRO(pathA);
        Assert.assertTrue("cannot open " + pathA, fdA > -1);
        try {
            long fdB = FF.openRO(pathB);
            Assert.assertTrue("cannot open " + pathB, fdB > -1);
            try {
                long sizeA = FF.length(fdA);
                long sizeB = FF.length(fdB);
                Assert.assertEquals(
                        "file sizes differ: " + pathA + "=" + sizeA + ", " + pathB + "=" + sizeB,
                        sizeA, sizeB
                );
                if (sizeA == 0) {
                    return;
                }

                long addrA = FF.mmap(fdA, sizeA, 0, Files.MAP_RO, MemoryTag.MMAP_DEFAULT);
                Assert.assertNotEquals("mmap failed for " + pathA, -1, addrA);
                try {
                    long addrB = FF.mmap(fdB, sizeB, 0, Files.MAP_RO, MemoryTag.MMAP_DEFAULT);
                    Assert.assertNotEquals("mmap failed for " + pathB, -1, addrB);
                    try {
                        if (!Vect.memeq(addrA, addrB, sizeA)) {
                            for (long i = 0; i < sizeA; i++) {
                                byte a = Unsafe.getUnsafe().getByte(addrA + i);
                                byte b = Unsafe.getUnsafe().getByte(addrB + i);
                                if (a != b) {
                                    Assert.fail(String.format(
                                            "files differ at offset %d: expected 0x%02X but was 0x%02X (fileSize=%d)",
                                            i, a & 0xFF, b & 0xFF, sizeA
                                    ));
                                }
                            }
                        }
                    } finally {
                        FF.munmap(addrB, sizeB, MemoryTag.MMAP_DEFAULT);
                    }
                } finally {
                    FF.munmap(addrA, sizeA, MemoryTag.MMAP_DEFAULT);
                }
            } finally {
                FF.close(fdB);
            }
        } finally {
            FF.close(fdA);
        }
    }

    private static void assertTxReaderMatchesState(TxnState state, TxReader txReader, String context) {
        Assert.assertEquals(context + ": txn", state.getTxn(), txReader.getTxn());
        Assert.assertEquals(context + ": version", state.getBaseVersion(), txReader.getVersion());
        Assert.assertEquals(context + ": transientRowCount", state.getTransientRowCount(), txReader.getTransientRowCount());
        Assert.assertEquals(context + ": fixedRowCount", state.getFixedRowCount(), txReader.getFixedRowCount());
        Assert.assertEquals(context + ": rowCount", state.getTransientRowCount() + state.getFixedRowCount(), txReader.getRowCount());
        Assert.assertEquals(context + ": minTimestamp", state.getMinTimestamp(), txReader.getMinTimestamp());
        Assert.assertEquals(context + ": maxTimestamp", state.getMaxTimestamp(), txReader.getMaxTimestamp());
        Assert.assertEquals(context + ": dataVersion", state.getDataVersion(), txReader.getDataVersion());
        Assert.assertEquals(context + ": partitionTableVersion", state.getPartitionTableVersion(), txReader.getPartitionTableVersion());
        Assert.assertEquals(context + ": columnVersion", state.getColumnVersion(), txReader.getColumnVersion());
        Assert.assertEquals(context + ": truncateVersion", state.getTruncateVersion(), txReader.getTruncateVersion());
        Assert.assertEquals(context + ": seqTxn", state.getSeqTxn(), txReader.getSeqTxn());
        Assert.assertEquals(context + ": lagRowCount", state.getLagRowCount(), txReader.getLagRowCount());
        Assert.assertEquals(context + ": lagTxnCount", Math.abs(state.getLagTxnCount()), txReader.getLagTxnCount());
        Assert.assertEquals(context + ": lagMinTimestamp", state.getLagMinTimestamp(), txReader.getLagMinTimestamp());
        Assert.assertEquals(context + ": lagMaxTimestamp", state.getLagMaxTimestamp(), txReader.getLagMaxTimestamp());

        // symbol segment: TxReader stores committed counts only
        Assert.assertEquals(context + ": symbolColumnCount", state.getMapWriterCount(), txReader.getSymbolColumnCount());
        for (int i = 0, n = state.getSymbols().size(); i < n; i++) {
            Assert.assertEquals(
                    context + ": symbol[" + i + "].count",
                    state.getSymbols().getQuick(i).count(),
                    txReader.getSymbolValueCount(i)
            );
        }

        // partition segment — TxReader patches last partition's row count with transientRowCount
        int partitionCount = state.getPartitions().size();
        Assert.assertEquals(context + ": partitionCount", partitionCount, txReader.getPartitionCount());
        for (int i = 0; i < partitionCount; i++) {
            TxnPartitionState sp = state.getPartitions().getQuick(i);
            boolean isLast = (i == partitionCount - 1);
            long expectedRowCount = isLast ? state.getTransientRowCount() : sp.rowCount();
            Assert.assertEquals(context + ": partition[" + i + "].timestampLo", sp.timestampLo(), txReader.getPartitionTimestampByIndex(i));
            Assert.assertEquals(context + ": partition[" + i + "].rowCount", expectedRowCount, txReader.getPartitionSize(i));
            Assert.assertEquals(context + ": partition[" + i + "].nameTxn", sp.nameTxn(), txReader.getPartitionNameTxn(i));
            Assert.assertEquals(context + ": partition[" + i + "].parquetFileSize", sp.parquetFileSize(), txReader.getPartitionParquetFileSize(i));
            Assert.assertEquals(context + ": partition[" + i + "].parquetFormat", sp.parquetFormat(), txReader.isPartitionParquet(i));
            Assert.assertEquals(context + ": partition[" + i + "].readOnly", sp.readOnly(), txReader.isPartitionReadOnly(i));
            Assert.assertEquals(context + ": partition[" + i + "].squashCount", sp.squashCount(), txReader.getPartitionSquashCount(i));
        }
    }

    private static void assertSelfRoundTrip(TxnState state, String context) {
        BoundedTxnReader reader = new BoundedTxnReader(FF);
        try (Path pathA = new Path(); Path pathB = new Path()) {
            pathA.of(configuration.getDbRoot()).concat("rt_a_" + context.replace(' ', '_')).$();
            pathB.of(configuration.getDbRoot()).concat("rt_b_" + context.replace(' ', '_')).$();

            TxnSerializer.write(state, pathA.$(), FF);
            TxnState readBack = reader.read(pathA.$());
            TxnSerializer.write(readBack, pathB.$(), FF);

            assertFilesIdentical(pathA.$(), pathB.$());
            assertTxnStatesEqual(state, readBack, context);
        }
    }

    private static void assertTxnStatesEqual(TxnState expected, TxnState actual, String context) {
        Assert.assertEquals(context + ": txn", expected.getTxn(), actual.getTxn());
        Assert.assertEquals(context + ": transientRowCount", expected.getTransientRowCount(), actual.getTransientRowCount());
        Assert.assertEquals(context + ": fixedRowCount", expected.getFixedRowCount(), actual.getFixedRowCount());
        Assert.assertEquals(context + ": minTimestamp", expected.getMinTimestamp(), actual.getMinTimestamp());
        Assert.assertEquals(context + ": maxTimestamp", expected.getMaxTimestamp(), actual.getMaxTimestamp());
        Assert.assertEquals(context + ": structureVersion", expected.getStructureVersion(), actual.getStructureVersion());
        Assert.assertEquals(context + ": dataVersion", expected.getDataVersion(), actual.getDataVersion());
        Assert.assertEquals(context + ": partitionTableVersion", expected.getPartitionTableVersion(), actual.getPartitionTableVersion());
        Assert.assertEquals(context + ": columnVersion", expected.getColumnVersion(), actual.getColumnVersion());
        Assert.assertEquals(context + ": truncateVersion", expected.getTruncateVersion(), actual.getTruncateVersion());
        Assert.assertEquals(context + ": seqTxn", expected.getSeqTxn(), actual.getSeqTxn());
        Assert.assertEquals(context + ": lagChecksum", expected.getLagChecksum(), actual.getLagChecksum());
        Assert.assertEquals(context + ": lagTxnCount", expected.getLagTxnCount(), actual.getLagTxnCount());
        Assert.assertEquals(context + ": lagRowCount", expected.getLagRowCount(), actual.getLagRowCount());
        Assert.assertEquals(context + ": lagMinTimestamp", expected.getLagMinTimestamp(), actual.getLagMinTimestamp());
        Assert.assertEquals(context + ": lagMaxTimestamp", expected.getLagMaxTimestamp(), actual.getLagMaxTimestamp());
        Assert.assertEquals(context + ": mapWriterCount", expected.getMapWriterCount(), actual.getMapWriterCount());

        Assert.assertEquals(context + ": symbol count", expected.getSymbols().size(), actual.getSymbols().size());
        for (int i = 0, n = expected.getSymbols().size(); i < n; i++) {
            TxnSymbolState es = expected.getSymbols().getQuick(i);
            TxnSymbolState as = actual.getSymbols().getQuick(i);
            Assert.assertEquals(context + ": symbol[" + i + "].count", es.count(), as.count());
            Assert.assertEquals(context + ": symbol[" + i + "].transientCount", es.transientCount(), as.transientCount());
        }

        Assert.assertEquals(context + ": partition count", expected.getPartitions().size(), actual.getPartitions().size());
        for (int i = 0, n = expected.getPartitions().size(); i < n; i++) {
            TxnPartitionState ep = expected.getPartitions().getQuick(i);
            TxnPartitionState ap = actual.getPartitions().getQuick(i);
            Assert.assertEquals(context + ": partition[" + i + "].timestampLo", ep.timestampLo(), ap.timestampLo());
            Assert.assertEquals(context + ": partition[" + i + "].rowCount", ep.rowCount(), ap.rowCount());
            Assert.assertEquals(context + ": partition[" + i + "].nameTxn", ep.nameTxn(), ap.nameTxn());
            Assert.assertEquals(context + ": partition[" + i + "].parquetFileSize", ep.parquetFileSize(), ap.parquetFileSize());
            Assert.assertEquals(context + ": partition[" + i + "].parquetFormat", ep.parquetFormat(), ap.parquetFormat());
            Assert.assertEquals(context + ": partition[" + i + "].readOnly", ep.readOnly(), ap.readOnly());
            Assert.assertEquals(context + ": partition[" + i + "].squashCount", ep.squashCount(), ap.squashCount());
        }

        for (int i = 0, n = actual.getIssues().size(); i < n; i++) {
            Assert.fail(context + ": unexpected issue: " + actual.getIssues().getQuick(i).message());
        }
    }

    private static TxnStateBuilder copyToBuilder(TxnState src) {
        TxnStateBuilder b = new TxnStateBuilder()
                .baseVersion(src.getBaseVersion())
                .txn(src.getTxn())
                .transientRowCount(src.getTransientRowCount())
                .fixedRowCount(src.getFixedRowCount())
                .minTimestamp(src.getMinTimestamp())
                .maxTimestamp(src.getMaxTimestamp())
                .structureVersion(src.getStructureVersion())
                .dataVersion(src.getDataVersion())
                .partitionTableVersion(src.getPartitionTableVersion())
                .columnVersion(src.getColumnVersion())
                .truncateVersion(src.getTruncateVersion())
                .seqTxn(src.getSeqTxn())
                .lagChecksum(src.getLagChecksum())
                .lagTxnCount(src.getLagTxnCount())
                .lagRowCount(src.getLagRowCount())
                .lagMinTimestamp(src.getLagMinTimestamp())
                .lagMaxTimestamp(src.getLagMaxTimestamp())
                .mapWriterCount(src.getMapWriterCount());
        for (int i = 0, n = src.getSymbols().size(); i < n; i++) {
            TxnSymbolState s = src.getSymbols().getQuick(i);
            b.symbols().add(new TxnSymbolState(s.index(), s.count(), s.transientCount()));
        }
        for (int i = 0, n = src.getPartitions().size(); i < n; i++) {
            TxnPartitionState p = src.getPartitions().getQuick(i);
            b.partitions().add(new TxnPartitionState(
                    p.index(), p.timestampLo(), p.rowCount(), p.nameTxn(),
                    p.parquetFileSize(), p.parquetFormat(), p.readOnly(), p.squashCount()
            ));
        }
        return b;
    }

    private static TxnState createMinimalState() {
        return minimalBuilder().build();
    }

    private static void createNonWalTableWithRows(String tableName, int rowCount) throws SqlException {
        execute("create table " + tableName + " (sym symbol, ts timestamp) timestamp(ts) partition by DAY");
        execute(
                "insert into "
                        + tableName
                        + " select rnd_symbol('AA', 'BB', 'CC'), timestamp_sequence('1970-01-01', "
                        + Micros.DAY_MICROS
                        + "L) from long_sequence("
                        + rowCount
                        + ")"
        );
    }

    private static void createTableWithTxnPartitions(String tableName, int partitionCount) throws SqlException {
        execute("create table " + tableName + " (sym symbol, ts timestamp) timestamp(ts) partition by DAY WAL");
        try (WalWriter walWriter = getWalWriter(tableName)) {
            long ts = 0;
            for (int i = 0; i < partitionCount; i++) {
                TableWriter.Row row = walWriter.newRow(ts);
                row.putSym(0, (i & 1) == 0 ? "AA" : "BB");
                row.append();
                ts += Micros.DAY_MICROS;
            }
            walWriter.commit();
        }
        waitForAppliedRows(tableName, partitionCount);
    }

    private static long fileLength(LPSZ path) {
        long fd = FF.openRO(path);
        Assert.assertTrue("cannot open file for length check", fd > -1);
        try {
            return FF.length(fd);
        } finally {
            FF.close(fd);
        }
    }

    private static long getRowCount(String tableName) throws SqlException {
        try (var factory = select("select count() from " + tableName)) {
            try (var cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                return cursor.getRecord().getLong(0);
            }
        }
    }

    private static TxnStateBuilder minimalBuilder() {
        return new TxnStateBuilder()
                .baseVersion(2)
                .txn(1)
                .transientRowCount(0)
                .fixedRowCount(0)
                .minTimestamp(Long.MAX_VALUE)
                .maxTimestamp(Long.MIN_VALUE)
                .structureVersion(0)
                .dataVersion(0)
                .partitionTableVersion(0)
                .columnVersion(0)
                .truncateVersion(0)
                .seqTxn(0)
                .lagTxnCount(0)
                .lagRowCount(0)
                .lagMinTimestamp(Long.MAX_VALUE)
                .lagMaxTimestamp(Long.MIN_VALUE)
                .mapWriterCount(0)
                .lagChecksum(TableUtils.calculateTxnLagChecksum(
                        1, 0, 0, Long.MAX_VALUE, Long.MIN_VALUE, 0
                ));
    }

    private static Path txnPathOf(String tableName, Path path) {
        TableToken tableToken = engine.verifyTableName(tableName);
        return path.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.TXN_FILE_NAME);
    }

    private static void waitForAppliedRows(String tableName, int expectedRows) throws SqlException {
        for (int i = 0; i < 20; i++) {
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            drainWalQueue(engine);
            if (getRowCount(tableName) == expectedRows) {
                return;
            }
        }
        Assert.assertEquals(expectedRows, getRowCount(tableName));
    }
}
