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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SymbolCountProvider;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Vect;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

public class TxnTest extends AbstractCairoTest {
    private static final Log LOG = LogFactory.getLog(TxnTest.class);

    @Test
    public void testFailedTxWriterDoesNotCorruptTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade errorFf = new TestFilesFacadeImpl() {
                @Override
                public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
                    return -1;
                }
            };

            FilesFacadeImpl cleanFf = new TestFilesFacadeImpl();
            assertMemoryLeak(() -> {
                String tableName = "txntest";
                TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
                model.timestamp();
                AbstractCairoTest.create(model);

                try (Path path = new Path()) {
                    TableToken tableToken = engine.verifyTableName(tableName);
                    path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                    int testPartitionCount = 3000;
                    try (TxWriter txWriter = new TxWriter(cleanFf, configuration).ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY)) {
                        // Add lots of partitions
                        for (int i = 0; i < testPartitionCount; i++) {
                            txWriter.updatePartitionSizeByTimestamp(i * Micros.DAY_MICROS, i + 1);
                        }
                        txWriter.updateMaxTimestamp(testPartitionCount * Micros.DAY_MICROS + 1);
                        txWriter.finishPartitionSizeUpdate();
                        txWriter.commit(new ObjList<>());
                    }

                    // Reopen without OS errors
                    try (TxWriter txWriter = new TxWriter(cleanFf, configuration).ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY)) {
                        // Read lots of partitions
                        Assert.assertEquals(testPartitionCount, txWriter.getPartitionCount());
                        for (int i = 0; i < testPartitionCount - 1; i++) {
                            Assert.assertEquals(i + 1, txWriter.getPartitionSize(i));
                        }
                    }

                    // Open with OS error to file extend
                    try (TxWriter ignored = new TxWriter(errorFf, configuration).ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY)) {
                        Assert.fail("Should not be able to extend on opening");
                    } catch (CairoException ex) {
                        // expected
                    }

                    // Reopen without OS errors
                    try (TxWriter txWriter = new TxWriter(cleanFf, configuration).ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY)) {
                        // Read lots of partitions
                        Assert.assertEquals(testPartitionCount, txWriter.getPartitionCount());
                        for (int i = 0; i < testPartitionCount - 1; i++) {
                            Assert.assertEquals(i + 1, txWriter.getPartitionSize(i));
                        }
                    }
                }
            });
        });
    }

    @Test
    public void testLoadAllFrom() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            assertMemoryLeak(() -> {
                String tableName = "txntest";
                TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
                model.timestamp();
                AbstractCairoTest.create(model);

                try (Path path = new Path()) {
                    TableToken tableToken = engine.verifyTableName(tableName);
                    path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                    int testPartitionCount = 2;
                    try (TxWriter txWriter = new TxWriter(ff, configuration)) {
                        txWriter.ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY);
                        for (int i = 0; i < testPartitionCount; i++) {
                            txWriter.updatePartitionSizeByTimestamp(i * Micros.DAY_MICROS, i + 1);
                        }
                        txWriter.updateMaxTimestamp(testPartitionCount * Micros.DAY_MICROS + 1);
                        txWriter.finishPartitionSizeUpdate();
                        txWriter.commit(new ObjList<>());
                    }

                    try (
                            TxReader txReader = new TxReader(ff);
                            MemoryCARW dumpMem = Vm.getCARWInstance(ff.getPageSize(), Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                            TxReader txCopyReader = new TxReader(ff);
                            MemoryCARW dumpCopyMem = Vm.getCARWInstance(ff.getPageSize(), Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)
                    ) {
                        txReader.ofRO(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY);

                        txReader.unsafeLoadAll();
                        final String expected = """
                                {txn: 1, attachedPartitions: [
                                {ts: '1970-01-01T00:00:00.000000Z', rowCount: 1, nameTxn: -1},
                                {ts: '1970-01-02T00:00:00.000000Z', rowCount: 2, nameTxn: -1}
                                ], transientRowCount: 2, fixedRowCount: 1, minTimestamp: '294247-01-10T04:00:54.775807Z', maxTimestamp: '1970-01-03T00:00:00.000001Z', dataVersion: 0, structureVersion: 0, partitionTableVersion: 0, columnVersion: 0, truncateVersion: 0, seqTxn: 0, symbolColumnCount: 0, lagRowCount: 0, lagMinTimestamp: '294247-01-10T04:00:54.775807Z', lagMaxTimestamp: '', lagTxnCount: 0, lagOrdered: true}""";
                        Assert.assertEquals(expected, txReader.toString());

                        txCopyReader.loadAllFrom(txReader);
                        Assert.assertEquals(expected, txCopyReader.toString());

                        Assert.assertTrue(txReader.getRecordSize() > 0);
                        Assert.assertEquals(txReader.getRecordSize(), txCopyReader.getRecordSize());

                        // Make sure to zero the memory before the dump to avoid garbage bytes in paddings.
                        dumpMem.jumpTo(txReader.getRecordSize());
                        dumpMem.zero();
                        dumpCopyMem.jumpTo(txCopyReader.getRecordSize());
                        dumpCopyMem.zero();

                        txReader.dumpTo(dumpMem);
                        txCopyReader.dumpTo(dumpCopyMem);
                        Assert.assertTrue(Vect.memeq(dumpMem.addressOf(0), dumpCopyMem.addressOf(0), txReader.getRecordSize()));
                    }
                }
            });
        });
    }

    @Test
    public void testLoadTxn() throws IOException {
        try (Path p = new Path()) {
            final String incrementalLoad;
            try (TxWriter tw = new TxWriter(engine.getConfiguration().getFilesFacade(), engine.getConfiguration())) {
                loadTxnWriter(tw, p, "/txn/sys.acl_entities~1/_txn");
                loadTxnWriter(tw, p, "/txn/sys.acl_passwords~5/_txn");
                incrementalLoad = tw.toString();
            }

            try (TxWriter tw = new TxWriter(engine.getConfiguration().getFilesFacade(), engine.getConfiguration())) {
                loadTxnWriter(tw, p, "/txn/sys.acl_passwords~5/_txn");
                TestUtils.assertEquals(incrementalLoad, tw.toString());
            }
        }
    }

    @Test
    public void testPartitionTopAndDonorRoundTrip() throws Exception {
        // Writes partition-top and DONOR-flag combinations through a full _txn commit, reloads
        // them in a fresh reader, and checks the packed slot-3 layout round-trips intact.
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            String tableName = "txntop";
            TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
            model.timestamp();
            AbstractCairoTest.create(model);

            try (Path path = new Path()) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                final int partitionCount = 4;

                try (TxWriter txWriter = new TxWriter(ff, configuration).ofRW(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY)) {
                    for (int i = 0; i < partitionCount; i++) {
                        txWriter.updatePartitionSizeByTimestamp(i * Micros.DAY_MICROS, (i + 1) * 100L);
                    }
                    txWriter.updateMaxTimestamp((partitionCount - 1) * Micros.DAY_MICROS + 1);
                    txWriter.finishPartitionSizeUpdate();

                    // partition 0: untouched -> sentinel {0, false}
                    // partition 1: suffix child -> partitionTop AND DONOR flag (packed long is negative)
                    txWriter.setPartitionTop(1 * Micros.DAY_MICROS, 37);
                    txWriter.setPartitionDonor(1 * Micros.DAY_MICROS);
                    // partition 2: prefix donor -> DONOR flag only, partitionTop stays 0
                    txWriter.setPartitionDonor(2 * Micros.DAY_MICROS);
                    // partition 3 (last): partitionTop only, no DONOR flag
                    txWriter.setPartitionTop(3 * Micros.DAY_MICROS, 5);

                    txWriter.commit(new ObjList<>());
                }

                try (TxReader txReader = new TxReader(ff)) {
                    txReader.ofRO(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
                    txReader.unsafeLoadAll();

                    Assert.assertEquals(partitionCount, txReader.getPartitionCount());

                    // partition 0: zero-migration sentinel
                    Assert.assertEquals(0, txReader.getPartitionTop(0));
                    Assert.assertFalse(txReader.isPartitionDonor(0));

                    // partition 1: donor + top both survive; mask-not-sign decode (a v > 0 ? v : 0
                    // decode would return 0 here because the packed long is negative)
                    Assert.assertEquals(37, txReader.getPartitionTop(1));
                    Assert.assertTrue(txReader.getPartitionTop(1) > 0);
                    Assert.assertTrue(txReader.isPartitionDonor(1));

                    // partition 2: donor-only
                    Assert.assertEquals(0, txReader.getPartitionTop(2));
                    Assert.assertTrue(txReader.isPartitionDonor(2));

                    // partition 3: top-only
                    Assert.assertEquals(5, txReader.getPartitionTop(3));
                    Assert.assertFalse(txReader.isPartitionDonor(3));

                    // by-timestamp resolution; an unknown timestamp resolves to 0
                    Assert.assertEquals(37, txReader.getPartitionTopByTimestamp(1 * Micros.DAY_MICROS));
                    Assert.assertEquals(0, txReader.getPartitionTopByTimestamp(999 * Micros.DAY_MICROS));
                }
            }
        });
    }

    @Test
    public void testSlot3FlagAndTopPreservation() throws Exception {
        // Exercises the read-modify-write setters: each must preserve the bits it does not own.
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            try (Path path = new Path()) {
                createDayTable("txnpreserve", 3, path);

                try (TxWriter txWriter = new TxWriter(ff, configuration).ofRW(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY)) {
                    final long ts = 0; // partition 0 (non-last)
                    final int idx = 0;

                    // setPartitionDonor preserves an existing partitionTop
                    txWriter.setPartitionTop(ts, 42);
                    Assert.assertEquals(42, txWriter.getPartitionTop(idx));
                    Assert.assertFalse(txWriter.isPartitionDonor(idx));
                    txWriter.setPartitionDonor(ts);
                    Assert.assertEquals(42, txWriter.getPartitionTop(idx));
                    Assert.assertTrue(txWriter.isPartitionDonor(idx));

                    // setPartitionTop preserves an existing DONOR flag
                    txWriter.setPartitionTop(ts, 7);
                    Assert.assertEquals(7, txWriter.getPartitionTop(idx));
                    Assert.assertTrue(txWriter.isPartitionDonor(idx));

                    // clearPartitionDonor preserves the partitionTop
                    txWriter.clearPartitionDonor(ts);
                    Assert.assertEquals(7, txWriter.getPartitionTop(idx));
                    Assert.assertFalse(txWriter.isPartitionDonor(idx));

                    // a donor-only partition has partitionTop 0
                    txWriter.setPartitionTop(ts, 0); // clears the top -> slot normalizes to -1L
                    Assert.assertEquals(0, txWriter.getPartitionTop(idx));
                    Assert.assertFalse(txWriter.isPartitionDonor(idx));
                    txWriter.setPartitionDonor(ts);
                    Assert.assertEquals(0, txWriter.getPartitionTop(idx));
                    Assert.assertTrue(txWriter.isPartitionDonor(idx));
                    txWriter.clearPartitionDonor(ts); // back to the sentinel
                    Assert.assertEquals(0, txWriter.getPartitionTop(idx));
                    Assert.assertFalse(txWriter.isPartitionDonor(idx));

                    // resetPartitionTop clears BOTH the top and the DONOR flag
                    txWriter.setPartitionTop(ts, 99);
                    txWriter.setPartitionDonor(ts);
                    Assert.assertEquals(99, txWriter.getPartitionTop(idx));
                    Assert.assertTrue(txWriter.isPartitionDonor(idx));
                    txWriter.resetPartitionTop(idx);
                    Assert.assertEquals(0, txWriter.getPartitionTop(idx));
                    Assert.assertFalse(txWriter.isPartitionDonor(idx));
                }
            }
        });
    }

    @Test
    public void testSlot3ParquetCollisionAsserts() throws Exception {
        boolean assertsEnabled = false;
        //noinspection AssertWithSideEffects,ConstantConditions
        assert assertsEnabled = true;
        Assume.assumeTrue("slot-3/parquet collision checks require -ea", assertsEnabled);

        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            try (Path path = new Path()) {
                createDayTable("txnpqcollision", 3, path);

                try (TxWriter txWriter = new TxWriter(ff, configuration).ofRW(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY)) {
                    // partition 0 -> parquet: the native packed slot-3 API must reject it
                    txWriter.setPartitionParquetFormat(0, 4096);
                    expectAssertionError(() -> txWriter.setPartitionTop(0, 10));
                    expectAssertionError(() -> txWriter.setPartitionDonor(0));
                    expectAssertionError(() -> txWriter.getPartitionTop(0));
                    expectAssertionError(() -> txWriter.isPartitionDonor(0));

                    // convert-to-parquet on a partition-top child must be rejected
                    txWriter.setPartitionTop(1 * Micros.DAY_MICROS, 5);
                    expectAssertionError(() -> txWriter.setPartitionParquetFormat(1 * Micros.DAY_MICROS, 4096));

                    // convert-to-parquet on a donor partition must be rejected
                    txWriter.setPartitionDonor(2 * Micros.DAY_MICROS);
                    expectAssertionError(() -> txWriter.setPartitionParquetFormat(2 * Micros.DAY_MICROS, 4096));
                }
            }
        });
    }

    @Test
    public void testSquashCounterOverflow() throws IOException {
        try (Path p = new Path()) {
            try (TxWriter tw = new TxWriter(engine.getConfiguration().getFilesFacade(), engine.getConfiguration())) {
                loadTxnWriter(tw, p, "/txn/sys.acl_entities~1/_txn");
                //noinspection StatementWithEmptyBody
                while (tw.incrementPartitionSquashCounter(0)) {
                }

                Assert.assertEquals(TxReader.PARTITION_SQUASH_COUNTER_MAX, tw.getPartitionSquashCount(0));

                Assert.assertFalse(tw.incrementPartitionSquashCounter(0));
                Assert.assertEquals(TxReader.PARTITION_SQUASH_COUNTER_MAX, tw.getPartitionSquashCount(0));
            }
        }
    }

    @Test
    public void testToString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            assertMemoryLeak(() -> {

                String tableName = "txntest";
                TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
                model.timestamp();
                AbstractCairoTest.create(model);

                try (Path path = new Path()) {
                    TableToken tableToken = engine.verifyTableName(tableName);
                    path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                    int testPartitionCount = 2;
                    try (TxWriter txWriter = new TxWriter(ff, configuration)) {
                        txWriter.ofRW(path.$(), TableUtils.getTimestampType(model), PartitionBy.DAY);
                        for (int i = 0; i < testPartitionCount; i++) {
                            txWriter.updatePartitionSizeByTimestamp(i * Micros.DAY_MICROS, i + 1);
                        }
                        TestUtils.assertContains(txWriter.toString(), """
                                [
                                {ts: '1970-01-01T00:00:00.000000Z', rowCount: 1, nameTxn: -1},
                                {ts: '1970-01-02T00:00:00.000000Z', rowCount: 2, nameTxn: -1}
                                ]""");
                    }
                }
            });
        });
    }

    @Test
    public void testTxReadTruncateConcurrent() throws Throwable {
        TestUtils.assertMemoryLeak(() -> {
            int readerThreads = 2;

            CyclicBarrier start = new CyclicBarrier(readerThreads + 1);
            AtomicInteger done = new AtomicInteger();
            AtomicInteger reloadCount = new AtomicInteger();
            int iterations = 1000;
            ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
            Rnd rnd = TestUtils.generateRandom(LOG);

            String tableName = "testTxReadWriteConcurrent";
            FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            int maxPartitionCount = Math.max((int) (Files.PAGE_SIZE / 8 / 4), 4096);
            int maxSymbolCount = (int) (Files.PAGE_SIZE / 8 / 4);
            AtomicInteger partitionCountCheck = new AtomicInteger();

            TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR);
            model.timestamp();
            AbstractCairoTest.create(model);
            int truncateIteration = 33;
            Thread writerThread = createWriterThread(
                    start,
                    done,
                    iterations,
                    exceptions,
                    rnd,
                    tableName,
                    ff,
                    maxPartitionCount,
                    maxSymbolCount,
                    partitionCountCheck,
                    truncateIteration,
                    TableUtils.getTimestampType(model)
            );

            Rnd readerRnd = new Rnd(rnd.nextLong(), rnd.nextLong());
            Thread[] readers = new Thread[readerThreads];
            for (int th = 0; th < readerThreads; th++) {
                Thread readerThread = new Thread(() -> {
                    try (
                            Path path = new Path();
                            TxReader txReader = new TxReader(ff)
                    ) {
                        TableToken tableToken = engine.verifyTableName(tableName);
                        path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                        txReader.ofRO(path.$(), TableUtils.getTimestampType(model), PartitionBy.HOUR);
                        MillisecondClock clock = engine.getConfiguration().getMillisecondClock();
                        long duration = 5_000;
                        start.await();
                        while (done.get() == 0 || partitionCountCheck.get() != txReader.getPartitionCount() - 1) {
                            TableUtils.safeReadTxn(txReader, clock, duration);
                            long txn = txReader.getTxn();

                            // Each writer iteration creates 2 txn commits.
                            // Every truncateIteration writer iteration truncates.
                            if (((txn - 1) / 2) % truncateIteration == 0) {
                                // must be truncated
                                if (txReader.getPartitionCount() > 1) {
                                    Assert.assertTrue(
                                            "Txn " + txn + " not read as truncated. Partition count: " + txReader.getPartitionCount(),
                                            txReader.getPartitionCount() < 2);
                                }
                            } else if (txReader.getPartitionCount() > 2) {
                                reloadCount.incrementAndGet();
                            }
                            if (readerRnd.nextBoolean()) {
                                txReader.ofRO(path.$(), TableUtils.getTimestampType(model), PartitionBy.HOUR);
                            }
                            Os.pause();
                        }

                    } catch (Throwable e) {
                        exceptions.add(e);
                        LOG.error().$(e).$();
                    }
                });
                readers[th] = readerThread;
                readerThread.start();
            }

            writerThread.start();

            writerThread.join();
            for (int th = 0; th < readerThreads; th++) {
                readers[th].join();
            }

            if (!exceptions.isEmpty()) {
                Assert.fail(exceptions.poll().toString());
            }
            Assert.assertTrue(reloadCount.get() > 10);
            LOG.infoW().$("total reload count ").$(reloadCount.get()).$();
        });
    }

    @Test
    public void testTxReadWriteConcurrent() throws Throwable {
        TestUtils.assertMemoryLeak(() -> {
            int readerThreads = 4;

            CyclicBarrier start = new CyclicBarrier(readerThreads + 1);
            AtomicInteger done = new AtomicInteger();
            AtomicInteger reloadCount = new AtomicInteger();
            int iterations = 1000;
            ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
            Rnd rnd = TestUtils.generateRandom(LOG);

            String tableName = "testTxReadWriteConcurrent";
            FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            int maxPartitionCount = Math.max((int) (Files.PAGE_SIZE / 8 / 4), 4096);
            int maxSymbolCount = (int) (Files.PAGE_SIZE / 8 / 4);
            AtomicInteger partitionCountCheck = new AtomicInteger();

            TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR);
            model.timestamp();
            AbstractCairoTest.create(model);
            Thread writerThread = createWriterThread(
                    start,
                    done,
                    iterations,
                    exceptions,
                    rnd,
                    tableName,
                    ff,
                    maxPartitionCount,
                    maxSymbolCount,
                    partitionCountCheck,
                    Integer.MAX_VALUE,
                    TableUtils.getTimestampType(model)
            );

            Rnd readerRnd = TestUtils.generateRandom(LOG);

            Thread[] readers = new Thread[readerThreads];
            for (int th = 0; th < readerThreads; th++) {
                Thread readerThread = new Thread(() -> {
                    try (
                            Path path = new Path();
                            TxReader txReader = new TxReader(ff)
                    ) {
                        TableToken tableToken = engine.verifyTableName(tableName);
                        path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                        txReader.ofRO(path.$(), TableUtils.getTimestampType(model), PartitionBy.HOUR);
                        MillisecondClock clock = engine.getConfiguration().getMillisecondClock();
                        long duration = 30_000;
                        start.await();
                        while (done.get() == 0) {
                            TableUtils.safeReadTxn(txReader, clock, duration);
                            reloadCount.incrementAndGet();
                            Assert.assertTrue(txReader.getPartitionCount() <= maxPartitionCount);
                            Assert.assertTrue(txReader.getSymbolColumnCount() <= maxSymbolCount);

                            for (int i = txReader.getSymbolColumnCount() - 1; i > -1; i--) {
                                if (i != txReader.getSymbolColumnCount()) {
                                    String trace = String.format(
                                            "[txn=%d, structureVersion=%d, partitionCount=%d, symbolCount=%d] ",
                                            txReader.getTxn(),
                                            txReader.getMetadataVersion(),
                                            txReader.getPartitionCount(),
                                            txReader.getSymbolColumnCount()
                                    );
                                    Assert.assertEquals(trace, i, txReader.getSymbolValueCount(i));
                                }
                            }

                            long offset = txReader.getTxn() - txReader.getMetadataVersion();
                            for (int i = txReader.getPartitionCount() - 2; i > -1; i--) {
                                if (offset + i != txReader.getPartitionSize(i)) {
                                    String trace = String.format(
                                            "[txn=%d, structureVersion=%d, partitionCount=%d, symbolCount=%d] ",
                                            txReader.getTxn(),
                                            txReader.getMetadataVersion(),
                                            txReader.getPartitionCount(),
                                            txReader.getSymbolColumnCount()
                                    );
                                    Assert.assertEquals(trace + buildActualSizes(txReader), offset + i, txReader.getPartitionSize(i));
                                }
                            }

                            if (readerRnd.nextBoolean()) {
                                // Reopen txn file
                                txReader.ofRO(path.$(), TableUtils.getTimestampType(model), PartitionBy.HOUR);
                            }
                        }
                        TableUtils.safeReadTxn(txReader, clock, duration);
                        Assert.assertEquals(partitionCountCheck.get(), txReader.getPartitionCount() - 1);

                    } catch (Throwable e) {
                        LOG.error().$(e).$();
                        exceptions.add(e);
                    }
                });
                readers[th] = readerThread;
                readerThread.start();
            }

            writerThread.start();

            writerThread.join();
            for (int th = 0; th < readerThreads; th++) {
                readers[th].join();
            }

            if (!exceptions.isEmpty()) {
                Assert.fail(exceptions.poll().toString());
            }
            Assert.assertTrue(reloadCount.get() > 10);
            LOG.infoW().$("total reload count ").$(reloadCount.get()).$();
        });
    }

    private static void expectAssertionError(Runnable runnable) {
        try {
            runnable.run();
            Assert.fail("expected AssertionError");
        } catch (AssertionError expected) {
            // expected
        }
    }

    private static void loadTxnWriter(TxWriter tw, Path p, String resourceFile) throws IOException {
        try (final InputStream is = TxnTest.class.getResourceAsStream(resourceFile)) {
            // Create temp file
            java.nio.file.Path tempFile = java.nio.file.Files.createTempFile("test-", ".tmp");
            tempFile.toFile().deleteOnExit(); // Ensure it is deleted on exit

            // Copy resource content to temp file
            java.nio.file.Files.copy(Objects.requireNonNull(is), tempFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

            p.of(tempFile.toString()).$();
            tw.ofRW(p.$(), ColumnType.TIMESTAMP, PartitionBy.MONTH);
            tw.unsafeLoadAll();
        }
    }

    private String buildActualSizes(TxReader txReader) {
        StringSink ss = new StringSink();
        for (int i = 0; i < txReader.getPartitionCount() - 1; i++) {
            if (i > 0) {
                ss.put(',');
            }
            ss.put(txReader.getPartitionSize(i));
        }
        return ss.toString();
    }

    private void createDayTable(String tableName, int partitionCount, Path path) {
        TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
        model.timestamp();
        AbstractCairoTest.create(model);
        TableToken tableToken = engine.verifyTableName(tableName);
        path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
        FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (TxWriter txWriter = new TxWriter(ff, configuration).ofRW(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY)) {
            for (int i = 0; i < partitionCount; i++) {
                txWriter.updatePartitionSizeByTimestamp(i * Micros.DAY_MICROS, (i + 1) * 100L);
            }
            txWriter.updateMaxTimestamp((partitionCount - 1) * Micros.DAY_MICROS + 1);
            txWriter.finishPartitionSizeUpdate();
            txWriter.commit(new ObjList<>());
        }
    }

    @NotNull
    private Thread createWriterThread(
            CyclicBarrier start,
            AtomicInteger done,
            int iterations,
            ConcurrentLinkedQueue<Throwable> exceptions,
            Rnd rnd,
            String tableName,
            FilesFacade ff,
            int maxPartitionCount,
            int maxSymbolCount,
            AtomicInteger partitionCountCheck,
            int truncateIteration,
            int timestampType
    ) {
        ObjList<SymbolCountProvider> symbolCounts = new ObjList<>();
        ObjList<SymbolCountProvider> zeroSymbolCounts = new ObjList<>();
        return new Thread(() -> {
            try (
                    Path path = new Path();
                    TxWriter txWriter = new TxWriter(ff, configuration)
            ) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                txWriter.ofRW(path.$(), timestampType, PartitionBy.HOUR);

                start.await();
                for (int j = 0; j < iterations; j++) {
                    if (j % truncateIteration == 0) {
                        txWriter.truncate(0, zeroSymbolCounts);
                        LOG.info().$("writer truncated at ").$(txWriter.getTxn()).$();
                        // Create last partition back.
                        txWriter.setMaxTimestamp((maxPartitionCount + 1) * Micros.HOUR_MICROS);
                        txWriter.updatePartitionSizeByTimestamp(txWriter.getMaxTimestamp() * Micros.HOUR_MICROS, 1);
                        txWriter.commit(symbolCounts);
                        partitionCountCheck.set(0);
                    } else {
                        // Set txn file with random number of symbols and random number of partitions
                        int symbolCount = rnd.nextInt(maxSymbolCount);
                        if (symbolCount > symbolCounts.size()) {
                            for (int i = symbolCounts.size(); i < symbolCount; i++) {
                                symbolCounts.add(new SymbolCountProviderImpl(i));
                                zeroSymbolCounts.add(new SymbolCountProviderImpl(0));
                            }
                        } else {
                            symbolCounts.setPos(symbolCount);
                            zeroSymbolCounts.setPos(symbolCount);
                        }
                        txWriter.bumpMetadataAndColumnStructureVersion(symbolCounts);

                        // Set random number of partitions
                        int partitionCount = rnd.nextInt(maxPartitionCount);
                        int partitions = txWriter.getPartitionCount() - 1; // Last partition always stays

                        long offset = txWriter.getTxn() + 1 - txWriter.getMetadataVersion();
                        // Add / Update
                        for (int i = 0; i < partitionCount; i++) {
                            txWriter.updatePartitionSizeByTimestamp(i * Micros.HOUR_MICROS, offset + i);
                        }
                        // Remove from the end
                        for (int i = partitionCount; i < partitions; i++) {
                            txWriter.removeAttachedPartitions(i * Micros.HOUR_MICROS);
                        }
                        txWriter.bumpPartitionTableVersion();
                        assert txWriter.getPartitionCount() - 1 == partitionCount;

                        txWriter.setMaxTimestamp(partitionCount * Micros.HOUR_MICROS);
                        txWriter.commit(symbolCounts);
                        partitionCountCheck.set(partitionCount);
                    }

                    if (rnd.nextBoolean()) {
                        // Reopen txn file for writing
                        txWriter.ofRW(path.$(), timestampType, PartitionBy.HOUR);
                    }

                    if (!exceptions.isEmpty()) {
                        break;
                    }
                }
            } catch (Throwable e) {
                exceptions.add(e);
                LOG.error().$(e).$();
            } finally {
                done.incrementAndGet();
            }
        });
    }

    static class SymbolCountProviderImpl implements SymbolCountProvider {
        private final int count;

        SymbolCountProviderImpl(int count) {
            this.count = count;
        }

        @Override
        public int getSymbolCount() {
            return count;
        }
    }

}
