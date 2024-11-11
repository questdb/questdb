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

package io.questdb.test.cairo;

import io.questdb.cairo.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

public class TxnTest extends AbstractCairoTest {
    protected final static Log LOG = LogFactory.getLog(TxnTest.class);

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
                    path.of(configuration.getRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                    int testPartitionCount = 3000;
                    try (TxWriter txWriter = new TxWriter(cleanFf, configuration).ofRW(path.$(), PartitionBy.DAY)) {
                        // Add lots of partitions
                        for (int i = 0; i < testPartitionCount; i++) {
                            txWriter.updatePartitionSizeByTimestamp(i * Timestamps.DAY_MICROS, i + 1);
                        }
                        txWriter.updateMaxTimestamp(testPartitionCount * Timestamps.DAY_MICROS + 1);
                        txWriter.finishPartitionSizeUpdate();
                        txWriter.commit(new ObjList<>());
                    }

                    // Reopen without OS errors
                    try (TxWriter txWriter = new TxWriter(cleanFf, configuration).ofRW(path.$(), PartitionBy.DAY)) {
                        // Read lots of partitions
                        Assert.assertEquals(testPartitionCount, txWriter.getPartitionCount());
                        for (int i = 0; i < testPartitionCount - 1; i++) {
                            Assert.assertEquals(i + 1, txWriter.getPartitionSize(i));
                        }
                    }

                    // Open with OS error to file extend
                    try (TxWriter ignored = new TxWriter(errorFf, configuration).ofRW(path.$(), PartitionBy.DAY)) {
                        Assert.fail("Should not be able to extend on opening");
                    } catch (CairoException ex) {
                        // expected
                    }

                    // Reopen without OS errors
                    try (TxWriter txWriter = new TxWriter(cleanFf, configuration).ofRW(path.$(), PartitionBy.DAY)) {
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
                    path.of(configuration.getRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                    int testPartitionCount = 2;
                    try (TxWriter txWriter = new TxWriter(ff, configuration).ofRW(path.$(), PartitionBy.DAY)) {
                        for (int i = 0; i < testPartitionCount; i++) {
                            txWriter.updatePartitionSizeByTimestamp(i * Timestamps.DAY_MICROS, i + 1);
                        }
                        TestUtils.assertContains(txWriter.toString(), "[\n" +
                                "{ts: '1970-01-01T00:00:00.000Z', rowCount: 1, nameTxn: -1},\n" +
                                "{ts: '1970-01-02T00:00:00.000Z', rowCount: 2, nameTxn: -1}\n" +
                                "]");
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
                    truncateIteration
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
                        path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                        txReader.ofRO(path.$(), PartitionBy.HOUR);
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
                                txReader.ofRO(path.$(), PartitionBy.HOUR);
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
                    Integer.MAX_VALUE
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
                        path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                        txReader.ofRO(path.$(), PartitionBy.HOUR);
                        MillisecondClock clock = engine.getConfiguration().getMillisecondClock();
                        long duration = 5_000;
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
                                txReader.ofRO(path.$(), PartitionBy.HOUR);
                            }
                        }
                        TableUtils.safeReadTxn(txReader, clock, duration);
                        Assert.assertEquals(partitionCountCheck.get(), txReader.getPartitionCount() - 1);

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
            int truncateIteration
    ) {
        ObjList<SymbolCountProvider> symbolCounts = new ObjList<>();
        ObjList<SymbolCountProvider> zeroSymbolCounts = new ObjList<>();
        return new Thread(() -> {
            try (
                    Path path = new Path();
                    TxWriter txWriter = new TxWriter(ff, configuration)
            ) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(engine.getConfiguration().getRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                txWriter.ofRW(path.$(), PartitionBy.HOUR);

                start.await();
                for (int j = 0; j < iterations; j++) {
                    if (j % truncateIteration == 0) {
                        txWriter.truncate(0, zeroSymbolCounts);
                        LOG.info().$("writer truncated at ").$(txWriter.getTxn()).$();
                        // Create last partition back.
                        txWriter.setMaxTimestamp((maxPartitionCount + 1) * Timestamps.HOUR_MICROS);
                        txWriter.updatePartitionSizeByTimestamp(txWriter.getMaxTimestamp() * Timestamps.HOUR_MICROS, 1);
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
                            txWriter.updatePartitionSizeByTimestamp(i * Timestamps.HOUR_MICROS, offset + i);
                        }
                        // Remove from the end
                        for (int i = partitionCount; i < partitions; i++) {
                            txWriter.removeAttachedPartitions(i * Timestamps.HOUR_MICROS);
                        }
                        txWriter.bumpPartitionTableVersion();
                        assert txWriter.getPartitionCount() - 1 == partitionCount;

                        txWriter.setMaxTimestamp(partitionCount * Timestamps.HOUR_MICROS);
                        txWriter.commit(symbolCounts);
                        partitionCountCheck.set(partitionCount);
                    }

                    if (rnd.nextBoolean()) {
                        // Reopen txn file for writing
                        txWriter.ofRW(path.$(), PartitionBy.HOUR);
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
