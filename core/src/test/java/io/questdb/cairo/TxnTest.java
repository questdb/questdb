/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cairo;

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class TxnTest extends AbstractCairoTest {
    protected final static Log LOG = LogFactory.getLog(TxnTest.class);

    @Test
    public void testFailedTxWriterDoesNotCorruptTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade errorFf = new FilesFacadeImpl() {
                @Override
                public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
                    return -1;
                }
            };

            FilesFacadeImpl cleanFf = new FilesFacadeImpl();
            assertMemoryLeak(() -> {

                String tableName = "txntest";
                try (Path path = new Path()) {
                    try (
                            MemoryMARW mem = Vm.getCMARWInstance();
                            TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                    ) {
                        model.timestamp();
                        TableUtils.createTable(
                                configuration,
                                mem,
                                path,
                                model,
                                1
                        );
                    }
                }

                try (Path path = new Path()) {
                    path.of(configuration.getRoot()).concat(tableName);
                    int testPartitionCount = 3000;
                    try (TxWriter txWriter = new TxWriter(cleanFf).ofRW(path, PartitionBy.DAY)) {
                        // Add lots of partitions
                        for (int i = 0; i < testPartitionCount; i++) {
                            txWriter.updatePartitionSizeByTimestamp(i * Timestamps.DAY_MICROS, i + 1);
                        }
                        txWriter.updateMaxTimestamp(testPartitionCount * Timestamps.DAY_MICROS + 1);
                        txWriter.finishPartitionSizeUpdate();
                        txWriter.commit(CommitMode.SYNC, new ObjList<>());
                    }

                    // Reopen without OS errors
                    try (TxWriter txWriter = new TxWriter(cleanFf).ofRW(path, PartitionBy.DAY)) {
                        // Read lots of partitions
                        Assert.assertEquals(testPartitionCount, txWriter.getPartitionCount());
                        for (int i = 0; i < testPartitionCount - 1; i++) {
                            Assert.assertEquals(i + 1, txWriter.getPartitionSize(i));
                        }
                    }

                    // Open with OS error to file extend
                    try (TxWriter ignored = new TxWriter(errorFf).ofRW(path, PartitionBy.DAY)) {
                        Assert.fail("Should not be able to extend on opening");
                    } catch (CairoException ex) {
                        // expected
                    }

                    // Reopen without OS errors
                    try (TxWriter txWriter = new TxWriter(cleanFf).ofRW(path, PartitionBy.DAY)) {
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
    public void testTxReadWriteConcurrent() throws Throwable {
        TestUtils.assertMemoryLeak(() -> {
            CyclicBarrier start = new CyclicBarrier(2);
            AtomicInteger done = new AtomicInteger();
            AtomicInteger reloadCount = new AtomicInteger();
            int iterations = 1000;
            ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
            Rnd rnd = TestUtils.generateRandom(LOG);

            String tableName = "testTxReadWriteConcurrent";
            FilesFacade ff = FilesFacadeImpl.INSTANCE;
            int maxPartitionCount = Math.max((int) (Files.PAGE_SIZE / 8 / 4), 4096);
            int maxSymbolCount = (int) (Files.PAGE_SIZE / 8 / 4);
            ObjList<SymbolCountProvider> symbolCounts = new ObjList<>();
            AtomicInteger partitionCountCheck = new AtomicInteger();

            try (Path path = new Path()) {
                try (
                        MemoryMARW mem = Vm.getCMARWInstance();
                        TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                ) {
                    model.timestamp();
                    TableUtils.createTable(
                            configuration,
                            mem,
                            path,
                            model,
                            1
                    );
                }
            }
            Thread writerThread = new Thread(() -> {
                try (
                        Path path = new Path();
                        TxWriter writer = new TxWriter(ff)
                ) {
                    path.of(engine.getConfiguration().getRoot()).concat(tableName);
                    writer.ofRW(path, PartitionBy.HOUR);
                    start.await();
                    int close = 0;
                    for (int j = 0; j < iterations; j++) {

                        int symbolCount = rnd.nextInt(maxSymbolCount);
                        if (symbolCount > symbolCounts.size()) {
                            for (int i = symbolCounts.size(); i < symbolCount; i++) {
                                symbolCounts.add(new SymbolCountProviderImpl(i));
                            }
                        } else {
                            symbolCounts.setPos(symbolCount);
                        }
                        writer.bumpStructureVersion(symbolCounts);

                        int partitionCount = rnd.nextInt(maxPartitionCount);
                        int partitions = writer.getPartitionCount();
                        if (partitionCount > partitions) {
                            // Add to the back
                            for (int i = partitions; i < partitionCount; i++) {
                                writer.updatePartitionSizeByTimestamp(i * Timestamps.HOUR_MICROS, i);
                            }
                        } else {
                            // Remove from the back
                            for (int i = partitionCount; i < partitions; i++) {
                                writer.removeAttachedPartitions(i * Timestamps.HOUR_MICROS);
                            }
                        }
                        assert writer.getPartitionCount() == partitionCount;
                        if (writer.getMaxTimestamp() < partitionCount * Timestamps.HOUR_MICROS) {
                            writer.updateMaxTimestamp(partitionCount * Timestamps.HOUR_MICROS);
                        }

                        writer.commit(CommitMode.NOSYNC, symbolCounts);
                        partitionCountCheck.set(partitionCount);
                        if (rnd.nextBoolean()) {
                            // Reopen txn file for writing
                            writer.ofRW(path, PartitionBy.HOUR);
                        }
                    }
                } catch (Throwable e) {
                    exceptions.add(e);
                    LOG.error().$(e).$();
                } finally {
                    done.incrementAndGet();
                }
            });

            Rnd readerRnd = TestUtils.generateRandom(LOG);
            Thread readerThread = new Thread(() -> {
                try (
                        Path path = new Path();
                        TxReader txReader = new TxReader(ff)
                ) {
                    path.of(engine.getConfiguration().getRoot()).concat(tableName);
                    txReader.ofRO(path, PartitionBy.HOUR);
                    MicrosecondClock microClock = engine.getConfiguration().getMicrosecondClock();
                    long duration = 500000;
                    start.await();
                    while (done.get() == 0 || partitionCountCheck.get() != txReader.getPartitionCount()) {
                        if (done.get() == 1) {
                            int i = 0;
                        }
                        TableUtils.safeReadTxn(txReader, microClock, duration);
                        reloadCount.incrementAndGet();
                        Assert.assertTrue(txReader.getPartitionCount() <= maxPartitionCount);
                        for (int i = txReader.getPartitionCount() - 2; i > -1; i--) {
                            Assert.assertEquals(i, txReader.getPartitionSize(i));
                        }
                        Assert.assertTrue(txReader.getSymbolColumnCount() <= maxSymbolCount);
                        for (int i = txReader.getSymbolColumnCount() - 1; i > -1; i--) {
                            Assert.assertEquals(i, txReader.getSymbolValueCount(i));
                        }
                        LOG.info()
                                .$("txn reloaded [symbols=").$(txReader.getSymbolColumnCount())
                                .$(", partitions=").$(txReader.getPartitionCount())
                                .I$();
                        if (readerRnd.nextBoolean()) {
                            // Reopen txn file
                            txReader.ofRO(path, PartitionBy.HOUR);
                        }
                    }

                } catch (Throwable e) {
                    exceptions.add(e);
                    LOG.error().$(e).$();
                }
            });

            writerThread.start();
            readerThread.start();

            writerThread.join();
            readerThread.join();

            if (exceptions.size() != 0) {
                Assert.fail(exceptions.poll().toString());
            }
            Assert.assertTrue(reloadCount.get() > 10);
            LOG.infoW().$("total reload count ").$(reloadCount.get()).$();
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
