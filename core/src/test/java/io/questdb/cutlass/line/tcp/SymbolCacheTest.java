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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.SPSequence;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SymbolCacheTest extends AbstractGriffinTest {

    @Test
    public void testConcurrency() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rndCache = new Rnd();
            final int N = 1_000_000;
            long ts = TimestampFormatUtils.parseTimestamp("2020-09-10T20:00:00.000000Z");
            final long incrementUs = 10000;
            final String constValue = "hello";
            FilesFacade ff = new FilesFacadeImpl();

            compiler.compile("create table x(a symbol, c int, b symbol capacity 10000000, ts timestamp) timestamp(ts) partition by DAY", sqlExecutionContext);

            try (
                    SymbolCache symbolCache = new SymbolCache(new DefaultLineTcpReceiverConfiguration());
                    Path path = new Path();
                    TxReader txReader = new TxReader(ff).ofRO(path.of(configuration.getRoot()).concat("x"), PartitionBy.DAY)
            ) {
                path.of(configuration.getRoot()).concat("x");
                symbolCache.of(configuration, path, "b", 1, txReader, -1);

                final CyclicBarrier barrier = new CyclicBarrier(2);
                final SOCountDownLatch haltLatch = new SOCountDownLatch(1);
                final AtomicBoolean cacheInError = new AtomicBoolean(false);

                RingQueue<Holder> wheel = new RingQueue<>(Holder::new, 256);
                SPSequence pubSeq = new SPSequence(wheel.getCycle());
                SCSequence subSeq = new SCSequence();
                pubSeq.then(subSeq).then(pubSeq);

                new Thread(() -> {
                    try {
                        barrier.await();
                        for (int i = 0; i < N; i++) {
                            // All keys should not be found, but we keep looking them up because
                            // we pretend we don't know this upfront. The aim is to cause
                            // race condition between lookup and table writer
                            final CharSequence value2 = rndCache.nextString(5);
                            symbolCache.keyOf(constValue);
                            symbolCache.keyOf(value2);
                            final long cursor = pubSeq.nextBully();
                            final Holder h = wheel.get(cursor);
                            // publish the value2 to the table writer
                            h.value1 = constValue;
                            h.value2 = Chars.toString(value2);
                            pubSeq.done(cursor);
                        }
                    } catch (Throwable e) {
                        cacheInError.set(true);
                        e.printStackTrace();
                    } finally {
                        haltLatch.countDown();
                    }
                }).start();


                try (TableWriter w = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "test")) {
                    barrier.await();

                    OUT:
                    for (int i = 0; i < N; i++) {
                        long cursor;
                        while (true) {
                            cursor = subSeq.next();
                            if (cursor < 0) {
                                // we should exist main loop even if we did not receive N values
                                // the publishing thread could get successful cache lookups and not publish value
                                // due to random generator producing duplicate strings
                                if (haltLatch.getCount() < 1) {
                                    break OUT;
                                }
                            } else {
                                break;
                            }
                        }
                        Holder h = wheel.get(cursor);
                        TableWriter.Row r = w.newRow(ts);
                        r.putSym(0, h.value1);
                        r.putInt(1, 0);
                        r.putSym(2, h.value2);
                        r.append();
                        subSeq.done(cursor);

                        if (i % 256 == 0) {
                            w.commit();
                        }
                        ts += incrementUs;
                    }
                    w.commit();
                } finally {
                    haltLatch.await();
                }
                Assert.assertFalse(cacheInError.get());
            }
            compiler.compile("drop table x", sqlExecutionContext);
        });
    }

    @Test
    public void testSimpleInteraction() throws Exception {
        String tableName = "tb1";
        FilesFacade ff = new FilesFacadeImpl();
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path();
                 TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                         .col("symCol1", ColumnType.SYMBOL)
                         .col("symCol2", ColumnType.SYMBOL);
                 SymbolCache cache = new SymbolCache(new DefaultLineTcpReceiverConfiguration() {
                     @Override
                     public long getSymbolCacheWaitUsBeforeReload() {
                         return 0;
                     }
                 })
            ) {
                CairoTestUtils.create(model);
                try (
                        TableWriter writer = new TableWriter(configuration, tableName, metrics);
                        MemoryMR txMem = Vm.getMRInstance();
                        TxReader txReader = new TxReader(ff).ofRO(path.of(configuration.getRoot()).concat(tableName), PartitionBy.DAY)
                ) {
                    int symColIndex1 = writer.getColumnIndex("symCol1");
                    int symColIndex2 = writer.getColumnIndex("symCol2");
                    long transientSymCountOffset = TableUtils.getSymbolWriterTransientIndexOffset(symColIndex2);
                    path.of(configuration.getRoot()).concat(tableName);

                    txMem.of(
                            configuration.getFilesFacade(),
                            path.concat(TableUtils.TXN_FILE_NAME).$(),
                            transientSymCountOffset + Integer.BYTES,
                            transientSymCountOffset + Integer.BYTES,
                            MemoryTag.MMAP_DEFAULT
                    );

                    cache.of(
                            configuration,
                            path.of(configuration.getRoot()).concat(tableName),
                            "symCol2",
                            symColIndex2,
                            txReader,
                            -1
                    );

                    TableWriter.Row r = writer.newRow();
                    r.putSym(symColIndex1, "sym11");
                    r.putSym(symColIndex2, "sym21");
                    r.append();
                    writer.commit();
                    Assert.assertEquals(0, txReader.unsafeReadSymbolCount(1));
                    Assert.assertEquals(1, txReader.unsafeReadSymbolTransientCount(1));
                    txReader.unsafeLoadAll();
                    Assert.assertEquals(1, txReader.unsafeReadSymbolCount(1));
                    Assert.assertEquals(1, txReader.unsafeReadSymbolTransientCount(1));

                    int rc = cache.keyOf("missing");
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, rc);
                    Assert.assertEquals(0, cache.getCacheValueCount());
                    rc = cache.keyOf("sym21");
                    Assert.assertEquals(0, rc);
                    Assert.assertEquals(1, cache.getCacheValueCount());

                    r = writer.newRow();
                    r.putSym(symColIndex1, "sym12");
                    r.putSym(symColIndex2, "sym21");
                    r.append();
                    writer.commit();
                    Assert.assertEquals(1, txReader.unsafeReadSymbolCount(1));
                    Assert.assertEquals(1, txReader.unsafeReadSymbolTransientCount(1));
                    rc = cache.keyOf("missing");
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, rc);
                    Assert.assertEquals(1, cache.getCacheValueCount());
                    rc = cache.keyOf("sym21");
                    Assert.assertEquals(0, rc);
                    Assert.assertEquals(1, cache.getCacheValueCount());

                    r = writer.newRow();
                    r.putSym(symColIndex1, "sym12");
                    r.putSym(symColIndex2, "sym22");
                    r.append();
                    Assert.assertEquals(1, txReader.unsafeReadSymbolCount(1));
                    Assert.assertEquals(2, txReader.unsafeReadSymbolTransientCount(1));
                    writer.commit();
                    Assert.assertEquals(1, txReader.unsafeReadSymbolCount(1));
                    Assert.assertEquals(2, txReader.unsafeReadSymbolTransientCount(1));
                    rc = cache.keyOf("sym21");
                    Assert.assertEquals(0, rc);
                    Assert.assertEquals(1, cache.getCacheValueCount());
                    rc = cache.keyOf("sym22");
                    Assert.assertEquals(1, rc);
                    Assert.assertEquals(2, cache.getCacheValueCount());

                    txReader.unsafeLoadAll();
                    // Test cached uncommitted symbols
                    r = writer.newRow();
                    r.putSym(symColIndex1, "sym12");
                    r.putSym(symColIndex2, "sym23");
                    r.append();
                    r = writer.newRow();
                    r.putSym(symColIndex1, "sym12");
                    r.putSym(symColIndex2, "sym24");
                    r.append();
                    r = writer.newRow();
                    r.putSym(symColIndex1, "sym12");
                    r.putSym(symColIndex2, "sym25");
                    r.append();

                    rc = cache.keyOf("sym22");
                    Assert.assertEquals(1, rc);
                    Assert.assertEquals(2, cache.getCacheValueCount());
                    rc = cache.keyOf("sym24");
                    Assert.assertEquals(3, rc);
                    Assert.assertEquals(3, cache.getCacheValueCount());
                    writer.commit();

                    // Test deleting a symbol column
                    writer.removeColumn("symCol1");
                    cache.close();
                    txMem.close();
                    path.of(configuration.getRoot()).concat(tableName);

                    cache.of(
                            configuration,
                            path.of(configuration.getRoot()).concat(tableName),
                            "symCol2",
                            0,
                            txReader,
                            -1
                    );

                    rc = cache.keyOf("sym24");
                    Assert.assertEquals(3, rc);
                    Assert.assertEquals(1, cache.getCacheValueCount());

                    r = writer.newRow();
                    r.putSym(symColIndex2, "sym26");
                    r.append();
                    rc = cache.keyOf("sym26");
                    Assert.assertEquals(5, rc);
                    Assert.assertEquals(2, cache.getCacheValueCount());
                    writer.commit();
                    rc = cache.keyOf("sym26");
                    Assert.assertEquals(5, rc);
                    Assert.assertEquals(2, cache.getCacheValueCount());
                }
            }
        });
    }

    @Test
    public void testAddSymbolColumnConcurrent() throws Throwable {
        ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
        assertMemoryLeak(() -> {
            CyclicBarrier start = new CyclicBarrier(2);
            AtomicInteger done = new AtomicInteger();
            AtomicInteger columnsAdded = new AtomicInteger();
            AtomicInteger reloadCount = new AtomicInteger();
            int totalColAddCount = 10;
            int rowsAdded = 1000;

            String tableName = "tbl_symcache_test";
            createTable(tableName, PartitionBy.DAY);
            Rnd rnd = new Rnd();

            Thread writerThread = new Thread(() -> {
                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, tableName, "test")) {
                    start.await();
                    for (int i = 0; i < totalColAddCount; i++) {
                        writer.addColumn("col" + i, ColumnType.SYMBOL);
                        int colCount = writer.getMetadata().getColumnCount();
                        columnsAdded.incrementAndGet();

                        for (int rowNum = 0; rowNum < rowsAdded; rowNum++) {
                            TableWriter.Row row = writer.newRow((i * rowsAdded + rowNum) * Timestamps.SECOND_MICROS);
                            String value = "val" + (i * rowsAdded + rowNum);
                            for (int col = 1; col < colCount; col++) {
                                if (rnd.nextBoolean()) {
                                    row.putSym(col, value);
                                }
                            }
                            row.append();
                        }

                        writer.commit();
                    }
                } catch (Throwable e) {
                    exceptions.add(e);
                    LOG.error().$(e).$();
                } finally {
                    done.incrementAndGet();
                }
            });

            Thread readerThread = new Thread(() -> {
                ObjList<SymbolCache> symbolCacheObjList = new ObjList<>();

                try (Path path = new Path();
                     TxReader txReader = new TxReader(configuration.getFilesFacade()).ofRO(path.of(configuration.getRoot()).concat(tableName), PartitionBy.DAY);
                     TableReader rdr = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), tableName)) {
                    start.await();
                    int colAdded = 0, newColsAdded;
                    while (colAdded < totalColAddCount) {
                        newColsAdded = columnsAdded.get();
                        rdr.reload();
                        for (int col = colAdded; col < newColsAdded; col++) {
                            SymbolCache symbolCache = new SymbolCache(new DefaultLineTcpReceiverConfiguration());
                            int symbolIndexInTxFile = col;
                            symbolCache.of(
                                    engine.getConfiguration(),
                                    path,
                                    "col" + col,
                                    symbolIndexInTxFile,
                                    txReader,
                                    rdr.getColumnVersionReader().getDefaultColumnNameTxn(col + 1)
                            );
                            symbolCacheObjList.add(symbolCache);
                        }

                        int symCount = symbolCacheObjList.size();
                        String value = "val" + ((newColsAdded - 1) * rowsAdded);
                        boolean found = false;
                        for (int sym = 0; sym < symCount; sym++) {
                            if (symbolCacheObjList.getQuick(sym).keyOf(value) != SymbolTable.VALUE_NOT_FOUND) {
                                found = true;
                            }
                        }
                        colAdded = newColsAdded;
                        if (found) {
                            reloadCount.incrementAndGet();
                        }
                    }
                } catch (Throwable e) {
                    exceptions.add(e);
                    LOG.error().$(e).$();
                } finally {
                    Misc.freeObjList(symbolCacheObjList);
                }
            });
            writerThread.start();
            readerThread.start();

            writerThread.join();
            readerThread.join();

            if (exceptions.size() != 0) {
                for (Throwable ex : exceptions) {
                    ex.printStackTrace();
                }
                Assert.fail();
            }
            Assert.assertTrue(reloadCount.get() > 0);
            LOG.infoW().$("total reload count ").$(reloadCount.get()).$();
        });
    }

    private void createTable(String tableName, int partitionBy) {
        try (Path path = new Path()) {
            try (
                    MemoryMARW mem = Vm.getCMARWInstance();
                    TableModel model = new TableModel(configuration, tableName, partitionBy)
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
    }

    private static class Holder implements Mutable {
        String value1;
        String value2;

        @Override
        public void clear() {
            value1 = null;
            value2 = null;
        }
    }
}
