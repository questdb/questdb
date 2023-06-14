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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.cutlass.line.tcp.DefaultLineTcpReceiverConfiguration;
import io.questdb.cutlass.line.tcp.SymbolCache;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.SPSequence;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

public class SymbolCacheTest extends AbstractGriffinTest {

    private static final long DBCS_MAX_SIZE = 256;

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
            createTable(tableName);
            Rnd rnd = new Rnd();

            Thread writerThread = new Thread(() -> {
                try (TableWriter writer = getWriter(tableName)) {
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
                DirectByteCharSequence dbcs = new DirectByteCharSequence();
                long mem = Unsafe.malloc(DBCS_MAX_SIZE, MemoryTag.NATIVE_DEFAULT);
                TableToken tableToken = engine.verifyTableName(tableName);
                try (Path path = new Path();
                     TxReader txReader = new TxReader(configuration.getFilesFacade()).ofRO(
                             path.of(configuration.getRoot()).concat(tableToken).concat(TXN_FILE_NAME).$(),
                             PartitionBy.DAY
                     );
                     TableReader rdr = getReader(tableName)
                ) {
                    path.of(configuration.getRoot()).concat(tableToken);
                    start.await();
                    int colAdded = 0, newColsAdded;
                    while (colAdded < totalColAddCount) {
                        newColsAdded = columnsAdded.get();
                        rdr.reload();
                        for (int col = colAdded; col < newColsAdded; col++) {
                            SymbolCache symbolCache = new SymbolCache(new DefaultLineTcpReceiverConfiguration());
                            symbolCache.of(
                                    engine.getConfiguration(),
                                    new TestTableWriterAPI(),
                                    col,
                                    path,
                                    "col" + col,
                                    col,
                                    txReader,
                                    rdr.getColumnVersionReader().getDefaultColumnNameTxn(col + 1)
                            );
                            symbolCacheObjList.add(symbolCache);
                        }

                        int symCount = symbolCacheObjList.size();
                        copyUtf8StringChars("val" + ((newColsAdded - 1) * rowsAdded), mem, dbcs);
                        boolean found = false;
                        for (int sym = 0; sym < symCount; sym++) {
                            if (symbolCacheObjList.getQuick(sym).keyOf(dbcs) != SymbolTable.VALUE_NOT_FOUND) {
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
                    Unsafe.free(mem, DBCS_MAX_SIZE, MemoryTag.NATIVE_DEFAULT);
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

    @Test
    public void testCloseResetsCapacity() throws Exception {
        final int N = 1024;
        final String tableName = "tb1";
        final FilesFacade ff = new TestFilesFacadeImpl();

        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path();
                 TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                         .col("symCol", ColumnType.SYMBOL);
                 SymbolCache cache = new SymbolCache(new DefaultLineTcpReceiverConfiguration() {
                     @Override
                     public long getSymbolCacheWaitUsBeforeReload() {
                         return 0;
                     }
                 })
            ) {
                CreateTableTestUtils.create(model);
                DirectByteCharSequence dbcs = new DirectByteCharSequence();
                long mem = Unsafe.malloc(DBCS_MAX_SIZE, MemoryTag.NATIVE_DEFAULT);
                TableToken tableToken = engine.verifyTableName(tableName);
                try (
                        TableWriter writer = newTableWriter(configuration, tableName, metrics);
                        TxReader txReader = new TxReader(ff).ofRO(
                                path.of(configuration.getRoot()).concat(tableToken).concat(TXN_FILE_NAME).$(),
                                PartitionBy.DAY
                        )
                ) {
                    int symColIndex = writer.getColumnIndex("symCol");

                    cache.of(
                            configuration,
                            writer,
                            symColIndex,
                            path.of(configuration.getRoot()).concat(tableToken),
                            "symCol",
                            symColIndex,
                            txReader,
                            -1
                    );

                    final int initialCapacity = cache.getCacheCapacity();
                    Assert.assertTrue(N > initialCapacity);

                    for (int i = 0; i < N; i++) {
                        TableWriter.Row r = writer.newRow();
                        r.putSym(symColIndex, "sym" + i);
                        r.append();
                    }
                    writer.commit();

                    for (int i = 0; i < N; i++) {
                        copyUtf8StringChars("sym" + i, mem, dbcs);
                        int rc = cache.keyOf(dbcs);
                        Assert.assertNotEquals(SymbolTable.VALUE_NOT_FOUND, rc);
                    }

                    Assert.assertEquals(N, cache.getCacheValueCount());
                    Assert.assertTrue(cache.getCacheCapacity() >= N);

                    cache.close();

                    // Close should shrink cache back to initial capacity.
                    Assert.assertEquals(0, cache.getCacheValueCount());
                    Assert.assertEquals(initialCapacity, cache.getCacheCapacity());
                } finally {
                    Unsafe.free(mem, DBCS_MAX_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testConcurrency() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rndCache = new Rnd();
            final int N = 500_000;
            long ts = TimestampFormatUtils.parseTimestamp("2020-09-10T20:00:00.000000Z");
            final long incrementUs = 10_000;
            final String constValue = "hello";
            long constMem = Unsafe.malloc(DBCS_MAX_SIZE, MemoryTag.NATIVE_DEFAULT);
            DirectByteCharSequence constDbcs = new DirectByteCharSequence();
            copyUtf8StringChars(constValue, constMem, constDbcs);
            FilesFacade ff = new TestFilesFacadeImpl();

            compiler.compile("create table x(a symbol, c int, b symbol capacity 10000000, ts timestamp) timestamp(ts) partition by DAY", sqlExecutionContext);
            TableToken tableToken = engine.verifyTableName("x");
            try (
                    SymbolCache symbolCache = new SymbolCache(new DefaultLineTcpReceiverConfiguration());
                    Path path = new Path();
                    TxReader txReader = new TxReader(ff).ofRO(
                            path.of(configuration.getRoot()).concat(tableToken).concat(TXN_FILE_NAME).$(),
                            PartitionBy.DAY
                    )
            ) {
                path.of(configuration.getRoot()).concat(tableToken);
                symbolCache.of(configuration, new TestTableWriterAPI(), 1, path, "b", 1, txReader, -1);

                final CyclicBarrier barrier = new CyclicBarrier(2);
                final SOCountDownLatch haltLatch = new SOCountDownLatch(1);
                final AtomicBoolean cacheInError = new AtomicBoolean(false);

                try (RingQueue<Holder> wheel = new RingQueue<>(Holder::new, 256)) {
                    SPSequence pubSeq = new SPSequence(wheel.getCycle());
                    SCSequence subSeq = new SCSequence();
                    pubSeq.then(subSeq).then(pubSeq);

                    new Thread(() -> {
                        long mem = Unsafe.malloc(DBCS_MAX_SIZE, MemoryTag.NATIVE_DEFAULT);
                        DirectByteCharSequence dbcs = new DirectByteCharSequence();
                        try {
                            barrier.await();
                            for (int i = 0; i < N; i++) {
                                // All keys should not be found, but we keep looking them up because
                                // we pretend we don't know this upfront. The aim is to cause
                                // race condition between lookup and table writer
                                copyUtf8StringChars(rndCache.nextString(5), mem, dbcs);
                                symbolCache.keyOf(constDbcs);
                                symbolCache.keyOf(dbcs);
                                final long cursor = pubSeq.nextBully();
                                final Holder h = wheel.get(cursor);
                                // publish the value2 to the table writer
                                h.value1 = constValue;
                                h.value2 = Chars.toString(dbcs);
                                pubSeq.done(cursor);
                            }
                        } catch (Throwable e) {
                            cacheInError.set(true);
                            e.printStackTrace();
                        } finally {
                            Unsafe.free(mem, DBCS_MAX_SIZE, MemoryTag.NATIVE_DEFAULT);
                            haltLatch.countDown();
                        }
                    }).start();

                    try (TableWriter w = getWriter("x")) {
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
            } finally {
                Unsafe.free(constMem, DBCS_MAX_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
            compiler.compile("drop table x", sqlExecutionContext);
        });
    }

    @Test
    public void testNonAsciiChars() throws Exception {
        final int N = 1024;
        final String tableName = "tb1";
        final String symbolPrefix = "аз_съм_грут";
        final FilesFacade ff = new FilesFacadeImpl();

        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path();
                 TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                         .col("symCol", ColumnType.SYMBOL);
                 SymbolCache cache = new SymbolCache(new DefaultLineTcpReceiverConfiguration() {
                     @Override
                     public long getSymbolCacheWaitUsBeforeReload() {
                         return 0;
                     }
                 })
            ) {
                CreateTableTestUtils.create(model);
                DirectByteCharSequence dbcs = new DirectByteCharSequence();
                long mem = Unsafe.malloc(DBCS_MAX_SIZE, MemoryTag.NATIVE_DEFAULT);
                TableToken tableToken = engine.verifyTableName(tableName);
                try (
                        TableWriter writer = new TableWriter(configuration, tableToken, metrics);
                        TxReader txReader = new TxReader(ff).ofRO(
                                path.of(configuration.getRoot()).concat(tableToken).concat(TXN_FILE_NAME).$(),
                                PartitionBy.DAY
                        )
                ) {
                    int symColIndex = writer.getColumnIndex("symCol");

                    cache.of(
                            configuration,
                            writer,
                            symColIndex,
                            path.of(configuration.getRoot()).concat(tableToken),
                            "symCol",
                            symColIndex,
                            txReader,
                            -1
                    );

                    final int initialCapacity = cache.getCacheCapacity();
                    Assert.assertTrue(N > initialCapacity);

                    for (int i = 0; i < N; i++) {
                        TableWriter.Row r = writer.newRow();
                        r.putSym(symColIndex, symbolPrefix + i);
                        r.append();
                    }
                    writer.commit();

                    for (int i = 0; i < N; i++) {
                        copyUtf8StringChars(symbolPrefix + i, mem, dbcs);
                        int rc = cache.keyOf(dbcs);
                        Assert.assertNotEquals(SymbolTable.VALUE_NOT_FOUND, rc);
                    }

                    Assert.assertEquals(N, cache.getCacheValueCount());
                } finally {
                    Unsafe.free(mem, DBCS_MAX_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testSimpleInteraction() throws Exception {
        String tableName = "tb1";
        FilesFacade ff = new TestFilesFacadeImpl();
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
                CreateTableTestUtils.create(model);
                long mem = Unsafe.malloc(DBCS_MAX_SIZE, MemoryTag.NATIVE_DEFAULT);
                DirectByteCharSequence dbcs = new DirectByteCharSequence();
                TableToken tableToken = engine.verifyTableName(tableName);
                try (
                        TableWriter writer = newTableWriter(configuration, tableName, metrics);
                        MemoryMR txMem = Vm.getMRInstance();
                        TxReader txReader = new TxReader(ff).ofRO(
                                path.of(configuration.getRoot()).concat(tableToken).concat(TXN_FILE_NAME).$(),
                                PartitionBy.DAY
                        )
                ) {
                    int symColIndex1 = writer.getColumnIndex("symCol1");
                    int symColIndex2 = writer.getColumnIndex("symCol2");
                    long transientSymCountOffset = TableUtils.getSymbolWriterTransientIndexOffset(symColIndex2);

                    txMem.of(
                            configuration.getFilesFacade(),
                            path,
                            transientSymCountOffset + Integer.BYTES,
                            transientSymCountOffset + Integer.BYTES,
                            MemoryTag.MMAP_DEFAULT
                    );

                    cache.of(
                            configuration,
                            new TestTableWriterAPI(),
                            symColIndex2,
                            path.of(configuration.getRoot()).concat(tableToken),
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

                    int rc = cache.keyOf(copyUtf8StringChars("missing", mem, dbcs));
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, rc);
                    Assert.assertEquals(0, cache.getCacheValueCount());
                    rc = cache.keyOf(copyUtf8StringChars("sym21", mem, dbcs));
                    Assert.assertEquals(0, rc);
                    Assert.assertEquals(1, cache.getCacheValueCount());

                    r = writer.newRow();
                    r.putSym(symColIndex1, "sym12");
                    r.putSym(symColIndex2, "sym21");
                    r.append();
                    writer.commit();
                    Assert.assertEquals(1, txReader.unsafeReadSymbolCount(1));
                    Assert.assertEquals(1, txReader.unsafeReadSymbolTransientCount(1));
                    rc = cache.keyOf(copyUtf8StringChars("missing", mem, dbcs));
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, rc);
                    Assert.assertEquals(1, cache.getCacheValueCount());
                    rc = cache.keyOf(copyUtf8StringChars("sym21", mem, dbcs));
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
                    rc = cache.keyOf(copyUtf8StringChars("sym21", mem, dbcs));
                    Assert.assertEquals(0, rc);
                    Assert.assertEquals(1, cache.getCacheValueCount());
                    rc = cache.keyOf(copyUtf8StringChars("sym22", mem, dbcs));
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

                    rc = cache.keyOf(copyUtf8StringChars("sym22", mem, dbcs));
                    Assert.assertEquals(1, rc);
                    Assert.assertEquals(2, cache.getCacheValueCount());
                    rc = cache.keyOf(copyUtf8StringChars("sym24", mem, dbcs));
                    Assert.assertEquals(3, rc);
                    Assert.assertEquals(3, cache.getCacheValueCount());
                    writer.commit();

                    // Test deleting a symbol column
                    writer.removeColumn("symCol1");
                    cache.close();
                    txMem.close();
                    path.of(configuration.getRoot()).concat(tableToken);

                    cache.of(
                            configuration,
                            new TestTableWriterAPI(),
                            0,
                            path,
                            "symCol2",
                            0,
                            txReader,
                            -1
                    );

                    rc = cache.keyOf(copyUtf8StringChars("sym24", mem, dbcs));
                    Assert.assertEquals(3, rc);
                    Assert.assertEquals(1, cache.getCacheValueCount());

                    r = writer.newRow();
                    r.putSym(symColIndex2, "sym26");
                    r.append();
                    rc = cache.keyOf(copyUtf8StringChars("sym26", mem, dbcs));
                    Assert.assertEquals(5, rc);
                    Assert.assertEquals(2, cache.getCacheValueCount());
                    writer.commit();
                    rc = cache.keyOf(copyUtf8StringChars("sym26", mem, dbcs));
                    Assert.assertEquals(5, rc);
                    Assert.assertEquals(2, cache.getCacheValueCount());
                } finally {
                    Unsafe.free(mem, DBCS_MAX_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testSymbolCountNonZeroWatermark() throws Exception {
        String tableName = "tb1";
        FilesFacade ff = new TestFilesFacadeImpl();
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path();
                 TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                         .col("symCol", ColumnType.SYMBOL);
                 SymbolCache cache = new SymbolCache(new DefaultLineTcpReceiverConfiguration() {
                     @Override
                     public long getSymbolCacheWaitUsBeforeReload() {
                         return 0;
                     }
                 })
            ) {
                CreateTableTestUtils.create(model);
                long mem = Unsafe.malloc(DBCS_MAX_SIZE, MemoryTag.NATIVE_DEFAULT);
                DirectByteCharSequence dbcs = new DirectByteCharSequence();
                TableToken tableToken = engine.verifyTableName(tableName);
                try (
                        TableWriter writer = newTableWriter(configuration, tableName, metrics);
                        MemoryMR txMem = Vm.getMRInstance();
                        TxReader txReader = new TxReader(ff).ofRO(
                                path.of(configuration.getRoot()).concat(tableToken).concat(TXN_FILE_NAME).$(),
                                PartitionBy.DAY
                        )
                ) {
                    int symColIndex = writer.getColumnIndex("symCol");
                    long transientSymCountOffset = TableUtils.getSymbolWriterTransientIndexOffset(symColIndex);

                    txMem.of(
                            configuration.getFilesFacade(),
                            path,
                            transientSymCountOffset + Integer.BYTES,
                            transientSymCountOffset + Integer.BYTES,
                            MemoryTag.MMAP_DEFAULT
                    );

                    TableWriter.Row r = writer.newRow();
                    r.putSym(symColIndex, "sym1");
                    r.append();
                    writer.commit();
                    txReader.unsafeLoadAll();
                    Assert.assertEquals(1, txReader.unsafeReadSymbolCount(0));
                    Assert.assertEquals(1, txReader.unsafeReadSymbolTransientCount(0));

                    cache.of(
                            configuration,
                            new TestTableWriterAPI(1),
                            symColIndex,
                            path.of(configuration.getRoot()).concat(tableToken),
                            "symCol",
                            symColIndex,
                            txReader,
                            -1
                    );

                    int rc = cache.keyOf(copyUtf8StringChars("missing", mem, dbcs));
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, rc);
                    Assert.assertEquals(0, cache.getCacheValueCount());
                    rc = cache.keyOf(copyUtf8StringChars("sym1", mem, dbcs));
                    Assert.assertEquals(0, rc);
                    Assert.assertEquals(1, cache.getCacheValueCount());

                    r = writer.newRow();
                    r.putSym(symColIndex, "sym2");
                    r.append();
                    writer.commit();
                    Assert.assertEquals(1, txReader.unsafeReadSymbolCount(0));
                    Assert.assertEquals(2, txReader.unsafeReadSymbolTransientCount(0));
                    rc = cache.keyOf(copyUtf8StringChars("missing", mem, dbcs));
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, rc);
                    Assert.assertEquals(1, cache.getCacheValueCount());
                    rc = cache.keyOf(copyUtf8StringChars("sym2", mem, dbcs));
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, rc);
                    Assert.assertEquals(1, cache.getCacheValueCount());
                } finally {
                    Unsafe.free(mem, DBCS_MAX_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testSymbolCountZeroWatermark() throws Exception {
        String tableName = "tb1";
        FilesFacade ff = new TestFilesFacadeImpl();
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path();
                 TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                         .col("symCol", ColumnType.SYMBOL);
                 SymbolCache cache = new SymbolCache(new DefaultLineTcpReceiverConfiguration() {
                     @Override
                     public long getSymbolCacheWaitUsBeforeReload() {
                         return 0;
                     }
                 })
            ) {
                CreateTableTestUtils.create(model);
                long mem = Unsafe.malloc(DBCS_MAX_SIZE, MemoryTag.NATIVE_DEFAULT);
                DirectByteCharSequence dbcs = new DirectByteCharSequence();
                TableToken tableToken = engine.verifyTableName(tableName);
                try (
                        TableWriter writer = newTableWriter(configuration, tableName, metrics);
                        MemoryMR txMem = Vm.getMRInstance();
                        TxReader txReader = new TxReader(ff).ofRO(
                                path.of(configuration.getRoot()).concat(tableToken).concat(TXN_FILE_NAME).$(),
                                PartitionBy.DAY
                        )
                ) {
                    int symColIndex = writer.getColumnIndex("symCol");
                    long transientSymCountOffset = TableUtils.getSymbolWriterTransientIndexOffset(symColIndex);

                    txMem.of(
                            configuration.getFilesFacade(),
                            path,
                            transientSymCountOffset + Integer.BYTES,
                            transientSymCountOffset + Integer.BYTES,
                            MemoryTag.MMAP_DEFAULT
                    );

                    TableWriter.Row r = writer.newRow();
                    r.putSym(symColIndex, "sym1");
                    r.append();
                    writer.commit();
                    txReader.unsafeLoadAll();
                    Assert.assertEquals(1, txReader.unsafeReadSymbolCount(0));
                    Assert.assertEquals(1, txReader.unsafeReadSymbolTransientCount(0));

                    cache.of(
                            configuration,
                            new TestTableWriterAPI(0),
                            symColIndex,
                            path.of(configuration.getRoot()).concat(tableToken),
                            "symCol",
                            symColIndex,
                            txReader,
                            -1
                    );

                    int rc = cache.keyOf(copyUtf8StringChars("sym1", mem, dbcs));
                    Assert.assertEquals(SymbolTable.VALUE_NOT_FOUND, rc);
                    Assert.assertEquals(0, cache.getCacheValueCount());
                } finally {
                    Unsafe.free(mem, DBCS_MAX_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    private static DirectByteCharSequence copyUtf8StringChars(String value, long mem, DirectByteCharSequence dbcs) {
        byte[] utf8Bytes = value.getBytes(StandardCharsets.UTF_8);
        Assert.assertTrue(utf8Bytes.length <= DBCS_MAX_SIZE);
        for (int i = 0, n = utf8Bytes.length; i < n; i++) {
            Unsafe.getUnsafe().putByte(mem + i, utf8Bytes[i]);
        }
        return dbcs.of(mem, mem + utf8Bytes.length);
    }

    private void createTable(String tableName) {
        try (
                TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
        ) {
            model.timestamp();
            TestUtils.create(model, engine);
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

    private static class TestTableWriterAPI implements TableWriterAPI {

        private final int watermark;

        public TestTableWriterAPI() {
            this(-1);
        }

        public TestTableWriterAPI(int watermark) {
            this.watermark = watermark;
        }

        @Override
        public void addColumn(@NotNull CharSequence columnName, int columnType) {
        }

        @Override
        public void addColumn(CharSequence columnName, int columnType, int symbolCapacity, boolean symbolCacheFlag, boolean isIndexed, int indexValueBlockCapacity) {
        }

        @Override
        public long apply(AlterOperation alterOp, boolean contextAllowsAnyStructureChanges) throws AlterTableContextException {
            return 0;
        }

        @Override
        public long apply(UpdateOperation operation) {
            return 0;
        }

        @Override
        public void close() {
        }

        @Override
        public long commit() {
            return 0;
        }

        @Override
        public TableRecordMetadata getMetadata() {
            return null;
        }

        @Override
        public long getMetadataVersion() {
            return 0;
        }

        @Override
        public int getSymbolCountWatermark(int columnIndex) {
            return watermark;
        }

        @Override
        public TableToken getTableToken() {
            return null;
        }

        @Override
        public long getUncommittedRowCount() {
            return 0;
        }

        @Override
        public void ic() {
        }

        @Override
        public void ic(long o3MaxLag) {
        }

        @Override
        public TableWriter.Row newRow() {
            return null;
        }

        @Override
        public TableWriter.Row newRow(long timestamp) {
            return null;
        }

        @Override
        public void rollback() {
        }

        @Override
        public boolean supportsMultipleWriters() {
            return true;
        }

        @Override
        public void truncate() {
        }

        @Override
        public void truncateSoft() {
        }
    }
}
