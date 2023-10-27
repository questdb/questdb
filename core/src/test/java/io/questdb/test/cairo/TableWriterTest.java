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

package io.questdb.test.cairo;

import io.questdb.Metrics;
import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.microtime.TimestampFormatCompiler;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class TableWriterTest extends AbstractCairoTest {

    public static final String PRODUCT = "product";
    private static final FilesFacade FF = TestFilesFacadeImpl.INSTANCE;
    private static final Log LOG = LogFactory.getLog(TableWriterTest.class);
    public static String PRODUCT_FS;

    @Before
    public void setUp() {
        super.setUp();
        PRODUCT_FS = PRODUCT;
        if (configuration.mangleTableDirNames()) {
            PRODUCT_FS += TableUtils.SYSTEM_TABLE_NAME_SUFFIX;
        }
    }

    @Test
    public void tesFrequentCommit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 100000;
            create(FF, PartitionBy.NONE, N);
            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {

                long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

                Rnd rnd = new Rnd();
                for (int i = 0; i < N; i++) {
                    ts = populateRow(writer, rnd, ts, 60L * 60000L * 1000L);
                    writer.commit();
                }
            }
        });
    }

    @Test
    public void testAddColumnAndOpenWriterByDay() throws Exception {
        testAddColumnAndOpenWriter(PartitionBy.DAY, 1000);
    }

    @Test
    public void testAddColumnAndOpenWriterByMonth() throws Exception {
        testAddColumnAndOpenWriter(PartitionBy.MONTH, 1000);
    }

    @Test
    public void testAddColumnAndOpenWriterByWeek() throws Exception {
        testAddColumnAndOpenWriter(PartitionBy.WEEK, 1000);
    }

    @Test
    public void testAddColumnAndOpenWriterByYear() throws Exception {
        testAddColumnAndOpenWriter(PartitionBy.YEAR, 1000);
    }

    @Test
    public void testAddColumnAndOpenWriterNonPartitioned() throws Exception {
        testAddColumnAndOpenWriter(PartitionBy.NONE, 100000);
    }

    @Test
    public void testAddColumnCannotRemoveMeta() throws Exception {
        String abcColumnNamePattern = Files.SEPARATOR + "abc.d";
        class X extends TestFilesFacadeImpl {
            @Override
            public int openRW(LPSZ name, long opts) {
                if (Utf8s.containsAscii(name, abcColumnNamePattern)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }

            @Override
            public boolean remove(LPSZ name) {
                return !Utf8s.endsWithAscii(name, TableUtils.META_FILE_NAME) && super.remove(name);
            }

            @Override
            public int rename(LPSZ name1, LPSZ name2) {
                if (Utf8s.endsWithAscii(name1, TableUtils.META_FILE_NAME)
                        && !Utf8s.containsAscii(name2, ".prev")) {
                    return -1;
                }
                return super.rename(name1, name2);
            }
        }
        testUnrecoverableAddColumn(new X());
    }

    @Test
    public void testAddColumnCannotRenameMeta() throws Exception {
        testAddColumnRecoverableFault(new MetaRenameDenyingFacade());
    }

    @Test
    public void testAddColumnCannotRenameMetaSwap() throws Exception {
        testAddColumnRecoverableFault(new SwapMetaRenameDenyingFacade());
    }

    @Test
    public void testAddColumnCannotRenameMetaSwapAndUseIndexedPrevMeta() throws Exception {
        FilesFacade ff = new SwapMetaRenameDenyingFacade() {
            int count = 5;

            @Override
            public int rename(LPSZ from, LPSZ to) {
                return (!Utf8s.containsAscii(to, TableUtils.META_PREV_FILE_NAME) || --count <= 0)
                        && super.rename(from, to) == Files.FILES_RENAME_OK ? Files.FILES_RENAME_OK
                        : Files.FILES_RENAME_ERR_OTHER;
            }
        };
        testAddColumnRecoverableFault(ff);
    }

    @Test
    public void testAddColumnCannotTouchSymbolMapFile() throws Exception {
        String abcColumnNamePattern = Files.SEPARATOR + "abc.d";
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean touch(LPSZ path) {
                return !Utf8s.containsAscii(path, abcColumnNamePattern) && super.touch(path);
            }
        };
        testAddColumnRecoverableNoFault(ff);
    }

    @Test
    public void testAddColumnCommitPartitioned() throws Exception {
        int count = 10000;
        create(FF, PartitionBy.DAY, count);
        Rnd rnd = new Rnd();
        long interval = 60000L * 1000L;
        long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
        try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
            ts = populateProducts(writer, rnd, ts, count, interval);
            Assert.assertEquals(count, writer.size());
            writer.addColumn("abc", ColumnType.STRING);
            // add more data including updating new column
            ts = populateTable2(writer, rnd, count, ts, interval);
            Assert.assertEquals(2 * count, writer.size());
            writer.rollback();
        }

        // append more
        try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
            populateTable2(writer, rnd, count, ts, interval);
            writer.commit();
            Assert.assertEquals(2 * count, writer.size());
        }
    }

    @Test
    public void testAddColumnConcurrentWithDataUpdates() throws Throwable {
        ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
        assertMemoryLeak(() -> {
            CyclicBarrier barrier = new CyclicBarrier(2);
            AtomicInteger columnsAdded = new AtomicInteger();
            AtomicInteger insertCount = new AtomicInteger();
            int totalColAddCount = 1000;
            writerCommandQueueCapacity = Numbers.ceilPow2(2 * totalColAddCount);
            int tableId = 11;
            // Reduce disk space by for the test run.
            dataAppendPageSize = 1 << 20; // 1MB

            TableToken tableToken;
            try (Path path = new Path()) {
                try (
                        MemoryMARW mem = Vm.getCMARWInstance();
                        TableModel model = new TableModel(configuration, "testAddColumnConcurrentWithDataUpdates", PartitionBy.NONE)
                ) {
                    model.timestamp();
                    tableToken = registerTableName(model.getTableName());
                    TableUtils.createTable(
                            configuration,
                            mem,
                            path,
                            model,
                            tableId,
                            tableToken.getDirName()
                    );
                }
            }

            // Write data in a loop getting writer in and out of pool
            Thread writeDataThread = new Thread(() -> {
                TestUtils.await(barrier);
                int i = 0;
                while (columnsAdded.get() < totalColAddCount && exceptions.isEmpty()) {
                    try (TableWriter writer = getWriter(tableToken)) {
                        TableWriter.Row row = writer.newRow((i++) * Timestamps.HOUR_MICROS);
                        row.append();
                        writer.commit();
                        insertCount.incrementAndGet();
                    } catch (EntryUnavailableException ex) {
                        // continue
                    } catch (Throwable e) {
                        exceptions.add(e);
                        LOG.error().$(e).$();
                        throw e;
                    }
                }
            });

            Thread addColumnsThread = new Thread(() -> {
                try {
                    TestUtils.await(barrier);
                    AlterOperationBuilder alterOperationBuilder = new AlterOperationBuilder();
                    for (int i = 0; i < totalColAddCount; i++) {
                        alterOperationBuilder.clear();
                        String columnName = "col" + i;
                        alterOperationBuilder
                                .ofAddColumn(0, tableToken, tableId)
                                .ofAddColumn(columnName, 5, ColumnType.INT, 0, false, false, 0);
                        AlterOperation alterOp = alterOperationBuilder.build();
                        try (TableWriter writer = engine.getWriterOrPublishCommand(tableToken, alterOp)) {
                            if (writer != null) {
                                writer.publishAsyncWriterCommand(alterOp);
                            }
                        }
                        columnsAdded.incrementAndGet();
                    }
                } catch (Throwable e) {
                    exceptions.add(e);
                    LOG.error().$(e).$();
                    throw e;
                }
            });
            writeDataThread.start();
            addColumnsThread.start();

            writeDataThread.join();
            addColumnsThread.join();

            if (!exceptions.isEmpty()) {
                for (Throwable ex : exceptions) {
                    ex.printStackTrace();
                }
                Assert.fail();
            }
            Assert.assertTrue(insertCount.get() > 0);

            try (TableReader rdr = getReader(tableToken)) {
                Assert.assertEquals(totalColAddCount + 1, rdr.getColumnCount());
            }

            LOG.infoW().$("total reload count ").$(insertCount.get()).$();
        });
    }

    @Test
    public void testAddColumnDuplicate() throws Exception {
        long ts = populateTable(FF, PartitionBy.MONTH);
        try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
            try {
                writer.addColumn("supplier", ColumnType.BOOLEAN);
                Assert.fail();
            } catch (CairoException ignore) {
            }
            populateProducts(writer, new Rnd(), ts, 10000, 6000 * 1000L);
            writer.commit();
            Assert.assertEquals(20000, writer.size());
        }
    }

    @Test
    public void testAddColumnFailToRemoveSymbolMapFiles() throws Exception {
        String abcColumnNamePatternK = Files.SEPARATOR + "abc.k";
        testAddColumnRecoverableNoFault(new TestFilesFacadeImpl() {

            @Override
            public boolean exists(LPSZ path) {
                return Utf8s.containsAscii(path, abcColumnNamePatternK) || super.exists(path);
            }

            @Override
            public boolean remove(LPSZ name) {
                return !Utf8s.containsAscii(name, abcColumnNamePatternK) && super.remove(name);
            }
        });
    }

    @Test
    public void testAddColumnFileOpenFail() throws Exception {
        String abcColumnNamePattern = Files.SEPARATOR + "abc.d";
        testAddColumnRecoverableFault(new TestFilesFacadeImpl() {
            @Override
            public int openRW(LPSZ name, long opts) {
                if (Utf8s.containsAscii(name, abcColumnNamePattern)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        });
    }

    @Test
    public void testAddColumnFileOpenFail2() throws Exception {
        String abcColumnNamePattern = Files.SEPARATOR + "abc.k";
        testAddColumnRecoverableFault(new TestFilesFacadeImpl() {
            @Override
            public int openRW(LPSZ name, long opts) {
                if (Utf8s.containsAscii(name, abcColumnNamePattern)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        });
    }

    @Test
    public void testAddColumnFileOpenFail3() throws Exception {
        String abcColumnNamePattern = Files.SEPARATOR + "abc.d";
        testUnrecoverableAddColumn(new TestFilesFacadeImpl() {
            int count = 1;

            @Override
            public int openRW(LPSZ name, long opts) {
                if (Utf8s.containsAscii(name, abcColumnNamePattern)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }

            @Override
            public int rename(LPSZ from, LPSZ to) {
                return !(Utf8s.endsWithAscii(from, TableUtils.META_PREV_FILE_NAME) && --count == 0)
                        && super.rename(from, to) == Files.FILES_RENAME_OK ? Files.FILES_RENAME_OK : Files.FILES_RENAME_ERR_OTHER;
            }
        });
    }

    @Test
    public void testAddColumnFileOpenFail4() throws Exception {
        String abcColumnNamePattern = Files.SEPARATOR + "abc.d";
        testAddColumnRecoverableFault(new TestFilesFacadeImpl() {
            @Override
            public int openRW(LPSZ name, long opts) {
                if (Utf8s.containsAscii(name, abcColumnNamePattern)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        });
    }

    @Test
    public void testAddColumnFileOpenFailAndIndexedPrev() throws Exception {
        String abcColumnNamePattern = Files.SEPARATOR + "abc.d";
        testUnrecoverableAddColumn(new TestFilesFacadeImpl() {
            int count = 2;
            int toCount = 5;

            @Override
            public int openRW(LPSZ name, long opts) {
                if (Utf8s.containsAscii(name, abcColumnNamePattern)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }

            @Override
            public int rename(LPSZ from, LPSZ to) {
                return (!Utf8s.containsAscii(from, TableUtils.META_PREV_FILE_NAME) || --count <= 0)
                        && (!Utf8s.containsAscii(to, TableUtils.META_PREV_FILE_NAME) || --toCount <= 0)
                        && super.rename(from, to) == Files.FILES_RENAME_OK ? Files.FILES_RENAME_OK : Files.FILES_RENAME_ERR_OTHER;
            }
        });
    }

    @Test
    public void testAddColumnHavingTroubleCreatingMetaSwap() throws Exception {
        int N = 10000;
        create(FF, PartitionBy.DAY, N);
        FilesFacade ff = new TestFilesFacadeImpl() {
            int count = 5;

            @Override
            public boolean exists(LPSZ path) {
                return Utf8s.containsAscii(path, TableUtils.META_SWAP_FILE_NAME) || super.exists(path);
            }

            @Override
            public boolean remove(LPSZ name) {
                if (Utf8s.containsAscii(name, TableUtils.META_SWAP_FILE_NAME)) {
                    return --count < 0;
                }
                return super.remove(name);
            }
        };

        try (TableWriter writer = newTableWriter(new DefaultTestCairoConfiguration(root) {
            @Override
            public @NotNull FilesFacade getFilesFacade() {
                return ff;
            }
        }, PRODUCT, metrics)) {
            writer.addColumn("xyz", ColumnType.STRING);
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();
            populateProducts(writer, rnd, ts, N, 6 * 60000 * 1000L);
            writer.commit();
            Assert.assertEquals(N, writer.size());
        }
    }

    @Test
    public void testAddColumnMetaOpenFail() throws Exception {
        testUnrecoverableAddColumn(new TestFilesFacadeImpl() {
            int counter = 2;

            @Override
            public int openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, TableUtils.META_FILE_NAME) && --counter == 0) {
                    return -1;
                }
                return super.openRO(name);
            }
        });
    }

    @Test
    public void testAddColumnNonPartitioned() throws Exception {
        int N = 100000;
        create(FF, PartitionBy.NONE, N);
        try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
            writer.addColumn("xyz", ColumnType.STRING);
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();
            populateProducts(writer, rnd, ts, N, 60L * 60000L * 1000L);
            writer.commit();
            Assert.assertEquals(N, writer.size());
        }
    }

    @Test
    public void testAddColumnPartitioned() throws Exception {
        int N = 10000;
        create(FF, PartitionBy.DAY, N);
        try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
            writer.addColumn("xyz", ColumnType.STRING);
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();
            populateProducts(writer, rnd, ts, N, 60000 * 1000L);
            writer.commit();
            Assert.assertEquals(N, writer.size());
        }
    }

    @Test
    public void testAddColumnRepairFail() throws Exception {
        class X extends FilesFacadeImpl {
            int counter = 2;

            @Override
            public int openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, TableUtils.META_FILE_NAME) && --counter == 0) {
                    return -1;
                }
                return super.openRO(name);
            }

            @Override
            public boolean remove(LPSZ name) {
                return !Utf8s.endsWithAscii(name, TableUtils.META_FILE_NAME) && super.remove(name);
            }
        }
        testAddColumnErrorFollowedByRepairFail(new X());
    }

    @Test
    public void testAddColumnRepairFail2() throws Exception {
        class X extends FilesFacadeImpl {
            int counter = 2;

            @Override
            public int openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, TableUtils.META_FILE_NAME) && --counter == 0) {
                    return -1;
                }
                return super.openRO(name);
            }

            @Override
            public int rename(LPSZ from, LPSZ to) {
                return !Utf8s.endsWithAscii(from, TableUtils.META_PREV_FILE_NAME)
                        && super.rename(from, to) == Files.FILES_RENAME_OK ? Files.FILES_RENAME_OK : Files.FILES_RENAME_ERR_OTHER;
            }
        }
        testAddColumnErrorFollowedByRepairFail(new X());
    }

    @Test
    public void testAddColumnSwpFileDelete() throws Exception {

        TestUtils.assertMemoryLeak(() -> {
            populateTable();
            // simulate existence of _meta.swp

            class X extends FilesFacadeImpl {
                boolean deleteAttempted = false;

                @Override
                public boolean exists(LPSZ path) {
                    return Utf8s.endsWithAscii(path, TableUtils.META_SWAP_FILE_NAME) || super.exists(path);
                }

                @Override
                public boolean remove(LPSZ name) {
                    if (Utf8s.endsWithAscii(name, TableUtils.META_SWAP_FILE_NAME)) {
                        return deleteAttempted = true;
                    }
                    return super.remove(name);
                }
            }

            X ff = new X();

            try (TableWriter writer = newTableWriter(new DefaultTestCairoConfiguration(root) {
                @Override
                public @NotNull FilesFacade getFilesFacade() {
                    return ff;
                }
            }, PRODUCT, metrics)) {
                Assert.assertEquals(20, writer.getColumnCount());
                writer.addColumn("abc", ColumnType.STRING);
                Assert.assertEquals(22, writer.getColumnCount());
                Assert.assertTrue(ff.deleteAttempted);
            }
        });
    }

    @Test
    public void testAddColumnSwpFileDeleteFail() throws Exception {
        // simulate existence of _meta.swp
        testAddColumnRecoverableFault(new TestFilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                return Utf8s.containsAscii(path, TableUtils.META_SWAP_FILE_NAME) || super.exists(path);
            }

            @Override
            public boolean remove(LPSZ name) {
                return !Utf8s.containsAscii(name, TableUtils.META_SWAP_FILE_NAME) && super.remove(name);
            }
        });
    }

    @Test
    public void testAddColumnToNonEmptyNonPartitioned() throws Exception {
        int n = 10000;
        create(FF, PartitionBy.NONE, n);
        populateAndColumnPopulate(n);
    }

    @Test
    public void testAddColumnToNonEmptyPartitioned() throws Exception {
        int n = 10000;
        create(FF, PartitionBy.DAY, n);
        populateAndColumnPopulate(n);
    }

    @Test
    public void testAddIndexAndFailOnceByDay() throws Exception {

        final FilesFacade ff = new TestFilesFacadeImpl() {
            int count = 5;

            @Override
            public int openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, "supplier.d") && count-- == 0) {
                    return -1;
                }
                return super.openRO(name);
            }
        };

        final CairoConfiguration configuration = new DefaultTestCairoConfiguration(AbstractCairoTest.configuration.getRoot()) {
            @Override
            public @NotNull FilesFacade getFilesFacade() {
                return ff;
            }
        };

        testAddIndexAndFailToIndexHalfWay(configuration, PartitionBy.DAY, 1000);
    }

    @Test
    public void testAddIndexAndFailOnceByNone() throws Exception {

        final FilesFacade ff = new TestFilesFacadeImpl() {
            int count = 1;

            @Override
            public boolean touch(LPSZ path) {
                if (Utf8s.endsWithAscii(path, "supplier.v") && --count == 0) {
                    return false;
                }
                return super.touch(path);
            }
        };

        final CairoConfiguration configuration = new DefaultTestCairoConfiguration(AbstractCairoTest.configuration.getRoot()) {
            @Override
            public @NotNull FilesFacade getFilesFacade() {
                return ff;
            }
        };

        testAddIndexAndFailToIndexHalfWay(configuration, PartitionBy.NONE, 500);
    }

    @Test
    public void testAddUnsupportedIndex() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                    .col("a", ColumnType.SYMBOL).cached(true)
                    .col("b", ColumnType.STRING)
                    .timestamp()) {
                CreateTableTestUtils.create(model);
            }

            final int N = 1000;
            try (TableWriter w = newTableWriter(configuration, "x", metrics)) {
                final Rnd rnd = new Rnd();
                for (int i = 0; i < N; i++) {
                    TableWriter.Row r = w.newRow();
                    r.putSym(0, rnd.nextChars(3));
                    r.putStr(1, rnd.nextChars(10));
                    r.append();
                }
                w.commit();

                try {
                    w.addColumn("c", ColumnType.STRING, 0, false, true, 1024, false);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "only supported");
                }

                for (int i = 0; i < N; i++) {
                    TableWriter.Row r = w.newRow();
                    r.putSym(0, rnd.nextChars(3));
                    r.putStr(1, rnd.nextChars(10));
                    r.append();
                }
                w.commit();

                // re-add column  with index flag switched off
                w.addColumn("c", ColumnType.STRING, 0, false, false, 0, false);
            }
        });
    }

    @Test
    public void testAddUnsupportedIndexCapacity() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                    .col("a", ColumnType.SYMBOL).cached(true)
                    .col("b", ColumnType.STRING)
                    .timestamp()) {
                CreateTableTestUtils.create(model);
            }

            final int N = 1000;
            try (TableWriter w = newTableWriter(configuration, "x", metrics)) {
                final Rnd rnd = new Rnd();
                for (int i = 0; i < N; i++) {
                    TableWriter.Row r = w.newRow();
                    r.putSym(0, rnd.nextChars(3));
                    r.putStr(1, rnd.nextChars(10));
                    r.append();
                }
                w.commit();

                try {
                    w.addColumn("c", ColumnType.SYMBOL, 0, false, true, 0, false);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "Invalid index value block capacity");
                }

                for (int i = 0; i < N; i++) {
                    TableWriter.Row r = w.newRow();
                    r.putSym(0, rnd.nextChars(3));
                    r.putStr(1, rnd.nextChars(10));
                    r.append();
                }
                w.commit();

                // re-add column  with index flag switched off
                w.addColumn("c", ColumnType.STRING, 0, false, false, 0, false);
            }
        });
    }

    @Test
    public void testAppendO3() throws Exception {
        int N = 10000;
        create(FF, PartitionBy.NONE, N);
        testO3RecordsFail(N);
    }

    @Test
    public void testAppendO3PartitionedNewerFirst() throws Exception {
        int N = 10000;
        create(FF, PartitionBy.DAY, N);
        testO3RecordsNewerThanOlder(N, configuration);
    }

    @Test
    public void testAutoCancelFirstRowNonPartitioned() throws Exception {
        int N = 10000;
        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.NONE, N);
            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {

                long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
                TableWriter.Row r = writer.newRow(ts);
                r.putInt(0, 1234);
                populateProducts(writer, new Rnd(), ts, N, 60 * 60000 * 1000L);
                Assert.assertEquals(N, writer.size());
            }
        });
    }

    @Test
    public void testCachedSymbol() {
        testSymbolCacheFlag(true);
    }

    @Test
    public void testCancelFailureFollowedByTableClose() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int N = 47;
            create(FF, PartitionBy.DAY, N);
            Rnd rnd = new Rnd();
            class X extends FilesFacadeImpl {
                @Override
                public boolean rmdir(Path name, boolean lazy) {
                    return false;
                }
            }

            X ff = new X();

            try (TableWriter writer = newTableWriter(new DefaultTestCairoConfiguration(root) {
                @Override
                public @NotNull FilesFacade getFilesFacade() {
                    return ff;
                }
            }, PRODUCT, metrics)) {
                long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
                ts = populateProducts(writer, rnd, ts, N, 60 * 60000 * 1000L);
                writer.commit();
                Assert.assertEquals(N, writer.size());

                TableWriter.Row r = writer.newRow(ts + 60 * 60000 * 1000L);
                r.putInt(0, rnd.nextInt());
                try {
                    r.cancel();
                    Assert.fail();
                } catch (CairoException ignore) {
                }
            }

            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                Assert.assertEquals(N, writer.size());
            }
        });
    }

    @Test
    public void testCancelFirstRowFailurePartitioned() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            class X extends FilesFacadeImpl {
                boolean fail = false;

                @Override
                public boolean allocate(int fd, long size) {
                    return !fail && super.allocate(fd, size);
                }
            }

            X ff = new X();
            Rnd rnd = new Rnd();
            int N = 94;
            create(ff, PartitionBy.DAY, N);
            long increment = 60 * 60000 * 1000L;
            try (TableWriter writer = newTableWriter(new DefaultTestCairoConfiguration(root) {
                @Override
                public @NotNull FilesFacade getFilesFacade() {
                    return ff;
                }
            }, PRODUCT, metrics)) {
                long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
                // add 48 hours
                ts = populateProducts(writer, rnd, ts, N / 2, increment);
                TableWriter.Row r = writer.newRow(ts + increment);
                r.putInt(0, rnd.nextPositiveInt());
                r.putStr(1, rnd.nextString(7));
                r.putSym(2, rnd.nextString(4));
                r.putSym(3, rnd.nextString(11));
                r.putDouble(4, rnd.nextDouble());

                ff.fail = true;
                try {
                    r.cancel();
                    Assert.fail();
                } catch (CairoException | CairoError ignore) {
                }
                // writer must be closed, we must not interact with writer anymore

                // test that we cannot commit
                try {
                    writer.commit();
                    Assert.fail();
                } catch (CairoError e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "distressed");
                }

                // test that we cannot rollback
                try {
                    writer.rollback();
                    Assert.fail();
                } catch (CairoError e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "distressed");
                }
            }
        });
    }

    @Test
    public void testCancelFirstRowNonPartitioned() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10000;
            create(FF, PartitionBy.NONE, N);
            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

                TableWriter.Row r = writer.newRow(ts);
                r.putInt(0, 1234);
                r.cancel();

                populateProducts(writer, new Rnd(), ts, N, 60 * 60000 * 1000L);
                Assert.assertEquals(N, writer.size());
            }
        });
    }

    @Test
    public void testCancelFirstRowPartitioned() throws Exception {
        ff = new TestFilesFacadeImpl() {

            @Override
            public int openRW(LPSZ name, long opts) {
                if (Utf8s.containsAscii(name, "2013-03-04") && Utf8s.endsWithAscii(name, "category.k")) {
                    return this.fd = super.openRW(name, opts);
                }
                return super.openRW(name, opts);
            }

            @Override
            public boolean rmdir(Path name, boolean lazy) {
                if (this.fd != -1) {
                    // Access denied, file is open
                    return false;
                }
                return super.rmdir(name, lazy);
            }
        };

        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.DAY, 4);
            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
                TableWriter.Row r = writer.newRow(ts);
                r.cancel();
                writer.commit();
                Assert.assertEquals(0, writer.size());
                Assert.assertEquals(2, getDirCount());
            }
        });
    }

    @Test
    public void testCancelFirstRowPartitioned2() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long increment = 60 * 60000 * 1000L;
            Rnd rnd = new Rnd();
            int N = 94;
            create(FF, PartitionBy.DAY, N);
            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
                // add 48 hours
                ts = populateProducts(writer, rnd, ts, N / 2, increment);

                TableWriter.Row r = writer.newRow(ts += increment);
                r.putInt(0, rnd.nextPositiveInt());
                r.putStr(1, rnd.nextString(7));
                r.putSym(2, rnd.nextString(4));
                r.putSym(3, rnd.nextString(11));
                r.putDouble(4, rnd.nextDouble());

                for (int i = 0; i < 1000; i++) {
                    r.cancel();
                }

                populateProducts(writer, rnd, ts, N / 2, increment);

                writer.commit();
                Assert.assertEquals(N, writer.size());
                Assert.assertEquals(6, getDirCount());
            }
        });
    }

    @Test
    public void testCancelFirstRowSecondPartition() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.DAY, 4);
            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                writer.newRow(TimestampFormatUtils.parseTimestamp("2013-03-01T00:00:00.000Z")).append();
                writer.newRow(TimestampFormatUtils.parseTimestamp("2013-03-01T00:00:00.000Z")).append();

                TableWriter.Row r = writer.newRow(TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z"));
                r.cancel();
                writer.commit();
                Assert.assertEquals(2, writer.size());


                writer.newRow(TimestampFormatUtils.parseTimestamp("2013-03-01T00:00:00.000Z")).append();
                writer.newRow(TimestampFormatUtils.parseTimestamp("2013-03-01T00:00:00.000Z")).append();
                r = writer.newRow(TimestampFormatUtils.parseTimestamp("2013-03-05T00:00:00.000Z"));
                r.cancel();
                r.cancel();
                Assert.assertEquals(4, writer.size());
            }

            // Last 2 rows are not committed, expect size to revert to 2
            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                Assert.assertEquals(2, writer.size());
            }
        });
    }

    @Test
    public void testCancelMidPartition() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd();
            final int N = 10000;
            create(FF, PartitionBy.DAY, N);

            // this contraption will verify that all timestamps that are
            // supposed to be stored have matching partitions
            try (MemoryARW vmem = Vm.getARWInstance(FF.getPageSize(), Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                    long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
                    int i = 0;

                    int cancelCount = 0;
                    while (i < N) {
                        TableWriter.Row r = writer.newRow(ts += 60000 * 1000L);
                        r.putInt(0, rnd.nextPositiveInt());
                        r.putStr(1, rnd.nextString(7));
                        r.putSym(2, rnd.nextString(4));
                        r.putSym(3, rnd.nextString(11));
                        r.putDouble(4, rnd.nextDouble());
                        if (rnd.nextPositiveInt() % 30 == 0) {
                            r.cancel();
                            cancelCount++;
                        } else {
                            r.append();
                            // second append() is expected to be a NOOP
                            r.append();
                            vmem.putLong(ts);
                            i++;
                        }
                    }
                    writer.commit();
                    Assert.assertEquals(N, writer.size());
                    Assert.assertTrue(cancelCount > 0);
                    verifyTimestampPartitions(vmem);
                }
            }
        });
    }

    @Test
    public void testCancelMidRowNonPartitioned() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int N = 10000;
            create(FF, PartitionBy.NONE, N);
            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

                int cancelCount = 0;
                Rnd rnd = new Rnd();
                int i = 0;
                TableWriter.Row r;
                while (i < N) {
                    r = writer.newRow(ts += 60000 * 1000L);
                    r.putInt(0, rnd.nextPositiveInt());
                    r.putStr(1, rnd.nextString(7));
                    r.putSym(2, rnd.nextString(4));
                    r.putSym(3, rnd.nextString(11));
                    r.putDouble(4, rnd.nextDouble());
                    if (rnd.nextBoolean()) {
                        r.append();
                        i++;
                    } else {
                        cancelCount++;
                    }
                }
                r = writer.newRow(ts);
                r.putSym(2, "XYZ");

                writer.commit();
                Assert.assertTrue(cancelCount > 0);
                Assert.assertEquals(N, writer.size());
            }
        });
    }

    @Test
    public void testCancelRowAfterAddColumn() throws Exception {
        int N = 10000;
        create(FF, PartitionBy.DAY, N);
        Rnd rnd = new Rnd();
        long interval = 60000 * 1000L;
        long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
        try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
            ts = populateProducts(writer, rnd, ts, N, interval);

            Assert.assertEquals(N, writer.size());

            writer.addColumn("abc", ColumnType.STRING);

            TableWriter.Row r = writer.newRow(ts);
            r.putInt(0, rnd.nextInt());
            r.cancel();

            Assert.assertEquals(Long.BYTES, writer.getStorageColumn(21).getAppendOffset());

            // add more data including updating new column
            ts = populateTable2(writer, rnd, N, ts, interval);
            Assert.assertEquals(2 * N, writer.size());

            writer.rollback();
        }

        // append more
        try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
            populateTable2(writer, rnd, N, ts, interval);
            writer.commit();
            Assert.assertEquals(2 * N, writer.size());
        }
    }

    @Test
    public void testCancelRowOutOfOrder() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int N = 47;
            create(FF, PartitionBy.DAY, N);
            Rnd rnd = new Rnd();

            DefaultCairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
                populateProducts(writer, rnd, ts, 1, 0);
                writer.commit();

                TableWriter.Row r = writer.newRow(TimestampFormatUtils.parseTimestamp("2013-03-02T09:00:00.000Z"));
                // One set of columns
                r.putInt(0, rnd.nextPositiveInt());  // productId
                r.putStr(1, "CANCELLED"); // productName
                r.putSym(2, "CANCELLED2"); // supplier
                r.putSym(3, "CANCELLED3"); // category
                r.cancel();

                // Another set of columns
                r = writer.newRow(ts);
                r.putSym(2, "GOOD"); // supplier
                r.putSym(3, "GOOD2"); // category
                r.putDouble(4, 123); // price
                r.putByte(5, (byte) 45); // locationByte
                r.putShort(6, (short) 678);
                r.append();

                writer.commit();

                Assert.assertEquals(2, writer.size());
            }

            try (TableWriter writer = newTableWriter(AbstractCairoTest.configuration, PRODUCT, metrics)) {
                Assert.assertEquals(2, writer.size());
            }

            try (TableReader rdr = newTableReader(configuration, PRODUCT)) {
                String expected = "productId\tproductName\tsupplier\tcategory\tprice\tlocationByte\tlocationShort\tlocationInt\tlocationLong\ttimestamp\n" +
                        "1148479920\tTJWCPSW\tHYRX\tPEHNRXGZSXU\t0.4621835429127854\tq\ttp0\tttmt7w\tcs4bdw4y4dpw\t2013-03-04T00:00:00.000000Z\n" +
                        "NaN\t\tGOOD\tGOOD2\t123.0\te\t0p6\t\t\t2013-03-04T00:00:00.000000Z\n";
                assertCursor(expected, rdr.getCursor(), rdr.getMetadata(), true);
            }
        });
    }

    @Test
    public void testCancelRowRecoveryFromAppendPosErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd();

            class X extends FilesFacadeImpl {
                boolean fail = false;

                @Override
                public boolean rmdir(Path name, boolean lazy) {
                    if (fail) {
                        return false;
                    }
                    return super.rmdir(name, lazy);
                }
            }

            X ff = new X();

            final int N = 10000;
            create(ff, PartitionBy.DAY, N);

            // this contraption will verify that all timestamps that are
            // supposed to be stored have matching partitions
            try (MemoryARW vmem = Vm.getARWInstance(ff.getPageSize(), Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                try (TableWriter writer = newTableWriter(new DefaultTestCairoConfiguration(root) {
                    @Override
                    public @NotNull FilesFacade getFilesFacade() {
                        return ff;
                    }
                }, PRODUCT, metrics)) {
                    long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
                    int i = 0;

                    int cancelCount = 0;
                    int failCount = 0;
                    while (i < N) {
                        TableWriter.Row r = writer.newRow(ts += 60000 * 1000L);
                        r.putInt(0, rnd.nextPositiveInt());
                        r.putStr(1, rnd.nextString(7));
                        r.putSym(2, rnd.nextString(4));
                        r.putSym(3, rnd.nextString(11));
                        r.putDouble(4, rnd.nextDouble());
                        if (rnd.nextPositiveInt() % 50 == 0) {
                            ff.fail = true;
                            try {
                                r.cancel();
                            } catch (CairoException ignored) {
                                failCount++;
                                ff.fail = false;
                                r.cancel();
                            } finally {
                                ff.fail = false;
                            }
                            cancelCount++;
                        } else {
                            r.append();
                            // second append() is expected to be a NOOP
                            r.append();
                            vmem.putLong(ts);
                            i++;
                        }
                    }
                    writer.commit();
                    Assert.assertEquals(N, writer.size());
                    Assert.assertTrue(cancelCount > 0);
                    Assert.assertTrue(failCount > 0);
                    verifyTimestampPartitions(vmem);
                }
            }
        });
    }

    @Test
    public void testCancelSecondRowNonPartitioned() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10000;
            create(FF, PartitionBy.NONE, N);
            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {

                long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

                TableWriter.Row r = writer.newRow(ts);
                r.putInt(0, 1234);
                r.append();

                r = writer.newRow(ts);
                r.putInt(0, 1234);
                r.cancel();

                Assert.assertEquals(1, writer.size());

                populateProducts(writer, new Rnd(), ts, N, 60 * 60000 * 1000L);
                Assert.assertEquals(N + 1, writer.size());
                writer.commit();
            }

            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                Assert.assertEquals(N + 1, writer.size());
            }
        });

    }

    @Test
    public void testCannotCreatePartitionDir() throws Exception {
        testConstructor(new TestFilesFacadeImpl() {
            @Override
            public int mkdirs(Path path, int mode) {
                if (Utf8s.endsWithAscii(path, "default" + Files.SEPARATOR)) {
                    return -1;
                }
                return super.mkdirs(path, mode);
            }
        });
    }

    @Test
    public void testCannotLock() throws Exception {
        create(FF, PartitionBy.NONE, 4);
        TestUtils.assertMemoryLeak(() -> {
            TestFilesFacade ff = new TestFilesFacade() {
                boolean ran = false;

                @Override
                public int openRW(LPSZ name, long opts) {
                    if (Utf8s.endsWithAscii(name, PRODUCT_FS + ".lock")) {
                        ran = true;
                        return -1;
                    }
                    return super.openRW(name, opts);
                }

                @Override
                public boolean wasCalled() {
                    return ran;
                }
            };

            try {
                newTableWriter(new DefaultTestCairoConfiguration(root) {
                    @Override
                    public @NotNull FilesFacade getFilesFacade() {
                        return ff;
                    }
                }, PRODUCT, metrics).close();
                Assert.fail();
            } catch (CairoException ignore) {
            }
            Assert.assertTrue(ff.wasCalled());
        });
    }

    @Test
    public void testCannotMapTxFile() throws Exception {
        testConstructor(new TestFilesFacadeImpl() {
            int count = 1;

            @Override
            public long mmap(int fd, long len, long offset, int flags, int memoryTag) {
                if (fd == this.fd) {
                    this.fd = -1;
                    return -1;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public int openRW(LPSZ name, long opts) {
                if (Utf8s.endsWithAscii(name, TableUtils.TXN_FILE_NAME) && --count == 0) {
                    return fd = super.openRW(name, opts);
                }
                return super.openRW(name, opts);
            }
        });
    }

    @Test
    public void testCannotOpenColumnFile() throws Exception {
        testConstructor(new TestFilesFacadeImpl() {
            @Override
            public int openRW(LPSZ name, long opts) {
                if (Utf8s.endsWithAscii(name, "productName.i")) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        });
    }

    @Test
    public void testCannotOpenSymbolMap() throws Exception {
        final int N = 100;
        create(FF, PartitionBy.NONE, N);
        populateTable0(FF, N);
        testConstructor(new TestFilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                return !Utf8s.endsWithAscii(path, "category.o") && super.exists(path);
            }
        }, false);
    }

    @Test
    public void testCannotOpenTodo() throws Exception {
        // trick constructor into thinking "_todo" file exists
        testConstructor(new TestFilesFacadeImpl() {
            int counter = 1;

            @Override
            public int openRW(LPSZ path, long opts) {
                if (Utf8s.endsWithAscii(path, TableUtils.TODO_FILE_NAME) && --counter == 0) {
                    return -1;
                }
                return super.openRW(path, opts);
            }
        });
    }

    @Test
    public void testCannotOpenTxFile() throws Exception {
        testConstructor(new TestFilesFacadeImpl() {
            int count = 1;

            @Override
            public int openRW(LPSZ name, long opts) {
                if (Utf8s.endsWithAscii(name, TableUtils.TXN_FILE_NAME) && --count == 0) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        });
    }

    @Test
    public void testCannotSetAppendPosition() throws Exception {
        final int N = 10000;
        create(FF, PartitionBy.NONE, N);
        populateTable0(FF, N);
        testConstructor(new TestFilesFacadeImpl() {

            @Override
            public long mmap(int fd, long len, long offset, int flags, int memoryTag) {
                if (fd == this.fd) {
                    return -1;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public int openRW(LPSZ name, long opts) {
                if (Utf8s.endsWithAscii(name, "supplier.d")) {
                    return fd = super.openRW(name, opts);
                }
                return super.openRW(name, opts);
            }
        }, false);
    }

    @Test
    public void testCannotSetAppendPositionOnDataFile() throws Exception {
        final int N = 10000;
        create(FF, PartitionBy.NONE, N);
        populateTable0(FF, N);
        testConstructor(new TestFilesFacadeImpl() {
            @Override
            public boolean allocate(int fd, long size) {
                if (this.fd == fd) {
                    return false;
                }
                return super.allocate(fd, size);
            }

            @Override
            public int openRW(LPSZ name, long opts) {
                if (Utf8s.endsWithAscii(name, "productName.i")) {
                    return fd = super.openRW(name, opts);
                }
                return super.openRW(name, opts);
            }
        }, false);
    }

    @Test
    public void testCannotSetAppendPositionOnIndexFile() throws Exception {
        final int N = 10000;
        create(FF, PartitionBy.NONE, N);
        populateTable0(FF, N);
        testConstructor(new TestFilesFacadeImpl() {

            @Override
            public boolean allocate(int fd, long size) {
                if (this.fd == fd) {
                    return false;
                }
                return super.allocate(fd, size);
            }

            @Override
            public int openRW(LPSZ name, long opts) {
                if (Utf8s.endsWithAscii(name, "productName.i")) {
                    return fd = super.openRW(name, opts);
                }
                return super.openRW(name, opts);
            }
        }, false);
    }

    @Test
    // tests scenario where truncate is supported (linux) but fails on close()
    public void testCannotTruncateColumnOnClose() throws Exception {
        int N = 100000;
        create(FF, PartitionBy.NONE, N);
        testTruncateOnClose(new TestFilesFacade() {
            int fd = -1;
            boolean ran = false;

            @Override
            public boolean isRestrictedFileSystem() {
                return false;
            }

            @Override
            public int openRW(LPSZ name, long opts) {
                if (Utf8s.endsWithAscii(name, "price.d")) {
                    return fd = super.openRW(name, opts);
                }
                return super.openRW(name, opts);
            }

            @Override
            public boolean truncate(int fd, long size) {
                if (this.fd == fd) {
                    ran = true;
                    return false;
                }
                return super.truncate(fd, size);
            }

            @Override
            public boolean wasCalled() {
                return fd != -1 && ran;
            }
        }, N);
    }

    @Test
    // tests scenario where truncate is not supported (Windows) but fails on close
    // truncate on close fails once and then succeeds
    // close is expected not to fail
    public void testCannotTruncateColumnOnCloseAndNotSupported() throws Exception {
        int N = 100000;
        create(FF, PartitionBy.NONE, N);
        testTruncateOnClose(new TestFilesFacade() {
            int fd = -1;
            boolean ran = false;

            @Override
            public boolean isRestrictedFileSystem() {
                return true;
            }

            @Override
            public int openRW(LPSZ name, long opts) {
                if (Utf8s.endsWithAscii(name, "price.d")) {
                    return fd = super.openRW(name, opts);
                }
                return super.openRW(name, opts);
            }

            @Override
            public boolean truncate(int fd, long size) {
                if (this.fd == fd) {
                    ran = true;
                    return false;
                }
                return super.truncate(fd, size);
            }

            @Override
            public boolean wasCalled() {
                return fd != -1 && ran;
            }
        }, N);
    }

    @Test
    // tests scenario where truncate is not supported (Windows) but fails on close
    // truncate on close fails all the time
    public void testCannotTruncateColumnOnCloseAndNotSupported2() throws Exception {
        int N = 100000;
        create(FF, PartitionBy.NONE, N);
        testTruncateOnClose(new TestFilesFacade() {
            int fd = -1;
            boolean ran = false;

            @Override
            public boolean isRestrictedFileSystem() {
                return true;
            }

            @Override
            public int openRW(LPSZ name, long opts) {
                if (Utf8s.endsWithAscii(name, "price.d")) {
                    return fd = super.openRW(name, opts);
                }
                return super.openRW(name, opts);
            }

            @Override
            public boolean truncate(int fd, long size) {
                if (this.fd == fd) {
                    ran = true;
                    return false;
                }
                return super.truncate(fd, size);
            }

            @Override
            public boolean wasCalled() {
                return fd != -1 && ran;
            }
        }, N);
    }

    @Test
    public void testCloseActivePartitionAndRollback() throws Exception {
        int N = 10000;
        create(FF, PartitionBy.DAY, N);
        try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
            Rnd rnd = new Rnd();
            populateProducts(writer, rnd, ts, N, 6 * 60000 * 1000L);
            writer.closeActivePartition(true);
            writer.rollback();
            Assert.assertEquals(0, writer.size());
        }
    }

    @Test
    public void testConstructorTruncatedTodo() throws Exception {
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long length(LPSZ name) {
                if (Utf8s.endsWithAscii(name, TableUtils.TODO_FILE_NAME)) {
                    return 12;
                }
                return super.length(name);
            }
        };

        TestUtils.assertMemoryLeak(() -> {
                    create(ff, PartitionBy.DAY, 10000);
                    try {
                        populateTable0(ff, 10000);
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "corrupt");
                    }
                }
        );
    }

    @Test
    public void testDayPartition() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10000;
            create(FF, PartitionBy.DAY, N);

            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                populateProducts(writer, new Rnd(), TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z"), N, 60000 * 1000L);
                writer.commit();
                Assert.assertEquals(N, writer.size());
            }

            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                Assert.assertEquals(N, writer.size());
            }
        });
    }

    @Test
    public void testDayPartitionTruncate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10000;
            create(FF, PartitionBy.DAY, N);
            Rnd rnd = new Rnd();
            long increment = 60000L * 1000;
            CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public long getDataAppendPageSize() {
                    return 1024 * 1024; // 1MB
                }
            };
            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

                for (int k = 0; k < 3; k++) {
                    ts = populateProducts(writer, rnd, ts, N, increment);
                    writer.commit();
                    Assert.assertEquals(N, writer.size());
                    writer.truncateSoft();
                }
            }

            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                long ts = TimestampFormatUtils.parseTimestamp("2014-03-04T00:00:00.000Z");
                Assert.assertEquals(0, writer.size());
                populateProducts(writer, rnd, ts, N, increment);
                writer.commit();
                Assert.assertEquals(N, writer.size());
            }
        });
    }

    @Test
    public void testDayPartitionTruncatePurgeSymbolTables() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10000;
            create(FF, PartitionBy.DAY, N);
            Rnd rnd = new Rnd();
            long increment = 60000L * 1000;
            CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public long getDataAppendPageSize() {
                    return 1024 * 1024; // 1MB
                }
            };
            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

                for (int k = 0; k < 3; k++) {
                    ts = populateProducts(writer, rnd, ts, N, increment);
                    writer.commit();
                    Assert.assertEquals(N, writer.size());
                    writer.truncate();
                }
            }

            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                long ts = TimestampFormatUtils.parseTimestamp("2014-03-04T00:00:00.000Z");
                Assert.assertEquals(0, writer.size());
                populateProducts(writer, rnd, ts, N, increment);
                writer.commit();
                Assert.assertEquals(N, writer.size());
            }
        });
    }

    @Test
    public void testDefaultPartition() throws Exception {
        populateTable();
    }

    @Test
    public void testGeoHashAsStringInvalid() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertGeoStr("ooo", 15, GeoHashes.NULL);
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertEquals("inconvertible value: `ooo` [STRING -> GEOHASH(3c)]", e.getFlyweightMessage());
            }
        });
    }

    @Test
    public void testGeoHashAsStringLongerThanType() throws Exception {
        TestUtils.assertMemoryLeak(() -> assertGeoStr("g912j", 15, 15649));
    }

    @Test
    public void testGeoHashAsStringLongerThanTypeUneven() throws Exception {
        TestUtils.assertMemoryLeak(() -> assertGeoStr("g912j", 11, 978));
    }

    @Test
    public void testGeoHashAsStringNull() throws Exception {
        TestUtils.assertMemoryLeak(() -> assertGeoStr(null, 15, GeoHashes.NULL));
    }

    @Test
    public void testGeoHashAsStringShorterThanType() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertGeoStr("g912j", 44, GeoHashes.NULL);
                Assert.fail();
            } catch (ImplicitCastException e) {
                TestUtils.assertEquals("inconvertible value: `g912j` [STRING -> GEOHASH(44b)]", e.getFlyweightMessage());
            }
        });
    }

    @Test
    public void testGeoHashAsStringVanilla() throws Exception {
        TestUtils.assertMemoryLeak(() -> assertGeoStr("g9", 10, 489));
    }

    @Test
    public void testGetColumnIndex() {
        CreateTableTestUtils.createAllTable(engine, PartitionBy.NONE);
        try (TableWriter writer = newTableWriter(configuration, "all", metrics)) {
            Assert.assertEquals(1, writer.getColumnIndex("short"));
            try {
                writer.getColumnIndex("bad");
                Assert.fail();
            } catch (CairoException ignore) {
            }
        }
    }

    @Test
    public void testIncorrectTodoCode() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CreateTableTestUtils.createAllTable(engine, PartitionBy.NONE);
            String all = "all";
            TableToken tableToken = engine.verifyTableName(all);
            try (
                    MemoryCMARW mem = Vm.getCMARWInstance();
                    Path path = new Path().of(root).concat(tableToken).concat(TableUtils.TODO_FILE_NAME).$()
            ) {
                mem.smallFile(TestFilesFacadeImpl.INSTANCE, path, MemoryTag.MMAP_DEFAULT);
                mem.putLong(32, 1);
                mem.putLong(40, 9990001L);
                mem.jumpTo(48);
            }
            try (TableWriter writer = newTableWriter(configuration, all, metrics)) {
                Assert.assertNotNull(writer);
                Assert.assertTrue(writer.isOpen());
            }

            try (TableWriter writer = newTableWriter(configuration, all, metrics)) {
                Assert.assertNotNull(writer);
                Assert.assertTrue(writer.isOpen());
            }
        });
    }

    @Test
    public void testIndexIsAddedToTable() throws NumericException {
        int partitionBy = PartitionBy.DAY;
        int N = 1000;
        TableToken tableToken;
        try (TableModel model = new TableModel(configuration, "test", partitionBy)) {
            model.col("sym1", ColumnType.SYMBOL);
            model.col("sym2", ColumnType.SYMBOL);
            model.col("sym3", ColumnType.SYMBOL);
            model.timestamp();

            tableToken = CreateTableTestUtils.create(model);
        }

        // insert data
        final Rnd rnd = new Rnd();
        long t = TimestampFormatUtils.parseTimestamp("2019-03-22T00:00:00.000000Z");
        long increment = 2_000_000;
        try (TableWriter w = getWriter(tableToken)) {
            testIndexIsAddedToTableAppendData(N, rnd, t, increment, w);
            w.commit();

            // truncate writer
            w.truncate();

            // add a couple of indexes
            w.addIndex("sym1", 1024);
            w.addIndex("sym2", 1024);

            Assert.assertTrue(w.getMetadata().isColumnIndexed(0));
            Assert.assertTrue(w.getMetadata().isColumnIndexed(1));
            Assert.assertFalse(w.getMetadata().isColumnIndexed(2));

            // here we reset random to ensure we re-insert the same values
            rnd.reset();
            testIndexIsAddedToTableAppendData(N, rnd, t, increment, w);
            w.commit();

            Assert.assertEquals(1, w.getPartitionCount());

            // ensure indexes can be read
            try (TableReader reader = getReader(tableToken)) {
                final TableReaderRecord record = (TableReaderRecord) reader.getCursor().getRecord();
                assertIndex(reader, record, 0);
                assertIndex(reader, record, 1);

                // check if we can still truncate the writer
                w.truncate();
                Assert.assertEquals(0, w.size());
                Assert.assertTrue(reader.reload());
                Assert.assertEquals(0, reader.size());
            }
        }
    }

    @Test
    public void testMetaFileDoesNotExist() throws Exception {
        testConstructor(new TestFilesFacadeImpl() {
            @Override
            public int openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, TableUtils.META_FILE_NAME)) {
                    return -1;
                }
                return super.openRO(name);
            }
        });
    }

    @Test
    public void testNonStandardPageSize() throws Exception {
        populateTable(new TestFilesFacadeImpl() {
            @Override
            public long getPageSize() {
                return super.getPageSize() * 500;
            }
        }, PartitionBy.MONTH);
    }

    @Test
    public void testNonStandardPageSize2() throws Exception {
        populateTable(new TestFilesFacadeImpl() {
            @Override
            public long getPageSize() {
                return 32 * 1024 * 1024;
            }
        }, PartitionBy.YEAR);
    }

    @Test
    public void testNulls() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CreateTableTestUtils.createAllTable(engine, PartitionBy.NONE);
            Rnd rnd = new Rnd();
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
            ts = testAppendNulls(rnd, ts);
            testAppendNulls(rnd, ts);
        });
    }

    @Test
    public void testO3AfterReopen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CreateTableTestUtils.createAllTableWithTimestamp(engine, PartitionBy.NONE);
            Rnd rnd = new Rnd();
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
            testAppendNulls(rnd, ts);
            try {
                testAppendNulls(rnd, ts);
                Assert.fail();
            } catch (CairoException ignore) {
            }
        });
    }

    @Test
    public void testO3AfterRowCancel() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TableModel model = new TableModel(configuration, "weather", PartitionBy.DAY)
                    .col("windspeed", ColumnType.DOUBLE)
                    .timestamp()) {
                CreateTableTestUtils.create(model);
            }

            try (TableWriter writer = newTableWriter(configuration, "weather", metrics)) {
                TableWriter.Row r;
                r = writer.newRow(IntervalUtils.parseFloorPartialTimestamp("2021-01-31"));
                r.putDouble(0, 1.0);
                r.append();

                // Out of order
                r = writer.newRow(IntervalUtils.parseFloorPartialTimestamp("2021-01-30"));
                r.putDouble(0, 1.0);
                r.cancel();

                // Back in order
                r = writer.newRow(IntervalUtils.parseFloorPartialTimestamp("2021-02-01"));
                r.putDouble(0, 1.0);
                r.append();

                Assert.assertEquals(2, writer.size());
                writer.commit();
            }

            long[] expectedTs = new long[]{
                    IntervalUtils.parseFloorPartialTimestamp("2021-01-31"),
                    IntervalUtils.parseFloorPartialTimestamp("2021-02-01")
            };
            try (TableReader reader = newTableReader(configuration, "weather")) {
                int col = reader.getMetadata().getColumnIndex("timestamp");
                RecordCursor cursor = reader.getCursor();
                final Record r = cursor.getRecord();
                int i = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals("Row " + i, expectedTs[i++], r.getTimestamp(col));
                }
                Assert.assertEquals(expectedTs.length, i);
            }
        });
    }

    @Test
    public void testO3PartitionTruncate() throws Exception {
        final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root);
        final String tableName = "testO3PartitionTruncate";

        try (TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                .col("productId", ColumnType.LONG256)
                .timestamp()
        ) {
            CreateTableTestUtils.create(model);

            try (TableWriter writer = newTableWriter(configuration, model.getName(), Metrics.disabled())) {
                // Add 46 rows in partition 2020-07-13T00
                long ts = IntervalUtils.parseFloorPartialTimestamp("2020-07-13");
                long increment = Timestamps.SECOND_MICROS;
                int rows = 46;
                for (int i = 0; i < rows; i++) {
                    TableWriter.Row row = writer.newRow(ts);
                    row.putLong256(0, i, i, i, i);
                    row.append();
                    ts += increment;
                }
                writer.commit();

                // Add 1 row in order in partition 2020-07-13T00
                TableWriter.Row row = writer.newRow(ts + 2 * increment);
                row.putLong256(0, 1, 1, 1, 1);
                row.append();

                // Add 1 row ooo in partition 2020-07-13T00
                row = writer.newRow(ts + 1);
                row.putLong256(0, 1, 1, 1, 1);
                row.append();

                // Add 1 row out of order in partition 2020-07-13T01
                row = writer.newRow(ts + Timestamps.HOUR_MICROS);
                row.putLong256(0, 2, 2, 2, 2);
                row.append();

                // Write rows to go beyond page in partition 2020-07-13T00
                int rowCount = (int) (Files.PAGE_SIZE / 32);
                for (int i = 0; i < rowCount; i++) {
                    TableWriter.Row row2 = writer.newRow(ts);
                    row2.putLong256(0, i, i, i, i);
                    row2.append();
                    ts += increment;
                }

                writer.commit();
            }

            try (TableReader rdr = newTableReader(configuration, tableName)) {
                TestUtils.printCursor(rdr.getCursor(), rdr.getMetadata(), true, sink, TestUtils.printer);
            }
        }
    }

    @Test
    public void testO3WithCancelRow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TableModel model = new TableModel(configuration, "weather", PartitionBy.DAY)
                    .col("windspeed", ColumnType.DOUBLE)
                    .timestamp()) {
                CreateTableTestUtils.create(model);
            }

            long[] tss = new long[]{
                    631150000000000L,
                    631152000000000L,
                    631160000000000L
            };
            try (TableWriter writer = newTableWriter(configuration, "weather", metrics)) {
                TableWriter.Row r = writer.newRow(tss[1]);
                r.putDouble(0, 1.0);
                r.append();

                // Out of order
                r = writer.newRow(tss[0]);
                r.putDouble(0, 2.0);
                r.append();

                r = writer.newRow(tss[2]);
                r.putDouble(0, 3.0);
                r.cancel();

                // Implicit commit
                writer.addColumn("timetocycle", ColumnType.DOUBLE);

                writer.newRow(tss[2]);
                r.putDouble(0, 3.0);
                r.putDouble(2, -1.0);
                r.append();

                writer.commit();
            }

            try (TableReader reader = newTableReader(configuration, "weather")) {
                int col = reader.getMetadata().getColumnIndex("timestamp");
                RecordCursor cursor = reader.getCursor();
                final Record r = cursor.getRecord();
                int i = 0;
                while (cursor.hasNext()) {
                    Assert.assertEquals("Row " + i, tss[i++], r.getTimestamp(col));
                }
                Assert.assertEquals(tss.length, i);
            }
        });
    }

    @Test
    public void testOpenUnsupportedIndex() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                    .col("a", ColumnType.SYMBOL).cached(true)
                    .col("b", ColumnType.STRING)
                    .col("c", ColumnType.STRING).indexed(true, 1024)
                    .timestamp()) {
                CreateTableTestUtils.create(model);
            }

            try {
                newTableWriter(configuration, "x", metrics).close();
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "only supported");
            }
        });
    }

    @Test
    public void testOpenWriterMissingTxFile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CreateTableTestUtils.createAllTable(engine, PartitionBy.NONE);
            try (Path path = new Path()) {
                String all = "all";
                TableToken tableToken = engine.verifyTableName(all);
                Assert.assertTrue(FF.remove(path.of(root).concat(tableToken).concat(TableUtils.TXN_FILE_NAME).$()));
                try {
                    newTableWriter(configuration, all, metrics).close();
                    Assert.fail();
                } catch (CairoException ignore) {
                }
            }
        });
    }

    @Test
    public void testRemoveColumnAfterTimestamp() throws Exception {
        try (TableModel model = new TableModel(configuration, "ABC", PartitionBy.DAY)
                .col("productId", ColumnType.INT)
                .col("productName", ColumnType.STRING)
                .timestamp()
                .col("supplier", ColumnType.SYMBOL)
                .col("category", ColumnType.SYMBOL)
                .col("price", ColumnType.DOUBLE)) {
            testRemoveColumn(model);
        }
    }

    @Test
    public void testRemoveColumnBeforeTimestamp() throws Exception {
        try (TableModel model = new TableModel(configuration, "ABC", PartitionBy.DAY)
                .col("productId", ColumnType.INT)
                .col("productName", ColumnType.STRING)
                .col("supplier", ColumnType.SYMBOL)
                .col("category", ColumnType.SYMBOL)
                .col("price", ColumnType.DOUBLE)
                .timestamp()) {
            testRemoveColumn(model);
        }
    }

    @Test
    public void testRemoveColumnBeforeTimestamp3Symbols() throws Exception {
        try (TableModel model = new TableModel(configuration, "ABC", PartitionBy.DAY)
                .col("productId", ColumnType.INT)
                .col("supplier", ColumnType.SYMBOL)
                .col("category", ColumnType.SYMBOL)
                .col("productName", ColumnType.SYMBOL)
                .col("price", ColumnType.DOUBLE)
                .timestamp()) {
            testRemoveColumn(model);
        }
    }

    @Test
    public void testRemoveColumnCannotOpenSwap() throws Exception {
        class X extends TestFilesFacade {
            boolean hit = false;

            @Override
            public int openRW(LPSZ name, long opts) {
                if (Utf8s.containsAscii(name, TableUtils.META_SWAP_FILE_NAME)) {
                    hit = true;
                    return -1;
                }
                return super.openRW(name, opts);
            }

            @Override
            public boolean wasCalled() {
                return hit;
            }
        }
        testRemoveColumnRecoverableFailure(new X());
    }

    @Test
    public void testRemoveColumnCannotRemoveAnyMetadataPrev() throws Exception {
        testRemoveColumnRecoverableFailure(new TestFilesFacade() {
            int exists = 0;
            int removes = 0;

            @Override
            public boolean exists(LPSZ path) {
                if (Utf8s.containsAscii(path, TableUtils.META_PREV_FILE_NAME)) {
                    exists++;
                    return true;
                }
                return super.exists(path);
            }

            @Override
            public boolean remove(LPSZ name) {
                if (Utf8s.containsAscii(name, TableUtils.META_PREV_FILE_NAME)) {
                    removes++;
                    return false;
                }
                return super.remove(name);
            }

            @Override
            public boolean wasCalled() {
                return exists > 0 && removes > 0;
            }
        });
    }

    @Test
    public void testRemoveColumnCannotRemoveFiles() throws Exception {
        removeColumn(new TestFilesFacade() {
            int count = 0;

            @Override
            public boolean remove(LPSZ name) {
                if (Utf8s.endsWithAscii(name, "supplier.d")) {
                    count++;
                    return false;
                }
                return super.remove(name);
            }

            @Override
            public boolean wasCalled() {
                return count > 0;
            }
        });
    }

    @Test
    public void testRemoveColumnCannotRemoveFiles2() throws Exception {
        removeColumn(new TestFilesFacade() {
            int count = 0;

            @Override
            public boolean remove(LPSZ name) {
                if (Utf8s.endsWithAscii(name, "supplier.d")) {
                    count++;
                    return false;
                }
                return super.remove(name);
            }

            @Override
            public boolean wasCalled() {
                return count > 0;
            }
        });
    }

    @Test
    public void testRemoveColumnCannotRemoveSomeMetadataPrev() throws Exception {
        removeColumn(new TestFilesFacade() {
            int count = 5;

            @Override
            public boolean exists(LPSZ path) {
                if (Utf8s.containsAscii(path, TableUtils.META_PREV_FILE_NAME) && --count > 0) {
                    return true;
                }
                return super.exists(path);
            }

            @Override
            public boolean remove(LPSZ name) {
                return !Utf8s.containsAscii(name, TableUtils.META_PREV_FILE_NAME) && super.remove(name);
            }

            @Override
            public boolean wasCalled() {
                return count <= 0;
            }
        });
    }

    @Test
    public void testRemoveColumnCannotRemoveSwap() throws Exception {
        class X extends TestFilesFacade {
            boolean hit = false;

            @Override
            public boolean exists(LPSZ path) {
                return Utf8s.containsAscii(path, TableUtils.META_SWAP_FILE_NAME) || super.exists(path);
            }

            @Override
            public boolean remove(LPSZ name) {
                if (Utf8s.containsAscii(name, TableUtils.META_SWAP_FILE_NAME)) {
                    hit = true;
                    return false;
                }
                return super.remove(name);
            }

            @Override
            public boolean wasCalled() {
                return hit;
            }
        }
        testRemoveColumnRecoverableFailure(new X());
    }

    @Test
    public void testRemoveColumnCannotRenameMeta() throws Exception {
        testRemoveColumnRecoverableFailure(new MetaRenameDenyingFacade());
    }

    @Test
    public void testRemoveColumnCannotRenameMetaSwap() throws Exception {
        testRemoveColumnRecoverableFailure(new SwapMetaRenameDenyingFacade());
    }

    @Test
    public void testRemoveColumnUnrecoverableRenameFailure() throws Exception {
        class X extends FilesFacadeImpl {
            int count = 2;

            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (Utf8s.endsWithAscii(to, TableUtils.META_FILE_NAME) && count-- > 0) {
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                return super.rename(from, to);
            }
        }
        testUnrecoverableRemoveColumn(new X());
    }

    @Test
    public void testRemoveTimestamp() throws Exception {
        try (TableModel model = new TableModel(configuration, "ABC", PartitionBy.NONE)
                .col("productId", ColumnType.INT)
                .col("productName", ColumnType.STRING)
                .col("category", ColumnType.SYMBOL)
                .col("price", ColumnType.DOUBLE)
                .timestamp()
                .col("supplier", ColumnType.SYMBOL)
        ) {
            CreateTableTestUtils.create(model);
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();
            try (TableWriter writer = newTableWriter(configuration, model.getName(), metrics)) {

                append10KProducts(ts, rnd, writer);

                writer.removeColumn("timestamp");

                append10KNoTimestamp(rnd, writer);

                writer.commit();

                Assert.assertEquals(20000, writer.size());
            }

            try (TableWriter writer = newTableWriter(configuration, model.getName(), metrics)) {
                append10KNoTimestamp(rnd, writer);
                writer.commit();
                Assert.assertEquals(30000, writer.size());
            }
        }
    }

    @Test
    public void testRemoveTimestampFromPartitionedTable() {
        try (TableModel model = new TableModel(configuration, "ABC", PartitionBy.DAY)
                .col("productId", ColumnType.INT)
                .col("productName", ColumnType.STRING)
                .col("category", ColumnType.SYMBOL)
                .col("price", ColumnType.DOUBLE)
                .timestamp()
                .col("supplier", ColumnType.SYMBOL)) {
            CreateTableTestUtils.create(model);
        }

        try (TableWriter writer = newTableWriter(configuration, "ABC", metrics)) {
            try {
                writer.removeColumn("timestamp");
                Assert.fail();
            } catch (CairoException ignore) {
            }
        }
    }

    @Test
    public void testRenameColumnAfterTimestamp() throws Exception {
        try (TableModel model = new TableModel(configuration, "ABC", PartitionBy.DAY)
                .col("productId", ColumnType.INT)
                .col("productName", ColumnType.STRING)
                .timestamp()
                .col("supplier", ColumnType.SYMBOL)
                .col("category", ColumnType.SYMBOL)
                .col("price", ColumnType.DOUBLE)) {
            testRenameColumn(model);
        }
    }

    @Test
    public void testRenameColumnBeforeTimestamp() throws Exception {
        try (TableModel model = new TableModel(configuration, "ABC", PartitionBy.DAY)
                .col("productId", ColumnType.INT)
                .col("productName", ColumnType.STRING)
                .col("supplier", ColumnType.SYMBOL)
                .col("category", ColumnType.SYMBOL)
                .col("price", ColumnType.DOUBLE)
                .timestamp()) {
            testRenameColumn(model);
        }
    }

    @Test
    public void testRenameColumnCannotOpenSwap() throws Exception {
        class X extends TestFilesFacade {
            boolean hit = false;

            @Override
            public int openRW(LPSZ name, long opts) {
                if (Utf8s.containsAscii(name, TableUtils.META_SWAP_FILE_NAME)) {
                    hit = true;
                    return -1;
                }
                return super.openRW(name, opts);
            }

            @Override
            public boolean wasCalled() {
                return hit;
            }
        }
        testRenameColumnRecoverableFailure(new X());
    }

    @Test
    public void testRenameColumnCannotRemoveAnyMetadataPrev() throws Exception {
        testRenameColumnRecoverableFailure(new TestFilesFacade() {
            int exists = 0;
            int removes = 0;

            @Override
            public boolean exists(LPSZ path) {
                if (Utf8s.containsAscii(path, TableUtils.META_PREV_FILE_NAME)) {
                    exists++;
                    return true;
                }
                return super.exists(path);
            }

            @Override
            public boolean remove(LPSZ name) {
                if (Utf8s.containsAscii(name, TableUtils.META_PREV_FILE_NAME)) {
                    removes++;
                    return false;
                }
                return super.remove(name);
            }

            @Override
            public boolean wasCalled() {
                return exists > 0 && removes > 0;
            }
        });
    }

    @Test
    public void testRenameColumnCannotRemoveCFile() throws Exception {
        renameColumn(new TestFilesFacade() {
            int count = 0;

            @Override
            public boolean remove(LPSZ name) {
                if (Utf8s.endsWithAscii(name, "supplier.c")) {
                    count++;
                    return false;
                }
                return super.remove(name);
            }

            @Override
            public boolean wasCalled() {
                return count > 0;
            }
        });
    }

    @Test
    public void testRenameColumnCannotRemoveDFile() throws Exception {
        renameColumn(new TestFilesFacade() {
            int count = 0;

            @Override
            public boolean remove(LPSZ name) {
                if (Utf8s.endsWithAscii(name, "supplier.d")) {
                    count++;
                    return false;
                }
                return super.remove(name);
            }

            @Override
            public boolean wasCalled() {
                return count > 0;
            }
        });
    }

    @Test
    public void testRenameColumnCannotRemoveSomeMetadataPrev() throws Exception {
        renameColumn(new TestFilesFacade() {
            int count = 5;

            @Override
            public boolean exists(LPSZ path) {
                if (Utf8s.containsAscii(path, TableUtils.META_PREV_FILE_NAME) && --count > 0) {
                    return true;
                }
                return super.exists(path);
            }

            @Override
            public boolean remove(LPSZ name) {
                return !Utf8s.containsAscii(name, TableUtils.META_PREV_FILE_NAME) && super.remove(name);
            }

            @Override
            public boolean wasCalled() {
                return count <= 0;
            }
        });
    }

    @Test
    public void testRenameColumnCannotRemoveSwap() throws Exception {
        class X extends TestFilesFacade {
            boolean hit = false;

            @Override
            public boolean exists(LPSZ path) {
                return Utf8s.containsAscii(path, TableUtils.META_SWAP_FILE_NAME) || super.exists(path);
            }

            @Override
            public boolean remove(LPSZ name) {
                if (Utf8s.containsAscii(name, TableUtils.META_SWAP_FILE_NAME)) {
                    hit = true;
                    return false;
                }
                return super.remove(name);
            }

            @Override
            public boolean wasCalled() {
                return hit;
            }
        }
        testRenameColumnRecoverableFailure(new X());
    }

    @Test
    public void testRenameColumnCannotRenameMeta() throws Exception {
        testRenameColumnRecoverableFailure(new MetaRenameDenyingFacade());
    }

    @Test
    public void testRenameColumnCannotRenameMetaSwap() throws Exception {
        testRenameColumnRecoverableFailure(new SwapMetaRenameDenyingFacade());
    }

    @Test
    public void testRenameColumnUnrecoverableRenameFailure() throws Exception {
        class X extends FilesFacadeImpl {
            int count = 2;

            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (Utf8s.endsWithAscii(to, TableUtils.META_FILE_NAME) && count-- > 0) {
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                return super.rename(from, to);
            }
        }
        testUnrecoverableRenameColumn(new X());
    }

    @Test
    public void testRenameTimestamp() throws Exception {
        try (TableModel model = new TableModel(configuration, "ABC", PartitionBy.NONE)
                .col("productId", ColumnType.INT)
                .col("productName", ColumnType.STRING)
                .col("category", ColumnType.SYMBOL)
                .col("price", ColumnType.DOUBLE)
                .timestamp()
                .col("supplier", ColumnType.SYMBOL)
        ) {
            CreateTableTestUtils.create(model);
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();
            try (TableWriter writer = newTableWriter(configuration, model.getName(), metrics)) {

                append10KProducts(ts, rnd, writer);

                writer.renameColumn("timestamp", "ts");

                append10KProducts(writer.getMaxTimestamp(), rnd, writer);

                writer.commit();

                Assert.assertEquals(20000, writer.size());
            }

            try (TableWriter writer = newTableWriter(configuration, model.getName(), metrics)) {
                append10KProducts(writer.getMaxTimestamp(), rnd, writer);
                writer.commit();
                Assert.assertEquals(30000, writer.size());
            }
        }
    }

    @Test
    public void testRenameTimestampFromPartitionedTable() throws Exception {
        try (TableModel model = new TableModel(configuration, "ABC", PartitionBy.DAY)
                .col("productId", ColumnType.INT)
                .col("productName", ColumnType.STRING)
                .col("category", ColumnType.SYMBOL)
                .col("price", ColumnType.DOUBLE)
                .timestamp()
                .col("supplier", ColumnType.SYMBOL)) {
            CreateTableTestUtils.create(model);
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();
            try (TableWriter writer = newTableWriter(configuration, model.getName(), metrics)) {

                append10KProducts(ts, rnd, writer);

                writer.renameColumn("timestamp", "ts");

                append10KProducts(writer.getMaxTimestamp(), rnd, writer);

                writer.commit();

                Assert.assertEquals(20000, writer.size());
            }

            try (TableWriter writer = newTableWriter(configuration, model.getName(), metrics)) {
                append10KProducts(writer.getMaxTimestamp(), rnd, writer);
                writer.commit();
                Assert.assertEquals(30000, writer.size());
            }
        }
    }

    @Test
    public void testRollbackNonPartitioned() throws Exception {
        final int N = 20000;
        create(FF, PartitionBy.NONE, N);
        testRollback(N);
    }

    @Test
    public void testRollbackPartitionRemoveFailure() throws Exception {
        final int N = 10000;
        create(FF, PartitionBy.DAY, N);

        class X extends FilesFacadeImpl {
            boolean removeAttempted = false;

            @Override
            public boolean rmdir(Path from, boolean lazy) {
                if (Utf8s.endsWithAscii(from, "2013-03-12.0")) {
                    removeAttempted = true;
                    return false;
                }
                return super.rmdir(from, lazy);
            }
        }

        X ff = new X();

        long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
        final long increment = 60000L * 1000;
        Rnd rnd = new Rnd();
        try (TableWriter writer = newTableWriter(new DefaultTestCairoConfiguration(root) {
            @Override
            public @NotNull FilesFacade getFilesFacade() {
                return ff;
            }
        }, PRODUCT, metrics)) {

            ts = populateProducts(writer, rnd, ts, N, increment);
            writer.commit();

            populateProducts(writer, rnd, ts, N, increment);

            Assert.assertEquals(2 * N, writer.size());
            writer.rollback();

            Assert.assertTrue(ff.removeAttempted);

            // make sure row rollback works after rollback
            writer.newRow(ts).cancel();

            // we should be able to repeat timestamps
            populateProducts(writer, rnd, ts, N, increment);
            writer.commit();

            Assert.assertEquals(2 * N, writer.size());
        }
    }

    @Test
    public void testRollbackPartitionRenameFailure() throws Exception {
        final int N = 10000;
        create(FF, PartitionBy.DAY, N);

        class X extends FilesFacadeImpl {
            boolean removeAttempted = false;

            @Override
            public boolean rmdir(Path path, boolean lazy) {
                if (Utf8s.endsWithAscii(path, "2013-03-12.0")) {
                    removeAttempted = true;
                    return false;
                }
                return super.rmdir(path, lazy);
            }
        }

        X ff = new X();

        long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
        final long increment = 60000L * 1000;
        Rnd rnd = new Rnd();
        try (TableWriter writer = newTableWriter(new DefaultTestCairoConfiguration(root) {
            @Override
            public @NotNull FilesFacade getFilesFacade() {
                return ff;
            }
        }, PRODUCT, metrics)) {

            ts = populateProducts(writer, rnd, ts, N, increment);
            writer.commit();

            populateProducts(writer, rnd, ts, N, increment);

            Assert.assertEquals(2 * N, writer.size());
            writer.rollback();

            Assert.assertTrue(ff.removeAttempted);

            // make sure row rollback works after rollback
            writer.newRow(ts).cancel();

            // we should be able to repeat timestamps
            populateProducts(writer, rnd, ts, N, increment);
            writer.commit();

            Assert.assertEquals(2 * N, writer.size());
        }
    }

    @Test
    public void testRollbackPartitioned() throws Exception {
        int N = 20000;
        create(FF, PartitionBy.DAY, N);
        testRollback(N);
    }

    @Test
    public void testRollbackRestoreNullSetters() throws Exception {
        assertMemoryLeak(() -> {
            create(FF, PartitionBy.DAY, 1);
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
            Rnd rnd = new Rnd();
            final long increment = 60000L * 1000L;
            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                ts = populateProducts(writer, rnd, ts, 5, increment);
                writer.commit();

                long maxTs = ts;
                ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
                populateProducts(writer, rnd, ts, 5, increment);

                writer.rollback();

                ts = maxTs;
                int nullRows = 10;
                for (int i = 0; i < nullRows; i++) {
                    TableWriter.Row row = writer.newRow(ts);
                    row.append();
                }

                writer.commit();
            }

            assertTable("productId\tproductName\tsupplier\tcategory\tprice\tlocationByte\tlocationShort\tlocationInt\tlocationLong\ttimestamp\n" +
                    "1148479920\tTJWCPSW\tHYRX\tPEHNRXGZSXU\t0.4621835429127854\tq\ttp0\tttmt7w\tcs4bdw4y4dpw\t2013-03-04T00:01:00.000000Z\n" +
                    "761275053\tHBHFOWL\tPDXY\tSBEOUOJSHRU\t0.6761934857077543\tf\t6js\tu0x8u6\twc8jw257kp8b\t2013-03-04T00:02:00.000000Z\n" +
                    "2034804966\tYRFBVTM\tHGOO\tZZVDZJMYICC\t0.2282233596526786\tp\tp16\t5ehgu7\tn5f7bnz2wzkr\t2013-03-04T00:03:00.000000Z\n" +
                    "1775935667\tEDYYCTG\tQOLY\tXWCKYLSUWDS\t0.2820020716674768\tr\t2q2\tcsded1\tvqnqb4qjen3k\t2013-03-04T00:04:00.000000Z\n" +
                    "68027832\tKJSMSSU\tQSRL\tTKVVSJOJIPH\t0.13006100084163252\t2\t06j\tuxnz7u\tpp3dqy3z5fzc\t2013-03-04T00:05:00.000000Z\n" +
                    "NaN\t\t\t\tNaN\t\t\t\t\t2013-03-04T00:05:00.000000Z\n" +
                    "NaN\t\t\t\tNaN\t\t\t\t\t2013-03-04T00:05:00.000000Z\n" +
                    "NaN\t\t\t\tNaN\t\t\t\t\t2013-03-04T00:05:00.000000Z\n" +
                    "NaN\t\t\t\tNaN\t\t\t\t\t2013-03-04T00:05:00.000000Z\n" +
                    "NaN\t\t\t\tNaN\t\t\t\t\t2013-03-04T00:05:00.000000Z\n" +
                    "NaN\t\t\t\tNaN\t\t\t\t\t2013-03-04T00:05:00.000000Z\n" +
                    "NaN\t\t\t\tNaN\t\t\t\t\t2013-03-04T00:05:00.000000Z\n" +
                    "NaN\t\t\t\tNaN\t\t\t\t\t2013-03-04T00:05:00.000000Z\n" +
                    "NaN\t\t\t\tNaN\t\t\t\t\t2013-03-04T00:05:00.000000Z\n" +
                    "NaN\t\t\t\tNaN\t\t\t\t\t2013-03-04T00:05:00.000000Z\n", PRODUCT);
        });
    }

    @Test
    public void testSetAppendPositionFailureBin2() throws Exception {
        testSetAppendPositionFailure();
    }

    @Test
    public void testSinglePartitionTruncate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.YEAR, 4);

            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                writer.truncate();
                Assert.assertEquals(0, writer.size());
            }

            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                Assert.assertEquals(0, writer.size());
            }
        });
    }

    @Test
    public void testSkipOverSpuriousDir() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.DAY, 10);

            try (Path path = new Path()) {
                // create random directories
                FilesFacade ff = configuration.getFilesFacade();

                path.of(configuration.getRoot()).concat(PRODUCT_FS).concat("somethingortheother").slash$();
                Assert.assertEquals(0, ff.mkdirs(path, configuration.getMkDirMode()));

                path.of(configuration.getRoot()).concat(PRODUCT_FS).concat("default").slash$();
                Assert.assertEquals(0, ff.mkdirs(path, configuration.getMkDirMode()));

                path.of(configuration.getRoot()).concat(PRODUCT_FS).concat("0001-01-01.123").slash$();
                Assert.assertEquals(0, ff.mkdirs(path, configuration.getMkDirMode()));

                path.of(configuration.getRoot()).concat(PRODUCT).concat(WalUtils.SEQ_DIR).slash$();
                Assert.assertEquals(0, ff.mkdirs(path, configuration.getMkDirMode()));

                path.of(configuration.getRoot()).concat(PRODUCT).concat(WalUtils.SEQ_DIR_DEPRECATED).slash$();
                Assert.assertEquals(0, ff.mkdirs(path, configuration.getMkDirMode()));

                newTableWriter(configuration, PRODUCT, metrics).close();

                path.of(configuration.getRoot()).concat(PRODUCT_FS).concat("default").slash$();
                Assert.assertTrue(ff.exists(path));

                path.of(configuration.getRoot()).concat(PRODUCT_FS).concat("somethingortheother").slash$();
                Assert.assertTrue(ff.exists(path));

                path.of(configuration.getRoot()).concat(PRODUCT_FS).concat("0001-01-01.123").slash$();
                Assert.assertFalse(ff.exists(path));

                path.of(configuration.getRoot()).concat(PRODUCT).concat(WalUtils.SEQ_DIR).slash$();
                Assert.assertTrue(ff.exists(path));

                path.of(configuration.getRoot()).concat(PRODUCT).concat(WalUtils.SEQ_DIR_DEPRECATED).slash$();
                Assert.assertTrue(ff.exists(path));
            }
        });
    }

    @Test
    public void testSkipOverSpuriousDirNonPartitioned() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.NONE, 10);

            try (Path path = new Path()) {
                // create random directories
                FilesFacade ff = configuration.getFilesFacade();

                path.of(configuration.getRoot()).concat(PRODUCT).concat("somethingortheother").slash$();
                Assert.assertEquals(0, ff.mkdirs(path, configuration.getMkDirMode()));

                path.of(configuration.getRoot()).concat(PRODUCT).concat("default").slash$();
                Assert.assertEquals(0, ff.mkdirs(path, configuration.getMkDirMode()));

                path.of(configuration.getRoot()).concat(PRODUCT).concat("0001-01-01.123").slash$();
                Assert.assertEquals(0, ff.mkdirs(path, configuration.getMkDirMode()));

                path.of(configuration.getRoot()).concat(PRODUCT).concat(WalUtils.SEQ_DIR).slash$();
                Assert.assertEquals(0, ff.mkdirs(path, configuration.getMkDirMode()));

                path.of(configuration.getRoot()).concat(PRODUCT).concat(WalUtils.SEQ_DIR_DEPRECATED).slash$();
                Assert.assertEquals(0, ff.mkdirs(path, configuration.getMkDirMode()));

                newTableWriter(configuration, PRODUCT, metrics).close();

                path.of(configuration.getRoot()).concat(PRODUCT).concat("default").slash$();
                Assert.assertTrue(ff.exists(path));

                path.of(configuration.getRoot()).concat(PRODUCT).concat("somethingortheother").slash$();
                Assert.assertTrue(ff.exists(path));

                path.of(configuration.getRoot()).concat(PRODUCT).concat("0001-01-01.123").slash$();
                Assert.assertTrue(ff.exists(path));

                path.of(configuration.getRoot()).concat(PRODUCT).concat(WalUtils.SEQ_DIR).slash$();
                Assert.assertTrue(ff.exists(path));

                path.of(configuration.getRoot()).concat(PRODUCT).concat(WalUtils.SEQ_DIR_DEPRECATED).slash$();
                Assert.assertTrue(ff.exists(path));
            }
        });
    }

    @Test
    public void testTableDoesNotExist() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                newTableWriter(configuration, PRODUCT, metrics).close();
                Assert.fail();
            } catch (CairoException e) {
                LOG.info().$((Sinkable) e).$();
            }
        });
    }

    @Test
    public void testTableExtensionCallback() throws Exception {
        TestUtils.assertMemoryLeak(
                () -> {
                    TableToken tableToken;
                    try (TableModel model = new TableModel(configuration, "xyz", PartitionBy.HOUR).col("x", ColumnType.LONG).timestamp()) {
                        tableToken = TestUtils.create(model, engine);
                    }

                    final Rnd rnd = new Rnd();
                    final ObjList<String> timestampsReported = new ObjList<>();
                    try (TableWriter w = getWriter(tableToken)) {
                        w.setExtensionListener(
                                timestamp -> timestampsReported.add(Timestamps.toString(timestamp))
                        );

                        TableWriter.Row r = w.newRow(TimestampFormatUtils.parseTimestamp("2022-03-10T10:11:00.000000Z"));
                        r.putLong(0, rnd.nextLong());
                        r.append();
                        w.commit();

                        r = w.newRow(TimestampFormatUtils.parseTimestamp("2022-03-10T12:22:00.000000Z"));
                        r.putLong(0, rnd.nextLong());
                        r.append();
                        w.commit();

                        // O3
                        r = w.newRow(TimestampFormatUtils.parseTimestamp("2022-03-10T11:34:00.000000Z"));
                        r.putLong(0, rnd.nextLong());
                        r.append();
                        w.commit();

                        // O3, one old one new
                        r = w.newRow(TimestampFormatUtils.parseTimestamp("2022-03-10T11:44:00.000000Z"));
                        r.putLong(0, rnd.nextLong());
                        r.append();

                        r = w.newRow(TimestampFormatUtils.parseTimestamp("2022-03-10T13:22:00.000000Z"));
                        r.putLong(0, rnd.nextLong());
                        r.append();
                        w.commit();

                        // O3 - very old
                        r = w.newRow(TimestampFormatUtils.parseTimestamp("2022-03-10T08:11:00.000000Z"));
                        r.putLong(0, rnd.nextLong());
                        r.append();
                        w.commit();
                    }
                    engine.releaseAllWriters();

                    TestUtils.assertEquals(
                            "[2022-03-10T10:00:00.000Z,2022-03-10T12:00:00.000Z,2022-03-10T13:00:00.000Z]",
                            timestampsReported
                    );
                }
        );
    }

    @Test
    public void testTableLock() {
        CreateTableTestUtils.createAllTable(engine, PartitionBy.NONE);

        try (TableWriter ignored = newTableWriter(configuration, "all", metrics)) {
            try {
                newTableWriter(configuration, "all", metrics).close();
                Assert.fail();
            } catch (CairoException ignored2) {
            }
        }
    }

    @Test
    public void testTableWriterCustomPageSize() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.DAY, 10000);
            CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public long getDataAppendPageSize() {
                    return getFilesFacade().getPageSize();
                }
            };
            try (TableWriter w = newTableWriter(configuration, PRODUCT, metrics)) {
                long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

                Rnd rnd = new Rnd();
                for (int i = 0; i < 100; i++) {
                    ts = populateRow(w, rnd, ts, 60L * 60000L * 1000L);
                }
                w.commit();

                for (int i = 0, n = w.getColumnCount(); i < n; i++) {
                    MemoryMA m = w.getStorageColumn(i);
                    if (m != null) {
                        Assert.assertEquals(configuration.getDataAppendPageSize(), m.getExtendSegmentSize());
                    }
                }
            }
        });
    }

    @Test
    public void testToString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.NONE, 4);
            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                Assert.assertEquals("TableWriter{name=product}", writer.toString());
            }
        });
    }

    @Test
    public void testTruncateMidO3Transaction() throws NumericException {
        testTruncate(TableWriterTest::danglingO3TransactionModifier);
    }

    @Test
    public void testTruncateMidRowAppend() throws NumericException {
        testTruncate(TableWriterTest::danglingRowModifier);
    }

    @Test
    public void testTruncateMidTransaction() throws NumericException {
        testTruncate(TableWriterTest::danglingTransactionModifier);
    }

    @Test
    public void testTwoByteUtf8() {
        String name = "соотечественник";
        try (TableModel model = new TableModel(configuration, name, PartitionBy.NONE)
                .col("секьюрити", ColumnType.STRING)
                .timestamp()) {
            CreateTableTestUtils.create(model);
        }

        Rnd rnd = new Rnd();
        try (TableWriter writer = newTableWriter(configuration, name, metrics)) {
            for (int i = 0; i < 1000000; i++) {
                TableWriter.Row r = writer.newRow();
                r.putStr(0, rnd.nextChars(5));
                r.append();
            }
            writer.commit();
            writer.addColumn("митинг", ColumnType.INT);
            Assert.assertEquals(0, writer.getColumnIndex("секьюрити"));
            Assert.assertEquals(2, writer.getColumnIndex("митинг"));
        }

        rnd.reset();
        try (TableReader reader = newTableReader(configuration, name)) {
            int col = reader.getMetadata().getColumnIndex("секьюрити");
            RecordCursor cursor = reader.getCursor();
            final Record r = cursor.getRecord();
            while (cursor.hasNext()) {
                TestUtils.assertEquals(rnd.nextChars(5), r.getStr(col));
            }
        }
    }

    @Test
    public void testTxCannotMap() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            class X extends CountingFilesFacade {
                @Override
                public long mmap(int fd, long len, long offset, int flags, int memoryTag) {
                    if (--count > 0) {
                        return super.mmap(fd, len, offset, flags, memoryTag);
                    }
                    return -1;
                }
            }
            X ff = new X();
            create(ff, PartitionBy.NONE, 4);
            try {
                ff.count = 0;
                newTableWriter(new DefaultTestCairoConfiguration(root) {
                    @Override
                    public @NotNull FilesFacade getFilesFacade() {
                        return ff;
                    }
                }, PRODUCT, metrics).close();
                Assert.fail();
            } catch (CairoException ignore) {
            }
        });
    }

    @Test
    public void testTxFileDoesNotExist() throws Exception {
        testConstructor(new TestFilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                return !Utf8s.endsWithAscii(path, TableUtils.TXN_FILE_NAME) && super.exists(path);
            }
        });
    }

    @Test
    public void testUnCachedSymbol() {
        testSymbolCacheFlag(false);
    }

    @Test
    public void testUtf8() {
        String name = "utf8";
        try (TableModel model = new TableModel(configuration, name, PartitionBy.NONE)
                .col("str", ColumnType.STRING)
                .col("sym", ColumnType.SYMBOL)
                .timestamp()) {
            CreateTableTestUtils.create(model);
        }
        String something = "Щось";
        String boring = "Таке-Сяке";
        try (TableWriter writer = newTableWriter(configuration, name, metrics)) {
            TableWriter.Row r = writer.newRow();
            TestUtils.putUtf8(r, something, 0, false);
            try {
                // putSymUtf8 is not implemented for TableWriter
                TestUtils.putUtf8(r, boring, 1, true);
                Assert.fail("UnsupportedOperationException");
            } catch (UnsupportedOperationException ignore) {
            }
            r.putSym(1, boring);
            r.append();
            writer.commit();
        }

        try (TableReader reader = newTableReader(configuration, name)) {
            RecordCursor cursor = reader.getCursor();
            final Record r = cursor.getRecord();
            while (cursor.hasNext()) {
                TestUtils.assertEquals(something, r.getStr(0).toString());
                TestUtils.assertEquals(boring, r.getSym(1).toString());
            }
        }
    }

    private static void danglingO3TransactionModifier(TableWriter w, Rnd rnd, long timestamp, long increment) {
        TableWriter.Row r = w.newRow(timestamp - increment * 4);
        r.putSym(0, rnd.nextString(5));
        r.putSym(1, rnd.nextString(5));
        r.append();

        r = w.newRow(timestamp - increment * 8);
        r.putSym(0, rnd.nextString(5));
        r.putSym(1, rnd.nextString(5));
        r.append();
    }

    private static void danglingRowModifier(TableWriter w, Rnd rnd, long timestamp, long increment) {
        TableWriter.Row r = w.newRow(timestamp);
        r.putSym(0, rnd.nextString(5));
        r.putSym(1, rnd.nextString(5));
    }

    private static void danglingTransactionModifier(TableWriter w, Rnd rnd, long timestamp, long increment) {
        TableWriter.Row r = w.newRow(timestamp);
        r.putSym(0, rnd.nextString(5));
        r.putSym(1, rnd.nextString(5));
        r.append();
    }

    private static long populateRow(TableWriter writer, Rnd rnd, long ts, long increment) {
        TableWriter.Row r = writer.newRow(ts += increment);
        r.putInt(0, rnd.nextPositiveInt());  // productId
        r.putStr(1, rnd.nextString(7)); // productName
        r.putSym(2, rnd.nextString(4)); // supplier
        r.putSym(3, rnd.nextString(11)); // category
        r.putDouble(4, rnd.nextDouble()); // price
        r.putByte(5, rnd.nextGeoHashByte(5)); // locationByte
        r.putShort(6, rnd.nextGeoHashShort(15)); // locationShort
        r.putInt(7, rnd.nextGeoHashInt(30)); // locationInt
        r.putLong(8, rnd.nextGeoHashLong(60)); // locationLong
        r.append();
        return ts;
    }

    private long append10KNoSupplier(long ts, Rnd rnd, TableWriter writer) {
        int productId = writer.getColumnIndex("productId");
        int productName = writer.getColumnIndex("productName");
        int category = writer.getColumnIndex("category");
        int price = writer.getColumnIndex("price");
        boolean isSym = ColumnType.isSymbol(writer.getMetadata().getColumnType(productName));

        for (int i = 0; i < 10000; i++) {
            TableWriter.Row r = writer.newRow(ts += 60000L * 1000L);
            r.putInt(productId, rnd.nextPositiveInt());
            if (!isSym) {
                r.putStr(productName, rnd.nextString(4));
            } else {
                r.putSym(productName, rnd.nextString(4));
            }
            r.putSym(category, rnd.nextString(11));
            r.putDouble(price, rnd.nextDouble());
            r.append();
        }
        return ts;
    }

    private void append10KNoTimestamp(Rnd rnd, TableWriter writer) {
        int productId = writer.getColumnIndex("productId");
        int productName = writer.getColumnIndex("productName");
        int supplier = writer.getColumnIndex("supplier");
        int category = writer.getColumnIndex("category");
        int price = writer.getColumnIndex("price");

        for (int i = 0; i < 10000; i++) {
            TableWriter.Row r = writer.newRow();
            r.putInt(productId, rnd.nextPositiveInt());
            r.putStr(productName, rnd.nextString(10));
            r.putSym(supplier, rnd.nextString(4));
            r.putSym(category, rnd.nextString(11));
            r.putDouble(price, rnd.nextDouble());
            r.append();
        }
    }

    private long append10KProducts(long ts, Rnd rnd, TableWriter writer) {
        int productId = writer.getColumnIndex("productId");
        int productName = writer.getColumnIndex("productName");
        int supplier = writer.getColumnIndex("supplier");
        int category = writer.getColumnIndex("category");
        int price = writer.getColumnIndex("price");
        boolean isSym = ColumnType.isSymbol(writer.getMetadata().getColumnType(productName));

        for (int i = 0; i < 10000; i++) {
            TableWriter.Row r = writer.newRow(ts += 60000L * 1000L);
            r.putInt(productId, rnd.nextPositiveInt());
            if (!isSym) {
                r.putStr(productName, rnd.nextString(4));
            } else {
                r.putSym(productName, rnd.nextString(4));
            }
            r.putSym(supplier, rnd.nextString(4));
            r.putSym(category, rnd.nextString(11));
            r.putDouble(price, rnd.nextDouble());
            r.append();
        }

        return ts;
    }

    private long append10KWithNewName(long ts, Rnd rnd, TableWriter writer) {
        int productId = writer.getColumnIndex("productId");
        int productName = writer.getColumnIndex("productName");
        int supplier = writer.getColumnIndex("sup");
        int category = writer.getColumnIndex("category");
        int price = writer.getColumnIndex("price");
        boolean isSym = ColumnType.isSymbol(writer.getMetadata().getColumnType(productName));

        for (int i = 0; i < 10000; i++) {
            TableWriter.Row r = writer.newRow(ts += 60000L * 1000L);
            r.putInt(productId, rnd.nextPositiveInt());
            if (!isSym) {
                r.putStr(productName, rnd.nextString(4));
            } else {
                r.putSym(productName, rnd.nextString(4));
            }
            r.putSym(supplier, rnd.nextString(4));
            r.putSym(category, rnd.nextString(11));
            r.putDouble(price, rnd.nextDouble());
            r.append();
        }
        return ts;
    }

    private void appendAndAssert10K(long ts, Rnd rnd) {
        try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
            Assert.assertEquals(20, writer.getColumnCount());
            populateProducts(writer, rnd, ts, 10000, 60000L * 1000L);
            writer.commit();
            Assert.assertEquals(30000, writer.size());
        }
    }

    private void assertGeoStr(String hash, int tableBits, long expected) {
        final String tableName = "geo1";
        try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)) {
            model.col("g", ColumnType.getGeoHashTypeWithBits(tableBits));
            CreateTableTestUtils.create(model);
        }

        try (TableWriter writer = newTableWriter(configuration, tableName, metrics)) {
            TableWriter.Row r = writer.newRow();
            r.putGeoStr(0, hash);
            r.append();
            writer.commit();
        }

        try (TableReader r = newTableReader(configuration, tableName)) {
            final RecordCursor cursor = r.getCursor();
            final Record record = cursor.getRecord();
            final int type = r.getMetadata().getColumnType(0);
            Assert.assertTrue(cursor.hasNext());
            final long actual;

            switch (ColumnType.tagOf(type)) {
                case ColumnType.GEOBYTE:
                    actual = record.getGeoByte(0);
                    break;
                case ColumnType.GEOSHORT:
                    actual = record.getGeoShort(0);
                    break;
                case ColumnType.GEOINT:
                    actual = record.getGeoInt(0);
                    break;
                default:
                    actual = record.getGeoLong(0);
                    break;
            }
            Assert.assertEquals(expected, actual);
        }
    }

    private void assertIndex(TableReader reader, TableReaderRecord record, int columnIndex) {
        final int partitionIndex = 0;
        reader.openPartition(partitionIndex);
        final BitmapIndexReader indexReader = reader.getBitmapIndexReader(partitionIndex, columnIndex, BitmapIndexReader.DIR_FORWARD);
        final SymbolMapReader r = reader.getSymbolMapReader(columnIndex);
        final int symbolCount = r.getSymbolCount();

        long calculatedRowCount = 0;
        for (int i = 0; i < symbolCount; i++) {
            final RowCursor rowCursor = indexReader.getCursor(true, i + 1, 0, Long.MAX_VALUE);
            while (rowCursor.hasNext()) {
                record.setRecordIndex(Rows.toRowID(partitionIndex, rowCursor.next()));
                Assert.assertEquals(i, record.getInt(columnIndex));
                calculatedRowCount++;
            }
        }
        Assert.assertEquals(reader.size(), calculatedRowCount);
    }

    private void create(FilesFacade ff, int partitionBy, int N) {
        try (TableModel model = new TableModel(new DefaultTestCairoConfiguration(root) {
            @Override
            public @NotNull FilesFacade getFilesFacade() {
                return ff;
            }
        }, PRODUCT, partitionBy)
                .col("productId", ColumnType.INT)
                .col("productName", ColumnType.STRING)
                .col("supplier", ColumnType.SYMBOL).symbolCapacity(N)
                .col("category", ColumnType.SYMBOL).symbolCapacity(N).indexed(true, 256)
                .col("price", ColumnType.DOUBLE)
                .col("locationByte", ColumnType.getGeoHashTypeWithBits(5))
                .col("locationShort", ColumnType.getGeoHashTypeWithBits(15))
                .col("locationInt", ColumnType.getGeoHashTypeWithBits(30))
                .col("locationLong", ColumnType.getGeoHashTypeWithBits(60))
                .timestamp()) {
            CreateTableTestUtils.create(model);
        }
    }

    private int getDirCount() {
        AtomicInteger count = new AtomicInteger();
        try (Path path = new Path()) {
            FF.iterateDir(path.of(root).concat(PRODUCT_FS).$(), (pUtf8NameZ, type) -> {
                if (type == Files.DT_DIR) {
                    count.incrementAndGet();
                }
            });
        }
        return count.get();
    }

    private void populateAndColumnPopulate(int n) throws NumericException {
        Rnd rnd = new Rnd();
        long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
        long interval = 60000L * 1000L;
        try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
            ts = populateProducts(writer, rnd, ts, n, interval);
            writer.commit();

            Assert.assertEquals(n, writer.size());

            writer.addColumn("abc", ColumnType.STRING);

            // add more data including updating new column
            ts = populateTable2(writer, rnd, n, ts, interval);

            writer.commit();

            Assert.assertEquals(2 * n, writer.size());
        }

        // append more
        try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
            populateTable2(writer, rnd, n, ts, interval);
            Assert.assertEquals(3 * n, writer.size());
            writer.commit();
            Assert.assertEquals(3 * n, writer.size());
        }
    }

    private long populateProducts(TableWriter writer, Rnd rnd, long ts, int count, long increment) {
        for (int i = 0; i < count; i++) {
            ts = populateRow(writer, rnd, ts, increment);
        }
        return ts;
    }

    private long populateTable0(FilesFacade ff, int N) throws NumericException {
        try (TableWriter writer = newTableWriter(new DefaultTestCairoConfiguration(root) {
            @Override
            public @NotNull FilesFacade getFilesFacade() {
                return ff;
            }
        }, PRODUCT, metrics)) {
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
            ts = populateProducts(writer, new Rnd(), ts, N, 60000L * 1000L);
            writer.commit();
            Assert.assertEquals(N, writer.size());
            return ts;
        }
    }

    private long populateTable2(TableWriter writer, Rnd rnd, int n, long ts, long interval) {
        for (int i = 0; i < n; i++) {
            ts = populateRow(writer, rnd, ts, interval);
        }
        return ts;
    }

    private void removeColumn(TestFilesFacade ff) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String name = "ABC";
            try (TableModel model = new TableModel(configuration, name, PartitionBy.NONE)
                    .col("productId", ColumnType.INT)
                    .col("productName", ColumnType.STRING)
                    .col("supplier", ColumnType.SYMBOL)
                    .col("category", ColumnType.SYMBOL)
                    .col("price", ColumnType.DOUBLE)
                    .timestamp()) {
                CreateTableTestUtils.create(model);
            }

            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();

            try (TableWriter writer = newTableWriter(new DefaultTestCairoConfiguration(root) {
                @Override
                public @NotNull FilesFacade getFilesFacade() {
                    return ff;
                }
            }, name, metrics)) {

                ts = append10KProducts(ts, rnd, writer);

                writer.removeColumn("supplier");

                // assert attempt to remove files
                Assert.assertTrue(ff.wasCalled());

                ts = append10KNoSupplier(ts, rnd, writer);

                writer.commit();

                Assert.assertEquals(20000, writer.size());
            }

            try (TableWriter writer = newTableWriter(configuration, name, metrics)) {
                append10KNoSupplier(ts, rnd, writer);
                writer.commit();
                Assert.assertEquals(30000, writer.size());
            }
        });
    }

    private void renameColumn(TestFilesFacade ff) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String name = "ABC";
            try (TableModel model = new TableModel(configuration, name, PartitionBy.NONE)
                    .col("productId", ColumnType.INT)
                    .col("productName", ColumnType.STRING)
                    .col("supplier", ColumnType.SYMBOL)
                    .col("category", ColumnType.SYMBOL)
                    .col("price", ColumnType.DOUBLE)
                    .timestamp()) {
                CreateTableTestUtils.create(model);
            }

            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();

            try (TableWriter writer = newTableWriter(new DefaultTestCairoConfiguration(root) {
                @Override
                public @NotNull FilesFacade getFilesFacade() {
                    return ff;
                }
            }, name, metrics)) {

                ts = append10KProducts(ts, rnd, writer);

                writer.renameColumn("supplier", "sup");

                // assert attempt to remove files
                Assert.assertTrue(ff.wasCalled());

                ts = append10KWithNewName(ts, rnd, writer);

                writer.commit();

                Assert.assertEquals(20000, writer.size());
            }

            try (TableWriter writer = newTableWriter(configuration, name, metrics)) {
                append10KWithNewName(ts, rnd, writer);
                writer.commit();
                Assert.assertEquals(30000, writer.size());
            }
        });
    }

    private void testAddColumnAndOpenWriter(int partitionBy, int N) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
            Rnd rnd = new Rnd();

            create(FF, partitionBy, N);
            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                ts = populateProducts(writer, rnd, ts, N, 60L * 60000L * 1000L);
                writer.commit();
                Assert.assertEquals(N, writer.size());
            }

            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                writer.addColumn("xyz", ColumnType.STRING);
            }

            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                for (int i = 0; i < N; i++) {
                    ts = populateRow(writer, rnd, ts, 60L * 60000 * 1000L);
                }
                writer.commit();
                Assert.assertEquals(N * 2, writer.size());
            }
        });
    }

    private void testAddColumnErrorFollowedByRepairFail(FilesFacade ff) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public @NotNull FilesFacade getFilesFacade() {
                    return ff;
                }
            };
            long ts = populateTable();
            Rnd rnd = new Rnd();
            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                ts = populateProducts(writer, rnd, ts, 10000, 60000L * 1000L);
                writer.commit();
                Assert.assertEquals(20000, writer.size());

                Assert.assertEquals(20, writer.getColumnCount());

                try {
                    writer.addColumn("abc", ColumnType.STRING);
                    Assert.fail();
                } catch (CairoError ignore) {
                }
            }

            try {
                newTableWriter(configuration, PRODUCT, metrics).close();
                Assert.fail();
            } catch (CairoException ignore) {
            }

            appendAndAssert10K(ts, rnd);
        });
    }

    private void testAddColumnRecoverableFault(FilesFacade ff) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long ts = populateTable();
            Rnd rnd = new Rnd();
            CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public @NotNull FilesFacade getFilesFacade() {
                    return ff;
                }
            };
            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                Assert.assertEquals(20, writer.getColumnCount());
                ts = populateProducts(writer, rnd, ts, 10000, 60000L * 1000L);
                writer.commit();
                try {
                    writer.addColumn("abc", ColumnType.SYMBOL);
                    Assert.fail();
                } catch (CairoException ignore) {
                }

                // ignore error and add more rows
                ts = populateProducts(writer, rnd, ts, 10000, 60000L * 1000L);
                writer.commit();
                Assert.assertEquals(30000, writer.size());
            }

            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                populateProducts(writer, rnd, ts, 10000, 60000L * 1000L);
                writer.commit();
                Assert.assertEquals(40000, writer.size());
            }
        });
    }

    private void testAddColumnRecoverableNoFault(FilesFacade ff) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long ts = populateTable();
            Rnd rnd = new Rnd();
            CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public @NotNull FilesFacade getFilesFacade() {
                    return ff;
                }
            };
            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                Assert.assertEquals(20, writer.getColumnCount());
                ts = populateProducts(writer, rnd, ts, 10000, 60000L * 1000L);
                writer.commit();
                writer.addColumn("abc", ColumnType.SYMBOL);

                // ignore error and add more rows
                ts = populateProducts(writer, rnd, ts, 10000, 60000L * 1000L);
                writer.commit();
                Assert.assertEquals(30000, writer.size());
            }

            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                populateProducts(writer, rnd, ts, 10000, 60000L * 1000L);
                writer.commit();
                Assert.assertEquals(40000, writer.size());
            }
        });
    }

    private void testAddIndexAndFailToIndexHalfWay(CairoConfiguration configuration, int partitionBy, int N) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
            Rnd rnd = new Rnd();

            create(FF, partitionBy, N);
            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                ts = populateProducts(writer, rnd, ts, N, 60L * 60000L * 1000L);
                writer.commit();
                Assert.assertEquals(N, writer.size());
            }

            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                writer.addIndex("supplier", configuration.getIndexValueBlockSize());
                Assert.fail();
            } catch (CairoException ignored) {
            }

            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                for (int i = 0; i < N; i++) {
                    TableWriter.Row r = writer.newRow(ts += 60L * 60000 * 1000L);
                    r.putInt(0, rnd.nextPositiveInt());
                    r.putStr(1, rnd.nextString(7));
                    r.putSym(2, rnd.nextString(4));
                    r.putSym(3, rnd.nextString(11));
                    r.putDouble(4, rnd.nextDouble());
                    r.append();
                }
                writer.commit();
                Assert.assertEquals(N * 2, writer.size());
            }

            // another attempt to create index
            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                writer.addIndex("supplier", configuration.getIndexValueBlockSize());
            }
        });
    }

    private long testAppendNulls(Rnd rnd, long ts) {
        final int blobLen = 64 * 1024;
        long blob = Unsafe.malloc(blobLen, MemoryTag.NATIVE_DEFAULT);
        try (TableWriter writer = newTableWriter(new DefaultTestCairoConfiguration(root) {
            @Override
            public @NotNull FilesFacade getFilesFacade() {
                return TableWriterTest.FF;
            }
        }, "all", metrics)) {
            long size = writer.size();
            for (int i = 0; i < 10000; i++) {
                TableWriter.Row r = writer.newRow(ts += 60L * 60000L * 1000L);
                if (rnd.nextBoolean()) {
                    r.putByte(2, rnd.nextByte());
                }

                if (rnd.nextBoolean()) {
                    r.putBool(8, rnd.nextBoolean());
                }

                if (rnd.nextBoolean()) {
                    r.putShort(1, rnd.nextShort());
                }

                if (rnd.nextBoolean()) {
                    r.putInt(0, rnd.nextInt());
                }

                if (rnd.nextBoolean()) {
                    r.putDouble(3, rnd.nextDouble());
                }

                if (rnd.nextBoolean()) {
                    r.putFloat(4, rnd.nextFloat());
                }

                if (rnd.nextBoolean()) {
                    r.putLong(5, rnd.nextLong());
                }

                if (rnd.nextBoolean()) {
                    r.putDate(10, ts);
                }

                if (rnd.nextBoolean()) {
                    rnd.nextChars(blob, blobLen / 2);
                    r.putBin(9, blob, blobLen);
                }

                r.append();
            }
            writer.commit();

            Assert.assertFalse(writer.inTransaction());
            Assert.assertEquals(size + 10000, writer.size());
        } finally {
            Unsafe.free(blob, blobLen, MemoryTag.NATIVE_DEFAULT);
        }
        return ts;
    }

    private void testConstructor(FilesFacade ff) throws Exception {
        testConstructor(ff, true);
    }

    private void testConstructor(FilesFacade ff, boolean create) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            if (create) {
                create(ff, PartitionBy.NONE, 4);
            }
            try {
                newTableWriter(new DefaultTestCairoConfiguration(root) {
                    @Override
                    public @NotNull FilesFacade getFilesFacade() {
                        return ff;
                    }
                }, PRODUCT, metrics).close();
                Assert.fail();
            } catch (CairoException e) {
                LOG.info().$((Sinkable) e).$();
            } catch (CairoError e) {
                LOG.info().$(e.getFlyweightMessage()).$();
            }
        });
    }

    private void testIndexIsAddedToTableAppendData(int N, Rnd rnd, long t, long increment, TableWriter w) {
        for (int i = 0; i < N; i++) {
            TableWriter.Row r = w.newRow(t);
            r.putSym(0, rnd.nextString(5));
            r.putSym(1, rnd.nextString(5));
            r.putSym(2, rnd.nextString(5));
            t += increment;
            r.append();
        }
    }

    private void testO3RecordsFail(int N) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {

                long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

                long size = 0;
                Rnd rnd = new Rnd();
                int i = 0;
                long failureCount = 0;
                while (i < N) {
                    TableWriter.Row r;
                    boolean fail = rnd.nextBoolean();
                    if (fail) {
                        try {
                            writer.newRow();
                            Assert.fail();
                        } catch (CairoException ignore) {
                            failureCount++;
                        }
                        continue;
                    } else {
                        ts += 60 * 6000L * 1000L;
                        r = writer.newRow(ts);
                    }
                    r.putInt(0, rnd.nextPositiveInt());
                    r.putStr(1, rnd.nextString(7));
                    r.putSym(2, rnd.nextString(4));
                    r.putSym(3, rnd.nextString(11));
                    r.putDouble(4, rnd.nextDouble());
                    r.append();
                    Assert.assertEquals(size + 1, writer.size());
                    size = writer.size();
                    i++;
                }
                writer.commit();
                Assert.assertEquals(N, writer.size());
                Assert.assertTrue(failureCount > 0);
            }

            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                Assert.assertEquals(N, writer.size());
            }
        });
    }

    private void testO3RecordsNewerThanOlder(int N, CairoConfiguration configuration) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {

                long ts;
                long ts1 = TimestampFormatUtils.parseTimestamp("2013-03-04T04:00:00.000Z");
                long ts2 = TimestampFormatUtils.parseTimestamp("2013-03-04T02:00:00.000Z");

                Rnd rnd = new Rnd();
                int i = 0;
                while (i < N) {
                    TableWriter.Row r;
                    if (i > N / 2) {
                        ts2 += 60 * 1000L;
                        ts = ts2;
                    } else {
                        ts1 += 60 * 1000L;
                        ts = ts1;
                    }
                    r = writer.newRow(ts);
                    r.putInt(0, rnd.nextPositiveInt());
                    r.putStr(1, rnd.nextString(7));
                    r.putSym(2, rnd.nextString(4));
                    r.putSym(3, rnd.nextString(11));
                    r.putDouble(4, rnd.nextDouble());
                    r.append();
                    i++;
                }
                writer.commit();
                Assert.assertEquals(N, writer.size());
            }

            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                Assert.assertEquals(N, writer.size());
            }
        });
    }

    private void testRemoveColumn(TableModel model) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CreateTableTestUtils.create(model);
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();
            try (TableWriter writer = newTableWriter(configuration, model.getName(), metrics)) {

                // optional
                writer.warmUp();

                ts = append10KProducts(ts, rnd, writer);

                writer.removeColumn("supplier");

                try (Path path = new Path()) {
                    path.of(root).concat(model.getName());
                    final int plen = path.size();
                    FF.iterateDir(path.$(), (pUtf8NameZ, type) -> {
                        if (FF.isDirOrSoftLinkDirNoDots(path, plen, pUtf8NameZ, type)) {
                            int nlen = path.size();
                            Assert.assertFalse(FF.exists(path.trimTo(nlen).concat("supplier.i").$()));
                            Assert.assertFalse(FF.exists(path.trimTo(nlen).concat("supplier.d").$()));
                            Assert.assertFalse(FF.exists(path.trimTo(nlen).concat("supplier.top").$()));
                        }
                    });
                }

                ts = append10KNoSupplier(ts, rnd, writer);

                writer.commit();

                Assert.assertEquals(20000, writer.size());
            }

            try (TableWriter writer = newTableWriter(configuration, model.getName(), metrics)) {
                append10KNoSupplier(ts, rnd, writer);
                writer.commit();
                Assert.assertEquals(30000, writer.size());
            }
        });
    }

    private void testRemoveColumnRecoverableFailure(TestFilesFacade ff) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.DAY, 10000);
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
            Rnd rnd = new Rnd();
            try (TableWriter writer = newTableWriter(new DefaultTestCairoConfiguration(root) {
                @Override
                public @NotNull FilesFacade getFilesFacade() {
                    return ff;
                }
            }, PRODUCT, metrics)) {
                ts = append10KProducts(ts, rnd, writer);
                writer.commit();

                try {
                    writer.removeColumn("productName");
                    Assert.fail();
                } catch (CairoException ignore) {
                }

                Assert.assertTrue(ff.wasCalled());

                ts = append10KProducts(ts, rnd, writer);
                writer.commit();
            }

            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                append10KProducts(ts, rnd, writer);
                writer.commit();
                Assert.assertEquals(30000, writer.size());
            }
        });
    }

    private void testRenameColumn(TableModel model) throws Exception {
        assertMemoryLeak(() -> {
            CreateTableTestUtils.create(model);
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();
            int columnTypeTag;
            TableToken tableToken;
            try (TableWriter writer = getWriter(model.getName())) {

                // optional
                writer.warmUp();

                ts = append10KProducts(ts, rnd, writer);
                columnTypeTag = ColumnType.tagOf(writer.getMetadata().getColumnType("supplier"));
                writer.renameColumn("supplier", "sup");

                try (Path path = new Path()) {
                    tableToken = writer.getTableToken();
                    path.of(root).concat(tableToken);
                    final int plen = path.size();
                    long colVersion = writer.getTxn() - 1;

                    if (columnTypeTag == ColumnType.SYMBOL) {
                        Assert.assertTrue(FF.exists(path.trimTo(plen).concat("sup.v." + colVersion).$()));
                        Assert.assertTrue(FF.exists(path.trimTo(plen).concat("sup.o." + colVersion).$()));
                        Assert.assertTrue(FF.exists(path.trimTo(plen).concat("sup.c." + colVersion).$()));
                        Assert.assertTrue(FF.exists(path.trimTo(plen).concat("sup.k." + colVersion).$()));
                    }
                    path.trimTo(plen);
                    FF.iterateDir(path.$(), (pUtf8NameZ, type) -> {
                        if (FF.isDirOrSoftLinkDirNoDots(path, plen, pUtf8NameZ, type)) {
                            int nlen = path.size();
                            Assert.assertTrue(FF.exists(path.trimTo(nlen).concat("sup.d." + colVersion).$()));
                            if (columnTypeTag == ColumnType.BINARY || columnTypeTag == ColumnType.STRING) {
                                Assert.assertTrue(FF.exists(path.trimTo(nlen).concat("sup.i." + colVersion).$()));
                            }
                        }
                    });
                }

                ts = append10KWithNewName(ts, rnd, writer);

                writer.commit();
                Assert.assertEquals(20000, writer.size());
            }

            engine.releaseInactive();
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                purgeJob.run(0);
            }

            try (Path path = new Path()) {
                path.of(root).concat(tableToken);
                final int plen = path.size();
                Assert.assertFalse(FF.exists(path.trimTo(plen).concat("supplier.v").$()));
                Assert.assertFalse(FF.exists(path.trimTo(plen).concat("supplier.o").$()));
                Assert.assertFalse(FF.exists(path.trimTo(plen).concat("supplier.c").$()));
                Assert.assertFalse(FF.exists(path.trimTo(plen).concat("supplier.k").$()));
                FF.iterateDir(path.$(), (pUtf8NameZ, type) -> {
                    if (FF.isDirOrSoftLinkDirNoDots(path, plen, pUtf8NameZ, type)) {
                        int nlen = path.size();
                        Assert.assertFalse(FF.exists(path.trimTo(nlen).concat("supplier.d").$()));
                        if (columnTypeTag == ColumnType.BINARY || columnTypeTag == ColumnType.STRING) {
                            Assert.assertFalse(FF.exists(path.trimTo(nlen).concat("supplier.i").$()));
                        }
                    }
                });
            }

            try (TableWriter writer = getWriter(model.getName())) {
                append10KWithNewName(ts, rnd, writer);
                writer.commit();
                Assert.assertEquals(30000, writer.size());
            }
        });
    }

    private void testRenameColumnRecoverableFailure(TestFilesFacade ff) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.DAY, 10000);
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
            Rnd rnd = new Rnd();
            try (TableWriter writer = newTableWriter(new DefaultTestCairoConfiguration(root) {
                @Override
                public @NotNull FilesFacade getFilesFacade() {
                    return ff;
                }
            }, PRODUCT, metrics)) {
                ts = append10KProducts(ts, rnd, writer);
                writer.commit();

                try {
                    writer.renameColumn("productName", "nameOfProduct");
                    Assert.fail();
                } catch (CairoException ignore) {
                }

                Assert.assertTrue(ff.wasCalled());

                ts = append10KProducts(ts, rnd, writer);
                writer.commit();
            }

            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                append10KProducts(ts, rnd, writer);
                writer.commit();
                Assert.assertEquals(30000, writer.size());
            }
        });
    }

    private void testRollback(int N) throws NumericException {
        long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
        Rnd rnd = new Rnd();
        final long increment = 60000L * 1000L;
        try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
            ts = populateProducts(writer, rnd, ts, N / 2, increment);
            writer.commit();

            populateProducts(writer, rnd, ts, N / 2, increment);

            Assert.assertEquals(N, writer.size());
            writer.rollback();
            Assert.assertEquals(N / 2, writer.size());
            writer.rollback();
            Assert.assertEquals(N / 2, writer.size());

            // make sure row rollback works after rollback
            writer.newRow(ts).cancel();

            // we should be able to repeat timestamps
            populateProducts(writer, rnd, ts, N / 2, increment);
            writer.commit();

            Assert.assertEquals(N, writer.size());
        }
    }

    private void testSetAppendPositionFailure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CreateTableTestUtils.createAllTable(engine, PartitionBy.NONE);

            class X extends FilesFacadeImpl {
                int fd = -1;

                @Override
                public boolean allocate(int fd, long size) {
                    if (fd == this.fd) {
                        return false;
                    }
                    return super.allocate(fd, size);
                }

                @Override
                public int openRW(LPSZ name, long opts) {
                    if (Utf8s.endsWithAscii(name, "bin.i")) {
                        return fd = super.openRW(name, opts);
                    }
                    return super.openRW(name, opts);
                }
            }
            final X ff = new X();
            testAppendNulls(new Rnd(), TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z"));
            try {
                newTableWriter(new DefaultTestCairoConfiguration(root) {
                    @Override
                    public @NotNull FilesFacade getFilesFacade() {
                        return ff;
                    }
                }, "all", metrics).close();
                Assert.fail();
            } catch (CairoException | CairoError ignore) {
            }
        });
    }

    private void testSymbolCacheFlag(boolean cacheFlag) {
        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("a", ColumnType.SYMBOL).cached(cacheFlag)
                .col("b", ColumnType.STRING)
                .col("c", ColumnType.SYMBOL).cached(!cacheFlag)
                .timestamp()) {
            CreateTableTestUtils.create(model);
        }

        int N = 1000;
        Rnd rnd = new Rnd();
        try (TableWriter writer = newTableWriter(configuration, "x", metrics)) {
            Assert.assertEquals(cacheFlag, writer.isSymbolMapWriterCached(0));
            Assert.assertNotEquals(cacheFlag, writer.isSymbolMapWriterCached(2));
            for (int i = 0; i < N; i++) {
                TableWriter.Row r = writer.newRow();
                r.putSym(0, rnd.nextChars(5));
                r.putStr(1, rnd.nextChars(10));
                r.append();
            }
            writer.commit();
        }

        try (TableReader reader = newTableReader(configuration, "x")) {
            rnd.reset();
            int count = 0;
            Assert.assertEquals(cacheFlag, reader.isColumnCached(0));
            Assert.assertNotEquals(cacheFlag, reader.isColumnCached(2));
            RecordCursor cursor = reader.getCursor();
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                TestUtils.assertEquals(rnd.nextChars(5), record.getSym(0));
                TestUtils.assertEquals(rnd.nextChars(10), record.getStr(1));
                count++;
            }

            Assert.assertEquals(N, count);
        }
    }

    private void testTruncate(TruncateModifier modifier) throws NumericException {
        int partitionBy = PartitionBy.DAY;
        int N = 1000;
        try (TableModel model = new TableModel(configuration, "test", partitionBy)) {
            model.col("sym1", ColumnType.SYMBOL);
            model.col("sym2", ColumnType.SYMBOL);
            model.col("sym3", ColumnType.SYMBOL);
            model.timestamp();

            TableToken tableToken = CreateTableTestUtils.create(model);

            // insert data
            final Rnd rnd = new Rnd();
            long t = TimestampFormatUtils.parseTimestamp("2019-03-22T00:00:00.000000Z");
            long increment = 2_000_000;
            try (TableWriter w = getWriter(tableToken)) {
                testIndexIsAddedToTableAppendData(N, rnd, t, increment, w);
                w.commit();

                long t1 = t;
                for (int i = 0; i < N / 2; i++) {
                    TableWriter.Row r = w.newRow(t1);
                    r.putSym(0, rnd.nextString(5));
                    r.putSym(1, rnd.nextString(5));
                    r.putSym(2, rnd.nextString(5));
                    t1 += increment;
                    r.append();
                }

                // modifier enters TableWriter in different states from which
                // truncate() call must be able to recover
                modifier.modify(w, rnd, t1, increment);

                // truncate writer mid-row-append
                w.truncate();

                // add a couple of indexes
                w.addIndex("sym1", 1024);
                w.addIndex("sym2", 1024);

                Assert.assertTrue(w.getMetadata().isColumnIndexed(0));
                Assert.assertTrue(w.getMetadata().isColumnIndexed(1));
                Assert.assertFalse(w.getMetadata().isColumnIndexed(2));

                // here we reset random to ensure we re-insert the same values
                rnd.reset();

                testIndexIsAddedToTableAppendData(N, rnd, t, increment, w);
                w.commit();

                Assert.assertEquals(1, w.getPartitionCount());

                // ensure indexes can be read
                try (TableReader reader = getReader(tableToken)) {
                    final TableReaderRecord record = (TableReaderRecord) reader.getCursor().getRecord();
                    assertIndex(reader, record, 0);
                    assertIndex(reader, record, 1);

                    // check if we can still truncate the writer
                    w.truncate();
                    Assert.assertEquals(0, w.size());
                    Assert.assertTrue(reader.reload());
                    Assert.assertEquals(0, reader.size());
                }

                // truncate again with indexers present
                w.truncate();

                // add the same data again and check indexes
                rnd.reset();

                testIndexIsAddedToTableAppendData(N, rnd, t, increment, w);
                w.commit();

                Assert.assertEquals(1, w.getPartitionCount());

                // ensure indexes can be read
                try (TableReader reader = getReader(tableToken)) {
                    final TableReaderRecord record = (TableReaderRecord) reader.getCursor().getRecord();
                    assertIndex(reader, record, 0);
                    assertIndex(reader, record, 1);

                    // check if we can still truncate the writer
                    w.truncate();
                    Assert.assertEquals(0, w.size());
                    Assert.assertTrue(reader.reload());
                    Assert.assertEquals(0, reader.size());
                }
            }
        }
    }

    private void testTruncateOnClose(TestFilesFacade ff, int N) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TableWriter writer = newTableWriter(new DefaultTestCairoConfiguration(root) {
                @Override
                public @NotNull FilesFacade getFilesFacade() {
                    return ff;
                }
            }, PRODUCT, metrics)) {
                long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
                Rnd rnd = new Rnd();
                populateProducts(writer, rnd, ts, N, 60 * 60000L * 1000L);
                writer.commit();
                Assert.assertEquals(N, writer.size());
            }
            Assert.assertTrue(ff.wasCalled());
        });
    }

    private void testUnrecoverableAddColumn(FilesFacade ff) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long ts = populateTable();
            Rnd rnd = new Rnd();
            try (TableWriter writer = newTableWriter(new DefaultTestCairoConfiguration(root) {
                @Override
                public @NotNull FilesFacade getFilesFacade() {
                    return ff;
                }
            }, PRODUCT, metrics)) {
                ts = populateProducts(writer, rnd, ts, 10000, 60000 * 1000L);
                writer.commit();

                Assert.assertEquals(20, writer.getColumnCount());

                try {
                    writer.addColumn("abc", ColumnType.STRING);
                    Assert.fail();
                } catch (CairoError ignore) {
                }
            }
            appendAndAssert10K(ts, rnd);
        });
    }

    private void testUnrecoverableRemoveColumn(FilesFacade ff) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public @NotNull FilesFacade getFilesFacade() {
                    return ff;
                }
            };
            final int N = 20000;
            create(FF, PartitionBy.DAY, N);
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
            Rnd rnd = new Rnd();
            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                ts = append10KProducts(ts, rnd, writer);
                writer.commit();

                try {
                    writer.removeColumn("supplier");
                    Assert.fail();
                } catch (CairoError ignore) {
                }
            }

            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                append10KProducts(ts, rnd, writer);
                writer.commit();
                Assert.assertEquals(N, writer.size());
            }
        });
    }

    private void testUnrecoverableRenameColumn(FilesFacade ff) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public @NotNull FilesFacade getFilesFacade() {
                    return ff;
                }
            };
            final int N = 20000;
            create(FF, PartitionBy.DAY, N);
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
            Rnd rnd = new Rnd();
            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                ts = append10KProducts(ts, rnd, writer);
                writer.commit();

                try {
                    writer.renameColumn("supplier", "sup");
                    Assert.fail();
                } catch (CairoError ignore) {
                }
            }

            try (TableWriter writer = newTableWriter(configuration, PRODUCT, metrics)) {
                append10KProducts(ts, rnd, writer);
                writer.commit();
                Assert.assertEquals(N, writer.size());
            }
        });
    }

    protected void assertTable(CharSequence expected, CharSequence tableName) {
        try (TableReader reader = newTableReader(configuration, Chars.toString(tableName))) {
            assertCursorTwoPass(expected, reader.getCursor(), reader.getMetadata());
        }
    }

    long populateTable() throws NumericException {
        return populateTable(TableWriterTest.FF, PartitionBy.DAY);
    }

    long populateTable(FilesFacade ff, int partitionBy) throws NumericException {
        int N = 10000;
        long used = Unsafe.getMemUsed();
        long fileCount = ff.getOpenFileCount();
        create(ff, partitionBy, N);
        long ts = populateTable0(ff, N);
        Assert.assertEquals(used, Unsafe.getMemUsed());
        Assert.assertEquals(fileCount, ff.getOpenFileCount());
        return ts;
    }

    void verifyTimestampPartitions(MemoryARW vmem) {
        int i;
        TimestampFormatCompiler compiler = new TimestampFormatCompiler();
        DateFormat fmt = compiler.compile("yyyy-MM-dd");
        DateLocale enGb = DateLocaleFactory.INSTANCE.getLocale("en-gb");

        try (Path vp = new Path()) {
            for (i = 0; i < 10000; i++) {
                vp.of(root).concat(PRODUCT_FS).slash();
                fmt.format(vmem.getLong(i * 8L), enGb, "UTC", vp);
                if (!FF.exists(vp.$())) {
                    Assert.fail();
                }
            }
        }
    }

    @FunctionalInterface
    private interface TruncateModifier {
        void modify(TableWriter w, Rnd rnd, long timestamp, long increment);
    }

    static class CountingFilesFacade extends FilesFacadeImpl {
        long count = Long.MAX_VALUE;
    }

    private static class MetaRenameDenyingFacade extends TestFilesFacade {
        boolean hit = false;

        @Override
        public int rename(LPSZ from, LPSZ to) {
            if (Utf8s.containsAscii(to, TableUtils.META_PREV_FILE_NAME)) {
                hit = true;
                return Files.FILES_RENAME_ERR_OTHER;
            }
            return super.rename(from, to);
        }

        @Override
        public boolean wasCalled() {
            return hit;
        }
    }

    private static class SwapMetaRenameDenyingFacade extends TestFilesFacade {
        boolean hit = false;

        @Override
        public int rename(LPSZ from, LPSZ to) {
            if (Utf8s.endsWithAscii(from, TableUtils.META_SWAP_FILE_NAME)) {
                hit = true;
                return Files.FILES_RENAME_ERR_OTHER;
            }
            return super.rename(from, to);
        }

        @Override
        public boolean wasCalled() {
            return hit;
        }
    }
}
