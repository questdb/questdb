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

import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.microtime.TimestampFormatCompiler;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.PropServerConfiguration.COMMIT_INTERVAL_DEFAULT;

public class TableWriterTest extends AbstractCairoTest {

    public static final String PRODUCT = "product";
    private static final FilesFacade FF = FilesFacadeImpl.INSTANCE;
    private static final Log LOG = LogFactory.getLog(TableWriterTest.class);

    @Test
    public void tesFrequentCommit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 100000;
            create(FF, PartitionBy.NONE, N);
            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {

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
    public void testAddColumnAndFailToReadTopFile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10000;
            create(FF, PartitionBy.DAY, N);
            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
                long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
                Rnd rnd = new Rnd();
                populateProducts(writer, rnd, ts, N, 60000 * 1000L);
                writer.addColumn("xyz", ColumnType.STRING);
                Assert.assertEquals(N, writer.size());
            }

            class X extends FilesFacadeImpl {
                long fd = -1;

                @Override
                public long openRO(LPSZ name) {
                    if (Chars.endsWith(name, "xyz.top")) {
                        return this.fd = super.openRO(name);
                    }
                    return super.openRO(name);
                }

                @Override
                public long readULong(long fd, long offset) {
                    if (fd == this.fd) {
                        this.fd = -1;
                        return -1;
                    }
                    return super.readULong(fd, offset);
                }
            }

            final X ff = new X();
            try {
                new TableWriter(new DefaultCairoConfiguration(root) {
                    @Override
                    public FilesFacade getFilesFacade() {
                        return ff;
                    }
                }, PRODUCT);
                Assert.fail();
            } catch (CairoException ignore) {

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
    public void testAddColumnAndOpenWriterByYear() throws Exception {
        testAddColumnAndOpenWriter(PartitionBy.YEAR, 1000);
    }

    @Test
    public void testAddColumnAndOpenWriterNonPartitioned() throws Exception {
        testAddColumnAndOpenWriter(PartitionBy.NONE, 100000);
    }

    @Test
    public void testAddColumnCannotRemoveMeta() throws Exception {
        class X extends FilesFacadeImpl {
            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "abc.d")) {
                    return -1;
                }
                return super.openRW(name);
            }

            @Override
            public boolean remove(LPSZ name) {
                return !Chars.endsWith(name, TableUtils.META_FILE_NAME) && super.remove(name);
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
            public boolean rename(LPSZ from, LPSZ to) {
                return (!Chars.contains(to, TableUtils.META_PREV_FILE_NAME) || --count <= 0) && super.rename(from, to);
            }
        };
        testAddColumnRecoverableFault(ff);
    }

    @Test
    public void testAddColumnCannotTouchSymbolMapFile() throws Exception {
        FilesFacade ff = new FilesFacadeImpl() {
            @Override
            public boolean touch(LPSZ path) {
                return !Chars.endsWith(path, "abc.c") && super.touch(path);
            }
        };
        testAddColumnRecoverableFault(ff);
    }

    @Test
    public void testAddColumnCommitPartitioned() throws Exception {
        int count = 10000;
        create(FF, PartitionBy.DAY, count);
        Rnd rnd = new Rnd();
        long interval = 60000L * 1000L;
        long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
        try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
            ts = populateProducts(writer, rnd, ts, count, interval);
            Assert.assertEquals(count, writer.size());
            writer.addColumn("abc", ColumnType.STRING);
            // add more data including updating new column
            ts = populateTable2(writer, rnd, count, ts, interval);
            Assert.assertEquals(2 * count, writer.size());
            writer.rollback();
        }

        // append more
        try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
            populateTable2(writer, rnd, count, ts, interval);
            writer.commit();
            Assert.assertEquals(2 * count, writer.size());
        }
    }

    @Test
    public void testAddColumnDuplicate() throws Exception {
        long ts = populateTable(FF, PartitionBy.MONTH);
        try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
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
        // simulate existence of _meta.swp
        testAddColumnRecoverableFault(new FilesFacadeImpl() {

            @Override
            public boolean exists(LPSZ path) {
                return Chars.endsWith(path, "abc.k") || super.exists(path);
            }

            @Override
            public boolean remove(LPSZ name) {
                return !Chars.endsWith(name, "abc.k") && super.remove(name);
            }
        });
    }

    @Test
    public void testAddColumnFileOpenFail() throws Exception {
        // simulate existence of _meta.swp
        testAddColumnRecoverableFault(new FilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "abc.d")) {
                    return -1;
                }
                return super.openRW(name);
            }
        });
    }

    @Test
    public void testAddColumnFileOpenFail2() throws Exception {
        testAddColumnRecoverableFault(new FilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "abc.k")) {
                    return -1;
                }
                return super.openRW(name);
            }
        });
    }

    @Test
    public void testAddColumnFileOpenFail3() throws Exception {
        // simulate existence of _meta.swp
        testUnrecoverableAddColumn(new FilesFacadeImpl() {
            int count = 1;

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "abc.d")) {
                    return -1;
                }
                return super.openRW(name);
            }

            @Override
            public boolean rename(LPSZ from, LPSZ to) {
                return !(Chars.endsWith(from, TableUtils.META_PREV_FILE_NAME) && --count == 0) && super.rename(from, to);
            }
        });
    }

    @Test
    public void testAddColumnFileOpenFail4() throws Exception {
        testAddColumnRecoverableFault(new FilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "abc.d")) {
                    return -1;
                }
                return super.openRW(name);
            }
        });
    }

    @Test
    public void testAddColumnFileOpenFailAndIndexedPrev() throws Exception {
        // simulate existence of _meta.swp
        testUnrecoverableAddColumn(new FilesFacadeImpl() {
            int count = 2;
            int toCount = 5;

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "abc.d")) {
                    return -1;
                }
                return super.openRW(name);
            }

            @Override
            public boolean rename(LPSZ from, LPSZ to) {
                return (!Chars.contains(from, TableUtils.META_PREV_FILE_NAME) || --count <= 0) && (!Chars.contains(to, TableUtils.META_PREV_FILE_NAME) || --toCount <= 0) && super.rename(from, to);
            }
        });
    }

    @Test
    public void testAddColumnHavingTroubleCreatingMetaSwap() throws Exception {
        int N = 10000;
        create(FF, PartitionBy.DAY, N);
        FilesFacade ff = new FilesFacadeImpl() {

            int count = 5;

            @Override
            public boolean exists(LPSZ path) {
                return Chars.contains(path, TableUtils.META_SWAP_FILE_NAME) || super.exists(path);
            }

            @Override
            public boolean remove(LPSZ name) {
                if (Chars.contains(name, TableUtils.META_SWAP_FILE_NAME)) {
                    return --count < 0;
                }
                return super.remove(name);
            }
        };

        try (TableWriter writer = new TableWriter(new DefaultCairoConfiguration(root) {
            @Override
            public FilesFacade getFilesFacade() {
                return ff;
            }
        }, PRODUCT)) {
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
        testUnrecoverableAddColumn(new FilesFacadeImpl() {
            int counter = 2;

            @Override
            public long openRO(LPSZ name) {
                if (Chars.endsWith(name, TableUtils.META_FILE_NAME) && --counter == 0) {
                    return -1L;
                }
                return super.openRO(name);
            }
        });
    }

    @Test
    public void testAddColumnNonPartitioned() throws Exception {
        int N = 100000;
        create(FF, PartitionBy.NONE, N);
        try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
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
        try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
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
            public long openRO(LPSZ name) {
                if (Chars.endsWith(name, TableUtils.META_FILE_NAME) && --counter == 0) {
                    return -1L;
                }
                return super.openRO(name);
            }

            @Override
            public boolean remove(LPSZ name) {
                return !Chars.endsWith(name, TableUtils.META_FILE_NAME) && super.remove(name);
            }
        }
        testAddColumnErrorFollowedByRepairFail(new X());
    }

    @Test
    public void testAddColumnRepairFail2() throws Exception {
        class X extends FilesFacadeImpl {
            int counter = 2;

            @Override
            public long openRO(LPSZ name) {
                if (Chars.endsWith(name, TableUtils.META_FILE_NAME) && --counter == 0) {
                    return -1L;
                }
                return super.openRO(name);
            }

            @Override
            public boolean rename(LPSZ from, LPSZ to) {
                return !Chars.endsWith(from, TableUtils.META_PREV_FILE_NAME) && super.rename(from, to);
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
                    return Chars.endsWith(path, TableUtils.META_SWAP_FILE_NAME) || super.exists(path);
                }

                @Override
                public boolean remove(LPSZ name) {
                    if (Chars.endsWith(name, TableUtils.META_SWAP_FILE_NAME)) {
                        return deleteAttempted = true;
                    }
                    return super.remove(name);
                }
            }

            X ff = new X();

            try (TableWriter writer = new TableWriter(new DefaultCairoConfiguration(root) {
                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            }, PRODUCT)) {
                Assert.assertEquals(20, writer.columns.size());
                writer.addColumn("abc", ColumnType.STRING);
                Assert.assertEquals(22, writer.columns.size());
                Assert.assertTrue(ff.deleteAttempted);
            }
        });
    }

    @Test
    public void testAddColumnSwpFileDeleteFail() throws Exception {
        // simulate existence of _meta.swp
        testAddColumnRecoverableFault(new FilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                return Chars.contains(path, TableUtils.META_SWAP_FILE_NAME) || super.exists(path);
            }

            @Override
            public boolean remove(LPSZ name) {
                return !Chars.contains(name, TableUtils.META_SWAP_FILE_NAME) && super.remove(name);
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
    public void testAddColumnTopFileWriteFail() throws Exception {
        // simulate existence of _meta.swp
        testAddColumnRecoverableFault(new FilesFacadeImpl() {
            long fd = -1;

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "abc.top")) {
                    return fd = super.openRW(name);
                }
                return super.openRW(name);
            }

            @Override
            public long write(long fd, long address, long len, long offset) {
                if (fd == this.fd) {
                    this.fd = -1;
                    return -1;
                }
                return super.write(fd, address, len, offset);
            }
        });
    }

    @Test
    public void testAddIndexAndFailOnceByDay() throws Exception {

        final FilesFacade ff = new FilesFacadeImpl() {
            int count = 5;

            @Override
            public long openRO(LPSZ name) {
                if (Chars.endsWith(name, "supplier.d") && count-- == 0) {
                    return -1;
                }
                return super.openRO(name);
            }
        };

        final CairoConfiguration configuration = new DefaultCairoConfiguration(AbstractCairoTest.configuration.getRoot()) {
            @Override
            public FilesFacade getFilesFacade() {
                return ff;
            }
        };

        testAddIndexAndFailToIndexHalfWay(configuration, PartitionBy.DAY, 1000);
    }

    @Test
    public void testAddIndexAndFailOnceByNone() throws Exception {

        final FilesFacade ff = new FilesFacadeImpl() {
            int count = 1;

            @Override
            public boolean touch(LPSZ path) {
                if (Chars.endsWith(path, "supplier.v") && --count == 0) {
                    return false;
                }
                return super.touch(path);
            }
        };

        final CairoConfiguration configuration = new DefaultCairoConfiguration(AbstractCairoTest.configuration.getRoot()) {
            @Override
            public FilesFacade getFilesFacade() {
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
                CairoTestUtils.create(model);
            }

            final int N = 1000;
            try (TableWriter w = new TableWriter(configuration, "x")) {
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
                CairoTestUtils.create(model);
            }

            final int N = 1000;
            try (TableWriter w = new TableWriter(configuration, "x")) {
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
        O3Utils.initBuf();
        int N = 10000;
        create(FF, PartitionBy.DAY, N);
        testO3RecordsNewerThanOlder(N, configuration);
    }

    @Test
    public void testAutoCancelFirstRowNonPartitioned() throws Exception {
        int N = 10000;
        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.NONE, N);
            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {

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
                public int rmdir(Path name) {
                    return -1;
                }
            }

            X ff = new X();

            try (TableWriter writer = new TableWriter(new DefaultCairoConfiguration(root) {
                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            }, PRODUCT)) {
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

            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
                Assert.assertEquals(N, writer.size());
            }
        });
    }

    @Test
    public void testCancelRowOutOfOrder() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int N = 47;
            create(FF, PartitionBy.DAY, N);
            Rnd rnd = new Rnd();

            DefaultCairoConfiguration configuration = new DefaultCairoConfiguration(root);
            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
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

            try (TableWriter writer = new TableWriter(AbstractCairoTest.configuration, PRODUCT)) {
                Assert.assertEquals(2, writer.size());
            }

            try(TableReader rdr = new TableReader(configuration, PRODUCT)) {
                String expected = "productId\tproductName\tsupplier\tcategory\tprice\tlocationByte\tlocationShort\tlocationInt\tlocationLong\ttimestamp\n" +
                        "1148479920\tTJWCPSW\tHYRX\tPEHNRXGZSXU\t0.4621835429127854\tq\ttp0\tttmt7w\tcs4bdw4y4dpw\t2013-03-04T00:00:00.000000Z\n" +
                        "NaN\t\tGOOD\tGOOD2\t123.0\te\t0p6\t\t\t2013-03-04T00:00:00.000000Z\n";
                assertCursor(expected, rdr.getCursor(), rdr.getMetadata(), true);
            }
        });
    }

    @Test
    public void testCancelFirstRowFailurePartitioned() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            class X extends FilesFacadeImpl {
                boolean fail = false;

                @Override
                public long read(long fd, long buf, long len, long offset) {
                    if (fail) {
                        return -1;
                    }
                    return super.read(fd, buf, len, offset);
                }

                @Override
                public boolean allocate(long fd, long size) {
                    return !fail && super.allocate(fd, size);
                }
            }

            X ff = new X();
            Rnd rnd = new Rnd();
            int N = 94;
            create(ff, PartitionBy.DAY, N);
            long increment = 60 * 60000 * 1000L;
            try (TableWriter writer = new TableWriter(new DefaultCairoConfiguration(root) {
                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            }, PRODUCT)) {
                long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
                // add 48 hours
                ts = populateProducts(writer, rnd, ts, N / 2, increment);
                TableWriter.Row r = writer.newRow(ts += increment);
                r.putInt(0, rnd.nextPositiveInt());
                r.putStr(1, rnd.nextString(7));
                r.putSym(2, rnd.nextString(4));
                r.putSym(3, rnd.nextString(11));
                r.putDouble(4, rnd.nextDouble());

                ff.fail = true;
                try {
                    r.cancel();
                    Assert.fail();
                } catch (CairoException ignore) {
                }
                ff.fail = false;
                r.cancel();

                populateProducts(writer, rnd, ts, N / 2, increment);

                writer.commit();
                Assert.assertEquals(N, writer.size());
                Assert.assertEquals(6, getDirCount());
            }
        });
    }

    @Test
    public void testCancelFirstRowNonPartitioned() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 10000;
            create(FF, PartitionBy.NONE, N);
            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {

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
        ff = new FilesFacadeImpl() {
            long kIndexFd = -1;

            @Override
            public boolean close(long fd) {
                if (fd == kIndexFd) {
                    kIndexFd = -1;
                }
                return super.close(fd);
            }

            @Override
            public long openRW(LPSZ name) {
                if (Chars.contains(name, "2013-03-04") && Chars.endsWith(name, "category.k")) {
                    return kIndexFd = super.openRW(name);
                }
                return super.openRW(name);
            }

            @Override
            public int rmdir(Path name) {
                if (kIndexFd != -1) {
                    // Access denied, file is open
                    return 5;
                }
                return super.rmdir(name);
            }
        };

        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.DAY, 4);
            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
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
            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
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
            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
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
            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
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
                try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
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
            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
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
        try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
            ts = populateProducts(writer, rnd, ts, N, interval);

            Assert.assertEquals(N, writer.size());

            writer.addColumn("abc", ColumnType.STRING);

            TableWriter.Row r = writer.newRow(ts);
            r.putInt(0, rnd.nextInt());
            r.cancel();

            Assert.assertEquals(Long.BYTES, writer.columns.getQuick(21).getAppendOffset());

            // add more data including updating new column
            ts = populateTable2(writer, rnd, N, ts, interval);
            Assert.assertEquals(2 * N, writer.size());

            writer.rollback();
        }

        // append more
        try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
            populateTable2(writer, rnd, N, ts, interval);
            writer.commit();
            Assert.assertEquals(2 * N, writer.size());
        }
    }

    @Test
    public void testCancelRowRecoveryFromAppendPosErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd();

            class X extends FilesFacadeImpl {
                boolean fail = false;

                @Override
                public int rmdir(Path name) {
                    if (fail) {
                        return -1;
                    }
                    return super.rmdir(name);
                }
            }

            X ff = new X();

            final int N = 10000;
            create(ff, PartitionBy.DAY, N);

            // this contraption will verify that all timestamps that are
            // supposed to be stored have matching partitions
            try (MemoryARW vmem = Vm.getARWInstance(ff.getPageSize(), Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                try (TableWriter writer = new TableWriter(new DefaultCairoConfiguration(root) {
                    @Override
                    public FilesFacade getFilesFacade() {
                        return ff;
                    }
                }, PRODUCT)) {
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
            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {

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

            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
                Assert.assertEquals(N + 1, writer.size());
            }
        });

    }

    @Test
    public void testCannotCreatePartitionDir() throws Exception {
        testConstructor(new FilesFacadeImpl() {
            @Override
            public int mkdirs(LPSZ path, int mode) {
                if (Chars.endsWith(path, "default" + Files.SEPARATOR)) {
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
                public long openRW(LPSZ name) {
                    if (Chars.endsWith(name, PRODUCT + ".lock")) {
                        ran = true;
                        return -1;
                    }
                    return super.openRW(name);
                }

                @Override
                public boolean wasCalled() {
                    return ran;
                }


            };

            try {
                new TableWriter(new DefaultCairoConfiguration(root) {
                    @Override
                    public FilesFacade getFilesFacade() {
                        return ff;
                    }
                }, PRODUCT);
                Assert.fail();
            } catch (CairoException ignore) {
            }
            Assert.assertTrue(ff.wasCalled());
        });
    }

    @Test
    public void testCannotMapTxFile() throws Exception {
        testConstructor(new FilesFacadeImpl() {
            int count = 2;
            long fd = -1;

            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (fd == this.fd) {
                    this.fd = -1;
                    return -1;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, TableUtils.TXN_FILE_NAME) && --count == 0) {
                    return fd = super.openRW(name);
                }
                return super.openRW(name);
            }
        });
    }

    @Test
    public void testCannotOpenColumnFile() throws Exception {
        testConstructor(new FilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "productName.i")) {
                    return -1;
                }
                return super.openRW(name);
            }
        });
    }

    @Test
    public void testCannotOpenSymbolMap() throws Exception {
        final int N = 100;
        create(FF, PartitionBy.NONE, N);
        populateTable0(FF, N);
        testConstructor(new FilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                return !Chars.endsWith(path, "category.o") && super.exists(path);
            }
        }, false);
    }

    @Test
    public void testCannotOpenTodo() throws Exception {
        // trick constructor into thinking "_todo" file exists
        testConstructor(new FilesFacadeImpl() {
            int counter = 2;

            @Override
            public long openRW(LPSZ path) {
                if (Chars.endsWith(path, TableUtils.TODO_FILE_NAME) && --counter == 0) {
                    return -1;
                }
                return super.openRW(path);
            }
        });
    }

    @Test
    public void testCannotOpenTxFile() throws Exception {
        testConstructor(new FilesFacadeImpl() {
            int count = 2;

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, TableUtils.TXN_FILE_NAME) && --count == 0) {
                    return -1;
                }
                return super.openRW(name);
            }
        });
    }

    @Test
    public void testCannotSetAppendPosition() throws Exception {
        final int N = 10000;
        create(FF, PartitionBy.NONE, N);
        populateTable0(FF, N);
        testConstructor(new FilesFacadeImpl() {
            long fd;

            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (fd == this.fd) {
                    return -1;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "supplier.d")) {
                    return fd = super.openRW(name);
                }
                return super.openRW(name);
            }
        }, false);
    }

    @Test
    public void testCannotSetAppendPositionOnDataFile() throws Exception {
        final int N = 10000;
        create(FF, PartitionBy.NONE, N);
        populateTable0(FF, N);
        testConstructor(new FilesFacadeImpl() {
            long fd;

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "productName.i")) {
                    return fd = super.openRW(name);
                }
                return super.openRW(name);
            }

            @Override
            public boolean allocate(long fd, long size) {
                if (this.fd == fd) {
                    return false;
                }
                return super.allocate(fd, size);
            }
        }, false);
    }

    @Test
    public void testCannotSetAppendPositionOnIndexFile() throws Exception {
        final int N = 10000;
        create(FF, PartitionBy.NONE, N);
        populateTable0(FF, N);
        testConstructor(new FilesFacadeImpl() {
            long fd;

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "productName.i")) {
                    return fd = super.openRW(name);
                }
                return super.openRW(name);
            }

            @Override
            public boolean allocate(long fd, long size) {
                if (this.fd == fd) {
                    return false;
                }
                return super.allocate(fd, size);
            }
        }, false);
    }

    @Test
    // tests scenario where truncate is supported (linux) but fails on close
    // close is expected not to fail
    public void testCannotTruncateColumnOnClose() throws Exception {
        int N = 100000;
        create(FF, PartitionBy.NONE, N);
        testTruncateOnClose(new TestFilesFacade() {
            long fd = -1;
            boolean ran = false;

            @Override
            public boolean isRestrictedFileSystem() {
                return false;
            }

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "price.d")) {
                    return fd = super.openRW(name);
                }
                return super.openRW(name);
            }

            @Override
            public boolean truncate(long fd, long size) {
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
    // tests scenario where truncate is not supported (windows) but fails on close
    // truncate on close fails once and then succeeds
    // close is expected not to fail
    public void testCannotTruncateColumnOnCloseAndNotSupported() throws Exception {
        int N = 100000;
        create(FF, PartitionBy.NONE, N);
        testTruncateOnClose(new TestFilesFacade() {
            long fd = -1;
            boolean ran = false;

            @Override
            public boolean isRestrictedFileSystem() {
                return true;
            }

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "price.d")) {
                    return fd = super.openRW(name);
                }
                return super.openRW(name);
            }

            @Override
            public boolean truncate(long fd, long size) {
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
    // tests scenario where truncate is not supported (windows) but fails on close
    // truncate on close fails all the time
    public void testCannotTruncateColumnOnCloseAndNotSupported2() throws Exception {
        int N = 100000;
        create(FF, PartitionBy.NONE, N);
        testTruncateOnClose(new TestFilesFacade() {
            long fd = -1;
            boolean ran = false;

            @Override
            public boolean isRestrictedFileSystem() {
                return true;
            }

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "price.d")) {
                    return fd = super.openRW(name);
                }
                return super.openRW(name);
            }

            @Override
            public boolean truncate(long fd, long size) {
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
        try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
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
        FilesFacade ff = new FilesFacadeImpl() {
            @Override
            public long length(LPSZ name) {
                if (Chars.endsWith(name, TableUtils.TODO_FILE_NAME)) {
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

            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
                populateProducts(writer, new Rnd(), TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z"), N, 60000 * 1000L);
                writer.commit();
                Assert.assertEquals(N, writer.size());
            }

            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
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
            CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                @Override
                public long getDataAppendPageSize() {
                    return 1024 * 1024; //1MB
                }
            };
            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {

                long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

                for (int k = 0; k < 3; k++) {
                    ts = populateProducts(writer, rnd, ts, N, increment);
                    writer.commit();
                    Assert.assertEquals(N, writer.size());
                    writer.truncate();
                }
            }

            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
                long ts = TimestampFormatUtils.parseTimestamp("2014-03-04T00:00:00.000Z");
                Assert.assertEquals(0, writer.size());
                populateProducts(writer, rnd, ts, N, increment);
                writer.commit();
                Assert.assertEquals(N, writer.size());
            }
        });
    }

    @Test
    public void testDayPartitionTruncateDirIterateFail() throws Exception {
        testTruncate(new CountingFilesFacade() {

            @Override
            public int findNext(long findPtr) {
                if (--count == 0) {
                    throw CairoException.instance(0).put("FindNext failed");
                }
                return super.findNext(findPtr);
            }
        });
    }

    @Test
    public void testDefaultPartition() throws Exception {
        populateTable();
    }

    @Test
    public void testGeoHashAsStringInvalid() throws Exception {
        TestUtils.assertMemoryLeak(() -> assertGeoStr("ooo", 15, GeoHashes.NULL));
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
        TestUtils.assertMemoryLeak(() -> assertGeoStr("g912j", 44, GeoHashes.NULL));
    }

    @Test
    public void testGeoHashAsStringVanilla() throws Exception {
        TestUtils.assertMemoryLeak(() -> assertGeoStr("g9", 10, 489));
    }

    @Test
    public void testGetColumnIndex() {
        CairoTestUtils.createAllTable(configuration, PartitionBy.NONE);
        try (TableWriter writer = new TableWriter(configuration, "all")) {
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
            CairoTestUtils.createAllTable(configuration, PartitionBy.NONE);
            try (
                    MemoryCMARW mem = Vm.getCMARWInstance();
                    Path path = new Path().of(root).concat("all").concat(TableUtils.TODO_FILE_NAME).$()
            ) {
                mem.smallFile(FilesFacadeImpl.INSTANCE, path, MemoryTag.MMAP_DEFAULT);
                mem.putLong(32, 1);
                mem.putLong(40, 9990001L);
                mem.jumpTo(48);
            }
            try (TableWriter writer = new TableWriter(configuration, "all")) {
                Assert.assertNotNull(writer);
                Assert.assertTrue(writer.isOpen());
            }

            try (TableWriter writer = new TableWriter(configuration, "all")) {
                Assert.assertNotNull(writer);
                Assert.assertTrue(writer.isOpen());
            }
        });
    }

    @Test
    public void testIndexIsAddedToTable() throws NumericException {
        int partitionBy = PartitionBy.DAY;
        int N = 1000;
        try (TableModel model = new TableModel(configuration, "test", partitionBy)) {
            model.col("sym1", ColumnType.SYMBOL);
            model.col("sym2", ColumnType.SYMBOL);
            model.col("sym3", ColumnType.SYMBOL);
            model.timestamp();

            CairoTestUtils.create(model);
        }

        // insert data
        final Rnd rnd = new Rnd();
        long t = TimestampFormatUtils.parseTimestamp("2019-03-22T00:00:00.000000Z");
        long increment = 2_000_000;
        try (TableWriter w = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "test", "test reason")) {
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
            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "test")) {
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
        testConstructor(new FilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Chars.endsWith(name, TableUtils.META_FILE_NAME)) {
                    return -1;
                }
                return super.openRO(name);
            }
        });
    }

    @Test
    public void testNonStandardPageSize() throws Exception {
        populateTable(new FilesFacadeImpl() {
            @Override
            public long getPageSize() {
                return super.getPageSize() * 500;
            }
        }, PartitionBy.MONTH);
    }

    @Test
    public void testNonStandardPageSize2() throws Exception {
        populateTable(new FilesFacadeImpl() {
            @Override
            public long getPageSize() {
                return 32 * 1024 * 1024;
            }
        }, PartitionBy.YEAR);
    }

    @Test
    public void testNulls() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CairoTestUtils.createAllTable(configuration, PartitionBy.NONE);
            Rnd rnd = new Rnd();
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
            ts = testAppendNulls(rnd, ts);
            testAppendNulls(rnd, ts);
        });
    }

    @Test
    public void testO3AfterReopen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CairoTestUtils.createAllTableWithTimestamp(configuration, PartitionBy.NONE);
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
                CairoTestUtils.create(model);
            }


            try (TableWriter writer = new TableWriter(configuration, "weather")) {
                TableWriter.Row r;
                r = writer.newRow(IntervalUtils.parseFloorPartialDate("2021-01-31"));
                r.putDouble(0, 1.0);
                r.append();

                // Out of order
                r = writer.newRow(IntervalUtils.parseFloorPartialDate("2021-01-30"));
                r.putDouble(0, 1.0);
                r.cancel();

                // Back in order
                r = writer.newRow(IntervalUtils.parseFloorPartialDate("2021-02-01"));
                r.putDouble(0, 1.0);
                r.append();

                Assert.assertEquals(2, writer.size());
                writer.commit();
            }

            long[] expectedTs = new long[]{
                    IntervalUtils.parseFloorPartialDate("2021-01-31"),
                    IntervalUtils.parseFloorPartialDate("2021-02-01")
            };
            try (TableReader reader = new TableReader(configuration, "weather")) {
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
    public void testO3WithCancelRow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TableModel model = new TableModel(configuration, "weather", PartitionBy.DAY)
                    .col("windspeed", ColumnType.DOUBLE)
                    .timestamp()) {
                CairoTestUtils.create(model);
            }

            long[] tss = new long[]{
                    631150000000000L,
                    631152000000000L,
                    631160000000000L
            };
            try (TableWriter writer = new TableWriter(configuration, "weather")) {
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

            try (TableReader reader = new TableReader(configuration, "weather")) {
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
                CairoTestUtils.create(model);
            }

            try {
                new TableWriter(configuration, "x");
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "only supported");
            }
        });
    }

    @Test
    public void testOpenWriterMissingTxFile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CairoTestUtils.createAllTable(configuration, PartitionBy.NONE);
            try (Path path = new Path()) {
                Assert.assertTrue(FF.remove(path.of(root).concat("all").concat(TableUtils.TXN_FILE_NAME).$()));
                try {
                    new TableWriter(configuration, "all");
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
            CairoTestUtils.create(model);
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
            CairoTestUtils.create(model);
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
            CairoTestUtils.create(model);
            testRemoveColumn(model);
        }
    }

    @Test
    public void testRemoveColumnCannotOpenSwap() throws Exception {
        class X extends TestFilesFacade {

            boolean hit = false;

            @Override
            public long openRW(LPSZ name) {
                if (Chars.contains(name, TableUtils.META_SWAP_FILE_NAME)) {
                    hit = true;
                    return -1;
                }
                return super.openRW(name);
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
                if (Chars.contains(path, TableUtils.META_PREV_FILE_NAME)) {
                    exists++;
                    return true;
                }
                return super.exists(path);
            }

            @Override
            public boolean remove(LPSZ name) {
                if (Chars.contains(name, TableUtils.META_PREV_FILE_NAME)) {
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
                if (Chars.endsWith(name, "supplier.d")) {
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
                if (Chars.endsWith(name, "supplier.k")) {
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
                if (Chars.contains(path, TableUtils.META_PREV_FILE_NAME) && --count > 0) {
                    return true;
                }
                return super.exists(path);
            }

            @Override
            public boolean remove(LPSZ name) {
                return !Chars.contains(name, TableUtils.META_PREV_FILE_NAME) && super.remove(name);
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
                return Chars.contains(path, TableUtils.META_SWAP_FILE_NAME) || super.exists(path);
            }

            @Override
            public boolean remove(LPSZ name) {
                if (Chars.contains(name, TableUtils.META_SWAP_FILE_NAME)) {
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
            public boolean rename(LPSZ from, LPSZ to) {
                if (Chars.endsWith(to, TableUtils.META_FILE_NAME) && count-- > 0) {
                    return false;
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
            CairoTestUtils.create(model);
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();
            try (TableWriter writer = new TableWriter(configuration, model.getName())) {

                append10KProducts(ts, rnd, writer);

                writer.removeColumn("timestamp");

                append10KNoTimestamp(rnd, writer);

                writer.commit();

                Assert.assertEquals(20000, writer.size());
            }

            try (TableWriter writer = new TableWriter(configuration, model.getName())) {
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
            CairoTestUtils.create(model);
        }

        try (TableWriter writer = new TableWriter(configuration, "ABC")) {
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
            CairoTestUtils.create(model);
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
            CairoTestUtils.create(model);
            testRenameColumn(model);
        }
    }

    @Test
    public void testRenameColumnCannotOpenSwap() throws Exception {
        class X extends TestFilesFacade {

            boolean hit = false;

            @Override
            public long openRW(LPSZ name) {
                if (Chars.contains(name, TableUtils.META_SWAP_FILE_NAME)) {
                    hit = true;
                    return -1;
                }
                return super.openRW(name);
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
                if (Chars.contains(path, TableUtils.META_PREV_FILE_NAME)) {
                    exists++;
                    return true;
                }
                return super.exists(path);
            }

            @Override
            public boolean remove(LPSZ name) {
                if (Chars.contains(name, TableUtils.META_PREV_FILE_NAME)) {
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
    public void testRenameColumnCannotRemoveDFile() throws Exception {
        renameColumn(new TestFilesFacade() {
            int count = 0;

            @Override
            public boolean rename(LPSZ name, LPSZ to) {
                if (Chars.endsWith(name, "supplier.d")) {
                    count++;
                    return false;
                }
                return super.rename(name, to);
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
                if (Chars.contains(path, TableUtils.META_PREV_FILE_NAME) && --count > 0) {
                    return true;
                }
                return super.exists(path);
            }

            @Override
            public boolean remove(LPSZ name) {
                return !Chars.contains(name, TableUtils.META_PREV_FILE_NAME) && super.remove(name);
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
                return Chars.contains(path, TableUtils.META_SWAP_FILE_NAME) || super.exists(path);
            }

            @Override
            public boolean remove(LPSZ name) {
                if (Chars.contains(name, TableUtils.META_SWAP_FILE_NAME)) {
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
            public boolean rename(LPSZ from, LPSZ to) {
                if (Chars.endsWith(to, TableUtils.META_FILE_NAME) && count-- > 0) {
                    return false;
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
            CairoTestUtils.create(model);
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();
            try (TableWriter writer = new TableWriter(configuration, model.getName())) {

                append10KProducts(ts, rnd, writer);

                writer.renameColumn("timestamp", "ts");

                append10KProducts(writer.getMaxTimestamp(), rnd, writer);

                writer.commit();

                Assert.assertEquals(20000, writer.size());
            }

            try (TableWriter writer = new TableWriter(configuration, model.getName())) {
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
            CairoTestUtils.create(model);
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();
            try (TableWriter writer = new TableWriter(configuration, model.getName())) {

                append10KProducts(ts, rnd, writer);

                writer.renameColumn("timestamp", "ts");

                append10KProducts(writer.getMaxTimestamp(), rnd, writer);

                writer.commit();

                Assert.assertEquals(20000, writer.size());
            }

            try (TableWriter writer = new TableWriter(configuration, model.getName())) {
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
            public int rmdir(Path from) {
                if (Chars.endsWith(from, "2013-03-12")) {
                    removeAttempted = true;
                    return 1;
                }
                return super.rmdir(from);
            }
        }

        X ff = new X();

        long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
        final long increment = 60000L * 1000;
        Rnd rnd = new Rnd();
        try (TableWriter writer = new TableWriter(new DefaultCairoConfiguration(root) {
            @Override
            public FilesFacade getFilesFacade() {
                return ff;
            }
        }, PRODUCT)) {

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
            public int rmdir(Path path) {
                if (Chars.endsWith(path, "2013-03-12")) {
                    removeAttempted = true;
                    return 1;
                }
                return super.rmdir(path);
            }
        }

        X ff = new X();

        long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
        final long increment = 60000L * 1000;
        Rnd rnd = new Rnd();
        try (TableWriter writer = new TableWriter(new DefaultCairoConfiguration(root) {
            @Override
            public FilesFacade getFilesFacade() {
                return ff;
            }
        }, PRODUCT)) {

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
    public void testSetAppendPositionFailureBin2() throws Exception {
        testSetAppendPositionFailure();
    }

    @Test
    public void testSinglePartitionTruncate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.YEAR, 4);

            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
                writer.truncate();
                Assert.assertEquals(0, writer.size());
            }

            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
                Assert.assertEquals(0, writer.size());
            }
        });
    }

    @Test
    public void testSkipOverSpuriousDir() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.DAY, 10);

            try (Path path = new Path()) {
                // create random directory
                path.of(configuration.getRoot()).concat(PRODUCT).concat("somethingortheother").slash$();
                Assert.assertEquals(0, configuration.getFilesFacade().mkdirs(path, configuration.getMkDirMode()));

                new TableWriter(configuration, PRODUCT).close();

                Assert.assertFalse(configuration.getFilesFacade().exists(path));
            }
        });
    }

    @Test
    public void testTableDoesNotExist() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                new TableWriter(configuration, PRODUCT);
                Assert.fail();
            } catch (CairoException e) {
                LOG.info().$((Sinkable) e).$();
            }
        });
    }

    @Test
    public void testTableLock() {
        CairoTestUtils.createAllTable(configuration, PartitionBy.NONE);

        try (TableWriter ignored = new TableWriter(configuration, "all")) {
            try {
                new TableWriter(configuration, "all");
                Assert.fail();
            } catch (CairoException ignored2) {
            }
        }
    }

    @Test
    public void testTableWriterCustomPageSize() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.DAY, 10000);
            CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                @Override
                public long getDataAppendPageSize() {
                    return getFilesFacade().getPageSize();
                }
            };
            try (TableWriter w = new TableWriter(configuration, PRODUCT)) {
                long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

                Rnd rnd = new Rnd();
                for (int i = 0; i < 100; i++) {
                    ts = populateRow(w, rnd, ts, 60L * 60000L * 1000L);
                }
                w.commit();

                for (int i = 0, n = w.columns.size(); i < n; i++) {
                    MemoryMAR m = w.columns.getQuick(i);
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
            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
                Assert.assertEquals("TableWriter{name=product}", writer.toString());
            }
        });
    }

    @Test
    public void testCommitInterval() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.NONE, 4);
            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
                writer.updateCommitInterval(0.0, 1000);
                writer.setMetaCommitLag(5_000_000);
                Assert.assertEquals(1000, writer.getCommitInterval());

                writer.updateCommitInterval(0.5, 1000);
                writer.setMetaCommitLag(5_000_000);
                Assert.assertEquals(2500, writer.getCommitInterval());

                writer.updateCommitInterval(0.5, 1000);
                writer.setMetaCommitLag(15_000_000);
                Assert.assertEquals(7500, writer.getCommitInterval());

                writer.updateCommitInterval(0.5, 3000);
                writer.setMetaCommitLag(0);
                Assert.assertEquals(3000, writer.getCommitInterval());
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
            CairoTestUtils.create(model);
        }

        Rnd rnd = new Rnd();
        try (TableWriter writer = new TableWriter(configuration, name)) {
            for (int i = 0; i < 1000000; i++) {
                TableWriter.Row r = writer.newRow();
                r.putStr(0, rnd.nextChars(5));
                r.append();
            }
            writer.commit(CommitMode.ASYNC);
            writer.addColumn("митинг", ColumnType.INT);
            Assert.assertEquals(0, writer.getColumnIndex("секьюрити"));
            Assert.assertEquals(2, writer.getColumnIndex("митинг"));
        }

        rnd.reset();
        try (TableReader reader = new TableReader(configuration, name)) {
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
                public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
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
                new TableWriter(new DefaultCairoConfiguration(root) {
                    @Override
                    public FilesFacade getFilesFacade() {
                        return ff;
                    }
                }, PRODUCT);
                Assert.fail();
            } catch (CairoException ignore) {
            }
        });
    }

    @Test
    public void testTxFileDoesNotExist() throws Exception {
        testConstructor(new FilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                return !Chars.endsWith(path, TableUtils.TXN_FILE_NAME) && super.exists(path);
            }
        });
    }

    @Test
    public void testUnCachedSymbol() {
        testSymbolCacheFlag(false);
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
        try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
            Assert.assertEquals(20, writer.columns.size());
            populateProducts(writer, rnd, ts, 10000, 60000L * 1000L);
            writer.commit(CommitMode.SYNC);
            Assert.assertEquals(30000, writer.size());
        }
    }

    private void assertGeoStr(String hash, int tableBits, long expected) {
        final String tableName = "geo1";
        try (TableModel model = new TableModel(configuration, tableName, PartitionBy.NONE)) {
            model.col("g", ColumnType.getGeoHashTypeWithBits(tableBits));
            CairoTestUtils.createTable(model);
        }

        try (TableWriter writer = new TableWriter(configuration, tableName)) {
            TableWriter.Row r = writer.newRow();
            r.putGeoStr(0, hash);
            r.append();
            writer.commit();
        }

        try (TableReader r = new TableReader(configuration, tableName)) {
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
        try (TableModel model = new TableModel(new DefaultCairoConfiguration(root) {
            @Override
            public FilesFacade getFilesFacade() {
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
            CairoTestUtils.create(model);
        }
    }

    private int getDirCount() {
        AtomicInteger count = new AtomicInteger();
        try (Path path = new Path()) {
            FF.iterateDir(path.of(root).concat(PRODUCT).$(), (pUtf8NameZ, type) -> {
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
        try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
            ts = populateProducts(writer, rnd, ts, n, interval);
            writer.commit(CommitMode.NOSYNC);

            Assert.assertEquals(n, writer.size());

            writer.addColumn("abc", ColumnType.STRING);

            // add more data including updating new column
            ts = populateTable2(writer, rnd, n, ts, interval);

            writer.commit();

            Assert.assertEquals(2 * n, writer.size());
        }

        // append more
        try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
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

    private long populateTable0(FilesFacade ff, int N) throws NumericException {
        try (TableWriter writer = new TableWriter(new DefaultCairoConfiguration(root) {
            @Override
            public FilesFacade getFilesFacade() {
                return ff;
            }
        }, PRODUCT)) {
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
                CairoTestUtils.create(model);
            }

            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();

            try (TableWriter writer = new TableWriter(new DefaultCairoConfiguration(root) {
                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            }, name)) {

                ts = append10KProducts(ts, rnd, writer);

                writer.removeColumn("supplier");

                // assert attempt to remove files
                Assert.assertTrue(ff.wasCalled());

                ts = append10KNoSupplier(ts, rnd, writer);

                writer.commit();

                Assert.assertEquals(20000, writer.size());
            }

            try (TableWriter writer = new TableWriter(configuration, name)) {
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
                CairoTestUtils.create(model);
            }

            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();

            try (TableWriter writer = new TableWriter(new DefaultCairoConfiguration(root) {
                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            }, name)) {

                ts = append10KProducts(ts, rnd, writer);

                writer.renameColumn("supplier", "sup");

                // assert attempt to remove files
                Assert.assertTrue(ff.wasCalled());

                ts = append10KWithNewName(ts, rnd, writer);

                writer.commit();

                Assert.assertEquals(20000, writer.size());
            }

            try (TableWriter writer = new TableWriter(configuration, name)) {
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
            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
                ts = populateProducts(writer, rnd, ts, N, 60L * 60000L * 1000L);
                writer.commit();
                Assert.assertEquals(N, writer.size());
            }

            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
                writer.addColumn("xyz", ColumnType.STRING);
            }

            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
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
            CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            };
            long ts = populateTable();
            Rnd rnd = new Rnd();
            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
                ts = populateProducts(writer, rnd, ts, 10000, 60000L * 1000L);
                writer.commit();
                Assert.assertEquals(20000, writer.size());

                Assert.assertEquals(20, writer.columns.size());

                try {
                    writer.addColumn("abc", ColumnType.STRING);
                    Assert.fail();
                } catch (CairoError ignore) {
                }
            }

            try {
                new TableWriter(configuration, PRODUCT);
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
            CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            };
            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
                Assert.assertEquals(20, writer.columns.size());
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

            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
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
            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
                ts = populateProducts(writer, rnd, ts, N, 60L * 60000L * 1000L);
                writer.commit();
                Assert.assertEquals(N, writer.size());
            }

            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
                writer.addIndex("supplier", configuration.getIndexValueBlockSize());
                Assert.fail();
            } catch (CairoException ignored) {
            }

            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
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
            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
                writer.addIndex("supplier", configuration.getIndexValueBlockSize());
            }
        });
    }

    private long testAppendNulls(Rnd rnd, long ts) {
        final int blobLen = 64 * 1024;
        long blob = Unsafe.malloc(blobLen, MemoryTag.NATIVE_DEFAULT);
        try (TableWriter writer = new TableWriter(new DefaultCairoConfiguration(root) {
            @Override
            public FilesFacade getFilesFacade() {
                return TableWriterTest.FF;
            }
        }, "all")) {
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
                new TableWriter(new DefaultCairoConfiguration(root) {
                    @Override
                    public FilesFacade getFilesFacade() {
                        return ff;
                    }
                }, PRODUCT);
                Assert.fail();
            } catch (CairoException e) {
                LOG.info().$((Sinkable) e).$();
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
            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {

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

            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
                Assert.assertEquals(N, writer.size());
            }
        });
    }

    private void testO3RecordsNewerThanOlder(int N, CairoConfiguration configuration) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {

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

            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
                Assert.assertEquals(N, writer.size());
            }
        });
    }

    private void testRemoveColumn(TableModel model) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CairoTestUtils.create(model);
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();
            try (TableWriter writer = new TableWriter(configuration, model.getName())) {

                // optional
                writer.warmUp();

                ts = append10KProducts(ts, rnd, writer);

                writer.removeColumn("supplier");

                try (Path path = new Path()) {
                    path.of(root).concat(model.getName());
                    final int plen = path.length();
                    FF.iterateDir(path.$(), (pUtf8NameZ, type) -> {
                        if (Files.isDir(pUtf8NameZ, type)) {
                            Assert.assertFalse(FF.exists(path.trimTo(plen).concat(pUtf8NameZ).concat("supplier.i").$()));
                            Assert.assertFalse(FF.exists(path.trimTo(plen).concat(pUtf8NameZ).concat("supplier.d").$()));
                            Assert.assertFalse(FF.exists(path.trimTo(plen).concat(pUtf8NameZ).concat("supplier.top").$()));
                        }
                    });
                }

                ts = append10KNoSupplier(ts, rnd, writer);

                writer.commit();

                Assert.assertEquals(20000, writer.size());
            }

            try (TableWriter writer = new TableWriter(configuration, model.getName())) {
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
            try (TableWriter writer = new TableWriter(new DefaultCairoConfiguration(root) {
                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            }, PRODUCT)) {
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

            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
                append10KProducts(ts, rnd, writer);
                writer.commit();
                Assert.assertEquals(30000, writer.size());
            }
        });
    }

    private void testRenameColumn(TableModel model) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CairoTestUtils.create(model);
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();
            try (TableWriter writer = new TableWriter(configuration, model.getName())) {

                // optional
                writer.warmUp();

                ts = append10KProducts(ts, rnd, writer);

                int columnTypeTag = ColumnType.tagOf(writer.getMetadata().getColumnType("supplier"));

                writer.renameColumn("supplier", "sup");

                try (Path path = new Path()) {
                    path.of(root).concat(model.getName());
                    final int plen = path.length();
                    if (columnTypeTag == ColumnType.SYMBOL) {
                        Assert.assertFalse(FF.exists(path.trimTo(plen).concat("supplier.v").$()));
                        Assert.assertFalse(FF.exists(path.trimTo(plen).concat("supplier.o").$()));
                        Assert.assertFalse(FF.exists(path.trimTo(plen).concat("supplier.c").$()));
                        Assert.assertFalse(FF.exists(path.trimTo(plen).concat("supplier.k").$()));
                        Assert.assertTrue(FF.exists(path.trimTo(plen).concat("sup.v").$()));
                        Assert.assertTrue(FF.exists(path.trimTo(plen).concat("sup.o").$()));
                        Assert.assertTrue(FF.exists(path.trimTo(plen).concat("sup.c").$()));
                        Assert.assertTrue(FF.exists(path.trimTo(plen).concat("sup.k").$()));
                    }
                    path.trimTo(plen);
                    FF.iterateDir(path.$(), (pUtf8NameZ, type) -> {
                        if (Files.isDir(pUtf8NameZ, type)) {
                            Assert.assertFalse(FF.exists(path.trimTo(plen).concat(pUtf8NameZ).concat("supplier.i").$()));
                            Assert.assertFalse(FF.exists(path.trimTo(plen).concat(pUtf8NameZ).concat("supplier.d").$()));
                            Assert.assertFalse(FF.exists(path.trimTo(plen).concat(pUtf8NameZ).concat("supplier.top").$()));
                            Assert.assertTrue(FF.exists(path.trimTo(plen).concat(pUtf8NameZ).concat("sup.d").$()));
                            if (columnTypeTag == ColumnType.BINARY || columnTypeTag == ColumnType.STRING) {
                                Assert.assertTrue(FF.exists(path.trimTo(plen).concat(pUtf8NameZ).concat("sup.i").$()));
                            }
                        }
                    });
                }

                ts = append10KWithNewName(ts, rnd, writer);

                writer.commit();

                Assert.assertEquals(20000, writer.size());
            }

            try (TableWriter writer = new TableWriter(configuration, model.getName())) {
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
            try (TableWriter writer = new TableWriter(new DefaultCairoConfiguration(root) {
                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            }, PRODUCT)) {
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

            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
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
        try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
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
            CairoTestUtils.createAllTable(configuration, PartitionBy.NONE);

            class X extends FilesFacadeImpl {
                long fd = -1;

                @Override
                public long openRW(LPSZ name) {
                    if (Chars.endsWith(name, "bin.i")) {
                        return fd = super.openRW(name);
                    }
                    return super.openRW(name);
                }

                @Override
                public boolean allocate(long fd, long size) {
                    if (fd == this.fd) {
                        return false;
                    }
                    return super.allocate(fd, size);
                }
            }
            final X ff = new X();
            testAppendNulls(new Rnd(), TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z"));
            try {
                new TableWriter(new DefaultCairoConfiguration(root) {
                    @Override
                    public FilesFacade getFilesFacade() {
                        return ff;
                    }
                }, "all");
                Assert.fail();
            } catch (CairoException ignore) {
            }
        });
    }

    private void testSymbolCacheFlag(boolean cacheFlag) {
        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("a", ColumnType.SYMBOL).cached(cacheFlag)
                .col("b", ColumnType.STRING)
                .col("c", ColumnType.SYMBOL).cached(!cacheFlag)
                .timestamp()) {
            CairoTestUtils.create(model);
        }

        int N = 1000;
        Rnd rnd = new Rnd();
        try (TableWriter writer = new TableWriter(configuration, "x")) {
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

        try (TableReader reader = new TableReader(configuration, "x")) {
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

            CairoTestUtils.create(model);
        }

        // insert data
        final Rnd rnd = new Rnd();
        long t = TimestampFormatUtils.parseTimestamp("2019-03-22T00:00:00.000000Z");
        long increment = 2_000_000;
        try (TableWriter w = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "test", "test reason")) {
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
            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "test")) {
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
            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "test")) {
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

    private void testTruncate(CountingFilesFacade ff) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 200;
            create(ff, PartitionBy.DAY, N);
            Rnd rnd = new Rnd();
            final long increment = 60 * 60000 * 1000L;
            CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }

                @Override
                public long getMiscAppendPageSize() {
                    return 1024 * 1024;
                }
            };
            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {

                long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

                for (int k = 0; k < 3; k++) {
                    ts = populateProducts(writer, rnd, ts, N, increment);
                    writer.commit();
                    Assert.assertEquals(N, writer.size());

                    // this truncate will fail quite early and will leave
                    // table in inconsistent state to recover from which
                    // truncate has to be repeated
                    try {
                        ff.count = 6;
                        writer.truncate();
                        Assert.fail();
                    } catch (CairoException e) {
                        LOG.info().$((Sinkable) e).$();
                    }

                    writer.truncate();
                }
            }

            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
                long ts = TimestampFormatUtils.parseTimestamp("2014-03-04T00:00:00.000Z");
                Assert.assertEquals(0, writer.size());
                populateProducts(writer, rnd, ts, 1000, increment);
                writer.commit();
                Assert.assertEquals(1000, writer.size());
            }

            // open writer one more time and just assert the size
            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
                Assert.assertEquals(1000, writer.size());
            }
        });
    }

    private void testTruncateOnClose(TestFilesFacade ff, int N) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TableWriter writer = new TableWriter(new DefaultCairoConfiguration(root) {
                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            }, PRODUCT)) {
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
            try (TableWriter writer = new TableWriter(new DefaultCairoConfiguration(root) {
                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            }, PRODUCT)) {
                ts = populateProducts(writer, rnd, ts, 10000, 60000 * 1000L);
                writer.commit();

                Assert.assertEquals(20, writer.columns.size());

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
            CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            };
            final int N = 20000;
            create(FF, PartitionBy.DAY, N);
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
            Rnd rnd = new Rnd();
            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
                ts = append10KProducts(ts, rnd, writer);
                writer.commit();

                try {
                    writer.removeColumn("supplier");
                    Assert.fail();
                } catch (CairoError ignore) {
                }
            }

            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
                append10KProducts(ts, rnd, writer);
                writer.commit();
                Assert.assertEquals(N, writer.size());
            }
        });
    }

    private void testUnrecoverableRenameColumn(FilesFacade ff) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CairoConfiguration configuration = new DefaultCairoConfiguration(root) {
                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            };
            final int N = 20000;
            create(FF, PartitionBy.DAY, N);
            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");
            Rnd rnd = new Rnd();
            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
                ts = append10KProducts(ts, rnd, writer);
                writer.commit();

                try {
                    writer.renameColumn("supplier", "sup");
                    Assert.fail();
                } catch (CairoError ignore) {
                }
            }

            try (TableWriter writer = new TableWriter(configuration, PRODUCT)) {
                append10KProducts(ts, rnd, writer);
                writer.commit();
                Assert.assertEquals(N, writer.size());
            }
        });
    }

    void verifyTimestampPartitions(MemoryARW vmem) {
        int i;
        TimestampFormatCompiler compiler = new TimestampFormatCompiler();
        DateFormat fmt = compiler.compile("yyyy-MM-dd");
        DateLocale enGb = DateLocaleFactory.INSTANCE.getLocale("en-gb");

        try (Path vp = new Path()) {
            for (i = 0; i < 10000; i++) {
                vp.of(root).concat(PRODUCT).slash();
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

    private static class SwapMetaRenameDenyingFacade extends TestFilesFacade {
        boolean hit = false;

        @Override
        public boolean rename(LPSZ from, LPSZ to) {
            if (Chars.endsWith(from, TableUtils.META_SWAP_FILE_NAME)) {
                hit = true;
                return false;
            }
            return super.rename(from, to);
        }

        @Override
        public boolean wasCalled() {
            return hit;
        }
    }

    private static class MetaRenameDenyingFacade extends TestFilesFacade {
        boolean hit = false;

        @Override
        public boolean rename(LPSZ from, LPSZ to) {
            if (Chars.contains(to, TableUtils.META_PREV_FILE_NAME)) {
                hit = true;
                return false;
            }
            return super.rename(from, to);
        }

        @Override
        public boolean wasCalled() {
            return hit;
        }
    }

    static class CountingFilesFacade extends FilesFacadeImpl {
        long count = Long.MAX_VALUE;
    }
}
