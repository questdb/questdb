/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.PartitionBy;
import com.questdb.ex.NumericException;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.*;
import com.questdb.ql.parser.AbstractOptimiserTest;
import com.questdb.std.Sinkable;
import com.questdb.std.str.CompositePath;
import com.questdb.std.str.LPSZ;
import com.questdb.std.str.NativeLPSZ;
import com.questdb.std.str.Path;
import com.questdb.std.time.*;
import com.questdb.store.ColumnType;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class TableWriterTest extends AbstractOptimiserTest {

    public static final String PRODUCT = "product";
    private static final FilesFacade FF = FilesFacadeImpl.INSTANCE;
    private static final Log LOG = LogFactory.getLog(TableWriterTest.class);
    private static CharSequence root;

    @BeforeClass
    public static void setUp() throws Exception {
        root = FACTORY_CONTAINER.getConfiguration().getJournalBase().getAbsolutePath();
    }

    @After
    public void tearDown0() throws Exception {
        try (CompositePath path = new CompositePath().of(root)) {
            Files.rmdir(path.$());
        }
    }

    @Test
    public void tesFrequentCommit() throws Exception {
        long used = Unsafe.getMemUsed();
        create(FF, PartitionBy.NONE);
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {

            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();
            for (int i = 0; i < 100000; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 60000);
                writer.commit();
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
        Assert.assertEquals(0L, FF.getOpenFileCount());
    }

    @Test
    public void testAddColumnAndFailToReadTopFile() throws Exception {
        create(FF, PartitionBy.DAY);

        long mem = Unsafe.getMemUsed();

        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
            int N = 10000;
            Rnd rnd = new Rnd();
            for (int i = 0; i < N; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 60000);
            }
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
            public long read(long fd, long buf, int len, long offset) {
                if (fd == this.fd) {
                    this.fd = -1;
                    return -1;
                }
                return super.read(fd, buf, len, offset);
            }
        }

        try {
            new TableWriter(new X(), root, PRODUCT);
            Assert.fail();
        } catch (CairoException ignore) {

        }

        Assert.assertEquals(mem, Unsafe.getMemUsed());
        Assert.assertEquals(0, Files.getOpenFileCount());
    }

    @Test
    public void testAddColumnCannotOpenTodo() throws Exception {
        testAddColumnRecoverableFault(new TodoAppendDenyingFacade());
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
                return !Chars.endsWith(name, TableWriter.META_FILE_NAME) && super.remove(name);
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
    public void testAddColumnCommitPartitioned() throws Exception {
        create(FF, PartitionBy.DAY);
        Rnd rnd = new Rnd();
        long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            for (int i = 0; i < 10000; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 60000);
            }

            Assert.assertEquals(10000, writer.size());

            writer.addColumn("abc", ColumnType.STRING);

            // add more data including updating new column
            ts = populateTable2(rnd, writer, ts, 10000);
            Assert.assertEquals(2 * 10000, writer.size());

            writer.rollback();
        }

        // append more
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            populateTable2(rnd, writer, ts, 10000);
            writer.commit();
            Assert.assertEquals(2 * 10000, writer.size());
        }
    }

    @Test
    public void testAddColumnDuplicate() throws Exception {
        long ts = populateTable(FF, PartitionBy.MONTH);
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            try {
                writer.addColumn("supplier", ColumnType.BOOLEAN);
                Assert.fail();
            } catch (CairoException ignore) {
            }
            Rnd rnd = new Rnd();
            for (int i = 0; i < 10000; i++) {
                ts = populateRow(writer, ts, rnd, 60000);
            }
            writer.commit();
            Assert.assertEquals(20000, writer.size());
        }
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
        // simulate existence of _meta.swp
        testAddColumnRecoverableFault(new FilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "abc.i")) {
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
                return !(Chars.endsWith(from, TableWriter.META_PREV_FILE_NAME) && --count == 0) && super.rename(from, to);
            }
        });
    }

    @Test
    public void testAddColumnMetaOpenFail() throws Exception {
        testUnrecoverableAddColumn(new FilesFacadeImpl() {
            int counter = 2;

            @Override
            public long openRO(LPSZ name) {
                if (Chars.endsWith(name, TableWriter.META_FILE_NAME) && --counter == 0) {
                    return -1L;
                }
                return super.openRO(name);
            }
        });
    }

    @Test
    public void testAddColumnMetaSwapRenameFail2() throws Exception {
        testUnrecoverableAddColumn(new FilesFacadeImpl() {
            int count = 1;

            @Override
            public boolean rename(LPSZ from, LPSZ to) {
                return !Chars.endsWith(from, TableWriter.META_SWAP_FILE_NAME) && super.rename(from, to);
            }

            @Override
            public boolean remove(LPSZ name) {
                return !(Chars.endsWith(name, TableWriter.TODO_FILE_NAME) && --count == 0) && super.remove(name);
            }
        });
    }

    @Test
    public void testAddColumnNonPartitioned() throws Exception {
        create(FF, PartitionBy.NONE);
        addColumnAndPopulate();
    }

    @Test
    public void testAddColumnPartitioned() throws Exception {
        create(FF, PartitionBy.DAY);
        addColumnAndPopulate();
    }

    @Test
    public void testAddColumnRepairFail() throws Exception {
        class X extends FilesFacadeImpl {
            int counter = 2;

            @Override
            public long openRO(LPSZ name) {
                if (Chars.endsWith(name, TableWriter.META_FILE_NAME) && --counter == 0) {
                    return -1L;
                }
                return super.openRO(name);
            }

            @Override
            public boolean remove(LPSZ name) {
                return !Chars.endsWith(name, TableWriter.META_FILE_NAME) && super.remove(name);
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
                if (Chars.endsWith(name, TableWriter.META_FILE_NAME) && --counter == 0) {
                    return -1L;
                }
                return super.openRO(name);
            }

            @Override
            public boolean rename(LPSZ from, LPSZ to) {
                return !Chars.endsWith(from, TableWriter.META_PREV_FILE_NAME) && super.rename(from, to);
            }
        }
        testAddColumnErrorFollowedByRepairFail(new X());
    }

    @Test
    public void testAddColumnSwpFileDelete() throws Exception {

        populateTable(FF);
        // simulate existence of _meta.swp

        class X extends FilesFacadeImpl {
            boolean deleteAttempted = false;

            @Override
            public boolean exists(LPSZ path) {
                return Chars.endsWith(path, TableWriter.META_SWAP_FILE_NAME) || super.exists(path);
            }

            @Override
            public boolean remove(LPSZ name) {
                if (Chars.endsWith(name, TableWriter.META_SWAP_FILE_NAME)) {
                    return deleteAttempted = true;
                }
                return super.remove(name);
            }
        }

        long mem = Unsafe.getMemUsed();

        X ff = new X();

        try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
            Assert.assertEquals(12, writer.columns.size());
            writer.addColumn("abc", ColumnType.STRING);
            Assert.assertEquals(14, writer.columns.size());
            Assert.assertTrue(ff.deleteAttempted);
        }

        Assert.assertEquals(0, Files.getOpenFileCount());
        Assert.assertEquals(mem, Unsafe.getMemUsed());
    }

    @Test
    public void testAddColumnSwpFileDeleteFail() throws Exception {
        // simulate existence of _meta.swp
        testAddColumnRecoverableFault(new FilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                return Chars.endsWith(path, TableWriter.META_SWAP_FILE_NAME) || super.exists(path);
            }

            @Override
            public boolean remove(LPSZ name) {
                return !Chars.endsWith(name, TableWriter.META_SWAP_FILE_NAME) && super.remove(name);
            }
        });
    }

    @Test
    public void testAddColumnSwpFileMapFail() throws Exception {
        testAddColumnRecoverableFault(new FilesFacadeImpl() {
            long fd = -1;

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, TableWriter.META_SWAP_FILE_NAME)) {
                    return fd = super.openRW(name);
                }
                return super.openRW(name);
            }

            @Override
            public long mmap(long fd, long len, long offset, int mode) {
                if (fd == this.fd) {
                    this.fd = -1;
                    return -1;
                }
                return super.mmap(fd, len, offset, mode);
            }
        });
    }

    @Test
    public void testAddColumnToNonEmptyNonPartitioned() throws Exception {
        create(FF, PartitionBy.NONE);
        populateAndColumnPopulate(1000000);
    }

    @Test
    public void testAddColumnToNonEmptyPartitioned() throws Exception {
        create(FF, PartitionBy.DAY);
        populateAndColumnPopulate(10000);
    }

    @Test
    public void testAddColumnTopFileWriteFail() throws Exception {
        // simulate existence of _meta.swp
        testAddColumnRecoverableFault(new FilesFacadeImpl() {
            long fd = -1;

            @Override
            public long append(long fd, long buf, int len) {
                if (fd == this.fd) {
                    this.fd = -1;
                    return -1;
                }
                return super.append(fd, buf, len);
            }

            @Override
            public long openAppend(LPSZ name) {
                if (Chars.endsWith(name, "abc.top")) {
                    return fd = super.openAppend(name);
                }
                return super.openAppend(name);
            }


        });
    }

    @Test
    public void testAppendOutOfOrder() throws Exception {
        create(FF, PartitionBy.NONE);
        testOutOfOrderRecords();
    }

    @Test
    public void testAppendOutOfOrderPartitioned() throws Exception {
        create(FF, PartitionBy.DAY);
        testOutOfOrderRecords();
    }

    @Test
    public void testAutoCancelFirstRowNonPartitioned() throws Exception {
        long used = Unsafe.getMemUsed();
        create(FF, PartitionBy.NONE);
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {

            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");


            TableWriter.Row r = writer.newRow(ts);
            r.putInt(0, 1234);

            Rnd rnd = new Rnd();
            for (int i = 0; i < 10000; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 60000);
            }
            Assert.assertEquals(10000, writer.size());
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
        Assert.assertEquals(0L, FF.getOpenFileCount());
    }

    @Test
    public void testCancelFailureFollowedByTableClose() throws Exception {
        long used = Unsafe.getMemUsed();
        create(FF, PartitionBy.DAY);
        Rnd rnd = new Rnd();
        final int N = 47;
        class X extends FilesFacadeImpl {
            long fd = -1;

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "supplier.i")) {
                    return fd = super.openRW(name);
                }
                return super.openRW(name);
            }

            @Override
            public long read(long fd, long buf, int len, long offset) {
                if (fd == this.fd) {
                    this.fd = -1;
                    return -1;
                }
                return super.read(fd, buf, len, offset);
            }
        }

        X ff = new X();

        try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
            for (int i = 0; i < N; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 60000);
            }
            writer.commit();
            Assert.assertEquals(N, writer.size());

            TableWriter.Row r = writer.newRow(ts + 60 * 60000);
            r.putInt(0, rnd.nextInt());
            try {
                r.cancel();
                Assert.fail();
            } catch (CairoException ignore) {
            }
        }

        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            Assert.assertEquals(N, writer.size());
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
        Assert.assertEquals(0L, FF.getOpenFileCount());
    }

    @Test
    public void testCancelFirstRowFailurePartitioned() throws Exception {
        class X extends FilesFacadeImpl {
            boolean fail = false;

            @Override
            public long read(long fd, long buf, int len, long offset) {
                if (fail) {
                    return -1;
                }
                return super.read(fd, buf, len, offset);
            }
        }

        X ff = new X();

        long used = Unsafe.getMemUsed();
        Rnd rnd = new Rnd();
        create(ff, PartitionBy.DAY);
        try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
            // add 48 hours
            for (int i = 0; i < 47; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 60000);
            }

            TableWriter.Row r = writer.newRow(ts += 60 * 60000);
            r.putInt(0, rnd.nextPositiveInt());
            r.putStr(1, rnd.nextString(7));
            r.putStr(2, rnd.nextString(4));
            r.putStr(3, rnd.nextString(11));
            r.putDouble(4, rnd.nextDouble());

            ff.fail = true;
            try {
                r.cancel();
                Assert.fail();
            } catch (CairoException ignore) {
            }
            ff.fail = false;
            r.cancel();

            for (int i = 0; i < 47; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 60000);
            }

            writer.commit();
            Assert.assertEquals(94, writer.size());
            Assert.assertTrue(getDirCount() == 6);
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
        Assert.assertEquals(0L, ff.getOpenFileCount());
    }

    @Test
    public void testCancelFirstRowNonPartitioned() throws Exception {
        long used = Unsafe.getMemUsed();
        create(FF, PartitionBy.NONE);
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {

            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");


            TableWriter.Row r = writer.newRow(ts);
            r.putInt(0, 1234);
            r.cancel();

            Rnd rnd = new Rnd();
            for (int i = 0; i < 10000; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 60000);
            }
            Assert.assertEquals(10000, writer.size());
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
        Assert.assertEquals(0L, FF.getOpenFileCount());
    }

    @Test
    public void testCancelFirstRowPartitioned() throws Exception {
        long used = Unsafe.getMemUsed();
        create(FF, PartitionBy.DAY);
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
            TableWriter.Row r = writer.newRow(ts);
            r.cancel();
            writer.commit();
            Assert.assertEquals(0, writer.size());
            Assert.assertTrue(getDirCount() == 2);
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
        Assert.assertEquals(0L, FF.getOpenFileCount());
    }

    @Test
    public void testCancelFirstRowPartitioned2() throws Exception {
        long used = Unsafe.getMemUsed();
        Rnd rnd = new Rnd();
        create(FF, PartitionBy.DAY);
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
            // add 48 hours
            for (int i = 0; i < 47; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 60000);
            }

            TableWriter.Row r = writer.newRow(ts += 60 * 60000);
            r.putInt(0, rnd.nextPositiveInt());
            r.putStr(1, rnd.nextString(7));
            r.putStr(2, rnd.nextString(4));
            r.putStr(3, rnd.nextString(11));
            r.putDouble(4, rnd.nextDouble());

            for (int i = 0; i < 1000; i++) {
                r.cancel();
            }

            for (int i = 0; i < 47; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 60000);
            }

            writer.commit();
            Assert.assertEquals(94, writer.size());
            Assert.assertTrue(getDirCount() == 6);
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
        Assert.assertEquals(0L, FF.getOpenFileCount());
    }

    @Test
    public void testCancelMidPartition() throws Exception {
        long used = Unsafe.getMemUsed();
        final Rnd rnd = new Rnd();
        create(FF, PartitionBy.DAY);

        // this contraption will verify that all timestamps that are
        // supposed to be stored have matching partitions
        try (VirtualMemory vmem = new VirtualMemory(FF.getPageSize())) {
            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
                int i = 0;
                final int N = 10000;

                int cancelCount = 0;
                while (i < N) {
                    TableWriter.Row r = writer.newRow(ts += 60 * 60000);
                    r.putInt(0, rnd.nextPositiveInt());
                    r.putStr(1, rnd.nextString(7));
                    r.putStr(2, rnd.nextString(4));
                    r.putStr(3, rnd.nextString(11));
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
                verifyTimestampPartitions(vmem, N);
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
        Assert.assertEquals(0L, FF.getOpenFileCount());
    }

    @Test
    public void testCancelMidRowNonPartitioned() throws Exception {
        long used = Unsafe.getMemUsed();
        create(FF, PartitionBy.NONE);
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

            int cancelCount = 0;
            Rnd rnd = new Rnd();
            int i = 0;
            TableWriter.Row r;
            while (i < 10000) {
                r = writer.newRow(ts += 60 * 60000);
                r.putInt(0, rnd.nextPositiveInt());
                r.putStr(1, rnd.nextString(7));
                r.putStr(2, rnd.nextString(4));
                r.putStr(3, rnd.nextString(11));
                r.putDouble(4, rnd.nextDouble());
                if (rnd.nextBoolean()) {
                    r.append();
                    i++;
                } else {
                    cancelCount++;
                }
            }
            r = writer.newRow(ts);
            r.putStr(2, "XYZ");

            writer.commit();
            Assert.assertTrue(cancelCount > 0);
            Assert.assertEquals(10000, writer.size());
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
        Assert.assertEquals(0L, FF.getOpenFileCount());
    }

    @Test
    public void testCancelRowAfterAddColumn() throws Exception {
        create(FF, PartitionBy.DAY);
        Rnd rnd = new Rnd();
        long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            for (int i = 0; i < 10000; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 60000);
            }

            Assert.assertEquals(10000, writer.size());

            writer.addColumn("abc", ColumnType.STRING);

            TableWriter.Row r = writer.newRow(ts);
            r.putInt(0, rnd.nextInt());
            r.cancel();

            Assert.assertEquals(0L, writer.columns.getQuick(13).getAppendOffset());

            // add more data including updating new column
            ts = populateTable2(rnd, writer, ts, 10000);
            Assert.assertEquals(2 * 10000, writer.size());

            writer.rollback();
        }

        // append more
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            populateTable2(rnd, writer, ts, 10000);
            writer.commit();
            Assert.assertEquals(2 * 10000, writer.size());
        }
    }

    @Test
    public void testCancelRowRecovery() throws Exception {
        long used = Unsafe.getMemUsed();
        final Rnd rnd = new Rnd();

        class X extends FilesFacadeImpl {
            boolean fail = false;

            @Override
            public boolean rmdir(CompositePath name) {
                return !fail && super.rmdir(name);
            }

            @Override
            public long read(long fd, long buf, int len, long offset) {
                return fail ? -1 : super.read(fd, buf, len, offset);
            }
        }

        X ff = new X();

        create(ff, PartitionBy.DAY);

        // this contraption will verify that all timestamps that are
        // supposed to be stored have matching partitions
        try (VirtualMemory vmem = new VirtualMemory(ff.getPageSize())) {
            try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
                int i = 0;
                final int N = 10000;

                int cancelCount = 0;
                while (i < N) {
                    TableWriter.Row r = writer.newRow(ts += 60 * 60000);
                    r.putInt(0, rnd.nextPositiveInt());
                    r.putStr(1, rnd.nextString(7));
                    r.putStr(2, rnd.nextString(4));
                    r.putStr(3, rnd.nextString(11));
                    r.putDouble(4, rnd.nextDouble());
                    if (rnd.nextPositiveInt() % 50 == 0) {
                        ff.fail = true;
                        try {
                            r.cancel();
                            Assert.fail();
                        } catch (CairoException ignored) {
                        }
                        ff.fail = false;
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
                verifyTimestampPartitions(vmem, N);
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
        Assert.assertEquals(0L, FF.getOpenFileCount());
    }

    @Test
    public void testCancelRowRecoveryFromAppendPosErrors() throws Exception {
        long used = Unsafe.getMemUsed();
        final Rnd rnd = new Rnd();

        class X extends FilesFacadeImpl {
            boolean fail = false;

            @Override
            public long read(long fd, long buf, int len, long offset) {
                if (fail) {
                    return -1;
                }
                return super.read(fd, buf, len, offset);
            }
        }

        X ff = new X();

        create(ff, PartitionBy.DAY);

        // this contraption will verify that all timestamps that are
        // supposed to be stored have matching partitions
        try (VirtualMemory vmem = new VirtualMemory(ff.getPageSize())) {
            try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
                int i = 0;
                final int N = 10000;

                int cancelCount = 0;
                int failCount = 0;
                while (i < N) {
                    TableWriter.Row r = writer.newRow(ts += 60 * 60000);
                    r.putInt(0, rnd.nextPositiveInt());
                    r.putStr(1, rnd.nextString(7));
                    r.putStr(2, rnd.nextString(4));
                    r.putStr(3, rnd.nextString(11));
                    r.putDouble(4, rnd.nextDouble());
                    if (rnd.nextPositiveInt() % 50 == 0) {
                        ff.fail = true;
                        try {
                            r.cancel();
                        } catch (CairoException ignored) {
                            failCount++;
                            ff.fail = false;
                            r.cancel();
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
                verifyTimestampPartitions(vmem, N);
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
        Assert.assertEquals(0L, FF.getOpenFileCount());
    }

    @Test
    public void testCannotCreatePartitionDir() throws Exception {
        testConstructor(new FilesFacadeImpl() {
            @Override
            public int mkdirs(LPSZ path, int mode) {
                if (Chars.endsWith(path, "default" + Path.SEPARATOR)) {
                    return -1;
                }
                return super.mkdirs(path, mode);
            }
        });
    }

    @Test
    public void testCannotMapTxFile() throws Exception {
        testConstructor(new FilesFacadeImpl() {
            int count = 2;
            long fd = -1;

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, TableWriter.TXN_FILE_NAME) && --count == 0) {
                    return fd = super.openRW(name);
                }
                return super.openRW(name);
            }

            @Override
            public long mmap(long fd, long len, long offset, int mode) {
                if (fd == this.fd) {
                    this.fd = -1;
                    return -1;
                }
                return super.mmap(fd, len, offset, mode);
            }
        });
    }

    @Test
    public void testCannotOpenColumnFile() throws Exception {
        testConstructor(new FilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "supplier.i")) {
                    return -1;
                }
                return super.openRW(name);
            }
        });
    }

    @Test
    public void testCannotOpenTodo() throws Exception {
        // trick constructor into thinking "_todo" file exists
        testConstructor(new FilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                return Chars.endsWith(path, TableWriter.TODO_FILE_NAME) || super.exists(path);
            }
        });
    }

    @Test
    public void testCannotOpenTxFile() throws Exception {
        testConstructor(new FilesFacadeImpl() {
            int count = 2;

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, TableWriter.TXN_FILE_NAME) && --count == 0) {
                    return -1;
                }
                return super.openRW(name);
            }
        });
    }

    @Test
    public void testCannotSetAppendPosition() throws Exception {
        create(FF, PartitionBy.NONE);
        populateTable0(FF);
        testConstructor(new FilesFacadeImpl() {
            long fd;

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "supplier.d")) {
                    return fd = super.openRW(name);
                }
                return super.openRW(name);
            }

            @Override
            public long read(long fd, long buf, int len, long offset) {
                if (fd == this.fd) {
                    this.fd = -1;
                    return -1;
                }
                return super.read(fd, buf, len, offset);
            }
        }, false);
    }

    @Test
    public void testCannotSetAppendPositionOnIndexFile() throws Exception {
        create(FF, PartitionBy.NONE);
        populateTable0(FF);
        testConstructor(new FilesFacadeImpl() {
            long fd;

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "supplier.i")) {
                    return fd = super.openRW(name);
                }
                return super.openRW(name);
            }

            @Override
            public long read(long fd, long buf, int len, long offset) {
                if (fd == this.fd) {
                    this.fd = -1;
                    return -1;
                }
                return super.read(fd, buf, len, offset);
            }
        }, false);
    }

    @Test
    public void testConstructorTruncatedTodo() throws Exception {
        FilesFacade ff = new FilesFacadeImpl() {
            long fd = 7686876823L;

            @Override
            public boolean exists(LPSZ path) {
                return Chars.endsWith(path, TableWriter.TODO_FILE_NAME) || super.exists(path);
            }

            @Override
            public long openRO(LPSZ name) {
                if (Chars.endsWith(name, TableWriter.TODO_FILE_NAME)) {
                    return this.fd;
                }
                return super.openRO(name);
            }

            @Override
            public long read(long fd, long buf, int len, long offset) {
                if (fd == this.fd) {
                    this.fd = -1;
                    return -1;
                }
                return super.read(fd, buf, len, offset);
            }
        };

        populateTable(ff);
    }

    @Test
    public void testDayPartition() throws Exception {
        long used = Unsafe.getMemUsed();
        create(FF, PartitionBy.DAY);
        int N = 100000;

        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {

            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();
            for (int i = 0; i < N; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 60000);
            }
            writer.commit();
            Assert.assertEquals(N, writer.size());
        }

        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            Assert.assertEquals((long) N, writer.size());
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
        Assert.assertEquals(0L, FF.getOpenFileCount());
    }

    @Test
    public void testDayPartitionRmDirError() throws Exception {
        testTruncate(new CountingFilesFacade() {
            @Override
            public boolean rmdir(CompositePath name) {
                return --count != 0 && super.rmdir(name);
            }
        }, true);
    }

    @Test
    public void testDayPartitionTruncate() throws Exception {
        long used = Unsafe.getMemUsed();
        create(FF, PartitionBy.DAY);
        Rnd rnd = new Rnd();
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {

            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

            for (int k = 0; k < 3; k++) {
                for (int i = 0; i < 100000; i++) {
                    ts = populateRow(writer, ts, rnd, 60 * 60000);
                }
                writer.commit();
                Assert.assertEquals(100000, writer.size());
                writer.truncate();
            }
        }

        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            long ts = DateFormatUtils.parseDateTime("2014-03-04T00:00:00.000Z");
            Assert.assertEquals(0, writer.size());
            for (int i = 0; i < 100000; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 60000);
            }
            writer.commit();
            Assert.assertEquals(100000, writer.size());
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
        Assert.assertEquals(0L, FF.getOpenFileCount());
    }

    @Test
    public void testDayPartitionTruncateError() throws Exception {
        testTruncate(new CountingFilesFacade() {
            @Override
            public boolean truncate(long fd, long size) {
                return --count != 0 && super.truncate(fd, size);
            }
        }, true);
    }

    @Test
    public void testDayPartitionTruncateErrorConstructorRecovery() throws Exception {
        class X extends CountingFilesFacade {
            @Override
            public boolean truncate(long fd, long size) {
                return --count != 0 && super.truncate(fd, size);
            }
        }
        testTruncate(new X(), false);
    }

    @Test
    public void testDefaultPartition() throws Exception {
        populateTable(FF);
    }

    @Test
    public void testFailureToOpenArchiveFile() throws Exception {
        testCommitRetryAfterFailure(new CountingFilesFacade() {
            @Override
            public long openAppend(LPSZ name) {
                if (--count < 1L) {
                    return -1;
                }
                return super.openAppend(name);
            }
        });
    }

    @Test
    public void testFailureToWriteArchiveFile() throws Exception {
        testCommitRetryAfterFailure(new CountingFilesFacade() {
            long fd = -1;

            @Override
            public long openAppend(LPSZ name) {
                if (--count < 1L) {
                    return fd = super.openAppend(name);
                }
                return super.openAppend(name);
            }

            @Override
            public long write(long fd, long address, long len, long offset) {
                if (fd == this.fd) {
                    // single shot failure
                    this.fd = -1;
                    return -1;
                }
                return super.write(fd, address, len, offset);
            }
        });
    }

    @Test
    public void testGetColumnIndex() throws Exception {
        createAllTable();
        try (TableWriter writer = new TableWriter(FF, root, "all")) {
            Assert.assertEquals(1, writer.getColumnIndex("short"));
            try {
                writer.getColumnIndex("bad");
                Assert.fail();
            } catch (CairoException ignore) {
            }
        }
    }

    @Test
    public void testMetaFileDoesNotExist() throws Exception {
        testConstructor(new FilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Chars.endsWith(name, TableWriter.META_FILE_NAME)) {
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
                return super.getPageSize() * super.getPageSize();
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
        long mem = Unsafe.getMemUsed();
        createAllTable();
        Rnd rnd = new Rnd();
        long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
        ts = testAppendNulls(rnd, FF, ts);
        testAppendNulls(rnd, FF, ts);
        Assert.assertEquals(mem, Unsafe.getMemUsed());
        Assert.assertEquals(0, FF.getOpenFileCount());
    }

    @Test
    public void testOutOfOrderAfterReopen() throws Exception {
        long mem = Unsafe.getMemUsed();
        createAllTable();
        Rnd rnd = new Rnd();
        long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
        testAppendNulls(rnd, FF, ts);
        try {
            testAppendNulls(rnd, FF, ts);
            Assert.fail();
        } catch (CairoException ignore) {
        }
        Assert.assertEquals(mem, Unsafe.getMemUsed());
        Assert.assertEquals(0, FF.getOpenFileCount());
    }

    @Test
    public void testRemoveColumnAfterTimestamp() throws Exception {
        JournalStructure struct = new JournalStructure("ABC").
                $int("productId").
                $str("productName").
                $sym("category").
                $double("price").
                $ts().
                $sym("supplier").$();

        testRemoveColumn(struct);
    }

    @Test
    public void testRemoveColumnBeforeTimestamp() throws Exception {
        JournalStructure struct = new JournalStructure("ABC").
                $int("productId").
                $str("productName").
                $sym("supplier").
                $sym("category").
                $double("price").
                $ts();

        testRemoveColumn(struct);
    }

    @Test
    public void testRemoveColumnCannotAppendTodo() throws Exception {
        testRemoveColumn(new TodoAppendDenyingFacade());
    }

    @Test
    public void testRemoveColumnCannotMMapSwap() throws Exception {
        class X extends FilesFacadeImpl {

            long fd = -1;

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, TableWriter.META_SWAP_FILE_NAME)) {
                    return fd = super.openRW(name);
                }
                return super.openRW(name);
            }

            @Override
            public long mmap(long fd, long len, long offset, int mode) {
                if (fd == this.fd) {
                    this.fd = -1;
                    return -1;
                }
                return super.mmap(fd, len, offset, mode);
            }
        }
        testRemoveColumn(new X());
    }

    @Test
    public void testRemoveColumnCannotOpenSwap() throws Exception {
        class X extends FilesFacadeImpl {

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, TableWriter.META_SWAP_FILE_NAME)) {
                    return -1;
                }
                return super.openRW(name);
            }
        }
        testRemoveColumn(new X());
    }

    @Test
    public void testRemoveColumnCannotOpenTodo() throws Exception {
        testRemoveColumn(new TodoOpenDenyingFacade());
    }

    @Test
    public void testRemoveColumnCannotRemoveFiles() throws Exception {
        JournalStructure struct = new JournalStructure("ABC").
                $int("productId").
                $str("productName").
                $sym("supplier").
                $sym("category").
                $double("price").
                $ts();

        String name = createTable(FF, struct.partitionBy(PartitionBy.DAY));

        class X extends FilesFacadeImpl {
            int count = 0;

            @Override
            public boolean remove(LPSZ name) {
                if (Chars.endsWith(name, "supplier.d")) {
                    count++;
                    return false;
                }
                return super.remove(name);
            }
        }

        long mem = Unsafe.getMemUsed();

        long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

        Rnd rnd = new Rnd();

        X ff = new X();

        try (TableWriter writer = new TableWriter(ff, root, name)) {

            ts = append10KProducts(ts, rnd, writer);

            writer.removeColumn("supplier");

            // assert attempt to remove files
            Assert.assertTrue(ff.count > 0);

            ts = append10KNoSupplier(ts, rnd, writer);

            writer.commit();

            Assert.assertEquals(20000, writer.size());
        }

        try (TableWriter writer = new TableWriter(ff, root, name)) {
            append10KNoSupplier(ts, rnd, writer);
            writer.commit();
            Assert.assertEquals(30000, writer.size());
        }

        Assert.assertEquals(mem, Unsafe.getMemUsed());
        Assert.assertEquals(0, Files.getOpenFileCount());
    }

    @Test
    public void testRemoveColumnCannotRemoveSwap() throws Exception {
        class X extends FilesFacadeImpl {
            @Override
            public boolean exists(LPSZ path) {
                return Chars.endsWith(path, TableWriter.META_SWAP_FILE_NAME) || super.exists(path);
            }

            @Override
            public boolean remove(LPSZ name) {
                return !Chars.endsWith(name, TableWriter.META_SWAP_FILE_NAME) && super.remove(name);
            }
        }
        testRemoveColumn(new X());
    }

    @Test
    public void testRemoveColumnCannotRenameMeta() throws Exception {
        testRemoveColumn(new MetaRenameDenyingFacade());
    }

    @Test
    public void testRemoveColumnCannotRenameMetaSwap() throws Exception {
        testRemoveColumn(new SwapMetaRenameDenyingFacade());
    }

    @Test
    public void testRemoveColumnUnrecoverableRemoveTodoFailure() throws Exception {
        class X extends FilesFacadeImpl {
            int count = 1;

            @Override
            public boolean remove(LPSZ name) {
                return (!Chars.endsWith(name, TableWriter.TODO_FILE_NAME) || --count != 0) && super.remove(name);
            }
        }
        testUnrecoverableRemoveColumn(new X());
    }

    @Test
    public void testRemoveColumnUnrecoverableRenameFailure() throws Exception {
        class X extends FilesFacadeImpl {
            int count = 2;

            @Override
            public boolean rename(LPSZ from, LPSZ to) {
                return !Chars.endsWith(to, TableWriter.META_FILE_NAME) || --count < 0 && super.rename(from, to);
            }
        }
        testUnrecoverableRemoveColumn(new X());
    }

    @Test
    public void testRemoveTimestamp() throws Exception {
        JournalStructure struct = new JournalStructure("ABC").
                $int("productId").
                $str("productName").
                $sym("category").
                $double("price").
                $ts().
                $sym("supplier").$();

        String name = createTable(FF, struct.partitionBy(PartitionBy.NONE));

        long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

        Rnd rnd = new Rnd();
        try (TableWriter writer = new TableWriter(FF, root, name)) {

            append10KProducts(ts, rnd, writer);

            writer.removeColumn("timestamp");

            append10KNoTimestamp(rnd, writer);

            writer.commit();

            Assert.assertEquals(20000, writer.size());
        }

        try (TableWriter writer = new TableWriter(FF, root, name)) {
            append10KNoTimestamp(rnd, writer);
            writer.commit();
            Assert.assertEquals(30000, writer.size());
        }
    }

    @Test
    public void testRemoveTimestampFromPartitionedTable() throws Exception {
        JournalStructure struct = new JournalStructure("ABC").
                $int("productId").
                $str("productName").
                $sym("category").
                $double("price").
                $ts().
                $sym("supplier").
                $();

        String name = createTable(FF, struct.partitionBy(PartitionBy.DAY));

        try (TableWriter writer = new TableWriter(FF, root, name)) {
            try {
                writer.removeColumn("timestamp");
                Assert.fail();
            } catch (CairoException ignore) {
            }
        }
    }

    @Test
    public void testRollbackNonPartitioned() throws Exception {
        create(FF, PartitionBy.NONE);
        testRollback();
    }

    @Test
    public void testRollbackPartitionRemoveFailure() throws Exception {
        create(FF, PartitionBy.DAY);

        class X extends FilesFacadeImpl {
            boolean removeAttempted = false;

            @Override
            public boolean rmdir(CompositePath name) {
                if (Chars.endsWith(name, "2014-08-22")) {
                    removeAttempted = true;
                    return false;
                }
                return super.rmdir(name);
            }
        }

        X ff = new X();

        long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
        Rnd rnd = new Rnd();
        try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {

            for (int i = 0; i < 10000; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 60000);
            }
            writer.commit();

            long timestampAfterCommit = ts;

            for (int i = 0; i < 10000; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 60000);
            }

            Assert.assertEquals(20000, writer.size());
            writer.rollback();

            Assert.assertTrue(ff.removeAttempted);

            ts = timestampAfterCommit;

            // make sure row rollback works after rollback
            writer.newRow(ts).cancel();

            // we should be able to repeat timestamps
            for (int i = 0; i < 10000; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 60000);
            }
            writer.commit();

            Assert.assertEquals(20000, writer.size());
        }
    }

    @Test
    public void testRollbackPartitioned() throws Exception {
        create(FF, PartitionBy.DAY);
        testRollback();
    }

    @Test
    public void testSetAppendPositionFailureBin1() throws Exception {
        testSetAppendPositionFailure("bin.d");
    }

    @Test
    public void testSetAppendPositionFailureBin2() throws Exception {
        testSetAppendPositionFailure("bin.i");
    }

    @Test
    public void testSinglePartitionTruncate() throws Exception {
        long used = Unsafe.getMemUsed();
        create(FF, PartitionBy.YEAR);

        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            writer.truncate();
            Assert.assertEquals(0, writer.size());
        }

        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            Assert.assertEquals(0, writer.size());
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
        Assert.assertEquals(0L, FF.getOpenFileCount());
    }

    @Test
    public void testTableDoesNotExist() throws Exception {
        long mem = Unsafe.getMemUsed();
        try {
            new TableWriter(FF, root, PRODUCT);
            Assert.fail();
        } catch (CairoException e) {
            LOG.info().$((Sinkable) e).$();
        }
        Assert.assertEquals(0, FF.getOpenFileCount());
        Assert.assertEquals(mem, Unsafe.getMemUsed());
    }

    @Test
    public void testTableLock() throws Exception {
        createAllTable();

        try (TableWriter ignored = new TableWriter(FF, root, "all")) {
            try {
                new TableWriter(FF, root, "all");
                Assert.fail();
            } catch (Exception ignored2) {
            }
        }
    }

    @Test
    public void testTruncateCannotAppendTodo() throws Exception {
        testTruncateRecoverableFailure(new TodoAppendDenyingFacade());
    }

    @Test
    public void testTruncateCannotCreateTodo() throws Exception {
        testTruncateRecoverableFailure(new TodoOpenDenyingFacade());
    }

    @Test
    public void testTruncateCannotRemoveTodo() throws Exception {
        class X extends FilesFacadeImpl {
            @Override
            public boolean remove(LPSZ name) {
                return !Chars.endsWith(name, TableWriter.TODO_FILE_NAME) && super.remove(name);
            }
        }

        X ff = new X();

        create(ff, PartitionBy.DAY);
        Rnd rnd = new Rnd();
        long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
        try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
            for (int i = 0; i < 1000; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 6000);
            }
            writer.commit();

            try {
                writer.truncate();
                Assert.fail();
            } catch (CairoError ignore) {
            }
            Assert.assertEquals(0, writer.size());
        }

        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            for (int i = 0; i < 1000; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 6000);
            }
            writer.commit();
            Assert.assertEquals(1000, writer.size());
        }
    }

    @Test
    public void testTxCannotMap() throws Exception {
        long mem = Unsafe.getMemUsed();
        class X extends CountingFilesFacade {
            @Override
            public long mmap(long fd, long len, long offset, int mode) {
                if (--count > 0) {
                    return super.mmap(fd, len, offset, mode);
                }
                return -1;
            }
        }
        X ff = new X();
        create(ff, PartitionBy.NONE);
        try {
            ff.count = 0;
            new TableWriter(ff, root, PRODUCT);
            Assert.fail();
        } catch (CairoException ignore) {
        }
        Assert.assertEquals(0, FF.getOpenFileCount());
        Assert.assertEquals(mem, Unsafe.getMemUsed());
    }

    @Test
    public void testTxFileDoesNotExist() throws Exception {
        testConstructor(new FilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                return !Chars.endsWith(path, TableWriter.TXN_FILE_NAME) && super.exists(path);
            }
        });
    }

    private static JournalStructure getTestStructure() {
        return new JournalStructure(PRODUCT).
                $int("productId").
                $str("productName").
                $sym("supplier").index().buckets(100).
                $sym("category").index().buckets(100).
                $double("price").
                $ts();
    }

    private void addColumnAndPopulate() throws NumericException {
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            writer.addColumn("xyz", ColumnType.STRING);
            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

            int N = 100000;
            Rnd rnd = new Rnd();
            for (int i = 0; i < N; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 60000);
            }
            writer.commit();
            Assert.assertEquals(N, writer.size());
        }
    }

    private long append10KNoSupplier(long ts, Rnd rnd, TableWriter writer) {
        int productId = writer.getColumnIndex("productId");
        int productName = writer.getColumnIndex("productName");
        int category = writer.getColumnIndex("category");
        int price = writer.getColumnIndex("price");

        for (int i = 0; i < 10000; i++) {
            TableWriter.Row r = writer.newRow(ts += (long) 60000);
            r.putInt(productId, rnd.nextPositiveInt());
            r.putStr(productName, rnd.nextString(4));
            r.putStr(category, rnd.nextString(11));
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
            TableWriter.Row r = writer.newRow(0);
            r.putInt(productId, rnd.nextPositiveInt());
            r.putStr(productName, rnd.nextString(10));
            r.putStr(supplier, rnd.nextString(4));
            r.putStr(category, rnd.nextString(11));
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

        for (int i = 0; i < 10000; i++) {
            TableWriter.Row r = writer.newRow(ts += (long) 60000);
            r.putInt(productId, rnd.nextPositiveInt());
            r.putStr(productName, rnd.nextString(10));
            r.putStr(supplier, rnd.nextString(4));
            r.putStr(category, rnd.nextString(11));
            r.putDouble(price, rnd.nextDouble());
            r.append();
        }

        return ts;
    }

    private void appendAndAssert10K(long ts, Rnd rnd) {
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            Assert.assertEquals(12, writer.columns.size());

            for (int i = 0; i < 10000; i++) {
                ts = populateRow(writer, ts, rnd, 60000);
            }
            writer.commit();
            Assert.assertEquals(30000, writer.size());
        }
    }

    private void create(FilesFacade ff, int partitionBy) {
        createTable(ff, getTestStructure().partitionBy(partitionBy));
    }

    private void createAllTable() {
        createTable(FF, new JournalStructure("all").
                $int("int").
                $short("short").
                $byte("byte").
                $double("double").
                $float("float").
                $long("long").
                $str("str").
                $sym("sym").
                $bool("bool").
                $bin("bin").
                $date("date"));
    }

    private String createTable(FilesFacade ff, JournalStructure struct) {
        String name = struct.getName();
        try (TableUtils tabU = new TableUtils(ff)) {
            if (tabU.exists(root, name) == 1) {
                tabU.create(root, struct.build(), 509);
            } else {
                throw CairoException.instance(0).put("Table ").put(name).put(" already exists");
            }
        }
        return name;
    }

    private int getDirCount() {
        AtomicInteger count = new AtomicInteger();
        try (CompositePath path = new CompositePath()) {
            FF.iterateDir(path.of(root).concat(PRODUCT).$(), (name, type) -> {
                if (type == Files.DT_DIR) {
                    count.incrementAndGet();
                }
            });
        }
        return count.get();
    }

    private void populateAndColumnPopulate(int n) throws NumericException {
        Rnd rnd = new Rnd();
        long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            for (int i = 0; i < n; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 60000);
            }
            writer.commit();

            Assert.assertEquals(n, writer.size());

            writer.addColumn("abc", ColumnType.STRING);

            // add more data including updating new column
            ts = populateTable2(rnd, writer, ts, n);

            writer.commit();

            Assert.assertEquals(2 * n, writer.size());
        }

        // append more
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            populateTable2(rnd, writer, ts, n);
            Assert.assertEquals(3 * n, writer.size());
            writer.commit();
            Assert.assertEquals(3 * n, writer.size());
        }
    }

    private long populateRow(TableWriter writer, long ts, Rnd rnd, long increment) {
        TableWriter.Row r = writer.newRow(ts += increment);
        r.putInt(0, rnd.nextPositiveInt());
        r.putStr(1, rnd.nextString(7));
        r.putStr(2, rnd.nextString(4));
        r.putStr(3, rnd.nextString(11));
        r.putDouble(4, rnd.nextDouble());
        r.append();
        return ts;
    }

    long populateTable(FilesFacade ff) throws NumericException {
        return populateTable(ff, PartitionBy.DAY);
    }

    long populateTable(FilesFacade ff, int partitionBy) throws NumericException {
        long used = Unsafe.getMemUsed();
        create(ff, partitionBy);
        long ts = populateTable0(ff);
        Assert.assertEquals(used, Unsafe.getMemUsed());
        Assert.assertEquals(0L, ff.getOpenFileCount());
        return ts;
    }

    private long populateTable0(FilesFacade ff) throws NumericException {
        try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
            Rnd rnd = new Rnd();
            for (int i = 0; i < 10000; i++) {
                ts = populateRow(writer, ts, rnd, 60000);
            }
            writer.commit();
            Assert.assertEquals(10000, writer.size());
            return ts;
        }
    }

    private long populateTable2(Rnd rnd, TableWriter writer, long ts, int n) {
        for (int i = 0; i < n; i++) {
            TableWriter.Row r = writer.newRow(ts += (long) (60 * 60000));
            r.putInt(0, rnd.nextPositiveInt());
            r.putStr(1, rnd.nextString(7));
            r.putStr(2, rnd.nextString(4));
            r.putStr(3, rnd.nextString(11));
            r.putDouble(4, rnd.nextDouble());
            r.putStr(6, rnd.nextString(5));
            r.append();
        }
        return ts;
    }

    private void testAddColumnErrorFollowedByRepairFail(FilesFacade ff) throws NumericException {
        long ts = populateTable(FF);
        long mem = Unsafe.getMemUsed();
        Rnd rnd = new Rnd();
        try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
            for (int i = 0; i < 10000; i++) {
                ts = populateRow(writer, ts, rnd, 60000);
            }
            writer.commit();
            Assert.assertEquals(20000, writer.size());

            Assert.assertEquals(12, writer.columns.size());

            try {
                writer.addColumn("abc", ColumnType.STRING);
                Assert.fail();
            } catch (CairoError ignore) {
            }
        }

        try {
            new TableWriter(ff, root, PRODUCT);
            Assert.fail();
        } catch (CairoException ignore) {
        }

        appendAndAssert10K(ts, rnd);

        Assert.assertEquals(mem, Unsafe.getMemUsed());
        Assert.assertEquals(0, Files.getOpenFileCount());
    }

    private void testAddColumnRecoverableFault(FilesFacade ff) throws NumericException {
        long ts = populateTable(FF);
        long mem = Unsafe.getMemUsed();
        Rnd rnd = new Rnd();
        try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
            Assert.assertEquals(12, writer.columns.size());
            for (int i = 0; i < 10000; i++) {
                ts = populateRow(writer, ts, rnd, 60000);
            }
            writer.commit();
            try {
                writer.addColumn("abc", ColumnType.STRING);
                Assert.fail();
            } catch (CairoException ignore) {
            }

            // ignore error and add more rows

            for (int i = 0; i < 10000; i++) {
                ts = populateRow(writer, ts, rnd, 60000);
            }
            writer.commit();
            Assert.assertEquals(30000, writer.size());
        }

        try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
            for (int i = 0; i < 10000; i++) {
                ts = populateRow(writer, ts, rnd, 60000);
            }
            writer.commit();
            Assert.assertEquals(40000, writer.size());
        }

        Assert.assertEquals(mem, Unsafe.getMemUsed());
        Assert.assertEquals(0, Files.getOpenFileCount());
    }

    private long testAppendNulls(Rnd rnd, FilesFacade ff, long ts) throws NumericException {
        final int blobLen = 64 * 1024;
        long blob = Unsafe.malloc(blobLen);
        try (TableWriter writer = new TableWriter(ff, root, "all")) {
            long size = writer.size();
            for (int i = 0; i < 10000; i++) {
                TableWriter.Row r = writer.newRow(ts += 60 * 60000);
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
                    rnd.nextChars(blob, blobLen);
                    r.putBin(9, blob, blobLen);
                }

                r.append();
            }
            writer.commit();

            Assert.assertEquals(size + 10000, writer.size());
        } finally {
            Unsafe.free(blob, blobLen);
        }
        return ts;
    }

    void testCommitRetryAfterFailure(CountingFilesFacade ff) throws NumericException {
        long failureCount = 0;
        long used = Unsafe.getMemUsed();
        create(ff, PartitionBy.DAY);
        boolean valid = false;
        try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {

            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();
            for (int i = 0; i < 100000; i++) {
                // one record per hour
                ts = populateRow(writer, ts, rnd, 60 * 60000);
                // do not commit often, let transaction size grow
                if (rnd.nextPositiveInt() % 100 == 0) {

                    // reduce frequency of failures
                    boolean fail = rnd.nextPositiveInt() % 20 == 0;
                    if (fail) {
                        // if we destined to fail, prepare to retry commit
                        try {
                            // do not fail on first partition, fail on last
                            ff.count = writer.txPartitionCount - 1;
                            valid = valid || writer.txPartitionCount > 1;
                            writer.commit();
                            // sometimes commit may pass because transaction does not span multiple partition
                            // out transaction size is random after all
                            // if this happens return count to non-failing state
                            ff.count = Long.MAX_VALUE;
                        } catch (CairoException ignore) {
                            failureCount++;
                            ff.count = Long.MAX_VALUE;
                            writer.commit();
                        }
                    } else {
                        writer.commit();
                    }
                }
            }
        }
        // test is valid if we covered cases of failed commit on transactions that span
        // multiple partitions
        Assert.assertTrue(valid);
        Assert.assertTrue(failureCount > 0);
        Assert.assertEquals(used, Unsafe.getMemUsed());
        Assert.assertEquals(0L, FF.getOpenFileCount());
    }

    private void testConstructor(FilesFacade ff) {
        testConstructor(ff, true);
    }

    private void testConstructor(FilesFacade ff, boolean create) {
        long mem = Unsafe.getMemUsed();
        if (create) {
            create(ff, PartitionBy.NONE);
        }
        try {
            new TableWriter(ff, root, PRODUCT);
            Assert.fail();
        } catch (CairoException e) {
            LOG.info().$((Sinkable) e).$();
        }
        Assert.assertEquals(0, ff.getOpenFileCount());
        Assert.assertEquals(mem, Unsafe.getMemUsed());
    }

    private void testOutOfOrderRecords() throws NumericException {
        long used = Unsafe.getMemUsed();
        int N = 10000;
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {

            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();
            int i = 0;
            long failureCount = 0;
            while (i < N) {
                TableWriter.Row r;
                boolean fail = rnd.nextBoolean();
                if (fail) {
                    try {
                        writer.newRow(0);
                        Assert.fail();
                    } catch (CairoException ignore) {
                        failureCount++;
                    }
                    continue;
                } else {
                    r = writer.newRow(ts += (long) (60 * 60000));
                }
                r.putInt(0, rnd.nextPositiveInt());
                r.putStr(1, rnd.nextString(7));
                r.putStr(2, rnd.nextString(4));
                r.putStr(3, rnd.nextString(11));
                r.putDouble(4, rnd.nextDouble());
                r.append();
                i++;
            }
            writer.commit();
            Assert.assertEquals(N, writer.size());
            Assert.assertTrue(failureCount > 0);
        }

        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            Assert.assertEquals((long) N, writer.size());
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
        Assert.assertEquals(0L, FF.getOpenFileCount());
    }

    private void testRemoveColumn(FilesFacade ff) throws NumericException {
        create(FF, PartitionBy.DAY);
        long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
        Rnd rnd = new Rnd();
        long mem = Unsafe.getMemUsed();
        try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
            ts = append10KProducts(ts, rnd, writer);
            writer.commit();

            try {
                writer.removeColumn("supplier");
                Assert.fail();
            } catch (CairoException ignore) {
            }

            ts = append10KProducts(ts, rnd, writer);
            writer.commit();
        }

        try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
            append10KProducts(ts, rnd, writer);
            writer.commit();
            Assert.assertEquals(30000, writer.size());
        }
        Assert.assertEquals(mem, Unsafe.getMemUsed());
        Assert.assertEquals(0, Files.getOpenFileCount());
    }

    private void testRemoveColumn(JournalStructure struct) throws NumericException {
        String name = createTable(FF, struct.partitionBy(PartitionBy.DAY));

        long mem = Unsafe.getMemUsed();

        long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

        Rnd rnd = new Rnd();
        try (TableWriter writer = new TableWriter(FF, root, name)) {

            ts = append10KProducts(ts, rnd, writer);

            writer.removeColumn("supplier");

            final NativeLPSZ lpsz = new NativeLPSZ();
            try (CompositePath path = new CompositePath()) {
                path.of(root).concat(name);
                final int plen = path.length();
                FF.iterateDir(path.$(), (file, type) -> {
                    lpsz.of(file);
                    if (type == Files.DT_DIR && !Chars.equals(lpsz, '.') && !Chars.equals(lpsz, "..")) {
                        Assert.assertFalse(FF.exists(path.trimTo(plen).concat(lpsz).concat("supplier.i").$()));
                        Assert.assertFalse(FF.exists(path.trimTo(plen).concat(lpsz).concat("supplier.d").$()));
                        Assert.assertFalse(FF.exists(path.trimTo(plen).concat(lpsz).concat("supplier.top").$()));
                    }
                });
            }

            ts = append10KNoSupplier(ts, rnd, writer);

            writer.commit();

            Assert.assertEquals(20000, writer.size());
        }

        try (TableWriter writer = new TableWriter(FF, root, name)) {
            append10KNoSupplier(ts, rnd, writer);
            writer.commit();
            Assert.assertEquals(30000, writer.size());
        }

        Assert.assertEquals(mem, Unsafe.getMemUsed());
        Assert.assertEquals(0, Files.getOpenFileCount());
    }

    private void testRollback() throws NumericException {
        long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
        Rnd rnd = new Rnd();
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            for (int i = 0; i < 10000; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 60000);
            }
            writer.commit();

            long timestampAfterCommit = ts;

            for (int i = 0; i < 10000; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 60000);
            }

            Assert.assertEquals(20000, writer.size());
            writer.rollback();
            Assert.assertEquals(10000, writer.size());
            writer.rollback();
            Assert.assertEquals(10000, writer.size());

            ts = timestampAfterCommit;

            // make sure row rollback works after rollback
            writer.newRow(ts).cancel();

            // we should be able to repeat timestamps
            for (int i = 0; i < 10000; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 60000);
            }
            writer.commit();

            Assert.assertEquals(20000, writer.size());
        }
    }

    private void testSetAppendPositionFailure(String failFile) throws NumericException {
        createAllTable();

        class X extends FilesFacadeImpl {
            long fd = -1;

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, failFile)) {
                    return fd = super.openRW(name);
                }
                return super.openRW(name);
            }

            @Override
            public long read(long fd, long buf, int len, long offset) {
                if (fd == this.fd) {
                    this.fd = -1;
                    return -1;
                }
                return super.read(fd, buf, len, offset);
            }
        }
        final X ff = new X();
        long mem = Unsafe.getMemUsed();
        testAppendNulls(new Rnd(), FF, DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z"));
        try {
            new TableWriter(ff, root, "all");
            Assert.fail();
        } catch (CairoException ignore) {
        }
        Assert.assertEquals(mem, Unsafe.getMemUsed());
        Assert.assertEquals(0, ff.getOpenFileCount());
    }

    private void testTruncate(CountingFilesFacade ff, boolean retry) throws NumericException {
        long used = Unsafe.getMemUsed();
        create(ff, PartitionBy.DAY);
        Rnd rnd = new Rnd();
        try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {

            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

            for (int k = 0; k < 3; k++) {
                for (int i = 0; i < 2000; i++) {
                    ts = populateRow(writer, ts, rnd, 60 * 60000);
                }
                writer.commit();
                Assert.assertEquals(2000, writer.size());

                // this truncate will fail quite early and will leave
                // table in inconsistent state to recover from which
                // truncate has to be repeated
                try {
                    ff.count = 3;
                    writer.truncate();
                    Assert.fail();
                } catch (CairoException e) {
                    LOG.info().$((Sinkable) e).$();
                }

                if (retry) {
                    // retry
                    writer.truncate();
                } else {
                    break;
                }
            }
        }

        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            long ts = DateFormatUtils.parseDateTime("2014-03-04T00:00:00.000Z");
            Assert.assertEquals(0, writer.size());
            for (int i = 0; i < 1000; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 60000);
            }
            writer.commit();
            Assert.assertEquals(1000, writer.size());
        }

        // open writer one more time and just assert the size
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            Assert.assertEquals(1000, writer.size());
        }

        Assert.assertEquals(used, Unsafe.getMemUsed());
        Assert.assertEquals(0L, ff.getOpenFileCount());
    }

    private void testTruncateRecoverableFailure(FilesFacade ff) throws NumericException {
        create(ff, PartitionBy.DAY);
        Rnd rnd = new Rnd();
        long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
        try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
            for (int i = 0; i < 1000; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 6000);
            }
            writer.commit();

            try {
                writer.truncate();
                Assert.fail();
            } catch (CairoException ignore) {
            }
            Assert.assertEquals(1000, writer.size());
        }

        try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
            for (int i = 0; i < 1000; i++) {
                ts = populateRow(writer, ts, rnd, 60 * 6000);
            }
            writer.commit();
            Assert.assertEquals(2000, writer.size());
        }
    }

    private void testUnrecoverableAddColumn(FilesFacade ff) throws NumericException {
        long ts = populateTable(FF);
        long mem = Unsafe.getMemUsed();
        Rnd rnd = new Rnd();
        try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
            for (int i = 0; i < 10000; i++) {
                ts = populateRow(writer, ts, rnd, 60000);
            }
            writer.commit();

            Assert.assertEquals(12, writer.columns.size());

            try {
                writer.addColumn("abc", ColumnType.STRING);
                Assert.fail();
            } catch (CairoError ignore) {
            }
        }

        appendAndAssert10K(ts, rnd);

        Assert.assertEquals(mem, Unsafe.getMemUsed());
        Assert.assertEquals(0, Files.getOpenFileCount());
    }

    private void testUnrecoverableRemoveColumn(FilesFacade ff) throws NumericException {
        create(FF, PartitionBy.DAY);
        long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
        Rnd rnd = new Rnd();
        long mem = Unsafe.getMemUsed();
        try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
            ts = append10KProducts(ts, rnd, writer);
            writer.commit();

            try {
                writer.removeColumn("supplier");
                Assert.fail();
            } catch (CairoError ignore) {
            }
        }

        try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
            append10KProducts(ts, rnd, writer);
            writer.commit();
            Assert.assertEquals(20000, writer.size());
        }
        Assert.assertEquals(mem, Unsafe.getMemUsed());
        Assert.assertEquals(0, Files.getOpenFileCount());
    }

    void verifyTimestampPartitions(VirtualMemory vmem, int n) {
        int i;
        DateFormatCompiler compiler = new DateFormatCompiler();
        DateFormat fmt = compiler.compile("yyyy-MM-dd");
        DateLocale enGb = DateLocaleFactory.INSTANCE.getDateLocale("en-gb");

        try (CompositePath vp = new CompositePath()) {
            for (i = 0; i < n; i++) {
                vp.of(root).concat(PRODUCT).put(Path.SEPARATOR);
                fmt.format(vmem.getLong(i * 8), enGb, "UTC", vp);
                if (!FF.exists(vp.$())) {
                    Assert.fail();
                }
            }
        }
    }

    private static class SwapMetaRenameDenyingFacade extends FilesFacadeImpl {
        @Override
        public boolean rename(LPSZ from, LPSZ to) {
            return !Chars.endsWith(from, TableWriter.META_SWAP_FILE_NAME) && super.rename(from, to);
        }
    }

    private static class MetaRenameDenyingFacade extends FilesFacadeImpl {
        @Override
        public boolean rename(LPSZ from, LPSZ to) {
            return !Chars.endsWith(to, TableWriter.META_PREV_FILE_NAME) && super.rename(from, to);
        }
    }

    private static class TodoOpenDenyingFacade extends FilesFacadeImpl {

        @Override
        public long openAppend(LPSZ name) {
            if (Chars.endsWith(name, TableWriter.TODO_FILE_NAME)) {
                return -1;
            }
            return super.openAppend(name);
        }
    }

    private static class TodoAppendDenyingFacade extends FilesFacadeImpl {
        long fd = -1;

        @Override
        public long append(long fd, long buf, int len) {
            if (fd == this.fd) {
                this.fd = -1;
                return -1;
            }
            return super.append(fd, buf, len);
        }

        @Override
        public long openAppend(LPSZ name) {
            if (Chars.endsWith(name, TableWriter.TODO_FILE_NAME)) {
                return fd = super.openAppend(name);
            }
            return super.openAppend(name);
        }


    }

    class CountingFilesFacade extends FilesFacadeImpl {
        long count = Long.MAX_VALUE;
    }
}