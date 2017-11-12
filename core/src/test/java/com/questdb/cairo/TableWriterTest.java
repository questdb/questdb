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

import com.questdb.ex.NumericException;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.*;
import com.questdb.ql.Record;
import com.questdb.std.Sinkable;
import com.questdb.std.str.LPSZ;
import com.questdb.std.str.NativeLPSZ;
import com.questdb.std.str.Path;
import com.questdb.std.time.*;
import com.questdb.store.ColumnType;
import com.questdb.store.PartitionBy;
import com.questdb.store.factory.configuration.JournalStructure;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class TableWriterTest extends AbstractCairoTest {

    public static final String PRODUCT = "product";
    private static final FilesFacade FF = FilesFacadeImpl.INSTANCE;
    private static final Log LOG = LogFactory.getLog(TableWriterTest.class);

    @Test
    public void tesFrequentCommit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.NONE);
            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {

                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

                Rnd rnd = new Rnd();
                for (int i = 0; i < 100000; i++) {
                    ts = populateRow(writer, ts, rnd, 60 * 60000);
                    writer.commit();
                }
            }
        });
    }

    @Test
    public void testAddColumnAndFailToReadTopFile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.DAY);
            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
                int N = 10000;
                Rnd rnd = new Rnd();
                populateProducts(writer, rnd, ts, N, 60000);
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
    public void testAddColumnCommitPartitioned() throws Exception {
        create(FF, PartitionBy.DAY);
        Rnd rnd = new Rnd();
        long interval = 60000L;
        int count = 10000;
        long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            ts = populateProducts(writer, rnd, ts, count, interval);
            Assert.assertEquals(count, writer.size());
            writer.addColumn("abc", ColumnType.STRING);
            // add more data including updating new column
            ts = populateTable2(rnd, writer, ts, count, interval);
            Assert.assertEquals(2 * count, writer.size());
            writer.rollback();
        }

        // append more
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            populateTable2(rnd, writer, ts, count, interval);
            writer.commit();
            Assert.assertEquals(2 * count, writer.size());
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
            populateProducts(writer, new Rnd(), ts, 10000, 6000);
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
                return !(Chars.endsWith(from, TableUtils.META_PREV_FILE_NAME) && --count == 0) && super.rename(from, to);
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
        create(FF, PartitionBy.DAY);
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

        try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
            writer.addColumn("xyz", ColumnType.STRING);
            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

            int N = 10000;
            Rnd rnd = new Rnd();
            populateProducts(writer, rnd, ts, N, 6 * 60000);
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
    public void testAddColumnMetaSwapRenameFail2() throws Exception {
        testUnrecoverableAddColumn(new FilesFacadeImpl() {
            int count = 1;

            @Override
            public boolean rename(LPSZ from, LPSZ to) {
                return !Chars.endsWith(from, TableUtils.META_SWAP_FILE_NAME) && super.rename(from, to);
            }

            @Override
            public boolean remove(LPSZ name) {
                return !(Chars.endsWith(name, TableUtils.TODO_FILE_NAME) && --count == 0) && super.remove(name);
            }
        });
    }

    @Test
    public void testAddColumnNonPartitioned() throws Exception {
        create(FF, PartitionBy.NONE);
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            writer.addColumn("xyz", ColumnType.STRING);
            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

            int N = 100000;
            Rnd rnd = new Rnd();
            populateProducts(writer, rnd, ts, N, 60 * 60000);
            writer.commit();
            Assert.assertEquals(N, writer.size());
        }
    }

    @Test
    public void testAddColumnPartitioned() throws Exception {
        create(FF, PartitionBy.DAY);
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            writer.addColumn("xyz", ColumnType.STRING);
            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

            int N = 10000;
            Rnd rnd = new Rnd();
            populateProducts(writer, rnd, ts, N, 60000);
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
            populateTable(FF);
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

            try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
                Assert.assertEquals(12, writer.columns.size());
                writer.addColumn("abc", ColumnType.STRING);
                Assert.assertEquals(14, writer.columns.size());
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
    public void testAddColumnSwpFileMapFail() throws Exception {
        testAddColumnRecoverableFault(new FilesFacadeImpl() {
            long fd = -1;

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, TableUtils.META_SWAP_FILE_NAME)) {
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
        populateAndColumnPopulate();
    }

    @Test
    public void testAddColumnToNonEmptyPartitioned() throws Exception {
        create(FF, PartitionBy.DAY);
        populateAndColumnPopulate();
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
        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.NONE);
            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {

                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
                TableWriter.Row r = writer.newRow(ts);
                r.putInt(0, 1234);
                populateProducts(writer, new Rnd(), ts, 10000, 60 * 60000);
                Assert.assertEquals(10000, writer.size());
            }
        });
    }

    @Test
    public void testCancelFailureFollowedByTableClose() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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
                ts = populateProducts(writer, rnd, ts, N, 60 * 60000);
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
        });
    }

    @Test
    public void testCancelFirstRowFailurePartitioned() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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
            Rnd rnd = new Rnd();
            create(ff, PartitionBy.DAY);
            try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
                // add 48 hours
                ts = populateProducts(writer, rnd, ts, 47, 60 * 60000);
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

                populateProducts(writer, rnd, ts, 47, 60 * 60000);

                writer.commit();
                Assert.assertEquals(94, writer.size());
                Assert.assertTrue(getDirCount() == 6);
            }
        });
    }

    @Test
    public void testCancelFirstRowNonPartitioned() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.NONE);
            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {

                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");


                TableWriter.Row r = writer.newRow(ts);
                r.putInt(0, 1234);
                r.cancel();

                populateProducts(writer, new Rnd(), ts, 10000, 60 * 60000);
                Assert.assertEquals(10000, writer.size());
            }
        });
    }

    @Test
    public void testCancelFirstRowPartitioned() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.DAY);
            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
                TableWriter.Row r = writer.newRow(ts);
                r.cancel();
                writer.commit();
                Assert.assertEquals(0, writer.size());
                Assert.assertTrue(getDirCount() == 2);
            }
        });
    }

    @Test
    public void testCancelFirstRowPartitioned2() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();
            create(FF, PartitionBy.DAY);
            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
                // add 48 hours
                ts = populateProducts(writer, rnd, ts, 47, 60 * 60000);

                TableWriter.Row r = writer.newRow(ts += 60 * 60000);
                r.putInt(0, rnd.nextPositiveInt());
                r.putStr(1, rnd.nextString(7));
                r.putStr(2, rnd.nextString(4));
                r.putStr(3, rnd.nextString(11));
                r.putDouble(4, rnd.nextDouble());

                for (int i = 0; i < 1000; i++) {
                    r.cancel();
                }

                populateProducts(writer, rnd, ts, 47, 60 * 60000);

                writer.commit();
                Assert.assertEquals(94, writer.size());
                Assert.assertTrue(getDirCount() == 6);
            }
        });
    }

    @Test
    public void testCancelMidPartition() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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
                        TableWriter.Row r = writer.newRow(ts += 60000);
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
        });
    }

    @Test
    public void testCancelMidRowNonPartitioned() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.NONE);
            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

                int cancelCount = 0;
                Rnd rnd = new Rnd();
                int i = 0;
                TableWriter.Row r;
                while (i < 10000) {
                    r = writer.newRow(ts += 60000);
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
        });
    }

    @Test
    public void testCancelRowAfterAddColumn() throws Exception {
        create(FF, PartitionBy.DAY);
        Rnd rnd = new Rnd();
        long interval = 60000;
        int count = 10000;
        long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            ts = populateProducts(writer, rnd, ts, count, interval);

            Assert.assertEquals(count, writer.size());

            writer.addColumn("abc", ColumnType.STRING);

            TableWriter.Row r = writer.newRow(ts);
            r.putInt(0, rnd.nextInt());
            r.cancel();

            Assert.assertEquals(0L, writer.columns.getQuick(13).getAppendOffset());

            // add more data including updating new column
            ts = populateTable2(rnd, writer, ts, count, interval);
            Assert.assertEquals(2 * count, writer.size());

            writer.rollback();
        }

        // append more
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            populateTable2(rnd, writer, ts, count, interval);
            writer.commit();
            Assert.assertEquals(2 * count, writer.size());
        }
    }

    @Test
    public void testCancelRowRecovery() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd();

            class X extends FilesFacadeImpl {
                boolean fail = false;

                @Override
                public boolean rmdir(Path name) {
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
        });
    }

    @Test
    public void testCancelRowRecoveryFromAppendPosErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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
                        TableWriter.Row r = writer.newRow(ts += 60000);
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
        create(FF, PartitionBy.NONE);
        TestUtils.assertMemoryLeak(() -> {
            TestFilesFacade ff = new TestFilesFacade() {
                boolean ran = false;

                @Override
                public boolean wasCalled() {
                    return ran;
                }

                @Override
                public long openRW(LPSZ name) {
                    if (Chars.endsWith(name, PRODUCT + ".lock")) {
                        ran = true;
                        return -1;
                    }
                    return super.openRW(name);
                }


            };

            try {
                new TableWriter(ff, root, PRODUCT);
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
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, TableUtils.TXN_FILE_NAME) && --count == 0) {
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
                return Chars.endsWith(path, TableUtils.TODO_FILE_NAME) || super.exists(path);
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
    // tests scenario where truncate is supported (linux) but fails on close
    // close is expected not to fail
    public void testCannotTruncateColumnOnClose() throws Exception {
        create(FF, PartitionBy.NONE);
        testTruncateOnClose(new TestFilesFacade() {
            long fd = -1;
            int count = 1;
            boolean ran = false;

            @Override
            public boolean wasCalled() {
                return fd != -1 && ran;
            }

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "price.d")) {
                    return fd = super.openRW(name);
                }
                return super.openRW(name);
            }

            @Override
            public boolean supportsTruncateMappedFiles() {
                return true;
            }

            @Override
            public boolean truncate(long fd, long size) {
                if (this.fd == fd && count-- == 0) {
                    ran = true;
                    return false;
                }
                return super.truncate(fd, size);
            }


        });
    }

    @Test
    // tests scenario where truncate is not supported (windows) but fails on close
    // truncate on close fails once and then succeeds
    // close is expected not to fail
    public void testCannotTruncateColumnOnCloseAndNotSupported() throws Exception {
        create(FF, PartitionBy.NONE);
        testTruncateOnClose(new TestFilesFacade() {
            long fd = -1;
            int count = 1;
            boolean ran = false;

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "price.d")) {
                    return fd = super.openRW(name);
                }
                return super.openRW(name);
            }

            @Override
            public boolean supportsTruncateMappedFiles() {
                return false;
            }

            @Override
            public boolean truncate(long fd, long size) {
                if (this.fd == fd && count-- == 0) {
                    ran = true;
                    return false;
                }
                return super.truncate(fd, size);
            }

            @Override
            public boolean wasCalled() {
                return fd != -1 && ran;
            }
        });
    }

    @Test
    // tests scenario where truncate is not supported (windows) but fails on close
    // truncate on close fails all the time
    public void testCannotTruncateColumnOnCloseAndNotSupported2() throws Exception {
        create(FF, PartitionBy.NONE);
        testTruncateOnClose(new TestFilesFacade() {
            long fd = -1;
            int count = 1;
            boolean ran = false;

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, "price.d")) {
                    return fd = super.openRW(name);
                }
                return super.openRW(name);
            }

            @Override
            public boolean supportsTruncateMappedFiles() {
                return false;
            }

            @Override
            public boolean truncate(long fd, long size) {
                if (this.fd == fd && count-- <= 0) {
                    ran = true;
                    return false;
                }
                return super.truncate(fd, size);
            }

            @Override
            public boolean wasCalled() {
                return fd != -1 && ran;
            }
        });
    }

    @Test
    public void testConstructorTruncatedTodo() throws Exception {
        FilesFacade ff = new FilesFacadeImpl() {
            long fd = 7686876823L;

            @Override
            public boolean exists(LPSZ path) {
                return Chars.endsWith(path, TableUtils.TODO_FILE_NAME) || super.exists(path);
            }

            @Override
            public long openRO(LPSZ name) {
                if (Chars.endsWith(name, TableUtils.TODO_FILE_NAME)) {
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
        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.DAY);
            int N = 10000;

            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
                populateProducts(writer, new Rnd(), DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z"), N, 60000);
                writer.commit();
                Assert.assertEquals(N, writer.size());
            }

            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
                Assert.assertEquals((long) N, writer.size());
            }
        });
    }

    @Test
    public void testDayPartitionRemoveFileError() throws Exception {
        CountingFilesFacade ff = new CountingFilesFacade() {
            @Override
            public boolean remove(LPSZ name) {
                if (Chars.endsWith(name, "test.dat")) {
                    if (--count == 0) {
                        return false;
                    }
                }
                return super.remove(name);
            }
        };


        TestUtils.assertMemoryLeak(() -> {
            create(ff, PartitionBy.DAY);

            try (Path path = new Path().of(root)) {
                path.concat(PRODUCT).concat("test.dat").$();
                Assert.assertTrue(Files.touch(path));
            }

            Rnd rnd = new Rnd();
            try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {

                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

                populateProducts(writer, rnd, ts, 200, 60 * 60000);
                writer.commit();
                Assert.assertEquals(200, writer.size());

                // this truncate will fail quite early and will leave
                // table in inconsistent state to recover from which
                // truncate has to be repeated
                try {
                    ff.count = 1;
                    writer.truncate();
                    Assert.fail();
                } catch (CairoException e) {
                    LOG.info().$((Sinkable) e).$();
                }
                writer.truncate();
            }

            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
                long ts = DateFormatUtils.parseDateTime("2014-03-04T00:00:00.000Z");
                Assert.assertEquals(0, writer.size());
                populateProducts(writer, rnd, ts, 1000, 60 * 60000);
                writer.commit();
                Assert.assertEquals(1000, writer.size());
            }

            // open writer one more time and just assert the size
            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
                Assert.assertEquals(1000, writer.size());
            }
        });
    }

    @Test
    public void testDayPartitionRmDirError() throws Exception {
        testTruncate(new CountingFilesFacade() {
            @Override
            public boolean rmdir(Path name) {
                return --count != 0 && super.rmdir(name);
            }
        }, true);
    }

    @Test
    public void testDayPartitionTruncate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.DAY);
            Rnd rnd = new Rnd();
            int count = 10000;
            long interval = 60000L;
            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {

                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

                for (int k = 0; k < 3; k++) {
                    ts = populateProducts(writer, rnd, ts, count, interval);
                    writer.commit();
                    Assert.assertEquals(count, writer.size());
                    writer.truncate();
                }
            }

            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
                long ts = DateFormatUtils.parseDateTime("2014-03-04T00:00:00.000Z");
                Assert.assertEquals(0, writer.size());
                populateProducts(writer, rnd, ts, count, interval);
                writer.commit();
                Assert.assertEquals(count, writer.size());
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
        }, true);
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
    public void testIncorrectTodoCode() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            createAllTable();
            long buf = Unsafe.malloc(8);
            try {
                Unsafe.getUnsafe().putLong(buf, 89808823424L);

                try (Path path = new Path().of(root).concat("all").concat(TableUtils.TODO_FILE_NAME).$()) {
                    long fd = Files.openRW(path);
                    Assert.assertTrue(fd != -1);
                    Assert.assertEquals(8, Files.write(fd, buf, 8, 0));
                    Files.close(fd);
                }
            } finally {
                Unsafe.free(buf, 8);
            }

            try (TableWriter writer = new TableWriter(FF, root, "all")) {
                Assert.assertNotNull(writer);
                Assert.assertTrue(writer.isOpen());
            }
        });
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
        TestUtils.assertMemoryLeak(() -> {
            createAllTable();
            Rnd rnd = new Rnd();
            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
            ts = testAppendNulls(rnd, FF, ts);
            testAppendNulls(rnd, FF, ts);
        });
    }

    @Test
    public void testOpenWriterMissingTxFile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            createAllTable();
            try (Path path = new Path()) {
                Assert.assertTrue(FF.remove(path.of(root).concat("all").concat(TableUtils.TXN_FILE_NAME).$()));
                try {
                    new TableWriter(FF, root, "all");
                    Assert.fail();
                } catch (CairoException ignore) {
                }
            }
        });
    }

    @Test
    public void testOutOfOrderAfterReopen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            createAllTable();
            Rnd rnd = new Rnd();
            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
            testAppendNulls(rnd, FF, ts);
            try {
                testAppendNulls(rnd, FF, ts);
                Assert.fail();
            } catch (CairoException ignore) {
            }
        });
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
        testRemoveColumnRecoverableFailure(new TodoAppendDenyingFacade());
    }

    @Test
    public void testRemoveColumnCannotMMapSwap() throws Exception {
        class X extends TestFilesFacade {

            long fd = -1;
            boolean hit = false;

            @Override
            public boolean wasCalled() {
                return hit;
            }

            @Override
            public long openRW(LPSZ name) {
                if (Chars.endsWith(name, TableUtils.META_SWAP_FILE_NAME)) {
                    return fd = super.openRW(name);
                }
                return super.openRW(name);
            }

            @Override
            public long mmap(long fd, long len, long offset, int mode) {
                if (fd == this.fd) {
                    this.fd = -1;
                    this.hit = true;
                    return -1;
                }
                return super.mmap(fd, len, offset, mode);
            }
        }
        testRemoveColumnRecoverableFailure(new X());
    }

    @Test
    public void testRemoveColumnCannotOpenSwap() throws Exception {
        class X extends TestFilesFacade {

            boolean hit = false;

            @Override
            public boolean wasCalled() {
                return hit;
            }

            @Override
            public long openRW(LPSZ name) {
                if (Chars.contains(name, TableUtils.META_SWAP_FILE_NAME)) {
                    hit = true;
                    return -1;
                }
                return super.openRW(name);
            }
        }
        testRemoveColumnRecoverableFailure(new X());
    }

    @Test
    public void testRemoveColumnCannotOpenTodo() throws Exception {
        testRemoveColumnRecoverableFailure(new TodoOpenDenyingFacade());
    }

    @Test
    public void testRemoveColumnCannotRemoveAnyMetadataPrev() throws Exception {
        testRemoveColumnRecoverableFailure(new TestFilesFacade() {
            int exists = 0;
            int removes = 0;

            @Override
            public boolean wasCalled() {
                return exists > 0 && removes > 0;
            }

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
        });
    }

    @Test
    public void testRemoveColumnCannotRemoveFiles() throws Exception {
        removeColumn(new TestFilesFacade() {
            int count = 0;

            @Override
            public boolean wasCalled() {
                return count > 0;
            }

            @Override
            public boolean remove(LPSZ name) {
                if (Chars.endsWith(name, "supplier.d")) {
                    count++;
                    return false;
                }
                return super.remove(name);
            }
        });
    }

    @Test
    public void testRemoveColumnCannotRemoveSomeMetadataPrev() throws Exception {
        removeColumn(new TestFilesFacade() {
            int count = 5;

            @Override
            public boolean wasCalled() {
                return count <= 0;
            }

            @Override
            public boolean exists(LPSZ path) {
                if (Chars.contains(path, TableUtils.META_PREV_FILE_NAME)) {
                    if (--count > 0) {
                        return true;
                    }
                }
                return super.exists(path);
            }

            @Override
            public boolean remove(LPSZ name) {
                return !Chars.contains(name, TableUtils.META_PREV_FILE_NAME) && super.remove(name);
            }
        });
    }

    @Test
    public void testRemoveColumnCannotRemoveSwap() throws Exception {
        class X extends TestFilesFacade {
            boolean hit = false;

            @Override
            public boolean wasCalled() {
                return hit;
            }

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
    public void testRemoveColumnUnrecoverableRemoveTodoFailure() throws Exception {
        class X extends FilesFacadeImpl {
            int count = 1;

            @Override
            public boolean remove(LPSZ name) {
                return (!Chars.endsWith(name, TableUtils.TODO_FILE_NAME) || --count != 0) && super.remove(name);
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
                if (Chars.endsWith(to, TableUtils.META_FILE_NAME)) {
                    if (count-- > 0) {
                        return false;
                    }
                }
                return super.rename(from, to);
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

        String name = CairoTestUtils.createTable(FF, root, struct.partitionBy(PartitionBy.NONE));

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

        String name = CairoTestUtils.createTable(FF, root, struct.partitionBy(PartitionBy.DAY));

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
            public boolean rmdir(Path name) {
                if (Chars.endsWith(name, "2013-03-12")) {
                    removeAttempted = true;
                    return false;
                }
                return super.rmdir(name);
            }
        }

        X ff = new X();

        long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
        final long interval = 60000L;
        int count = 10000;
        Rnd rnd = new Rnd();
        try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {

            ts = populateProducts(writer, rnd, ts, count, interval);
            writer.commit();

            long timestampAfterCommit = ts;

            populateProducts(writer, rnd, ts, count, interval);

            Assert.assertEquals(2 * count, writer.size());
            writer.rollback();

            Assert.assertTrue(ff.removeAttempted);

            ts = timestampAfterCommit;

            // make sure row rollback works after rollback
            writer.newRow(ts).cancel();

            // we should be able to repeat timestamps
            populateProducts(writer, rnd, ts, count, interval);
            writer.commit();

            Assert.assertEquals(2 * count, writer.size());
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
        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.YEAR);

            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
                writer.truncate();
                Assert.assertEquals(0, writer.size());
            }

            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
                Assert.assertEquals(0, writer.size());
            }
        });
    }

    @Test
    public void testTableDoesNotExist() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                new TableWriter(FF, root, PRODUCT);
                Assert.fail();
            } catch (CairoException e) {
                LOG.info().$((Sinkable) e).$();
            }
        });
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
    public void testToString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.NONE);
            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
                Assert.assertEquals("TableWriter{name=product}", writer.toString());
            }
        });
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
                return !Chars.endsWith(name, TableUtils.TODO_FILE_NAME) && super.remove(name);
            }
        }

        X ff = new X();

        create(ff, PartitionBy.DAY);
        Rnd rnd = new Rnd();
        long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
        try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
            ts = populateProducts(writer, rnd, ts, 1000, 60 * 60000);
            writer.commit();

            try {
                writer.truncate();
                Assert.fail();
            } catch (CairoError ignore) {
            }
            Assert.assertEquals(0, writer.size());
        }

        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            populateProducts(writer, rnd, ts, 1000, 60 * 60000);
            writer.commit();
            Assert.assertEquals(1000, writer.size());
        }
    }

    @Test
    public void testTwoByteUtf8() throws Exception {
        JournalStructure struct = new JournalStructure("соотечественник").$str("секьюрити").$ts();
        CairoTestUtils.createTable(FF, root, struct);

        Rnd rnd = new Rnd();
        try (TableWriter writer = new TableWriter(FF, root, struct.getName())) {
            for (int i = 0; i < 1000000; i++) {
                TableWriter.Row r = writer.newRow(0);
                r.putStr(0, rnd.nextChars(5));
                r.append();
            }
            writer.commit();
            writer.addColumn("митинг", ColumnType.INT);
            Assert.assertEquals(0, writer.getColumnIndex("секьюрити"));
            Assert.assertEquals(2, writer.getColumnIndex("митинг"));
        }

        rnd.reset();
        try (TableReader reader = new TableReader(FF, root, struct.getName())) {
            int col = reader.getMetadata().getColumnIndex("секьюрити");
            while (reader.hasNext()) {
                Record r = reader.next();
                TestUtils.assertEquals(rnd.nextChars(5), r.getFlyweightStr(col));
            }
        }
    }

    @Test
    public void testTxCannotMap() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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

    private static JournalStructure getTestStructure() {
        return new JournalStructure(PRODUCT).
                $int("productId").
                $str("productName").
                $sym("supplier").index().buckets(100).
                $sym("category").index().buckets(100).
                $double("price").
                $ts();
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
            populateProducts(writer, rnd, ts, 10000, 60000);
            writer.commit();
            Assert.assertEquals(30000, writer.size());
        }
    }

    private void create(FilesFacade ff, int partitionBy) {
        CairoTestUtils.createTable(ff, root, getTestStructure().partitionBy(partitionBy));
    }

    private void createAllTable() {
        CairoTestUtils.createTable(FF, root, new JournalStructure("all").
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

    private int getDirCount() {
        AtomicInteger count = new AtomicInteger();
        try (Path path = new Path()) {
            FF.iterateDir(path.of(root).concat(PRODUCT).$(), (name, type) -> {
                if (type == Files.DT_DIR) {
                    count.incrementAndGet();
                }
            });
        }
        return count.get();
    }

    private void populateAndColumnPopulate() throws NumericException {
        Rnd rnd = new Rnd();
        int n = 10000;
        long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
        long interval = 60000;
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            ts = populateProducts(writer, rnd, ts, n, interval);
            writer.commit();

            Assert.assertEquals(n, writer.size());

            writer.addColumn("abc", ColumnType.STRING);

            // add more data including updating new column
            ts = populateTable2(rnd, writer, ts, n, interval);

            writer.commit();

            Assert.assertEquals(2 * n, writer.size());
        }

        // append more
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            populateTable2(rnd, writer, ts, n, interval);
            Assert.assertEquals(3 * n, writer.size());
            writer.commit();
            Assert.assertEquals(3 * n, writer.size());
        }
    }

    private long populateProducts(TableWriter writer, Rnd rnd, long ts, int count, long increment) {
        for (int i = 0; i < count; i++) {
            ts = populateRow(writer, ts, rnd, increment);
        }
        return ts;
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
        long fileCount = ff.getOpenFileCount();
        create(ff, partitionBy);
        long ts = populateTable0(ff);
        Assert.assertEquals(used, Unsafe.getMemUsed());
        Assert.assertEquals(fileCount, ff.getOpenFileCount());
        return ts;
    }

    private long populateTable0(FilesFacade ff) throws NumericException {
        try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
            ts = populateProducts(writer, new Rnd(), ts, 10000, 60000);
            writer.commit();
            Assert.assertEquals(10000, writer.size());
            return ts;
        }
    }

    private long populateTable2(Rnd rnd, TableWriter writer, long ts, int n, long interval) {
        for (int i = 0; i < n; i++) {
            TableWriter.Row r = writer.newRow(ts += interval);
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

    private void removeColumn(TestFilesFacade ff) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            JournalStructure struct = new JournalStructure("ABC").
                    $int("productId").
                    $str("productName").
                    $sym("supplier").
                    $sym("category").
                    $double("price").
                    $ts();

            String name = CairoTestUtils.createTable(FF, root, struct.partitionBy(PartitionBy.DAY));

            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();

            try (TableWriter writer = new TableWriter(ff, root, name)) {

                ts = append10KProducts(ts, rnd, writer);

                writer.removeColumn("supplier");

                // assert attempt to remove files
                Assert.assertTrue(ff.wasCalled());

                ts = append10KNoSupplier(ts, rnd, writer);

                writer.commit();

                Assert.assertEquals(20000, writer.size());
            }

            try (TableWriter writer = new TableWriter(ff, root, name)) {
                append10KNoSupplier(ts, rnd, writer);
                writer.commit();
                Assert.assertEquals(30000, writer.size());
            }
        });
    }

    private void testAddColumnAndOpenWriter(int partitionBy, int N) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
            Rnd rnd = new Rnd();

            create(FF, partitionBy);
            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
                ts = populateProducts(writer, rnd, ts, N, 60 * 60000);
                writer.commit();
                Assert.assertEquals(N, writer.size());
            }

            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
                writer.addColumn("xyz", ColumnType.STRING);
            }

            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
                for (int i = 0; i < N; i++) {
                    TableWriter.Row r = writer.newRow(ts += 60L * 60000);
                    r.putInt(0, rnd.nextPositiveInt());
                    r.putStr(1, rnd.nextString(7));
                    r.putStr(2, rnd.nextString(4));
                    r.putStr(3, rnd.nextString(11));
                    r.putDouble(4, rnd.nextDouble());
                    r.putStr(6, rnd.nextString(10));
                    r.append();
                }
                writer.commit();
                Assert.assertEquals(N * 2, writer.size());
            }
        });
    }

    private void testAddColumnErrorFollowedByRepairFail(FilesFacade ff) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long ts = populateTable(FF);
            Rnd rnd = new Rnd();
            try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
                ts = populateProducts(writer, rnd, ts, 10000, 60000);
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
        });
    }

    private void testAddColumnRecoverableFault(FilesFacade ff) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long ts = populateTable(FF);
            Rnd rnd = new Rnd();
            try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
                Assert.assertEquals(12, writer.columns.size());
                ts = populateProducts(writer, rnd, ts, 10000, 60000);
                writer.commit();
                try {
                    writer.addColumn("abc", ColumnType.STRING);
                    Assert.fail();
                } catch (CairoException ignore) {
                }

                // ignore error and add more rows
                ts = populateProducts(writer, rnd, ts, 10000, 60000);
                writer.commit();
                Assert.assertEquals(30000, writer.size());
            }

            try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
                populateProducts(writer, rnd, ts, 10000, 60000);
                writer.commit();
                Assert.assertEquals(40000, writer.size());
            }
        });
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
                    rnd.nextChars(blob, blobLen / 2);
                    r.putBin(9, blob, blobLen);
                }

                r.append();
            }
            writer.commit();

            Assert.assertFalse(writer.inTransaction());
            Assert.assertEquals(size + 10000, writer.size());
        } finally {
            Unsafe.free(blob, blobLen);
        }
        return ts;
    }

    void testCommitRetryAfterFailure(CountingFilesFacade ff) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long failureCount = 0;
            create(ff, PartitionBy.DAY);
            boolean valid = false;
            try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {

                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

                Rnd rnd = new Rnd();
                for (int i = 0; i < 10000; i++) {
                    // one record per hour
                    ts = populateRow(writer, ts, rnd, 10 * 60000);
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
        });
    }

    private void testConstructor(FilesFacade ff) throws Exception {
        testConstructor(ff, true);
    }

    private void testConstructor(FilesFacade ff, boolean create) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            if (create) {
                create(ff, PartitionBy.NONE);
            }
            try {
                new TableWriter(ff, root, PRODUCT);
                Assert.fail();
            } catch (CairoException e) {
                LOG.info().$((Sinkable) e).$();
            }
        });
    }

    private void testOutOfOrderRecords() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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
        });
    }

    private void testRemoveColumn(JournalStructure struct) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String name = CairoTestUtils.createTable(FF, root, struct.partitionBy(PartitionBy.DAY));
            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();
            try (TableWriter writer = new TableWriter(FF, root, name)) {

                // optional
                writer.warmUp();

                ts = append10KProducts(ts, rnd, writer);

                writer.removeColumn("supplier");

                final NativeLPSZ lpsz = new NativeLPSZ();
                try (Path path = new Path()) {
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
        });
    }

    private void testRemoveColumnRecoverableFailure(TestFilesFacade ff) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(FF, PartitionBy.DAY);
            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
            Rnd rnd = new Rnd();
            try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
                ts = append10KProducts(ts, rnd, writer);
                writer.commit();

                try {
                    writer.removeColumn("supplier");
                    Assert.fail();
                } catch (CairoException ignore) {
                }

                Assert.assertTrue(ff.wasCalled());

                ts = append10KProducts(ts, rnd, writer);
                writer.commit();
            }

            try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
                append10KProducts(ts, rnd, writer);
                writer.commit();
                Assert.assertEquals(30000, writer.size());
            }
        });
    }

    private void testRollback() throws NumericException {
        long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
        Rnd rnd = new Rnd();
        try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
            ts = populateProducts(writer, rnd, ts, 10000, 60000);
            writer.commit();

            long timestampAfterCommit = ts;

            populateProducts(writer, rnd, ts, 10000, 60000);

            Assert.assertEquals(20000, writer.size());
            writer.rollback();
            Assert.assertEquals(10000, writer.size());
            writer.rollback();
            Assert.assertEquals(10000, writer.size());

            ts = timestampAfterCommit;

            // make sure row rollback works after rollback
            writer.newRow(ts).cancel();

            // we should be able to repeat timestamps
            populateProducts(writer, rnd, ts, 10000, 60000);
            writer.commit();

            Assert.assertEquals(20000, writer.size());
        }
    }

    private void testSetAppendPositionFailure(String failFile) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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
            testAppendNulls(new Rnd(), FF, DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z"));
            try {
                new TableWriter(ff, root, "all");
                Assert.fail();
            } catch (CairoException ignore) {
            }
        });
    }

    private void testTruncate(CountingFilesFacade ff, boolean retry) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            create(ff, PartitionBy.DAY);
            Rnd rnd = new Rnd();
            try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {

                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

                for (int k = 0; k < 3; k++) {
                    ts = populateProducts(writer, rnd, ts, 200, 60 * 60000);
                    writer.commit();
                    Assert.assertEquals(200, writer.size());

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
                populateProducts(writer, rnd, ts, 1000, 60 * 60000);
                writer.commit();
                Assert.assertEquals(1000, writer.size());
            }

            // open writer one more time and just assert the size
            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
                Assert.assertEquals(1000, writer.size());
            }
        });
    }

    private void testTruncateOnClose(TestFilesFacade ff) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
                int N = 100000;
                Rnd rnd = new Rnd();
                populateProducts(writer, rnd, ts, N, 60 * 60000);
                writer.commit();
                Assert.assertEquals(N, writer.size());
            }
            Assert.assertTrue(ff.wasCalled());
        });
    }

    private void testTruncateRecoverableFailure(FilesFacade ff) throws NumericException {
        create(ff, PartitionBy.DAY);
        Rnd rnd = new Rnd();
        long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
        try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
            ts = populateProducts(writer, rnd, ts, 1000, 60 * 60000);
            writer.commit();

            try {
                writer.truncate();
                Assert.fail();
            } catch (CairoException ignore) {
            }
            Assert.assertEquals(1000, writer.size());
        }

        try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
            populateProducts(writer, rnd, ts, 1000, 60 * 60000);
            writer.commit();
            Assert.assertEquals(2000, writer.size());
        }
    }

    private void testUnrecoverableAddColumn(FilesFacade ff) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long ts = populateTable(FF);
            Rnd rnd = new Rnd();
            try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
                ts = populateProducts(writer, rnd, ts, 10000, 60000);
                writer.commit();

                Assert.assertEquals(12, writer.columns.size());

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
            create(FF, PartitionBy.DAY);
            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
            Rnd rnd = new Rnd();
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
        });
    }

    void verifyTimestampPartitions(VirtualMemory vmem, int n) {
        int i;
        DateFormatCompiler compiler = new DateFormatCompiler();
        DateFormat fmt = compiler.compile("yyyy-MM-dd");
        DateLocale enGb = DateLocaleFactory.INSTANCE.getDateLocale("en-gb");

        try (Path vp = new Path()) {
            for (i = 0; i < n; i++) {
                vp.of(root).concat(PRODUCT).put(Files.SEPARATOR);
                fmt.format(vmem.getLong(i * 8), enGb, "UTC", vp);
                if (!FF.exists(vp.$())) {
                    Assert.fail();
                }
            }
        }
    }

    private static class SwapMetaRenameDenyingFacade extends TestFilesFacade {
        boolean hit = false;

        @Override
        public boolean wasCalled() {
            return hit;
        }

        @Override
        public boolean rename(LPSZ from, LPSZ to) {
            if (Chars.endsWith(from, TableUtils.META_SWAP_FILE_NAME)) {
                hit = true;
                return false;
            }
            return super.rename(from, to);
        }
    }

    private static class MetaRenameDenyingFacade extends TestFilesFacade {
        boolean hit = false;

        @Override
        public boolean wasCalled() {
            return hit;
        }

        @Override
        public boolean rename(LPSZ from, LPSZ to) {
            if (Chars.contains(to, TableUtils.META_PREV_FILE_NAME)) {
                hit = true;
                return false;
            }
            return super.rename(from, to);
        }
    }

    private static class TodoOpenDenyingFacade extends TestFilesFacade {

        boolean hit = false;

        @Override
        public boolean wasCalled() {
            return hit;
        }

        @Override
        public long openAppend(LPSZ name) {
            if (Chars.endsWith(name, TableUtils.TODO_FILE_NAME)) {
                hit = true;
                return -1;
            }
            return super.openAppend(name);
        }
    }

    private static class TodoAppendDenyingFacade extends TestFilesFacade {
        long fd = -1;
        boolean hit = false;

        @Override
        public long append(long fd, long buf, int len) {
            if (fd == this.fd) {
                this.fd = -1;
                this.hit = true;
                return -1;
            }
            return super.append(fd, buf, len);
        }

        @Override
        public boolean wasCalled() {
            return hit;
        }


        @Override
        public long openAppend(LPSZ name) {
            if (Chars.endsWith(name, TableUtils.TODO_FILE_NAME)) {
                return fd = super.openAppend(name);
            }
            return super.openAppend(name);
        }


    }

    class CountingFilesFacade extends FilesFacadeImpl {
        long count = Long.MAX_VALUE;
    }
}