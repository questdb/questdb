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
import com.questdb.std.str.Path;
import com.questdb.std.time.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

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
                if (this.fd == fd) {
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
                    if (rnd.nextBoolean()) {
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
                    if (rnd.nextBoolean()) {
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
                    if (rnd.nextBoolean()) {
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
                    return -1;
                }
                return super.read(fd, buf, len, offset);
            }
        }, false);
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
        });
    }

    @Test
    public void testNonStandardPageSize2() throws Exception {
        populateTable(new FilesFacadeImpl() {
            @Override
            public long getPageSize() {
                return 32 * 1024 * 1024;
            }
        });
    }

    @Test
    public void testNulls() throws Exception {
        long mem = Unsafe.getMemUsed();
        createAllTable();
        Rnd rnd = new Rnd();
        testAppendNulls(rnd, FF);
        testAppendNulls(rnd, FF);
        Assert.assertEquals(mem, Unsafe.getMemUsed());
        Assert.assertEquals(0, FF.getOpenFileCount());
    }

    @Test
    public void testSetAppendPositionFailureBin() throws Exception {
        testSetAppendPositionFailure("bin.d");
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

    private void create(FilesFacade ff, int partitionBy) {
        try (TableUtils tabU = new TableUtils(ff)) {
            if (tabU.exists(root, PRODUCT) == 1) {
                tabU.create(root, getTestStructure().partitionBy(partitionBy).build(), 509);
            } else {
                throw CairoException.instance(0).put("Table ").put(PRODUCT).put(" already exists");
            }
        }
    }

    private void createAllTable() {
        JournalStructure struct = new JournalStructure("all").
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
                $date("date");

        try (TableUtils tu = new TableUtils(FF)) {
            tu.create(root, struct.build(), 509);
        }
    }

    private int getDirCount() {
        int dirCount = 0;
        try (CompositePath path = new CompositePath()) {
            path.of(root).concat(PRODUCT).$();
            long find = FF.findFirst(path);
            Assert.assertTrue(find > 0);

            try {
                do {
                    if (FF.findType(find) == Files.DT_DIR) {
                        dirCount++;
                    }
                } while (FF.findNext(find));
            } finally {
                FF.findClose(find);
            }
        }
        return dirCount;
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

    void populateTable(FilesFacade ff) throws NumericException {
        long used = Unsafe.getMemUsed();
        create(ff, PartitionBy.MONTH);
        populateTable0(ff);
        Assert.assertEquals(used, Unsafe.getMemUsed());
        Assert.assertEquals(0L, ff.getOpenFileCount());

    }

    private void populateTable0(FilesFacade ff) throws NumericException {
        try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {
            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();
            for (int i = 0; i < 100000; i++) {
                ts = populateRow(writer, ts, rnd, 60000);
            }
            writer.commit();
            Assert.assertEquals(100000, writer.size());
        }
    }

    private void testAppendNulls(Rnd rnd, FilesFacade ff) throws NumericException {
        final int blobLen = 64 * 1024;
        long blob = Unsafe.malloc(blobLen);
        try (TableWriter writer = new TableWriter(ff, root, "all")) {
            long size = writer.size();
            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
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
                    return -1;
                }
                return super.read(fd, buf, len, offset);
            }
        }
        final X ff = new X();
        long mem = Unsafe.getMemUsed();
        testAppendNulls(new Rnd(), FF);
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

    void verifyTimestampPartitions(VirtualMemory vmem, int n) {
        int i;
        DateFormatCompiler compiler = new DateFormatCompiler();
        DateFormat fmt = compiler.compile("yyyy-MM-dd", false);
        DateLocale enGb = DateLocaleFactory.INSTANCE.getDateLocale("en-gb");

        try (CompositePath vp = new CompositePath()) {
            for (i = 0; i < n; i++) {
                vp.of(root).concat(PRODUCT).put(Path.SEPARATOR);
                fmt.format(vmem.getLong(i * 8), enGb, "UTC", vp);
                if (!FF.exists(vp.$())) {
                    System.out.println(vp.toString());
                    Assert.fail();
                }
            }
        }
    }

    class CountingFilesFacade extends FilesFacadeImpl {
        long count = Long.MAX_VALUE;
    }
}