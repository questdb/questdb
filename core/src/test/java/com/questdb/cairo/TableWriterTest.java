package com.questdb.cairo;

import com.questdb.PartitionBy;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.misc.*;
import com.questdb.ql.parser.AbstractOptimiserTest;
import com.questdb.std.str.CompositePath;
import com.questdb.std.str.Path;
import com.questdb.std.time.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TableWriterTest extends AbstractOptimiserTest {

    public static final String PRODUCT = "product";
    private static final FilesFacade FF = FilesFacadeImpl.INSTANCE;
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
        try (TableUtils tabU = new TableUtils(FF)) {
            tabU.create(root, getTestStructure().partitionBy(PartitionBy.NONE).build(), 509);
            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {

                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

                Rnd rnd = new Rnd();
                for (int i = 0; i < 100000; i++) {
                    ts = populateRow(writer, ts, rnd);
                    writer.commit();
                }
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testAutoCancelFirstRowNonPartitioned() throws Exception {
        long used = Unsafe.getMemUsed();
        try (TableUtils tabU = new TableUtils(FF)) {
            tabU.create(root, getTestStructure().partitionBy(PartitionBy.NONE).build(), 509);
            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {

                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");


                TableWriter.Row r = writer.newRow(ts);
                r.putInt(0, 1234);

                Rnd rnd = new Rnd();
                for (int i = 0; i < 10000; i++) {
                    ts = populateRow(writer, ts, rnd);
                }
                Assert.assertEquals(10000, writer.size());
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testCancelFirstRowNonPartitioned() throws Exception {
        long used = Unsafe.getMemUsed();
        try (TableUtils tabU = new TableUtils(FF)) {
            tabU.create(root, getTestStructure().partitionBy(PartitionBy.NONE).build(), 509);
            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {

                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");


                TableWriter.Row r = writer.newRow(ts);
                r.putInt(0, 1234);
                r.cancel();

                Rnd rnd = new Rnd();
                for (int i = 0; i < 10000; i++) {
                    ts = populateRow(writer, ts, rnd);
                }
                Assert.assertEquals(10000, writer.size());
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testCancelFirstRowPartitioned() throws Exception {
        long used = Unsafe.getMemUsed();
        try (TableUtils tabU = new TableUtils(FF)) {
            tabU.create(root, getTestStructure().partitionBy(PartitionBy.DAY).build(), 509);
            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
                TableWriter.Row r = writer.newRow(ts);
                r.cancel();
                writer.commit();
                Assert.assertEquals(0, writer.size());
                Assert.assertTrue(getDirCount() == 2);
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testCancelMidPartition() throws Exception {
        long used = Unsafe.getMemUsed();
        final Rnd rnd = new Rnd();
        try (TableUtils tabU = new TableUtils(FF)) {
            tabU.create(root, getTestStructure().partitionBy(PartitionBy.DAY).build(), 509);

            // this contraption will verify that all timestamps that are
            // supposed to be stored have matching partitions
            try (CompositePath vp = new CompositePath()) {
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

                        DateFormatCompiler compiler = new DateFormatCompiler();
                        DateFormat fmt = compiler.compile("yyyy-MM-dd", false);
                        DateLocale enGb = DateLocaleFactory.INSTANCE.getDateLocale("en-gb");

                        for (i = 0; i < N; i++) {
                            vp.of(root).concat(PRODUCT).put(Path.SEPARATOR);
                            fmt.format(vmem.getLong(i * 8), enGb, "UTC", vp);
                            if (!FF.exists(vp.$())) {
                                System.out.println(vp.toString());
                                Assert.fail();
                            }
                        }
                    }
                }
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testCancelMidRowNonPartitioned() throws Exception {
        long used = Unsafe.getMemUsed();
        try (TableUtils tabU = new TableUtils(FF)) {
            tabU.create(root, getTestStructure().partitionBy(PartitionBy.NONE).build(), 509);
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
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testDayPartition() throws Exception {
        long used = Unsafe.getMemUsed();

        try (TableUtils tabU = new TableUtils(FF)) {
            tabU.create(root, getTestStructure().partitionBy(PartitionBy.DAY).build(), 509);
            int N = 100000;

            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {

                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

                Rnd rnd = new Rnd();
                for (int i = 0; i < N; i++) {
                    ts = populateRow(writer, ts, rnd);
                }
                writer.commit();
                Assert.assertEquals(N, writer.size());
            }

            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
                Assert.assertEquals((long) N, writer.size());
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testDayPartitionTruncate() throws Exception {
        long used = Unsafe.getMemUsed();
        try (TableUtils tabU = new TableUtils(FF)) {
            tabU.create(root, getTestStructure().partitionBy(PartitionBy.DAY).build(), 509);
            Rnd rnd = new Rnd();
            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {

                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

                for (int k = 0; k < 3; k++) {
                    for (int i = 0; i < 100000; i++) {
                        ts = populateRow(writer, ts, rnd);
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
                    ts = populateRow(writer, ts, rnd);
                }
                writer.commit();
                Assert.assertEquals(100000, writer.size());
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testDefaultPartition() throws Exception {
        long used = Unsafe.getMemUsed();

        try (TableUtils tabU = new TableUtils(FF)) {
            tabU.create(root, getTestStructure().build(), 509);

            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {

                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

                Rnd rnd = new Rnd();
                for (int i = 0; i < 100000; i++) {
                    TableWriter.Row r = writer.newRow(ts += 60000);
                    r.putInt(0, rnd.nextPositiveInt());
                    r.putStr(1, rnd.nextString(7));
                    r.putStr(2, rnd.nextString(4));
                    r.putStr(3, rnd.nextString(11));
                    r.putDouble(4, rnd.nextDouble());
                    r.append();
                }
                writer.commit();
                Assert.assertEquals(100000, writer.size());
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testNonStandardPageSize() throws Exception {
        long used = Unsafe.getMemUsed();

        class Facade extends FilesFacadeImpl {
            @Override
            public long getPageSize() {
                return super.getPageSize() * super.getPageSize();
            }
        }

        Facade ff = new Facade();

        try (TableUtils tabU = new TableUtils(ff)) {
            tabU.create(root, getTestStructure().partitionBy(PartitionBy.MONTH).build(), 509);

            try (TableWriter writer = new TableWriter(ff, root, PRODUCT)) {

                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

                Rnd rnd = new Rnd();
                for (int i = 0; i < 100000; i++) {
                    TableWriter.Row r = writer.newRow(ts += 60000);
                    r.putInt(0, rnd.nextPositiveInt());
                    r.putStr(1, rnd.nextString(7));
                    r.putStr(2, rnd.nextString(4));
                    r.putStr(3, rnd.nextString(11));
                    r.putDouble(4, rnd.nextDouble());
                    r.append();
                }
                writer.commit();
                Assert.assertEquals(100000, writer.size());
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testSinglePartitionTruncate() throws Exception {
        long used = Unsafe.getMemUsed();
        try (TableUtils tabU = new TableUtils(FF)) {

            tabU.create(root, getTestStructure().partitionBy(PartitionBy.YEAR).build(), 509);

            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
                writer.truncate();
                Assert.assertEquals(0, writer.size());
            }

            try (TableWriter writer = new TableWriter(FF, root, PRODUCT)) {
                Assert.assertEquals(0, writer.size());
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
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

    private long populateRow(TableWriter writer, long ts, Rnd rnd) {
        TableWriter.Row r = writer.newRow(ts += 60 * 60000);
        r.putInt(0, rnd.nextPositiveInt());
        r.putStr(1, rnd.nextString(7));
        r.putStr(2, rnd.nextString(4));
        r.putStr(3, rnd.nextString(11));
        r.putDouble(4, rnd.nextDouble());
        r.append();
        return ts;
    }
}