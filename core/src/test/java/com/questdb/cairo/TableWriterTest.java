package com.questdb.cairo;

import com.questdb.PartitionBy;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.misc.Files;
import com.questdb.misc.Rnd;
import com.questdb.misc.Unsafe;
import com.questdb.ql.parser.AbstractOptimiserTest;
import com.questdb.std.str.CompositePath;
import com.questdb.std.time.DateFormatUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TableWriterTest extends AbstractOptimiserTest {

    public static final String PRODUCT = "product";
    private static CharSequence root;

    @BeforeClass
    public static void setUp() throws Exception {
        root = FACTORY_CONTAINER.getConfiguration().getJournalBase().getAbsolutePath();
    }

    @AfterClass
    public static void tearDown0() throws Exception {
        try (CompositePath path = new CompositePath().of(root)) {
            Files.rmdir(path);
        }
    }

    @Test
    public void tesFrequentCommit() throws Exception {
        long used = Unsafe.getMemUsed();
        create(getTestStructure().partitionBy(PartitionBy.NONE));
        try (TableWriter writer = new TableWriter(root, PRODUCT)) {

            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();
            for (int i = 0; i < 100000; i++) {
                ts = populateRow(writer, ts, rnd);
                writer.commit();
            }
        }

        TableUtils.freeThreadLocals();
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testDayPartition() throws Exception {
        long used = Unsafe.getMemUsed();

        create(getTestStructure().partitionBy(PartitionBy.DAY));
        int N = 100000;

        try (TableWriter writer = new TableWriter(root, PRODUCT)) {

            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();
            for (int i = 0; i < N; i++) {
                ts = populateRow(writer, ts, rnd);
            }
            writer.commit();
            Assert.assertEquals(N, writer.size());
        }

        try (TableWriter writer = new TableWriter(root, PRODUCT)) {
            Assert.assertEquals((long) N, writer.size());
        }

        TableUtils.freeThreadLocals();
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testDayPartitionTruncate() throws Exception {
        long used = Unsafe.getMemUsed();

        create(getTestStructure().partitionBy(PartitionBy.DAY));

        Rnd rnd = new Rnd();
        try (TableWriter writer = new TableWriter(root, PRODUCT)) {

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

        try (TableWriter writer = new TableWriter(root, PRODUCT)) {
            long ts = DateFormatUtils.parseDateTime("2014-03-04T00:00:00.000Z");
            Assert.assertEquals(0, writer.size());
            for (int i = 0; i < 100000; i++) {
                ts = populateRow(writer, ts, rnd);
            }
            writer.commit();
            Assert.assertEquals(100000, writer.size());
        }

        TableUtils.freeThreadLocals();
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testDefaultPartition() throws Exception {
        long used = Unsafe.getMemUsed();


        create(getTestStructure());

        try (TableWriter writer = new TableWriter(root, PRODUCT)) {

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
        }

        TableUtils.freeThreadLocals();
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testSinglePartitionTruncate() throws Exception {
        long used = Unsafe.getMemUsed();

        create(getTestStructure().partitionBy(PartitionBy.YEAR));

        try (TableWriter writer = new TableWriter(root, PRODUCT)) {
            writer.truncate();
            Assert.assertEquals(0, writer.size());
        }

        try (TableWriter writer = new TableWriter(root, PRODUCT)) {
            Assert.assertEquals(0, writer.size());
        }

        TableUtils.freeThreadLocals();
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

    private static void create(JournalStructure struct) {
        TableUtils.create(root, struct.build(), 509);
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