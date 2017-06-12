package com.questdb.cairo;

import com.questdb.PartitionBy;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.misc.Files;
import com.questdb.misc.Rnd;
import com.questdb.misc.Unsafe;
import com.questdb.ql.parser.AbstractOptimiserTest;
import com.questdb.std.time.DateFormatUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class TableWriterTest extends AbstractOptimiserTest {

    @Test
    public void tesFrequentCommit() throws Exception {
        long used = Unsafe.getMemUsed();

        JournalMetadata m = new JournalStructure("products").
                $int("productId").
                $str("productName").
                $sym("supplier").index().buckets(100).
                $sym("category").index().buckets(100).
                $double("price").
                $ts().
                partitionBy(PartitionBy.NONE).
                build();

        final CharSequence root = FACTORY_CONTAINER.getConfiguration().getJournalBase().getAbsolutePath();
        TableUtils.create(root, m, 509);

        try {

            try (TableWriter writer = new TableWriter(root, m.getName())) {

                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

                Rnd rnd = new Rnd();
                for (int i = 0; i < 100000; i++) {
                    ts = populateRow(writer, ts, rnd);
                    writer.commit();
                }
            }

            TableUtils.freeThreadLocals();
            Assert.assertEquals(used, Unsafe.getMemUsed());
        } finally {
            Files.deleteOrException(new File(root.toString(), m.getName()));
        }
    }

    @Test
    public void testDayPartition() throws Exception {
        long used = Unsafe.getMemUsed();

        JournalMetadata m = new JournalStructure("products").
                $int("productId").
                $str("productName").
                $sym("supplier").index().buckets(100).
                $sym("category").index().buckets(100).
                $double("price").
                $ts().
                partitionBy(PartitionBy.DAY).
                build();

        CharSequence root = FACTORY_CONTAINER.getConfiguration().getJournalBase().getAbsolutePath();
        TableUtils.create(root, m, 509);

        try {

            try (TableWriter writer = new TableWriter(root, m.getName())) {

                long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

                Rnd rnd = new Rnd();
                for (int i = 0; i < 100000; i++) {
                    ts = populateRow(writer, ts, rnd);
                }
                writer.commit();
                Assert.assertEquals(100000, writer.size());
            }

            try (TableWriter writer = new TableWriter(root, m.getName())) {
                Assert.assertEquals(100000, writer.size());
            }

            TableUtils.freeThreadLocals();
            Assert.assertEquals(used, Unsafe.getMemUsed());
        } finally {
            Files.deleteOrException(new File(root.toString(), m.getName()));
        }
    }

    @Test
    public void testDayPartitionTruncate() throws Exception {
        long used = Unsafe.getMemUsed();

        JournalMetadata m = new JournalStructure("products").
                $int("productId").
                $str("productName").
                $sym("supplier").index().buckets(100).
                $sym("category").index().buckets(100).
                $double("price").
                $ts().
                partitionBy(PartitionBy.DAY).
                build();

        CharSequence root = FACTORY_CONTAINER.getConfiguration().getJournalBase().getAbsolutePath();
        TableUtils.create(root, m, 509);

        try {

            Rnd rnd = new Rnd();
            try (TableWriter writer = new TableWriter(root, m.getName())) {

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

            try (TableWriter writer = new TableWriter(root, m.getName())) {
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
        } finally {
            Files.deleteOrException(new File(root.toString(), m.getName()));
        }
    }

    @Test
    public void testDefaultPartition() throws Exception {
        long used = Unsafe.getMemUsed();

        JournalMetadata m = new JournalStructure("products").
                $int("productId").
                $str("productName").
                $sym("supplier").index().buckets(100).
                $sym("category").index().buckets(100).
                $double("price").$ts().build();

        CharSequence root = FACTORY_CONTAINER.getConfiguration().getJournalBase().getAbsolutePath();

        TableUtils.create(root, m, 509);

        try {

            try (TableWriter writer = new TableWriter(root, m.getName())) {

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
        } finally {
            Files.deleteOrException(new File(root.toString(), m.getName()));
        }

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