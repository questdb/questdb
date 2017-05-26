package com.questdb.cairo;

import com.questdb.PartitionBy;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.misc.Rnd;
import com.questdb.misc.Unsafe;
import com.questdb.ql.parser.AbstractOptimiserTest;
import com.questdb.std.time.DateFormatUtils;
import org.junit.Assert;
import org.junit.Test;

public class TableWriterTest extends AbstractOptimiserTest {

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

        try (TableWriter writer = new TableWriter(FACTORY_CONTAINER.getConfiguration().getJournalBase().getAbsolutePath() + "/test", m)) {

            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

            Rnd rnd = new Rnd();
            for (int i = 0; i < 100000; i++) {
                TableWriter.Row r = writer.newRow(ts += 60 * 60000);
                r.putInt(0, rnd.nextPositiveInt());
                r.putStr(1, rnd.nextString(7));
                r.putStr(2, rnd.nextString(4));
                r.putStr(3, rnd.nextString(11));
                r.putDouble(4, rnd.nextDouble());
                r.append();
            }
            writer.commit();
        }

        Assert.assertEquals(used, Unsafe.getMemUsed());
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

        try (TableWriter writer = new TableWriter(FACTORY_CONTAINER.getConfiguration().getJournalBase().getAbsolutePath() + "/test", m)) {

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

        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

}