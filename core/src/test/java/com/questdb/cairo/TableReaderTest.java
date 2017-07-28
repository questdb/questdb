package com.questdb.cairo;

import com.questdb.PartitionBy;
import com.questdb.ex.NumericException;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.FilesFacade;
import com.questdb.misc.FilesFacadeImpl;
import com.questdb.misc.Rnd;
import com.questdb.misc.Unsafe;
import com.questdb.ql.parser.AbstractOptimiserTest;
import com.questdb.std.time.DateFormatUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TableReaderTest extends AbstractOptimiserTest {
    private static final FilesFacade FF = FilesFacadeImpl.INSTANCE;
    private static final Log LOG = LogFactory.getLog(TableReaderTest.class);
    private static CharSequence root;

    @BeforeClass
    public static void setUp() throws Exception {
        root = FACTORY_CONTAINER.getConfiguration().getJournalBase().getAbsolutePath();
    }

    @Test
    public void testBasicRead() throws Exception {
        long mem = Unsafe.getMemUsed();
        createAllTable();
        Rnd rnd = new Rnd();
        long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
        ts = testAppendNulls(rnd, FF, ts);
//        testAppendNulls(rnd, FF, ts);

        try (TableReader reader = new TableReader(FF, root, "all")) {
            Assert.assertEquals(10000, reader.size());
            reader.readAll();
        }

        Assert.assertEquals(mem, Unsafe.getMemUsed());
        Assert.assertEquals(0, FF.getOpenFileCount());

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
                $date("date").partitionBy(PartitionBy.NONE));
    }

    private void createTable(FilesFacade ff, JournalStructure struct) {
        String name = struct.getName();
        try (TableUtils tabU = new TableUtils(ff)) {
            if (tabU.exists(root, name) == 1) {
                tabU.create(root, struct.build(), 509);
            } else {
                throw CairoException.instance(0).put("Table ").put(name).put(" already exists");
            }
        }
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
}