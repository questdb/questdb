package com.questdb.cairo;

import com.questdb.PartitionBy;
import com.questdb.ex.NumericException;
import com.questdb.misc.*;
import com.questdb.ql.Record;
import com.questdb.std.BinarySequence;
import com.questdb.std.LongList;
import com.questdb.std.str.StringSink;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.store.ColumnType;
import com.questdb.test.tools.TestUtils;
import com.questdb.txt.RecordSourcePrinter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TableReaderTest extends AbstractCairoTest {
    public static final int MUST_SWITCH = 1;
    public static final int MUST_NOT_SWITCH = 2;
    public static final int DONT_CARE = 0;
    private static final FilesFacade FF = FilesFacadeImpl.INSTANCE;
    private static final int blobLen = 64 * 1024;
    private static final RecordAssert BATCH1_ASSERTER = (r, exp, ts, blob) -> {
        if (exp.nextBoolean()) {
            Assert.assertEquals(exp.nextByte(), r.get(2));
        } else {
            Assert.assertEquals(0, r.get(2));
        }

        if (exp.nextBoolean()) {
            Assert.assertEquals(exp.nextBoolean(), r.getBool(8));
        } else {
            Assert.assertFalse(r.getBool(8));
        }

        if (exp.nextBoolean()) {
            Assert.assertEquals(exp.nextShort(), r.getShort(1));
        } else {
            Assert.assertEquals(0, r.getShort(1));
        }

        if (exp.nextBoolean()) {
            Assert.assertEquals(exp.nextInt(), r.getInt(0));
        } else {
            Assert.assertEquals(Numbers.INT_NaN, r.getInt(0));
        }

        if (exp.nextBoolean()) {
            Assert.assertEquals(exp.nextDouble(), r.getDouble(3), 0.00000001);
        } else {
            Assert.assertTrue(Double.isNaN(r.getDouble(3)));
        }

        if (exp.nextBoolean()) {
            Assert.assertEquals(exp.nextFloat(), r.getFloat(4), 0.000001f);
        } else {
            Assert.assertTrue(Float.isNaN(r.getFloat(4)));
        }

        if (exp.nextBoolean()) {
            Assert.assertEquals(exp.nextLong(), r.getLong(5));
        } else {
            Assert.assertEquals(Numbers.LONG_NaN, r.getLong(5));
        }

        if (exp.nextBoolean()) {
            Assert.assertEquals(ts, r.getDate(10));
        } else {
            Assert.assertEquals(Numbers.LONG_NaN, r.getDate(10));
        }

        assertBin(r, exp, blob, 9);

        if (exp.nextBoolean()) {
            assertStrColumn(exp.nextChars(10), r, 6);
        } else {
            assertNullStr(r, 6);
        }
    };
    private static final RecordAssert BATCH2_BEFORE_ASSERTER = (r, rnd, ts, blob) -> assertNullStr(r, 11);
    private static final RecordAssert BATCH1_7_ASSERTER = (r, exp, ts, blob) -> {
        if (exp.nextBoolean()) {
            Assert.assertEquals(exp.nextByte(), r.get(1));
        } else {
            Assert.assertEquals(0, r.get(1));
        }

        if (exp.nextBoolean()) {
            Assert.assertEquals(exp.nextBoolean(), r.getBool(7));
        } else {
            Assert.assertFalse(r.getBool(7));
        }

        if (exp.nextBoolean()) {
            Assert.assertEquals(exp.nextShort(), r.getShort(0));
        } else {
            Assert.assertEquals(0, r.getShort(0));
        }

        if (exp.nextBoolean()) {
            exp.nextInt();
        }

        if (exp.nextBoolean()) {
            Assert.assertEquals(exp.nextDouble(), r.getDouble(2), 0.00000001);
        } else {
            Assert.assertTrue(Double.isNaN(r.getDouble(2)));
        }

        if (exp.nextBoolean()) {
            Assert.assertEquals(exp.nextFloat(), r.getFloat(3), 0.000001f);
        } else {
            Assert.assertTrue(Float.isNaN(r.getFloat(3)));
        }

        if (exp.nextBoolean()) {
            Assert.assertEquals(exp.nextLong(), r.getLong(4));
        } else {
            Assert.assertEquals(Numbers.LONG_NaN, r.getLong(4));
        }

        if (exp.nextBoolean()) {
            Assert.assertEquals(ts, r.getDate(9));
        } else {
            Assert.assertEquals(Numbers.LONG_NaN, r.getDate(9));
        }

        assertBin(r, exp, blob, 8);

        if (exp.nextBoolean()) {
            assertStrColumn(exp.nextChars(10), r, 5);
        } else {
            assertNullStr(r, 5);
        }
        Assert.assertEquals(0, r.getInt(20));
    };

    private static final RecordAssert BATCH_2_7_BEFORE_ASSERTER = (r, rnd, ts, blob) -> assertNullStr(r, 10);
    private static final RecordAssert BATCH_3_7_BEFORE_ASSERTER = (r, rnd, ts, blob) -> Assert.assertEquals(0, r.getInt(11));
    private static final RecordAssert BATCH_4_7_BEFORE_ASSERTER = (r, rnd, ts, blob) -> {
        Assert.assertEquals(0, r.getShort(12));
        Assert.assertFalse(r.getBool(13));
        Assert.assertEquals(0, r.get(14));
        Assert.assertTrue(Float.isNaN(r.getFloat(15)));
        Assert.assertTrue(Double.isNaN(r.getDouble(16)));
        assertNullStr(r, 17);
        Assert.assertEquals(Numbers.LONG_NaN, r.getLong(18));
        Assert.assertEquals(Numbers.LONG_NaN, r.getDate(19));
    };

    private static final RecordAssert BATCH2_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH1_ASSERTER.assertRecord(r, rnd, ts, blob);
        if ((rnd.nextPositiveInt() & 3) == 0) {
            assertStrColumn(rnd.nextChars(15), r, 11);
        }
    };

    private static final RecordAssert BATCH2_7_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH1_7_ASSERTER.assertRecord(r, rnd, ts, blob);
        if ((rnd.nextPositiveInt() & 3) == 0) {
            assertStrColumn(rnd.nextChars(15), r, 10);
        }
    };

    private static final RecordAssert BATCH3_BEFORE_ASSERTER = (r, rnd, ts, blob) -> Assert.assertEquals(0, r.getInt(12));

    private static final RecordAssert BATCH3_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH2_ASSERTER.assertRecord(r, rnd, ts, blob);

        if ((rnd.nextPositiveInt() & 3) == 0) {
            Assert.assertEquals(rnd.nextInt(), r.getInt(12));
        }
    };

    private static final RecordAssert BATCH3_7_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH2_7_ASSERTER.assertRecord(r, rnd, ts, blob);

        if ((rnd.nextPositiveInt() & 3) == 0) {
            Assert.assertEquals(rnd.nextInt(), r.getInt(11));
        }
    };

    private static final RecordAssert BATCH4_BEFORE_ASSERTER = (r, rnd, ts, blob) -> {
        Assert.assertEquals(0, r.getShort(13));
        Assert.assertFalse(r.getBool(14));
        Assert.assertEquals(0, r.get(15));
        Assert.assertTrue(Float.isNaN(r.getFloat(16)));
        Assert.assertTrue(Double.isNaN(r.getDouble(17)));
        assertNullStr(r, 18);
        Assert.assertEquals(Numbers.LONG_NaN, r.getLong(19));
        Assert.assertEquals(Numbers.LONG_NaN, r.getDate(20));
        Assert.assertNull(r.getBin2(21));
        Assert.assertEquals(-1L, r.getBinLen(21));
    };

    private static final RecordAssert BATCH5_BEFORE_ASSERTER = (r, rnd, ts, blob) -> {
        Assert.assertEquals(0, r.getShort(13));
        Assert.assertFalse(r.getBool(14));
        Assert.assertEquals(0, r.get(15));
        Assert.assertTrue(Float.isNaN(r.getFloat(16)));
        Assert.assertTrue(Double.isNaN(r.getDouble(17)));
        assertNullStr(r, 18);
        Assert.assertEquals(Numbers.LONG_NaN, r.getLong(19));
        Assert.assertEquals(Numbers.LONG_NaN, r.getDate(20));
    };

    private static final RecordAssert BATCH4_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH3_ASSERTER.assertRecord(r, rnd, ts, blob);

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextShort(), r.getShort(13));
        } else {
            Assert.assertEquals(0, r.getShort(13));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextBoolean(), r.getBool(14));
        } else {
            Assert.assertFalse(r.getBool(14));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextByte(), r.get(15));
        } else {
            Assert.assertEquals(0, r.get(15));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextFloat(), r.getFloat(16), 0.00000001f);
        } else {
            Assert.assertTrue(Float.isNaN(r.getFloat(16)));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextDouble(), r.getDouble(17), 0.0000001d);
        } else {
            Assert.assertTrue(Double.isNaN(r.getDouble(17)));
        }

        if (rnd.nextBoolean()) {
            assertStrColumn(rnd.nextChars(10), r, 18);
        } else {
            assertNullStr(r, 18);
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getLong(19));
        } else {
            Assert.assertEquals(Numbers.LONG_NaN, r.getLong(19));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getDate(20));
        } else {
            Assert.assertEquals(Numbers.LONG_NaN, r.getDate(20));
        }

        assertBin(r, rnd, blob, 21);
    };

    private static final RecordAssert BATCH6_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH3_ASSERTER.assertRecord(r, rnd, ts, blob);

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextShort(), r.getShort(13));
        } else {
            Assert.assertEquals(0, r.getShort(13));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextBoolean(), r.getBool(14));
        } else {
            Assert.assertFalse(r.getBool(14));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextByte(), r.get(15));
        } else {
            Assert.assertEquals(0, r.get(15));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextFloat(), r.getFloat(16), 0.00000001f);
        } else {
            Assert.assertTrue(Float.isNaN(r.getFloat(16)));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextDouble(), r.getDouble(17), 0.0000001d);
        } else {
            Assert.assertTrue(Double.isNaN(r.getDouble(17)));
        }

        if (rnd.nextBoolean()) {
            assertStrColumn(rnd.nextChars(10), r, 18);
        } else {
            assertNullStr(r, 18);
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getLong(19));
        } else {
            Assert.assertEquals(Numbers.LONG_NaN, r.getLong(19));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getDate(20));
        } else {
            Assert.assertEquals(Numbers.LONG_NaN, r.getDate(20));
        }
    };

    private static final RecordAssert BATCH6_7_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH3_7_ASSERTER.assertRecord(r, rnd, ts, blob);

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextShort(), r.getShort(12));
        } else {
            Assert.assertEquals(0, r.getShort(12));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextBoolean(), r.getBool(13));
        } else {
            Assert.assertFalse(r.getBool(13));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextByte(), r.get(14));
        } else {
            Assert.assertEquals(0, r.get(14));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextFloat(), r.getFloat(15), 0.00000001f);
        } else {
            Assert.assertTrue(Float.isNaN(r.getFloat(15)));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextDouble(), r.getDouble(16), 0.0000001d);
        } else {
            Assert.assertTrue(Double.isNaN(r.getDouble(16)));
        }

        if (rnd.nextBoolean()) {
            assertStrColumn(rnd.nextChars(10), r, 17);
        } else {
            assertNullStr(r, 17);
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getLong(18));
        } else {
            Assert.assertEquals(Numbers.LONG_NaN, r.getLong(18));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getDate(19));
        } else {
            Assert.assertEquals(Numbers.LONG_NaN, r.getDate(19));
        }
    };

    private static final RecordAssert BATCH5_7_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH6_7_ASSERTER.assertRecord(r, rnd, ts, blob);

        // generate blob to roll forward random generator, don't assert blob value
        if (rnd.nextBoolean()) {
            rnd.nextChars(blob, blobLen / 2);
        }
    };

    private static final RecordAssert BATCH5_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH6_ASSERTER.assertRecord(r, rnd, ts, blob);

        // generate blob to roll forward random generator, don't assert blob value
        if (rnd.nextBoolean()) {
            rnd.nextChars(blob, blobLen / 2);
        }
    };


    private static final FieldGenerator BATCH1_GENERATOR = (r, rnd, ts, blob) -> {
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

        if (rnd.nextBoolean()) {
            r.putStr(6, rnd.nextChars(10));
        }
    };

    private static final FieldGenerator BATCH2_GENERATOR = (r, rnd, ts, blob) -> {
        BATCH1_GENERATOR.generate(r, rnd, ts, blob);

        if ((rnd.nextPositiveInt() & 3) == 0) {
            r.putStr(11, rnd.nextChars(15));
        }
    };

    private static final FieldGenerator BATCH3_GENERATOR = (r, rnd, ts, blob) -> {
        BATCH2_GENERATOR.generate(r, rnd, ts, blob);

        if ((rnd.nextPositiveInt() & 3) == 0) {
            r.putInt(12, rnd.nextInt());
        }
    };

    private static final FieldGenerator BATCH4_GENERATOR = (r, rnd, ts, blob) -> {
        BATCH3_GENERATOR.generate(r, rnd, ts, blob);

        if (rnd.nextBoolean()) {
            r.putShort(13, rnd.nextShort());
        }

        if (rnd.nextBoolean()) {
            r.putBool(14, rnd.nextBoolean());
        }

        if (rnd.nextBoolean()) {
            r.putByte(15, rnd.nextByte());
        }

        if (rnd.nextBoolean()) {
            r.putFloat(16, rnd.nextFloat());
        }

        if (rnd.nextBoolean()) {
            r.putDouble(17, rnd.nextDouble());
        }

        if (rnd.nextBoolean()) {
            r.putStr(18, rnd.nextChars(10));
        }

        if (rnd.nextBoolean()) {
            r.putLong(19, rnd.nextLong());
        }

        if (rnd.nextBoolean()) {
            r.putDate(20, rnd.nextLong());
        }

        if (rnd.nextBoolean()) {
            rnd.nextChars(blob, blobLen / 2);
            r.putBin(21, blob, blobLen);
        }
    };

    private static final FieldGenerator BATCH6_GENERATOR = (r, rnd, ts, blob) -> {
        BATCH3_GENERATOR.generate(r, rnd, ts, blob);

        if (rnd.nextBoolean()) {
            r.putShort(13, rnd.nextShort());
        }

        if (rnd.nextBoolean()) {
            r.putBool(14, rnd.nextBoolean());
        }

        if (rnd.nextBoolean()) {
            r.putByte(15, rnd.nextByte());
        }

        if (rnd.nextBoolean()) {
            r.putFloat(16, rnd.nextFloat());
        }

        if (rnd.nextBoolean()) {
            r.putDouble(17, rnd.nextDouble());
        }

        if (rnd.nextBoolean()) {
            r.putStr(18, rnd.nextChars(10));
        }

        if (rnd.nextBoolean()) {
            r.putLong(19, rnd.nextLong());
        }

        if (rnd.nextBoolean()) {
            r.putDate(20, rnd.nextLong());
        }
    };

    @Test
    public void testReadByDay() throws Exception {
        CairoTestUtils.createAllTable(root, PartitionBy.DAY);
        TestUtils.assertMemoryLeak(this::testTableCursor);
    }

    @Test
    public void testReadByMonth() throws Exception {
        CairoTestUtils.createAllTable(root, PartitionBy.MONTH);
        final String expected = "int\tshort\tbyte\tdouble\tfloat\tlong\tstr\tsym\tbool\tbin\tdate\n" +
                "73575701\t0\t0\tNaN\t0.7097\t-1675638984090602536\t\t\tfalse\t\t\n" +
                "NaN\t0\t89\tNaN\tNaN\t6236292340460979716\tPMIUPLYJVB\t\ttrue\t\t\n" +
                "NaN\t0\t60\tNaN\t0.6454\t-2715397729034539921\tOEYVSCKKDJ\t\tfalse\t\t2013-03-11T12:00:00.000Z\n" +
                "NaN\t0\t113\tNaN\tNaN\t-6905112719978615298\t\t\tfalse\t\t2013-03-14T00:00:00.000Z\n" +
                "-801004676\t0\t0\t0.000242509581\tNaN\t-7064303592482002884\t\t\tfalse\t\t2013-03-16T12:00:00.000Z\n" +
                "NaN\t-24062\t0\t272.000000000000\t0.4387\tNaN\tTGSOOWYGSD\t\ttrue\t\t\n" +
                "-640548855\t3093\t0\t-960.000000000000\t0.6056\t8522034740532776473\t\t\tfalse\t\t\n" +
                "61976253\t0\t0\t0.000000000000\t0.5092\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\t0.000041838453\t0.3185\tNaN\tZMGKVSIWRP\t\tfalse\t\t\n" +
                "NaN\t0\t22\tNaN\tNaN\tNaN\t\t\tfalse\t\t2013-03-29T00:00:00.000Z\n" +
                "777797854\t0\t0\tNaN\tNaN\t-3561042134139364009\t\t\tfalse\t\t2013-03-31T12:00:00.000Z\n" +
                "NaN\t0\t106\tNaN\t0.4809\tNaN\t\t\ttrue\t\t\n" +
                "1204862303\t-20282\t0\tNaN\tNaN\t8715974436393319034\t\t\ttrue\t\t2013-04-05T12:00:00.000Z\n" +
                "NaN\t0\t53\t-222.738281250000\tNaN\tNaN\tWOWDODUFGU\t\tfalse\t\t\n" +
                "2095297876\t21923\t0\t256.000000000000\t0.8653\t-6418805892627297273\t\t\ttrue\t\t2013-04-10T12:00:00.000Z\n" +
                "NaN\t-19019\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t2013-04-13T00:00:00.000Z\n" +
                "NaN\t-25663\t0\t-92.000000000000\t0.3888\tNaN\t\t\ttrue\t\t2013-04-15T12:00:00.000Z\n" +
                "-1601554634\t3379\t0\t1020.793212890625\tNaN\tNaN\tKSJZPZZKUM\t\tfalse\t\t\n" +
                "NaN\t26260\t0\t46.750000000000\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "-1050143454\t0\t0\tNaN\tNaN\t-4944873630491810081\t\t\tfalse\t\t\n" +
                "NaN\t0\t34\t0.013004892273\tNaN\tNaN\t\t\tfalse\t\t2013-04-25T12:00:00.000Z\n" +
                "-1242020108\t-11546\t-82\t0.000000004717\t0.4724\tNaN\t\t\tfalse\t\t\n" +
                "1512203086\t0\t0\t797.846359252930\tNaN\t6753493100272204912\t\t\tfalse\t\t\n" +
                "77063638\t870\t-100\tNaN\tNaN\tNaN\t\t\ttrue\t\t2013-05-03T00:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t27339\t0\t0.000000010539\tNaN\tNaN\t\t\tfalse\t\t2013-05-08T00:00:00.000Z\n" +
                "235357628\t0\t-120\t0.000156953447\tNaN\tNaN\tTTNGDKZVVS\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\tJIVTDTVOFK\t\tfalse\t\t\n" +
                "NaN\t0\t-122\tNaN\tNaN\tNaN\tKNSVTCFVTQ\t\tfalse\t\t2013-05-15T12:00:00.000Z\n" +
                "NaN\t30566\t20\tNaN\t0.9818\t-5466726161969343552\t\t\tfalse\t\t2013-05-18T00:00:00.000Z\n" +
                "NaN\t-15656\t0\tNaN\t0.6098\t-6217010216024734623\t\t\ttrue\t\t2013-05-20T12:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\t0.3901\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t2882\t0\t-105.500000000000\tNaN\tNaN\tOKIIHSWTEH\t\tfalse\t\t2013-05-25T12:00:00.000Z\n" +
                "NaN\t0\t-104\tNaN\tNaN\tNaN\t\t\tfalse\t\t2013-05-28T00:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\tODHWKFENXM\t\tfalse\t\t\n" +
                "-659184089\t-13800\t-2\t2.392256617546\t0.6519\t-7751136309261449149\tOKYULJBQTL\t\tfalse\t\t2013-06-02T00:00:00.000Z\n" +
                "-1883104688\t0\t0\tNaN\t0.6757\t8196181258827495370\t\t\tfalse\t\t\n" +
                "-2139859771\t0\t-79\t344.729835510254\tNaN\tNaN\tIBJCVFPFBC\t\tfalse\t\t\n" +
                "NaN\t10225\t0\t-572.296875000000\t0.2967\t-5767634907351262282\tTBWDVSZOIX\t\tfalse\t\t\n" +
                "NaN\t0\t-19\tNaN\t0.7135\t8969196340878943365\t\t\tfalse\t\t2013-06-12T00:00:00.000Z\n" +
                "NaN\t0\t-38\t-86.000000000000\tNaN\tNaN\tXKELTCVZXQ\t\tfalse\t\t2013-06-14T12:00:00.000Z\n" +
                "NaN\t29304\t0\tNaN\tNaN\t-1515294165892907204\t\t\tfalse\t\t2013-06-17T00:00:00.000Z\n" +
                "NaN\t17701\t0\t0.916345536709\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "-2139370311\t-32277\t65\tNaN\tNaN\t-7601183786211855388\t\t\ttrue\t\t2013-06-22T00:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t2013-06-24T12:00:00.000Z\n" +
                "1198081577\t0\t0\tNaN\tNaN\t-7481237079546197800\t\t\ttrue\t\t2013-06-27T00:00:00.000Z\n" +
                "NaN\t-17836\t0\tNaN\t0.5251\t-7316664068900365888\t\t\tfalse\t\t\n" +
                "NaN\t0\t60\t-1024.000000000000\t0.9287\t1451757238409137883\t\t\tfalse\t\t\n" +
                "632261185\t14561\t0\t447.342773437500\tNaN\tNaN\t\t\tfalse\t\t2013-07-04T12:00:00.000Z\n" +
                "NaN\t0\t-96\t0.005665154895\t0.5212\tNaN\tJEQMWTHZNH\t\ttrue\t\t\n" +
                "NaN\t4882\t0\t-721.570068359375\tNaN\tNaN\t\t\tfalse\t\t2013-07-09T12:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "1216600919\t23298\t-117\tNaN\tNaN\tNaN\tMNWBTIVJEW\t\ttrue\t\t2013-07-17T00:00:00.000Z\n" +
                "NaN\t0\t119\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t17913\t0\t0.020569519140\t0.5912\tNaN\t\t\tfalse\t\t2013-07-22T00:00:00.000Z\n" +
                "1610742551\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t2013-07-24T12:00:00.000Z\n" +
                "-1205646285\t0\t0\tNaN\t0.9289\t8642403514325899452\t\t\tfalse\t\t2013-07-27T00:00:00.000Z\n" +
                "NaN\t-23295\t0\tNaN\t0.8926\t-9150462926608062120\tWQLJNZCGCT\t\ttrue\t\t\n" +
                "NaN\t0\t0\tNaN\t0.6798\t-5864575418630714346\t\t\tfalse\t\t2013-08-01T00:00:00.000Z\n" +
                "1683275019\t-26804\t0\t4.128064155579\t0.2032\tNaN\t\t\tfalse\t\t2013-08-03T12:00:00.000Z\n" +
                "NaN\t0\t-94\tNaN\tNaN\t-9051427420978437586\t\t\tfalse\t\t2013-08-06T00:00:00.000Z\n" +
                "1934973454\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\tNaN\t-5303405449347886958\t\t\tfalse\t\t2013-08-11T00:00:00.000Z\n" +
                "NaN\t29170\t14\t-953.945312500000\t0.6473\t-6346552295544744665\t\t\ttrue\t\t2013-08-13T12:00:00.000Z\n" +
                "-1551250112\t0\t0\tNaN\tNaN\t-9153807758920642614\tUJXSVCWGHT\t\tfalse\t\t\n" +
                "-636263795\t0\t-76\tNaN\t0.5857\tNaN\t\t\ttrue\t\t\n" +
                "NaN\t-27755\t0\tNaN\t0.7426\t5259909879721818696\t\t\tfalse\t\t\n" +
                "731479609\t-20511\t0\t28.810546875000\t0.7764\t-8118558905916637434\t\t\tfalse\t\t\n" +
                "-1334703041\t-1358\t0\t0.000000017793\t0.3070\t-3883507671731232196\t\t\ttrue\t\t2013-08-26T00:00:00.000Z\n" +
                "NaN\t25020\t-107\tNaN\t0.6154\tNaN\tUYHVBTQZNP\t\ttrue\t\t\n" +
                "NaN\t0\t58\tNaN\tNaN\t-5516374931389294840\t\t\tfalse\t\t2013-08-31T00:00:00.000Z\n" +
                "964528173\t0\t0\t0.003956267610\t0.8607\t-3067936391188226389\t\t\tfalse\t\t2013-09-02T12:00:00.000Z\n" +
                "NaN\t0\t-105\t0.000000338487\tNaN\tNaN\t\t\tfalse\t\t2013-09-05T00:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\t7354668637892666879\t\t\tfalse\t\t\n" +
                "-448961895\t0\t0\tNaN\tNaN\t-9161200384798064634\tGZKHINLGPK\t\tfalse\t\t\n" +
                "NaN\t-27897\t40\tNaN\tNaN\t-7520515938192868059\t\t\ttrue\t\t2013-09-12T12:00:00.000Z\n" +
                "371473906\t0\t0\tNaN\t0.6473\t4656851861563783983\t\t\tfalse\t\t\n" +
                "-1346903540\t0\t0\tNaN\t0.5894\t-8299437881884939478\tNYWRPCINVX\t\tfalse\t\t2013-09-17T12:00:00.000Z\n" +
                "-1948757473\t0\t-46\tNaN\tNaN\t-6824321255992266244\t\t\tfalse\t\t\n" +
                "-268192526\t10310\t0\tNaN\t0.5699\t-6083767479706055886\t\t\ttrue\t\t\n" +
                "1294560337\t0\t0\t0.000000126063\tNaN\tNaN\t\t\tfalse\t\t2013-09-25T00:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\t0.5528\t-6393296971707706969\t\t\tfalse\t\t2013-09-27T12:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\t-8345528960002638166\t\t\ttrue\t\t\n" +
                "1744814812\t455\t0\t0.001713166508\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t-127\t0.015959210694\t0.7250\t8725410536784858046\t\t\tfalse\t\t\n" +
                "NaN\t0\t16\t939.765625000000\t0.7319\t-7413379514400996037\t\t\ttrue\t\t2013-10-07T12:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\t0.7553\tNaN\t\t\tfalse\t\t\n" +
                "1168191792\t21539\t-123\tNaN\tNaN\t-7702162217093176347\t\t\ttrue\t\t2013-10-12T12:00:00.000Z\n" +
                "NaN\t-21809\t0\tNaN\tNaN\t-3135568653781063174\t\t\tfalse\t\t2013-10-15T00:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t2013-10-17T12:00:00.000Z\n" +
                "2003366662\t0\t0\tNaN\tNaN\t-1621062241930578783\t\t\tfalse\t\t2013-10-20T00:00:00.000Z\n" +
                "NaN\t4277\t0\tNaN\tNaN\tNaN\tMHDSESFOOY\t\ttrue\t\t2013-10-22T12:00:00.000Z\n" +
                "1375853278\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t2013-10-25T00:00:00.000Z\n" +
                "NaN\t-19723\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t-52\tNaN\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "NaN\t0\t116\tNaN\tNaN\t-4675510353991993979\t\t\ttrue\t\t2013-11-01T12:00:00.000Z\n" +
                "NaN\t0\t72\tNaN\tNaN\t-1653512736810729151\t\t\tfalse\t\t2013-11-04T00:00:00.000Z\n" +
                "NaN\t-22994\t0\tNaN\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\tOJZOVQGFZU\t\tfalse\t\t\n";
        TestUtils.assertMemoryLeak(() -> testTableCursor(60 * 60 * 60000, expected));
    }

    @Test
    public void testReadByYear() throws Exception {
        CairoTestUtils.createAllTable(root, PartitionBy.YEAR);
        final String expected = "int\tshort\tbyte\tdouble\tfloat\tlong\tstr\tsym\tbool\tbin\tdate\n" +
                "73575701\t0\t0\tNaN\t0.7097\t-1675638984090602536\t\t\tfalse\t\t\n" +
                "NaN\t0\t89\tNaN\tNaN\t6236292340460979716\tPMIUPLYJVB\t\ttrue\t\t\n" +
                "NaN\t0\t60\tNaN\t0.6454\t-2715397729034539921\tOEYVSCKKDJ\t\tfalse\t\t2013-08-31T00:00:00.000Z\n" +
                "NaN\t0\t113\tNaN\tNaN\t-6905112719978615298\t\t\tfalse\t\t2013-10-30T00:00:00.000Z\n" +
                "-801004676\t0\t0\t0.000242509581\tNaN\t-7064303592482002884\t\t\tfalse\t\t2013-12-29T00:00:00.000Z\n" +
                "NaN\t-24062\t0\t272.000000000000\t0.4387\tNaN\tTGSOOWYGSD\t\ttrue\t\t\n" +
                "-640548855\t3093\t0\t-960.000000000000\t0.6056\t8522034740532776473\t\t\tfalse\t\t\n" +
                "61976253\t0\t0\t0.000000000000\t0.5092\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\t0.000041838453\t0.3185\tNaN\tZMGKVSIWRP\t\tfalse\t\t\n" +
                "NaN\t0\t22\tNaN\tNaN\tNaN\t\t\tfalse\t\t2014-10-25T00:00:00.000Z\n" +
                "777797854\t0\t0\tNaN\tNaN\t-3561042134139364009\t\t\tfalse\t\t2014-12-24T00:00:00.000Z\n" +
                "NaN\t0\t106\tNaN\t0.4809\tNaN\t\t\ttrue\t\t\n" +
                "1204862303\t-20282\t0\tNaN\tNaN\t8715974436393319034\t\t\ttrue\t\t2015-04-23T00:00:00.000Z\n" +
                "NaN\t0\t53\t-222.738281250000\tNaN\tNaN\tWOWDODUFGU\t\tfalse\t\t\n" +
                "2095297876\t21923\t0\t256.000000000000\t0.8653\t-6418805892627297273\t\t\ttrue\t\t2015-08-21T00:00:00.000Z\n" +
                "NaN\t-19019\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t2015-10-20T00:00:00.000Z\n" +
                "NaN\t-25663\t0\t-92.000000000000\t0.3888\tNaN\t\t\ttrue\t\t2015-12-19T00:00:00.000Z\n" +
                "-1601554634\t3379\t0\t1020.793212890625\tNaN\tNaN\tKSJZPZZKUM\t\tfalse\t\t\n" +
                "NaN\t26260\t0\t46.750000000000\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "-1050143454\t0\t0\tNaN\tNaN\t-4944873630491810081\t\t\tfalse\t\t\n" +
                "NaN\t0\t34\t0.013004892273\tNaN\tNaN\t\t\tfalse\t\t2016-08-15T00:00:00.000Z\n" +
                "-1242020108\t-11546\t-82\t0.000000004717\t0.4724\tNaN\t\t\tfalse\t\t\n" +
                "1512203086\t0\t0\t797.846359252930\tNaN\t6753493100272204912\t\t\tfalse\t\t\n" +
                "77063638\t870\t-100\tNaN\tNaN\tNaN\t\t\ttrue\t\t2017-02-11T00:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t27339\t0\t0.000000010539\tNaN\tNaN\t\t\tfalse\t\t2017-06-11T00:00:00.000Z\n" +
                "235357628\t0\t-120\t0.000156953447\tNaN\tNaN\tTTNGDKZVVS\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\tJIVTDTVOFK\t\tfalse\t\t\n" +
                "NaN\t0\t-122\tNaN\tNaN\tNaN\tKNSVTCFVTQ\t\tfalse\t\t2017-12-08T00:00:00.000Z\n" +
                "NaN\t30566\t20\tNaN\t0.9818\t-5466726161969343552\t\t\tfalse\t\t2018-02-06T00:00:00.000Z\n" +
                "NaN\t-15656\t0\tNaN\t0.6098\t-6217010216024734623\t\t\ttrue\t\t2018-04-07T00:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\t0.3901\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t2882\t0\t-105.500000000000\tNaN\tNaN\tOKIIHSWTEH\t\tfalse\t\t2018-08-05T00:00:00.000Z\n" +
                "NaN\t0\t-104\tNaN\tNaN\tNaN\t\t\tfalse\t\t2018-10-04T00:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\tODHWKFENXM\t\tfalse\t\t\n" +
                "-659184089\t-13800\t-2\t2.392256617546\t0.6519\t-7751136309261449149\tOKYULJBQTL\t\tfalse\t\t2019-02-01T00:00:00.000Z\n" +
                "-1883104688\t0\t0\tNaN\t0.6757\t8196181258827495370\t\t\tfalse\t\t\n" +
                "-2139859771\t0\t-79\t344.729835510254\tNaN\tNaN\tIBJCVFPFBC\t\tfalse\t\t\n" +
                "NaN\t10225\t0\t-572.296875000000\t0.2967\t-5767634907351262282\tTBWDVSZOIX\t\tfalse\t\t\n" +
                "NaN\t0\t-19\tNaN\t0.7135\t8969196340878943365\t\t\tfalse\t\t2019-09-29T00:00:00.000Z\n" +
                "NaN\t0\t-38\t-86.000000000000\tNaN\tNaN\tXKELTCVZXQ\t\tfalse\t\t2019-11-28T00:00:00.000Z\n" +
                "NaN\t29304\t0\tNaN\tNaN\t-1515294165892907204\t\t\tfalse\t\t2020-01-27T00:00:00.000Z\n" +
                "NaN\t17701\t0\t0.916345536709\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "-2139370311\t-32277\t65\tNaN\tNaN\t-7601183786211855388\t\t\ttrue\t\t2020-05-26T00:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t2020-07-25T00:00:00.000Z\n" +
                "1198081577\t0\t0\tNaN\tNaN\t-7481237079546197800\t\t\ttrue\t\t2020-09-23T00:00:00.000Z\n" +
                "NaN\t-17836\t0\tNaN\t0.5251\t-7316664068900365888\t\t\tfalse\t\t\n" +
                "NaN\t0\t60\t-1024.000000000000\t0.9287\t1451757238409137883\t\t\tfalse\t\t\n" +
                "632261185\t14561\t0\t447.342773437500\tNaN\tNaN\t\t\tfalse\t\t2021-03-22T00:00:00.000Z\n" +
                "NaN\t0\t-96\t0.005665154895\t0.5212\tNaN\tJEQMWTHZNH\t\ttrue\t\t\n" +
                "NaN\t4882\t0\t-721.570068359375\tNaN\tNaN\t\t\tfalse\t\t2021-07-20T00:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "1216600919\t23298\t-117\tNaN\tNaN\tNaN\tMNWBTIVJEW\t\ttrue\t\t2022-01-16T00:00:00.000Z\n" +
                "NaN\t0\t119\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t17913\t0\t0.020569519140\t0.5912\tNaN\t\t\tfalse\t\t2022-05-16T00:00:00.000Z\n" +
                "1610742551\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t2022-07-15T00:00:00.000Z\n" +
                "-1205646285\t0\t0\tNaN\t0.9289\t8642403514325899452\t\t\tfalse\t\t2022-09-13T00:00:00.000Z\n" +
                "NaN\t-23295\t0\tNaN\t0.8926\t-9150462926608062120\tWQLJNZCGCT\t\ttrue\t\t\n" +
                "NaN\t0\t0\tNaN\t0.6798\t-5864575418630714346\t\t\tfalse\t\t2023-01-11T00:00:00.000Z\n" +
                "1683275019\t-26804\t0\t4.128064155579\t0.2032\tNaN\t\t\tfalse\t\t2023-03-12T00:00:00.000Z\n" +
                "NaN\t0\t-94\tNaN\tNaN\t-9051427420978437586\t\t\tfalse\t\t2023-05-11T00:00:00.000Z\n" +
                "1934973454\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\tNaN\t-5303405449347886958\t\t\tfalse\t\t2023-09-08T00:00:00.000Z\n" +
                "NaN\t29170\t14\t-953.945312500000\t0.6473\t-6346552295544744665\t\t\ttrue\t\t2023-11-07T00:00:00.000Z\n" +
                "-1551250112\t0\t0\tNaN\tNaN\t-9153807758920642614\tUJXSVCWGHT\t\tfalse\t\t\n" +
                "-636263795\t0\t-76\tNaN\t0.5857\tNaN\t\t\ttrue\t\t\n" +
                "NaN\t-27755\t0\tNaN\t0.7426\t5259909879721818696\t\t\tfalse\t\t\n" +
                "731479609\t-20511\t0\t28.810546875000\t0.7764\t-8118558905916637434\t\t\tfalse\t\t\n" +
                "-1334703041\t-1358\t0\t0.000000017793\t0.3070\t-3883507671731232196\t\t\ttrue\t\t2024-09-02T00:00:00.000Z\n" +
                "NaN\t25020\t-107\tNaN\t0.6154\tNaN\tUYHVBTQZNP\t\ttrue\t\t\n" +
                "NaN\t0\t58\tNaN\tNaN\t-5516374931389294840\t\t\tfalse\t\t2024-12-31T00:00:00.000Z\n" +
                "964528173\t0\t0\t0.003956267610\t0.8607\t-3067936391188226389\t\t\tfalse\t\t2025-03-01T00:00:00.000Z\n" +
                "NaN\t0\t-105\t0.000000338487\tNaN\tNaN\t\t\tfalse\t\t2025-04-30T00:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\t7354668637892666879\t\t\tfalse\t\t\n" +
                "-448961895\t0\t0\tNaN\tNaN\t-9161200384798064634\tGZKHINLGPK\t\tfalse\t\t\n" +
                "NaN\t-27897\t40\tNaN\tNaN\t-7520515938192868059\t\t\ttrue\t\t2025-10-27T00:00:00.000Z\n" +
                "371473906\t0\t0\tNaN\t0.6473\t4656851861563783983\t\t\tfalse\t\t\n" +
                "-1346903540\t0\t0\tNaN\t0.5894\t-8299437881884939478\tNYWRPCINVX\t\tfalse\t\t2026-02-24T00:00:00.000Z\n" +
                "-1948757473\t0\t-46\tNaN\tNaN\t-6824321255992266244\t\t\tfalse\t\t\n" +
                "-268192526\t10310\t0\tNaN\t0.5699\t-6083767479706055886\t\t\ttrue\t\t\n" +
                "1294560337\t0\t0\t0.000000126063\tNaN\tNaN\t\t\tfalse\t\t2026-08-23T00:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\t0.5528\t-6393296971707706969\t\t\tfalse\t\t2026-10-22T00:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\t-8345528960002638166\t\t\ttrue\t\t\n" +
                "1744814812\t455\t0\t0.001713166508\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t-127\t0.015959210694\t0.7250\t8725410536784858046\t\t\tfalse\t\t\n" +
                "NaN\t0\t16\t939.765625000000\t0.7319\t-7413379514400996037\t\t\ttrue\t\t2027-06-19T00:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\t0.7553\tNaN\t\t\tfalse\t\t\n" +
                "1168191792\t21539\t-123\tNaN\tNaN\t-7702162217093176347\t\t\ttrue\t\t2027-10-17T00:00:00.000Z\n" +
                "NaN\t-21809\t0\tNaN\tNaN\t-3135568653781063174\t\t\tfalse\t\t2027-12-16T00:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t2028-02-14T00:00:00.000Z\n" +
                "2003366662\t0\t0\tNaN\tNaN\t-1621062241930578783\t\t\tfalse\t\t2028-04-14T00:00:00.000Z\n" +
                "NaN\t4277\t0\tNaN\tNaN\tNaN\tMHDSESFOOY\t\ttrue\t\t2028-06-13T00:00:00.000Z\n" +
                "1375853278\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t2028-08-12T00:00:00.000Z\n" +
                "NaN\t-19723\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t-52\tNaN\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "NaN\t0\t116\tNaN\tNaN\t-4675510353991993979\t\t\ttrue\t\t2029-02-08T00:00:00.000Z\n" +
                "NaN\t0\t72\tNaN\tNaN\t-1653512736810729151\t\t\tfalse\t\t2029-04-09T00:00:00.000Z\n" +
                "NaN\t-22994\t0\tNaN\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\tOJZOVQGFZU\t\tfalse\t\t\n";
        TestUtils.assertMemoryLeak(() -> testTableCursor(24 * 60 * 60 * 60000L, expected));
    }

    @Test
    public void testReadNonPartitioned() throws Exception {
        CairoTestUtils.createAllTable(root, PartitionBy.NONE);
        TestUtils.assertMemoryLeak(this::testTableCursor);
    }

    @Test
    public void testReloadByDaySwitch() throws Exception {
        testReload(PartitionBy.DAY, 150, 6 * 60000L, MUST_SWITCH);
    }

    @Test
    public void testReloadByMonthSamePartition() throws Exception {
        testReload(PartitionBy.MONTH, 15, 60L * 60000, MUST_NOT_SWITCH);
    }

    @Test
    public void testReloadByMonthSwitch() throws Exception {
        testReload(PartitionBy.MONTH, 15, 24 * 60L * 60000, MUST_SWITCH);
    }

    @Test
    public void testReloadByYearSamePartition() throws Exception {
        testReload(PartitionBy.YEAR, 100, 60 * 60000 * 24L, MUST_NOT_SWITCH);
    }

    @Test
    public void testReloadByYearSwitch() throws Exception {
        testReload(PartitionBy.YEAR, 200, 60 * 60000 * 24L, MUST_SWITCH);
    }

    @Test
    public void testReloadDaySamePartition() throws Exception {
        testReload(PartitionBy.DAY, 10, 60L * 60000, MUST_NOT_SWITCH);
    }

    @Test
    public void testReloadNonPartitioned() throws Exception {
        testReload(PartitionBy.NONE, 10, 60L * 60000, DONT_CARE);
    }

    private static long allocBlob() {
        return Unsafe.malloc(blobLen);
    }

    private static void freeBlob(long blob) {
        Unsafe.free(blob, blobLen);
    }

    private static void assertBin(Record r, Rnd exp, long blob, int index) {
        if (exp.nextBoolean()) {
            exp.nextChars(blob, blobLen / 2);
            Assert.assertEquals(blobLen, r.getBinLen(index));
            BinarySequence sq = r.getBin2(index);
            for (int l = 0; l < blobLen; l++) {
                byte b = sq.byteAt(l);
                boolean result = Unsafe.getUnsafe().getByte(blob + l) != b;
                if (result) {
                    Assert.fail("Error at [" + l + "]: expected=" + Unsafe.getUnsafe().getByte(blob + l) + ", actual=" + b);
                }
            }
        } else {
            Assert.assertEquals(-1, r.getBinLen(index));
        }
    }

    private static void assertStrColumn(CharSequence expected, Record r, int index) {
        TestUtils.assertEquals(expected, r.getFlyweightStr(index));
        TestUtils.assertEquals(expected, r.getFlyweightStrB(index));
        Assert.assertFalse(r.getFlyweightStr(index) == r.getFlyweightStrB(6));
        Assert.assertEquals(expected.length(), r.getStrLen(index));
    }

    private static void assertNullStr(Record r, int index) {
        Assert.assertNull(r.getFlyweightStr(index));
        Assert.assertNull(r.getFlyweightStrB(index));
        Assert.assertEquals(-1, r.getStrLen(index));
    }

    private void assertBatch2(int count, long increment, long ts, long blob, TableReader reader) {
        reader.toTop();
        Rnd exp = new Rnd();
        long ts2 = assertPartialCursor(reader, exp, ts, increment, blob, 3 * count, (r, rnd13, ts13, blob13) -> {
            BATCH1_ASSERTER.assertRecord(r, rnd13, ts13, blob13);
            BATCH2_BEFORE_ASSERTER.assertRecord(r, rnd13, ts13, blob13);
        });
        assertPartialCursor(reader, exp, ts2, increment, blob, count, BATCH2_ASSERTER);
    }

    private void assertBatch3(int count, long increment, long ts, long blob, TableReader reader) {
        Rnd exp = new Rnd();
        long ts2;
        reader.toTop();
        ts2 = assertPartialCursor(reader, exp, ts, increment, blob, 3 * count, (r, rnd1, ts1, blob1) -> {
            BATCH1_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH2_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH3_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
        });

        ts2 = assertPartialCursor(reader, exp, ts2, increment, blob, count, (r, rnd12, ts12, blob12) -> {
            BATCH2_ASSERTER.assertRecord(r, rnd12, ts12, blob12);
            BATCH3_BEFORE_ASSERTER.assertRecord(r, rnd12, ts12, blob12);
        });

        assertPartialCursor(reader, exp, ts2, increment, blob, count, BATCH3_ASSERTER);
    }

    private void assertBatch4(int count, long increment, long ts, long blob, TableReader reader) {
        Rnd exp;
        long ts2;
        exp = new Rnd();
        reader.toTop();
        ts2 = assertPartialCursor(reader, exp, ts, increment, blob, 3 * count, (r, rnd1, ts1, blob1) -> {
            BATCH1_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH2_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH3_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH4_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
        });

        ts2 = assertPartialCursor(reader, exp, ts2, increment, blob, count, (r, rnd12, ts12, blob12) -> {
            BATCH2_ASSERTER.assertRecord(r, rnd12, ts12, blob12);
            BATCH3_BEFORE_ASSERTER.assertRecord(r, rnd12, ts12, blob12);
            BATCH4_BEFORE_ASSERTER.assertRecord(r, rnd12, ts12, blob12);
        });

        ts2 = assertPartialCursor(reader, exp, ts2, increment, blob, count, (r, rnd14, ts14, blob14) -> {
            BATCH4_BEFORE_ASSERTER.assertRecord(r, rnd14, ts14, blob14);
            BATCH3_ASSERTER.assertRecord(r, rnd14, ts14, blob14);
        });

        assertPartialCursor(reader, exp, ts2, increment, blob, count, BATCH4_ASSERTER);
    }

    private long assertBatch5(int count, long increment, long ts, long blob, TableReader reader, Rnd exp) {
        long ts2;
        reader.toTop();
        ts2 = assertPartialCursor(reader, exp, ts, increment, blob, 3 * count, (r, rnd1, ts1, blob1) -> {
            BATCH1_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH2_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH3_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH5_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
        });

        ts2 = assertPartialCursor(reader, exp, ts2, increment, blob, count, (r, rnd1, ts1, blob1) -> {
            BATCH2_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH3_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH5_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
        });

        ts2 = assertPartialCursor(reader, exp, ts2, increment, blob, count, (r, rnd1, ts1, blob1) -> {
            BATCH5_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH3_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
        });

        return assertPartialCursor(reader, exp, ts2, increment, blob, count, BATCH5_ASSERTER);
    }

    private void assertBatch6(int count, long increment, long ts, long blob, TableReader reader) {
        Rnd exp;
        long ts2;
        exp = new Rnd();
        ts2 = assertBatch5(count, increment, ts, blob, reader, exp);
        assertPartialCursor(reader, exp, ts2, increment, blob, count, BATCH6_ASSERTER);
    }

    private void assertBatch7(int count, long increment, long ts, long blob, TableReader reader) {
        reader.toTop();
        Rnd exp = new Rnd();
        long ts2 = assertPartialCursor(reader, exp, ts, increment, blob, 3 * count, (r, rnd1, ts1, blob1) -> {
            BATCH1_7_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH_2_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH_3_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH_4_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
        });

        ts2 = assertPartialCursor(reader, exp, ts2, increment, blob, count, (r, rnd1, ts1, blob1) -> {
            BATCH2_7_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH_3_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH_4_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
        });

        ts2 = assertPartialCursor(reader, exp, ts2, increment, blob, count, (r, rnd1, ts1, blob1) -> {
            BATCH3_7_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH_4_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
        });

        ts2 = assertPartialCursor(reader, exp, ts2, increment, blob, count, BATCH5_7_ASSERTER);
        assertPartialCursor(reader, exp, ts2, increment, blob, count, BATCH6_7_ASSERTER);
    }

    private void assertCursor(TableReader reader, Rnd rnd, long ts, long increment, long blob, long expectedSize, RecordAssert asserter) {
        Assert.assertEquals(expectedSize, reader.size());
        reader.toTop();
        assertPartialCursor(reader, rnd, ts, increment, blob, expectedSize, asserter);
    }

    private long assertPartialCursor(TableReader reader, Rnd rnd, long ts, long increment, long blob, long expectedSize, RecordAssert asserter) {
        int count = 0;
        while (reader.hasNext() && count < expectedSize) {
            count++;
            asserter.assertRecord(reader.next(), rnd, ts += increment, blob);
        }
        // did our loop run?
        Assert.assertEquals(expectedSize, count);
        return ts;
    }

    private long testAppend(Rnd rnd, FilesFacade ff, long ts, int count, long inc, long blob, int testPartitionSwitch, FieldGenerator generator) throws NumericException {
        try (TableWriter writer = new TableWriter(ff, root, "all")) {
            return testAppend(writer, rnd, ts, count, inc, blob, testPartitionSwitch, generator);
        }
    }

    private long testAppend(TableWriter writer, Rnd rnd, long ts, int count, long inc, long blob, int testPartitionSwitch, FieldGenerator generator) {
        long size = writer.size();

        long timestamp = writer.getMaxTimestamp();

        for (int i = 0; i < count; i++) {
            TableWriter.Row r = writer.newRow(ts += inc);
            generator.generate(r, rnd, ts, blob);
            r.append();
        }
        writer.commit();

        if (testPartitionSwitch == MUST_SWITCH) {
            Assert.assertFalse(CairoTestUtils.isSamePartition(timestamp, writer.getMaxTimestamp(), writer.getPartitionBy()));
        } else if (testPartitionSwitch == MUST_NOT_SWITCH) {
            Assert.assertTrue(CairoTestUtils.isSamePartition(timestamp, writer.getMaxTimestamp(), writer.getPartitionBy()));
        }

        Assert.assertEquals(size + count, writer.size());
        return ts;
    }

    private void testReload(int partitionBy, int count, long increment, final int testPartitionSwitch) throws Exception {
        CairoTestUtils.createAllTable(root, partitionBy);

        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();

            long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");

            long blob = allocBlob();
            try {

                // test if reader behaves correctly when table is empty

                try (TableReader reader = new TableReader(FF, root, "all")) {
                    // reader can see all the rows ?
                    assertCursor(reader, new Rnd(), ts, increment, blob, 0, BATCH1_ASSERTER);
                }

                // create table with first batch populating all columns (there could be null values too)
                long nextTs = testAppend(rnd, FF, ts, count, increment, blob, 0, BATCH1_GENERATOR);

                try (TableReader reader = new TableReader(FF, root, "all")) {

                    // make sure we can see first batch right after table is open
                    assertCursor(reader, new Rnd(), ts, increment, blob, count, BATCH1_ASSERTER);

                    // try reload when table hasn't changed
                    Assert.assertFalse(reader.reload());

                    // add second batch to test if reload of open table will pick it up
                    nextTs = testAppend(rnd, FF, nextTs, count, increment, blob, testPartitionSwitch, BATCH1_GENERATOR);

                    // if we don't reload reader it should still see first batch
                    // reader can see all the rows ?
                    assertCursor(reader, new Rnd(), ts, increment, blob, count, BATCH1_ASSERTER);

                    // reload should be successful because we have new data in the table
                    Assert.assertTrue(reader.reload());

                    // check if we can see second batch after reader was reloaded
                    assertCursor(reader, new Rnd(), ts, increment, blob, 2 * count, BATCH1_ASSERTER);

                    // writer will inflate last partition in order to optimise appends
                    // reader must be able to cope with that
                    try (TableWriter writer = new TableWriter(FF, root, "all")) {

                        // this is a bit of paranoid check, but make sure our reader doesn't flinch when new writer is open
                        assertCursor(reader, new Rnd(), ts, increment, blob, 2 * count, BATCH1_ASSERTER);

                        // also make sure that there is nothing to reload, we've not done anything to data after all
                        Assert.assertFalse(reader.reload());

                        // check that we can still see two batches after no-op reload
                        // we rule out possibility of reload() corrupting table state
                        assertCursor(reader, new Rnd(), ts, increment, blob, 2 * count, BATCH1_ASSERTER);

                        // just for no reason add third batch
                        nextTs = testAppend(writer, rnd, nextTs, count, increment, blob, 0, BATCH1_GENERATOR);

                        // table must be able to reload now
                        Assert.assertTrue(reader.reload());

                        // and we should see three batches of data
                        assertCursor(reader, new Rnd(), ts, increment, blob, 3 * count, BATCH1_ASSERTER);

                        // this is where things get interesting
                        // add single column
                        writer.addColumn("str2", ColumnType.STRING);

                        // populate table with fourth batch, this time we also populate new column
                        // we expect that values of new column will be NULL for first three batches and non-NULL for fourth
                        nextTs = testAppend(writer, rnd, nextTs, count, increment, blob, 0, BATCH2_GENERATOR);

                        // reload table, check if it was positive effort
                        // todo: reload() will not reload structure just yet, make sure it does
                        Assert.assertTrue(reader.reload());
                        // this is a temporary arrangement, we know that structure changed, so we force reload structure
                        // normally reader doesn't know if structure changed, reload() method should be able to determine that
                        // on its own
                        reader.reloadStruct();

                        // two-step assert checks 3/4 rows checking that new column is NUL
                        // the last 1/3 is checked including new column
                        // this is why we need to use same random state and timestamp
                        assertBatch2(count, increment, ts, blob, reader);

                        // good job we got as far as this
                        // now add another column and populate fifth batch, including new column
                        // reading this table will ensure tops are preserved

                        writer.addColumn("int2", ColumnType.INT);

                        nextTs = testAppend(writer, rnd, nextTs, count, increment, blob, 0, BATCH3_GENERATOR);

                        Assert.assertTrue(reader.reload());
                        reader.reloadStruct();

                        assertBatch3(count, increment, ts, blob, reader);

                        // now append more columns that would overflow column buffer and force table to use different
                        // algo when retaining resources

                        writer.addColumn("short2", ColumnType.SHORT);
                        writer.addColumn("bool2", ColumnType.BOOLEAN);
                        writer.addColumn("byte2", ColumnType.BYTE);
                        writer.addColumn("float2", ColumnType.FLOAT);
                        writer.addColumn("double2", ColumnType.DOUBLE);
                        writer.addColumn("sym2", ColumnType.SYMBOL);
                        writer.addColumn("long2", ColumnType.LONG);
                        writer.addColumn("date2", ColumnType.DATE);
                        writer.addColumn("bin2", ColumnType.BINARY);

                        // populate new columns and start asserting batches, which would assert that new columns are
                        // retrospectively "null" in existing records
                        nextTs = testAppend(writer, rnd, nextTs, count, increment, blob, 0, BATCH4_GENERATOR);

                        Assert.assertTrue(reader.reload());
                        reader.reloadStruct();

                        assertBatch4(count, increment, ts, blob, reader);

                        // now delete last column

                        writer.removeColumn("bin2");

                        Assert.assertTrue(reader.reload());
                        reader.reloadStruct();

                        // and assert that all columns that have not been deleted contain correct values

                        assertBatch5(count, increment, ts, blob, reader, new Rnd());

                        // append all columns excluding the one we just deleted
                        nextTs = testAppend(writer, rnd, nextTs, count, increment, blob, 0, BATCH6_GENERATOR);

                        Assert.assertTrue(reader.reload());

                        // and assert that all columns that have not been deleted contain correct values
                        assertBatch6(count, increment, ts, blob, reader);

                        // remove first column and add new column by same name
                        writer.removeColumn("int");
                        writer.addColumn("int", ColumnType.INT);

                        Assert.assertTrue(reader.reload());
                        reader.reloadStruct();

                        assertBatch7(count, increment, ts, blob, reader);

                    }
                }
            } finally {
                freeBlob(blob);
            }
        });
    }

    private void testTableCursor(long increment, String expected) throws IOException, NumericException {
        Rnd rnd = new Rnd();
        int N = 100;
        long ts = DateFormatUtils.parseDateTime("2013-03-04T00:00:00.000Z");
        long blob = allocBlob();
        try {
            testAppend(rnd, FF, ts, N, increment, blob, 0, BATCH1_GENERATOR);

            final StringSink sink = new StringSink();
            final RecordSourcePrinter printer = new RecordSourcePrinter(sink);
            final LongList rows = new LongList();
            try (TableReader reader = new TableReader(FF, root, "all")) {
                Assert.assertEquals(N, reader.size());
                printer.print(reader, true, reader.getMetadata());
                TestUtils.assertEquals(expected, sink);

                sink.clear();
                reader.toTop();

                printer.print(reader, true, reader.getMetadata());
                TestUtils.assertEquals(expected, sink);

                reader.toTop();
                while (reader.hasNext()) {
                    rows.add(reader.next().getRowId());
                }

                Rnd exp = new Rnd();
                for (int i = 0, n = rows.size(); i < n; i++) {
                    BATCH1_ASSERTER.assertRecord(reader.recordAt(rows.getQuick(i)), exp, ts += increment, blob);
                }
            }
        } finally {
            freeBlob(blob);
        }
    }

    private void testTableCursor() throws IOException, NumericException {
        final String expected = "int\tshort\tbyte\tdouble\tfloat\tlong\tstr\tsym\tbool\tbin\tdate\n" +
                "73575701\t0\t0\tNaN\t0.7097\t-1675638984090602536\t\t\tfalse\t\t\n" +
                "NaN\t0\t89\tNaN\tNaN\t6236292340460979716\tPMIUPLYJVB\t\ttrue\t\t\n" +
                "NaN\t0\t60\tNaN\t0.6454\t-2715397729034539921\tOEYVSCKKDJ\t\tfalse\t\t2013-03-04T03:00:00.000Z\n" +
                "NaN\t0\t113\tNaN\tNaN\t-6905112719978615298\t\t\tfalse\t\t2013-03-04T04:00:00.000Z\n" +
                "-801004676\t0\t0\t0.000242509581\tNaN\t-7064303592482002884\t\t\tfalse\t\t2013-03-04T05:00:00.000Z\n" +
                "NaN\t-24062\t0\t272.000000000000\t0.4387\tNaN\tTGSOOWYGSD\t\ttrue\t\t\n" +
                "-640548855\t3093\t0\t-960.000000000000\t0.6056\t8522034740532776473\t\t\tfalse\t\t\n" +
                "61976253\t0\t0\t0.000000000000\t0.5092\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\t0.000041838453\t0.3185\tNaN\tZMGKVSIWRP\t\tfalse\t\t\n" +
                "NaN\t0\t22\tNaN\tNaN\tNaN\t\t\tfalse\t\t2013-03-04T10:00:00.000Z\n" +
                "777797854\t0\t0\tNaN\tNaN\t-3561042134139364009\t\t\tfalse\t\t2013-03-04T11:00:00.000Z\n" +
                "NaN\t0\t106\tNaN\t0.4809\tNaN\t\t\ttrue\t\t\n" +
                "1204862303\t-20282\t0\tNaN\tNaN\t8715974436393319034\t\t\ttrue\t\t2013-03-04T13:00:00.000Z\n" +
                "NaN\t0\t53\t-222.738281250000\tNaN\tNaN\tWOWDODUFGU\t\tfalse\t\t\n" +
                "2095297876\t21923\t0\t256.000000000000\t0.8653\t-6418805892627297273\t\t\ttrue\t\t2013-03-04T15:00:00.000Z\n" +
                "NaN\t-19019\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t2013-03-04T16:00:00.000Z\n" +
                "NaN\t-25663\t0\t-92.000000000000\t0.3888\tNaN\t\t\ttrue\t\t2013-03-04T17:00:00.000Z\n" +
                "-1601554634\t3379\t0\t1020.793212890625\tNaN\tNaN\tKSJZPZZKUM\t\tfalse\t\t\n" +
                "NaN\t26260\t0\t46.750000000000\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "-1050143454\t0\t0\tNaN\tNaN\t-4944873630491810081\t\t\tfalse\t\t\n" +
                "NaN\t0\t34\t0.013004892273\tNaN\tNaN\t\t\tfalse\t\t2013-03-04T21:00:00.000Z\n" +
                "-1242020108\t-11546\t-82\t0.000000004717\t0.4724\tNaN\t\t\tfalse\t\t\n" +
                "1512203086\t0\t0\t797.846359252930\tNaN\t6753493100272204912\t\t\tfalse\t\t\n" +
                "77063638\t870\t-100\tNaN\tNaN\tNaN\t\t\ttrue\t\t2013-03-05T00:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t27339\t0\t0.000000010539\tNaN\tNaN\t\t\tfalse\t\t2013-03-05T02:00:00.000Z\n" +
                "235357628\t0\t-120\t0.000156953447\tNaN\tNaN\tTTNGDKZVVS\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\tJIVTDTVOFK\t\tfalse\t\t\n" +
                "NaN\t0\t-122\tNaN\tNaN\tNaN\tKNSVTCFVTQ\t\tfalse\t\t2013-03-05T05:00:00.000Z\n" +
                "NaN\t30566\t20\tNaN\t0.9818\t-5466726161969343552\t\t\tfalse\t\t2013-03-05T06:00:00.000Z\n" +
                "NaN\t-15656\t0\tNaN\t0.6098\t-6217010216024734623\t\t\ttrue\t\t2013-03-05T07:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\t0.3901\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t2882\t0\t-105.500000000000\tNaN\tNaN\tOKIIHSWTEH\t\tfalse\t\t2013-03-05T09:00:00.000Z\n" +
                "NaN\t0\t-104\tNaN\tNaN\tNaN\t\t\tfalse\t\t2013-03-05T10:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\tODHWKFENXM\t\tfalse\t\t\n" +
                "-659184089\t-13800\t-2\t2.392256617546\t0.6519\t-7751136309261449149\tOKYULJBQTL\t\tfalse\t\t2013-03-05T12:00:00.000Z\n" +
                "-1883104688\t0\t0\tNaN\t0.6757\t8196181258827495370\t\t\tfalse\t\t\n" +
                "-2139859771\t0\t-79\t344.729835510254\tNaN\tNaN\tIBJCVFPFBC\t\tfalse\t\t\n" +
                "NaN\t10225\t0\t-572.296875000000\t0.2967\t-5767634907351262282\tTBWDVSZOIX\t\tfalse\t\t\n" +
                "NaN\t0\t-19\tNaN\t0.7135\t8969196340878943365\t\t\tfalse\t\t2013-03-05T16:00:00.000Z\n" +
                "NaN\t0\t-38\t-86.000000000000\tNaN\tNaN\tXKELTCVZXQ\t\tfalse\t\t2013-03-05T17:00:00.000Z\n" +
                "NaN\t29304\t0\tNaN\tNaN\t-1515294165892907204\t\t\tfalse\t\t2013-03-05T18:00:00.000Z\n" +
                "NaN\t17701\t0\t0.916345536709\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "-2139370311\t-32277\t65\tNaN\tNaN\t-7601183786211855388\t\t\ttrue\t\t2013-03-05T20:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t2013-03-05T21:00:00.000Z\n" +
                "1198081577\t0\t0\tNaN\tNaN\t-7481237079546197800\t\t\ttrue\t\t2013-03-05T22:00:00.000Z\n" +
                "NaN\t-17836\t0\tNaN\t0.5251\t-7316664068900365888\t\t\tfalse\t\t\n" +
                "NaN\t0\t60\t-1024.000000000000\t0.9287\t1451757238409137883\t\t\tfalse\t\t\n" +
                "632261185\t14561\t0\t447.342773437500\tNaN\tNaN\t\t\tfalse\t\t2013-03-06T01:00:00.000Z\n" +
                "NaN\t0\t-96\t0.005665154895\t0.5212\tNaN\tJEQMWTHZNH\t\ttrue\t\t\n" +
                "NaN\t4882\t0\t-721.570068359375\tNaN\tNaN\t\t\tfalse\t\t2013-03-06T03:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "1216600919\t23298\t-117\tNaN\tNaN\tNaN\tMNWBTIVJEW\t\ttrue\t\t2013-03-06T06:00:00.000Z\n" +
                "NaN\t0\t119\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t17913\t0\t0.020569519140\t0.5912\tNaN\t\t\tfalse\t\t2013-03-06T08:00:00.000Z\n" +
                "1610742551\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t2013-03-06T09:00:00.000Z\n" +
                "-1205646285\t0\t0\tNaN\t0.9289\t8642403514325899452\t\t\tfalse\t\t2013-03-06T10:00:00.000Z\n" +
                "NaN\t-23295\t0\tNaN\t0.8926\t-9150462926608062120\tWQLJNZCGCT\t\ttrue\t\t\n" +
                "NaN\t0\t0\tNaN\t0.6798\t-5864575418630714346\t\t\tfalse\t\t2013-03-06T12:00:00.000Z\n" +
                "1683275019\t-26804\t0\t4.128064155579\t0.2032\tNaN\t\t\tfalse\t\t2013-03-06T13:00:00.000Z\n" +
                "NaN\t0\t-94\tNaN\tNaN\t-9051427420978437586\t\t\tfalse\t\t2013-03-06T14:00:00.000Z\n" +
                "1934973454\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t0\tNaN\tNaN\t-5303405449347886958\t\t\tfalse\t\t2013-03-06T16:00:00.000Z\n" +
                "NaN\t29170\t14\t-953.945312500000\t0.6473\t-6346552295544744665\t\t\ttrue\t\t2013-03-06T17:00:00.000Z\n" +
                "-1551250112\t0\t0\tNaN\tNaN\t-9153807758920642614\tUJXSVCWGHT\t\tfalse\t\t\n" +
                "-636263795\t0\t-76\tNaN\t0.5857\tNaN\t\t\ttrue\t\t\n" +
                "NaN\t-27755\t0\tNaN\t0.7426\t5259909879721818696\t\t\tfalse\t\t\n" +
                "731479609\t-20511\t0\t28.810546875000\t0.7764\t-8118558905916637434\t\t\tfalse\t\t\n" +
                "-1334703041\t-1358\t0\t0.000000017793\t0.3070\t-3883507671731232196\t\t\ttrue\t\t2013-03-06T22:00:00.000Z\n" +
                "NaN\t25020\t-107\tNaN\t0.6154\tNaN\tUYHVBTQZNP\t\ttrue\t\t\n" +
                "NaN\t0\t58\tNaN\tNaN\t-5516374931389294840\t\t\tfalse\t\t2013-03-07T00:00:00.000Z\n" +
                "964528173\t0\t0\t0.003956267610\t0.8607\t-3067936391188226389\t\t\tfalse\t\t2013-03-07T01:00:00.000Z\n" +
                "NaN\t0\t-105\t0.000000338487\tNaN\tNaN\t\t\tfalse\t\t2013-03-07T02:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\t7354668637892666879\t\t\tfalse\t\t\n" +
                "-448961895\t0\t0\tNaN\tNaN\t-9161200384798064634\tGZKHINLGPK\t\tfalse\t\t\n" +
                "NaN\t-27897\t40\tNaN\tNaN\t-7520515938192868059\t\t\ttrue\t\t2013-03-07T05:00:00.000Z\n" +
                "371473906\t0\t0\tNaN\t0.6473\t4656851861563783983\t\t\tfalse\t\t\n" +
                "-1346903540\t0\t0\tNaN\t0.5894\t-8299437881884939478\tNYWRPCINVX\t\tfalse\t\t2013-03-07T07:00:00.000Z\n" +
                "-1948757473\t0\t-46\tNaN\tNaN\t-6824321255992266244\t\t\tfalse\t\t\n" +
                "-268192526\t10310\t0\tNaN\t0.5699\t-6083767479706055886\t\t\ttrue\t\t\n" +
                "1294560337\t0\t0\t0.000000126063\tNaN\tNaN\t\t\tfalse\t\t2013-03-07T10:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\t0.5528\t-6393296971707706969\t\t\tfalse\t\t2013-03-07T11:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\t-8345528960002638166\t\t\ttrue\t\t\n" +
                "1744814812\t455\t0\t0.001713166508\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t-127\t0.015959210694\t0.7250\t8725410536784858046\t\t\tfalse\t\t\n" +
                "NaN\t0\t16\t939.765625000000\t0.7319\t-7413379514400996037\t\t\ttrue\t\t2013-03-07T15:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\t0.7553\tNaN\t\t\tfalse\t\t\n" +
                "1168191792\t21539\t-123\tNaN\tNaN\t-7702162217093176347\t\t\ttrue\t\t2013-03-07T17:00:00.000Z\n" +
                "NaN\t-21809\t0\tNaN\tNaN\t-3135568653781063174\t\t\tfalse\t\t2013-03-07T18:00:00.000Z\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t2013-03-07T19:00:00.000Z\n" +
                "2003366662\t0\t0\tNaN\tNaN\t-1621062241930578783\t\t\tfalse\t\t2013-03-07T20:00:00.000Z\n" +
                "NaN\t4277\t0\tNaN\tNaN\tNaN\tMHDSESFOOY\t\ttrue\t\t2013-03-07T21:00:00.000Z\n" +
                "1375853278\t0\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t2013-03-07T22:00:00.000Z\n" +
                "NaN\t-19723\t0\tNaN\tNaN\tNaN\t\t\tfalse\t\t\n" +
                "NaN\t0\t-52\tNaN\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "NaN\t0\t116\tNaN\tNaN\t-4675510353991993979\t\t\ttrue\t\t2013-03-08T01:00:00.000Z\n" +
                "NaN\t0\t72\tNaN\tNaN\t-1653512736810729151\t\t\tfalse\t\t2013-03-08T02:00:00.000Z\n" +
                "NaN\t-22994\t0\tNaN\tNaN\tNaN\t\t\ttrue\t\t\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\tOJZOVQGFZU\t\tfalse\t\t\n";
        testTableCursor(60 * 60000, expected);
    }

    @FunctionalInterface
    private interface RecordAssert {
        void assertRecord(Record r, Rnd rnd, long ts, long blob);
    }

    @FunctionalInterface
    private interface FieldGenerator {
        void generate(TableWriter.Row r, Rnd rnd, long ts, long blob);
    }
}