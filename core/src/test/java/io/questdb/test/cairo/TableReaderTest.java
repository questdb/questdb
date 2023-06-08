/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.cairo;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.griffin.SqlException;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class TableReaderTest extends AbstractCairoTest {
    public static final int DO_NOT_CARE = 0;
    public static final int MUST_NOT_SWITCH = 2;
    public static final int MUST_SWITCH = 1;
    private static final RecordAssert BATCH2_BEFORE_ASSERTER = (r, rnd, ts, blob) -> assertNullStr(r, 11);
    private static final RecordAssert BATCH3_BEFORE_ASSERTER = (r, rnd, ts, blob) -> Assert.assertEquals(Numbers.INT_NaN, r.getInt(12));
    private static final RecordAssert BATCH4_BEFORE_ASSERTER = (r, rnd, ts, blob) -> {
        Assert.assertEquals(0, r.getShort(13));
        Assert.assertFalse(r.getBool(14));
        Assert.assertEquals(0, r.getByte(15));
        Assert.assertTrue(Float.isNaN(r.getFloat(16)));
        Assert.assertTrue(Double.isNaN(r.getDouble(17)));
        Assert.assertNull(r.getSym(18));
        Assert.assertEquals(Numbers.LONG_NaN, r.getLong(19));
        Assert.assertEquals(Numbers.LONG_NaN, r.getDate(20));
        Assert.assertNull(r.getBin(21));
        Assert.assertEquals(TableUtils.NULL_LEN, r.getBinLen(21));
    };
    private static final RecordAssert BATCH5_BEFORE_ASSERTER = (r, rnd, ts, blob) -> {
        Assert.assertEquals(0, r.getShort(13));
        Assert.assertFalse(r.getBool(14));
        Assert.assertEquals(0, r.getByte(15));
        Assert.assertTrue(Float.isNaN(r.getFloat(16)));
        Assert.assertTrue(Double.isNaN(r.getDouble(17)));
        Assert.assertNull(r.getSym(18));
        Assert.assertEquals(Numbers.LONG_NaN, r.getLong(19));
        Assert.assertEquals(Numbers.LONG_NaN, r.getDate(20));
    };
    private static final RecordAssert BATCH_2_7_BEFORE_ASSERTER = (r, rnd, ts, blob) -> assertNullStr(r, 10);
    private static final RecordAssert BATCH_2_9_BEFORE_ASSERTER = (r, rnd, ts, blob) -> assertNullStr(r, 9);
    private static final RecordAssert BATCH_3_7_BEFORE_ASSERTER = (r, rnd, ts, blob) -> Assert.assertEquals(Numbers.INT_NaN, r.getInt(11));
    private static final RecordAssert BATCH_3_9_BEFORE_ASSERTER = (r, rnd, ts, blob) -> Assert.assertEquals(Numbers.INT_NaN, r.getInt(10));
    private static final RecordAssert BATCH_4_7_BEFORE_ASSERTER = (r, rnd, ts, blob) -> {
        Assert.assertEquals(0, r.getShort(12));
        Assert.assertFalse(r.getBool(13));
        Assert.assertEquals(0, r.getByte(14));
        Assert.assertTrue(Float.isNaN(r.getFloat(15)));
        Assert.assertTrue(Double.isNaN(r.getDouble(16)));
        Assert.assertNull(r.getSym(17));
        Assert.assertEquals(Numbers.LONG_NaN, r.getLong(18));
        Assert.assertEquals(Numbers.LONG_NaN, r.getDate(19));
    };
    private static final RecordAssert BATCH_4_9_BEFORE_ASSERTER = (r, rnd, ts, blob) -> {
        Assert.assertEquals(0, r.getShort(11));
        Assert.assertFalse(r.getBool(12));
        Assert.assertEquals(0, r.getByte(13));
        Assert.assertTrue(Float.isNaN(r.getFloat(14)));
        Assert.assertTrue(Double.isNaN(r.getDouble(15)));
        Assert.assertNull(r.getSym(16));
        Assert.assertEquals(Numbers.LONG_NaN, r.getLong(17));
        Assert.assertEquals(Numbers.LONG_NaN, r.getDate(18));
    };
    private static final int CANNOT_DELETE = -1;
    private static final int blobLen = 64 * 1024;
    private static final RecordAssert BATCH1_ASSERTER = (r, exp, ts, blob) -> {
        if (exp.nextBoolean()) {
            Assert.assertEquals(exp.nextByte(), r.getByte(2));
        } else {
            Assert.assertEquals(0, r.getByte(2));
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

        if (exp.nextBoolean()) {
            TestUtils.assertEquals(exp.nextChars(7), r.getSym(7));
        } else {
            Assert.assertNull(r.getSym(7));
        }
    };
    private static final RecordAssert BATCH2_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH1_ASSERTER.assertRecord(r, rnd, ts, blob);
        if ((rnd.nextPositiveInt() & 3) == 0) {
            assertStrColumn(rnd.nextChars(15), r, 11);
        }
    };
    private static final RecordAssert BATCH3_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH2_ASSERTER.assertRecord(r, rnd, ts, blob);

        if ((rnd.nextPositiveInt() & 3) == 0) {
            Assert.assertEquals(rnd.nextInt(), r.getInt(12));
        }
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
            Assert.assertEquals(rnd.nextByte(), r.getByte(15));
        } else {
            Assert.assertEquals(0, r.getByte(15));
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
            TestUtils.assertEquals(rnd.nextChars(10), r.getSym(18));
        } else {
            Assert.assertNull(r.getSym(18));
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
            Assert.assertEquals(rnd.nextByte(), r.getByte(15));
        } else {
            Assert.assertEquals(0, r.getByte(15));
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
            TestUtils.assertEquals(rnd.nextChars(10), r.getSym(18));
        } else {
            Assert.assertNull(r.getSym(18));
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
    private static final RecordAssert BATCH5_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH6_ASSERTER.assertRecord(r, rnd, ts, blob);

        // generate blob to roll forward random generator, don't assert blob value
        if (rnd.nextBoolean()) {
            rnd.nextChars(blob, blobLen / 2);
        }
    };
    private static final RecordAssert BATCH1_7_ASSERTER = (r, exp, ts, blob) -> {
        if (exp.nextBoolean()) {
            Assert.assertEquals(exp.nextByte(), r.getByte(1));
        } else {
            Assert.assertEquals(0, r.getByte(1));
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

        if (exp.nextBoolean()) {
            TestUtils.assertEquals(exp.nextChars(7), r.getSym(6));
        } else {
            Assert.assertNull(r.getSym(6));
        }

        Assert.assertEquals(Numbers.INT_NaN, r.getInt(20));
    };
    private static final RecordAssert BATCH2_7_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH1_7_ASSERTER.assertRecord(r, rnd, ts, blob);
        if ((rnd.nextPositiveInt() & 3) == 0) {
            assertStrColumn(rnd.nextChars(15), r, 10);
        }
    };
    private static final RecordAssert BATCH3_7_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH2_7_ASSERTER.assertRecord(r, rnd, ts, blob);

        if ((rnd.nextPositiveInt() & 3) == 0) {
            Assert.assertEquals(rnd.nextInt(), r.getInt(11));
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
            Assert.assertEquals(rnd.nextByte(), r.getByte(14));
        } else {
            Assert.assertEquals(0, r.getByte(14));
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
            TestUtils.assertEquals(rnd.nextChars(10), r.getSym(17));
        } else {
            Assert.assertNull(r.getSym(17));
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
    private static final RecordAssert BATCH1_9_ASSERTER = (r, exp, ts, blob) -> {
        if (exp.nextBoolean()) {
            Assert.assertEquals(exp.nextByte(), r.getByte(1));
        } else {
            Assert.assertEquals(0, r.getByte(1));
        }

        if (exp.nextBoolean()) {
            Assert.assertEquals(exp.nextBoolean(), r.getBool(6));
        } else {
            Assert.assertFalse(r.getBool(6));
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
            Assert.assertEquals(ts, r.getDate(8));
        } else {
            Assert.assertEquals(Numbers.LONG_NaN, r.getDate(8));
        }

        assertBin(r, exp, blob, 7);

        if (exp.nextBoolean()) {
            assertStrColumn(exp.nextChars(10), r, 5);
        } else {
            assertNullStr(r, 5);
        }

        // exercise random generator for column we removed
        if (exp.nextBoolean()) {
            exp.nextChars(7);
        }
        Assert.assertEquals(Numbers.INT_NaN, r.getInt(19));
    };
    private static final RecordAssert BATCH2_9_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH1_9_ASSERTER.assertRecord(r, rnd, ts, blob);
        if ((rnd.nextPositiveInt() & 3) == 0) {
            assertStrColumn(rnd.nextChars(15), r, 9);
        }
    };
    private static final RecordAssert BATCH3_9_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH2_9_ASSERTER.assertRecord(r, rnd, ts, blob);

        if ((rnd.nextPositiveInt() & 3) == 0) {
            Assert.assertEquals(rnd.nextInt(), r.getInt(10));
        }
    };
    private static final RecordAssert BATCH6_9_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH3_9_ASSERTER.assertRecord(r, rnd, ts, blob);

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextShort(), r.getShort(11));
        } else {
            Assert.assertEquals(0, r.getShort(11));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextBoolean(), r.getBool(12));
        } else {
            Assert.assertFalse(r.getBool(12));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextByte(), r.getByte(13));
        } else {
            Assert.assertEquals(0, r.getByte(13));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextFloat(), r.getFloat(14), 0.00000001f);
        } else {
            Assert.assertTrue(Float.isNaN(r.getFloat(14)));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextDouble(), r.getDouble(15), 0.0000001d);
        } else {
            Assert.assertTrue(Double.isNaN(r.getDouble(15)));
        }

        if (rnd.nextBoolean()) {
            TestUtils.assertEquals(rnd.nextChars(10), r.getSym(16));
        } else {
            Assert.assertNull(r.getSym(16));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getLong(17));
        } else {
            Assert.assertEquals(Numbers.LONG_NaN, r.getLong(17));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getDate(18));
        } else {
            Assert.assertEquals(Numbers.LONG_NaN, r.getDate(18));
        }
    };
    private static final RecordAssert BATCH5_9_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH6_9_ASSERTER.assertRecord(r, rnd, ts, blob);

        // generate blob to roll forward random generator, don't assert blob value
        if (rnd.nextBoolean()) {
            rnd.nextChars(blob, blobLen / 2);
        }
    };
    private static final FieldGenerator BATCH1_GENERATOR = (r1, rnd1, ts1, blob1) -> {
        if (rnd1.nextBoolean()) {
            r1.putByte(2, rnd1.nextByte());
        }

        if (rnd1.nextBoolean()) {
            r1.putBool(8, rnd1.nextBoolean());
        }

        if (rnd1.nextBoolean()) {
            r1.putShort(1, rnd1.nextShort());
        }

        if (rnd1.nextBoolean()) {
            r1.putInt(0, rnd1.nextInt());
        }

        if (rnd1.nextBoolean()) {
            r1.putDouble(3, rnd1.nextDouble());
        }

        if (rnd1.nextBoolean()) {
            r1.putFloat(4, rnd1.nextFloat());
        }

        if (rnd1.nextBoolean()) {
            r1.putLong(5, rnd1.nextLong());
        }

        if (rnd1.nextBoolean()) {
            r1.putDate(10, ts1);
        }

        if (rnd1.nextBoolean()) {
            rnd1.nextChars(blob1, blobLen / 2);
            r1.putBin(9, blob1, blobLen);
        }

        if (rnd1.nextBoolean()) {
            r1.putStr(6, rnd1.nextChars(10));
        }

        if (rnd1.nextBoolean()) {
            r1.putSym(7, rnd1.nextChars(7));
        }
    };
    private static final FieldGenerator BATCH2_GENERATOR = (r1, rnd1, ts1, blob1) -> {
        BATCH1_GENERATOR.generate(r1, rnd1, ts1, blob1);

        if ((rnd1.nextPositiveInt() & 3) == 0) {
            r1.putStr(11, rnd1.nextChars(15));
        }
    };
    private static final FieldGenerator BATCH3_GENERATOR = (r1, rnd1, ts1, blob1) -> {
        BATCH2_GENERATOR.generate(r1, rnd1, ts1, blob1);

        if ((rnd1.nextPositiveInt() & 3) == 0) {
            r1.putInt(12, rnd1.nextInt());
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
            r.putSym(18, rnd.nextChars(10));
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
            r.putSym(18, rnd.nextChars(10));
        }

        if (rnd.nextBoolean()) {
            r.putLong(19, rnd.nextLong());
        }

        if (rnd.nextBoolean()) {
            r.putDate(20, rnd.nextLong());
        }
    };
    private static final RecordAssert BATCH8_ASSERTER = (r, rnd, ts, blob) -> {
        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextByte(), r.getByte(1));
        } else {
            Assert.assertEquals(0, r.getByte(1));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextBoolean(), r.getBool(7));
        } else {
            Assert.assertFalse(r.getBool(7));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextShort(), r.getShort(0));
        } else {
            Assert.assertEquals(0, r.getShort(0));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextDouble(), r.getDouble(2), 0.0000001d);
        } else {
            Assert.assertTrue(Double.isNaN(r.getDouble(2)));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextFloat(), r.getFloat(3), 0.000001f);
        } else {
            Assert.assertTrue(Float.isNaN(r.getFloat(3)));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getLong(4));
        } else {
            Assert.assertEquals(Numbers.LONG_NaN, r.getLong(4));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(ts, r.getDate(9));
        } else {
            Assert.assertEquals(Numbers.LONG_NaN, r.getDate(9));
        }

        assertBin(r, rnd, blob, 8);

        if (rnd.nextBoolean()) {
            assertStrColumn(rnd.nextChars(10), r, 5);
        } else {
            assertNullStr(r, 5);
        }

        if (rnd.nextBoolean()) {
            TestUtils.assertEquals(rnd.nextChars(7), r.getSym(6));
        } else {
            Assert.assertNull(r.getSym(6));
        }

        if ((rnd.nextPositiveInt() & 3) == 0) {
            assertStrColumn(rnd.nextChars(15), r, 10);
        } else {
            assertNullStr(r, 10);
        }

        if ((rnd.nextPositiveInt() & 3) == 0) {
            Assert.assertEquals(rnd.nextInt(), r.getInt(11));
        } else {
            Assert.assertEquals(Numbers.INT_NaN, r.getInt(11));
        }

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
            Assert.assertEquals(rnd.nextByte(), r.getByte(14));
        } else {
            Assert.assertEquals(0, r.getByte(14));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextFloat(), r.getFloat(15), 0.000001f);
        } else {
            Assert.assertTrue(Float.isNaN(r.getFloat(15)));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextDouble(), r.getDouble(16), 0.0000001d);
        } else {
            Assert.assertTrue(Double.isNaN(r.getDouble(16)));
        }

        if (rnd.nextBoolean()) {
            TestUtils.assertEquals(rnd.nextChars(10), r.getSym(17));
        } else {
            Assert.assertNull(r.getSym(17));
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

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextInt(), r.getInt(20));
        } else {
            Assert.assertEquals(Numbers.INT_NaN, r.getInt(20));
        }
    };
    private static final RecordAssert BATCH8_9_ASSERTER = (r, rnd, ts, blob) -> {
        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextByte(), r.getByte(1));
        } else {
            Assert.assertEquals(0, r.getByte(1));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextBoolean(), r.getBool(6));
        } else {
            Assert.assertFalse(r.getBool(6));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextShort(), r.getShort(0));
        } else {
            Assert.assertEquals(0, r.getShort(0));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextDouble(), r.getDouble(2), 0.0000001d);
        } else {
            Assert.assertTrue(Double.isNaN(r.getDouble(2)));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextFloat(), r.getFloat(3), 0.000001f);
        } else {
            Assert.assertTrue(Float.isNaN(r.getFloat(3)));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getLong(4));
        } else {
            Assert.assertEquals(Numbers.LONG_NaN, r.getLong(4));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(ts, r.getDate(8));
        } else {
            Assert.assertEquals(Numbers.LONG_NaN, r.getDate(8));
        }

        assertBin(r, rnd, blob, 7);

        if (rnd.nextBoolean()) {
            assertStrColumn(rnd.nextChars(10), r, 5);
        } else {
            assertNullStr(r, 5);
        }

        if (rnd.nextBoolean()) {
            rnd.nextChars(7);
        }

        if ((rnd.nextPositiveInt() & 3) == 0) {
            assertStrColumn(rnd.nextChars(15), r, 9);
        } else {
            assertNullStr(r, 9);
        }

        if ((rnd.nextPositiveInt() & 3) == 0) {
            Assert.assertEquals(rnd.nextInt(), r.getInt(10));
        } else {
            Assert.assertEquals(Numbers.INT_NaN, r.getInt(10));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextShort(), r.getShort(11));
        } else {
            Assert.assertEquals(0, r.getShort(11));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextBoolean(), r.getBool(12));
        } else {
            Assert.assertFalse(r.getBool(12));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextByte(), r.getByte(13));
        } else {
            Assert.assertEquals(0, r.getByte(13));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextFloat(), r.getFloat(14), 0.000001f);
        } else {
            Assert.assertTrue(Float.isNaN(r.getFloat(14)));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextDouble(), r.getDouble(15), 0.0000001d);
        } else {
            Assert.assertTrue(Double.isNaN(r.getDouble(15)));
        }

        if (rnd.nextBoolean()) {
            TestUtils.assertEquals(rnd.nextChars(10), r.getSym(16));
        } else {
            Assert.assertNull(r.getSym(16));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getLong(17));
        } else {
            Assert.assertEquals(Numbers.LONG_NaN, r.getLong(17));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getDate(18));
        } else {
            Assert.assertEquals(Numbers.LONG_NaN, r.getDate(18));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextInt(), r.getInt(19));
        } else {
            Assert.assertEquals(Numbers.INT_NaN, r.getInt(19));
        }

        Assert.assertNull(r.getSym(20));
    };
    private static final RecordAssert BATCH9_ASSERTER = (r, rnd, ts, blob) -> {
        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextByte(), r.getByte(1));
        } else {
            Assert.assertEquals(0, r.getByte(1));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextBoolean(), r.getBool(6));
        } else {
            Assert.assertFalse(r.getBool(6));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextShort(), r.getShort(0));
        } else {
            Assert.assertEquals(0, r.getShort(0));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextDouble(), r.getDouble(2), 0.0000001d);
        } else {
            Assert.assertTrue(Double.isNaN(r.getDouble(2)));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextFloat(), r.getFloat(3), 0.000001f);
        } else {
            Assert.assertTrue(Float.isNaN(r.getFloat(3)));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getLong(4));
        } else {
            Assert.assertEquals(Numbers.LONG_NaN, r.getLong(4));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(ts, r.getDate(8));
        } else {
            Assert.assertEquals(Numbers.LONG_NaN, r.getDate(8));
        }

        assertBin(r, rnd, blob, 7);

        if (rnd.nextBoolean()) {
            assertStrColumn(rnd.nextChars(10), r, 5);
        } else {
            assertNullStr(r, 5);
        }

        if ((rnd.nextPositiveInt() & 3) == 0) {
            assertStrColumn(rnd.nextChars(15), r, 9);
        } else {
            assertNullStr(r, 9);
        }

        if ((rnd.nextPositiveInt() & 3) == 0) {
            Assert.assertEquals(rnd.nextInt(), r.getInt(10));
        } else {
            Assert.assertEquals(Numbers.INT_NaN, r.getInt(10));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextShort(), r.getShort(11));
        } else {
            Assert.assertEquals(0, r.getShort(11));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextBoolean(), r.getBool(12));
        } else {
            Assert.assertFalse(r.getBool(12));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextByte(), r.getByte(13));
        } else {
            Assert.assertEquals(0, r.getByte(13));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextFloat(), r.getFloat(14), 0.000001f);
        } else {
            Assert.assertTrue(Float.isNaN(r.getFloat(14)));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextDouble(), r.getDouble(15), 0.0000001d);
        } else {
            Assert.assertTrue(Double.isNaN(r.getDouble(15)));
        }

        if (rnd.nextBoolean()) {
            TestUtils.assertEquals(rnd.nextChars(10), r.getSym(16));
        } else {
            Assert.assertNull(r.getSym(16));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getLong(17));
        } else {
            Assert.assertEquals(Numbers.LONG_NaN, r.getLong(17));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getDate(18));
        } else {
            Assert.assertEquals(Numbers.LONG_NaN, r.getDate(18));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextInt(), r.getInt(19));
        } else {
            Assert.assertEquals(Numbers.INT_NaN, r.getInt(19));
        }

        if (rnd.nextBoolean()) {
            TestUtils.assertEquals(rnd.nextChars(8), r.getSym(20));
        } else {
            Assert.assertNull(r.getSym(20));
        }
    };
    private static final FieldGenerator BATCH8_GENERATOR = (r, rnd, ts, blob) -> {
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

        if (rnd.nextBoolean()) {
            r.putSym(7, rnd.nextChars(7));
        }

        if ((rnd.nextPositiveInt() & 3) == 0) {
            r.putStr(11, rnd.nextChars(15));
        }

        if ((rnd.nextPositiveInt() & 3) == 0) {
            r.putInt(12, rnd.nextInt());
        }

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
            r.putSym(18, rnd.nextChars(10));
        }

        if (rnd.nextBoolean()) {
            r.putLong(19, rnd.nextLong());
        }

        if (rnd.nextBoolean()) {
            r.putDate(20, rnd.nextLong());
        }

        if (rnd.nextBoolean()) {
            r.putInt(22, rnd.nextInt());
        }
    };
    private static final FieldGenerator BATCH9_GENERATOR = (r, rnd, ts, blob) -> {
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


        if ((rnd.nextPositiveInt() & 3) == 0) {
            r.putStr(11, rnd.nextChars(15));
        }

        if ((rnd.nextPositiveInt() & 3) == 0) {
            r.putInt(12, rnd.nextInt());
        }

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
            r.putSym(18, rnd.nextChars(10));
        }

        if (rnd.nextBoolean()) {
            r.putLong(19, rnd.nextLong());
        }

        if (rnd.nextBoolean()) {
            r.putDate(20, rnd.nextLong());
        }

        if (rnd.nextBoolean()) {
            r.putInt(22, rnd.nextInt());
        }

        if (rnd.nextBoolean()) {
            r.putSym(23, rnd.nextChars(8));
        }
    };

    @Test
    public void testAddColumnConcurrentWithDataUpdates() throws Throwable {
        ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
        assertMemoryLeak(() -> {
            CyclicBarrier start = new CyclicBarrier(2);
            AtomicInteger done = new AtomicInteger();
            AtomicInteger columnsAdded = new AtomicInteger();
            AtomicInteger reloadCount = new AtomicInteger();
            int totalColAddCount = Os.type == Os.LINUX_ARM64 || Os.type == Os.LINUX_AMD64 ? 100 : 10;

            String tableName = "tbl_meta_test";
            TableToken tableToken = createTable(tableName, PartitionBy.DAY);

            Thread writerThread = new Thread(() -> {
                try (TableWriter writer = getWriter(tableToken)) {
                    start.await();
                    for (int i = 0; i < totalColAddCount; i++) {
                        writer.addColumn("col" + i, ColumnType.SYMBOL);
                        columnsAdded.incrementAndGet();

                        TableWriter.Row row = writer.newRow(i * Timestamps.HOUR_MICROS);
                        row.append();

                        writer.commit();
                    }
                } catch (Throwable e) {
                    exceptions.add(e);
                    LOG.error().$(e).$();
                } finally {
                    done.incrementAndGet();
                }
            });

            Thread readerThread = new Thread(() -> {
                try (TableReader reader = getReader(tableToken)) {
                    start.await();
                    int colAdded = 0, newColsAdded;
                    while (colAdded < totalColAddCount) {
                        if (colAdded < (newColsAdded = columnsAdded.get())) {
                            reader.reload();
                            Assert.assertEquals(reader.getTxnMetadataVersion(), reader.getMetadata().getMetadataVersion());
                            colAdded = newColsAdded;
                            reloadCount.incrementAndGet();
                        }
                        Os.pause();
                    }
                } catch (Throwable e) {
                    exceptions.add(e);
                    LOG.error().$(e).$();
                }
            });
            writerThread.start();
            readerThread.start();

            writerThread.join();
            readerThread.join();

            if (exceptions.size() != 0) {
                for (Throwable ex : exceptions) {
                    ex.printStackTrace();
                }
                Assert.fail();
            }
            Assert.assertTrue(reloadCount.get() > 0);
            LOG.infoW().$("total reload count ").$(reloadCount.get()).$();
        });
    }

    @Test
    public void testAddColumnPartitionConcurrent() throws Throwable {
        ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
        assertMemoryLeak(() -> {
            CyclicBarrier start = new CyclicBarrier(2);
            AtomicInteger done = new AtomicInteger();
            AtomicInteger columnsAdded = new AtomicInteger();
            AtomicInteger reloadCount = new AtomicInteger();
            int totalColAddCount = 100;

            String tableName = "tbl_meta_test";
            TableToken tableToken = createTable(tableName, PartitionBy.HOUR);
            Rnd rnd = TestUtils.generateRandom(LOG);

            Thread writerThread = new Thread(() -> {
                try (TableWriter writer = getWriter(tableToken)) {
                    start.await();
                    for (int i = 0; i < totalColAddCount; i++) {
                        writer.addColumn("col" + i, ColumnType.SYMBOL);
                        columnsAdded.incrementAndGet();

                        if (rnd.nextBoolean()) {
                            // Add partition
                            TableWriter.Row row = writer.newRow(i * Timestamps.HOUR_MICROS);
                            row.append();
                            writer.commit();
                        }

                        if (rnd.nextBoolean() && writer.getPartitionCount() > 0) {
                            // Remove partition
                            int partitionNum = rnd.nextInt() % writer.getPartitionCount();
                            writer.removePartition(partitionNum * Timestamps.HOUR_MICROS);
                        }
                    }
                } catch (Throwable e) {
                    exceptions.add(e);
                    LOG.error().$(e).$();
                } finally {
                    done.incrementAndGet();
                }
            });

            Thread readerThread = new Thread(() -> {
                try (TableReader reader = getReader(tableToken)) {
                    start.await();
                    int colAdded = -1;
                    while (colAdded < totalColAddCount) {
                        if (colAdded < columnsAdded.get()) {
                            if (reader.reload()) {
                                Assert.assertEquals(reader.getTxnMetadataVersion(), reader.getMetadata().getMetadataVersion());
                                colAdded = reader.getMetadata().getColumnCount();
                                reloadCount.incrementAndGet();
                            }
                        }
                        Os.pause();
                    }
                } catch (Throwable e) {
                    exceptions.add(e);
                    LOG.error().$(e).$();
                }
            });
            writerThread.start();
            readerThread.start();

            writerThread.join();
            readerThread.join();

            if (exceptions.size() != 0) {
                Throwable ex = exceptions.poll();
                ex.printStackTrace();
                throw new Exception(ex);
            }
            LOG.infoW().$("total reload count ").$(reloadCount.get()).$();
        });
    }

    @Test
    public void testAddColumnPartitionConcurrentCreateReader() throws Throwable {
        ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
        assertMemoryLeak(() -> {
            CyclicBarrier start = new CyclicBarrier(2);
            AtomicInteger done = new AtomicInteger();
            AtomicInteger columnsAdded = new AtomicInteger();
            AtomicInteger reloadCount = new AtomicInteger();
            int totalColAddCount = Os.type == Os.LINUX_AMD64 || Os.type == Os.LINUX_ARM64 ? 500 : 50;

            String tableName = "tbl_meta_test";
            TableToken tableToken = createTable(tableName, PartitionBy.HOUR);
            Rnd rnd = TestUtils.generateRandom(LOG);

            Thread writerThread = new Thread(() -> {
                try (TableWriter writer = getWriter(tableToken)) {
                    start.await();
                    for (int i = 0; i < totalColAddCount; i++) {
                        writer.addColumn("col" + i, ColumnType.SYMBOL);
                        columnsAdded.incrementAndGet();

                        if (rnd.nextBoolean()) {
                            // Add partition
                            TableWriter.Row row = writer.newRow(i * Timestamps.HOUR_MICROS);
                            row.append();
                            writer.commit();
                        }

                        if (rnd.nextBoolean() && writer.getPartitionCount() > 0) {
                            // Remove partition
                            int partitionNum = rnd.nextInt() % writer.getPartitionCount();
                            writer.removePartition(partitionNum * Timestamps.HOUR_MICROS);
                        }
                    }
                } catch (Throwable e) {
                    exceptions.add(e);
                    LOG.error().$(e).$();
                } finally {
                    done.incrementAndGet();
                }
            });

            Thread readerThread = new Thread(() -> {
                try {
                    start.await();
                    int colAdded = -1, newColsAdded;
                    while (colAdded < totalColAddCount) {
                        if (colAdded < (newColsAdded = columnsAdded.get())) {
                            try (TableReader reader = getReader(tableToken)) {
                                Assert.assertEquals(reader.getTxnMetadataVersion(), reader.getMetadata().getMetadataVersion());
                                colAdded = newColsAdded;
                                reloadCount.incrementAndGet();
                            }
                            engine.releaseAllReaders();
                        }
                        Os.pause();
                    }
                } catch (Throwable e) {
                    exceptions.add(e);
                    LOG.error().$(e).$();
                }
            });
            writerThread.start();
            readerThread.start();

            writerThread.join();
            readerThread.join();
            Assert.assertTrue(reloadCount.get() > totalColAddCount / 10);
            LOG.infoW().$("total reload count ").$(reloadCount.get()).$();
        });

        if (exceptions.size() != 0) {
            throw exceptions.poll();
        }
    }

    @Test
    public void testAppendNullTimestamp() throws Exception {
        try (TableModel model = new TableModel(configuration, "all", PartitionBy.NONE)
                .col("int", ColumnType.INT)
//                .timestamp("t") // cannot insert null as a timestamp on designated columns
        ) {
            CreateTableTestUtils.createTableWithVersionAndId(model, engine, ColumnType.VERSION, 1);

            TestUtils.assertMemoryLeak(() -> {
                try (TableWriter w = newTableWriter(configuration, "all", metrics)) {
                    TableWriter.Row r = w.newRow(Numbers.LONG_NaN);
                    r.putInt(0, 100);
                    r.append();
                    w.commit();

                    Assert.assertEquals(1, w.size());
                }

                try (TableReader reader = newTableReader(configuration, "all")) {
                    Assert.assertEquals(1, reader.getPartitionCount());
                    Assert.assertEquals(1, reader.openPartition(0));
                }
            });
        }
    }

    @Test
    public void testAsyncRemoveAndReloadSymRetried() throws Exception {
        AtomicInteger counterRef = new AtomicInteger(CANNOT_DELETE);
        String columnName = "b";
        String suffix = "";
        TestFilesFacade ff = new TestFilesFacade() {
            @Override
            public int called() {
                return counterRef.get();
            }

            @Override
            public boolean remove(LPSZ name) {
                if (Chars.endsWith(name, columnName + ".v" + suffix)) {
                    if (counterRef.get() == CANNOT_DELETE) {
                        return false;
                    }
                    counterRef.incrementAndGet();
                }
                return super.remove(name);
            }

            @Override
            public boolean wasCalled() {
                return counterRef.get() > 0;
            }
        };

        assertMemoryLeak(ff, () -> {

            // create table with two string columns
            try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                    .col("a", ColumnType.SYMBOL)
                    .col("b", ColumnType.SYMBOL)) {
                CreateTableTestUtils.create(model);
            }

            Rnd rnd = new Rnd();
            final int N = 1000;

            // populate table and delete column
            try (TableWriter writer = getWriter("x")) {
                appendTwoSymbols(writer, rnd, 1);
                writer.commit();

                try (TableReader reader = newTableReader(configuration, "x")) {
                    long counter = 0;

                    rnd.reset();
                    RecordCursor cursor = reader.getCursor();
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        Assert.assertEquals(rnd.nextChars(10), record.getSym(0));
                        Assert.assertEquals(rnd.nextChars(15), record.getSym(1));
                        counter++;
                    }

                    Assert.assertEquals(N, counter);

                    // this should write metadata without column "b" but will ignore
                    // file delete failures
                    writer.removeColumn("b");

                    // now when we add new column by same name it must not pick up files we failed to delete previously
                    writer.addColumn("b", ColumnType.SYMBOL);

                    // SymbolMap must be cleared when we try to do add values to new column
                    appendTwoSymbols(writer, rnd, 2);
                    writer.commit();

                    // now assert what reader sees
                    Assert.assertTrue(reader.reload());
                    Assert.assertEquals(N * 2, reader.size());

                    rnd.reset();
                    cursor.toTop();
                    counter = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(rnd.nextChars(10), record.getSym(0));
                        if (counter < N) {
                            // roll random generator to make sure it returns same values
                            rnd.nextChars(15);
                            Assert.assertNull(record.getSym(1));
                        } else {
                            Assert.assertEquals(rnd.nextChars(15), record.getSym(1));
                        }
                        counter++;
                    }

                    Assert.assertEquals(N * 2, counter);
                }
            }

            Assert.assertFalse(ff.wasCalled());
            try (ColumnPurgeJob job = new ColumnPurgeJob(engine, null)) {
                job.run(0);
            }

            checkColumnPurgeRemovesFiles(counterRef, ff, 1);
        });
    }

    @Test
    public void testAsyncRemoveLastSym() throws Exception {
        AtomicInteger counterRef = new AtomicInteger(CANNOT_DELETE);
        TestFilesFacade ff = createColumnDeleteCounterFileFacade(counterRef, "b", "");

        // create table with two string columns
        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("a", ColumnType.SYMBOL)
                .col("b", ColumnType.SYMBOL)) {
            CreateTableTestUtils.create(model);
        }

        testRemoveSymbol(counterRef, ff, "b", 2);
    }

    @Test
    public void testAsyncRemoveLastSymWithColTop() throws Exception {
        AtomicInteger counterRef = new AtomicInteger(CANNOT_DELETE);
        TestFilesFacade ff = createColumnDeleteCounterFileFacade(counterRef, "c", ".0");

        // create table with two string columns
        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("a", ColumnType.SYMBOL)
                .col("b", ColumnType.SYMBOL)) {
            CreateTableTestUtils.create(model);
        }

        try (TableWriter tw = getWriter("x")) {
            tw.addColumn("c", ColumnType.SYMBOL);
        }
        engine.releaseInactive();
        testRemoveSymbol(counterRef, ff, "c", 6);
    }

    @Test
    public void testAsyncSymbolRename() throws Exception {
        AtomicInteger counterRef = new AtomicInteger(CANNOT_DELETE);
        TestFilesFacade ff = createColumnDeleteCounterFileFacade(counterRef, "b", "");

        // create table with two string columns
        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("a", ColumnType.SYMBOL)
                .col("b", ColumnType.SYMBOL)) {
            CreateTableTestUtils.create(model);
        }

        testAsyncColumnRename(counterRef, ff, "b");
    }

    @Test
    public void testAsyncSymbolRenameWithNameVersion() throws Exception {
        AtomicInteger counterRef = new AtomicInteger(CANNOT_DELETE);
        TestFilesFacade ff = createColumnDeleteCounterFileFacade(counterRef, "c", ".0");

        // create table with two string columns
        try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("a", ColumnType.SYMBOL)
                .col("b", ColumnType.SYMBOL)) {
            CreateTableTestUtils.create(model);
        }
        try (TableWriter tw = getWriter("x")) {
            tw.addColumn("c", ColumnType.SYMBOL);
        }
        engine.releaseInactive();

        testAsyncColumnRename(counterRef, ff, "c");
    }

    @Test
    public void testCharAsString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {

            try (TableModel model = new TableModel(
                    configuration,
                    "char_test",
                    PartitionBy.NONE
            ).col("cc", ColumnType.STRING)) {
                CreateTableTestUtils.create(model);
            }
            char[] data = {'a', 'b', 'f', 'g'};
            try (TableWriter writer = newTableWriter(configuration, "char_test", metrics)) {

                for (int i = 0, n = data.length; i < n; i++) {
                    TableWriter.Row r = writer.newRow();
                    r.putStr(0, data[i]);
                    r.append();
                }
                writer.commit();
            }

            try (TableReader reader = newTableReader(configuration, "char_test")) {
                final RecordCursor cursor = reader.getCursor();
                final Record record = cursor.getRecord();
                int index = 0;
                while (cursor.hasNext()) {
                    Assert.assertTrue(index < data.length);
                    CharSequence value = record.getStr(0);
                    Assert.assertEquals(1, value.length());
                    Assert.assertEquals(data[index], value.charAt(0));
                    index++;
                }
            }
        });
    }

    @Test
    public void testConcurrentReloadByDay() throws Exception {
        testConcurrentReloadSinglePartition(PartitionBy.DAY);
    }

    @Test
    // flapping test
    public void testConcurrentReloadMultipleByDay() throws Exception {
        testConcurrentReloadMultiplePartitions(PartitionBy.DAY, 100000);
    }

    @Test
    public void testConcurrentReloadMultipleByMonth() throws Exception {
        testConcurrentReloadMultiplePartitions(PartitionBy.MONTH, 3000000);
    }

    @Test
    public void testConcurrentReloadMultipleByWeek() throws Exception {
        testConcurrentReloadMultiplePartitions(PartitionBy.WEEK, 700000);
    }

    @Test
    public void testConcurrentReloadMultipleByYear() throws Exception {
        testConcurrentReloadMultiplePartitions(PartitionBy.MONTH, 12 * 3000000);
    }

    @Test
    public void testConcurrentReloadNonPartitioned() throws Exception {
        testConcurrentReloadSinglePartition(PartitionBy.NONE);
    }

    public void testConcurrentReloadSinglePartition(int partitionBy) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // model data
            LongList list = new LongList();
            final int N = 1024;
            final int scale = 10000;
            for (int i = 0; i < N; i++) {
                list.add(i);
            }

            // model table
            try (TableModel model = new TableModel(configuration, "w", partitionBy).col("l", ColumnType.LONG)) {
                CreateTableTestUtils.create(model);
            }

            final int threads = 2;
            final CyclicBarrier startBarrier = new CyclicBarrier(threads);
            final SOCountDownLatch stopLatch = new SOCountDownLatch(threads);
            final AtomicInteger errors = new AtomicInteger(0);

            // start writer
            new Thread(() -> {
                try {
                    startBarrier.await();
                    try (TableWriter writer = newTableWriter(configuration, "w", metrics)) {
                        for (int i = 0; i < N * scale; i++) {
                            TableWriter.Row row = writer.newRow();
                            row.putLong(0, list.getQuick(i % N));
                            row.append();
                            writer.commit();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    errors.incrementAndGet();
                } finally {
                    stopLatch.countDown();
                }
            }).start();

            // start reader
            new Thread(() -> {
                try {
                    startBarrier.await();
                    try (TableReader reader = newTableReader(configuration, "w")) {
                        RecordCursor cursor = reader.getCursor();
                        final Record record = cursor.getRecord();
                        do {
                            // we deliberately ignore result of reload()
                            // to create more race conditions
                            reader.reload();
                            cursor.toTop();
                            int count = 0;
                            while (cursor.hasNext()) {
                                Assert.assertEquals(list.get(count++ % N), record.getLong(0));
                            }

                            if (count == N * scale) {
                                break;
                            }
                        } while (true);
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    errors.incrementAndGet();
                } finally {
                    stopLatch.countDown();
                }
            }).start();

            stopLatch.await();
            Assert.assertEquals(0, errors.get());
        });
    }

    @Test
    public void testLong256WriterReopen() throws Exception {
        // we had a bug where size of LONG256 column was incorrectly defined
        // this caused TableWriter to incorrectly calculate append position in constructor
        // subsequent records would have been appended to far away from records from first writer instance
        // and table reader would not be able to read data consistently
        TestUtils.assertMemoryLeak(() -> {
            // create table with two string columns
            try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("a", ColumnType.LONG256)) {
                CreateTableTestUtils.create(model);
            }

            try (TableWriter w = newTableWriter(configuration, "x", metrics)) {
                TableWriter.Row r = w.newRow();
                r.putLong256(0, 1, 2, 3, 4);
                r.append();
                w.commit();
            }

            try (TableWriter w = newTableWriter(configuration, "x", metrics)) {
                TableWriter.Row r = w.newRow();
                r.putLong256(0, 5, 6, 7, 8);
                r.append();
                w.commit();
            }

            try (TableReader r = newTableReader(configuration, "x")) {
                sink.clear();
                printer.print(r.getCursor(), r.getMetadata(), true, sink);
            }

            TestUtils.assertEquals("a\n" +
                    "0x04000000000000000300000000000000020000000000000001\n" +
                    "0x08000000000000000700000000000000060000000000000005\n", sink);
        });
    }

    @Test
    public void testManySymbolReloadTest() throws Exception {
        String tableName = "testManySymbolReloadTest";
        TableToken tableToken = createTable(tableName, PartitionBy.HOUR);

        assertMemoryLeak(() -> {
            try (TableReader reader = getReader(tableToken)) {
                int partitionsToAdd = Os.type == Os.LINUX_ARM64 || Os.type == Os.LINUX_AMD64 ? (int) (Files.PAGE_SIZE / Long.BYTES / 4) + 1 : 10;
                try (TableWriter writer = getWriter(tableToken)) {
                    int symbolsToAdd = Os.type == Os.LINUX_ARM64 || Os.type == Os.LINUX_AMD64 ? (int) (Files.PAGE_SIZE / Long.BYTES / 4) + 1 : 10;
                    for (int i = 0; i < symbolsToAdd; i++) {
                        writer.addColumn("col" + i, ColumnType.SYMBOL);
                    }

                    for (int i = 0; i < partitionsToAdd; i++) {
                        writer.newRow(i * Timestamps.HOUR_MICROS).append();
                    }
                    writer.commit();
                }
                reader.reload();
                Assert.assertEquals(partitionsToAdd, reader.getPartitionCount());
            }
        });
    }

    @Test
    public void testMetadataFileDoesNotExist() throws Exception {
        String tableName = "testMetadataFileDoesNotExist";
        TableToken tableToken = createTable(tableName, PartitionBy.HOUR);
        spinLockTimeout = 10;
        AtomicInteger openCount = new AtomicInteger(1000);

        assertMemoryLeak(() -> {
            try (Path temp = new Path()) {
                temp.of(engine.getConfiguration().getRoot()).concat("dummy_non_existing_path").$();
                ff = new TestFilesFacadeImpl() {
                    @Override
                    public int openRO(LPSZ name) {
                        if (Chars.endsWith(name, TableUtils.META_FILE_NAME) && openCount.decrementAndGet() < 0) {
                            return TestFilesFacadeImpl.INSTANCE.openRO(temp);
                        }
                        return TestFilesFacadeImpl.INSTANCE.openRO(name);
                    }
                };

                try (TableReader reader = getReader(tableToken)) {
                    try (TableWriter writer = getWriter(tableToken)) {
                        writer.addColumn("col10", ColumnType.SYMBOL);
                    }
                    engine.releaseAllWriters();
                    try {
                        openCount.set(0);
                        reader.reload();
                        Assert.fail();
                    } catch (CairoException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "Metadata read timeout");
                    }
                }
            }
        });
    }

    @Test
    public void testMetadataFileDoesNotExist2() throws Exception {
        String tableName = "testMetadataFileDoesNotExist";
        TableToken tableToken = createTable(tableName, PartitionBy.HOUR);
        spinLockTimeout = 10;
        AtomicInteger openCount = new AtomicInteger(1000);

        assertMemoryLeak(() -> {
            try (Path temp = new Path()) {
                temp.of(engine.getConfiguration().getRoot()).concat("dummy_non_existing_path").$();
                ff = new TestFilesFacadeImpl() {

                    @Override
                    public long length(int fd) {
                        if (fd == this.fd) {
                            return Files.length(temp);
                        }
                        return Files.length(fd);
                    }

                    @Override
                    public long length(LPSZ name) {
                        if (Chars.endsWith(name, TableUtils.META_FILE_NAME) && openCount.decrementAndGet() < 0) {
                            return Files.length(temp);
                        }
                        return Files.length(name);
                    }

                    @Override
                    public int openRO(LPSZ name) {
                        if (Chars.endsWith(name, TableUtils.META_FILE_NAME) && openCount.decrementAndGet() < 0) {
                            return this.fd = TestFilesFacadeImpl.INSTANCE.openRO(name);
                        }
                        return TestFilesFacadeImpl.INSTANCE.openRO(name);
                    }
                };

                try (TableReader reader = getReader(tableToken)) {
                    try (TableWriter writer = getWriter(tableToken)) {
                        writer.addColumn("col10", ColumnType.SYMBOL);
                    }
                    engine.releaseAllWriters();
                    try {
                        openCount.set(0);
                        reader.reload();
                        Assert.fail();
                    } catch (CairoException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "Metadata read timeout");
                    }
                }
            }
        });
    }

    @Test
    public void testMetadataVersionDoesNotMatch() throws Exception {
        String tableName = "testMetadataVersionDoesNotMatch";
        TableToken tableToken = createTable(tableName, PartitionBy.HOUR);
        spinLockTimeout = 10;

        assertMemoryLeak(() -> {
            try (TableReader reader = getReader(tableToken)) {
                try (TableWriter writer = getWriter(tableToken)) {
                    writer.addColumn("col10", ColumnType.SYMBOL);
                }
                try (
                        Path path = getPath(tableName);
                        MemoryMARW mem = Vm.getMARWInstance(
                                TestFilesFacadeImpl.INSTANCE,
                                path,
                                -1,
                                Files.PAGE_SIZE,
                                MemoryTag.NATIVE_DEFAULT,
                                configuration.getWriterFileOpenOpts()
                        )
                ) {
                    mem.putLong(TableUtils.META_OFFSET_METADATA_VERSION, 0);
                }

                try {
                    reader.reload();
                    Assert.fail();
                } catch (CairoException ex) {
                    TestUtils.assertContains(ex.getFlyweightMessage(), "Metadata read timeout");
                }
            }

            engine.releaseAllReaders();
            try (TableReader ignored = getReader(tableToken)) {
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "Metadata read timeout");
            }
        });
    }

    @Test
    public void testNullValueRecovery() throws Exception {
        final String expected = "int\tshort\tbyte\tdouble\tfloat\tlong\tstr\tsym\tbool\tbin\tdate\n" +
                "NaN\t0\t0\tNaN\tNaN\tNaN\t\tabc\ttrue\t\t\n";

        TestUtils.assertMemoryLeak(() -> {
            CreateTableTestUtils.createAllTable(engine, PartitionBy.NONE);

            try (TableWriter w = newTableWriter(configuration, "all", metrics)) {
                TableWriter.Row r = w.newRow(1000000); // <-- higher timestamp
                r.putInt(0, 10);
                r.putByte(1, (byte) 56);
                r.putDouble(2, 4.3223);
                r.putStr(6, "xyz");
                r.cancel();

                r = w.newRow(100000); // <-- lower timestamp
                r.putSym(7, "abc");
                r.putBool(8, true);
                r.append();

                w.commit();
            }

            try (TableReader r = newTableReader(configuration, "all")) {
                sink.clear();
                printer.print(r.getCursor(), r.getMetadata(), true, sink);
                TestUtils.assertEquals(expected, sink);
            }
        });
    }

    @Test
    public void testOver2GFile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                    .col("a", ColumnType.LONG)) {
                CreateTableTestUtils.create(model);
            }

            long N = 280000000;
            Rnd rnd = new Rnd();
            try (TableWriter writer = newTableWriter(configuration, "x", metrics)) {
                for (int i = 0; i < N; i++) {
                    TableWriter.Row r = writer.newRow();
                    r.putLong(0, rnd.nextLong());
                    r.append();
                }
                writer.commit();
            }

            try (TableReader reader = newTableReader(configuration, "x")) {
                int count = 0;
                rnd.reset();
                RecordCursor cursor = reader.getCursor();
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    Assert.assertEquals(rnd.nextLong(), record.getLong(0));
                    count++;
                }
                Assert.assertEquals(N, count);
            }
        });
    }

    @Test
    public void testPartialString() {
        CreateTableTestUtils.createAllTable(engine, PartitionBy.NONE);
        int N = 10000;
        Rnd rnd = new Rnd();
        try (TableWriter writer = newTableWriter(configuration, "all", metrics)) {
            int col = writer.getMetadata().getColumnIndex("str");
            for (int i = 0; i < N; i++) {
                TableWriter.Row r = writer.newRow();
                CharSequence chars = rnd.nextChars(15);
                r.putStr(col, chars, 2, 10);
                r.append();
            }
            writer.commit();

            // add more rows for good measure and rollback

            for (int i = 0; i < N; i++) {
                TableWriter.Row r = writer.newRow();
                CharSequence chars = rnd.nextChars(15);
                r.putStr(col, chars, 2, 10);
                r.append();
            }
            writer.rollback();

            rnd.reset();

            try (TableReader reader = newTableReader(configuration, "all")) {
                col = reader.getMetadata().getColumnIndex("str");
                int count = 0;
                RecordCursor cursor = reader.getCursor();
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    CharSequence expected = rnd.nextChars(15);
                    CharSequence actual = record.getStr(col);
                    Assert.assertTrue(Chars.equals(expected, 2, 10, actual, 0, 8));
                    count++;
                }
                Assert.assertEquals(N, count);
            }
        }
    }

    @Test
    public void testReadByDay() throws Exception {
        CreateTableTestUtils.createAllTable(engine, PartitionBy.DAY);
        TestUtils.assertMemoryLeak(this::testTableCursor);
    }

    @Test
    public void testReadByMonth() throws Exception {
        CreateTableTestUtils.createAllTable(engine, PartitionBy.MONTH);
        TestUtils.assertMemoryLeak(() -> testTableCursor(60 * 60 * 60000));
    }

    @Test
    public void testReadByWeek() throws Exception {
        CreateTableTestUtils.createAllTable(engine, PartitionBy.WEEK);
        TestUtils.assertMemoryLeak(() -> testTableCursor(7 * 60 * 60000L));
    }

    @Test
    public void testReadByYear() throws Exception {
        CreateTableTestUtils.createAllTable(engine, PartitionBy.YEAR);
        TestUtils.assertMemoryLeak(() -> testTableCursor(24 * 60 * 60 * 60000L));
    }

    @Test
    public void testReadEmptyTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CreateTableTestUtils.createAllTable(engine, PartitionBy.NONE);
            try (TableWriter ignored1 = newTableWriter(configuration, "all", metrics)) {

                // open another writer, which should fail
                try {
                    try (TableWriter ignore = newTableWriter(configuration, "all", metrics)) {
                        Assert.fail();
                    }
                } catch (CairoException ignored) {

                }

                try (TableReader reader = newTableReader(configuration, "all")) {
                    Assert.assertFalse(reader.getCursor().hasNext());
                }
            }
        });
    }

    @Test
    public void testReadLong256Four() {
        try (TableModel model = new TableModel(configuration, "w", PartitionBy.DAY).col("l", ColumnType.LONG256).timestamp()) {
            CreateTableTestUtils.create(model);
        }

        final int N = 1_000_000;
        final Rnd rnd = new Rnd();
        long timestamp = 0;
        try (TableWriter writer = newTableWriter(configuration, "w", metrics)) {
            for (int i = 0; i < N; i++) {
                TableWriter.Row row = writer.newRow(timestamp);
                row.putLong256(0, "0x" + padHexLong(rnd.nextLong()) + padHexLong(rnd.nextLong()) + padHexLong(rnd.nextLong()) + padHexLong(rnd.nextLong()));
                row.append();
            }
            writer.commit();
        }

        rnd.reset();
        final StringSink sink = new StringSink();
        try (TableReader reader = newTableReader(configuration, "w")) {
            final RecordCursor cursor = reader.getCursor();
            final Record record = cursor.getRecord();
            int count = 0;
            while (cursor.hasNext()) {
                sink.clear();
                record.getLong256(0, sink);
                TestUtils.assertEquals("0x" + padHexLong(rnd.nextLong()) + padHexLong(rnd.nextLong()) + padHexLong(rnd.nextLong()) + padHexLong(rnd.nextLong()), sink);
                count++;
            }
            Assert.assertEquals(N, count);
        }
    }

    @Test
    public void testReadLong256One() {
        try (TableModel model = new TableModel(configuration, "w", PartitionBy.DAY).col("l", ColumnType.LONG256).timestamp()) {
            CreateTableTestUtils.create(model);
        }

        final int N = 1_000_000;
        final Rnd rnd = new Rnd();
        long timestamp = 0;
        try (TableWriter writer = newTableWriter(configuration, "w", metrics)) {
            for (int i = 0; i < N; i++) {
                TableWriter.Row row = writer.newRow(timestamp);
                row.putLong256(0, "0x" + padHexLong(rnd.nextLong()));
                row.append();
            }
            writer.commit();
        }

        rnd.reset();
        final StringSink sink = new StringSink();
        try (TableReader reader = newTableReader(configuration, "w")) {
            final RecordCursor cursor = reader.getCursor();
            final Record record = cursor.getRecord();
            int count = 0;
            while (cursor.hasNext()) {
                sink.clear();
                record.getLong256(0, sink);
                TestUtils.assertEquals("0x" + padHexLong(rnd.nextLong()), sink);
                count++;
            }
            Assert.assertEquals(N, count);
        }
    }

    @Test
    public void testReadLong256Three() {
        try (TableModel model = new TableModel(configuration, "w", PartitionBy.DAY).col("l", ColumnType.LONG256).timestamp()) {
            CreateTableTestUtils.create(model);
        }

        final int N = 1_000_000;
        final Rnd rnd = new Rnd();
        long timestamp = 0;
        try (TableWriter writer = newTableWriter(configuration, "w", metrics)) {
            for (int i = 0; i < N; i++) {
                TableWriter.Row row = writer.newRow(timestamp);
                row.putLong256(0, "0x" + padHexLong(rnd.nextLong()) + padHexLong(rnd.nextLong()) + padHexLong(rnd.nextLong()));
                row.append();
            }
            writer.commit();
        }

        rnd.reset();
        final StringSink sink = new StringSink();
        try (TableReader reader = newTableReader(configuration, "w")) {
            final RecordCursor cursor = reader.getCursor();
            final Record record = cursor.getRecord();
            int count = 0;
            while (cursor.hasNext()) {
                sink.clear();
                record.getLong256(0, sink);
                TestUtils.assertEquals("0x" + padHexLong(rnd.nextLong()) + padHexLong(rnd.nextLong()) + padHexLong(rnd.nextLong()), sink);
                count++;
            }
            Assert.assertEquals(N, count);
        }
    }

    @Test
    public void testReadLong256Two() {
        try (TableModel model = new TableModel(configuration, "w", PartitionBy.DAY).col("l", ColumnType.LONG256).timestamp()) {
            CreateTableTestUtils.create(model);
        }

        final int N = 1_000_000;
        final Rnd rnd = new Rnd();
        long timestamp = 0;
        try (TableWriter writer = newTableWriter(configuration, "w", metrics)) {
            for (int i = 0; i < N; i++) {
                TableWriter.Row row = writer.newRow(timestamp);
                row.putLong256(0, "0x" + padHexLong(rnd.nextLong()) + padHexLong(rnd.nextLong()));
                row.append();
            }
            writer.commit();
        }

        rnd.reset();
        final StringSink sink = new StringSink();
        try (TableReader reader = newTableReader(configuration, "w")) {
            final RecordCursor cursor = reader.getCursor();
            final Record record = cursor.getRecord();
            int count = 0;
            while (cursor.hasNext()) {
                sink.clear();
                record.getLong256(0, sink);
                TestUtils.assertEquals("0x" + padHexLong(rnd.nextLong()) + padHexLong(rnd.nextLong()), sink);
                count++;
            }
            Assert.assertEquals(N, count);
        }
    }

    @Test
    public void testReadNonPartitioned() throws Exception {
        CreateTableTestUtils.createAllTable(engine, PartitionBy.NONE);
        TestUtils.assertMemoryLeak(this::testTableCursor);
    }

    @Test
    public void testReaderAndWriterRace() throws Exception {
        TestUtils.assertMemoryLeak(() -> {

            try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)) {
                CreateTableTestUtils.create(model.timestamp());
            }

            SOCountDownLatch stopLatch = new SOCountDownLatch(2);
            CyclicBarrier barrier = new CyclicBarrier(2);
            int count = 1000000;
            AtomicInteger reloadCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);

            try (
                    TableWriter writer = newTableWriter(configuration, "x", metrics);
                    TableReader reader = newTableReader(configuration, "x")
            ) {
                new Thread(() -> {
                    try {
                        TestUtils.await(barrier);
                        for (int i = 0; i < count; i++) {
                            TableWriter.Row row = writer.newRow(i);
                            row.append();
                            writer.commit();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        errorCount.incrementAndGet();
                    } finally {
                        stopLatch.countDown();
                    }

                }).start();

                new Thread(() -> {
                    try {
                        barrier.await();
                        int max = 0;
                        RecordCursor cursor = reader.getCursor();
                        while (max < count) {
                            if (reader.reload()) {
                                reloadCount.incrementAndGet();
                                cursor.toTop();
                                int localCount = 0;
                                while (cursor.hasNext()) {
                                    localCount++;
                                }
                                if (localCount > max) {
                                    max = localCount;
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        errorCount.incrementAndGet();
                    } finally {
                        stopLatch.countDown();
                    }
                }).start();

                stopLatch.await();

                Assert.assertTrue(reloadCount.get() > 0);
                Assert.assertEquals(0, errorCount.get());
            }
        });
    }

    @Test
    public void testReaderGoesToPoolWhenCommitHappen() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = "testReaderGoesToPoolWhenCommitHappen";
            try (TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY).col("l", ColumnType.LONG)) {
                TableToken tableToken = CreateTableTestUtils.create(model);

                int rowCount = 10;
                try (TableWriter writer = newTableWriter(configuration, tableName, metrics)) {
                    try (TableReader ignore = getReader(tableToken)) {
                        for (int i = 0; i < rowCount; i++) {
                            TableWriter.Row row = writer.newRow();
                            row.putLong(0, i);
                            row.append();
                        }
                        writer.commit();
                    }
                }

                try (TableReader reader = getReader(tableToken)) {
                    Assert.assertEquals(rowCount, reader.size());
                }
            }
        });

    }

    @Test
    public void testReaderKeepsNLatestOpenPartitions() {
        final int openPartitionsLimit = 2;
        final int expectedPartitions = 12;
        final long tsStep = Timestamps.SECOND_MICROS;
        final int rows = expectedPartitions * 60 * 60;

        maxOpenPartitions = openPartitionsLimit;

        try (TableModel model = new TableModel(configuration, "x", PartitionBy.HOUR).col("i", ColumnType.INT).timestamp()) {
            TestUtils.create(model, engine);
        }

        try (TableWriter writer = newTableWriter(configuration, "x", metrics)) {
            for (int i = 0; i < rows; i++) {
                TableWriter.Row row = writer.newRow(i * tsStep);
                row.putInt(0, i);
                row.append();
            }
            writer.commit();
        }

        try (TableReader reader = newTableReader(configuration, "x")) {
            Assert.assertEquals(expectedPartitions, reader.getPartitionCount());
            Assert.assertEquals(0, reader.getOpenPartitionCount());
            assertOpenPartitionCount(reader);

            final RecordCursor cursor = reader.getCursor();
            final Record record = cursor.getRecord();
            int count = 0;
            while (cursor.hasNext()) {
                Assert.assertEquals(count, record.getInt(0));
                count++;
            }
            Assert.assertEquals(rows, count);
            Assert.assertEquals(expectedPartitions, reader.getOpenPartitionCount());
            assertOpenPartitionCount(reader);

            reader.goPassive();
            Assert.assertFalse(reader.reload());

            Assert.assertEquals(openPartitionsLimit, reader.getOpenPartitionCount());
            assertOpenPartitionCount(reader);

            cursor.toTop();
            count = 0;
            while (cursor.hasNext()) {
                Assert.assertEquals(count, record.getInt(0));
                count++;
            }
            Assert.assertEquals(rows, count);
            Assert.assertEquals(expectedPartitions, reader.getOpenPartitionCount());
            assertOpenPartitionCount(reader);
        }
    }

    @Test
    public void testReaderReloadWhenColumnAddedBeforeTheData() throws Exception {
        assertMemoryLeak(
                () -> {
                    // model table
                    try (TableModel model = new TableModel(configuration, "w", PartitionBy.HOUR).col("l", ColumnType.LONG).timestamp()) {
                        CreateTableTestUtils.create(model);
                    }

                    try (
                            TableWriter w = newTableWriter(configuration, "w", metrics);
                            TableReader r = newTableReader(configuration, "w")
                    ) {
                        // Create and cancel row to ensure partition entry and NULL max timestamp
                        // this used to trigger a problem with very last reload of the reader.
                        w.newRow(TimestampFormatUtils.parseTimestamp("2016-03-02T10:00:00.000000Z")).cancel();

                        // before adding any data add column
                        w.addColumn("xyz", ColumnType.SYMBOL);

                        Assert.assertTrue(r.reload());

                        TableWriter.Row row = w.newRow(TimestampFormatUtils.parseTimestamp("2016-03-02T10:00:00.000000Z"));
                        row.append();
                        w.commit();

                        Assert.assertTrue(r.reload());

                        sink.clear();
                        TestUtils.printer.print(r.getCursor(), r.getMetadata(), true, sink);
                        TestUtils.assertEquals(
                                "l\ttimestamp\txyz\n" +
                                        "NaN\t2016-03-02T10:00:00.000000Z\t\n",
                                sink
                        );
                    }
                }
        );
    }

    @Test
    public void testReloadByDaySwitch() throws Exception {
        testReload(PartitionBy.DAY, 15, 60L * 60000, MUST_SWITCH);
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
    public void testReloadByWeekSamePartition() throws Exception {
        testReload(PartitionBy.WEEK, 15, 60L * 60000, MUST_NOT_SWITCH);
    }

    @Test
    public void testReloadByWeekSwitch() throws Exception {
        testReload(PartitionBy.WEEK, 15, 7 * 60L * 60000, MUST_SWITCH);
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
        testReload(PartitionBy.NONE, 10, 60L * 60000, DO_NOT_CARE);
    }

    @Test
    public void testReloadWithTrailingNullString() throws NumericException {
        final String tableName = "reload_test";
        try (TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)) {
            model.col("str", ColumnType.STRING);
            model.timestamp();
            CreateTableTestUtils.create(model);
        }

        try (TableReader reader = newTableReader(configuration, tableName)) {

            Assert.assertFalse(reader.reload());

            final int N = 100;
            final int M = 1_000_000;
            final Rnd rnd = new Rnd();

            try (TableWriter writer = newTableWriter(configuration, tableName, metrics)) {
                long timestamp = TimestampFormatUtils.parseUTCTimestamp("2019-01-31T10:00:00.000001Z");
                long timestampStep = 500;

                for (int i = 0; i < N; i++) {
                    TableWriter.Row row = writer.newRow(timestamp);
                    row.putStr(0, rnd.nextChars(7));
                    row.append();
                    timestamp += timestampStep;
                }

                writer.commit();

                Assert.assertTrue(reader.reload());

                rnd.reset();
                RecordCursor cursor = reader.getCursor();
                final Record record = cursor.getRecord();

                while (cursor.hasNext()) {
                    TestUtils.assertEquals(rnd.nextChars(7), record.getStr(0));
                }

                // rnd is aligned to where we left our writer, just continue
                // from this point to append and setSize files

                for (int i = 0; i < M; i++) {
                    TableWriter.Row row = writer.newRow(timestamp);
                    row.putStr(0, rnd.nextChars(7));
                    row.append();
                    timestamp += timestampStep;
                }

                // and add the NULL at the end, which could cause reload issue
                TableWriter.Row row = writer.newRow(timestamp);
                row.putStr(0, null);
                row.append();

                writer.commit();

                // this reload must be able to setSize its files following file expansion by writer
                Assert.assertTrue(reader.reload());

                int count = 0;
                cursor = reader.getCursor();
                rnd.reset();
                while (cursor.hasNext()) {
                    if (count == N + M) {
                        Assert.assertNull(record.getStr(0));
                    } else {
                        TestUtils.assertEquals(rnd.nextChars(7), record.getStr(0));
                    }
                    count++;
                }

                Assert.assertEquals(N + M + 1, count);
            }
        }
    }

    @Test
    public void testReloadWithoutData() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TableModel model = new TableModel(configuration, "tab", PartitionBy.DAY).col("x", ColumnType.SYMBOL).col("y", ColumnType.LONG)) {
                CreateTableTestUtils.create(model);
            }

            try (TableWriter writer = newTableWriter(configuration, "tab", metrics)) {
                TableWriter.Row r = writer.newRow();
                r.putSym(0, "hello");
                r.append();

                writer.rollback();

                try (TableReader reader = newTableReader(configuration, "tab")) {
                    writer.addColumn("z", ColumnType.SYMBOL);
                    Assert.assertTrue(reader.reload());
                    writer.addColumn("w", ColumnType.INT);
                    Assert.assertTrue(reader.reload());
                }
            }
        });
    }

    @Test
    public void testRemoveActivePartitionByDay() throws Exception {
        testRemoveActivePartition(PartitionBy.DAY, current -> Timestamps.addDays(Timestamps.floorDD(current), 1), "2017-12-15");
    }

    @Test
    public void testRemoveActivePartitionByMonth() throws Exception {
        testRemoveActivePartition(PartitionBy.MONTH, current -> Timestamps.addMonths(Timestamps.floorMM(current), 1), "2018-04");
    }

    @Test
    public void testRemoveActivePartitionByWeek() throws Exception {
        testRemoveActivePartition(PartitionBy.WEEK, current -> Timestamps.addWeeks(Timestamps.floorWW(current), 1), "2017-W51");
    }

    @Test
    public void testRemoveActivePartitionByYear() throws Exception {
        testRemoveActivePartition(PartitionBy.YEAR, current -> Timestamps.addYear(Timestamps.floorYYYY(current), 1), "2021");
    }

    @Test
    public void testRemoveDefaultPartition() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 100;
            int N_PARTITIONS = 5;
            long timestampUs = TimestampFormatUtils.parseTimestamp("2017-12-11T00:00:00.000Z");
            long stride = 100;
            int bandStride = 1000;
            int totalCount = 0;

            // model table
            try (TableModel model = new TableModel(configuration, "w", PartitionBy.NONE).col("l", ColumnType.LONG).timestamp()) {
                CreateTableTestUtils.create(model);
            }

            try (TableWriter writer = newTableWriter(configuration, "w", metrics)) {

                for (int k = 0; k < N_PARTITIONS; k++) {
                    long band = k * bandStride;
                    for (int i = 0; i < N; i++) {
                        TableWriter.Row row = writer.newRow(timestampUs);
                        row.putLong(0, band + i);
                        row.append();
                        writer.commit();
                        timestampUs += stride;
                    }
                    timestampUs = Timestamps.addDays(Timestamps.floorDD(timestampUs), 1);
                }

                Assert.assertEquals(N * N_PARTITIONS, writer.size());

                Assert.assertFalse(writer.removePartition(0));

                Assert.assertEquals(N * N_PARTITIONS, writer.size());
            }

            // now open table reader having partition gap
            try (TableReader reader = newTableReader(configuration, "w")) {

                Assert.assertEquals(N * N_PARTITIONS, reader.size());

                RecordCursor cursor = reader.getCursor();
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    record.getLong(0);
                    totalCount++;
                }
            }

            Assert.assertEquals(N * N_PARTITIONS, totalCount);
        });
    }

    @Test
    public void testRemoveFirstPartitionByDay() throws Exception {
        testRemovePartition(PartitionBy.DAY, "2017-12-11", 0, current -> Timestamps.addDays(Timestamps.floorDD(current), 1));
    }

    @Test
    public void testRemoveFirstPartitionByDayReload() throws Exception {
        testRemovePartitionReload(PartitionBy.DAY, "2017-12-11", 0, current -> Timestamps.addDays(Timestamps.floorDD(current), 1));
    }

    @Test
    public void testRemoveFirstPartitionByDayReloadTwo() throws Exception {
        testRemovePartitionReload(PartitionBy.DAY, "2017-12-11", 0, current -> Timestamps.addDays(Timestamps.floorDD(current), 2));
    }

    @Test
    public void testRemoveFirstPartitionByDayTwo() throws Exception {
        testRemovePartition(PartitionBy.DAY, "2017-12-11", 0, current -> Timestamps.addDays(Timestamps.floorDD(current), 2));
    }

    @Test
    public void testRemoveFirstPartitionByMonth() throws Exception {
        testRemovePartition(PartitionBy.MONTH, "2017-12", 0, current -> Timestamps.addMonths(Timestamps.floorMM(current), 1));
    }

    @Test
    public void testRemoveFirstPartitionByMonthReload() throws Exception {
        testRemovePartitionReload(PartitionBy.MONTH, "2017-12", 0, current -> Timestamps.addMonths(Timestamps.floorMM(current), 1));
    }

    @Test
    public void testRemoveFirstPartitionByMonthReloadTwo() throws Exception {
        testRemovePartitionReload(PartitionBy.MONTH, "2017-12", 0, current -> Timestamps.addMonths(Timestamps.floorMM(current), 2));
    }

    @Test
    public void testRemoveFirstPartitionByMonthTwo() throws Exception {
        testRemovePartition(PartitionBy.MONTH, "2017-12", 0, current -> Timestamps.addMonths(Timestamps.floorMM(current), 2));
    }

    @Test
    public void testRemoveFirstPartitionByWeek() throws Exception {
        testRemovePartition(PartitionBy.WEEK, "2017-W50", 0, current -> Timestamps.addWeeks(Timestamps.floorWW(current), 1));
    }

    @Test
    public void testRemoveFirstPartitionByWeekReload() throws Exception {
        testRemovePartitionReload(PartitionBy.WEEK, "2017-W50", 0, current -> Timestamps.addWeeks(Timestamps.floorWW(current), 1));
    }

    @Test
    public void testRemoveFirstPartitionByWeekReloadTwo() throws Exception {
        testRemovePartitionReload(PartitionBy.WEEK, "2017-W50", 0, current -> Timestamps.addWeeks(Timestamps.floorWW(current), 2));
    }

    @Test
    public void testRemoveFirstPartitionByWeekTwo() throws Exception {
        testRemovePartition(PartitionBy.WEEK, "2017-W50", 0, current -> Timestamps.addWeeks(Timestamps.floorWW(current), 2));
    }

    @Test
    public void testRemoveFirstPartitionByYear() throws Exception {
        testRemovePartition(PartitionBy.YEAR, "2017", 0, current -> Timestamps.addYear(Timestamps.floorYYYY(current), 1));
    }

    @Test
    public void testRemoveFirstPartitionByYearReload() throws Exception {
        testRemovePartitionReload(PartitionBy.YEAR, "2017", 0, current -> Timestamps.addYear(Timestamps.floorYYYY(current), 1));
    }

    @Test
    public void testRemoveFirstPartitionByYearReloadTwo() throws Exception {
        testRemovePartitionReload(PartitionBy.YEAR, "2017", 0, current -> Timestamps.addYear(Timestamps.floorYYYY(current), 2));
    }

    @Test
    public void testRemoveFirstPartitionByYearTwo() throws Exception {
        testRemovePartition(PartitionBy.YEAR, "2017", 0, current -> Timestamps.addYear(Timestamps.floorYYYY(current), 2));
    }

    @Test
    public void testRemovePartitionByDay() throws Exception {
        testRemovePartition(PartitionBy.DAY, "2017-12-14", 3000, current -> Timestamps.addDays(Timestamps.floorDD(current), 1));
    }

    @Test
    public void testRemovePartitionByDayCannotDeleteDir() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 100;
            int N_PARTITIONS = 5;
            long timestampUs = TimestampFormatUtils.parseTimestamp("2017-12-11T00:00:00.000Z");
            long stride = 100;
            int bandStride = 1000;
            int totalCount = 0;

            FilesFacade ff = new TestFilesFacadeImpl() {
                @Override
                public int rmdir(Path name) {
                    if (Chars.endsWith(name, "2017-12-14" + Files.SEPARATOR)) {
                        return 1;
                    }
                    return super.rmdir(name);
                }
            };

            CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public FilesFacade getFilesFacade() {
                    return ff;
                }
            };

            // model table
            try (TableModel model = new TableModel(configuration, "w", PartitionBy.DAY).col("l", ColumnType.LONG).timestamp()) {
                CreateTableTestUtils.create(model);
            }

            try (TableWriter writer = newTableWriter(configuration, "w", metrics)) {

                for (int k = 0; k < N_PARTITIONS; k++) {
                    long band = k * bandStride;
                    for (int i = 0; i < N; i++) {
                        TableWriter.Row row = writer.newRow(timestampUs);
                        row.putLong(0, band + i);
                        row.append();
                        writer.commit();
                        timestampUs += stride;
                    }
                    timestampUs = Timestamps.addDays(Timestamps.floorDD(timestampUs), 1);
                }

                Assert.assertEquals(N * N_PARTITIONS, writer.size());

                DateFormat fmt = PartitionBy.getPartitionDirFormatMethod(PartitionBy.DAY);
                assert fmt != null;
                final long timestamp = fmt.parse("2017-12-14", null);

                Assert.assertTrue(writer.removePartition(timestamp));
                Assert.assertFalse(writer.removePartition(timestamp));

                Assert.assertEquals(N * (N_PARTITIONS - 1), writer.size());
            }

            // now open table reader having partition gap
            try (TableReader reader = newTableReader(configuration, "w")) {

                Assert.assertEquals(N * (N_PARTITIONS - 1), reader.size());

                int previousBand = -1;
                int bandCount = 0;
                RecordCursor cursor = reader.getCursor();
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    long value = record.getLong(0);
                    int band = (int) ((value / bandStride) * bandStride);
                    if (band != previousBand) {
                        // make sure we don#t pick up deleted partition
                        Assert.assertNotEquals(3000, band);
                        if (previousBand != -1) {
                            Assert.assertEquals(N, bandCount);
                        }
                        previousBand = band;
                        bandCount = 0;
                    }
                    bandCount++;
                    totalCount++;
                }
                Assert.assertEquals(N, bandCount);
            }

            Assert.assertEquals(N * (N_PARTITIONS - 1), totalCount);
        });
    }

    @Test
    public void testRemovePartitionByDayReload() throws Exception {
        testRemovePartitionReload(PartitionBy.DAY, "2017-12-14", 3000, current -> Timestamps.addDays(Timestamps.floorDD(current), 1));
    }

    @Test
    public void testRemovePartitionByMonth() throws Exception {
        testRemovePartition(PartitionBy.MONTH, "2018-01", 1000, current -> Timestamps.addMonths(Timestamps.floorMM(current), 1));
    }

    @Test
    public void testRemovePartitionByMonthReload() throws Exception {
        testRemovePartitionReload(PartitionBy.MONTH, "2018-01", 1000, current -> Timestamps.addMonths(Timestamps.floorMM(current), 1));
    }

    @Test
    public void testRemovePartitionByWeek() throws Exception {
        testRemovePartition(PartitionBy.WEEK, "2017-W51", 1000, current -> Timestamps.addWeeks(Timestamps.floorWW(current), 1));
    }

    @Test
    public void testRemovePartitionByWeekReload() throws Exception {
        testRemovePartitionReload(PartitionBy.WEEK, "2017-W51", 1000, current -> Timestamps.addWeeks(Timestamps.floorWW(current), 1));
    }

    @Test
    public void testRemovePartitionByYear() throws Exception {
        testRemovePartition(PartitionBy.YEAR, "2020", 3000, current -> Timestamps.addYear(Timestamps.floorYYYY(current), 1));
    }

    @Test
    public void testRemovePartitionByYearReload() throws Exception {
        testRemovePartitionReload(PartitionBy.YEAR, "2020", 3000, current -> Timestamps.addYear(Timestamps.floorYYYY(current), 1));
    }

    @Test
    public void testStringColumnRemove() throws Exception {
        AtomicInteger counterRef = new AtomicInteger(CANNOT_DELETE);
        TestFilesFacade ff = createColumnDeleteCounterFileFacade(counterRef, "b", "");

        assertMemoryLeak(ff, () -> {

            // create table with two string columns
            try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("a", ColumnType.STRING).col("b", ColumnType.STRING)) {
                CreateTableTestUtils.create(model);
            }

            Rnd rnd = new Rnd();
            final int N = 1000;
            // make sure we forbid deleting column "b" files

            try (TableWriter writer = getWriter("x")) {
                for (int i = 0; i < N; i++) {
                    TableWriter.Row row = writer.newRow();
                    row.putStr(0, rnd.nextChars(10));
                    row.putStr(1, rnd.nextChars(15));
                    row.append();
                }
                writer.commit();

                try (TableReader reader = getReader("x")) {
                    long counter = 0;

                    rnd.reset();
                    RecordCursor cursor = reader.getCursor();
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        Assert.assertEquals(rnd.nextChars(10), record.getStr(0));
                        Assert.assertEquals(rnd.nextChars(15), record.getStr(1));
                        counter++;
                    }

                    Assert.assertEquals(N, counter);

                    // this should write metadata without column "b" but will ignore
                    // file delete failures
                    writer.removeColumn("b");

                    // It used to be: this must fail because we cannot delete foreign files
                    // but with column version file we can handle it.
                    writer.addColumn("b", ColumnType.STRING);

                    // now assert what reader sees
                    Assert.assertTrue(reader.reload());
                    Assert.assertEquals(N, reader.size());

                    rnd.reset();
                    cursor.toTop();
                    while (cursor.hasNext()) {
                        Assert.assertEquals(rnd.nextChars(10), record.getStr(0));
                        // roll random generator to make sure it returns same values
                        rnd.nextChars(15);
                        counter++;
                    }

                    Assert.assertEquals(N * 2, counter);
                }
            }
        });
    }

    @Test
    public void testSymbolIndex() throws Exception {
        String expected = "{\"columnCount\":3,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"SYMBOL\",\"indexed\":true,\"indexValueBlockCapacity\":2},{\"index\":1,\"name\":\"b\",\"type\":\"INT\"},{\"index\":2,\"name\":\"timestamp\",\"type\":\"TIMESTAMP\"}],\"timestampIndex\":2}";

        TestUtils.assertMemoryLeak(() -> {
            try (TableModel model = new TableModel(configuration, "x", PartitionBy.DAY)
                    .col("a", ColumnType.SYMBOL).indexed(true, 2)
                    .col("b", ColumnType.INT)
                    .timestamp()) {
                CreateTableTestUtils.create(model);
            }

            int N = 1000;
            long ts = TimestampFormatUtils.parseTimestamp("2018-01-06T10:00:00.000Z");
            final Rnd rnd = new Rnd();
            try (TableWriter writer = newTableWriter(configuration, "x", metrics)) {
                sink.clear();
                writer.getMetadata().toJson(sink);
                TestUtils.assertEquals(expected, sink);

                for (int i = 0; i < N; i++) {
                    TableWriter.Row row = writer.newRow(ts + ((long) i) * 2 * 360000000L);
                    row.putSym(0, rnd.nextChars(3));
                    row.putInt(1, rnd.nextInt());
                    row.append();
                    writer.commit();
                }
            }

            try (TableReader reader = newTableReader(configuration, "x")) {
                sink.clear();
                reader.getMetadata().toJson(sink);
                TestUtils.assertEquals(expected, sink);
            }
        });
    }

    @Test
    public void testUnsuccessfulFileRemoveAndReloadStr() throws Exception {
        AtomicInteger counterRef = new AtomicInteger(CANNOT_DELETE);
        TestFilesFacade ff = createColumnDeleteCounterFileFacade(counterRef, "b", "");
        assertMemoryLeak(ff, () -> {

            // create table with two string columns
            try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("a", ColumnType.SYMBOL).col("b", ColumnType.STRING)) {
                CreateTableTestUtils.create(model);
            }

            Rnd rnd = new Rnd();
            final int N = 1000;
            // make sure we forbid deleting column "b" files

            // populate table and delete column
            try (TableWriter writer = getWriter("x")) {
                for (int i = 0; i < N; i++) {
                    TableWriter.Row row = writer.newRow();
                    row.putSym(0, rnd.nextChars(10));
                    row.putStr(1, rnd.nextChars(15));
                    row.append();
                }
                writer.commit();

                try (TableReader reader = getReader("x")) {
                    long counter = 0;

                    rnd.reset();
                    RecordCursor cursor = reader.getCursor();
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        Assert.assertEquals(rnd.nextChars(10), record.getSym(0));
                        Assert.assertEquals(rnd.nextChars(15), record.getStr(1));
                        counter++;
                    }

                    Assert.assertEquals(N, counter);

                    // this should write metadata without column "b" but will ignore
                    // file delete failures
                    writer.removeColumn("b");

                    // now when we add new column by same name it must not pick up files we failed to delete previously
                    writer.addColumn("b", ColumnType.STRING);

                    for (int i = 0; i < N; i++) {
                        TableWriter.Row row = writer.newRow();
                        row.putSym(0, rnd.nextChars(10));
                        row.putStr(2, rnd.nextChars(15));
                        row.append();
                    }
                    writer.commit();

                    // now assert what reader sees
                    Assert.assertTrue(reader.reload());
                    Assert.assertEquals(N * 2, reader.size());

                    rnd.reset();
                    cursor.toTop();
                    counter = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(rnd.nextChars(10), record.getSym(0));
                        if (counter < N) {
                            // roll random generator to make sure it returns same values
                            rnd.nextChars(15);
                            Assert.assertNull(record.getStr(1));
                        } else {
                            Assert.assertEquals(rnd.nextChars(15), record.getStr(1));
                        }
                        counter++;
                    }

                    Assert.assertEquals(N * 2, counter);
                }
            }

            checkColumnPurgeRemovesFiles(counterRef, ff, 2);
        });
    }

    @Test
    public void testUnsuccessfulFileRename() throws Exception {
        AtomicInteger counterRef = new AtomicInteger(CANNOT_DELETE);
        TestFilesFacade ff = createColumnDeleteCounterFileFacade(counterRef, "b", "");

        assertMemoryLeak(ff, () -> {

            // create table with two string columns
            try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("a", ColumnType.STRING).col("b", ColumnType.STRING)) {
                CreateTableTestUtils.create(model);
            }

            Rnd rnd = new Rnd();
            final int N = 1000;

            // populate table and delete column
            try (TableWriter writer = getWriter("x")) {
                for (int i = 0; i < N; i++) {
                    TableWriter.Row row = writer.newRow();
                    row.putStr(0, rnd.nextChars(10));
                    row.putStr(1, rnd.nextChars(15));
                    row.append();
                }
                writer.commit();

                try (TableReader reader = getReader("x")) {
                    long counter = 0;

                    rnd.reset();
                    RecordCursor cursor = reader.getCursor();
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        Assert.assertEquals(rnd.nextChars(10), record.getStr(0));
                        Assert.assertEquals(rnd.nextChars(15), record.getStr(1));
                        counter++;
                    }

                    Assert.assertEquals(N, counter);

                    // this should write metadata without column "b" but will ignore
                    // file rename failures
                    writer.renameColumn("b", "bb");

                    // It used to be: this must fail because we cannot delete foreign files
                    // but with column version file we can handle it.
                    writer.addColumn("b", ColumnType.STRING);

                    // now assert what reader sees
                    Assert.assertTrue(reader.reload()); // This fails with could not open read-only .. /bb.i.
                    Assert.assertEquals(N, reader.size());

                    rnd.reset();
                    cursor.toTop();
                    while (cursor.hasNext()) {
                        Assert.assertEquals(rnd.nextChars(10), record.getStr(0));
                        // roll random generator to make sure it returns same values
                        rnd.nextChars(15);
                        counter++;
                    }

                    Assert.assertEquals(N * 2, counter);
                }
            }
            engine.releaseInactive();
            checkColumnPurgeRemovesFiles(counterRef, ff, 2);
        });
    }

    @Test
    public void testUnsuccessfulRemoveAndReloadSym() throws Exception {
        AtomicInteger counterRef = new AtomicInteger(CANNOT_DELETE);
        TestFilesFacade ff = createColumnDeleteCounterFileFacade(counterRef, "b", "");

        assertMemoryLeak(ff, () -> {

            // create table with two string columns
            try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                    .col("a", ColumnType.SYMBOL)
                    .col("b", ColumnType.SYMBOL)) {
                CreateTableTestUtils.create(model);
            }

            Rnd rnd = new Rnd();
            final int N = 1000;

            // populate table and delete column
            try (TableWriter writer = getWriter("x")) {
                appendTwoSymbols(writer, rnd, 1);
                writer.commit();

                try (TableReader reader = newTableReader(configuration, "x")) {
                    long counter = 0;

                    rnd.reset();
                    RecordCursor cursor = reader.getCursor();
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        Assert.assertEquals(rnd.nextChars(10), record.getSym(0));
                        Assert.assertEquals(rnd.nextChars(15), record.getSym(1));
                        counter++;
                    }

                    Assert.assertEquals(N, counter);

                    // this should write metadata without column "b" but will ignore
                    // file delete failures
                    writer.removeColumn("b");

                    // now when we add new column by same name it must not pick up files we failed to delete previously
                    writer.addColumn("b", ColumnType.SYMBOL);

                    // SymbolMap must be cleared when we try to do add values to new column
                    appendTwoSymbols(writer, rnd, 2);
                    writer.commit();

                    // now assert what reader sees
                    Assert.assertTrue(reader.reload());
                    Assert.assertEquals(N * 2, reader.size());

                    rnd.reset();
                    cursor.toTop();
                    counter = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(rnd.nextChars(10), record.getSym(0));
                        if (counter < N) {
                            // roll random generator to make sure it returns same values
                            rnd.nextChars(15);
                            Assert.assertNull(record.getSym(1));
                        } else {
                            Assert.assertEquals(rnd.nextChars(15), record.getSym(1));
                        }
                        counter++;
                    }

                    Assert.assertEquals(N * 2, counter);
                }
            }
            checkColumnPurgeRemovesFiles(counterRef, ff, 4);
        });
    }

    @Test
    public void testUnsuccessfulRemoveAndReloadSymTwice() throws Exception {
        AtomicInteger counterRef = new AtomicInteger(CANNOT_DELETE);
        TestFilesFacade ff = createColumnDeleteCounterFileFacade(counterRef, "b", "");

        assertMemoryLeak(ff, () -> {
            // create table with two string columns
            try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                    .col("a", ColumnType.SYMBOL)
                    .col("b", ColumnType.SYMBOL).indexed(true, 256)) {
                CreateTableTestUtils.create(model);
            }

            Rnd rnd = new Rnd();
            final int N = 1000;
            // make sure we forbid deleting column "b" files

            // populate table and delete column
            try (TableWriter writer = getWriter("x")) {
                appendTwoSymbols(writer, rnd, 1);
                writer.commit();

                try (TableReader reader = newTableReader(configuration, "x")) {
                    long counter = 0;

                    rnd.reset();
                    RecordCursor cursor = reader.getCursor();
                    Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        Assert.assertEquals(rnd.nextChars(10), record.getSym(0));
                        Assert.assertEquals(rnd.nextChars(15), record.getSym(1));
                        counter++;
                    }

                    Assert.assertEquals(N, counter);

                    // this should write metadata without column "b" but will ignore
                    // file delete failures
                    writer.removeColumn("b");
                    reader.reload();

                    // now when we add new column by same name it must not pick up files we failed to delete previously
                    writer.addColumn("b", ColumnType.SYMBOL);

                    // SymbolMap must be cleared when we try to do add values to new column
                    appendTwoSymbols(writer, rnd, 2);
                    writer.commit();

                    // now assert what reader sees
                    Assert.assertTrue(reader.reload());
                    Assert.assertEquals(N * 2, reader.size());

                    rnd.reset();
                    cursor.toTop();
                    counter = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(rnd.nextChars(10), record.getSym(0));
                        if (counter < N) {
                            // roll random generator to make sure it returns same values
                            rnd.nextChars(15);
                            Assert.assertNull(record.getSym(1));
                        } else {
                            Assert.assertEquals(rnd.nextChars(15), record.getSym(1));
                        }
                        counter++;
                    }

                    Assert.assertEquals(N * 2, counter);
                }

                checkColumnPurgeRemovesFiles(counterRef, ff, 5);
            }
        });
    }

    @Test
    public void testUnsuccessfulRemoveExplicitColCloseAndReloadSym() throws Exception {
        AtomicInteger counterRef = new AtomicInteger(CANNOT_DELETE);
        TestFilesFacade ff = createColumnDeleteCounterFileFacade(counterRef, "b", "");

        assertMemoryLeak(ff, () -> {

            // create table with two string columns
            try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("a", ColumnType.SYMBOL).col("b", ColumnType.SYMBOL).indexed(true, 256)) {
                CreateTableTestUtils.create(model);
            }

            Rnd rnd = new Rnd();
            final int N = 1000;
            // make sure we forbid deleting column "b" files

            // populate table and delete column
            try (TableWriter writer = getWriter("x")) {
                appendTwoSymbols(writer, rnd, 1);
                writer.commit();

                try (TableReader reader = newTableReader(configuration, "x")) {
                    long counter = 0;

                    rnd.reset();
                    RecordCursor cursor = reader.getCursor();
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        Assert.assertEquals(rnd.nextChars(10), record.getSym(0));
                        Assert.assertEquals(rnd.nextChars(15), record.getSym(1));
                        counter++;
                    }

                    Assert.assertEquals(N, counter);

                    // this should write metadata without column "b" but will ignore
                    // file delete failures
                    writer.removeColumn("b");

                    // now when we add new column by same name it must not pick up files we failed to delete previously
                    writer.addColumn("b", ColumnType.SYMBOL);

                    // SymbolMap must be cleared when we try to do add values to new column
                    appendTwoSymbols(writer, rnd, 2);
                    writer.commit();

                    // now assert what reader sees
                    Assert.assertTrue(reader.reload());
                    Assert.assertEquals(N * 2, reader.size());

                    rnd.reset();
                    cursor.toTop();
                    counter = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(rnd.nextChars(10), record.getSym(0));
                        if (counter < N) {
                            // roll random generator to make sure it returns same values
                            rnd.nextChars(15);
                            Assert.assertNull(record.getSym(1));
                        } else {
                            Assert.assertEquals(rnd.nextChars(15), record.getSym(1));
                        }
                        counter++;
                    }

                    Assert.assertEquals(N * 2, counter);
                }
            }

            checkColumnPurgeRemovesFiles(counterRef, ff, 5);
        });
    }

    private static long allocBlob() {
        return Unsafe.malloc(blobLen, MemoryTag.NATIVE_DEFAULT);
    }

    private static void assertBin(Record r, Rnd exp, long blob, int index) {
        if (exp.nextBoolean()) {
            exp.nextChars(blob, blobLen / 2);
            Assert.assertEquals(blobLen, r.getBinLen(index));
            BinarySequence sq = r.getBin(index);
            for (int l = 0; l < blobLen; l++) {
                byte b = sq.byteAt(l);
                boolean result = Unsafe.getUnsafe().getByte(blob + l) != b;
                if (result) {
                    Assert.fail("Error at [" + l + "]: expected=" + Unsafe.getUnsafe().getByte(blob + l) + ", actual=" + b);
                }
            }
        } else {
            Assert.assertEquals(TableUtils.NULL_LEN, r.getBinLen(index));
        }
    }

    private static void assertNullStr(Record r, int index) {
        Assert.assertNull(r.getStr(index));
        Assert.assertNull(r.getStrB(index));
        Assert.assertEquals(TableUtils.NULL_LEN, r.getStrLen(index));
    }

    private static void assertStrColumn(CharSequence expected, Record r, int index) {
        TestUtils.assertEquals(expected, r.getStr(index));
        TestUtils.assertEquals(expected, r.getStrB(index));
        Assert.assertNotSame(r.getStr(index), r.getStrB(index));
        Assert.assertEquals(expected.length(), r.getStrLen(index));
    }

    private static void checkColumnPurgeRemovesFiles(AtomicInteger counterRef, TestFilesFacade ff, int removeCallsExpected) throws SqlException {
        Assert.assertFalse(ff.wasCalled());
        counterRef.set(0);
        try (ColumnPurgeJob job = new ColumnPurgeJob(engine, null)) {
            job.run(0);
        }
        Assert.assertTrue(ff.called() >= removeCallsExpected);
    }

    @NotNull
    private static TestFilesFacade createColumnDeleteCounterFileFacade(AtomicInteger counterRef, String columnName, final String suffix) {
        return new TestFilesFacade() {
            @Override
            public int called() {
                return counterRef.get();
            }

            @Override
            public boolean remove(LPSZ name) {
                if (
                        Chars.endsWith(name, columnName + ".i") ||
                                Chars.endsWith(name, columnName + ".d" + suffix) ||
                                Chars.endsWith(name, columnName + ".o" + suffix) ||
                                Chars.endsWith(name, columnName + ".k" + suffix) ||
                                Chars.endsWith(name, columnName + ".c" + suffix) ||
                                Chars.endsWith(name, columnName + ".v" + suffix)
                ) {
                    if (counterRef.get() == CANNOT_DELETE) {
                        return false;
                    }
                    counterRef.incrementAndGet();
                }
                return super.remove(name);
            }

            @Override
            public boolean wasCalled() {
                return counterRef.get() > 0;
            }
        };
    }

    private static void freeBlob(long blob) {
        Unsafe.free(blob, blobLen, MemoryTag.NATIVE_DEFAULT);
    }

    private static Path getPath(String tableName) {
        TableToken tableToken = engine.verifyTableName(tableName);
        return new Path().of(engine.getConfiguration().getRoot()).concat(tableToken).concat(TableUtils.META_FILE_NAME).$();
    }

    private static String padHexLong(long value) {
        String s = Long.toHexString(value);
        if (s.length() % 2 == 0) {
            return s;
        }
        return "0" + s;
    }

    private void appendTwoSymbols(TableWriter writer, Rnd rnd, int index2) {
        for (int i = 0; i < 1000; i++) {
            TableWriter.Row row = writer.newRow();
            row.putSym(0, rnd.nextChars(10));
            row.putSym(index2, rnd.nextChars(15));
            row.append();
        }
    }

    private void assertBatch2(int count, long increment, long ts, long blob, TableReader reader) {
        RecordCursor cursor = reader.getCursor();
        Rnd exp = new Rnd();
        long ts2 = assertPartialCursor(cursor, exp, ts, increment, blob, 3L * count, (r, rnd13, ts13, blob13) -> {
            BATCH1_ASSERTER.assertRecord(r, rnd13, ts13, blob13);
            BATCH2_BEFORE_ASSERTER.assertRecord(r, rnd13, ts13, blob13);
        });
        assertPartialCursor(cursor, exp, ts2, increment, blob, count, BATCH2_ASSERTER);
    }

    private void assertBatch3(int count, long increment, long ts, long blob, TableReader reader) {
        Rnd exp = new Rnd();
        long ts2;
        RecordCursor cursor = reader.getCursor();
        ts2 = assertPartialCursor(cursor, exp, ts, increment, blob, 3L * count, (r, rnd1, ts1, blob1) -> {
            BATCH1_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH2_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH3_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
        });

        ts2 = assertPartialCursor(cursor, exp, ts2, increment, blob, count, (r, rnd12, ts12, blob12) -> {
            BATCH2_ASSERTER.assertRecord(r, rnd12, ts12, blob12);
            BATCH3_BEFORE_ASSERTER.assertRecord(r, rnd12, ts12, blob12);
        });

        assertPartialCursor(cursor, exp, ts2, increment, blob, count, BATCH3_ASSERTER);
    }

    private void assertBatch4(int count, long increment, long ts, long blob, TableReader reader) {
        Rnd exp;
        long ts2;
        exp = new Rnd();
        RecordCursor cursor = reader.getCursor();
        ts2 = assertPartialCursor(cursor, exp, ts, increment, blob, 3L * count, (r, rnd1, ts1, blob1) -> {
            BATCH1_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH2_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH3_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH4_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
        });

        ts2 = assertPartialCursor(cursor, exp, ts2, increment, blob, count, (r, rnd12, ts12, blob12) -> {
            BATCH2_ASSERTER.assertRecord(r, rnd12, ts12, blob12);
            BATCH3_BEFORE_ASSERTER.assertRecord(r, rnd12, ts12, blob12);
            BATCH4_BEFORE_ASSERTER.assertRecord(r, rnd12, ts12, blob12);
        });

        ts2 = assertPartialCursor(cursor, exp, ts2, increment, blob, count, (r, rnd14, ts14, blob14) -> {
            BATCH4_BEFORE_ASSERTER.assertRecord(r, rnd14, ts14, blob14);
            BATCH3_ASSERTER.assertRecord(r, rnd14, ts14, blob14);
        });

        assertPartialCursor(cursor, exp, ts2, increment, blob, count, BATCH4_ASSERTER);
    }

    private long assertBatch5(int count, long increment, long ts, long blob, RecordCursor cursor, Rnd exp) {
        long ts2;

        cursor.toTop();
        ts2 = assertPartialCursor(cursor, exp, ts, increment, blob, 3L * count, (r, rnd1, ts1, blob1) -> {
            BATCH1_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH2_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH3_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH5_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
        });

        ts2 = assertPartialCursor(cursor, exp, ts2, increment, blob, count, (r, rnd1, ts1, blob1) -> {
            BATCH2_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH3_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH5_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
        });

        ts2 = assertPartialCursor(cursor, exp, ts2, increment, blob, count, (r, rnd1, ts1, blob1) -> {
            BATCH5_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH3_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
        });

        return assertPartialCursor(cursor, exp, ts2, increment, blob, count, BATCH5_ASSERTER);
    }

    private void assertBatch6(int count, long increment, long ts, long blob, RecordCursor cursor) {
        Rnd exp;
        long ts2;
        exp = new Rnd();
        cursor.toTop();
        ts2 = assertBatch5(count, increment, ts, blob, cursor, exp);
        assertPartialCursor(cursor, exp, ts2, increment, blob, count, BATCH6_ASSERTER);
    }

    private void assertBatch7(int count, long increment, long ts, long blob, RecordCursor cursor) {
        cursor.toTop();
        Rnd exp = new Rnd();
        long ts2 = assertPartialCursor(cursor, exp, ts, increment, blob, 3L * count, (r, rnd1, ts1, blob1) -> {
            BATCH1_7_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH_2_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH_3_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH_4_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
        });

        ts2 = assertPartialCursor(cursor, exp, ts2, increment, blob, count, (r, rnd1, ts1, blob1) -> {
            BATCH2_7_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH_3_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH_4_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
        });

        ts2 = assertPartialCursor(cursor, exp, ts2, increment, blob, count, (r, rnd1, ts1, blob1) -> {
            BATCH3_7_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH_4_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
        });

        ts2 = assertPartialCursor(cursor, exp, ts2, increment, blob, count, BATCH5_7_ASSERTER);
        assertPartialCursor(cursor, exp, ts2, increment, blob, count, BATCH6_7_ASSERTER);
    }

    private void assertBatch8(int count, long increment, long ts, long blob, RecordCursor cursor) {
        cursor.toTop();
        Rnd exp = new Rnd();
        long ts2 = assertPartialCursor(cursor, exp, ts, increment, blob, 3L * count, (r, rnd1, ts1, blob1) -> {
            BATCH1_7_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH_2_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH_3_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH_4_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
        });

        ts2 = assertPartialCursor(cursor, exp, ts2, increment, blob, count, (r, rnd1, ts1, blob1) -> {
            BATCH2_7_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH_3_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH_4_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
        });

        ts2 = assertPartialCursor(cursor, exp, ts2, increment, blob, count, (r, rnd1, ts1, blob1) -> {
            BATCH3_7_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH_4_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
        });

        ts2 = assertPartialCursor(cursor, exp, ts2, increment, blob, count, BATCH5_7_ASSERTER);
        ts2 = assertPartialCursor(cursor, exp, ts2, increment, blob, count, BATCH6_7_ASSERTER);
        assertPartialCursor(cursor, exp, ts2, increment, blob, count, BATCH8_ASSERTER);
    }

    private void assertBatch9(int count, long increment, long ts, long blob, RecordCursor cursor) {
        cursor.toTop();
        Rnd exp = new Rnd();
        long ts2 = assertPartialCursor(cursor, exp, ts, increment, blob, 3L * count, (r, rnd1, ts1, blob1) -> {
            BATCH1_9_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH_2_9_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH_3_9_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH_4_9_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
        });

        ts2 = assertPartialCursor(cursor, exp, ts2, increment, blob, count, (r, rnd1, ts1, blob1) -> {
            BATCH2_9_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH_3_9_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH_4_9_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
        });

        ts2 = assertPartialCursor(cursor, exp, ts2, increment, blob, count, (r, rnd1, ts1, blob1) -> {
            BATCH3_9_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
            BATCH_4_9_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
        });

        ts2 = assertPartialCursor(cursor, exp, ts2, increment, blob, count, BATCH5_9_ASSERTER);
        ts2 = assertPartialCursor(cursor, exp, ts2, increment, blob, count, BATCH6_9_ASSERTER);
        ts2 = assertPartialCursor(cursor, exp, ts2, increment, blob, count, BATCH8_9_ASSERTER);
        assertPartialCursor(cursor, exp, ts2, increment, blob, count, BATCH9_ASSERTER);
    }

    private void assertCursor(RecordCursor cursor, long ts, long increment, long blob, long expectedSize, RecordAssert asserter) {
        Rnd rnd = new Rnd();
        final Record record = cursor.getRecord();
        cursor.toTop();
        int count = 0;
        long timestamp = ts;
        LongList rows = new LongList((int) expectedSize);

        while (count < expectedSize && cursor.hasNext()) {
            count++;
            asserter.assertRecord(record, rnd, timestamp += increment, blob);
            rows.add(record.getRowId());
        }
        // did our loop run?
        Assert.assertEquals(expectedSize, count);

        // assert rowid access, method 3
        rnd.reset();
        timestamp = ts;
        final Record rec = cursor.getRecordB();
        for (int i = 0; i < count; i++) {
            cursor.recordAt(rec, rows.getQuick(i));
            asserter.assertRecord(rec, rnd, timestamp += increment, blob);
        }
    }

    private long assertPartialCursor(RecordCursor cursor, Rnd rnd, long ts, long increment, long blob, long expectedSize, RecordAssert asserter) {
        int count = 0;
        Record record = cursor.getRecord();
        while (count < expectedSize && cursor.hasNext()) {
            count++;
            asserter.assertRecord(record, rnd, ts += increment, blob);
        }
        // did our loop run?
        Assert.assertEquals(expectedSize, count);
        return ts;
    }

    private TableToken createTable(String tableName, int partitionBy) {
        try (TableModel model = new TableModel(configuration, tableName, partitionBy)) {
            model.timestamp();
            return CreateTableTestUtils.create(model);
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
            Assert.assertFalse(isSamePartition(timestamp, writer.getMaxTimestamp(), writer.getPartitionBy()));
        } else if (testPartitionSwitch == MUST_NOT_SWITCH) {
            Assert.assertTrue(isSamePartition(timestamp, writer.getMaxTimestamp(), writer.getPartitionBy()));
        }

        Assert.assertEquals(size + count, writer.size());
        return ts;
    }

    private long testAppend(Rnd rnd, CairoConfiguration configuration, long ts, int count, long inc, long blob, int testPartitionSwitch) {
        try (TableWriter writer = newTableWriter(configuration, "all", metrics)) {
            return testAppend(writer, rnd, ts, count, inc, blob, testPartitionSwitch, TableReaderTest.BATCH1_GENERATOR);
        }
    }

    private void testAsyncColumnRename(AtomicInteger counterRef, TestFilesFacade ff, String columnName) throws Exception {
        assertMemoryLeak(ff, () -> {
            Rnd rnd = new Rnd();
            final int N = 1000;

            // populate table and delete column
            try (TableWriter writer = getWriter("x")) {
                appendTwoSymbols(writer, rnd, 1);
                writer.commit();

                try (TableReader reader = newTableReader(configuration, "x")) {
                    long counter = 0;

                    rnd.reset();
                    RecordCursor cursor = reader.getCursor();
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        Assert.assertEquals(rnd.nextChars(10), record.getSym(0));
                        Assert.assertEquals(rnd.nextChars(15), record.getSym(1));
                        counter++;
                    }

                    Assert.assertEquals(N, counter);

                    // this should write metadata without column "b" but will ignore
                    // file delete failures
                    writer.renameColumn(columnName, "d");

                    // now when we add new column by same name it must not pick up files we failed to delete previously
                    writer.addColumn(columnName, ColumnType.SYMBOL);

                    // SymbolMap must be cleared when we try to do add values to new column
                    appendTwoSymbols(writer, rnd, 2);
                    writer.commit();

                    // now assert what reader sees
                    Assert.assertTrue(reader.reload());
                    Assert.assertEquals(N * 2, reader.size());

                    rnd.reset();
                    cursor.toTop();
                    counter = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(rnd.nextChars(10), record.getSym(0));
                        if (counter < N) {
                            // roll random generator to make sure it returns same values
                            rnd.nextChars(15);
                            Assert.assertNull(record.getSym(2));
                        } else {
                            Assert.assertEquals(rnd.nextChars(15), record.getSym(2));
                        }
                        counter++;
                    }

                    Assert.assertEquals(N * 2, counter);
                }
            }
            engine.releaseInactive();
            checkColumnPurgeRemovesFiles(counterRef, ff, 6);
        });
    }

    private void testConcurrentReloadMultiplePartitions(int partitionBy, long stride) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int N = 1024_0000;

            // model table
            try (TableModel model = new TableModel(configuration, "w", partitionBy).col("l", ColumnType.LONG).timestamp()) {
                CreateTableTestUtils.create(model);
            }

            final int threads = 2;
            final CyclicBarrier startBarrier = new CyclicBarrier(threads);
            final SOCountDownLatch stopLatch = new SOCountDownLatch(threads);
            final AtomicInteger errors = new AtomicInteger(0);

            // start writer
            new Thread(() -> {
                try {
                    startBarrier.await();
                    long timestampUs = TimestampFormatUtils.parseTimestamp("2017-12-11T00:00:00.000Z");
                    try (TableWriter writer = newTableWriter(configuration, "w", metrics)) {
                        for (int i = 0; i < N; i++) {
                            TableWriter.Row row = writer.newRow(timestampUs);
                            row.putLong(0, i);
                            row.append();
                            writer.commit();
                            timestampUs += stride;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    errors.incrementAndGet();
                } finally {
                    stopLatch.countDown();
                }
            }).start();

            // start reader
            new Thread(() -> {
                try {
                    startBarrier.await();
                    try (TableReader reader = newTableReader(configuration, "w")) {
                        RecordCursor cursor = reader.getCursor();
                        final Record record = cursor.getRecord();
                        sink.clear();
                        ((Sinkable) record).toSink(sink);
                        TestUtils.assertEquals("TableReaderRecord [columnBase=0, recordIndex=-1]", sink);
                        do {
                            // we deliberately ignore result of reload()
                            // to create more race conditions
                            reader.reload();
                            cursor.toTop();
                            int count = 0;
                            while (cursor.hasNext()) {
                                if (count++ != record.getLong(0)) {
                                    sink.clear();
                                    sink.put("Test [count=").put(count--).put(", rec=").put(record.getLong(0)).put("],");
                                    ((Sinkable) record).toSink(sink);
                                    Assert.fail(sink.toString());
                                }
                            }

                            if (count == N) {
                                break;
                            }
                        } while (true);
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    errors.incrementAndGet();
                } finally {
                    stopLatch.countDown();
                }
            }).start();

            stopLatch.await();
            Assert.assertEquals(0, errors.get());

            // check that we had multiple partitions created during the test
            try (TableReader reader = newTableReader(configuration, "w")) {
                Assert.assertTrue(reader.getPartitionCount() > 10);
            }
        });
    }

    private void testReload(int partitionBy, int count, long inct, final int testPartitionSwitch) throws Exception {
        final long increment = inct * 1000;

        CreateTableTestUtils.createAllTable(engine, partitionBy);

        TestUtils.assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();

            long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z");

            long blob = allocBlob();
            try {
                // test if reader behaves correctly when table is empty

                try (TableReader reader = newTableReader(configuration, "all")) {
                    // can we reload empty table?
                    Assert.assertFalse(reader.reload());
                    // reader can see all the rows ? Meaning none?
                    assertCursor(reader.getCursor(), ts, increment, blob, 0, null);
                }

                try (TableReader reader = newTableReader(configuration, "all")) {
                    RecordCursor cursor = reader.getCursor();
                    // this combination of reload/iterate/reload is deliberate
                    // we make sure that reload() behavior is not affected by
                    // iterating empty result set
                    Assert.assertFalse(reader.reload());
                    assertCursor(cursor, ts, increment, blob, 0, null);
                    Assert.assertFalse(reader.reload());
                    assertOpenPartitionCount(reader);

                    // create table with first batch populating all columns (there could be null values too)
                    long nextTs = testAppend(rnd, configuration, ts, count, increment, blob, 0);

                    // can we reload from empty to first batch?
                    Assert.assertTrue(reader.reload());
                    assertOpenPartitionCount(reader);

                    // make sure we can see first batch right after table is open
                    assertCursor(cursor, ts, increment, blob, count, BATCH1_ASSERTER);

                    // create another reader to make sure it can load data from constructor
                    try (TableReader reader2 = newTableReader(configuration, "all")) {
                        // make sure we can see first batch right after table is open
                        assertCursor(reader2.getCursor(), ts, increment, blob, count, BATCH1_ASSERTER);
                    }

                    // try to reload when table hasn't changed
                    Assert.assertFalse(reader.reload());
                    assertOpenPartitionCount(reader);

                    // add second batch to test if reload of open table will pick it up
                    nextTs = testAppend(rnd, configuration, nextTs, count, increment, blob, testPartitionSwitch);

                    // if we don't reload reader it should still see first batch
                    // reader can see all the rows ?
                    cursor.toTop();
                    assertPartialCursor(cursor, new Rnd(), ts, increment, blob, count / 4, BATCH1_ASSERTER);

                    // reload should be successful because we have new data in the table
                    Assert.assertTrue(reader.reload());
                    assertOpenPartitionCount(reader);

                    // check if we can see second batch after reader was reloaded
                    assertCursor(cursor, ts, increment, blob, 2L * count, BATCH1_ASSERTER);

                    // writer will inflate last partition in order to optimise appends
                    // reader must be able to cope with that
                    try (TableWriter writer = newTableWriter(configuration, "all", metrics)) {

                        // this is a bit of paranoid check, but make sure our reader doesn't flinch when new writer is open
                        assertCursor(cursor, ts, increment, blob, 2L * count, BATCH1_ASSERTER);

                        // also make sure that there is nothing to reload, we've not done anything to data after all
                        Assert.assertFalse(reader.reload());
                        assertOpenPartitionCount(reader);

                        // check that we can still see two batches after no-op reload
                        // we rule out possibility of reload() corrupting table state
                        assertCursor(cursor, ts, increment, blob, 2L * count, BATCH1_ASSERTER);

                        // just for no reason add third batch
                        nextTs = testAppend(writer, rnd, nextTs, count, increment, blob, 0, BATCH1_GENERATOR);

                        // table must be able to reload now
                        Assert.assertTrue(reader.reload());
                        assertOpenPartitionCount(reader);

                        // and we should see three batches of data
                        assertCursor(cursor, ts, increment, blob, 3L * count, BATCH1_ASSERTER);

                        // this is where things get interesting
                        // add single column
                        writer.addColumn("str2", ColumnType.STRING);

                        // populate table with fourth batch, this time we also populate new column
                        // we expect that values of new column will be NULL for first three batches and non-NULL for fourth
                        nextTs = testAppend(writer, rnd, nextTs, count, increment, blob, 0, BATCH2_GENERATOR);

                        // reload table, check if it was positive effort
                        Assert.assertTrue(reader.reload());
                        assertOpenPartitionCount(reader);

                        // two-step assert checks 3/4 rows checking that new column is NUL
                        // the last 1/3 is checked including new column
                        // this is why we need to use same random state and timestamp
                        assertBatch2(count, increment, ts, blob, reader);

                        // good job we got as far as this
                        // lets now add another column and populate fifth batch, including new column
                        // reading this table will ensure tops are preserved

                        writer.addColumn("int2", ColumnType.INT);

                        nextTs = testAppend(writer, rnd, nextTs, count, increment, blob, 0, BATCH3_GENERATOR);

                        Assert.assertTrue(reader.reload());
                        assertOpenPartitionCount(reader);

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
                        assertOpenPartitionCount(reader);

                        assertBatch4(count, increment, ts, blob, reader);

                        // now delete last column
                        writer.removeColumn("bin2");

                        Assert.assertTrue(reader.reload());
                        assertOpenPartitionCount(reader);

                        // and assert that all columns that have not been deleted contain correct values

                        assertBatch5(count, increment, ts, blob, cursor, new Rnd());

                        // append all columns excluding the one we just deleted
                        nextTs = testAppend(writer, rnd, nextTs, count, increment, blob, 0, BATCH6_GENERATOR);

                        Assert.assertTrue(reader.reload());
                        assertOpenPartitionCount(reader);

                        // and assert that all columns that have not been deleted contain correct values
                        assertBatch6(count, increment, ts, blob, cursor);

                        // remove first column and add new column by same name
                        writer.removeColumn("int");
                        writer.addColumn("int", ColumnType.INT);

                        Assert.assertTrue(reader.reload());
                        assertOpenPartitionCount(reader);

                        assertBatch7(count, increment, ts, blob, cursor);

                        Assert.assertFalse(reader.reload());

                        nextTs = testAppend(writer, rnd, nextTs, count, increment, blob, 0, BATCH8_GENERATOR);

                        Assert.assertTrue(reader.reload());
                        assertOpenPartitionCount(reader);

                        assertBatch8(count, increment, ts, blob, cursor);

                        writer.removeColumn("sym");
                        writer.addColumn("sym", ColumnType.SYMBOL);
                        Assert.assertTrue(reader.reload());
                        assertOpenPartitionCount(reader);

                        testAppend(writer, rnd, nextTs, count, increment, blob, 0, BATCH9_GENERATOR);

                        Assert.assertTrue(reader.reload());
                        assertOpenPartitionCount(reader);

                        assertBatch9(count, increment, ts, blob, cursor);
                    }
                }
            } finally {
                freeBlob(blob);
            }
        });
    }

    private void testRemoveActivePartition(int partitionBy, NextPartitionTimestampProvider provider, CharSequence partitionNameToDelete) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int N = 100;
            final int N_PARTITIONS = 5;
            final long stride = 100;
            final int bandStride = 1000;
            final String tableName = "table_by_" + PartitionBy.toString(partitionBy);

            int totalCount = 0;
            long timestampUs = TimestampFormatUtils.parseTimestamp("2017-12-11T00:00:00.000Z");

            // model table
            try (TableModel model = new TableModel(configuration, tableName, partitionBy).col("l", ColumnType.LONG).timestamp()) {
                CreateTableTestUtils.create(model);
            }

            try (TableWriter writer = newTableWriter(configuration, tableName, metrics)) {

                for (int k = 0; k < N_PARTITIONS; k++) {
                    long band = k * bandStride;
                    for (int i = 0; i < N; i++) {
                        TableWriter.Row row = writer.newRow(timestampUs);
                        row.putLong(0, band + i);
                        row.append();
                        timestampUs += stride;
                    }
                    writer.commit();
                    timestampUs = provider.getNext(timestampUs);
                }

                final int expectedSize = N_PARTITIONS * N;
                Assert.assertEquals(expectedSize, writer.size());


                // now open table reader having partition gap
                try (TableReader reader = newTableReader(configuration, tableName)) {

                    Assert.assertEquals(expectedSize, reader.size());
                    RecordCursor cursor = reader.getCursor();
                    Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        record.getLong(0);
                        totalCount++;
                    }
                    Assert.assertEquals(expectedSize, totalCount);

                    DateFormat fmt = PartitionBy.getPartitionDirFormatMethod(partitionBy);
                    Assert.assertTrue(
                            // active partition
                            writer.removePartition(fmt.parse(partitionNameToDelete, null))
                    );

                    // check writer
                    final long newExpectedSize = (N_PARTITIONS - 1) * N;
                    Assert.assertEquals(newExpectedSize, writer.size());

                    // check reader
                    reader.reload();
                    totalCount = 0;
                    Assert.assertEquals(newExpectedSize, reader.size());
                    cursor = reader.getCursor();
                    record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        record.getLong(0);
                        totalCount++;
                    }
                    Assert.assertEquals(newExpectedSize, totalCount);
                }
            }
        });
    }

    private void testRemovePartition(int partitionBy, CharSequence partitionNameToDelete, int affectedBand, NextPartitionTimestampProvider provider) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 100;
            int N_PARTITIONS = 5;
            long timestampUs = TimestampFormatUtils.parseTimestamp("2017-12-11T10:00:00.000Z");
            long stride = 100;
            int bandStride = 1000;
            int totalCount = 0;

            // model table
            try (TableModel model = new TableModel(configuration, "w", partitionBy).col("l", ColumnType.LONG).timestamp()) {
                CreateTableTestUtils.create(model);
            }

            try (TableWriter writer = newTableWriter(configuration, "w", metrics)) {

                for (int k = 0; k < N_PARTITIONS; k++) {
                    long band = k * bandStride;
                    for (int i = 0; i < N; i++) {
                        TableWriter.Row row = writer.newRow(timestampUs);
                        row.putLong(0, band + i);
                        row.append();
                        writer.commit();
                        timestampUs += stride;
                    }
                    timestampUs = provider.getNext(timestampUs);
                }

                Assert.assertEquals(N * N_PARTITIONS, writer.size());

                DateFormat fmt = PartitionBy.getPartitionDirFormatMethod(partitionBy);
                final long timestamp = fmt.parse(partitionNameToDelete, null);

                Assert.assertTrue(writer.removePartition(timestamp));
                Assert.assertFalse(writer.removePartition(timestamp));

                Assert.assertEquals(N * (N_PARTITIONS - 1), writer.size());
            }

            // now open table reader having partition gap
            try (TableReader reader = newTableReader(configuration, "w")) {

                Assert.assertEquals(N * (N_PARTITIONS - 1), reader.size());

                int previousBand = -1;
                int bandCount = 0;
                RecordCursor cursor = reader.getCursor();
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    long value = record.getLong(0);
                    int band = (int) ((value / bandStride) * bandStride);
                    if (band != previousBand) {
                        // make sure we don#t pick up deleted partition
                        Assert.assertNotEquals(affectedBand, band);
                        if (previousBand != -1) {
                            Assert.assertEquals(N, bandCount);
                        }
                        previousBand = band;
                        bandCount = 0;
                    }
                    bandCount++;
                    totalCount++;
                }
                Assert.assertEquals(N, bandCount);
            }

            Assert.assertEquals(N * (N_PARTITIONS - 1), totalCount);
        });
    }

    private void testRemovePartitionReload(int partitionBy, CharSequence partitionNameToDelete, int affectedBand, NextPartitionTimestampProvider provider) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int N = 100;
            int N_PARTITIONS = 5;
            long timestampUs = TimestampFormatUtils.parseTimestamp("2017-12-11T00:00:00.000Z");
            long stride = 100;
            int bandStride = 1000;
            int totalCount = 0;

            // model table
            try (TableModel model = new TableModel(configuration, "w", partitionBy).col("l", ColumnType.LONG).timestamp()) {
                CreateTableTestUtils.create(model);
            }

            try (TableWriter writer = newTableWriter(configuration, "w", metrics)) {

                for (int k = 0; k < N_PARTITIONS; k++) {
                    long band = k * bandStride;
                    for (int i = 0; i < N; i++) {
                        TableWriter.Row row = writer.newRow(timestampUs);
                        row.putLong(0, band + i);
                        row.append();
                        writer.commit();
                        timestampUs += stride;
                    }
                    timestampUs = provider.getNext(timestampUs);
                }

                Assert.assertEquals(N * N_PARTITIONS, writer.size());

                // now open table reader having partition gap
                try (TableReader reader = newTableReader(configuration, "w")) {

                    Assert.assertEquals(N * N_PARTITIONS, reader.size());
                    RecordCursor cursor = reader.getCursor();
                    Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        record.getLong(0);
                        totalCount++;
                    }
                    Assert.assertEquals(N * N_PARTITIONS, totalCount);


                    DateFormat fmt = PartitionBy.getPartitionDirFormatMethod(partitionBy);
                    Assert.assertTrue(
                            writer.removePartition(fmt.parse(partitionNameToDelete, null))
                    );

                    Assert.assertEquals(N * (N_PARTITIONS - 1), writer.size());

                    reader.reload();

                    totalCount = 0;

                    Assert.assertEquals(N * (N_PARTITIONS - 1), reader.size());

                    int previousBand = -1;
                    int bandCount = 0;
                    cursor = reader.getCursor();
                    record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        long value = record.getLong(0);
                        int band = (int) ((value / bandStride) * bandStride);
                        if (band != previousBand) {
                            // make sure we don#t pick up deleted partition
                            Assert.assertNotEquals(affectedBand, band);
                            if (previousBand != -1) {
                                Assert.assertEquals(N, bandCount);
                            }
                            previousBand = band;
                            bandCount = 0;
                        }
                        bandCount++;
                        totalCount++;
                    }
                    Assert.assertEquals(N, bandCount);
                }
            }

            Assert.assertEquals(N * (N_PARTITIONS - 1), totalCount);
        });
    }

    private void testRemoveSymbol(AtomicInteger counterRef, TestFilesFacade ff, String columnName, int deleteAttempts) throws Exception {
        Rnd rnd = new Rnd();
        final int N = 1000;
        assertMemoryLeak(ff, () -> {

            // populate table and delete column
            try (TableWriter writer = getWriter("x")) {
                appendTwoSymbols(writer, rnd, 1);
                writer.commit();

                try (TableReader reader = getReader("x")) {
                    long counter = 0;

                    rnd.reset();
                    RecordCursor cursor = reader.getCursor();
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        Assert.assertEquals(rnd.nextChars(10), record.getSym(0));
                        Assert.assertEquals(rnd.nextChars(15), record.getSym(1));
                        counter++;
                    }

                    Assert.assertEquals(N, counter);

                    // this should write metadata without column "b" but will ignore
                    // file delete failures
                    writer.removeColumn(columnName);

                    Assert.assertTrue(reader.reload());

                    Assert.assertEquals(N, reader.size());

                    rnd.reset();
                    cursor.toTop();
                    counter = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(rnd.nextChars(10), record.getSym(0));
                        // roll random generator to make sure it returns same values
                        rnd.nextChars(15);
                        counter++;
                    }

                    Assert.assertEquals(N, counter);
                }
            }

            checkColumnPurgeRemovesFiles(counterRef, ff, deleteAttempts);
        });
    }

    private void testTableCursor(long inc) throws NumericException {
        Rnd rnd = new Rnd();
        int N = 100;
        long ts = TimestampFormatUtils.parseTimestamp("2013-03-04T00:00:00.000Z") / 1000;
        long blob = allocBlob();
        try {
            testAppend(rnd, configuration, ts, N, inc, blob, 0);
            final LongList rows = new LongList();
            try (TableReader reader = newTableReader(configuration, "all")) {
                Assert.assertEquals(N, reader.size());

                RecordCursor cursor = reader.getCursor();
                final Record record = cursor.getRecord();
                assertCursor(cursor, ts, inc, blob, N, BATCH1_ASSERTER);

                cursor.toTop();
                while (cursor.hasNext()) {
                    rows.add(record.getRowId());
                }

                Rnd exp = new Rnd();
                final Record rec = cursor.getRecordB();
                for (int i = 0, n = rows.size(); i < n; i++) {
                    cursor.recordAt(rec, rows.getQuick(i));
                    BATCH1_ASSERTER.assertRecord(rec, exp, ts += inc, blob);
                }
            }
        } finally {
            freeBlob(blob);
        }
    }

    private void testTableCursor() throws NumericException {
        testTableCursor(60 * 60000);
    }

    static void assertOpenPartitionCount(TableReader reader) {
        Assert.assertEquals(reader.calculateOpenPartitionCount(), reader.getOpenPartitionCount());
    }

    static boolean isSamePartition(long timestampA, long timestampB, int partitionBy) {
        switch (partitionBy) {
            case PartitionBy.NONE:
                return true;
            case PartitionBy.DAY:
                return Timestamps.floorDD(timestampA) == Timestamps.floorDD(timestampB);
            case PartitionBy.MONTH:
                return Timestamps.floorMM(timestampA) == Timestamps.floorMM(timestampB);
            case PartitionBy.WEEK:
                return Timestamps.floorWW(timestampA) == Timestamps.floorWW(timestampB);
            case PartitionBy.YEAR:
                return Timestamps.floorYYYY(timestampA) == Timestamps.floorYYYY(timestampB);
            case PartitionBy.HOUR:
                return Timestamps.floorHH(timestampA) == Timestamps.floorHH(timestampB);
            default:
                throw CairoException.critical(0).put("Cannot compare timestamps for unsupported partition type: [").put(partitionBy).put(']');
        }
    }

    @FunctionalInterface
    private interface FieldGenerator {
        void generate(TableWriter.Row r, Rnd rnd, long ts, long blob);
    }

    @FunctionalInterface
    private interface NextPartitionTimestampProvider {
        long getNext(long current);
    }

    private interface RecordAssert {
        void assertRecord(Record r, Rnd rnd, long ts, long blob);
    }
}
