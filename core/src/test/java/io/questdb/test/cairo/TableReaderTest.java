/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.vm.NullMemoryCMR;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.griffin.SqlException;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.std.datetime.DateLocaleFactory.EN_LOCALE;

public class TableReaderTest extends AbstractCairoTest {
    public static final int DO_NOT_CARE = 0;
    public static final int MUST_NOT_SWITCH = 2;
    public static final int MUST_SWITCH = 1;
    private static final RecordAssert BATCH2_BEFORE_ASSERTER = (r, rnd, ts, blob) -> assertNullStr(r, 13);
    private static final RecordAssert BATCH3_BEFORE_ASSERTER = (r, rnd, ts, blob) -> Assert.assertEquals(Numbers.INT_NULL, r.getInt(14));
    private static final RecordAssert BATCH4_BEFORE_ASSERTER = (r, rnd, ts, blob) -> {
        Assert.assertEquals(0, r.getShort(15));
        Assert.assertFalse(r.getBool(16));
        Assert.assertEquals(0, r.getByte(17));
        Assert.assertTrue(Float.isNaN(r.getFloat(18)));
        Assert.assertTrue(Double.isNaN(r.getDouble(19)));
        Assert.assertNull(r.getSymA(20));
        Assert.assertEquals(Numbers.LONG_NULL, r.getLong(21));
        Assert.assertEquals(Numbers.LONG_NULL, r.getDate(22));
        Assert.assertNull(r.getBin(23));
        Assert.assertEquals(TableUtils.NULL_LEN, r.getBinLen(23));
    };
    private static final RecordAssert BATCH5_BEFORE_ASSERTER = (r, rnd, ts, blob) -> {
        Assert.assertEquals(0, r.getShort(15));
        Assert.assertFalse(r.getBool(16));
        Assert.assertEquals(0, r.getByte(17));
        Assert.assertTrue(Float.isNaN(r.getFloat(18)));
        Assert.assertTrue(Double.isNaN(r.getDouble(19)));
        Assert.assertNull(r.getSymA(20));
        Assert.assertEquals(Numbers.LONG_NULL, r.getLong(21));
        Assert.assertEquals(Numbers.LONG_NULL, r.getDate(22));
    };
    private static final RecordAssert BATCH_2_7_BEFORE_ASSERTER = (r, rnd, ts, blob) -> assertNullStr(r, 12);
    private static final RecordAssert BATCH_2_9_BEFORE_ASSERTER = (r, rnd, ts, blob) -> assertNullStr(r, 11);
    private static final RecordAssert BATCH_3_7_BEFORE_ASSERTER = (r, rnd, ts, blob) -> Assert.assertEquals(Numbers.INT_NULL, r.getInt(13));
    private static final RecordAssert BATCH_3_9_BEFORE_ASSERTER = (r, rnd, ts, blob) -> Assert.assertEquals(Numbers.INT_NULL, r.getInt(12));
    private static final RecordAssert BATCH_4_7_BEFORE_ASSERTER = (r, rnd, ts, blob) -> {
        Assert.assertEquals(0, r.getShort(14));
        Assert.assertFalse(r.getBool(15));
        Assert.assertEquals(0, r.getByte(16));
        Assert.assertTrue(Float.isNaN(r.getFloat(17)));
        Assert.assertTrue(Double.isNaN(r.getDouble(18)));
        Assert.assertNull(r.getSymA(19));
        Assert.assertEquals(Numbers.LONG_NULL, r.getLong(20));
        Assert.assertEquals(Numbers.LONG_NULL, r.getDate(21));
    };
    private static final RecordAssert BATCH_4_9_BEFORE_ASSERTER = (r, rnd, ts, blob) -> {
        Assert.assertEquals(0, r.getShort(13));
        Assert.assertFalse(r.getBool(14));
        Assert.assertEquals(0, r.getByte(15));
        Assert.assertTrue(Float.isNaN(r.getFloat(16)));
        Assert.assertTrue(Double.isNaN(r.getDouble(17)));
        Assert.assertNull(r.getSymA(18));
        Assert.assertEquals(Numbers.LONG_NULL, r.getLong(19));
        Assert.assertEquals(Numbers.LONG_NULL, r.getDate(20));
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
            Assert.assertEquals(Numbers.INT_NULL, r.getInt(0));
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
            Assert.assertEquals(Numbers.LONG_NULL, r.getLong(5));
        }

        if (exp.nextBoolean()) {
            Assert.assertEquals(ts, r.getDate(10));
        } else {
            Assert.assertEquals(Numbers.LONG_NULL, r.getDate(10));
        }

        assertBin(r, exp, blob, 9);

        if (exp.nextBoolean()) {
            assertStrColumn(exp.nextChars(10), r, 6);
        } else {
            assertNullStr(r, 6);
        }

        if (exp.nextBoolean()) {
            TestUtils.assertEquals(exp.nextChars(7), r.getSymA(7));
        } else {
            Assert.assertNull(r.getSymA(7));
        }
    };
    private static final RecordAssert BATCH2_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH1_ASSERTER.assertRecord(r, rnd, ts, blob);
        if ((rnd.nextPositiveInt() & 3) == 0) {
            assertStrColumn(rnd.nextChars(15), r, 13);
        }
    };
    private static final RecordAssert BATCH3_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH2_ASSERTER.assertRecord(r, rnd, ts, blob);

        if ((rnd.nextPositiveInt() & 3) == 0) {
            Assert.assertEquals(rnd.nextInt(), r.getInt(14));
        }
    };
    private static final RecordAssert BATCH4_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH3_ASSERTER.assertRecord(r, rnd, ts, blob);

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextShort(), r.getShort(15));
        } else {
            Assert.assertEquals(0, r.getShort(15));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextBoolean(), r.getBool(16));
        } else {
            Assert.assertFalse(r.getBool(16));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextByte(), r.getByte(17));
        } else {
            Assert.assertEquals(0, r.getByte(17));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextFloat(), r.getFloat(18), 0.00000001f);
        } else {
            Assert.assertTrue(Float.isNaN(r.getFloat(18)));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextDouble(), r.getDouble(19), 0.0000001d);
        } else {
            Assert.assertTrue(Double.isNaN(r.getDouble(19)));
        }

        if (rnd.nextBoolean()) {
            TestUtils.assertEquals(rnd.nextChars(10), r.getSymA(20));
        } else {
            Assert.assertNull(r.getSymA(20));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getLong(21));
        } else {
            Assert.assertEquals(Numbers.LONG_NULL, r.getLong(21));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getDate(22));
        } else {
            Assert.assertEquals(Numbers.LONG_NULL, r.getDate(22));
        }

        assertBin(r, rnd, blob, 23);
    };
    private static final RecordAssert BATCH6_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH3_ASSERTER.assertRecord(r, rnd, ts, blob);

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextShort(), r.getShort(15));
        } else {
            Assert.assertEquals(0, r.getShort(15));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextBoolean(), r.getBool(16));
        } else {
            Assert.assertFalse(r.getBool(16));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextByte(), r.getByte(17));
        } else {
            Assert.assertEquals(0, r.getByte(17));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextFloat(), r.getFloat(18), 0.00000001f);
        } else {
            Assert.assertTrue(Float.isNaN(r.getFloat(18)));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextDouble(), r.getDouble(19), 0.0000001d);
        } else {
            Assert.assertTrue(Double.isNaN(r.getDouble(19)));
        }

        if (rnd.nextBoolean()) {
            TestUtils.assertEquals(rnd.nextChars(10), r.getSymA(20));
        } else {
            Assert.assertNull(r.getSymA(20));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getLong(21));
        } else {
            Assert.assertEquals(Numbers.LONG_NULL, r.getLong(21));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getDate(22));
        } else {
            Assert.assertEquals(Numbers.LONG_NULL, r.getDate(22));
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
            Assert.assertEquals(Numbers.LONG_NULL, r.getLong(4));
        }

        if (exp.nextBoolean()) {
            Assert.assertEquals(ts, r.getDate(9));
        } else {
            Assert.assertEquals(Numbers.LONG_NULL, r.getDate(9));
        }

        assertBin(r, exp, blob, 8);

        if (exp.nextBoolean()) {
            assertStrColumn(exp.nextChars(10), r, 5);
        } else {
            assertNullStr(r, 5);
        }

        if (exp.nextBoolean()) {
            TestUtils.assertEquals(exp.nextChars(7), r.getSymA(6));
        } else {
            Assert.assertNull(r.getSymA(6));
        }

        Assert.assertEquals(Numbers.INT_NULL, r.getInt(22));
    };
    private static final RecordAssert BATCH2_7_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH1_7_ASSERTER.assertRecord(r, rnd, ts, blob);
        if ((rnd.nextPositiveInt() & 3) == 0) {
            assertStrColumn(rnd.nextChars(15), r, 12);
        }
    };
    private static final RecordAssert BATCH3_7_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH2_7_ASSERTER.assertRecord(r, rnd, ts, blob);

        if ((rnd.nextPositiveInt() & 3) == 0) {
            Assert.assertEquals(rnd.nextInt(), r.getInt(13));
        }
    };
    private static final RecordAssert BATCH6_7_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH3_7_ASSERTER.assertRecord(r, rnd, ts, blob);

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextShort(), r.getShort(14));
        } else {
            Assert.assertEquals(0, r.getShort(14));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextBoolean(), r.getBool(15));
        } else {
            Assert.assertFalse(r.getBool(15));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextByte(), r.getByte(16));
        } else {
            Assert.assertEquals(0, r.getByte(16));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextFloat(), r.getFloat(17), 0.00000001f);
        } else {
            Assert.assertTrue(Float.isNaN(r.getFloat(17)));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextDouble(), r.getDouble(18), 0.0000001d);
        } else {
            Assert.assertTrue(Double.isNaN(r.getDouble(18)));
        }

        if (rnd.nextBoolean()) {
            TestUtils.assertEquals(rnd.nextChars(10), r.getSymA(19));
        } else {
            Assert.assertNull(r.getSymA(19));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getLong(20));
        } else {
            Assert.assertEquals(Numbers.LONG_NULL, r.getLong(20));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getDate(21));
        } else {
            Assert.assertEquals(Numbers.LONG_NULL, r.getDate(21));
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
            Assert.assertEquals(Numbers.LONG_NULL, r.getLong(4));
        }

        if (exp.nextBoolean()) {
            Assert.assertEquals(ts, r.getDate(8));
        } else {
            Assert.assertEquals(Numbers.LONG_NULL, r.getDate(8));
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
        Assert.assertEquals(Numbers.INT_NULL, r.getInt(21));
    };
    private static final RecordAssert BATCH2_9_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH1_9_ASSERTER.assertRecord(r, rnd, ts, blob);
        if ((rnd.nextPositiveInt() & 3) == 0) {
            assertStrColumn(rnd.nextChars(15), r, 11);
        }
    };
    private static final RecordAssert BATCH3_9_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH2_9_ASSERTER.assertRecord(r, rnd, ts, blob);

        if ((rnd.nextPositiveInt() & 3) == 0) {
            Assert.assertEquals(rnd.nextInt(), r.getInt(12));
        }
    };
    private static final RecordAssert BATCH6_9_ASSERTER = (r, rnd, ts, blob) -> {
        BATCH3_9_ASSERTER.assertRecord(r, rnd, ts, blob);

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
            TestUtils.assertEquals(rnd.nextChars(10), r.getSymA(18));
        } else {
            Assert.assertNull(r.getSymA(18));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getLong(19));
        } else {
            Assert.assertEquals(Numbers.LONG_NULL, r.getLong(19));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getDate(20));
        } else {
            Assert.assertEquals(Numbers.LONG_NULL, r.getDate(20));
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
            r1.putStr(13, rnd1.nextChars(15));
        }
    };
    private static final FieldGenerator BATCH3_GENERATOR = (r1, rnd1, ts1, blob1) -> {
        BATCH2_GENERATOR.generate(r1, rnd1, ts1, blob1);

        if ((rnd1.nextPositiveInt() & 3) == 0) {
            r1.putInt(14, rnd1.nextInt());
        }
    };
    private static final FieldGenerator BATCH4_GENERATOR = (r, rnd, ts, blob) -> {
        BATCH3_GENERATOR.generate(r, rnd, ts, blob);

        if (rnd.nextBoolean()) {
            r.putShort(15, rnd.nextShort());
        }

        if (rnd.nextBoolean()) {
            r.putBool(16, rnd.nextBoolean());
        }

        if (rnd.nextBoolean()) {
            r.putByte(17, rnd.nextByte());
        }

        if (rnd.nextBoolean()) {
            r.putFloat(18, rnd.nextFloat());
        }

        if (rnd.nextBoolean()) {
            r.putDouble(19, rnd.nextDouble());
        }

        if (rnd.nextBoolean()) {
            r.putSym(20, rnd.nextChars(10));
        }

        if (rnd.nextBoolean()) {
            r.putLong(21, rnd.nextLong());
        }

        if (rnd.nextBoolean()) {
            r.putDate(22, rnd.nextLong());
        }

        if (rnd.nextBoolean()) {
            rnd.nextChars(blob, blobLen / 2);
            r.putBin(23, blob, blobLen);
        }
    };
    private static final FieldGenerator BATCH6_GENERATOR = (r, rnd, ts, blob) -> {
        BATCH3_GENERATOR.generate(r, rnd, ts, blob);

        if (rnd.nextBoolean()) {
            r.putShort(15, rnd.nextShort());
        }

        if (rnd.nextBoolean()) {
            r.putBool(16, rnd.nextBoolean());
        }

        if (rnd.nextBoolean()) {
            r.putByte(17, rnd.nextByte());
        }

        if (rnd.nextBoolean()) {
            r.putFloat(18, rnd.nextFloat());
        }

        if (rnd.nextBoolean()) {
            r.putDouble(19, rnd.nextDouble());
        }

        if (rnd.nextBoolean()) {
            r.putSym(20, rnd.nextChars(10));
        }

        if (rnd.nextBoolean()) {
            r.putLong(21, rnd.nextLong());
        }

        if (rnd.nextBoolean()) {
            r.putDate(22, rnd.nextLong());
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
            Assert.assertEquals(Numbers.LONG_NULL, r.getLong(4));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(ts, r.getDate(9));
        } else {
            Assert.assertEquals(Numbers.LONG_NULL, r.getDate(9));
        }

        assertBin(r, rnd, blob, 8);

        if (rnd.nextBoolean()) {
            assertStrColumn(rnd.nextChars(10), r, 5);
        } else {
            assertNullStr(r, 5);
        }

        if (rnd.nextBoolean()) {
            TestUtils.assertEquals(rnd.nextChars(7), r.getSymA(6));
        } else {
            Assert.assertNull(r.getSymA(6));
        }

        if ((rnd.nextPositiveInt() & 3) == 0) {
            assertStrColumn(rnd.nextChars(15), r, 12);
        } else {
            assertNullStr(r, 12);
        }

        if ((rnd.nextPositiveInt() & 3) == 0) {
            Assert.assertEquals(rnd.nextInt(), r.getInt(13));
        } else {
            Assert.assertEquals(Numbers.INT_NULL, r.getInt(13));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextShort(), r.getShort(14));
        } else {
            Assert.assertEquals(0, r.getShort(14));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextBoolean(), r.getBool(15));
        } else {
            Assert.assertFalse(r.getBool(15));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextByte(), r.getByte(16));
        } else {
            Assert.assertEquals(0, r.getByte(16));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextFloat(), r.getFloat(17), 0.000001f);
        } else {
            Assert.assertTrue(Float.isNaN(r.getFloat(17)));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextDouble(), r.getDouble(18), 0.0000001d);
        } else {
            Assert.assertTrue(Double.isNaN(r.getDouble(18)));
        }

        if (rnd.nextBoolean()) {
            TestUtils.assertEquals(rnd.nextChars(10), r.getSymA(19));
        } else {
            Assert.assertNull(r.getSymA(19));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getLong(20));
        } else {
            Assert.assertEquals(Numbers.LONG_NULL, r.getLong(20));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getDate(21));
        } else {
            Assert.assertEquals(Numbers.LONG_NULL, r.getDate(21));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextInt(), r.getInt(22));
        } else {
            Assert.assertEquals(Numbers.INT_NULL, r.getInt(22));
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
            Assert.assertEquals(Numbers.LONG_NULL, r.getLong(4));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(ts, r.getDate(8));
        } else {
            Assert.assertEquals(Numbers.LONG_NULL, r.getDate(8));
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
            assertStrColumn(rnd.nextChars(15), r, 11);
        } else {
            assertNullStr(r, 11);
        }

        if ((rnd.nextPositiveInt() & 3) == 0) {
            Assert.assertEquals(rnd.nextInt(), r.getInt(12));
        } else {
            Assert.assertEquals(Numbers.INT_NULL, r.getInt(12));
        }

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
            Assert.assertEquals(rnd.nextFloat(), r.getFloat(16), 0.000001f);
        } else {
            Assert.assertTrue(Float.isNaN(r.getFloat(16)));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextDouble(), r.getDouble(17), 0.0000001d);
        } else {
            Assert.assertTrue(Double.isNaN(r.getDouble(17)));
        }

        if (rnd.nextBoolean()) {
            TestUtils.assertEquals(rnd.nextChars(10), r.getSymA(18));
        } else {
            Assert.assertNull(r.getSymA(18));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getLong(19));
        } else {
            Assert.assertEquals(Numbers.LONG_NULL, r.getLong(19));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getDate(20));
        } else {
            Assert.assertEquals(Numbers.LONG_NULL, r.getDate(20));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextInt(), r.getInt(21));
        } else {
            Assert.assertEquals(Numbers.INT_NULL, r.getInt(21));
        }

        Assert.assertNull(r.getSymA(22));
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
            Assert.assertEquals(Numbers.LONG_NULL, r.getLong(4));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(ts, r.getDate(8));
        } else {
            Assert.assertEquals(Numbers.LONG_NULL, r.getDate(8));
        }

        assertBin(r, rnd, blob, 7);

        if (rnd.nextBoolean()) {
            assertStrColumn(rnd.nextChars(10), r, 5);
        } else {
            assertNullStr(r, 5);
        }

        if ((rnd.nextPositiveInt() & 3) == 0) {
            assertStrColumn(rnd.nextChars(15), r, 11);
        } else {
            assertNullStr(r, 11);
        }

        if ((rnd.nextPositiveInt() & 3) == 0) {
            Assert.assertEquals(rnd.nextInt(), r.getInt(12));
        } else {
            Assert.assertEquals(Numbers.INT_NULL, r.getInt(12));
        }

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
            Assert.assertEquals(rnd.nextFloat(), r.getFloat(16), 0.000001f);
        } else {
            Assert.assertTrue(Float.isNaN(r.getFloat(16)));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextDouble(), r.getDouble(17), 0.0000001d);
        } else {
            Assert.assertTrue(Double.isNaN(r.getDouble(17)));
        }

        if (rnd.nextBoolean()) {
            TestUtils.assertEquals(rnd.nextChars(10), r.getSymA(18));
        } else {
            Assert.assertNull(r.getSymA(18));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getLong(19));
        } else {
            Assert.assertEquals(Numbers.LONG_NULL, r.getLong(19));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextLong(), r.getDate(20));
        } else {
            Assert.assertEquals(Numbers.LONG_NULL, r.getDate(20));
        }

        if (rnd.nextBoolean()) {
            Assert.assertEquals(rnd.nextInt(), r.getInt(21));
        } else {
            Assert.assertEquals(Numbers.INT_NULL, r.getInt(21));
        }

        if (rnd.nextBoolean()) {
            TestUtils.assertEquals(rnd.nextChars(8), r.getSymA(22));
        } else {
            Assert.assertNull(r.getSymA(22));
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
            r.putStr(13, rnd.nextChars(15));
        }

        if ((rnd.nextPositiveInt() & 3) == 0) {
            r.putInt(14, rnd.nextInt());
        }

        if (rnd.nextBoolean()) {
            r.putShort(15, rnd.nextShort());
        }

        if (rnd.nextBoolean()) {
            r.putBool(16, rnd.nextBoolean());
        }

        if (rnd.nextBoolean()) {
            r.putByte(17, rnd.nextByte());
        }

        if (rnd.nextBoolean()) {
            r.putFloat(18, rnd.nextFloat());
        }

        if (rnd.nextBoolean()) {
            r.putDouble(19, rnd.nextDouble());
        }

        if (rnd.nextBoolean()) {
            r.putSym(20, rnd.nextChars(10));
        }

        if (rnd.nextBoolean()) {
            r.putLong(21, rnd.nextLong());
        }

        if (rnd.nextBoolean()) {
            r.putDate(22, rnd.nextLong());
        }

        if (rnd.nextBoolean()) {
            r.putInt(24, rnd.nextInt());
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
            r.putStr(13, rnd.nextChars(15));
        }

        if ((rnd.nextPositiveInt() & 3) == 0) {
            r.putInt(14, rnd.nextInt());
        }

        if (rnd.nextBoolean()) {
            r.putShort(15, rnd.nextShort());
        }

        if (rnd.nextBoolean()) {
            r.putBool(16, rnd.nextBoolean());
        }

        if (rnd.nextBoolean()) {
            r.putByte(17, rnd.nextByte());
        }

        if (rnd.nextBoolean()) {
            r.putFloat(18, rnd.nextFloat());
        }

        if (rnd.nextBoolean()) {
            r.putDouble(19, rnd.nextDouble());
        }

        if (rnd.nextBoolean()) {
            r.putSym(20, rnd.nextChars(10));
        }

        if (rnd.nextBoolean()) {
            r.putLong(21, rnd.nextLong());
        }

        if (rnd.nextBoolean()) {
            r.putDate(22, rnd.nextLong());
        }

        if (rnd.nextBoolean()) {
            r.putInt(24, rnd.nextInt());
        }

        if (rnd.nextBoolean()) {
            r.putSym(25, rnd.nextChars(8));
        }
    };
    private static final Rnd rnd = TestUtils.generateRandom(null);
    private TimestampDriver timestampDriver;
    private int timestampType;

    @Override
    public void setUp() {
        setProperty(PropertyKey.CAIRO_DEFAULT_SYMBOL_INDEX_TYPE, rnd.nextBoolean() ? "BITMAP" : "POSTING");
        super.setUp();
    }

    @Before
    public void setUp2() {
        timestampType = rnd.nextBoolean() ? ColumnType.TIMESTAMP_MICRO : ColumnType.TIMESTAMP_NANO;
        this.timestampDriver = ColumnType.getTimestampDriver(timestampType);
    }

    @Test
    public void testAddColumnConcurrentWithDataUpdates() throws Throwable {
        ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
        assertMemoryLeak(() -> {
            CyclicBarrier start = new CyclicBarrier(2);
            AtomicInteger done = new AtomicInteger();
            AtomicInteger columnsAdded = new AtomicInteger();
            AtomicInteger reloadCount = new AtomicInteger();
            int totalColAddCount = Os.isLinux() ? 100 : 10;

            String tableName = "tbl_meta_test";
            TableToken tableToken = createTable(tableName, PartitionBy.DAY);

            Thread writerThread = new Thread(() -> {
                try (TableWriter writer = getWriter(tableToken)) {
                    start.await();
                    for (int i = 0; i < totalColAddCount; i++) {
                        writer.addColumn("col" + i, ColumnType.SYMBOL, AllowAllSecurityContext.INSTANCE);
                        columnsAdded.incrementAndGet();

                        TableWriter.Row row = writer.newRow(timestampDriver.fromHours(i));
                        row.append();

                        writer.commit();
                    }
                } catch (Throwable e) {
                    exceptions.add(e);
                    LOG.error().$(e).$();
                } finally {
                    Path.clearThreadLocals();
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
                } finally {
                    Path.clearThreadLocals();
                }
            });
            writerThread.start();
            readerThread.start();

            writerThread.join();
            readerThread.join();

            if (!exceptions.isEmpty()) {
                for (Throwable ex : exceptions) {
                    ex.printStackTrace(System.out);
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
                        writer.addColumn("col" + i, ColumnType.SYMBOL, AllowAllSecurityContext.INSTANCE);
                        columnsAdded.incrementAndGet();

                        if (rnd.nextBoolean()) {
                            // Add partition
                            TableWriter.Row row = writer.newRow(timestampDriver.fromHours(i));
                            row.append();
                            writer.commit();
                        }

                        if (rnd.nextBoolean() && writer.getPartitionCount() > 0) {
                            // Remove partition
                            int partitionNum = rnd.nextInt() % writer.getPartitionCount();
                            writer.removePartition(timestampDriver.fromHours(partitionNum));
                        }
                    }
                } catch (Throwable e) {
                    exceptions.add(e);
                    LOG.error().$(e).$();
                } finally {
                    done.incrementAndGet();
                    Path.clearThreadLocals();
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
                } finally {
                    Path.clearThreadLocals();
                }
            });
            writerThread.start();
            readerThread.start();

            writerThread.join();
            readerThread.join();

            if (!exceptions.isEmpty()) {
                Throwable ex = exceptions.poll();
                ex.printStackTrace(System.out);
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
            int totalColAddCount = Os.isLinux() ? 100 : 50;

            String tableName = "tbl_meta_test";
            TableToken tableToken = createTable(tableName, PartitionBy.HOUR);
            Rnd rnd = TestUtils.generateRandom(LOG);

            Thread writerThread = new Thread(() -> {
                try (TableWriter writer = getWriter(tableToken)) {
                    start.await();
                    for (int i = 0; i < totalColAddCount; i++) {
                        writer.addColumn("col" + i, ColumnType.SYMBOL, AllowAllSecurityContext.INSTANCE);
                        columnsAdded.incrementAndGet();

                        if (rnd.nextBoolean()) {
                            // Add partition
                            TableWriter.Row row = writer.newRow(timestampDriver.fromHours(i));
                            row.append();
                            writer.commit();
                        }

                        if (rnd.nextBoolean() && writer.getPartitionCount() > 0) {
                            // Remove partition
                            int partitionNum = rnd.nextInt() % writer.getPartitionCount();
                            writer.removePartition(timestampDriver.fromHours(partitionNum));
                        }
                    }
                } catch (Throwable e) {
                    exceptions.add(e);
                    LOG.error().$(e).$();
                } finally {
                    done.incrementAndGet();
                    Path.clearThreadLocals();
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
                } finally {
                    Path.clearThreadLocals();
                }
            });
            writerThread.start();
            readerThread.start();

            writerThread.join();
            readerThread.join();
            Assert.assertTrue(reloadCount.get() > totalColAddCount / 10);
            LOG.infoW().$("total reload count ").$(reloadCount.get()).$();
        });

        if (!exceptions.isEmpty()) {
            throw exceptions.poll();
        }
    }

    @Test
    public void testAppendNullTimestamp() throws Exception {
        TableModel model = new TableModel(configuration, "all", PartitionBy.NONE)
                .col("int", ColumnType.INT);
//                .timestamp("t") // cannot insert null as a timestamp on designated columns
        CreateTableTestUtils.createTableWithVersionAndId(model, engine, ColumnType.VERSION, 1);

        assertMemoryLeak(() -> {
            try (TableWriter writer = newOffPoolWriter(configuration, "all")) {
                TableWriter.Row r = writer.newRow(Numbers.LONG_NULL);
                r.putInt(0, 100);
                r.append();
                writer.commit();

                Assert.assertEquals(1, writer.size());
            }

            try (TableReader reader = newOffPoolReader(configuration, "all")) {
                Assert.assertEquals(1, reader.getPartitionCount());
                Assert.assertEquals(1, reader.openPartition(0));
            }
        });
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
            public boolean removeQuiet(LPSZ name) {
                if (Utf8s.endsWithAscii(name, columnName + ".v" + suffix)) {
                    if (counterRef.get() == CANNOT_DELETE) {
                        return false;
                    }
                    counterRef.incrementAndGet();
                }
                return super.removeQuiet(name);
            }

            @Override
            public boolean wasCalled() {
                return counterRef.get() > 0;
            }
        };

        assertMemoryLeak(
                ff, () -> {
                    // create table with two string columns
                    TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                            .col("a", ColumnType.SYMBOL)
                            .col("b", ColumnType.SYMBOL);
                    AbstractCairoTest.create(model);

                    Rnd rnd = new Rnd();
                    final int N = 1000;

                    // populate table and delete column
                    try (TableWriter writer = getWriter("x")) {
                        appendTwoSymbols(writer, rnd, 1);
                        writer.commit();

                        try (
                                TableReader reader = newOffPoolReader(configuration, "x");
                                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
                        ) {
                            long counter = 0;

                            rnd.reset();
                            final Record record = cursor.getRecord();
                            while (cursor.hasNext()) {
                                Assert.assertEquals(rnd.nextChars(10), record.getSymA(0));
                                Assert.assertEquals(rnd.nextChars(15), record.getSymA(1));
                                counter++;
                            }

                            Assert.assertEquals(N, counter);

                            // this should write metadata without column "b" but will ignore
                            // file delete failures
                            writer.removeColumn("b");

                            // now when we add new column by same name it must not pick up files we failed to delete previously
                            writer.addColumn("b", ColumnType.SYMBOL, AllowAllSecurityContext.INSTANCE);

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
                                Assert.assertEquals(rnd.nextChars(10), record.getSymA(0));
                                if (counter < N) {
                                    // roll random generator to make sure it returns same values
                                    rnd.nextChars(15);
                                    Assert.assertNull(record.getSymA(1));
                                } else {
                                    Assert.assertEquals(rnd.nextChars(15), record.getSymA(1));
                                }
                                counter++;
                            }

                            Assert.assertEquals(N * 2, counter);
                        }
                    }

                    Assert.assertFalse(ff.wasCalled());
                    try (ColumnPurgeJob job = new ColumnPurgeJob(engine)) {
                        job.run(0);
                    }

                    checkColumnPurgeRemovesFiles(counterRef, ff, 1);
                }
        );
    }

    @Test
    public void testAsyncRemoveLastSym() throws Exception {
        AtomicInteger counterRef = new AtomicInteger(CANNOT_DELETE);
        TestFilesFacade ff = createColumnDeleteCounterFileFacade(counterRef, "b", "");

        // create table with two string columns
        TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("a", ColumnType.SYMBOL)
                .col("b", ColumnType.SYMBOL);
        AbstractCairoTest.create(model);

        testRemoveSymbol(counterRef, ff, "b", 2);
    }

    @Test
    public void testAsyncRemoveLastSymWithColTop() throws Exception {
        AtomicInteger counterRef = new AtomicInteger(CANNOT_DELETE);
        TestFilesFacade ff = createColumnDeleteCounterFileFacade(counterRef, "c", ".0");

        // create table with two string columns
        TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("a", ColumnType.SYMBOL)
                .col("b", ColumnType.SYMBOL);
        AbstractCairoTest.create(model);

        try (TableWriter tw = getWriter("x")) {
            tw.addColumn("c", ColumnType.SYMBOL, AllowAllSecurityContext.INSTANCE);
        }
        engine.releaseInactive();
        testRemoveSymbol(counterRef, ff, "c", 6);
    }

    @Test
    public void testAsyncSymbolRename() throws Exception {
        AtomicInteger counterRef = new AtomicInteger(CANNOT_DELETE);
        TestFilesFacade ff = createColumnDeleteCounterFileFacade(counterRef, "b", "");

        // create table with two string columns
        TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("a", ColumnType.SYMBOL)
                .col("b", ColumnType.SYMBOL);
        AbstractCairoTest.create(model);
        testAsyncColumnRename(counterRef, ff, "b");
    }

    @Test
    public void testAsyncSymbolRenameWithNameVersion() throws Exception {
        AtomicInteger counterRef = new AtomicInteger(CANNOT_DELETE);
        TestFilesFacade ff = createColumnDeleteCounterFileFacade(counterRef, "c", ".0");

        // create table with two string columns
        TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                .col("a", ColumnType.SYMBOL)
                .col("b", ColumnType.SYMBOL);
        AbstractCairoTest.create(model);
        try (TableWriter tw = getWriter("x")) {
            tw.addColumn("c", ColumnType.SYMBOL, AllowAllSecurityContext.INSTANCE);
        }
        engine.releaseInactive();

        testAsyncColumnRename(counterRef, ff, "c");
    }

    @Test
    public void testCharAsString() throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(
                    configuration,
                    "char_test",
                    PartitionBy.NONE
            ).col("cc", ColumnType.STRING);
            AbstractCairoTest.create(model);
            char[] data = {'a', 'b', 'f', 'g'};
            try (TableWriter writer = newOffPoolWriter(configuration, "char_test")) {

                for (int i = 0, n = data.length; i < n; i++) {
                    TableWriter.Row r = writer.newRow();
                    r.putStr(0, data[i]);
                    r.append();
                }
                writer.commit();
            }

            try (
                    TableReader reader = newOffPoolReader(configuration, "char_test");
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                final Record record = cursor.getRecord();
                int index = 0;
                while (cursor.hasNext()) {
                    Assert.assertTrue(index < data.length);
                    CharSequence value = record.getStrA(0);
                    Assert.assertNotNull(value);
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
        assertMemoryLeak(() -> {
            // model data
            LongList list = new LongList();
            final int N = 1024;
            final int scale = 10000;
            for (int i = 0; i < N; i++) {
                list.add(i);
            }

            // model table
            TableModel model = new TableModel(configuration, "w", partitionBy).col("l", ColumnType.LONG);
            AbstractCairoTest.create(model);

            final int threads = 2;
            final CyclicBarrier startBarrier = new CyclicBarrier(threads);
            final SOCountDownLatch stopLatch = new SOCountDownLatch(threads);
            final AtomicInteger errors = new AtomicInteger(0);

            // start writer
            new Thread(() -> {
                try {
                    startBarrier.await();
                    try (TableWriter writer = newOffPoolWriter(configuration, "w")) {
                        for (int i = 0; i < N * scale; i++) {
                            TableWriter.Row row = writer.newRow();
                            row.putLong(0, list.getQuick(i % N));
                            row.append();
                            writer.commit();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    errors.incrementAndGet();
                } finally {
                    Path.clearThreadLocals();
                    stopLatch.countDown();
                }
            }).start();

            // start reader
            new Thread(() -> {
                try {
                    startBarrier.await();
                    try (
                            TableReader reader = newOffPoolReader(configuration, "w");
                            TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
                    ) {
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
                    e.printStackTrace(System.out);
                    errors.incrementAndGet();
                } finally {
                    Path.clearThreadLocals();
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
        assertMemoryLeak(() -> {
            // create table with two string columns
            TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("a", ColumnType.LONG256);
            AbstractCairoTest.create(model);

            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                TableWriter.Row r = writer.newRow();
                r.putLong256(0, 1, 2, 3, 4);
                r.append();
                writer.commit();
            }

            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                TableWriter.Row r = writer.newRow();
                r.putLong256(0, 5, 6, 7, 8);
                r.append();
                writer.commit();
            }

            try (
                    TableReader reader = newOffPoolReader(configuration, "x");
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                println(reader.getMetadata(), cursor);
            }

            TestUtils.assertEquals(
                    """
                            a
                            0x04000000000000000300000000000000020000000000000001
                            0x08000000000000000700000000000000060000000000000005
                            """,
                    sink
            );
        });
    }

    @Test
    public void testManySymbolReloadTest() throws Exception {
        String tableName = "testManySymbolReloadTest";
        TableToken tableToken = createTable(tableName, PartitionBy.HOUR);

        assertMemoryLeak(() -> {
            try (TableReader reader = getReader(tableToken)) {
                int partitionsToAdd = Os.isLinux() ? (int) (Files.PAGE_SIZE / Long.BYTES / 4) + 1 : 10;
                try (TableWriter writer = getWriter(tableToken)) {
                    int symbolsToAdd = Os.isLinux() ? (int) (Files.PAGE_SIZE / Long.BYTES / 4) + 1 : 10;
                    for (int i = 0; i < symbolsToAdd; i++) {
                        writer.addColumn("col" + i, ColumnType.SYMBOL, AllowAllSecurityContext.INSTANCE);
                    }

                    for (int i = 0; i < partitionsToAdd; i++) {
                        writer.newRow(timestampDriver.fromHours(i)).append();
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
        node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, 10);
        spinLockTimeout = 10;
        AtomicInteger openCount = new AtomicInteger(1000);

        assertMemoryLeak(() -> {
            try (Path temp = new Path()) {
                temp.of(engine.getConfiguration().getDbRoot()).concat("dummy_non_existing_path");
                ff = new TestFilesFacadeImpl() {
                    @Override
                    public long openRO(LPSZ name) {
                        if (Utf8s.endsWithAscii(name, TableUtils.META_FILE_NAME) && openCount.decrementAndGet() < 0) {
                            return TestFilesFacadeImpl.INSTANCE.openRO(temp.$());
                        }
                        return TestFilesFacadeImpl.INSTANCE.openRO(name);
                    }
                };

                try (TableReader reader = getReader(tableToken)) {
                    try (TableWriter writer = getWriter(tableToken)) {
                        writer.addColumn("col10", ColumnType.SYMBOL, AllowAllSecurityContext.INSTANCE);
                    }
                    engine.releaseAllWriters();
                    try {
                        spinLockTimeout = 100;
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
        node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, 10);
        spinLockTimeout = 10;
        AtomicInteger openCount = new AtomicInteger(1000);

        assertMemoryLeak(() -> {
            try (Path temp = new Path()) {
                temp.of(engine.getConfiguration().getDbRoot()).concat("dummy_non_existing_path").$();
                ff = new TestFilesFacadeImpl() {

                    @Override
                    public long length(long fd) {
                        if (fd == this.fd) {
                            return Files.length(temp.$());
                        }
                        return Files.length(fd);
                    }

                    @Override
                    public long length(LPSZ name) {
                        if (Utf8s.endsWithAscii(name, TableUtils.META_FILE_NAME) && openCount.decrementAndGet() < 0) {
                            return Files.length(temp.$());
                        }
                        return Files.length(name);
                    }

                    @Override
                    public long openRO(LPSZ name) {
                        if (Utf8s.endsWithAscii(name, TableUtils.META_FILE_NAME) && openCount.decrementAndGet() < 0) {
                            return this.fd = TestFilesFacadeImpl.INSTANCE.openRO(name);
                        }
                        return TestFilesFacadeImpl.INSTANCE.openRO(name);
                    }
                };

                try (TableReader reader = getReader(tableToken)) {
                    try (TableWriter writer = getWriter(tableToken)) {
                        writer.addColumn("col10", ColumnType.SYMBOL, AllowAllSecurityContext.INSTANCE);
                    }
                    engine.releaseAllWriters();
                    // minimise time we spend opening metadata that cannot be opened.
                    spinLockTimeout = 100;
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
        node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, 10);
        spinLockTimeout = 10;

        assertMemoryLeak(() -> {
            try (TableReader reader = getReader(tableToken)) {
                try (TableWriter writer = getWriter(tableToken)) {
                    writer.addColumn("col10", ColumnType.SYMBOL, AllowAllSecurityContext.INSTANCE);
                }
                try (
                        Path path = getPath(tableName);
                        MemoryMARW mem = Vm.getCMARWInstance(
                                TestFilesFacadeImpl.INSTANCE,
                                path.$(),
                                -1,
                                Files.PAGE_SIZE,
                                MemoryTag.NATIVE_DEFAULT,
                                configuration.getWriterFileOpenOpts()
                        )
                ) {
                    mem.putLong(TableUtils.META_OFFSET_METADATA_VERSION, 0);
                }

                try {
                    spinLockTimeout = 100;
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
        final String expected = replaceTimestampSuffix1(
                """
                        int\tshort\tbyte\tdouble\tfloat\tlong\tstr\tsym\tbool\tbin\tdate\tvarchar\ttimestamp
                        null\t0\t0\tnull\tnull\tnull\t\tabc\ttrue\t\t\t\t1970-01-01T00:00:00.100000Z
                        """,
                ColumnType.nameOf(timestampType)
        );

        assertMemoryLeak(() -> {
            CreateTableTestUtils.createAllTable(engine, PartitionBy.NONE, timestampType);

            try (TableWriter writer = newOffPoolWriter(configuration, "all")) {
                TableWriter.Row r = writer.newRow(timestampDriver.fromMicros(1000000)); // <-- higher timestamp
                r.putInt(0, 10);
                r.putByte(1, (byte) 56);
                r.putDouble(2, 4.3223);
                r.putStr(6, "xyz");
                r.cancel();

                r = writer.newRow(timestampDriver.fromMicros(100000)); // <-- lower timestamp
                r.putSym(7, "abc");
                r.putBool(8, true);
                r.append();

                writer.commit();
            }

            try (
                    TableReader reader = newOffPoolReader(configuration, "all");
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                println(reader.getMetadata(), cursor);
                TestUtils.assertEquals(expected, sink);
            }
        });
    }

    @Test
    public void testOver2GFile() throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                    .col("a", ColumnType.LONG);
            AbstractCairoTest.create(model);

            long N = 280000000;
            Rnd rnd = new Rnd();
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < N; i++) {
                    TableWriter.Row r = writer.newRow();
                    r.putLong(0, rnd.nextLong());
                    r.append();
                }
                writer.commit();
            }

            try (
                    TableReader reader = newOffPoolReader(configuration, "x");
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                int count = 0;
                rnd.reset();
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
        CreateTableTestUtils.createAllTable(engine, PartitionBy.NONE, timestampType);
        int N = 10000;
        Rnd rnd = new Rnd();
        try (TableWriter writer = newOffPoolWriter(configuration, "all")) {
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

            try (
                    TableReader reader = newOffPoolReader(configuration, "all");
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                col = reader.getMetadata().getColumnIndex("str");
                int count = 0;
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    CharSequence expected = rnd.nextChars(15);
                    CharSequence actual = record.getStrA(col);
                    Assert.assertNotNull(actual);
                    Assert.assertTrue(Chars.equals(expected, 2, 10, actual, 0, 8));
                    count++;
                }
                Assert.assertEquals(N, count);
            }
        }
    }

    @Test
    public void testReadByDay() throws Exception {
        CreateTableTestUtils.createAllTable(engine, PartitionBy.DAY, timestampType);
        assertMemoryLeak(this::testTableCursor);
    }

    @Test
    public void testReadByMonth() throws Exception {
        CreateTableTestUtils.createAllTable(engine, PartitionBy.MONTH, timestampType);
        assertMemoryLeak(() -> testTableCursor(60 * 60 * 60000));
    }

    @Test
    public void testReadByWeek() throws Exception {
        CreateTableTestUtils.createAllTable(engine, PartitionBy.WEEK, timestampType);
        assertMemoryLeak(() -> testTableCursor(7 * 60 * 60000L));
    }

    @Test
    public void testReadByYear() throws Exception {
        CreateTableTestUtils.createAllTable(engine, PartitionBy.YEAR, timestampType);
        assertMemoryLeak(() -> testTableCursor(24 * 60 * 60 * 60000L));
    }

    @Test
    public void testReadEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            CreateTableTestUtils.createAllTable(engine, PartitionBy.NONE, timestampType);
            try (TableWriter ignored1 = newOffPoolWriter(configuration, "all")) {

                // open another writer, which should fail
                try {
                    try (TableWriter ignore = newOffPoolWriter(configuration, "all")) {
                        Assert.fail();
                    }
                } catch (CairoException ignored) {
                }

                try (
                        TableReader reader = newOffPoolReader(configuration, "all");
                        TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
                ) {
                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    @Test
    public void testReadLong256Four() {
        TableModel model = new TableModel(configuration, "w", PartitionBy.DAY).col("l", ColumnType.LONG256).timestamp(timestampType);
        AbstractCairoTest.create(model);

        final int N = 1_000_000;
        final Rnd rnd = new Rnd();
        long timestamp = 0;
        try (TableWriter writer = newOffPoolWriter(configuration, "w")) {
            for (int i = 0; i < N; i++) {
                TableWriter.Row row = writer.newRow(timestamp);
                row.putLong256(0, "0x" + padHexLong(rnd.nextLong()) + padHexLong(rnd.nextLong()) + padHexLong(rnd.nextLong()) + padHexLong(rnd.nextLong()));
                row.append();
            }
            writer.commit();
        }

        rnd.reset();
        final StringSink sink = new StringSink();
        try (
                TableReader reader = newOffPoolReader(configuration, "w");
                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
        ) {
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
        TableModel model = new TableModel(configuration, "w", PartitionBy.DAY).col("l", ColumnType.LONG256).timestamp(timestampType);
        AbstractCairoTest.create(model);

        final int N = 1_000_000;
        final Rnd rnd = new Rnd();
        long timestamp = 0;
        try (TableWriter writer = newOffPoolWriter(configuration, "w")) {
            for (int i = 0; i < N; i++) {
                TableWriter.Row row = writer.newRow(timestamp);
                row.putLong256(0, "0x" + padHexLong(rnd.nextLong()));
                row.append();
            }
            writer.commit();
        }

        rnd.reset();
        final StringSink sink = new StringSink();
        try (
                TableReader reader = newOffPoolReader(configuration, "w");
                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
        ) {
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
        TableModel model = new TableModel(configuration, "w", PartitionBy.DAY).col("l", ColumnType.LONG256).timestamp(timestampType);
        AbstractCairoTest.create(model);

        final int N = 1_000_000;
        final Rnd rnd = new Rnd();
        long timestamp = 0;
        try (TableWriter writer = newOffPoolWriter(configuration, "w")) {
            for (int i = 0; i < N; i++) {
                TableWriter.Row row = writer.newRow(timestamp);
                row.putLong256(0, "0x" + padHexLong(rnd.nextLong()) + padHexLong(rnd.nextLong()) + padHexLong(rnd.nextLong()));
                row.append();
            }
            writer.commit();
        }

        rnd.reset();
        final StringSink sink = new StringSink();
        try (
                TableReader reader = newOffPoolReader(configuration, "w");
                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
        ) {
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
        TableModel model = new TableModel(configuration, "w", PartitionBy.DAY).col("l", ColumnType.LONG256).timestamp(timestampType);
        AbstractCairoTest.create(model);

        final int N = 1_000_000;
        final Rnd rnd = new Rnd();
        long timestamp = 0;
        try (TableWriter writer = newOffPoolWriter(configuration, "w")) {
            for (int i = 0; i < N; i++) {
                TableWriter.Row row = writer.newRow(timestamp);
                row.putLong256(0, "0x" + padHexLong(rnd.nextLong()) + padHexLong(rnd.nextLong()));
                row.append();
            }
            writer.commit();
        }

        rnd.reset();
        final StringSink sink = new StringSink();
        try (
                TableReader reader = newOffPoolReader(configuration, "w");
                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
        ) {
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
        CreateTableTestUtils.createAllTable(engine, PartitionBy.NONE, timestampType);
        assertMemoryLeak(this::testTableCursor);
    }

    @Test
    public void testReaderAndWriterRace() throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.NONE);
            AbstractCairoTest.create(model.timestamp(timestampType));

            SOCountDownLatch stopLatch = new SOCountDownLatch(2);
            CyclicBarrier barrier = new CyclicBarrier(2);
            int count = 1000000;
            AtomicInteger reloadCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);

            try (
                    TableWriter writer = newOffPoolWriter(configuration, "x");
                    TableReader reader = newOffPoolReader(configuration, "x")
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
                        e.printStackTrace(System.out);
                        errorCount.incrementAndGet();
                    } finally {
                        stopLatch.countDown();
                    }

                }).start();

                new Thread(() -> {
                    try {
                        barrier.await();
                        int max = 0;
                        try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)) {
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
                        }
                    } catch (Exception e) {
                        e.printStackTrace(System.out);
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
            TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY).col("l", ColumnType.LONG);
            TableToken tableToken = AbstractCairoTest.create(model);

            int rowCount = 10;
            try (TableWriter writer = newOffPoolWriter(configuration, tableName)) {
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
        });
    }

    @Test
    public void testReaderKeepsNLatestOpenPartitions() {
        final int openPartitionsLimit = 2;
        final int expectedPartitions = 12;
        final long tsStep = timestampDriver.fromSeconds(1);
        final int rows = expectedPartitions * 60 * 60;

        node1.setProperty(PropertyKey.CAIRO_INACTIVE_READER_MAX_OPEN_PARTITIONS, openPartitionsLimit);

        TableModel model = new TableModel(configuration, "x", PartitionBy.HOUR).col("i", ColumnType.INT).timestamp(timestampType);
        TestUtils.createTable(engine, model);

        try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
            for (int i = 0; i < rows; i++) {
                TableWriter.Row row = writer.newRow(i * tsStep);
                row.putInt(0, i);
                row.append();
            }
            writer.commit();
        }

        try (
                TableReader reader = newOffPoolReader(configuration, "x");
                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
        ) {
            Assert.assertEquals(expectedPartitions, reader.getPartitionCount());
            Assert.assertEquals(0, reader.getOpenPartitionCount());
            assertOpenPartitionCount(reader);

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
        assertMemoryLeak(() -> {
            // model table
            TableModel model = new TableModel(configuration, "w", PartitionBy.HOUR).col("l", ColumnType.LONG).timestamp(timestampType);
            AbstractCairoTest.create(model);

            try (
                    TableWriter writer = newOffPoolWriter(configuration, "w");
                    TableReader reader = newOffPoolReader(configuration, "w");
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                // Create and cancel row to ensure partition entry and NULL max timestamp
                // this used to trigger a problem with very last reload of the reader.
                writer.newRow(timestampDriver.parseFloorLiteral("2016-03-02T10:00:00.000000Z")).cancel();

                // before adding any data add column
                writer.addColumn("xyz", ColumnType.SYMBOL, AllowAllSecurityContext.INSTANCE);

                Assert.assertTrue(reader.reload());

                TableWriter.Row row = writer.newRow(timestampDriver.parseFloorLiteral("2016-03-02T10:00:00.000000Z"));
                row.append();
                writer.commit();

                Assert.assertTrue(reader.reload());

                cursor.toTop();
                println(reader.getMetadata(), cursor);
                TestUtils.assertEquals(
                        replaceTimestampSuffix(
                                """
                                        l\ttimestamp\txyz
                                        null\t2016-03-02T10:00:00.000000Z\t
                                        """,
                                ColumnType.nameOf(timestampType)
                        ),
                        sink
                );
            }
        });
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
    public void testReloadWithTrailingNullString() {
        final String tableName = "reload_test";
        TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY);
        model.col("str", ColumnType.STRING);
        model.timestamp(timestampType);
        AbstractCairoTest.create(model);

        try (
                TableReader reader = newOffPoolReader(configuration, tableName);
                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
        ) {
            Assert.assertFalse(reader.reload());

            final int N = 100;
            final int M = 1_000_000;
            final Rnd rnd = new Rnd();

            try (TableWriter writer = newOffPoolWriter(configuration, tableName)) {
                long timestamp = timestampDriver.parseFloorLiteral("2019-01-31T10:00:00.000001Z");
                long timestampStep = 500;

                for (int i = 0; i < N; i++) {
                    TableWriter.Row row = writer.newRow(timestamp);
                    row.putStr(0, rnd.nextChars(7));
                    row.append();
                    timestamp += timestampStep;
                }

                writer.commit();

                Assert.assertTrue(reader.reload());

                cursor.toTop();
                rnd.reset();
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    TestUtils.assertEquals(rnd.nextChars(7), record.getStrA(0));
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
                cursor.toTop();
                rnd.reset();
                while (cursor.hasNext()) {
                    if (count == N + M) {
                        Assert.assertNull(record.getStrA(0));
                    } else {
                        TestUtils.assertEquals(rnd.nextChars(7), record.getStrA(0));
                    }
                    count++;
                }

                Assert.assertEquals(N + M + 1, count);
            }
        }
    }

    @Test
    public void testReloadWithoutData() throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "tab", PartitionBy.DAY).col("x", ColumnType.SYMBOL).col("y", ColumnType.LONG);
            AbstractCairoTest.create(model);

            try (TableWriter writer = newOffPoolWriter(configuration, "tab")) {
                TableWriter.Row r = writer.newRow();
                r.putSym(0, "hello");
                r.append();

                writer.rollback();

                try (TableReader reader = newOffPoolReader(configuration, "tab")) {
                    writer.addColumn("z", ColumnType.SYMBOL, AllowAllSecurityContext.INSTANCE);
                    Assert.assertTrue(reader.reload());
                    writer.addColumn("w", ColumnType.INT, AllowAllSecurityContext.INSTANCE);
                    Assert.assertTrue(reader.reload());
                }
            }
        });
    }

    @Test
    public void testRemoveActivePartitionByDay() throws Exception {
        testRemoveActivePartition(PartitionBy.DAY, current -> timestampDriver.addDays(timestampDriver.getPartitionFloorMethod(PartitionBy.DAY).floor(current), 1), "2017-12-15");
    }

    @Test
    public void testRemoveActivePartitionByMonth() throws Exception {
        testRemoveActivePartition(PartitionBy.MONTH, current -> timestampDriver.addMonths(timestampDriver.getPartitionFloorMethod(PartitionBy.MONTH).floor(current), 1), "2018-04");
    }

    @Test
    public void testRemoveActivePartitionByWeek() throws Exception {
        testRemoveActivePartition(PartitionBy.WEEK, current -> timestampDriver.addWeeks(timestampDriver.getPartitionFloorMethod(PartitionBy.WEEK).floor(current), 1), "2017-W51");
    }

    @Test
    public void testRemoveActivePartitionByYear() throws Exception {
        testRemoveActivePartition(PartitionBy.YEAR, current -> timestampDriver.addYears(timestampDriver.getPartitionFloorMethod(PartitionBy.YEAR).floor(current), 1), "2021");
    }

    @Test
    public void testRemoveDefaultPartition() throws Exception {
        assertMemoryLeak(() -> {
            int N = 100;
            int N_PARTITIONS = 5;
            long timestampUs = timestampDriver.parseFloorLiteral("2017-12-11T00:00:00.000Z");
            long stride = 100;
            int bandStride = 1000;
            int totalCount = 0;

            // model table
            TableModel model = new TableModel(configuration, "w", PartitionBy.NONE).col("l", ColumnType.LONG).timestamp(timestampType);
            AbstractCairoTest.create(model);

            try (TableWriter writer = newOffPoolWriter(configuration, "w")) {
                for (int k = 0; k < N_PARTITIONS; k++) {
                    long band = k * bandStride;
                    for (int i = 0; i < N; i++) {
                        TableWriter.Row row = writer.newRow(timestampUs);
                        row.putLong(0, band + i);
                        row.append();
                        writer.commit();
                        timestampUs += stride;
                    }
                    timestampUs = timestampDriver.addDays(timestampDriver.startOfDay(timestampUs, 0), 1);
                }

                Assert.assertEquals(N * N_PARTITIONS, writer.size());
                Assert.assertFalse(writer.removePartition(0));
                Assert.assertEquals(N * N_PARTITIONS, writer.size());
            }

            // now open table reader having partition gap
            try (
                    TableReader reader = newOffPoolReader(configuration, "w");
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                Assert.assertEquals(N * N_PARTITIONS, reader.size());

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
        testRemovePartition(PartitionBy.DAY, "2017-12-11", 0, current -> timestampDriver.addDays(timestampDriver.getPartitionFloorMethod(PartitionBy.DAY).floor(current), 1));
    }

    @Test
    public void testRemoveFirstPartitionByDayReload() throws Exception {
        testRemovePartitionReload(PartitionBy.DAY, "2017-12-11", 0, current -> timestampDriver.addDays(timestampDriver.getPartitionFloorMethod(PartitionBy.DAY).floor(current), 1));
    }

    @Test
    public void testRemoveFirstPartitionByDayReloadTwo() throws Exception {
        testRemovePartitionReload(PartitionBy.DAY, "2017-12-11", 0, current -> timestampDriver.addDays(timestampDriver.getPartitionFloorMethod(PartitionBy.DAY).floor(current), 2));
    }

    @Test
    public void testRemoveFirstPartitionByDayTwo() throws Exception {
        testRemovePartition(PartitionBy.DAY, "2017-12-11", 0, current -> timestampDriver.addDays(timestampDriver.getPartitionFloorMethod(PartitionBy.DAY).floor(current), 2));
    }

    @Test
    public void testRemoveFirstPartitionByMonth() throws Exception {
        testRemovePartition(PartitionBy.MONTH, "2017-12", 0, current -> timestampDriver.addMonths(timestampDriver.getPartitionFloorMethod(PartitionBy.MONTH).floor(current), 1));
    }

    @Test
    public void testRemoveFirstPartitionByMonthReload() throws Exception {
        testRemovePartitionReload(PartitionBy.MONTH, "2017-12", 0, current -> timestampDriver.addMonths(timestampDriver.getPartitionFloorMethod(PartitionBy.MONTH).floor(current), 1));
    }

    @Test
    public void testRemoveFirstPartitionByMonthReloadTwo() throws Exception {
        testRemovePartitionReload(PartitionBy.MONTH, "2017-12", 0, current -> timestampDriver.addMonths(timestampDriver.getPartitionFloorMethod(PartitionBy.MONTH).floor(current), 2));
    }

    @Test
    public void testRemoveFirstPartitionByMonthTwo() throws Exception {
        testRemovePartition(PartitionBy.MONTH, "2017-12", 0, current -> timestampDriver.addMonths(timestampDriver.getPartitionFloorMethod(PartitionBy.MONTH).floor(current), 2));
    }

    @Test
    public void testRemoveFirstPartitionByWeek() throws Exception {
        testRemovePartition(PartitionBy.WEEK, "2017-W50", 0, current -> timestampDriver.addWeeks(timestampDriver.getPartitionFloorMethod(PartitionBy.WEEK).floor(current), 1));
    }

    @Test
    public void testRemoveFirstPartitionByWeekReload() throws Exception {
        testRemovePartitionReload(PartitionBy.WEEK, "2017-W50", 0, current -> timestampDriver.addWeeks(timestampDriver.getPartitionFloorMethod(PartitionBy.WEEK).floor(current), 1));
    }

    @Test
    public void testRemoveFirstPartitionByWeekReloadTwo() throws Exception {
        testRemovePartitionReload(PartitionBy.WEEK, "2017-W50", 0, current -> timestampDriver.addWeeks(timestampDriver.getPartitionFloorMethod(PartitionBy.WEEK).floor(current), 2));
    }

    @Test
    public void testRemoveFirstPartitionByWeekTwo() throws Exception {
        testRemovePartition(PartitionBy.WEEK, "2017-W50", 0, current -> timestampDriver.addWeeks(timestampDriver.getPartitionFloorMethod(PartitionBy.WEEK).floor(current), 2));
    }

    @Test
    public void testRemoveFirstPartitionByYear() throws Exception {
        testRemovePartition(PartitionBy.YEAR, "2017", 0, current -> timestampDriver.addYears(timestampDriver.getPartitionFloorMethod(PartitionBy.YEAR).floor(current), 1));
    }

    @Test
    public void testRemoveFirstPartitionByYearReload() throws Exception {
        testRemovePartitionReload(PartitionBy.YEAR, "2017", 0, current -> timestampDriver.addYears(timestampDriver.getPartitionFloorMethod(PartitionBy.YEAR).floor(current), 1));
    }

    @Test
    public void testRemoveFirstPartitionByYearReloadTwo() throws Exception {
        testRemovePartitionReload(PartitionBy.YEAR, "2017", 0, current -> timestampDriver.addYears(timestampDriver.getPartitionFloorMethod(PartitionBy.YEAR).floor(current), 2));
    }

    @Test
    public void testRemoveFirstPartitionByYearTwo() throws Exception {
        testRemovePartition(PartitionBy.YEAR, "2017", 0, current -> timestampDriver.addYears(timestampDriver.getPartitionFloorMethod(PartitionBy.YEAR).floor(current), 2));
    }

    @Test
    public void testRemovePartitionByDay() throws Exception {
        testRemovePartition(PartitionBy.DAY, "2017-12-14", 3000, current -> timestampDriver.addDays(timestampDriver.getPartitionFloorMethod(PartitionBy.DAY).floor(current), 1));
    }

    @Test
    public void testRemovePartitionByDayCannotDeleteDir() throws Exception {
        assertMemoryLeak(() -> {
            int N = 100;
            int N_PARTITIONS = 5;
            long timestampUs = timestampDriver.parseFloorLiteral("2017-12-11T00:00:00.000Z");
            long stride = 100;
            int bandStride = 1000;
            int totalCount = 0;

            final FilesFacade ff = new TestFilesFacadeImpl() {
                @Override
                public boolean rmdir(Path name, boolean lazy) {
                    if (Utf8s.endsWithAscii(name, "2017-12-14" + Files.SEPARATOR)) {
                        return false;
                    }
                    return super.rmdir(name, lazy);
                }
            };

            CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public @NotNull FilesFacade getFilesFacade() {
                    return ff;
                }
            };

            // model table
            TableModel model = new TableModel(configuration, "w", PartitionBy.DAY).col("l", ColumnType.LONG).timestamp(timestampType);
            AbstractCairoTest.create(model);

            try (TableWriter writer = newOffPoolWriter(configuration, "w")) {
                for (int k = 0; k < N_PARTITIONS; k++) {
                    long band = k * bandStride;
                    for (int i = 0; i < N; i++) {
                        TableWriter.Row row = writer.newRow(timestampUs);
                        row.putLong(0, band + i);
                        row.append();
                        writer.commit();
                        timestampUs += stride;
                    }
                    timestampUs = timestampDriver.addDays(timestampDriver.startOfDay(timestampUs, 0), 1);
                }

                Assert.assertEquals(N * N_PARTITIONS, writer.size());

                DateFormat fmt = PartitionBy.getPartitionDirFormatMethod(writer.getMetadata().getTimestampType(), PartitionBy.DAY);
                assert fmt != null;
                final long timestamp = fmt.parse("2017-12-14", EN_LOCALE);

                Assert.assertTrue(writer.removePartition(timestamp));
                Assert.assertFalse(writer.removePartition(timestamp));

                Assert.assertEquals(N * (N_PARTITIONS - 1), writer.size());
            }

            // now open table reader having partition gap
            try (
                    TableReader reader = newOffPoolReader(configuration, "w");
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                Assert.assertEquals(N * (N_PARTITIONS - 1), reader.size());

                int previousBand = -1;
                int bandCount = 0;
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
        testRemovePartitionReload(PartitionBy.DAY, "2017-12-14", 3000, current -> timestampDriver.addDays(timestampDriver.getPartitionFloorMethod(PartitionBy.DAY).floor(current), 1));
    }

    @Test
    public void testRemovePartitionByMonth() throws Exception {
        testRemovePartition(PartitionBy.MONTH, "2018-01", 1000, current -> timestampDriver.addMonths(timestampDriver.getPartitionFloorMethod(PartitionBy.MONTH).floor(current), 1));
    }

    @Test
    public void testRemovePartitionByMonthReload() throws Exception {
        testRemovePartitionReload(PartitionBy.MONTH, "2018-01", 1000, current -> timestampDriver.addMonths(timestampDriver.getPartitionFloorMethod(PartitionBy.MONTH).floor(current), 1));
    }

    @Test
    public void testRemovePartitionByWeek() throws Exception {
        testRemovePartition(PartitionBy.WEEK, "2017-W51", 1000, current -> timestampDriver.addWeeks(timestampDriver.getPartitionFloorMethod(PartitionBy.WEEK).floor(current), 1));
    }

    @Test
    public void testRemovePartitionByWeekReload() throws Exception {
        testRemovePartitionReload(PartitionBy.WEEK, "2017-W51", 1000, current -> timestampDriver.addWeeks(timestampDriver.getPartitionFloorMethod(PartitionBy.WEEK).floor(current), 1));
    }

    @Test
    public void testRemovePartitionByYear() throws Exception {
        testRemovePartition(PartitionBy.YEAR, "2020", 3000, current -> timestampDriver.addYears(timestampDriver.getPartitionFloorMethod(PartitionBy.YEAR).floor(current), 1));
    }

    @Test
    public void testRemovePartitionByYearReload() throws Exception {
        testRemovePartitionReload(PartitionBy.YEAR, "2020", 3000, current -> timestampDriver.addYears(timestampDriver.getPartitionFloorMethod(PartitionBy.YEAR).floor(current), 1));
    }


    @Test
    public void testSetActiveColumnsAllColumnsOpenFlagFastPath() throws Exception {
        // Verifies the per-partition flag: after openPartition opens all columns,
        // subsequent calls skip the missing-columns scan.
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.LONG)
                    .timestamp(timestampType);
            TestUtils.createTable(engine, model);

            long tsStep = timestampDriver.fromSeconds(60 * 60);
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < 24; i++) {
                    TableWriter.Row row = writer.newRow(i * tsStep);
                    row.putInt(0, i);
                    row.putLong(1, i * 100L);
                    row.append();
                }
                writer.commit();
            }

            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                // no setActiveColumns — all columns open
                reader.openPartition(0);

                int columnBase = reader.getColumnBase(0);
                Object memA = reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 0));
                Object memB = reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 1));
                Assert.assertNotNull(memA);
                Assert.assertNotNull(memB);

                // second openPartition — flag should be 1, same memory objects
                reader.openPartition(0);
                Assert.assertSame(memA, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 0)));
                Assert.assertSame(memB, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 1)));
            }
        });
    }

    @Test
    public void testSetActiveColumnsBroadeningMultiplePartitions() throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.LONG)
                    .col("c", ColumnType.DOUBLE)
                    .timestamp(timestampType);
            TestUtils.createTable(engine, model);

            // 3 days = 3 partitions
            long tsStep = timestampDriver.fromSeconds(60 * 60);
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < 72; i++) {
                    TableWriter.Row row = writer.newRow(i * tsStep);
                    row.putInt(0, i);
                    row.putLong(1, i * 100L);
                    row.putDouble(2, i * 1.5);
                    row.append();
                }
                writer.commit();
            }

            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                Assert.assertEquals(3, reader.getPartitionCount());

                // open all 3 partitions with only column "a"
                IntList narrowSet = new IntList();
                narrowSet.add(0);
                reader.setActiveColumns(narrowSet);
                for (int p = 0; p < 3; p++) {
                    reader.openPartition(p);
                }

                // verify "b" is not mapped in any partition
                for (int p = 0; p < 3; p++) {
                    int columnBase = reader.getColumnBase(p);
                    Assert.assertNull(reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 1)));
                }

                // broaden to include "b"
                IntList broadSet = new IntList();
                broadSet.add(0);
                broadSet.add(1);
                reader.setActiveColumns(broadSet);

                // "b" should now be mapped in all 3 already-open partitions after openPartition
                for (int p = 0; p < 3; p++) {
                    reader.openPartition(p);
                    int columnBase = reader.getColumnBase(p);
                    Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 0)));
                    Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 1)));
                    // "c" still not mapped
                    Assert.assertNull(reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 2)));
                }
            }
        });
    }

    @Test
    public void testSetActiveColumnsDataCorrectness() throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.LONG)
                    .col("c", ColumnType.DOUBLE)
                    .timestamp(timestampType);
            TestUtils.createTable(engine, model);

            long tsStep = timestampDriver.fromSeconds(60 * 60);
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < 48; i++) {
                    TableWriter.Row row = writer.newRow(i * tsStep);
                    row.putInt(0, i);
                    row.putLong(1, i * 100L);
                    row.putDouble(2, i * 1.5);
                    row.append();
                }
                writer.commit();
            }

            // set active columns to "a" and timestamp, read data via cursor
            IntList columnIndexes = new IntList();
            columnIndexes.add(0); // a
            columnIndexes.add(3); // timestamp

            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                reader.setActiveColumns(columnIndexes);

                try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)) {
                    final Record record = cursor.getRecord();
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(count, record.getInt(0));
                        count++;
                    }
                    Assert.assertEquals(48, count);
                }
            }
        });
    }

    @Test
    public void testSetActiveColumnsEmptyListMapsAllColumns() throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.LONG)
                    .timestamp(timestampType);
            TestUtils.createTable(engine, model);

            long tsStep = timestampDriver.fromSeconds(60 * 60);
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < 24; i++) {
                    TableWriter.Row row = writer.newRow(i * tsStep);
                    row.putInt(0, i);
                    row.putLong(1, i * 100L);
                    row.append();
                }
                writer.commit();
            }

            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                // empty list means all columns, same as null
                reader.setActiveColumns(new IntList());
                reader.openPartition(0);

                int columnBase = reader.getColumnBase(0);
                for (int i = 0; i < reader.getColumnCount(); i++) {
                    Assert.assertNotSame(
                            "column " + i + " should be mapped",
                            NullMemoryCMR.INSTANCE,
                            reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, i))
                    );
                }
            }
        });
    }

    @Test
    public void testSetActiveColumnsFreedColumnDetectedOnBroadening() throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("s", ColumnType.STRING)
                    .col("c", ColumnType.DOUBLE)
                    .timestamp(timestampType);
            TestUtils.createTable(engine, model);

            long tsStep = timestampDriver.fromSeconds(60 * 60);
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < 24; i++) {
                    TableWriter.Row row = writer.newRow(i * tsStep);
                    row.putInt(0, i);
                    row.putStr(1, "str_" + i);
                    row.putDouble(2, i * 1.5);
                    row.append();
                }
                writer.commit();
            }

            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                // Open partition with all columns mapped
                reader.openPartition(0);
                int columnBase = reader.getColumnBase(0);
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 1)));

                // Close the partition — column memory objects are freed
                // (pageAddress set to 0) but stay in the columns list for reuse
                reader.closePartitionByIndex(0);

                // Set narrow active set and reopen the partition.
                // Only column "a" is mapped; "s" slot still holds the closed object.
                IntList narrowSet = new IntList();
                narrowSet.add(0);
                narrowSet.add(3); // timestamp
                reader.setActiveColumns(narrowSet);
                reader.openPartition(0);

                // Verify "s" (STRING, index 1) is not mapped.
                // The slot holds a closed MemoryCMRDetachedImpl, not null/NullMemoryCMR.
                columnBase = reader.getColumnBase(0);
                int sPrimary = TableReader.getPrimaryColumnIndex(columnBase, 1);
                Assert.assertNotNull(reader.getColumn(sPrimary));
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(sPrimary));

                // Broaden active set — must detect the closed object and remap
                IntList allColumns = new IntList();
                allColumns.add(0);
                allColumns.add(1);
                allColumns.add(2);
                allColumns.add(3);
                reader.setActiveColumns(allColumns);
                reader.openPartition(0);

                // "s" should now be properly mapped with a non-zero address
                columnBase = reader.getColumnBase(0);
                sPrimary = TableReader.getPrimaryColumnIndex(columnBase, 1);
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(sPrimary));
                Assert.assertTrue(
                        "STRING column primary should have non-zero address after broadening",
                        reader.getColumn(sPrimary).addressOf(0) != 0
                );
                Assert.assertTrue(
                        "STRING column aux should have non-zero address after broadening",
                        reader.getColumn(sPrimary + 1).addressOf(0) != 0
                );

                // Verify data is readable through cursor
                try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)) {
                    final Record record = cursor.getRecord();
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(count, record.getInt(0));
                        TestUtils.assertEquals("str_" + count, record.getStrA(1));
                        count++;
                    }
                    Assert.assertEquals(24, count);
                }
            }
        });
    }

    @Test
    public void testSetActiveColumnsGoPassiveClearsState() throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.LONG)
                    .timestamp(timestampType);
            TestUtils.createTable(engine, model);

            long tsStep = timestampDriver.fromSeconds(60 * 60);
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < 24; i++) {
                    TableWriter.Row row = writer.newRow(i * tsStep);
                    row.putInt(0, i);
                    row.putLong(1, i * 100L);
                    row.append();
                }
                writer.commit();
            }

            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                // set narrow active set, open partition
                IntList narrowSet = new IntList();
                narrowSet.add(0);
                reader.setActiveColumns(narrowSet);
                reader.openPartition(0);

                int columnBase = reader.getColumnBase(0);
                // "b" not mapped
                Assert.assertNull(reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 1)));

                // simulate pool return and re-checkout
                reader.goPassive();
                reader.goActive();

                // after goPassive, active columns should be cleared
                // opening a new partition should map all columns
                if (reader.getPartitionCount() > 0) {
                    reader.openPartition(0);
                    columnBase = reader.getColumnBase(0);
                    for (int i = 0; i < reader.getColumnCount(); i++) {
                        Assert.assertNotSame(
                                "column " + i + " should be mapped after goPassive/goActive",
                                NullMemoryCMR.INSTANCE,
                                reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, i))
                        );
                    }
                }
            }
        });
    }

    @Test
    public void testSetActiveColumnsGoPassiveRetainsPartitionWithNarrowSet() throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.LONG)
                    .col("c", ColumnType.DOUBLE)
                    .timestamp(timestampType);
            TestUtils.createTable(engine, model);

            // 3 days = 3 partitions
            long tsStep = timestampDriver.fromSeconds(60 * 60);
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < 72; i++) {
                    TableWriter.Row row = writer.newRow(i * tsStep);
                    row.putInt(0, i);
                    row.putLong(1, i * 100L);
                    row.putDouble(2, i * 1.5);
                    row.append();
                }
                writer.commit();
            }

            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                // open last partition with only column "a"
                IntList narrowSet = new IntList();
                narrowSet.add(0);
                reader.setActiveColumns(narrowSet);
                reader.openPartition(2);

                int columnBase = reader.getColumnBase(2);
                Assert.assertNull(reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 1)));

                // simulate pool return and re-checkout
                reader.goPassive();
                reader.goActive();

                // partition 2 should still be open (within maxOpenPartitions)
                // now set active columns to all columns
                IntList allColumns = new IntList();
                allColumns.add(0);
                allColumns.add(1);
                allColumns.add(2);
                allColumns.add(3);
                reader.setActiveColumns(allColumns);
                reader.openPartition(2);

                // all columns should now be mapped in the retained partition
                columnBase = reader.getColumnBase(2);
                for (int i = 0; i < reader.getColumnCount(); i++) {
                    Assert.assertNotNull(
                            "column " + i + " should be mapped after goPassive/goActive + setActiveColumns",
                            reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, i))
                    );
                    Assert.assertNotSame(
                            NullMemoryCMR.INSTANCE,
                            reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, i))
                    );
                }
            }
        });
    }

    @Test
    public void testSetActiveColumnsIndexedColumns() throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("sym", ColumnType.SYMBOL).indexed(true, 4)
                    .col("c", ColumnType.DOUBLE)
                    .timestamp(timestampType);
            TestUtils.createTable(engine, model);

            long tsStep = timestampDriver.fromSeconds(60 * 60);
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < 24; i++) {
                    TableWriter.Row row = writer.newRow(i * tsStep);
                    row.putInt(0, i);
                    row.putSym(1, "s" + (i % 5));
                    row.putDouble(2, i * 1.5);
                    row.append();
                }
                writer.commit();
            }

            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                // exclude indexed SYMBOL column from active set
                IntList columnIndexes = new IntList();
                columnIndexes.add(0); // a
                columnIndexes.add(3); // timestamp
                reader.setActiveColumns(columnIndexes);
                reader.openPartition(0);

                int columnBase = reader.getColumnBase(0);
                // "a" mapped
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 0)));
                // "sym" not mapped
                Assert.assertNull(reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 1)));

                // broaden to include the indexed SYMBOL column
                IntList allColumns = new IntList();
                allColumns.add(0);
                allColumns.add(1);
                allColumns.add(2);
                allColumns.add(3);
                reader.setActiveColumns(allColumns);
                reader.openPartition(0);

                // all columns should now be mapped
                columnBase = reader.getColumnBase(0);
                for (int i = 0; i < reader.getColumnCount(); i++) {
                    Assert.assertNotSame(
                            "column " + i + " should be mapped",
                            NullMemoryCMR.INSTANCE,
                            reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, i))
                    );
                }

                // verify symbol data is readable through cursor
                try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)) {
                    final Record record = cursor.getRecord();
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(count, record.getInt(0));
                        Assert.assertEquals("s" + (count % 5), record.getSymA(1).toString());
                        count++;
                    }
                    Assert.assertEquals(24, count);
                }
            }
        });
    }

    @Test
    public void testSetActiveColumnsNarrowing() throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.LONG)
                    .col("c", ColumnType.DOUBLE)
                    .timestamp(timestampType);
            TestUtils.createTable(engine, model);

            long tsStep = timestampDriver.fromSeconds(60 * 60);
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < 24; i++) {
                    TableWriter.Row row = writer.newRow(i * tsStep);
                    row.putInt(0, i);
                    row.putLong(1, i * 100L);
                    row.putDouble(2, i * 1.5);
                    row.append();
                }
                writer.commit();
            }

            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                // set wide active set, open partition
                IntList wideSet = new IntList();
                wideSet.add(0);
                wideSet.add(1);
                wideSet.add(2);
                reader.setActiveColumns(wideSet);
                reader.openPartition(0);

                int columnBase = reader.getColumnBase(0);
                // all 3 columns should be mapped
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 0)));
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 1)));
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 2)));

                // narrow to only "a" - already-mapped columns should remain accessible
                IntList narrowSet = new IntList();
                narrowSet.add(0);
                reader.setActiveColumns(narrowSet);

                // "b" and "c" were already mapped and should still be accessible
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 0)));
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 1)));
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 2)));
            }
        });
    }

    @Test
    public void testSetActiveColumnsNarrowingThenOpenNewPartition() throws Exception {
        // Verifies that after narrowing the active column set, opening a
        // previously-unopened partition only maps the narrow set.
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.LONG)
                    .col("c", ColumnType.DOUBLE)
                    .timestamp(timestampType);
            TestUtils.createTable(engine, model);

            // 3 days = 3 partitions
            long tsStep = timestampDriver.fromSeconds(60 * 60);
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < 72; i++) {
                    TableWriter.Row row = writer.newRow(i * tsStep);
                    row.putInt(0, i);
                    row.putLong(1, i * 100L);
                    row.putDouble(2, i * 1.5);
                    row.append();
                }
                writer.commit();
            }

            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                Assert.assertEquals(3, reader.getPartitionCount());

                // open partition 0 with all 3 data columns
                IntList wideSet = new IntList();
                wideSet.add(0);
                wideSet.add(1);
                wideSet.add(2);
                reader.setActiveColumns(wideSet);
                reader.openPartition(0);

                int columnBase0 = reader.getColumnBase(0);
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase0, 0)));
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase0, 1)));
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase0, 2)));

                // narrow to only "a", then open partition 1 (never opened before)
                IntList narrowSet = new IntList();
                narrowSet.add(0);
                reader.setActiveColumns(narrowSet);
                reader.openPartition(1);

                int columnBase1 = reader.getColumnBase(1);
                // "a" mapped in partition 1
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase1, 0)));
                // "b" and "c" NOT mapped in partition 1
                Assert.assertNull(reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase1, 1)));
                Assert.assertNull(reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase1, 2)));
            }
        });
    }

    @Test
    public void testSetActiveColumnsNullMapsAllColumns() throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.LONG)
                    .col("c", ColumnType.DOUBLE)
                    .timestamp(timestampType);
            TestUtils.createTable(engine, model);

            long tsStep = timestampDriver.fromSeconds(60 * 60);
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < 24; i++) {
                    TableWriter.Row row = writer.newRow(i * tsStep);
                    row.putInt(0, i);
                    row.putLong(1, i * 100L);
                    row.putDouble(2, i * 1.5);
                    row.append();
                }
                writer.commit();
            }

            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                // null means all columns
                reader.setActiveColumns(null);
                reader.openPartition(0);

                int columnBase = reader.getColumnBase(0);
                for (int i = 0; i < reader.getColumnCount(); i++) {
                    Assert.assertNotSame(
                            "column " + i + " should be mapped",
                            NullMemoryCMR.INSTANCE,
                            reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, i))
                    );
                }
            }
        });
    }

    @Test
    public void testSetActiveColumnsOnlyMapsRequestedColumns() throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.LONG)
                    .col("c", ColumnType.DOUBLE)
                    .timestamp(timestampType);
            TestUtils.createTable(engine, model);

            long tsStep = timestampDriver.fromSeconds(60 * 60);
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < 24; i++) {
                    TableWriter.Row row = writer.newRow(i * tsStep);
                    row.putInt(0, i);
                    row.putLong(1, i * 100L);
                    row.putDouble(2, i * 1.5);
                    row.append();
                }
                writer.commit();
            }

            // set active columns to only column "a" (index 0) and timestamp (index 3)
            IntList columnIndexes = new IntList();
            columnIndexes.add(0);
            columnIndexes.add(3);

            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                reader.setActiveColumns(columnIndexes);
                reader.openPartition(0);

                int columnBase = reader.getColumnBase(0);
                // column "a" (index 0) should be mapped
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 0)));
                // timestamp (index 3) should be mapped
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 3)));
                // columns "b" (index 1) and "c" (index 2) should NOT be mapped
                Assert.assertNull(reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 1)));
                Assert.assertNull(reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 2)));
            }
        });
    }

    @Test
    public void testSetActiveColumnsOpenMissingColumnsAllColumnsOpenParquet() throws Exception {
        // Verifies that Parquet partitions handle active column filtering
        // correctly: openPartition0 initializes only active columns, and
        // repeated openPartition calls are idempotent.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (a INT, b LONG, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(a, b, ts) VALUES
                            (1, 100, '2020-01-01T01:00:00.000Z'),
                            (2, 200, '2020-01-01T02:00:00.000Z'),
                            (3, 300, '2020-01-02T01:00:00.000Z')
                            """
            );
            drainWalQueue();

            // Convert first partition to Parquet
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            try (TableReader reader = getReader("x")) {
                // Set a narrow active column set
                IntList narrowSet = new IntList();
                narrowSet.add(0); // a
                reader.setActiveColumns(narrowSet);

                // Open Parquet partition — should work without issues
                reader.openPartition(0);

                // Open native partition
                reader.openPartition(1);

                // Calling openPartition again on the Parquet partition should
                // be idempotent
                reader.openPartition(0);
            }
        });
    }

    @Test
    public void testSetActiveColumnsOpenMissingColumnsErrorPath() throws Exception {
        // Verifies that openMissingColumnsInPartition() properly cleans up
        // (restores path, closes columns, resets partition state) when
        // reloadColumnAt() fails midway through opening missing columns.
        AtomicInteger counter = new AtomicInteger();
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                // Fail the first attempt to open b.d. Column b is skipped during
                // the narrow openPartition0, so this triggers during broadening.
                if (Utf8s.endsWithAscii(name, "b.d") && counter.incrementAndGet() == 1) {
                    return -1;
                }
                return TestFilesFacadeImpl.INSTANCE.openRO(name);
            }
        };

        assertMemoryLeak(ff, () -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.LONG)
                    .col("c", ColumnType.DOUBLE)
                    .timestamp(timestampType);
            TestUtils.createTable(engine, model);

            long tsStep = timestampDriver.fromSeconds(60 * 60);
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < 24; i++) {
                    TableWriter.Row row = writer.newRow(i * tsStep);
                    row.putInt(0, i);
                    row.putLong(1, i * 100L);
                    row.putDouble(2, i * 1.5);
                    row.append();
                }
                writer.commit();
            }

            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                // Open partition with only column "a". Column b is not in the
                // active set, so openPartitionColumns() skips it entirely.
                IntList narrowSet = new IntList();
                narrowSet.add(0);
                reader.setActiveColumns(narrowSet);
                reader.openPartition(0);
                assertOpenPartitionCount(reader);

                int columnBase = reader.getColumnBase(0);
                Assert.assertNull(reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 1)));

                // Broaden to include "b". openMissingColumnsInPartition() tries to
                // open b.d (counter becomes 1), which fails.
                IntList broadSet = new IntList();
                broadSet.add(0);
                broadSet.add(1);
                broadSet.add(2);
                broadSet.add(3);
                reader.setActiveColumns(broadSet);

                try {
                    reader.openPartition(0);
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not open");
                }

                // The error path must decrement openPartitionCount to keep
                // it consistent with the actual number of open partitions.
                assertOpenPartitionCount(reader);

                // After the error the partition must be marked as closed
                // (size == -1) so re-opening goes through openPartition0.
                // The next attempt should succeed because counter > 1.
                reader.openPartition(0);
                assertOpenPartitionCount(reader);
                columnBase = reader.getColumnBase(0);
                for (int i = 0; i < reader.getColumnCount(); i++) {
                    Assert.assertNotSame(
                            "column " + i + " should be mapped after recovery",
                            NullMemoryCMR.INSTANCE,
                            reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, i))
                    );
                }
            }
        });
    }

    @Test
    public void testSetActiveColumnsOpenMissingColumnsParquetBroadening() throws Exception {
        // Verifies that openMissingColumnsInPartition() correctly handles
        // Parquet partitions when the active column set broadens. With the
        // fix, ALL_COLUMNS_OPEN=0 after a narrow open triggers
        // openMissingColumnsInPartition on broadening. Without the fix,
        // ALL_COLUMNS_OPEN=1 would skip it entirely.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (a INT, sym SYMBOL INDEX, b LONG, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(a, sym, b, ts) VALUES
                            (1, 's0', 100, '2020-01-01T01:00:00.000Z'),
                            (2, 's1', 200, '2020-01-01T02:00:00.000Z'),
                            (3, 's0', 300, '2020-01-01T03:00:00.000Z')
                            """
            );
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            try (TableReader reader = getReader("x")) {
                // Close partition left open by drainWalQueue() to get a clean state
                reader.closePartitionByIndex(0);

                // Open Parquet partition with only column "a"
                IntList narrowSet = new IntList();
                narrowSet.add(0); // a
                reader.setActiveColumns(narrowSet);
                reader.openPartition(0);

                // Broaden to include all columns (indexed SYMBOL, LONG, timestamp)
                reader.setActiveColumns(null);
                reader.openPartition(0);

                // Verify bitmap index reader works for the previously-skipped indexed column
                int symIndex = reader.getMetadata().getColumnIndex("sym");
                IndexReader indexReader = reader.getIndexReader(0, symIndex, IndexReader.DIR_BACKWARD);
                Assert.assertNotNull(indexReader);
                Assert.assertTrue(indexReader.isOpen());
            }
        });
    }

    @Test
    public void testSetActiveColumnsOpenPartitionColumnsErrorPathParquet() throws Exception {
        // Verifies that openPartitionCount stays consistent when
        // openPartitionColumns() throws during Parquet partition open.
        // The bitmap index .k file open is the only I/O in this path
        // that can fail, and it requires a non-null (previously created)
        // bitmap index reader in the slot.
        AtomicInteger failCounter = new AtomicInteger();
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, ".k") && failCounter.get() > 0 && failCounter.decrementAndGet() >= 0) {
                    return -1;
                }
                return TestFilesFacadeImpl.INSTANCE.openRO(name);
            }
        };

        assertMemoryLeak(ff, () -> {
            execute(
                    """
                            CREATE TABLE x (a INT, sym SYMBOL INDEX, b LONG, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(a, sym, b, ts) VALUES
                            (1, 's0', 100, '2020-01-01T01:00:00.000Z'),
                            (2, 's1', 200, '2020-01-01T02:00:00.000Z'),
                            (3, 's0', 300, '2020-01-01T03:00:00.000Z')
                            """
            );
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            try (TableReader reader = getReader("x")) {
                // Close partition left open by drainWalQueue() to get a clean state.
                reader.closePartitionByIndex(0);

                // Open the Parquet partition and access the bitmap index reader
                // to populate the bitmapIndexes slot with a non-null reader.
                reader.openPartition(0);
                int symIndex = reader.getMetadata().getColumnIndex("sym");
                reader.getIndexReader(0, symIndex, IndexReader.DIR_BACKWARD);
                assertOpenPartitionCount(reader);

                // Close the partition. The bitmap index reader is closed but
                // the slot remains non-null, so the next openPartition0 will
                // call indexReader.of() which does I/O.
                reader.closePartitionByIndex(0);
                assertOpenPartitionCount(reader);

                // Arm the failure: next .k file open will fail.
                failCounter.set(1);
                try {
                    reader.openPartition(0);
                    Assert.fail("expected CairoException");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "could not open");
                }

                // openPartitionCount must stay consistent after the failure.
                assertOpenPartitionCount(reader);

                // Disarm failure and verify recovery.
                failCounter.set(0);
                reader.openPartition(0);
                assertOpenPartitionCount(reader);
            }
        });
    }

    @Test
    public void testSetActiveColumnsPoolReuseWithDifferentSet() throws Exception {
        // Simulates: reader used with set A, returned to pool,
        // next user activates set B (different columns). openPartition must open B's columns.
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.LONG)
                    .col("c", ColumnType.DOUBLE)
                    .timestamp(timestampType);
            TestUtils.createTable(engine, model);

            long tsStep = timestampDriver.fromSeconds(60 * 60);
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < 24; i++) {
                    TableWriter.Row row = writer.newRow(i * tsStep);
                    row.putInt(0, i);
                    row.putLong(1, i * 100L);
                    row.putDouble(2, i * 1.5);
                    row.append();
                }
                writer.commit();
            }

            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                // User A: only "a"
                IntList setA = new IntList();
                setA.add(0);
                reader.setActiveColumns(setA);
                reader.openPartition(0);

                int columnBase = reader.getColumnBase(0);
                Assert.assertNull(reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 1)));
                Assert.assertNull(reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 2)));

                // simulate pool return and re-checkout
                reader.goPassive();
                reader.goActive();

                // User B: only "b" and "c"
                IntList setB = new IntList();
                setB.add(1);
                setB.add(2);
                reader.setActiveColumns(setB);
                reader.openPartition(0);

                columnBase = reader.getColumnBase(0);
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 1)));
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 2)));
            }
        });
    }

    @Test
    public void testSetActiveColumnsPoolReuseWithoutSetActiveColumns() throws Exception {
        // Simulates: reader used with narrow column set, returned to pool,
        // next user gets it without calling setActiveColumns and reads all columns.
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.LONG)
                    .timestamp(timestampType);
            TestUtils.createTable(engine, model);

            long tsStep = timestampDriver.fromSeconds(60);
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < 10; i++) {
                    TableWriter.Row row = writer.newRow(i * tsStep);
                    row.putInt(0, i);
                    row.putLong(1, i * 100L);
                    row.append();
                }
                writer.commit();
            }

            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                // User A: narrow set, open partition
                IntList narrowSet = new IntList();
                narrowSet.add(0); // only "a"
                reader.setActiveColumns(narrowSet);
                reader.openPartition(0);

                int columnBase = reader.getColumnBase(0);
                Assert.assertNull(reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 1)));

                // simulate pool return and re-checkout
                reader.goPassive();
                reader.goActive();

                // User B: no setActiveColumns call, reads all columns
                reader.openPartition(0);
                columnBase = reader.getColumnBase(0);
                for (int i = 0; i < reader.getColumnCount(); i++) {
                    Assert.assertNotNull(
                            "column " + i + " should be mapped",
                            reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, i))
                    );
                }

                // verify data
                try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)) {
                    final Record record = cursor.getRecord();
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(count, record.getInt(0));
                        Assert.assertEquals(count * 100L, record.getLong(1));
                        count++;
                    }
                    Assert.assertEquals(10, count);
                }
            }
        });
    }

    @Test
    public void testSetActiveColumnsReloadColumnFiles() throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.LONG)
                    .col("c", ColumnType.DOUBLE)
                    .timestamp(timestampType);
            TestUtils.createTable(engine, model);

            // use 30-min step so all rows land in a single partition (day 0)
            long tsStep = timestampDriver.fromSeconds(30 * 60);
            try (
                    TableWriter writer = newOffPoolWriter(configuration, "x");
                    TableReader reader = newOffPoolReader(configuration, "x")
            ) {
                // write first batch - 24 rows in partition 0
                for (int i = 0; i < 24; i++) {
                    TableWriter.Row row = writer.newRow(i * tsStep);
                    row.putInt(0, i);
                    row.putLong(1, i * 100L);
                    row.putDouble(2, i * 1.5);
                    row.append();
                }
                writer.commit();

                // set active columns to "a" only, open partition
                IntList columnIndexes = new IntList();
                columnIndexes.add(0);
                reader.setActiveColumns(columnIndexes);
                Assert.assertTrue(reader.reload());
                reader.openPartition(0);

                int columnBase = reader.getColumnBase(0);
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 0)));

                // append more rows to same partition (triggers reloadColumnFiles path)
                for (int i = 24; i < 48; i++) {
                    TableWriter.Row row = writer.newRow(i * tsStep);
                    row.putInt(0, i);
                    row.putLong(1, i * 100L);
                    row.putDouble(2, i * 1.5);
                    row.append();
                }
                writer.commit();

                // reload should succeed - reloadColumnFiles must skip inactive columns
                Assert.assertTrue(reader.reload());

                // verify data correctness through cursor
                try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)) {
                    final Record record = cursor.getRecord();
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(count, record.getInt(0));
                        count++;
                    }
                    Assert.assertEquals(48, count);
                }
            }
        });
    }

    @Test
    public void testSetActiveColumnsReloadColumnFilesGrowsAllOpenColumns() throws Exception {
        // Verifies that reloadColumnFiles() grows ALL already-open columns,
        // not just the active ones. Previously, narrowing active columns before
        // a reload left already-mapped inactive columns at stale sizes.
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.LONG)
                    .col("c", ColumnType.DOUBLE)
                    .timestamp(timestampType);
            TestUtils.createTable(engine, model);

            long tsStep = timestampDriver.fromSeconds(30 * 60);
            try (
                    TableWriter writer = newOffPoolWriter(configuration, "x");
                    TableReader reader = newOffPoolReader(configuration, "x")
            ) {
                // write first batch — 24 rows in partition 0
                for (int i = 0; i < 24; i++) {
                    TableWriter.Row row = writer.newRow(i * tsStep);
                    row.putInt(0, i);
                    row.putLong(1, i * 100L);
                    row.putDouble(2, i * 1.5);
                    row.append();
                }
                writer.commit();

                // open partition with ALL columns (ALL_COLUMNS_OPEN = 1)
                reader.setActiveColumns(null);
                Assert.assertTrue(reader.reload());
                reader.openPartition(0);

                // narrow to column "a" only
                IntList narrowSet = new IntList();
                narrowSet.add(0);
                reader.setActiveColumns(narrowSet);

                // append more rows to the same partition
                for (int i = 24; i < 48; i++) {
                    TableWriter.Row row = writer.newRow(i * tsStep);
                    row.putInt(0, i);
                    row.putLong(1, i * 100L);
                    row.putDouble(2, i * 1.5);
                    row.append();
                }
                writer.commit();

                // reload — reloadColumnFiles must grow columns b, c too
                Assert.assertTrue(reader.reload());

                // broaden back to all columns
                reader.setActiveColumns(null);
                reader.openPartition(0);

                // verify ALL columns read correctly for all 48 rows
                try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)) {
                    final Record record = cursor.getRecord();
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(count, record.getInt(0));
                        Assert.assertEquals(count * 100L, record.getLong(1));
                        Assert.assertEquals(count * 1.5, record.getDouble(2), 0.0001);
                        count++;
                    }
                    Assert.assertEquals(48, count);
                }
            }
        });
    }

    @Test
    public void testSetActiveColumnsSameSetDoesNotReopenPartitions() throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("b", ColumnType.LONG)
                    .timestamp(timestampType);
            TestUtils.createTable(engine, model);

            long tsStep = timestampDriver.fromSeconds(60 * 60);
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < 24; i++) {
                    TableWriter.Row row = writer.newRow(i * tsStep);
                    row.putInt(0, i);
                    row.putLong(1, i * 100L);
                    row.append();
                }
                writer.commit();
            }

            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                IntList columnIndexes = new IntList();
                columnIndexes.add(0);
                reader.setActiveColumns(columnIndexes);
                reader.openPartition(0);

                int columnBase = reader.getColumnBase(0);
                // grab reference to mapped column memory
                Object colAMem = reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 0));
                Assert.assertNotNull(colAMem);

                // re-apply same set - should not trigger reopening
                IntList sameSet = new IntList();
                sameSet.add(0);
                reader.setActiveColumns(sameSet);

                // same memory object should still be there (not remapped)
                Assert.assertSame(colAMem, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 0)));
                // "b" should still be unmapped
                Assert.assertNull(reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 1)));
            }
        });
    }

    @Test
    public void testSetActiveColumnsSqlIntegration() throws Exception {
        // End-to-end test: the active columns optimization must produce correct
        // results when driven by the SQL engine (FullPartitionFrameCursorFactory
        // and IntervalPartitionFrameCursorFactory call setActiveColumns internally).
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x AS (
                                SELECT
                                    x::INT AS a,
                                    (x * 100)::LONG AS b,
                                    (x * 1.5) AS c,
                                    timestamp_sequence(0, 36_000_000_000) ts
                                FROM long_sequence(5)
                            ) TIMESTAMP(ts) PARTITION BY DAY
                            """
            );

            // SELECT subset of columns — only a and ts (FullPartitionFrameCursorFactory)
            assertQueryNoLeakCheck(
                    """
                            a\tts
                            1\t1970-01-01T00:00:00.000000Z
                            2\t1970-01-01T10:00:00.000000Z
                            3\t1970-01-01T20:00:00.000000Z
                            4\t1970-01-02T06:00:00.000000Z
                            5\t1970-01-02T16:00:00.000000Z
                            """,
                    "SELECT a, ts FROM x",
                    null,
                    "ts",
                    true,
                    true
            );

            // SELECT different subset — only b and c
            assertQueryNoLeakCheck(
                    """
                            b\tc
                            100\t1.5
                            200\t3.0
                            300\t4.5
                            400\t6.0
                            500\t7.5
                            """,
                    "SELECT b, c FROM x",
                    null,
                    null,
                    true,
                    true
            );

            // SELECT all columns
            assertQueryNoLeakCheck(
                    """
                            a\tb\tc\tts
                            1\t100\t1.5\t1970-01-01T00:00:00.000000Z
                            2\t200\t3.0\t1970-01-01T10:00:00.000000Z
                            3\t300\t4.5\t1970-01-01T20:00:00.000000Z
                            4\t400\t6.0\t1970-01-02T06:00:00.000000Z
                            5\t500\t7.5\t1970-01-02T16:00:00.000000Z
                            """,
                    "SELECT * FROM x",
                    null,
                    "ts",
                    true,
                    true
            );

            // SELECT with WHERE (interval filter — uses IntervalPartitionFrameCursorFactory)
            assertQueryNoLeakCheck(
                    """
                            a\tts
                            4\t1970-01-02T06:00:00.000000Z
                            5\t1970-01-02T16:00:00.000000Z
                            """,
                    "SELECT a, ts FROM x WHERE ts IN '1970-01-02'",
                    null,
                    "ts",
                    true,
                    false
            );
        });
    }

    @Test
    public void testSetActiveColumnsSymbolColumns() throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("sym", ColumnType.SYMBOL)
                    .col("c", ColumnType.DOUBLE)
                    .timestamp(timestampType);
            TestUtils.createTable(engine, model);

            long tsStep = timestampDriver.fromSeconds(60 * 60);
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < 24; i++) {
                    TableWriter.Row row = writer.newRow(i * tsStep);
                    row.putInt(0, i);
                    row.putSym(1, "s" + (i % 5));
                    row.putDouble(2, i * 1.5);
                    row.append();
                }
                writer.commit();
            }

            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                // activate only "a" and timestamp, excluding the SYMBOL column
                IntList columnIndexes = new IntList();
                columnIndexes.add(0); // a
                columnIndexes.add(3); // timestamp
                reader.setActiveColumns(columnIndexes);
                reader.openPartition(0);

                int columnBase = reader.getColumnBase(0);
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 0)));
                // "sym" not mapped
                Assert.assertNull(reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 1)));

                // verify "a" data is readable
                try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)) {
                    final Record record = cursor.getRecord();
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(count, record.getInt(0));
                        count++;
                    }
                    Assert.assertEquals(24, count);
                }

                // broaden to include the SYMBOL column and verify data
                IntList allColumns = new IntList();
                allColumns.add(0);
                allColumns.add(1);
                allColumns.add(2);
                allColumns.add(3);
                reader.setActiveColumns(allColumns);
                reader.openPartition(0);

                columnBase = reader.getColumnBase(0);
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 1)));

                // verify symbol data is readable through cursor
                try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)) {
                    final Record record = cursor.getRecord();
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(count, record.getInt(0));
                        Assert.assertEquals("s" + (count % 5), record.getSymA(1).toString());
                        count++;
                    }
                    Assert.assertEquals(24, count);
                }
            }
        });
    }

    @Test
    public void testSetActiveColumnsVarSizeColumns() throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("s", ColumnType.STRING)
                    .col("c", ColumnType.DOUBLE)
                    .timestamp(timestampType);
            TestUtils.createTable(engine, model);

            long tsStep = timestampDriver.fromSeconds(60 * 60);
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < 24; i++) {
                    TableWriter.Row row = writer.newRow(i * tsStep);
                    row.putInt(0, i);
                    row.putStr(1, "str_" + i);
                    row.putDouble(2, i * 1.5);
                    row.append();
                }
                writer.commit();
            }

            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                // activate only "s" (var-size) and timestamp
                IntList columnIndexes = new IntList();
                columnIndexes.add(1); // s
                columnIndexes.add(3); // timestamp
                reader.setActiveColumns(columnIndexes);
                reader.openPartition(0);

                int columnBase = reader.getColumnBase(0);
                // "a" not mapped
                Assert.assertNull(reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 0)));
                // "s" primary (data) and secondary (aux) both mapped
                int sPrimary = TableReader.getPrimaryColumnIndex(columnBase, 1);
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(sPrimary));
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(sPrimary + 1));
                // "c" not mapped
                Assert.assertNull(reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 2)));

                // now broaden to include "s" and "a" - verify "a" gets mapped
                IntList broadSet = new IntList();
                broadSet.add(0);
                broadSet.add(1);
                broadSet.add(3);
                reader.setActiveColumns(broadSet);
                reader.openPartition(0);

                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 0)));
                // "s" still mapped
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(sPrimary));
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(sPrimary + 1));
            }
        });
    }

    @Test
    public void testSetActiveColumnsVarSizeColumnsVarchar() throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .col("v", ColumnType.VARCHAR)
                    .col("c", ColumnType.DOUBLE)
                    .timestamp(timestampType);
            TestUtils.createTable(engine, model);

            long tsStep = timestampDriver.fromSeconds(60 * 60);
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < 24; i++) {
                    TableWriter.Row row = writer.newRow(i * tsStep);
                    row.putInt(0, i);
                    row.putVarchar(1, new io.questdb.std.str.Utf8String("vc_" + i));
                    row.putDouble(2, i * 1.5);
                    row.append();
                }
                writer.commit();
            }

            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                // activate only VARCHAR and timestamp, exclude "a" and "c"
                IntList columnIndexes = new IntList();
                columnIndexes.add(1); // v
                columnIndexes.add(3); // timestamp
                reader.setActiveColumns(columnIndexes);
                reader.openPartition(0);

                int columnBase = reader.getColumnBase(0);
                // "a" not mapped
                Assert.assertNull(reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 0)));
                // "v" primary (data) and secondary (aux) both mapped
                int vPrimary = TableReader.getPrimaryColumnIndex(columnBase, 1);
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(vPrimary));
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(vPrimary + 1));
                // "c" not mapped
                Assert.assertNull(reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase, 2)));

                // broaden to include all columns and verify data
                IntList allColumns = new IntList();
                allColumns.add(0);
                allColumns.add(1);
                allColumns.add(2);
                allColumns.add(3);
                reader.setActiveColumns(allColumns);
                reader.openPartition(0);

                // verify all data is readable through cursor
                try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)) {
                    final Record record = cursor.getRecord();
                    int count = 0;
                    while (cursor.hasNext()) {
                        Assert.assertEquals(count, record.getInt(0));
                        TestUtils.assertEquals("vc_" + count, record.getVarcharA(1));
                        Assert.assertEquals(count * 1.5, record.getDouble(2), 0.0001);
                        count++;
                    }
                    Assert.assertEquals(24, count);
                }
            }
        });
    }

    @Test
    public void testSetActiveColumnsWithColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .timestamp(timestampType);
            TestUtils.createTable(engine, model);

            long tsStep = timestampDriver.fromSeconds(60 * 60);
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                // write first batch (1 partition)
                for (int i = 0; i < 24; i++) {
                    TableWriter.Row row = writer.newRow(i * tsStep);
                    row.putInt(0, i);
                    row.append();
                }
                writer.commit();

                // add column "b" mid-table - partition 0 will have column top for "b"
                writer.addColumn("b", ColumnType.LONG, AllowAllSecurityContext.INSTANCE);

                // write second batch (partition 1) - "b" (writer index 2) has data here
                for (int i = 24; i < 48; i++) {
                    TableWriter.Row row = writer.newRow(i * tsStep);
                    row.putInt(0, i);
                    row.putLong(2, i * 100L);
                    row.append();
                }
                writer.commit();
            }

            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                // after addColumn("b"), layout is: a(0), timestamp(1), b(2)
                // activate only "b" (index 2) and timestamp (index 1)
                IntList columnIndexes = new IntList();
                columnIndexes.add(1); // timestamp
                columnIndexes.add(2); // b
                reader.setActiveColumns(columnIndexes);

                // open both partitions
                reader.openPartition(0);
                reader.openPartition(1);

                int columnBase0 = reader.getColumnBase(0);
                int columnBase1 = reader.getColumnBase(1);
                // "a" (index 0) should not be mapped in either partition
                Assert.assertNull(reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase0, 0)));
                Assert.assertNull(reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase1, 0)));
                // "b" (index 2) should be mapped in partition 1 (has data)
                Assert.assertNotSame(NullMemoryCMR.INSTANCE, reader.getColumn(TableReader.getPrimaryColumnIndex(columnBase1, 2)));

                // verify data via cursor
                try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)) {
                    final Record record = cursor.getRecord();
                    int count = 0;
                    while (cursor.hasNext()) {
                        if (count >= 24) {
                            // partition 1: "b" (index 2) has data
                            Assert.assertEquals(count * 100L, record.getLong(2));
                        }
                        count++;
                    }
                    Assert.assertEquals(48, count);
                }
            }
        });
    }

    @Test
    public void testStringColumnRemove() throws Exception {
        AtomicInteger counterRef = new AtomicInteger(CANNOT_DELETE);
        TestFilesFacade ff = createColumnDeleteCounterFileFacade(counterRef, "b", "");

        assertMemoryLeak(
                ff, () -> {
                    // create table with two string columns
                    TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("a", ColumnType.STRING).col("b", ColumnType.STRING);
                    AbstractCairoTest.create(model);

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

                        try (
                                TableReader reader = getReader("x");
                                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
                        ) {
                            long counter = 0;

                            rnd.reset();
                            final Record record = cursor.getRecord();
                            while (cursor.hasNext()) {
                                Assert.assertEquals(rnd.nextChars(10), record.getStrA(0));
                                Assert.assertEquals(rnd.nextChars(15), record.getStrA(1));
                                counter++;
                            }

                            Assert.assertEquals(N, counter);

                            // this should write metadata without column "b" but will ignore
                            // file delete failures
                            writer.removeColumn("b");

                            // It used to be: this must fail because we cannot delete foreign files
                            // but with column version file we can handle it.
                            writer.addColumn("b", ColumnType.STRING, AllowAllSecurityContext.INSTANCE);

                            // now assert what reader sees
                            Assert.assertTrue(reader.reload());
                            Assert.assertEquals(N, reader.size());

                            rnd.reset();
                            cursor.toTop();
                            while (cursor.hasNext()) {
                                Assert.assertEquals(rnd.nextChars(10), record.getStrA(0));
                                // roll random generator to make sure it returns same values
                                rnd.nextChars(15);
                                counter++;
                            }

                            Assert.assertEquals(N * 2, counter);
                        }
                    }
                }
        );
    }

    @Test
    public void testSymbolIndex() throws Exception {
        String expected = "{\"columnCount\":3,\"columns\":[{\"index\":0,\"name\":\"a\",\"type\":\"SYMBOL\",\"indexed\":true,\"indexValueBlockCapacity\":2},{\"index\":1,\"name\":\"b\",\"type\":\"INT\"},{\"index\":2,\"name\":\"timestamp\",\"type\":\"" + ColumnType.nameOf(timestampType) + "\"}],\"timestampIndex\":2}";

        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY)
                    .col("a", ColumnType.SYMBOL).indexed(true, 2)
                    .col("b", ColumnType.INT)
                    .timestamp(timestampType);
            AbstractCairoTest.create(model);

            int N = 1000;
            long ts = timestampDriver.parseFloorLiteral("2018-01-06T10:00:00.000Z");
            final Rnd rnd = new Rnd();
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
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

            try (TableReader reader = newOffPoolReader(configuration, "x")) {
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
        assertMemoryLeak(
                ff, () -> {
                    // create table with two string columns
                    TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("a", ColumnType.SYMBOL).col("b", ColumnType.STRING);
                    AbstractCairoTest.create(model);

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

                        try (
                                TableReader reader = getReader("x");
                                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
                        ) {
                            long counter = 0;

                            rnd.reset();
                            final Record record = cursor.getRecord();
                            while (cursor.hasNext()) {
                                Assert.assertEquals(rnd.nextChars(10), record.getSymA(0));
                                Assert.assertEquals(rnd.nextChars(15), record.getStrA(1));
                                counter++;
                            }

                            Assert.assertEquals(N, counter);

                            // this should write metadata without column "b" but will ignore
                            // file delete failures
                            writer.removeColumn("b");

                            // now when we add new column by same name it must not pick up files we failed to delete previously
                            writer.addColumn("b", ColumnType.STRING, AllowAllSecurityContext.INSTANCE);

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
                                Assert.assertEquals(rnd.nextChars(10), record.getSymA(0));
                                if (counter < N) {
                                    // roll random generator to make sure it returns same values
                                    rnd.nextChars(15);
                                    Assert.assertNull(record.getStrA(1));
                                } else {
                                    Assert.assertEquals(rnd.nextChars(15), record.getStrA(1));
                                }
                                counter++;
                            }

                            Assert.assertEquals(N * 2, counter);
                        }
                    }

                    checkColumnPurgeRemovesFiles(counterRef, ff, 2);
                }
        );
    }

    @Test
    public void testUnsuccessfulFileRename() throws Exception {
        AtomicInteger counterRef = new AtomicInteger(CANNOT_DELETE);
        TestFilesFacade ff = createColumnDeleteCounterFileFacade(counterRef, "b", "");

        assertMemoryLeak(
                ff, () -> {
                    // create table with two string columns
                    TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("a", ColumnType.STRING).col("b", ColumnType.STRING);
                    AbstractCairoTest.create(model);

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

                        try (
                                TableReader reader = getReader("x");
                                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
                        ) {
                            long counter = 0;

                            rnd.reset();
                            final Record record = cursor.getRecord();
                            while (cursor.hasNext()) {
                                Assert.assertEquals(rnd.nextChars(10), record.getStrA(0));
                                Assert.assertEquals(rnd.nextChars(15), record.getStrA(1));
                                counter++;
                            }

                            Assert.assertEquals(N, counter);

                            // this should write metadata without column "b" but will ignore
                            // file rename failures
                            writer.renameColumn("b", "bb");

                            // It used to be: this must fail because we cannot delete foreign files
                            // but with column version file we can handle it.
                            writer.addColumn("b", ColumnType.STRING, AllowAllSecurityContext.INSTANCE);

                            // now assert what reader sees
                            Assert.assertTrue(reader.reload()); // This fails with could not open read-only .. /bb.i.
                            Assert.assertEquals(N, reader.size());

                            rnd.reset();
                            cursor.toTop();
                            while (cursor.hasNext()) {
                                Assert.assertEquals(rnd.nextChars(10), record.getStrA(0));
                                // roll random generator to make sure it returns same values
                                rnd.nextChars(15);
                                counter++;
                            }

                            Assert.assertEquals(N * 2, counter);
                        }
                    }
                    engine.releaseInactive();
                    checkColumnPurgeRemovesFiles(counterRef, ff, 2);
                }
        );
    }

    @Test
    public void testUnsuccessfulRemoveAndReloadSym() throws Exception {
        AtomicInteger counterRef = new AtomicInteger(CANNOT_DELETE);
        TestFilesFacade ff = createColumnDeleteCounterFileFacade(counterRef, "b", "");

        assertMemoryLeak(
                ff, () -> {

                    // create table with two string columns
                    TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                            .col("a", ColumnType.SYMBOL)
                            .col("b", ColumnType.SYMBOL);
                    AbstractCairoTest.create(model);

                    Rnd rnd = new Rnd();
                    final int N = 1000;

                    // populate table and delete column
                    try (TableWriter writer = getWriter("x")) {
                        appendTwoSymbols(writer, rnd, 1);
                        writer.commit();

                        try (
                                TableReader reader = newOffPoolReader(configuration, "x");
                                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
                        ) {
                            long counter = 0;

                            rnd.reset();
                            final Record record = cursor.getRecord();
                            while (cursor.hasNext()) {
                                Assert.assertEquals(rnd.nextChars(10), record.getSymA(0));
                                Assert.assertEquals(rnd.nextChars(15), record.getSymA(1));
                                counter++;
                            }

                            Assert.assertEquals(N, counter);

                            // this should write metadata without column "b" but will ignore
                            // file delete failures
                            writer.removeColumn("b");

                            // now when we add new column by same name it must not pick up files we failed to delete previously
                            writer.addColumn("b", ColumnType.SYMBOL, AllowAllSecurityContext.INSTANCE);

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
                                Assert.assertEquals(rnd.nextChars(10), record.getSymA(0));
                                if (counter < N) {
                                    // roll random generator to make sure it returns same values
                                    rnd.nextChars(15);
                                    Assert.assertNull(record.getSymA(1));
                                } else {
                                    Assert.assertEquals(rnd.nextChars(15), record.getSymA(1));
                                }
                                counter++;
                            }

                            Assert.assertEquals(N * 2, counter);
                        }
                    }
                    checkColumnPurgeRemovesFiles(counterRef, ff, 4);
                }
        );
    }

    @Test
    public void testUnsuccessfulRemoveAndReloadSymTwice() throws Exception {
        AtomicInteger counterRef = new AtomicInteger(CANNOT_DELETE);
        TestFilesFacade ff = createColumnDeleteCounterFileFacade(counterRef, "b", "");

        assertMemoryLeak(
                ff, () -> {
                    // create table with two string columns
                    TableModel model = new TableModel(configuration, "x", PartitionBy.NONE)
                            .col("a", ColumnType.SYMBOL)
                            .col("b", ColumnType.SYMBOL).indexed(true, 256);
                    AbstractCairoTest.create(model);

                    Rnd rnd = new Rnd();
                    final int N = 1000;
                    // make sure we forbid deleting column "b" files

                    // populate table and delete column
                    try (TableWriter writer = getWriter("x")) {
                        appendTwoSymbols(writer, rnd, 1);
                        writer.commit();

                        try (
                                TableReader reader = newOffPoolReader(configuration, "x");
                                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
                        ) {
                            long counter = 0;

                            rnd.reset();
                            Record record = cursor.getRecord();
                            while (cursor.hasNext()) {
                                Assert.assertEquals(rnd.nextChars(10), record.getSymA(0));
                                Assert.assertEquals(rnd.nextChars(15), record.getSymA(1));
                                counter++;
                            }

                            Assert.assertEquals(N, counter);

                            // this should write metadata without column "b" but will ignore
                            // file delete failures
                            writer.removeColumn("b");
                            reader.reload();

                            // now when we add new column by same name it must not pick up files we failed to delete previously
                            writer.addColumn("b", ColumnType.SYMBOL, AllowAllSecurityContext.INSTANCE);

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
                                Assert.assertEquals(rnd.nextChars(10), record.getSymA(0));
                                if (counter < N) {
                                    // roll random generator to make sure it returns same values
                                    rnd.nextChars(15);
                                    Assert.assertNull(record.getSymA(1));
                                } else {
                                    Assert.assertEquals(rnd.nextChars(15), record.getSymA(1));
                                }
                                counter++;
                            }

                            Assert.assertEquals(N * 2, counter);
                        }

                        checkColumnPurgeRemovesFiles(counterRef, ff, 5);
                    }
                }
        );
    }

    @Test
    public void testUnsuccessfulRemoveExplicitColCloseAndReloadSym() throws Exception {
        AtomicInteger counterRef = new AtomicInteger(CANNOT_DELETE);
        TestFilesFacade ff = createColumnDeleteCounterFileFacade(counterRef, "b", "");

        assertMemoryLeak(
                ff, () -> {
                    // create table with two string columns
                    TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("a", ColumnType.SYMBOL).col("b", ColumnType.SYMBOL).indexed(true, 256);
                    AbstractCairoTest.create(model);

                    Rnd rnd = new Rnd();
                    final int N = 1000;
                    // make sure we forbid deleting column "b" files

                    // populate table and delete column
                    try (TableWriter writer = getWriter("x")) {
                        appendTwoSymbols(writer, rnd, 1);
                        writer.commit();

                        try (
                                TableReader reader = newOffPoolReader(configuration, "x");
                                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
                        ) {
                            long counter = 0;

                            rnd.reset();
                            final Record record = cursor.getRecord();
                            while (cursor.hasNext()) {
                                Assert.assertEquals(rnd.nextChars(10), record.getSymA(0));
                                Assert.assertEquals(rnd.nextChars(15), record.getSymA(1));
                                counter++;
                            }

                            Assert.assertEquals(N, counter);

                            // this should write metadata without column "b" but will ignore
                            // file delete failures
                            writer.removeColumn("b");

                            // now when we add new column by same name it must not pick up files we failed to delete previously
                            writer.addColumn("b", ColumnType.SYMBOL, AllowAllSecurityContext.INSTANCE);

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
                                Assert.assertEquals(rnd.nextChars(10), record.getSymA(0));
                                if (counter < N) {
                                    // roll random generator to make sure it returns same values
                                    rnd.nextChars(15);
                                    Assert.assertNull(record.getSymA(1));
                                } else {
                                    Assert.assertEquals(rnd.nextChars(15), record.getSymA(1));
                                }
                                counter++;
                            }

                            Assert.assertEquals(N * 2, counter);
                        }
                    }

                    checkColumnPurgeRemovesFiles(counterRef, ff, 5);
                }
        );
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
        Assert.assertNull(r.getStrA(index));
        Assert.assertNull(r.getStrB(index));
        Assert.assertEquals(TableUtils.NULL_LEN, r.getStrLen(index));
    }

    private static void assertStrColumn(CharSequence expected, Record r, int index) {
        TestUtils.assertEquals(expected, r.getStrA(index));
        TestUtils.assertEquals(expected, r.getStrB(index));
        Assert.assertNotSame(r.getStrA(index), r.getStrB(index));
        Assert.assertEquals(expected.length(), r.getStrLen(index));
    }

    private static void checkColumnPurgeRemovesFiles(AtomicInteger counterRef, TestFilesFacade ff, int removeCallsExpected) throws SqlException {
        Assert.assertFalse(ff.wasCalled());
        counterRef.set(0);
        try (ColumnPurgeJob job = new ColumnPurgeJob(engine)) {
            job.run(0);
        }
        int actual = ff.called();
        Assert.assertTrue("Expected at least " + removeCallsExpected + " file removals, but got " + actual, actual >= removeCallsExpected);
    }

    @NotNull
    private static TestFilesFacade createColumnDeleteCounterFileFacade(AtomicInteger counterRef, String columnName, final String suffix) {
        return new TestFilesFacade() {
            @Override
            public int called() {
                return counterRef.get();
            }

            @Override
            public boolean removeQuiet(LPSZ name) {
                if (
                        Utf8s.endsWithAscii(name, columnName + ".i")
                                || Utf8s.endsWithAscii(name, columnName + ".d" + suffix)
                                || Utf8s.endsWithAscii(name, columnName + ".o" + suffix)
                                || Utf8s.endsWithAscii(name, columnName + ".k" + suffix)
                                || Utf8s.endsWithAscii(name, columnName + ".c" + suffix)
                                || Utf8s.endsWithAscii(name, columnName + ".v" + suffix)
                ) {
                    if (counterRef.get() == CANNOT_DELETE) {
                        return false;
                    }
                    counterRef.incrementAndGet();
                }
                return super.removeQuiet(name);
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
        return new Path().of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat(TableUtils.META_FILE_NAME);
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
        try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)) {
            Rnd exp = new Rnd();
            long ts2 = assertPartialCursor(
                    cursor, exp, ts, increment, blob, 3L * count, (r, rnd13, ts13, blob13) -> {
                        BATCH1_ASSERTER.assertRecord(r, rnd13, ts13, blob13);
                        BATCH2_BEFORE_ASSERTER.assertRecord(r, rnd13, ts13, blob13);
                    }
            );
            assertPartialCursor(cursor, exp, ts2, increment, blob, count, BATCH2_ASSERTER);
        }
    }

    private void assertBatch3(int count, long increment, long ts, long blob, TableReader reader) {
        Rnd exp = new Rnd();
        long ts2;
        try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)) {
            ts2 = assertPartialCursor(
                    cursor, exp, ts, increment, blob, 3L * count, (r, rnd1, ts1, blob1) -> {
                        BATCH1_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                        BATCH2_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                        BATCH3_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    }
            );

            ts2 = assertPartialCursor(
                    cursor, exp, ts2, increment, blob, count, (r, rnd12, ts12, blob12) -> {
                        BATCH2_ASSERTER.assertRecord(r, rnd12, ts12, blob12);
                        BATCH3_BEFORE_ASSERTER.assertRecord(r, rnd12, ts12, blob12);
                    }
            );

            assertPartialCursor(cursor, exp, ts2, increment, blob, count, BATCH3_ASSERTER);
        }
    }

    private void assertBatch4(int count, long increment, long ts, long blob, TableReader reader) {
        Rnd exp;
        long ts2;
        exp = new Rnd();
        try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)) {
            ts2 = assertPartialCursor(
                    cursor, exp, ts, increment, blob, 3L * count, (r, rnd1, ts1, blob1) -> {
                        BATCH1_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                        BATCH2_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                        BATCH3_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                        BATCH4_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    }
            );

            ts2 = assertPartialCursor(
                    cursor, exp, ts2, increment, blob, count, (r, rnd12, ts12, blob12) -> {
                        BATCH2_ASSERTER.assertRecord(r, rnd12, ts12, blob12);
                        BATCH3_BEFORE_ASSERTER.assertRecord(r, rnd12, ts12, blob12);
                        BATCH4_BEFORE_ASSERTER.assertRecord(r, rnd12, ts12, blob12);
                    }
            );

            ts2 = assertPartialCursor(
                    cursor, exp, ts2, increment, blob, count, (r, rnd14, ts14, blob14) -> {
                        BATCH4_BEFORE_ASSERTER.assertRecord(r, rnd14, ts14, blob14);
                        BATCH3_ASSERTER.assertRecord(r, rnd14, ts14, blob14);
                    }
            );

            assertPartialCursor(cursor, exp, ts2, increment, blob, count, BATCH4_ASSERTER);
        }
    }

    private long assertBatch5(int count, long increment, long ts, long blob, RecordCursor cursor, Rnd exp) {
        long ts2;

        cursor.toTop();
        ts2 = assertPartialCursor(
                cursor, exp, ts, increment, blob, 3L * count, (r, rnd1, ts1, blob1) -> {
                    BATCH1_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    BATCH2_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    BATCH3_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    BATCH5_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                }
        );

        ts2 = assertPartialCursor(
                cursor, exp, ts2, increment, blob, count, (r, rnd1, ts1, blob1) -> {
                    BATCH2_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    BATCH3_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    BATCH5_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                }
        );

        ts2 = assertPartialCursor(
                cursor, exp, ts2, increment, blob, count, (r, rnd1, ts1, blob1) -> {
                    BATCH5_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    BATCH3_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                }
        );

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
        long ts2 = assertPartialCursor(
                cursor, exp, ts, increment, blob, 3L * count, (r, rnd1, ts1, blob1) -> {
                    BATCH1_7_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    BATCH_2_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    BATCH_3_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    BATCH_4_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                }
        );

        ts2 = assertPartialCursor(
                cursor, exp, ts2, increment, blob, count, (r, rnd1, ts1, blob1) -> {
                    BATCH2_7_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    BATCH_3_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    BATCH_4_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                }
        );

        ts2 = assertPartialCursor(
                cursor, exp, ts2, increment, blob, count, (r, rnd1, ts1, blob1) -> {
                    BATCH3_7_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    BATCH_4_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                }
        );

        ts2 = assertPartialCursor(cursor, exp, ts2, increment, blob, count, BATCH5_7_ASSERTER);
        assertPartialCursor(cursor, exp, ts2, increment, blob, count, BATCH6_7_ASSERTER);
    }

    private void assertBatch8(int count, long increment, long ts, long blob, RecordCursor cursor) {
        cursor.toTop();
        Rnd exp = new Rnd();
        long ts2 = assertPartialCursor(
                cursor, exp, ts, increment, blob, 3L * count, (r, rnd1, ts1, blob1) -> {
                    BATCH1_7_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    BATCH_2_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    BATCH_3_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    BATCH_4_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                }
        );

        ts2 = assertPartialCursor(
                cursor, exp, ts2, increment, blob, count, (r, rnd1, ts1, blob1) -> {
                    BATCH2_7_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    BATCH_3_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    BATCH_4_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                }
        );

        ts2 = assertPartialCursor(
                cursor, exp, ts2, increment, blob, count, (r, rnd1, ts1, blob1) -> {
                    BATCH3_7_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    BATCH_4_7_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                }
        );

        ts2 = assertPartialCursor(cursor, exp, ts2, increment, blob, count, BATCH5_7_ASSERTER);
        ts2 = assertPartialCursor(cursor, exp, ts2, increment, blob, count, BATCH6_7_ASSERTER);
        assertPartialCursor(cursor, exp, ts2, increment, blob, count, BATCH8_ASSERTER);
    }

    private void assertBatch9(int count, long increment, long ts, long blob, RecordCursor cursor) {
        cursor.toTop();
        Rnd exp = new Rnd();
        long ts2 = assertPartialCursor(
                cursor, exp, ts, increment, blob, 3L * count, (r, rnd1, ts1, blob1) -> {
                    BATCH1_9_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    BATCH_2_9_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    BATCH_3_9_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    BATCH_4_9_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                }
        );

        ts2 = assertPartialCursor(
                cursor, exp, ts2, increment, blob, count, (r, rnd1, ts1, blob1) -> {
                    BATCH2_9_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    BATCH_3_9_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    BATCH_4_9_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                }
        );

        ts2 = assertPartialCursor(
                cursor, exp, ts2, increment, blob, count, (r, rnd1, ts1, blob1) -> {
                    BATCH3_9_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                    BATCH_4_9_BEFORE_ASSERTER.assertRecord(r, rnd1, ts1, blob1);
                }
        );

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
        TableModel model = new TableModel(configuration, tableName, partitionBy);
        model.timestamp(timestampType);
        return AbstractCairoTest.create(model);
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
        try (TableWriter writer = newOffPoolWriter(configuration, "all")) {
            return testAppend(writer, rnd, ts, count, inc, blob, testPartitionSwitch, TableReaderTest.BATCH1_GENERATOR);
        }
    }

    private void testAsyncColumnRename(AtomicInteger counterRef, TestFilesFacade ff, String columnName) throws Exception {
        assertMemoryLeak(
                ff, () -> {
                    Rnd rnd = new Rnd();
                    final int N = 1000;

                    // populate table and delete column
                    try (TableWriter writer = getWriter("x")) {
                        appendTwoSymbols(writer, rnd, 1);
                        writer.commit();

                        try (
                                TableReader reader = newOffPoolReader(configuration, "x");
                                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
                        ) {
                            long counter = 0;

                            rnd.reset();
                            final Record record = cursor.getRecord();
                            while (cursor.hasNext()) {
                                Assert.assertEquals(rnd.nextChars(10), record.getSymA(0));
                                Assert.assertEquals(rnd.nextChars(15), record.getSymA(1));
                                counter++;
                            }

                            Assert.assertEquals(N, counter);

                            // this should write metadata without column "b" but will ignore
                            // file delete failures
                            writer.renameColumn(columnName, "d");

                            // now when we add new column by same name it must not pick up files we failed to delete previously
                            writer.addColumn(columnName, ColumnType.SYMBOL, AllowAllSecurityContext.INSTANCE);

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
                                Assert.assertEquals(rnd.nextChars(10), record.getSymA(0));
                                if (counter < N) {
                                    // roll random generator to make sure it returns same values
                                    rnd.nextChars(15);
                                    Assert.assertNull(record.getSymA(2));
                                } else {
                                    Assert.assertEquals(rnd.nextChars(15), record.getSymA(2));
                                }
                                counter++;
                            }

                            Assert.assertEquals(N * 2, counter);
                        }
                    }
                    engine.releaseInactive();
                    checkColumnPurgeRemovesFiles(counterRef, ff, 6);
                }
        );
    }

    private void testConcurrentReloadMultiplePartitions(int partitionBy, long stride1) throws Exception {
        assertMemoryLeak(() -> {
            final int N = 1024_0000;
            long stride = timestampDriver.fromMicros(stride1);
            // model table
            TableModel model = new TableModel(configuration, "w", partitionBy).col("l", ColumnType.LONG).timestamp(timestampType);
            AbstractCairoTest.create(model);

            final int threads = 2;
            final CyclicBarrier startBarrier = new CyclicBarrier(threads);
            final SOCountDownLatch stopLatch = new SOCountDownLatch(threads);
            final AtomicInteger errors = new AtomicInteger(0);

            // start writer
            new Thread(() -> {
                try {
                    startBarrier.await();
                    long timestampUs = timestampDriver.parseFloorLiteral("2017-12-11T00:00:00.000Z");
                    try (TableWriter writer = newOffPoolWriter(configuration, "w")) {
                        for (int i = 0; i < N; i++) {
                            TableWriter.Row row = writer.newRow(timestampUs);
                            row.putLong(0, i);
                            row.append();
                            writer.commit();
                            timestampUs += stride;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    errors.incrementAndGet();
                } finally {
                    Path.clearThreadLocals();
                    stopLatch.countDown();
                }
            }).start();

            // start reader
            new Thread(() -> {
                try {
                    startBarrier.await();
                    try (
                            TableReader reader = newOffPoolReader(configuration, "w");
                            TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
                    ) {
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
                    e.printStackTrace(System.out);
                    errors.incrementAndGet();
                } finally {
                    Path.clearThreadLocals();
                    stopLatch.countDown();
                }
            }).start();

            stopLatch.await();
            Assert.assertEquals(0, errors.get());

            // check that we had multiple partitions created during the test
            try (TableReader reader = newOffPoolReader(configuration, "w")) {
                Assert.assertTrue(reader.getPartitionCount() > 10);
            }
        });
    }

    private void testReload(int partitionBy, int count, long inc, final int testPartitionSwitch) throws Exception {
        final long increment = timestampDriver.fromMillis(inc);

        CreateTableTestUtils.createAllTable(engine, partitionBy, timestampType);

        assertMemoryLeak(() -> {
            Rnd rnd = new Rnd();

            long ts = timestampDriver.parseFloorLiteral("2013-03-04T00:00:00.000Z");

            long blob = allocBlob();
            try {
                // test if reader behaves correctly when table is empty

                try (
                        TableReader reader = newOffPoolReader(configuration, "all");
                        TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
                ) {
                    // can we reload empty table?
                    Assert.assertFalse(reader.reload());
                    // reader can see all the rows ? Meaning none?
                    assertCursor(cursor, ts, increment, blob, 0, null);
                }

                try (
                        TableReader reader = newOffPoolReader(configuration, "all");
                        TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
                ) {
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
                    try (
                            TableReader reader2 = newOffPoolReader(configuration, "all");
                            TestTableReaderRecordCursor cursor2 = new TestTableReaderRecordCursor().of(reader2)
                    ) {
                        // make sure we can see first batch right after table is open
                        assertCursor(cursor2, ts, increment, blob, count, BATCH1_ASSERTER);
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
                    try (TableWriter writer = newOffPoolWriter(configuration, "all")) {
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
                        writer.addColumn("str2", ColumnType.STRING, AllowAllSecurityContext.INSTANCE);

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

                        writer.addColumn("int2", ColumnType.INT, AllowAllSecurityContext.INSTANCE);

                        nextTs = testAppend(writer, rnd, nextTs, count, increment, blob, 0, BATCH3_GENERATOR);

                        Assert.assertTrue(reader.reload());
                        assertOpenPartitionCount(reader);

                        assertBatch3(count, increment, ts, blob, reader);

                        // now append more columns that would overflow column buffer and force table to use different
                        // algo when retaining resources

                        writer.addColumn("short2", ColumnType.SHORT, AllowAllSecurityContext.INSTANCE);
                        writer.addColumn("bool2", ColumnType.BOOLEAN, AllowAllSecurityContext.INSTANCE);
                        writer.addColumn("byte2", ColumnType.BYTE, AllowAllSecurityContext.INSTANCE);
                        writer.addColumn("float2", ColumnType.FLOAT, AllowAllSecurityContext.INSTANCE);
                        writer.addColumn("double2", ColumnType.DOUBLE, AllowAllSecurityContext.INSTANCE);
                        writer.addColumn("sym2", ColumnType.SYMBOL, AllowAllSecurityContext.INSTANCE);
                        writer.addColumn("long2", ColumnType.LONG, AllowAllSecurityContext.INSTANCE);
                        writer.addColumn("date2", ColumnType.DATE, AllowAllSecurityContext.INSTANCE);
                        writer.addColumn("bin2", ColumnType.BINARY, AllowAllSecurityContext.INSTANCE);

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
                        writer.addColumn("int", ColumnType.INT, AllowAllSecurityContext.INSTANCE);

                        Assert.assertTrue(reader.reload());
                        assertOpenPartitionCount(reader);

                        assertBatch7(count, increment, ts, blob, cursor);

                        Assert.assertFalse(reader.reload());

                        nextTs = testAppend(writer, rnd, nextTs, count, increment, blob, 0, BATCH8_GENERATOR);

                        Assert.assertTrue(reader.reload());
                        assertOpenPartitionCount(reader);

                        assertBatch8(count, increment, ts, blob, cursor);

                        writer.removeColumn("sym");
                        writer.addColumn("sym", ColumnType.SYMBOL, AllowAllSecurityContext.INSTANCE);
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
        assertMemoryLeak(() -> {
            final int N = 100;
            final int N_PARTITIONS = 5;
            final long stride = 100;
            final int bandStride = 1000;
            final String tableName = "table_by_" + PartitionBy.toString(partitionBy);

            int totalCount = 0;
            long timestampUs = timestampDriver.parseFloorLiteral("2017-12-11T00:00:00.000Z");

            // model table
            TableModel model = new TableModel(configuration, tableName, partitionBy).col("l", ColumnType.LONG).timestamp(timestampType);
            AbstractCairoTest.create(model);

            try (TableWriter writer = newOffPoolWriter(configuration, tableName)) {
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
                try (
                        TableReader reader = newOffPoolReader(configuration, tableName);
                        TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
                ) {
                    Assert.assertEquals(expectedSize, reader.size());
                    Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        record.getLong(0);
                        totalCount++;
                    }
                    Assert.assertEquals(expectedSize, totalCount);

                    DateFormat fmt = PartitionBy.getPartitionDirFormatMethod(reader.getMetadata().getTimestampType(), partitionBy);
                    Assert.assertTrue(
                            // active partition
                            writer.removePartition(fmt.parse(partitionNameToDelete, EN_LOCALE))
                    );

                    // check writer
                    final long newExpectedSize = (N_PARTITIONS - 1) * N;
                    Assert.assertEquals(newExpectedSize, writer.size());

                    // check reader
                    reader.reload();
                    totalCount = 0;
                    Assert.assertEquals(newExpectedSize, reader.size());
                    cursor.toTop();
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
        assertMemoryLeak(() -> {
            int N = 100;
            int N_PARTITIONS = 5;
            long timestampUs = timestampDriver.parseFloorLiteral("2017-12-11T10:00:00.000Z");
            long stride = 100;
            int bandStride = 1000;
            int totalCount = 0;

            // model table
            TableModel model = new TableModel(configuration, "w", partitionBy).col("l", ColumnType.LONG).timestamp(timestampType);
            AbstractCairoTest.create(model);

            try (TableWriter writer = newOffPoolWriter(configuration, "w")) {
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

                DateFormat fmt = PartitionBy.getPartitionDirFormatMethod(writer.getMetadata().getTimestampType(), partitionBy);
                final long timestamp = fmt.parse(partitionNameToDelete, EN_LOCALE);

                Assert.assertTrue(writer.removePartition(timestamp));
                Assert.assertFalse(writer.removePartition(timestamp));

                Assert.assertEquals(N * (N_PARTITIONS - 1), writer.size());
            }

            // now open table reader having partition gap
            try (
                    TableReader reader = newOffPoolReader(configuration, "w");
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                Assert.assertEquals(N * (N_PARTITIONS - 1), reader.size());

                int previousBand = -1;
                int bandCount = 0;
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
        assertMemoryLeak(() -> {
            int N = 100;
            int N_PARTITIONS = 5;
            long timestampUs = timestampDriver.parseFloorLiteral("2017-12-11T00:00:00.000Z");
            long stride = 100;
            int bandStride = 1000;
            int totalCount = 0;

            // model table
            TableModel model = new TableModel(configuration, "w", partitionBy).col("l", ColumnType.LONG).timestamp(timestampType);
            AbstractCairoTest.create(model);

            try (TableWriter writer = newOffPoolWriter(configuration, "w")) {
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
                try (
                        TableReader reader = newOffPoolReader(configuration, "w");
                        TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
                ) {
                    Assert.assertEquals(N * N_PARTITIONS, reader.size());
                    Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        record.getLong(0);
                        totalCount++;
                    }
                    Assert.assertEquals(N * N_PARTITIONS, totalCount);

                    DateFormat fmt = PartitionBy.getPartitionDirFormatMethod(reader.getMetadata().getTimestampType(), partitionBy);
                    Assert.assertTrue(
                            writer.removePartition(fmt.parse(partitionNameToDelete, EN_LOCALE))
                    );

                    Assert.assertEquals(N * (N_PARTITIONS - 1), writer.size());

                    reader.reload();

                    totalCount = 0;

                    Assert.assertEquals(N * (N_PARTITIONS - 1), reader.size());

                    int previousBand = -1;
                    int bandCount = 0;
                    cursor.toTop();
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
        assertMemoryLeak(
                ff, () -> {
                    // populate table and delete column
                    try (TableWriter writer = getWriter("x")) {
                        appendTwoSymbols(writer, rnd, 1);
                        writer.commit();

                        try (
                                TableReader reader = getReader("x");
                                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
                        ) {
                            long counter = 0;

                            rnd.reset();
                            final Record record = cursor.getRecord();
                            while (cursor.hasNext()) {
                                Assert.assertEquals(rnd.nextChars(10), record.getSymA(0));
                                Assert.assertEquals(rnd.nextChars(15), record.getSymA(1));
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
                                Assert.assertEquals(rnd.nextChars(10), record.getSymA(0));
                                // roll random generator to make sure it returns same values
                                rnd.nextChars(15);
                                counter++;
                            }

                            Assert.assertEquals(N, counter);
                        }
                    }

                    checkColumnPurgeRemovesFiles(counterRef, ff, deleteAttempts);
                }
        );
    }

    private void testTableCursor(long inc) throws NumericException {
        Rnd rnd = new Rnd();
        int N = 100;
        long ts = timestampDriver.parseFloorLiteral("2013-03-04T00:00:00.000Z") / 1000;
        long blob = allocBlob();
        try {
            testAppend(rnd, configuration, ts, N, inc, blob, 0);
            final LongList rows = new LongList();
            try (
                    TableReader reader = newOffPoolReader(configuration, "all");
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                Assert.assertEquals(N, reader.size());

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

    boolean isSamePartition(long timestampA, long timestampB, int partitionBy) {
        return switch (partitionBy) {
            case PartitionBy.NONE -> true;
            case PartitionBy.DAY, PartitionBy.MONTH, PartitionBy.WEEK, PartitionBy.YEAR, PartitionBy.HOUR -> {
                TimestampDriver.TimestampFloorMethod partitionByMethod = timestampDriver.getPartitionFloorMethod(partitionBy);
                yield partitionByMethod.floor(timestampA) == partitionByMethod.floor(timestampB);
            }
            default ->
                    throw CairoException.critical(0).put("Cannot compare timestamps for unsupported partition type: [").put(partitionBy).put(']');
        };
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
