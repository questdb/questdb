/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.arr.NoopArrayWriteState;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cutlass.line.tcp.ArrayBinaryFormatParser;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ArrayTest extends AbstractCairoTest {

    public static long arrayViewToBinaryFormat(DirectArray array, long addr) {
        long offset = 0;
        Unsafe.getUnsafe().putByte(addr + offset, (byte) array.getElemType());
        offset++;
        Unsafe.getUnsafe().putByte(addr + offset, (byte) array.getDimCount());
        offset++;
        for (int i = 0, dims = array.getDimCount(); i < dims; i++) {
            Unsafe.getUnsafe().putInt(addr + offset, array.getDimLen(i));
            offset += 4;
        }
        int flatSize = array.borrowedFlatView().size();
        Vect.memcpy(addr + offset, array.ptr(), flatSize);
        return offset + flatSize;
    }

    @Test
    public void test2dFrom1d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE samba (ask_price DOUBLE[], ask_size DOUBLE[])");
            execute("CREATE TABLE tango (ask DOUBLE[][])");
            execute("INSERT INTO samba VALUES (ARRAY[1.0, 2, 3], ARRAY[4.0, 5, 6]), (ARRAY[7.0, 8, 9], ARRAY[10.0, 11, 12])");
            execute("INSERT INTO tango SELECT ARRAY[[ask_price[1], ask_price[2]], [ask_size[1], ask_size[2]]] FROM samba");
            execute("INSERT INTO tango SELECT ARRAY[ask_price, ask_size] FROM samba");
            execute("INSERT INTO tango SELECT ARRAY[ask_price[1:3], ask_size[2:4]] FROM samba");
            assertSql("ask\n" +
                            "[[1.0,2.0],[4.0,5.0]]\n" +
                            "[[7.0,8.0],[10.0,11.0]]\n" +
                            "[[1.0,2.0,3.0],[4.0,5.0,6.0]]\n" +
                            "[[7.0,8.0,9.0],[10.0,11.0,12.0]]\n" +
                            "[[1.0,2.0],[5.0,6.0]]\n" +
                            "[[7.0,8.0],[11.0,12.0]]\n",
                    "tango");
        });
    }

    @Test
    public void testAccess1d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT ARRAY[1.0, 2, 3] arr1, ARRAY[1.0, 2, 3] arr2 FROM long_sequence(1))");
            execute("INSERT INTO tango VALUES (ARRAY[1.0, 2, 3], null)");
            execute("INSERT INTO tango VALUES (null, null)");
            assertSql("x\n2.0\n2.0\nnull\n", "SELECT arr1[2] x FROM tango");
            assertSql("x\n2.0\n2.0\nnull\n", "SELECT arr1[arr1[2]::int] x FROM tango");
            assertSql("x\n2.0\nnull\nnull\n", "SELECT arr1[arr2[2]::int] x FROM tango");
            assertPlanNoLeakCheck(
                    "SELECT arr1[arr2[2]::int] x FROM tango",
                    "VirtualRecord\n" +
                            "  functions: [arr1[arr2[2]::int]]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tango\n"
            );
        });
    }

    @Test
    public void testAccess3d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT " +
                    "ARRAY[ [[1.0, 2], [3.0, 4]], [[5.0, 6], [7.0, 8]] ] arr FROM long_sequence(1))");
            assertSql("x\n2.0\n", "SELECT arr[1, 1, 2] x FROM tango");
            assertSql("x\n6.0\n", "SELECT arr[2, 1, 2] x FROM tango");
            assertSql("x\n8.0\n", "SELECT arr[2, 2, 2] x FROM tango");
            assertSql("x\n8.0\n", "SELECT arr[2, 2][2] x FROM tango");
            assertSql("x\n8.0\n", "SELECT arr[2][2, 2] x FROM tango");
            assertSql("x\n8.0\n", "SELECT arr[2][2][2] x FROM tango");
            assertPlanNoLeakCheck(
                    "SELECT arr[2][2][2] x FROM tango",
                    "VirtualRecord\n" +
                            "  functions: [arr[2,2,2]]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tango\n"
            );
        });
    }

    @Test
    public void testAccessInvalid() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT ARRAY[[1.0, 2], [3.0, 4]] arr1, ARRAY[-1, -2] arr2 FROM long_sequence(1))");

            assertExceptionNoLeakCheck("SELECT arr1[] FROM tango",
                    12, "empty brackets");
            assertExceptionNoLeakCheck("SELECT arr1['1', 1] FROM tango",
                    12, "invalid type for array access [type=4]");
            assertExceptionNoLeakCheck("SELECT arr1[1, 1::long] FROM tango",
                    16, "invalid type for array access [type=6]");
            assertExceptionNoLeakCheck("SELECT arr1[1, true] FROM tango",
                    15, "invalid type for array access [type=1]");
            assertExceptionNoLeakCheck("SELECT arr1[1, '1'] FROM tango",
                    15, "invalid type for array access [type=4]");
            assertExceptionNoLeakCheck("SELECT arr1[1, 1, 1] FROM tango",
                    18, "too many array access arguments [nArgs=3, nDims=2]");
            assertExceptionNoLeakCheck("SELECT arr1[0, 1] FROM tango",
                    12, "array index must be positive [dim=1, index=0]");
            assertExceptionNoLeakCheck("SELECT arr1[1:2, 0] FROM tango",
                    17, "array index must be positive [dim=2, index=0]");
            assertExceptionNoLeakCheck("SELECT arr1[1, 0] FROM tango",
                    15, "array index must be positive [dim=2, index=0]");
            assertExceptionNoLeakCheck("SELECT arr1[1, arr2[1]::int] FROM tango",
                    22, "array index must be positive [dim=2, index=-1, dimLen=2]");
            assertExceptionNoLeakCheck("SELECT arr1[1:2][arr2[1]::int] FROM tango",
                    24, "array index must be positive [dim=1, index=-1, dimLen=1]");
        });
    }

    @Test
    public void testAccessOutOfBounds() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT ARRAY[[1.0, 2], [3.0, 4]] arr FROM long_sequence(1))");

            assertSql("x\nnull\n", "SELECT arr[1, 3] x FROM tango");
            assertSql("x\nnull\n", "SELECT arr[3, 1] x FROM tango");

            assertSql("x\n[]\n", "SELECT arr[1:1] x FROM tango");
            assertSql("x\n[]\n", "SELECT arr[2:1] x FROM tango");
            assertSql("x\n[[3.0,4.0]]\n", "SELECT arr[2:5] x FROM tango");
            assertSql("x\n[]\n", "SELECT arr[3:3] x FROM tango");
            assertSql("x\n[]\n", "SELECT arr[3:5] x FROM tango");

            assertSql("x\n[]\n", "SELECT arr[1, 1:1] x FROM tango");
            assertSql("x\n[]\n", "SELECT arr[1, 2:1] x FROM tango");
            assertSql("x\n[]\n", "SELECT arr[1, 3:3] x FROM tango");
            assertSql("x\n[]\n", "SELECT arr[1, 3:5] x FROM tango");
            assertSql("x\n[]\n", "SELECT arr[1, 3:] x FROM tango");

            assertSql("x\n[2.0]\n", "SELECT arr[1, 2:5] x FROM tango");
        });
    }

    @Test
    public void testAccessWithNonConstants() throws Exception {
        assertMemoryLeak(() -> {
            String subArr11 = "[1.0,2.0]";
            String subArr12 = "[3.0,4.0]";
            String subArr21 = "[5.0,6.0]";
            String subArr22 = "[7.0,8.0]";
            String subArr1 = "[" + subArr11 + "," + subArr12 + "]";
            String subArr2 = "[" + subArr21 + "," + subArr22 + "]";
            String fullArray = "[" + subArr1 + "," + subArr2 + "]";
            execute("CREATE TABLE tango AS (SELECT 1 i, 2 j, ARRAY" + fullArray + " arr FROM long_sequence(1))");
            assertSql("x\n" + subArr1 + "\n", "SELECT arr[i] x FROM tango");
            assertSql("x\n" + subArr1 + "\n", "SELECT arr[j-i] x FROM tango");
            assertSql("x\n" + subArr12 + "\n", "SELECT arr[i,j] x FROM tango");
            assertSql("x\n[" + subArr1 + "]\n", "SELECT arr[i:j] x FROM tango");
            assertSql("x\n[" + subArr1 + "]\n", "SELECT arr[i:j+j-i-i] x FROM tango");
            assertSql("x\n[" + subArr1 + "]\n", "SELECT arr[j-i:i+i] x FROM tango");
        });
    }

    @Test
    public void testAccessWithNullIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (n INT, arr DOUBLE[], arr2 DOUBLE[][])");
            execute("INSERT INTO tango VALUES (null, ARRAY[1.0, 2], ARRAY[[1.0, 2], [3.0, 4]])");
            assertSql("[]\nnull\n", "SELECT arr[null::int] FROM tango");
            assertSql("[]\nnull\n", "SELECT arr[n] FROM tango");
            assertSql("[]\nnull\n", "SELECT arr[1:null] FROM tango");
            assertSql("[]\nnull\n", "SELECT arr[null:2] FROM tango");
            assertSql("[]\nnull\n", "SELECT arr[1:n] FROM tango");
            assertSql("[]\nnull\n", "SELECT arr[n:2] FROM tango");
            assertSql("[]\nnull\n", "SELECT arr2[1, null::int] FROM tango");
            assertSql("[]\nnull\n", "SELECT arr2[1, n] FROM tango");
            assertSql("[]\nnull\n", "SELECT arr2[1, n:2] FROM tango");
            assertSql("[]\nnull\n", "SELECT arr2[1, 1:n] FROM tango");
            assertSql("[]\nnull\n", "SELECT arr2[1:2, 1:n] FROM tango");
        });
    }

    @Test
    public void testAddColumnUnsupportedType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (n LONG)");
            assertException("ALTER TABLE tango ADD COLUMN arr BYTE[]", 33, "unsupported array element type [type=BYTE]");
            assertException("ALTER TABLE tango ADD COLUMN arr SHORT[]", 33, "unsupported array element type [type=SHORT]");
            assertException("ALTER TABLE tango ADD COLUMN arr INT[]", 33, "unsupported array element type [type=INT]");
            assertException("ALTER TABLE tango ADD COLUMN arr LONG[]", 33, "unsupported array element type [type=LONG]");
            assertException("ALTER TABLE tango ADD COLUMN arr FLOAT[]", 33, "unsupported array element type [type=FLOAT]");
            assertException("ALTER TABLE tango ADD COLUMN arr BOOLEAN[]", 33, "unsupported array element type [type=BOOLEAN]");
            assertException("ALTER TABLE tango ADD COLUMN arr CHAR[]", 33, "unsupported array element type [type=CHAR]");
            assertException("ALTER TABLE tango ADD COLUMN arr STRING[]", 33, "unsupported array element type [type=STRING]");
            assertException("ALTER TABLE tango ADD COLUMN arr VARCHAR[]", 33, "unsupported array element type [type=VARCHAR]");
            assertException("ALTER TABLE tango ADD COLUMN arr ARRAY[]", 33, "unsupported array element type [type=ARRAY]");
            assertException("ALTER TABLE tango ADD COLUMN arr BINARY[]", 33, "unsupported array element type [type=BINARY]");
            assertException("ALTER TABLE tango ADD COLUMN arr DATE[]", 33, "unsupported array element type [type=DATE]");
            assertException("ALTER TABLE tango ADD COLUMN arr TIMESTAMP[]", 33, "unsupported array element type [type=TIMESTAMP]");
            assertException("ALTER TABLE tango ADD COLUMN arr UUID[]", 33, "unsupported array element type [type=UUID]");
            assertException("ALTER TABLE tango ADD COLUMN arr LONG128[]", 33, "unsupported array element type [type=LONG128]");
            assertException("ALTER TABLE tango ADD COLUMN arr GEOHASH[]", 33, "unsupported array element type [type=GEOHASH]");
        });
    }

    @Test
    public void testArrayCanBeClearedAfterInstantiation() throws Exception {
        assertMemoryLeak(() -> {
            try (DirectArray array = new DirectArray(configuration)) {
                array.clear();
            }
        });
    }

    @Test
    public void testAutoCastToDouble() throws Exception {
        assertMemoryLeak(() -> {
            assertSql("arr\n[1.0,2.0]\n", "SELECT ARRAY[1, 2] arr FROM long_sequence(1)");
            assertSql("arr\n[[1.0,2.0],[3.0,4.0]]\n", "SELECT ARRAY[[1, 2], [3, 4]] arr FROM long_sequence(1)");
        });
    }

    @Test
    public void testBasicArithmetic1d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE[], b DOUBLE[])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[2.0, 3.0], ARRAY[4.0, 5]), " +
                    "(ARRAY[6.0, 7], ARRAY[8.0, 9])");
            assertSql("sum\n[6.0,8.0]\n[14.0,16.0]\n", "SELECT a + b sum FROM tango");
            assertSql("diff\n[-2.0,-2.0]\n[-2.0,-2.0]\n", "SELECT a - b diff FROM tango");
            assertSql("product\n[8.0,15.0]\n[48.0,63.0]\n", "SELECT a * b product FROM tango");
        });
    }

    @Test
    public void testBasicArithmetic3d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE[][][], b DOUBLE[][][])");
            execute("INSERT INTO tango VALUES (" +
                    "ARRAY[ [ [2.0, 3], [4.0, 5] ], [ [6.0, 7], [8.0, 9] ]  ], " +
                    "ARRAY[ [ [10.0, 11], [12.0, 13] ], [ [14.0, 15], [16.0, 17] ]  ]" +
                    ")");
            assertSql("sum\n[[[12.0,14.0],[16.0,18.0]],[[20.0,22.0],[24.0,26.0]]]\n",
                    "SELECT a + b sum FROM tango");
            assertSql("diff\n[[[-8.0,-8.0],[-8.0,-8.0]],[[-8.0,-8.0],[-8.0,-8.0]]]\n",
                    "SELECT a - b diff FROM tango");
            assertSql("product\n[[[20.0,33.0],[48.0,65.0]],[[84.0,105.0],[128.0,153.0]]]\n",
                    "SELECT a * b product FROM tango");
        });
    }

    @Test
    public void testBasicArithmeticInvalid() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE[], b DOUBLE[], c DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[2.0, 3], ARRAY[4.0, 5, 6], ARRAY[ [1.0, 2], [3.0, 4] ])");
            assertException("SELECT a + c from tango", 7,
                    "arrays have different number of dimensions [nDimsLeft=1, nDimsRight=2]");
            assertException("SELECT a + b from tango", 7,
                    "arrays have different shapes [leftShape=[2], rightShape=[3]]");
        });
    }

    @Test
    public void testCaseWhen() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP, i int, a DOUBLE[]) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO tango VALUES " +
                    "('2020-01-01T00:00:00.000Z', 0, ARRAY[]), " +
                    "('2021-01-01T00:00:00.000Z', 1, ARRAY[-1.0]), " +
                    "('2022-01-01T00:00:00.000Z', 2, ARRAY[-1.0, -2.0]), " +
                    "('2023-01-01T00:00:00.000Z', 3, ARRAY[-1.0, -2.0, -3.0]), " +
                    "('2024-01-01T00:00:00.000Z', 4, ARRAY[-1.0, -2.0, -3.0, -4.0]), " +
                    "('2025-01-01T00:00:00.000Z', 5, ARRAY[-1.0, -2.0, -3.0, -4.0, -5.0]);"
            );

            drainWalQueue();

            assertQuery("case\tts\ti\ta\n" +
                            "[]\t2020-01-01T00:00:00.000000Z\t0\t[]\n" +
                            "[1.0]\t2021-01-01T00:00:00.000000Z\t1\t[-1.0]\n" +
                            "[1.0,2.0]\t2022-01-01T00:00:00.000000Z\t2\t[-1.0,-2.0]\n" +
                            "[1.0,2.0,3.0]\t2023-01-01T00:00:00.000000Z\t3\t[-1.0,-2.0,-3.0]\n" +
                            "[1.0,2.0,3.0,4.0]\t2024-01-01T00:00:00.000000Z\t4\t[-1.0,-2.0,-3.0,-4.0]\n" +
                            "[-1.0,-2.0,-3.0,-4.0,-5.0]\t2025-01-01T00:00:00.000000Z\t5\t[-1.0,-2.0,-3.0,-4.0,-5.0]\n",
                    "select \n" +
                            "  case \n" +
                            "    when ts in '2020' then array[]::double[]\n" +
                            "    when ts in '2021' then array[1.0] \n" +
                            "    when ts in '2022' then '{1.0, 2.0}'::double[] \n" +
                            "    when ts in '2023' then array[1.0, 2.0, 3.0] \n" +
                            "    when ts in '2024' then array[1.0, 2.0, 3.0, 4.0] \n" +
                            "    else a \n" +
                            "  end, *\n" +
                            "from tango;",
                    null,
                    "ts",
                    true,
                    true);

            assertQuery("case\tts\ti\ta\n" +
                            "empty\t2020-01-01T00:00:00.000000Z\t0\t[]\n" +
                            "literal\t2021-01-01T00:00:00.000000Z\t1\t[-1.0]\n" +
                            "casting\t2022-01-01T00:00:00.000000Z\t2\t[-1.0,-2.0]\n" +
                            "whatever\t2023-01-01T00:00:00.000000Z\t3\t[-1.0,-2.0,-3.0]\n" +
                            "whatever\t2024-01-01T00:00:00.000000Z\t4\t[-1.0,-2.0,-3.0,-4.0]\n" +
                            "whatever\t2025-01-01T00:00:00.000000Z\t5\t[-1.0,-2.0,-3.0,-4.0,-5.0]\n",
                    "select \n" +
                            "  case \n" +
                            "    when a = ARRAY[-1.0] then 'literal'\n" +
                            "    when a = '{-1,-2}'::double[] then 'casting'\n" +
                            "    when a = ARRAY[[1.0],[2.0]] then 'never'\n" +
                            "    when a = ARRAY[]::double[] then 'empty'\n" +
                            "    else 'whatever'\n" +
                            "  end, *\n" +
                            "from tango;",
                    null,
                    "ts",
                    true,
                    true);

            assertException("select \n" +
                            "  case \n" +
                            "    when ts in '2021' then array[1.0] \n" +
                            "    when ts in '2024' then 1 \n" +
                            "    else a \n" +
                            "  end, *\n" +
                            "from tango;",
                    82,
                    "inconvertible types: INT -> DOUBLE[]");
        });
    }

    @Test
    public void testChangeColumnToUnsupportedType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (n LONG)");
            assertException("ALTER TABLE tango ALTER COLUMN n TYPE BYTE[]", 38, "unsupported array element type [type=BYTE]");
            assertException("ALTER TABLE tango ALTER COLUMN n TYPE SHORT[]", 38, "unsupported array element type [type=SHORT]");
            assertException("ALTER TABLE tango ALTER COLUMN n TYPE INT[]", 38, "unsupported array element type [type=INT]");
            assertException("ALTER TABLE tango ALTER COLUMN n TYPE LONG[]", 38, "unsupported array element type [type=LONG]");
            assertException("ALTER TABLE tango ALTER COLUMN n TYPE FLOAT[]", 38, "unsupported array element type [type=FLOAT]");
            assertException("ALTER TABLE tango ALTER COLUMN n TYPE BOOLEAN[]", 38, "unsupported array element type [type=BOOLEAN]");
            assertException("ALTER TABLE tango ALTER COLUMN n TYPE CHAR[]", 38, "unsupported array element type [type=CHAR]");
            assertException("ALTER TABLE tango ALTER COLUMN n TYPE STRING[]", 38, "unsupported array element type [type=STRING]");
            assertException("ALTER TABLE tango ALTER COLUMN n TYPE VARCHAR[]", 38, "unsupported array element type [type=VARCHAR]");
            assertException("ALTER TABLE tango ALTER COLUMN n TYPE ARRAY[]", 38, "unsupported array element type [type=ARRAY]");
            assertException("ALTER TABLE tango ALTER COLUMN n TYPE BINARY[]", 38, "unsupported array element type [type=BINARY]");
            assertException("ALTER TABLE tango ALTER COLUMN n TYPE DATE[]", 38, "unsupported array element type [type=DATE]");
            assertException("ALTER TABLE tango ALTER COLUMN n TYPE TIMESTAMP[]", 38, "unsupported array element type [type=TIMESTAMP]");
            assertException("ALTER TABLE tango ALTER COLUMN n TYPE UUID[]", 38, "unsupported array element type [type=UUID]");
            assertException("ALTER TABLE tango ALTER COLUMN n TYPE LONG128[]", 38, "unsupported array element type [type=LONG128]");
            assertException("ALTER TABLE tango ALTER COLUMN n TYPE GEOHASH[]", 38, "unsupported array element type [type=GEOHASH]");
        });
    }

    @Test
    public void testConcatFailsGracefully() throws Exception {
        assertMemoryLeak(() -> assertException(
                "SELECT ARRAY[1.0] || ARRAY[2.0, 3.0] FROM long_sequence(1)",
                12,
                "unsupported type: DOUBLE[]"
        ));
    }

    @Test
    public void testCreateAsSelect2d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE, b DOUBLE)");
            execute("INSERT INTO tango VALUES (1.0, 2.0)");
            execute("CREATE TABLE samba AS (SELECT ARRAY[[a, a], [b, b]] arr FROM tango)");
            assertSql("arr\n[[1.0,1.0],[2.0,2.0]]\n", "samba");
        });
    }

    @Test
    public void testCreateAsSelectDoubleNoWAL() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table blah as (\n" +
                            "select rnd_varchar() a, rnd_double_array(1, 0) arr, rnd_int() b from long_sequence(10)\n" +
                            ");"
            );

            assertQuery(
                    "a\tarr\tb\n" +
                            "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t[0.4224356661645131,0.7094360487171202,0.38539947865244994,0.0035983672154330515,0.3288176907679504,0.021651819007252326,0.6217326707853098,0.8146807944500559,0.4022810626779558,0.38179758047769774]\t2137969456\n" +
                            "ޤ~2\uDAC6\uDED3ڎ\t[0.8847591603509142,0.4900510449885239,0.8258367614088108,0.04142812470232493,0.92050039469858,0.5182451971820676,0.8664158914718532,0.17370570324289436,0.5659429139861241]\t-1201923128\n" +
                            "kV>1c\t[0.5406709846540508]\t-89906802\n" +
                            "\uEB3Fiɮ\uD9D7\uDFE5\uDAE9\uDF46OFг\uDBAE\uDD12ɜ\t[0.6247427794126656,0.026836863013701473,0.03993124821273464,0.49765193229684157,0.07246172621937097,0.4892743433711657,0.8231249461985348,0.053594208204197136,0.26369335635512836,0.22895725920713628,0.9820662735672192,0.5357010561860446,0.8595900073631431,0.6583311519893554,0.8259739777067459]\t-1280991111\n" +
                            "qqjbzK.k\t[0.5992548493051852,0.6455967424250787,0.6202777455654276,0.029080850168636263,0.10459352312331183,0.5346019596733254,0.9418719455092096,0.6341292894843615,0.7340656260730631]\t-292438036\n" +
                            "\uDA30\uDEE01W씌䒙\uD8F2\uDE8E>\uDAE6\uDEE3g\t[0.7203170014947307,0.706473302224657,0.7422641630544511,0.04173263630897883,0.5677191487344088,0.2677326840703891,0.5425297056895126,0.09618589590900506,0.4835256202036067,0.868788610834602,0.49154607371672154]\t-1479209616\n" +
                            "1t:4x(b$Bl\t[0.5459599769700721]\t-1923096605\n" +
                            "\uD9EC\uDED3ץ;\t[0.5551537218890106,0.7195457109208119,0.5740181242665339,0.6001225339624721,0.30028279396280155,0.95820305972778,0.8407989131363496,0.28964821678040487,0.47705013264569973,0.6887925530449002,0.8108032283138068]\t1350645064\n" +
                            "Ʉh볱9\t[0.3004874521886858,0.3521084750492214,0.1511578096923386,0.18746631995449403,0.29150980082006395,0.7272119755925095,0.5676527283594215,0.8402964708129546,0.7732229848518976,0.587752738240427,0.4667778758533798,0.17498425722537903,0.9797944775606992,0.7795623293844108,0.29242748475227853,0.7527907209539796]\t389866845\n" +
                            "Q,?/qbOk\t[0.32093405888189597,0.8406396365644468,0.34224858614452547,0.27068535446692277,0.0031075670450616544,0.1238462308888072,0.20952665593357767,0.16923843067953104,0.7198854503668188,0.5174107449677378,0.38509066982448115]\t526232578\n",
                    "select * from blah",
                    true
            );
        });
    }

    @Test
    public void testCreateTableAllSupportedTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (" +
                            "d double[][][]" +
//                    ", b boolean[][][][]" +
//                    ", bt byte[][][][][][][][]" +
//                    ", f float[]" +
//                    ", i int[][]" +
//                    ", l long[][]" +
//                    ", s short[][][][][]" +
//                    ", dt date[][][][]" +
//                    ", ts timestamp[][]" +
//                    ", l2 long256[][][]" +
//                    ", u uuid[][][][]" +
//                    ", ip ipv4[][]" +
                            ", c double)"
            );

            String[] expectedColumnNames = {
                    "d",
//                    "b",
//                    "bt",
//                    "f",
//                    "i",
//                    "l",
//                    "s",
//                    "dt",
//                    "ts",
//                    "l2",
//                    "u",
//                    "ip",
                    "c",
            };

            String[] expectedColumnTypes = {
                    "DOUBLE[][][]",
//                    "BOOLEAN[][][][]",
//                    "BYTE[][][][][][][][]",
//                    "FLOAT[]",
//                    "INT[][]",
//                    "LONG[][]",
//                    "SHORT[][][][][]",
//                    "DATE[][][][]",
//                    "TIMESTAMP[][]",
//                    "LONG256[][][]",
//                    "UUID[][][][]",
//                    "IPv4[][]",
                    "DOUBLE"
            };

            Assert.assertEquals(expectedColumnNames.length, expectedColumnTypes.length);
            // check the metadata
            try (TableMetadata m = engine.getTableMetadata(engine.verifyTableName("x"))) {
                Assert.assertEquals(expectedColumnNames.length, m.getColumnCount());

                for (int i = 0, n = expectedColumnNames.length; i < n; i++) {
                    Assert.assertEquals(expectedColumnNames[i], m.getColumnName(i));
                    Assert.assertEquals(expectedColumnTypes[i], ColumnType.nameOf(m.getColumnType(i)));
                }
            }
        });
    }

    @Test
    public void testCreateTableWithUnsupportedColumnType() throws Exception {
        assertMemoryLeak(() -> {
            assertException("CREATE TABLE tango (arr BYTE[])", 24, "unsupported array element type [type=BYTE]");
            assertException("CREATE TABLE tango (arr SHORT[])", 24, "unsupported array element type [type=SHORT]");
            assertException("CREATE TABLE tango (arr INT[])", 24, "unsupported array element type [type=INT]");
            assertException("CREATE TABLE tango (arr LONG[])", 24, "unsupported array element type [type=LONG]");
            assertException("CREATE TABLE tango (arr FLOAT[])", 24, "unsupported array element type [type=FLOAT]");
            assertException("CREATE TABLE tango (arr BOOLEAN[])", 24, "unsupported array element type [type=BOOLEAN]");
            assertException("CREATE TABLE tango (arr CHAR[])", 24, "unsupported array element type [type=CHAR]");
            assertException("CREATE TABLE tango (arr STRING[])", 24, "unsupported array element type [type=STRING]");
            assertException("CREATE TABLE tango (arr VARCHAR[])", 24, "unsupported array element type [type=VARCHAR]");
            assertException("CREATE TABLE tango (arr ARRAY[])", 24, "unsupported array element type [type=ARRAY]");
            assertException("CREATE TABLE tango (arr BINARY[])", 24, "unsupported array element type [type=BINARY]");
            assertException("CREATE TABLE tango (arr DATE[])", 24, "unsupported array element type [type=DATE]");
            assertException("CREATE TABLE tango (arr TIMESTAMP[])", 24, "unsupported array element type [type=TIMESTAMP]");
            assertException("CREATE TABLE tango (arr UUID[])", 24, "unsupported array element type [type=UUID]");
            assertException("CREATE TABLE tango (arr LONG128[])", 24, "unsupported array element type [type=LONG128]");
            assertException("CREATE TABLE tango (arr GEOHASH[])", 24, "unsupported array element type [type=GEOHASH]");
        });
    }

    @Test
    public void testDedup() throws Exception {
        // this validates that dedup works with table with array columns
        // as long as the array columns are not part of the dedup key
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP, uniq LONG, arr DOUBLE[])" +
                    " TIMESTAMP(ts) PARTITION BY HOUR WAL" +
                    " DEDUP UPSERT KEYS (ts, uniq)");
            execute("INSERT INTO tango VALUES (1, 1, ARRAY[1.0, 2, 3, 4, 5])");
            execute("INSERT INTO tango VALUES (2, 2, ARRAY[6.0, 7, 8])");
            execute("INSERT INTO tango VALUES (1, 1, ARRAY[9.0, 10, 11])");
            drainWalQueue();
            assertSql("ts\tuniq\tarr\n" +
                            "1970-01-01T00:00:00.000001Z\t1\t[9.0,10.0,11.0]\n" +
                            "1970-01-01T00:00:00.000002Z\t2\t[6.0,7.0,8.0]\n",
                    "tango");
        });
    }

    @Test
    public void testDudupArrayAsKey() throws Exception {
        // when an array is part of the dedup key
        // it fails gracefully and with an informative error message
        assertException("CREATE TABLE tango (ts TIMESTAMP, arr DOUBLE[])" +
                        " TIMESTAMP(ts) PARTITION BY HOUR WAL" +
                        " DEDUP UPSERT KEYS (ts, arr)",
                107,
                "dedup key columns cannot include ARRAY [column=arr, type=DOUBLE[]]"
        );
    }

    @Test
    public void testEmptyArrayToJsonDouble() {
        try (DirectArray array = new DirectArray(configuration);
             DirectUtf8Sink sink = new DirectUtf8Sink(20)
        ) {
            array.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 1));
            array.setDimLen(0, 0);
            array.applyShape();
            sink.clear();
            ArrayTypeDriver.arrayToJson(array, sink, NoopArrayWriteState.INSTANCE);
            assertEquals("[]", sink.toString());
        }
    }

    @Test
    public void testEqualsArrayLiterals() throws Exception {
        assertMemoryLeak(() -> {
            assertSql("eq\ntrue\n", "SELECT (ARRAY[[1.0, 3]] = ARRAY[[1.0, 3]]) eq FROM long_sequence(1)");
            assertSql("eq\ntrue\n", "SELECT (ARRAY[[1.0, 3], [5.0, 7]] = ARRAY[[1.0, 3], [5.0, 7]]) eq FROM long_sequence(1)");
            assertSql("eq\nfalse\n", "SELECT (ARRAY[[1.0, 3]] = ARRAY[[1.0, 4]]) eq FROM long_sequence(1)");
            assertSql("eq\nfalse\n", "SELECT (ARRAY[[1.0, 3]] = ARRAY[[1.0, 3, 3]]) eq FROM long_sequence(1)");
            assertSql("eq\nfalse\n", "SELECT (ARRAY[[1.0, 3, 3]] = ARRAY[[1.0, 3]]) eq FROM long_sequence(1)");
            assertSql("eq\nfalse\n", "SELECT (ARRAY[[1.0, 3]] = ARRAY[1.0, 3]) eq FROM long_sequence(1)");
        });
    }

    @Test
    public void testEqualsColumnAndLiteral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][])");
            execute("INSERT INTO tango VALUES (ARRAY[[1.0, 3], [5.0, 7]])");
            assertSql("eq\ntrue\n", "SELECT (arr = ARRAY[[1.0, 3], [5.0, 7]]) eq FROM tango");
            assertSql("eq\nfalse\n", "SELECT (arr = ARRAY[[1.0, 4], [5.0, 7]]) eq FROM tango");
            assertSql("eq\nfalse\n", "SELECT (arr = ARRAY[[1.0, 3, 3], [5.0, 7, 9]]) eq FROM tango");
            assertSql("eq\nfalse\n", "SELECT (arr = ARRAY[[1.0, 3]]) eq FROM tango");

            assertSql("eq\ntrue\n", "SELECT (ARRAY[[1.0, 3], [5.0, 7]] = arr) eq FROM tango");
            assertSql("eq\nfalse\n", "SELECT (ARRAY[[1.0, 4], [5.0, 7]] = arr) eq FROM tango");
            assertSql("eq\nfalse\n", "SELECT (ARRAY[[1.0, 3, 3], [5.0, 7, 9]] = arr) eq FROM tango");
            assertSql("eq\nfalse\n", "SELECT (ARRAY[[1.0, 3]] = arr) eq FROM tango");
        });
    }

    @Test
    public void testEqualsDifferentDimensionality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (left DOUBLE[][], right DOUBLE[])");
            execute("INSERT INTO tango VALUES (ARRAY[[1.0, 3]], ARRAY[1.0, 3])"
            );
            assertSql("eq\nfalse\n", "SELECT (left = right) eq FROM tango");
        });
    }

    @Test
    public void testEqualsSameDimensionality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (left DOUBLE[][], right DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[[1.0, 3]], ARRAY[[1.0, 3]]), " +
                    "(ARRAY[[1.0, 3], [5.0, 7]], ARRAY[[1.0, 3], [5.0, 7]]), " +
                    "(ARRAY[[1.0, 3]], ARRAY[[1.0, 4]]), " +
                    "(ARRAY[[1.0, 3]], ARRAY[[1.0, 3, 3]]), " +
                    "(ARRAY[[1.0, 3, 3]], ARRAY[[1.0, 3]])"

            );
            assertSql("eq\ntrue\ntrue\nfalse\nfalse\nfalse\n", "SELECT (left = right) eq FROM tango");
        });
    }

    @Test
    public void testEqualsSliceSubarray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (left DOUBLE[][], right DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[[1.0, 3], [5.0, 7]], ARRAY[[1.0, 2], [5.0, 7]]), " +
                    "(ARRAY[[1.0], [3.0]], ARRAY[[2.0], [3.0]]), " +
                    "(ARRAY[[1.0], [3.0]], ARRAY[[2.0], [1.0]])"
            );
            assertSql("eq\ntrue\ntrue\nfalse\n", "SELECT (left[2] = right[2]) eq FROM tango");
            assertSql("eq\ntrue\ntrue\nfalse\n", "SELECT (left[2:] = right[2:]) eq FROM tango");
            assertSql("eq\nfalse\nfalse\ntrue\n", "SELECT (left[1:2] = right[2:]) eq FROM tango");
        });
    }

    @Test
    public void testExplicitCastDimensionalityChange() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("cast\n" +
                            "[[1.0,2.0]]\n",
                    "SELECT ARRAY[1.0, 2.0]::double[][]",
                    true
            );

            // no element arrays
            assertQuery("cast\n" +
                            "[]\n", // arrays with no elements are always printed as []
                    "SELECT ARRAY[]::double[][]",
                    true
            );

            // casting to fewer dimensions is not allowed
            assertException("SELECT ARRAY[[1.0], [2.0]]::double[]",
                    26,
                    "cannot cast array to lower dimension [from=DOUBLE[][] (2D), to=DOUBLE[] (1D)]. Use array flattening operation (e.g. 'arr[:]' or 'flatten_array(arr)') instead"
            );
        });
    }

    @Test
    public void testExplicitCastFromArrayToStr() throws Exception {
        assertMemoryLeak(() -> {
            assertSql("cast\n" +
                            "[1.0]\n",
                    "SELECT ARRAY[1.0]::string FROM long_sequence(1)");

            assertSql("cast\n" +
                            "[1.0,2.0]\n",
                    "SELECT ARRAY[1.0, 2.0]::string FROM long_sequence(1)");

            assertSql("cast\n" +
                            "[[1.0,2.0],[3.0,4.0]]\n",
                    "SELECT ARRAY[[1.0, 2.0], [3.0, 4.0]]::string FROM long_sequence(1)");

            // array with no elements is always printed as []
            assertSql("cast\n" +
                            "[]\n",
                    "SELECT ARRAY[[], []]::double[][]::string FROM long_sequence(1)");

            // null case, 'assertSql()' prints 'null' as an empty string
            assertSql("cast\n" +
                            "\n",
                    "SELECT NULL::double[]::string FROM long_sequence(1)");
        });
    }

    @Test
    public void testExplicitCastFromArrayToVarchar() throws Exception {
        assertMemoryLeak(() -> {
            assertSql("cast\n" +
                            "[1.0]\n",
                    "SELECT ARRAY[1.0]::varchar FROM long_sequence(1)");

            assertSql("cast\n" +
                            "[1.0,2.0]\n",
                    "SELECT ARRAY[1.0, 2.0]::varchar FROM long_sequence(1)");

            assertSql("cast\n" +
                            "[[1.0,2.0],[3.0,4.0]]\n",
                    "SELECT ARRAY[[1.0, 2.0], [3.0, 4.0]]::varchar FROM long_sequence(1)");

            // array with no elements is always printed as []
            assertSql("cast\n" +
                            "[]\n",
                    "SELECT ARRAY[[], []]::double[][]::varchar FROM long_sequence(1)");

            // null case, 'assertSql()' prints 'null' as an empty string
            assertSql("cast\n" +
                            "\n",
                    "SELECT NULL::double[]::varchar FROM long_sequence(1)");
        });
    }

    @Test
    public void testExplicitCastFromScalarToArray() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("cast\n" +
                            "[1.0]\n",
                    "SELECT 1.0::double[] FROM long_sequence(1)",
                    true
            );

            // null
            assertQuery("cast\n" +
                            "null\n",
                    "SELECT NULL::double::double[] FROM long_sequence(1)",
                    true);

            // 2D
            assertQuery("cast\n" +
                            "[[1.0]]\n",
                    "SELECT 1.0::double[][] FROM long_sequence(1)",
                    true
            );

            // 2D with null
            assertQuery("cast\n" +
                            "null\n",
                    "SELECT NULL::double::double[][] FROM long_sequence(1)",
                    true);
        });
    }

    @Test
    public void testExplicitCastFromStrToArray() throws Exception {
        assertMemoryLeak(() -> {
            assertSql("cast\n" +
                    "[1.0,2.0]\n", "SELECT '{1, 2}'::double[] FROM long_sequence(1)");

            // quoted elements
            assertSql("cast\n" +
                    "[1.0,2.0]\n", "SELECT '{\"1\", \"2\"}'::double[] FROM long_sequence(1)");

            // quoted elements with spaces, 2D array
            assertSql("cast\n" +
                    "[[1.0,2.0],[3.0,4.0]]\n", "SELECT '{{\"1\", \"2\"}, {\"3\", \"4\"}}'::double[][] FROM long_sequence(1)");

            // 2D array
            assertSql("cast\n" +
                    "[[1.0,2.0],[3.0,4.0]]\n", "SELECT '{{1,2}, {3, 4}}'::double[][] FROM long_sequence(1)");

            // 2D array with null - nulls are not allowed, casting fails and explicit casting produces NULL on the output
            assertSql("cast\n" +
                    "null\n", "SELECT '{{1,2}, {3, NULL}}'::double[][] FROM long_sequence(1)");

            // empty arrays are always printed as [], regardless of dimensionality. at least of now. this may change.
            assertSql("cast\n" +
                    "[]\n", "SELECT '{{}, {}}'::double[][] FROM long_sequence(1)");

            // empty array can be cast to higher dimensionality -> empty array
            assertSql("cast\n" +
                    "[]\n", "SELECT '{}'::double[][] FROM long_sequence(1)");

            // but empty array cannot cast to lower dimensionality -> NULL
            assertSql("cast\n" +
                    "null\n", "SELECT '{{},{}}'::double[] FROM long_sequence(1)");

            assertSql("cast\n" +
                    "null\n", "SELECT NULL::double[] FROM long_sequence(1)");

            assertSql("cast\n" +
                    "null\n", "SELECT 'not an array'::double[] FROM long_sequence(1)");

            // 2D array explicitly cast to 1D array -> null
            assertSql("cast\n" +
                    "null\n", "SELECT '{{1,2}, {3, 4}}'::double[] FROM long_sequence(1)");

            assertSql("cast\n" +
                    "null\n", "SELECT '{nonsense, 2}'::double[] FROM long_sequence(1)");

        });
    }

    @Test
    public void testFilterByColumnEqLiteral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr1 DOUBLE[], arr2 DOUBLE[])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[1.0, 2], ARRAY[3.0, 4]), " +
                    "(ARRAY[5.0, 6], ARRAY[5.0, 6]), " +
                    "(ARRAY[4.0, 5], ARRAY[5.0, 6])"
            );
            assertSql("arr1\n[5.0,6.0]\n", "SELECT arr1 FROM tango WHERE arr1 = arr2");
        });
    }

    @Test
    public void testFlatten() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][][], flatten_dim INT)");
            execute("INSERT INTO tango VALUES (ARRAY[[[1.0, 2], [3.0, 4]], [[5.0, 6], [7.0, 8]]], 1)");
            assertSql("arr\n[[1.0,2.0],[3.0,4.0],[5.0,6.0],[7.0,8.0]]\n", "SELECT flatten(arr, 1) arr FROM tango");
            assertSql("arr\n[[1.0,2.0],[3.0,4.0],[5.0,6.0],[7.0,8.0]]\n", "SELECT flatten(arr, flatten_dim) arr FROM tango");
            assertSql("arr\n[[1.0,2.0,3.0,4.0],[5.0,6.0,7.0,8.0]]\n", "SELECT flatten(arr, 2) arr FROM tango");
            assertSql("arr\n[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0]\n", "SELECT flatten(flatten(arr, 1), 1) arr FROM tango");
            assertSql("arr\n[[[2.0],[4.0]],[[6.0],[8.0]]]\n", "SELECT arr[1:, 1:, 2:3] arr FROM tango");
            assertSql("arr\n[[2.0,4.0],[6.0,8.0]]\n", "SELECT arr[1:, 1:, 2] arr FROM tango");
            assertSql("arr\n[[2.0,4.0],[6.0,8.0]]\n", "SELECT flatten(arr[1:, 1:, 2:3], 3) arr FROM tango");
        });
    }

    @Test
    public void testFlattenInvalid() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][][], flatten_dim INT)");
            execute("INSERT INTO tango VALUES (ARRAY[[[1.0, 2], [3.0, 4]], [[5.0, 6], [7.0, 8]]], 4)");
            assertException("SELECT flatten(arr, 3) FROM tango", 20,
                    "cannot flatten dim with stride = 1 and length > 1 [dim=3, dimLen=2, nDims=3]");
            assertException("SELECT flatten(arr, 4) FROM tango", 20,
                    "dimension to flatten out of range [nDims=3, flattenDim=4]");
            assertException("SELECT flatten(arr, flatten_dim) FROM tango", 20,
                    "dimension to flatten out of range [nDims=3, flattenDim=4]");
        });
    }

    @Test
    public void testGroupByArrayKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][], i int)");
            execute("INSERT INTO tango VALUES (ARRAY[[1.0, 2.0], [3.0, 4.0]], 0)");
            execute("INSERT INTO tango VALUES (ARRAY[[1.0, 2.0], [3.0, 4.0]], 1)");
            execute("INSERT INTO tango VALUES (ARRAY[[1.0, 2.0], [3.0, 4.1]], 2)");
            execute("INSERT INTO tango VALUES (ARRAY[[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]], 0)");
            execute("INSERT INTO tango VALUES (null, 0)");

            assertQuery("arr\tcount\n" +
                            "[[1.0,2.0],[3.0,4.0]]\t2\n" +
                            "[[1.0,2.0],[3.0,4.1]]\t1\n" +
                            "[[1.0,2.0],[3.0,4.0],[5.0,6.0]]\t1\n" +
                            "null\t1\n",
                    "select arr, count(*)\n" +
                            "from tango\n" +
                            "group by arr;",
                    true);
        });
    }

    @Test
    public void testGroupByOnSliceKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][], i int)");
            execute("INSERT INTO tango VALUES (ARRAY[[1.0, 2.0], [3.0, 4.0], [5, 6]], 1)");
            execute("INSERT INTO tango VALUES (ARRAY[[1.0, 2.0], [33.0, 4.0]], 3)");
            execute("INSERT INTO tango VALUES (ARRAY[[2.0, 3.0], [1.0, 4.0]], 5)");

            assertQuery("[]\tsum\n" +
                            "[[1.0,2.0]]\t4\n" +
                            "[[2.0,3.0]]\t5\n",
                    "select arr[1:2], sum(i)\n" +
                            "from tango\n",
                    true);

        });
    }

    @Test
    public void testInsertAsSelectDoubleNoWAL() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table blah (a double[][])");
            execute("insert into blah select rnd_double_array(2, 1) from long_sequence(10)");

            assertQuery(
                    "a\n" +
                            "[[null,null,null,null,0.0843832076262595,0.6508594025855301,0.7905675319675964,0.22452340856088226,null,0.6254021542412018,0.4621835429127854,0.5599161804800813,null,0.2390529010846525,0.6693837147631712]]\n" +
                            "[[0.38539947865244994,0.0035983672154330515,0.3288176907679504,null,0.9771103146051203,0.24808812376657652,0.6381607531178513,null],[null,0.38179758047769774,0.12026122412833129,0.6700476391801052,0.3435685332942956,null,null,0.810161274171258]]\n" +
                            "[[null,null],[null,null],[null,0.29313719347837397],[null,null],[null,null],[0.931192737286751,null],[0.8001121139739173,null],[0.92050039469858,null]]\n" +
                            "[[null,null,0.40455469747939254,null,0.5659429139861241,0.8828228366697741,null,null,null,null,0.9566236549439661,null,null,null],[0.8164182592467494,null,0.5449155021518948,null,null,null,0.9640289041849747,0.7133910271555843,null,0.5891216483879789,null,null,0.48558682958070665,null],[0.44804689668613573,null,null,0.14830552335848957,null,null,0.6806873134626418,0.625966045857722,0.8733293804420821,null,0.17833722747266334,null,null,0.026836863013701473]]\n" +
                            "[[null,null,null,null,0.07246172621937097],[0.4892743433711657,0.8231249461985348,null,0.4295631643526773,null],[0.7632615004324503,0.8816905018995145,null,0.5357010561860446,null],[null,null,null,0.7458169804091256,null],[0.4274704286353759,null,null,null,0.7777024823107295],[null,0.7445998836567925,0.2825582712777682,0.2711532808184136,null],[null,null,null,0.029080850168636263,0.10459352312331183],[null,0.20585069039325443,null,0.9884011094887449,0.9457212646911386],[0.05024615679069011,0.9946372046359034,0.6940904779678791,0.5391626621794673,null],[0.4416432347777828,null,null,null,0.2445295612285482],[null,0.043606408996349044,null,null,0.7260468106076399]]\n" +
                            "[[null,null,0.06381657870188628,null,0.35731092171284307,0.9583687530177664,null,null,null,null,null,0.6069927532469744,null,null,null,null],[0.062027497477155635,0.6901976778065181,0.7586254118589676,null,null,null,null,null,0.5677191487344088,0.2677326840703891,null,0.23507754029460548,0.20727557301543031,null,0.9292491654871197,null],[0.49154607371672154,0.4167781163798937,0.3454148777596554,null,null,null,0.9423671624137644,null,null,0.7873229912811514,null,null,0.5859332388599638,null,0.3460851141092931,null],[null,null,null,null,null,0.6226001464598434,0.4346135812930124,0.8786111112537701,0.996637725831904,null,0.30028279396280155,null,0.8196554745841765,0.9130151105125102,null,null],[null,null,null,null,null,0.007985454958725269,0.5090837921075583,null,null,null,null,0.8775452659546193,0.8379891991223047,null,null,null],[null,null,null,0.5626370294064983,null,0.49199001716312474,0.6292086569587337,null,null,0.5779007672652298,0.5692090442741059,null,null,null,null,null],[0.7732229848518976,0.587752738240427,0.4667778758533798,null,0.7202789791127316,null,0.7407842990690816,0.9790787740413469,null,0.7527907209539796,null,0.9546417330809595,0.848083900630095,0.4698648140712085,0.8911615631017953,null],[null,0.7431472218131966,0.5889504900909748,null,null,null,0.798471808479839,null,null,null,null,null,0.8574212636138532,0.8280460741052847,0.7842455970681089,null],[null,null,0.3889200123396954,0.933609514582851,null,0.17202485647400034,null,0.4610963091405301,0.5673376522667354,0.48782086416459025,null,0.13312214396754163,0.9435138098640453,null,0.17094358360735395,null],[null,null,0.5449970817079417,null,null,null,null,0.3058008320091107,null,null,0.6479617440673516,0.5900836401674938,0.12217702189166091,0.7717552767944976,null,null]]\n" +
                            "[[null,0.4627885105398635,0.4028291715584078,null,null,null,null,null,null,0.5501133139397699,0.7134500775259477,null,0.734728770956117,null,null],[0.8531407145325477,null,0.009302399817494589,null,null,0.32824342042623134,null,0.4086323159337839,null,null,null,null,null,null,0.04404000858917945],[0.14295673988709012,null,null,0.36078878996232167,null,null,null,0.7397816490927717,null,0.8386104714017393,0.9561778292078881,0.11048000399634927,null,null,0.11947100943679911],[null,null,null,null,null,0.5335953576307257,null,0.8504099903010793,null,null,null,null,0.15274858078119136,null,0.7468602267994937],[0.55200903114214,null,0.12483505553793961,0.024056391028085766,null,null,0.3663509090570607,null,null,0.6940917925148332,null,null,0.4564667537900823,null,0.4412051102084278],[null,0.43159834345466475,null,null,0.97613283653158,null,0.7707892345682454,0.8782062052833822,null,null,0.8376372223926546,0.365427022047211,null,null,null],[null,null,0.31861843394057765,null,0.9370193388878216,0.39201296350741366,null,0.28813952005117305,0.65372393289891,null,0.9375691350784857,null,0.4913342104187668,null,null],[null,null,null,null,null,0.38881940598288367,0.4444125234732249,null,null,null,null,0.5261234649527643,0.030750139424332357,0.20921704056371593,0.681606585145203],[0.11134244333117826,null,0.5863937813368164,0.2103287968720018,0.3242526975448907,0.42558021324800144,null,0.6068565916347403,null,0.004918542726028763,null,0.008134052047644613,0.1339704489137793,0.4950615235019964,0.04558283749364911],[0.3595576962747611,null,null,0.0966240354078981,null,null,null,null,0.9694731343686098,0.24584615213823513,null,0.5965069739835686,null,0.16979644136429572,0.28122627418701307],[null,null,0.8545896910200949,null,0.40609845936584743,0.041230021906704994,null,0.6852762111021103,0.08039440728458325,0.7600550885615773,0.05890936334115593,0.023600615130049185,null,0.7630648900646654,null]]\n" +
                            "[[0.48573429889865705,null,null,null,0.6198590038961462],[null,0.7280036952357564,0.6404197786416339,0.828928908465152,null],[null,0.38000152873098747,0.5157225592346661,null,0.16320835762949149],[0.6952925744703682,null,null,null,null],[null,null,0.535993442770838,0.5725722946886976,null],[null,null,null,0.38392106356809774,null]]\n" +
                            "[[null],[null],[0.5320636725174561],[0.13226561658653546]]\n" +
                            "[[0.22371932699681862,null,0.38656452532530694,null,0.4019292440508081,null,null,null,0.6367746812001958],[null,null,0.7910659228440695,0.9578716688144072,null,null,0.7861254708288083,0.1319044042993568,0.45862629276476996],[0.3812506482325819,null,0.4104855595304533,null,0.7587860024773928,0.48422909268940273,0.9176263114713273,null,0.6281252905002019],[null,0.2824076895992761,0.9109198044456538,null,0.858967821197869,0.1900488162112337,null,null,0.736755875734414],[0.12465120312903266,0.04734223739255916,0.10424082472921137,null,0.9266929571641075,0.19823647700531244,null,0.265224199619046,null],[0.8306929906890365,null,null,null,null,0.4613501223216129,0.056099346819408535,null,null],[null,0.6355534187114189,0.19073234832401043,null,0.4246651384043666,0.3031376204022046,0.08533575092925538,0.6266142259812271,0.8925004728084927],[null,null,0.8645117110218422,0.7298540433653912,0.588707402066503,null,0.9891642698247116,null,null],[null,null,0.761296424148768,null,0.43990342764801993,0.6130518815428464,0.9755586311085417,0.5522442336842381,0.9385037871004874],[0.16227550791363532,null,null,0.9154548873622441,0.8470755372946043,0.8061988461374605,0.6343133564417237,null,null],[0.9213721848053825,0.30394683981054627,null,0.4510483042269017,null,null,null,null,0.38106875419134767],[0.3838483044911978,null,0.6224023788514188,null,0.3836027299974998,null,0.8151906661765794,null,0.3197593740177185],[null,null,0.4176571781712538,null,null,null,0.5191884769160172,null,null],[0.7200031730502818,null,0.11919556761688443,0.7183071061096172,null,null,0.6391251028594114,null,null],[0.1790475858715116,0.7504512900310369,0.9583685768295167,null,null,null,null,null,0.018689012580364706],[0.8940422626709261,null,0.7536836395346167,0.04727174057972261,null,null,null,0.28598292472656794,null]]\n",
                    "select * from blah",
                    true
            );
        });
    }

    @Test
    public void testInsertAsSelectLiteral1d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE, b DOUBLE)");
            execute("CREATE TABLE samba (arr DOUBLE[])");
            execute("INSERT INTO tango VALUES (1.0, 2.0)");
            execute("INSERT INTO samba SELECT ARRAY[a, b] FROM tango");
            assertSql("arr\n[1.0,2.0]\n", "samba");
        });
    }

    @Test
    public void testInsertAsSelectLiteral2d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE, b DOUBLE)");
            execute("CREATE TABLE samba (arr DOUBLE[][])");
            execute("INSERT INTO tango VALUES (1.0, 2.0)");
            execute("INSERT INTO samba SELECT ARRAY[[a, a], [b, b]] FROM tango");
            assertSql("arr\n[[1.0,1.0],[2.0,2.0]]\n", "samba");
        });
    }

    @Test
    public void testInsertEmpty1d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[])");
            execute("INSERT INTO tango VALUES (ARRAY[])");
            assertSql("arr\n[]\n", "tango");
        });
    }

    @Test
    public void testInsertEmpty2d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][])");
            execute("INSERT INTO tango VALUES (ARRAY[[]])");
            execute("INSERT INTO tango VALUES (ARRAY[[],[]])");
            execute("INSERT INTO tango VALUES (ARRAY[[],[],[]])");
            assertSql("arr\n[]\n[]\n[]\n", "tango");
        });
    }

    @Test
    public void testInsertEmpty3d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][][])");
            execute("INSERT INTO tango VALUES (ARRAY[[[]]])");
            execute("INSERT INTO tango VALUES (ARRAY[[[]],[[]]])");
            execute("INSERT INTO tango VALUES (ARRAY[[[],[]]])");
            assertSql("arr\n[]\n[]\n[]\n", "tango");
        });
    }

    @Test
    public void testInsertTransposed() throws Exception {
        assertMemoryLeak(() -> {
            String original = "[[1.0,2.0],[3.0,4.0],[5.0,6.0]]";
            String transposed = "[[1.0,3.0,5.0],[2.0,4.0,6.0]]";
            execute("CREATE TABLE tango AS (SELECT ARRAY" + original + " arr FROM long_sequence(1))");
            execute("INSERT INTO tango SELECT transpose(arr) FROM tango");
            assertSql("arr\n" + original + '\n' + transposed + '\n', "SELECT arr FROM tango");
        });
    }

    @Test
    public void testLength() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[[1.0, 1]]), " +
                    "(ARRAY[[2.0, 2], [2.0, 2], [2.0, 2]]), " +
                    "(ARRAY[[2.0, 3, 3], [3.0, 3, 3]])"
            );
            assertSql("len\n1\n3\n2\n", "SELECT dim_length(arr, 1) len FROM tango");
            assertSql("len\n2\n2\n3\n", "SELECT dim_length(arr, 2) len FROM tango");
            assertSql("len\n1\n2\n3\n", "SELECT dim_length(arr, arr[1, 1]::int) len FROM tango");
        });
    }

    @Test
    public void testLengthInvalid() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT ARRAY[[1.0, 2], [3.0, 4], [5.0, 6]] arr FROM long_sequence(1))");
            assertExceptionNoLeakCheck("SELECT dim_length(arr, 0) len FROM tango",
                    23, "array dimension out of bounds [dim=0, nDims=2]");
            assertExceptionNoLeakCheck("SELECT dim_length(arr, 3) len FROM tango",
                    23, "array dimension out of bounds [dim=3, nDims=2]");
            assertExceptionNoLeakCheck("SELECT dim_length(arr, arr[2, 1]::int) len FROM tango",
                    32, "array dimension out of bounds [dim=3, nDims=2]");
        });
    }

    @Test
    public void testLevelTwoPrice1D() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ask_price DOUBLE[], ask_size DOUBLE[])");
            execute("INSERT INTO tango VALUES (ARRAY[1.0, 2], ARRAY[1.0, 1])");
            assertSql("l2\n1.0\n", "SELECT l2price(1.0, ask_size, ask_price) l2 FROM tango");
            assertSql("l2\n1.5\n", "SELECT l2price(2.0, ask_size, ask_price) l2 FROM tango");
        });
    }

    @Test
    public void testLevelTwoPrice2D() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ask DOUBLE[][])");
            execute("INSERT INTO tango VALUES (ARRAY[[1.0, 1], [1.0, 2]])");
            assertSql("l2\n1.0\n", "SELECT l2price(1.0, ask[1], ask[2]) l2 FROM tango");
            assertSql("l2\n1.5\n", "SELECT l2price(2.0, ask[1], ask[2]) l2 FROM tango");
        });
    }

    @Test
    public void testMatrixMultiply() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (left DOUBLE[][], right DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[[1.0, 3]], ARRAY[[5.0], [7.0]]), " +
                    "(ARRAY[[1.0, 1, 1], [2.0, 2, 2]], ARRAY[[3.0], [5.0], [7.0]])");
            assertSql("product\n[[26.0]]\n[[15.0],[30.0]]\n", "SELECT matmul(left, right) AS product FROM tango");
        });
    }

    @Test
    public void testMatrixMultiplyInvalid() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT " +
                    "ARRAY[[1.0, 2.0]] left2d, ARRAY[1.0] left1d, " +
                    "ARRAY[[1.0]] right2d, ARRAY[1.0] right1d "
                    + "FROM long_sequence(1))");
            assertExceptionNoLeakCheck("SELECT matmul(left1d, right1d) FROM tango",
                    14, "left array is not two-dimensional");
            assertExceptionNoLeakCheck("SELECT matmul(left2d, right1d) FROM tango",
                    22, "right array is not two-dimensional");
            assertExceptionNoLeakCheck("SELECT matmul(left2d, right2d) FROM tango",
                    14, "left array row length doesn't match right array column length");
        });
    }

    @Test
    public void testMatrixMultiplyTransposed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT ARRAY[[1.0, 2], [3.0, 4], [5.0, 6]] arr FROM long_sequence(1))");
            assertSql("product\n[[5.0,11.0,17.0],[11.0,25.0,39.0],[17.0,39.0,61.0]]\n",
                    "SELECT matmul(arr, transpose(arr)) product FROM tango");
        });
    }

    @Test
    public void testMultiplySlice1d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE[], b DOUBLE[])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[2.0, 3.0], ARRAY[4.0, 5]), " +
                    "(ARRAY[6.0, 7], ARRAY[8.0, 9])," +
                    "(null, null)");
            assertSql("product\n[15.0]\n[63.0]\nnull\n", "SELECT a[2:] * b[2:] product FROM tango");
        });
    }

    @Test
    public void testMultiplySlice3d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE[][][], b DOUBLE[][][])");
            execute("INSERT INTO tango VALUES " +
                    "( ARRAY[ [ [2.0, 3], [4.0, 5] ], [ [6.0, 7], [8.0, 9] ]  ], " +
                    "  ARRAY[ [ [10.0, 11], [12.0, 13] ], [ [14.0, 15], [16.0, 17] ]  ] ), " +
                    "( null, null )");
            assertSql("product\n[[[34.0]]]\nnull\n",
                    "SELECT a[1:2, 1:2, 1:2] * b[2:, 2:, 2:] product FROM tango");
        });
    }

    @Test
    public void testNativeFormatParser() {
        final long allocSize = 2048;
        long mem = Unsafe.malloc(allocSize, MemoryTag.NATIVE_DEFAULT);
        try (DirectArray array = new DirectArray(configuration);
             ArrayBinaryFormatParser parserNative = new ArrayBinaryFormatParser();
             DirectUtf8Sink sink = new DirectUtf8Sink(100)
        ) {
            // [[1.0, 2], [3.0, 4], [5.0, 6]]
            array.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 2));
            array.setDimLen(0, 3);
            array.setDimLen(1, 2);
            array.applyShape();
            MemoryA memA = array.startMemoryA();
            memA.putDouble(1);
            memA.putDouble(2);
            memA.putDouble(3);
            memA.putDouble(4);
            memA.putDouble(5);
            memA.putDouble(6);
            sink.clear();
            ArrayTypeDriver.arrayToJson(array, sink, NoopArrayWriteState.INSTANCE);
            String textViewStr = sink.toString();

            long start = mem;
            sink.clear();
            parserNative.reset();
            arrayViewToBinaryFormat(array, mem);
            boolean finish;
            do {
                long size = parserNative.getNextExpectSize();
                finish = parserNative.processNextBinaryPart(start);
                start += size;
            } while (!finish);

            ArrayTypeDriver.arrayToJson(parserNative.getArray(), sink, NoopArrayWriteState.INSTANCE);
            assertEquals(textViewStr, sink.toString());
        } catch (ArrayBinaryFormatParser.ParseException e) {
            throw new RuntimeException(e);
        } finally {
            Unsafe.free(mem, allocSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testNotEqualsSameDimensionality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (left DOUBLE[][], right DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[[1.0, 3]], ARRAY[[1.0, 3]]), " +
                    "(ARRAY[[1.0, 3], [5.0, 7]], ARRAY[[1.0, 3], [5.0, 7]]), " +
                    "(ARRAY[[1.0, 3]], ARRAY[[1.0, 4]]), " +
                    "(ARRAY[[1.0, 3]], ARRAY[[1.0, 3, 3]]), " +
                    "(ARRAY[[1.0, 3, 3]], ARRAY[[1.0, 3]])"

            );
            assertSql("eq\nfalse\nfalse\ntrue\ntrue\ntrue\n", "SELECT (left != right) eq FROM tango");
        });
    }

    @Test
    public void testNullArray() throws Exception {
        execute("CREATE TABLE tango (arr DOUBLE[])");
        execute("CREATE TABLE samba (left DOUBLE[][], right DOUBLE[][])");
        execute("INSERT INTO tango VALUES (null)");
        execute("INSERT INTO samba VALUES (null, null)");
        execute("INSERT INTO samba VALUES (ARRAY[[1.0],[2.0]], null)");
        execute("INSERT INTO samba VALUES (null, ARRAY[[1.0],[2.0]])");
        assertSql("arr\nnull\n", "tango");
        assertSql("arr\nnull\n", "SELECT arr FROM tango");
        assertSql("arr\nnull\n", "SELECT transpose(arr) arr FROM tango");
        assertSql("arr\nnull\n", "SELECT l2price(1.0, arr, arr) arr FROM tango");
    }

    @Test
    public void testOpComposition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT ARRAY[[1.0,2.0],[3.0,4.0],[5.0,6.0]] arr FROM long_sequence(1))");
            assertSql("x\n[[3.0,4.0]]\n", "SELECT arr[2:3] x FROM tango");
            assertSql("x\n[3.0,4.0]\n", "SELECT arr[2] x FROM tango");
            assertSql("x\n[[3.0],[4.0]]\n", "SELECT transpose(arr[2:3]) x FROM tango");
            assertSql("x\n[2.0,4.0,6.0]\n", "SELECT transpose(arr)[2] x FROM tango");
            assertSql("x\n4.0\n", "SELECT arr[2][2] x FROM tango");
            assertSql("x\n[4.0]\n", "SELECT arr[2][2:3] x FROM tango");
            assertSql("x\n[5.0,6.0]\n", "SELECT arr[2:4][2] x FROM tango");
            assertSql("x\n[[5.0,6.0]]\n", "SELECT arr[2:4][2:3] x FROM tango");
        });
    }

    @Test
    public void testOrderBy() throws Exception {
        // this test is to ensure that we can order results set containing arrays
        // but array column is NOT used as part of the ORDER BY clause
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts timestamp, i int, arr double[]) timestamp(ts) partition by DAY");
            execute("INSERT INTO tango VALUES ('2001-01', 1, '{1.0, 2.0}')");
            execute("INSERT INTO tango VALUES ('2001-02', 2, '{3.0, 4.0}')");
            execute("INSERT INTO tango VALUES ('2001-03', 3, '{5.0, 6.0, 7.0}')");
            execute("INSERT INTO tango VALUES ('2001-04', 42, null)");

            assertQuery("ts\ti\tarr\n" +
                            "2001-04-01T00:00:00.000000Z\t42\tnull\n" +
                            "2001-03-01T00:00:00.000000Z\t3\t[5.0,6.0,7.0]\n" +
                            "2001-02-01T00:00:00.000000Z\t2\t[3.0,4.0]\n" +
                            "2001-01-01T00:00:00.000000Z\t1\t[1.0,2.0]\n",
                    "select * from tango order by i desc",
                    true
            );

            // test also with no rowId - this simulates ordering output of a factory which does not support rowId
            assertQuery("ts\ti\tarr\n" +
                            "2001-04-01T00:00:00.000000Z\t42\tnull\n" +
                            "2001-03-01T00:00:00.000000Z\t3\t[5.0,6.0,7.0]\n" +
                            "2001-02-01T00:00:00.000000Z\t2\t[3.0,4.0]\n" +
                            "2001-01-01T00:00:00.000000Z\t1\t[1.0,2.0]\n",
                    "select * from '*!*tango' order by i desc",
                    true
            );
        });
    }

    @Test
    public void testOrderByArrayColFailsGracefully() throws Exception {
        assertException("select * from tab order by arr",
                "create table tab as (select rnd_double_array(2, 1) arr from long_sequence(10))",
                27,
                "DOUBLE[][] is not a supported type in ORDER BY clause"
        );
    }

    @Test
    public void testPartitionConversionToParquetFailsGracefully() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts timestamp, i int, arr double[]) timestamp(ts) partition by DAY");
            execute("INSERT INTO tango VALUES ('2001-01', 1, '{1.0, 2.0}')");
            execute("INSERT INTO tango VALUES ('2001-02', 1, '{1.0, 2.0}')");

            // with predicate
            assertException(
                    "ALTER TABLE tango CONVERT PARTITION TO PARQUET where ts in '2001-01';",
                    39,
                    "tables with array columns cannot be converted to Parquet partitions yet [table=tango, column=arr]"
            );

            // with list
            assertException(
                    "ALTER TABLE tango CONVERT PARTITION TO PARQUET list '2001-01';",
                    39,
                    "tables with array columns cannot be converted to Parquet partitions yet [table=tango, column=arr]"
            );
        });
    }

    @Test
    public void testProjection() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE[], b DOUBLE[])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[2.0, 3.0], ARRAY[4.0, 5]), " +
                    "(ARRAY[6.0, 7], ARRAY[8.0, 9])");

            assertQuery("a1\tb1\ta2\tb2\n" +
                            "[2.0,3.0]\t[4.0,5.0]\t[2.0,3.0]\t[4.0,5.0]\n" +
                            "[6.0,7.0]\t[8.0,9.0]\t[6.0,7.0]\t[8.0,9.0]\n",
                    "select a as a1, b as b1, a as a2, b as b2 from 'tango' ",
                    true
            );
        });
    }

    @Test
    public void testRndDoubleFunctionEdgeCases() throws Exception {
        assertMemoryLeak(() -> {
            assertExceptionNoLeakCheck(
                    "select rnd_double_array(10, 0, 1000)",
                    7,
                    "array element count exceeds max"
            );

            assertExceptionNoLeakCheck(
                    "select rnd_double_array(10, 0, 0)",
                    31,
                    "maxDimLen must be positive int [maxDimLen=0]"
            );

            assertSql("rnd_double_array\n" +
                            "null\n",
                    "select rnd_double_array(0, 0, 1000)"
            );

            assertExceptionNoLeakCheck(
                    "select rnd_double_array(1, -1, 1000)",
                    27,
                    "invalid NaN rate [nanRate=-1]"
            );

            // not enough dim lens
            assertExceptionNoLeakCheck(
                    "select rnd_double_array(2, 1, 0, 1)",
                    33,
                    "not enough values for dim length [dimensionCount=2, dimLengths=1]"
            );

            assertExceptionNoLeakCheck(
                    "select rnd_double_array(2, 1, 0, 1, 2, 4)",
                    39,
                    "too many values for dim length [dimensionCount=2, dimLengths=3]"
            );

            assertSql(
                    "rnd_double_array\n" +
                            "[[0.13123360041292131,null],[null,0.22452340856088226]]\n",
                    "select rnd_double_array(2, 1, 0, 2, 2)"
            );

            assertSql(
                    "QUERY PLAN\n" +
                            "VirtualRecord\n" +
                            "  functions: [rnd_double_array(2,1,ignored,2,2)]\n" +
                            "    long_sequence count: 1\n",
                    "explain select rnd_double_array(2, 1, 0, 2, 2)"
            );

            assertSql(
                    "QUERY PLAN\n" +
                            "VirtualRecord\n" +
                            "  functions: [rnd_double_array(3,1,4)]\n" +
                            "    long_sequence count: 1\n",
                    "explain select rnd_double_array(3, 1, 4)"
            );
        });
    }

    @Test
    public void testSelectLiteral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE, b DOUBLE)");
            execute("INSERT INTO tango VALUES (1.0, 2.0)");
            assertSql("ARRAY\n[1.0,2.0]\n", "SELECT ARRAY[a, b] FROM tango");
            assertSql("ARRAY\n[[1.0],[2.0]]\n", "SELECT ARRAY[[a], [b]] FROM tango");
        });
    }

    @Test
    public void testSelectLiteralInvalid() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE, b DOUBLE)");
            assertExceptionNoLeakCheck("SELECT ARRAY[a FROM tango",
                    15, "dangling literal");
            assertExceptionNoLeakCheck("SELECT ARRAY[a, [a] FROM tango",
                    20, "dangling literal");
            assertExceptionNoLeakCheck("SELECT ARRAY[a, [a]] FROM tango",
                    16, "mixed array and non-array elements");
            assertExceptionNoLeakCheck("SELECT ARRAY[[a], a] FROM tango",
                    18, "mixed array and non-array elements");
            assertExceptionNoLeakCheck("SELECT ARRAY[[a], [a, a]] FROM tango",
                    18, "element counts in sub-arrays don't match");
            assertExceptionNoLeakCheck("SELECT ARRAY[[a, a], [a]] FROM tango",
                    21, "element counts in sub-arrays don't match");
            assertExceptionNoLeakCheck("SELECT ARRAY[[[a], [a]], [a]] FROM tango",
                    25, "sub-arrays don't match in number of dimensions");
            assertExceptionNoLeakCheck("SELECT ARRAY[[[a], [a]], [a, a]] FROM tango",
                    25, "sub-arrays don't match in number of dimensions");
        });
    }

    @Test
    public void testSlice1d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT ARRAY[1.0,2.0,3.0] arr FROM long_sequence(1))");
            assertSql("slice\n[1.0]\n", "SELECT arr[1:2] slice from tango");
            assertSql("slice\n[1.0,2.0]\n", "SELECT arr[1:3] slice from tango");
        });
    }

    @Test
    public void testSlice2d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT ARRAY[[1.0, 2], [3.0, 4], [5.0, 6]] arr FROM long_sequence(1))");
            assertSql("slice\n[[1.0,2.0]]\n", "SELECT arr[1:2] slice FROM tango");
            assertSql("slice\n[[3.0,4.0],[5.0,6.0]]\n", "SELECT arr[2:] slice FROM tango");
            assertSql("slice\n[[5.0]]\n", "SELECT arr[3:, 1:2] slice FROM tango");
            assertSql("slice\n[6.0]\n", "SELECT arr[3:, 2] slice FROM tango");
            assertSql("slice\n[[1.0,2.0],[3.0,4.0]]\n", "SELECT arr[1:3] slice FROM tango");
            assertSql("slice\n[[1.0],[3.0]]\n", "SELECT arr[1:3, 1:2] slice FROM tango");
            assertSql("element\n4.0\n", "SELECT arr[2, 2] element FROM tango");
        });
    }

    @Test
    public void testSliceInvalid() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT ARRAY[[1.0, 2], [3.0, 4], [5.0, 6]] arr FROM long_sequence(1))");
            assertExceptionNoLeakCheck("SELECT arr[:1] FROM tango",
                    11, "undefined bind variable: :1"
            );
            assertExceptionNoLeakCheck("SELECT arr[0:1] FROM tango",
                    12, "array slice bounds must be positive [dim=1, lowerBound=0]"
            );
            assertExceptionNoLeakCheck("SELECT arr[1:0] FROM tango",
                    12, "array slice bounds must be positive [dim=1, upperBound=0]"
            );
            assertExceptionNoLeakCheck("SELECT arr[1:(arr[1, 1] - 1)::int] FROM tango",
                    12, "array slice bounds must be positive [dim=1, dimLen=3, lowerBound=1, upperBound=0]"
            );
            assertExceptionNoLeakCheck("SELECT arr[(arr[1, 1] - 1)::int : 2] FROM tango",
                    32, "array slice bounds must be positive [dim=1, dimLen=3, lowerBound=0, upperBound=2]"
            );
        });
    }

    @Test
    public void testSliceOutOfBounds() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT ARRAY[[1.0, 2], [3.0, 4], [5.0, 6]] arr FROM long_sequence(1))");
            assertSql("x\n[[1.0,2.0],[3.0,4.0],[5.0,6.0]]\n", "SELECT arr[1:5] x FROM tango");
            assertSql("x\n[]\n", "SELECT arr[4:5] x FROM tango");
            assertSql("x\n[]\n", "SELECT arr[2:1] x FROM tango");
        });
    }

    @Test
    public void testSliceTransposed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT ARRAY[[1.0, 2], [3.0, 4], [5.0, 6]] arr FROM long_sequence(1))");
            // transposed array: [[1,3,5],[2,4,6]]; slice takes first row, and first two elements from it
            assertSql("slice\n[[1.0,3.0]]\n", "SELECT transpose(arr)[1:2, 1:3] slice FROM tango");
        });
    }

    @Test
    public void testSubArray3d() throws Exception {
        assertMemoryLeak(() -> {
            String subArr00 = "[1.0,2.0]";
            String subArr01 = "[3.0,4.0]";
            String subArr10 = "[5.0,6.0]";
            String subArr11 = "[7.0,8.0]";
            String subArr0 = "[" + subArr00 + "," + subArr01 + "]";
            String subArr1 = "[" + subArr10 + "," + subArr11 + "]";
            String fullArray = "[" + subArr0 + "," + subArr1 + "]";
            execute("CREATE TABLE tango AS (SELECT ARRAY" + fullArray + " arr FROM long_sequence(1))");
            assertSql("x\n" + subArr0 + "\n", "SELECT arr[1] x FROM tango");
            assertSql("x\n" + subArr1 + "\n", "SELECT arr[2] x FROM tango");
            assertSql("x\n" + subArr00 + "\n", "SELECT arr[1,1] x FROM tango");
            assertSql("x\n" + subArr01 + "\n", "SELECT arr[1,2] x FROM tango");
            assertSql("x\n" + subArr10 + "\n", "SELECT arr[2,1] x FROM tango");
            assertSql("x\n" + subArr11 + "\n", "SELECT arr[2,2] x FROM tango");
            assertSql("x\n[[4.0]]\n", "SELECT arr[1:2,2:3,2] x FROM tango");
            assertSql("x\n[" + subArr01 + "," + subArr11 + "]\n", "SELECT arr[1:,2] x FROM tango");
            assertSql("x\n[[4.0],[8.0]]\n", "SELECT arr[1:,2:3,2] x FROM tango");
            assertSql("x\n[[7.0,8.0]]\n", "SELECT arr[2,2:] x FROM tango");
            assertSql("x\n[[[7.0,8.0]]]\n", "SELECT arr[2:,2:] x FROM tango");
            assertSql("x\n[8.0]\n", "SELECT arr[2,2,2:] x FROM tango");
            assertSql("x\n[[8.0]]\n", "SELECT arr[2,2:,2:] x FROM tango");
            assertSql("x\n[[8.0]]\n", "SELECT arr[2,3-1:,2:] x FROM tango");
            assertPlanNoLeakCheck(
                    "SELECT arr[2,3-1:,2:] x FROM tango",
                    "VirtualRecord\n" +
                            "  functions: [arr[2,3-1:,2:]]\n" +
                            "    PageFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tango\n"
            );
        });
    }

    @Test
    public void testSubArrayInvalid() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT ARRAY[[[1.0, 2], [3.0, 4]], [[5.0, 6], [7.0, 8]]] arr FROM long_sequence(1))");
            assertExceptionNoLeakCheck("SELECT arr[0] FROM tango",
                    11, "array index must be positive [dim=1, index=0]"
            );
            assertExceptionNoLeakCheck("SELECT arr[1, 0] FROM tango",
                    14, "array index must be positive [dim=2, index=0]"
            );
        });
    }

    @Test
    public void testSubArrayOutOfBounds() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT ARRAY[[[1.0, 2], [3.0, 4]], [[5.0, 6], [7.0, 8]]] arr FROM long_sequence(1))");

            assertSql("x\n[]\n", "SELECT arr[3] x FROM tango");
            assertSql("x\n[]\n", "SELECT arr[2, 3] x FROM tango");
        });
    }

    @Test
    public void testToJsonDouble() {
        try (DirectArray array = new DirectArray(configuration);
             DirectUtf8Sink sink = new DirectUtf8Sink(20)
        ) {
            array.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, 2));
            array.setDimLen(0, 2);
            array.setDimLen(1, 2);
            array.applyShape();
            MemoryA memA = array.startMemoryA();
            memA.putDouble(1.0);
            memA.putDouble(2.0);
            memA.putDouble(3.0);
            memA.putDouble(4.0);
            sink.clear();
            ArrayTypeDriver.arrayToJson(array, sink, NoopArrayWriteState.INSTANCE);
            assertEquals("[[1.0,2.0],[3.0,4.0]]", sink.toString());
        }
    }

    @Test
    public void testTranspose() throws Exception {
        assertMemoryLeak(() -> {
            String original = "[[1.0,2.0],[3.0,4.0],[5.0,6.0]]";
            String transposed = "[[1.0,3.0,5.0],[2.0,4.0,6.0]]";
            assertSql("transposed\n" + transposed + "\n",
                    "SELECT transpose(ARRAY" + original + ") transposed FROM long_sequence(1)");
            execute("CREATE TABLE tango AS (SELECT ARRAY" + original + " arr FROM long_sequence(1))");
            assertSql("original\n" + original + '\n', "SELECT arr original FROM tango");
            assertSql("transposed\n" + transposed + "\n", "SELECT transpose(arr) transposed FROM tango");
            assertSql("twice_transposed\n" + original + '\n', "SELECT transpose(transpose(arr)) twice_transposed FROM tango");
        });
    }

    @Test
    public void testTransposeSubArray() throws Exception {
        assertMemoryLeak(() -> {
            String original = "[[[1.0,2.0],[3.0,4.0],[5.0,6.0]]]";
            String subTransposed = "[[1.0,3.0,5.0],[2.0,4.0,6.0]]";
            assertSql("transposed\n" + subTransposed + "\n",
                    "SELECT transpose(ARRAY" + original + "[1]) transposed FROM long_sequence(1)");
            execute("CREATE TABLE tango AS (SELECT ARRAY" + original + " arr FROM long_sequence(1))");
            assertSql("original\n" + original + '\n', "SELECT arr original FROM tango");
            assertSql("transposed\n" + subTransposed + "\n", "SELECT transpose(arr[1]) transposed FROM tango");
        });
    }

    @Test
    public void testTypeCast() {
        for (int i = 1; i < ColumnType.ARRAY_NDIMS_LIMIT; i++) {
            for (short j = ColumnType.BOOLEAN; j <= ColumnType.IPv4; j++) {
                if (!ColumnType.isSupportedArrayElementType(j)) {
                    continue;
                }
                Assert.assertTrue(ColumnType.isAssignableFrom(
                        ColumnType.encodeArrayType(j, i),
                        ColumnType.encodeArrayType(j, i)
                ));
                Assert.assertTrue(ColumnType.isAssignableFrom(
                        ColumnType.NULL,
                        ColumnType.encodeArrayType(j, i)
                ));
            }
        }

        for (int i = 1; i < ColumnType.ARRAY_NDIMS_LIMIT; i++) {
            for (short j = ColumnType.BOOLEAN; j <= ColumnType.IPv4; j++) {
                if (!ColumnType.isSupportedArrayElementType(j)) {
                    continue;
                }
                // not assignable from scalar to any array
                Assert.assertFalse(ColumnType.isAssignableFrom(j, ColumnType.encodeArrayType(j, i)));
                // ... nor the other way around
                Assert.assertFalse(ColumnType.isAssignableFrom(ColumnType.encodeArrayType(j, i), j));
            }
        }
    }

    @Test
    public void testUnionAll() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tango (a double)");
            execute("insert into tango values (null)");

            // 2 arrays of the same type and dimensionality
            assertQuery("ARRAY\n" +
                            "[1.0,2.0]\n" +
                            "[3.0,4.0,5.0]\n",
                    "SELECT ARRAY[1.0, 2.0] UNION ALL SELECT ARRAY[3.0, 4.0, 5.0] FROM long_sequence(1)",
                    null, null, false, true
            );

            // with scalar double
            assertQuery("ARRAY\n" +
                            "[1.0,2.0]\n" +
                            "[3.0]\n",
                    "SELECT ARRAY[1.0, 2.0] UNION ALL SELECT 3.0 FROM long_sequence(1)",
                    null, null, false, true
            );

            // with double::null
            assertQuery("ARRAY\n" +
                            "[1.0,2.0]\n" +
                            "null\n",
                    "SELECT ARRAY[1.0, 2.0] UNION ALL SELECT * from tango",
                    null, null, false, true
            );

            // with string
            assertQuery("ARRAY\n" +
                            "[1.0,2.0]\n" +
                            "foo\n",
                    "SELECT ARRAY[1.0, 2.0] UNION ALL SELECT 'foo' FROM long_sequence(1)",
                    null, null, false, true
            );

            // 1D and 2D arrays
            assertQuery("ARRAY\n" +
                            "[[1.0,2.0]]\n" +
                            "[[3.0,4.0],[5.0,6.0]]\n",
                    "SELECT ARRAY[1.0, 2.0] UNION ALL SELECT ARRAY[[3.0, 4.0], [5.0, 6.0]] FROM long_sequence(1)",
                    null, null, false, true
            );
        });
    }

    @Test
    public void testUnionDifferentDims() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "select" +
                            " timestamp_sequence(500000000000L,1000000L) ts, " +
                            " rnd_double_array(2,1) arr " +
                            " from long_sequence(10)" +
                            ") timestamp (ts) partition by DAY"
            );

            // Unlike x, y has single-dimension array.
            execute(
                    "create table y as (" +
                            "select" +
                            " timestamp_sequence(0L,100000000L) ts, " +
                            " rnd_double_array(1,1) arr " +
                            " from long_sequence(10)" +
                            ") timestamp (ts) partition by DAY"
            );

            execute("create table z as (x union all y)");

            assertSql(
                    "ts\tarr\n" +
                            "1970-01-06T18:53:20.000000Z\t[[null,null,null,null,0.0843832076262595,0.6508594025855301,0.7905675319675964,0.22452340856088226,null,0.6254021542412018,0.4621835429127854,0.5599161804800813,null,0.2390529010846525,0.6693837147631712]]\n" +
                            "1970-01-06T18:53:21.000000Z\t[[0.38539947865244994,0.0035983672154330515,0.3288176907679504,null,0.9771103146051203,0.24808812376657652,0.6381607531178513,null],[null,0.38179758047769774,0.12026122412833129,0.6700476391801052,0.3435685332942956,null,null,0.810161274171258]]\n" +
                            "1970-01-06T18:53:22.000000Z\t[[null,null],[null,null],[null,0.29313719347837397],[null,null],[null,null],[0.931192737286751,null],[0.8001121139739173,null],[0.92050039469858,null]]\n" +
                            "1970-01-06T18:53:23.000000Z\t[[null,null,0.40455469747939254,null,0.5659429139861241,0.8828228366697741,null,null,null,null,0.9566236549439661,null,null,null],[0.8164182592467494,null,0.5449155021518948,null,null,null,0.9640289041849747,0.7133910271555843,null,0.5891216483879789,null,null,0.48558682958070665,null],[0.44804689668613573,null,null,0.14830552335848957,null,null,0.6806873134626418,0.625966045857722,0.8733293804420821,null,0.17833722747266334,null,null,0.026836863013701473]]\n" +
                            "1970-01-06T18:53:24.000000Z\t[[null,null,null,null,0.07246172621937097],[0.4892743433711657,0.8231249461985348,null,0.4295631643526773,null],[0.7632615004324503,0.8816905018995145,null,0.5357010561860446,null],[null,null,null,0.7458169804091256,null],[0.4274704286353759,null,null,null,0.7777024823107295],[null,0.7445998836567925,0.2825582712777682,0.2711532808184136,null],[null,null,null,0.029080850168636263,0.10459352312331183],[null,0.20585069039325443,null,0.9884011094887449,0.9457212646911386],[0.05024615679069011,0.9946372046359034,0.6940904779678791,0.5391626621794673,null],[0.4416432347777828,null,null,null,0.2445295612285482],[null,0.043606408996349044,null,null,0.7260468106076399]]\n" +
                            "1970-01-06T18:53:25.000000Z\t[[null,null,0.06381657870188628,null,0.35731092171284307,0.9583687530177664,null,null,null,null,null,0.6069927532469744,null,null,null,null],[0.062027497477155635,0.6901976778065181,0.7586254118589676,null,null,null,null,null,0.5677191487344088,0.2677326840703891,null,0.23507754029460548,0.20727557301543031,null,0.9292491654871197,null],[0.49154607371672154,0.4167781163798937,0.3454148777596554,null,null,null,0.9423671624137644,null,null,0.7873229912811514,null,null,0.5859332388599638,null,0.3460851141092931,null],[null,null,null,null,null,0.6226001464598434,0.4346135812930124,0.8786111112537701,0.996637725831904,null,0.30028279396280155,null,0.8196554745841765,0.9130151105125102,null,null],[null,null,null,null,null,0.007985454958725269,0.5090837921075583,null,null,null,null,0.8775452659546193,0.8379891991223047,null,null,null],[null,null,null,0.5626370294064983,null,0.49199001716312474,0.6292086569587337,null,null,0.5779007672652298,0.5692090442741059,null,null,null,null,null],[0.7732229848518976,0.587752738240427,0.4667778758533798,null,0.7202789791127316,null,0.7407842990690816,0.9790787740413469,null,0.7527907209539796,null,0.9546417330809595,0.848083900630095,0.4698648140712085,0.8911615631017953,null],[null,0.7431472218131966,0.5889504900909748,null,null,null,0.798471808479839,null,null,null,null,null,0.8574212636138532,0.8280460741052847,0.7842455970681089,null],[null,null,0.3889200123396954,0.933609514582851,null,0.17202485647400034,null,0.4610963091405301,0.5673376522667354,0.48782086416459025,null,0.13312214396754163,0.9435138098640453,null,0.17094358360735395,null],[null,null,0.5449970817079417,null,null,null,null,0.3058008320091107,null,null,0.6479617440673516,0.5900836401674938,0.12217702189166091,0.7717552767944976,null,null]]\n" +
                            "1970-01-06T18:53:26.000000Z\t[[null,0.4627885105398635,0.4028291715584078,null,null,null,null,null,null,0.5501133139397699,0.7134500775259477,null,0.734728770956117,null,null],[0.8531407145325477,null,0.009302399817494589,null,null,0.32824342042623134,null,0.4086323159337839,null,null,null,null,null,null,0.04404000858917945],[0.14295673988709012,null,null,0.36078878996232167,null,null,null,0.7397816490927717,null,0.8386104714017393,0.9561778292078881,0.11048000399634927,null,null,0.11947100943679911],[null,null,null,null,null,0.5335953576307257,null,0.8504099903010793,null,null,null,null,0.15274858078119136,null,0.7468602267994937],[0.55200903114214,null,0.12483505553793961,0.024056391028085766,null,null,0.3663509090570607,null,null,0.6940917925148332,null,null,0.4564667537900823,null,0.4412051102084278],[null,0.43159834345466475,null,null,0.97613283653158,null,0.7707892345682454,0.8782062052833822,null,null,0.8376372223926546,0.365427022047211,null,null,null],[null,null,0.31861843394057765,null,0.9370193388878216,0.39201296350741366,null,0.28813952005117305,0.65372393289891,null,0.9375691350784857,null,0.4913342104187668,null,null],[null,null,null,null,null,0.38881940598288367,0.4444125234732249,null,null,null,null,0.5261234649527643,0.030750139424332357,0.20921704056371593,0.681606585145203],[0.11134244333117826,null,0.5863937813368164,0.2103287968720018,0.3242526975448907,0.42558021324800144,null,0.6068565916347403,null,0.004918542726028763,null,0.008134052047644613,0.1339704489137793,0.4950615235019964,0.04558283749364911],[0.3595576962747611,null,null,0.0966240354078981,null,null,null,null,0.9694731343686098,0.24584615213823513,null,0.5965069739835686,null,0.16979644136429572,0.28122627418701307],[null,null,0.8545896910200949,null,0.40609845936584743,0.041230021906704994,null,0.6852762111021103,0.08039440728458325,0.7600550885615773,0.05890936334115593,0.023600615130049185,null,0.7630648900646654,null]]\n" +
                            "1970-01-06T18:53:27.000000Z\t[[0.48573429889865705,null,null,null,0.6198590038961462],[null,0.7280036952357564,0.6404197786416339,0.828928908465152,null],[null,0.38000152873098747,0.5157225592346661,null,0.16320835762949149],[0.6952925744703682,null,null,null,null],[null,null,0.535993442770838,0.5725722946886976,null],[null,null,null,0.38392106356809774,null]]\n" +
                            "1970-01-06T18:53:28.000000Z\t[[null],[null],[0.5320636725174561],[0.13226561658653546]]\n" +
                            "1970-01-06T18:53:29.000000Z\t[[0.22371932699681862,null,0.38656452532530694,null,0.4019292440508081,null,null,null,0.6367746812001958],[null,null,0.7910659228440695,0.9578716688144072,null,null,0.7861254708288083,0.1319044042993568,0.45862629276476996],[0.3812506482325819,null,0.4104855595304533,null,0.7587860024773928,0.48422909268940273,0.9176263114713273,null,0.6281252905002019],[null,0.2824076895992761,0.9109198044456538,null,0.858967821197869,0.1900488162112337,null,null,0.736755875734414],[0.12465120312903266,0.04734223739255916,0.10424082472921137,null,0.9266929571641075,0.19823647700531244,null,0.265224199619046,null],[0.8306929906890365,null,null,null,null,0.4613501223216129,0.056099346819408535,null,null],[null,0.6355534187114189,0.19073234832401043,null,0.4246651384043666,0.3031376204022046,0.08533575092925538,0.6266142259812271,0.8925004728084927],[null,null,0.8645117110218422,0.7298540433653912,0.588707402066503,null,0.9891642698247116,null,null],[null,null,0.761296424148768,null,0.43990342764801993,0.6130518815428464,0.9755586311085417,0.5522442336842381,0.9385037871004874],[0.16227550791363532,null,null,0.9154548873622441,0.8470755372946043,0.8061988461374605,0.6343133564417237,null,null],[0.9213721848053825,0.30394683981054627,null,0.4510483042269017,null,null,null,null,0.38106875419134767],[0.3838483044911978,null,0.6224023788514188,null,0.3836027299974998,null,0.8151906661765794,null,0.3197593740177185],[null,null,0.4176571781712538,null,null,null,0.5191884769160172,null,null],[0.7200031730502818,null,0.11919556761688443,0.7183071061096172,null,null,0.6391251028594114,null,null],[0.1790475858715116,0.7504512900310369,0.9583685768295167,null,null,null,null,null,0.018689012580364706],[0.8940422626709261,null,0.7536836395346167,0.04727174057972261,null,null,null,0.28598292472656794,null]]\n" +
                            "1970-01-01T00:00:00.000000Z\t[[0.16638148883943538,0.42955172504398687,null,null,null,null,0.5455175324785665,0.8248550185892197,null,0.8669667625804924]]\n" +
                            "1970-01-01T00:01:40.000000Z\t[[0.8542659024913913,null,0.13027802162685043,null,null,0.7820780604033565,0.11594908641822632,0.865090011761414,null,null,0.3669999679163578,0.7116036820921031]]\n" +
                            "1970-01-01T00:03:20.000000Z\t[[null,null,0.7995154334778554,null,0.04653410879629005]]\n" +
                            "1970-01-01T00:05:00.000000Z\t[[null,null,null,null]]\n" +
                            "1970-01-01T00:06:40.000000Z\t[[0.32317345869453706,0.9632550816669818,null,null,null,null,0.7436419445622273,null,null,null,null,0.9700250276928207,0.9251043257912728,null,null,null]]\n" +
                            "1970-01-01T00:08:20.000000Z\t[[null,null]]\n" +
                            "1970-01-01T00:10:00.000000Z\t[[0.04229155272030727,null,0.11500943478849246,null,0.23290767295012593,0.5892857157920004,0.0983023224719013,null,0.04969674919093636,0.9615445349922865]]\n" +
                            "1970-01-01T00:11:40.000000Z\t[[null,null]]\n" +
                            "1970-01-01T00:13:20.000000Z\t[[0.8503316000896455,0.8671405978559277,null,null,null,null,null,null,null,null,null,null]]\n" +
                            "1970-01-01T00:15:00.000000Z\t[[0.543578943048389,0.2524690658195553,null,null,0.8503583964852268,null,0.9236914780318218,0.9759588687260482,null,0.39303711474170466,0.10999942140960017,null,0.8267002457663516,null,null]]\n",
                    "z"
            );
        });
    }

    @Test
    public void testUnionDistinct() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table alpha (arr double[])");
            execute("create table bravo (arr double[])");

            execute("insert into alpha values (ARRAY[1.0, 2.0])");
            assertQuery("arr\n[1.0,2.0]\n",
                    "select * from alpha union select * from bravo",
                    null,
                    null,
                    false,
                    false
            );

            execute("insert into bravo values (ARRAY[1.0, 2.0])");
            assertQuery("arr\n[1.0,2.0]\n",
                    "select * from alpha union select * from bravo",
                    null,
                    null,
                    false,
                    false
            );

            execute("insert into alpha values (ARRAY[1.0, 2.0, 3.0])");
            assertQuery("arr\n" +
                            "[1.0,2.0]\n" +
                            "[1.0,2.0,3.0]\n",
                    "select * from alpha union select * from bravo",
                    null,
                    null,
                    false,
                    false
            );

            execute("insert into bravo values (ARRAY[1.0, 2.0, 3.0])");
            assertQuery("arr\n" +
                            "[1.0,2.0]\n" +
                            "[1.0,2.0,3.0]\n",
                    "select * from alpha union select * from bravo",
                    null,
                    null,
                    false,
                    false
            );

            execute("insert into alpha values (ARRAY[])");
            assertQuery("arr\n" +
                            "[1.0,2.0]\n" +
                            "[1.0,2.0,3.0]\n" +
                            "[]\n",
                    "select * from alpha union select * from bravo",
                    null,
                    null,
                    false,
                    false
            );

            execute("insert into bravo values (ARRAY[])");
            assertQuery("arr\n" +
                            "[1.0,2.0]\n" +
                            "[1.0,2.0,3.0]\n" +
                            "[]\n",
                    "select * from alpha union select * from bravo",
                    null,
                    null,
                    false,
                    false
            );

            execute("insert into alpha values (null)");
            assertQuery("arr\n" +
                            "[1.0,2.0]\n" +
                            "[1.0,2.0,3.0]\n" +
                            "[]\n" +
                            "null\n",
                    "select * from alpha union select * from bravo",
                    null,
                    null,
                    false,
                    false
            );
        });
    }

    @Test
    public void testUnsupportedDimensionality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (a DOUBLE[][][][][][][][][][][][][][][][])");
            try (TableMetadata m = engine.getTableMetadata(engine.verifyTableName("x"))) {
                Assert.assertEquals(1, m.getColumnCount());
                Assert.assertEquals("a", m.getColumnName(0));
                Assert.assertEquals("DOUBLE[][][][][][][][][][][][][][][][]", ColumnType.nameOf(m.getColumnType(0)));
            }
            assertExceptionNoLeakCheck(
                    "CREATE TABLE y (a DOUBLE[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][])", // 33 dimensions
                    18,
                    "too many array dimensions [nDims=33, maxNDims=32]"
            );
        });
    }
}
