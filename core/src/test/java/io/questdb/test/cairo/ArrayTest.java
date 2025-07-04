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

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.DerivedArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.arr.NoopArrayWriteState;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cutlass.line.tcp.ArrayBinaryFormatParser;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
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

    public void assertBroadcastShape(IntList shapeLeft, IntList shapeRight, IntList shapeOutExpected) throws Exception {
        assertMemoryLeak(() -> {
            try (DirectArray left = new DirectArray();
                 DirectArray right = new DirectArray()
            ) {
                left.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, shapeLeft.size()));
                right.setType(ColumnType.encodeArrayType(ColumnType.DOUBLE, shapeRight.size()));

                for (int i = 0; i < shapeLeft.size(); i++) {
                    left.setDimLen(i, shapeLeft.get(i));
                }
                left.applyShape();
                for (int i = 0; i < shapeRight.size(); i++) {
                    right.setDimLen(i, shapeRight.get(i));
                }
                right.applyShape();
                IntList shapeOut = new IntList();
                DerivedArrayView.computeBroadcastShape(left, right, shapeOut, -1);
                System.out.println(shapeOut);
                Assert.assertEquals(shapeOutExpected, shapeOut);
            }
        });
    }

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, String.valueOf("true"));
        super.setUp();
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
    public void testArrayAddScalarValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE[], b DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[2.0, null], ARRAY[[2.0, 3], [4.0, 5]]), " +
                    "(ARRAY[6.0, 7], ARRAY[[8.0, 9]])," +
                    "(null, null)");
            assertSql("column\tcolumn1\tcolumn2\tcolumn3\n" +
                    "[7.0,NaN]\t[[5.0,7.0],[9.0,11.0]]\t[11.0,16.0]\t[[41.0,51.0]]\n" +
                    "[19.0,22.0]\t[[17.0,19.0]]\t[41.0,46.0]\t[]\n" +
                    "null\tnull\tnull\tnull\n", "SELECT a * 3.0 + 1.0, b * 2.0 + 1.0, b[1] * 5.0 + 1.0, b[2:] * 10.0 + 1.0 FROM tango");
            assertSql("column\tcolumn1\n" +
                    "[5.0,NaN]\t[[4.0,6.0],[5.0,7.0]]\n" +
                    "[9.0,10.0]\t[[10.0],[11.0]]\n" +
                    "null\tnull\n", "SELECT transpose(a) + 3.0, transpose(b) + 2.0 FROM tango");
            assertSql("column\tcolumn1\tcolumn2\tcolumn3\n" +
                    "[5.0,NaN]\t[[4.0,5.0],[6.0,7.0]]\t[7.0,8.0]\t[[14.0,15.0]]\n" +
                    "[9.0,10.0]\t[[10.0,11.0]]\t[13.0,14.0]\t[]\n" +
                    "null\tnull\tnull\tnull\n", "SELECT 3.0 + a, 2.0 + b, 5.0 + b[1], 10.0 + b[2:] FROM tango");
        });
    }

    @Test
    public void testArrayAvg() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr1 DOUBLE[], arr2 DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[1.0, 9, 10, 12, 8, null, 20], ARRAY[[1.0, 9, 10, 12, 8, null, 20]]), " +
                    "(ARRAY[], ARRAY[[null]])," +
                    "(null, null)"
            );
            assertSql("array_avg\tarray_avg1\tarray_avg2\n" +
                            "10.0\t11.8\t5.0\n" +
                            "null\tnull\tnull\n" +
                            "null\tnull\tnull\n",
                    "SELECT array_avg(arr1), array_avg(arr1[2:]), array_avg(arr1[1:3]) FROM tango");
            assertSql("array_avg\tarray_avg1\tarray_avg2\tarray_avg3\tarray_avg4\n" +
                            "10.0\t10.0\t10.0\t10.0\tnull\n" +
                            "null\tnull\tnull\tnull\tnull\n" +
                            "null\tnull\tnull\tnull\tnull\n",
                    "SELECT array_avg(arr2), array_avg(transpose(arr2)), array_avg(arr2[1]), array_avg(arr2[1:]), array_avg(arr2[2:]) FROM tango");
        });
    }

    @Test
    public void testArrayAvgNonVanilla() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]]]);"
            );
            assertSql("array_avg\tarray_avg1\n" +
                            "5.0\t5.0\n",
                    "SELECT array_avg(arr), array_avg(transpose(arr)) FROM tango");
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
    public void testArrayCount() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr1 DOUBLE[], arr2 DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[1.0, 9, 10, 12, 8, null, 20, 12], ARRAY[[1.0, 9, 10, 12, 8, null, 20, 12]]), " +
                    "(ARRAY[], ARRAY[[null]])," +
                    "(null, null)"
            );
            assertSql("array_count\tarray_count1\tarray_count2\n" +
                            "7\t6\t2\n" +
                            "0\t0\t0\n" +
                            "0\t0\t0\n",
                    "SELECT array_count(arr1), array_count(arr1[2:]), array_count(arr1[1:3]) FROM tango");

            assertSql("array_count\tarray_count1\tarray_count2\tarray_count3\tarray_count4\n" +
                            "7\t7\t7\t7\t0\n" +
                            "0\t0\t0\t0\t0\n" +
                            "0\t0\t0\t0\t0\n",
                    "SELECT array_count(arr2), array_count(transpose(arr2)), array_count(arr2[1]), array_count(arr2[1:]), array_count(arr2[2:]) FROM tango");
        });
    }

    @Test
    public void testArrayCountNonVanilla() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]]]);"
            );
            assertSql("array_count\tarray_count1\n" +
                            "9\t9\n",
                    "SELECT array_count(arr), array_count(transpose(arr)) FROM tango");
        });
    }

    @Test
    public void testArrayCumSum() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr1 DOUBLE[], arr2 DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[1.0, 9, 10, 12, 8, null, 20, 12], ARRAY[[1.0, 9, 10, 12, 8, null, 20, 12]]), " +
                    "(ARRAY[null], ARRAY[[null]])," +
                    "(null, null)"
            );
            assertSql("array_cum_sum\tarray_cum_sum1\tarray_cum_sum2\n" +
                            "[1.0,10.0,20.0,32.0,40.0,40.0,60.0,72.0]\t[9.0,19.0,31.0,39.0,39.0,59.0,71.0]\t[1.0,10.0]\n" +
                            "[0.0]\t[]\t[0.0]\n" +
                            "null\tnull\tnull\n",
                    "SELECT array_cum_sum(arr1), array_cum_sum(arr1[2:]), array_cum_sum(arr1[1:3]) FROM tango");

            assertSql("array_cum_sum\tarray_cum_sum1\tarray_cum_sum2\tarray_cum_sum3\tarray_cum_sum4\n" +
                            "[1.0,10.0,20.0,32.0,40.0,40.0,60.0,72.0]\t[1.0,10.0,20.0,32.0,40.0,40.0,60.0,72.0]\t[1.0,10.0,20.0,32.0,40.0,40.0,60.0,72.0]\t[1.0,10.0,20.0,32.0,40.0,40.0,60.0,72.0]\t[]\n" +
                            "[0.0]\t[0.0]\t[0.0]\t[0.0]\t[]\n" +
                            "null\tnull\tnull\tnull\tnull\n",
                    "SELECT array_cum_sum(arr2), array_cum_sum(transpose(arr2)), array_cum_sum(arr2[1]), array_cum_sum(arr2[1:]), array_cum_sum(arr2[2:]) FROM tango");
        });
    }

    @Test
    public void testArrayDivScalarValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE[], b DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[2.0, null], ARRAY[[2.0, 3], [4.0, 5]]), " +
                    "(ARRAY[6.0, 7], ARRAY[[8.0, 9]])," +
                    "(null, null)");
            assertSql("column\tcolumn1\tcolumn2\tcolumn3\n" +
                    "[12.0,NaN]\t[[8.0,12.0],[16.0,20.0]]\t[20.0,30.0]\t[[80.0,100.0]]\n" +
                    "[36.0,42.0]\t[[32.0,36.0]]\t[80.0,90.0]\t[]\n" +
                    "null\tnull\tnull\tnull\n", "SELECT a * 3.0/0.5, b * 2.0/0.5, b[1] * 5.0 / 0.5, b[2:] * 10.0 / 0.5 FROM tango");
            assertSql("column\tcolumn1\n" +
                    "[4.0,NaN]\t[[4.0,8.0],[6.0,10.0]]\n" +
                    "[12.0,14.0]\t[[16.0],[18.0]]\n" +
                    "null\tnull\n", "SELECT transpose(a)/0.5, transpose(b)/0.5 FROM tango");
            assertSql("column\tcolumn1\n" +
                    "[Infinity,NaN]\t[NaN,NaN]\n" +
                    "[Infinity,Infinity]\t[NaN,NaN]\n" +
                    "null\tnull\n", "SELECT a/0.0, a/null::double FROM tango");
        });
    }

    @Test
    public void testArrayDotProduct() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (left DOUBLE[][], right DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[[1.0, 3], [2.0, 5.0]], ARRAY[[1.0, 5.0], [7.0, 2.0]]), " +
                    "(ARRAY[[1.0, 1]], ARRAY[[5.0, null]])");
            assertSql("product\n" +
                    "40.0\n" +
                    "5.0\n", "SELECT dot_product(left, right) AS product FROM tango");
            assertSql("product\n" +
                    "40.0\n" +
                    "5.0\n", "SELECT dot_product(transpose(left), transpose(right)) AS product FROM tango");
            assertExceptionNoLeakCheck("SELECT dot_product(Array[1.0], Array[[1.0]]) AS product FROM tango",
                    24, "arrays have different number of dimensions [nDimsLeft=1, nDimsRight=2]");
            assertExceptionNoLeakCheck("SELECT dot_product(Array[1.0], Array[1.0, 2.0]) AS product FROM tango",
                    24, "arrays have different shapes [leftShape=[1], rightShape=[2]]");
        });
    }

    @Test
    public void testArrayDotProductScalarValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (left DOUBLE[][], right DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[[1.0, 3], [2.0, 5.0]], ARRAY[[1.0, 5.0], [7.0, 2.0]]), " +
                    "(ARRAY[[1.0, 1]], ARRAY[[5.0, null]])");
            assertSql("dot_product\tdot_product1\tdot_product2\n" +
                    "11.0\t30.0\tnull\n" +
                    "2.0\t10.0\tnull\n", "SELECT dot_product(left, 1.0), dot_product(right, 2.0), dot_product(left, null::double) FROM tango");
            assertSql("dot_product\tdot_product1\tdot_product2\n" +
                    "11.0\t30.0\t30.0\n" +
                    "2.0\t10.0\t10.0\n", "SELECT dot_product(transpose(left), 1.0), dot_product(transpose(right), 2.0), dot_product(2.0, transpose(right)) FROM tango");
        });
    }

    @Test
    public void testArrayFirstFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ts timestamp, x int, v double[]) timestamp(ts) partition by DAY");
            execute("insert into test(ts,x,v) values ('2022-02-24', 1, ARRAY[1.0,1.0]), ('2022-02-24', 2, null), ('2022-02-24', 3, ARRAY[2.0,2.0])");

            assertQuery(
                    "ts\tx\tv\n" +
                            "2022-02-24T00:00:00.000000Z\t1\t[1.0,1.0]\n" +
                            "2022-02-24T00:00:00.000000Z\t2\tnull\n" +
                            "2022-02-24T00:00:00.000000Z\t3\t[2.0,2.0]\n",
                    "select ts, x, first(v) as v from test sample by 1s",
                    "ts",
                    true,
                    true
            );

            assertPlanNoLeakCheck(
                    "select ts, x, first(v) as v from test sample by 1s",
                    "Radix sort light\n" +
                            "  keys: [ts]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [ts,x]\n" +
                            "      values: [first(v)]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: test\n"
            );
        });
    }

    @Test
    public void testArrayFunctionInAggregation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tango (ts timestamp, a double, arr double[]) timestamp(ts) partition by DAY");
            execute("insert into tango values " +
                    "('2025-06-26', 1.0, ARRAY[1.0,2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0])," +
                    "('2025-06-26', 10.0, null)," +
                    "('2025-06-27', 18.0, ARRAY[11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0])," +
                    "('2025-06-27', 25.0, ARRAY[21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0])");
            assertQueryAndPlan(
                    "ts\tv\n" +
                            "2025-06-26T00:00:00.000000Z\t1\n" +
                            "2025-06-27T00:00:00.000000Z\t8\n",
                    "Radix sort light\n" +
                            "  keys: [ts]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [ts]\n" +
                            "      values: [max(array_position(arr, a))]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tango\n",
                    "select ts, max(array_position(arr, a)) as v from tango sample by 1d",
                    "ts",
                    true,
                    true
            );

            assertQueryAndPlan(
                    "ts\tv\n" +
                            "2025-06-26T00:00:00.000000Z\t2\n" +
                            "2025-06-27T00:00:00.000000Z\t6\n",
                    "Radix sort light\n" +
                            "  keys: [ts]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [ts]\n" +
                            "      values: [min(insertion_point(arr,a))]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tango\n",
                    "select ts, min(insertion_point(arr, a)) as v from tango sample by 1d",
                    "ts",
                    true,
                    true
            );

            assertQueryAndPlan(
                    "ts\tv\n" +
                            "2025-06-26T00:00:00.000000Z\t10\n" +
                            "2025-06-27T00:00:00.000000Z\t20\n",
                    "Radix sort light\n" +
                            "  keys: [ts]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [ts]\n" +
                            "      values: [sum(array_count(arr))]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tango\n",
                    "select ts, sum(array_count(arr)) as v from tango sample by 1d",
                    "ts",
                    true,
                    true
            );

            assertQueryAndPlan(
                    "ts\tv\n" +
                            "2025-06-26T00:00:00.000000Z\t5.5\n" +
                            "2025-06-27T00:00:00.000000Z\t41.0\n",
                    "Radix sort light\n" +
                            "  keys: [ts]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [ts]\n" +
                            "      values: [sum(array_avg(arr))]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tango\n",
                    "select ts, sum(array_avg(arr)) as v from tango sample by 1d",
                    "ts",
                    true,
                    true
            );

            assertQueryAndPlan(
                    "ts\tarray_sum\tsum\n" +
                            "2025-06-26T00:00:00.000000Z\t220.0\t1.0\n" +
                            "2025-06-26T00:00:00.000000Z\t0.0\t10.0\n" +
                            "2025-06-27T00:00:00.000000Z\t770.0\t18.0\n" +
                            "2025-06-27T00:00:00.000000Z\t1320.0\t25.0\n",
                    "Radix sort light\n" +
                            "  keys: [ts]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [ts,array_sum]\n" +
                            "      values: [sum(a)]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tango\n",
                    "select ts, array_sum(array_cum_sum(arr)), sum(a) from tango sample by 1d",
                    "ts",
                    true,
                    true
            );

            assertQueryAndPlan(
                    "ts\tdot_product\tfirst\n" +
                            "2025-06-26T00:00:00.000000Z\t110.0\t1.0\n" +
                            "2025-06-26T00:00:00.000000Z\tnull\t10.0\n" +
                            "2025-06-27T00:00:00.000000Z\t310.0\t18.0\n" +
                            "2025-06-27T00:00:00.000000Z\t510.0\t25.0\n",
                    "Radix sort light\n" +
                            "  keys: [ts]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [ts,dot_product]\n" +
                            "      values: [first(a)]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tango\n",
                    "select ts, dot_product(arr, 2), first(a) from tango sample by 1d",
                    "ts",
                    true,
                    true
            );

            assertQueryAndPlan(
                    "ts\tsum\n" +
                            "2025-06-26T00:00:00.000000Z\t147.5\n" +
                            "2025-06-27T00:00:00.000000Z\t1045.0\n",
                    "Radix sort light\n" +
                            "  keys: [ts]\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [ts]\n" +
                            "      values: [sum(array_sum(arr*5+3-1/2))]\n" +
                            "      filter: null\n" +
                            "        PageFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tango\n",
                    "select ts, sum(array_sum((arr * 5 + 3 - 1)/2)) from tango sample by 1d",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testArrayMultiplyScalarValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE[], b DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[2.0, null], ARRAY[[2.0, 3], [4.0, 5]]), " +
                    "(ARRAY[6.0, 7], ARRAY[[8.0, 9]])," +
                    "(null, null)");
            assertSql("column\tcolumn1\tcolumn2\tcolumn3\n" +
                    "[6.0,NaN]\t[[4.0,6.0],[8.0,10.0]]\t[10.0,15.0]\t[[40.0,50.0]]\n" +
                    "[18.0,21.0]\t[[16.0,18.0]]\t[40.0,45.0]\t[]\n" +
                    "null\tnull\tnull\tnull\n", "SELECT a * 3.0, b * 2.0, b[1] * 5.0, b[2:] * 10.0 FROM tango");
            assertSql("column\tcolumn1\n" +
                    "[6.0,NaN]\t[[4.0,8.0],[6.0,10.0]]\n" +
                    "[18.0,21.0]\t[[16.0],[18.0]]\n" +
                    "null\tnull\n", "SELECT transpose(a) * 3.0, transpose(b) * 2.0 FROM tango");
            assertSql("column\tcolumn1\tcolumn2\tcolumn3\n" +
                    "[6.0,NaN]\t[[4.0,6.0],[8.0,10.0]]\t[10.0,15.0]\t[[40.0,50.0]]\n" +
                    "[18.0,21.0]\t[[16.0,18.0]]\t[40.0,45.0]\t[]\n" +
                    "null\tnull\tnull\tnull\n", "SELECT 3.0 * a, 2.0 * b, 5.0 * b[1], 10.0 * b[2:] FROM tango");
        });
    }

    @Test
    public void testArrayPosition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr1 DOUBLE[], arr2 DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[1.0, 9, 10, 12, 8, null, 20, 12], ARRAY[[1.0, 9, 10, 12, 8, null, 20, 12]]), " +
                    "(ARRAY[null], ARRAY[[null]])," +
                    "(null, null)"
            );
            assertSql("array_position\tarray_position1\tarray_position2\tarray_position3\n" +
                            "5\t6\tnull\t1\n" +
                            "null\t1\tnull\tnull\n" +
                            "null\tnull\tnull\tnull\n",
                    "SELECT " +
                            "array_position(arr1, 8), " +
                            "array_position(arr1, null), " +
                            "array_position(arr1, 11), " +
                            "array_position(arr1[2:], 9) " +
                            "FROM tango");

            assertSql("array_position\tarray_position1\tarray_position2\tarray_position3\n" +
                            "5\t6\tnull\t1\n" +
                            "null\t1\tnull\tnull\n" +
                            "null\tnull\tnull\tnull\n",
                    "SELECT " +
                            "array_position(arr2[1], 8), " +
                            "array_position(arr2[1], null), " +
                            "array_position(arr2[1], 11), " +
                            "array_position(arr2[1][2:], 9) " +
                            "FROM tango");

            assertSql("array_position\tarray_position1\tarray_position2\tarray_position3\n" +
                            "1\t2\t3\t1\n" +
                            "1\t1\t1\tnull\n" +
                            "null\tnull\tnull\tnull\n",
                    "SELECT " +
                            "array_position(arr1, arr1[1]), " +
                            "array_position(arr1, arr1[2]), " +
                            "array_position(arr1, arr1[3]), " +
                            "array_position(arr1[2:], arr1[2]) " +
                            "FROM tango");
            assertExceptionNoLeakCheck("SELECT array_position(arr2, 0) len FROM tango",
                    22, "array is not one-dimensional");
        });
    }

    @Test
    public void testArrayPositionNanInfinity() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[])");
            execute("INSERT INTO tango VALUES (ARRAY[1.0/0.0, 0.0/0.0, -1.0/0.0])");
            assertSql("array_position\n2\n", "SELECT array_position(arr, 0.0/0.0) FROM tango");
            //TODO These two assertions document the current behavior, but it isn't the desired one.
            // The function should find infinities as well.
            assertSql("array_position\nnull\n", "SELECT array_position(arr, 1.0/0.0) FROM tango");
            assertSql("array_position\nnull\n", "SELECT array_position(arr, -1.0/0.0) FROM tango");
        });
    }

    @Test
    public void testArrayPositionNonVanilla() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[[1.0], [9], [10], [12], [8], [null], [20], [12]]) "
            );
            assertSql("array_position\tarray_position1\tarray_position2\tarray_position3\n" +
                            "5\t6\tnull\t1\n",
                    "SELECT " +
                            "array_position(transpose(arr)[1], 8), " +
                            "array_position(transpose(arr)[1], null), " +
                            "array_position(transpose(arr)[1], 11), " +
                            "array_position(transpose(arr)[1, 2:], 9) " +
                            "FROM tango");
        });
    }

    @Test
    public void testArraySubtractScalarValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE[], b DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[2.0, null], ARRAY[[2.0, 3], [4.0, 5]]), " +
                    "(ARRAY[6.0, 7], ARRAY[[8.0, 9]])," +
                    "(null, null)");
            assertSql("column\tcolumn1\tcolumn2\tcolumn3\n" +
                    "[5.0,NaN]\t[[3.0,5.0],[7.0,9.0]]\t[9.0,14.0]\t[[39.0,49.0]]\n" +
                    "[17.0,20.0]\t[[15.0,17.0]]\t[39.0,44.0]\t[]\n" +
                    "null\tnull\tnull\tnull\n", "SELECT a * 3.0 - 1.0, b * 2.0 - 1.0, b[1] * 5.0 - 1.0, b[2:] * 10.0 - 1.0 FROM tango");
            assertSql("column\tcolumn1\n" +
                    "[-1.0,NaN]\t[[0.0,2.0],[1.0,3.0]]\n" +
                    "[3.0,4.0]\t[[6.0],[7.0]]\n" +
                    "null\tnull\n", "SELECT transpose(a) - 3.0, transpose(b) - 2.0 FROM tango");
            assertSql("column\n" +
                    "[NaN,NaN]\n" +
                    "[NaN,NaN]\n" +
                    "null\n", "SELECT a - null::double FROM tango");
        });
    }

    @Test
    public void testArraySum() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr1 DOUBLE[], arr2 DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[1.0, 9, 10, 12, 8, null, 20, 12], ARRAY[[1.0, 9, 10, 12, 8, null, 20, 12]]), " +
                    "(ARRAY[null], ARRAY[[null]])," +
                    "(null, null)"
            );
            assertSql("array_sum\tarray_sum1\tarray_sum2\n" +
                            "72.0\t71.0\t10.0\n" +
                            "0.0\t0.0\t0.0\n" +
                            "0.0\t0.0\t0.0\n",
                    "SELECT array_sum(arr1), array_sum(arr1[2:]), array_sum(arr1[1:3]) FROM tango");

            assertSql("array_sum\tarray_sum1\tarray_sum2\tarray_sum3\tarray_sum4\n" +
                            "72.0\t72.0\t72.0\t72.0\t0.0\n" +
                            "0.0\t0.0\t0.0\t0.0\t0.0\n" +
                            "0.0\t0.0\t0.0\t0.0\t0.0\n",
                    "SELECT array_sum(arr2), array_sum(transpose(arr2)), array_sum(arr2[1]), array_sum(arr2[1:]), array_sum(arr2[2:]) FROM tango");
        });
    }

    @Test
    public void testArraySumNonVanilla() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]]]);"
            );
            assertSql("array_sum\tarray_sum1\n" +
                            "45.0\t45.0\n",
                    "SELECT array_sum(arr), array_sum(transpose(arr)) FROM tango");
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
    public void testBasicArithmeticAutoBroadcast() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE[][], b DOUBLE[])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[[0.0, 0, 0], [10, 10, 10], [20, 20, 20], [30, 30, 30]], ARRAY[0, 1, 2])");
            assertSql("column\tcolumn1\tcolumn2\tcolumn3\n" +
                            "[[0.0,1.0,2.0],[10.0,11.0,12.0],[20.0,21.0,22.0],[30.0,31.0,32.0]]\t[[0.0,-1.0,-2.0],[10.0,9.0,8.0],[20.0,19.0,18.0],[30.0,29.0,28.0]]\t[[0.0,0.0,0.0],[0.0,10.0,20.0],[0.0,20.0,40.0],[0.0,30.0,60.0]]\t[[NaN,0.0,0.0],[Infinity,10.0,5.0],[Infinity,20.0,10.0],[Infinity,30.0,15.0]]\n",
                    "SELECT a + b, a - b, a * b, a / b FROM tango");
            assertSql("column\n" +
                            "[[0.0,-1.0,-4.0],[100.0,99.0,96.0],[400.0,399.0,396.0],[900.0,899.0,896.0]]\n",
                    "SELECT (a + b) * (a - b) from tango");
            execute("CREATE TABLE tango1 (a DOUBLE[][], b DOUBLE[])");
            execute("INSERT INTO tango1 VALUES " +
                    "(ARRAY[[1.0, 2.0]], ARRAY[0, 1, 2])");
            assertException("select a + b from tango1",
                    7,
                    "arrays have incompatible shapes [leftShape=[1,2], rightShape=[3]]");
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
    public void testComputeBroadcastShape() throws Exception {
        IntList shapeLeft = new IntList();
        IntList shapeRight = new IntList();
        IntList shapeOutExpected = new IntList();

        fillIntList(shapeLeft, 1);
        fillIntList(shapeRight, 2);
        fillIntList(shapeOutExpected, 2);
        assertBroadcastShape(shapeLeft, shapeRight, shapeOutExpected);

        fillIntList(shapeLeft, 1, 1);
        fillIntList(shapeRight, 2, 2);
        fillIntList(shapeOutExpected, 2, 2);
        assertBroadcastShape(shapeLeft, shapeRight, shapeOutExpected);

        fillIntList(shapeLeft, 1, 2);
        fillIntList(shapeRight, 2, 1);
        fillIntList(shapeOutExpected, 2, 2);
        assertBroadcastShape(shapeLeft, shapeRight, shapeOutExpected);

        fillIntList(shapeLeft, 1);
        fillIntList(shapeRight, 1, 1);
        fillIntList(shapeOutExpected, 1, 1);
        assertBroadcastShape(shapeLeft, shapeRight, shapeOutExpected);

        fillIntList(shapeLeft, 1);
        fillIntList(shapeRight, 1, 2);
        fillIntList(shapeOutExpected, 1, 2);
        assertBroadcastShape(shapeLeft, shapeRight, shapeOutExpected);

        fillIntList(shapeLeft, 1);
        fillIntList(shapeRight, 2, 2);
        fillIntList(shapeOutExpected, 2, 2);
        assertBroadcastShape(shapeLeft, shapeRight, shapeOutExpected);

        fillIntList(shapeLeft, 1, 2);
        fillIntList(shapeRight, 2, 1, 2);
        fillIntList(shapeOutExpected, 2, 1, 2);
        assertBroadcastShape(shapeLeft, shapeRight, shapeOutExpected);

        fillIntList(shapeLeft, 1, 2);
        fillIntList(shapeRight, 2, 2, 1);
        fillIntList(shapeOutExpected, 2, 2, 2);
        assertBroadcastShape(shapeLeft, shapeRight, shapeOutExpected);
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
                            "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t[0.6276954028373309,0.6693837147631712,0.7094360487171202,0.8756771741121929,0.1985581797355932,0.0035983672154330515,0.5249321062686694,0.690540444367637,0.021651819007252326,0.21583224269349388]\t-1271909747\n" +
                            "鉾檲\\~2\t[0.810161274171258,0.4138164748227684,0.3762501709498378,0.022965637512889825,0.2459345277606021,0.29313719347837397,0.975019885372507,0.16474369169931913,0.4900510449885239,0.7643643144642823,0.9075843364017028,0.04142812470232493,0.18769708157331322,0.7997733229967019,0.5182451971820676]\t1985398001\n" +
                            "톬F\uD9E6\uDECD1Ѓ\t[0.5778947915182423,0.8164182592467494,0.8685154305419587,0.5449155021518948]\t-731466113\n" +
                            "!{j<\t[0.6359144993891355,0.5891216483879789,0.4971342426836798,0.6752509547112409,0.5065228336156442,0.9047642416961028,0.2553319339703062,0.34947269997137365,0.14830552335848957,0.2879973939681931,0.812339703450908,0.3456897991538844,0.625966045857722]\t639125092\n" +
                            "\uDAB2\uDF79軦۽㒾\uD99D\uDEA7K\t[0.4295631643526773,0.26369335635512836,0.7632615004324503,0.5699444693578853,0.9820662735672192]\t1907911110\n" +
                            "㓆\u0093ً\uDAF5\uDE17qRӽ-\uDBED\uDC98\t[0.38422543844715473,0.48964139862697853,0.5391626621794673,0.17180291960857297,0.4416432347777828,0.2065823085842221,0.8584308438045006,0.2445295612285482,0.6590829275055244,0.043606408996349044,0.5290006415737116,0.3568111021227658,0.6707018622395736,0.7229359906306887]\t-720881601\n" +
                            "I62Duiz\t[0.4609277382153818,0.5780746276543334,0.40791879008699594,0.12663676991275652,0.21485589614090927,0.8720995238279701,0.062027497477155635,0.7203170014947307,0.09303344348778264,0.7586254118589676,0.7422641630544511,0.08890450062949395]\t-529035657\n" +
                            "\uD958\uDD6A\uDA43\uDFF0-㔍x钷M\t[0.5459599769700721,0.04001697462715281,0.5859332388599638,0.08712007604601191,0.3460851141092931,0.5780819331422455,0.18586435581637295,0.18852800970933203,0.818064803221824,0.5551537218890106,0.4346135812930124,0.2559680920632348,0.5740181242665339]\t-1501720177\n" +
                            "~)1>\uDAEE\uDC4FƑ䈔b\t[0.2394591643144588,0.8775452659546193,0.0024457698760806945,0.19736767249829557,0.11591855759299885,0.3121271759430503]\t-772867311\n" +
                            "\uDA76\uDDD4*\uDB87\uDF60-\t[0.587752738240427,0.03192108074989719,0.988853350870454]\t-996946108\n",
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
    public void testDiv() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE[], b DOUBLE[])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[2.0, 3.0], ARRAY[4.0, 0]), " +
                    "(ARRAY[6.0, null], ARRAY[8.0, 9])," +
                    "(null, null)");
            assertSql("div\n" +
                    "[0.5,Infinity]\n" +
                    "[0.75,NaN]\n" +
                    "null\n", "SELECT a / b div FROM tango");
        });
    }

    @Test
    public void testDivSlice3d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE[][][], b DOUBLE[][][])");
            execute("INSERT INTO tango VALUES " +
                    "( ARRAY[ [ [2.0, 3], [4.0, 5] ], [ [6.0, 7], [8.0, 9] ]  ], " +
                    "  ARRAY[ [ [10.0, 11], [12.0, 13] ], [ [14.0, 15], [16.0, 20] ]  ] ), " +
                    "( null, null )");
            assertSql("div\n" +
                            "[[[0.1]]]\n" +
                            "null\n",
                    "SELECT a[1:2, 1:2, 1:2] / b[2:, 2:, 2:] div FROM tango");
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
            ArrayTypeDriver.arrayToJson(array, sink, NoopArrayWriteState.INSTANCE, false);
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
            assertQuery("cast\n[[1.0,2.0]]\n",
                    "SELECT ARRAY[1.0, 2.0]::double[][]",
                    true
            );

            // no element arrays
            assertQuery("cast\n[]\n", // arrays with no elements are always printed as []
                    "SELECT ARRAY[]::double[][]",
                    true
            );

            // casting to fewer dimensions is not allowed
            assertException("SELECT ARRAY[[1.0], [2.0]]::double[]",
                    26,
                    "cannot cast array to lower dimension [from=DOUBLE[][] (2D), to=DOUBLE[] (1D)]. " +
                            "Use array flattening operation (e.g. 'flatten(arr)') instead"
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
            execute("CREATE TABLE tango (arr DOUBLE[][][])");
            execute("INSERT INTO tango VALUES (ARRAY[[[1.0, 2, 3], [4.0, 5, 6]], [[7.0, 8, 9], [10.0, 11, 12]]])");
            assertSql("arr\n[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0,11.0,12.0]\n",
                    "SELECT flatten(arr) arr FROM tango");
            assertSql("arr\n[[[2.0,3.0],[5.0,6.0]],[[8.0,9.0],[11.0,12.0]]]\n",
                    "SELECT arr[1:, 1:, 2:4] arr FROM tango");
            assertSql("arr\n[2.0,3.0,5.0,6.0,8.0,9.0,11.0,12.0]\n",
                    "SELECT flatten(arr[1:, 1:, 2:4]) arr FROM tango");
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
            execute("CREATE TABLE blah (a DOUBLE[][])");
            execute("INSERT INTO blah SELECT rnd_double_array(2, 2) FROM long_sequence(10)");

            assertQuery(
                    "a\n" +
                            "[[0.12966659791573354,0.299199045961845,NaN,NaN,NaN,NaN,NaN,0.9856290845874263,NaN,0.5093827001617407,0.11427984775756228,0.5243722859289777,NaN,NaN,0.7261136209823622]]\n" +
                            "[[0.6778564558839208,0.8756771741121929,NaN,NaN,0.33608255572515877,0.690540444367637,NaN,0.21583224269349388,0.15786635599554755],[NaN,NaN,0.12503042190293423,NaN,0.9687423276940171,NaN,NaN,NaN,NaN],[NaN,NaN,NaN,0.7883065830055033,NaN,0.4138164748227684,0.5522494170511608,0.2459345277606021,NaN],[NaN,0.8847591603509142,0.4900510449885239,NaN,0.9075843364017028,NaN,0.18769708157331322,0.16381374773748514,0.6590341607692226],[NaN,NaN,NaN,0.8837421918800907,0.05384400312338511,NaN,0.7230015763133606,0.12105630273556178,NaN],[0.5406709846540508,NaN,0.9269068519549879,NaN,NaN,NaN,0.1202416087573498,NaN,0.6230184956534065],[0.42020442539326086,NaN,NaN,0.4971342426836798,NaN,0.5065228336156442,NaN,NaN,0.03167026265669903],[NaN,NaN,0.2879973939681931,NaN,NaN,NaN,NaN,0.24008362859107102,NaN],[0.9455893004802433,NaN,NaN,0.2185865835029681,NaN,0.24079155981438216,0.10643046345788132,0.5244255672762055,0.0171850098561398],[0.09766834710724581,NaN,0.053594208204197136,0.26369335635512836,0.22895725920713628,0.9820662735672192,NaN,0.32424562653969957,0.8998921791869131],[NaN,NaN,NaN,0.33746104579374825,0.18740488620384377,0.10527282622013212,0.8291193369353376,0.32673950830571696,NaN],[0.18336217509438513,0.9862476361578772,0.8693768930398866,0.8189713915910615,0.5185631921367574,NaN,NaN,NaN,0.29659296554924697]]\n" +
                            "[[NaN,NaN,NaN,NaN,0.13264292470570205,0.38422543844715473,NaN,NaN,NaN,NaN,0.7668146556860689,NaN,0.05158459929273784,NaN,NaN],[NaN,0.5466900921405317,NaN,0.11128296489732104,0.6707018622395736,0.07594017197103131,NaN,NaN,0.5716129058692643,0.05094182589333662,NaN,NaN,0.4609277382153818,0.5691053034055052,0.12663676991275652],[0.11371841836123953,NaN,NaN,0.7203170014947307,NaN,NaN,NaN,NaN,0.7704949839249925,0.8144207168582307,NaN,NaN,NaN,0.2836347139481469,NaN],[NaN,NaN,NaN,0.24001459007748394,NaN,NaN,NaN,0.741970173888595,0.25353478516307626,0.2739985338660311,NaN,0.8001632261203552,NaN,0.7404912278395417,0.08909442703907178],[0.8439276969435359,NaN,NaN,0.08712007604601191,0.8551850405049611,0.18586435581637295,0.5637742551872849,NaN,NaN,NaN,0.7195457109208119,NaN,0.23493793601747937,NaN,0.6334964081687151],[0.6721404635638454,0.7707249647497968,0.8813290192134411,0.17405556853190263,0.823395724427589,NaN,0.8108032283138068,NaN,NaN,0.7530494527849502,0.49153268154777974,0.0024457698760806945,0.29168465906260244,0.3121271759430503,0.3004874521886858],[NaN,0.7653255982993546,NaN,NaN,NaN,NaN,0.37873228328689634,NaN,0.7272119755925095,NaN,0.7467013668130107,0.5794665369115236,NaN,0.5308756766878475,0.03192108074989719],[NaN,0.17498425722537903,NaN,0.34257201464152764,NaN,NaN,0.29242748475227853,NaN,0.11296257318851766,NaN,0.23405440872043592,0.1479745625593103,NaN,0.8115426881784433,NaN]]\n" +
                            "[[NaN,NaN,0.04321289940104611,0.8217652538598936,0.6397125243912908,0.29419791719259025,0.865629565918467,NaN,NaN,0.16923843067953104],[0.7198854503668188,0.5174107449677378,0.38509066982448115,NaN,NaN,NaN,0.5475429391562822,0.6977332212252165,NaN,NaN],[0.4268921400209912,0.9997797234031688,0.5234892454427748,NaN,NaN,NaN,NaN,0.5169565007469263,0.7039785408034679,0.8461211697505234],[NaN,0.537020248377422,0.8766908646423737,NaN,NaN,0.31852531484741486,NaN,0.605050319285447,0.9683642405595932,0.3549235578142891],[0.04211401699125483,NaN,NaN,0.0032519916115479885,0.2703179181043681,0.729536610842768,0.3317641556575974,0.8895915828662114,NaN,NaN],[NaN,NaN,0.1599211504269954,0.5251698097331752,NaN,0.18442756220221035,NaN,0.48422587819911567,0.2970515836513553,NaN],[0.7826107801293182,NaN,0.3218450864634881,0.8034049105590781,NaN,NaN,0.40425101135606667,0.9412663583926286,NaN,NaN],[0.8376764297590714,0.15241451173695408,NaN,0.743599174001969,NaN,NaN,0.9001273812517414,0.5629104624260136,0.6001215594928115,0.8920252905736616]]\n" +
                            "[[0.6741248448728824,0.030997441190531494,NaN,NaN],[0.8853675629694284,4.945923013344178E-5,NaN,0.0016532800623808575],[0.23567419576658333,NaN,0.3489278573518253,NaN],[NaN,0.07383464174908916,0.8791439438812569,0.7110275609764849]]\n" +
                            "[[0.10820602386069589,NaN,NaN,0.11286092606280262,0.7370823954391381],[NaN,0.533524384058538,0.6749208267946962,NaN,0.3124458010612313],[NaN,NaN,0.7943185767500432,0.4137003695612732,NaN],[NaN,0.32449127848036263,0.41886400558338654,0.8409080254825717,0.06001827721556019],[NaN,NaN,NaN,0.6542559878565383,NaN],[NaN,NaN,NaN,NaN,0.0846754178136283],[NaN,NaN,0.5815065874358148,NaN,0.4039042639581232],[0.4375759068189693,0.8802810667279274,0.6944149053754287,0.27755720049807464,0.8985777419215233],[NaN,0.9815126662068089,0.22252546562577824,NaN,NaN],[0.6240138444047509,0.8869397617459538,NaN,NaN,NaN],[NaN,NaN,0.2266157317795261,0.7430101994511517,0.6993909595959196]]\n" +
                            "[[0.7617663592833062,NaN,0.021177977444738705,NaN,NaN,NaN,0.8303845449546206,NaN,NaN],[NaN,0.7636347764664544,0.2195743166842714,NaN,NaN,NaN,0.5823910118974169,0.05942010834028011,NaN],[NaN,0.9887681426881507,NaN,0.39211484750712344,NaN,0.29120049877582566,0.7876429805027644,0.16011053107067486,NaN],[0.2712934077694782,NaN,NaN,NaN,NaN,0.11768763788456049,0.06052105248562101,0.18684267640195917,0.6884149023727977],[NaN,NaN,0.47107881346820746,NaN,NaN,NaN,0.8637346569402254,NaN,0.0049253368387782714],[0.8973562700864572,NaN,NaN,0.7020508159399581,0.5862806534829702,NaN,NaN,NaN,NaN],[0.40462097000890584,0.9113999356978634,0.08941116322563869,NaN,0.4758209004780879,0.8203418140538824,0.22122747948030208,0.48731616038337855,0.05579995341081423],[NaN,0.14756353014849555,0.25604136769205754,0.44172683242560085,0.2696094902942793,0.899050586403365,NaN,NaN,0.2753819635358048],[NaN,NaN,NaN,NaN,0.019529452719755813,0.5330584032999529,0.9766284858951397,0.1999576586778039,0.47930730718677406],[0.6936669914583254,NaN,NaN,NaN,0.5940502728139653,NaN,NaN,0.10287867683029772,0.33261541215518553],[0.8756165114231503,NaN,0.307622006691777,0.6376518594972684,0.6453590096721576,NaN,0.37418657418528656,NaN,0.3981872443575455]]\n" +
                            "[[NaN,NaN,NaN,NaN],[0.27144997281940675,NaN,NaN,0.8642800031609658],[0.837738444021418,NaN,NaN,0.5081836003758192],[NaN,NaN,0.7706329763519386,NaN],[0.7874929839944909,0.6320839159367109,NaN,0.7409092302023607]]\n" +
                            "[[0.6288088087840823,0.1288393076713259,0.5350165471764692,NaN,0.3257868894353412,NaN,0.6927480038605662,NaN,NaN,0.265774789314454,0.9610592594899304,NaN,0.236380596505666,NaN,NaN,NaN],[NaN,NaN,NaN,0.06970926959068269,NaN,0.18170646835643245,NaN,0.7407581616916364,0.527776712010911,NaN,0.4701492486769596,0.7407568814442186,0.3123904307505546,0.19188599215569557,NaN,0.29916480738910844],[NaN,0.7217315729790722,0.2875739269292986,0.9347912212983339,NaN,NaN,0.991107083990332,0.6699251221933199,0.1402656562190583,NaN,NaN,0.722395777265195,0.770831153825552,0.5265022802557376,NaN,NaN],[NaN,NaN,0.31168586687815647,NaN,0.1779779439502811,0.4338972476284021,NaN,NaN,0.6627303823338926,NaN,NaN,0.8440228885347915,0.4374309393168063,NaN,NaN,NaN]]\n" +
                            "[[0.3436802159856278,0.28400807705010733,NaN],[0.47486309648420666,NaN,0.8877241452424108],[0.591069738864946,0.6051467286553064,0.13660430775944932],[0.8941438652004624,0.5952054059375091,0.5878334718062131],[NaN,NaN,NaN],[NaN,0.5613174142074612,0.48432558936820347],[0.7729111631116361,0.19245855538083634,0.8245822920507528],[0.8235056484964091,0.8645536237512511,0.8096078909402364]]\n",
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
    public void testInsertNonVanilla() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[])");
            execute("INSERT INTO tango VALUES (ARRAY[1.0, 2, 3][2:])");
            assertSql("arr\n[2.0,3.0]\n", "tango");
        });
    }

    @Test
    public void testInsertPoint() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr1 DOUBLE[], arr2 DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[9.0, 10, 12, 20, 22, 22, 22, 100, 1000, 1001], ARRAY[[9.0, 10, 12, 20, 22, 22, 22, 100, 1000, 1001]]), " +
                    "(ARRAY[1001.0, 1000, 100, 22, 22, 22, 20, 12, 10, 9], ARRAY[[1001.0, 1000, 100, 22, 22, 22, 20, 12, 10, 9]])," +
                    "(null, null)"
            );
            assertSql("i1\ti2\ti3\ti4\ti5\ti6\ti7\n" +
                            "1\t11\t2\t11\t4\t8\t10\n" +
                            "11\t1\t11\t2\t8\t7\t3\n" +
                            "null\tnull\tnull\tnull\tnull\tnull\tnull\n",
                    "SELECT " +
                            "insertion_point(arr1, 8, false) i1, " +
                            "insertion_point(arr1, 2000, false) i2, " +
                            "insertion_point(arr1, 9, false) i3, " +
                            "insertion_point(arr1, 1001, false) i4, " +
                            "insertion_point(arr1, 18, false) i5, " +
                            "insertion_point(arr1, 22, false) i6, " +
                            "insertion_point(arr1[1:], 1000, false) i7, " +
                            "FROM tango");
            assertSql("i1\ti2\ti3\ti4\ti5\ti6\ti7\n" +
                            "1\t11\t1\t10\t4\t5\t9\n" +
                            "11\t1\t10\t1\t8\t4\t2\n" +
                            "null\tnull\tnull\tnull\tnull\tnull\tnull\n",
                    "SELECT " +
                            "insertion_point(arr1, 8, true) i1, " +
                            "insertion_point(arr1, 2000, true) i2, " +
                            "insertion_point(arr1, 9, true) i3, " +
                            "insertion_point(arr1, 1001, true) i4, " +
                            "insertion_point(arr1, 18, true) i5, " +
                            "insertion_point(arr1, 22, true) i6, " +
                            "insertion_point(arr1[1:], 1000, true) i7, " +
                            "FROM tango");
            assertSql("i1\ti2\ti3\ti4\ti5\ti6\ti7\n" +
                            "1\t11\t2\t11\t4\t8\t10\n" +
                            "11\t1\t11\t2\t8\t7\t3\n" +
                            "null\tnull\tnull\tnull\tnull\tnull\tnull\n",
                    "SELECT " +
                            "insertion_point(arr2[1], 8) i1, " +
                            "insertion_point(arr2[1], 2000) i2, " +
                            "insertion_point(arr2[1], 9) i3, " +
                            "insertion_point(arr2[1], 1001) i4, " +
                            "insertion_point(arr2[1], 18) i5, " +
                            "insertion_point(arr2[1], 22) i6, " +
                            "insertion_point(arr2[1, 1:], 1000) i7, " +
                            "FROM tango");

            assertExceptionNoLeakCheck("SELECT insertion_point(arr2, 0) len FROM tango",
                    23, "array is not one-dimensional");
        });
    }

    @Test
    public void testInsertPointNonVanilla() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[[9.0], [10], [12], [20], [22], [22], [22], [100], [1000], [1001]]), " +
                    "(ARRAY[[1001.0], [1000], [100], [22], [22], [22], [20], [12], [10], [9]])," +
                    "(null)"
            );
            assertSql("i1\ti2\ti3\ti4\ti5\ti6\ti7\n" +
                            "1\t11\t2\t11\t4\t8\t10\n" +
                            "11\t1\t11\t2\t8\t7\t3\n" +
                            "null\tnull\tnull\tnull\tnull\tnull\tnull\n",
                    "SELECT " +
                            "insertion_point(transpose(arr)[1], 8, false) i1, " +
                            "insertion_point(transpose(arr)[1], 2000, false) i2, " +
                            "insertion_point(transpose(arr)[1], 9, false) i3, " +
                            "insertion_point(transpose(arr)[1], 1001, false) i4, " +
                            "insertion_point(transpose(arr)[1], 18, false) i5, " +
                            "insertion_point(transpose(arr)[1], 22, false) i6, " +
                            "insertion_point(transpose(arr)[1, 1:], 1000, false) i7, " +
                            "FROM tango");
            assertSql("i1\ti2\ti3\ti4\ti5\ti6\ti7\n" +
                            "1\t11\t1\t10\t4\t5\t9\n" +
                            "11\t1\t10\t1\t8\t4\t2\n" +
                            "null\tnull\tnull\tnull\tnull\tnull\tnull\n",
                    "SELECT " +
                            "insertion_point(transpose(arr)[1], 8, true) i1, " +
                            "insertion_point(transpose(arr)[1], 2000, true) i2, " +
                            "insertion_point(transpose(arr)[1], 9, true) i3, " +
                            "insertion_point(transpose(arr)[1], 1001, true) i4, " +
                            "insertion_point(transpose(arr)[1], 18, true) i5, " +
                            "insertion_point(transpose(arr)[1], 22, true) i6, " +
                            "insertion_point(transpose(arr)[1][1:], 1000, true) i7, " +
                            "FROM tango");
            assertSql("i1\ti2\ti3\ti4\ti5\ti6\ti7\n" +
                            "1\t11\t2\t11\t4\t8\t10\n" +
                            "11\t1\t11\t2\t8\t7\t3\n" +
                            "null\tnull\tnull\tnull\tnull\tnull\tnull\n",
                    "SELECT " +
                            "insertion_point(transpose(arr)[1], 8) i1, " +
                            "insertion_point(transpose(arr)[1], 2000) i2, " +
                            "insertion_point(transpose(arr)[1], 9) i3, " +
                            "insertion_point(transpose(arr)[1], 1001) i4, " +
                            "insertion_point(transpose(arr)[1], 18) i5, " +
                            "insertion_point(transpose(arr)[1], 22) i6, " +
                            "insertion_point(transpose(arr)[1, 1:], 1000) i7, " +
                            "FROM tango");
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
    public void testMatView() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ts timestamp, x int, v double[]) timestamp(ts) partition by DAY WAL dedup upsert keys (ts, x) ");
            execute("create materialized view test_mv as select ts, x, first(v) as v from test sample by 1s");
            execute("insert into test(ts,x,v) values ('2022-02-24', 1, ARRAY[1.0,1.0]), ('2022-02-24', 2, null), ('2022-02-24', 3, ARRAY[2.0,2.0])");

            drainWalAndMatViewQueues();

            assertSql(
                    "ts\tx\tv\n" +
                            "2022-02-24T00:00:00.000000Z\t1\t[1.0,1.0]\n" +
                            "2022-02-24T00:00:00.000000Z\t2\tnull\n" +
                            "2022-02-24T00:00:00.000000Z\t3\t[2.0,2.0]\n",
                    "test"
            );
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
    public void testMatrixMultiplyAutoBroadcasting() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT " +
                    "ARRAY[[2.0, 3.0],[4.0, 5.0], [6.0, 7.0]] left, ARRAY[1.0, 2.0] right " +
                    "FROM long_sequence(1))");
            assertSql("product\n" +
                    "[[8.0],[14.0],[20.0]]\n", "SELECT matmul(left, right) AS product FROM tango");
        });
    }

    @Test
    public void testMatrixMultiplyInvalid() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT " +
                    "ARRAY[[[1.0, 2.0]]] left3d, ARRAY[1.0] left1d, " +
                    "ARRAY[[[1.0]]] right3d, ARRAY[1.0, 2.0] right1d "
                    + "FROM long_sequence(1))");
            assertExceptionNoLeakCheck("SELECT matmul(left1d, right1d) FROM tango",
                    14, "left array row length doesn't match right array column length [leftRowLen=1, rightColLen=2]");
            assertExceptionNoLeakCheck("SELECT matmul(left3d, right1d) FROM tango",
                    22, "left array is not one or two-dimensional");
            assertExceptionNoLeakCheck("SELECT matmul(left1d, right3d) FROM tango",
                    22, "right array is not one or two-dimensional");
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
            ArrayTypeDriver.arrayToJson(array, sink, NoopArrayWriteState.INSTANCE, false);
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

            ArrayTypeDriver.arrayToJson(parserNative.getArray(), sink, NoopArrayWriteState.INSTANCE, false);
            assertEquals(textViewStr, sink.toString());
        } catch (ArrayBinaryFormatParser.ParseException e) {
            throw new RuntimeException(e);
        } finally {
            Unsafe.free(mem, allocSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testNegArrayValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE[], b DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[2.0, null], ARRAY[[2.0, 4], [4.0, 8]]), " +
                    "(ARRAY[16.0, 0], ARRAY[[8.0, 4]])," +
                    "(null, null)");

            assertSql("column\tcolumn1\tcolumn2\tcolumn3\n" +
                    "[6.0,NaN]\t[[4.0,0.0],[0.0,-8.0]]\t[0.0,-2.0]\t[[-2.0,-6.0]]\n" +
                    "[-8.0,8.0]\t[[-8.0,0.0]]\t[-6.0,-2.0]\t[]\n" +
                    "null\tnull\tnull\tnull\n", "SELECT - a + 8, (- b + 4.0) * 2.0, - b[1] + 2.0, - b[2:] + 2.0 FROM tango");
            assertSql("column\tcolumn1\n" +
                    "[14.0,NaN]\t[[14.0,12.0],[12.0,8.0]]\n" +
                    "[0.0,16.0]\t[[8.0],[12.0]]\n" +
                    "null\tnull\n", "SELECT - transpose(a) + 16.0, - transpose(b) + 16.0 FROM tango");
            assertSql("column\tcolumn1\n" +
                    "[-2.0,NaN]\t[NaN,NaN]\n" +
                    "[-16.0,0.0]\t[NaN,NaN]\n" +
                    "null\tnull\n", "SELECT - a + 0.0, - a + null::double FROM tango");
        });
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
    public void testRndDoubleArray() throws Exception {
        assertMemoryLeak(() -> {
            assertSql("rnd_double_array\n[NaN]\n",
                    "SELECT rnd_double_array('1', '1', '1', '1')");
            assertSql("rnd_double_array\n[NaN]\n",
                    "SELECT rnd_double_array(1::byte, 1::byte, 0::byte, 1::byte)");
            assertSql("rnd_double_array\n[NaN]\n",
                    "SELECT rnd_double_array(1::short, 1::short, 0::short, 1::short)");
            assertSql("rnd_double_array\n[NaN]\n",
                    "SELECT rnd_double_array(1::int, 1::int, 0::int, 1::int)");
            assertSql("rnd_double_array\n[NaN]\n",
                    "SELECT rnd_double_array(1::long, 1::long, 0::long, 1::long)");
        });
    }

    @Test
    public void testRndDoubleFunctionEdgeCases() throws Exception {
        assertMemoryLeak(() -> {
            assertExceptionNoLeakCheck(
                    "SELECT rnd_double_array('1'::char)",
                    25,
                    "nDims must be an integer"
            );
            assertExceptionNoLeakCheck(
                    "SELECT rnd_double_array(1, '1'::char)",
                    28,
                    "nanRate must be an integer"
            );
            assertExceptionNoLeakCheck(
                    "SELECT rnd_double_array(1, 1, '1'::char)",
                    31,
                    "maxDimLength must be an integer"
            );
            assertExceptionNoLeakCheck(
                    "SELECT rnd_double_array(1, 1, 0, '1'::char)",
                    34,
                    "dimLength must be an integer"
            );

            assertExceptionNoLeakCheck(
                    "SELECT rnd_double_array()",
                    7,
                    "`rnd_double_array` requires arguments: rnd_double_array(LONG constant, VARARG constant)"
            );

            assertExceptionNoLeakCheck(
                    "select rnd_double_array(10, 0, 1000)",
                    7,
                    "array element count exceeds max"
            );

            assertExceptionNoLeakCheck(
                    "select rnd_double_array(10, 0, 0)",
                    31,
                    "maxDimLength must be a positive integer [maxDimLength=0]"
            );

            assertSql("rnd_double_array\nnull\n",
                    "select rnd_double_array(0, 0, 1000)"
            );

            assertExceptionNoLeakCheck(
                    "SELECT rnd_double_array(33)",
                    24,
                    "maximum for nDims is 32"
            );

            assertExceptionNoLeakCheck(
                    "select rnd_double_array(1, -1, 1000)",
                    27,
                    "invalid nanRate [nanRate=-1]"
            );

            // not enough dim lens
            assertExceptionNoLeakCheck(
                    "select rnd_double_array(2, 1, 0, 1)",
                    33,
                    "not enough dim lengths [nDims=2, nDimLengths=1]"
            );

            assertExceptionNoLeakCheck(
                    "select rnd_double_array(2, 1, 0, 1, 2, 4)",
                    39,
                    "too many dim lengths [nDims=2, nDimLengths=3]"
            );

            assertSql(
                    "rnd_double_array\n[[NaN,NaN],[NaN,0.9856290845874263]]\n",
                    "select rnd_double_array(2, 2, 0, 2, 2)"
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
    public void testScalarDivArrayValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE[], b DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[2.0, null], ARRAY[[2.0, 4], [4.0, 8]]), " +
                    "(ARRAY[16.0, 0], ARRAY[[8.0, 4]])," +
                    "(null, null)");
            assertSql("column\tcolumn1\tcolumn2\tcolumn3\n" +
                    "[4.0,NaN]\t[[4.0,2.0],[2.0,1.0]]\t[0.5,0.25]\t[[5.0,2.5]]\n" +
                    "[0.5,Infinity]\t[[1.0,2.0]]\t[0.125,0.25]\t[]\n" +
                    "null\tnull\tnull\tnull\n", "SELECT 8.0 / a, 4.0 / b * 2.0, 2.0 / b[1] * 0.5, 2 / b[2:] * 10 FROM tango");
            assertSql("column\tcolumn1\n" +
                    "[8.0,NaN]\t[[8.0,4.0],[4.0,2.0]]\n" +
                    "[1.0,Infinity]\t[[2.0],[4.0]]\n" +
                    "null\tnull\n", "SELECT 16.0 / transpose(a), 16.0 / transpose(b)FROM tango");
            assertSql("column\tcolumn1\n" +
                    "[0.0,NaN]\t[NaN,NaN]\n" +
                    "[0.0,NaN]\t[NaN,NaN]\n" +
                    "null\tnull\n", "SELECT 0.0 / a, null::double / a FROM tango");
        });
    }

    @Test
    public void testScalarMinusArrayValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE[], b DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[2.0, null], ARRAY[[2.0, 4], [4.0, 8]]), " +
                    "(ARRAY[16.0, 0], ARRAY[[8.0, 4]])," +
                    "(null, null)");
            assertSql("column\tcolumn1\tcolumn2\tcolumn3\n" +
                    "[6.0,NaN]\t[[4.0,0.0],[0.0,-8.0]]\t[0.0,-2.0]\t[[-2.0,-6.0]]\n" +
                    "[-8.0,8.0]\t[[-8.0,0.0]]\t[-6.0,-2.0]\t[]\n" +
                    "null\tnull\tnull\tnull\n", "SELECT 8.0 - a, (4.0 - b)* 2.0, 2.0 - b[1], 2 - b[2:] FROM tango");
            assertSql("column\tcolumn1\n" +
                    "[14.0,NaN]\t[[14.0,12.0],[12.0,8.0]]\n" +
                    "[0.0,16.0]\t[[8.0],[12.0]]\n" +
                    "null\tnull\n", "SELECT 16.0 - transpose(a), 16.0 - transpose(b)FROM tango");
            assertSql("column\tcolumn1\n" +
                    "[-2.0,NaN]\t[NaN,NaN]\n" +
                    "[-16.0,0.0]\t[NaN,NaN]\n" +
                    "null\tnull\n", "SELECT 0.0 - a, null::double - a FROM tango");
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
    public void testShift() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr1 DOUBLE[], arr2 DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[1.0, 9, 10, 12, 8, null, 20, 12], ARRAY[[1.0, 9, 10],[12, 8, null]]), " +
                    "(ARRAY[], ARRAY[[],[]])," +
                    "(null, null)"
            );
            assertSql("shift\tshift1\tshift2\tshift3\n" +
                            "[999.0,999.0,999.0,1.0,9.0,10.0,12.0,8.0]\t[999.0,9.0,10.0,12.0,8.0,NaN,20.0]\t[999.0,999.0]\t[NaN,NaN,NaN,1.0,9.0,10.0,12.0,8.0]\n" +
                            "[]\t[]\t[]\t[]\n" +
                            "null\tnull\tnull\tnull\n",
                    "SELECT shift(arr1, 3, 999.0), shift(arr1[2:], 1, 999.0), shift(arr1[1:3], 10, 999.0), shift(arr1, 3) FROM tango");
            assertSql("shift\tshift1\tshift2\tshift3\n" +
                            "[12.0,8.0,NaN,20.0,12.0,999.0,999.0,999.0]\t[10.0,12.0,8.0,NaN,20.0,12.0,999.0]\t[999.0,999.0]\t[12.0,8.0,NaN,20.0,12.0,NaN,NaN,NaN]\n" +
                            "[]\t[]\t[]\t[]\n" +
                            "null\tnull\tnull\tnull\n",
                    "SELECT shift(arr1, -3, 999.0), shift(arr1[2:], -1, 999.0), shift(arr1[1:3], -10, 999.0), shift(arr1, -3) FROM tango");
            assertSql("shift\tshift1\tshift2\tshift3\n" +
                            "[[999.0,1.0,9.0],[999.0,12.0,8.0]]\t[[9.0,10.0,999.0],[8.0,NaN,999.0]]\t[[999.0,999.0,999.0],[999.0,999.0,999.0]]\t[[10.0,NaN,NaN],[NaN,NaN,NaN]]\n" +
                            "[]\t[]\t[]\t[]\n" +
                            "null\tnull\tnull\tnull\n",
                    "SELECT shift(arr2, 1, 999.0), shift(arr2[1:], -1, 999.0), shift(arr2, 5, 999.0), shift(arr2, -2) FROM tango");
        });
    }

    @Test
    public void testShiftNonConst() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[], distance INT, filler DOUBLE)");
            execute("INSERT INTO tango VALUES (ARRAY[1.0, 2.0, 3.0], 1, 6.0)");
            assertSql("shift\n[6.0,1.0,2.0]\n", "SELECT shift(arr, distance, filler) FROM tango");
        });
    }

    @Test
    public void testShiftNonVanilla() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[[1.0, 9], [10, 12], [8, null], [20, 12]]), " +
                    "(ARRAY[[]])," +
                    "(null)"
            );
            assertSql("transpose\n" +
                            "[[1.0,10.0,8.0,20.0],[9.0,12.0,NaN,12.0]]\n" +
                            "[]\n" +
                            "null\n",
                    "SELECT transpose(arr) FROM tango");

            assertSql("shift\tshift1\tshift2\tshift3\n" +
                            "[999.0,999.0,1.0,10.0]\t[999.0,10.0,8.0]\t[999.0,999.0]\t[NaN,NaN,NaN,1.0]\n" +
                            "null\tnull\tnull\tnull\n" +
                            "null\tnull\tnull\tnull\n",
                    "SELECT shift(transpose(arr)[1], 2, 999.0), shift(transpose(arr)[1, 2:], 1, 999.0), shift(transpose(arr)[1, 1:3], 10, 999.0), shift(transpose(arr)[1], 3) FROM tango");

            assertSql("shift\tshift1\tshift2\tshift3\n" +
                            "[8.0,20.0,999.0,999.0]\t[8.0,20.0,999.0]\t[999.0,999.0]\t[20.0,NaN,NaN,NaN]\n" +
                            "null\tnull\tnull\tnull\n" +
                            "null\tnull\tnull\tnull\n",
                    "SELECT shift(transpose(arr)[1], -2, 999.0), shift(transpose(arr)[1, 2:], -1, 999.0), shift(transpose(arr)[1, 1:3], -10, 999.0), shift(transpose(arr)[1], -3) FROM tango");
            assertSql("shift\tshift1\tshift2\tshift3\n" +
                            "[[999.0,1.0,10.0,8.0],[999.0,9.0,12.0,NaN]]\t[[10.0,8.0,20.0,999.0],[12.0,NaN,12.0,999.0]]\t[[999.0,999.0,999.0,999.0],[999.0,999.0,999.0,999.0]]\t[[8.0,20.0,NaN,NaN],[NaN,12.0,NaN,NaN]]\n" +
                            "[]\t[]\t[]\t[]\n" +
                            "null\tnull\tnull\tnull\n",
                    "SELECT shift(transpose(arr), 1, 999.0), shift(transpose(arr)[1:], -1, 999.0), shift(transpose(arr), 5, 999.0), shift(transpose(arr), -2) FROM tango");
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

            assertSql("x\nnull\n", "SELECT arr[3] x FROM tango");
            assertSql("x\nnull\n", "SELECT arr[2, 3] x FROM tango");
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
            ArrayTypeDriver.arrayToJson(array, sink, NoopArrayWriteState.INSTANCE, false);
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
                            " rnd_double_array(2,2) arr " +
                            " from long_sequence(10)" +
                            ") timestamp (ts) partition by DAY"
            );

            // Unlike x, y has single-dimension array.
            execute(
                    "create table y as (" +
                            "select" +
                            " timestamp_sequence(0L,100000000L) ts, " +
                            " rnd_double_array(1,2) arr " +
                            " from long_sequence(10)" +
                            ") timestamp (ts) partition by DAY"
            );

            execute("create table z as (x union all y)");

            assertSql(
                    "ts\tarr\n" +
                            "1970-01-06T18:53:20.000000Z\t[[0.12966659791573354,0.299199045961845,NaN,NaN,NaN,NaN,NaN,0.9856290845874263,NaN,0.5093827001617407,0.11427984775756228,0.5243722859289777,NaN,NaN,0.7261136209823622]]\n" +
                            "1970-01-06T18:53:21.000000Z\t[[0.6778564558839208,0.8756771741121929,NaN,NaN,0.33608255572515877,0.690540444367637,NaN,0.21583224269349388,0.15786635599554755],[NaN,NaN,0.12503042190293423,NaN,0.9687423276940171,NaN,NaN,NaN,NaN],[NaN,NaN,NaN,0.7883065830055033,NaN,0.4138164748227684,0.5522494170511608,0.2459345277606021,NaN],[NaN,0.8847591603509142,0.4900510449885239,NaN,0.9075843364017028,NaN,0.18769708157331322,0.16381374773748514,0.6590341607692226],[NaN,NaN,NaN,0.8837421918800907,0.05384400312338511,NaN,0.7230015763133606,0.12105630273556178,NaN],[0.5406709846540508,NaN,0.9269068519549879,NaN,NaN,NaN,0.1202416087573498,NaN,0.6230184956534065],[0.42020442539326086,NaN,NaN,0.4971342426836798,NaN,0.5065228336156442,NaN,NaN,0.03167026265669903],[NaN,NaN,0.2879973939681931,NaN,NaN,NaN,NaN,0.24008362859107102,NaN],[0.9455893004802433,NaN,NaN,0.2185865835029681,NaN,0.24079155981438216,0.10643046345788132,0.5244255672762055,0.0171850098561398],[0.09766834710724581,NaN,0.053594208204197136,0.26369335635512836,0.22895725920713628,0.9820662735672192,NaN,0.32424562653969957,0.8998921791869131],[NaN,NaN,NaN,0.33746104579374825,0.18740488620384377,0.10527282622013212,0.8291193369353376,0.32673950830571696,NaN],[0.18336217509438513,0.9862476361578772,0.8693768930398866,0.8189713915910615,0.5185631921367574,NaN,NaN,NaN,0.29659296554924697]]\n" +
                            "1970-01-06T18:53:22.000000Z\t[[NaN,NaN,NaN,NaN,0.13264292470570205,0.38422543844715473,NaN,NaN,NaN,NaN,0.7668146556860689,NaN,0.05158459929273784,NaN,NaN],[NaN,0.5466900921405317,NaN,0.11128296489732104,0.6707018622395736,0.07594017197103131,NaN,NaN,0.5716129058692643,0.05094182589333662,NaN,NaN,0.4609277382153818,0.5691053034055052,0.12663676991275652],[0.11371841836123953,NaN,NaN,0.7203170014947307,NaN,NaN,NaN,NaN,0.7704949839249925,0.8144207168582307,NaN,NaN,NaN,0.2836347139481469,NaN],[NaN,NaN,NaN,0.24001459007748394,NaN,NaN,NaN,0.741970173888595,0.25353478516307626,0.2739985338660311,NaN,0.8001632261203552,NaN,0.7404912278395417,0.08909442703907178],[0.8439276969435359,NaN,NaN,0.08712007604601191,0.8551850405049611,0.18586435581637295,0.5637742551872849,NaN,NaN,NaN,0.7195457109208119,NaN,0.23493793601747937,NaN,0.6334964081687151],[0.6721404635638454,0.7707249647497968,0.8813290192134411,0.17405556853190263,0.823395724427589,NaN,0.8108032283138068,NaN,NaN,0.7530494527849502,0.49153268154777974,0.0024457698760806945,0.29168465906260244,0.3121271759430503,0.3004874521886858],[NaN,0.7653255982993546,NaN,NaN,NaN,NaN,0.37873228328689634,NaN,0.7272119755925095,NaN,0.7467013668130107,0.5794665369115236,NaN,0.5308756766878475,0.03192108074989719],[NaN,0.17498425722537903,NaN,0.34257201464152764,NaN,NaN,0.29242748475227853,NaN,0.11296257318851766,NaN,0.23405440872043592,0.1479745625593103,NaN,0.8115426881784433,NaN]]\n" +
                            "1970-01-06T18:53:23.000000Z\t[[NaN,NaN,0.04321289940104611,0.8217652538598936,0.6397125243912908,0.29419791719259025,0.865629565918467,NaN,NaN,0.16923843067953104],[0.7198854503668188,0.5174107449677378,0.38509066982448115,NaN,NaN,NaN,0.5475429391562822,0.6977332212252165,NaN,NaN],[0.4268921400209912,0.9997797234031688,0.5234892454427748,NaN,NaN,NaN,NaN,0.5169565007469263,0.7039785408034679,0.8461211697505234],[NaN,0.537020248377422,0.8766908646423737,NaN,NaN,0.31852531484741486,NaN,0.605050319285447,0.9683642405595932,0.3549235578142891],[0.04211401699125483,NaN,NaN,0.0032519916115479885,0.2703179181043681,0.729536610842768,0.3317641556575974,0.8895915828662114,NaN,NaN],[NaN,NaN,0.1599211504269954,0.5251698097331752,NaN,0.18442756220221035,NaN,0.48422587819911567,0.2970515836513553,NaN],[0.7826107801293182,NaN,0.3218450864634881,0.8034049105590781,NaN,NaN,0.40425101135606667,0.9412663583926286,NaN,NaN],[0.8376764297590714,0.15241451173695408,NaN,0.743599174001969,NaN,NaN,0.9001273812517414,0.5629104624260136,0.6001215594928115,0.8920252905736616]]\n" +
                            "1970-01-06T18:53:24.000000Z\t[[0.6741248448728824,0.030997441190531494,NaN,NaN],[0.8853675629694284,4.945923013344178E-5,NaN,0.0016532800623808575],[0.23567419576658333,NaN,0.3489278573518253,NaN],[NaN,0.07383464174908916,0.8791439438812569,0.7110275609764849]]\n" +
                            "1970-01-06T18:53:25.000000Z\t[[0.10820602386069589,NaN,NaN,0.11286092606280262,0.7370823954391381],[NaN,0.533524384058538,0.6749208267946962,NaN,0.3124458010612313],[NaN,NaN,0.7943185767500432,0.4137003695612732,NaN],[NaN,0.32449127848036263,0.41886400558338654,0.8409080254825717,0.06001827721556019],[NaN,NaN,NaN,0.6542559878565383,NaN],[NaN,NaN,NaN,NaN,0.0846754178136283],[NaN,NaN,0.5815065874358148,NaN,0.4039042639581232],[0.4375759068189693,0.8802810667279274,0.6944149053754287,0.27755720049807464,0.8985777419215233],[NaN,0.9815126662068089,0.22252546562577824,NaN,NaN],[0.6240138444047509,0.8869397617459538,NaN,NaN,NaN],[NaN,NaN,0.2266157317795261,0.7430101994511517,0.6993909595959196]]\n" +
                            "1970-01-06T18:53:26.000000Z\t[[0.7617663592833062,NaN,0.021177977444738705,NaN,NaN,NaN,0.8303845449546206,NaN,NaN],[NaN,0.7636347764664544,0.2195743166842714,NaN,NaN,NaN,0.5823910118974169,0.05942010834028011,NaN],[NaN,0.9887681426881507,NaN,0.39211484750712344,NaN,0.29120049877582566,0.7876429805027644,0.16011053107067486,NaN],[0.2712934077694782,NaN,NaN,NaN,NaN,0.11768763788456049,0.06052105248562101,0.18684267640195917,0.6884149023727977],[NaN,NaN,0.47107881346820746,NaN,NaN,NaN,0.8637346569402254,NaN,0.0049253368387782714],[0.8973562700864572,NaN,NaN,0.7020508159399581,0.5862806534829702,NaN,NaN,NaN,NaN],[0.40462097000890584,0.9113999356978634,0.08941116322563869,NaN,0.4758209004780879,0.8203418140538824,0.22122747948030208,0.48731616038337855,0.05579995341081423],[NaN,0.14756353014849555,0.25604136769205754,0.44172683242560085,0.2696094902942793,0.899050586403365,NaN,NaN,0.2753819635358048],[NaN,NaN,NaN,NaN,0.019529452719755813,0.5330584032999529,0.9766284858951397,0.1999576586778039,0.47930730718677406],[0.6936669914583254,NaN,NaN,NaN,0.5940502728139653,NaN,NaN,0.10287867683029772,0.33261541215518553],[0.8756165114231503,NaN,0.307622006691777,0.6376518594972684,0.6453590096721576,NaN,0.37418657418528656,NaN,0.3981872443575455]]\n" +
                            "1970-01-06T18:53:27.000000Z\t[[NaN,NaN,NaN,NaN],[0.27144997281940675,NaN,NaN,0.8642800031609658],[0.837738444021418,NaN,NaN,0.5081836003758192],[NaN,NaN,0.7706329763519386,NaN],[0.7874929839944909,0.6320839159367109,NaN,0.7409092302023607]]\n" +
                            "1970-01-06T18:53:28.000000Z\t[[0.6288088087840823,0.1288393076713259,0.5350165471764692,NaN,0.3257868894353412,NaN,0.6927480038605662,NaN,NaN,0.265774789314454,0.9610592594899304,NaN,0.236380596505666,NaN,NaN,NaN],[NaN,NaN,NaN,0.06970926959068269,NaN,0.18170646835643245,NaN,0.7407581616916364,0.527776712010911,NaN,0.4701492486769596,0.7407568814442186,0.3123904307505546,0.19188599215569557,NaN,0.29916480738910844],[NaN,0.7217315729790722,0.2875739269292986,0.9347912212983339,NaN,NaN,0.991107083990332,0.6699251221933199,0.1402656562190583,NaN,NaN,0.722395777265195,0.770831153825552,0.5265022802557376,NaN,NaN],[NaN,NaN,0.31168586687815647,NaN,0.1779779439502811,0.4338972476284021,NaN,NaN,0.6627303823338926,NaN,NaN,0.8440228885347915,0.4374309393168063,NaN,NaN,NaN]]\n" +
                            "1970-01-06T18:53:29.000000Z\t[[0.3436802159856278,0.28400807705010733,NaN],[0.47486309648420666,NaN,0.8877241452424108],[0.591069738864946,0.6051467286553064,0.13660430775944932],[0.8941438652004624,0.5952054059375091,0.5878334718062131],[NaN,NaN,NaN],[NaN,0.5613174142074612,0.48432558936820347],[0.7729111631116361,0.19245855538083634,0.8245822920507528],[0.8235056484964091,0.8645536237512511,0.8096078909402364]]\n" +
                            "1970-01-01T00:00:00.000000Z\t[[0.2056999100146133,NaN,0.42934437054513563,0.7066431848881077,0.2969423112431254,0.20424854637540668,NaN,0.8967196946317529,NaN,NaN,0.845815880104404,NaN,0.6609657087067649,NaN,0.1761365701299611,NaN]]\n" +
                            "1970-01-01T00:01:40.000000Z\t[[0.9128848579835603,0.8510208445183796]]\n" +
                            "1970-01-01T00:03:20.000000Z\t[[NaN,0.9470551382496946,0.5458550805896514,0.0396096812427591,0.07828852114693607,0.844088760011128,0.1767960157639903,NaN,NaN,0.3074410595329138,0.3711772113004822,NaN,0.687941299680927,0.27289838138048383,0.15115804374180808,NaN]]\n" +
                            "1970-01-01T00:05:00.000000Z\t[[0.8019738363360789,0.9577766506748923,NaN,NaN,NaN,0.3853382179165218,0.008444033230580739]]\n" +
                            "1970-01-01T00:06:40.000000Z\t[[0.9622544279671161,0.05985593677636569]]\n" +
                            "1970-01-01T00:08:20.000000Z\t[[NaN,NaN,0.5945632194415877,NaN,NaN]]\n" +
                            "1970-01-01T00:10:00.000000Z\t[[NaN]]\n" +
                            "1970-01-01T00:11:40.000000Z\t[[NaN,NaN,0.6338382817759625]]\n" +
                            "1970-01-01T00:13:20.000000Z\t[[NaN]]\n" +
                            "1970-01-01T00:15:00.000000Z\t[[NaN,0.4058708191934428,0.20590841547148808,0.4879274348488539,NaN,0.42774600405593644,0.5582188416249704]]\n",
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

    private static void fillIntList(IntList list, int... values) {
        list.clear();
        for (int i : values) {
            list.add(i);
        }
    }
}
