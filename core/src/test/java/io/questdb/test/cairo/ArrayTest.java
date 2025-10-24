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

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
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
            assertSql("x\n2.0\n2.0\nnull\n", "SELECT arr1[2::long] x FROM tango");
            assertSql("x\n2.0\n2.0\nnull\n", "SELECT arr1['2'] x FROM tango");
            assertSql("x\n2.0\n2.0\nnull\n", "SELECT arr1[arr1[2]::long] x FROM tango");
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
            execute("CREATE TABLE tango AS (SELECT " +
                    "ARRAY[[1.0, 2], [3.0, 4]] arr1, " +
                    "ARRAY[0.0, 999_999_999_999] arr2 " +
                    "FROM long_sequence(1))");

            assertExceptionNoLeakCheck("SELECT arr1[] FROM tango",
                    12, "empty brackets");
            assertExceptionNoLeakCheck("SELECT arr1[1, 999_999_999_999] FROM tango",
                    15, "int overflow on array index [dim=2, index=999999999999]");
            assertExceptionNoLeakCheck("SELECT arr1[1, true] FROM tango",
                    15, "invalid type for array access [type=1]");
            assertExceptionNoLeakCheck("SELECT arr1[1, 1, 1] FROM tango",
                    15, "too many array access arguments [nDims=2, nArgs=3]");
            assertExceptionNoLeakCheck("SELECT arr1[0] FROM tango",
                    12, "array index must be non-zero [dim=1, index=0]");
            assertExceptionNoLeakCheck("SELECT arr1[0, 1] FROM tango",
                    12, "array index must be non-zero [dim=1, index=0]");
            assertExceptionNoLeakCheck("SELECT arr1[1:999_999_999_999] FROM tango",
                    13, "there is no matching operator `:` with the argument types: INT : LONG");
            assertExceptionNoLeakCheck("SELECT arr1[999_999_999_999:1] FROM tango",
                    27, "there is no matching operator `:` with the argument types: LONG : INT");
            assertExceptionNoLeakCheck("SELECT arr1[999_999_999_999:999_999_999_999] FROM tango",
                    27, "there is no matching operator `:` with the argument types: LONG : LONG");
            assertExceptionNoLeakCheck("SELECT arr1[1:2, 0] FROM tango",
                    17, "array index must be non-zero [dim=2, index=0]");
            assertExceptionNoLeakCheck("SELECT arr1[1, 0] FROM tango",
                    15, "array index must be non-zero [dim=2, index=0]");
            assertExceptionNoLeakCheck("SELECT arr1[1, 1, 1] FROM tango",
                    15, "too many array access arguments [nDims=2, nArgs=3]");
            assertExceptionNoLeakCheck("SELECT arr1[1][1, 1] FROM tango",
                    18, "too many array access arguments [nDims=2, nArgs=3]");
            assertExceptionNoLeakCheck("SELECT arr1[1][1][1] FROM tango",
                    17, "there is no matching function `[]` with the argument types: (DOUBLE, INT)");
            assertExceptionNoLeakCheck("SELECT arr1[1, arr2[1]::int] FROM tango",
                    22, "array index must be non-zero [dim=2, index=0]");
            assertExceptionNoLeakCheck("SELECT arr1[1:2][arr2[1]::int] FROM tango",
                    24, "array index must be non-zero [dim=1, index=0]");
            assertExceptionNoLeakCheck("SELECT arr1[1, arr2[2]::long] FROM tango",
                    22, "int overflow on array index [dim=2, index=999999999999]");
        });
    }

    @Test
    public void testAccessNegativeIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (n INT, arr DOUBLE[], arr2 DOUBLE[][])");
            execute("INSERT INTO tango VALUES (-2, ARRAY[1.0, 2, 3, 4], ARRAY[[1.0, 2], [3.0, 4]])");
            assertSql("[]\n4.0\n", "SELECT arr[-1] FROM tango");
            assertSql("[]\n3.0\n", "SELECT arr[-2] FROM tango");
            assertSql("[]\n3.0\n", "SELECT arr[n] FROM tango");
            assertSql("[]\n[1.0,2.0,3.0]\n", "SELECT arr[1:-1] FROM tango");
            assertSql("[]\n[3.0,4.0]\n", "SELECT arr[-2:5] FROM tango");
            assertSql("[]\n[1.0,2.0,3.0]\n", "SELECT arr[1:-1] FROM tango");
            assertSql("[]\n[2.0,3.0]\n", "SELECT arr[2:-1] FROM tango");
            assertSql("[]\n[1.0,2.0]\n", "SELECT arr[1:-2] FROM tango");
            assertSql("[]\n[1.0,2.0]\n", "SELECT arr[1:n] FROM tango");
            assertSql("[]\n[3.0,4.0]\n", "SELECT arr[n:5] FROM tango");
            assertSql("[]\n2.0\n", "SELECT arr2[1, -1] FROM tango");
            assertSql("[]\n1.0\n", "SELECT arr2[1, n] FROM tango");
            assertSql("[]\n[3.0]\n", "SELECT arr2[2, n:2] FROM tango");
            assertSql("[]\n[]\n", "SELECT arr2[1, 1:n] FROM tango");
            assertSql("[]\n[]\n", "SELECT arr2[1:2, 1:n] FROM tango");
        });
    }

    @Test
    public void testAccessNullIndex() throws Exception {
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
    public void testAccessOutOfBounds() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT ARRAY[[1.0, 2], [3.0, 4]] arr FROM long_sequence(1))");

            assertSql("x\nnull\n", "SELECT arr[1, 3] x FROM tango");
            assertSql("x\nnull\n", "SELECT arr[3, 1] x FROM tango");
            assertSql("x\nnull\n", "SELECT arr[1, -3] x FROM tango");
            assertSql("x\nnull\n", "SELECT arr[-3, 1] x FROM tango");

            assertSql("x\n[]\n", "SELECT arr[1:1] x FROM tango");
            assertSql("x\n[]\n", "SELECT arr[2:1] x FROM tango");
            assertSql("x\n[[3.0,4.0]]\n", "SELECT arr[2:5] x FROM tango");
            assertSql("x\n[]\n", "SELECT arr[3:3] x FROM tango");
            assertSql("x\n[]\n", "SELECT arr[3:5] x FROM tango");
            assertSql("x\n[]\n", "SELECT arr[3:-5] x FROM tango");
            assertSql("x\n[]\n", "SELECT arr[-1:-2] x FROM tango");

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
            assertException("ALTER TABLE tango ADD COLUMN arr ARRAY[]", 33, "the system supports type-safe arrays, e.g. `type[]`. Supported types are: DOUBLE. More types incoming.");
            assertException("ALTER TABLE tango ADD COLUMN arr BINARY[]", 33, "unsupported array element type [type=BINARY]");
            assertException("ALTER TABLE tango ADD COLUMN arr DATE[]", 33, "unsupported array element type [type=DATE]");
            assertException("ALTER TABLE tango ADD COLUMN arr TIMESTAMP[]", 33, "unsupported array element type [type=TIMESTAMP]");
            assertException("ALTER TABLE tango ADD COLUMN arr UUID[]", 33, "unsupported array element type [type=UUID]");
            assertException("ALTER TABLE tango ADD COLUMN arr LONG128[]", 33, "unsupported array element type [type=LONG128]");
            assertException("ALTER TABLE tango ADD COLUMN arr GEOHASH[]", 33, "unsupported array element type [type=GEOHASH]");
            assertException("ALTER TABLE tango ADD COLUMN arr DECIMAL[]", 33, "unsupported array element type [type=DECIMAL]");
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
                    "[7.0,null]\t[[5.0,7.0],[9.0,11.0]]\t[11.0,16.0]\t[[41.0,51.0]]\n" +
                    "[19.0,22.0]\t[[17.0,19.0]]\t[41.0,46.0]\t[]\n" +
                    "null\tnull\tnull\tnull\n", "SELECT a * 3.0 + 1.0, b * 2.0 + 1.0, b[1] * 5.0 + 1.0, b[2:] * 10.0 + 1.0 FROM tango");
            assertSql("column\tcolumn1\n" +
                    "[5.0,null]\t[[4.0,6.0],[5.0,7.0]]\n" +
                    "[9.0,10.0]\t[[10.0],[11.0]]\n" +
                    "null\tnull\n", "SELECT transpose(a) + 3.0, transpose(b) + 2.0 FROM tango");
            assertSql("column\tcolumn1\tcolumn2\tcolumn3\n" +
                    "[5.0,null]\t[[4.0,5.0],[6.0,7.0]]\t[7.0,8.0]\t[[14.0,15.0]]\n" +
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
                            "null\tnull\tnull\n" +
                            "null\tnull\tnull\n",
                    "SELECT array_cum_sum(arr1), array_cum_sum(arr1[2:]), array_cum_sum(arr1[1:3]) FROM tango");

            assertSql("array_cum_sum\tarray_cum_sum1\tarray_cum_sum2\tarray_cum_sum3\tarray_cum_sum4\n" +
                            "[1.0,10.0,20.0,32.0,40.0,40.0,60.0,72.0]\t[1.0,10.0,20.0,32.0,40.0,40.0,60.0,72.0]\t[1.0,10.0,20.0,32.0,40.0,40.0,60.0,72.0]\t[1.0,10.0,20.0,32.0,40.0,40.0,60.0,72.0]\tnull\n" +
                            "null\tnull\tnull\tnull\tnull\n" +
                            "null\tnull\tnull\tnull\tnull\n",
                    "SELECT array_cum_sum(arr2), array_cum_sum(transpose(arr2)), array_cum_sum(arr2[1]), array_cum_sum(arr2[1:]), array_cum_sum(arr2[2:]) FROM tango");
        });
    }

    @Test
    public void testArrayCumSumBehaviourMixedNulls() throws Exception {
        assertMemoryLeak(() -> assertSqlWithTypes("a\tarray_cum_sum\n" +
                        "[null,1.2,null,5.3]:DOUBLE[]\t[null,1.2,1.2,6.5]:DOUBLE[]\n",
                "select array[null, 1.2, null, 5.3] as a, array_cum_sum(a);\n")
        );
    }

    @Test
    public void testArrayCumSumKahan() throws Exception {
        assertSqlWithTypes("array_cum_sum\n" +
                        "[10000.0,10003.14159,10005.85987]:DOUBLE[]\n",
                "SELECT array_cum_sum(array[10000d, 3.14159, 2.71828]);");
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
                    "[12.0,null]\t[[8.0,12.0],[16.0,20.0]]\t[20.0,30.0]\t[[80.0,100.0]]\n" +
                    "[36.0,42.0]\t[[32.0,36.0]]\t[80.0,90.0]\t[]\n" +
                    "null\tnull\tnull\tnull\n", "SELECT a * 3.0/0.5, b * 2.0/0.5, b[1] * 5.0 / 0.5, b[2:] * 10.0 / 0.5 FROM tango");
            assertSql("column\tcolumn1\n" +
                    "[4.0,null]\t[[4.0,8.0],[6.0,10.0]]\n" +
                    "[12.0,14.0]\t[[16.0],[18.0]]\n" +
                    "null\tnull\n", "SELECT transpose(a)/0.5, transpose(b)/0.5 FROM tango");
            assertSql("column\tcolumn1\n" +
                    "[null,null]\t[null,null]\n" +
                    "[null,null]\t[null,null]\n" +
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
                    24, "arrays have different number of dimensions [dimsLeft=1, dimsRight=2]");
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
                            "2025-06-26T00:00:00.000000Z\tnull\t10.0\n" +
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
                    "[6.0,null]\t[[4.0,6.0],[8.0,10.0]]\t[10.0,15.0]\t[[40.0,50.0]]\n" +
                    "[18.0,21.0]\t[[16.0,18.0]]\t[40.0,45.0]\t[]\n" +
                    "null\tnull\tnull\tnull\n", "SELECT a * 3.0, b * 2.0, b[1] * 5.0, b[2:] * 10.0 FROM tango");
            assertSql("column\tcolumn1\n" +
                    "[6.0,null]\t[[4.0,8.0],[6.0,10.0]]\n" +
                    "[18.0,21.0]\t[[16.0],[18.0]]\n" +
                    "null\tnull\n", "SELECT transpose(a) * 3.0, transpose(b) * 2.0 FROM tango");
            assertSql("column\tcolumn1\tcolumn2\tcolumn3\n" +
                    "[6.0,null]\t[[4.0,6.0],[8.0,10.0]]\t[10.0,15.0]\t[[40.0,50.0]]\n" +
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
    public void testArrayPositionNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[])");
            execute("INSERT INTO tango VALUES (ARRAY[1.0/0.0, 0.0/0.0, -1.0/0.0])");
            assertSql("array_position\n1\n", "SELECT array_position(arr, 0.0/0.0) FROM tango");
            assertSql("array_position\n1\n", "SELECT array_position(arr, 1.0/0.0) FROM tango");
            assertSql("array_position\n1\n", "SELECT array_position(arr, -1.0/0.0) FROM tango");
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
                    "[5.0,null]\t[[3.0,5.0],[7.0,9.0]]\t[9.0,14.0]\t[[39.0,49.0]]\n" +
                    "[17.0,20.0]\t[[15.0,17.0]]\t[39.0,44.0]\t[]\n" +
                    "null\tnull\tnull\tnull\n", "SELECT a * 3.0 - 1.0, b * 2.0 - 1.0, b[1] * 5.0 - 1.0, b[2:] * 10.0 - 1.0 FROM tango");
            assertSql("column\tcolumn1\n" +
                    "[-1.0,null]\t[[0.0,2.0],[1.0,3.0]]\n" +
                    "[3.0,4.0]\t[[6.0],[7.0]]\n" +
                    "null\tnull\n", "SELECT transpose(a) - 3.0, transpose(b) - 2.0 FROM tango");
            assertSql("column\n" +
                    "[null,null]\n" +
                    "[null,null]\n" +
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
                            "null\tnull\tnull\n" +
                            "null\tnull\tnull\n",
                    "SELECT array_sum(arr1), array_sum(arr1[2:]), array_sum(arr1[1:3]) FROM tango");

            assertSql("array_sum\tarray_sum1\tarray_sum2\tarray_sum3\tarray_sum4\n" +
                            "72.0\t72.0\t72.0\t72.0\tnull\n" +
                            "null\tnull\tnull\tnull\tnull\n" +
                            "null\tnull\tnull\tnull\tnull\n",
                    "SELECT array_sum(arr2), array_sum(transpose(arr2)), array_sum(arr2[1]), array_sum(arr2[1:]), array_sum(arr2[2:]) FROM tango");
        });
    }

    @Test
    public void testArraySumAndCumSumNullBehaviour() throws Exception {
        assertMemoryLeak(() -> {
            assertSqlWithTypes("i\tarray_sum\n" +
                            "[null,null]:DOUBLE[]\tnull:DOUBLE\n" +
                            "[null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]:DOUBLE[]\tnull:DOUBLE\n" +
                            "[null,null,null,null,null,null,null]:DOUBLE[]\tnull:DOUBLE\n" +
                            "[null,null,null,null,null,null,null]:DOUBLE[]\tnull:DOUBLE\n" +
                            "[null,null,null]:DOUBLE[]\tnull:DOUBLE\n" +
                            "[null,null,null,null]:DOUBLE[]\tnull:DOUBLE\n" +
                            "[null,null,null,null,null,null,null,null,null,null,null,null]:DOUBLE[]\tnull:DOUBLE\n" +
                            "[null,null]:DOUBLE[]\tnull:DOUBLE\n" +
                            "[null,null,null]:DOUBLE[]\tnull:DOUBLE\n" +
                            "[null,null,null,null,null,null,null]:DOUBLE[]\tnull:DOUBLE\n",
                    "select rnd_double_array(1,1) i, array_sum(i) from long_sequence(10);\n");
            assertSqlWithTypes("i\tarray_cum_sum\n" +
                            "[null,null]:DOUBLE[]\tnull:DOUBLE[]\n" +
                            "[null,null,null,null,null,null]:DOUBLE[]\tnull:DOUBLE[]\n" +
                            "[null,null]:DOUBLE[]\tnull:DOUBLE[]\n" +
                            "[null,null]:DOUBLE[]\tnull:DOUBLE[]\n" +
                            "[null,null,null]:DOUBLE[]\tnull:DOUBLE[]\n" +
                            "[null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]:DOUBLE[]\tnull:DOUBLE[]\n" +
                            "[null,null]:DOUBLE[]\tnull:DOUBLE[]\n" +
                            "[null,null,null,null,null,null,null,null,null,null,null,null,null,null]:DOUBLE[]\tnull:DOUBLE[]\n" +
                            "[null,null]:DOUBLE[]\tnull:DOUBLE[]\n" +
                            "[null,null,null,null,null]:DOUBLE[]\tnull:DOUBLE[]\n",
                    "select rnd_double_array(1,1) i, array_cum_sum(i) from long_sequence(10);\n");
        });
    }

    @Test
    public void testArraySumKahan() throws Exception {
        assertSqlWithTypes("array_sum\n" +
                        "10005.85987:DOUBLE\n",
                "SELECT array_sum(array[10000d, 3.14159, 2.71828]);");
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
                            "[[0.0,1.0,2.0],[10.0,11.0,12.0],[20.0,21.0,22.0],[30.0,31.0,32.0]]\t[[0.0,-1.0,-2.0],[10.0,9.0,8.0],[20.0,19.0,18.0],[30.0,29.0,28.0]]\t[[0.0,0.0,0.0],[0.0,10.0,20.0],[0.0,20.0,40.0],[0.0,30.0,60.0]]\t[[null,0.0,0.0],[null,10.0,5.0],[null,20.0,10.0],[null,30.0,15.0]]\n",
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
            assertException("ALTER TABLE tango ALTER COLUMN n TYPE ARRAY[]", 38, "the system supports type-safe arrays, e.g. `type[]`. Supported types are: DOUBLE. More types incoming.");
            assertException("ALTER TABLE tango ALTER COLUMN n TYPE BINARY[]", 38, "unsupported array element type [type=BINARY]");
            assertException("ALTER TABLE tango ALTER COLUMN n TYPE DATE[]", 38, "unsupported array element type [type=DATE]");
            assertException("ALTER TABLE tango ALTER COLUMN n TYPE TIMESTAMP[]", 38, "unsupported array element type [type=TIMESTAMP]");
            assertException("ALTER TABLE tango ALTER COLUMN n TYPE UUID[]", 38, "unsupported array element type [type=UUID]");
            assertException("ALTER TABLE tango ALTER COLUMN n TYPE LONG128[]", 38, "unsupported array element type [type=LONG128]");
            assertException("ALTER TABLE tango ALTER COLUMN n TYPE GEOHASH[]", 38, "unsupported array element type [type=GEOHASH]");
            assertException("ALTER TABLE tango ALTER COLUMN n TYPE DECIMAL[]", 38, "unsupported array element type [type=DECIMAL]");
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
                            "&\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t[0.6778564558839208,0.3100545983862456,0.38539947865244994,0.8799634725391621]\t-2119387831\n" +
                            "衞͛Ԉ龘\t[0.6761934857077543,0.8912587536603974]\t458818940\n" +
                            "o#/ZUA\t[0.7763904674818695,0.05048190020054388,0.8847591603509142,0.0011075361080621349,0.931192737286751,0.8258367614088108,0.8001121139739173,0.38642336707855873,0.92050039469858,0.16381374773748514,0.456344569609078]\t1857212401\n" +
                            "\uDB8D\uDE4Eᯤ\\篸{\uD9D7\uDFE5\uDAE9\uDF46OF\t[0.9441658975532605,0.6806873134626418]\t-68027832\n" +
                            "?hhV4|\t[0.3901731258748704,0.03993124821273464,0.10643046345788132]\t1238491107\n" +
                            "7=\"+z\t[0.9759534636690222,0.5893398488053903]\t-246923735\n" +
                            "p-鳓w\t[0.8593131480724349,0.021189232728939578,0.10527282622013212]\t-1613687261\n" +
                            "qRӽ\t[0.6797562990945702,0.8189713915910615,0.10459352312331183,0.7365115215570027,0.20585069039325443,0.9418719455092096]\t-623471113\n" +
                            "E\"+~M/8KS\t[0.17180291960857297,0.4416432347777828,0.2065823085842221,0.8584308438045006,0.2445295612285482]\t-1465751763\n" +
                            "Gk珣zx6쪎\t[0.5780746276543334,0.40791879008699594,0.12663676991275652,0.21485589614090927]\t-365989785\n",
                    "select * from blah",
                    true
            );
        });
    }

    @Test
    public void testCreateTableAllSupportedTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (" +
                    "d double[][][], " +
                    "c double)"
            );

            String[] expectedColumnNames = {
                    "d",
                    "c",
            };

            String[] expectedColumnTypes = {
                    "DOUBLE[][][]",
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
            assertException("CREATE TABLE tango (arr ARRAY[])", 24, "the system supports type-safe arrays, e.g. `type[]`. Supported types are: DOUBLE. More types incoming.");
            assertException("CREATE TABLE tango (arr BINARY[])", 24, "unsupported array element type [type=BINARY]");
            assertException("CREATE TABLE tango (arr DATE[])", 24, "unsupported array element type [type=DATE]");
            assertException("CREATE TABLE tango (arr TIMESTAMP[])", 24, "unsupported array element type [type=TIMESTAMP]");
            assertException("CREATE TABLE tango (arr UUID[])", 24, "unsupported array element type [type=UUID]");
            assertException("CREATE TABLE tango (arr LONG128[])", 24, "unsupported array element type [type=LONG128]");
            assertException("CREATE TABLE tango (arr GEOHASH[])", 24, "unsupported array element type [type=GEOHASH]");
            assertException("CREATE TABLE tango (arr DECIMAL[])", 24, "unsupported array element type [type=DECIMAL]");
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
                    "[0.5,null]\n" +
                    "[0.75,null]\n" +
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
    public void testEmptyArray() throws Exception {
        assertMemoryLeak(() -> {
            assertSql("ARRAY\n[]\n", "SELECT ARRAY[]");
            assertSql("ARRAY\n[]\n", "SELECT * FROM (SELECT ARRAY[])");
            assertSql("ARRAY\n[]\n", "WITH q1 AS (SELECT ARRAY[]) SELECT * FROM q1");
            execute("CREATE TABLE tango AS (SELECT ARRAY[])");
            assertSql("ARRAY\n[]\n", "tango");
        });
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
                            "[[null,0.20447441837877756],[null,null]]\n" +
                            "[[0.3491070363730514,0.7611029514995744],[0.4217768841969397,null],[0.7261136209823622,0.4224356661645131],[null,0.3100545983862456],[0.1985581797355932,0.33608255572515877],[0.690540444367637,null],[0.21583224269349388,0.15786635599554755],[null,null],[0.12503042190293423,null],[0.9687423276940171,null],[null,null],[null,null],[null,null],[0.7883065830055033,null],[0.4138164748227684,0.5522494170511608],[0.2459345277606021,null]]\n" +
                            "[[0.7643643144642823,null],[null,null],[0.18769708157331322,0.16381374773748514],[0.6590341607692226,null],[null,null],[0.8837421918800907,0.05384400312338511],[null,0.7230015763133606],[0.12105630273556178,null],[0.5406709846540508,null],[0.9269068519549879,null],[null,null],[0.1202416087573498,null]]\n" +
                            "[[null,null,0.4971342426836798,null],[0.5065228336156442,null,null,0.03167026265669903],[null,null,0.2879973939681931,null],[null,null,null,0.24008362859107102]]\n" +
                            "[[0.2185865835029681,null],[0.24079155981438216,0.10643046345788132],[0.5244255672762055,0.0171850098561398],[0.09766834710724581,null],[0.053594208204197136,0.26369335635512836],[0.22895725920713628,0.9820662735672192],[null,0.32424562653969957],[0.8998921791869131,null],[null,null],[0.33746104579374825,0.18740488620384377],[0.10527282622013212,0.8291193369353376],[0.32673950830571696,null],[0.18336217509438513,0.9862476361578772],[0.8693768930398866,0.8189713915910615]]\n" +
                            "[[0.29659296554924697,0.24642266252221556],[null,null],[null,0.13264292470570205],[0.38422543844715473,null],[null,null],[null,0.7668146556860689],[null,0.05158459929273784],[null,null]]\n" +
                            "[[0.3568111021227658,0.05758228485190853,0.6729405590773638,null,0.5716129058692643],[0.05094182589333662,null,null,0.4609277382153818,0.5691053034055052],[0.12663676991275652,0.11371841836123953,null,null,0.7203170014947307],[null,null,null,null,0.7704949839249925],[0.8144207168582307,null,null,null,0.2836347139481469]]\n" +
                            "[[0.08675950660182763,null],[0.741970173888595,0.25353478516307626],[0.2739985338660311,null],[0.8001632261203552,null],[0.7404912278395417,0.08909442703907178],[0.8439276969435359,null],[null,0.08712007604601191]]\n" +
                            "[[0.5637742551872849,null],[null,null],[0.7195457109208119,null],[0.23493793601747937,null],[0.6334964081687151,0.6721404635638454]]\n" +
                            "[[0.17405556853190263,0.823395724427589,null,0.8108032283138068,null,null,0.7530494527849502,0.49153268154777974,0.0024457698760806945,0.29168465906260244,0.3121271759430503,0.3004874521886858],[null,0.7653255982993546,null,null,null,null,0.37873228328689634,null,0.7272119755925095,null,0.7467013668130107,0.5794665369115236],[null,0.5308756766878475,0.03192108074989719,null,0.17498425722537903,null,0.34257201464152764,null,null,0.29242748475227853,null,0.11296257318851766],[null,0.23405440872043592,0.1479745625593103,null,0.8115426881784433,null,0.32093405888189597,null,0.04321289940104611,0.8217652538598936,0.6397125243912908,0.29419791719259025],[0.865629565918467,null,null,0.16923843067953104,0.7198854503668188,0.5174107449677378,0.38509066982448115,null,null,null,0.5475429391562822,0.6977332212252165],[null,null,0.4268921400209912,0.9997797234031688,0.5234892454427748,null,null,null,null,0.5169565007469263,0.7039785408034679,0.8461211697505234],[null,0.537020248377422,0.8766908646423737,null,null,0.31852531484741486,null,0.605050319285447,0.9683642405595932,0.3549235578142891,0.04211401699125483,null],[null,0.0032519916115479885,0.2703179181043681,0.729536610842768,0.3317641556575974,0.8895915828662114,null,null,null,null,0.1599211504269954,0.5251698097331752],[null,0.18442756220221035,null,0.48422587819911567,0.2970515836513553,null,0.7826107801293182,null,0.3218450864634881,0.8034049105590781,null,null],[0.40425101135606667,0.9412663583926286,null,null,0.8376764297590714,0.15241451173695408,null,0.743599174001969,null,null,0.9001273812517414,0.5629104624260136],[0.6001215594928115,0.8920252905736616,0.09977691656157406,null,0.2862717364877081,null,null,null,0.8853675629694284,4.945923013344178E-5,null,0.0016532800623808575]]\n",
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
                    23, "array dimension out of bounds [dim=0]");
            assertExceptionNoLeakCheck("SELECT dim_length(arr, 3) len FROM tango",
                    23, "array dimension out of bounds [dim=3, dims=2]");
            assertExceptionNoLeakCheck("SELECT dim_length(arr, arr[2, 1]::int) len FROM tango",
                    32, "array dimension out of bounds [dim=3, dims=2]");
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
                    14, "left array is not one or two-dimensional");
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
    public void testNegArrayValue() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE[], b DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[2.0, null], ARRAY[[2.0, 4], [4.0, 8]]), " +
                    "(ARRAY[16.0, 0], ARRAY[[8.0, 4]])," +
                    "(null, null)");

            assertSql("column\tcolumn1\tcolumn2\tcolumn3\n" +
                    "[6.0,null]\t[[4.0,0.0],[0.0,-8.0]]\t[0.0,-2.0]\t[[-2.0,-6.0]]\n" +
                    "[-8.0,8.0]\t[[-8.0,0.0]]\t[-6.0,-2.0]\t[]\n" +
                    "null\tnull\tnull\tnull\n", "SELECT - a + 8, (- b + 4.0) * 2.0, - b[1] + 2.0, - b[2:] + 2.0 FROM tango");
            assertSql("column\tcolumn1\n" +
                    "[14.0,null]\t[[14.0,12.0],[12.0,8.0]]\n" +
                    "[0.0,16.0]\t[[8.0],[12.0]]\n" +
                    "null\tnull\n", "SELECT - transpose(a) + 16.0, - transpose(b) + 16.0 FROM tango");
            assertSql("column\tcolumn1\n" +
                    "[-2.0,null]\t[null,null]\n" +
                    "[-16.0,0.0]\t[null,null]\n" +
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
    public void testNullArrayComparisons() throws Exception {
        assertMemoryLeak(() -> {
            assertSql("eq\ntrue\n", "SELECT (null::double[] = null::double[]) eq FROM long_sequence(1)");
            assertSql("eq\ntrue\n", "SELECT (null::double[] = NaN::double[]) eq FROM long_sequence(1)");
            assertSql("eq\ntrue\n", "SELECT (NaN::double[] = null::double[]) eq FROM long_sequence(1)");
            assertSql("eq\ntrue\n", "SELECT (NaN::double[] = NaN::double[]) eq FROM long_sequence(1)");

            assertSql("eq\nfalse\n", "SELECT (null::double[] = ARRAY[1.0, 2.0]) eq FROM long_sequence(1)");
            assertSql("eq\nfalse\n", "SELECT (ARRAY[1.0, 2.0] = null::double[]) eq FROM long_sequence(1)");
            assertSql("eq\nfalse\n", "SELECT (NaN::double[] = ARRAY[1.0, 2.0]) eq FROM long_sequence(1)");
            assertSql("eq\nfalse\n", "SELECT (ARRAY[1.0, 2.0] = NaN::double[]) eq FROM long_sequence(1)");

            assertSql("eq\ntrue\n", "SELECT (ARRAY[null::double, null::double] = ARRAY[null::double, null::double]) eq FROM long_sequence(1)");
            assertSql("eq\ntrue\n", "SELECT (ARRAY[NaN, NaN] = ARRAY[NaN, NaN]) eq FROM long_sequence(1)");
            assertSql("eq\ntrue\n", "SELECT (ARRAY[null::double, NaN] = ARRAY[null::double, NaN]) eq FROM long_sequence(1)");
            assertSql("eq\ntrue\n", "SELECT (ARRAY[NaN, null::double] = ARRAY[NaN, null::double]) eq FROM long_sequence(1)");

            assertSql("eq\nfalse\n", "SELECT (ARRAY[1.0, 2.0] = ARRAY[NaN, 2.0]) eq FROM long_sequence(1)");
            assertSql("eq\nfalse\n", "SELECT (ARRAY[1.0, null::double] = ARRAY[1.0, 2.0]) eq FROM long_sequence(1)");
        });
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
    public void testParquet() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts timestamp, i int, arr double[]) timestamp(ts) partition by DAY");
            execute("INSERT INTO tango VALUES ('2001-01', 1, '{1.0, 2.0}')");
            execute("INSERT INTO tango VALUES ('2001-02', 1, '{1.0, 2.0}')");

            final String expected = "ts\ti\tarr\n" +
                    "2001-01-01T00:00:00.000000Z\t1\t[1.0,2.0]\n" +
                    "2001-02-01T00:00:00.000000Z\t1\t[1.0,2.0]\n";

            execute("ALTER TABLE tango CONVERT PARTITION TO PARQUET where ts in '2001-01';");
            assertQuery(
                    expected,
                    "tango",
                    "ts",
                    true,
                    true
            );

            execute("ALTER TABLE tango CONVERT PARTITION TO NATIVE where ts in '2001-01';");
            assertQuery(
                    expected,
                    "tango",
                    "ts",
                    true,
                    true
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

            assertQuery(
                    "a1\tb1\ta2\tb2\n" +
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
            assertSql("rnd_double_array\n[0.08486964232560668,0.299199045961845]\n",
                    "SELECT rnd_double_array(1)");
            assertSql("rnd_double_array\n[null]\n",
                    "SELECT rnd_double_array('1', '1', '1', '1')");
            assertSql("rnd_double_array\n[null]\n",
                    "SELECT rnd_double_array(1::byte, 1::byte, 0::byte, 1::byte)");
            assertSql("rnd_double_array\n[null]\n",
                    "SELECT rnd_double_array(1::short, 1::short, 0::short, 1::short)");
            assertSql("rnd_double_array\n[null]\n",
                    "SELECT rnd_double_array(1::int, 1::int, 0::int, 1::int)");
            assertSql("rnd_double_array\n[null]\n",
                    "SELECT rnd_double_array(1::long, 1::long, 0::long, 1::long)");
        });
    }

    @Test
    public void testRndDoubleFunctionEdgeCases() throws Exception {
        assertMemoryLeak(() -> {
            assertExceptionNoLeakCheck(
                    "SELECT rnd_double_array()",
                    7,
                    "`rnd_double_array` requires arguments: rnd_double_array(LONG constant, VARARG constant)"
            );
            assertExceptionNoLeakCheck(
                    "SELECT rnd_double_array(true)",
                    7,
                    "wrong number of arguments for function `rnd_double_array`; expected: 2, provided: 1"
            );

            assertExceptionNoLeakCheck(
                    "SELECT rnd_double_array(1, true)",
                    27,
                    "nanRate must be an integer"
            );
            assertExceptionNoLeakCheck(
                    "SELECT rnd_double_array(1, 1, true)",
                    30,
                    "maxDimLength must be an integer"
            );
            assertExceptionNoLeakCheck(
                    "SELECT rnd_double_array(1, 1, 0, true)",
                    33,
                    "dimLength must be an integer"
            );


            assertExceptionNoLeakCheck(
                    "select rnd_double_array(10, 0, 1000000)",
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
                    "rnd_double_array\n" +
                            "[[null,0.9856290845874263],[null,0.5093827001617407]]\n",
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
                    "[4.0,null]\t[[4.0,2.0],[2.0,1.0]]\t[0.5,0.25]\t[[5.0,2.5]]\n" +
                    "[0.5,null]\t[[1.0,2.0]]\t[0.125,0.25]\t[]\n" +
                    "null\tnull\tnull\tnull\n", "SELECT 8.0 / a, 4.0 / b * 2.0, 2.0 / b[1] * 0.5, 2 / b[2:] * 10 FROM tango");
            assertSql("column\tcolumn1\n" +
                    "[8.0,null]\t[[8.0,4.0],[4.0,2.0]]\n" +
                    "[1.0,null]\t[[2.0],[4.0]]\n" +
                    "null\tnull\n", "SELECT 16.0 / transpose(a), 16.0 / transpose(b)FROM tango");
            assertSql("column\tcolumn1\n" +
                    "[0.0,null]\t[null,null]\n" +
                    "[0.0,null]\t[null,null]\n" +
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
                    "[6.0,null]\t[[4.0,0.0],[0.0,-8.0]]\t[0.0,-2.0]\t[[-2.0,-6.0]]\n" +
                    "[-8.0,8.0]\t[[-8.0,0.0]]\t[-6.0,-2.0]\t[]\n" +
                    "null\tnull\tnull\tnull\n", "SELECT 8.0 - a, (4.0 - b)* 2.0, 2.0 - b[1], 2 - b[2:] FROM tango");
            assertSql("column\tcolumn1\n" +
                    "[14.0,null]\t[[14.0,12.0],[12.0,8.0]]\n" +
                    "[0.0,16.0]\t[[8.0],[12.0]]\n" +
                    "null\tnull\n", "SELECT 16.0 - transpose(a), 16.0 - transpose(b)FROM tango");
            assertSql("column\tcolumn1\n" +
                    "[-2.0,null]\t[null,null]\n" +
                    "[-16.0,0.0]\t[null,null]\n" +
                    "null\tnull\n", "SELECT 0.0 - a, null::double - a FROM tango");
        });
    }

    @Test
    public void testSelectDistinct() throws Exception {
        execute(
                "CREATE TABLE 'market_data' ( \n" +
                        "\ttimestamp TIMESTAMP,\n" +
                        "\tsymbol SYMBOL CAPACITY 16384 CACHE,\n" +
                        "\tbids DOUBLE[][],\n" +
                        "\tasks DOUBLE[][]\n" +
                        ") timestamp(timestamp) PARTITION BY HOUR WAL;"
        );

        execute("insert into market_data select timestamp_sequence('2025-05-03', 10000), rnd_symbol('GBPUSD', 'GBPAUD'), rnd_double_array(2), rnd_double_array(2) from long_sequence(10)");

        drainWalQueue();

        assertQuery(
                "[]\n" +
                        "0.6217326707853098\n" +
                        "0.43117716480568924\n" +
                        "0.0396096812427591\n" +
                        "0.9344604857394011\n" +
                        "0.04173263630897883\n" +
                        "0.6583311519893554\n" +
                        "0.23405440872043592\n" +
                        "0.2199453379647608\n" +
                        "0.8615841627702753\n" +
                        "0.8796413468565342\n",
                "select distinct bids[1][2] from market_data",
                true
        );

        assertQuery(
                "bids\n" +
                        "[[0.0843832076262595,0.9344604857394011,0.13123360041292131],[0.7905675319675964,0.19202208853547864,0.8899286912289663]]\n" +
                        "[[0.9771103146051203,0.6217326707853098,0.15786635599554755,0.6381607531178513,0.4022810626779558,0.5793466326862211,0.9038068796506872,0.12026122412833129,0.6761934857077543],[0.8912587536603974,0.3435685332942956,0.42281342727402726,0.26922103479744897,0.7664256753596138,0.5298405941762054,0.5522494170511608,0.8445258177211064,0.7763904674818695],[0.05048190020054388,0.8847591603509142,0.0011075361080621349,0.931192737286751,0.8258367614088108,0.8001121139739173,0.38642336707855873,0.92050039469858,0.16381374773748514]]\n" +
                        "[[0.8998921791869131,0.6583311519893554,0.30716667810043663,0.33746104579374825,0.8593131480724349,0.021189232728939578,0.10527282622013212],[0.11785316212653119,0.8221637568563206,0.32673950830571696,0.2825582712777682,0.18336217509438513,0.6455967424250787,0.48524046868499715],[0.8693768930398866,0.029080850168636263,0.7381752894013154,0.5185631921367574,0.5346019596733254,0.9859070322196475,0.29659296554924697],[0.6341292894843615,0.9457212646911386,0.2672120489216767,0.5025890936351257,0.9946372046359034,0.38422543844715473,0.48964139862697853],[0.5391626621794673,0.17180291960857297,0.4416432347777828,0.2065823085842221,0.8584308438045006,0.2445295612285482,0.6590829275055244]]\n" +
                        "[[0.7704949839249925,0.04173263630897883,0.1264215196329228,0.14261321308606745,0.2677326840703891,0.4440250924606578],[0.23507754029460548,0.09618589590900506,0.24001459007748394,0.08675950660182763,0.868788610834602,0.741970173888595],[0.6107894368996438,0.4167781163798937,0.2739985338660311,0.05514933756198426,0.8001632261203552,0.9359814814085834],[0.7404912278395417,0.2093569947644236,0.7873229912811514,0.8439276969435359,0.7079450575401371,0.03973283003449557],[0.33504146853216143,0.8551850405049611,0.8321000514308267,0.7769285766561033,0.5637742551872849,0.6226001464598434],[0.6213434403332111,0.7195457109208119,0.8786111112537701,0.23493793601747937,0.6001225339624721,0.6334964081687151],[0.18158967304439033,0.95820305972778,0.7707249647497968,0.9130151105125102,0.28964821678040487,0.17405556853190263],[0.4729022357373792,0.6887925530449002,0.007985454958725269,0.5796722100538578,0.9691503953677446,0.7530494527849502]]\n" +
                        "[[0.9934423708117267,0.23405440872043592],[0.848083900630095,0.794252253619802],[0.9058900298408074,0.8911615631017953],[0.4249052453180263,0.11047315214793696]]\n" +
                        "[[0.21047933106727745,0.8796413468565342,0.04404000858917945],[0.40425101135606667,0.41496612044075665,0.03314618075579956],[0.36078878996232167,0.8376764297590714,0.2325041018786207],[0.7397816490927717,0.10799057399629297,0.8386104714017393],[0.8353079103853974,0.9001273812517414,0.11048000399634927]]\n" +
                        "[[0.4913342104187668,0.8615841627702753,0.3189857960358504],[0.4375759068189693,0.07425696969451101,0.38881940598288367],[0.6944149053754287,0.5976614546411813,0.42044603754797416],[0.8985777419215233,0.5261234649527643,0.9815126662068089],[0.9246085617322545,0.20921704056371593,0.25470289113531375],[0.6240138444047509,0.11134244333117826,0.8472016167803409],[0.5863937813368164,0.2362963290561556,0.62456679681861],[0.3242526975448907,0.7430101994511517,0.6519511297254407],[0.7903520704337446,0.4755193286163272,0.7617663592833062],[0.8148792629172324,0.021177977444738705,0.9926343068414145],[0.1339704489137793,0.8303845449546206,0.4523282839107191],[0.04558283749364911,0.7636347764664544,0.5394562515552983],[0.9562577128401444,0.0966240354078981,0.5675831821917149],[0.21224614178286005,0.05942010834028011,0.7259967771911617]]\n" +
                        "[[0.6936669914583254,0.43117716480568924,0.9578716688144072,0.5940502728139653,0.17914853671380093,0.30878646825073175,0.1319044042993568,0.33261541215518553,0.5079751443209725,0.3812506482325819,0.2703044758382739,0.4104855595304533],[0.6376518594972684,0.7587860024773928,0.011263511839942453,0.32613652012030125,0.9176263114713273,0.8457748651234394,0.6281252905002019,0.6504194217741501,0.2824076895992761,0.8054745482045382,0.27144997281940675,0.7573042043889733],[0.6931441108030082,0.1900488162112337,0.837738444021418,0.02633639777833019,0.8658616916564643,0.12465120312903266,0.36986619304630497,0.7706329763519386,0.10424082472921137,0.7874929839944909,0.9266929571641075,0.551184165451474],[0.7751886508004251,0.7659949103776245,0.7417434132166958,0.6288088087840823,0.9379038084870472,0.5763691784056397,0.5350165471764692,0.4613501223216129,0.3257868894353412,0.43619251546485505,0.6927480038605662,0.6051204746298999],[0.9410396704938232,0.19073234832401043,0.9610592594899304,0.4246651384043666,0.236380596505666,0.34085516645580494,0.08533575092925538,0.7564214859398338,0.8718394349472115,0.8925004728084927,0.45388767393986074,0.6728521416263535],[0.8413721135371649,0.7298540433653912,0.527776712010911,0.981074259037815,0.4701492486769596,0.4573258867972624,0.8139041928326346,0.3123904307505546,0.761296424148768,0.9510816389659975,0.43990342764801993,0.3726618828334195]]\n" +
                        "[[0.48432558936820347,0.2199453379647608],[0.12934061164115174,0.19245855538083634],[0.40354999638471434,0.8940422626709261],[0.8235056484964091,0.7536836395346167],[0.5570298738371094,0.8096078909402364],[0.819524120126593,0.2056999100146133],[0.2017974971999763,0.42934437054513563],[0.16638148883943538,0.3911828760735948],[0.2969423112431254,0.9940353811420282]]\n" +
                        "[[0.3669999679163578,0.0396096812427591,0.7234181773407536,0.7184108604451028],[0.844088760011128,0.4911664452671409,0.7776474810620265,0.17857143325827407],[0.3074410595329138,0.4151433463570058,0.9930633230891175,0.32317345869453706],[0.12027057950578746,0.27289838138048383,0.440645592676968,0.5999750613543716],[0.7436419445622273,0.8019738363360789,0.49950663682485574,0.9925168599192057],[0.6600337469738781,0.9251043257912728,0.060162223725059416,0.008444033230580739],[0.7573509485315202,0.9622544279671161,0.04229155272030727,0.9207177299535534],[0.11500943478849246,0.6013488560104849,0.23290767295012593,0.288531163175191],[0.9582280075093402,0.0983023224719013,0.22243351688740076,0.04969674919093636],[0.11493568065083815,0.4287405981191371,0.4058708191934428,0.22148534962398414],[0.8503316000896455,0.4879274348488539,0.45039214871547917,0.42774600405593644],[0.7593868458005614,0.32484768130656416,0.9520909221021127,0.2562499563674363],[0.6385649970707139,0.3622980885814183,0.2524690658195553,0.8825940193001498],[0.2917796053045747,0.9809851788419132,0.05339947229164044,0.7771965216814184]]\n",
                "select distinct \n" +
                        "bids  from market_data;",
                true
        );

        assertQuery(
                "ARRAY\n" +
                        "[[1.0,2.0],[3.0,4.0]]\n",
                "select distinct ARRAY[[1.0, 2.0], [3.0, 4.0]] from long_sequence(10)",
                true
        );
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
    public void testShardedMapCursorArrayAccess() throws Exception {

        assertMemoryLeak(() -> {
            execute("CREATE TABLE AAPL_orderbook (\n" +
                    "\ttimestamp TIMESTAMP,\n" +
                    "\tasks DOUBLE[][]\n" +
                    ") timestamp(timestamp)\n" +
                    "PARTITION BY HOUR WAL;");
            execute("INSERT INTO AAPL_orderbook (timestamp, asks) \n" +
                    "SELECT dateadd('s', x::int, '2023-08-25T08:00:02.264552Z') as timestamp, ARRAY[\n" +
                    "  [176.8,177.27,182.0,182.3,183.7,185.0,190.0,null, null, null],\n" +
                    "  [26.0,400.0,7.0,15.0,10.0,5.0,2.0,0.0,0.0,0.0],\n" +
                    "  [1.0,1.0,1.0, 1.0,1.0,1.0,1.0,0.0,0.0,0.0]\n" +
                    " ] as asks\n" +
                    "\tFROM long_sequence(3_000_000)\n" +
                    ";");

            drainWalQueue();

            assertQueryNoLeakCheck(
                    "timestamp\tavg_price\tbest_price\tdrift\n" +
                            "2023-08-25T08:00:00.000000Z\t176.79999999999998\t176.8\t-2.8421709430404007E-14\n" +
                            "2023-08-25T08:01:00.000000Z\t176.79999999999993\t176.8\t-8.526512829121202E-14\n" +
                            "2023-08-25T08:02:00.000000Z\t176.79999999999993\t176.8\t-8.526512829121202E-14\n" +
                            "2023-08-25T08:03:00.000000Z\t176.79999999999993\t176.8\t-8.526512829121202E-14\n" +
                            "2023-08-25T08:04:00.000000Z\t176.79999999999993\t176.8\t-8.526512829121202E-14\n",
                    "SELECT * FROM (DECLARE\n" +
                            "\t@price := 1,\n" +
                            "\t@size := 2,\n" +
                            "\t@avg_price := avg(l2price(0.1, asks[@size], asks[@price])),\n" +
                            "\t@best_price := asks[@price, 1]\n" +
                            "\tSELECT \n" +
                            "\t\ttimestamp,\n" +
                            "\t\t@avg_price as avg_price,\n" +
                            "\t\t@best_price as best_price,\n" +
                            "\t\t@avg_price - @best_price as drift\n" +
                            "\tFROM AAPL_orderbook\n" +
                            "\tSAMPLE BY 1m) LIMIT 5;",
                    "timestamp",
                    true,
                    true);
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
                            "[999.0,999.0,999.0,1.0,9.0,10.0,12.0,8.0]\t[999.0,9.0,10.0,12.0,8.0,null,20.0]\t[999.0,999.0]\t[null,null,null,1.0,9.0,10.0,12.0,8.0]\n" +
                            "[]\t[]\t[]\t[]\n" +
                            "null\tnull\tnull\tnull\n",
                    "SELECT shift(arr1, 3, 999.0), shift(arr1[2:], 1, 999.0), shift(arr1[1:3], 10, 999.0), shift(arr1, 3) FROM tango");
            assertSql("shift\tshift1\tshift2\tshift3\n" +
                            "[12.0,8.0,null,20.0,12.0,999.0,999.0,999.0]\t[10.0,12.0,8.0,null,20.0,12.0,999.0]\t[999.0,999.0]\t[12.0,8.0,null,20.0,12.0,null,null,null]\n" +
                            "[]\t[]\t[]\t[]\n" +
                            "null\tnull\tnull\tnull\n",
                    "SELECT shift(arr1, -3, 999.0), shift(arr1[2:], -1, 999.0), shift(arr1[1:3], -10, 999.0), shift(arr1, -3) FROM tango");
            assertSql("shift\tshift1\tshift2\tshift3\n" +
                            "[[999.0,1.0,9.0],[999.0,12.0,8.0]]\t[[9.0,10.0,999.0],[8.0,null,999.0]]\t[[999.0,999.0,999.0],[999.0,999.0,999.0]]\t[[10.0,null,null],[null,null,null]]\n" +
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
                            "[[1.0,10.0,8.0,20.0],[9.0,12.0,null,12.0]]\n" +
                            "[]\n" +
                            "null\n",
                    "SELECT transpose(arr) FROM tango");

            assertSql("shift\tshift1\tshift2\tshift3\n" +
                            "[999.0,999.0,1.0,10.0]\t[999.0,10.0,8.0]\t[999.0,999.0]\t[null,null,null,1.0]\n" +
                            "null\tnull\tnull\tnull\n" +
                            "null\tnull\tnull\tnull\n",
                    "SELECT shift(transpose(arr)[1], 2, 999.0), shift(transpose(arr)[1, 2:], 1, 999.0), shift(transpose(arr)[1, 1:3], 10, 999.0), shift(transpose(arr)[1], 3) FROM tango");

            assertSql("shift\tshift1\tshift2\tshift3\n" +
                            "[8.0,20.0,999.0,999.0]\t[8.0,20.0,999.0]\t[999.0,999.0]\t[20.0,null,null,null]\n" +
                            "null\tnull\tnull\tnull\n" +
                            "null\tnull\tnull\tnull\n",
                    "SELECT shift(transpose(arr)[1], -2, 999.0), shift(transpose(arr)[1, 2:], -1, 999.0), shift(transpose(arr)[1, 1:3], -10, 999.0), shift(transpose(arr)[1], -3) FROM tango");
            assertSql("shift\tshift1\tshift2\tshift3\n" +
                            "[[999.0,1.0,10.0,8.0],[999.0,9.0,12.0,null]]\t[[10.0,8.0,20.0,999.0],[12.0,null,12.0,999.0]]\t[[999.0,999.0,999.0,999.0],[999.0,999.0,999.0,999.0]]\t[[8.0,20.0,null,null],[null,12.0,null,null]]\n" +
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
                    12, "array slice bounds must be non-zero [dim=1, lowerBound=0, upperBound=1]"
            );
            assertExceptionNoLeakCheck("SELECT arr[1:0] FROM tango",
                    12, "array slice bounds must be non-zero [dim=1, lowerBound=1, upperBound=0]"
            );
            assertExceptionNoLeakCheck("SELECT arr[1:2, 1:2, 1:2] FROM tango",
                    17, "too many array access arguments [nDims=2, nArgs=3]"
            );
            assertExceptionNoLeakCheck("SELECT arr[1:(arr[1, 1] - 1)::int] FROM tango",
                    12, "array slice bounds must be non-zero [dim=1, upperBound=0]"
            );
            assertExceptionNoLeakCheck("SELECT arr[(arr[1, 1] - 1)::int : 2] FROM tango",
                    32, "array slice bounds must be non-zero [dim=1, lowerBound=0]"
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
            assertSql("x\n[]\n", "SELECT arr[1:-3] x FROM tango");
            assertSql("x\n[]\n", "SELECT arr[1:-100] x FROM tango");
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
                            "1970-01-06T18:53:20.000000Z\t[[null,0.20447441837877756],[null,null]]\n" +
                            "1970-01-06T18:53:21.000000Z\t[[0.3491070363730514,0.7611029514995744],[0.4217768841969397,null],[0.7261136209823622,0.4224356661645131],[null,0.3100545983862456],[0.1985581797355932,0.33608255572515877],[0.690540444367637,null],[0.21583224269349388,0.15786635599554755],[null,null],[0.12503042190293423,null],[0.9687423276940171,null],[null,null],[null,null],[null,null],[0.7883065830055033,null],[0.4138164748227684,0.5522494170511608],[0.2459345277606021,null]]\n" +
                            "1970-01-06T18:53:22.000000Z\t[[0.7643643144642823,null],[null,null],[0.18769708157331322,0.16381374773748514],[0.6590341607692226,null],[null,null],[0.8837421918800907,0.05384400312338511],[null,0.7230015763133606],[0.12105630273556178,null],[0.5406709846540508,null],[0.9269068519549879,null],[null,null],[0.1202416087573498,null]]\n" +
                            "1970-01-06T18:53:23.000000Z\t[[null,null,0.4971342426836798,null],[0.5065228336156442,null,null,0.03167026265669903],[null,null,0.2879973939681931,null],[null,null,null,0.24008362859107102]]\n" +
                            "1970-01-06T18:53:24.000000Z\t[[0.2185865835029681,null],[0.24079155981438216,0.10643046345788132],[0.5244255672762055,0.0171850098561398],[0.09766834710724581,null],[0.053594208204197136,0.26369335635512836],[0.22895725920713628,0.9820662735672192],[null,0.32424562653969957],[0.8998921791869131,null],[null,null],[0.33746104579374825,0.18740488620384377],[0.10527282622013212,0.8291193369353376],[0.32673950830571696,null],[0.18336217509438513,0.9862476361578772],[0.8693768930398866,0.8189713915910615]]\n" +
                            "1970-01-06T18:53:25.000000Z\t[[0.29659296554924697,0.24642266252221556],[null,null],[null,0.13264292470570205],[0.38422543844715473,null],[null,null],[null,0.7668146556860689],[null,0.05158459929273784],[null,null]]\n" +
                            "1970-01-06T18:53:26.000000Z\t[[0.3568111021227658,0.05758228485190853,0.6729405590773638,null,0.5716129058692643],[0.05094182589333662,null,null,0.4609277382153818,0.5691053034055052],[0.12663676991275652,0.11371841836123953,null,null,0.7203170014947307],[null,null,null,null,0.7704949839249925],[0.8144207168582307,null,null,null,0.2836347139481469]]\n" +
                            "1970-01-06T18:53:27.000000Z\t[[0.08675950660182763,null],[0.741970173888595,0.25353478516307626],[0.2739985338660311,null],[0.8001632261203552,null],[0.7404912278395417,0.08909442703907178],[0.8439276969435359,null],[null,0.08712007604601191]]\n" +
                            "1970-01-06T18:53:28.000000Z\t[[0.5637742551872849,null],[null,null],[0.7195457109208119,null],[0.23493793601747937,null],[0.6334964081687151,0.6721404635638454]]\n" +
                            "1970-01-06T18:53:29.000000Z\t[[0.17405556853190263,0.823395724427589,null,0.8108032283138068,null,null,0.7530494527849502,0.49153268154777974,0.0024457698760806945,0.29168465906260244,0.3121271759430503,0.3004874521886858],[null,0.7653255982993546,null,null,null,null,0.37873228328689634,null,0.7272119755925095,null,0.7467013668130107,0.5794665369115236],[null,0.5308756766878475,0.03192108074989719,null,0.17498425722537903,null,0.34257201464152764,null,null,0.29242748475227853,null,0.11296257318851766],[null,0.23405440872043592,0.1479745625593103,null,0.8115426881784433,null,0.32093405888189597,null,0.04321289940104611,0.8217652538598936,0.6397125243912908,0.29419791719259025],[0.865629565918467,null,null,0.16923843067953104,0.7198854503668188,0.5174107449677378,0.38509066982448115,null,null,null,0.5475429391562822,0.6977332212252165],[null,null,0.4268921400209912,0.9997797234031688,0.5234892454427748,null,null,null,null,0.5169565007469263,0.7039785408034679,0.8461211697505234],[null,0.537020248377422,0.8766908646423737,null,null,0.31852531484741486,null,0.605050319285447,0.9683642405595932,0.3549235578142891,0.04211401699125483,null],[null,0.0032519916115479885,0.2703179181043681,0.729536610842768,0.3317641556575974,0.8895915828662114,null,null,null,null,0.1599211504269954,0.5251698097331752],[null,0.18442756220221035,null,0.48422587819911567,0.2970515836513553,null,0.7826107801293182,null,0.3218450864634881,0.8034049105590781,null,null],[0.40425101135606667,0.9412663583926286,null,null,0.8376764297590714,0.15241451173695408,null,0.743599174001969,null,null,0.9001273812517414,0.5629104624260136],[0.6001215594928115,0.8920252905736616,0.09977691656157406,null,0.2862717364877081,null,null,null,0.8853675629694284,4.945923013344178E-5,null,0.0016532800623808575]]\n" +
                            "1970-01-01T00:00:00.000000Z\t[[0.3489278573518253,null,null,0.07383464174908916,0.8791439438812569]]\n" +
                            "1970-01-01T00:01:40.000000Z\t[[null,0.10820602386069589,null,null,0.11286092606280262,0.7370823954391381,null,0.533524384058538,0.6749208267946962,null,0.3124458010612313,null]]\n" +
                            "1970-01-01T00:03:20.000000Z\t[[0.4137003695612732,null,null,0.32449127848036263,0.41886400558338654,0.8409080254825717,0.06001827721556019,null,null,null]]\n" +
                            "1970-01-01T00:05:00.000000Z\t[[null,null]]\n" +
                            "1970-01-01T00:06:40.000000Z\t[[null,null,null,0.5815065874358148,null]]\n" +
                            "1970-01-01T00:08:20.000000Z\t[[0.020390884194626757,null,null]]\n" +
                            "1970-01-01T00:10:00.000000Z\t[[0.42044603754797416,0.47603861281459736,0.9815126662068089,0.22252546562577824]]\n" +
                            "1970-01-01T00:11:40.000000Z\t[[null,0.8869397617459538,null,null,null,null,null]]\n" +
                            "1970-01-01T00:13:20.000000Z\t[[null,null,0.6993909595959196]]\n" +
                            "1970-01-01T00:15:00.000000Z\t[[0.8148792629172324,null,0.9926343068414145,null,0.8303845449546206,null,null,null,0.7636347764664544,0.2195743166842714,null,null,null,0.5823910118974169,0.05942010834028011]]\n",
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

    private static void assertBroadcastShape(IntList shapeLeft, IntList shapeRight, IntList shapeOutExpected) throws Exception {
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

    private static void fillIntList(IntList list, int... values) {
        list.clear();
        for (int i : values) {
            list.add(i);
        }
    }
}
