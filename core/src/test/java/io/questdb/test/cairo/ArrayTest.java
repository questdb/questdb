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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DerivedArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.arr.NoopArrayWriteState;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cutlass.line.tcp.ArrayBinaryFormatParser;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
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
        Unsafe.putByte(addr + offset, (byte) array.getElemType());
        offset++;
        Unsafe.putByte(addr + offset, (byte) array.getDimCount());
        offset++;
        for (int i = 0, dims = array.getDimCount(); i < dims; i++) {
            Unsafe.putInt(addr + offset, array.getDimLen(i));
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
            assertQuery("tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ask
                            [[1.0,2.0],[4.0,5.0]]
                            [[7.0,8.0],[10.0,11.0]]
                            [[1.0,2.0,3.0],[4.0,5.0,6.0]]
                            [[7.0,8.0,9.0],[10.0,11.0,12.0]]
                            [[1.0,2.0],[5.0,6.0]]
                            [[7.0,8.0],[11.0,12.0]]
                            """);
        });
    }

    @Test
    public void testAccess1d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT ARRAY[1.0, 2, 3] arr1, ARRAY[1.0, 2, 3] arr2 FROM long_sequence(1))");
            execute("INSERT INTO tango VALUES (ARRAY[1.0, 2, 3], null)");
            execute("INSERT INTO tango VALUES (null, null)");
            assertQuery("SELECT arr1[2] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n2.0\n2.0\nnull\n");
            assertQuery("SELECT arr1[2::long] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n2.0\n2.0\nnull\n");
            assertQuery("SELECT arr1['2'] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n2.0\n2.0\nnull\n");
            assertQuery("SELECT arr1[arr1[2]::long] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n2.0\n2.0\nnull\n");
            assertQuery("SELECT arr1[arr2[2]::int] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n2.0\nnull\nnull\n");
            assertQuery("SELECT arr1[arr2[2]::int] x FROM tango")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [arr1[arr2[2]::int]]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tango
                            """);
        });
    }

    @Test
    public void testAccess3d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT " +
                    "ARRAY[ [[1.0, 2], [3.0, 4]], [[5.0, 6], [7.0, 8]] ] arr FROM long_sequence(1))");
            assertQuery("SELECT arr[1, 1, 2] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n2.0\n");
            assertQuery("SELECT arr[2, 1, 2] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n6.0\n");
            assertQuery("SELECT arr[2, 2, 2] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n8.0\n");
            assertQuery("SELECT arr[2, 2][2] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n8.0\n");
            assertQuery("SELECT arr[2][2, 2] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n8.0\n");
            assertQuery("SELECT arr[2][2][2] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n8.0\n");
            assertQuery("SELECT arr[2][2][2] x FROM tango")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [arr[2,2,2]]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tango
                            """);
        });
    }

    @Test
    public void testAccessConstantIndex1d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[])");
            execute("INSERT INTO tango VALUES (ARRAY[10.0, 20, 30])");
            execute("INSERT INTO tango VALUES (null)");
            // constant positive indices on 1D column hit the fast path
            assertQuery("SELECT arr[1] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n10.0\nnull\n");
            assertQuery("SELECT arr[2] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n20.0\nnull\n");
            assertQuery("SELECT arr[3] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n30.0\nnull\n");
            // out of bounds
            assertQuery("SELECT arr[4] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\nnull\nnull\n");
        });
    }

    @Test
    public void testAccessConstantIndex2d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][])");
            execute("INSERT INTO tango VALUES (ARRAY[[1.0, 2, 3], [4.0, 5, 6]])");
            execute("INSERT INTO tango VALUES (null)");
            // constant positive indices on 2D column hit the fast path
            assertQuery("SELECT arr[1, 2] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n2.0\nnull\n");
            assertQuery("SELECT arr[2, 1] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n4.0\nnull\n");
            assertQuery("SELECT arr[2, 3] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n6.0\nnull\n");
            // out of bounds
            assertQuery("SELECT arr[3, 1] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\nnull\nnull\n");
            assertQuery("SELECT arr[1, 4] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\nnull\nnull\n");
        });
    }

    @Test
    public void testAccessConstantIndexMultiPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP, arr1d DOUBLE[], arr2d DOUBLE[][]) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tango VALUES
                    ('2025-01-01', ARRAY[10.0, 20, 30], ARRAY[[1.0, 2], [3.0, 4]]),
                    ('2025-01-01', null, null),
                    ('2025-01-02', ARRAY[40.0, 50, 60], ARRAY[[5.0, 6], [7.0, 8]]),
                    ('2025-01-02', ARRAY[70.0], ARRAY[[9.0]]),
                    ('2025-01-03', ARRAY[], ARRAY[]),
                    ('2025-01-03', ARRAY[80.0, 90], ARRAY[[10.0, 11], [12.0, 13]])
                    """);
            // 1D constant index across partitions
            assertQuery("SELECT arr1d[1] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            10.0
                            null
                            40.0
                            70.0
                            null
                            80.0
                            """);
            assertQuery("SELECT arr1d[2] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            20.0
                            null
                            50.0
                            null
                            null
                            90.0
                            """);
            // 2D constant index across partitions
            assertQuery("SELECT arr2d[1, 2] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            2.0
                            null
                            6.0
                            null
                            null
                            11.0
                            """);
            assertQuery("SELECT arr2d[2, 1] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x
                            3.0
                            null
                            7.0
                            null
                            null
                            12.0
                            """);
        });
    }

    @Test
    public void testAccessConstantNullIndexFreesArrayLiteral() throws Exception {
        // A constant NULL index folds the whole access to a NULL constant, which keeps neither
        // argument, so the factory has to free the array itself. A constant array literal holds
        // its shape and values in native memory, so dropping that free leaks it.
        assertQuery("SELECT ARRAY[[1.0, 2], [3.0, 4]][1, NULL::long] x FROM long_sequence(1)")
                .expectSize()
                .returns("x\nnull\n");
    }

    @Test
    public void testAccessFirstElement1d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[])");
            execute("INSERT INTO tango VALUES (ARRAY[10.0, 20, 30])");
            execute("INSERT INTO tango VALUES (ARRAY[42.0])");
            execute("INSERT INTO tango VALUES (null)");
            // constant index 1 on 1D array hits the first-element fast path
            assertQuery("SELECT arr[1] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n10.0\n42.0\nnull\n");
            assertQuery("SELECT arr[1] x FROM tango")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [arr[1]]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tango
                            """);
        });
    }

    @Test
    public void testAccessFirstElement2d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][])");
            execute("INSERT INTO tango VALUES (ARRAY[[1.0, 2], [3.0, 4]])");
            execute("INSERT INTO tango VALUES (null)");
            // constant indices (1,1) on 2D array hits the first-element fast path
            assertQuery("SELECT arr[1, 1] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n1.0\nnull\n");
            assertQuery("SELECT arr[1][1] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n1.0\nnull\n");
            assertQuery("SELECT arr[1][1] x FROM tango")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [arr[1,1]]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tango
                            """);
        });
    }

    @Test
    public void testAccessFirstElement3d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT " +
                    "ARRAY[ [[1.0, 2], [3.0, 4]], [[5.0, 6], [7.0, 8]] ] arr FROM long_sequence(1))");
            // constant indices (1,1,1) on 3D array hits the first-element fast path
            assertQuery("SELECT arr[1, 1, 1] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n1.0\n");
            assertQuery("SELECT arr[1][1][1] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n1.0\n");
            assertQuery("SELECT arr[1][1, 1] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n1.0\n");
            assertQuery("SELECT arr[1, 1][1] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n1.0\n");
        });
    }

    @Test
    public void testAccessFirstElementEmpty() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[], arr2 DOUBLE[][])");
            execute("INSERT INTO tango VALUES (ARRAY[], ARRAY[])");
            // first-element access on empty arrays returns null
            assertQuery("SELECT arr[1] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\nnull\n");
            assertQuery("SELECT arr2[1, 1] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\nnull\n");
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
                    15, "invalid type for array access [type=BOOLEAN]");
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
            assertQuery("SELECT arr[-1] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\n4.0\n");
            assertQuery("SELECT arr[-2] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\n3.0\n");
            assertQuery("SELECT arr[n] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\n3.0\n");
            assertQuery("SELECT arr[1:-1] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\n[1.0,2.0,3.0]\n");
            assertQuery("SELECT arr[-2:5] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\n[3.0,4.0]\n");
            assertQuery("SELECT arr[1:-1] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\n[1.0,2.0,3.0]\n");
            assertQuery("SELECT arr[2:-1] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\n[2.0,3.0]\n");
            assertQuery("SELECT arr[1:-2] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\n[1.0,2.0]\n");
            assertQuery("SELECT arr[1:n] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\n[1.0,2.0]\n");
            assertQuery("SELECT arr[n:5] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\n[3.0,4.0]\n");
            assertQuery("SELECT arr2[1, -1] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\n2.0\n");
            assertQuery("SELECT arr2[1, n] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\n1.0\n");
            assertQuery("SELECT arr2[2, n:2] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\n[3.0]\n");
            assertQuery("SELECT arr2[1, 1:n] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\n[]\n");
            assertQuery("SELECT arr2[1:2, 1:n] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\n[]\n");
        });
    }

    @Test
    public void testAccessNullIndex() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (n INT, arr DOUBLE[], arr2 DOUBLE[][])");
            execute("INSERT INTO tango VALUES (null, ARRAY[1.0, 2], ARRAY[[1.0, 2], [3.0, 4]])");
            assertQuery("SELECT arr[null::int] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\nnull\n");
            assertQuery("SELECT arr[n] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\nnull\n");
            assertQuery("SELECT arr[1:null] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\nnull\n");
            assertQuery("SELECT arr[null:2] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\nnull\n");
            assertQuery("SELECT arr[1:n] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\nnull\n");
            assertQuery("SELECT arr[n:2] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\nnull\n");
            assertQuery("SELECT arr2[1, null::int] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\nnull\n");
            assertQuery("SELECT arr2[1, n] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\nnull\n");
            assertQuery("SELECT arr2[1, n:2] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\nnull\n");
            assertQuery("SELECT arr2[1, 1:n] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\nnull\n");
            assertQuery("SELECT arr2[1:2, 1:n] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("[]\nnull\n");
        });
    }

    @Test
    public void testAccessOutOfBounds() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT ARRAY[[1.0, 2], [3.0, 4]] arr FROM long_sequence(1))");

            assertQuery("SELECT arr[1, 3] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\nnull\n");
            assertQuery("SELECT arr[3, 1] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\nnull\n");
            assertQuery("SELECT arr[1, -3] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\nnull\n");
            assertQuery("SELECT arr[-3, 1] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\nnull\n");

            assertQuery("SELECT arr[1:1] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[]\n");
            assertQuery("SELECT arr[2:1] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[]\n");
            assertQuery("SELECT arr[2:5] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[[3.0,4.0]]\n");
            assertQuery("SELECT arr[3:3] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[]\n");
            assertQuery("SELECT arr[3:5] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[]\n");
            assertQuery("SELECT arr[3:-5] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[]\n");
            assertQuery("SELECT arr[-1:-2] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[]\n");

            assertQuery("SELECT arr[1, 1:1] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[]\n");
            assertQuery("SELECT arr[1, 2:1] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[]\n");
            assertQuery("SELECT arr[1, 3:3] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[]\n");
            assertQuery("SELECT arr[1, 3:5] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[]\n");
            assertQuery("SELECT arr[1, 3:] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[]\n");

            assertQuery("SELECT arr[1, 2:5] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[2.0]\n");
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
            assertQuery("SELECT arr[i] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n" + subArr1 + "\n");
            assertQuery("SELECT arr[j-i] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n" + subArr1 + "\n");
            assertQuery("SELECT arr[i,j] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n" + subArr12 + "\n");
            assertQuery("SELECT arr[i:j] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[" + subArr1 + "]\n");
            assertQuery("SELECT arr[i:j+j-i-i] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[" + subArr1 + "]\n");
            assertQuery("SELECT arr[j-i:i+i] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[" + subArr1 + "]\n");
        });
    }

    @Test
    public void testAddColumnUnsupportedType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (n LONG)");
            assertQuery("ALTER TABLE tango ADD COLUMN arr BYTE[]")
                    .noLeakCheck()
                    .fails(33, "unsupported array element type [type=BYTE]");
            assertQuery("ALTER TABLE tango ADD COLUMN arr SHORT[]")
                    .noLeakCheck()
                    .fails(33, "unsupported array element type [type=SHORT]");
            assertQuery("ALTER TABLE tango ADD COLUMN arr INT[]")
                    .noLeakCheck()
                    .fails(33, "unsupported array element type [type=INT]");
            assertQuery("ALTER TABLE tango ADD COLUMN arr LONG[]")
                    .noLeakCheck()
                    .fails(33, "unsupported array element type [type=LONG]");
            assertQuery("ALTER TABLE tango ADD COLUMN arr FLOAT[]")
                    .noLeakCheck()
                    .fails(33, "unsupported array element type [type=FLOAT]");
            assertQuery("ALTER TABLE tango ADD COLUMN arr BOOLEAN[]")
                    .noLeakCheck()
                    .fails(33, "unsupported array element type [type=BOOLEAN]");
            assertQuery("ALTER TABLE tango ADD COLUMN arr CHAR[]")
                    .noLeakCheck()
                    .fails(33, "unsupported array element type [type=CHAR]");
            assertQuery("ALTER TABLE tango ADD COLUMN arr STRING[]")
                    .noLeakCheck()
                    .fails(33, "unsupported array element type [type=STRING]");
            assertQuery("ALTER TABLE tango ADD COLUMN arr VARCHAR[]")
                    .noLeakCheck()
                    .fails(33, "unsupported array element type [type=VARCHAR]");
            assertQuery("ALTER TABLE tango ADD COLUMN arr ARRAY[]")
                    .noLeakCheck()
                    .fails(33, "the system supports type-safe arrays, e.g. `type[]`. Supported types are: DOUBLE. More types incoming.");
            assertQuery("ALTER TABLE tango ADD COLUMN arr BINARY[]")
                    .noLeakCheck()
                    .fails(33, "unsupported array element type [type=BINARY]");
            assertQuery("ALTER TABLE tango ADD COLUMN arr DATE[]")
                    .noLeakCheck()
                    .fails(33, "unsupported array element type [type=DATE]");
            assertQuery("ALTER TABLE tango ADD COLUMN arr TIMESTAMP[]")
                    .noLeakCheck()
                    .fails(33, "unsupported array element type [type=TIMESTAMP]");
            assertQuery("ALTER TABLE tango ADD COLUMN arr UUID[]")
                    .noLeakCheck()
                    .fails(33, "unsupported array element type [type=UUID]");
            assertQuery("ALTER TABLE tango ADD COLUMN arr LONG128[]")
                    .noLeakCheck()
                    .fails(33, "unsupported array element type [type=LONG128]");
            assertQuery("ALTER TABLE tango ADD COLUMN arr GEOHASH[]")
                    .noLeakCheck()
                    .fails(33, "unsupported array element type [type=GEOHASH]");
            assertQuery("ALTER TABLE tango ADD COLUMN arr DECIMAL[]")
                    .noLeakCheck()
                    .fails(33, "unsupported array element type [type=DECIMAL]");
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
            assertQuery("SELECT a * 3.0 + 1.0, b * 2.0 + 1.0, b[1] * 5.0 + 1.0, b[2:] * 10.0 + 1.0 FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column\tcolumn1\tcolumn2\tcolumn3
                            [7.0,null]\t[[5.0,7.0],[9.0,11.0]]\t[11.0,16.0]\t[[41.0,51.0]]
                            [19.0,22.0]\t[[17.0,19.0]]\t[41.0,46.0]\t[]
                            null\tnull\tnull\tnull
                            """);
            assertQuery("SELECT transpose(a) + 3.0, transpose(b) + 2.0 FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column\tcolumn1
                            [5.0,null]\t[[4.0,6.0],[5.0,7.0]]
                            [9.0,10.0]\t[[10.0],[11.0]]
                            null\tnull
                            """);
            assertQuery("SELECT 3.0 + a, 2.0 + b, 5.0 + b[1], 10.0 + b[2:] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column\tcolumn1\tcolumn2\tcolumn3
                            [5.0,null]\t[[4.0,5.0],[6.0,7.0]]\t[7.0,8.0]\t[[14.0,15.0]]
                            [9.0,10.0]\t[[10.0,11.0]]\t[13.0,14.0]\t[]
                            null\tnull\tnull\tnull
                            """);
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
            assertQuery("SELECT array_avg(arr1), array_avg(arr1[2:]), array_avg(arr1[1:3]) FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            array_avg\tarray_avg1\tarray_avg2
                            10.0\t11.8\t5.0
                            null\tnull\tnull
                            null\tnull\tnull
                            """);
            assertQuery("SELECT array_avg(arr2), array_avg(transpose(arr2)), array_avg(arr2[1]), array_avg(arr2[1:]), array_avg(arr2[2:]) FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            array_avg\tarray_avg1\tarray_avg2\tarray_avg3\tarray_avg4
                            10.0\t10.0\t10.0\t10.0\tnull
                            null\tnull\tnull\tnull\tnull
                            null\tnull\tnull\tnull\tnull
                            """);
        });
    }

    @Test
    public void testArrayAvgNonVanilla() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]]]);"
            );
            assertQuery("SELECT array_avg(arr), array_avg(transpose(arr)) FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            array_avg\tarray_avg1
                            5.0\t5.0
                            """);
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
    public void testArrayConsumersOverSampleByFillPrevGap() throws Exception {
        // SAMPLE BY ... FILL(PREV) fills a gap from the preceding bucket, but the buckets before a
        // key's first row have nothing to carry forward. The record has no array to hand out for
        // those, so every array consumer must see a NULL array there instead of reaching into an
        // ArrayView that is not there.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP, s SYMBOL, arr DOUBLE[]) TIMESTAMP(ts) PARTITION BY DAY");
            // Key 'b' starts two buckets late, so its 00:00 and 00:01 buckets have no prevailing row.
            execute("INSERT INTO tango VALUES " +
                    "('2023-01-01T00:00:00.000000Z', 'a', ARRAY[1.0, 2.0]), " +
                    "('2023-01-01T00:02:00.000000Z', 'b', ARRAY[3.0, 4.0, 5.0])");
            final String filled = "SELECT ts, s, first(arr) a FROM tango SAMPLE BY 1m FILL(PREV) ORDER BY s, ts";
            // array_sum() reads the array through the ArrayView route, with no column index to take
            // the direct accessor, so it is the consumer that sees the gap record head-on.
            assertQuery("SELECT s, array_sum(a) total FROM (" + filled + ")")
                    .noLeakCheck()
                    .returns("""
                            s\ttotal
                            a\t3.0
                            a\t3.0
                            a\t3.0
                            b\tnull
                            b\tnull
                            b\t12.0
                            """);
            // dim_length() takes the direct accessor instead, the other of the two routes.
            assertQuery("SELECT s, dim_length(a, 1) len FROM (" + filled + ")")
                    .noLeakCheck()
                    .returns("""
                            s\tlen
                            a\t2
                            a\t2
                            a\t2
                            b\tnull
                            b\tnull
                            b\t3
                            """);
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
            assertQuery("SELECT array_count(arr1), array_count(arr1[2:]), array_count(arr1[1:3]) FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            array_count\tarray_count1\tarray_count2
                            7\t6\t2
                            0\t0\t0
                            0\t0\t0
                            """);

            assertQuery("SELECT array_count(arr2), array_count(transpose(arr2)), array_count(arr2[1]), array_count(arr2[1:]), array_count(arr2[2:]) FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            array_count\tarray_count1\tarray_count2\tarray_count3\tarray_count4
                            7\t7\t7\t7\t0
                            0\t0\t0\t0\t0
                            0\t0\t0\t0\t0
                            """);
        });
    }

    @Test
    public void testArrayCountNonVanilla() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]]]);"
            );
            assertQuery("SELECT array_count(arr), array_count(transpose(arr)) FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            array_count\tarray_count1
                            9\t9
                            """);
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
            assertQuery("SELECT array_cum_sum(arr1), array_cum_sum(arr1[2:]), array_cum_sum(arr1[1:3]) FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            array_cum_sum\tarray_cum_sum1\tarray_cum_sum2
                            [1.0,10.0,20.0,32.0,40.0,40.0,60.0,72.0]\t[9.0,19.0,31.0,39.0,39.0,59.0,71.0]\t[1.0,10.0]
                            null\tnull\tnull
                            null\tnull\tnull
                            """);

            assertQuery("SELECT array_cum_sum(arr2), array_cum_sum(transpose(arr2)), array_cum_sum(arr2[1]), array_cum_sum(arr2[1:]), array_cum_sum(arr2[2:]) FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            array_cum_sum\tarray_cum_sum1\tarray_cum_sum2\tarray_cum_sum3\tarray_cum_sum4
                            [1.0,10.0,20.0,32.0,40.0,40.0,60.0,72.0]\t[1.0,10.0,20.0,32.0,40.0,40.0,60.0,72.0]\t[1.0,10.0,20.0,32.0,40.0,40.0,60.0,72.0]\t[1.0,10.0,20.0,32.0,40.0,40.0,60.0,72.0]\tnull
                            null\tnull\tnull\tnull\tnull
                            null\tnull\tnull\tnull\tnull
                            """);
        });
    }

    @Test
    public void testArrayCumSumBehaviourMixedNulls() throws Exception {
        assertMemoryLeak(() -> assertSqlWithTypes("""
                        a\tarray_cum_sum
                        [null,1.2,null,5.3]:DOUBLE[]\t[null,1.2,1.2,6.5]:DOUBLE[]
                        """,
                "select array[null, 1.2, null, 5.3] as a, array_cum_sum(a);\n")
        );
    }

    @Test
    public void testArrayCumSumKahan() throws Exception {
        assertSqlWithTypes("""
                        array_cum_sum
                        [10000.0,10003.14159,10005.85987]:DOUBLE[]
                        """,
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
            assertQuery("SELECT a * 3.0/0.5, b * 2.0/0.5, b[1] * 5.0 / 0.5, b[2:] * 10.0 / 0.5 FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column\tcolumn1\tcolumn2\tcolumn3
                            [12.0,null]\t[[8.0,12.0],[16.0,20.0]]\t[20.0,30.0]\t[[80.0,100.0]]
                            [36.0,42.0]\t[[32.0,36.0]]\t[80.0,90.0]\t[]
                            null\tnull\tnull\tnull
                            """);
            assertQuery("SELECT transpose(a)/0.5, transpose(b)/0.5 FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column\tcolumn1
                            [4.0,null]\t[[4.0,8.0],[6.0,10.0]]
                            [12.0,14.0]\t[[16.0],[18.0]]
                            null\tnull
                            """);
            assertQuery("SELECT a/0.0, a/null::double FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column\tcolumn1
                            [null,null]\t[null,null]
                            [null,null]\t[null,null]
                            null\tnull
                            """);
        });
    }

    @Test
    public void testArrayDotProduct() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (left DOUBLE[][], right DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[[1.0, 3], [2.0, 5.0]], ARRAY[[1.0, 5.0], [7.0, 2.0]]), " +
                    "(ARRAY[[1.0, 1]], ARRAY[[5.0, null]])");
            assertQuery("SELECT dot_product(left, right) AS product FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            product
                            40.0
                            5.0
                            """);
            assertQuery("SELECT dot_product(transpose(left), transpose(right)) AS product FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            product
                            40.0
                            5.0
                            """);
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
            assertQuery("SELECT dot_product(left, 1.0), dot_product(right, 2.0), dot_product(left, null::double) FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            dot_product\tdot_product1\tdot_product2
                            11.0\t30.0\tnull
                            2.0\t10.0\tnull
                            """);
            assertQuery("SELECT dot_product(transpose(left), 1.0), dot_product(transpose(right), 2.0), dot_product(2.0, transpose(right)) FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            dot_product\tdot_product1\tdot_product2
                            11.0\t30.0\t30.0
                            2.0\t10.0\t10.0
                            """);
        });
    }

    @Test
    public void testArrayDotProductTransposedOperand() throws Exception {
        // transpose([[1,3],[2,5]]) = [[1,2],[3,5]], right = [[1,5],[7,2]]:
        // 1*1 + 2*5 + 3*7 + 5*2 = 42.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (left DOUBLE[][], right DOUBLE[][])");
            execute("INSERT INTO tango VALUES (ARRAY[[1.0, 3.0], [2.0, 5.0]], ARRAY[[1.0, 5.0], [7.0, 2.0]])");
            assertQuery("SELECT dot_product(transpose(left), right) AS product FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            product
                            42.0
                            """);
        });
    }

    @Test
    public void testArrayFirstFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ts timestamp, x int, v double[]) timestamp(ts) partition by DAY");
            execute("insert into test(ts,x,v) values ('2022-02-24', 1, ARRAY[1.0,1.0]), ('2022-02-24', 2, null), ('2022-02-24', 3, ARRAY[2.0,2.0])");

            assertQuery("select ts, x, first(v) as v from test sample by 1s")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tx\tv
                            2022-02-24T00:00:00.000000Z\t1\t[1.0,1.0]
                            2022-02-24T00:00:00.000000Z\t2\tnull
                            2022-02-24T00:00:00.000000Z\t3\t[2.0,2.0]
                            """);

            assertQuery("select ts, x, first(v) as v from test sample by 1s")
                    .noLeakCheck()
                    .assertsPlan("""
                            Encode sort light
                              keys: [ts]
                                Async Group By workers: 1
                                  keys: [ts,x]
                                  keyFunctions: [timestamp_floor_utc('1s',ts)]
                                  values: [first(v)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: test
                            """);
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
            assertQuery("select ts, max(array_position(arr, a)) as v from tango sample by 1d")
                    .withPlan("""
                            Encode sort light
                              keys: [ts]
                                Async Group By workers: 1
                                  keys: [ts]
                                  keyFunctions: [timestamp_floor_utc('1d',ts)]
                                  values: [max(array_position(arr, a))]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tango
                            """)
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv
                            2025-06-26T00:00:00.000000Z\t1
                            2025-06-27T00:00:00.000000Z\t8
                            """);

            assertQuery("select ts, min(insertion_point(arr, a)) as v from tango sample by 1d")
                    .withPlan("""
                            Encode sort light
                              keys: [ts]
                                Async Group By workers: 1
                                  keys: [ts]
                                  keyFunctions: [timestamp_floor_utc('1d',ts)]
                                  values: [min(insertion_point(arr,a))]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tango
                            """)
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv
                            2025-06-26T00:00:00.000000Z\t2
                            2025-06-27T00:00:00.000000Z\t6
                            """);

            assertQuery("select ts, sum(array_count(arr)) as v from tango sample by 1d")
                    .withPlan("""
                            Encode sort light
                              keys: [ts]
                                Async Group By workers: 1
                                  keys: [ts]
                                  keyFunctions: [timestamp_floor_utc('1d',ts)]
                                  values: [sum(array_count(arr))]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tango
                            """)
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv
                            2025-06-26T00:00:00.000000Z\t10
                            2025-06-27T00:00:00.000000Z\t20
                            """);

            assertQuery("select ts, sum(array_avg(arr)) as v from tango sample by 1d")
                    .withPlan("""
                            Encode sort light
                              keys: [ts]
                                Async Group By workers: 1
                                  keys: [ts]
                                  keyFunctions: [timestamp_floor_utc('1d',ts)]
                                  values: [sum(array_avg(arr))]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tango
                            """)
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv
                            2025-06-26T00:00:00.000000Z\t5.5
                            2025-06-27T00:00:00.000000Z\t41.0
                            """);

            assertQuery("select ts, array_sum(array_cum_sum(arr)), sum(a) from tango sample by 1d")
                    .withPlan("""
                            Encode sort light
                              keys: [ts]
                                Async Group By workers: 1
                                  keys: [ts,array_sum]
                                  keyFunctions: [timestamp_floor_utc('1d',ts),array_sum(array_cum_sum(arr))]
                                  values: [sum(a)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tango
                            """)
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tarray_sum\tsum
                            2025-06-26T00:00:00.000000Z\t220.0\t1.0
                            2025-06-26T00:00:00.000000Z\tnull\t10.0
                            2025-06-27T00:00:00.000000Z\t770.0\t18.0
                            2025-06-27T00:00:00.000000Z\t1320.0\t25.0
                            """);

            assertQuery("select ts, dot_product(arr, 2), first(a) from tango sample by 1d")
                    .withPlan("""
                            Encode sort light
                              keys: [ts]
                                Async Group By workers: 1
                                  keys: [ts,dot_product]
                                  keyFunctions: [timestamp_floor_utc('1d',ts),dot_product(arr,2)]
                                  values: [first(a)]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tango
                            """)
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tdot_product\tfirst
                            2025-06-26T00:00:00.000000Z\t110.0\t1.0
                            2025-06-26T00:00:00.000000Z\tnull\t10.0
                            2025-06-27T00:00:00.000000Z\t310.0\t18.0
                            2025-06-27T00:00:00.000000Z\t510.0\t25.0
                            """);

            assertQuery("select ts, sum(array_sum((arr * 5 + 3 - 1)/2)) from tango sample by 1d")
                    .withPlan("""
                            Encode sort light
                              keys: [ts]
                                Async Group By workers: 1
                                  keys: [ts]
                                  keyFunctions: [timestamp_floor_utc('1d',ts)]
                                  values: [sum(array_sum(arr*5+3-1/2))]
                                  filter: null
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tango
                            """)
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tsum
                            2025-06-26T00:00:00.000000Z\t147.5
                            2025-06-27T00:00:00.000000Z\t1045.0
                            """);
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
            assertQuery("SELECT a * 3.0, b * 2.0, b[1] * 5.0, b[2:] * 10.0 FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column\tcolumn1\tcolumn2\tcolumn3
                            [6.0,null]\t[[4.0,6.0],[8.0,10.0]]\t[10.0,15.0]\t[[40.0,50.0]]
                            [18.0,21.0]\t[[16.0,18.0]]\t[40.0,45.0]\t[]
                            null\tnull\tnull\tnull
                            """);
            assertQuery("SELECT transpose(a) * 3.0, transpose(b) * 2.0 FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column\tcolumn1
                            [6.0,null]\t[[4.0,8.0],[6.0,10.0]]
                            [18.0,21.0]\t[[16.0],[18.0]]
                            null\tnull
                            """);
            assertQuery("SELECT 3.0 * a, 2.0 * b, 5.0 * b[1], 10.0 * b[2:] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column\tcolumn1\tcolumn2\tcolumn3
                            [6.0,null]\t[[4.0,6.0],[8.0,10.0]]\t[10.0,15.0]\t[[40.0,50.0]]
                            [18.0,21.0]\t[[16.0,18.0]]\t[40.0,45.0]\t[]
                            null\tnull\tnull\tnull
                            """);
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
            assertQuery("SELECT " +
                    "array_position(arr1, 8), " +
                    "array_position(arr1, null), " +
                    "array_position(arr1, 11), " +
                    "array_position(arr1[2:], 9) " +
                    "FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            array_position\tarray_position1\tarray_position2\tarray_position3
                            5\t6\tnull\t1
                            null\t1\tnull\tnull
                            null\tnull\tnull\tnull
                            """);

            assertQuery("SELECT " +
                    "array_position(arr2[1], 8), " +
                    "array_position(arr2[1], null), " +
                    "array_position(arr2[1], 11), " +
                    "array_position(arr2[1][2:], 9) " +
                    "FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            array_position\tarray_position1\tarray_position2\tarray_position3
                            5\t6\tnull\t1
                            null\t1\tnull\tnull
                            null\tnull\tnull\tnull
                            """);

            assertQuery("SELECT " +
                    "array_position(arr1, arr1[1]), " +
                    "array_position(arr1, arr1[2]), " +
                    "array_position(arr1, arr1[3]), " +
                    "array_position(arr1[2:], arr1[2]) " +
                    "FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            array_position\tarray_position1\tarray_position2\tarray_position3
                            1\t2\t3\t1
                            1\t1\t1\tnull
                            null\tnull\tnull\tnull
                            """);
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
            assertQuery("SELECT " +
                    "array_position(transpose(arr)[1], 8), " +
                    "array_position(transpose(arr)[1], null), " +
                    "array_position(transpose(arr)[1], 11), " +
                    "array_position(transpose(arr)[1, 2:], 9) " +
                    "FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            array_position\tarray_position1\tarray_position2\tarray_position3
                            5\t6\tnull\t1
                            """);
        });
    }

    @Test
    public void testArrayPositionNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[])");
            execute("INSERT INTO tango VALUES (ARRAY[1.0/0.0, 0.0/0.0, -1.0/0.0])");
            assertQuery("SELECT array_position(arr, 0.0/0.0) FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("array_position\n1\n");
            assertQuery("SELECT array_position(arr, 1.0/0.0) FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("array_position\n1\n");
            assertQuery("SELECT array_position(arr, -1.0/0.0) FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("array_position\n1\n");
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
            assertQuery("SELECT a * 3.0 - 1.0, b * 2.0 - 1.0, b[1] * 5.0 - 1.0, b[2:] * 10.0 - 1.0 FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column\tcolumn1\tcolumn2\tcolumn3
                            [5.0,null]\t[[3.0,5.0],[7.0,9.0]]\t[9.0,14.0]\t[[39.0,49.0]]
                            [17.0,20.0]\t[[15.0,17.0]]\t[39.0,44.0]\t[]
                            null\tnull\tnull\tnull
                            """);
            assertQuery("SELECT transpose(a) - 3.0, transpose(b) - 2.0 FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column\tcolumn1
                            [-1.0,null]\t[[0.0,2.0],[1.0,3.0]]
                            [3.0,4.0]\t[[6.0],[7.0]]
                            null\tnull
                            """);
            assertQuery("SELECT a - null::double FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column
                            [null,null]
                            [null,null]
                            null
                            """);
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
            assertQuery("SELECT array_sum(arr1), array_sum(arr1[2:]), array_sum(arr1[1:3]) FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            array_sum\tarray_sum1\tarray_sum2
                            72.0\t71.0\t10.0
                            null\tnull\tnull
                            null\tnull\tnull
                            """);

            assertQuery("SELECT array_sum(arr2), array_sum(transpose(arr2)), array_sum(arr2[1]), array_sum(arr2[1:]), array_sum(arr2[2:]) FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            array_sum\tarray_sum1\tarray_sum2\tarray_sum3\tarray_sum4
                            72.0\t72.0\t72.0\t72.0\tnull
                            null\tnull\tnull\tnull\tnull
                            null\tnull\tnull\tnull\tnull
                            """);
        });
    }

    @Test
    public void testArraySumAndCumSumNullBehaviour() throws Exception {
        assertMemoryLeak(() -> {
            assertSqlWithTypes("""
                            i\tarray_sum
                            [null,null]:DOUBLE[]\tnull:DOUBLE
                            [null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]:DOUBLE[]\tnull:DOUBLE
                            [null,null,null,null,null,null,null]:DOUBLE[]\tnull:DOUBLE
                            [null,null,null,null,null,null,null]:DOUBLE[]\tnull:DOUBLE
                            [null,null,null]:DOUBLE[]\tnull:DOUBLE
                            [null,null,null,null]:DOUBLE[]\tnull:DOUBLE
                            [null,null,null,null,null,null,null,null,null,null,null,null]:DOUBLE[]\tnull:DOUBLE
                            [null,null]:DOUBLE[]\tnull:DOUBLE
                            [null,null,null]:DOUBLE[]\tnull:DOUBLE
                            [null,null,null,null,null,null,null]:DOUBLE[]\tnull:DOUBLE
                            """,
                    "select rnd_double_array(1,1) i, array_sum(i) from long_sequence(10);\n");
            assertSqlWithTypes("""
                            i\tarray_cum_sum
                            [null,null]:DOUBLE[]\tnull:DOUBLE[]
                            [null,null,null,null,null,null]:DOUBLE[]\tnull:DOUBLE[]
                            [null,null]:DOUBLE[]\tnull:DOUBLE[]
                            [null,null]:DOUBLE[]\tnull:DOUBLE[]
                            [null,null,null]:DOUBLE[]\tnull:DOUBLE[]
                            [null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]:DOUBLE[]\tnull:DOUBLE[]
                            [null,null]:DOUBLE[]\tnull:DOUBLE[]
                            [null,null,null,null,null,null,null,null,null,null,null,null,null,null]:DOUBLE[]\tnull:DOUBLE[]
                            [null,null]:DOUBLE[]\tnull:DOUBLE[]
                            [null,null,null,null,null]:DOUBLE[]\tnull:DOUBLE[]
                            """,
                    "select rnd_double_array(1,1) i, array_cum_sum(i) from long_sequence(10);\n");
        });
    }

    @Test
    public void testArraySumKahan() throws Exception {
        assertSqlWithTypes("""
                        array_sum
                        10005.85987:DOUBLE
                        """,
                "SELECT array_sum(array[10000d, 3.14159, 2.71828]);");
    }

    @Test
    public void testArraySumNonVanilla() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]]]);"
            );
            assertQuery("SELECT array_sum(arr), array_sum(transpose(arr)) FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            array_sum\tarray_sum1
                            45.0\t45.0
                            """);
        });
    }

    @Test
    public void testAutoCastToDouble() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("SELECT ARRAY[1, 2] arr FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("arr\n[1.0,2.0]\n");
            assertQuery("SELECT ARRAY[[1, 2], [3, 4]] arr FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("arr\n[[1.0,2.0],[3.0,4.0]]\n");
        });
    }

    @Test
    public void testBadSlicingTypeFailsGracefully() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE tango AS (SELECT ARRAY[
                       [ [ 1,  2,  3], [ 4,  5,  6], [ 7,  8,  9] ],
                       [ [10, 11, 12], [13, 14, 15], [16, 17, 18] ],
                       [ [19, 20, 21], [22, 23, 24], [25, 26, 27] ]
                    ] arr from long_sequence(1));""");
            assertExceptionNoLeakCheck("SELECT arr[1, 3.0] subarr FROM tango;",
                    14, "invalid type for array access [type=DOUBLE]"
            );
        });
    }

    @Test
    public void testBasicArithmetic1d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE[], b DOUBLE[])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[2.0, 3.0], ARRAY[4.0, 5]), " +
                    "(ARRAY[6.0, 7], ARRAY[8.0, 9])");
            assertQuery("SELECT a + b sum FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("sum\n[6.0,8.0]\n[14.0,16.0]\n");
            assertQuery("SELECT a - b diff FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("diff\n[-2.0,-2.0]\n[-2.0,-2.0]\n");
            assertQuery("SELECT a * b product FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("product\n[8.0,15.0]\n[48.0,63.0]\n");
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
            assertQuery("SELECT a + b sum FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("sum\n[[[12.0,14.0],[16.0,18.0]],[[20.0,22.0],[24.0,26.0]]]\n");
            assertQuery("SELECT a - b diff FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("diff\n[[[-8.0,-8.0],[-8.0,-8.0]],[[-8.0,-8.0],[-8.0,-8.0]]]\n");
            assertQuery("SELECT a * b product FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("product\n[[[20.0,33.0],[48.0,65.0]],[[84.0,105.0],[128.0,153.0]]]\n");
        });
    }

    @Test
    public void testBasicArithmeticAutoBroadcast() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE[][], b DOUBLE[])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[[0.0, 0, 0], [10, 10, 10], [20, 20, 20], [30, 30, 30]], ARRAY[0, 1, 2])");
            assertQuery("SELECT a + b, a - b, a * b, a / b FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column\tcolumn1\tcolumn2\tcolumn3
                            [[0.0,1.0,2.0],[10.0,11.0,12.0],[20.0,21.0,22.0],[30.0,31.0,32.0]]\t[[0.0,-1.0,-2.0],[10.0,9.0,8.0],[20.0,19.0,18.0],[30.0,29.0,28.0]]\t[[0.0,0.0,0.0],[0.0,10.0,20.0],[0.0,20.0,40.0],[0.0,30.0,60.0]]\t[[null,0.0,0.0],[null,10.0,5.0],[null,20.0,10.0],[null,30.0,15.0]]
                            """);
            assertQuery("SELECT (a + b) * (a - b) from tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column
                            [[0.0,-1.0,-4.0],[100.0,99.0,96.0],[400.0,399.0,396.0],[900.0,899.0,896.0]]
                            """);
            execute("CREATE TABLE tango1 (a DOUBLE[][], b DOUBLE[])");
            execute("INSERT INTO tango1 VALUES " +
                    "(ARRAY[[1.0, 2.0]], ARRAY[0, 1, 2])");
            assertQuery("select a + b from tango1")
                    .noLeakCheck()
                    .fails(7, "arrays have incompatible shapes [leftShape=[1,2], rightShape=[3]]");
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

            assertQuery("""
                    select\s
                      case\s
                        when ts in '2020' then array[]::double[]
                        when ts in '2021' then array[1.0]\s
                        when ts in '2022' then '{1.0, 2.0}'::double[]\s
                        when ts in '2023' then array[1.0, 2.0, 3.0]\s
                        when ts in '2024' then array[1.0, 2.0, 3.0, 4.0]\s
                        else a\s
                      end, *
                    from tango;""")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            case\tts\ti\ta
                            []\t2020-01-01T00:00:00.000000Z\t0\t[]
                            [1.0]\t2021-01-01T00:00:00.000000Z\t1\t[-1.0]
                            [1.0,2.0]\t2022-01-01T00:00:00.000000Z\t2\t[-1.0,-2.0]
                            [1.0,2.0,3.0]\t2023-01-01T00:00:00.000000Z\t3\t[-1.0,-2.0,-3.0]
                            [1.0,2.0,3.0,4.0]\t2024-01-01T00:00:00.000000Z\t4\t[-1.0,-2.0,-3.0,-4.0]
                            [-1.0,-2.0,-3.0,-4.0,-5.0]\t2025-01-01T00:00:00.000000Z\t5\t[-1.0,-2.0,-3.0,-4.0,-5.0]
                            """);

            assertQuery("""
                    select\s
                      case\s
                        when a = ARRAY[-1.0] then 'literal'
                        when a = '{-1,-2}'::double[] then 'casting'
                        when a = ARRAY[[1.0],[2.0]] then 'never'
                        when a = ARRAY[]::double[] then 'empty'
                        else 'whatever'
                      end, *
                    from tango;""")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            case\tts\ti\ta
                            empty\t2020-01-01T00:00:00.000000Z\t0\t[]
                            literal\t2021-01-01T00:00:00.000000Z\t1\t[-1.0]
                            casting\t2022-01-01T00:00:00.000000Z\t2\t[-1.0,-2.0]
                            whatever\t2023-01-01T00:00:00.000000Z\t3\t[-1.0,-2.0,-3.0]
                            whatever\t2024-01-01T00:00:00.000000Z\t4\t[-1.0,-2.0,-3.0,-4.0]
                            whatever\t2025-01-01T00:00:00.000000Z\t5\t[-1.0,-2.0,-3.0,-4.0,-5.0]
                            """);

            assertQuery("""
                    select\s
                      case\s
                        when ts in '2021' then array[1.0]\s
                        when ts in '2024' then 1\s
                        else a\s
                      end, *
                    from tango;""")
                    .noLeakCheck()
                    .fails(82, "inconvertible types: INT -> DOUBLE[]");
        });
    }

    @Test
    public void testChangeColumnToUnsupportedType() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (n LONG)");
            assertQuery("ALTER TABLE tango ALTER COLUMN n TYPE BYTE[]")
                    .noLeakCheck()
                    .fails(38, "unsupported array element type [type=BYTE]");
            assertQuery("ALTER TABLE tango ALTER COLUMN n TYPE SHORT[]")
                    .noLeakCheck()
                    .fails(38, "unsupported array element type [type=SHORT]");
            assertQuery("ALTER TABLE tango ALTER COLUMN n TYPE INT[]")
                    .noLeakCheck()
                    .fails(38, "unsupported array element type [type=INT]");
            assertQuery("ALTER TABLE tango ALTER COLUMN n TYPE LONG[]")
                    .noLeakCheck()
                    .fails(38, "unsupported array element type [type=LONG]");
            assertQuery("ALTER TABLE tango ALTER COLUMN n TYPE FLOAT[]")
                    .noLeakCheck()
                    .fails(38, "unsupported array element type [type=FLOAT]");
            assertQuery("ALTER TABLE tango ALTER COLUMN n TYPE BOOLEAN[]")
                    .noLeakCheck()
                    .fails(38, "unsupported array element type [type=BOOLEAN]");
            assertQuery("ALTER TABLE tango ALTER COLUMN n TYPE CHAR[]")
                    .noLeakCheck()
                    .fails(38, "unsupported array element type [type=CHAR]");
            assertQuery("ALTER TABLE tango ALTER COLUMN n TYPE STRING[]")
                    .noLeakCheck()
                    .fails(38, "unsupported array element type [type=STRING]");
            assertQuery("ALTER TABLE tango ALTER COLUMN n TYPE VARCHAR[]")
                    .noLeakCheck()
                    .fails(38, "unsupported array element type [type=VARCHAR]");
            assertQuery("ALTER TABLE tango ALTER COLUMN n TYPE ARRAY[]")
                    .noLeakCheck()
                    .fails(38, "the system supports type-safe arrays, e.g. `type[]`. Supported types are: DOUBLE. More types incoming.");
            assertQuery("ALTER TABLE tango ALTER COLUMN n TYPE BINARY[]")
                    .noLeakCheck()
                    .fails(38, "unsupported array element type [type=BINARY]");
            assertQuery("ALTER TABLE tango ALTER COLUMN n TYPE DATE[]")
                    .noLeakCheck()
                    .fails(38, "unsupported array element type [type=DATE]");
            assertQuery("ALTER TABLE tango ALTER COLUMN n TYPE TIMESTAMP[]")
                    .noLeakCheck()
                    .fails(38, "unsupported array element type [type=TIMESTAMP]");
            assertQuery("ALTER TABLE tango ALTER COLUMN n TYPE UUID[]")
                    .noLeakCheck()
                    .fails(38, "unsupported array element type [type=UUID]");
            assertQuery("ALTER TABLE tango ALTER COLUMN n TYPE LONG128[]")
                    .noLeakCheck()
                    .fails(38, "unsupported array element type [type=LONG128]");
            assertQuery("ALTER TABLE tango ALTER COLUMN n TYPE GEOHASH[]")
                    .noLeakCheck()
                    .fails(38, "unsupported array element type [type=GEOHASH]");
            assertQuery("ALTER TABLE tango ALTER COLUMN n TYPE DECIMAL[]")
                    .noLeakCheck()
                    .fails(38, "unsupported array element type [type=DECIMAL]");
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
        assertQuery("SELECT ARRAY[1.0] || ARRAY[2.0, 3.0] FROM long_sequence(1)")
                .fails(12, "unsupported type: DOUBLE[]");
    }

    @Test
    public void testCreateAsSelect2d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE, b DOUBLE)");
            execute("INSERT INTO tango VALUES (1.0, 2.0)");
            execute("CREATE TABLE samba AS (SELECT ARRAY[[a, a], [b, b]] arr FROM tango)");
            assertQuery("samba")
                    .noLeakCheck()
                    .expectSize()
                    .returns("arr\n[[1.0,1.0],[2.0,2.0]]\n");
        });
    }

    @Test
    public void testCreateAsSelectDoubleNoWAL() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            create table blah as (
                            select rnd_varchar() a, rnd_double_array(1, 0) arr, rnd_int() b from long_sequence(10)
                            );"""
            );

            assertQuery("select * from blah")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a\tarr\tb
                            &\uDA1F\uDE98|\uD924\uDE04۲ӄǈ2L\t[0.6778564558839208,0.3100545983862456,0.38539947865244994,0.8799634725391621]\t-2119387831
                            衞͛Ԉ龘\t[0.6761934857077543,0.8912587536603974]\t458818940
                            o#/ZUA\t[0.7763904674818695,0.05048190020054388,0.8847591603509142,0.0011075361080621349,0.931192737286751,0.8258367614088108,0.8001121139739173,0.38642336707855873,0.92050039469858,0.16381374773748514,0.456344569609078]\t1857212401
                            \uDB8D\uDE4Eᯤ\\篸{\uD9D7\uDFE5\uDAE9\uDF46OF\t[0.9441658975532605,0.6806873134626418]\t-68027832
                            ?hhV4|\t[0.3901731258748704,0.03993124821273464,0.10643046345788132]\t1238491107
                            7="+z\t[0.9759534636690222,0.5893398488053903]\t-246923735
                            p-鳓w\t[0.8593131480724349,0.021189232728939578,0.10527282622013212]\t-1613687261
                            qRӽ\t[0.6797562990945702,0.8189713915910615,0.10459352312331183,0.7365115215570027,0.20585069039325443,0.9418719455092096]\t-623471113
                            E"+~M/8KS\t[0.17180291960857297,0.4416432347777828,0.2065823085842221,0.8584308438045006,0.2445295612285482]\t-1465751763
                            Gk珣zx6쪎\t[0.5780746276543334,0.40791879008699594,0.12663676991275652,0.21485589614090927]\t-365989785
                            """);
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
            assertQuery("CREATE TABLE tango (arr BYTE[])")
                    .noLeakCheck()
                    .fails(24, "unsupported array element type [type=BYTE]");
            assertQuery("CREATE TABLE tango (arr SHORT[])")
                    .noLeakCheck()
                    .fails(24, "unsupported array element type [type=SHORT]");
            assertQuery("CREATE TABLE tango (arr INT[])")
                    .noLeakCheck()
                    .fails(24, "unsupported array element type [type=INT]");
            assertQuery("CREATE TABLE tango (arr LONG[])")
                    .noLeakCheck()
                    .fails(24, "unsupported array element type [type=LONG]");
            assertQuery("CREATE TABLE tango (arr FLOAT[])")
                    .noLeakCheck()
                    .fails(24, "unsupported array element type [type=FLOAT]");
            assertQuery("CREATE TABLE tango (arr BOOLEAN[])")
                    .noLeakCheck()
                    .fails(24, "unsupported array element type [type=BOOLEAN]");
            assertQuery("CREATE TABLE tango (arr CHAR[])")
                    .noLeakCheck()
                    .fails(24, "unsupported array element type [type=CHAR]");
            assertQuery("CREATE TABLE tango (arr STRING[])")
                    .noLeakCheck()
                    .fails(24, "unsupported array element type [type=STRING]");
            assertQuery("CREATE TABLE tango (arr VARCHAR[])")
                    .noLeakCheck()
                    .fails(24, "unsupported array element type [type=VARCHAR]");
            assertQuery("CREATE TABLE tango (arr ARRAY[])")
                    .noLeakCheck()
                    .fails(24, "the system supports type-safe arrays, e.g. `type[]`. Supported types are: DOUBLE. More types incoming.");
            assertQuery("CREATE TABLE tango (arr BINARY[])")
                    .noLeakCheck()
                    .fails(24, "unsupported array element type [type=BINARY]");
            assertQuery("CREATE TABLE tango (arr DATE[])")
                    .noLeakCheck()
                    .fails(24, "unsupported array element type [type=DATE]");
            assertQuery("CREATE TABLE tango (arr TIMESTAMP[])")
                    .noLeakCheck()
                    .fails(24, "unsupported array element type [type=TIMESTAMP]");
            assertQuery("CREATE TABLE tango (arr UUID[])")
                    .noLeakCheck()
                    .fails(24, "unsupported array element type [type=UUID]");
            assertQuery("CREATE TABLE tango (arr LONG128[])")
                    .noLeakCheck()
                    .fails(24, "unsupported array element type [type=LONG128]");
            assertQuery("CREATE TABLE tango (arr GEOHASH[])")
                    .noLeakCheck()
                    .fails(24, "unsupported array element type [type=GEOHASH]");
            assertQuery("CREATE TABLE tango (arr DECIMAL[])")
                    .noLeakCheck()
                    .fails(24, "unsupported array element type [type=DECIMAL]");
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
            assertQuery("tango")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tuniq\tarr
                            1970-01-01T00:00:00.000001Z\t1\t[9.0,10.0,11.0]
                            1970-01-01T00:00:00.000002Z\t2\t[6.0,7.0,8.0]
                            """);
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
            assertQuery("SELECT a / b div FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            div
                            [0.5,null]
                            [0.75,null]
                            null
                            """);
        });
    }

    @Test
    public void testArrayBinaryOpNullArrayBeforeNonNull() throws Exception {
        // A null array result resets each element-wise operator's reused output
        // buffer; a following non-null row must still compute correctly. The null
        // row comes first here, so even a single scan exercises the null ->
        // non-null transition for every operator.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE[], b DOUBLE[])");
            execute("INSERT INTO tango VALUES " +
                    "(null, null), " +
                    "(ARRAY[2.0, 3.0], ARRAY[4.0, 6.0])");
            assertQuery("SELECT a / b aDivB, a * b aMulB, a + b aAddB, a - b aSubB FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            aDivB\taMulB\taAddB\taSubB
                            null\tnull\tnull\tnull
                            [0.5,0.5]\t[8.0,18.0]\t[6.0,9.0]\t[-2.0,-3.0]
                            """);
        });
    }

    @Test
    public void testDivNullArrayBeforeNonNull() throws Exception {
        // A null array result resets the function's reused output buffer; a
        // following non-null row must still compute correctly. Order matters:
        // the null row comes first here, so even a single scan exercises the
        // null -> non-null transition.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE[], b DOUBLE[])");
            execute("INSERT INTO tango VALUES " +
                    "(null, null), " +
                    "(ARRAY[2.0, 3.0], ARRAY[4.0, 6.0])");
            assertQuery("SELECT a / b div FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            div
                            null
                            [0.5,0.5]
                            """);
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
            assertQuery("SELECT a[1:2, 1:2, 1:2] / b[2:, 2:, 2:] div FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            div
                            [[[0.1]]]
                            null
                            """);
        });
    }

    @Test
    public void testDudupArrayAsKey() throws Exception {
        // when an array is part of the dedup key
        // it fails gracefully and with an informative error message
        assertQuery("CREATE TABLE tango (ts TIMESTAMP, arr DOUBLE[])" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL" +
                " DEDUP UPSERT KEYS (ts, arr)")
                .fails(107, "dedup key columns cannot include ARRAY [column=arr, type=DOUBLE[]]");
    }

    @Test
    public void testEmptyArray() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("SELECT ARRAY[]")
                    .noLeakCheck()
                    .expectSize()
                    .returns("ARRAY\n[]\n");
            assertQuery("SELECT * FROM (SELECT ARRAY[])")
                    .noLeakCheck()
                    .expectSize()
                    .returns("ARRAY\n[]\n");
            assertQuery("WITH q1 AS (SELECT ARRAY[]) SELECT * FROM q1")
                    .noLeakCheck()
                    .expectSize()
                    .returns("ARRAY\n[]\n");
            execute("CREATE TABLE tango AS (SELECT ARRAY[])");
            assertQuery("tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("ARRAY\n[]\n");
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
            assertQuery("SELECT (ARRAY[[1.0, 3]] = ARRAY[[1.0, 3]]) eq FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\ntrue\n");
            assertQuery("SELECT (ARRAY[[1.0, 3], [5.0, 7]] = ARRAY[[1.0, 3], [5.0, 7]]) eq FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\ntrue\n");
            assertQuery("SELECT (ARRAY[[1.0, 3]] = ARRAY[[1.0, 4]]) eq FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\nfalse\n");
            assertQuery("SELECT (ARRAY[[1.0, 3]] = ARRAY[[1.0, 3, 3]]) eq FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\nfalse\n");
            assertQuery("SELECT (ARRAY[[1.0, 3, 3]] = ARRAY[[1.0, 3]]) eq FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\nfalse\n");
            assertQuery("SELECT (ARRAY[[1.0, 3]] = ARRAY[1.0, 3]) eq FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\nfalse\n");
        });
    }

    @Test
    public void testEqualsColumnAndLiteral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][])");
            execute("INSERT INTO tango VALUES (ARRAY[[1.0, 3], [5.0, 7]])");
            assertQuery("SELECT (arr = ARRAY[[1.0, 3], [5.0, 7]]) eq FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\ntrue\n");
            assertQuery("SELECT (arr = ARRAY[[1.0, 4], [5.0, 7]]) eq FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\nfalse\n");
            assertQuery("SELECT (arr = ARRAY[[1.0, 3, 3], [5.0, 7, 9]]) eq FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\nfalse\n");
            assertQuery("SELECT (arr = ARRAY[[1.0, 3]]) eq FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\nfalse\n");

            assertQuery("SELECT (ARRAY[[1.0, 3], [5.0, 7]] = arr) eq FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\ntrue\n");
            assertQuery("SELECT (ARRAY[[1.0, 4], [5.0, 7]] = arr) eq FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\nfalse\n");
            assertQuery("SELECT (ARRAY[[1.0, 3, 3], [5.0, 7, 9]] = arr) eq FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\nfalse\n");
            assertQuery("SELECT (ARRAY[[1.0, 3]] = arr) eq FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\nfalse\n");
        });
    }

    @Test
    public void testEqualsDifferentDimensionality() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (left DOUBLE[][], right DOUBLE[])");
            execute("INSERT INTO tango VALUES (ARRAY[[1.0, 3]], ARRAY[1.0, 3])"
            );
            assertQuery("SELECT (left = right) eq FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\nfalse\n");
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
            assertQuery("SELECT (left = right) eq FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\ntrue\ntrue\nfalse\nfalse\nfalse\n");
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
            assertQuery("SELECT (left[2] = right[2]) eq FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\ntrue\ntrue\nfalse\n");
            assertQuery("SELECT (left[2:] = right[2:]) eq FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\ntrue\ntrue\nfalse\n");
            assertQuery("SELECT (left[1:2] = right[2:]) eq FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\nfalse\nfalse\ntrue\n");
        });
    }

    @Test
    public void testExplicitCastDimensionalityChange() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("SELECT ARRAY[1.0, 2.0]::double[][]")
                    .noLeakCheck()
                    .expectSize()
                    .returns("cast\n[[1.0,2.0]]\n");

            // no element arrays
            assertQuery(// arrays with no elements are always printed as []
                    "SELECT ARRAY[]::double[][]")
                    .noLeakCheck()
                    .expectSize()
                    .returns("cast\n[]\n");

            // casting to fewer dimensions is not allowed
            assertQuery("SELECT ARRAY[[1.0], [2.0]]::double[]")
                    .noLeakCheck()
                    .fails(26, "cannot cast array to lower dimension [from=DOUBLE[][] (2D), to=DOUBLE[] (1D)]. " +
                            "Use array flattening operation (e.g. 'flatten(arr)') instead");
        });
    }

    @Test
    public void testExplicitCastFromArrayToStr() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("SELECT ARRAY[1.0]::string FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            [1.0]
                            """);

            assertQuery("SELECT ARRAY[1.0, 2.0]::string FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            [1.0,2.0]
                            """);

            assertQuery("SELECT ARRAY[[1.0, 2.0], [3.0, 4.0]]::string FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            [[1.0,2.0],[3.0,4.0]]
                            """);

            // array with no elements is always printed as []
            assertQuery("SELECT ARRAY[[], []]::double[][]::string FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            []
                            """);

            // null case, 'assertSql()' prints 'null' as an empty string
            assertQuery("SELECT NULL::double[]::string FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            
                            """);
        });
    }

    @Test
    public void testExplicitCastFromArrayToVarchar() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("SELECT ARRAY[1.0]::varchar FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            [1.0]
                            """);

            assertQuery("SELECT ARRAY[1.0, 2.0]::varchar FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            [1.0,2.0]
                            """);

            assertQuery("SELECT ARRAY[[1.0, 2.0], [3.0, 4.0]]::varchar FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            [[1.0,2.0],[3.0,4.0]]
                            """);

            // array with no elements is always printed as []
            assertQuery("SELECT ARRAY[[], []]::double[][]::varchar FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            []
                            """);

            // null case, 'assertSql()' prints 'null' as an empty string
            assertQuery("SELECT NULL::double[]::varchar FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            
                            """);
        });
    }

    @Test
    public void testExplicitCastFromScalarToArray() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("SELECT 1.0::double[] FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            [1.0]
                            """);

            // null
            assertQuery("SELECT NULL::double::double[] FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            null
                            """);

            // 2D
            assertQuery("SELECT 1.0::double[][] FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            [[1.0]]
                            """);

            // 2D with null
            assertQuery("SELECT NULL::double::double[][] FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            null
                            """);
        });
    }

    @Test
    public void testExplicitCastFromStrToArray() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("SELECT '{1, 2}'::double[] FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            [1.0,2.0]
                            """);

            // quoted elements
            assertQuery("SELECT '{\"1\", \"2\"}'::double[] FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            [1.0,2.0]
                            """);

            // quoted elements with spaces, 2D array
            assertQuery("SELECT '{{\"1\", \"2\"}, {\"3\", \"4\"}}'::double[][] FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            [[1.0,2.0],[3.0,4.0]]
                            """);

            // 2D array
            assertQuery("SELECT '{{1,2}, {3, 4}}'::double[][] FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            [[1.0,2.0],[3.0,4.0]]
                            """);

            // 2D array with null - nulls are not allowed, casting fails and explicit casting produces NULL on the output
            assertQuery("SELECT '{{1,2}, {3, NULL}}'::double[][] FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            null
                            """);

            // empty arrays are always printed as [], regardless of dimensionality. at least of now. this may change.
            assertQuery("SELECT '{{}, {}}'::double[][] FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            []
                            """);

            // empty array can be cast to higher dimensionality -> empty array
            assertQuery("SELECT '{}'::double[][] FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            []
                            """);

            // but empty array cannot cast to lower dimensionality -> NULL
            assertQuery("SELECT '{{},{}}'::double[] FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            null
                            """);

            assertQuery("SELECT NULL::double[] FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            null
                            """);

            assertQuery("SELECT 'not an array'::double[] FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            null
                            """);

            // 2D array explicitly cast to 1D array -> null
            assertQuery("SELECT '{{1,2}, {3, 4}}'::double[] FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            null
                            """);

            assertQuery("SELECT '{nonsense, 2}'::double[] FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            cast
                            null
                            """);

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
            assertQuery("SELECT arr1 FROM tango WHERE arr1 = arr2")
                    .noLeakCheck()
                    .returns("arr1\n[5.0,6.0]\n");
        });
    }

    @Test
    public void testFlatten() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][][])");
            execute("INSERT INTO tango VALUES (ARRAY[[[1.0, 2, 3], [4.0, 5, 6]], [[7.0, 8, 9], [10.0, 11, 12]]])");
            assertQuery("SELECT flatten(arr) arr FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("arr\n[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0,11.0,12.0]\n");
            assertQuery("SELECT arr[1:, 1:, 2:4] arr FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("arr\n[[[2.0,3.0],[5.0,6.0]],[[8.0,9.0],[11.0,12.0]]]\n");
            assertQuery("SELECT flatten(arr[1:, 1:, 2:4]) arr FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("arr\n[2.0,3.0,5.0,6.0,8.0,9.0,11.0,12.0]\n");
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

            assertQuery("""
                    select arr, count(*)
                    from tango
                    group by arr;""")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            arr\tcount
                            [[1.0,2.0],[3.0,4.0]]\t2
                            [[1.0,2.0],[3.0,4.1]]\t1
                            [[1.0,2.0],[3.0,4.0],[5.0,6.0]]\t1
                            null\t1
                            """);
        });
    }

    @Test
    public void testGroupByOnSliceKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][], i int)");
            execute("INSERT INTO tango VALUES (ARRAY[[1.0, 2.0], [3.0, 4.0], [5, 6]], 1)");
            execute("INSERT INTO tango VALUES (ARRAY[[1.0, 2.0], [33.0, 4.0]], 3)");
            execute("INSERT INTO tango VALUES (ARRAY[[2.0, 3.0], [1.0, 4.0]], 5)");

            assertQuery("""
                    select arr[1:2], sum(i)
                    from tango
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            []\tsum
                            [[1.0,2.0]]\t4
                            [[2.0,3.0]]\t5
                            """);

        });
    }

    @Test
    public void testInsertAsSelectDoubleNoWAL() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE blah (a DOUBLE[][])");
            execute("INSERT INTO blah SELECT rnd_double_array(2, 2) FROM long_sequence(10)");

            assertQuery("select * from blah")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a
                            [[null,0.20447441837877756],[null,null]]
                            [[0.3491070363730514,0.7611029514995744],[0.4217768841969397,null],[0.7261136209823622,0.4224356661645131],[null,0.3100545983862456],[0.1985581797355932,0.33608255572515877],[0.690540444367637,null],[0.21583224269349388,0.15786635599554755],[null,null],[0.12503042190293423,null],[0.9687423276940171,null],[null,null],[null,null],[null,null],[0.7883065830055033,null],[0.4138164748227684,0.5522494170511608],[0.2459345277606021,null]]
                            [[0.7643643144642823,null],[null,null],[0.18769708157331322,0.16381374773748514],[0.6590341607692226,null],[null,null],[0.8837421918800907,0.05384400312338511],[null,0.7230015763133606],[0.12105630273556178,null],[0.5406709846540508,null],[0.9269068519549879,null],[null,null],[0.1202416087573498,null]]
                            [[null,null,0.4971342426836798,null],[0.5065228336156442,null,null,0.03167026265669903],[null,null,0.2879973939681931,null],[null,null,null,0.24008362859107102]]
                            [[0.2185865835029681,null],[0.24079155981438216,0.10643046345788132],[0.5244255672762055,0.0171850098561398],[0.09766834710724581,null],[0.053594208204197136,0.26369335635512836],[0.22895725920713628,0.9820662735672192],[null,0.32424562653969957],[0.8998921791869131,null],[null,null],[0.33746104579374825,0.18740488620384377],[0.10527282622013212,0.8291193369353376],[0.32673950830571696,null],[0.18336217509438513,0.9862476361578772],[0.8693768930398866,0.8189713915910615]]
                            [[0.29659296554924697,0.24642266252221556],[null,null],[null,0.13264292470570205],[0.38422543844715473,null],[null,null],[null,0.7668146556860689],[null,0.05158459929273784],[null,null]]
                            [[0.3568111021227658,0.05758228485190853,0.6729405590773638,null,0.5716129058692643],[0.05094182589333662,null,null,0.4609277382153818,0.5691053034055052],[0.12663676991275652,0.11371841836123953,null,null,0.7203170014947307],[null,null,null,null,0.7704949839249925],[0.8144207168582307,null,null,null,0.2836347139481469]]
                            [[0.08675950660182763,null],[0.741970173888595,0.25353478516307626],[0.2739985338660311,null],[0.8001632261203552,null],[0.7404912278395417,0.08909442703907178],[0.8439276969435359,null],[null,0.08712007604601191]]
                            [[0.5637742551872849,null],[null,null],[0.7195457109208119,null],[0.23493793601747937,null],[0.6334964081687151,0.6721404635638454]]
                            [[0.17405556853190263,0.823395724427589,null,0.8108032283138068,null,null,0.7530494527849502,0.49153268154777974,0.0024457698760806945,0.29168465906260244,0.3121271759430503,0.3004874521886858],[null,0.7653255982993546,null,null,null,null,0.37873228328689634,null,0.7272119755925095,null,0.7467013668130107,0.5794665369115236],[null,0.5308756766878475,0.03192108074989719,null,0.17498425722537903,null,0.34257201464152764,null,null,0.29242748475227853,null,0.11296257318851766],[null,0.23405440872043592,0.1479745625593103,null,0.8115426881784433,null,0.32093405888189597,null,0.04321289940104611,0.8217652538598936,0.6397125243912908,0.29419791719259025],[0.865629565918467,null,null,0.16923843067953104,0.7198854503668188,0.5174107449677378,0.38509066982448115,null,null,null,0.5475429391562822,0.6977332212252165],[null,null,0.4268921400209912,0.9997797234031688,0.5234892454427748,null,null,null,null,0.5169565007469263,0.7039785408034679,0.8461211697505234],[null,0.537020248377422,0.8766908646423737,null,null,0.31852531484741486,null,0.605050319285447,0.9683642405595932,0.3549235578142891,0.04211401699125483,null],[null,0.0032519916115479885,0.2703179181043681,0.729536610842768,0.3317641556575974,0.8895915828662114,null,null,null,null,0.1599211504269954,0.5251698097331752],[null,0.18442756220221035,null,0.48422587819911567,0.2970515836513553,null,0.7826107801293182,null,0.3218450864634881,0.8034049105590781,null,null],[0.40425101135606667,0.9412663583926286,null,null,0.8376764297590714,0.15241451173695408,null,0.743599174001969,null,null,0.9001273812517414,0.5629104624260136],[0.6001215594928115,0.8920252905736616,0.09977691656157406,null,0.2862717364877081,null,null,null,0.8853675629694284,4.945923013344178E-5,null,0.0016532800623808575]]
                            """);
        });
    }

    @Test
    public void testInsertAsSelectLiteral1d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE, b DOUBLE)");
            execute("CREATE TABLE samba (arr DOUBLE[])");
            execute("INSERT INTO tango VALUES (1.0, 2.0)");
            execute("INSERT INTO samba SELECT ARRAY[a, b] FROM tango");
            assertQuery("samba")
                    .noLeakCheck()
                    .expectSize()
                    .returns("arr\n[1.0,2.0]\n");
        });
    }

    @Test
    public void testInsertAsSelectLiteral2d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE, b DOUBLE)");
            execute("CREATE TABLE samba (arr DOUBLE[][])");
            execute("INSERT INTO tango VALUES (1.0, 2.0)");
            execute("INSERT INTO samba SELECT ARRAY[[a, a], [b, b]] FROM tango");
            assertQuery("samba")
                    .noLeakCheck()
                    .expectSize()
                    .returns("arr\n[[1.0,1.0],[2.0,2.0]]\n");
        });
    }

    @Test
    public void testInsertEmpty1d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[])");
            execute("INSERT INTO tango VALUES (ARRAY[])");
            assertQuery("tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("arr\n[]\n");
        });
    }

    @Test
    public void testInsertEmpty2d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][])");
            execute("INSERT INTO tango VALUES (ARRAY[[]])");
            execute("INSERT INTO tango VALUES (ARRAY[[],[]])");
            execute("INSERT INTO tango VALUES (ARRAY[[],[],[]])");
            assertQuery("tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("arr\n[]\n[]\n[]\n");
        });
    }

    @Test
    public void testInsertEmpty3d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][][])");
            execute("INSERT INTO tango VALUES (ARRAY[[[]]])");
            execute("INSERT INTO tango VALUES (ARRAY[[[]],[[]]])");
            execute("INSERT INTO tango VALUES (ARRAY[[[],[]]])");
            assertQuery("tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("arr\n[]\n[]\n[]\n");
        });
    }

    @Test
    public void testInsertNonVanilla() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[])");
            execute("INSERT INTO tango VALUES (ARRAY[1.0, 2, 3][2:])");
            assertQuery("tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("arr\n[2.0,3.0]\n");
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
            assertQuery("SELECT " +
                    "insertion_point(arr1, 8, false) i1, " +
                    "insertion_point(arr1, 2000, false) i2, " +
                    "insertion_point(arr1, 9, false) i3, " +
                    "insertion_point(arr1, 1001, false) i4, " +
                    "insertion_point(arr1, 18, false) i5, " +
                    "insertion_point(arr1, 22, false) i6, " +
                    "insertion_point(arr1[1:], 1000, false) i7, " +
                    "FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            i1\ti2\ti3\ti4\ti5\ti6\ti7
                            1\t11\t2\t11\t4\t8\t10
                            11\t1\t11\t2\t8\t7\t3
                            null\tnull\tnull\tnull\tnull\tnull\tnull
                            """);
            assertQuery("SELECT " +
                    "insertion_point(arr1, 8, true) i1, " +
                    "insertion_point(arr1, 2000, true) i2, " +
                    "insertion_point(arr1, 9, true) i3, " +
                    "insertion_point(arr1, 1001, true) i4, " +
                    "insertion_point(arr1, 18, true) i5, " +
                    "insertion_point(arr1, 22, true) i6, " +
                    "insertion_point(arr1[1:], 1000, true) i7, " +
                    "FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            i1\ti2\ti3\ti4\ti5\ti6\ti7
                            1\t11\t1\t10\t4\t5\t9
                            11\t1\t10\t1\t8\t4\t2
                            null\tnull\tnull\tnull\tnull\tnull\tnull
                            """);
            assertQuery("SELECT " +
                    "insertion_point(arr2[1], 8) i1, " +
                    "insertion_point(arr2[1], 2000) i2, " +
                    "insertion_point(arr2[1], 9) i3, " +
                    "insertion_point(arr2[1], 1001) i4, " +
                    "insertion_point(arr2[1], 18) i5, " +
                    "insertion_point(arr2[1], 22) i6, " +
                    "insertion_point(arr2[1, 1:], 1000) i7, " +
                    "FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            i1\ti2\ti3\ti4\ti5\ti6\ti7
                            1\t11\t2\t11\t4\t8\t10
                            11\t1\t11\t2\t8\t7\t3
                            null\tnull\tnull\tnull\tnull\tnull\tnull
                            """);

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
            assertQuery("SELECT " +
                    "insertion_point(transpose(arr)[1], 8, false) i1, " +
                    "insertion_point(transpose(arr)[1], 2000, false) i2, " +
                    "insertion_point(transpose(arr)[1], 9, false) i3, " +
                    "insertion_point(transpose(arr)[1], 1001, false) i4, " +
                    "insertion_point(transpose(arr)[1], 18, false) i5, " +
                    "insertion_point(transpose(arr)[1], 22, false) i6, " +
                    "insertion_point(transpose(arr)[1, 1:], 1000, false) i7, " +
                    "FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            i1\ti2\ti3\ti4\ti5\ti6\ti7
                            1\t11\t2\t11\t4\t8\t10
                            11\t1\t11\t2\t8\t7\t3
                            null\tnull\tnull\tnull\tnull\tnull\tnull
                            """);
            assertQuery("SELECT " +
                    "insertion_point(transpose(arr)[1], 8, true) i1, " +
                    "insertion_point(transpose(arr)[1], 2000, true) i2, " +
                    "insertion_point(transpose(arr)[1], 9, true) i3, " +
                    "insertion_point(transpose(arr)[1], 1001, true) i4, " +
                    "insertion_point(transpose(arr)[1], 18, true) i5, " +
                    "insertion_point(transpose(arr)[1], 22, true) i6, " +
                    "insertion_point(transpose(arr)[1][1:], 1000, true) i7, " +
                    "FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            i1\ti2\ti3\ti4\ti5\ti6\ti7
                            1\t11\t1\t10\t4\t5\t9
                            11\t1\t10\t1\t8\t4\t2
                            null\tnull\tnull\tnull\tnull\tnull\tnull
                            """);
            assertQuery("SELECT " +
                    "insertion_point(transpose(arr)[1], 8) i1, " +
                    "insertion_point(transpose(arr)[1], 2000) i2, " +
                    "insertion_point(transpose(arr)[1], 9) i3, " +
                    "insertion_point(transpose(arr)[1], 1001) i4, " +
                    "insertion_point(transpose(arr)[1], 18) i5, " +
                    "insertion_point(transpose(arr)[1], 22) i6, " +
                    "insertion_point(transpose(arr)[1, 1:], 1000) i7, " +
                    "FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            i1\ti2\ti3\ti4\ti5\ti6\ti7
                            1\t11\t2\t11\t4\t8\t10
                            11\t1\t11\t2\t8\t7\t3
                            null\tnull\tnull\tnull\tnull\tnull\tnull
                            """);
        });
    }

    @Test
    public void testInsertTransposed() throws Exception {
        assertMemoryLeak(() -> {
            String original = "[[1.0,2.0],[3.0,4.0],[5.0,6.0]]";
            String transposed = "[[1.0,3.0,5.0],[2.0,4.0,6.0]]";
            execute("CREATE TABLE tango AS (SELECT ARRAY" + original + " arr FROM long_sequence(1))");
            execute("INSERT INTO tango SELECT transpose(arr) FROM tango");
            assertQuery("SELECT arr FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("arr\n" + original + '\n' + transposed + '\n');
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
            assertQuery("SELECT dim_length(arr, 1) len FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("len\n1\n3\n2\n");
            assertQuery("SELECT dim_length(arr, 2) len FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("len\n2\n2\n3\n");
            assertQuery("SELECT dim_length(arr, arr[1, 1]::int) len FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("len\n1\n2\n3\n");
        });
    }

    @Test
    public void testLengthColumnTop() throws Exception {
        // A column added to a table that already has rows has a column top: the rows below it hold no
        // data for the column, and the page frame hands out a zero aux address for them. dim_length()
        // reads the shape header straight out of the aux/data vectors, so it has to answer NULL for
        // those rows rather than reading from address zero. The pre-existing rows below the top are
        // the only ones that reach that branch.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP, arr DOUBLE[][]) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tango VALUES " +
                    "('1970-01-01T00:00:00.000000Z', ARRAY[[1.0, 2], [3.0, 4], [5.0, 6]]), " +
                    "('1970-01-01T00:00:01.000000Z', ARRAY[[1.0, 2], [3.0, 4], [5.0, 6]])"
            );
            execute("ALTER TABLE tango ADD COLUMN arr2 DOUBLE[][]");
            execute("INSERT INTO tango VALUES " +
                    "('1970-01-01T00:00:02.000000Z', ARRAY[[1.0, 2]], ARRAY[[1.0, 2, 3], [4.0, 5, 6]])"
            );
            // The first two rows sit below the column top: no shape to read, so NULL.
            assertQuery("SELECT dim_length(arr2, 1) len FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("len\nnull\nnull\n2\n");
            assertQuery("SELECT dim_length(arr2, 2) len FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("len\nnull\nnull\n3\n");
        });
    }

    @Test
    public void testLengthConstantArrayInvalidDim() throws Exception {
        // A constant array argument makes the whole call constant, so the parser folds it by calling
        // getInt(null) - and folding never runs init(), which is where the dimensionality check used
        // to live. The check therefore has to happen at compile time, in newInstance(). A NULL array
        // is no different: a dimension the array does not have is out of bounds whether or not there
        // is an array to measure, so it must report the error rather than quietly answer NULL.
        assertMemoryLeak(() -> {
            assertExceptionNoLeakCheck("SELECT dim_length(ARRAY[1.0, 2, 3], 2)",
                    36, "array dimension out of bounds [dim=2, dims=1]");
            assertExceptionNoLeakCheck("SELECT dim_length(ARRAY[[1.0, 2], [3.0, 4]], 3)",
                    45, "array dimension out of bounds [dim=3, dims=2]");
            assertExceptionNoLeakCheck("SELECT dim_length(NULL::double[], 2)",
                    34, "array dimension out of bounds [dim=2, dims=1]");
            assertExceptionNoLeakCheck("SELECT dim_length(NULL::double[][], 3)",
                    36, "array dimension out of bounds [dim=3, dims=2]");
            // In bounds, the constant array still folds and answers.
            assertQuery("SELECT dim_length(ARRAY[[1.0, 2], [3.0, 4], [5.0, 6]], 2) len")
                    .noLeakCheck()
                    .expectSize()
                    .returns("len\n2\n");
            // In bounds over a NULL array: no shape to read, so NULL.
            assertQuery("SELECT dim_length(NULL::double[][], 2) len")
                    .noLeakCheck()
                    .expectSize()
                    .returns("len\nnull\n");
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
    public void testLengthNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][], n INT)");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[[1.0, 2], [3.0, 4], [5.0, 6]], 1), " +
                    "(NULL, 1), " +
                    "(ARRAY[[1.0, 2], [3.0, 4], [5.0, 6]], NULL), " +
                    "(NULL, NULL)"
            );
            // A NULL array carries no shape, so there is no length to report and dim_length() returns
            // NULL. It must not read the shape it does not have.
            assertQuery("SELECT dim_length(arr, 1) len FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("len\n3\nnull\n3\nnull\n");
            // Same, for a non-constant dimension, which takes the other of the two function paths.
            // A NULL dimension is not an out-of-range dimension: it is the absence of a dimension to
            // measure, so it returns NULL instead of failing the query. This matches the sibling
            // array-access function, where arr[NULL] is NULL.
            assertQuery("SELECT dim_length(arr, n) len FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("len\n3\nnull\nnull\nnull\n");
            // Same, with the dimension NULL at compile time, which takes the constant path.
            assertQuery("SELECT dim_length(arr, NULL::int) len FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("len\nnull\nnull\nnull\nnull\n");
            // The constant path folds to a NULL constant and keeps neither argument, so it must free
            // the array argument itself. A constant array literal holds native memory, so dropping
            // that close() leaks it, and assertMemoryLeak() catches it here.
            assertQuery("SELECT dim_length(ARRAY[[1.0, 2], [3.0, 4]], NULL::int) len FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("len\nnull\n");
        });
    }

    @Test
    public void testLengthOverArrayExpression() throws Exception {
        // Every other dim_length() test passes a plain array column, which takes the shape-header fast
        // path and never builds an ArrayView. An array-valued expression has no column index, so both
        // function paths fall back to the ArrayView route instead - the only route that can see a NULL
        // ArrayView, which carries no shape and so has no length to report.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][], n INT)");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[[1.0, 2], [3.0, 4], [5.0, 6]], 1), " +
                    "(NULL, 1), " +
                    "(NULL, NULL)"
            );
            // Constant dimension: the ConstFunc path.
            assertQuery("SELECT dim_length(transpose(arr), 1) len FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("len\n2\nnull\nnull\n");
            assertQuery("SELECT dim_length(transpose(arr), 2) len FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("len\n3\nnull\nnull\n");
            // Non-constant dimension: the Func path.
            assertQuery("SELECT dim_length(transpose(arr), n) len FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("len\n2\nnull\nnull\n");
        });
    }

    @Test
    public void testLengthOverConstantFalseWindowJoin() throws Exception {
        // A WINDOW JOIN whose ON clause folds to a constant false wraps the master in an
        // ExtraNullColumnCursorFactory, which splices a synthetic NULL column in for every
        // aggregate of the vacant right side. That record has no array to hand out for the
        // spliced columns, so both direct array accessors have to report the array as NULL
        // instead of reaching into an ArrayView that is not there.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE prices (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO trades VALUES " +
                    "('2023-01-01T09:10:00.000000Z', 'AAA', 100.0), " +
                    "('2023-01-01T09:11:00.000000Z', 'BBB', 200.0)");
            execute("INSERT INTO prices VALUES ('2023-01-01T09:00:00.000000Z', 'AAA', 1.0)");
            final String join = "SELECT t.ts ts, array_agg(p.price) arr FROM trades t " +
                    "WINDOW JOIN prices p ON (0 = 1) " +
                    "RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING";
            // dim_length() over the spliced aggregate: the getArrayDimLen() accessor.
            assertQuery("SELECT ts, dim_length(arr, 1) len FROM (" + join + ")")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tlen
                            2023-01-01T09:10:00.000000Z\tnull
                            2023-01-01T09:11:00.000000Z\tnull
                            """);
            // Indexing the same spliced aggregate: the getArrayDouble1d2d() accessor, which
            // reads the array the same way and so shares the fault.
            assertQuery("SELECT ts, arr[1] x FROM (" + join + ")")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tx
                            2023-01-01T09:10:00.000000Z\tnull
                            2023-01-01T09:11:00.000000Z\tnull
                            """);
            // Both accessors above are guarded in Record itself, so they would still return NULL
            // even if the record handed out a Java null. array_sum() takes the ArrayView route,
            // which reads the record's array unguarded, and so is the one that pins the record.
            assertQuery("SELECT ts, array_sum(arr) total FROM (" + join + ")")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\ttotal
                            2023-01-01T09:10:00.000000Z\tnull
                            2023-01-01T09:11:00.000000Z\tnull
                            """);
        });
    }

    @Test
    public void testLengthOverLateMaterializedParquetFrame() throws Exception {
        // PageFrameFilteredMemoryRecord is the record an async GROUP BY reducer puts in front of a
        // parquet frame when it late-materializes: it decodes the filter columns, runs the filter, and
        // only then reads the rest, mapping each surviving row back to its physical index. Its direct
        // array accessors have to apply that mapping - one that forwarded the filtered index straight
        // to the shape header would read a different row's array, and one that fell through to
        // Record's ArrayView default would silently lose the O(1) shape read.
        //
        // Reaching it needs all three: a parquet frame (AsyncFilterContext.shouldUseLateMaterialization
        // returns false for native ones), an aggregate (the plain filter path uses the unfiltered
        // record), and a filter over a column the projection does not otherwise read. A WHERE over a
        // native table meets none of them.
        //
        // The two halves carry different shapes, and arr[1, 1] carries the row's own x, so a wrong row
        // mapping shows up as the wrong dimension length or the wrong sum rather than as no rows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP, tag SYMBOL, arr DOUBLE[][]) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tango SELECT (x * 1_000_000)::timestamp, " +
                    "CASE WHEN x % 1000 = 0 THEN 'keep' ELSE 'drop' END, " +
                    "ARRAY[[x::double, 2.0]] FROM long_sequence(5_000)");
            execute("INSERT INTO tango SELECT ((5_000 + x) * 1_000_000)::timestamp, " +
                    "CASE WHEN x % 1000 = 0 THEN 'keep' ELSE 'drop' END, " +
                    "ARRAY[[x::double], [2.0], [3.0]] FROM long_sequence(5_000)");
            // A row in a later partition seals the first one, which is what CONVERT needs.
            execute("INSERT INTO tango VALUES ('1970-01-02T00:00:00.000000Z', 'drop', ARRAY[[1.0]])");
            execute("ALTER TABLE tango CONVERT PARTITION TO PARQUET LIST '1970-01-01'");
            // Five rows survive the filter in each half: x = 1000..5000 step 1000.
            assertQuery("SELECT dim_length(arr, 1) d1, count(*) c, sum(arr[1, 1]) total " +
                    "FROM tango WHERE tag = 'keep' GROUP BY d1 ORDER BY d1")
                    .noLeakCheck()
                    .expectSize()
                    // The async reducer is the only thing that installs the filtered record. Pinned by
                    // the substring the JIT and non-JIT plans share, since jit mode varies by build.
                    .withPlanContaining("Group By workers:", "filter: tag='keep'")
                    .returns("""
                            d1\tc\ttotal
                            1\t5\t15000.0
                            3\t5\t15000.0
                            """);
        });
    }

    @Test
    public void testLengthOverLateMaterializedParquetNullAndColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP, marker SYMBOL, tag SYMBOL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tango SELECT (x * 1_000_000)::timestamp, " +
                    "CASE WHEN x = 1000 THEN 'a_top' ELSE 'drop' END, " +
                    "CASE WHEN x = 1000 THEN 'keep' ELSE 'drop' END FROM long_sequence(5_000)");
            execute("ALTER TABLE tango ADD COLUMN arr DOUBLE[][]");
            execute("""
                    INSERT INTO tango VALUES
                        ('1970-01-01T01:23:21.000000Z', 'b_null', 'keep', NULL),
                        ('1970-01-01T01:23:22.000000Z', 'c_value', 'keep', ARRAY[[1.0, 2], [3.0, 4]])
                    """);
            execute("INSERT INTO tango SELECT ((5_002 + x) * 1_000_000)::timestamp, 'drop', 'drop', " +
                    "ARRAY[[x::double]] FROM long_sequence(5_000)");
            execute("INSERT INTO tango VALUES " +
                    "('1970-01-02T00:00:00.000000Z', 'drop', 'drop', ARRAY[[1.0]])");
            execute("ALTER TABLE tango CONVERT PARTITION TO PARQUET LIST '1970-01-01'");

            assertQuery("SELECT marker, dim_length(arr, 1) d1, dim_length(arr, 2) d2, " +
                    "sum(arr[1, 1]) first FROM tango WHERE tag = 'keep' GROUP BY marker, d1, d2 ORDER BY marker")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Group By workers:", "filter: tag='keep'")
                    .returns("""
                            marker\td1\td2\tfirst
                            a_top\tnull\tnull\tnull
                            b_null\tnull\tnull\tnull
                            c_value\t2\t2\t1.0
                            """);
        });
    }

    @Test
    public void testLengthOverParquet() throws Exception {
        // The parquet scan rebuilds the aux vector in the same layout the native one uses, so the
        // shape-header fast path is meant to read it identically. Nothing pinned that: every other
        // dim_length() test runs over a native frame, so a parquet aux layout change would go
        // unnoticed until it returned wrong lengths. The last row stays native, which puts both frame
        // types in one scan.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP, arr DOUBLE[][]) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tango VALUES " +
                    "('1970-01-01T00:00:00.000000Z', ARRAY[[1.0, 2, 3], [4.0, 5, 6]]), " +
                    "('1970-01-01T00:00:01.000000Z', ARRAY[[1.0, 2]]), " +
                    "('1970-01-01T00:00:02.000000Z', NULL), " +
                    "('1970-01-02T00:00:00.000000Z', ARRAY[[1.0], [2.0], [3.0]])"
            );
            execute("ALTER TABLE tango CONVERT PARTITION TO PARQUET LIST '1970-01-01'");
            assertQuery("SELECT dim_length(arr, 1) d1, dim_length(arr, 2) d2, arr[1, 1] first FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            d1\td2\tfirst
                            2\t3\t1.0
                            1\t2\t1.0
                            null\tnull\tnull
                            3\t1\t1.0
                            """);
        });
    }

    @Test
    public void testLengthOverParquetOneDimension() throws Exception {
        testLengthOverParquetOneDimension(true);
        testLengthOverParquetOneDimension(false);
    }

    @Test
    public void testLengthReadsShapeWithoutMaterializingArray() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[][], n INT)");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[[1.0, 2, 3], [4.0, 5, 6]], 1), " +
                    "(ARRAY[[1.0, 2]], 2), " +
                    "(NULL, 3)"
            );
            // In a filter the function reads straight off the page frame, which is the path that
            // reads the shape header directly instead of materializing the array.
            assertQuery("SELECT n FROM tango WHERE dim_length(arr, 2) = 3")
                    .noLeakCheck()
                    .returns("n\n1\n");
            // A NULL array has no shape, so it matches no length.
            assertQuery("SELECT n FROM tango WHERE dim_length(arr, 1) IS NULL")
                    .noLeakCheck()
                    .returns("n\n3\n");
        });
    }

    @Test
    public void testLevelTwoPrice1D() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ask_price DOUBLE[], ask_size DOUBLE[])");
            execute("INSERT INTO tango VALUES (ARRAY[1.0, 2], ARRAY[1.0, 1])");
            assertQuery("SELECT l2price(1.0, ask_size, ask_price) l2 FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("l2\n1.0\n");
            assertQuery("SELECT l2price(2.0, ask_size, ask_price) l2 FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("l2\n1.5\n");
        });
    }

    @Test
    public void testLevelTwoPrice2D() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ask DOUBLE[][])");
            execute("INSERT INTO tango VALUES (ARRAY[[1.0, 1], [1.0, 2]])");
            assertQuery("SELECT l2price(1.0, ask[1], ask[2]) l2 FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("l2\n1.0\n");
            assertQuery("SELECT l2price(2.0, ask[1], ask[2]) l2 FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("l2\n1.5\n");
        });
    }

    @Test
    public void testMatView() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test (ts timestamp, x int, v double[]) timestamp(ts) partition by DAY WAL dedup upsert keys (ts, x) ");
            execute("create materialized view test_mv as select ts, x, first(v) as v from test sample by 1s");
            execute("insert into test(ts,x,v) values ('2022-02-24', 1, ARRAY[1.0,1.0]), ('2022-02-24', 2, null), ('2022-02-24', 3, ARRAY[2.0,2.0])");

            drainWalAndMatViewQueues();

            assertQuery("test")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\tv
                            2022-02-24T00:00:00.000000Z\t1\t[1.0,1.0]
                            2022-02-24T00:00:00.000000Z\t2\tnull
                            2022-02-24T00:00:00.000000Z\t3\t[2.0,2.0]
                            """);
        });
    }

    @Test
    public void testMatrixMultiply() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (left DOUBLE[][], right DOUBLE[][])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[[1.0, 3]], ARRAY[[5.0], [7.0]]), " +
                    "(ARRAY[[1.0, 1, 1], [2.0, 2, 2]], ARRAY[[3.0], [5.0], [7.0]])");
            assertQuery("SELECT matmul(left, right) AS product FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("product\n[[26.0]]\n[[15.0],[30.0]]\n");
        });
    }

    @Test
    public void testMatrixMultiplyAutoBroadcasting() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT " +
                    "ARRAY[[2.0, 3.0],[4.0, 5.0], [6.0, 7.0]] left, ARRAY[1.0, 2.0] right " +
                    "FROM long_sequence(1))");
            assertQuery("SELECT matmul(left, right) AS product FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            product
                            [[8.0],[14.0],[20.0]]
                            """);
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
            assertQuery("SELECT matmul(arr, transpose(arr)) product FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("product\n[[5.0,11.0,17.0],[11.0,25.0,39.0],[17.0,39.0,61.0]]\n");
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
            assertQuery("SELECT a[2:] * b[2:] product FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("product\n[15.0]\n[63.0]\nnull\n");
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
            assertQuery("SELECT a[1:2, 1:2, 1:2] * b[2:, 2:, 2:] product FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("product\n[[[34.0]]]\nnull\n");
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

            assertQuery("SELECT - a + 8, (- b + 4.0) * 2.0, - b[1] + 2.0, - b[2:] + 2.0 FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column\tcolumn1\tcolumn2\tcolumn3
                            [6.0,null]\t[[4.0,0.0],[0.0,-8.0]]\t[0.0,-2.0]\t[[-2.0,-6.0]]
                            [-8.0,8.0]\t[[-8.0,0.0]]\t[-6.0,-2.0]\t[]
                            null\tnull\tnull\tnull
                            """);
            assertQuery("SELECT - transpose(a) + 16.0, - transpose(b) + 16.0 FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column\tcolumn1
                            [14.0,null]\t[[14.0,12.0],[12.0,8.0]]
                            [0.0,16.0]\t[[8.0],[12.0]]
                            null\tnull
                            """);
            assertQuery("SELECT - a + 0.0, - a + null::double FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column\tcolumn1
                            [-2.0,null]\t[null,null]
                            [-16.0,0.0]\t[null,null]
                            null\tnull
                            """);
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
            assertQuery("SELECT (left != right) eq FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\nfalse\nfalse\ntrue\ntrue\ntrue\n");
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
        assertQuery("tango")
                .noLeakCheck()
                .expectSize()
                .returns("arr\nnull\n");
        assertQuery("SELECT arr FROM tango")
                .noLeakCheck()
                .expectSize()
                .returns("arr\nnull\n");
        assertQuery("SELECT transpose(arr) arr FROM tango")
                .noLeakCheck()
                .expectSize()
                .returns("arr\nnull\n");
        assertQuery("SELECT l2price(1.0, arr, arr) arr FROM tango")
                .noLeakCheck()
                .expectSize()
                .returns("arr\nnull\n");
    }

    @Test
    public void testNullArrayComparisons() throws Exception {
        assertMemoryLeak(() -> {
            assertQuery("SELECT (null::double[] = null::double[]) eq FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\ntrue\n");
            assertQuery("SELECT (null::double[] = NaN::double[]) eq FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\ntrue\n");
            assertQuery("SELECT (NaN::double[] = null::double[]) eq FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\ntrue\n");
            assertQuery("SELECT (NaN::double[] = NaN::double[]) eq FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\ntrue\n");

            assertQuery("SELECT (null::double[] = ARRAY[1.0, 2.0]) eq FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\nfalse\n");
            assertQuery("SELECT (ARRAY[1.0, 2.0] = null::double[]) eq FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\nfalse\n");
            assertQuery("SELECT (NaN::double[] = ARRAY[1.0, 2.0]) eq FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\nfalse\n");
            assertQuery("SELECT (ARRAY[1.0, 2.0] = NaN::double[]) eq FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\nfalse\n");

            assertQuery("SELECT (ARRAY[null::double, null::double] = ARRAY[null::double, null::double]) eq FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\ntrue\n");
            assertQuery("SELECT (ARRAY[NaN, NaN] = ARRAY[NaN, NaN]) eq FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\ntrue\n");
            assertQuery("SELECT (ARRAY[null::double, NaN] = ARRAY[null::double, NaN]) eq FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\ntrue\n");
            assertQuery("SELECT (ARRAY[NaN, null::double] = ARRAY[NaN, null::double]) eq FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\ntrue\n");

            assertQuery("SELECT (ARRAY[1.0, 2.0] = ARRAY[NaN, 2.0]) eq FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\nfalse\n");
            assertQuery("SELECT (ARRAY[1.0, null::double] = ARRAY[1.0, 2.0]) eq FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("eq\nfalse\n");
        });
    }

    @Test
    public void testNullArraySingletonSurvivesConsumerClose() throws Exception {
        assertMemoryLeak(() -> {
            // The singleton is a BorrowedArray, whose close() is a no-op. A DirectArray would have
            // gone to UNDEFINED here and failed the type assertion below.
            final ArrayView first = NullConstant.NULL.getArray(null);
            Assert.assertEquals(ColumnType.NULL, first.getType());
            first.close();

            final ArrayView second = NullConstant.NULL.getArray(null);
            Assert.assertSame(first, second);
            Assert.assertEquals(ColumnType.NULL, second.getType());
            Assert.assertEquals(0, second.getDimCount());
            Assert.assertEquals(0, second.getCardinality());
            Assert.assertEquals(0, second.getFlatViewLength());
        });
    }

    @Test
    public void testOpComposition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT ARRAY[[1.0,2.0],[3.0,4.0],[5.0,6.0]] arr FROM long_sequence(1))");
            assertQuery("SELECT arr[2:3] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[[3.0,4.0]]\n");
            assertQuery("SELECT arr[2] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[3.0,4.0]\n");
            assertQuery("SELECT transpose(arr[2:3]) x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[[3.0],[4.0]]\n");
            assertQuery("SELECT transpose(arr)[2] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[2.0,4.0,6.0]\n");
            assertQuery("SELECT arr[2][2] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n4.0\n");
            assertQuery("SELECT arr[2][2:3] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[4.0]\n");
            assertQuery("SELECT arr[2:4][2] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[5.0,6.0]\n");
            assertQuery("SELECT arr[2:4][2:3] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[[5.0,6.0]]\n");
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

            assertQuery("select * from tango order by i desc")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts\ti\tarr
                            2001-04-01T00:00:00.000000Z\t42\tnull
                            2001-03-01T00:00:00.000000Z\t3\t[5.0,6.0,7.0]
                            2001-02-01T00:00:00.000000Z\t2\t[3.0,4.0]
                            2001-01-01T00:00:00.000000Z\t1\t[1.0,2.0]
                            """);

            // test also with no rowId - this simulates ordering output of a factory which does not support rowId
            assertQuery("select * from '*!*tango' order by i desc")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts\ti\tarr
                            2001-04-01T00:00:00.000000Z\t42\tnull
                            2001-03-01T00:00:00.000000Z\t3\t[5.0,6.0,7.0]
                            2001-02-01T00:00:00.000000Z\t2\t[3.0,4.0]
                            2001-01-01T00:00:00.000000Z\t1\t[1.0,2.0]
                            """);
        });
    }

    @Test
    public void testOrderByArrayColFailsGracefully() throws Exception {
        assertQuery("select * from tab order by arr")
                .ddl("create table tab as (select rnd_double_array(2, 1) arr from long_sequence(10))")
                .fails(27, "DOUBLE[][] is not a supported type in ORDER BY clause");
    }

    @Test
    public void testParquet() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts timestamp, i int, arr double[]) timestamp(ts) partition by DAY");
            execute("INSERT INTO tango VALUES ('2001-01', 1, '{1.0, 2.0}')");
            execute("INSERT INTO tango VALUES ('2001-02', 1, '{1.0, 2.0}')");

            final String expected = """
                    ts\ti\tarr
                    2001-01-01T00:00:00.000000Z\t1\t[1.0,2.0]
                    2001-02-01T00:00:00.000000Z\t1\t[1.0,2.0]
                    """;

            execute("ALTER TABLE tango CONVERT PARTITION TO PARQUET where ts in '2001-01';");
            assertQuery("tango")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns(expected);

            execute("ALTER TABLE tango CONVERT PARTITION TO NATIVE where ts in '2001-01';");
            assertQuery("tango")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns(expected);
        });
    }

    @Test
    public void testProjection() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE[], b DOUBLE[])");
            execute("INSERT INTO tango VALUES " +
                    "(ARRAY[2.0, 3.0], ARRAY[4.0, 5]), " +
                    "(ARRAY[6.0, 7], ARRAY[8.0, 9])");

            assertQuery("select a as a1, b as b1, a as a2, b as b2 from 'tango' ")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            a1\tb1\ta2\tb2
                            [2.0,3.0]\t[4.0,5.0]\t[2.0,3.0]\t[4.0,5.0]
                            [6.0,7.0]\t[8.0,9.0]\t[6.0,7.0]\t[8.0,9.0]
                            """);
        });
    }

    @Test
    public void testRndArrayBadTypes() throws Exception {
        assertMemoryLeak(() -> assertExceptionNoLeakCheck("select rnd_double_array(1, 100.0, 10), rnd_varchar() from long_sequence(5);",
                27, "nanRate must be an integer"
        ));
    }

    @Test
    public void testRndDoubleArray() throws Exception {
        assertMemoryLeak(() -> {
            // returnsOnce(): rnd_double_array() produces fresh random values on each execution,
            // so a single cursor pass is asserted (no re-read that would see new values).
            assertQuery("SELECT rnd_double_array(1)")
                    .noLeakCheck()
                    .returnsOnce("rnd_double_array\n[0.08486964232560668,0.299199045961845]\n");
            assertQuery("SELECT rnd_double_array('1', '1', '1', '1')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("rnd_double_array\n[null]\n");
            assertQuery("SELECT rnd_double_array(1::byte, 1::byte, 0::byte, 1::byte)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("rnd_double_array\n[null]\n");
            assertQuery("SELECT rnd_double_array(1::short, 1::short, 0::short, 1::short)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("rnd_double_array\n[null]\n");
            assertQuery("SELECT rnd_double_array(1::int, 1::int, 0::int, 1::int)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("rnd_double_array\n[null]\n");
            assertQuery("SELECT rnd_double_array(1::long, 1::long, 0::long, 1::long)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("rnd_double_array\n[null]\n");
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

            assertQuery("select rnd_double_array(0, 0, 1000)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("rnd_double_array\nnull\n");

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

            assertQuery("select rnd_double_array(2, 2, 0, 2, 2)")
                    .noLeakCheck()
                    .returnsOnce("""
                            rnd_double_array
                            [[null,0.9856290845874263],[null,0.5093827001617407]]
                            """);

            assertQuery("select rnd_double_array(2, 1, 0, 2, 2)")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [rnd_double_array(2,1,ignored,2,2)]
                                long_sequence count: 1
                            """);

            assertQuery("select rnd_double_array(3, 1, 4)")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [rnd_double_array(3,1,4)]
                                long_sequence count: 1
                            """);
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
            assertQuery("SELECT 8.0 / a, 4.0 / b * 2.0, 2.0 / b[1] * 0.5, 2 / b[2:] * 10 FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column\tcolumn1\tcolumn2\tcolumn3
                            [4.0,null]\t[[4.0,2.0],[2.0,1.0]]\t[0.5,0.25]\t[[5.0,2.5]]
                            [0.5,null]\t[[1.0,2.0]]\t[0.125,0.25]\t[]
                            null\tnull\tnull\tnull
                            """);
            assertQuery("SELECT 16.0 / transpose(a), 16.0 / transpose(b)FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column\tcolumn1
                            [8.0,null]\t[[8.0,4.0],[4.0,2.0]]
                            [1.0,null]\t[[2.0],[4.0]]
                            null\tnull
                            """);
            assertQuery("SELECT 0.0 / a, null::double / a FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column\tcolumn1
                            [0.0,null]\t[null,null]
                            [0.0,null]\t[null,null]
                            null\tnull
                            """);
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
            assertQuery("SELECT 8.0 - a, (4.0 - b)* 2.0, 2.0 - b[1], 2 - b[2:] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column\tcolumn1\tcolumn2\tcolumn3
                            [6.0,null]\t[[4.0,0.0],[0.0,-8.0]]\t[0.0,-2.0]\t[[-2.0,-6.0]]
                            [-8.0,8.0]\t[[-8.0,0.0]]\t[-6.0,-2.0]\t[]
                            null\tnull\tnull\tnull
                            """);
            assertQuery("SELECT 16.0 - transpose(a), 16.0 - transpose(b)FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column\tcolumn1
                            [14.0,null]\t[[14.0,12.0],[12.0,8.0]]
                            [0.0,16.0]\t[[8.0],[12.0]]
                            null\tnull
                            """);
            assertQuery("SELECT 0.0 - a, null::double - a FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            column\tcolumn1
                            [-2.0,null]\t[null,null]
                            [-16.0,0.0]\t[null,null]
                            null\tnull
                            """);
        });
    }

    @Test
    public void testSelectDistinct() throws Exception {
        execute(
                """
                        CREATE TABLE 'market_data' (
                          timestamp TIMESTAMP,
                          symbol SYMBOL CAPACITY 16384 CACHE,
                          bids DOUBLE[][],
                          asks DOUBLE[][]
                        ) timestamp(timestamp) PARTITION BY HOUR WAL;
                        """
        );

        execute("insert into market_data select timestamp_sequence('2025-05-03', 10000), rnd_symbol('GBPUSD', 'GBPAUD'), rnd_double_array(2), rnd_double_array(2) from long_sequence(10)");

        drainWalQueue();

        assertQuery("select distinct bids[1][2] bid from market_data order by bid")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        bid
                        0.0396096812427591
                        0.04173263630897883
                        0.2199453379647608
                        0.23405440872043592
                        0.43117716480568924
                        0.6217326707853098
                        0.6583311519893554
                        0.8615841627702753
                        0.8796413468565342
                        0.9344604857394011
                        """);

        assertQuery("select distinct bids from market_data;")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        bids
                        [[0.0843832076262595,0.9344604857394011,0.13123360041292131],[0.7905675319675964,0.19202208853547864,0.8899286912289663]]
                        [[0.9771103146051203,0.6217326707853098,0.15786635599554755,0.6381607531178513,0.4022810626779558,0.5793466326862211,0.9038068796506872,0.12026122412833129,0.6761934857077543],[0.8912587536603974,0.3435685332942956,0.42281342727402726,0.26922103479744897,0.7664256753596138,0.5298405941762054,0.5522494170511608,0.8445258177211064,0.7763904674818695],[0.05048190020054388,0.8847591603509142,0.0011075361080621349,0.931192737286751,0.8258367614088108,0.8001121139739173,0.38642336707855873,0.92050039469858,0.16381374773748514]]
                        [[0.8998921791869131,0.6583311519893554,0.30716667810043663,0.33746104579374825,0.8593131480724349,0.021189232728939578,0.10527282622013212],[0.11785316212653119,0.8221637568563206,0.32673950830571696,0.2825582712777682,0.18336217509438513,0.6455967424250787,0.48524046868499715],[0.8693768930398866,0.029080850168636263,0.7381752894013154,0.5185631921367574,0.5346019596733254,0.9859070322196475,0.29659296554924697],[0.6341292894843615,0.9457212646911386,0.2672120489216767,0.5025890936351257,0.9946372046359034,0.38422543844715473,0.48964139862697853],[0.5391626621794673,0.17180291960857297,0.4416432347777828,0.2065823085842221,0.8584308438045006,0.2445295612285482,0.6590829275055244]]
                        [[0.7704949839249925,0.04173263630897883,0.1264215196329228,0.14261321308606745,0.2677326840703891,0.4440250924606578],[0.23507754029460548,0.09618589590900506,0.24001459007748394,0.08675950660182763,0.868788610834602,0.741970173888595],[0.6107894368996438,0.4167781163798937,0.2739985338660311,0.05514933756198426,0.8001632261203552,0.9359814814085834],[0.7404912278395417,0.2093569947644236,0.7873229912811514,0.8439276969435359,0.7079450575401371,0.03973283003449557],[0.33504146853216143,0.8551850405049611,0.8321000514308267,0.7769285766561033,0.5637742551872849,0.6226001464598434],[0.6213434403332111,0.7195457109208119,0.8786111112537701,0.23493793601747937,0.6001225339624721,0.6334964081687151],[0.18158967304439033,0.95820305972778,0.7707249647497968,0.9130151105125102,0.28964821678040487,0.17405556853190263],[0.4729022357373792,0.6887925530449002,0.007985454958725269,0.5796722100538578,0.9691503953677446,0.7530494527849502]]
                        [[0.9934423708117267,0.23405440872043592],[0.848083900630095,0.794252253619802],[0.9058900298408074,0.8911615631017953],[0.4249052453180263,0.11047315214793696]]
                        [[0.21047933106727745,0.8796413468565342,0.04404000858917945],[0.40425101135606667,0.41496612044075665,0.03314618075579956],[0.36078878996232167,0.8376764297590714,0.2325041018786207],[0.7397816490927717,0.10799057399629297,0.8386104714017393],[0.8353079103853974,0.9001273812517414,0.11048000399634927]]
                        [[0.4913342104187668,0.8615841627702753,0.3189857960358504],[0.4375759068189693,0.07425696969451101,0.38881940598288367],[0.6944149053754287,0.5976614546411813,0.42044603754797416],[0.8985777419215233,0.5261234649527643,0.9815126662068089],[0.9246085617322545,0.20921704056371593,0.25470289113531375],[0.6240138444047509,0.11134244333117826,0.8472016167803409],[0.5863937813368164,0.2362963290561556,0.62456679681861],[0.3242526975448907,0.7430101994511517,0.6519511297254407],[0.7903520704337446,0.4755193286163272,0.7617663592833062],[0.8148792629172324,0.021177977444738705,0.9926343068414145],[0.1339704489137793,0.8303845449546206,0.4523282839107191],[0.04558283749364911,0.7636347764664544,0.5394562515552983],[0.9562577128401444,0.0966240354078981,0.5675831821917149],[0.21224614178286005,0.05942010834028011,0.7259967771911617]]
                        [[0.6936669914583254,0.43117716480568924,0.9578716688144072,0.5940502728139653,0.17914853671380093,0.30878646825073175,0.1319044042993568,0.33261541215518553,0.5079751443209725,0.3812506482325819,0.2703044758382739,0.4104855595304533],[0.6376518594972684,0.7587860024773928,0.011263511839942453,0.32613652012030125,0.9176263114713273,0.8457748651234394,0.6281252905002019,0.6504194217741501,0.2824076895992761,0.8054745482045382,0.27144997281940675,0.7573042043889733],[0.6931441108030082,0.1900488162112337,0.837738444021418,0.02633639777833019,0.8658616916564643,0.12465120312903266,0.36986619304630497,0.7706329763519386,0.10424082472921137,0.7874929839944909,0.9266929571641075,0.551184165451474],[0.7751886508004251,0.7659949103776245,0.7417434132166958,0.6288088087840823,0.9379038084870472,0.5763691784056397,0.5350165471764692,0.4613501223216129,0.3257868894353412,0.43619251546485505,0.6927480038605662,0.6051204746298999],[0.9410396704938232,0.19073234832401043,0.9610592594899304,0.4246651384043666,0.236380596505666,0.34085516645580494,0.08533575092925538,0.7564214859398338,0.8718394349472115,0.8925004728084927,0.45388767393986074,0.6728521416263535],[0.8413721135371649,0.7298540433653912,0.527776712010911,0.981074259037815,0.4701492486769596,0.4573258867972624,0.8139041928326346,0.3123904307505546,0.761296424148768,0.9510816389659975,0.43990342764801993,0.3726618828334195]]
                        [[0.48432558936820347,0.2199453379647608],[0.12934061164115174,0.19245855538083634],[0.40354999638471434,0.8940422626709261],[0.8235056484964091,0.7536836395346167],[0.5570298738371094,0.8096078909402364],[0.819524120126593,0.2056999100146133],[0.2017974971999763,0.42934437054513563],[0.16638148883943538,0.3911828760735948],[0.2969423112431254,0.9940353811420282]]
                        [[0.3669999679163578,0.0396096812427591,0.7234181773407536,0.7184108604451028],[0.844088760011128,0.4911664452671409,0.7776474810620265,0.17857143325827407],[0.3074410595329138,0.4151433463570058,0.9930633230891175,0.32317345869453706],[0.12027057950578746,0.27289838138048383,0.440645592676968,0.5999750613543716],[0.7436419445622273,0.8019738363360789,0.49950663682485574,0.9925168599192057],[0.6600337469738781,0.9251043257912728,0.060162223725059416,0.008444033230580739],[0.7573509485315202,0.9622544279671161,0.04229155272030727,0.9207177299535534],[0.11500943478849246,0.6013488560104849,0.23290767295012593,0.288531163175191],[0.9582280075093402,0.0983023224719013,0.22243351688740076,0.04969674919093636],[0.11493568065083815,0.4287405981191371,0.4058708191934428,0.22148534962398414],[0.8503316000896455,0.4879274348488539,0.45039214871547917,0.42774600405593644],[0.7593868458005614,0.32484768130656416,0.9520909221021127,0.2562499563674363],[0.6385649970707139,0.3622980885814183,0.2524690658195553,0.8825940193001498],[0.2917796053045747,0.9809851788419132,0.05339947229164044,0.7771965216814184]]
                        """);

        assertQuery("select distinct ARRAY[[1.0, 2.0], [3.0, 4.0]] from long_sequence(10)")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        ARRAY
                        [[1.0,2.0],[3.0,4.0]]
                        """);
    }

    @Test
    public void testSelectLiteral() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (a DOUBLE, b DOUBLE)");
            execute("INSERT INTO tango VALUES (1.0, 2.0)");
            assertQuery("SELECT ARRAY[a, b] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("ARRAY\n[1.0,2.0]\n");
            assertQuery("SELECT ARRAY[[a], [b]] FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("ARRAY\n[[1.0],[2.0]]\n");
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
        // Smaller dataset on slow CI runners (Mac, Windows). Lower the parallel GROUP BY sharding
        // threshold there so the query still takes the sharded map path this test exercises; the
        // first five 1-minute groups are unchanged, so the asserted result is identical.
        final int rowCount;
        if (Os.isLinux()) {
            rowCount = 3_000_000;
        } else {
            rowCount = 300_000;
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 100);
        }
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE AAPL_orderbook (
                    \ttimestamp TIMESTAMP,
                    \tasks DOUBLE[][]
                    ) timestamp(timestamp)
                    PARTITION BY HOUR WAL;""");
            execute("""
                    INSERT INTO AAPL_orderbook (timestamp, asks)\s
                    SELECT dateadd('s', x::int, '2023-08-25T08:00:02.264552Z') as timestamp, ARRAY[
                      [176.8,177.27,182.0,182.3,183.7,185.0,190.0,null, null, null],
                      [26.0,400.0,7.0,15.0,10.0,5.0,2.0,0.0,0.0,0.0],
                      [1.0,1.0,1.0, 1.0,1.0,1.0,1.0,0.0,0.0,0.0]
                     ] as asks
                    \tFROM long_sequence(""" + rowCount + ");");

            drainWalQueue();

            assertQuery("""
                    SELECT * FROM (DECLARE
                    \t@price := 1,
                    \t@size := 2,
                    \t@avg_price := avg(l2price(0.1, asks[@size], asks[@price])),
                    \t@best_price := asks[@price, 1]
                    \tSELECT\s
                    \t\ttimestamp,
                    \t\t@avg_price as avg_price,
                    \t\t@best_price as best_price,
                    \t\t@avg_price - @best_price as drift
                    \tFROM AAPL_orderbook
                    \tSAMPLE BY 1m) LIMIT 5;""")
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .returns("""
                            timestamp\tavg_price\tbest_price\tdrift
                            2023-08-25T08:00:00.000000Z\t176.79999999999998\t176.8\t-2.842170943040401E-14
                            2023-08-25T08:01:00.000000Z\t176.79999999999993\t176.8\t-8.526512829121202E-14
                            2023-08-25T08:02:00.000000Z\t176.79999999999993\t176.8\t-8.526512829121202E-14
                            2023-08-25T08:03:00.000000Z\t176.79999999999993\t176.8\t-8.526512829121202E-14
                            2023-08-25T08:04:00.000000Z\t176.79999999999993\t176.8\t-8.526512829121202E-14
                            """);
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
            assertQuery("SELECT shift(arr1, 3, 999.0), shift(arr1[2:], 1, 999.0), shift(arr1[1:3], 10, 999.0), shift(arr1, 3) FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            shift\tshift1\tshift2\tshift3
                            [999.0,999.0,999.0,1.0,9.0,10.0,12.0,8.0]\t[999.0,9.0,10.0,12.0,8.0,null,20.0]\t[999.0,999.0]\t[null,null,null,1.0,9.0,10.0,12.0,8.0]
                            []\t[]\t[]\t[]
                            null\tnull\tnull\tnull
                            """);
            assertQuery("SELECT shift(arr1, -3, 999.0), shift(arr1[2:], -1, 999.0), shift(arr1[1:3], -10, 999.0), shift(arr1, -3) FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            shift\tshift1\tshift2\tshift3
                            [12.0,8.0,null,20.0,12.0,999.0,999.0,999.0]\t[10.0,12.0,8.0,null,20.0,12.0,999.0]\t[999.0,999.0]\t[12.0,8.0,null,20.0,12.0,null,null,null]
                            []\t[]\t[]\t[]
                            null\tnull\tnull\tnull
                            """);
            assertQuery("SELECT shift(arr2, 1, 999.0), shift(arr2[1:], -1, 999.0), shift(arr2, 5, 999.0), shift(arr2, -2) FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            shift\tshift1\tshift2\tshift3
                            [[999.0,1.0,9.0],[999.0,12.0,8.0]]\t[[9.0,10.0,999.0],[8.0,null,999.0]]\t[[999.0,999.0,999.0],[999.0,999.0,999.0]]\t[[10.0,null,null],[null,null,null]]
                            []\t[]\t[]\t[]
                            null\tnull\tnull\tnull
                            """);
        });
    }

    @Test
    public void testShiftNonConst() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (arr DOUBLE[], distance INT, filler DOUBLE)");
            execute("INSERT INTO tango VALUES (ARRAY[1.0, 2.0, 3.0], 1, 6.0)");
            assertQuery("SELECT shift(arr, distance, filler) FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("shift\n[6.0,1.0,2.0]\n");
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
            assertQuery("SELECT transpose(arr) FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            transpose
                            [[1.0,10.0,8.0,20.0],[9.0,12.0,null,12.0]]
                            []
                            null
                            """);

            assertQuery("SELECT shift(transpose(arr)[1], 2, 999.0), shift(transpose(arr)[1, 2:], 1, 999.0), shift(transpose(arr)[1, 1:3], 10, 999.0), shift(transpose(arr)[1], 3) FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            shift\tshift1\tshift2\tshift3
                            [999.0,999.0,1.0,10.0]\t[999.0,10.0,8.0]\t[999.0,999.0]\t[null,null,null,1.0]
                            null\tnull\tnull\tnull
                            null\tnull\tnull\tnull
                            """);

            assertQuery("SELECT shift(transpose(arr)[1], -2, 999.0), shift(transpose(arr)[1, 2:], -1, 999.0), shift(transpose(arr)[1, 1:3], -10, 999.0), shift(transpose(arr)[1], -3) FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            shift\tshift1\tshift2\tshift3
                            [8.0,20.0,999.0,999.0]\t[8.0,20.0,999.0]\t[999.0,999.0]\t[20.0,null,null,null]
                            null\tnull\tnull\tnull
                            null\tnull\tnull\tnull
                            """);
            assertQuery("SELECT shift(transpose(arr), 1, 999.0), shift(transpose(arr)[1:], -1, 999.0), shift(transpose(arr), 5, 999.0), shift(transpose(arr), -2) FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            shift\tshift1\tshift2\tshift3
                            [[999.0,1.0,10.0,8.0],[999.0,9.0,12.0,null]]\t[[10.0,8.0,20.0,999.0],[12.0,null,12.0,999.0]]\t[[999.0,999.0,999.0,999.0],[999.0,999.0,999.0,999.0]]\t[[8.0,20.0,null,null],[null,12.0,null,null]]
                            []\t[]\t[]\t[]
                            null\tnull\tnull\tnull
                            """);
        });
    }

    @Test
    public void testSlice1d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT ARRAY[1.0,2.0,3.0] arr FROM long_sequence(1))");
            assertQuery("SELECT arr[1:2] slice from tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("slice\n[1.0]\n");
            assertQuery("SELECT arr[1:3] slice from tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("slice\n[1.0,2.0]\n");
        });
    }

    @Test
    public void testSlice2d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT ARRAY[[1.0, 2], [3.0, 4], [5.0, 6]] arr FROM long_sequence(1))");
            assertQuery("SELECT arr[1:2] slice FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("slice\n[[1.0,2.0]]\n");
            assertQuery("SELECT arr[2:] slice FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("slice\n[[3.0,4.0],[5.0,6.0]]\n");
            assertQuery("SELECT arr[3:, 1:2] slice FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("slice\n[[5.0]]\n");
            assertQuery("SELECT arr[3:, 2] slice FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("slice\n[6.0]\n");
            assertQuery("SELECT arr[1:3] slice FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("slice\n[[1.0,2.0],[3.0,4.0]]\n");
            assertQuery("SELECT arr[1:3, 1:2] slice FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("slice\n[[1.0],[3.0]]\n");
            assertQuery("SELECT arr[2, 2] element FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("element\n4.0\n");
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
            assertQuery("SELECT arr[1:5] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[[1.0,2.0],[3.0,4.0],[5.0,6.0]]\n");
            assertQuery("SELECT arr[4:5] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[]\n");
            assertQuery("SELECT arr[2:1] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[]\n");
            assertQuery("SELECT arr[1:-3] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[]\n");
            assertQuery("SELECT arr[1:-100] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[]\n");
        });
    }

    @Test
    public void testSliceTransposed() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT ARRAY[[1.0, 2], [3.0, 4], [5.0, 6]] arr FROM long_sequence(1))");
            // transposed array: [[1,3,5],[2,4,6]]; slice takes first row, and first two elements from it
            assertQuery("SELECT transpose(arr)[1:2, 1:3] slice FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("slice\n[[1.0,3.0]]\n");
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
            assertQuery("SELECT arr[1] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n" + subArr0 + "\n");
            assertQuery("SELECT arr[2] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n" + subArr1 + "\n");
            assertQuery("SELECT arr[1,1] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n" + subArr00 + "\n");
            assertQuery("SELECT arr[1,2] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n" + subArr01 + "\n");
            assertQuery("SELECT arr[2,1] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n" + subArr10 + "\n");
            assertQuery("SELECT arr[2,2] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n" + subArr11 + "\n");
            assertQuery("SELECT arr[1:2,2:3,2] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[[4.0]]\n");
            assertQuery("SELECT arr[1:,2] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[" + subArr01 + "," + subArr11 + "]\n");
            assertQuery("SELECT arr[1:,2:3,2] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[[4.0],[8.0]]\n");
            assertQuery("SELECT arr[2,2:] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[[7.0,8.0]]\n");
            assertQuery("SELECT arr[2:,2:] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[[[7.0,8.0]]]\n");
            assertQuery("SELECT arr[2,2,2:] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[8.0]\n");
            assertQuery("SELECT arr[2,2:,2:] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[[8.0]]\n");
            assertQuery("SELECT arr[2,3-1:,2:] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[[8.0]]\n");
            assertQuery("SELECT arr[2,3-1:,2:] x FROM tango")
                    .noLeakCheck()
                    .assertsPlan("""
                            VirtualRecord
                              functions: [arr[2,2:,2:]]
                                PageFrame
                                    Row forward scan
                                    Frame forward scan on: tango
                            """);
        });
    }

    @Test
    public void testSubArrayOutOfBounds() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango AS (SELECT ARRAY[[[1.0, 2], [3.0, 4]], [[5.0, 6], [7.0, 8]]] arr FROM long_sequence(1))");

            assertQuery("SELECT arr[3] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\nnull\n");
            assertQuery("SELECT arr[2, 3] x FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\nnull\n");
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
            assertQuery("SELECT transpose(ARRAY" + original + ") transposed FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("transposed\n" + transposed + "\n");
            execute("CREATE TABLE tango AS (SELECT ARRAY" + original + " arr FROM long_sequence(1))");
            assertQuery("SELECT arr original FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("original\n" + original + '\n');
            assertQuery("SELECT transpose(arr) transposed FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("transposed\n" + transposed + "\n");
            assertQuery("SELECT transpose(transpose(arr)) twice_transposed FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("twice_transposed\n" + original + '\n');
        });
    }

    @Test
    public void testTransposeSubArray() throws Exception {
        assertMemoryLeak(() -> {
            String original = "[[[1.0,2.0],[3.0,4.0],[5.0,6.0]]]";
            String subTransposed = "[[1.0,3.0,5.0],[2.0,4.0,6.0]]";
            assertQuery("SELECT transpose(ARRAY" + original + "[1]) transposed FROM long_sequence(1)")
                    .noLeakCheck()
                    .expectSize()
                    .returns("transposed\n" + subTransposed + "\n");
            execute("CREATE TABLE tango AS (SELECT ARRAY" + original + " arr FROM long_sequence(1))");
            assertQuery("SELECT arr original FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("original\n" + original + '\n');
            assertQuery("SELECT transpose(arr[1]) transposed FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("transposed\n" + subTransposed + "\n");
        });
    }

    @Test
    public void testTypeCast() {
        for (int i = 1; i < ColumnType.ARRAY_NDIMS_LIMIT; i++) {
            for (short j = ColumnType.BOOLEAN; j <= ColumnType.IPv4; j++) {
                if (!ColumnType.isSupportedArrayElementType(j)) {
                    continue;
                }
                Assert.assertTrue(ColumnType.isConvertibleFrom(
                        ColumnType.encodeArrayType(j, i),
                        ColumnType.encodeArrayType(j, i)
                ));
                Assert.assertTrue(ColumnType.isConvertibleFrom(
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
                Assert.assertFalse(ColumnType.isConvertibleFrom(j, ColumnType.encodeArrayType(j, i)));
                // ... nor the other way around
                Assert.assertFalse(ColumnType.isConvertibleFrom(ColumnType.encodeArrayType(j, i), j));
            }
        }
    }

    @Test
    public void testUnionAll() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tango (a double)");
            execute("insert into tango values (null)");

            // 2 arrays of the same type and dimensionality
            assertQuery("SELECT ARRAY[1.0, 2.0] UNION ALL SELECT ARRAY[3.0, 4.0, 5.0] FROM long_sequence(1)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ARRAY
                            [1.0,2.0]
                            [3.0,4.0,5.0]
                            """);

            // with scalar double
            assertQuery("SELECT ARRAY[1.0, 2.0] UNION ALL SELECT 3.0 FROM long_sequence(1)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ARRAY
                            [1.0,2.0]
                            [3.0]
                            """);

            // with double::null
            assertQuery("SELECT ARRAY[1.0, 2.0] UNION ALL SELECT * from tango")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ARRAY
                            [1.0,2.0]
                            null
                            """);

            // with string
            assertQuery("SELECT ARRAY[1.0, 2.0] UNION ALL SELECT 'foo' FROM long_sequence(1)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ARRAY
                            [1.0,2.0]
                            foo
                            """);

            // 1D and 2D arrays
            assertQuery("SELECT ARRAY[1.0, 2.0] UNION ALL SELECT ARRAY[[3.0, 4.0], [5.0, 6.0]] FROM long_sequence(1)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ARRAY
                            [[1.0,2.0]]
                            [[3.0,4.0],[5.0,6.0]]
                            """);
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

            assertQuery("z")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts\tarr
                            1970-01-06T18:53:20.000000Z\t[[null,0.20447441837877756],[null,null]]
                            1970-01-06T18:53:21.000000Z\t[[0.3491070363730514,0.7611029514995744],[0.4217768841969397,null],[0.7261136209823622,0.4224356661645131],[null,0.3100545983862456],[0.1985581797355932,0.33608255572515877],[0.690540444367637,null],[0.21583224269349388,0.15786635599554755],[null,null],[0.12503042190293423,null],[0.9687423276940171,null],[null,null],[null,null],[null,null],[0.7883065830055033,null],[0.4138164748227684,0.5522494170511608],[0.2459345277606021,null]]
                            1970-01-06T18:53:22.000000Z\t[[0.7643643144642823,null],[null,null],[0.18769708157331322,0.16381374773748514],[0.6590341607692226,null],[null,null],[0.8837421918800907,0.05384400312338511],[null,0.7230015763133606],[0.12105630273556178,null],[0.5406709846540508,null],[0.9269068519549879,null],[null,null],[0.1202416087573498,null]]
                            1970-01-06T18:53:23.000000Z\t[[null,null,0.4971342426836798,null],[0.5065228336156442,null,null,0.03167026265669903],[null,null,0.2879973939681931,null],[null,null,null,0.24008362859107102]]
                            1970-01-06T18:53:24.000000Z\t[[0.2185865835029681,null],[0.24079155981438216,0.10643046345788132],[0.5244255672762055,0.0171850098561398],[0.09766834710724581,null],[0.053594208204197136,0.26369335635512836],[0.22895725920713628,0.9820662735672192],[null,0.32424562653969957],[0.8998921791869131,null],[null,null],[0.33746104579374825,0.18740488620384377],[0.10527282622013212,0.8291193369353376],[0.32673950830571696,null],[0.18336217509438513,0.9862476361578772],[0.8693768930398866,0.8189713915910615]]
                            1970-01-06T18:53:25.000000Z\t[[0.29659296554924697,0.24642266252221556],[null,null],[null,0.13264292470570205],[0.38422543844715473,null],[null,null],[null,0.7668146556860689],[null,0.05158459929273784],[null,null]]
                            1970-01-06T18:53:26.000000Z\t[[0.3568111021227658,0.05758228485190853,0.6729405590773638,null,0.5716129058692643],[0.05094182589333662,null,null,0.4609277382153818,0.5691053034055052],[0.12663676991275652,0.11371841836123953,null,null,0.7203170014947307],[null,null,null,null,0.7704949839249925],[0.8144207168582307,null,null,null,0.2836347139481469]]
                            1970-01-06T18:53:27.000000Z\t[[0.08675950660182763,null],[0.741970173888595,0.25353478516307626],[0.2739985338660311,null],[0.8001632261203552,null],[0.7404912278395417,0.08909442703907178],[0.8439276969435359,null],[null,0.08712007604601191]]
                            1970-01-06T18:53:28.000000Z\t[[0.5637742551872849,null],[null,null],[0.7195457109208119,null],[0.23493793601747937,null],[0.6334964081687151,0.6721404635638454]]
                            1970-01-06T18:53:29.000000Z\t[[0.17405556853190263,0.823395724427589,null,0.8108032283138068,null,null,0.7530494527849502,0.49153268154777974,0.0024457698760806945,0.29168465906260244,0.3121271759430503,0.3004874521886858],[null,0.7653255982993546,null,null,null,null,0.37873228328689634,null,0.7272119755925095,null,0.7467013668130107,0.5794665369115236],[null,0.5308756766878475,0.03192108074989719,null,0.17498425722537903,null,0.34257201464152764,null,null,0.29242748475227853,null,0.11296257318851766],[null,0.23405440872043592,0.1479745625593103,null,0.8115426881784433,null,0.32093405888189597,null,0.04321289940104611,0.8217652538598936,0.6397125243912908,0.29419791719259025],[0.865629565918467,null,null,0.16923843067953104,0.7198854503668188,0.5174107449677378,0.38509066982448115,null,null,null,0.5475429391562822,0.6977332212252165],[null,null,0.4268921400209912,0.9997797234031688,0.5234892454427748,null,null,null,null,0.5169565007469263,0.7039785408034679,0.8461211697505234],[null,0.537020248377422,0.8766908646423737,null,null,0.31852531484741486,null,0.605050319285447,0.9683642405595932,0.3549235578142891,0.04211401699125483,null],[null,0.0032519916115479885,0.2703179181043681,0.729536610842768,0.3317641556575974,0.8895915828662114,null,null,null,null,0.1599211504269954,0.5251698097331752],[null,0.18442756220221035,null,0.48422587819911567,0.2970515836513553,null,0.7826107801293182,null,0.3218450864634881,0.8034049105590781,null,null],[0.40425101135606667,0.9412663583926286,null,null,0.8376764297590714,0.15241451173695408,null,0.743599174001969,null,null,0.9001273812517414,0.5629104624260136],[0.6001215594928115,0.8920252905736616,0.09977691656157406,null,0.2862717364877081,null,null,null,0.8853675629694284,4.945923013344178E-5,null,0.0016532800623808575]]
                            1970-01-01T00:00:00.000000Z\t[[0.3489278573518253,null,null,0.07383464174908916,0.8791439438812569]]
                            1970-01-01T00:01:40.000000Z\t[[null,0.10820602386069589,null,null,0.11286092606280262,0.7370823954391381,null,0.533524384058538,0.6749208267946962,null,0.3124458010612313,null]]
                            1970-01-01T00:03:20.000000Z\t[[0.4137003695612732,null,null,0.32449127848036263,0.41886400558338654,0.8409080254825717,0.06001827721556019,null,null,null]]
                            1970-01-01T00:05:00.000000Z\t[[null,null]]
                            1970-01-01T00:06:40.000000Z\t[[null,null,null,0.5815065874358148,null]]
                            1970-01-01T00:08:20.000000Z\t[[0.020390884194626757,null,null]]
                            1970-01-01T00:10:00.000000Z\t[[0.42044603754797416,0.47603861281459736,0.9815126662068089,0.22252546562577824]]
                            1970-01-01T00:11:40.000000Z\t[[null,0.8869397617459538,null,null,null,null,null]]
                            1970-01-01T00:13:20.000000Z\t[[null,null,0.6993909595959196]]
                            1970-01-01T00:15:00.000000Z\t[[0.8148792629172324,null,0.9926343068414145,null,0.8303845449546206,null,null,null,0.7636347764664544,0.2195743166842714,null,null,null,0.5823910118974169,0.05942010834028011]]
                            """);
        });
    }

    @Test
    public void testUnionDistinct() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table alpha (arr double[])");
            execute("create table bravo (arr double[])");

            execute("insert into alpha values (ARRAY[1.0, 2.0])");
            assertQuery("select * from alpha union select * from bravo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("arr\n[1.0,2.0]\n");

            execute("insert into bravo values (ARRAY[1.0, 2.0])");
            assertQuery("select * from alpha union select * from bravo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("arr\n[1.0,2.0]\n");

            execute("insert into alpha values (ARRAY[1.0, 2.0, 3.0])");
            assertQuery("select * from alpha union select * from bravo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            arr
                            [1.0,2.0]
                            [1.0,2.0,3.0]
                            """);

            execute("insert into bravo values (ARRAY[1.0, 2.0, 3.0])");
            assertQuery("select * from alpha union select * from bravo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            arr
                            [1.0,2.0]
                            [1.0,2.0,3.0]
                            """);

            execute("insert into alpha values (ARRAY[])");
            assertQuery("select * from alpha union select * from bravo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            arr
                            [1.0,2.0]
                            [1.0,2.0,3.0]
                            []
                            """);

            execute("insert into bravo values (ARRAY[])");
            assertQuery("select * from alpha union select * from bravo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            arr
                            [1.0,2.0]
                            [1.0,2.0,3.0]
                            []
                            """);

            execute("insert into alpha values (null)");
            assertQuery("select * from alpha union select * from bravo")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            arr
                            [1.0,2.0]
                            [1.0,2.0,3.0]
                            []
                            null
                            """);
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

    private void testLengthOverParquetOneDimension(boolean rawArrayEncoding) throws Exception {
        // A 1D double array's length comes from the aux entry's data size rather than from its shape
        // header (see PageFrameMemoryRecord.getArrayDimLen0): the entry is Double.BYTES * (length + 1)
        // bytes wide, so reading the shape - and with it a data-vector cache line per row - is
        // avoidable. Every other page-frame dim_length() test stores DOUBLE[][], which takes the
        // shape path, so nothing exercised this.
        // Both parquet array encodings are covered: the raw one copies the native entry verbatim,
        // while the levels one rebuilds it in the decoder, so only the latter could drift from the
        // layout the fast path assumes. The last row stays native, putting both frame types in one
        // scan.
        // The empty array is pinned on a native frame below rather than here, because the levels
        // encoding loses it: an ARRAY[] stored in a partition that also holds a NULL array reads back
        // as a one-element [null]. That is the decoder writing a shape of 1, not this path
        // misreading it - the fast path's assert agrees with the shape header, and plain SELECT arr
        // reproduces it on an unmodified master without going near getArrayDimLen0() at all. The raw
        // encoding, which is the default, round-trips ARRAY[] correctly.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_RAW_ARRAY_ENCODING_ENABLED, String.valueOf(rawArrayEncoding));
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP, arr DOUBLE[]) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tango VALUES
                    ('1970-01-01T00:00:00.000000Z', ARRAY[1.0, 2.0, 3.0]),
                    ('1970-01-01T00:00:01.000000Z', ARRAY[9.0]),
                    ('1970-01-01T00:00:02.000000Z', NULL),
                    ('1970-01-02T00:00:00.000000Z', ARRAY[4.0, 5.0])
                    """);
            execute("ALTER TABLE tango CONVERT PARTITION TO PARQUET LIST '1970-01-01'");
            assertQuery("SELECT dim_length(arr, 1) d1, arr[1] first FROM tango")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            d1\tfirst
                            3\t1.0
                            1\t9.0
                            null\tnull
                            2\t4.0
                            """);

            // The empty array pins the +1 in the size-to-length conversion: its data entry is the
            // bare shape-plus-padding, so it must read back as 0 rather than as 1.
            execute("CREATE TABLE tango_native (arr DOUBLE[])");
            execute("INSERT INTO tango_native VALUES (ARRAY[]), (ARRAY[7.0]), (NULL)");
            assertQuery("SELECT dim_length(arr, 1) d1 FROM tango_native")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            d1
                            0
                            1
                            null
                            """);
            execute("DROP TABLE tango_native");
            execute("DROP TABLE tango");
        });
    }
}
