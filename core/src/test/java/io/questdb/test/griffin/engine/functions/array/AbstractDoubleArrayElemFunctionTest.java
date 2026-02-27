/*******************************************************************************
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

package io.questdb.test.griffin.engine.functions.array;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public abstract class AbstractDoubleArrayElemFunctionTest extends AbstractCairoTest {

    protected abstract String funcName();

    // --- shared tests: identical expected output for all 4 functions ---

    @Test
    public void test2dAllNanNonNull() throws Exception {
        assertElemWise(
                "[[null,null],[null,null]]",
                "ARRAY[[null, null], [null, null]]",
                "ARRAY[[null, null], [null, null]]"
        );
    }

    @Test
    public void test2dAllNull() throws Exception {
        assertElemWise("null", "null::double[][]", "null::double[][]", "null::double[][]");
    }

    @Test
    public void test2dZeroLengthPlusValid() throws Exception {
        // [1:1] with exclusive upper bound selects zero rows → (0,3) shape
        // Zero-length array should be treated like NULL (skipped), result is the other array
        assertElemWise(
                "[[1.0,2.0,3.0]]",
                "ARRAY[[10.0, 20.0, 30.0]][1:1]",
                "ARRAY[[1.0, 2.0, 3.0]]"
        );
    }

    @Test
    public void test2dNanAtGrownPositions() throws Exception {
        assertElemWise(
                "[[1.0,2.0,null],[3.0,4.0,null],[null,null,null]]",
                "ARRAY[[1.0, 2.0], [3.0, 4.0]]",
                "ARRAY[[null, null, null], [null, null, null], [null, null, null]]"
        );
    }

    @Test
    public void test2dNullPlusShaped() throws Exception {
        assertElemWise(
                "[[1.0,2.0,3.0],[4.0,5.0,6.0]]",
                "null::double[][]",
                "ARRAY[[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]"
        );
    }

    @Test
    public void testAllNanNonNullArrays() throws Exception {
        assertElemWise("[null,null]", "ARRAY[null, null]", "ARRAY[null, null]");
    }

    @Test
    public void testEmptyArrayPlusValid() throws Exception {
        assertElemWise("[1.0,2.0]", "ARRAY[]::double[]", "ARRAY[1.0, 2.0]");
    }

    @Test
    public void testNanAtAllPositionsInOneArg() throws Exception {
        assertElemWise("[4.0,6.0]", "ARRAY[null, null]", "ARRAY[4.0, 6.0]");
    }

    @Test
    public void testNDimsMismatch1dPlus2d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (a DOUBLE[], b DOUBLE[][])");
            assertException(
                    "SELECT " + funcName() + "(a, b) FROM tab",
                    7,
                    "dimension"
            );
        });
    }

    @Test
    public void testNDimsMismatch2dPlus3d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (a DOUBLE[][], b DOUBLE[][][])");
            assertException(
                    "SELECT " + funcName() + "(a, b) FROM tab",
                    7,
                    "dimension"
            );
        });
    }

    @Test
    public void testNullPlusNanPlusValid() throws Exception {
        assertElemWise("[1.0,2.0]", "null::double[]", "ARRAY[1.0, null]", "ARRAY[null, 2.0]");
    }

    @Test
    public void testShortArrayPlusNanInLongArray() throws Exception {
        assertElemWise("[1.0,2.0,3.0]", "ARRAY[1.0]", "ARRAY[null, 2.0, 3.0]");
    }

    @Test
    public void testShortPlusLongPlusNanOverlap() throws Exception {
        assertElemWise("[1.0,2.0,3.0]", "ARRAY[1.0, null, 3.0]", "ARRAY[null, 2.0]");
    }

    @Test
    public void testSingleArgResolvesToGroupBy() throws Exception {
        String sql = "SELECT " + funcName() + "(ARRAY[1.0, 2.0])";
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                funcName() + "\n[1.0,2.0]\n",
                sql, null, false, true
        ));
    }

    protected void assertElemWise(String expected, String... arrayArgs) throws Exception {
        String sql = "SELECT " + funcName() + "(" + String.join(", ", arrayArgs) + ")";
        assertMemoryLeak(() -> assertQueryNoLeakCheck(
                funcName() + "\n" + expected + "\n",
                sql, null, true, true
        ));
    }

    protected void assertElemWiseFromTable(String expected, String ddl, String[] inserts, String selectSql) throws Exception {
        assertMemoryLeak(() -> {
            execute(ddl);
            for (String insert : inserts) {
                execute(insert);
            }
            assertQueryNoLeakCheck(
                    funcName() + "\n" + expected + "\n",
                    selectSql, null, true, true
            );
        });
    }
}
