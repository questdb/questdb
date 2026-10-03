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

package io.questdb.test.griffin.engine.functions.array.arrayelem;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
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
    public void test2dEmptySlicePlusValid() throws Exception {
        // a[1:3, 2:2] has shape (2,0) and keeps a non-zero offset into its parent;
        // the empty slice contributes no elements, just like NULL
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE[][], b DOUBLE[][])");
            execute("INSERT INTO t VALUES (ARRAY[[5.0, 6.0], [7.0, 8.0]], ARRAY[[1.0], [null]])");
            assertQuery("SELECT " + funcName() + "(a[1:3, 2:2], b) x FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("x\n[[1.0],[null]]\n");
        });
    }

    @Test
    public void testBind1dPlus2dColumnFails() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (a DOUBLE[][])");
            execute("INSERT INTO t VALUES (ARRAY[[1.0, 2.0], [3.0, 4.0]])");
            assertBindDimensionMismatch("SELECT " + funcName() + "(a, $1) x FROM t", "{1.0,2.0}");
        });
    }

    @Test
    public void testBind2dAfter1dConstantFails() throws Exception {
        assertMemoryLeak(() -> assertBindDimensionMismatch(
                "SELECT " + funcName() + "(ARRAY[1.0, 2.0], $1) x",
                "{{1.0,2.0,5.0},{3.0,4.0,9.0}}"
        ));
    }

    @Test
    public void testBind2dBefore1dConstantFails() throws Exception {
        assertMemoryLeak(() -> assertBindDimensionMismatch(
                "SELECT " + funcName() + "($1, ARRAY[1.0, 2.0]) x",
                "{{1.0,2.0,5.0},{3.0,4.0,9.0}}"
        ));
    }

    @Test
    public void testBind2dExpressionAfter1dConstantFails() throws Exception {
        assertMemoryLeak(() -> assertBindDimensionMismatch(
                "SELECT " + funcName() + "(ARRAY[1.0, 2.0], $1 * 2.0) x",
                "{{1.0,2.0,5.0},{3.0,4.0,9.0}}"
        ));
    }

    @Test
    public void testBindEmpty2dPlusValid() throws Exception {
        // the empty bound value has shape (2,0) and no elements, so it is skipped like NULL
        assertMemoryLeak(() -> assertBindReturns(
                "x\n[[1.0],[null]]\n",
                "SELECT " + funcName() + "($1, ARRAY[[1.0], [null]]) x",
                "{{},{}}"
        ));
    }

    @Test
    public void testBindMatching1dConstant() throws Exception {
        assertMemoryLeak(() -> assertBindReturns(
                "x\n[1.0,2.0]\n",
                "SELECT " + funcName() + "(ARRAY[1.0, null], $1) x",
                "{NaN,2.0}"
        ));
    }

    @Test
    public void testBindsDimensionMismatchFails() throws Exception {
        assertMemoryLeak(() -> assertBindDimensionMismatch(
                "SELECT " + funcName() + "($1, $2) x",
                "{1.0,2.0}",
                "{{1.0,2.0},{3.0,4.0}}"
        ));
    }

    @Test
    public void testBindsInsert() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (arr DOUBLE[], ts TIMESTAMP) TIMESTAMP(ts)");
            final String sql = "INSERT INTO t(arr, ts) VALUES (" + funcName() + "($1, $2), 0)";
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                defineWeakDimsBinds(2);
                try (InsertOperation insert = compiler.compile(sql, sqlExecutionContext).popInsertOperation()) {
                    bindVariableService.setStr(0, "{{1.0,2.0},{3.0,4.0}}");
                    bindVariableService.setStr(1, "{{1.0,2.0},{3.0,4.0}}");
                    try {
                        insert.execute(sqlExecutionContext);
                        Assert.fail("expected inconvertible types");
                    } catch (SqlException e) {
                        Assert.assertEquals(31, e.getPosition());
                        TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible types");
                    }
                }

                defineWeakDimsBinds(2);
                try (InsertOperation insert = compiler.compile(sql, sqlExecutionContext).popInsertOperation()) {
                    bindVariableService.setStr(0, "{1.0,NaN}");
                    bindVariableService.setStr(1, "{NaN,2.0}");
                    insert.execute(sqlExecutionContext);
                }
            }
            assertQuery("SELECT arr FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .returns("arr\n[1.0,2.0]\n");
        });
    }

    @Test
    public void testBindsMatching1d() throws Exception {
        assertMemoryLeak(() -> assertBindReturns(
                "x\n[1.0,2.0]\n",
                "SELECT " + funcName() + "($1, $2) x",
                "{1.0,NaN}",
                "{NaN,2.0}"
        ));
    }

    @Test
    public void testBindsUnbound() throws Exception {
        assertMemoryLeak(() -> {
            defineWeakDimsBinds(2);
            try (RecordCursorFactory factory = select("SELECT " + funcName() + "($1, $2) x")) {
                assertFactory(factory)
                        .withContext(sqlExecutionContext)
                        .expectSize()
                        .returns("x\nnull\n");
            }
        });
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
            assertQuery("SELECT " + funcName() + "(a, b) FROM tab")
                    .fails(7, "dimension");
        });
    }

    @Test
    public void testNDimsMismatch2dPlus3d() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (a DOUBLE[][], b DOUBLE[][][])");
            assertQuery("SELECT " + funcName() + "(a, b) FROM tab")
                    .fails(7, "dimension");
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
        assertMemoryLeak(() -> assertQuery(sql)
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns(funcName() + "\n[1.0,2.0]\n"));
    }

    // compiles with weak-dims array binds first and binds the values afterwards, the way PG wire does
    private void assertBindDimensionMismatch(String sql, String... values) throws Exception {
        defineWeakDimsBinds(values.length);
        try (RecordCursorFactory factory = select(sql)) {
            for (int i = 0; i < values.length; i++) {
                bindVariableService.setStr(i, values[i]);
            }
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                println(factory, cursor);
                Assert.fail("expected dimension mismatch, got: " + sink);
            } catch (SqlException e) {
                Assert.assertEquals(7, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "dimension mismatch");
            }
        }
    }

    private void assertBindReturns(String expected, String sql, String... values) throws Exception {
        defineWeakDimsBinds(values.length);
        try (RecordCursorFactory factory = select(sql)) {
            for (int i = 0; i < values.length; i++) {
                bindVariableService.setStr(i, values[i]);
            }
            assertFactory(factory)
                    .withContext(sqlExecutionContext)
                    .expectSize()
                    .returns(expected);
        }
    }

    private void defineWeakDimsBinds(int count) throws SqlException {
        bindVariableService.clear();
        for (int i = 0; i < count; i++) {
            bindVariableService.define(i, ColumnType.encodeArrayTypeWithWeakDims(ColumnType.DOUBLE, true), 0);
        }
    }

    protected void assertElemWise(String expected, String... arrayArgs) throws Exception {
        String sql = "SELECT " + funcName() + "(" + String.join(", ", arrayArgs) + ")";
        assertMemoryLeak(() -> assertQuery(sql)
                .noLeakCheck()
                .expectSize()
                .returns(funcName() + "\n" + expected + "\n"));
    }
}
