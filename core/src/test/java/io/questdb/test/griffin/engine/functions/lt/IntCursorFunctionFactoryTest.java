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

package io.questdb.test.griffin.engine.functions.lt;

import org.junit.Test;

/**
 * Tests for the {@code int < (sub-query)} / {@code int > (sub-query)} operators, where the right-hand
 * side is a scalar sub-query (cursor). The left operand is coerced up to the width of the cursor scalar,
 * so an {@code int} compared with a {@code long} sub-query is compared losslessly as longs.
 *
 * @see io.questdb.griffin.engine.functions.lt.LtIntCursorFunctionFactory
 * @see io.questdb.griffin.engine.functions.lt.GtIntCursorFunctionFactory
 */
public class IntCursorFunctionFactoryTest extends AbstractCursorFunctionFactoryTest {

    @Test
    public void testWorkerStateSharedExecutesCursorOnceAndRefreshes() throws Exception {
        // an INT cursor scalar -> the int comparison mode of the worker-state contract
        assertWorkerStateSharedBehavior("int", "i", "int");
    }

    @Test
    public void testByteAndShortLeftOperands() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table s as (select x::short sh, x::byte b from long_sequence(5))");
            // short left, int cursor scalar (3) -> long comparison
            assertQuery("select sh from s where sh < (select 3)")
                    .noLeakCheck()
                    .returns("sh\n1\n2\n");
            // byte left, int cursor scalar (3) -> long comparison
            assertQuery("select b from s where b > (select 3)")
                    .noLeakCheck()
                    .returns("b\n4\n5\n");
        });
    }

    @Test
    public void testDoubleCursorScalar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(10))");
            // avg(i) = 5.5 -> double comparison mode
            assertQuery("select i from t where i < (select avg(i) from t)")
                    .noLeakCheck()
                    .returns("i\n1\n2\n3\n4\n5\n");
            assertQuery("select i from t where i > (select avg(i) from t)")
                    .noLeakCheck()
                    .returns("i\n6\n7\n8\n9\n10\n");
        });
    }

    @Test
    public void testErrorMultipleColumns() throws Exception {
        // the < and > factories duplicate the validation code, so both must be asserted
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(10))");
            assertQuery("select i from t where i > (select max(i), 1 x from t)")
                    .fails(27, "select must provide exactly one column");
            assertQuery("select i from t where i < (select max(i), 1 x from t)")
                    .fails(27, "select must provide exactly one column");
        });
    }

    @Test
    public void testFloatCursorColumn() throws Exception {
        // A FLOAT-typed cursor scalar routes to the DoubleCursorFunc FLOAT arm (read via Record#getFloat).
        // A float value derived from a table column widens to DOUBLE in projection, so a FLOAT constant
        // sub-query is used to keep the cursor column FLOAT and actually exercise the getFloat(0) path.
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(10))");
            // i > 5.5f -> 6..10
            assertQuery("select i from t where i > (select cast(5.5 as float))")
                    .noLeakCheck()
                    .returns("i\n6\n7\n8\n9\n10\n");
            // i < 5.5f -> 1..5
            assertQuery("select i from t where i < (select cast(5.5 as float))")
                    .noLeakCheck()
                    .returns("i\n1\n2\n3\n4\n5\n");
            // negated operators over the FLOAT arm
            assertQuery("select i from t where i >= (select cast(5.5 as float))")
                    .noLeakCheck()
                    .returns("i\n6\n7\n8\n9\n10\n");
        });
    }

    @Test
    public void testErrorNonNumericCursorColumn() throws Exception {
        // the < and > factories duplicate the validation code, so both must be asserted
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(10))");
            assertQuery("select i from t where i > (select 'abc' from t)")
                    .fails(27, "cannot compare INT and STRING");
            assertQuery("select i from t where i < (select 'abc' from t)")
                    .fails(27, "cannot compare INT and STRING");
        });
    }

    @Test
    public void testMultiRowCursorFails() throws Exception {
        // A scalar sub-query yielding more than one row is an error, reported at the sub-query position.
        // The INT/LONG/DOUBLE(FLOAT) cursor modes are separately implemented readers inside each factory,
        // so each mode is asserted for both operators.
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(10))");
            // INT cursor mode
            assertQuery("select i from t where i > (select x::int from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
            assertQuery("select i from t where i < (select x::int from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
            // LONG cursor mode
            assertQuery("select i from t where i > (select x from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
            assertQuery("select i from t where i < (select x from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
            // DOUBLE cursor mode
            assertQuery("select i from t where i > (select x::double from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
            assertQuery("select i from t where i < (select x::double from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
            // FLOAT cursor mode (shares DoubleCursorFunc, distinct reader arm)
            assertQuery("select i from t where i > (select x::float from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
            assertQuery("select i from t where i < (select x::float from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
        });
    }

    @Test
    public void testTypedNumericCursorScalars() throws Exception {
        // pins the BYTE/SHORT readers of the cursor scalar and the typed FLOAT/DOUBLE NULL
        // branches of the double comparison mode
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(10))");
            // BYTE cursor scalar
            assertQuery("select i from t where i > (select 5::byte)")
                    .noLeakCheck()
                    .returns("i\n6\n7\n8\n9\n10\n");
            assertQuery("select i from t where i < (select 5::byte)")
                    .noLeakCheck()
                    .returns("i\n1\n2\n3\n4\n");
            // SHORT cursor scalar, boundaries via the negated operators
            assertQuery("select i from t where i >= (select 5::short)")
                    .noLeakCheck()
                    .returns("i\n5\n6\n7\n8\n9\n10\n");
            assertQuery("select i from t where i <= (select 5::short)")
                    .noLeakCheck()
                    .returns("i\n1\n2\n3\n4\n5\n");
            // typed FLOAT/DOUBLE NULL scalars route through the double comparison mode and
            // must match no rows for every operator
            assertQuery("select i from t where i > (select null::double)")
                    .noLeakCheck()
                    .returns("i\n");
            assertQuery("select i from t where i < (select null::float)")
                    .noLeakCheck()
                    .returns("i\n");
            assertQuery("select i from t where i >= (select null::double)")
                    .noLeakCheck()
                    .returns("i\n");
            assertQuery("select i from t where i <= (select null::float)")
                    .noLeakCheck()
                    .returns("i\n");
        });
    }

    @Test
    public void testBareLiteralNullComparison() throws Exception {
        // End-to-end guard for the ColumnType NULL->CURSOR overload fix: a bare `null` literal is a scalar,
        // never a cursor. `i <= null` (i.e. not(i > null)) must compile to a scalar null-comparison instead
        // of binding to the `>(?C)` cursor-comparison factory and blowing up on getRecordCursorFactory().
        assertBareNullBehavior("int", "i");
    }

    @Test
    public void testGreaterThanIntCursor() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(10))");
            // min(i) = 1 -> i > 1 -> 2..10
            assertQuery("select i from t where i > (select min(i) from t)")
                    .noLeakCheck()
                    .returns("i\n2\n3\n4\n5\n6\n7\n8\n9\n10\n");
            // negated: i <= 1 -> 1
            assertQuery("select i from t where i <= (select min(i) from t)")
                    .noLeakCheck()
                    .returns("i\n1\n");
        });
    }

    @Test
    public void testGreaterThanLongOverflowsInt() throws Exception {
        assertMemoryLeak(() -> {
            // int values near INT_MAX; cursor scalar 5_000_000_000 overflows int range
            execute("create table t as (select (2000000000 + x)::int i from long_sequence(3))");
            // 2000000001..2000000003 > 5_000_000_000 -> none.
            // if the long scalar were narrowed to int (705032704) each row would wrongly match.
            assertQuery("select i from t where i > (select 5000000000)")
                    .noLeakCheck()
                    .returns("i\n");
        });
    }

    @Test
    public void testLessThanIntCursor() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::int i from long_sequence(10))");
            // max(i) = 10 -> i < 10 -> 1..9
            assertQuery("select i from t where i < (select max(i) from t)")
                    .noLeakCheck()
                    .returns("i\n1\n2\n3\n4\n5\n6\n7\n8\n9\n");
            // negated: i >= 10 -> 10
            assertQuery("select i from t where i >= (select max(i) from t)")
                    .noLeakCheck()
                    .returns("i\n10\n");
        });
    }

    @Test
    public void testLessThanLongOverflowsInt() throws Exception {
        assertMemoryLeak(() -> {
            // int values near INT_MAX; cursor scalar 5_000_000_000 overflows int range
            execute("create table t as (select (2000000000 + x)::int i from long_sequence(3))");
            // every int is < 5_000_000_000. if the long scalar were narrowed to int (705032704),
            // these rows would be wrongly dropped.
            assertQuery("select i from t where i < (select 5000000000)")
                    .noLeakCheck()
                    .returns("i\n2000000001\n2000000002\n2000000003\n");
        });
    }

    @Test
    public void testNullAndEmptyCursorSelectNoRows() throws Exception {
        assertNullAndEmptyCursorBehavior("int", "i");
    }

    @Test
    public void testNullLeftColumn() throws Exception {
        assertNullLeftColumnBehavior("int", "i");
    }

    @Test
    public void testParallelGroupByWithIntCursorPredicate() throws Exception {
        // max(qty)/2 = 50000 is an int cursor scalar -> long comparison mode
        assertParallelGroupByWithCursorPredicateBehavior("int");
    }

    @Test
    public void testPlanAsyncFilterLongMode() throws Exception {
        assertPlanAsyncFilterLongModeBehavior("int", "i");
    }

    @Test
    public void testParallelAsyncFilterAndKeyedSumLessThan() throws Exception {
        assertParallelAsyncFilterAndKeyedSumLessThanBehavior("int");
    }

}
