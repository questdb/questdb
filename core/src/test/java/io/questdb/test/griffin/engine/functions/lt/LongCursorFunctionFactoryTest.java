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
 * Tests for the {@code long < (sub-query)} / {@code long > (sub-query)} operators, where the right-hand
 * side is a scalar sub-query (cursor). When the cursor scalar is an integer type the comparison is done
 * as a {@code long} comparison, so {@code long} values beyond 2^53 keep full precision (a comparison via
 * {@code double} would conflate them).
 *
 * @see io.questdb.griffin.engine.functions.lt.LtLongCursorFunctionFactory
 * @see io.questdb.griffin.engine.functions.lt.GtLongCursorFunctionFactory
 */
public class LongCursorFunctionFactoryTest extends AbstractCursorFunctionFactoryTest {

    // 2^53, 2^53+1, 2^53+2 : the middle value is NOT representable as a double (it rounds to 2^53),
    // so any comparison performed via double would conflate 2^53 and 2^53+1.
    private static final long POW2_53 = 9007199254740992L;

    @Test
    public void testWorkerStateSharedExecutesCursorOnceAndRefreshes() throws Exception {
        // a LONG cursor scalar -> the long comparison mode of the worker-state contract
        assertWorkerStateSharedBehavior("long", "l", "long");
    }

    @Test
    public void testDoubleCursorScalar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::long l from long_sequence(10))");
            // avg(l) = 5.5 -> double comparison mode
            assertQuery("select l from t where l < (select avg(l) from t)")
                    .noLeakCheck()
                    .returns("l\n1\n2\n3\n4\n5\n");
            assertQuery("select l from t where l > (select avg(l) from t)")
                    .noLeakCheck()
                    .returns("l\n6\n7\n8\n9\n10\n");
        });
    }

    @Test
    public void testErrorMultipleColumns() throws Exception {
        // the < and > factories duplicate the validation code, so both must be asserted
        assertMemoryLeak(() -> {
            execute("create table t as (select x::long l from long_sequence(10))");
            assertQuery("select l from t where l > (select max(l), 1 x from t)")
                    .fails(27, "select must provide exactly one column");
            assertQuery("select l from t where l < (select max(l), 1 x from t)")
                    .fails(27, "select must provide exactly one column");
        });
    }

    @Test
    public void testFloatCursorColumn() throws Exception {
        // A FLOAT-typed cursor scalar routes to the DoubleCursorFunc FLOAT arm (read via Record#getFloat).
        // A float value derived from a table column widens to DOUBLE in projection, so a FLOAT constant
        // sub-query is used to keep the cursor column FLOAT and actually exercise the getFloat(0) path.
        assertMemoryLeak(() -> {
            execute("create table t as (select x::long l from long_sequence(10))");
            // l > 5.5f -> 6..10
            assertQuery("select l from t where l > (select cast(5.5 as float))")
                    .noLeakCheck()
                    .returns("l\n6\n7\n8\n9\n10\n");
            // l < 5.5f -> 1..5
            assertQuery("select l from t where l < (select cast(5.5 as float))")
                    .noLeakCheck()
                    .returns("l\n1\n2\n3\n4\n5\n");
            // negated operators over the FLOAT arm
            assertQuery("select l from t where l <= (select cast(5.5 as float))")
                    .noLeakCheck()
                    .returns("l\n1\n2\n3\n4\n5\n");
        });
    }

    @Test
    public void testIntCursorColumn() throws Exception {
        // An INT-typed cursor scalar goes through readScalarLong's INT branch (Numbers.intToLong).
        // That mapping is the sole guard that an INT_NULL cursor scalar becomes LONG_NULL (matching no
        // rows) rather than -2147483648L; a plain widening would make l > (select null::int) match rows.
        assertMemoryLeak(() -> {
            execute("create table t as (select x::long l from long_sequence(10))");
            // non-null INT cursor scalar: l > 5 -> 6..10, l < 5 -> 1..4
            assertQuery("select l from t where l > (select 5::int)")
                    .noLeakCheck()
                    .returns("l\n6\n7\n8\n9\n10\n");
            assertQuery("select l from t where l < (select 5::int)")
                    .noLeakCheck()
                    .returns("l\n1\n2\n3\n4\n");
            // negated: l <= 5 -> 1..5
            assertQuery("select l from t where l <= (select 5::int)")
                    .noLeakCheck()
                    .returns("l\n1\n2\n3\n4\n5\n");
            // INT_NULL cursor scalar must map to LONG_NULL -> matches no rows for every operator
            assertQuery("select l from t where l > (select null::int)")
                    .noLeakCheck()
                    .returns("l\n");
            assertQuery("select l from t where l < (select null::int)")
                    .noLeakCheck()
                    .returns("l\n");
            assertQuery("select l from t where l >= (select null::int)")
                    .noLeakCheck()
                    .returns("l\n");
            assertQuery("select l from t where l <= (select null::int)")
                    .noLeakCheck()
                    .returns("l\n");
        });
    }

    @Test
    public void testErrorNonNumericCursorColumn() throws Exception {
        // the < and > factories duplicate the validation code, so both must be asserted
        assertMemoryLeak(() -> {
            execute("create table t as (select x::long l from long_sequence(10))");
            assertQuery("select l from t where l > (select 'abc' from t)")
                    .fails(27, "cannot compare LONG and STRING");
            assertQuery("select l from t where l < (select 'abc' from t)")
                    .fails(27, "cannot compare LONG and STRING");
        });
    }

    @Test
    public void testMultiRowCursorFails() throws Exception {
        // A scalar sub-query yielding more than one row is an error, reported at the sub-query position.
        // The LONG and DOUBLE(FLOAT) cursor modes are separately implemented readers inside each factory,
        // so each mode is asserted for both operators.
        assertMemoryLeak(() -> {
            execute("create table t as (select x::long l from long_sequence(10))");
            // LONG cursor mode
            assertQuery("select l from t where l > (select x::long from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
            assertQuery("select l from t where l < (select x::long from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
            // DOUBLE cursor mode
            assertQuery("select l from t where l > (select x::double from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
            assertQuery("select l from t where l < (select x::double from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
            // FLOAT cursor mode (shares DoubleCursorFunc, distinct reader arm)
            assertQuery("select l from t where l > (select x::float from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
            assertQuery("select l from t where l < (select x::float from long_sequence(2))")
                    .fails(27, "scalar sub-query returned more than one row");
        });
    }

    @Test
    public void testTypedNumericCursorScalars() throws Exception {
        // pins the BYTE/SHORT readers of the cursor scalar and the typed FLOAT/DOUBLE NULL
        // branches of the double comparison mode
        assertMemoryLeak(() -> {
            execute("create table t as (select x::long l from long_sequence(10))");
            // BYTE cursor scalar
            assertQuery("select l from t where l > (select 5::byte)")
                    .noLeakCheck()
                    .returns("l\n6\n7\n8\n9\n10\n");
            assertQuery("select l from t where l < (select 5::byte)")
                    .noLeakCheck()
                    .returns("l\n1\n2\n3\n4\n");
            // SHORT cursor scalar, boundaries via the negated operators
            assertQuery("select l from t where l >= (select 5::short)")
                    .noLeakCheck()
                    .returns("l\n5\n6\n7\n8\n9\n10\n");
            assertQuery("select l from t where l <= (select 5::short)")
                    .noLeakCheck()
                    .returns("l\n1\n2\n3\n4\n5\n");
            // typed FLOAT/DOUBLE NULL scalars route through the double comparison mode and
            // must match no rows for every operator
            assertQuery("select l from t where l > (select null::double)")
                    .noLeakCheck()
                    .returns("l\n");
            assertQuery("select l from t where l < (select null::float)")
                    .noLeakCheck()
                    .returns("l\n");
            assertQuery("select l from t where l >= (select null::double)")
                    .noLeakCheck()
                    .returns("l\n");
            assertQuery("select l from t where l <= (select null::float)")
                    .noLeakCheck()
                    .returns("l\n");
        });
    }

    @Test
    public void testBareLiteralNullComparison() throws Exception {
        // End-to-end guard for the ColumnType NULL->CURSOR overload fix: a bare `null` literal is a scalar,
        // never a cursor. `l <= null` (i.e. not(l > null)) must compile to a scalar null-comparison instead
        // of binding to the `>(?C)` cursor-comparison factory and blowing up on getRecordCursorFactory().
        assertBareNullBehavior("long", "l");
    }

    @Test
    public void testGreaterThanLongCursor() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::long l from long_sequence(10))");
            assertQuery("select l from t where l > (select min(l) from t)")
                    .noLeakCheck()
                    .returns("l\n2\n3\n4\n5\n6\n7\n8\n9\n10\n");
            // negated: l <= min -> 1
            assertQuery("select l from t where l <= (select min(l) from t)")
                    .noLeakCheck()
                    .returns("l\n1\n");
        });
    }

    @Test
    public void testGreaterThanPreservesLongPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (l long)");
            execute("insert into t values (" + POW2_53 + "), (" + (POW2_53 + 1) + "), (" + (POW2_53 + 2) + ")");
            // l > 2^53 : as long -> {2^53+1, 2^53+2}. via double, 2^53+1 == 2^53 -> it would be dropped.
            assertQuery("select l from t where l > (select " + POW2_53 + ")")
                    .noLeakCheck()
                    .returns("l\n" + (POW2_53 + 1) + "\n" + (POW2_53 + 2) + "\n");
        });
    }

    @Test
    public void testLessThanLongCursor() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select x::long l from long_sequence(10))");
            assertQuery("select l from t where l < (select max(l) from t)")
                    .noLeakCheck()
                    .returns("l\n1\n2\n3\n4\n5\n6\n7\n8\n9\n");
            // negated: l >= max -> 10
            assertQuery("select l from t where l >= (select max(l) from t)")
                    .noLeakCheck()
                    .returns("l\n10\n");
        });
    }

    @Test
    public void testLessThanPreservesLongPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t (l long)");
            execute("insert into t values (" + POW2_53 + "), (" + (POW2_53 + 1) + "), (" + (POW2_53 + 2) + ")");
            // l < 2^53+1 : as long -> {2^53}. via double, 2^53+1 rounds to 2^53, so nothing would match.
            assertQuery("select l from t where l < (select " + (POW2_53 + 1) + ")")
                    .noLeakCheck()
                    .returns("l\n" + POW2_53 + "\n");
        });
    }

    @Test
    public void testNullAndEmptyCursorSelectNoRows() throws Exception {
        assertNullAndEmptyCursorBehavior("long", "l");
    }

    @Test
    public void testNullLeftColumn() throws Exception {
        assertNullLeftColumnBehavior("long", "l");
    }

    @Test
    public void testParallelGroupByWithLongCursorPredicate() throws Exception {
        // max(qty)/2 = 50000 is a long cursor scalar -> long comparison mode
        assertParallelGroupByWithCursorPredicateBehavior("long");
    }

    @Test
    public void testPlanAsyncFilterLongMode() throws Exception {
        assertPlanAsyncFilterLongModeBehavior("long", "l");
    }

    @Test
    public void testParallelAsyncFilterAndKeyedSumLessThan() throws Exception {
        assertParallelAsyncFilterAndKeyedSumLessThanBehavior("long");
    }

}
