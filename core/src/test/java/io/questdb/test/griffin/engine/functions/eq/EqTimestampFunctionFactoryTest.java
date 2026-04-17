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

package io.questdb.test.griffin.engine.functions.eq;

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.eq.EqTimestampFunctionFactory;
import io.questdb.std.NumericException;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class EqTimestampFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testEquals() throws SqlException, NumericException {
        long t1 = MicrosTimestampDriver.floor("2020-12-31T23:59:59.000000Z");
        long t2 = MicrosTimestampDriver.floor("2020-12-31T23:59:59.000001Z");
        callBySignature("=(NN)", t1, t1).andAssert(true);
        callBySignature("=(NN)", t1, t2).andAssert(false);

        long t3 = NanosTimestampDriver.floor("2020-12-31T23:59:59.000000000Z");
        long t4 = NanosTimestampDriver.floor("2020-12-31T23:59:59.000000001Z");
        callBySignature("=(NN)", t3, t3).andAssert(true);
        callBySignature("=(NN)", t3, t4).andAssert(false);
    }

    @Test
    public void testFunc() throws Exception {
        // Func: matched precision, both sides dynamic (column references).
        assertMemoryLeak(() -> {
            execute("create table x (a timestamp, b timestamp)");
            execute("insert into x values " +
                    "('2020-01-01T00:00:00.000001Z', '2020-01-01T00:00:00.000001Z'), " +
                    "('2020-01-01T00:00:00.000002Z', '2020-01-01T00:00:00.000003Z')");
            assertSql("""
                            column
                            true
                            false
                            """,
                    "select a = b from x");
        });
    }

    @Test
    public void testLeftConstFuncConverting() throws Exception {
        // LeftConstFunc via the converting branch: const is on the lower-precision
        // side (MICRO), column on the higher-precision side (NANO). The factory
        // pre-converts the const value once.
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp_ns)");
            execute("insert into x values " +
                    "('2020-01-01T00:00:00.000001000Z'), " +
                    "('2020-01-01T00:00:00.000002000Z')");
            assertSql("""
                            column
                            true
                            false
                            """,
                    "select '2020-01-01T00:00:00.000001Z'::timestamp = ts from x");
            // Flipped: triggers the initial swap-normalization path as well.
            assertSql("""
                            column
                            true
                            false
                            """,
                    "select ts = '2020-01-01T00:00:00.000001Z'::timestamp from x");
        });
    }

    @Test
    public void testLeftConstFuncMatchedWithCacheSwap() throws Exception {
        // LeftConstFunc via the matched-types branch, hit through the cache-swap:
        // const is placed on the right in the source, factory swaps it onto the
        // left so the cached-value comparison runs.
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp)");
            execute("insert into x values " +
                    "('2020-01-01T00:00:00.000001Z'), " +
                    "('2020-01-01T00:00:00.000002Z')");
            assertSql("""
                            column
                            true
                            false
                            """,
                    "select ts = '2020-01-01T00:00:00.000001Z'::timestamp from x");
        });
    }

    @Test
    public void testLeftConvertRightRunTimeConstFunc() throws Exception {
        // LeftConvertRightRunTimeConstFunc: column on the lower-precision side
        // (MICRO), runtime-const bind variable on the higher-precision side
        // (STRING, which resolves to TIMESTAMP_NS via implicit cast). The bug fix
        // caches the bind variable's timestamp in init() so the STRING parse does
        // not repeat per row.
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp)");
            execute("insert into x values " +
                    "('2020-01-01T00:00:00.000001Z'), " +
                    "('2020-01-01T00:00:00.000002Z')");
            bindVariableService.clear();
            bindVariableService.setStr("bind", "2020-01-01T00:00:00.000002Z");
            assertSql("""
                            column
                            false
                            true
                            """,
                    "select ts = :bind from x");
        });
    }

    @Test
    public void testLeftRunTimeConstFunc() throws Exception {
        // LeftRunTimeConstFunc: runtime-const on the lower-precision side (MICRO
        // bind variable), column on the higher-precision side (NANO). The factory
        // converts and caches the bind value in init().
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp_ns)");
            execute("insert into x values " +
                    "('2020-01-01T00:00:00.000001000Z'), " +
                    "('2020-01-01T00:00:00.000002000Z')");
            bindVariableService.clear();
            bindVariableService.setTimestamp("bind", MicrosTimestampDriver.floor("2020-01-01T00:00:00.000002Z"));
            assertSql("""
                            column
                            false
                            true
                            """,
                    "select ts = :bind from x");
            // Flipped: exercises the initial swap-normalization before the
            // left-converts left-runtime-const path.
            assertSql("""
                            column
                            false
                            true
                            """,
                    "select :bind = ts from x");
        });
    }

    @Test
    public void testMatchedLeftRunTimeConstFunc() throws Exception {
        // MatchedLeftRunTimeConstFunc: matched precision with a runtime-const bind
        // variable. Covers direct left placement and the cache-swap path for a
        // right-side bind variable.
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp)");
            execute("insert into x values " +
                    "('2020-01-01T00:00:00.000001Z'), " +
                    "('2020-01-01T00:00:00.000002Z')");
            bindVariableService.clear();
            bindVariableService.setTimestamp("bind", MicrosTimestampDriver.floor("2020-01-01T00:00:00.000002Z"));
            assertSql("""
                            column
                            false
                            true
                            """,
                    "select ts = :bind from x");
            assertSql("""
                            column
                            false
                            true
                            """,
                    "select :bind = ts from x");
        });
    }

    @Test
    public void testLeftConvertFuncBothDynamic() throws Exception {
        // LeftConvertFunc: both sides are dynamic (column references), different precision
        // types. The lower-precision side is converted per row.
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, ts_ns timestamp_ns)");
            // Exact match: 1 microsecond == 1000 nanoseconds
            execute("insert into x values ('2020-01-01T00:00:00.000001Z', '2020-01-01T00:00:00.000001000Z')");
            // Non-match: 1 microsecond != 1001 nanoseconds (sub-microsecond precision)
            execute("insert into x values ('2020-01-01T00:00:00.000001Z', '2020-01-01T00:00:00.000001001Z')");
            // Non-match: 2 microseconds != 1 microsecond in nanos
            execute("insert into x values ('2020-01-01T00:00:00.000002Z', '2020-01-01T00:00:00.000001000Z')");
            assertSql("""
                            column
                            true
                            false
                            false
                            """,
                    "select ts = ts_ns from x");
            // Symmetric: reversed operands should give the same result
            assertSql("""
                            column
                            true
                            false
                            false
                            """,
                    "select ts_ns = ts from x");
        });
    }

    @Test
    public void testLeftConvertRightConstFunc() throws Exception {
        // LeftConvertRightConstFunc: column on the lower-precision side (MICRO),
        // constant literal on the higher-precision side (NANO). The factory caches
        // the constant NANO value at construction time, and per-row converts the
        // column's MICRO value to the NANO domain for the comparison.
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp)");
            // Exact match: 1 microsecond aligns to 1000 nanoseconds
            execute("insert into x values ('2020-01-01T00:00:00.000001Z')");
            // Non-match: 2 microseconds != 1000 nanoseconds
            execute("insert into x values ('2020-01-01T00:00:00.000002Z')");
            // Non-match: '2020-01-01T00:00:00.000001001Z' (1001 ns) cannot be matched
            // by a MICRO column (which can only represent multiples of 1000 ns)
            execute("insert into x values ('2020-01-01T00:00:00.000001Z')");
            assertSql("""
                            column
                            true
                            false
                            true
                            """,
                    "select ts = '2020-01-01T00:00:00.000001000Z'::timestamp_ns from x");
            // Flipped: NANO const on the left, MICRO column on the right.
            // The factory performs the symmetry swap (NANO->left becomes lower after swap:
            // left=MICRO col, right=NANO const) and still uses LeftConvertRightConstFunc.
            assertSql("""
                            column
                            true
                            false
                            true
                            """,
                    "select '2020-01-01T00:00:00.000001000Z'::timestamp_ns = ts from x");
        });
    }

    @Test
    public void testLeftConvertRightConstFuncSubMicrosecondNanoConst() throws Exception {
        // Regression: a NANO const with sub-microsecond precision should never match
        // a MICRO column value, since the MICRO column can only represent multiples
        // of 1000 nanoseconds.
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp)");
            execute("insert into x values ('2020-01-01T00:00:00.000001Z')");
            // '...000001001Z' has 1 extra nanosecond → cannot match any MICRO value
            assertSql("""
                            column
                            false
                            """,
                    "select ts = '2020-01-01T00:00:00.000001001Z'::timestamp_ns from x");
        });
    }

    @Test
    public void testNegatedFunc() throws Exception {
        // Func with negation (!=): matched precision, both sides dynamic.
        assertMemoryLeak(() -> {
            execute("create table x (a timestamp, b timestamp)");
            execute("insert into x values " +
                    "('2020-01-01T00:00:00.000001Z', '2020-01-01T00:00:00.000001Z'), " +
                    "('2020-01-01T00:00:00.000002Z', '2020-01-01T00:00:00.000003Z')");
            assertSql("""
                            column
                            false
                            true
                            """,
                    "select a != b from x");
        });
    }

    @Test
    public void testNegatedLeftConstFuncConverting() throws Exception {
        // LeftConstFunc via converting branch with negation (!=):
        // const MICRO, column NANO.
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp_ns)");
            execute("insert into x values " +
                    "('2020-01-01T00:00:00.000001000Z'), " +
                    "('2020-01-01T00:00:00.000002000Z')");
            assertSql("""
                            column
                            false
                            true
                            """,
                    "select '2020-01-01T00:00:00.000001Z'::timestamp != ts from x");
        });
    }

    @Test
    public void testNegatedLeftConvertRightConstFunc() throws Exception {
        // LeftConvertRightConstFunc with negation (!=):
        // column MICRO, const NANO. The pre-cached NANO const is compared using !=.
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp)");
            execute("insert into x values " +
                    "('2020-01-01T00:00:00.000001Z'), " +
                    "('2020-01-01T00:00:00.000002Z')");
            assertSql("""
                            column
                            false
                            true
                            """,
                    "select ts != '2020-01-01T00:00:00.000001000Z'::timestamp_ns from x");
        });
    }

    @Test
    public void testNegatedLeftRunTimeConstFunc() throws Exception {
        // LeftRunTimeConstFunc with negation (!=):
        // runtime-const MICRO bind variable, column NANO. The converted value is
        // cached in init() and used for != comparison.
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp_ns)");
            execute("insert into x values " +
                    "('2020-01-01T00:00:00.000001000Z'), " +
                    "('2020-01-01T00:00:00.000002000Z')");
            bindVariableService.clear();
            bindVariableService.setTimestamp("bind", MicrosTimestampDriver.floor("2020-01-01T00:00:00.000001Z"));
            assertSql("""
                            column
                            false
                            true
                            """,
                    "select ts != :bind from x");
        });
    }

    @Test
    public void testNegatedMatchedLeftRunTimeConstFunc() throws Exception {
        // MatchedLeftRunTimeConstFunc with negation (!=): matched precision with
        // runtime-const bind variable using the != operator.
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp)");
            execute("insert into x values " +
                    "('2020-01-01T00:00:00.000001Z'), " +
                    "('2020-01-01T00:00:00.000002Z')");
            bindVariableService.clear();
            bindVariableService.setTimestamp("bind", MicrosTimestampDriver.floor("2020-01-01T00:00:00.000001Z"));
            assertSql("""
                            column
                            false
                            true
                            """,
                    "select ts != :bind from x");
        });
    }

    @Test
    public void testNullTimestampEquality() throws Exception {
        // Null timestamp values: a null timestamp should not equal any non-null value,
        // and null == null is implementation-defined (QuestDB uses Long.MIN_VALUE sentinel).
        assertMemoryLeak(() -> {
            execute("create table x (a timestamp, b timestamp)");
            execute("insert into x values (null, null)");
            execute("insert into x values ('2020-01-01T00:00:00.000001Z', null)");
            execute("insert into x values (null, '2020-01-01T00:00:00.000001Z')");
            execute("insert into x values ('2020-01-01T00:00:00.000001Z', '2020-01-01T00:00:00.000001Z')");
            assertSql("""
                            column
                            true
                            false
                            false
                            true
                            """,
                    "select a = b from x");
        });
    }

    @Test
    public void testNullTimestampEqualityMixedPrecision() throws Exception {
        // Null handling for mixed-precision comparison (MICRO column, NANO column).
        // Null on either or both sides should not produce a false match.
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, ts_ns timestamp_ns)");
            execute("insert into x values (null, null)");
            execute("insert into x values ('2020-01-01T00:00:00.000001Z', null)");
            execute("insert into x values (null, '2020-01-01T00:00:00.000001000Z')");
            execute("insert into x values ('2020-01-01T00:00:00.000001Z', '2020-01-01T00:00:00.000001000Z')");
            assertSql("""
                            column
                            true
                            false
                            false
                            true
                            """,
                    "select ts = ts_ns from x");
        });
    }

    @Test
    public void testSwapNormalizationSymmetry() throws Exception {
        // Verifies the symmetry swap: regardless of which operand order is written
        // in SQL, equality results must be identical. Tests all three code paths:
        // const-column, column-const, and column-column.
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, ts_ns timestamp_ns)");
            execute("insert into x values ('2020-01-01T00:00:00.000001Z', '2020-01-01T00:00:00.000001000Z')");
            execute("insert into x values ('2020-01-01T00:00:00.000002Z', '2020-01-01T00:00:00.000001000Z')");

            // Column (MICRO) = Column (NANO)  vs  Column (NANO) = Column (MICRO)
            assertSql("""
                            a\tb
                            true\ttrue
                            false\tfalse
                            """,
                    "select ts = ts_ns a, ts_ns = ts b from x");

            // Column (MICRO) = Const (NANO)  vs  Const (NANO) = Column (MICRO)
            assertSql("""
                            a\tb
                            true\ttrue
                            false\tfalse
                            """,
                    "select ts = '2020-01-01T00:00:00.000001000Z'::timestamp_ns a, " +
                            "'2020-01-01T00:00:00.000001000Z'::timestamp_ns = ts b from x");

            // Const (MICRO) = Column (NANO)  vs  Column (NANO) = Const (MICRO)
            assertSql("""
                            a\tb
                            true\ttrue
                            false\tfalse
                            """,
                    "select '2020-01-01T00:00:00.000001Z'::timestamp = ts_ns a, " +
                            "ts_ns = '2020-01-01T00:00:00.000001Z'::timestamp b from x");
        });
    }

    @Test
    public void testMixedMicrosAndNanos() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (" +
                    "select " +
                    "timestamp_sequence(1000000, 1000000) as ts, " +
                    "timestamp_sequence_ns(0, 2000000000) as ts_ns " +
                    "from long_sequence(10)" +
                    ") timestamp(ts)");
            assertQuery("""
                            ts\tts_ns\tcolumn
                            1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:00.000000000Z\tfalse
                            1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:02.000000000Z\ttrue
                            1970-01-01T00:00:03.000000Z\t1970-01-01T00:00:04.000000000Z\tfalse
                            1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:06.000000000Z\tfalse
                            1970-01-01T00:00:05.000000Z\t1970-01-01T00:00:08.000000000Z\tfalse
                            1970-01-01T00:00:06.000000Z\t1970-01-01T00:00:10.000000000Z\tfalse
                            1970-01-01T00:00:07.000000Z\t1970-01-01T00:00:12.000000000Z\tfalse
                            1970-01-01T00:00:08.000000Z\t1970-01-01T00:00:14.000000000Z\tfalse
                            1970-01-01T00:00:09.000000Z\t1970-01-01T00:00:16.000000000Z\tfalse
                            1970-01-01T00:00:10.000000Z\t1970-01-01T00:00:18.000000000Z\tfalse
                            """,
                    "select ts, ts_ns, ts = ts_ns from x");
            assertQuery("""
                            ts\tts_ns\tcolumn
                            1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:00.000000000Z\tfalse
                            1970-01-01T00:00:02.000000Z\t1970-01-01T00:00:02.000000000Z\ttrue
                            1970-01-01T00:00:03.000000Z\t1970-01-01T00:00:04.000000000Z\tfalse
                            1970-01-01T00:00:04.000000Z\t1970-01-01T00:00:06.000000000Z\tfalse
                            1970-01-01T00:00:05.000000Z\t1970-01-01T00:00:08.000000000Z\tfalse
                            1970-01-01T00:00:06.000000Z\t1970-01-01T00:00:10.000000000Z\tfalse
                            1970-01-01T00:00:07.000000Z\t1970-01-01T00:00:12.000000000Z\tfalse
                            1970-01-01T00:00:08.000000Z\t1970-01-01T00:00:14.000000000Z\tfalse
                            1970-01-01T00:00:09.000000Z\t1970-01-01T00:00:16.000000000Z\tfalse
                            1970-01-01T00:00:10.000000Z\t1970-01-01T00:00:18.000000000Z\tfalse
                            """,
                    "select ts, ts_ns, ts_ns = ts from x");

            assertQuery("""
                            ts\tcolumn\tcolumn1
                            1970-01-01T00:00:01.000000Z\tfalse\tfalse
                            1970-01-01T00:00:02.000000Z\tfalse\tfalse
                            1970-01-01T00:00:03.000000Z\ttrue\tfalse
                            1970-01-01T00:00:04.000000Z\tfalse\tfalse
                            1970-01-01T00:00:05.000000Z\tfalse\tfalse
                            1970-01-01T00:00:06.000000Z\tfalse\tfalse
                            1970-01-01T00:00:07.000000Z\tfalse\tfalse
                            1970-01-01T00:00:08.000000Z\tfalse\tfalse
                            1970-01-01T00:00:09.000000Z\tfalse\tfalse
                            1970-01-01T00:00:10.000000Z\tfalse\tfalse
                            """,
                    "select ts, ts = '1970-01-01T00:00:03.000000000Z',ts = '1970-01-01T00:00:03.000000001Z'  from x");
            assertQuery("""
                            ts_ns\tcolumn
                            1970-01-01T00:00:00.000000000Z\tfalse
                            1970-01-01T00:00:02.000000000Z\tfalse
                            1970-01-01T00:00:04.000000000Z\ttrue
                            1970-01-01T00:00:06.000000000Z\tfalse
                            1970-01-01T00:00:08.000000000Z\tfalse
                            1970-01-01T00:00:10.000000000Z\tfalse
                            1970-01-01T00:00:12.000000000Z\tfalse
                            1970-01-01T00:00:14.000000000Z\tfalse
                            1970-01-01T00:00:16.000000000Z\tfalse
                            1970-01-01T00:00:18.000000000Z\tfalse
                            """,
                    "select ts_ns, '1970-01-01T00:00:04.000000Z' = ts_ns  from x");
        });
    }

    @Test
    public void testNotEquals() throws SqlException, NumericException {
        long t1 = MicrosTimestampDriver.floor("2020-12-31T23:59:59.000000Z");
        long t2 = MicrosTimestampDriver.floor("2020-12-31T23:59:59.000001Z");
        callBySignature("<>(NN)", t1, t1).andAssert(false);
        callBySignature("<>(NN)", t1, t2).andAssert(true);

        long t3 = NanosTimestampDriver.floor("2020-12-31T23:59:59.000000000Z");
        long t4 = NanosTimestampDriver.floor("2020-12-31T23:59:59.000000001Z");
        callBySignature("<>(NN)", t3, t3).andAssert(false);
        callBySignature("<>(NN)", t3, t4).andAssert(true);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new EqTimestampFunctionFactory();
    }
}