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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.eq.EqTimestampFunctionFactory;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Assert;
import org.junit.Test;

public class EqTimestampFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testConvertedBothConstantFoldsToBooleanConstant() throws Exception {
        // Constants of different precision (MICRO on one side, NANO on the other):
        // factory converts at factory time and folds to a BooleanConstant. No per-row
        // timestamp calls, no per-row driver.from() conversions.
        CountingTimestampFunction nanos = new CountingTimestampFunction(
                ColumnType.TIMESTAMP_NANO,
                NanosTimestampDriver.floor("2020-01-01T00:00:00.000001000Z"),
                true,
                false
        );
        CountingTimestampFunction micros = new CountingTimestampFunction(
                ColumnType.TIMESTAMP,
                MicrosTimestampDriver.floor("2020-01-01T00:00:00.000001Z"),
                true,
                false
        );
        ObjList<Function> args = new ObjList<>();
        args.add(nanos);
        args.add(micros);

        try (Function function = getFunctionFactory().newInstance(0, args, null, configuration, sqlExecutionContext)) {
            Assert.assertTrue(function.isConstant());
            Assert.assertTrue(function.getBool(null));

            for (int i = 0; i < 1_000; i++) {
                Assert.assertTrue(function.getBool(null));
            }
            Assert.assertEquals(1, nanos.getTimestampCalls);
            Assert.assertEquals(1, micros.getTimestampCalls);
        }
    }

    @Test
    public void testConvertedBothRuntimeConstCachesBoth() throws Exception {
        // Two runtime-constants of different precisions: both sides are cached in
        // init() and the left side is converted once, not per row.
        CountingTimestampFunction micros = new CountingTimestampFunction(
                ColumnType.TIMESTAMP,
                MicrosTimestampDriver.floor("2020-01-01T00:00:00.000001Z"),
                false,
                true
        );
        CountingTimestampFunction nanos = new CountingTimestampFunction(
                ColumnType.TIMESTAMP_NANO,
                NanosTimestampDriver.floor("2020-01-01T00:00:00.000001000Z"),
                false,
                true
        );
        ObjList<Function> args = new ObjList<>();
        args.add(micros);
        args.add(nanos);

        try (Function function = getFunctionFactory().newInstance(0, args, null, configuration, sqlExecutionContext)) {
            Assert.assertEquals(0, micros.getTimestampCalls);
            Assert.assertEquals(0, nanos.getTimestampCalls);

            function.init(null, sqlExecutionContext);
            Assert.assertEquals(1, micros.getTimestampCalls);
            Assert.assertEquals(1, nanos.getTimestampCalls);

            for (int i = 0; i < 1_000; i++) {
                Assert.assertTrue(function.getBool(null));
            }
            Assert.assertEquals(1, micros.getTimestampCalls);
            Assert.assertEquals(1, nanos.getTimestampCalls);
        }
    }

    @Test
    public void testConvertedRuntimeConstAndConstantCachesBoth() throws Exception {
        // Runtime-const on the lower-precision side (MICRO), constant on the
        // higher-precision side (NANO): factory captures the constant right value at
        // construction and caches the converted runtime-const left value in init().
        // No per-row evaluation or conversion occurs.
        CountingTimestampFunction runtimeConst = new CountingTimestampFunction(
                ColumnType.TIMESTAMP,
                MicrosTimestampDriver.floor("2020-01-01T00:00:00.000001Z"),
                false,
                true
        );
        CountingTimestampFunction constant = new CountingTimestampFunction(
                ColumnType.TIMESTAMP_NANO,
                NanosTimestampDriver.floor("2020-01-01T00:00:00.000001000Z"),
                true,
                false
        );
        ObjList<Function> args = new ObjList<>();
        args.add(runtimeConst);
        args.add(constant);

        try (Function function = getFunctionFactory().newInstance(0, args, null, configuration, sqlExecutionContext)) {
            Assert.assertEquals(1, constant.getTimestampCalls);
            Assert.assertEquals(0, runtimeConst.getTimestampCalls);

            function.init(null, sqlExecutionContext);
            Assert.assertEquals(1, constant.getTimestampCalls);
            Assert.assertEquals(1, runtimeConst.getTimestampCalls);

            for (int i = 0; i < 1_000; i++) {
                Assert.assertTrue(function.getBool(null));
            }
            Assert.assertEquals(1, constant.getTimestampCalls);
            Assert.assertEquals(1, runtimeConst.getTimestampCalls);
        }
    }

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
            assertQueryNoLeakCheck("""
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
            assertQueryNoLeakCheck("""
                            column
                            true
                            false
                            """,
                    "select '2020-01-01T00:00:00.000001Z'::timestamp = ts from x");
            // Flipped: triggers the initial swap-normalization path as well.
            assertQueryNoLeakCheck("""
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
            assertQueryNoLeakCheck("""
                            column
                            true
                            false
                            """,
                    "select ts = '2020-01-01T00:00:00.000001Z'::timestamp from x");
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
            assertQueryNoLeakCheck("""
                            column
                            false
                            true
                            """,
                    "select ts = :bind from x");
            // Flipped: exercises the initial swap-normalization before the
            // left-converts left-runtime-const path.
            assertQueryNoLeakCheck("""
                            column
                            false
                            true
                            """,
                    "select :bind = ts from x");
        });
    }

    @Test
    public void testMatchedBothConstantFoldsToBooleanConstant() throws Exception {
        // Both sides are constants of matching precision: factory folds to a
        // BooleanConstant, so getTimestamp is only called at factory time and getBool
        // performs no timestamp evaluation.
        CountingTimestampFunction left = new CountingTimestampFunction(
                ColumnType.TIMESTAMP,
                MicrosTimestampDriver.floor("2020-01-01T00:00:00.000001Z"),
                true,
                false
        );
        CountingTimestampFunction right = new CountingTimestampFunction(
                ColumnType.TIMESTAMP,
                MicrosTimestampDriver.floor("2020-01-01T00:00:00.000001Z"),
                true,
                false
        );
        ObjList<Function> args = new ObjList<>();
        args.add(left);
        args.add(right);

        try (Function function = getFunctionFactory().newInstance(0, args, null, configuration, sqlExecutionContext)) {
            Assert.assertTrue(function.isConstant());
            Assert.assertEquals(1, left.getTimestampCalls);
            Assert.assertEquals(1, right.getTimestampCalls);

            for (int i = 0; i < 1_000; i++) {
                Assert.assertTrue(function.getBool(null));
            }
            Assert.assertEquals(1, left.getTimestampCalls);
            Assert.assertEquals(1, right.getTimestampCalls);
        }
    }

    @Test
    public void testMatchedBothConstantFoldsToBooleanConstantFalse() throws Exception {
        // Same folding as above, but values differ so the folded BooleanConstant is
        // false.
        CountingTimestampFunction left = new CountingTimestampFunction(
                ColumnType.TIMESTAMP,
                MicrosTimestampDriver.floor("2020-01-01T00:00:00.000001Z"),
                true,
                false
        );
        CountingTimestampFunction right = new CountingTimestampFunction(
                ColumnType.TIMESTAMP,
                MicrosTimestampDriver.floor("2020-01-01T00:00:00.000002Z"),
                true,
                false
        );
        ObjList<Function> args = new ObjList<>();
        args.add(left);
        args.add(right);

        try (Function function = getFunctionFactory().newInstance(0, args, null, configuration, sqlExecutionContext)) {
            Assert.assertTrue(function.isConstant());
            Assert.assertFalse(function.getBool(null));
        }
    }

    @Test
    public void testMatchedBothRuntimeConstCachesBothInInit() throws Exception {
        // Two runtime-constants of matching precision: both sides are cached in init()
        // so no per-row timestamp evaluations happen.
        CountingTimestampFunction left = new CountingTimestampFunction(
                ColumnType.TIMESTAMP,
                MicrosTimestampDriver.floor("2020-01-01T00:00:00.000001Z"),
                false,
                true
        );
        CountingTimestampFunction right = new CountingTimestampFunction(
                ColumnType.TIMESTAMP,
                MicrosTimestampDriver.floor("2020-01-01T00:00:00.000001Z"),
                false,
                true
        );
        ObjList<Function> args = new ObjList<>();
        args.add(left);
        args.add(right);

        try (Function function = getFunctionFactory().newInstance(0, args, null, configuration, sqlExecutionContext)) {
            Assert.assertEquals(0, left.getTimestampCalls);
            Assert.assertEquals(0, right.getTimestampCalls);

            function.init(null, sqlExecutionContext);
            Assert.assertEquals(1, left.getTimestampCalls);
            Assert.assertEquals(1, right.getTimestampCalls);

            for (int i = 0; i < 1_000; i++) {
                Assert.assertTrue(function.getBool(null));
            }
            Assert.assertEquals(1, left.getTimestampCalls);
            Assert.assertEquals(1, right.getTimestampCalls);
        }
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
            assertQueryNoLeakCheck("""
                            column
                            false
                            true
                            """,
                    "select ts = :bind from x");
            assertQueryNoLeakCheck("""
                            column
                            false
                            true
                            """,
                    "select :bind = ts from x");
        });
    }

    @Test
    public void testMatchedRuntimeConstAndConstantPrefersConstant() throws Exception {
        // Swap moves the constant to the left and the runtime-const to the right. The
        // constant side is captured at construction; the runtime-const side is cached
        // once during init() so no per-row virtual call is made.
        CountingTimestampFunction runtimeConst = new CountingTimestampFunction(
                ColumnType.TIMESTAMP,
                MicrosTimestampDriver.floor("2020-01-01T00:00:00.000001Z"),
                false,
                true
        );
        CountingTimestampFunction constant = new CountingTimestampFunction(
                ColumnType.TIMESTAMP,
                MicrosTimestampDriver.floor("2020-01-01T00:00:00.000001Z"),
                true,
                false
        );
        ObjList<Function> args = new ObjList<>();
        args.add(runtimeConst);
        args.add(constant);

        try (Function function = getFunctionFactory().newInstance(0, args, null, configuration, sqlExecutionContext)) {
            Assert.assertSame(constant, ((BinaryFunction) function).getLeft());
            Assert.assertSame(runtimeConst, ((BinaryFunction) function).getRight());
            Assert.assertEquals(1, constant.getTimestampCalls);
            Assert.assertEquals(0, runtimeConst.getTimestampCalls);

            function.init(null, sqlExecutionContext);
            Assert.assertEquals(1, constant.getTimestampCalls);
            Assert.assertEquals(1, runtimeConst.getTimestampCalls);

            for (int i = 0; i < 1_000; i++) {
                Assert.assertTrue(function.getBool(null));
            }
            Assert.assertEquals(1, constant.getTimestampCalls);
            Assert.assertEquals(1, runtimeConst.getTimestampCalls);
        }
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

    @Test
    public void testNotEqualsNewInstancePaths() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (a timestamp, b timestamp, ts timestamp, ts_ns timestamp_ns)");
            execute("insert into x values " +
                    "('2020-01-01T00:00:00.000001Z', '2020-01-01T00:00:00.000001Z', '2020-01-01T00:00:00.000001Z', '2020-01-01T00:00:00.000001000Z'), " +
                    "('2020-01-01T00:00:00.000002Z', '2020-01-01T00:00:00.000003Z', '2020-01-01T00:00:00.000002Z', '2020-01-01T00:00:00.000002000Z'), " +
                    "('2020-01-01T00:00:00.000003Z', '2020-01-01T00:00:00.000004Z', '2020-01-01T00:00:00.000003Z', '2020-01-01T00:00:00.000004000Z')");
            bindVariableService.clear();
            bindVariableService.setStr("str_bind", "2020-01-01T00:00:00.000002Z");
            bindVariableService.setTimestamp("micro_bind", MicrosTimestampDriver.floor("2020-01-01T00:00:00.000002Z"));
            assertQueryNoLeakCheck("""
                            column\tcolumn1\tcolumn2\tcolumn3\tcolumn4\tcolumn5\tcolumn6
                            false\tfalse\tfalse\ttrue\ttrue\ttrue\ttrue
                            true\ttrue\tfalse\tfalse\tfalse\tfalse\tfalse
                            true\ttrue\ttrue\ttrue\ttrue\ttrue\ttrue
                            """,
                    "select " +
                            "a != b, " +
                            "ts != '2020-01-01T00:00:00.000001Z'::timestamp, " +
                            "ts != ts_ns, " +
                            "ts != '2020-01-01T00:00:00.000002000Z'::timestamp_ns, " +
                            "ts != :str_bind, " +
                            ":micro_bind != ts_ns, " +
                            ":micro_bind != ts " +
                            "from x");
        });
    }

    @Test
    public void testNullTimestampHandling() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, ts_ns timestamp_ns)");
            execute("insert into x values " +
                    "(null, null), " +
                    "('2020-01-01T00:00:00.000001Z', '2020-01-01T00:00:00.000001000Z'), " +
                    "(null, '2020-01-01T00:00:00.000002000Z'), " +
                    "('2020-01-01T00:00:00.000003Z', null)");
            bindVariableService.clear();
            bindVariableService.setTimestamp("null_bind", Numbers.LONG_NULL);
            bindVariableService.setStr("null_str_bind", null);
            assertQueryNoLeakCheck("""
                            column\tcolumn1\tcolumn2\tcolumn3\tcolumn4\tcolumn5\tcolumn6\tcolumn7\tcolumn8\tcolumn9
                            true\ttrue\ttrue\ttrue\ttrue\ttrue\ttrue\ttrue\ttrue\ttrue
                            false\tfalse\ttrue\tfalse\tfalse\tfalse\tfalse\ttrue\tfalse\tfalse
                            true\ttrue\ttrue\ttrue\ttrue\tfalse\tfalse\ttrue\tfalse\ttrue
                            false\tfalse\ttrue\tfalse\tfalse\ttrue\ttrue\ttrue\ttrue\tfalse
                            """,
                    "select " +
                            "null::timestamp = ts, " +
                            "ts = null::timestamp, " +
                            "null::timestamp = null::timestamp, " +
                            ":null_bind = ts, " +
                            "ts = :null_bind, " +
                            "null::timestamp = ts_ns, " +
                            "ts_ns = null::timestamp, " +
                            "null::timestamp = null::timestamp_ns, " +
                            ":null_bind = ts_ns, " +
                            "ts = :null_str_bind " +
                            "from x");
        });
    }

    @Test
    public void testStringBindTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp)");
            execute("insert into x values " +
                    "('2020-01-01T00:00:00.000001Z'), " +
                    "('2020-01-01T00:00:00.000002Z')");
            bindVariableService.clear();
            bindVariableService.setStr("bind", "2020-01-01T00:00:00.000002Z");
            assertQueryNoLeakCheck("""
                            column
                            false
                            true
                            """,
                    "select ts = :bind from x");
        });
    }

    @Test
    public void testStringBindTimestampNsColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp_ns)");
            execute("insert into x values " +
                    "('2020-01-01T00:00:00.000002000Z'), " +
                    "('2020-01-01T00:00:00.000002123Z'), " +
                    "('2020-01-01T00:00:00.000002124Z')");
            bindVariableService.clear();
            bindVariableService.setStr("bind", "2020-01-01T00:00:00.000002123Z");
            assertQueryNoLeakCheck("""
                            column
                            false
                            true
                            false
                            """,
                    "select ts = :bind from x");
            assertQueryNoLeakCheck("""
                            column
                            false
                            true
                            false
                            """,
                    "select :bind = ts from x");
        });
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new EqTimestampFunctionFactory();
    }

    private static class CountingTimestampFunction extends TimestampFunction {
        private final boolean constant;
        private final boolean runtimeConstant;
        private final long value;
        private int getTimestampCalls;

        private CountingTimestampFunction(int timestampType, long value, boolean constant, boolean runtimeConstant) {
            super(timestampType);
            this.value = value;
            this.constant = constant;
            this.runtimeConstant = runtimeConstant;
        }

        @Override
        public long getTimestamp(Record rec) {
            getTimestampCalls++;
            return value;
        }

        @Override
        public boolean isConstant() {
            return constant;
        }

        @Override
        public boolean isRuntimeConstant() {
            return runtimeConstant;
        }
    }
}
