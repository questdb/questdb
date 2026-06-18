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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.RuntimeConstFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RuntimeConstFunctionTest extends BaseFunctionFactoryTest {

    private static final AtomicInteger evalCounter = new AtomicInteger();

    @Test
    public void testCompositeRuntimeConstArgIsWrappedAndEvaluatedOnce() throws SqlException {
        evalCounter.set(0);
        registerTestFunctions();

        FunctionParser parser = createFunctionParser();
        // boundary's left arg is per-row, so its runtime-constant right arg is a maximal subtree to fold.
        Function f = parseFunction("boundary(per_row(), rc_unary(rc_unary(rc_count())))", new GenericRecordMetadata(), parser);

        // structural: wrapped exactly once, at the boundary (maximal subtree, no double-wrapping below)
        assertTrue(f instanceof BinaryFunction);
        Function right = ((BinaryFunction) f).getRight();
        assertTrue("right arg must be folded", right instanceof RuntimeConstFunction);
        Function inner = ((RuntimeConstFunction) right).getArg();
        assertFalse("inner subtree must not be double-wrapped", inner instanceof RuntimeConstFunction);
        assertTrue(inner instanceof UnaryFunction);
        assertFalse(((UnaryFunction) inner).getArg() instanceof RuntimeConstFunction);

        // behavioral: evaluated once in init(), regardless of row count
        f.init(null, sqlExecutionContext);
        assertEquals(1, evalCounter.get());
        for (int i = 0; i < 100; i++) {
            assertEquals(7, f.getInt(null));
        }
        assertEquals("runtime-constant subtree must be evaluated once per cursor", 1, evalCounter.get());
        f.close();
    }

    @Test
    public void testRuntimeConstLeafIsNotWrapped() throws SqlException {
        evalCounter.set(0);
        registerTestFunctions();

        FunctionParser parser = createFunctionParser();
        // rc_count() is a trivial runtime-constant leaf (already caches); wrapping would only add overhead.
        Function f = parseFunction("boundary(per_row(), rc_count())", new GenericRecordMetadata(), parser);

        assertTrue(f instanceof BinaryFunction);
        Function right = ((BinaryFunction) f).getRight();
        assertFalse("runtime-constant leaf must not be folded", right instanceof RuntimeConstFunction);

        // not folded -> re-evaluated per row
        f.init(null, sqlExecutionContext);
        for (int i = 0; i < 5; i++) {
            assertEquals(7, f.getInt(null));
        }
        assertEquals(5, evalCounter.get());
        f.close();
    }

    @Test
    public void testRuntimeConstParentIsNotFoldedAtChildBoundary() throws SqlException {
        evalCounter.set(0);
        registerTestFunctions();

        FunctionParser parser = createFunctionParser();
        // wholly runtime constant: no enclosing boundary, so nothing is wrapped (we only fold at boundaries)
        Function f = parseFunction("rc_unary(rc_unary(rc_count()))", new GenericRecordMetadata(), parser);

        assertFalse("a runtime-constant root has no enclosing boundary to fold against", f instanceof RuntimeConstFunction);
        assertTrue(f instanceof UnaryFunction);
        assertFalse(((UnaryFunction) f).getArg() instanceof RuntimeConstFunction);
        f.close();
    }

    @Test
    public void testEndToEndCorrectnessTimestampThresholdInCase() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades AS (" +
                            "  SELECT (x * 1000000)::timestamp ts, x amount FROM long_sequence(10)" +
                            ") TIMESTAMP(ts) PARTITION BY NONE"
            );

            // runtime-constant threshold ($1 + dateadd) in a CASE: folded, but must match the
            // literal-threshold reference (where dateadd of constants folds at compile time).
            bindVariableService.clear();
            bindVariableService.setTimestamp(0, 6_000_000L);

            assertSqlCursors(
                    "SELECT sum(CASE WHEN ts >= dateadd('s', -2, 6000000::timestamp) THEN amount ELSE 0 END) s FROM trades",
                    "SELECT sum(CASE WHEN ts >= dateadd('s', -2, $1::timestamp) THEN amount ELSE 0 END) s FROM trades"
            );

            // reference value: 6s threshold - 2s => 4s; rows ts >= 4s have amount 4..10 -> 49
            assertQuery(
                    "SELECT sum(CASE WHEN ts >= dateadd('s', -2, 6000000::timestamp) THEN amount ELSE 0 END) s FROM trades"
            ).noRandomAccess().expectSize().returns("s\n49\n");
        });
    }

    @Test
    public void testEndToEndPlanIsTransparent() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE trades AS (" +
                            "  SELECT (x * 1000000)::timestamp ts, x amount FROM long_sequence(10)" +
                            ") TIMESTAMP(ts) PARTITION BY NONE"
            );

            // the wrapper delegates toPlan() to its arg, so it stays invisible in plans
            StringSink planSink = new StringSink();
            printSql(
                    "EXPLAIN SELECT sum(CASE WHEN ts >= dateadd('s', -2, now()) THEN amount ELSE 0 END) s FROM trades",
                    planSink
            );
            String plan = planSink.toString();
            assertFalse("wrapper must not leak into the plan: " + plan, plan.contains("RuntimeConst"));
            assertTrue("plan should still mention the folded dateadd subtree: " + plan, plan.contains("dateadd"));
        });
    }

    private static void registerTestFunctions() {
        functions.clear();
        // Zero-arg, per-row (neither constant nor runtime constant) leaf.
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "per_row()";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
                return new IntFunction() {
                    @Override
                    public int getInt(Record rec) {
                        return 0;
                    }
                };
            }
        });
        // Zero-arg runtime-constant leaf that counts how many times it is evaluated.
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "rc_count()";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
                return new IntFunction() {
                    @Override
                    public int getInt(Record rec) {
                        evalCounter.incrementAndGet();
                        return 7;
                    }

                    @Override
                    public boolean isRuntimeConstant() {
                        return true;
                    }

                    @Override
                    public boolean isThreadSafe() {
                        return true;
                    }
                };
            }
        });
        // Runtime-constant unary (composite) pass-through.
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "rc_unary(I)";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
                final Function arg = args.getQuick(0);
                return new RcUnaryFunction(arg);
            }
        });
        // non-runtime-constant boundary; returns only its right arg, so left needs no record
        functions.add(new FunctionFactory() {
            @Override
            public String getSignature() {
                return "boundary(II)";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
                final Function left = args.getQuick(0);
                final Function right = args.getQuick(1);
                return new BoundaryFunction(left, right);
            }
        });
    }

    private static class BoundaryFunction extends IntFunction implements BinaryFunction {
        private final Function left;
        private final Function right;

        private BoundaryFunction(Function left, Function right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public int getInt(Record rec) {
            return right.getInt(rec);
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public String getName() {
            return "boundary";
        }

        @Override
        public Function getRight() {
            return right;
        }
    }

    private static class RcUnaryFunction extends IntFunction implements UnaryFunction {
        private final Function arg;

        private RcUnaryFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public int getInt(Record rec) {
            return arg.getInt(rec);
        }

        @Override
        public boolean isRuntimeConstant() {
            return true;
        }

        @Override
        public String getName() {
            return "rc_unary";
        }
    }
}
