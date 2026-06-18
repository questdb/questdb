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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.ByteFunction;
import io.questdb.griffin.engine.functions.CharFunction;
import io.questdb.griffin.engine.functions.DateFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.FloatFunction;
import io.questdb.griffin.engine.functions.GeoByteFunction;
import io.questdb.griffin.engine.functions.GeoIntFunction;
import io.questdb.griffin.engine.functions.GeoLongFunction;
import io.questdb.griffin.engine.functions.GeoShortFunction;
import io.questdb.griffin.engine.functions.IPv4Function;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.RuntimeConstFunction;
import io.questdb.griffin.engine.functions.ShortFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.UuidFunction;
import io.questdb.std.IntHashSet;
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
    public void testAllFoldableTypesCacheAndRoundTrip() throws SqlException {
        // The wrapper reads its arg once in init() through a type-specific getter, caching it into
        // longValue/doubleValue/longValueHi, then serves the cached primitive every row. Each foldable
        // type has its own init()/getter pair; exercise all of them so a mis-wired getter, a truncating
        // cast or a hi/lo swap cannot regress unnoticed. We build the wrapper directly over a typed leaf
        // to isolate the per-type round-trip - the boundary-wrapping decision is covered by the
        // structural tests and testIsFoldableTypeMatrix. Each leaf throws UnsupportedOperationException
        // from every getter except its native one, so init() reading the wrong getter fails loudly.

        // BOOLEAN
        {
            final int[] c = {0};
            assertCachedRoundTrip(new BooleanFunction() {
                @Override
                public boolean getBool(Record rec) {
                    c[0]++;
                    return true;
                }

                @Override
                public boolean isRuntimeConstant() {
                    return true;
                }
            }, c, f -> f.getBool(null), Boolean.TRUE);
        }

        // BYTE
        {
            final int[] c = {0};
            assertCachedRoundTrip(new ByteFunction() {
                @Override
                public byte getByte(Record rec) {
                    c[0]++;
                    return (byte) 90;
                }

                @Override
                public boolean isRuntimeConstant() {
                    return true;
                }
            }, c, f -> f.getByte(null), (byte) 90);
        }

        // SHORT
        {
            final int[] c = {0};
            assertCachedRoundTrip(new ShortFunction() {
                @Override
                public short getShort(Record rec) {
                    c[0]++;
                    return (short) 12_345;
                }

                @Override
                public boolean isRuntimeConstant() {
                    return true;
                }
            }, c, f -> f.getShort(null), (short) 12_345);
        }

        // CHAR
        {
            final int[] c = {0};
            assertCachedRoundTrip(new CharFunction() {
                @Override
                public char getChar(Record rec) {
                    c[0]++;
                    return 'Q';
                }

                @Override
                public boolean isRuntimeConstant() {
                    return true;
                }
            }, c, f -> f.getChar(null), 'Q');
        }

        // INT
        {
            final int[] c = {0};
            assertCachedRoundTrip(new IntFunction() {
                @Override
                public int getInt(Record rec) {
                    c[0]++;
                    return 1_234_567;
                }

                @Override
                public boolean isRuntimeConstant() {
                    return true;
                }
            }, c, f -> f.getInt(null), 1_234_567);
        }

        // LONG
        {
            final int[] c = {0};
            assertCachedRoundTrip(new LongFunction() {
                @Override
                public long getLong(Record rec) {
                    c[0]++;
                    return 9_876_543_210L;
                }

                @Override
                public boolean isRuntimeConstant() {
                    return true;
                }
            }, c, f -> f.getLong(null), 9_876_543_210L);
        }

        // FLOAT
        {
            final int[] c = {0};
            assertCachedRoundTrip(new FloatFunction() {
                @Override
                public float getFloat(Record rec) {
                    c[0]++;
                    return 3.5f;
                }

                @Override
                public boolean isRuntimeConstant() {
                    return true;
                }
            }, c, f -> f.getFloat(null), 3.5f);
        }

        // DOUBLE
        {
            final int[] c = {0};
            assertCachedRoundTrip(new DoubleFunction() {
                @Override
                public double getDouble(Record rec) {
                    c[0]++;
                    return 6.25;
                }

                @Override
                public boolean isRuntimeConstant() {
                    return true;
                }
            }, c, f -> f.getDouble(null), 6.25);
        }

        // DATE
        {
            final int[] c = {0};
            assertCachedRoundTrip(new DateFunction() {
                @Override
                public long getDate(Record rec) {
                    c[0]++;
                    return 1_700_000_000_000L;
                }

                @Override
                public boolean isRuntimeConstant() {
                    return true;
                }
            }, c, f -> f.getDate(null), 1_700_000_000_000L);
        }

        // TIMESTAMP
        {
            final int[] c = {0};
            assertCachedRoundTrip(new TimestampFunction(ColumnType.TIMESTAMP) {
                @Override
                public long getTimestamp(Record rec) {
                    c[0]++;
                    return 1_700_000_000_000_000L;
                }

                @Override
                public boolean isRuntimeConstant() {
                    return true;
                }
            }, c, f -> f.getTimestamp(null), 1_700_000_000_000_000L);
        }

        // IPv4
        {
            final int[] c = {0};
            assertCachedRoundTrip(new IPv4Function() {
                @Override
                public int getIPv4(Record rec) {
                    c[0]++;
                    return 0x01020304;
                }

                @Override
                public boolean isRuntimeConstant() {
                    return true;
                }
            }, c, f -> f.getIPv4(null), 0x01020304);
        }

        // GEOBYTE
        {
            final int[] c = {0};
            assertCachedRoundTrip(new GeoByteFunction(ColumnType.getGeoHashTypeWithBits(5)) {
                @Override
                public byte getGeoByte(Record rec) {
                    c[0]++;
                    return (byte) 21;
                }

                @Override
                public boolean isRuntimeConstant() {
                    return true;
                }
            }, c, f -> f.getGeoByte(null), (byte) 21);
        }

        // GEOSHORT
        {
            final int[] c = {0};
            assertCachedRoundTrip(new GeoShortFunction(ColumnType.getGeoHashTypeWithBits(12)) {
                @Override
                public short getGeoShort(Record rec) {
                    c[0]++;
                    return (short) 2_047;
                }

                @Override
                public boolean isRuntimeConstant() {
                    return true;
                }
            }, c, f -> f.getGeoShort(null), (short) 2_047);
        }

        // GEOINT
        {
            final int[] c = {0};
            assertCachedRoundTrip(new GeoIntFunction(ColumnType.getGeoHashTypeWithBits(24)) {
                @Override
                public int getGeoInt(Record rec) {
                    c[0]++;
                    return 1_048_575;
                }

                @Override
                public boolean isRuntimeConstant() {
                    return true;
                }
            }, c, f -> f.getGeoInt(null), 1_048_575);
        }

        // GEOLONG
        {
            final int[] c = {0};
            assertCachedRoundTrip(new GeoLongFunction(ColumnType.getGeoHashTypeWithBits(40)) {
                @Override
                public long getGeoLong(Record rec) {
                    c[0]++;
                    return 1_099_511_627_775L;
                }

                @Override
                public boolean isRuntimeConstant() {
                    return true;
                }
            }, c, f -> f.getGeoLong(null), 1_099_511_627_775L);
        }

        // UUID (128-bit: both halves must be cached, independently and without swapping)
        {
            final int[] c = {0};
            final long lo = 0x1122334455667788L;
            final long hi = 0x99AABBCCDDEEFF00L;
            final RuntimeConstFunction f = new RuntimeConstFunction(new UuidFunction() {
                @Override
                public long getLong128Hi(Record rec) {
                    c[0]++;
                    return hi;
                }

                @Override
                public long getLong128Lo(Record rec) {
                    c[0]++;
                    return lo;
                }

                @Override
                public boolean isRuntimeConstant() {
                    return true;
                }
            });
            try {
                f.init(null, sqlExecutionContext);
                final int afterInit = c[0];
                assertEquals("uuid: both halves read in init()", 2, afterInit);
                for (int i = 0; i < 64; i++) {
                    assertEquals("uuid lo", lo, f.getLong128Lo(null));
                    assertEquals("uuid hi", hi, f.getLong128Hi(null));
                }
                assertEquals("uuid must be cached, not re-evaluated per row", afterInit, c[0]);
            } finally {
                f.close();
            }
        }
    }

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
    public void testIsFoldableTypeMatrix() {
        // Exactly these fixed-width scalar tags fold; every other type (variable-width STRING/VARCHAR/
        // BINARY, SYMBOL, LONG256, the 128-bit LONG128, decimals, intervals, arrays, cursors, ...)
        // delegates to the arg and is served per row. This doubles as a tripwire: the foldable set
        // here must stay in lockstep with the init() switch and getters in RuntimeConstFunction -
        // adding a tag without a matching init() case would make the wrapper throw at runtime, so a
        // new foldable type forces a new round-trip case in testAllFoldableTypesCacheAndRoundTrip too.
        IntHashSet foldable = new IntHashSet();
        foldable.add(ColumnType.BOOLEAN);
        foldable.add(ColumnType.BYTE);
        foldable.add(ColumnType.SHORT);
        foldable.add(ColumnType.CHAR);
        foldable.add(ColumnType.INT);
        foldable.add(ColumnType.LONG);
        foldable.add(ColumnType.FLOAT);
        foldable.add(ColumnType.DOUBLE);
        foldable.add(ColumnType.DATE);
        foldable.add(ColumnType.TIMESTAMP);
        foldable.add(ColumnType.IPv4);
        foldable.add(ColumnType.GEOBYTE);
        foldable.add(ColumnType.GEOSHORT);
        foldable.add(ColumnType.GEOINT);
        foldable.add(ColumnType.GEOLONG);
        foldable.add(ColumnType.UUID);

        for (short tag = ColumnType.UNDEFINED; tag <= ColumnType.NULL; tag++) {
            assertEquals(
                    "isFoldableType mismatch for " + ColumnType.nameOf(tag),
                    foldable.contains(tag),
                    RuntimeConstFunction.isFoldableType(tag)
            );
        }

        // isFoldableType keys off tagOf(): encoded variants fold like their tag, and the deliberate
        // exclusions (notably LONG128, even though the 128-bit UUID folds) stay excluded.
        assertTrue(RuntimeConstFunction.isFoldableType(ColumnType.getGeoHashTypeWithBits(5)));   // GEOBYTE
        assertTrue(RuntimeConstFunction.isFoldableType(ColumnType.getGeoHashTypeWithBits(12)));  // GEOSHORT
        assertTrue(RuntimeConstFunction.isFoldableType(ColumnType.getGeoHashTypeWithBits(24)));  // GEOINT
        assertTrue(RuntimeConstFunction.isFoldableType(ColumnType.getGeoHashTypeWithBits(40)));  // GEOLONG
        assertTrue(RuntimeConstFunction.isFoldableType(ColumnType.TIMESTAMP_NANO));
        assertFalse(RuntimeConstFunction.isFoldableType(ColumnType.LONG128));
        assertFalse(RuntimeConstFunction.isFoldableType(ColumnType.getDecimalType(18, 3)));
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

    private void assertCachedRoundTrip(Function arg, int[] counter, ValueReader reader, Object expected) throws SqlException {
        final RuntimeConstFunction f = new RuntimeConstFunction(arg);
        try {
            f.init(null, sqlExecutionContext);
            final int afterInit = counter[0];
            assertEquals("arg must be evaluated exactly once in init()", 1, afterInit);
            for (int i = 0; i < 64; i++) {
                assertEquals("cached value must round-trip", expected, reader.read(f));
            }
            assertEquals("runtime-constant value must be cached, not re-evaluated per row", afterInit, counter[0]);
        } finally {
            f.close();
        }
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

    @FunctionalInterface
    private interface ValueReader {
        Object read(RuntimeConstFunction f);
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
