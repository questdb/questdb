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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.FunctionResolver;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UuidFunction;
import io.questdb.griffin.engine.functions.eq.EqUuidStrFunctionFactory;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.griffin.FunctionBindingHarness.wideSchema;

public class FunctionBinderWideCastTest extends AbstractCairoTest {
    @Test
    public void testGeoHashColumnsKeepEveryPrecisionAcrossLayouts() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                 FunctionBindingHarness binder = new FunctionBindingHarness(engine, new FunctionParser(configuration, engine.getFunctionFactoryCache()))) {
                for (int bits = 1; bits <= 60; bits++) {
                    final int type = ColumnType.getGeoHashTypeWithBits(bits);
                    final OutputSchema input = wideSchema(type);
                    final OutputSchema pruned = new OutputSchema().add(70, "value", type, true);
                    final OutputSchema reordered = new OutputSchema().add(80, "unused", ColumnType.INT, true).add(70, "value", type, true);
                    final BoundExpression expression = binder.bind(compiler.parseExpression("value"), input, null, sqlExecutionContext);
                    try (Function owner = binder.instantiate(expression, pruned, sqlExecutionContext);
                         Function worker = binder.instantiate(expression, reordered, sqlExecutionContext)) {
                        Assert.assertEquals(type, owner.getType());
                        Assert.assertEquals(type, worker.getType());
                        binder.clear();
                        compiler.clear();
                        input.clear();
                        pruned.clear();
                        reordered.clear();
                        final long value = (1L << bits) - 1;
                        Assert.assertEquals(value, GeoHashes.getGeoLong(type, owner, geoRecord(0, value)));
                        Assert.assertEquals(value, GeoHashes.getGeoLong(type, worker, geoRecord(1, value)));
                        Assert.assertEquals(GeoHashes.NULL, GeoHashes.getGeoLong(type, owner, geoRecord(0, GeoHashes.NULL)));
                    }
                }
            }
        });
    }

    @Test
    public void testGeoHashNarrowingConstantsAndNullsRetainFullTypes() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                 FunctionBindingHarness binder = new FunctionBindingHarness(engine, new FunctionParser(configuration, engine.getFunctionFactoryCache()))) {
                final int sourceType = ColumnType.getGeoHashTypeWithBits(60);
                for (int bits : new int[]{1, 7, 8, 15, 16, 31, 32, 60}) {
                    final int type = ColumnType.getGeoHashTypeWithBits(bits);
                    final String target = "geohash(" + bits + "b)";
                    final OutputSchema input = wideSchema(sourceType);
                    final OutputSchema output = new OutputSchema().add(70, "value", sourceType, true);
                    final BoundExpression expression = binder.bind(compiler.parseExpression("cast(value as " + target + ")"), input, null, sqlExecutionContext);
                    try (Function owner = binder.instantiate(expression, output, sqlExecutionContext);
                         Function worker = binder.instantiate(expression, input, sqlExecutionContext)) {
                        binder.clear();
                        Assert.assertEquals(type, owner.getType());
                        final long value = 0x123456789abcdefL;
                        Assert.assertEquals(value >> (60 - bits), GeoHashes.getGeoLong(type, owner, geoRecord(0, value)));
                        Assert.assertEquals(value >> (60 - bits), GeoHashes.getGeoLong(type, worker, geoRecord(48, value)));
                        Assert.assertEquals(GeoHashes.NULL, GeoHashes.getGeoLong(type, owner, geoRecord(0, GeoHashes.NULL)));
                    }
                    for (String source : new String[]{"1L", "null"}) {
                        final OutputSchema empty = new OutputSchema();
                        final BoundExpression constant = binder.bind(compiler.parseExpression("cast(" + source + " as " + target + ")"), empty, null, sqlExecutionContext);
                        Assert.assertTrue(constant instanceof ConstantExpression);
                        try (Function owner = binder.instantiate(constant, empty, sqlExecutionContext);
                             Function worker = binder.instantiate(constant, empty, sqlExecutionContext)) {
                            binder.clear();
                            compiler.clear();
                            Assert.assertEquals(type, owner.getType());
                            Assert.assertEquals(type, worker.getType());
                            final long expected = source.equals("null") ? GeoHashes.NULL : 1;
                            Assert.assertEquals(expected, GeoHashes.getGeoLong(type, owner, null));
                            Assert.assertEquals(expected, GeoHashes.getGeoLong(type, worker, null));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testInvalidUuidTextClosesBothArgumentsAndPreservesCleanupFailure() throws Exception {
        assertMemoryLeak(() -> {
            final int[] closes = new int[2];
            final RuntimeException first = new RuntimeException("uuid close");
            final RuntimeException second = new RuntimeException("text close");
            final ObjList<Function> args = new ObjList<>();
            args.add(new UuidFunction() {
                @Override
                public void close() {
                    closes[0]++;
                    throw first;
                }

                @Override
                public long getLong128Hi(Record rec) {
                    return 0;
                }

                @Override
                public long getLong128Lo(Record rec) {
                    return 0;
                }
            });
            args.add(new StrFunction() {
                @Override
                public void close() {
                    closes[1]++;
                    throw second;
                }

                @Override
                public CharSequence getStrA(Record rec) {
                    return "invalid uuid";
                }

                @Override
                public CharSequence getStrB(Record rec) {
                    return getStrA(rec);
                }

                @Override
                public boolean isConstant() {
                    return true;
                }
            });
            final RuntimeException actual = Assert.assertThrows(RuntimeException.class,
                    () -> new EqUuidStrFunctionFactory().newInstance(0, args, new IntList(), configuration, sqlExecutionContext));
            Assert.assertSame(first, actual);
            Assert.assertArrayEquals(new Throwable[]{second}, actual.getSuppressed());
            Assert.assertArrayEquals(new int[]{1, 1}, closes);
            Assert.assertNull(args.getQuick(0));
            Assert.assertNull(args.getQuick(1));
        });
    }

    @Test
    public void testInvalidUuidTextClosesNativeOperandForEveryAlias() throws Exception {
        assertMemoryLeak(() -> {
            final long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
            final boolean[] allocated = {false};
            final FunctionParser parser = new FunctionParser(configuration, new FunctionResolver(configuration, engine.getFunctionFactoryCache()) {
                @Override
                public Function createFunction(FunctionFactoryDescriptor overload, int position, CharSequence name,
                                               ObjList<Function> args, IntList positions, SqlExecutionContext context) throws SqlException {
                    final Function result = super.createFunction(overload, position, name, args, positions, context);
                    if (Chars.equals(name, "in")) {
                        allocated[0] |= Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS) > before;
                    }
                    return result;
                }
            });
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                 FunctionBindingHarness binder = new FunctionBindingHarness(engine, parser)) {
                final OutputSchema input = new OutputSchema().add(70, "id", ColumnType.LONG, true);
                final String operand = "(CASE WHEN id IN (1,2,3) THEN '00000000-0000-0000-0000-000000000001'::uuid ELSE null::uuid END)";
                for (String op : new String[]{"=", "!=", "<>"}) {
                    for (boolean swapped : new boolean[]{false, true}) {
                        final String sql = swapped ? "'invalid uuid'" + op + operand : operand + op + "'invalid uuid'";
                        final BoundExpression expression = binder.bind(compiler.parseExpression(sql), input, null, sqlExecutionContext);
                        Assert.assertTrue(allocated[0]);
                        Assert.assertTrue(expression instanceof ConstantExpression);
                        Assert.assertEquals(before, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS));
                        try (Function owner = binder.instantiate(expression, new OutputSchema(), sqlExecutionContext)) {
                            Assert.assertEquals(!op.equals("="), owner.getBool(null));
                        }
                        binder.clear();
                    }
                }
            }
        });
    }

    @Test
    public void testWideConstantsOwnCopiedPayloadsAfterBinderReuse() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                 FunctionBindingHarness binder = new FunctionBindingHarness(engine, new FunctionParser(configuration, engine.getFunctionFactoryCache()))) {
                final OutputSchema empty = new OutputSchema();
                for (String sql : new String[]{"'00000000-0000-0002-0000-000000000001'::uuid",
                        "0x0000000000000004000000000000000300000000000000020000000000000001::long256",
                        "null::uuid", "null::long256"}) {
                    final BoundExpression expression = binder.bind(compiler.parseExpression(sql), empty, null, sqlExecutionContext);
                    Assert.assertTrue(expression instanceof ConstantExpression);
                    try (Function owner = binder.instantiate(expression, empty, sqlExecutionContext);
                         Function worker = binder.instantiate(expression, empty, sqlExecutionContext)) {
                        binder.clear();
                        binder.bind(compiler.parseExpression("'other'"), empty, null, sqlExecutionContext);
                        binder.clear();
                        compiler.clear();
                        final boolean isNull = sql.startsWith("null");
                        if (owner.getType() == ColumnType.UUID) {
                            Assert.assertEquals(isNull ? Numbers.LONG_NULL : 1, owner.getLong128Lo(null));
                            Assert.assertEquals(isNull ? Numbers.LONG_NULL : 2, worker.getLong128Hi(null));
                        } else {
                            final Long256 a = owner.getLong256A(null);
                            final Long256 b = worker.getLong256A(null);
                            assertLong256(a, isNull ? Numbers.LONG_NULL : 1, isNull ? Numbers.LONG_NULL : 2,
                                    isNull ? Numbers.LONG_NULL : 3, isNull ? Numbers.LONG_NULL : 4);
                            assertLong256(b, a.getLong0(), a.getLong1(), a.getLong2(), a.getLong3());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testWideTextCastsOwnIndependentBuffersAfterPruning() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                 FunctionBindingHarness binder = new FunctionBindingHarness(engine, new FunctionParser(configuration, engine.getFunctionFactoryCache()))) {
                for (int sourceType : new int[]{ColumnType.UUID, ColumnType.LONG256, ColumnType.getGeoHashTypeWithBits(60)}) {
                    for (String target : new String[]{"string", "varchar"}) {
                        final OutputSchema full = wideSchema(sourceType);
                        final OutputSchema pruned = new OutputSchema().add(70, "value", sourceType, true);
                        final BoundExpression expression = binder.bind(compiler.parseExpression("value::" + target), full, null, sqlExecutionContext);
                        try (Function owner = binder.instantiate(expression, pruned, sqlExecutionContext);
                             Function worker = binder.instantiate(expression, full, sqlExecutionContext)) {
                            Assert.assertNotSame(owner, worker);
                            Assert.assertFalse(owner.isThreadSafe());
                            binder.clear();
                            compiler.clear();
                            final Record a = wideRecord(0, 1);
                            final Record b = wideRecord(0, 2);
                            final Record c = wideRecord(48, 3);
                            if (target.equals("string")) {
                                final CharSequence first = owner.getStrA(a);
                                final String copied = first.toString();
                                final CharSequence second = owner.getStrB(b);
                                Assert.assertNotEquals(copied, second.toString());
                                Assert.assertNotEquals(copied, worker.getStrA(c).toString());
                                TestUtils.assertEquals(copied, first);
                            } else {
                                final Utf8Sequence first = owner.getVarcharA(a);
                                final String copied = Utf8s.toString(first);
                                Assert.assertNotEquals(copied, Utf8s.toString(owner.getVarcharB(b)));
                                Assert.assertNotEquals(copied, Utf8s.toString(worker.getVarcharA(c)));
                                Assert.assertEquals(copied, Utf8s.toString(first));
                            }
                        }
                    }
                }
            }
        });
    }

    private static void assertLong256(Long256 value, long l0, long l1, long l2, long l3) {
        Assert.assertEquals(l0, value.getLong0());
        Assert.assertEquals(l1, value.getLong1());
        Assert.assertEquals(l2, value.getLong2());
        Assert.assertEquals(l3, value.getLong3());
    }

    private static Record geoRecord(int index, long value) {
        return wideRecord(index, value);
    }

    private static Record wideRecord(int index, long value) {
        return new Record() {
            private final Long256Impl wide = new Long256Impl();

            {
                wide.setAll(value, 2, 3, 4);
            }

            @Override
            public byte getGeoByte(int columnIndex) {
                Assert.assertEquals(index, columnIndex);
                return (byte) value;
            }

            @Override
            public int getGeoInt(int columnIndex) {
                Assert.assertEquals(index, columnIndex);
                return (int) value;
            }

            @Override
            public long getGeoLong(int columnIndex) {
                Assert.assertEquals(index, columnIndex);
                return value;
            }

            @Override
            public short getGeoShort(int columnIndex) {
                Assert.assertEquals(index, columnIndex);
                return (short) value;
            }

            @Override
            public long getLong128Lo(int columnIndex) {
                Assert.assertEquals(index, columnIndex);
                return value;
            }

            @Override
            public long getLong128Hi(int columnIndex) {
                Assert.assertEquals(index, columnIndex);
                return 2;
            }

            @Override
            public Long256 getLong256A(int columnIndex) {
                Assert.assertEquals(index, columnIndex);
                return wide;
            }

            @Override
            public Long256 getLong256B(int columnIndex) {
                return getLong256A(columnIndex);
            }
        };
    }
}
