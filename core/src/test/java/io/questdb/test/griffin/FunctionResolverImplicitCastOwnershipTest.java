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
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.FunctionArray;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class FunctionResolverImplicitCastOwnershipTest extends AbstractCairoTest {
    @Test
    public void testArrayConstantIsFreedWhenOriginalRootCloseFails() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionArray array = new FunctionArray(ColumnType.DOUBLE, 1);
            array.setDimLen(0, 2);
            array.applyShape(configuration, 0);
            array.putFunction(0, new DoubleConstant(1));
            array.putFunction(1, new DoubleConstant(2));
            final RuntimeException closeFailure = new IllegalStateException("array root close");
            final int[] closeCount = {0};
            final FunctionFactory factory = new FunctionFactory() {
                @Override
                public String getSignature() {
                    return "owned_array()";
                }

                @Override
                public Function newInstance(int position, ObjList<Function> args, IntList argPositions,
                                            CairoConfiguration configuration, SqlExecutionContext context) {
                    return new ArrayFunction() {
                        {
                            type = ColumnType.encodeArrayType(ColumnType.DOUBLE, 1);
                        }

                        @Override
                        public void close() {
                            closeCount[0]++;
                            array.close();
                            throw closeFailure;
                        }

                        @Override
                        public ArrayView getArray(Record rec) {
                            return array;
                        }

                        @Override
                        public boolean isConstant() {
                            return true;
                        }
                    };
                }
            };
            final FunctionParser parser = new FunctionParser(configuration,
                    new FunctionFactoryCache(configuration, List.of(factory)));
            final ExpressionNode node = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.FUNCTION, "owned_array", 0, 17);
            try {
                parser.parseFunction(node, null, sqlExecutionContext);
                Assert.fail();
            } catch (IllegalStateException e) {
                Assert.assertSame(closeFailure, e);
            }
            Assert.assertEquals(1, closeCount[0]);
        });
    }

    @Test
    public void testArrayWrapperRetainsConstantInputWhenFoldingReturnsSameFunction() throws Exception {
        assertMemoryLeak(() -> {
            final TrackingString input = new TrackingString("{1,2}", true);
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            try (Function result = parser.getFunctionResolver().createImplicitCast(17, input, ColumnType.encodeArrayType(ColumnType.DOUBLE, 1), sqlExecutionContext)) {
                Assert.assertTrue(result.isConstant());
                Assert.assertEquals(0, input.closeCount);
                Assert.assertEquals(1, result.getArray(null).getDouble(0), 0);
                Assert.assertEquals(2, result.getArray(null).getDouble(1), 0);
            }
            Assert.assertEquals(1, input.closeCount);
        });
    }

    @Test
    public void testCloseFailureAfterFoldIsNotRetried() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            for (int target : new int[]{ColumnType.TIMESTAMP, ColumnType.getGeoHashTypeWithBits(5)}) {
                final TrackingString input = new TrackingString(target == ColumnType.TIMESTAMP ? "1970-01-01" : "u", true);
                input.closeFailure = new IllegalStateException("input close");
                try {
                    parser.getFunctionResolver().createImplicitCast(17, input, target, sqlExecutionContext);
                    Assert.fail();
                } catch (IllegalStateException e) {
                    Assert.assertSame(input.closeFailure, e);
                    Assert.assertEquals(0, e.getSuppressed().length);
                }
                Assert.assertEquals(1, input.closeCount);
            }
        });
    }

    @Test
    public void testConstantWrapperClosesInputOnce() throws Exception {
        assertMemoryLeak(() -> {
            final TrackingString input = new TrackingString("1970-01-01T00:00:00.000001Z", true);
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            try (Function result = parser.getFunctionResolver().createImplicitCast(17, input, ColumnType.TIMESTAMP, sqlExecutionContext)) {
                Assert.assertTrue(result.isConstant());
                Assert.assertEquals(1, result.getTimestamp(null));
                Assert.assertEquals(1, input.closeCount);
            }
            Assert.assertEquals(1, input.closeCount);
        });
    }

    @Test
    public void testFoldFailurePreservesPrimaryAndClosesInputOnce() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final TrackingString input = new TrackingString(null, true);
            input.readFailure = new IllegalStateException("input read");
            input.closeFailure = new IllegalStateException("input close");
            try {
                parser.getFunctionResolver().createImplicitCast(17, input, ColumnType.TIMESTAMP, sqlExecutionContext);
                Assert.fail();
            } catch (IllegalStateException e) {
                Assert.assertSame(input.readFailure, e);
                Assert.assertArrayEquals(new Throwable[]{input.closeFailure}, e.getSuppressed());
            }
            Assert.assertEquals(1, input.closeCount);
        });
    }

    @Test
    public void testIndependentConstantClosesInputOnce() throws Exception {
        assertMemoryLeak(() -> {
            final TrackingString input = new TrackingString("u", true);
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final int type = ColumnType.getGeoHashTypeWithBits(5);
            try (Function result = parser.getFunctionResolver().createImplicitCast(17, input, type, sqlExecutionContext)) {
                Assert.assertEquals(type, result.getType());
                Assert.assertEquals(26, result.getGeoByte(null));
                Assert.assertEquals(1, input.closeCount);
            }
            Assert.assertEquals(1, input.closeCount);
        });
    }

    @Test
    public void testNoCastLeavesInputWithCaller() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final TrackingString input = new TrackingString("unchanged", true);
            try (input) {
                Assert.assertNull(parser.getFunctionResolver().createImplicitCast(17, input, ColumnType.BINARY, sqlExecutionContext));
                Assert.assertEquals(0, input.closeCount);
                TestUtils.assertEquals("unchanged", input.getStrA(null));
            }
            Assert.assertEquals(1, input.closeCount);
        });
    }

    @Test
    public void testNonConstantWrapperOwnsInput() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final TrackingString input = new TrackingString("1970-01-01T00:00:00.000001Z", false);
            try (Function result = parser.getFunctionResolver().createImplicitCast(17, input, ColumnType.TIMESTAMP, sqlExecutionContext)) {
                Assert.assertFalse(result.isConstant());
                Assert.assertEquals(0, input.closeCount);
                Assert.assertEquals(1, result.getTimestamp(null));
                input.value = "1970-01-01T00:00:00.000002Z";
                Assert.assertEquals(2, result.getTimestamp(null));
            }
            Assert.assertEquals(1, input.closeCount);
        });
    }

    @Test
    public void testRawFactoryFailurePreservesPrimaryAndClosesInput() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final TrackingString input = new TrackingString("u", true);
            input.closeFailure = new IllegalStateException("input close");
            try {
                parser.getFunctionResolver().createImplicitCast(17, input, ColumnType.getGeoHashTypeWithBits(10), sqlExecutionContext);
                Assert.fail();
            } catch (SqlException e) {
                Assert.assertEquals(17, e.getPosition());
                TestUtils.assertContains(e.getFlyweightMessage(), "string is too short");
                Assert.assertArrayEquals(new Throwable[]{input.closeFailure}, e.getSuppressed());
            }
            Assert.assertEquals(1, input.closeCount);
        });
    }

    private static class TrackingString extends StrFunction {
        private final boolean constant;
        private int closeCount;
        private RuntimeException closeFailure;
        private RuntimeException readFailure;
        private String value;

        private TrackingString(String value, boolean constant) {
            this.value = value;
            this.constant = constant;
        }

        @Override
        public void close() {
            closeCount++;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }

        @Override
        public CharSequence getStrA(Record rec) {
            if (readFailure != null) {
                throw readFailure;
            }
            return value;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            return getStrA(rec);
        }

        @Override
        public boolean isConstant() {
            return constant;
        }
    }
}
