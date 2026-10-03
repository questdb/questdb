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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.eq.EqIntFunctionFactory;
import io.questdb.griffin.engine.functions.eq.EqUuidStrFunctionFactory;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class FunctionBinderEqualityTest extends AbstractCairoTest {
    @Test
    public void testEqualityPairsStayWithinTheirRegistrationAndPreserveConstantFlags() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionFactory first = new EqUuidStrFunctionFactory();
            final FunctionFactory second = new EqUuidStrFunctionFactory();
            final FunctionFactory symmetric = new EqIntFunctionFactory();
            final FunctionFactory flagged = new FunctionFactory() {
                @Override
                public String getSignature() {
                    return "=(Ii)";
                }

                @Override
                public boolean isBoolean() {
                    return true;
                }

                @Override
                public Function newInstance(int position, ObjList<Function> args, IntList argPositions,
                                            CairoConfiguration configuration, SqlExecutionContext executionContext) {
                    return BooleanConstant.FALSE;
                }
            };
            final FunctionFactoryCache cache = new FunctionFactoryCache(configuration, List.of(first, second, symmetric, flagged));
            final ObjList<FunctionFactoryDescriptor> equalities = cache.getOverloadList("=");
            Assert.assertEquals(7, equalities.size());
            Assert.assertSame(first, equalities.getQuick(0).getFactory());
            Assert.assertSame(second, equalities.getQuick(2).getFactory());
            Assert.assertSame(equalities.getQuick(1), equalities.getQuick(0).getCommutedEquality());
            Assert.assertSame(equalities.getQuick(3), equalities.getQuick(2).getCommutedEquality());
            Assert.assertSame(symmetric, equalities.getQuick(4).getFactory());
            Assert.assertSame(equalities.getQuick(4), equalities.getQuick(4).getCommutedEquality());
            Assert.assertSame(flagged, equalities.getQuick(5).getFactory());
            Assert.assertNotSame(equalities.getQuick(5), equalities.getQuick(5).getCommutedEquality());
            for (int i = 0; i < equalities.size(); i++) {
                final FunctionFactoryDescriptor descriptor = equalities.getQuick(i);
                final FunctionFactoryDescriptor commuted = descriptor.getCommutedEquality();
                Assert.assertSame(descriptor, commuted.getCommutedEquality());
                Assert.assertEquals(descriptor.getArgTypeWithFlags(0), commuted.getArgTypeWithFlags(1));
                Assert.assertEquals(descriptor.getArgTypeWithFlags(1), commuted.getArgTypeWithFlags(0));
            }
            for (String token : new String[]{"!=", "<>"}) {
                final ObjList<FunctionFactoryDescriptor> negated = cache.getOverloadList(token);
                for (int i = 0; i < negated.size(); i++) {
                    Assert.assertNull(negated.getQuick(i).getCommutedEquality());
                }
            }
        });
    }

    @Test
    public void testCommutedEqualityPreservesSelectedOverloadAndArgumentPositions() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionFactoryCache cache = new FunctionFactoryCache(configuration,
                    List.of(new EqUuidStrFunctionFactory(), new EqUuidStrFunctionFactory()));
            final OutputSchema full = new OutputSchema().add(1, "unused", ColumnType.INT, true).add(7, "value", ColumnType.UUID, true);
            final OutputSchema pruned = new OutputSchema().add(7, "value", ColumnType.UUID, true);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                 FunctionBindingHarness binder = new FunctionBindingHarness(new FunctionParser(configuration, cache))) {
                final FunctionExpression original = (FunctionExpression) binder.bind(
                        compiler.parseExpression("'00000000-0000-0000-0000-000000000001' = value"), full, null, sqlExecutionContext);
                final FunctionExpression commuted = binder.commuteEquality(original);
                Assert.assertSame(original.getOverload().getCommutedEquality(), commuted.getOverload());
                Assert.assertSame(original.argumentAt(1), commuted.argumentAt(0));
                Assert.assertSame(original.argumentAt(0), commuted.argumentAt(1));
                Assert.assertEquals(original.getArgumentPosition(1), commuted.getArgumentPosition(0));
                Assert.assertEquals(original.getArgumentPosition(0), commuted.getArgumentPosition(1));
                Assert.assertEquals(original.getPosition(), commuted.getPosition());
                Assert.assertEquals(original.getFunctionFlags(), commuted.getFunctionFlags());
                final FunctionExpression restored = binder.commuteEquality(commuted);
                Assert.assertSame(original.getOverload(), restored.getOverload());
                Assert.assertSame(original.argumentAt(0), restored.argumentAt(0));
                try (Function owner = binder.instantiate(commuted, pruned, sqlExecutionContext);
                     Function retained = binder.instantiate(original, full, sqlExecutionContext)) {
                    binder.clear();
                    compiler.clear();
                    Assert.assertTrue(owner.getBool(uuidRecord(0, 1)));
                    Assert.assertFalse(owner.getBool(uuidRecord(0, 2)));
                    Assert.assertTrue(retained.getBool(uuidRecord(1, 1)));
                    Assert.assertFalse(retained.getBool(uuidRecord(1, 2)));
                }
            }
        });
    }

    private static Record uuidRecord(int index, long lo) {
        return new Record() {
            @Override
            public long getLong128Hi(int columnIndex) {
                Assert.assertEquals(index, columnIndex);
                return 0;
            }

            @Override
            public long getLong128Lo(int columnIndex) {
                Assert.assertEquals(index, columnIndex);
                return lo;
            }
        };
    }
}
