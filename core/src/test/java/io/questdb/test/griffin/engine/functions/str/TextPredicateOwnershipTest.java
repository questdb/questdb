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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.griffin.engine.functions.bool.InStrFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastVarcharToStrFunctionFactory;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.regex.ILikeStrFunctionFactory;
import io.questdb.griffin.engine.functions.regex.ILikeVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.regex.LikeStrFunctionFactory;
import io.questdb.griffin.engine.functions.regex.LikeVarcharFunctionFactory;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TextPredicateOwnershipTest extends AbstractCairoTest {
    @Test
    public void testDiscardedProbeCloseFailureIsNotRepeatedBySelectedConstruction() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final ObjList<FunctionFactory> factories = factories();
            for (int i = 0; i < factories.size(); i++) {
                final FunctionFactory factory = factories.getQuick(i);
                final CountingValue value = new CountingValue(true);
                try (Function ignored = parser.getFunctionResolver().createFunction(new FunctionFactoryDescriptor(factory), 0, factory.getSignature(),
                        args(factory, value, true), new IntList(), sqlExecutionContext)) {
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "discarded text probe");
                }
                Assert.assertEquals(1, value.closeCount);
            }
        });
    }

    @Test
    public void testEmptyMembershipAndNullOrEmptyLikePatternsCloseNativeProbe() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<FunctionFactory> factories = factories();
            for (int i = 0; i < factories.size(); i++) {
                final FunctionFactory factory = factories.getQuick(i);
                for (boolean isNullPattern : new boolean[]{false, true}) {
                    final CountingValue value = new CountingValue(false);
                    final ObjList<Function> args = args(factory, value, isNullPattern);
                    try (Function result = factory.newInstance(0, args, new IntList(), configuration, sqlExecutionContext)) {
                        Assert.assertFalse(result.getBool(null));
                        Assert.assertEquals(factory.getSignature(), 1, value.closeCount);
                    }
                    Assert.assertEquals(1, value.closeCount);
                }
            }
        });
    }

    private static ObjList<Function> args(FunctionFactory factory, CountingValue value, boolean isNullPattern) {
        final Function probe = factory.getSignature().contains("Ø") ? value : new CastVarcharToStrFunctionFactory.Func(value);
        final ObjList<Function> args = new ObjList<>(probe);
        if (!factory.getSignature().startsWith("in(")) {
            args.add(isNullPattern ? StrConstant.NULL : StrConstant.EMPTY);
        }
        return args;
    }

    private static ObjList<FunctionFactory> factories() {
        return new ObjList<>(new LikeStrFunctionFactory(), new ILikeStrFunctionFactory(),
                new LikeVarcharFunctionFactory(), new ILikeVarcharFunctionFactory(),
                new InStrFunctionFactory(), new InVarcharFunctionFactory());
    }

    private static class CountingValue extends VarcharFunction {
        private final boolean isCloseFailure;
        private final DirectUtf8Sink sink = new DirectUtf8Sink(16);
        private int closeCount;

        private CountingValue(boolean isCloseFailure) {
            this.isCloseFailure = isCloseFailure;
        }

        @Override
        public void close() {
            closeCount++;
            sink.close();
            if (isCloseFailure) {
                throw new IllegalStateException("discarded text probe");
            }
        }

        @Override
        public Utf8Sequence getVarcharA(Record record) {
            throw new AssertionError("discarded probe must not be evaluated");
        }

        @Override
        public Utf8Sequence getVarcharB(Record record) {
            throw new AssertionError("discarded probe must not be evaluated");
        }
    }
}
