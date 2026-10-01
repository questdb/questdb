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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.cast.CastStrToVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.griffin.engine.functions.constants.VarcharConstant;
import io.questdb.griffin.engine.functions.str.LeftStrFunctionFactory;
import io.questdb.griffin.engine.functions.str.LeftVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.str.PositionFunctionFactory;
import io.questdb.griffin.engine.functions.str.PositionVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.str.ReplaceStrFunctionFactory;
import io.questdb.griffin.engine.functions.str.ReplaceVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.str.RightStrFunctionFactory;
import io.questdb.griffin.engine.functions.str.RightVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.str.StrPosFunctionFactory;
import io.questdb.griffin.engine.functions.str.StrPosVarcharFunctionFactory;
import io.questdb.griffin.engine.functions.str.SubStringFunctionFactory;
import io.questdb.griffin.engine.functions.str.SubStringVarcharFunctionFactory;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TextFunctionOwnershipTest extends AbstractCairoTest {
    @Test
    public void testNullCountAndSearchReleaseDiscardedValue() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<FunctionFactory> factories = new ObjList<>(
                    new LeftStrFunctionFactory(), new RightStrFunctionFactory(),
                    new LeftVarcharFunctionFactory(), new RightVarcharFunctionFactory(),
                    new StrPosFunctionFactory(), new PositionFunctionFactory(),
                    new StrPosVarcharFunctionFactory(), new PositionVarcharFunctionFactory());
            for (int i = 0; i < factories.size(); i++) {
                final FunctionFactory factory = factories.getQuick(i);
                final boolean isVarchar = factory.getSignature().contains("Ø");
                final CountingText value = new CountingText(isVarchar, false);
                final Function nullArgument = i < 4 ? IntConstant.NULL : isVarchar ? VarcharConstant.NULL : StrConstant.NULL;
                try (Function result = factory.newInstance(0, new ObjList<>(value.asFunction(), nullArgument), new IntList(), configuration, sqlExecutionContext)) {
                    if (i < 4) {
                        Assert.assertNull(result.getStrA(null));
                    } else {
                        Assert.assertEquals(Numbers.INT_NULL, result.getInt(null));
                    }
                    Assert.assertEquals(factory.getSignature(), 1, value.closeCount);
                    Assert.assertEquals(0, value.readCount);
                }
                Assert.assertEquals(1, value.closeCount);
            }
        });
    }

    @Test
    public void testSubstringNullAndEmptyBranchesReleaseAllArguments() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<FunctionFactory> factories = new ObjList<>(new SubStringFunctionFactory(), new SubStringVarcharFunctionFactory());
            for (int i = 0; i < factories.size(); i++) {
                for (int length : new int[]{0, Numbers.INT_NULL}) {
                    final CountingText value = new CountingText(i == 1, false);
                    try (Function result = factories.getQuick(i).newInstance(0,
                            new ObjList<>(value.asFunction(), IntConstant.newInstance(1), IntConstant.newInstance(length)),
                            new IntList(), configuration, sqlExecutionContext)) {
                        Assert.assertEquals(ColumnType.STRING, result.getType());
                        Assert.assertEquals(length == 0 ? "" : null, result.getStrA(null));
                        Assert.assertEquals(1, value.closeCount);
                        Assert.assertEquals(0, value.readCount);
                    }
                    Assert.assertEquals(1, value.closeCount);
                }
            }
        });
    }

    @Test
    public void testReplaceRetainsReturnedChildAndReleasesDiscardedArguments() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<FunctionFactory> factories = new ObjList<>(new ReplaceStrFunctionFactory(), new ReplaceVarcharFunctionFactory());
            for (int i = 0; i < factories.size(); i++) {
                final FunctionFactory factory = factories.getQuick(i);
                final Function empty = i == 1 ? VarcharConstant.EMPTY : StrConstant.EMPTY;
                final Function nullValue = i == 1 ? VarcharConstant.NULL : StrConstant.NULL;
                final CountingText value = new CountingText(i == 1, false);
                final CountingText replacement = new CountingText(i == 1, false);
                final Function valueFunction = value.asFunction();
                try (Function result = factory.newInstance(0, new ObjList<>(valueFunction, empty, replacement.asFunction()),
                        new IntList(), configuration, sqlExecutionContext)) {
                    Assert.assertSame(valueFunction, result);
                    Assert.assertEquals(0, value.closeCount);
                    Assert.assertEquals(1, replacement.closeCount);
                    Assert.assertEquals(0, replacement.readCount);
                }
                Assert.assertEquals(1, value.closeCount);
                Assert.assertEquals(1, replacement.closeCount);

                final CountingText term = new CountingText(i == 1, false);
                final CountingText other = new CountingText(i == 1, false);
                try (Function result = factory.newInstance(0, new ObjList<>(nullValue, term.asFunction(), other.asFunction()),
                        new IntList(), configuration, sqlExecutionContext)) {
                    Assert.assertSame(nullValue, result);
                    Assert.assertEquals(1, term.closeCount);
                    Assert.assertEquals(1, other.closeCount);
                }
                final CountingText discarded = new CountingText(i == 1, false);
                final CountingText discardedTerm = new CountingText(i == 1, false);
                try (Function result = factory.newInstance(0, new ObjList<>(discarded.asFunction(), discardedTerm.asFunction(), nullValue),
                        new IntList(), configuration, sqlExecutionContext)) {
                    Assert.assertNull(result.getStrA(null));
                    Assert.assertEquals(1, discarded.closeCount);
                    Assert.assertEquals(1, discardedTerm.closeCount);
                }
            }
        });
    }

    @Test
    public void testCloseFailureDoesNotDoubleCloseDetachedOrRetainedArguments() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final FunctionFactoryDescriptor descriptor = new FunctionFactoryDescriptor(new ReplaceVarcharFunctionFactory());
            for (boolean isReturnArgument : new boolean[]{false, true}) {
                final CountingText value = new CountingText(true, !isReturnArgument);
                final CountingText other = new CountingText(true, isReturnArgument);
                final ObjList<Function> args = isReturnArgument
                        ? new ObjList<>(value.asFunction(), VarcharConstant.EMPTY, other.asFunction())
                        : new ObjList<>(value.asFunction(), other.asFunction(), VarcharConstant.NULL);
                try (Function ignored = parser.createFunction(descriptor, 0, "replace", args, new IntList(), sqlExecutionContext)) {
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "text argument close");
                }
                Assert.assertEquals(1, value.closeCount);
                Assert.assertEquals(1, other.closeCount);
                Assert.assertEquals(0, value.readCount);
                Assert.assertEquals(0, other.readCount);
            }
        });
    }

    private static class CountingText extends StrFunction {
        private final Utf8String bytes = new Utf8String("abc");
        private final boolean isCloseFailure;
        private final boolean isVarchar;
        private int closeCount;
        private int readCount;

        private CountingText(boolean isVarchar, boolean isCloseFailure) {
            this.isVarchar = isVarchar;
            this.isCloseFailure = isCloseFailure;
        }

        @Override
        public void close() {
            closeCount++;
            if (isCloseFailure) {
                throw new IllegalStateException("text argument close");
            }
        }

        @Override
        public CharSequence getStrA(Record record) {
            readCount++;
            return "abc";
        }

        @Override
        public CharSequence getStrB(Record record) {
            return getStrA(record);
        }

        @Override
        public int getStrLen(Record record) {
            readCount++;
            return 3;
        }

        private Function asFunction() {
            return isVarchar ? new CastStrToVarcharFunctionFactory.Func(this) : this;
        }

        @Override
        public Utf8Sequence getVarcharA(Record record) {
            readCount++;
            return bytes;
        }

        @Override
        public Utf8Sequence getVarcharB(Record record) {
            return getVarcharA(record);
        }

        @Override
        public int getVarcharSize(Record record) {
            readCount++;
            return bytes.size();
        }
    }
}
