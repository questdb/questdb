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

package io.questdb.test.griffin.engine.functions.conditional;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.conditional.SwitchFunctionFactory;
import io.questdb.griffin.engine.functions.columns.SymbolColumn;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.constants.StrConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SwitchFunctionOwnershipTest extends AbstractCairoTest {
    @Test
    public void testUnusedBooleanElseAndOverwrittenNullBranchesCloseExactlyOnce() throws Exception {
        assertMemoryLeak(() -> {
            final SwitchFunctionFactory factory = new SwitchFunctionFactory();
            for (int branch = 0; branch < 3; branch++) {
                final CountingBoolean discarded = new CountingBoolean(false);
                final ObjList<Function> args = args(branch, discarded);
                final Function key = args.getQuick(0);
                try (Function result = factory.newInstance(0, args, positions(args.size()), configuration, sqlExecutionContext)) {
                    Assert.assertEquals(1, discarded.closeCount);
                    if (branch < 2) {
                        Assert.assertTrue(result.getBool(null));
                    }
                }
                Assert.assertEquals(1, discarded.closeCount);
                if (key instanceof CountingSymbol symbol) {
                    Assert.assertEquals(1, symbol.closeCount);
                }
            }
        });
    }

    @Test
    public void testDiscardCloseFailureDoesNotDoubleCloseThroughParserCleanup() throws Exception {
        assertMemoryLeak(() -> {
            final FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            final FunctionFactoryDescriptor descriptor = new FunctionFactoryDescriptor(new SwitchFunctionFactory());
            for (int branch = 0; branch < 3; branch++) {
                final CountingBoolean discarded = new CountingBoolean(true);
                final ObjList<Function> args = args(branch, discarded);
                final Function key = args.getQuick(0);
                try (Function ignored = parser.createFunction(descriptor, 0, "switch", args, positions(args.size()), sqlExecutionContext)) {
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "discarded CASE branch");
                }
                Assert.assertEquals(1, discarded.closeCount);
                if (key instanceof CountingSymbol symbol) {
                    Assert.assertEquals(1, symbol.closeCount);
                }
            }
        });
    }

    @Test
    public void testSingleBranchSymbolSpecializationOwnsItsKey() throws Exception {
        assertMemoryLeak(() -> {
            final CountingSymbol key = new CountingSymbol();
            final ObjList<Function> args = new ObjList<>(key, StrConstant.newInstance("'a'"), new IntConstant(1), new IntConstant(0));
            try (Function result = new SwitchFunctionFactory().newInstance(0, args, positions(args.size()), configuration, sqlExecutionContext)) {
                Assert.assertFalse(result.isThreadSafe());
                Assert.assertEquals(0, key.closeCount);
            }
            Assert.assertEquals(1, key.closeCount);
        });
    }

    @Test
    public void testSymbolSpecializationsWithDifferentBranchesAreNotEquivalent() throws Exception {
        assertMemoryLeak(() -> {
            final SymbolColumn key = new SymbolColumn(0, true);
            final SwitchFunctionFactory factory = new SwitchFunctionFactory();
            final ObjList<Function> first = new ObjList<>(key, StrConstant.newInstance("'a'"), new IntConstant(1), new IntConstant(0));
            final ObjList<Function> second = new ObjList<>(key, StrConstant.newInstance("'b'"), new IntConstant(2), new IntConstant(0));
            try (Function left = factory.newInstance(0, first, positions(first.size()), configuration, sqlExecutionContext);
                 Function right = factory.newInstance(0, second, positions(second.size()), configuration, sqlExecutionContext)) {
                Assert.assertTrue(left.isEquivalentTo(left));
                Assert.assertFalse(left.isEquivalentTo(right));
                Assert.assertFalse(right.isEquivalentTo(left));
            }
        });
    }

    private static ObjList<Function> args(int branch, CountingBoolean discarded) {
        if (branch == 0) {
            return new ObjList<>(BooleanConstant.TRUE, BooleanConstant.TRUE, BooleanConstant.TRUE,
                    BooleanConstant.FALSE, BooleanConstant.FALSE, discarded);
        }
        return new ObjList<>(branch == 1 ? StrConstant.NULL : new CountingSymbol(), StrConstant.NULL, discarded,
                StrConstant.NULL, BooleanConstant.TRUE, BooleanConstant.FALSE);
    }

    private static IntList positions(int count) {
        final IntList result = new IntList();
        for (int i = 0; i < count; i++) {
            result.add(i);
        }
        return result;
    }

    private static class CountingBoolean extends BooleanFunction {
        private final boolean isCloseFailure;
        private final DirectUtf8Sink sink = new DirectUtf8Sink(16);
        private int closeCount;

        private CountingBoolean(boolean isCloseFailure) {
            this.isCloseFailure = isCloseFailure;
        }

        @Override
        public void close() {
            closeCount++;
            sink.close();
            if (isCloseFailure) {
                throw new IllegalStateException("discarded CASE branch");
            }
        }

        @Override
        public boolean getBool(Record record) {
            throw new AssertionError("discarded CASE branch must not be evaluated");
        }
    }

    private static class CountingSymbol extends SymbolFunction {
        private final DirectUtf8Sink sink = new DirectUtf8Sink(16);
        private int closeCount;

        @Override
        public void close() {
            closeCount++;
            sink.close();
        }

        @Override
        public int getInt(Record record) {
            throw new AssertionError("uninitialized key must not be evaluated");
        }

        @Override
        public boolean isSymbolTableStatic() {
            return true;
        }

        @Override
        public CharSequence getSymbol(Record record) {
            throw new AssertionError("uninitialized key must not be evaluated");
        }

        @Override
        public CharSequence getSymbolB(Record record) {
            throw new AssertionError("uninitialized key must not be evaluated");
        }

        @Override
        public CharSequence valueBOf(int key) {
            throw new AssertionError("uninitialized key must not be evaluated");
        }

        @Override
        public CharSequence valueOf(int key) {
            throw new AssertionError("uninitialized key must not be evaluated");
        }
    }
}
