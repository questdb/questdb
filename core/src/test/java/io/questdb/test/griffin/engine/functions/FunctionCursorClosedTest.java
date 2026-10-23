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

package io.questdb.test.griffin.engine.functions;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.QuaternaryFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

public class FunctionCursorClosedTest {

    @Test
    public void testBinaryNotifiesEveryArgument() {
        final CursorClosedFunction left = new CursorClosedFunction("left failure");
        final CursorClosedFunction right = new CursorClosedFunction("right failure");
        assertFailures(new Binary(left, right), left, right);
    }

    @Test
    public void testListNotifiesEveryFunction() {
        final CursorClosedFunction first = new CursorClosedFunction("first failure");
        final CursorClosedFunction second = new CursorClosedFunction(null);
        final CursorClosedFunction third = new CursorClosedFunction("third failure");
        final ObjList<Function> functions = new ObjList<>();
        functions.add(first);
        functions.add(null);
        functions.add(second);
        functions.add(third);

        final RuntimeException primary = new RuntimeException("primary");
        Assert.assertSame(primary, Function.cursorClosed(primary, functions));
        Assert.assertEquals(2, primary.getSuppressed().length);
        Assert.assertSame(first.failure, primary.getSuppressed()[0]);
        Assert.assertSame(third.failure, primary.getSuppressed()[1]);

        final RuntimeException e = Assert.assertThrows(RuntimeException.class, () -> Function.cursorClosed(functions));
        Assert.assertSame(first.failure, e);
        Assert.assertEquals(2, first.cursorClosedCount);
        Assert.assertEquals(2, second.cursorClosedCount);
        Assert.assertEquals(2, third.cursorClosedCount);

        Assert.assertNull(Function.cursorClosed(null, (ObjList<Function>) null));
        Function.cursorClosed((ObjList<Function>) null);
    }

    @Test
    public void testMultiArgNotifiesEveryArgument() {
        final CursorClosedFunction first = new CursorClosedFunction("first failure");
        final CursorClosedFunction second = new CursorClosedFunction(null);
        final CursorClosedFunction third = new CursorClosedFunction("third failure");
        assertFailures(new MultiArg(first, second, third), first, second, third);
    }

    @Test
    public void testMultiArgPropagatesCursorClosed() {
        final CursorClosedFunction first = new CursorClosedFunction(null);
        final CursorClosedFunction second = new CursorClosedFunction(null);
        new MultiArg(first, second).cursorClosed();
        Assert.assertEquals(1, first.cursorClosedCount);
        Assert.assertEquals(1, second.cursorClosedCount);
    }

    @Test
    public void testQuaternaryNotifiesEveryArgument() {
        final CursorClosedFunction func0 = new CursorClosedFunction("func0 failure");
        final CursorClosedFunction func1 = new CursorClosedFunction(null);
        final CursorClosedFunction func2 = new CursorClosedFunction(null);
        final CursorClosedFunction func3 = new CursorClosedFunction("func3 failure");
        assertFailures(new Quaternary(func0, func1, func2, func3), func0, func1, func2, func3);
    }

    @Test
    public void testTernaryNotifiesEveryArgument() {
        final CursorClosedFunction left = new CursorClosedFunction("left failure");
        final CursorClosedFunction center = new CursorClosedFunction(null);
        final CursorClosedFunction right = new CursorClosedFunction("right failure");
        assertFailures(new Ternary(left, center, right), left, center, right);
    }

    // Asserts that cursorClosed() reached every argument once, rethrew the first failure and
    // attached the later ones to it as suppressed.
    private static void assertFailures(Function function, CursorClosedFunction... args) {
        final RuntimeException e = Assert.assertThrows(RuntimeException.class, function::cursorClosed);
        RuntimeException primary = null;
        int suppressedCount = 0;
        for (CursorClosedFunction arg : args) {
            Assert.assertEquals(1, arg.cursorClosedCount);
            if (arg.failure != null) {
                if (primary == null) {
                    primary = arg.failure;
                } else {
                    Assert.assertSame(arg.failure, e.getSuppressed()[suppressedCount++]);
                }
            }
        }
        Assert.assertSame(primary, e);
        Assert.assertEquals(suppressedCount, e.getSuppressed().length);
    }

    private static class Binary extends LongFunction implements BinaryFunction {
        private final Function left;
        private final Function right;

        private Binary(Function left, Function right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public long getLong(Record rec) {
            return 0;
        }

        @Override
        public Function getRight() {
            return right;
        }
    }

    private static class CursorClosedFunction extends LongFunction {
        private final RuntimeException failure;
        private int cursorClosedCount;

        private CursorClosedFunction(String failureMessage) {
            this.failure = failureMessage != null ? new RuntimeException(failureMessage) : null;
        }

        @Override
        public void cursorClosed() {
            cursorClosedCount++;
            if (failure != null) {
                throw failure;
            }
        }

        @Override
        public long getLong(Record rec) {
            return 0;
        }
    }

    private static class MultiArg extends LongFunction implements MultiArgFunction {
        private final ObjList<Function> args = new ObjList<>();

        private MultiArg(Function... args) {
            for (Function arg : args) {
                this.args.add(arg);
            }
        }

        @Override
        public ObjList<Function> args() {
            return args;
        }

        @Override
        public long getLong(Record rec) {
            return 0;
        }
    }

    private static class Quaternary extends LongFunction implements QuaternaryFunction {
        private final Function func0;
        private final Function func1;
        private final Function func2;
        private final Function func3;

        private Quaternary(Function func0, Function func1, Function func2, Function func3) {
            this.func0 = func0;
            this.func1 = func1;
            this.func2 = func2;
            this.func3 = func3;
        }

        @Override
        public Function getFunc0() {
            return func0;
        }

        @Override
        public Function getFunc1() {
            return func1;
        }

        @Override
        public Function getFunc2() {
            return func2;
        }

        @Override
        public Function getFunc3() {
            return func3;
        }

        @Override
        public long getLong(Record rec) {
            return 0;
        }
    }

    private static class Ternary extends LongFunction implements TernaryFunction {
        private final Function center;
        private final Function left;
        private final Function right;

        private Ternary(Function left, Function center, Function right) {
            this.left = left;
            this.center = center;
            this.right = right;
        }

        @Override
        public Function getCenter() {
            return center;
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public long getLong(Record rec) {
            return 0;
        }

        @Override
        public Function getRight() {
            return right;
        }
    }
}
