/*******************************************************************************
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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.groupby.GroupingFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

/**
 * GROUPING() and GROUPING_ID() take key column references, which carry none of the flags the
 * planner asks of a function, so SQL never reaches the flags of their arguments. The function
 * still carries them up from its arguments, as every function holding functions must (see
 * FunctionFlagsForwardingTest): these arguments stand in for ones that have them.
 */
public class GroupingFunctionTest {

    @Test
    public void testFlagsCarryUpFromArguments() {
        assertFlags(false, false, false);
        assertFlags(true, false, false);
        assertFlags(false, true, false);
        assertFlags(false, false, true);
    }

    private static Function arg(boolean hasPerCallState, boolean isNonDeterministic, boolean isRandom) {
        return new IntFunction() {
            @Override
            public int getInt(Record rec) {
                return 0;
            }

            @Override
            public boolean hasPerCallState() {
                return hasPerCallState;
            }

            @Override
            public boolean isNonDeterministic() {
                return isNonDeterministic;
            }

            @Override
            public boolean isRandom() {
                return isRandom;
            }
        };
    }

    // the second of two arguments has the flags, so the function must read past the first
    private static void assertFlags(boolean hasPerCallState, boolean isNonDeterministic, boolean isRandom) {
        final ObjList<Function> args = new ObjList<>();
        args.add(arg(false, false, false));
        args.add(arg(hasPerCallState, isNonDeterministic, isRandom));
        final IntList columns = new IntList();
        columns.add(0);
        columns.add(1);
        final GroupingFunction function = new GroupingFunction(columns, args, columns, 0);
        try {
            Assert.assertEquals(hasPerCallState, function.hasPerCallState());
            Assert.assertEquals(isNonDeterministic, function.isNonDeterministic());
            Assert.assertEquals(isRandom, function.isRandom());
            // Function derives stability from determinism
            Assert.assertEquals(!isNonDeterministic, function.isStableWithinExecution());
            // the bitmask is the grouping set's, whatever the arguments are
            Assert.assertFalse(function.isOrderSensitive());
        } finally {
            function.close();
        }
    }
}
