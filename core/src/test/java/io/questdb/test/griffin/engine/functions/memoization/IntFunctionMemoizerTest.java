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

package io.questdb.test.griffin.engine.functions.memoization;

import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.memoization.IntFunctionMemoizer;
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

/**
 * The memoizer exists so a projection referenced more than once evaluates its delegate once per
 * row. An INT expression carries one value, so a single memo serves every width: the memoizer
 * caches {@code getInt()} and inherits {@code IntFunction.getLong()}, which is
 * {@code Numbers.intToLong(getInt())}.
 */
public class IntFunctionMemoizerTest {

    @Test
    public void testNullSurvivesEveryWidth() {
        // INT_NULL must not sign-extend into a real -2147483648 at 64 bits.
        NullIntFunction fn = new NullIntFunction();
        IntFunctionMemoizer memoizer = new IntFunctionMemoizer(fn);

        Assert.assertEquals(Numbers.INT_NULL, memoizer.getInt(null));
        Assert.assertEquals(Numbers.LONG_NULL, memoizer.getLong(null));
        Assert.assertEquals(Numbers.LONG_NULL, memoizer.getTimestamp(null));
        Assert.assertEquals(Numbers.LONG_NULL, memoizer.getDate(null));
    }

    @Test
    public void testRedrawsAfterMemoize() {
        CountingIntFunction fn = new CountingIntFunction();
        IntFunctionMemoizer memoizer = new IntFunctionMemoizer(fn);

        Assert.assertEquals(1, memoizer.getInt(null));
        memoizer.memoize(null);
        Assert.assertEquals(2, memoizer.getInt(null));
        Assert.assertEquals(2L, memoizer.getLong(null));
        Assert.assertEquals(2, fn.evaluations);
    }

    @Test
    public void testWidthsComeFromOneEvaluation() {
        // A non-deterministic delegate redraws per evaluation, so the memo is what keeps the INT
        // read and the 64-bit read on the same draw.
        CountingIntFunction fn = new CountingIntFunction();
        IntFunctionMemoizer memoizer = new IntFunctionMemoizer(fn);

        Assert.assertEquals(1, memoizer.getInt(null));
        Assert.assertEquals(1L, memoizer.getLong(null));
        Assert.assertEquals(1L, memoizer.getTimestamp(null));
        Assert.assertEquals(1, fn.evaluations);
    }

    @Test
    public void testWidthsComeFromOneEvaluationLongFirst() {
        CountingIntFunction fn = new CountingIntFunction();
        IntFunctionMemoizer memoizer = new IntFunctionMemoizer(fn);

        Assert.assertEquals(1L, memoizer.getLong(null));
        Assert.assertEquals(1, memoizer.getInt(null));
        Assert.assertEquals(1, fn.evaluations);
    }

    private static class CountingIntFunction extends IntFunction {
        int evaluations;

        @Override
        public int getInt(Record rec) {
            return ++evaluations;
        }

        @Override
        public boolean isNonDeterministic() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("counting()");
        }
    }

    private static class NullIntFunction extends IntFunction {
        @Override
        public int getInt(Record rec) {
            return Numbers.INT_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("null()");
        }
    }
}
