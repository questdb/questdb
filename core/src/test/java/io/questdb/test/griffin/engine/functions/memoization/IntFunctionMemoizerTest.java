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
import io.questdb.griffin.engine.functions.LongWidthIntFunction;
import io.questdb.griffin.engine.functions.memoization.IntFunctionMemoizer;
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

/**
 * The memoizer exists so a projection referenced more than once evaluates its delegate once
 * per row. A width-unstable delegate is read at both widths by that projection - the INT
 * column itself and an {@code alias::LONG} sibling reaching it through
 * {@link io.questdb.griffin.engine.functions.columns.IntWideColumn} - so the two widths must
 * come from the SAME evaluation.
 */
public class IntFunctionMemoizerTest {

    @Test
    public void testRowStableDelegateKeepsBothWidths() {
        // A deterministic delegate deriving its two widths independently (json_extract does this)
        // genuinely has two different values per row, and both must survive. Reading it twice is
        // safe precisely because it is row stable.
        IndependentWidthFunction fn = new IndependentWidthFunction();
        IntFunctionMemoizer memoizer = new IntFunctionMemoizer(fn);

        Assert.assertEquals(7, memoizer.getInt(null));
        Assert.assertEquals(4_000_000_000L, memoizer.getLong(null));
    }

    @Test
    public void testRowUnstableDelegateAtLossyOperatorNarrowsTheWideValue() {
        // Division is the operator whose two widths do NOT agree on their low 32 bits, so the
        // memoizer cannot serve both from one draw. It resolves that the way InLongFunctionFactory
        // and COALESCE do - the long width is authoritative and the INT read is its narrowing -
        // because a row-unstable INT expression has no single stable value to wrap anyway. Pinned
        // here so the choice cannot be flipped silently.
        DivergentWidthFunction fn = new DivergentWidthFunction();
        IntFunctionMemoizer memoizer = new IntFunctionMemoizer(fn);

        Assert.assertEquals((int) DivergentWidthFunction.WIDE, memoizer.getInt(null));
        Assert.assertEquals(DivergentWidthFunction.WIDE, memoizer.getLong(null));
        Assert.assertEquals(1, fn.evaluations);
        // The int-width formula would have produced this instead, and it is NOT the narrowing.
        Assert.assertNotEquals(DivergentWidthFunction.NARROW, (int) DivergentWidthFunction.WIDE);
    }

    @Test
    public void testRowUnstableDelegateEvaluatedOncePerRow() {
        CountingIntFunction fn = new CountingIntFunction();
        IntFunctionMemoizer memoizer = new IntFunctionMemoizer(fn);

        final int i = memoizer.getInt(null);
        final long l = memoizer.getLong(null);

        Assert.assertEquals(1, fn.evaluations);
        // One draw, read at two widths: the INT read is the wrap of the wide value.
        Assert.assertEquals(CountingIntFunction.BASE + 1, l);
        Assert.assertEquals((int) (CountingIntFunction.BASE + 1), i);
    }

    @Test
    public void testRowUnstableDelegateEvaluatedOncePerRowLongFirst() {
        CountingIntFunction fn = new CountingIntFunction();
        IntFunctionMemoizer memoizer = new IntFunctionMemoizer(fn);

        final long l = memoizer.getLong(null);
        final int i = memoizer.getInt(null);

        Assert.assertEquals(1, fn.evaluations);
        Assert.assertEquals(CountingIntFunction.BASE + 1, l);
        Assert.assertEquals((int) (CountingIntFunction.BASE + 1), i);
    }

    @Test
    public void testRowUnstableDelegateNullSurvivesNarrowing() {
        // A plain (int) narrowing of LONG_NULL is 0, which is a perfectly ordinary INT value.
        NullIntFunction fn = new NullIntFunction();
        IntFunctionMemoizer memoizer = new IntFunctionMemoizer(fn);

        Assert.assertEquals(Numbers.INT_NULL, memoizer.getInt(null));
        Assert.assertEquals(Numbers.LONG_NULL, memoizer.getLong(null));
    }

    @Test
    public void testRowUnstableDelegateRedrawsAfterMemoize() {
        CountingIntFunction fn = new CountingIntFunction();
        IntFunctionMemoizer memoizer = new IntFunctionMemoizer(fn);

        Assert.assertEquals(CountingIntFunction.BASE + 1, memoizer.getLong(null));
        memoizer.memoize(null);
        Assert.assertEquals(CountingIntFunction.BASE + 2, memoizer.getLong(null));
        Assert.assertEquals((int) (CountingIntFunction.BASE + 2), memoizer.getInt(null));
        Assert.assertEquals(2, fn.evaluations);
    }

    @Test
    public void testWidthStableDelegateReadOnce() {
        WidthStableFunction fn = new WidthStableFunction();
        IntFunctionMemoizer memoizer = new IntFunctionMemoizer(fn);

        Assert.assertEquals(7, memoizer.getInt(null));
        Assert.assertEquals(7L, memoizer.getLong(null));
        Assert.assertEquals(1, fn.evaluations);
    }

    /**
     * Width unstable and row unstable: every evaluation is a fresh draw, and the wide half does
     * not fit in 32 bits, so a second draw is visible at both widths.
     */
    private static class CountingIntFunction extends LongWidthIntFunction {
        static final long BASE = 4_000_000_000L;
        int evaluations;

        @Override
        public int getInt(Record rec) {
            return (int) draw();
        }

        @Override
        public long getLong(Record rec) {
            return draw();
        }

        @Override
        public boolean isNonDeterministic() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("counting()");
        }

        private long draw() {
            return BASE + ++evaluations;
        }
    }

    /**
     * Models {@code (1000000 * 1000000) / 7}, the counterexample recorded in
     * {@code RuntimeConstFunction}: the int-width quotient is not the narrowing of the wide one,
     * so the two widths are genuinely different numbers rather than a wrap of one another.
     */
    private static class DivergentWidthFunction extends LongWidthIntFunction {
        static final int NARROW = -103911424;
        static final long WIDE = 142857142857L;
        int evaluations;

        @Override
        public int getInt(Record rec) {
            evaluations++;
            return NARROW;
        }

        @Override
        public long getLong(Record rec) {
            evaluations++;
            return WIDE;
        }

        @Override
        public boolean isNonDeterministic() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("divergent()");
        }
    }

    /**
     * Width unstable but row stable, with two widths that are not derivable from one another.
     */
    private static class IndependentWidthFunction extends LongWidthIntFunction {
        @Override
        public int getInt(Record rec) {
            return 7;
        }

        @Override
        public long getLong(Record rec) {
            return 4_000_000_000L;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("independent()");
        }
    }

    private static class NullIntFunction extends LongWidthIntFunction {
        @Override
        public int getInt(Record rec) {
            return Numbers.INT_NULL;
        }

        @Override
        public long getLong(Record rec) {
            return Numbers.LONG_NULL;
        }

        @Override
        public boolean isNonDeterministic() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("null()");
        }
    }

    private static class WidthStableFunction extends IntFunction {
        int evaluations;

        @Override
        public int getInt(Record rec) {
            evaluations++;
            return 7;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("stable()");
        }
    }
}
