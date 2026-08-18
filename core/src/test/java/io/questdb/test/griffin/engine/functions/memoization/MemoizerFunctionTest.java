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
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.memoization.DoubleFunctionMemoizer;
import org.junit.Assert;
import org.junit.Test;

/**
 * The invalidation half of the memoizer contract: a memoizer caches a value, not a position, so
 * every event that moves the cursor without producing a row has to drop the cache. A cursor that
 * skips one serves the previous row's value with no fault to show for it - the shape of the live
 * view O3 replay bug that {@code ProjectingRecordCursor} shipped with.
 * <p>
 * DoubleFunctionMemoizer stands in for all eighteen: they differ only in the getter that fills the
 * cache, and every one of them clears through the same interface defaults.
 */
public class MemoizerFunctionTest {

    @Test
    public void testClearMemoRecomputesOnTheNextGetter() {
        final CountingDoubleFunction arg = new CountingDoubleFunction(1.5);
        final DoubleFunctionMemoizer memoizer = new DoubleFunctionMemoizer(arg);

        Assert.assertEquals(1.5, memoizer.getDouble(null), 0.0);
        Assert.assertEquals(1, arg.callCount);
        // Second read of the same row answers from the cache.
        Assert.assertEquals(1.5, memoizer.getDouble(null), 0.0);
        Assert.assertEquals(1, arg.callCount);

        arg.value = 2.5;
        memoizer.clearMemo();
        Assert.assertEquals(2.5, memoizer.getDouble(null), 0.0);
        Assert.assertEquals(2, arg.callCount);
    }

    @Test
    public void testInitClearsTheCachedValue() throws SqlException {
        final CountingDoubleFunction arg = new CountingDoubleFunction(1.5);
        final DoubleFunctionMemoizer memoizer = new DoubleFunctionMemoizer(arg);

        Assert.assertEquals(1.5, memoizer.getDouble(null), 0.0);

        // A rebind onto another cursor. Without the clear the first read on the new base
        // answers with the last row of the previous one.
        arg.value = 2.5;
        memoizer.init(null, null);
        Assert.assertEquals(2.5, memoizer.getDouble(null), 0.0);
        Assert.assertEquals(2, arg.callCount);
        Assert.assertEquals(1, arg.initCount);
    }

    @Test
    public void testToTopClearsTheCachedValue() {
        final CountingDoubleFunction arg = new CountingDoubleFunction(1.5);
        final DoubleFunctionMemoizer memoizer = new DoubleFunctionMemoizer(arg);

        Assert.assertEquals(1.5, memoizer.getDouble(null), 0.0);

        // A rewind. Without the clear the first row of the new traversal answers with the
        // last row of the previous one.
        arg.value = 2.5;
        memoizer.toTop();
        Assert.assertEquals(2.5, memoizer.getDouble(null), 0.0);
        Assert.assertEquals(2, arg.callCount);
        // The clear must not cost the delegation: the wrapped function rewinds too.
        Assert.assertEquals(1, arg.toTopCount);
    }

    private static class CountingDoubleFunction extends DoubleFunction {
        int callCount;
        int initCount;
        int toTopCount;
        double value;

        private CountingDoubleFunction(double value) {
            this.value = value;
        }

        @Override
        public double getDouble(Record rec) {
            callCount++;
            return value;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            initCount++;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toTop() {
            toTopCount++;
        }
    }
}
