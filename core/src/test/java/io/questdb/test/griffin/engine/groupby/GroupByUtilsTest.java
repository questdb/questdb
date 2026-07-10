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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

/**
 * Exact-close-count tests for {@link GroupByUtils#freeAssembledProjectionFunctions}, which walks
 * the positional correspondence produced by assembleGroupByFunctions: paired slots alias the same
 * Function, a null outer slot (timestamp placeholder) consumes no inner slot, a key rewrite
 * replaces the outer entry while the parsed original stays in the paired inner slot, and a
 * mid-assembly failure can leave the last non-null outer entry without an inner counterpart.
 * Every function must be closed exactly once in every configuration.
 */
public class GroupByUtilsTest {

    @Test
    public void testFreeAliasedSlots() {
        // fully aliased projection: both lists share every reference
        CloseCountingFunction f0 = new CloseCountingFunction();
        CloseCountingFunction f1 = new CloseCountingFunction();
        ObjList<Function> outer = list(f0, f1);
        ObjList<Function> inner = list(f0, f1);
        GroupByUtils.freeAssembledProjectionFunctions(outer, inner);
        Assert.assertEquals(1, f0.closeCount);
        Assert.assertEquals(1, f1.closeCount);
        Assert.assertEquals(0, outer.size());
        Assert.assertEquals(0, inner.size());
    }

    @Test
    public void testFreeInnerOnly() {
        // callers may pass a null outer list; every inner entry is freed once
        CloseCountingFunction f0 = new CloseCountingFunction();
        ObjList<Function> inner = list(f0);
        GroupByUtils.freeAssembledProjectionFunctions(null, inner);
        Assert.assertEquals(1, f0.closeCount);
        Assert.assertEquals(0, inner.size());
    }

    @Test
    public void testFreeNullLists() {
        GroupByUtils.freeAssembledProjectionFunctions(null, null);
    }

    @Test
    public void testFreeOuterOnly() {
        // callers may pass a null inner list; every outer entry is freed once
        CloseCountingFunction f0 = new CloseCountingFunction();
        ObjList<Function> outer = list(f0);
        GroupByUtils.freeAssembledProjectionFunctions(outer, null);
        Assert.assertEquals(1, f0.closeCount);
        Assert.assertEquals(0, outer.size());
    }

    @Test
    public void testFreePartiallyAssembledSlots() {
        // a failure between outer.add(func) and inner.add(func) leaves the last non-null outer
        // entry unpaired; it must still be freed exactly once and the pairs before it must not
        // shift
        CloseCountingFunction f0 = new CloseCountingFunction();
        CloseCountingFunction tail = new CloseCountingFunction();
        ObjList<Function> outer = list(f0, tail);
        ObjList<Function> inner = list(f0);
        GroupByUtils.freeAssembledProjectionFunctions(outer, inner);
        Assert.assertEquals(1, f0.closeCount);
        Assert.assertEquals(1, tail.closeCount);
    }

    @Test
    public void testFreeReplacedSlots() {
        // the key rewrite replaced both outer entries with column-ref functions; the parsed
        // originals are reachable only through the paired inner slots and every function must
        // close exactly once
        CloseCountingFunction original0 = new CloseCountingFunction();
        CloseCountingFunction original1 = new CloseCountingFunction();
        CloseCountingFunction replacement0 = new CloseCountingFunction();
        CloseCountingFunction replacement1 = new CloseCountingFunction();
        ObjList<Function> outer = list(replacement0, replacement1);
        ObjList<Function> inner = list(original0, original1);
        GroupByUtils.freeAssembledProjectionFunctions(outer, inner);
        Assert.assertEquals(1, original0.closeCount);
        Assert.assertEquals(1, original1.closeCount);
        Assert.assertEquals(1, replacement0.closeCount);
        Assert.assertEquals(1, replacement1.closeCount);
    }

    @Test
    public void testFreeTimestampNullSlotKeepsAlignment() {
        // the timestamp column appends null to outer and nothing to inner; the pairs after it
        // must stay aligned: aliased entries close once, a replaced entry's original closes once
        CloseCountingFunction aliased = new CloseCountingFunction();
        CloseCountingFunction original = new CloseCountingFunction();
        CloseCountingFunction replacement = new CloseCountingFunction();
        ObjList<Function> outer = new ObjList<>();
        outer.add(aliased);
        outer.add(null);
        outer.add(replacement);
        ObjList<Function> inner = list(aliased, original);
        GroupByUtils.freeAssembledProjectionFunctions(outer, inner);
        Assert.assertEquals(1, aliased.closeCount);
        Assert.assertEquals(1, original.closeCount);
        Assert.assertEquals(1, replacement.closeCount);
    }

    private static ObjList<Function> list(Function... functions) {
        ObjList<Function> result = new ObjList<>();
        for (Function f : functions) {
            result.add(f);
        }
        return result;
    }

    private static class CloseCountingFunction extends LongFunction {
        int closeCount;

        @Override
        public void close() {
            closeCount++;
        }

        @Override
        public long getLong(Record rec) {
            return 0;
        }
    }
}
