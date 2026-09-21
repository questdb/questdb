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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.PerWorkerFunctionList;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.std.MemoryTracker;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.BitSet;

/**
 * Exact-close-count tests for {@link GroupByUtils#freeAssembledProjectionFunctions}, which walks
 * the positional correspondence produced by assembleGroupByFunctions: paired slots alias the same
 * Function, a null outer slot (timestamp placeholder) consumes no inner slot, a key rewrite
 * replaces the outer entry while the parsed original stays in the paired inner slot, and a
 * mid-assembly failure can leave the last non-null outer entry without an inner counterpart.
 * Every function must be closed exactly once in every configuration.
 * <p>
 * Also covers {@link GroupByUtils#setAllocator}: for an ownership-aware
 * {@link PerWorkerFunctionList} the traversal must probe the owned bits O(owned) times per call
 * instead of scanning all retained functions, while a plain list keeps receiving the allocator
 * on every function. Beyond call counts, the mixed borrowed/owned test asserts allocator
 * identity: a shared owner function borrowed into several worker slots must never receive any
 * worker allocator, while each owned clone must receive exactly its own slot's allocator
 * instance.
 */
public class GroupByUtilsTest {

    @Test
    public void testExtractNonGroupByFunctionsAllGroupBy() {
        final GroupByFunction groupBy0 = new AllocatorCountingGroupByFunction();
        final GroupByFunction groupBy1 = new AllocatorCountingGroupByFunction();
        final ObjList<Function> source = list(groupBy0, groupBy1);

        final ObjList<Function> result = GroupByUtils.extractNonGroupByFunctions(source);

        Assert.assertNotSame(source, result);
        Assert.assertEquals(0, result.size());
        Assert.assertEquals(2, source.size());
        Assert.assertSame(groupBy0, source.getQuick(0));
        Assert.assertSame(groupBy1, source.getQuick(1));
    }

    @Test
    public void testExtractNonGroupByFunctionsAllNonGroupBy() {
        final Function function0 = new CloseCountingFunction();
        final Function function1 = new CloseCountingFunction();
        final ObjList<Function> source = list(function0, function1);

        final ObjList<Function> result = GroupByUtils.extractNonGroupByFunctions(source);

        Assert.assertNotSame(source, result);
        Assert.assertEquals(2, result.size());
        Assert.assertSame(function0, result.getQuick(0));
        Assert.assertSame(function1, result.getQuick(1));
        Assert.assertEquals(2, source.size());
        Assert.assertSame(function0, source.getQuick(0));
        Assert.assertSame(function1, source.getQuick(1));
    }

    @Test
    public void testExtractNonGroupByFunctionsEmpty() {
        final ObjList<Function> source = new ObjList<>();

        final ObjList<Function> result = GroupByUtils.extractNonGroupByFunctions(source);

        Assert.assertNotSame(source, result);
        Assert.assertEquals(0, result.size());
        Assert.assertEquals(0, source.size());
    }

    @Test
    public void testExtractNonGroupByFunctionsMixed() {
        final GroupByFunction groupBy0 = new AllocatorCountingGroupByFunction();
        final Function function0 = new CloseCountingFunction();
        final GroupByFunction groupBy1 = new AllocatorCountingGroupByFunction();
        final Function function1 = new CloseCountingFunction();
        final ObjList<Function> source = list(groupBy0, function0, groupBy1, function1);

        final ObjList<Function> result = GroupByUtils.extractNonGroupByFunctions(source);

        Assert.assertNotSame(source, result);
        Assert.assertEquals(2, result.size());
        Assert.assertSame(function0, result.getQuick(0));
        Assert.assertSame(function1, result.getQuick(1));
        Assert.assertEquals(4, source.size());
        Assert.assertSame(groupBy0, source.getQuick(0));
        Assert.assertSame(function0, source.getQuick(1));
        Assert.assertSame(groupBy1, source.getQuick(2));
        Assert.assertSame(function1, source.getQuick(3));
    }

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
    public void testFreeSurvivesThrowingClose() {
        // a throwing close() must not stop the walk: every other function still closes exactly
        // once, both lists end up cleared, and the first failure propagates with later
        // failures attached as suppressed
        ThrowingCloseFunction aliasedThrower = new ThrowingCloseFunction("close failure 0");
        ThrowingCloseFunction replacementThrower = new ThrowingCloseFunction("close failure 1");
        CloseCountingFunction original = new CloseCountingFunction();
        ObjList<Function> outer = list(aliasedThrower, replacementThrower);
        ObjList<Function> inner = list(aliasedThrower, original);

        RuntimeException e = Assert.assertThrows(
                RuntimeException.class,
                () -> GroupByUtils.freeAssembledProjectionFunctions(outer, inner)
        );
        Assert.assertEquals("close failure 0", e.getMessage());
        Assert.assertEquals(1, e.getSuppressed().length);
        Assert.assertEquals("close failure 1", e.getSuppressed()[0].getMessage());
        Assert.assertEquals("the replaced slot's original must close despite the earlier failures", 1, original.closeCount);
        Assert.assertEquals(1, aliasedThrower.closeAttempts);
        Assert.assertEquals(1, replacementThrower.closeAttempts);
        Assert.assertEquals(0, outer.size());
        Assert.assertEquals(0, inner.size());
    }

    @Test
    public void testFreeSuppressesFailuresOnPrimaryAndContinuesCleanup() {
        final ThrowingCloseFunction aliasedThrower = new ThrowingCloseFunction("close failure 0");
        final ThrowingCloseFunction replacementThrower = new ThrowingCloseFunction("close failure 1");
        final CloseCountingFunction original = new CloseCountingFunction();
        final ObjList<Function> outer = list(aliasedThrower, replacementThrower);
        final ObjList<Function> inner = list(aliasedThrower, original);
        final RuntimeException primary = new RuntimeException("primary");

        GroupByUtils.freeAssembledProjectionFunctions(outer, inner, primary);

        Assert.assertEquals(1, primary.getSuppressed().length);
        Assert.assertSame(aliasedThrower.failure, primary.getSuppressed()[0]);
        Assert.assertEquals(1, aliasedThrower.failure.getSuppressed().length);
        Assert.assertSame(replacementThrower.failure, aliasedThrower.failure.getSuppressed()[0]);
        Assert.assertEquals(1, original.closeCount);
        Assert.assertEquals(1, aliasedThrower.closeAttempts);
        Assert.assertEquals(1, replacementThrower.closeAttempts);
        Assert.assertEquals(0, outer.size());
        Assert.assertEquals(0, inner.size());

        GroupByUtils.freeAssembledProjectionFunctions(outer, inner, primary);
        Assert.assertEquals(1, primary.getSuppressed().length);
        Assert.assertEquals(1, original.closeCount);
        Assert.assertEquals(1, aliasedThrower.closeAttempts);
        Assert.assertEquals(1, replacementThrower.closeAttempts);
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

    @Test
    public void testSetAllocatorForwardsExactWorkerAllocatorToOwnedClonesOnly() {
        // mixed borrowed/owned per-worker lists: the shared owner functions are borrowed into
        // both worker slots and must never receive any worker allocator, while each owned
        // clone must receive exactly one call carrying its own slot's allocator instance
        final AllocatorCountingGroupByFunction sharedOwner0 = new AllocatorCountingGroupByFunction();
        final AllocatorCountingGroupByFunction sharedOwner1 = new AllocatorCountingGroupByFunction();
        final AllocatorCountingGroupByFunction ownedClone0 = new AllocatorCountingGroupByFunction();
        final AllocatorCountingGroupByFunction ownedClone1 = new AllocatorCountingGroupByFunction();
        final GroupByAllocator workerAllocator0 = new NoopGroupByAllocator();
        final GroupByAllocator workerAllocator1 = new NoopGroupByAllocator();

        final PerWorkerFunctionList<GroupByFunction> workerSlot0 = new PerWorkerFunctionList<>(3);
        workerSlot0.add(sharedOwner0, false);
        workerSlot0.add(ownedClone0, true);
        workerSlot0.add(sharedOwner1, false);

        final PerWorkerFunctionList<GroupByFunction> workerSlot1 = new PerWorkerFunctionList<>(3);
        workerSlot1.add(sharedOwner0, false);
        workerSlot1.add(ownedClone1, true);
        workerSlot1.add(sharedOwner1, false);

        GroupByUtils.setAllocator(workerSlot0, workerAllocator0);
        GroupByUtils.setAllocator(workerSlot1, workerAllocator1);

        Assert.assertEquals("borrowed shared owner function must not receive a worker allocator", 0, sharedOwner0.setAllocatorCount);
        Assert.assertEquals("borrowed shared owner function must not receive a worker allocator", 0, sharedOwner1.setAllocatorCount);
        Assert.assertNull(sharedOwner0.lastAllocator);
        Assert.assertNull(sharedOwner1.lastAllocator);
        Assert.assertEquals(1, ownedClone0.setAllocatorCount);
        Assert.assertSame("owned clone must receive its own worker slot's allocator instance", workerAllocator0, ownedClone0.lastAllocator);
        Assert.assertEquals(1, ownedClone1.setAllocatorCount);
        Assert.assertSame("owned clone must receive its own worker slot's allocator instance", workerAllocator1, ownedClone1.lastAllocator);
    }

    @Test
    public void testSetAllocatorProbesOwnershipBitsOncePerOwnedSlot() throws Exception {
        // sparse ownership: the traversal must probe the owned bits once per owned slot (plus
        // the terminating probe), not once per retained function, and only the owned worker
        // clones may receive the allocator
        final int functionCount = 100;
        final PerWorkerFunctionList<GroupByFunction> list = new PerWorkerFunctionList<>(functionCount);
        for (int i = 0; i < functionCount; i++) {
            list.add(new AllocatorCountingGroupByFunction(), i == 10 || i == 90);
        }
        final CountingBitSet ownedBits = swapOwnedBits(list);
        GroupByUtils.setAllocator(list, null);
        for (int i = 0; i < functionCount; i++) {
            final int expectedCalls = (i == 10 || i == 90) ? 1 : 0;
            Assert.assertEquals(
                    "function at index " + i,
                    expectedCalls,
                    ((AllocatorCountingGroupByFunction) list.getQuick(i)).setAllocatorCount
            );
        }
        Assert.assertTrue(
                "setAllocator must probe the ownership bits O(owned) times, not O(size), but probed "
                        + ownedBits.probeCount + " times",
                ownedBits.probeCount <= 3
        );
    }

    @Test
    public void testSetAllocatorSetsEveryFunctionOnPlainList() {
        // non-ownership-aware fallback: every function of a plain list receives the allocator
        final ObjList<GroupByFunction> functions = new ObjList<>();
        functions.add(new AllocatorCountingGroupByFunction());
        functions.add(new AllocatorCountingGroupByFunction());
        GroupByUtils.setAllocator(functions, null);
        for (int i = 0, n = functions.size(); i < n; i++) {
            Assert.assertEquals(1, ((AllocatorCountingGroupByFunction) functions.getQuick(i)).setAllocatorCount);
        }
    }

    @Test
    public void testSetAllocatorSkipsFullyBorrowedListWithoutScanningIt() throws Exception {
        // the common per-worker case: every function is thread-safe and borrowed from the
        // owner, so setAllocator must cost a single ownership probe per worker instead of one
        // probe per retained function, and no function may receive the worker allocator
        final int functionCount = 100;
        final PerWorkerFunctionList<GroupByFunction> list = new PerWorkerFunctionList<>(functionCount);
        for (int i = 0; i < functionCount; i++) {
            list.add(new AllocatorCountingGroupByFunction(), false);
        }
        final CountingBitSet ownedBits = swapOwnedBits(list);
        GroupByUtils.setAllocator(list, null);
        for (int i = 0; i < functionCount; i++) {
            Assert.assertEquals(0, ((AllocatorCountingGroupByFunction) list.getQuick(i)).setAllocatorCount);
        }
        Assert.assertTrue(
                "setAllocator on a fully borrowed list must be O(1), but probed the ownership bits "
                        + ownedBits.probeCount + " times",
                ownedBits.probeCount <= 1
        );
    }

    private static ObjList<Function> list(Function... functions) {
        ObjList<Function> result = new ObjList<>();
        for (Function f : functions) {
            result.add(f);
        }
        return result;
    }

    private static CountingBitSet swapOwnedBits(PerWorkerFunctionList<?> list) throws Exception {
        final Field field = PerWorkerFunctionList.class.getDeclaredField("ownedFunctions");
        field.setAccessible(true);
        final CountingBitSet countingBits = new CountingBitSet();
        countingBits.or((BitSet) field.get(list));
        field.set(list, countingBits);
        return countingBits;
    }

    private static class AllocatorCountingGroupByFunction extends LongFunction implements GroupByFunction {
        private GroupByAllocator lastAllocator;
        private int setAllocatorCount;

        @Override
        public void computeFirst(MapValue mapValue, Record record, long rowId) {
        }

        @Override
        public void computeNext(MapValue mapValue, Record record, long rowId) {
        }

        @Override
        public long getLong(Record rec) {
            return 0;
        }

        @Override
        public int getValueIndex() {
            return 0;
        }

        @Override
        public void initValueIndex(int valueIndex) {
        }

        @Override
        public void initValueTypes(ArrayColumnTypes columnTypes) {
        }

        @Override
        public void setAllocator(GroupByAllocator allocator) {
            lastAllocator = allocator;
            setAllocatorCount++;
        }

        @Override
        public void setNull(MapValue mapValue) {
        }
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

    private static class CountingBitSet extends BitSet {
        private int probeCount;

        @Override
        public boolean get(int bitIndex) {
            probeCount++;
            return super.get(bitIndex);
        }

        @Override
        public int nextSetBit(int fromIndex) {
            probeCount++;
            return super.nextSetBit(fromIndex);
        }
    }

    private static class NoopGroupByAllocator implements GroupByAllocator {

        @Override
        public long allocated() {
            return 0;
        }

        @Override
        public void clear() {
        }

        @Override
        public void close() {
        }

        @Override
        public void free(long ptr, long size) {
        }

        @Override
        public long malloc(long size) {
            return 0;
        }

        @Override
        public long realloc(long ptr, long oldSize, long newSize) {
            return 0;
        }

        @Override
        public void reopen() {
        }

        @Override
        public void setMemoryTracker(MemoryTracker tracker) {
        }
    }

    private static class ThrowingCloseFunction extends LongFunction {
        private final RuntimeException failure;
        private int closeAttempts;

        private ThrowingCloseFunction(String message) {
            this.failure = new RuntimeException(message);
        }

        @Override
        public void close() {
            closeAttempts++;
            throw failure;
        }

        @Override
        public long getLong(Record rec) {
            return 0;
        }
    }
}
