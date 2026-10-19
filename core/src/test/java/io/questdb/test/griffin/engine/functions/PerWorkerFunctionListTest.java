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
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.PerWorkerFunctionList;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

public class PerWorkerFunctionListTest {

    @Test
    public void testClearResetsOwnershipMetadata() {
        // The corruption sequence from the review finding: without a consistent
        // clear() the stale ownership bit from the first add() would make close()
        // free the borrowed function added after the clear.
        final PerWorkerFunctionList<Function> list = new PerWorkerFunctionList<>(4);
        final TrackingFunction owned = new TrackingFunction();
        final TrackingFunction borrowed = new TrackingFunction();
        list.add(owned, true);
        list.clear();
        Assert.assertEquals(0, list.size());
        list.add(borrowed, false);
        PerWorkerFunctionList.close(list);
        Assert.assertEquals("borrowed function belongs to the owner list and must survive close", 0, borrowed.closeCount);
        Assert.assertEquals("clear() drops elements without closing them, like ObjList.clear()", 0, owned.closeCount);
        Assert.assertSame(borrowed, list.getQuick(0));
    }

    @Test
    public void testCloseFreesOnlyOwnedFunctionsAndIsIdempotent() {
        final PerWorkerFunctionList<Function> list = new PerWorkerFunctionList<>(4);
        final TrackingFunction borrowed = new TrackingFunction();
        final TrackingFunction owned = new TrackingFunction();
        list.add(borrowed, false);
        list.add(owned, true);
        PerWorkerFunctionList.close(list);
        Assert.assertEquals(0, borrowed.closeCount);
        Assert.assertEquals(1, owned.closeCount);
        Assert.assertSame(borrowed, list.getQuick(0));
        Assert.assertNull(list.getQuick(1));
        // second close must not double-free
        PerWorkerFunctionList.close(list);
        Assert.assertEquals(0, borrowed.closeCount);
        Assert.assertEquals(1, owned.closeCount);
    }

    @Test
    public void testCloseSurvivesThrowingOwnedFunction() {
        // close() must attempt every owned slot even when an earlier owned function throws:
        // the first failure propagates as the primary, later failures attach as suppressed,
        // and every owned slot is nulled so a repeated close cannot double-free
        final PerWorkerFunctionList<Function> list = new PerWorkerFunctionList<>(4);
        final ThrowingCloseFunction throwingFirst = new ThrowingCloseFunction("close failure 0");
        final TrackingFunction borrowed = new TrackingFunction();
        final ThrowingCloseFunction throwingSecond = new ThrowingCloseFunction("close failure 1");
        final TrackingFunction owned = new TrackingFunction();
        list.add(throwingFirst, true);
        list.add(borrowed, false);
        list.add(throwingSecond, true);
        list.add(owned, true);

        final RuntimeException e = Assert.assertThrows(RuntimeException.class, () -> PerWorkerFunctionList.close(list));
        Assert.assertEquals("close failure 0", e.getMessage());
        Assert.assertEquals(1, e.getSuppressed().length);
        Assert.assertEquals("close failure 1", e.getSuppressed()[0].getMessage());
        Assert.assertEquals("later owned functions must close despite the earlier failure", 1, owned.closeCount);
        Assert.assertEquals(0, borrowed.closeCount);
        Assert.assertNull("the throwing slot must be nulled before the close attempt", list.getQuick(0));
        Assert.assertSame(borrowed, list.getQuick(1));
        Assert.assertNull(list.getQuick(2));
        Assert.assertNull(list.getQuick(3));

        // second close must not re-close or re-throw: every owned slot is already null
        PerWorkerFunctionList.close(list);
        Assert.assertEquals(1, owned.closeCount);
        Assert.assertEquals(1, throwingFirst.closeAttempts);
        Assert.assertEquals(1, throwingSecond.closeAttempts);
    }

    @Test
    public void testCloseWithPrimarySuppressesFailuresAndContinuesCleanup() {
        final PerWorkerFunctionList<Function> list = new PerWorkerFunctionList<>(4);
        final ThrowingCloseFunction throwingFirst = new ThrowingCloseFunction("close failure 0");
        final TrackingFunction borrowed = new TrackingFunction();
        final ThrowingCloseFunction throwingSecond = new ThrowingCloseFunction("close failure 1");
        final TrackingFunction owned = new TrackingFunction();
        list.add(throwingFirst, true);
        list.add(borrowed, false);
        list.add(throwingSecond, true);
        list.add(owned, true);
        final RuntimeException primary = new RuntimeException("primary");

        PerWorkerFunctionList.close(list, primary);

        Assert.assertEquals(1, primary.getSuppressed().length);
        Assert.assertSame(throwingFirst.failure, primary.getSuppressed()[0]);
        Assert.assertEquals(1, throwingFirst.failure.getSuppressed().length);
        Assert.assertSame(throwingSecond.failure, throwingFirst.failure.getSuppressed()[0]);
        Assert.assertEquals(1, owned.closeCount);
        Assert.assertEquals(0, borrowed.closeCount);
        Assert.assertNull(list.getQuick(0));
        Assert.assertSame(borrowed, list.getQuick(1));
        Assert.assertNull(list.getQuick(2));
        Assert.assertNull(list.getQuick(3));

        PerWorkerFunctionList.close(list, primary);
        Assert.assertEquals(1, primary.getSuppressed().length);
        Assert.assertEquals(1, throwingFirst.closeAttempts);
        Assert.assertEquals(1, throwingSecond.closeAttempts);
        Assert.assertEquals(1, owned.closeCount);
    }

    @Test
    public void testInheritedStructuralMutatorsAreRejected() {
        final PerWorkerFunctionList<Function> list = new PerWorkerFunctionList<>(4);
        final TrackingFunction function = new TrackingFunction();
        list.add(function, true);
        final ObjList<Function> other = new ObjList<>();
        other.add(new TrackingFunction());
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.add(function));
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.addAll(other));
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.addAll(other, 0, 1));
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.addReverseAll(other));
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.extendAndSet(2, function));
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.extendPos(4));
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.getAndSetQuick(0, function));
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.insert(0, 1, null));
        Assert.assertThrows(UnsupportedOperationException.class, list::popLast);
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.remove(0));
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.remove(0, 0));
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.remove(function));
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.set(0, function));
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.set(0, 1, function));
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.setAll(1, function));
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.setPos(0));
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.setQuick(0, function));
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.sort((_, _) -> 0));
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.sort(0, 1, (_, _) -> 0));
        // rejected mutators must leave the list and ownership metadata intact
        Assert.assertEquals(1, list.size());
        Assert.assertSame(function, list.getQuick(0));
        Assert.assertTrue(PerWorkerFunctionList.isOwned(list, 0));
        PerWorkerFunctionList.close(list);
        Assert.assertEquals(1, function.closeCount);
    }

    @Test
    public void testInitFailuresCanBeRetriedOnPerWorkerList() throws Exception {
        final PerWorkerFunctionList<Function> donationFailureList = new PerWorkerFunctionList<>(3);
        final TrackingFunction borrowed = new TrackingFunction();
        final TrackingFunction firstClone = new TrackingFunction();
        final TrackingFunction secondClone = new TrackingFunction();
        donationFailureList.add(borrowed, false);
        donationFailureList.add(firstClone, true);
        donationFailureList.add(secondClone, true);
        final ObjList<Function> donationOwners = new ObjList<>();
        final TrackingFunction borrowedOwner = new TrackingFunction();
        final TrackingFunction firstOwner = new ThrowOnceTrackingFunction(false, true);
        final TrackingFunction secondOwner = new TrackingFunction();
        donationOwners.add(borrowedOwner);
        donationOwners.add(firstOwner);
        donationOwners.add(secondOwner);

        Assert.assertThrows(RuntimeException.class, () -> PerWorkerFunctionList.init(donationFailureList, donationOwners, null, null));
        Assert.assertEquals(0, borrowedOwner.offerStateCount);
        Assert.assertEquals(1, firstOwner.offerStateCount);
        Assert.assertEquals(0, firstClone.initCount);
        Assert.assertEquals(0, secondOwner.offerStateCount);
        Assert.assertEquals(0, secondClone.initCount);
        PerWorkerFunctionList.init(donationFailureList, donationOwners, null, null);
        Assert.assertEquals(2, firstOwner.offerStateCount);
        Assert.assertEquals(1, firstClone.initCount);
        Assert.assertEquals(1, secondOwner.offerStateCount);
        Assert.assertEquals(1, secondClone.initCount);

        final PerWorkerFunctionList<Function> initFailureList = new PerWorkerFunctionList<>(2);
        final TrackingFunction failingClone = new ThrowOnceTrackingFunction(true, false);
        final TrackingFunction trailingClone = new TrackingFunction();
        initFailureList.add(failingClone, true);
        initFailureList.add(trailingClone, true);
        final ObjList<Function> initOwners = new ObjList<>();
        final TrackingFunction firstInitOwner = new TrackingFunction();
        final TrackingFunction secondInitOwner = new TrackingFunction();
        initOwners.add(firstInitOwner);
        initOwners.add(secondInitOwner);

        Assert.assertThrows(RuntimeException.class, () -> PerWorkerFunctionList.init(initFailureList, initOwners, null, null));
        Assert.assertEquals(1, firstInitOwner.offerStateCount);
        Assert.assertEquals(1, failingClone.initCount);
        Assert.assertEquals(0, secondInitOwner.offerStateCount);
        Assert.assertEquals(0, trailingClone.initCount);
        PerWorkerFunctionList.init(initFailureList, initOwners, null, null);
        Assert.assertEquals(2, firstInitOwner.offerStateCount);
        Assert.assertEquals(2, failingClone.initCount);
        Assert.assertEquals(1, secondInitOwner.offerStateCount);
        Assert.assertEquals(1, trailingClone.initCount);
    }

    @Test
    public void testInitFailuresCanBeRetriedOnPlainList() throws Exception {
        final ObjList<Function> donationFailureList = new ObjList<>();
        final TrackingFunction firstClone = new TrackingFunction();
        final TrackingFunction secondClone = new TrackingFunction();
        final TrackingFunction thirdClone = new TrackingFunction();
        donationFailureList.add(firstClone);
        donationFailureList.add(secondClone);
        donationFailureList.add(thirdClone);
        final ObjList<Function> donationOwners = new ObjList<>();
        final TrackingFunction firstOwner = new TrackingFunction();
        final TrackingFunction secondOwner = new ThrowOnceTrackingFunction(false, true);
        final TrackingFunction thirdOwner = new TrackingFunction();
        donationOwners.add(firstOwner);
        donationOwners.add(secondOwner);
        donationOwners.add(thirdOwner);

        Assert.assertThrows(RuntimeException.class, () -> PerWorkerFunctionList.init(donationFailureList, donationOwners, null, null));
        Assert.assertEquals(1, firstOwner.offerStateCount);
        Assert.assertEquals(1, secondOwner.offerStateCount);
        Assert.assertEquals(0, thirdOwner.offerStateCount);
        Assert.assertEquals(0, firstClone.initCount);
        Assert.assertEquals(0, secondClone.initCount);
        Assert.assertEquals(0, thirdClone.initCount);
        PerWorkerFunctionList.init(donationFailureList, donationOwners, null, null);
        Assert.assertEquals(2, firstOwner.offerStateCount);
        Assert.assertEquals(2, secondOwner.offerStateCount);
        Assert.assertEquals(1, thirdOwner.offerStateCount);
        Assert.assertEquals(1, firstClone.initCount);
        Assert.assertEquals(1, secondClone.initCount);
        Assert.assertEquals(1, thirdClone.initCount);

        final ObjList<Function> initFailureList = new ObjList<>();
        final TrackingFunction initializedClone = new TrackingFunction();
        final TrackingFunction failingClone = new ThrowOnceTrackingFunction(true, false);
        final TrackingFunction trailingClone = new TrackingFunction();
        initFailureList.add(initializedClone);
        initFailureList.add(failingClone);
        initFailureList.add(trailingClone);
        final ObjList<Function> initOwners = new ObjList<>();
        final TrackingFunction initializedOwner = new TrackingFunction();
        final TrackingFunction failingOwner = new TrackingFunction();
        final TrackingFunction trailingOwner = new TrackingFunction();
        initOwners.add(initializedOwner);
        initOwners.add(failingOwner);
        initOwners.add(trailingOwner);

        Assert.assertThrows(RuntimeException.class, () -> PerWorkerFunctionList.init(initFailureList, initOwners, null, null));
        Assert.assertEquals(1, initializedOwner.offerStateCount);
        Assert.assertEquals(1, failingOwner.offerStateCount);
        Assert.assertEquals(1, trailingOwner.offerStateCount);
        Assert.assertEquals(1, initializedClone.initCount);
        Assert.assertEquals(1, failingClone.initCount);
        Assert.assertEquals(0, trailingClone.initCount);
        PerWorkerFunctionList.init(initFailureList, initOwners, null, null);
        Assert.assertEquals(2, initializedOwner.offerStateCount);
        Assert.assertEquals(2, failingOwner.offerStateCount);
        Assert.assertEquals(2, trailingOwner.offerStateCount);
        Assert.assertEquals(2, initializedClone.initCount);
        Assert.assertEquals(2, failingClone.initCount);
        Assert.assertEquals(1, trailingClone.initCount);
    }

    @Test
    public void testLifecycleHelpersTouchOnlyOwnedFunctions() throws Exception {
        final PerWorkerFunctionList<Function> list = new PerWorkerFunctionList<>(4);
        final TrackingFunction borrowed = new TrackingFunction();
        final TrackingFunction owned = new TrackingFunction();
        list.add(borrowed, false);
        list.add(owned, true);
        Assert.assertFalse(PerWorkerFunctionList.isOwned(list, 0));
        Assert.assertTrue(PerWorkerFunctionList.isOwned(list, 1));

        final ObjList<Function> ownerFunctions = new ObjList<>();
        final TrackingFunction ownerOfOwned = new TrackingFunction();
        ownerFunctions.add(borrowed);
        ownerFunctions.add(ownerOfOwned);
        PerWorkerFunctionList.init(list, ownerFunctions, null, null);
        Assert.assertEquals(0, borrowed.initCount);
        Assert.assertEquals(1, owned.initCount);
        Assert.assertEquals("only the owner aligned with the owned slot donates state", 1, ownerOfOwned.offerStateCount);
        Assert.assertSame(owned, ownerOfOwned.lastOfferStateTarget);
        Assert.assertEquals("the owner aligned with the borrowed slot must not donate to itself", 0, borrowed.offerStateCount);

        PerWorkerFunctionList.clear(list);
        Assert.assertEquals(0, borrowed.clearCount);
        Assert.assertEquals(1, owned.clearCount);

        PerWorkerFunctionList.toTop(list);
        Assert.assertEquals(0, borrowed.toTopCount);
        Assert.assertEquals(1, owned.toTopCount);
    }

    @Test
    public void testNextOwnedIteratesOwnedSlotsOnly() {
        final PerWorkerFunctionList<Function> list = new PerWorkerFunctionList<>(4);
        Assert.assertEquals(-1, list.nextOwned(0));
        list.add(new TrackingFunction(), false);
        list.add(new TrackingFunction(), true);
        list.add(new TrackingFunction(), false);
        list.add(new TrackingFunction(), true);
        Assert.assertEquals(1, list.nextOwned(0));
        Assert.assertEquals(1, list.nextOwned(1));
        Assert.assertEquals(3, list.nextOwned(2));
        Assert.assertEquals(3, list.nextOwned(3));
        Assert.assertEquals(-1, list.nextOwned(4));
        list.clear();
        Assert.assertEquals(-1, list.nextOwned(0));
    }

    @Test
    public void testPlainListClearClearsEveryFunction() {
        // fallback contract: a plain ObjList owns all of its functions, so clear() must
        // reset every one of them, not just marked slots
        final ObjList<Function> list = new ObjList<>();
        final TrackingFunction first = new TrackingFunction();
        final TrackingFunction second = new TrackingFunction();
        list.add(first);
        list.add(second);
        PerWorkerFunctionList.clear(list);
        Assert.assertEquals(1, first.clearCount);
        Assert.assertEquals(1, second.clearCount);
        Assert.assertSame("clear() must not drop the elements", first, list.getQuick(0));
        Assert.assertSame(second, list.getQuick(1));
    }

    @Test
    public void testPlainListCloseFreesEveryFunctionAndNullsSlots() {
        // fallback contract: a plain ObjList owns all of its functions, so close() must free
        // every one of them and null the slots so a repeated close cannot double-free
        final ObjList<Function> list = new ObjList<>();
        final TrackingFunction first = new TrackingFunction();
        final TrackingFunction second = new TrackingFunction();
        list.add(first);
        list.add(second);
        PerWorkerFunctionList.close(list);
        Assert.assertEquals(1, first.closeCount);
        Assert.assertEquals(1, second.closeCount);
        Assert.assertNull(list.getQuick(0));
        Assert.assertNull(list.getQuick(1));
        // second close must not double-free
        PerWorkerFunctionList.close(list);
        Assert.assertEquals(1, first.closeCount);
        Assert.assertEquals(1, second.closeCount);
    }

    @Test
    public void testPlainListInitDonatesOwnerStateAndInitsEveryFunction() throws Exception {
        // fallback contract: every slot of a plain ObjList is a worker-local clone, so init()
        // must donate state from the aligned owner to every clone and initialize every clone
        final ObjList<Function> list = new ObjList<>();
        final TrackingFunction firstClone = new TrackingFunction();
        final TrackingFunction secondClone = new TrackingFunction();
        list.add(firstClone);
        list.add(secondClone);

        final ObjList<Function> ownerFunctions = new ObjList<>();
        final TrackingFunction firstOwner = new TrackingFunction();
        final TrackingFunction secondOwner = new TrackingFunction();
        ownerFunctions.add(firstOwner);
        ownerFunctions.add(secondOwner);

        PerWorkerFunctionList.init(list, ownerFunctions, null, null);
        Assert.assertEquals(1, firstOwner.offerStateCount);
        Assert.assertSame("each owner donates to the clone aligned with it", firstClone, firstOwner.lastOfferStateTarget);
        Assert.assertEquals(1, secondOwner.offerStateCount);
        Assert.assertSame(secondClone, secondOwner.lastOfferStateTarget);
        Assert.assertEquals(1, firstClone.initCount);
        Assert.assertEquals(1, secondClone.initCount);
        Assert.assertEquals("owners are initialized by their own factory, not by the worker list", 0, firstOwner.initCount);
        Assert.assertEquals(0, secondOwner.initCount);

        // without owner functions there is no donation, but every clone still initializes
        PerWorkerFunctionList.init(list, null, null, null);
        Assert.assertEquals(1, firstOwner.offerStateCount);
        Assert.assertEquals(1, secondOwner.offerStateCount);
        Assert.assertEquals(2, firstClone.initCount);
        Assert.assertEquals(2, secondClone.initCount);
    }

    @Test
    public void testPlainListIsOwnedReportsEverySlotOwned() {
        final ObjList<Function> list = new ObjList<>();
        list.add(new TrackingFunction());
        list.add(new TrackingFunction());
        Assert.assertTrue(PerWorkerFunctionList.isOwned(list, 0));
        Assert.assertTrue(PerWorkerFunctionList.isOwned(list, 1));
    }

    @Test
    public void testPlainListToTopRewindsEveryFunction() {
        // fallback contract: a plain ObjList owns all of its functions, so toTop() must
        // rewind every one of them
        final ObjList<Function> list = new ObjList<>();
        final TrackingFunction first = new TrackingFunction();
        final TrackingFunction second = new TrackingFunction();
        list.add(first);
        list.add(second);
        PerWorkerFunctionList.toTop(list);
        Assert.assertEquals(1, first.toTopCount);
        Assert.assertEquals(1, second.toTopCount);
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

    private static class ThrowOnceTrackingFunction extends TrackingFunction {
        private boolean isInitFailurePending;
        private boolean isOfferFailurePending;

        private ThrowOnceTrackingFunction(boolean isInitFailurePending, boolean isOfferFailurePending) {
            this.isInitFailurePending = isInitFailurePending;
            this.isOfferFailurePending = isOfferFailurePending;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            super.init(symbolTableSource, executionContext);
            if (isInitFailurePending) {
                isInitFailurePending = false;
                throw new RuntimeException("init failure");
            }
        }

        @Override
        public void offerStateTo(Function that) {
            super.offerStateTo(that);
            if (isOfferFailurePending) {
                isOfferFailurePending = false;
                throw new RuntimeException("offer failure");
            }
        }
    }

    private static class TrackingFunction extends LongFunction {
        private int clearCount;
        private int closeCount;
        private int initCount;
        private Function lastOfferStateTarget;
        private int offerStateCount;
        private int toTopCount;

        @Override
        public void clear() {
            clearCount++;
        }

        @Override
        public void close() {
            closeCount++;
        }

        @Override
        public long getLong(Record rec) {
            return 0;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            initCount++;
        }

        @Override
        public void offerStateTo(Function that) {
            offerStateCount++;
            lastOfferStateTarget = that;
        }

        @Override
        public void toTop() {
            toTopCount++;
        }
    }
}
