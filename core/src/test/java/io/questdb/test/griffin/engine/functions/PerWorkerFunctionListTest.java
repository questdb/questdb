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
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.remove((Object) function));
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.set(0, function));
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.set(0, 1, function));
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.setAll(1, function));
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.setPos(0));
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.setQuick(0, function));
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.sort((a, b) -> 0));
        Assert.assertThrows(UnsupportedOperationException.class, () -> list.sort(0, 1, (a, b) -> 0));
        // rejected mutators must leave the list and ownership metadata intact
        Assert.assertEquals(1, list.size());
        Assert.assertSame(function, list.getQuick(0));
        Assert.assertTrue(PerWorkerFunctionList.isOwned(list, 0));
        PerWorkerFunctionList.close(list);
        Assert.assertEquals(1, function.closeCount);
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
        ownerFunctions.add(borrowed);
        ownerFunctions.add(new TrackingFunction());
        PerWorkerFunctionList.init(list, ownerFunctions, null, null);
        Assert.assertEquals(0, borrowed.initCount);
        Assert.assertEquals(1, owned.initCount);

        PerWorkerFunctionList.clear(list);
        Assert.assertEquals(0, borrowed.clearCount);
        Assert.assertEquals(1, owned.clearCount);

        PerWorkerFunctionList.toTop(list);
        Assert.assertEquals(0, borrowed.toTopCount);
        Assert.assertEquals(1, owned.toTopCount);
    }

    private static class TrackingFunction extends LongFunction {
        private int clearCount;
        private int closeCount;
        private int initCount;
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
        public void toTop() {
            toTopCount++;
        }
    }
}
