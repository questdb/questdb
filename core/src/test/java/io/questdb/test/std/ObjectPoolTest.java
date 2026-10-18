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

package io.questdb.test.std;

import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.RetainedCapacity;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class ObjectPoolTest extends AbstractTest {
    private static final int INITIAL_SIZE = 4;
    private ObjectPool<TestObject> pool;
    private Rnd rnd;

    @Before
    public void setUp() {
        pool = new ObjectPool<>(TestObject::new, INITIAL_SIZE);
        rnd = TestUtils.generateRandom(null);
    }

    @Test
    public void testBoundedPoolClearsEachObjectOncePerUse() {
        final ObjectPool<TestObject> bounded = new ObjectPool<>(TestObject::new, INITIAL_SIZE, 8);
        final ObjList<TestObject> taken = take(bounded, 6);
        for (int i = 0; i < 6; i++) {
            Assert.assertEquals(1, taken.getQuick(i).getClearCount());
        }
        bounded.clear();
        final ObjList<TestObject> again = take(bounded, 6);
        for (int i = 0; i < 6; i++) {
            Assert.assertSame(taken.getQuick(i), again.getQuick(i));
            Assert.assertEquals(2, again.getQuick(i).getClearCount());
        }
    }

    @Test
    public void testBoundedReleaseClearsObject() {
        final ObjectPool<TestObject> bounded = new ObjectPool<>(TestObject::new, INITIAL_SIZE, 8);
        final ObjList<TestObject> taken = take(bounded, 2);
        bounded.release(taken.getQuick(0));
        Assert.assertEquals(0, taken.getQuick(0).getValue());
        Assert.assertEquals(2, taken.getQuick(1).getValue());
        Assert.assertSame(taken.getQuick(0), bounded.next());
        Assert.assertEquals(0, taken.getQuick(0).getValue());
        Assert.assertEquals(2, taken.getQuick(0).getClearCount());
    }

    @Test
    public void testBorrowAfterClear() {
        TestObject obj1 = pool.next();
        pool.release(obj1);
        pool.clear();

        TestObject obj2 = pool.next();
        Assert.assertNotNull("Should be able to borrow after clear", obj2);
    }

    @Test
    public void testClearAtOrBelowFloorKeepsEveryObject() {
        final ObjectPool<TestObject> bounded = new ObjectPool<>(TestObject::new, INITIAL_SIZE, 16);
        final ObjList<TestObject> taken = take(bounded, 10);
        Assert.assertEquals(16, bounded.getCapacity());

        bounded.clear();
        Assert.assertEquals(16, bounded.getCapacity());
        for (int i = 0; i < 10; i++) {
            Assert.assertSame(taken.getQuick(i), bounded.peekQuick(i));
            Assert.assertEquals(0, taken.getQuick(i).getValue());
        }
    }

    @Test
    public void testClearKeepsObjectsRecentUsesTook() {
        final ObjectPool<TestObject> bounded = new ObjectPool<>(TestObject::new, INITIAL_SIZE, 8);
        final ObjList<TestObject> taken = take(bounded, 20);
        Assert.assertEquals(32, bounded.getCapacity());

        bounded.clear();
        Assert.assertEquals(20, bounded.getCapacity());
        for (int i = 0; i < 20; i++) {
            Assert.assertSame(taken.getQuick(i), bounded.peekQuick(i));
            Assert.assertEquals(0, taken.getQuick(i).getValue());
        }
    }

    @Test
    public void testClearUnboundedKeepsPeakAndClearsLazily() {
        final ObjList<TestObject> taken = take(pool, 20);
        pool.clear();
        Assert.assertEquals(32, pool.getCapacity());
        for (int i = 0; i < 20; i++) {
            Assert.assertSame(taken.getQuick(i), pool.peekQuick(i));
            Assert.assertEquals(i + 1, taken.getQuick(i).getValue());
        }
        Assert.assertEquals(0, pool.next().getValue());
    }

    @Test
    public void testFloorBelowInitialSizeKeepsInitialSize() {
        final ObjectPool<TestObject> bounded = new ObjectPool<>(TestObject::new, INITIAL_SIZE, 1);
        final ObjList<TestObject> taken = take(bounded, 10);
        Assert.assertEquals(16, bounded.getCapacity());

        bounded.clear();
        Assert.assertEquals(10, bounded.getCapacity());
        clearPastRetention(bounded);
        Assert.assertEquals(INITIAL_SIZE, bounded.getCapacity());
        for (int i = 0; i < INITIAL_SIZE; i++) {
            Assert.assertSame(taken.getQuick(i), bounded.peekQuick(i));
            Assert.assertEquals(0, taken.getQuick(i).getValue());
        }
    }

    @Test
    public void testGrowing() {
        TestObject[] objects = new TestObject[INITIAL_SIZE * 2];

        // borrow more than initial size
        for (int i = 0; i < objects.length; i++) {
            objects[i] = pool.next();
            Assert.assertNotNull("Should get object on borrow", objects[i]);
        }

        for (TestObject obj : objects) {
            pool.release(obj);
        }

        TestObject[] objectsB = new TestObject[INITIAL_SIZE * 2];
        for (int i = 0; i < objects.length; i++) {
            objectsB[i] = pool.next();
            Assert.assertNotNull("Should get object on borrow", objects[i]);
        }

        Arrays.sort(objects, Comparator.comparingInt(System::identityHashCode));
        Arrays.sort(objectsB, Comparator.comparingInt(System::identityHashCode));

        for (int i = 0; i < objects.length; i++) {
            Assert.assertSame("Should get same object", objects[i], objectsB[i]);
        }
    }

    @Test
    public void testIndividualReleasesCountTowardsPeak() {
        final ObjectPool<TestObject> bounded = new ObjectPool<>(TestObject::new, INITIAL_SIZE, 8);
        final ObjList<TestObject> taken = take(bounded, 20);
        for (int i = 19; i > -1; i--) {
            bounded.release(taken.getQuick(i));
        }
        bounded.clear();
        Assert.assertEquals(20, bounded.getCapacity());
    }

    @Test
    public void testNextReturnsNewObject() {
        TestObject obj = pool.next();
        Assert.assertNotNull("Pool should return non-null object", obj);
    }

    @Test
    public void testObjectIdentityPreserved() {
        TestObject obj1 = pool.next();
        TestObject obj2 = pool.next();
        pool.release(obj1);

        TestObject obj3 = pool.next();
        Assert.assertTrue("Should get same object instance back", obj1 == obj3 || obj2 == obj3);
    }

    @Test
    public void testObjectStateAfterReturn() {
        TestObject obj = pool.next();
        obj.setValue(42);
        pool.release(obj);

        TestObject obj2 = pool.next();
        Assert.assertSame("Should get same object back", obj, obj2);
        Assert.assertEquals("Object should be cleared after return", 0, obj.getValue());
        Assert.assertTrue("Object clear should have been called", obj.wasCleared());
    }

    @Test
    public void testOneOffUseReturnsToFloor() {
        final ObjectPool<TestObject> bounded = new ObjectPool<>(TestObject::new, INITIAL_SIZE, 8);
        final ObjList<TestObject> taken = take(bounded, 20);
        bounded.clear();
        for (int i = 0; i < RetainedCapacity.WINDOW_USES; i++) {
            take(bounded, 2);
            bounded.clear();
            Assert.assertEquals(20, bounded.getCapacity());
        }
        for (int i = 0; i < RetainedCapacity.WINDOW_USES; i++) {
            take(bounded, 2);
            bounded.clear();
        }
        Assert.assertEquals(8, bounded.getCapacity());
        for (int i = 0; i < 8; i++) {
            Assert.assertSame(taken.getQuick(i), bounded.peekQuick(i));
        }

        final ObjList<TestObject> again = take(bounded, 9);
        Assert.assertNotSame(taken.getQuick(8), again.getQuick(8));
        Assert.assertEquals(16, bounded.getCapacity());
    }

    @Test
    public void testPeekQuick() {
        TestObject obj0 = pool.next();
        TestObject obj1 = pool.next();
        TestObject obj2 = pool.next();

        Assert.assertSame("Should get same object back", obj0, pool.peekQuick(0));
        Assert.assertSame("Should get same object back", obj1, pool.peekQuick(1));
        Assert.assertSame("Should get same object back", obj2, pool.peekQuick(2));

        pool.release(obj1);
        Assert.assertSame("Should get same object back", obj0, pool.peekQuick(0));
        Assert.assertSame("Should get same object back", obj2, pool.peekQuick(1));
    }

    @Test
    public void testRecurringUseKeepsSameObjects() {
        final ObjectPool<TestObject> bounded = new ObjectPool<>(TestObject::new, INITIAL_SIZE, 8);
        final ObjList<TestObject> first = take(bounded, 20);
        bounded.clear();
        for (int round = 0; round < 3 * RetainedCapacity.WINDOW_USES; round++) {
            if (round % (RetainedCapacity.WINDOW_USES / 2) == 0) {
                final ObjList<TestObject> taken = take(bounded, 20);
                Assert.assertEquals(20, bounded.getCapacity());
                for (int i = 0; i < 20; i++) {
                    Assert.assertSame(first.getQuick(i), taken.getQuick(i));
                }
            } else {
                take(bounded, 2);
            }
            bounded.clear();
            Assert.assertEquals(20, bounded.getCapacity());
            Assert.assertEquals(0, bounded.getPos());
        }
    }

    @Test
    public void testReleaseNotFromPool() {
        TestObject notFromPool = new TestObject();
        try {
            pool.release(notFromPool);
        } catch (AssertionError expected) {

        }

        TestObject ignore = pool.next();
        try {
            pool.release(notFromPool);
        } catch (AssertionError expected) {

        }
    }

    @Test
    public void testReturnInDifferentOrder() {
        int size = rnd.nextInt(INITIAL_SIZE * 10) + 1;

        List<TestObject> objectsA = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            objectsA.add(pool.next());
        }

        // return in random order
        rnd.shuffle(objectsA);
        for (TestObject obj : objectsA) {
            pool.release(obj);
        }

        List<TestObject> objectsB = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            objectsB.add(pool.next());
        }

        objectsA.sort(Comparator.comparingInt(System::identityHashCode));
        objectsB.sort(Comparator.comparingInt(System::identityHashCode));

        for (int i = 0; i < size; i++) {
            Assert.assertSame("Should get same object back", objectsA.get(i), objectsB.get(i));
        }
    }

    @Test
    public void testRewindClearsReleasedObjects() {
        final ObjectPool<TestObject> bounded = new ObjectPool<>(TestObject::new, INITIAL_SIZE, 8);
        final ObjList<TestObject> taken = take(bounded, 6);
        bounded.rewind(2);
        Assert.assertEquals(2, bounded.getPos());
        Assert.assertEquals(1, taken.getQuick(0).getValue());
        Assert.assertEquals(2, taken.getQuick(1).getValue());
        for (int i = 2; i < 6; i++) {
            Assert.assertEquals(0, taken.getQuick(i).getValue());
        }
    }

    @Test
    public void testRewindPastFloorClearsEveryReleasedObject() {
        final ObjectPool<TestObject> bounded = new ObjectPool<>(TestObject::new, INITIAL_SIZE, 8);
        final ObjList<TestObject> taken = take(bounded, 20);
        bounded.rewind(4);
        Assert.assertEquals(4, bounded.getPos());
        Assert.assertEquals(32, bounded.getCapacity());
        for (int i = 0; i < 4; i++) {
            Assert.assertEquals(i + 1, taken.getQuick(i).getValue());
        }
        for (int i = 4; i < 20; i++) {
            Assert.assertEquals(0, taken.getQuick(i).getValue());
        }
    }

    private static void clearPastRetention(ObjectPool<TestObject> pool) {
        for (int i = 0, n = 2 * RetainedCapacity.WINDOW_USES; i < n; i++) {
            pool.clear();
        }
    }

    private static ObjList<TestObject> take(ObjectPool<TestObject> pool, int count) {
        final ObjList<TestObject> taken = new ObjList<>();
        for (int i = 0; i < count; i++) {
            final TestObject obj = pool.next();
            obj.setValue(i + 1);
            taken.add(obj);
        }
        return taken;
    }

    private static class TestObject implements Mutable {
        private int clearCount = 0;
        private int value = 0;

        @Override
        public void clear() {
            value = 0;
            clearCount++;
        }

        public int getClearCount() {
            return clearCount;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public boolean wasCleared() {
            return clearCount > 0;
        }
    }
}
