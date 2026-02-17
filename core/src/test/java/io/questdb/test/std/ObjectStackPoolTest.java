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

package io.questdb.test.std;

import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;
import io.questdb.std.ObjectStackPool;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests for ObjectStackPool focusing directly on its public API behavior.
 */
public class ObjectStackPoolTest {

    public static final int INITIAL_CAPACITY = 8;

    private ObjectFactory<TestMutable> factory;
    private ObjectStackPool<TestMutable> pool;

    @Before
    public void setUp() {
        // Create a real factory
        factory = TestMutable::new;

        pool = new ObjectStackPool<>(factory, INITIAL_CAPACITY);
    }

    @Test
    public void testClear() {
        // Get some objects
        TestMutable ignore1 = pool.next();
        TestMutable ignore2 = pool.next();

        // Clear the pool - this calls resetCapacity()
        pool.clear();

        // The stack should be reset (capacity reduced) but outieCount should remain unchanged
        assertEquals("Size should be 0 after clear", 0, pool.getSize());
        assertEquals("OutieCount should remain unchanged", 2, pool.getOutieCount());
    }

    @Test
    public void testReset() {
        // Get some objects
        TestMutable ignore1 = pool.next();
        TestMutable ignore2 = pool.next();

        // Clear the pool - this calls resetCapacity()
        pool.resetCapacity();

        // The stack should be reset (capacity reduced) but outieCount should remain unchanged
        assertEquals("Size should be 0 after clear", INITIAL_CAPACITY - 2, pool.getSize());
        assertEquals("OutieCount should remain unchanged", 2, pool.getOutieCount());
    }

    @Test
    public void testExpandAndReleasePow2Capacity() {
        // Create a pool with small initial size
        ObjectStackPool<TestMutable> smallPool = new ObjectStackPool<>(factory, 4);

        // Get 10 objects to force expansion
        TestMutable[] objects = new TestMutable[10];
        for (int i = 0; i < objects.length; i++) {
            objects[i] = smallPool.next();
        }

        // Release all objects in reverse order
        for (int i = objects.length - 1; i >= 0; i--) {
            smallPool.release(objects[i]);
        }

        // Check final state - only up to initialSize objects should be in the pool
        assertEquals("Pool should contain initial size objects", smallPool.getInitialCapacity() * 4, smallPool.getSize());
        assertEquals("OutieCount should be 0", 0, smallPool.getOutieCount());
    }

    @Test
    public void testExpandAndReleasePow() {
        // Create a pool with small initial size
        // not power of 2 capacity
        ObjectStackPool<TestMutable> smallPool = new ObjectStackPool<>(factory, 5);
        // ceil pow2 is 8, however, this is a pool, not a pure stack
        // when pool is filled with 8 mutables, its capacity doubles (it doubles after push, not before)
        Assert.assertEquals(16, smallPool.getCapacity());

        // Get 10 objects to force expansion
        TestMutable[] objects = new TestMutable[10];
        for (int i = 0; i < objects.length; i++) {
            objects[i] = smallPool.next();
        }

        // Release all objects in reverse order
        for (int i = objects.length - 1; i >= 0; i--) {
            smallPool.release(objects[i]);
        }

        // Check final state - only up to initialSize objects should be in the pool
        assertEquals("Pool should contain initial size objects", smallPool.getInitialCapacity() * 2, smallPool.getSize());
        assertEquals("OutieCount should be 0", 0, smallPool.getOutieCount());
    }

    @Test
    public void testFuzzRandomOperations() {
        // Create a pool with a small initial size to force expansion
        ObjectStackPool<TestMutable> smallPool = new ObjectStackPool<>(factory, 3);

        // Perform a series of random next() and release() operations
        TestMutable[] objects = new TestMutable[20];
        int objCount = 0;

        for (int i = 0; i < 100; i++) {
            if (Math.random() < 0.7 && objCount < objects.length) {
                // 70% chance to call next() if we have room in our array
                objects[objCount++] = smallPool.next();
            } else if (objCount > 0) {
                // Otherwise release a random object if we have any
                int idx = (int) (Math.random() * objCount);
                smallPool.release(objects[idx]);

                // Move the last object to the released position
                objCount--;
                if (idx < objCount) {
                    objects[idx] = objects[objCount];
                }
                objects[objCount] = null;
            }
        }

        // Check that outieCount matches our tracking
        assertEquals("OutieCount should match our tracking", objCount, smallPool.getOutieCount());

        // Release all remaining objects
        for (int i = 0; i < objCount; i++) {
            if (objects[i] != null) {
                smallPool.release(objects[i]);
            }
        }

        // All objects should now be returned (up to initialSize)
        assertEquals("OutieCount should be 0", 0, smallPool.getOutieCount());
        smallPool.resetCapacity();
        assertTrue("Size should be <= initialSize", smallPool.getSize() <= smallPool.getInitialCapacity());
    }

    @Test
    public void testGetCapacity() {
        // This test is just to verify the getCapacity method works
        int capacity = pool.getCapacity();
        assertTrue("Capacity should be positive", capacity > 0);
    }

    @Test
    public void testInitialization() {
        // Verify initial state
        assertEquals("Initial size should match constructor parameter", INITIAL_CAPACITY, pool.getInitialCapacity());
        assertEquals("Initial size of stack should match constructor parameter", INITIAL_CAPACITY, pool.getSize());
        assertEquals("Initial outieCount should be 0", 0, pool.getOutieCount());
    }

    @Test
    public void testNextEmptyPool() {
        // Create a custom pool with smaller initial size
        ObjectStackPool<TestMutable> smallPool = new ObjectStackPool<>(factory, 2);

        // Get all objects out of the pool
        TestMutable ignore1 = smallPool.next();
        TestMutable ignore2 = smallPool.next();

        // Verify state
        assertEquals("Size should be 0", 0, smallPool.getSize());
        assertEquals("OutieCount should be 2", 2, smallPool.getOutieCount());

        // Get one more object - this should trigger expansion
        TestMutable obj3 = smallPool.next();

        // Verify the expansion worked
        assertNotNull("Object from expanded pool should not be null", obj3);
        assertTrue("Object from expanded pool should be cleared", obj3.isCleared());

        // Check updated state - pool should have expanded to hold 2 more objects
        // and 1 object was taken from the expanded pool
        assertEquals("Size should be 1 after expansion", 1, smallPool.getSize());
        assertEquals("OutieCount should be 3", 3, smallPool.getOutieCount());

        // Verify the capacity actually expanded
        assertTrue("Capacity should have doubled", smallPool.getCapacity() > 2);
    }

    @Test
    public void testNextMultipleExpansions() {
        // Create a very small pool to force multiple expansions
        ObjectStackPool<TestMutable> tinyPool = new ObjectStackPool<>(factory, 1);

        // Get 10 objects, forcing several expansions
        TestMutable[] objects = new TestMutable[10];
        for (int i = 0; i < objects.length; i++) {
            objects[i] = tinyPool.next();
            assertNotNull("Object should not be null", objects[i]);
            assertTrue("Object should be cleared", objects[i].isCleared());
        }

        // Verify final state
        assertEquals("OutieCount should match objects taken", objects.length, tinyPool.getOutieCount());
        // pool size doubles when it gets to 10, we started with pool size 1, popped 10 items. At the time of the last
        // item pop, the capacity should be 16, after 10 items the size (not capacity) should be 6
        assertEquals("Pool should be empty", 6, tinyPool.getSize());
        assertEquals(16, tinyPool.getCapacity());
    }

    @Test
    public void testNext_normalOperation() {
        // Get an object from the pool
        TestMutable obj = pool.next();

        // Object should not be null and should be cleared
        assertNotNull("Object from pool should not be null", obj);
        assertTrue("Object should be cleared", obj.isCleared());

        // Check pool state
        assertEquals("Size should decrease by 1", 7, pool.getSize());
        assertEquals("OutieCount should increase by 1", 1, pool.getOutieCount());
    }

    @Test
    public void testReleaseAllObjects() {
        // Get all 5 objects
        TestMutable[] objects = new TestMutable[INITIAL_CAPACITY];
        for (int i = 0; i < objects.length; i++) {
            objects[i] = pool.next();
        }

        assertEquals("Pool should be empty", 0, pool.getSize());
        assertEquals("OutieCount should match objects taken", objects.length, pool.getOutieCount());

        // Release all objects
        for (TestMutable obj : objects) {
            pool.release(obj);
        }

        // Check pool state
        assertEquals("All objects should be back in the pool", INITIAL_CAPACITY, pool.getSize());
        assertEquals("OutieCount should be 0", 0, pool.getOutieCount());
    }

    @Test
    public void testReleaseOverCapacity() {
        // Create a pool with a small initial size
        ObjectStackPool<TestMutable> smallPool = new ObjectStackPool<>(factory, 2);

        // Get three objects to exceed initialSize
        TestMutable ignored = smallPool.next();
        TestMutable obj2 = smallPool.next();
        TestMutable obj3 = smallPool.next();

        assertEquals(4, smallPool.getCapacity());

        // Release an object when outieCount > initialSize
        smallPool.release(obj3);

        // Since outieCount (2) is still >= initialSize (2), the object should be added back to the stack
        assertEquals("Size should not increase", 2, smallPool.getSize());
        assertEquals("OutieCount should decrease", 2, smallPool.getOutieCount());

        // Now release another object to get below initialSize
        smallPool.release(obj2);

        // Now the object should be added back to the stack
        assertEquals("Size should increase", 3, smallPool.getSize());
        assertEquals("OutieCount should be 1", 1, smallPool.getOutieCount());
    }

    @Test
    public void testRelease_normalOperation() {
        // Get an object from the pool
        TestMutable obj = pool.next();
        obj.setCleared(false); // Reset cleared flag to verify next() always clears

        // Release it back
        pool.release(obj);

        // Check pool state
        assertEquals("Size should be back to original", INITIAL_CAPACITY, pool.getSize());
        assertEquals("OutieCount should be 0", 0, pool.getOutieCount());

        // Get the same object again
        TestMutable objAgain = pool.next();

        // Object should always be cleared on next() call
        assertTrue("Object should be cleared when obtained via next()", objAgain.isCleared());
    }

    @Test
    public void testRelease_nullObject() {
        // Get an object to increase outieCount
        TestMutable ignore = pool.next();

        int initialSize = pool.getSize();
        int initialOutieCount = pool.getOutieCount();

        // Release a null object (should not throw)
        pool.release(null);

        // State should remain unchanged
        assertEquals("Size should not change when releasing null", initialSize, pool.getSize());
        assertEquals("OutieCount should not change when releasing null", initialOutieCount, pool.getOutieCount());
    }

    @Test
    public void testResetCapacity() {
        var pool = new ObjectStackPool<>(factory, INITIAL_CAPACITY);
        // extract 5x INITIAL CAPACITY
        var list = new ObjList<TestMutable>();
        for (int i = 0, n = INITIAL_CAPACITY * 5; i < n; i++) {
            list.add(pool.next());
        }

        for (int i = 0, n = list.size(); i < n; i++) {
            pool.release(list.getQuick(i));
        }

        // Reset capacity - this shrinks the pool
        pool.resetCapacity();

        // The stack should be empty after capacity reset, but outieCount remains the same
        assertEquals("Size should be initial capacity after resetCapacity", INITIAL_CAPACITY - 1, pool.getSize());
        assertEquals("OutieCount should remain unchanged", 0, pool.getOutieCount());

        // after partial pool consumption and capacity reset, we should be able to extract at least
        // INITIAL CAPACITY number of elements and more
        for (int i = 0, n = INITIAL_CAPACITY * 4; i < n; i++) {
            Assert.assertNotNull(pool.next());
        }
        pool.resetCapacity();
        assertEquals("Size should be initial capacity after resetCapacity", INITIAL_CAPACITY - 1, pool.getSize());
    }

    /**
     * Simple implementation of Mutable for testing
     */
    private static class TestMutable implements Mutable {
        private boolean cleared = false;

        @Override
        public void clear() {
            cleared = true;
        }

        public boolean isCleared() {
            return cleared;
        }

        public void setCleared(boolean cleared) {
            this.cleared = cleared;
        }
    }
}