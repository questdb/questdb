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

import io.questdb.std.IntList;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class IntListTest {

    @Test
    public void testAddAll() {
        IntList src = new IntList();
        for (int i = 0; i < 100; i++) {
            src.add(i);
        }

        IntList dst = new IntList();
        dst.clear();
        dst.addAll(src);

        Assert.assertEquals(dst, src);
    }

    @Test
    public void testBasicShift() {
        IntList list = new IntList();
        list.add(1);
        list.add(2);
        list.add(3);

        list.rshift(2);

        Assert.assertEquals(5, list.size());
        Assert.assertEquals(IntList.NO_ENTRY_VALUE, list.get(0));
        Assert.assertEquals(IntList.NO_ENTRY_VALUE, list.get(1));
        Assert.assertEquals(1, list.get(2));
        Assert.assertEquals(2, list.get(3));
        Assert.assertEquals(3, list.get(4));
    }

    @Test
    public void testBinarySearchFuzz() {
        final int N = 997; // prime
        final int skipRate = 4;
        for (int c = 0; c < N; c++) {
            for (int i = 0; i < skipRate; i++) {
                testBinarySearchFuzz0(c, i);
            }
        }

        testBinarySearchFuzz0(1, 0);
    }

    @Test
    public void testEquals() {
        final IntList list1 = new IntList();
        list1.add(1);
        list1.add(2);
        list1.add(3);

        // different order
        final IntList list2 = new IntList();
        list2.add(1);
        list2.add(3);
        list2.add(2);
        Assert.assertNotEquals(list1, list2);

        // longer
        final IntList list3 = new IntList();
        list3.add(1);
        list3.add(2);
        list3.add(3);
        list3.add(4);
        Assert.assertNotEquals(list1, list3);

        // shorter
        final IntList list4 = new IntList();
        list4.add(1);
        list4.add(2);
        Assert.assertNotEquals(list1, list4);

        // empty
        final IntList list5 = new IntList();
        Assert.assertNotEquals(list1, list5);

        // null
        Assert.assertNotEquals(null, list1);

        // equals
        final IntList list6 = new IntList();
        list6.add(1);
        list6.add(2);
        list6.add(3);
        Assert.assertEquals(list1, list6);
    }

    @Test
    public void testIndexOf() {
        IntList list = new IntList();
        for (int i = 100; i > -1; i--) {
            list.add(i);
        }

        for (int i = 100; i > -1; i--) {
            Assert.assertEquals(100 - i, list.indexOf(i, 0, 101));
        }
    }

    @Test
    public void testLargeShift() {
        IntList list = new IntList();
        list.add(42);

        // Shift by a large number
        list.rshift(1000);

        Assert.assertEquals(1001, list.size());
        Assert.assertEquals(IntList.NO_ENTRY_VALUE, list.get(0));
        Assert.assertEquals(IntList.NO_ENTRY_VALUE, list.get(999));
        Assert.assertEquals(42, list.get(1000));
    }

    @Test
    public void testRestoreInitialCapacity() {
        final int N = 1000;
        IntList list = new IntList();
        int initialCapacity = list.capacity();

        for (int i = 0; i < N; i++) {
            list.add(i);
        }

        Assert.assertEquals(N, list.size());
        Assert.assertTrue(list.capacity() >= N);

        list.restoreInitialCapacity();
        Assert.assertEquals(0, list.size());
        Assert.assertEquals(initialCapacity, list.capacity());
    }

    @Test
    public void testShiftEmptyList() {
        IntList list = new IntList();

        list.rshift(3);

        Assert.assertEquals(3, list.size());
        Assert.assertEquals(IntList.NO_ENTRY_VALUE, list.get(0));
        Assert.assertEquals(IntList.NO_ENTRY_VALUE, list.get(1));
        Assert.assertEquals(IntList.NO_ENTRY_VALUE, list.get(2));
    }

    @Test
    public void testShiftRequiringResize() {
        // Create a list with initial capacity of 3
        IntList list = new IntList(3);
        list.add(1);
        list.add(2);
        list.add(3);

        // This will require expanding the array
        list.rshift(2);

        Assert.assertEquals(5, list.size());
        Assert.assertEquals(IntList.NO_ENTRY_VALUE, list.get(0));
        Assert.assertEquals(IntList.NO_ENTRY_VALUE, list.get(1));
        Assert.assertEquals(1, list.get(2));
        Assert.assertEquals(2, list.get(3));
        Assert.assertEquals(3, list.get(4));
    }

    @Test
    public void testShiftWithinCapacity() {
        // Create a list with initial capacity of 10
        IntList list = new IntList(10);
        list.add(1);
        list.add(2);

        // This shouldn't require expanding the array
        list.rshift(3);

        Assert.assertEquals(5, list.size());
        Assert.assertEquals(IntList.NO_ENTRY_VALUE, list.get(0));
        Assert.assertEquals(IntList.NO_ENTRY_VALUE, list.get(1));
        Assert.assertEquals(IntList.NO_ENTRY_VALUE, list.get(2));
        Assert.assertEquals(1, list.get(3));
        Assert.assertEquals(2, list.get(4));
    }

    @Test
    public void testShiftZero() {
        IntList list = new IntList();
        list.add(1);
        list.add(2);

        list.rshift(0);

        Assert.assertEquals(2, list.size());
        Assert.assertEquals(1, list.get(0));
        Assert.assertEquals(2, list.get(1));
    }

    @Test
    public void testSmoke() {
        IntList list = new IntList();
        for (int i = 0; i < 100; i++) {
            list.add(i);
        }
        Assert.assertEquals(100, list.size());
        Assert.assertTrue(list.capacity() >= 100);

        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(i, list.getQuick(i));
            Assert.assertTrue(list.contains(i));
        }

        for (int i = 0; i < 100; i++) {
            list.remove(i);
        }
        Assert.assertEquals(0, list.size());
    }

    @Test
    public void testSortGroups() {
        Rnd rnd = TestUtils.generateRandom(null);
        int n = 1 + rnd.nextInt(20);
        sortGroupsRandom(n, rnd.nextInt(10000), rnd);
    }

    @Test
    public void testSortGroupsDuplicatePivot() {
        // Test case where multiple groups equal the pivot
        // This tests if partition correctly handles equal elements

        int groupSize = 1;
        IntList list = new IntList();

        // Many duplicates including pivot
        list.add(3);
        list.add(1);
        list.add(3);
        list.add(5);
        list.add(3);
        list.add(2);
        list.add(3);  // Pivot - multiple elements equal to it

        list.sortGroups(groupSize);

        // Should be: [1, 2, 3, 3, 3, 3, 5]
        Assert.assertEquals(1, list.get(0));
        Assert.assertEquals(2, list.get(1));
        for (int i = 2; i <= 5; i++) {
            Assert.assertEquals(3, list.get(i));
        }
        Assert.assertEquals(5, list.get(6));
    }

    @Test
    public void testSortGroupsEqualElements() {
        Rnd rnd = TestUtils.generateRandom(null);
        int n = 1 + rnd.nextInt(20);
        checkSortGroupsEqualElements(n, rnd.nextInt(10000), rnd);
    }

    @Test
    public void testSortGroupsFuzzTest() {
        // Comprehensive fuzz test to verify quicksort correctness under random conditions
        Rnd rnd = new Rnd();
        int iterations = 1000;  // Number of random test cases

        for (int iter = 0; iter < iterations; iter++) {
            // Random group size between 1 and 10
            int groupSize = rnd.nextInt(10) + 1;

            // Random number of groups between 0 and 100
            int numGroups = rnd.nextInt(101);

            IntList list = new IntList(numGroups * groupSize);
            int[][] expectedGroups = new int[numGroups][groupSize];

            // Generate random data
            for (int i = 0; i < numGroups; i++) {
                for (int j = 0; j < groupSize; j++) {
                    int value = rnd.nextInt(1000) - 500;  // Random values between -500 and 499
                    expectedGroups[i][j] = value;
                    list.add(value);
                }
            }

            // Sort using Java's Arrays.sort for comparison
            Arrays.sort(expectedGroups, (a, b) -> {
                for (int k = 0; k < groupSize; k++) {
                    int cmp = Integer.compare(a[k], b[k]);
                    if (cmp != 0) return cmp;
                }
                return 0;
            });

            // Sort using our implementation
            list.sortGroups(groupSize);

            // Verify the results match
            for (int i = 0; i < numGroups; i++) {
                for (int j = 0; j < groupSize; j++) {
                    int expectedValue = expectedGroups[i][j];
                    int actualValue = list.get(i * groupSize + j);
                    Assert.assertEquals(
                            String.format("Mismatch at group %d, element %d (iteration %d, groupSize %d, numGroups %d)",
                                    i, j, iter, groupSize, numGroups),
                            expectedValue, actualValue
                    );
                }
            }
        }
    }

    @Test
    public void testSortGroupsFuzzTestBoundaryValues() {
        // Test with boundary values (Integer.MIN_VALUE, Integer.MAX_VALUE, 0)
        Rnd rnd = new Rnd();

        for (int iter = 0; iter < 100; iter++) {
            int groupSize = rnd.nextInt(5) + 1;
            int numGroups = rnd.nextInt(50) + 1;
            IntList list = new IntList(numGroups * groupSize);

            // Generate data with boundary values
            int[] boundaryValues = {Integer.MIN_VALUE, Integer.MAX_VALUE, 0, -1, 1};

            for (int i = 0; i < numGroups; i++) {
                for (int j = 0; j < groupSize; j++) {
                    if (rnd.nextDouble() < 0.3) {
                        // 30% chance to use a boundary value
                        list.add(boundaryValues[rnd.nextInt(boundaryValues.length)]);
                    } else {
                        // 70% chance for random value
                        list.add(rnd.nextInt());
                    }
                }
            }

            // Create expected result using Java's sort
            int[][] expected = new int[numGroups][groupSize];
            for (int i = 0; i < numGroups; i++) {
                for (int j = 0; j < groupSize; j++) {
                    expected[i][j] = list.get(i * groupSize + j);
                }
            }

            Arrays.sort(expected, (a, b) -> {
                for (int k = 0; k < groupSize; k++) {
                    int cmp = Integer.compare(a[k], b[k]);
                    if (cmp != 0) return cmp;
                }
                return 0;
            });

            // Sort with our implementation
            list.sortGroups(groupSize);

            // Verify
            for (int i = 0; i < numGroups; i++) {
                for (int j = 0; j < groupSize; j++) {
                    Assert.assertEquals(
                            String.format("Boundary value test failed at group %d, element %d", i, j),
                            expected[i][j],
                            list.get(i * groupSize + j)
                    );
                }
            }
        }
    }

    @Test
    public void testSortGroupsFuzzTestEdgeCases() {
        // Fuzz test focusing on edge cases
        Rnd rnd = TestUtils.generateRandom(null);

        // Test 1: All identical elements
        for (int groupSize = 1; groupSize <= 5; groupSize++) {
            IntList list = new IntList();
            int value = rnd.nextInt(100);
            int numGroups = rnd.nextInt(50) + 1;

            for (int i = 0; i < numGroups * groupSize; i++) {
                list.add(value);
            }

            list.sortGroups(groupSize);

            // All elements should still be the same value
            for (int i = 0; i < numGroups * groupSize; i++) {
                Assert.assertEquals(value, list.get(i));
            }
        }

        // Test 2: Already sorted data
        for (int groupSize = 1; groupSize <= 5; groupSize++) {
            IntList list = new IntList();
            int numGroups = rnd.nextInt(50) + 1;

            for (int i = 0; i < numGroups; i++) {
                for (int j = 0; j < groupSize; j++) {
                    list.add(i * 10 + j);  // Ascending values
                }
            }

            IntList copy = new IntList(list);
            list.sortGroups(groupSize);

            // Should remain unchanged
            for (int i = 0; i < list.size(); i++) {
                Assert.assertEquals(copy.get(i), list.get(i));
            }
        }

        // Test 3: Reverse sorted data
        for (int groupSize = 1; groupSize <= 5; groupSize++) {
            IntList list = new IntList();
            int numGroups = rnd.nextInt(50) + 1;

            for (int i = numGroups - 1; i >= 0; i--) {
                for (int j = groupSize - 1; j >= 0; j--) {
                    list.add(i * 10 + j);  // Descending values
                }
            }

            list.sortGroups(groupSize);

            // Verify sorted in ascending order
            for (int i = 0; i < numGroups - 1; i++) {
                int firstElemCurrent = list.get(i * groupSize);
                int firstElemNext = list.get((i + 1) * groupSize);
                Assert.assertTrue(firstElemCurrent <= firstElemNext);
            }
        }

        // Test 4: Many duplicates
        for (int groupSize = 1; groupSize <= 5; groupSize++) {
            IntList list = new IntList();
            int numGroups = rnd.nextInt(50) + 10;
            int numUniqueValues = rnd.nextInt(5) + 1;  // Only 1-5 unique values

            for (int i = 0; i < numGroups; i++) {
                for (int j = 0; j < groupSize; j++) {
                    list.add(rnd.nextInt(numUniqueValues));
                }
            }

            list.sortGroups(groupSize);

            // Verify sorted order
            for (int i = 0; i < numGroups - 1; i++) {
                for (int j = 0; j < groupSize; j++) {
                    int currentVal = list.get(i * groupSize + j);
                    int nextVal = list.get((i + 1) * groupSize + j);
                    if (currentVal != nextVal) {
                        Assert.assertTrue(
                                String.format("Groups not in order at position %d", i),
                                currentVal <= nextVal
                        );
                        break;
                    }
                }
            }
        }
    }

    @Test
    public void testSortGroupsFuzzTestLargeScale() {
        // Test with large datasets to ensure no stack overflow or memory issues
        Rnd rnd = TestUtils.generateRandom(null);

        // Test with various large sizes
        int[] testSizes = {1000, 5000, 10000, 50000};
        int[] groupSizes = {1, 2, 3, 5};

        for (int size : testSizes) {
            for (int groupSize : groupSizes) {
                int numGroups = size / groupSize;
                // Ensure size is a multiple of groupSize
                int actualSize = numGroups * groupSize;
                IntList list = new IntList(actualSize);

                // Generate random data
                for (int i = 0; i < actualSize; i++) {
                    list.add(rnd.nextInt());
                }

                // This should not throw StackOverflowError
                try {
                    list.sortGroups(groupSize);

                    // Basic verification - check sorted order
                    for (int i = 0; i < numGroups - 1; i++) {
                        boolean inOrder = false;
                        for (int j = 0; j < groupSize; j++) {
                            int currentVal = list.get(i * groupSize + j);
                            int nextVal = list.get((i + 1) * groupSize + j);
                            if (currentVal < nextVal) {
                                inOrder = true;
                                break;
                            } else if (currentVal > nextVal) {
                                Assert.fail(String.format(
                                        "Groups out of order at group %d (size %d, groupSize %d)",
                                        i, size, groupSize
                                ));
                            }
                        }
                        if (!inOrder) {
                            // All elements are equal, which is fine
                            for (int j = 0; j < groupSize; j++) {
                                Assert.assertEquals(
                                        list.get(i * groupSize + j),
                                        list.get((i + 1) * groupSize + j)
                                );
                            }
                        }
                    }
                } catch (StackOverflowError e) {
                    Assert.fail(String.format(
                            "StackOverflowError with size %d and groupSize %d",
                            size, groupSize
                    ));
                }
            }
        }
    }

    @Test
    public void testSortGroupsLastElementNotPivot() {
        // Test to verify the partition logic handles boundaries correctly
        // The algorithm uses high as exclusive, so pivotIndex = high - 1 is correct

        int groupSize = 1;
        IntList list = new IntList();

        // Simple case: sort single integers
        list.add(5);  // Group 0
        list.add(2);  // Group 1
        list.add(8);  // Group 2
        list.add(1);  // Group 3
        list.add(3);  // Group 4

        list.sortGroups(groupSize);

        // Verify correct sorting
        Assert.assertEquals(1, list.get(0));
        Assert.assertEquals(2, list.get(1));
        Assert.assertEquals(3, list.get(2));
        Assert.assertEquals(5, list.get(3));
        Assert.assertEquals(8, list.get(4));

        // Additional verification - check all elements are in order
        for (int i = 0; i < list.size() - 1; i++) {
            Assert.assertTrue("Elements should be in ascending order",
                    list.get(i) <= list.get(i + 1));
        }
    }

    @Test
    public void testSortGroupsNotSupported() {
        IntList l = new IntList();

        l.add(1);
        try {
            l.sortGroups(2);
            Assert.fail();
        } catch (IllegalStateException e) {
            // expected
        }
        l.sortGroups(1);

        try {
            l.sortGroups(-1);
            Assert.fail();
        } catch (IllegalStateException e) {
            // expected
        }
    }

    @Test
    public void testSortGroupsPartitionBoundsIssue() {
        // This test demonstrates the off-by-one error in partition bounds
        // The bug is that pivotIndex = high - 1, but the code treats high as exclusive
        // This causes the last group to potentially not be sorted correctly

        int groupSize = 2;
        IntList list = new IntList();

        // Create a specific pattern that exposes the boundary issue
        // Groups: [3,1], [1,2], [2,3], [4,4]
        // The last group [4,4] might not participate correctly in partitioning
        list.add(3);
        list.add(1);  // Group 0
        list.add(1);
        list.add(2);  // Group 1
        list.add(2);
        list.add(3);  // Group 2
        list.add(4);
        list.add(4);  // Group 3 - this group may be mishandled

        // Make a copy for comparison
        IntList expected = new IntList();
        expected.add(1);
        expected.add(2);  // Group 1 sorted first
        expected.add(2);
        expected.add(3);  // Group 2
        expected.add(3);
        expected.add(1);  // Group 0
        expected.add(4);
        expected.add(4);  // Group 3

        list.sortGroups(groupSize);

        // Verify all groups are properly sorted
        // The bug would cause the last group to not be properly considered
        for (int i = 0; i < list.size(); i++) {
            Assert.assertEquals("Element at position " + i + " should match expected",
                    expected.get(i), list.get(i));
        }
    }

    @Test
    public void testSortGroupsPartitionCorrectness() {
        // More comprehensive test for partition correctness
        // Tests if the partition function correctly divides elements around pivot

        int groupSize = 2;
        IntList list = new IntList();

        // Groups that will test partition boundaries
        // Groups: [5,1], [2,2], [8,3], [1,0], [6,4], [3,3]
        list.add(5);
        list.add(1);  // Group 0
        list.add(2);
        list.add(2);  // Group 1
        list.add(8);
        list.add(3);  // Group 2
        list.add(1);
        list.add(0);  // Group 3
        list.add(6);
        list.add(4);  // Group 4
        list.add(3);
        list.add(3);  // Group 5 - will be pivot in first call

        list.sortGroups(groupSize);

        // Verify groups are sorted correctly
        // Expected order: [1,0], [2,2], [3,3], [5,1], [6,4], [8,3]
        Assert.assertEquals(1, list.get(0));
        Assert.assertEquals(0, list.get(1));
        Assert.assertEquals(2, list.get(2));
        Assert.assertEquals(2, list.get(3));
        Assert.assertEquals(3, list.get(4));
        Assert.assertEquals(3, list.get(5));
        Assert.assertEquals(5, list.get(6));
        Assert.assertEquals(1, list.get(7));
        Assert.assertEquals(6, list.get(8));
        Assert.assertEquals(4, list.get(9));
        Assert.assertEquals(8, list.get(10));
        Assert.assertEquals(3, list.get(11));
    }

    @Test
    public void testSortGroupsPartitionLoopBoundary() {
        // Test the partition loop boundary: for (int j = low; j < high; j++)
        // The claimed bug: when j reaches high-1 (the pivot index), we're comparing pivot with itself
        // Let's create a scenario where this self-comparison could cause issues

        int groupSize = 1;
        IntList list = new IntList();

        // Specific pattern to test partition behavior
        // If pivot is compared with itself, it might affect the partition point
        list.add(3);
        list.add(1);
        list.add(4);
        list.add(2);  // This will be the pivot in first partition

        list.sortGroups(groupSize);

        // Verify correct sorting
        Assert.assertEquals(1, list.get(0));
        Assert.assertEquals(2, list.get(1));
        Assert.assertEquals(3, list.get(2));
        Assert.assertEquals(4, list.get(3));
    }

    @Test
    public void testSortGroupsPivotComparisonBug() {
        // This test checks if the partition function correctly handles the pivot
        // The potential bug: loop goes j < high, but pivotIndex = high - 1
        // This means when j = high - 1, we compare pivot with itself

        int groupSize = 1;
        IntList list = new IntList();

        // Create a pattern where self-comparison would matter
        // All elements same except last
        list.add(5);
        list.add(5);
        list.add(5);
        list.add(5);
        list.add(1); // Different last element

        list.sortGroups(groupSize);

        // Should be sorted as [1, 5, 5, 5, 5]
        Assert.assertEquals(1, list.get(0));
        for (int i = 1; i < 5; i++) {
            Assert.assertEquals(5, list.get(i));
        }
    }

    @Test
    public void testSortGroupsStackOverflowScenario() {
        // Create a scenario that would trigger deep recursion in quicksort
        // by creating reverse sorted data which causes worst-case O(n) recursion depth
        int groupSize = 3;
        int numGroups = 30000; // Large size to ensure stack overflow with recursive implementation

        IntList list = newIntList(numGroups, groupSize);

        // Verify the result is correctly sorted
        for (int i = 0; i < numGroups - 1; i++) {
            int baseIndex = i * groupSize;
            int nextBaseIndex = (i + 1) * groupSize;

            // Compare first elements of consecutive groups - should be in ascending order
            Assert.assertTrue("Groups should be sorted in ascending order",
                    list.getQuick(baseIndex) <= list.getQuick(nextBaseIndex));
        }
    }

    private static void checkSortGroupsEqualElements(int n, int elements, Rnd rnd) {
        IntList list = new IntList(elements * n);
        int[][] arrays = new int[elements][];

        for (int i = 0; i < elements; i++) {
            arrays[i] = new int[n];
            int equalValue = rnd.nextInt(5);
            for (int j = 0; j < n; j++) {
                arrays[i][j] = equalValue;
                list.add(arrays[i][j]);
            }
        }

        sortAndCompare(n, elements, arrays, list);
    }

    private static @NotNull IntList newIntList(int numGroups, int groupSize) {
        IntList list = new IntList(numGroups * groupSize);

        // Add groups in REVERSE sorted order to trigger worst-case quicksort behavior
        // This will cause the pivot (last element) to always be the smallest, leading to maximum recursion
        for (int i = numGroups - 1; i >= 0; i--) {
            list.add(i);           // First element of group (descending)
            list.add(i * 2);       // Second element
            list.add(i * 3);       // Third element
        }

        // This should not throw StackOverflowError with the fixed implementation
        list.sortGroups(groupSize);
        return list;
    }

    private static void sortAndCompare(int n, int elements, int[][] arrays, IntList list) {
        Arrays.sort(arrays, (int[] a, int[] b) -> {
            for (int i = 0; i < n; i++) {
                int comparison = Integer.compare(a[i], b[i]);
                if (comparison != 0) {
                    return comparison;
                }
            }
            return 0;
        });

        list.sortGroups(n);

        for (int i = 0; i < elements; i++) {
            for (int j = 0; j < n; j++) {
                Assert.assertEquals(arrays[i][j], list.get(i * n + j));
            }
        }
    }

    private static void sortGroupsRandom(int n, int elements, Rnd rnd) {
        IntList list = new IntList(elements * n);
        int[][] arrays = new int[elements][];

        for (int i = 0; i < elements; i++) {
            arrays[i] = new int[n];
            for (int j = 0; j < n; j++) {
                arrays[i][j] = rnd.nextInt();
                list.add(arrays[i][j]);
            }
        }

        sortAndCompare(n, elements, arrays, list);
    }

    private void testBinarySearchFuzz0(int N, int skipRate) {
        final Rnd rnd = new Rnd();
        final IntList list = new IntList();
        final IntList skipList = new IntList();
        for (int i = 0; i < N; i++) {
            // not skipping ?
            if (skipRate == 0 || rnd.nextInt(skipRate) != 0) {
                list.add(i);
                skipList.add(1);
            } else {
                skipList.add(0);
            }
        }

        // test scan UP
        final int M = list.size();

        for (int i = 0; i < N; i++) {
            int pos = list.binarySearchUniqueList(i);
            int skip = skipList.getQuick(i);

            // the value was skipped
            if (skip == 0) {
                Assert.assertTrue(pos < 0);

                pos = -pos - 1;
                if (pos > 0) {
                    Assert.assertTrue(list.getQuick(pos - 1) < i);
                }

                if (pos < M) {
                    Assert.assertTrue(list.getQuick(pos) > i);
                }
            } else {
                Assert.assertTrue(pos > -1);
                if (pos > 0) {
                    Assert.assertTrue(list.getQuick(pos - 1) < i);
                }
                Assert.assertEquals(list.getQuick(pos), i);
            }
        }


        // search max value (greater than anything in the list)

        int pos = list.binarySearchUniqueList(N);
        Assert.assertTrue(pos < 0);

        pos = -pos - 1;
        Assert.assertEquals(pos, list.size());

        // search min value (less than anything in the list)

        pos = list.binarySearchUniqueList(-1);
        Assert.assertTrue(pos < 0);

        pos = -pos - 1;
        Assert.assertEquals(0, pos);
    }
}
