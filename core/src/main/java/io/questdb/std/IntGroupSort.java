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

package io.questdb.std;

/**
 * Sort an integer array which is actually intrusively storing a group (tuple) of N integers.
 * The group is considered ordered (for sorting purposes) from most significant int to least significant.
 */
public class IntGroupSort {

    // Compare two groups of 3 elements each
    private static int compareGroups(int[] array, int i, int j, int n) {
        for (int k = 0; k < n; k++) {
            int comparison = Integer.compare(array[i * n + k], array[j * n + k]);
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    // Partition function for QuickSort
    private static int partition(int[] array, int low, int high, int n) {
        int pivotIndex = high - 1;
        int i = low - 1;

        for (int j = low; j < high; j++) {
            if (compareGroups(array, j, pivotIndex, n) < 0) {
                swapGroups(array, ++i, j, n);
            }
        }

        swapGroups(array, ++i, pivotIndex, n);
        return i;
    }

    private static void quickSortImpl(IntList intList, int low, int high, int n) {
        // Use the tail of the IntList (unused capacity) as a stack to avoid StackOverflowError
        // Stack stores pairs of (low, high) bounds for segments to sort

        // Stack grows from the end of the data
        int stackStart = intList.size();
        int stackPos = stackStart;

        //   Quicksort Stack Depth Analysis
        //
        //  Best/Average Case: O(log n)
        //  - With good pivot selection, each partition roughly halves the data
        //  - For n elements: log₂(n) recursion levels
        //  - Each level needs 2 stack entries (low, high)
        //  - So stack size ≈ 2 × log₂(n)
        //
        //  Worst Case: O(n)
        // But our algorithm uses a key optimization: "process smaller partition immediately"
        // We push the larger partition first (defer it), then smaller partition last (process next)
        // This ensures the stack depth stays logarithmic even in worst case!
        //
        //  Concrete Examples:
        //
        //  | Data Size     | log₂(n) | Stack Pairs Needed | Stack Entries (×2) |
        //  |---------------|---------|--------------------|--------------------|
        //  | 1,000         | ~10     | ~10                | ~20                |
        //  | 1,000,000     | ~20     | ~20                | ~40                |
        //  | 1,000,000,000 | ~30     | ~30                | ~60                |
        //
        //  Why 64 is Safe:
        //
        //  - 64 ÷ 2 = 32 partition pairs
        //  - This handles up to 2³² = 4+ billion elements
        int[] array = intList.resetCapacityInternal(stackPos + 64); // Ensures that we have enough space

        try {
            // Push initial range onto stack
            array[stackPos++] = low;
            array[stackPos++] = high;

            while (stackPos > stackStart) {
                // Pop range from stack
                high = array[--stackPos];
                low = array[--stackPos];

                if (low + 1 < high) {
                    int pi = partition(array, low, high, n);

                    // Push the larger partition first, then smaller one
                    // This ensures the stack depth remains logarithmic
                    if (pi - low > high - pi - 1) {
                        // Left partition is larger
                        array[stackPos++] = low;
                        array[stackPos++] = pi;
                        array[stackPos++] = pi + 1;
                        array[stackPos++] = high;
                    } else {
                        // Right partition is larger or equal
                        array[stackPos++] = pi + 1;
                        array[stackPos++] = high;
                        array[stackPos++] = low;
                        array[stackPos++] = pi;
                    }
                }
            }
        } finally {
            // Restore original size of IntList
            intList.setPos(stackStart);
        }
    }

    // Swap two groups of 3 elements each
    private static void swapGroups(int[] array, int i, int j, int n) {
        if (i != j) {
            for (int k = 0; k < n; k++) {
                int temp = array[i * n + k];
                array[i * n + k] = array[j * n + k];
                array[j * n + k] = temp;
            }
        }
    }

    /**
     * Sort an integer list which is actually intrusively storing a group (tuple) of N integers.
     * Uses the unused capacity of the IntList as temporary stack space to avoid StackOverflowError.
     *
     * @param n       number of integers in a group
     * @param intList IntList containing the data to sort
     * @param groupLo start index of the group
     * @param groupHi end index of the group
     */
    static void quickSort(int n, IntList intList, int groupLo, int groupHi) {
        assert groupHi >= groupLo;
        quickSortImpl(intList, groupLo, groupHi, n);
    }
}

