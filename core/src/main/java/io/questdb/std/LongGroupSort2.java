/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
 * Sort an long array which is actually intrusively storing a group (tuple) of N longs.
 * The group is considered ordered (for sorting purposes) from most significant int to least significant.
 */
public class LongGroupSort2 {
    // Partition function for QuickSort
    private static int partitionCompareByElementImpl(long[] array, int compareElementByIndex, int low, int high, int n) {
        int pivotIndex = high - 1;
        int i = low - 1;

        int pivotElementIndex = pivotIndex * n + compareElementByIndex;
        for (int j = low; j < high; j++) {
            if (array[j * n + compareElementByIndex] < array[pivotElementIndex]) {
                swapGroups(array, ++i, j, n);
            }
        }

        swapGroups(array, ++i, pivotIndex, n);
        return i;
    }

    private static void quickSortIterativeInPlace(long[] array, int compareElementByIndex, int low, int high, int n) {
        while (low + 1 < high) {
            int pi = partitionCompareByElementImpl(array, compareElementByIndex, low, high, n);

            // Process smaller partition first (tail-recursive elimination)
            if (pi - low < high - (pi + 1)) {
                quickSortIterativeInPlace(array, compareElementByIndex, low, pi, n);
                low = pi + 1; // Continue sorting the larger part in the loop
            } else {
                quickSortIterativeInPlace(array, compareElementByIndex, pi + 1, high, n);
                high = pi; // Continue sorting the smaller part in the loop
            }
        }
    }

    // Swap function to exchange groups
    private static void swapGroups(long[] array, int i, int j, int n) {
        if (i != j) {
            for (int k = 0; k < n; k++) {
                long temp = array[i * n + k];
                array[i * n + k] = array[j * n + k];
                array[j * n + k] = temp;
            }
        }
    }

    /**
     * Sorts a long array that stores a group (tuple) of N longs.
     *
     * @param n                     Number of longs in a group.
     * @param compareByElementIndex Index of the element in the group to compare by.
     * @param array                 Array to sort.
     * @param groupLo               Start index of the group.
     * @param groupHi               End index of the group.
     */
    static void quickSort(int n, int compareByElementIndex, long[] array, int groupLo, int groupHi) {
        assert groupHi >= groupLo;
        quickSortIterativeInPlace(array, compareByElementIndex, groupLo, groupHi, n);
    }
}

