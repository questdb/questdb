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
public class LongGroupSort {

    // Compare two groups of 3 elements each
    private static int compareGroups(long[] array, int i, int j, int n) {
        for (int k = 0; k < n; k++) {
            int comparison = Long.compare(array[i * n + k], array[j * n + k]);
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    // Partition function for QuickSort
    private static int partitionCompareAllImpl(long[] array, int low, int high, int n) {
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

    // Partition function for QuickSort
    private static int partitionCompareBy2ElementsImpl(long[] array, int compareElementByIndex1, int compareElementByIndex2, int low, int high, int n) {
        int pivotIndex = high - 1;
        int i = low - 1;

        int pivotElementIndex1 = pivotIndex * n + compareElementByIndex1;
        int pivotElementIndex2 = pivotIndex * n + compareElementByIndex2;

        for (int j = low; j < high; j++) {
            if (array[j * n + compareElementByIndex1] < array[pivotElementIndex1]) {
                swapGroups(array, ++i, j, n);
            } else if (array[j * n + compareElementByIndex1] == array[pivotElementIndex1]
                    && array[j * n + compareElementByIndex2] < array[pivotElementIndex2]) {
                swapGroups(array, ++i, j, n);
            }
        }

        swapGroups(array, ++i, pivotIndex, n);
        return i;
    }

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

    private static void quickSortCompareAllImpl(long[] array, int low, int high, int n) {
        if (low + 1 < high) {
            int pi = partitionCompareAllImpl(array, low, high, n);

            quickSortCompareAllImpl(array, low, pi, n);  // Before pi
            quickSortCompareAllImpl(array, pi + 1, high, n); // After pi
        }
    }


    private static void quickSortCompareBy2ElementsImpl(long[] array, int compareElementByIndex1, int compareElementByIndex2, int low, int high, int n) {
        if (low + 1 < high) {
            int pi = partitionCompareBy2ElementsImpl(array, compareElementByIndex1, compareElementByIndex2, low, high, n);

            quickSortCompareBy2ElementsImpl(array, compareElementByIndex1, compareElementByIndex2, low, pi, n);  // Before pi
            quickSortCompareBy2ElementsImpl(array, compareElementByIndex1, compareElementByIndex2, pi + 1, high, n); // After pi
        }
    }


    private static void quickSortCompareByElementImpl(long[] array, int compareElementByIndex, int low, int high, int n) {
        while (low + 1 < high) {
            int pi = partitionCompareByElementImpl(array, compareElementByIndex, low, high, n);

            // Process smaller partition first (tail-recursive elimination)
            if (pi - low < high - (pi + 1)) {
                quickSortCompareByElementImpl(array, compareElementByIndex, low, pi, n);
                low = pi + 1; // Continue sorting the larger part in the loop
            } else {
                quickSortCompareByElementImpl(array, compareElementByIndex, pi + 1, high, n);
                high = pi; // Continue sorting the smaller part in the loop
            }
        }
    }

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
     * Sort an long array which is actually intrusively storing a group (tuple) of N longs.
     *
     * @param n       number of longs in a group
     * @param array   array to sort
     * @param groupLo start index of the group
     * @param groupHi end index of the group
     */
    static void quickSort(int n, long[] array, int groupLo, int groupHi) {
        assert groupHi >= groupLo;
        quickSortCompareAllImpl(array, groupLo, groupHi, n);
    }

    /**
     * Sort an long array which is actually intrusively storing a group (tuple) of N longs.
     *
     * @param n                     number of longs in a group
     * @param compareByElementIndex index of the element in the group to compare by
     * @param array                 array to sort
     * @param groupLo               start index of the group
     * @param groupHi               end index of the group
     */
    static void quickSort(int n, int compareByElementIndex, long[] array, int groupLo, int groupHi) {
        assert groupHi >= groupLo;
        quickSortCompareByElementImpl(array, compareByElementIndex, groupLo, groupHi, n);
    }

    /**
     * Sort an long array which is actually intrusively storing a group (tuple) of N longs.
     *
     * @param n                      number of longs in a group
     * @param compareByElementIndex1 index of the element 1 in the group to compare by
     * @param compareByElementIndex2 index of the element 2 in the group to compare by
     * @param array                  array to sort
     * @param groupLo                start index of the group
     * @param groupHi                end index of the group
     */
    static void quickSort(int n, int compareByElementIndex1, int compareByElementIndex2, long[] array, int groupLo, int groupHi) {
        assert groupHi >= groupLo;
        quickSortCompareBy2ElementsImpl(array, compareByElementIndex1, compareByElementIndex2, groupLo, groupHi, n);
    }
}

