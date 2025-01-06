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

    private static void quickSortImpl(int[] array, int low, int high, int n) {
        if (low + 1 < high) {
            int pi = partition(array, low, high, n);

            quickSortImpl(array, low, pi, n);  // Before pi
            quickSortImpl(array, pi + 1, high, n); // After pi
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
     * Sort an integer array which is actually intrusively storing a group (tuple) of N integers.
     *
     * @param n       number of integers in a group
     * @param array   array to sort
     * @param groupLo start index of the group
     * @param groupHi end index of the group
     */
    static void quickSort(int n, int[] array, int groupLo, int groupHi) {
        assert groupHi >= groupLo;
        quickSortImpl(array, groupLo, groupHi, n);
    }
}

