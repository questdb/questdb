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

public class Int3Sort {

    private static final int N = 3;

    public static void quickSort(int[] array, int low, int high) {
        if (low + 1 < high) {
            int pi = partition(array, low, high);

            quickSort(array, low, pi);  // Before pi
            quickSort(array, pi + 1, high); // After pi
        }
    }

    // Compare two groups of 3 elements each
    private static int compareGroups(int[] array, int i, int j) {
        for (int k = 0; k < N; k++) {
            int comparison = Integer.compare(array[i * N + k], array[j * N + k]);
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }

    // Partition function for QuickSort
    private static int partition(int[] array, int low, int high) {
        int pivotIndex = high - 1;
        int i = low - 1;

        for (int j = low; j < high; j++) {
            if (compareGroups(array, j, pivotIndex) < 0) {
                swapGroups(array, ++i, j);
            }
        }

        swapGroups(array, ++i, pivotIndex);
        return i;
    }

    // Swap two groups of 3 elements each
    private static void swapGroups(int[] array, int i, int j) {
        if (i != j) {
            for (int k = 0; k < N; k++) {
                int temp = array[i * N + k];
                array[i * N + k] = array[j * N + k];
                array[j * N + k] = temp;
            }
        }
    }
}
