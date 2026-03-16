/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__\__|____/|____/
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

import io.questdb.cairo.ColumnType;

public class MultiKeySort {

    private static final int INSERTION_SORT_THRESHOLD = 47;
    private static final int MAX_RUN_COUNT = 67;
    private static final int QUICKSORT_THRESHOLD = 286;
    private static final int MAX_RUN_LENGTH = 33;

    private final int[] secondaryIndices;
    private final int[] columnTypes;
    private final long timestampOffset;
    private final long rowBlockSize;
    private final RowBlockReader reader;

    public MultiKeySort(
            int[] secondaryColumnIndices,
            int[] columnTypes,
            long timestampOffset,
            long rowBlockSize,
            RowBlockReader reader
    ) {
        this.secondaryIndices = secondaryColumnIndices;
        this.columnTypes = columnTypes;
        this.timestampOffset = timestampOffset;
        this.rowBlockSize = rowBlockSize;
        this.reader = reader;
    }

    public interface RowBlockReader {
        long get(long rowAddress, int columnIndex, long offset);
        int getInt(long rowAddress, int columnIndex, long offset);
        long getLong(long rowAddress, int columnIndex, long offset);
        double getDouble(long rowAddress, int columnIndex, long offset);
        int getSymbol(long rowAddress, int columnIndex, long offset);
        int getVarcharHash(long rowAddress, int columnIndex, long offset);
    }

    public void sort(long partitionDataAddress, int n) {
        if (n <= 1) {
            return;
        }

        if (n < QUICKSORT_THRESHOLD) {
            quickSort(partitionDataAddress, 0, n - 1);
        } else {
            mergeSort(partitionDataAddress, n);
        }
    }

    private void quickSort(long dataAddress, int left, int right) {
        if (right - left < QUICKSORT_THRESHOLD) {
            insertionSort(dataAddress, left, right);
            return;
        }

        int[] run = new int[MAX_RUN_COUNT + 1];
        int count = 0;
        run[0] = left;

        for (int k = left; k < right; run[count] = k) {
            if (compareRows(dataAddress, k, k + 1) < 0) {
                while (++k <= right && compareRows(dataAddress, k - 1, k) <= 0) ;
            } else if (compareRows(dataAddress, k, k + 1) > 0) {
                for (int lo = run[count] - 1, hi = k; ++lo < --hi; ) {
                    swapRows(dataAddress, lo, hi);
                }
            } else {
                for (int m = MAX_RUN_LENGTH; ++k <= right && compareRows(dataAddress, k - 1, k) == 0; ) {
                    if (--m == 0) {
                        quickSort(dataAddress, left, right);
                        return;
                    }
                }
            }

            if (++count == MAX_RUN_COUNT) {
                quickSort(dataAddress, left, right);
                return;
            }
        }

        if (run[count] == right++) {
            run[++count] = right;
        } else if (count == 1) {
            return;
        }

        long[] tempIndices = new long[right - left + 1];
        long[] indices = new long[right - left + 1];
        
        for (int i = 0; i < right - left + 1; i++) {
            indices[i] = left + i;
        }

        int width = 1;
        while (width < right - left + 1) {
            for (int i = left; i <= right; i += 2 * width) {
                int start1 = i;
                int end1 = Math.min(i + width - 1, right);
                int start2 = Math.min(end1 + 1, right);
                int end2 = Math.min(i + 2 * width - 1, right);

                if (start2 <= right) {
                    mergeRange(indices, tempIndices, start1, end1, start2, end2, dataAddress);
                }
            }
            long[] temp = indices;
            indices = tempIndices;
            tempIndices = temp;
            width *= 2;
        }
    }

    private void mergeRange(long[] indices, long[] temp, int start1, int end1, int start2, int end2, long dataAddress) {
        int i = start1;
        int j = start2;
        int k = start1;

        while (i <= end1 && j <= end2) {
            if (compareRows(dataAddress, (int) indices[i], (int) indices[j]) <= 0) {
                temp[k++] = indices[i++];
            } else {
                temp[k++] = indices[j++];
            }
        }

        while (i <= end1) {
            temp[k++] = indices[i++];
        }

        while (j <= end2) {
            temp[k++] = indices[j++];
        }

        System.arraycopy(temp, start1, indices, start1, end2 - start1 + 1);
    }

    private void insertionSort(long dataAddress, int left, int right) {
        for (int i = left, j = i; i < right; j = ++i) {
            long ai = dataAddress + (long) (i + 1) * rowBlockSize;
            while (compareRows(dataAddress, ai, dataAddress + (long) j * rowBlockSize) < 0) {
                swapRows(dataAddress, j + 1, j);
                if (j-- == left) {
                    break;
                }
            }
        }
    }

    private void mergeSort(long dataAddress, int n) {
        long[] indices = new long[n];
        long[] tempIndices = new long[n];

        for (int i = 0; i < n; i++) {
            indices[i] = i;
        }

        int width = 1;
        while (width < n) {
            for (int i = 0; i < n; i += 2 * width) {
                int left = i;
                int mid = Math.min(i + width, n);
                int right = Math.min(i + 2 * width, n);
                merge(indices, tempIndices, left, mid, right, dataAddress);
            }
            long[] temp = indices;
            indices = tempIndices;
            tempIndices = temp;
            width *= 2;
        }

        if (indices != tempIndices) {
            System.arraycopy(indices, 0, tempIndices, 0, n);
        }
    }

    private void merge(long[] indices, long[] temp, int left, int mid, int right, long dataAddress) {
        int i = left;
        int j = mid;
        int k = left;

        while (i < mid && j < right) {
            if (compareRowsByIndex(dataAddress, indices[i], indices[j]) <= 0) {
                temp[k++] = indices[i++];
            } else {
                temp[k++] = indices[j++];
            }
        }

        while (i < mid) {
            temp[k++] = indices[i++];
        }

        while (j < right) {
            temp[k++] = indices[j++];
        }

        System.arraycopy(temp, left, indices, left, right - left);
    }

    private int compareRows(long dataAddress, int row1, int row2) {
        long addr1 = dataAddress + (long) row1 * rowBlockSize;
        long addr2 = dataAddress + (long) row2 * rowBlockSize;
        return compareRows(dataAddress, addr1, addr2);
    }

    private int compareRows(long dataAddress, long addr1, long addr2) {
        int cmp = Long.compare(
                reader.getLong(addr1, 0, timestampOffset),
                reader.getLong(addr2, 0, timestampOffset)
        );
        if (cmp != 0) {
            return cmp;
        }

        for (int i = 0; i < secondaryIndices.length; i++) {
            int colIdx = secondaryIndices[i];
            int type = columnTypes[colIdx];

            cmp = compareColumn(addr1, addr2, colIdx, type);
            if (cmp != 0) {
                return cmp;
            }
        }

        return 0;
    }

    private int compareRowsByIndex(long dataAddress, long idx1, long idx2) {
        long addr1 = dataAddress + idx1 * rowBlockSize;
        long addr2 = dataAddress + idx2 * rowBlockSize;
        return compareRows(dataAddress, addr1, addr2);
    }

    private int compareColumn(long addr1, long addr2, int columnIndex, int type) {
        long offset = columnIndex * rowBlockSize;

        switch (type) {
            case ColumnType.INT:
                return Integer.compare(
                        reader.getInt(addr1, columnIndex, offset),
                        reader.getInt(addr2, columnIndex, offset)
                );
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP:
                return Long.compare(
                        reader.getLong(addr1, columnIndex, offset),
                        reader.getLong(addr2, columnIndex, offset)
                );
            case ColumnType.DOUBLE:
            case ColumnType.FLOAT:
                return Double.compare(
                        reader.getDouble(addr1, columnIndex, offset),
                        reader.getDouble(addr2, columnIndex, offset)
                );
            case ColumnType.SYMBOL:
                return Integer.compare(
                        reader.getSymbol(addr1, columnIndex, offset),
                        reader.getSymbol(addr2, columnIndex, offset)
                );
            case ColumnType.STRING:
            case ColumnType.VARCHAR:
                return Integer.compare(
                        reader.getVarcharHash(addr1, columnIndex, offset),
                        reader.getVarcharHash(addr2, columnIndex, offset)
                );
            default:
                return 0;
        }
    }

    private void swapRows(long dataAddress, int i, int j) {
        long rowI = dataAddress + (long) i * rowBlockSize;
        long rowJ = dataAddress + (long) j * rowBlockSize;

        int numCols = columnTypes.length;
        for (int c = 0; c < numCols; c++) {
            int type = columnTypes[c];
            long offset = c * rowBlockSize;

            switch (type) {
                case ColumnType.INT: {
                    int valI = reader.getInt(rowI, c, offset);
                    int valJ = reader.getInt(rowJ, c, offset);
                    writeInt(rowI, c, offset, valJ);
                    writeInt(rowJ, c, offset, valI);
                    break;
                }
                case ColumnType.LONG:
                case ColumnType.DATE:
                case ColumnType.TIMESTAMP: {
                    long valI = reader.getLong(rowI, c, offset);
                    long valJ = reader.getLong(rowJ, c, offset);
                    writeLong(rowI, c, offset, valJ);
                    writeLong(rowJ, c, offset, valI);
                    break;
                }
                case ColumnType.DOUBLE:
                case ColumnType.FLOAT: {
                    double valI = reader.getDouble(rowI, c, offset);
                    double valJ = reader.getDouble(rowJ, c, offset);
                    writeDouble(rowI, c, offset, valJ);
                    writeDouble(rowJ, c, offset, valI);
                    break;
                }
                case ColumnType.SYMBOL: {
                    int valI = reader.getSymbol(rowI, c, offset);
                    int valJ = reader.getSymbol(rowJ, c, offset);
                    writeSymbol(rowI, c, offset, valJ);
                    writeSymbol(rowJ, c, offset, valI);
                    break;
                }
                case ColumnType.STRING:
                case ColumnType.VARCHAR: {
                    int hashI = reader.getVarcharHash(rowI, c, offset);
                    int hashJ = reader.getVarcharHash(rowJ, c, offset);
                    writeVarcharHash(rowI, c, offset, hashJ);
                    writeVarcharHash(rowJ, c, offset, hashI);
                    break;
                }
            }
        }
    }

    private void writeInt(long rowAddress, int columnIndex, long offset, int value) {
        Unsafe.getUnsafe().putInt(rowAddress + offset, value);
    }

    private void writeLong(long rowAddress, int columnIndex, long offset, long value) {
        Unsafe.getUnsafe().putLong(rowAddress + offset, value);
    }

    private void writeDouble(long rowAddress, int columnIndex, long offset, double value) {
        Unsafe.getUnsafe().putDouble(rowAddress + offset, value);
    }

    private void writeSymbol(long rowAddress, int columnIndex, long offset, int value) {
        Unsafe.getUnsafe().putInt(rowAddress + offset, value);
    }

    private void writeVarcharHash(long rowAddress, int columnIndex, long offset, int value) {
        Unsafe.getUnsafe().putInt(rowAddress + offset, value);
    }
}