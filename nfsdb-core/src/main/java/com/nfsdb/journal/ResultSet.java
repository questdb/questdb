/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal;

import com.nfsdb.journal.column.SymbolTable;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.iterators.ParallelIterator;
import com.nfsdb.journal.iterators.ResultSetBufferedIterator;
import com.nfsdb.journal.iterators.ResultSetIterator;
import com.nfsdb.journal.iterators.ResultSetParallelIterator;
import com.nfsdb.journal.utils.Rows;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;

import java.util.Iterator;
import java.util.Random;

public class ResultSet<T> implements Iterable<T> {
    private final Journal<T> journal;
    private final TLongList rowIDs;

    public T[] read() throws JournalException {
        return journal.read(rowIDs);
    }

    public void read(int index, T obj) throws JournalException {
        journal.read(rowIDs.get(index), obj);
    }

    public Journal<T> getJournal() {
        return journal;
    }

    public long getRowID(int index) {
        return rowIDs.get(index);
    }

    public ResultSet<T> sort(String... columnNames) throws JournalException {
        return sort(Order.ASC, getColumnIndexes(columnNames));
    }

    public ResultSet<T> sort(Order order, String... columnNames) throws JournalException {
        return sort(order, getColumnIndexes(columnNames));
    }

    public ResultSet<T> sort(Order order, int... columnIndices) throws JournalException {
        if (size() > 0) {
            quickSort(order, 0, size() - 1, columnIndices);
        }
        return this;
    }

    public ResultSet<T> sort() {
        rowIDs.sort();
        return this;
    }

    public long[] readTimestamps() throws JournalException {
        int timestampColIndex = journal.getMetadata().getTimestampColumnIndex();
        long[] result = new long[size()];

        for (int i = 0, rowIDsLength = rowIDs.size(); i < rowIDsLength; i++) {
            result[i] = getLong(i, timestampColIndex);
        }
        return result;
    }

    public int size() {
        return rowIDs.size();
    }

    public long getLong(int rsIndex, int columnIndex) throws JournalException {
        long rowID = rowIDs.get(rsIndex);
        return journal.getPartition(Rows.toPartitionIndex(rowID), true).getLong(Rows.toLocalRowID(rowID), columnIndex);
    }

    public long getLong(int rsIndex, int columnIndex, long defaultValue) throws JournalException {
        long rowID = rowIDs.get(rsIndex);
        return journal.getPartition(Rows.toPartitionIndex(rowID), true).getLong(Rows.toLocalRowID(rowID), columnIndex, defaultValue);
    }

    public long getInt(int rsIndex, int columnIndex) throws JournalException {
        long rowID = rowIDs.get(rsIndex);
        return journal.getPartition(Rows.toPartitionIndex(rowID), true).getInt(Rows.toLocalRowID(rowID), columnIndex);
    }

    public long getInt(int rsIndex, int columnIndex, int defaultValue) throws JournalException {
        long rowID = rowIDs.get(rsIndex);
        return journal.getPartition(Rows.toPartitionIndex(rowID), true).getInt(Rows.toLocalRowID(rowID), columnIndex, defaultValue);
    }

    public String getString(int rsIndex, int columnIndex) throws JournalException {
        long rowID = rowIDs.get(rsIndex);
        return journal.getPartition(Rows.toPartitionIndex(rowID), true).getString(Rows.toLocalRowID(rowID), columnIndex);
    }

    public String getSymbol(int rsIndex, int columnIndex) throws JournalException {
        long rowID = rowIDs.get(rsIndex);
        return journal.getPartition(Rows.toPartitionIndex(rowID), true).getSymbol(Rows.toLocalRowID(rowID), columnIndex);
    }

    public boolean isNull(int rsIndex, int columnIndex) throws JournalException {
        long rowID = rowIDs.get(rsIndex);
        return journal.getPartition(Rows.toPartitionIndex(rowID), true).isNull(Rows.toLocalRowID(rowID), columnIndex);
    }

    public double getDouble(int rsIndex, int columnIndex) throws JournalException {
        long rowID = rowIDs.get(rsIndex);
        return journal.getPartition(Rows.toPartitionIndex(rowID), true).getDouble(Rows.toLocalRowID(rowID), columnIndex);
    }

    public double getDouble(int rsIndex, int columnIndex, double defaultValue) throws JournalException {
        long rowID = rowIDs.get(rsIndex);
        return journal.getPartition(Rows.toPartitionIndex(rowID), true).getDouble(Rows.toLocalRowID(rowID), columnIndex, defaultValue);
    }

    @Override
    public Iterator<T> iterator() {
        return new ResultSetIterator<>(this);
    }

    public ResultSetBufferedIterator<T> bufferedIterator() {
        return new ResultSetBufferedIterator<>(this);
    }

    public ParallelIterator<T> parallelIterator() {
        return parallelIterator(1024);
    }

    public ParallelIterator<T> parallelIterator(int bufferSize) {
        return new ResultSetParallelIterator<>(this, bufferSize);
    }

    public T readFirst() throws JournalException {
        return size() > 0 ? read(0) : null;
    }

    public T read(int rsIndex) throws JournalException {
        return journal.read(rowIDs.get(rsIndex));
    }

    public T readLast() throws JournalException {
        return size() > 0 ? read(size() - 1) : null;
    }

    /**
     * Creates subset of ResultSet by result set row numbers.
     *
     * @param lo low end point of result set (inclusive)
     * @param hi high end point of result set (exclusive)
     * @return a subset of result set from lo (inclusive) to hi (exclusive)
     */
    public ResultSet<T> subset(int lo, int hi) {
        TLongList rowIDs = this.rowIDs.subList(lo, hi);
        return new ResultSet<>(journal, rowIDs);
    }

    public ResultSet<T> shuffle(Random random) {
        TLongArrayList rows = new TLongArrayList(this.rowIDs);
        rows.shuffle(random);
        return new ResultSet<>(journal, rows);
    }

    private static <T> int compare(Journal<T> journal, int[] columns, long rightRowID, long leftRowID) throws JournalException {
        int result = 0;
        long leftLocalRowID = Rows.toLocalRowID(leftRowID);
        long rightLocalRowID = Rows.toLocalRowID(rightRowID);

        Partition<T> leftPart = journal.getPartition(Rows.toPartitionIndex(leftRowID), true);
        Partition<T> rightPart = journal.getPartition(Rows.toPartitionIndex(rightRowID), true);

        for (int column : columns) {
            Journal.ColumnMetadata meta = journal.getColumnMetadata(column);

            String leftStr;
            String rightStr;

            switch (meta.meta.type) {
                case STRING:
                    leftStr = leftPart.getString(leftLocalRowID, column);
                    rightStr = rightPart.getString(rightLocalRowID, column);

                    if (leftStr == null && rightStr == null) {
                        result = 0;
                    } else if (leftStr == null) {
                        result = 1;
                    } else if (rightStr == null) {
                        result = -1;
                    } else {
                        result = rightStr.compareTo(leftStr);
                    }
                    break;
                default:
                    switch (meta.meta.type) {
                        case INT:
                            result = compare(rightPart.getInt(rightLocalRowID, column), leftPart.getInt(leftLocalRowID, column));
                            break;
                        case LONG:
                            result = compare(rightPart.getLong(rightLocalRowID, column), leftPart.getLong(leftLocalRowID, column));
                            break;
                        case DOUBLE:
                            result = compare(rightPart.getDouble(rightLocalRowID, column), leftPart.getDouble(leftLocalRowID, column));
                            break;
                        case SYMBOL:
                            int leftSymIndex = leftPart.getInt(leftLocalRowID, column, SymbolTable.VALUE_IS_NULL);
                            int rightSymIndex = rightPart.getInt(rightLocalRowID, column, SymbolTable.VALUE_IS_NULL);

                            if (leftSymIndex == SymbolTable.VALUE_IS_NULL && rightSymIndex == SymbolTable.VALUE_IS_NULL) {
                                result = 0;
                            } else if (leftSymIndex == SymbolTable.VALUE_IS_NULL) {
                                result = 1;
                            } else if (rightSymIndex == SymbolTable.VALUE_IS_NULL) {
                                result = -1;
                            } else {
                                leftStr = meta.symbolTable.value(leftSymIndex);
                                rightStr = meta.symbolTable.value(rightSymIndex);

                                if (leftStr == null || rightStr == null) {
                                    throw new JournalException("Corrupt column [%s] !", meta);
                                }

                                result = rightStr.compareTo(leftStr);
                            }
                            break;
                        default:
                            throw new JournalException("Unsupported type: " + meta.meta.type);
                    }
            }


            if (result != 0) {
                break;
            }
        }
        return result;
    }

    private static int compare(long a, long b) {
        if (a == b) {
            return 0;
        } else if (a > b) {
            return 1;
        } else {
            return -1;
        }
    }

    private static int compare(int a, int b) {
        if (a == b) {
            return 0;
        } else if (a > b) {
            return 1;
        } else {
            return -1;
        }
    }

    private static int compare(double a, double b) {
        if (a == b) {
            return 0;
        } else if (a > b) {
            return 1;
        } else {
            return -1;
        }
    }

    void quickSort(Order order, int lo, int hi, int... columnIndices) throws JournalException {

        if (lo >= hi) {
            return;
        }

        int pIndex = lo + (hi - lo) / 2;
        long pivot = rowIDs.get(pIndex);

        int multiplier = 1;

        if (order == Order.DESC) {
            multiplier = -1;
        }

        int i = lo;
        int j = hi;

        while (i <= j) {

            while (multiplier * compare(journal, columnIndices, rowIDs.get(i), pivot) < 0) {
                i++;
            }

            while (multiplier * compare(journal, columnIndices, pivot, rowIDs.get(j)) < 0) {
                j--;
            }

            if (i <= j) {
                long temp = rowIDs.get(i);
                rowIDs.set(i, rowIDs.get(j));
                rowIDs.set(j, temp);
                i++;
                j--;
            }
        }
        quickSort(order, lo, j, columnIndices);
        quickSort(order, i, hi, columnIndices);
    }

    private int[] getColumnIndexes(String... columnNames) {
        int columnIndices[] = new int[columnNames.length];
        for (int i = 0, columnNamesLength = columnNames.length; i < columnNamesLength; i++) {
            columnIndices[i] = journal.getMetadata().getColumnIndex(columnNames[i]);
        }
        return columnIndices;
    }

    public enum Order {
        ASC, DESC
    }

    ResultSet(Journal<T> journal, TLongList rowIDs) {
        this.journal = journal;
        this.rowIDs = rowIDs;
    }

}
