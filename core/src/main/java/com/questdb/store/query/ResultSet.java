/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.store.query;

import com.questdb.std.LongList;
import com.questdb.std.Rnd;
import com.questdb.std.Rows;
import com.questdb.std.ex.JournalException;
import com.questdb.store.ColumnType;
import com.questdb.store.Journal;
import com.questdb.store.Partition;
import com.questdb.store.SymbolTable;
import com.questdb.store.factory.configuration.ColumnMetadata;
import com.questdb.store.query.iter.ResultSetBufferedIterator;
import com.questdb.store.query.iter.ResultSetIterator;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

public class ResultSet<T> implements Iterable<T> {
    private final Journal<T> journal;
    private final LongList rowIDs;

    ResultSet(Journal<T> journal, LongList rowIDs) {
        this.journal = journal;
        this.rowIDs = rowIDs;
    }

    public ResultSetBufferedIterator<T> bufferedIterator() {
        return new ResultSetBufferedIterator<>(this);
    }

    public double getDouble(int rsIndex, int columnIndex) throws JournalException {
        long rowID = rowIDs.get(rsIndex);
        return journal.getPartition(Rows.toPartitionIndex(rowID), true).getDouble(Rows.toLocalRowID(rowID), columnIndex);
    }

    public long getInt(int rsIndex, int columnIndex) throws JournalException {
        long rowID = rowIDs.get(rsIndex);
        return journal.getPartition(Rows.toPartitionIndex(rowID), true).getInt(Rows.toLocalRowID(rowID), columnIndex);
    }

    public Journal<T> getJournal() {
        return journal;
    }

    public long getLong(int rsIndex, int columnIndex) throws JournalException {
        long rowID = rowIDs.get(rsIndex);
        return journal.getPartition(Rows.toPartitionIndex(rowID), true).getLong(Rows.toLocalRowID(rowID), columnIndex);
    }

    public long getRowID(int index) {
        return rowIDs.get(index);
    }

    public String getString(int rsIndex, int columnIndex) throws JournalException {
        long rowID = rowIDs.get(rsIndex);
        return journal.getPartition(Rows.toPartitionIndex(rowID), true).getStr(Rows.toLocalRowID(rowID), columnIndex);
    }

    public CharSequence getSymbol(int rsIndex, int columnIndex) throws JournalException {
        long rowID = rowIDs.get(rsIndex);
        return journal.getPartition(Rows.toPartitionIndex(rowID), true).getSym(Rows.toLocalRowID(rowID), columnIndex);
    }

    @Override
    @NotNull
    public Iterator<T> iterator() {
        return new ResultSetIterator<>(this);
    }

    public T[] read() throws JournalException {
        return journal.read(rowIDs);
    }

    public void read(int index, T obj) throws JournalException {
        journal.read(rowIDs.get(index), obj);
    }

    public T read(int rsIndex) throws JournalException {
        return journal.read(rowIDs.get(rsIndex));
    }

    public T readFirst() throws JournalException {
        return size() > 0 ? read(0) : null;
    }

    public T readLast() throws JournalException {
        int size = size();
        return size > 0 ? read(size - 1) : null;
    }

    public long[] readTimestamps() throws JournalException {
        int timestampColIndex = journal.getMetadata().getTimestampIndex();
        long[] result = new long[size()];

        for (int i = 0, rowIDsLength = rowIDs.size(); i < rowIDsLength; i++) {
            result[i] = getLong(i, timestampColIndex);
        }
        return result;
    }

    public ResultSet<T> shuffle(Rnd rnd) {
        LongList rows = new LongList(this.rowIDs);
        rows.shuffle(rnd);
        return new ResultSet<>(journal, rows);
    }

    public int size() {
        return rowIDs.size();
    }

    public ResultSet<T> sort(String... columnNames) throws JournalException {
        return sort(Order.ASC, getColumnIndexes(columnNames));
    }

    public ResultSet<T> sort(Order order, String... columnNames) throws JournalException {
        return sort(order, getColumnIndexes(columnNames));
    }

    public ResultSet<T> sort() {
        rowIDs.sort();
        return this;
    }

    /**
     * Creates subset of ResultSet by result set row numbers.
     *
     * @param lo low end point of result set (inclusive)
     * @param hi high end point of result set (exclusive)
     * @return a subset of result set from lo (inclusive) to hi (exclusive)
     */
    public ResultSet<T> subset(int lo, int hi) {
        return new ResultSet<>(journal, this.rowIDs.subset(lo, hi));
    }

    private static <T> int compare(Journal<T> journal, int[] columns, long rightRowID, long leftRowID) throws JournalException {
        int result = 0;
        long leftLocalRowID = Rows.toLocalRowID(leftRowID);
        long rightLocalRowID = Rows.toLocalRowID(rightRowID);

        Partition<T> leftPart = journal.getPartition(Rows.toPartitionIndex(leftRowID), true);
        Partition<T> rightPart = journal.getPartition(Rows.toPartitionIndex(rightRowID), true);

        for (int column : columns) {
            ColumnMetadata meta = journal.getMetadata().getColumnQuick(column);

            String leftStr;
            String rightStr;

            switch (meta.type) {
                case ColumnType.STRING:
                    leftStr = leftPart.getStr(leftLocalRowID, column);
                    rightStr = rightPart.getStr(rightLocalRowID, column);

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
                    switch (meta.type) {
                        case ColumnType.INT:
                            result = compare(rightPart.getInt(rightLocalRowID, column), leftPart.getInt(leftLocalRowID, column));
                            break;
                        case ColumnType.LONG:
                        case ColumnType.DATE:
                            result = compare(rightPart.getLong(rightLocalRowID, column), leftPart.getLong(leftLocalRowID, column));
                            break;
                        case ColumnType.DOUBLE:
                            result = compare(rightPart.getDouble(rightLocalRowID, column), leftPart.getDouble(leftLocalRowID, column));
                            break;
                        case ColumnType.FLOAT:
                            result = compare(rightPart.getFloat(rightLocalRowID, column), leftPart.getFloat(leftLocalRowID, column));
                            break;
                        case ColumnType.SYMBOL:
                            int leftSymIndex = leftPart.getInt(leftLocalRowID, column);
                            int rightSymIndex = rightPart.getInt(rightLocalRowID, column);

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
                            throw new JournalException("Unsupported type: " + meta.type);
                    }
            }


            if (result != 0) {
                break;
            }
        }
        return result;
    }

    private static int compare(long a, long b) {
        return Long.compare(a, b);
    }

    private static int compare(int a, int b) {
        return Integer.compare(a, b);
    }

    private static int compare(double a, double b) {
        return Double.compare(a, b);
    }

    private static int compare(float a, float b) {
        return Float.compare(a, b);
    }

    private int[] getColumnIndexes(String... columnNames) {
        int columnIndices[] = new int[columnNames.length];
        for (int i = 0, columnNamesLength = columnNames.length; i < columnNamesLength; i++) {
            columnIndices[i] = journal.getMetadata().getColumnIndex(columnNames[i]);
        }
        return columnIndices;
    }

    private void quickSort(Order order, int lo, int hi, int... columnIndices) throws JournalException {

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

    private ResultSet<T> sort(Order order, int... columnIndices) throws JournalException {
        int size = size();
        if (size > 0) {
            quickSort(order, 0, size - 1, columnIndices);
        }
        return this;
    }

    public enum Order {
        ASC, DESC
    }

}
