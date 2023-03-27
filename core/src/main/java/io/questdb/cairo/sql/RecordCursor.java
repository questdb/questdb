/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.cairo.sql;

import java.io.Closeable;

/**
 * A cursor for managing position of operations over multiple records.
 * <p>
 * Interfaces which extend Closeable are not optionally-closeable.
 * close() method must be called after other calls are complete.
 */
public interface RecordCursor extends Closeable, SymbolTableSource {

    /**
     * RecordCursor must be closed after other method calls are finished.
     */
    @Override
    void close();

    /**
     * @return record at current position
     */
    Record getRecord();

    /**
     * May be used to compare references with getRecord
     *
     * @return record at current position
     */
    Record getRecordB();

    /**
     * Cached instance of symbol table for the given column. The method
     * guarantees that symbol table instance is reused across multiple invocations.
     *
     * @param columnIndex numeric index of the column
     * @return instance of symbol table or null, when column is not Symbol
     */
    default SymbolTable getSymbolTable(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return true if more records may be accessed, otherwise false
     * @throws io.questdb.cairo.DataUnavailableException when the queried partition is in cold storage
     */
    boolean hasNext();

    /**
     * Returns true if the cursor is using an index, false otherwise
     *
     * @return true if the cursor is using an index, false otherwise
     */
    default boolean isUsingIndex() {
        return false;
    }

    /**
     * Creates new instance of symbol table, usually returned by {@link #getSymbolTable(int)}. Symbol table clones are
     * used in concurrent SQL execution. They are assigned to individual threads.
     *
     * @param columnIndex numeric index of the column
     * @return clone of symbol table or the same instance when instance is immutable(empty column)
     */
    default SymbolTable newSymbolTable(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Positions record at given row id. The row id must have been previously obtained from Record instance.
     *
     * @param record  to position
     * @param atRowId rowid of the desired record
     */
    void recordAt(Record record, long atRowId);

    /**
     * Not every record cursor has a size, may return -1, in this case, keep going until hasNext()
     * indicated there are no more records to access.
     *
     * @return size of records available to the cursor
     */
    long size();

    /**
     * Tries to position the record at the given row count to skip in an efficient way.
     * Rows are counted top of table.
     * <p>
     * Supported by some record cursors that support random access (e.g. tables ordered by
     * designated timestamp).
     *
     * @param rowCount row count to skip down the cursor
     * @return true if a fast skip is supported by the cursor and was executed, false otherwise
     * @throws io.questdb.cairo.DataUnavailableException when the queried partition is in cold storage
     */
    default boolean skipTo(long rowCount) {
        return false;
    }

    /**
     * Return the cursor to the beginning of the page frame.
     * Sets location to first column.
     */
    void toTop();
}
