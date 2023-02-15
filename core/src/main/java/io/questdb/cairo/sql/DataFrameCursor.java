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

import io.questdb.cairo.TableReader;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

/**
 * A cursor for managing position of operations within data frames
 * <p>
 * Interfaces which extend Closeable are not optionally-closeable.
 * close() method must be called after other calls are complete.
 */
public interface DataFrameCursor extends Closeable, SymbolTableSource {

    /**
     * Must be closed after other calls are complete
     */
    @Override
    void close();

    StaticSymbolTable getSymbolTable(int columnIndex);

    // same TableReader is available on each data frame
    TableReader getTableReader();

    StaticSymbolTable newSymbolTable(int columnIndex);

    /**
     * @return the next element in the data frame
     * @throws io.questdb.cairo.DataUnavailableException when the queried partition is in cold storage
     */
    @Nullable DataFrame next();

    /**
     * Reload the data frame and return the cursor to the beginning of
     * the data frame
     *
     * @return true when reload data has changed, false otherwise
     */
    @TestOnly
    boolean reload();

    /**
     * @return number of items in the data frame
     */
    long size();

    /**
     * Positions data frame at the given row number.
     *
     * @param rowCount absolute row number in table. Rows are numbered 0...row_count-1
     * @return data frame and position (lo) of given rowCount (according to cursor order).
     * @throws io.questdb.cairo.DataUnavailableException when the queried partition is in cold storage
     */
    default @Nullable DataFrame skipTo(long rowCount) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return true if cursor supports random record access (without having to iterate through all results).
     */
    default boolean supportsRandomAccess() {
        return false;
    }

    /**
     * Return the cursor to the beginning of the data frame
     */
    void toTop();
}
