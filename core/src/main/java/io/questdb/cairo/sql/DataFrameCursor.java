/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import java.io.Closeable;

/**
 * A cursor for managing position of operations within data frames
 *
 * Interfaces which extend Closeable are not optionally-closeable.
 * close() method must be called after other calls are complete.
 */
public interface DataFrameCursor extends Closeable, SymbolTableSource  {

    // same TableReader is available on each data frame
    TableReader getTableReader();

    /**
     * Reload the data frame and return the cursor to the beginning of
     * the data frame
     * @return true when reload data has changed, false otherwise
     */
    boolean reload();

    /**
     * @return the next element in the data frame
     */
    @Nullable DataFrame next();

    /**
     * Must be closed after other calls are complete
     */
    @Override
    void close();

    /**
     * Return the cursor to the beginning of the data frame
     */
    void toTop();

    /**
     * @return number of items in the data frame
     */
    long size();

    StaticSymbolTable getSymbolTable(int columnIndex);
}
