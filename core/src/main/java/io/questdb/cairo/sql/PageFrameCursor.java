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

package io.questdb.cairo.sql;

import io.questdb.std.IntList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

public interface PageFrameCursor extends QuietCloseable, SymbolTableSource {

    void calculateSize(RecordCursor.Counter counter);

    /**
     * Returns local (query) to table reader index mapping.
     * Used to map local column indexes to indexes from the Parquet file.
     * Such mapping requires knowing the corresponding table reader indexes.
     */
    IntList getColumnIndexes();

    @Override
    StaticSymbolTable getSymbolTable(int columnIndex);

    @Nullable
    PageFrame next();

    /**
     * @return number of rows in all page frames
     */
    long size();

    /**
     * @return true if cursor supports fast size calculation,
     * i.e. {@link #calculateSize(RecordCursor.Counter)} is properly implemented.
     */
    boolean supportsSizeCalculation();

    /**
     * Returns the cursor to the beginning of the page frame.
     */
    void toTop();
}
