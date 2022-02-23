/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

public interface PageFrameCursor extends Closeable, SymbolTableSource {

    @Override
    void close(); // we don't throw IOException

    /**
     * Return the REAL row id of given row on current page .
     * This is used for e.g. updating rows.
     *
     * @param rowIndex - page index of row
     * @return real row id
     */
    long getUpdateRowId(long rowIndex);

    @Nullable PageFrame next();

    /**
     * Return the cursor to the beginning of the page frame.
     * Sets page address to first column.
     */
    void toTop();

    /**
     * @return size of page in bytes
     */
    long size();
}
