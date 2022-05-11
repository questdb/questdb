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

import io.questdb.cairo.BitmapIndexReader;
import io.questdb.std.LongList;

public interface PageFrame {

    void copyColumnAddressesTo(LongList destColumnAddresses);

    BitmapIndexReader getBitmapIndexReader(int columnIndex, int dirForward);

    /**
     * Return the size the column as power 2 of the bytes e.g. long == 3, int == 2 etc.
     *
     * @param columnIndex index of column
     * @return logarithm base 2 of size of column in bytes
     */
    int getColumnShiftBits(int columnIndex);

    /**
     * Return the address of the start of the page frame or if this page represents
     * a column top (a column that was added to the table when other columns already had data) then return 0
     *
     * @param columnIndex index of column
     * @return address of column or 0 if column is empty
     */
    long getPageAddress(int columnIndex);

    /**
     * Index page for variable-length column types, such as String and Binary
     * @param columnIndex index of variable length column
     * @return contiguous memory address containing offsets for variable value entries
     */
    long getIndexPageAddress(int columnIndex);

    /**
     * Return the size of the page frame column in bytes.
     *
     * @param columnIndex index of column
     * @return size of column in bytes
     */
    long getPageSize(int columnIndex);

    int getPartitionIndex();

    long getPartitionLo();

    long getPartitionHi();
}
