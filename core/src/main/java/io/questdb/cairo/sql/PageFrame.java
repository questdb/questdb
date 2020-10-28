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

import io.questdb.cairo.ColumnType;

public interface PageFrame {
    /**
     * 
     * Return the address of the start of the page frame or if this page represents a column top (a column that was added to the table when other columns already had data) then return 0
     * 
     * @param columnIndex
     * @return
     */
    long getPageAddress(int columnIndex);

    long getPageValueCount(int columnIndex);

    /**
     * 
     * Return the size of the page frame or if the page represents a column top (a column that was added to the table when other columns already had data), then return the number of of empty rows at the top of a column
     * 
     * @param columnIndex
     * @return
     */
    default long getPageSize(int columnIndex) {
        throw new UnsupportedOperationException();
    }

    default long getFirstTimestamp() {
        throw new UnsupportedOperationException();
    }

    public static long getPageFrameNRows(PageFrame pageFrame, int columnIndex, int columnType) {
        assert ColumnType.isFixedLength(columnType);
        if (pageFrame.getPageAddress(columnIndex) != 0) {
            return pageFrame.getPageSize(columnIndex) >> ColumnType.pow2SizeOf(columnType);
        }
        return pageFrame.getPageSize(columnIndex);
    }
}
