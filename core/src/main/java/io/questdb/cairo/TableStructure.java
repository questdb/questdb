/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
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

package io.questdb.cairo;

import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.view.ViewDefinition;

public interface TableStructure {

    int getColumnCount();

    CharSequence getColumnName(int columnIndex);

    int getColumnType(int columnIndex);

    int getIndexBlockCapacity(int columnIndex);

    default MatViewDefinition getMatViewDefinition() {
        return null;
    }

    int getMaxUncommittedRows();

    long getO3MaxLag();

    int getPartitionBy();

    boolean getSymbolCacheFlag(int columnIndex);

    int getSymbolCapacity(int columnIndex);

    CharSequence getTableName();

    int getTimestampIndex();

    /**
     * Returns the time-to-live (TTL) of the data in this table:
     * if positive, it's in hours;
     * if negative, it's in months (and the actual value is positive);
     * zero means "no TTL".
     */
    default int getTtlHoursOrMonths() {
        return 0; // TTL disabled by default
    }

    default ViewDefinition getViewDefinition() {
        return null;
    }

    default void init(TableToken tableToken) {
    }

    /**
     * Returns the index type for the column.
     *
     * @param columnIndex the column index
     * @return the index type (see {@link IndexType})
     */
    byte getIndexType(int columnIndex);

    boolean isDedupKey(int columnIndex);

    default boolean isIndexed(int columnIndex) {
        return IndexType.isIndexed(getIndexType(columnIndex));
    }

    default boolean isMatView() {
        return false;
    }

    default boolean isView() {
        return false;
    }

    boolean isWalEnabled();
}
