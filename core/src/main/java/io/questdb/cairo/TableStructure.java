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

package io.questdb.cairo;

import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.std.Numbers;

public interface TableStructure {

    int getColumnCount();

    CharSequence getColumnName(int columnIndex);

    int getColumnType(int columnIndex);

    int getIndexBlockCapacity(int columnIndex);

    default MatViewDefinition getMatViewDefinition() {
        return null;
    }

    /**
     * Returns incremental refresh limit for the materialized view:
     * if positive, it's in hours;
     * if negative, it's in months (and the actual value is positive);
     * zero means "no refresh limit".
     */
    default int getMatViewRefreshLimitHoursOrMonths() {
        return 0; // disabled by default
    }

    default int getMatViewTimerInterval() {
        return 0; // disabled by default
    }

    default char getMatViewTimerIntervalUnit() {
        return 0; // disabled by default
    }

    default long getMatViewTimerStart() {
        return Numbers.LONG_NULL; // disabled by default
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

    default void init(TableToken tableToken) {
    }

    boolean isDedupKey(int columnIndex);

    boolean isIndexed(int columnIndex);

    default boolean isMatView() {
        return false;
    }

    boolean isWalEnabled();
}
