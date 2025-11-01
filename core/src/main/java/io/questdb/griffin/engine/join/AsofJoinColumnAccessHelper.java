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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.TimeFrameRecordCursor;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

/**
 * When joining on a single symbol column, detects when the slave column doesn't have
 * the symbol at all (by inspecting its int-to-symbol mapping), avoiding linear search
 * in that case.
 */
public interface AsofJoinColumnAccessHelper {
    @Transient
    CharSequence getMasterValue(Record masterRecord);

    /**
     * Used when joining on a single symbol column, returns the symbol key in the slave
     * column corresponding to the symbol key in the master column. If it returns
     * {@link StaticSymbolTable#VALUE_NOT_FOUND}, the slave column doesn't have the symbol.
     */
    int getSlaveKey(Record masterRecord);

    @NotNull
    StaticSymbolTable getSlaveSymbolTable();

    /**
     * Used when joining on a single symbol column, detects when the slave column doesn't
     * have the symbol at all (by inspecting its int-to-symbol mapping). This allows the
     * record cursor to skip any searching for the matching slave row.
     */
    default boolean isShortCircuit(Record masterRecord) {
        return getSlaveKey(masterRecord) == StaticSymbolTable.VALUE_NOT_FOUND;
    }

    void of(TimeFrameRecordCursor slaveCursor);

    default void of(RecordCursor slaveCursor) {
        throw new UnsupportedOperationException();
    }

}
