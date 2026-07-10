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

package io.questdb.griffin.engine.functions;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.SqlException;

/**
 * Helpers shared by the cursor-comparison factories (for example {@code col > (select ...)}),
 * where the right-hand operand is a scalar sub-query cursor expected to yield at most one row.
 */
public final class ScalarSubQueryUtils {

    private ScalarSubQueryUtils() {
    }

    /**
     * Enforces that a scalar sub-query cursor holds no further rows once its single value has
     * been consumed. Must be called only after the first row has already been read and its value
     * extracted into a stable (non-flyweight) field, because this advances the cursor by one row.
     *
     * @param cursor   the sub-query cursor, positioned on its first (already-read) row
     * @param position the parse position of the sub-query, used for the error marker
     * @throws SqlException if the cursor yields a second row
     */
    public static void assertNoMoreRows(RecordCursor cursor, int position) throws SqlException {
        if (cursor.hasNext()) {
            throw SqlException.$(position, "scalar sub-query returned more than one row");
        }
    }
}
