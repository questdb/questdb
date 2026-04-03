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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.Record;
import io.questdb.std.str.Utf8Sequence;

/**
 * Abstraction between UnnestRecord and the data being unnested.
 * <p>
 * For typed arrays ({@link ArrayUnnestSource}), one source produces one
 * output column (one array = one column).
 * For JSON UNNEST ({@link JsonUnnestSource}), one source produces N output
 * columns (one JSON expression = N declared COLUMNS).
 * <p>
 * All getters default to throwing {@link UnsupportedOperationException}.
 * Implementations override only the getters they support.
 */
public interface UnnestSource {

    /**
     * Returns the sub-array at the given element index (for 2D+ arrays).
     *
     * @param sourceCol    column within this source (0-based)
     * @param elementIndex current element index (0-based)
     * @param columnType   the expected column type
     * @return the array view, or null if out of bounds or null
     */
    default ArrayView getArray(int sourceCol, int elementIndex, int columnType) {
        throw new UnsupportedOperationException();
    }

    default boolean getBool(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the number of output columns this source produces.
     * For arrays: always 1 (one array = one column).
     */
    int getColumnCount();

    default long getDate(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    default double getDouble(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    default int getInt(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    long getLong(int sourceCol, int elementIndex);

    default short getShort(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    default CharSequence getStrA(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    default CharSequence getStrB(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    default int getStrLen(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    default long getTimestamp(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    default Utf8Sequence getVarcharA(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    default Utf8Sequence getVarcharB(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    default int getVarcharSize(int sourceCol, int elementIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Binds to the current base record. Called when the master cursor
     * advances to a new row.
     *
     * @param baseRecord the current base record
     * @return the number of elements (rows to produce from this source),
     * or 0 for NULL or empty arrays
     */
    int init(Record baseRecord);
}
