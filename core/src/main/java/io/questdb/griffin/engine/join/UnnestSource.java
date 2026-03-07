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
    ArrayView getArray(int sourceCol, int elementIndex, int columnType);

    boolean getBool(int sourceCol, int elementIndex);

    byte getByte(int sourceCol, int elementIndex);

    char getChar(int sourceCol, int elementIndex);

    /**
     * Returns the number of output columns this source produces.
     * For arrays: always 1 (one array = one column).
     */
    int getColumnCount();

    /**
     * Returns the output column type for the given source column.
     *
     * @param sourceCol column within this source (0-based)
     * @return QuestDB ColumnType constant
     */
    int getColumnType(int sourceCol);

    long getDate(int sourceCol, int elementIndex);

    double getDouble(int sourceCol, int elementIndex);

    float getFloat(int sourceCol, int elementIndex);

    int getInt(int sourceCol, int elementIndex);

    long getLong(int sourceCol, int elementIndex);

    short getShort(int sourceCol, int elementIndex);

    CharSequence getStrA(int sourceCol, int elementIndex);

    CharSequence getStrB(int sourceCol, int elementIndex);

    int getStrLen(int sourceCol, int elementIndex);

    long getTimestamp(int sourceCol, int elementIndex);

    Utf8Sequence getVarcharA(int sourceCol, int elementIndex);

    Utf8Sequence getVarcharB(int sourceCol, int elementIndex);

    int getVarcharSize(int sourceCol, int elementIndex);

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
