/*+*****************************************************************************
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

package io.questdb.griffin.engine.functions.json;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;

/**
 * The SHORT-typed {@code json_extract}, whose wider reads sign-extend {@link #getShort(Record)}
 * instead of re-parsing the JSON at each width.
 * <p>
 * The base class derives every width from its own native parse, so a JSON number outside the SHORT
 * range reads as {@code 0} through {@code getShort} (SHORT has no null sentinel) but as its full
 * value through {@code getLong} / {@code getDouble}. That gives one SHORT expression two values per
 * row: {@code SELECT j} prints 0 while {@code j + 0L} - which resolves {@code +(LL)} and reads
 * {@code getLong()} - prints the number. A SHORT expression carries exactly one value, the one its
 * two bytes hold, so every wider read sign-extends that value, as {@code ShortFunction} does.
 * <p>
 * The type is fixed at compile time, so the factory picks this class rather than the base branching
 * on it per row.
 */
public class JsonExtractShortFunction extends JsonExtractFunction {

    public JsonExtractShortFunction(int targetType, Function json, Function path, int maxSize) {
        super(targetType, json, path, maxSize);
    }

    @Override
    public long getDate(Record rec) {
        return getShort(rec);
    }

    @Override
    public double getDouble(Record rec) {
        return getShort(rec);
    }

    @Override
    public float getFloat(Record rec) {
        return getShort(rec);
    }

    @Override
    public int getInt(Record rec) {
        return getShort(rec);
    }

    @Override
    public long getLong(Record rec) {
        return getShort(rec);
    }

    @Override
    public long getTimestamp(Record rec) {
        return getShort(rec);
    }
}
