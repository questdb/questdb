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

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.std.Numbers;

/**
 * The DATE-typed {@code json_extract}, whose other reads derive from {@link #getDate(Record)} instead
 * of parsing the JSON again at each width.
 * <p>
 * The base class derives every width from its own native parse, and for a DATE target those parses
 * disagree three ways. {@code getTimestamp} takes the JSON number straight through
 * {@code queryPointerValue} and hands it back unscaled, so {@code {"a":43200000}} - noon as a DATE -
 * reads 43,200,000 microseconds, or 43.2 seconds, at timestamp width. {@code getLong} and
 * {@code getDouble} run {@code queryPointerLong} / {@code queryPointerDouble}, which only accept a
 * JSON number, so a DATE parsed out of a JSON string reads NULL through both while the declared
 * getter reads the date. And a fractional JSON number truncates in {@code getDate} but not in
 * {@code getDouble}, so {@code {"a":2.5}} reads 2 as a DATE and 2.5 as a DOUBLE.
 * <p>
 * A DATE expression carries exactly one value, the one its eight bytes hold, so every other read
 * derives from that value, as {@code DateFunction} does: LONG hands the milliseconds back unchanged,
 * DOUBLE and FLOAT widen them, and TIMESTAMP scales them to microseconds.
 * <p>
 * All four are reachable from SQL, by two separate routes. Implicit promotion goes through DATE's
 * overload set - {@code DATE, TIMESTAMP, LONG, DOUBLE} - because {@code SqlParser} removes the cast
 * node, leaving a bare DATE-typed function that {@code FunctionParser} binds to a wider slot with no
 * cast function in between: {@code hour(N)} and the other {@code N}-slot date functions read
 * {@code getTimestamp()}, and {@code *(LL)} reads {@code getLong()} because {@code *} has no DATE
 * overload. An explicit cast resolves on the argument's own tag rather than by overload distance, so
 * it reaches getters the overload set omits: {@code cast(Md)} ({@code ::double}) reads
 * {@code getDouble()} and {@code cast(Mf)} ({@code ::real}) reads {@code getFloat()}, even though
 * FLOAT is absent from DATE's overload set.
 * <p>
 * The type is fixed at compile time, so the factory picks this class rather than the base branching
 * on it per row.
 */
public class JsonExtractDateFunction extends JsonExtractFunction {

    public JsonExtractDateFunction(int targetType, Function json, Function path, int maxSize) {
        super(targetType, json, path, maxSize);
    }

    @Override
    public double getDouble(Record rec) {
        final long value = getDate(rec);
        return value != Numbers.LONG_NULL ? value : Double.NaN;
    }

    @Override
    public float getFloat(Record rec) {
        final long value = getDate(rec);
        return value != Numbers.LONG_NULL ? value : Float.NaN;
    }

    @Override
    public long getLong(Record rec) {
        return getDate(rec);
    }

    @Override
    public long getTimestamp(Record rec) {
        // DATE promotes to TIMESTAMP at microsecond precision, the same fixed unit DateFunction uses;
        // fromDate() maps LONG_NULL to LONG_NULL.
        return MicrosTimestampDriver.INSTANCE.fromDate(getDate(rec));
    }
}
