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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.std.Numbers;

/**
 * The TIMESTAMP-typed {@code json_extract}, whose other reads derive from {@link #getTimestamp(Record)}
 * instead of parsing the JSON again at each width.
 * <p>
 * The base class derives every width from its own native parse, and for a TIMESTAMP target those
 * parses disagree three ways. {@code getDate} takes the JSON number straight through
 * {@code queryPointerValue} and hands it back unscaled, so {@code {"a":5000}} - five milliseconds as a
 * microsecond TIMESTAMP - reads 5,000 milliseconds, or five seconds, at date width. {@code getLong}
 * and {@code getDouble} run {@code queryPointerLong} / {@code queryPointerDouble}, which only accept a
 * JSON number, so a TIMESTAMP parsed out of a JSON string reads NULL through both while the declared
 * getter reads the timestamp. And a fractional JSON number truncates in {@code getTimestamp} but not
 * in {@code getDouble}.
 * <p>
 * A TIMESTAMP expression carries exactly one value, the one its eight bytes hold, so every other read
 * derives from that value, as {@code TimestampFunction} does: LONG hands the ticks back unchanged,
 * DOUBLE and FLOAT widen them, and DATE scales them down to milliseconds through the target's own
 * driver - the divisor differs between {@code TIMESTAMP} and {@code TIMESTAMP_NS}, so it has to come
 * from the declared type rather than a fixed unit.
 * <p>
 * All four are reachable from SQL, by two separate routes. Implicit promotion goes through TIMESTAMP's
 * overload set - {@code TIMESTAMP, LONG, DATE, DOUBLE} - because {@code SqlParser} removes the cast
 * node, leaving a bare TIMESTAMP-typed function that {@code FunctionParser} binds to a wider slot with
 * no cast function in between: {@code *(LL)} reads {@code getLong()} because {@code *} has no
 * TIMESTAMP overload. An explicit cast resolves on the argument's own tag rather than by overload
 * distance, so it reaches getters the overload set omits: {@code cast(Nm)} ({@code ::date}) reads
 * {@code getDate()}, {@code cast(Nd)} ({@code ::double}) reads {@code getDouble()}, {@code cast(Nf)}
 * ({@code ::real}) reads {@code getFloat()} even though FLOAT is absent from TIMESTAMP's overload set,
 * and {@code cast(Nn)} - the precision change {@code ::timestamp_ns} - reads {@code getLong()}.
 * <p>
 * The type is fixed at compile time, so the factory picks this class rather than the base branching
 * on it per row.
 */
public class JsonExtractTimestampFunction extends JsonExtractFunction {
    private final TimestampDriver targetDriver;

    public JsonExtractTimestampFunction(int targetType, Function json, Function path, int maxSize) {
        super(targetType, json, path, maxSize);
        // The declared type's own driver. The base class resolves a driver too, but through
        // getTimestampType(), which answers the different question of how to parse a JSON string as a
        // timestamp; for a TIMESTAMP target the two coincide, for a DATE target they do not.
        this.targetDriver = ColumnType.getTimestampDriver(targetType);
    }

    @Override
    public long getDate(Record rec) {
        // toDate() maps LONG_NULL to LONG_NULL.
        return targetDriver.toDate(getTimestamp(rec));
    }

    @Override
    public double getDouble(Record rec) {
        final long value = getTimestamp(rec);
        return value != Numbers.LONG_NULL ? value : Double.NaN;
    }

    @Override
    public float getFloat(Record rec) {
        final long value = getTimestamp(rec);
        return value != Numbers.LONG_NULL ? value : Float.NaN;
    }

    @Override
    public long getLong(Record rec) {
        return getTimestamp(rec);
    }
}
