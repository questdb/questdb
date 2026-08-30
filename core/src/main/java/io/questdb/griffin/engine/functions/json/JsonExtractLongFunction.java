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
import io.questdb.std.Numbers;

/**
 * The LONG-typed {@code json_extract}, whose other reads derive from {@link #getLong(Record)} instead
 * of parsing the JSON again at each width.
 * <p>
 * The base class derives every width from its own native parse, and for a LONG target those parses
 * disagree three ways. {@code getLong} truncates a fractional JSON number through
 * {@code queryPointerLong}, so {@code {"a":2.5}} reads 2; {@code getDouble} re-parses it through
 * {@code queryPointerDouble} and reads 2.5; and {@code getDate} / {@code getTimestamp} take a third
 * route through {@code queryPointerValue} that also parses a date literal out of a JSON string, so
 * {@code {"a":"1970-01-01T00:00:00.000002Z"}} reads NULL as a LONG but 2 as a TIMESTAMP. That gives
 * one LONG expression up to three values per row: {@code SELECT j} prints 2 while {@code j + 0.0} -
 * which resolves {@code +(DD)} and reads {@code getDouble()} - prints 2.5.
 * <p>
 * A LONG expression carries exactly one value, the one its eight bytes hold, so every other read
 * returns that value, as {@code LongFunction} does.
 * <p>
 * Only {@link #getDouble(Record)} is reachable from SQL today, through the {@code (LONG, DOUBLE)}
 * promotion. {@code getFloat} is not - FLOAT is absent from LONG's overload set - and neither are
 * {@code getDate} / {@code getTimestamp}: every LONG-to-TIMESTAMP promotion routes through a cast
 * function that reads {@code getLong()} rather than reading this function at timestamp width, which a
 * probe over coalesce / CASE / nullif / hour / micros / to_str / dateadd / to_utc / to_timezone /
 * date_trunc confirmed. The three are overridden anyway so no future boundary can reach a second
 * value, matching {@code LongFunction} and the sibling INT and SHORT variants; only the
 * {@code getDouble} row of the contract is test-covered.
 * <p>
 * The type is fixed at compile time, so the factory picks this class rather than the base branching
 * on it per row.
 */
public class JsonExtractLongFunction extends JsonExtractFunction {

    public JsonExtractLongFunction(int targetType, Function json, Function path, int maxSize) {
        super(targetType, json, path, maxSize);
    }

    @Override
    public long getDate(Record rec) {
        return getLong(rec);
    }

    @Override
    public double getDouble(Record rec) {
        final long value = getLong(rec);
        return value != Numbers.LONG_NULL ? value : Double.NaN;
    }

    @Override
    public float getFloat(Record rec) {
        return Numbers.longToFloat(getLong(rec));
    }

    @Override
    public long getTimestamp(Record rec) {
        return getLong(rec);
    }
}
