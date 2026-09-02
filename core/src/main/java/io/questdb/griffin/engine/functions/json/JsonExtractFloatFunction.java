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
 * The FLOAT-typed {@code json_extract}, whose {@link #getDouble(Record)} widens
 * {@link #getFloat(Record)} instead of returning the untruncated parse.
 * <p>
 * The base class parses the JSON at double width and merely narrows that for {@code getFloat}, so a
 * payload with no exact float carries two values: {@code {"a":0.1}} reads 0.1 through
 * {@code getDouble} but 0.10000000149011612 through {@code getFloat}. {@code SELECT j + 0.0} - which
 * resolves {@code +(DD)} and reads {@code getDouble()} - therefore printed a number the same
 * expression could not hold, and {@code WHERE j = 0.1} matched where a FLOAT column holding 0.1 does
 * not.
 * <p>
 * A FLOAT expression carries exactly one value, the one its four bytes hold, so the double read
 * widens the float read, as {@code FloatFunction} does. FLOAT promotes only to FLOAT and DOUBLE, so
 * these two getters are the only reachable reads; {@code FloatFunction} rejects the rest outright.
 * <p>
 * The type is fixed at compile time, so the factory picks this class rather than the base branching
 * on it per row.
 */
public class JsonExtractFloatFunction extends JsonExtractFunction {

    public JsonExtractFloatFunction(int targetType, Function json, Function path, int maxSize) {
        super(targetType, json, path, maxSize);
    }

    @Override
    public double getDouble(Record rec) {
        return getFloat(rec);
    }

    @Override
    public float getFloat(Record rec) {
        // super.getDouble() is the one native parse. Calling it directly rather than getDouble() is
        // what breaks the cycle: the base's getFloat() is (float) getDouble(), so deriving getDouble
        // from an un-overridden getFloat would recurse.
        return (float) super.getDouble(rec);
    }
}
