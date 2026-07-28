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
 * The INT-typed {@code json_extract}, whose 64-bit reads sign-extend {@link #getInt(Record)}
 * instead of re-parsing the JSON at long width.
 * <p>
 * The base class derives each width from its own native parse, so a JSON number outside the INT
 * range reads as NULL under {@code queryPointerInt} (which raises {@code NUMBER_OUT_OF_RANGE}) and
 * as its full value under {@code queryPointerLong}. That gives one INT expression two values per
 * row: {@code SELECT j} prints null while {@code j + 0L} - which resolves {@code +(LL)} and reads
 * {@code getLong()} - prints the number. An INT expression carries exactly one value, the one its
 * four bytes hold, so every 64-bit read of it widens that value, as {@code IntFunction} does.
 * <p>
 * The type is fixed at compile time, so the factory picks this class rather than the base branching
 * on it per row.
 */
public class JsonExtractIntFunction extends JsonExtractFunction {

    public JsonExtractIntFunction(int targetType, Function json, Function path, int maxSize) {
        super(targetType, json, path, maxSize);
    }

    @Override
    public long getDate(Record rec) {
        return Numbers.intToLong(getInt(rec));
    }

    @Override
    public long getLong(Record rec) {
        return Numbers.intToLong(getInt(rec));
    }

    @Override
    public long getTimestamp(Record rec) {
        return Numbers.intToLong(getInt(rec));
    }
}
