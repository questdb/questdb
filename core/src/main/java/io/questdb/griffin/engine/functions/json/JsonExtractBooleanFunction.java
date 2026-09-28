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
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;

/**
 * The BOOLEAN-typed {@code json_extract}, whose every other width renders {@link #getBool(Record)}
 * instead of re-reading the JSON at the width being read.
 * <p>
 * The base class derives each width from its own native parse, so {@code {"x":1}} reads as
 * {@code false} through {@code queryPointerBoolean} - a JSON number is not a JSON boolean - and as
 * {@code 1} through {@code queryPointerLong}. That gives one BOOLEAN expression two values per row:
 * {@code SELECT j::boolean} prints false while {@code j::boolean::long} prints 1. A BOOLEAN
 * expression carries exactly one value, the one its single byte holds, so every other width renders
 * that value, as {@code BooleanFunction} does: 1/0 numerically, true/false textually, T/F as a CHAR.
 * <p>
 * The textual and temporal widths were not merely inconsistent. BOOLEAN takes the constructor's
 * default branch, which leaves {@code destUtf8Sink} null, so the base's {@code getVarcharA},
 * {@code getStrA}, {@code getSymbol}, {@code getDate} and {@code getTimestamp} all reached an
 * extraction path that dereferences it. {@code getByte} and {@code getChar} threw
 * {@code UnsupportedOperationException} outright. Deriving them from {@code getBool} removes both
 * failure modes along with the divergence.
 * <p>
 * {@code getIPv4} keeps the base, which has the same null-sink problem, because BOOLEAN has no cast
 * to IPv4 - there is no {@code cast(Tz)} factory - so nothing reaches it.
 * <p>
 * The type is fixed at compile time, so the factory picks this class rather than the base branching
 * on it per row.
 */
public class JsonExtractBooleanFunction extends JsonExtractFunction {
    private static final Utf8String UTF_8_FALSE = new Utf8String("false");
    private static final Utf8String UTF_8_TRUE = new Utf8String("true");

    public JsonExtractBooleanFunction(int targetType, Function json, Function path, int maxSize) {
        super(targetType, json, path, maxSize);
    }

    @Override
    public byte getByte(Record rec) {
        return (byte) (getBool(rec) ? 1 : 0);
    }

    @Override
    public char getChar(Record rec) {
        return getBool(rec) ? 'T' : 'F';
    }

    @Override
    public long getDate(Record rec) {
        return getBool(rec) ? 1 : 0;
    }

    @Override
    public double getDouble(Record rec) {
        return getBool(rec) ? 1 : 0;
    }

    @Override
    public float getFloat(Record rec) {
        return getBool(rec) ? 1 : 0;
    }

    @Override
    public int getInt(Record rec) {
        return getBool(rec) ? 1 : 0;
    }

    @Override
    public long getLong(Record rec) {
        return getBool(rec) ? 1 : 0;
    }

    @Override
    public short getShort(Record rec) {
        return (short) (getBool(rec) ? 1 : 0);
    }

    @Override
    public CharSequence getStrA(Record rec) {
        return getBool(rec) ? "true" : "false";
    }

    @Override
    public CharSequence getStrB(Record rec) {
        return getStrA(rec);
    }

    @Override
    public long getTimestamp(Record rec) {
        return getBool(rec) ? 1 : 0;
    }

    @Override
    public Utf8Sequence getVarcharA(Record rec) {
        return getBool(rec) ? UTF_8_TRUE : UTF_8_FALSE;
    }

    @Override
    public Utf8Sequence getVarcharB(Record rec) {
        return getVarcharA(rec);
    }
}
