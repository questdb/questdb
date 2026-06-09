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

package io.questdb.griffin.engine.functions.window;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.Numbers;

public interface WindowTimestampFunction extends WindowFunction {
    // getTimestamp() returns the stored value in the result's native unit: timestamp ticks for a
    // TIMESTAMP result, milliseconds for a DATE result (the value functions store a DATE argument
    // as milliseconds). A DATE result is already milliseconds; timestamp ticks need converting so
    // that reading a DATE window column, or casting a TIMESTAMP window result to DATE, yields the
    // right value. toDate() passes LONG_NULL through.
    @Override
    default long getDate(Record rec) {
        final long val = getTimestamp(rec);
        return ColumnType.isTimestamp(getType()) ? ColumnType.getTimestampDriver(getType()).toDate(val) : val;
    }

    @Override
    default double getDouble(Record rec) {
        final long val = getTimestamp(rec);
        return val != Numbers.LONG_NULL ? val : Double.NaN;
    }

    @Override
    default float getFloat(Record rec) {
        final long val = getTimestamp(rec);
        return val != Numbers.LONG_NULL ? val : Float.NaN;
    }

    @Override
    default long getLong(Record rec) {
        return getTimestamp(rec);
    }
}
