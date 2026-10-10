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


package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.std.Numbers;

/**
 * Base of the functions that shift a timestamp argument between time zones. A NULL timestamp stays NULL; subclasses
 * shift only non-NULL timestamps.
 */
abstract class AbstractTimestampShiftFunction extends TimestampFunction {
    protected final Function timestampFunc;

    protected AbstractTimestampShiftFunction(Function timestampFunc, int timestampType) {
        super(timestampType);
        this.timestampFunc = timestampFunc;
    }

    @Override
    public final long getTimestamp(Record rec) {
        final long timestamp = timestampFunc.getTimestamp(rec);
        return timestamp != Numbers.LONG_NULL ? shift(rec, timestamp) : Numbers.LONG_NULL;
    }

    protected abstract long shift(Record rec, long timestamp);
}
