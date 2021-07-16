/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;

import java.io.Closeable;

import static io.questdb.std.datetime.TimeZoneRuleFactory.RESOLUTION_MICROS;
import static io.questdb.std.datetime.microtime.Timestamps.MINUTE_MICROS;

public abstract class AbstractSampleByCursor implements NoRandomAccessRecordCursor, Closeable {
    protected final TimestampSampler timestampSampler;
    protected final Function timezoneNameFunc;
    protected final int timezoneNameFuncPos;
    protected final Function offsetFunc;
    protected final int offsetFuncPos;
    protected long tzOffset;
    protected long prevDst;
    protected long fixedOffset;
    protected TimeZoneRules rules;
    protected long nextDstUTC;
    protected long localEpoch;

    public AbstractSampleByCursor(
            TimestampSampler timestampSampler,
            Function timezoneNameFunc,
            int timezoneNameFuncPos,
            Function offsetFunc,
            int offsetFuncPos
    ) {
        this.timestampSampler = timestampSampler;
        this.timezoneNameFunc = timezoneNameFunc;
        this.timezoneNameFuncPos = timezoneNameFuncPos;
        this.offsetFunc = offsetFunc;
        this.offsetFuncPos = offsetFuncPos;
    }

    @Override
    public void close() {
        Misc.free(timezoneNameFunc);
        Misc.free(offsetFunc);
    }

    protected void parseParams(RecordCursor base, SqlExecutionContext executionContext) throws SqlException {
        // factory guarantees that base cursor is not empty
        timezoneNameFunc.init(base, executionContext);
        offsetFunc.init(base, executionContext);
        this.rules = null;
        final CharSequence tz = timezoneNameFunc.getStr(null);
        if (tz != null) {
            try {
                long opt = Timestamps.parseOffset(tz);
                if (opt == Long.MIN_VALUE) {
                    // this is timezone name
                    // fixed rules means the timezone does not have historical or daylight time changes
                    this.rules = TimestampFormatUtils.enLocale.getZoneRules(
                            Numbers.decodeLowInt(TimestampFormatUtils.enLocale.matchZone(tz, 0, tz.length())),
                            RESOLUTION_MICROS
                    );
                } else {
                    // here timezone is in numeric offset format
                    tzOffset = Numbers.decodeLowInt(opt) * MINUTE_MICROS;
                    nextDstUTC = Long.MAX_VALUE;
                }
            } catch (NumericException e) {
                throw SqlException.$(timezoneNameFuncPos, "invalid timezone: ").put(tz);
            }
        } else {
            this.tzOffset = 0;
            this.nextDstUTC = Long.MAX_VALUE;
        }

        final CharSequence offset = offsetFunc.getStr(null);
        if (offset != null) {
            final long val = Timestamps.parseOffset(offset);
            if (val == Numbers.LONG_NaN) {
                // bad value for offset
                throw SqlException.$(offsetFuncPos, "invalid offset: ").put(offset);
            }
            this.fixedOffset = Numbers.decodeLowInt(val) * MINUTE_MICROS;
        } else {
            fixedOffset = Long.MIN_VALUE;
        }
    }
}
