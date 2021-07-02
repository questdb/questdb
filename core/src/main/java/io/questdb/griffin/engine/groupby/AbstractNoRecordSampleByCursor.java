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

import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionInterruptor;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.TimeZoneRulesMicros;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;

import static io.questdb.std.datetime.TimeZoneRuleFactory.RESOLUTION_MICROS;
import static io.questdb.std.datetime.microtime.Timestamps.MINUTE_MICROS;

public abstract class AbstractNoRecordSampleByCursor implements NoRandomAccessRecordCursor {
    protected final TimestampSampler timestampSampler;
    protected final int timestampIndex;
    protected final ObjList<GroupByFunction> groupByFunctions;
    private final ObjList<Function> recordFunctions;
    protected Record baseRecord;
    protected long sampleLocalEpoch;
    protected long nextTimestamp;
    protected RecordCursor base;
    protected SqlExecutionInterruptor interruptor;
    protected long tzOffset;
    protected long fixedOffset;
    protected long topTzOffset;
    protected long localEpoch;
    protected TimeZoneRules rules;
    private long topLocalEpoch;

    public AbstractNoRecordSampleByCursor(
            ObjList<Function> recordFunctions,
            int timestampIndex, // index of timestamp column in base cursor
            TimestampSampler timestampSampler,
            ObjList<GroupByFunction> groupByFunctions
    ) {
        this.timestampIndex = timestampIndex;
        this.timestampSampler = timestampSampler;
        this.recordFunctions = recordFunctions;
        this.groupByFunctions = groupByFunctions;
    }

    @Override
    public void close() {
        base.close();
        interruptor = null;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return (SymbolTable) recordFunctions.getQuick(columnIndex);
    }

    @Override
    public void toTop() {
        GroupByUtils.toTop(recordFunctions);
        this.base.toTop();
        this.localEpoch = topLocalEpoch;
        this.sampleLocalEpoch = topLocalEpoch;
        // timezone offset is liable to change when we pass over DST edges
        this.tzOffset = topTzOffset;
    }

    @Override
    public long size() {
        return -1;
    }

    public void of(
            RecordCursor base,
            SqlExecutionContext executionContext,
            Function timezoneNameFunc,
            int timezoneNameFuncPos,
            Function offsetFunc,
            int offsetFuncPos
    ) throws SqlException {
        // factory guarantees that base cursor is not empty
        timezoneNameFunc.init(base, executionContext);
        offsetFunc.init(base, executionContext);

        this.rules = null;
        this.base = base;
        this.baseRecord = base.getRecord();
        final long timestamp = baseRecord.getTimestamp(timestampIndex);

        final CharSequence tz = timezoneNameFunc.getStr(null);
        if (tz != null) {
            try {
                long opt = Timestamps.parseOffset(tz);
                if (opt == Long.MIN_VALUE) {
                    // this is timezone name
                    TimeZoneRules rules = TimestampFormatUtils.enLocale.getZoneRules(
                            Numbers.decodeLowInt(TimestampFormatUtils.enLocale.matchZone(tz, 0, tz.length())),
                            RESOLUTION_MICROS
                    );
                    // fixed rules means the timezone does not have historical or daylight time changes
                    tzOffset = rules.getOffset(timestamp);
                    if (rules instanceof TimeZoneRulesMicros) {
                        this.rules = rules;
                    }
                } else {
                    // here timezone is in numeric offset format
                    tzOffset = Numbers.decodeLowInt(opt) * MINUTE_MICROS;
                }
            } catch (NumericException e) {
                Misc.free(base);
                throw SqlException.$(timezoneNameFuncPos, "invalid timezone: ").put(tz);
            }
        } else {
            this.tzOffset = 0;
        }

        final CharSequence offset = offsetFunc.getStr(null);

        if (offset != null) {
            final long val = Timestamps.parseOffset(offset);
            if (val == Numbers.LONG_NaN) {
                // bad value for offset
                Misc.free(base);
                throw SqlException.$(offsetFuncPos, "invalid offset: ").put(offset);
            }

            this.fixedOffset = Numbers.decodeLowInt(val) * MINUTE_MICROS;
        } else {
            fixedOffset = 0;
        }

        if (tzOffset == 0 && fixedOffset == 0) {
            // this is the default path, we align time intervals to the first observation
            timestampSampler.setStart(timestamp + tzOffset);
        } else if (fixedOffset > 0) {
            timestampSampler.setStart(timestamp + tzOffset + this.fixedOffset - timestampSampler.getBucketSize());
        } else {
            timestampSampler.setStart(tzOffset);
        }
        this.topTzOffset = tzOffset;
        this.topLocalEpoch = this.localEpoch = timestampSampler.round(timestamp + tzOffset);
        interruptor = executionContext.getSqlExecutionInterruptor();
    }

    protected long getBaseRecordTimestamp() {
        return baseRecord.getTimestamp(timestampIndex);
    }

    protected class TimestampFunc extends TimestampFunction implements Function {

        @Override
        public long getTimestamp(Record rec) {
            return sampleLocalEpoch - tzOffset;
        }
    }
}
