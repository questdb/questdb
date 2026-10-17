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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.TimeZoneRules;

import java.io.Closeable;

public abstract class AbstractSampleByCursor implements NoRandomAccessRecordCursor, Closeable {
    protected final Function offsetFunc;
    protected final int offsetFuncPos;
    protected final Function sampleFromFunc;
    protected final int sampleFromFuncPos;
    protected final int sampleFromFuncType;
    protected final Function sampleToFunc;
    protected final int sampleToFuncPos;
    protected final int sampleToFuncType;
    protected final char samplingIntervalUnit;
    protected final TimestampDriver timestampDriver;
    protected final Function timezoneNameFunc;
    protected final int timezoneNameFuncPos;
    private final TimestampSampler calendarSampler;
    private final SampleByTimezone timezone = new SampleByTimezone();
    protected long fixedOffset;
    protected long localEpoch;
    protected TimestampSampler timestampSampler;
    protected long tzOffset;
    private TimezoneFloorTimestampSampler zonedSampler;

    public AbstractSampleByCursor(
            TimestampSampler timestampSampler,
            int timestampType,
            Function timezoneNameFunc,
            int timezoneNameFuncPos,
            Function offsetFunc,
            int offsetFuncPos,
            Function sampleFromFunc,
            int sampleFromFuncPos,
            Function sampleToFunc,
            int sampleToFuncPos,
            char samplingIntervalUnit
    ) {
        this.calendarSampler = timestampSampler;
        this.timestampSampler = timestampSampler;
        this.samplingIntervalUnit = samplingIntervalUnit;
        this.timezoneNameFunc = timezoneNameFunc;
        this.timezoneNameFuncPos = timezoneNameFuncPos;
        this.offsetFunc = offsetFunc;
        this.offsetFuncPos = offsetFuncPos;
        this.sampleFromFunc = sampleFromFunc;
        this.sampleFromFuncPos = sampleFromFuncPos;
        this.sampleToFunc = sampleToFunc;
        this.sampleToFuncPos = sampleToFuncPos;
        this.timestampDriver = ColumnType.getTimestampDriver(timestampType);
        this.sampleFromFuncType = ColumnType.getTimestampType(sampleFromFunc.getType());
        this.sampleToFuncType = ColumnType.getTimestampType(sampleToFunc.getType());
    }

    @Override
    public void close() {
        // The temporal parameter functions (timezone, offset, FROM, TO) are borrowed from the
        // owning factory, which closes them exactly once at teardown; per-execution cursor close
        // must leave them usable for the next execution of the cached factory.
    }

    /**
     * Anchors the calendar grid at FROM, or at OFFSET without FROM, and returns FROM, Long.MIN_VALUE without it.
     */
    protected long anchorCalendarGrid() {
        if (sampleFromFunc != timestampDriver.getTimestampConstantNull()) {
            final long from = sampleFromFunc.getTimestamp(null);
            timestampSampler.setLocalAnchor(from != Long.MIN_VALUE ? localFrom(from) : 0);
            return from;
        }
        timestampSampler.setOffset(fixedOffset != Long.MIN_VALUE ? fixedOffset : 0);
        return Long.MIN_VALUE;
    }

    // SampleByFactoryGenerator hands sub-day strides their FROM in UTC and longer strides in local time.
    protected long localFrom(long from) {
        final long timestamp = timestampDriver.from(from, sampleFromFuncType);
        return CommonUtils.isSubDayUnit(samplingIntervalUnit) ? timestamp + tzOffset : timestamp;
    }

    /**
     * Resolves TIME ZONE and OFFSET for an execution and picks the bucket grid timestamp_floor_utc floors to:
     * a sub-day stride steps in the zone's standard offset, a longer stride on the zone's local calendar.
     */
    protected void parseParams(RecordCursor base, SqlExecutionContext executionContext) throws SqlException {
        timezone.of(timezoneNameFunc, timezoneNameFuncPos, offsetFunc, offsetFuncPos, timestampDriver, base, executionContext);
        final TimeZoneRules rules = timezone.getRules();
        timestampSampler = calendarSampler;
        tzOffset = timezone.getTzOffset();
        if (rules != null) {
            if (CommonUtils.isSubDayUnit(samplingIntervalUnit)) {
                tzOffset = rules.getStandardOffset();
            } else {
                if (zonedSampler == null) {
                    zonedSampler = new TimezoneFloorTimestampSampler(calendarSampler, rules, samplingIntervalUnit);
                } else {
                    zonedSampler.setTzRules(rules);
                }
                timestampSampler = zonedSampler;
            }
        }
        fixedOffset = timezone.getFixedOffset();
    }
}
