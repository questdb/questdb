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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionInterruptor;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;

public abstract class AbstractNoRecordSampleByCursor implements NoRandomAccessRecordCursor {
    protected final TimestampSampler timestampSampler;
    protected final int timestampIndex;
    protected final ObjList<GroupByFunction> groupByFunctions;
    private final ObjList<Function> recordFunctions;
    protected Record baseRecord;
    protected long lastTimestamp;
    protected long nextTimestamp;
    protected RecordCursor base;
    protected SqlExecutionInterruptor interruptor;
    protected long baselineOffset;

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
    }

    @Override
    public long size() {
        return -1;
    }

    public void of(RecordCursor base, SqlExecutionContext executionContext, Function timezoneNameFunc, Function offsetFunc) {
        // factory guarantees that base cursor is not empty
        long alignmentOffset = Numbers.LONG_NaN;
        final CharSequence tz = timezoneNameFunc.getStr(null);
        if (tz != null) {
            try {
                alignmentOffset = Timestamps.toTimezone(0, TimestampFormatUtils.enLocale, tz);
            } catch (NumericException e) {
                throw CairoException.instance(0);
            }
        }

        long offset = offsetFunc.getLong(null);

        if (offset != Numbers.LONG_NaN) {
            if (alignmentOffset == Numbers.LONG_NaN) {
                alignmentOffset = offset;
            } else {
                alignmentOffset += offset;
            }
        }

        this.base = base;
        this.baseRecord = base.getRecord();
        final long timestamp = baseRecord.getTimestamp(timestampIndex);
        this.nextTimestamp = timestampSampler.round(timestamp);
        this.baselineOffset = alignmentOffset == Numbers.LONG_NaN ? timestamp - nextTimestamp : alignmentOffset;
        this.lastTimestamp = this.nextTimestamp = timestampSampler.round(timestamp - baselineOffset);
        interruptor = executionContext.getSqlExecutionInterruptor();
    }

    protected long getBaseRecordTimestamp() {
        return timestampSampler.round(baseRecord.getTimestamp(timestampIndex) - baselineOffset);
    }

    protected class TimestampFunc extends TimestampFunction implements Function {

        @Override
        public long getTimestamp(Record rec) {
            return lastTimestamp + baselineOffset;
        }
    }
}
