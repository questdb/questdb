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
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionInterruptor;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.std.ObjList;

public abstract class AbstractNoRecordSampleByCursor implements DelegatingRecordCursor, NoRandomAccessRecordCursor {
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

    protected long getBaseRecordTimestamp() {
        return timestampSampler.round(baseRecord.getTimestamp(timestampIndex) - baselineOffset);
    }

    @Override
    public void of(RecordCursor base, SqlExecutionContext executionContext) {
        // factory guarantees that base cursor is not empty
        this.base = base;
        this.baseRecord = base.getRecord();
        final long timestamp = baseRecord.getTimestamp(timestampIndex);
        this.nextTimestamp = timestampSampler.round(timestamp);
        this.baselineOffset = timestamp - nextTimestamp;
        this.lastTimestamp = this.nextTimestamp;
        interruptor = executionContext.getSqlExecutionInterruptor();
    }

    protected class TimestampFunc extends TimestampFunction implements Function {

        public TimestampFunc(int position) {
            super(position);
        }

        @Override
        public long getTimestamp(Record rec) {
            return lastTimestamp + baselineOffset;
        }
    }
}
