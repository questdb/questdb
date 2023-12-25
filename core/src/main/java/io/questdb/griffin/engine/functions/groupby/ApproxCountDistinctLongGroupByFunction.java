/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Hash;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.hyperloglog.HyperLogLog;

public class ApproxCountDistinctLongGroupByFunction extends LongFunction implements UnaryFunction, GroupByFunction {
    private final Function arg;
    private final ObjList<HyperLogLog> hyperLogLogs = new ObjList<>();
    private int nextHyperLogLogIndex;
    private int valueIndex;
    private int overwrittenFlagIndex;
    private int hyperLogLogIndex;

    public ApproxCountDistinctLongGroupByFunction(Function arg) {
        this.arg = arg;
    }

    @Override
    public void clear() {
        hyperLogLogs.clear();
        nextHyperLogLogIndex = 0;
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        final HyperLogLog hyperLogLog;
        if (hyperLogLogs.size() <= nextHyperLogLogIndex) {
            hyperLogLogs.extendAndSet(nextHyperLogLogIndex, hyperLogLog = new HyperLogLog());
        } else {
            hyperLogLog = hyperLogLogs.getQuick(nextHyperLogLogIndex);
            hyperLogLog.clear();
        }

        final long val = arg.getLong(record);
        if (val != Numbers.LONG_NaN) {
            final long hash = Hash.murmur3ToLong(val);
            hyperLogLog.add(hash);
        }
        mapValue.putInt(hyperLogLogIndex, nextHyperLogLogIndex++);
        mapValue.putBool(overwrittenFlagIndex, false);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        final HyperLogLog hyperLogLog = hyperLogLogs.getQuick(mapValue.getInt(hyperLogLogIndex));
        final long val = arg.getLong(record);
        if (val != Numbers.LONG_NaN) {
            final long hash = Hash.murmur3ToLong(val);
            hyperLogLog.add(hash);
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public long getLong(Record rec) {
        if (rec.getBool(overwrittenFlagIndex)) {
            return rec.getLong(valueIndex);
        }
        if (hyperLogLogs.size() == 0) {
            return Numbers.LONG_NaN;
        }

        final HyperLogLog hyperLogLog = hyperLogLogs.getQuick(rec.getInt(hyperLogLogIndex));
        return hyperLogLog.computeCardinality();
    }

    @Override
    public String getName() {
        return "approx_count_distinct";
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isReadThreadSafe() {
        return false;
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        this.overwrittenFlagIndex = valueIndex + 1;
        this.hyperLogLogIndex = valueIndex + 2;
        columnTypes.add(ColumnType.LONG);
        columnTypes.add(ColumnType.BOOLEAN);
        columnTypes.add(ColumnType.INT);
    }

    @Override
    public void setEmpty(MapValue mapValue) {
        overwrite(mapValue, 0L);
    }

    @Override
    public void setLong(MapValue mapValue, long value) {
        overwrite(mapValue, value);
    }

    @Override
    public void setNull(MapValue mapValue) {
        overwrite(mapValue, Numbers.LONG_NaN);
    }

    private void overwrite(MapValue mapValue, long value) {
        mapValue.putLong(valueIndex, value);
        mapValue.putBool(overwrittenFlagIndex, true);
    }
}
