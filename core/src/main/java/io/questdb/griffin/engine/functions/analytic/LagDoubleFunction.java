/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.functions.analytic;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.engine.analytic.AnalyticFunction;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;

import java.io.Closeable;

class LagDoubleFunction extends DoubleFunction implements ScalarFunction, AnalyticFunction, Closeable {
    private final Map map;
    private final VirtualRecord partitionByRecord;
    private final RecordSink partitionBySink;
    private final DoubleFunction base;
    private final int lag;
    private int columnIndex;

    public LagDoubleFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, DoubleFunction base, int lag) {
        this.map = map;
        this.partitionByRecord = partitionByRecord;
        this.partitionBySink = partitionBySink;
        this.base = base;
        this.lag = lag;
    }

    @Override
    public void close() {
        Misc.free(map);
        Misc.free(partitionByRecord.getFunctions());
    }

    @Override
    public double getDouble(Record rec) {
        // not called
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isReadThreadSafe() {
        return false;
    }

    @Override
    public void pass1(Record record, long recordOffset, AnalyticSPI spi) {
        partitionByRecord.of(record);
        MapKey key = map.withKey();
        key.put(partitionByRecord, partitionBySink);
        MapValue value = key.createValue();
        if(value.isNew()) {
            // initialize all lags with NaN
            for(int i=0;i<this.lag;i++) {
                value.putDouble(i, Double.NaN);
            }
        }
        double cVal = base.getDouble(record);

        // index 0 always keeps the oldest entry which we want to get
        double oVal = value.getDouble(0);
        // shift left (i.e. make room for "latest" value to be added at right side)
        for(int i=1;i<this.lag;i++) {
            value.putDouble(i-1, value.getDouble(i));
        }

        // set the last queue item on right
        value.putDouble(this.lag - 1, cVal);

        Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), oVal);
    }

    @Override
    public void preparePass2(RecordCursor cursor) {
    }

    @Override
    public void pass2(Record record) {
    }

    @Override
    public void reset() {
        map.clear();
    }

    @Override
    public void setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    @Override
    public boolean supportsRandomAccess() {
        return false;
    }
}
