/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.ql.ops;

import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.ql.AggregatorFunction;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordSource;
import com.nfsdb.ql.collections.MapRecordValueInterceptor;
import com.nfsdb.ql.collections.MapValues;
import com.nfsdb.storage.ColumnType;

public class AvgDoubleAggregationFunction implements AggregatorFunction, MapRecordValueInterceptor {

    private final ColumnMetadata sourceColumn;
    private int columnIndex;
    private int countIdx;
    private int sumIdx;
    private int avgIdx;

    public AvgDoubleAggregationFunction(ColumnMetadata sourceColumn) {
        this.sourceColumn = sourceColumn;
    }

    @Override
    public void beforeRecord(MapValues values) {
        values.putDouble(avgIdx, values.getDouble(sumIdx) / values.getLong(countIdx));
    }

    @Override
    public void calculate(Record rec, MapValues values) {
        if (values.isNew()) {
            values.putLong(countIdx, 1);
            values.putDouble(sumIdx, rec.getDouble(columnIndex));
        } else {
            values.putLong(countIdx, values.getLong(countIdx) + 1);
            values.putDouble(sumIdx, values.getDouble(sumIdx) + rec.getDouble(columnIndex));
        }
    }

    @Override
    public ColumnMetadata[] getColumns() {
        return new ColumnMetadata[]{
                new ColumnMetadata().setName("$count").setType(ColumnType.LONG)
                , new ColumnMetadata().setName("$sum").setType(ColumnType.DOUBLE)
                , new ColumnMetadata().setName("avg").setType(ColumnType.DOUBLE)
        };
    }

    @Override
    public void mapColumn(int k, int i) {
        switch (k) {
            case 0:
                countIdx = i;
                break;
            case 1:
                sumIdx = i;
                break;
            case 2:
                avgIdx = i;
                break;
            default:
                throw new JournalRuntimeException("Internal bug. Column mismatch");
        }
    }

    @Override
    public void prepareSource(RecordSource<? extends Record> source) {
        this.columnIndex = source.getMetadata().getColumnIndex(sourceColumn.name);
    }
}
