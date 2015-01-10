/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.lang.cst.impl.agg;


import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.collections.mmap.MapRecordSource;
import com.nfsdb.collections.mmap.MapRecordValueInterceptor;
import com.nfsdb.collections.mmap.MapValues;
import com.nfsdb.collections.mmap.MultiMap;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.lang.cst.impl.qry.GenericRecordSource;
import com.nfsdb.lang.cst.impl.qry.Record;
import com.nfsdb.lang.cst.impl.qry.RecordMetadata;
import com.nfsdb.lang.cst.impl.qry.RecordSource;
import com.nfsdb.utils.Dates;

import java.util.List;

public class ResampledSource extends AbstractImmutableIterator<Record> implements GenericRecordSource {

    private final MultiMap map;
    private final RecordSource<? extends Record> rowSource;
    private final int[] keyIndices;
    private final int tsIndex;
    private final List<AggregatorFunction> aggregators;
    private final SampleBy sampleBy;
    private MapRecordSource mapRecordSource;
    private Record nextRecord = null;

    public ResampledSource(RecordSource<? extends Record> rowSource, List<ColumnMetadata> keyColumns, List<AggregatorFunction> aggregators, ColumnMetadata timestampMetadata, SampleBy sampleBy) {

        MultiMap.Builder builder = new MultiMap.Builder();
        int keyColumnsSize = keyColumns.size();
        this.keyIndices = new int[keyColumnsSize];
        // define key columns

        this.tsIndex = rowSource.getMetadata().getColumnIndex(timestampMetadata.name);
        builder.keyColumn(timestampMetadata);
        for (int i = 0; i < keyColumnsSize; i++) {
            builder.keyColumn(keyColumns.get(i));
            keyIndices[i] = rowSource.getMetadata().getColumnIndex(keyColumns.get(i).name);
        }

        this.aggregators = aggregators;

        // take value columns from aggregator function
        int index = 0;
        for (int i = 0, sz = aggregators.size(); i < sz; i++) {
            AggregatorFunction func = aggregators.get(i);

            func.prepareSource(rowSource);

            ColumnMetadata[] columns = func.getColumns();
            for (int k = 0, len = columns.length; k < len; k++) {
                builder.valueColumn(columns[k]);
                func.mapColumn(k, index++);
            }

            if (func instanceof MapRecordValueInterceptor) {
                builder.interceptor((MapRecordValueInterceptor) func);
            }
        }

        this.map = builder.build();
        this.rowSource = rowSource;
        this.sampleBy = sampleBy;
    }

    @Override
    public void reset() {
        rowSource.reset();
        map.clear();
    }

    @Override
    public RecordMetadata getMetadata() {
        return map.getMetadata();
    }

    @Override
    public boolean hasNext() {
        return mapRecordSource != null && mapRecordSource.hasNext() || buildMap();
    }

    private boolean buildMap() {

        long current = 0;
        long sample;
        boolean first = true;
        Record rec;


        map.clear();

        if (nextRecord != null) {
            rec = nextRecord;
        } else {
            if (!rowSource.hasNext()) {
                return false;
            }
            rec = rowSource.next();
        }

        do {
            switch (sampleBy) {
                case YEAR:
                    sample = Dates.floorYYYY(rec.getLong(tsIndex));
                    break;
                case MONTH:
                    sample = Dates.floorMM(rec.getLong(tsIndex));
                    break;
                case DAY:
                    sample = Dates.floorDD(rec.getLong(tsIndex));
                    break;
                case HOUR:
                    sample = Dates.floorHH(rec.getLong(tsIndex));
                    break;
                case MINUTE:
                    sample = Dates.floorMI(rec.getLong(tsIndex));
                    break;
                default:
                    sample = 0;
            }

            if (first) {
                current = sample;
                first = false;
            } else if (sample != current) {
                nextRecord = rec;
                break;
            }

            // we are inside of time window, compute aggregates
            MultiMap.Key key = map.claimKey();
            key.putLong(sample);
            for (int i = 0; i < keyIndices.length; i++) {
                switch (rowSource.getMetadata().getColumnType(i + 1)) {
                    case LONG:
                        key.putLong(rec.getLong(keyIndices[i]));
                        break;
                    case INT:
                        key.putInt(rec.getInt(keyIndices[i]));
                        break;
                    case STRING:
                        key.putStr(rec.getStr(keyIndices[i]));
                        break;
                    case SYMBOL:
                        key.putInt(rec.getInt(keyIndices[i]));
                        break;
                    default:
                        throw new JournalRuntimeException("Unsupported type: " + rowSource.getMetadata().getColumnType(i + 1));
                }
            }
            key.commit();

            MapValues values = map.claimSlot(key);

            for (int i = 0, sz = aggregators.size(); i < sz; i++) {
                aggregators.get(i).calculate(rec, values);
            }

            if (!rowSource.hasNext()) {
                nextRecord = null;
                break;
            }

            rec = rowSource.next();

        } while (true);

        return (mapRecordSource = map.getRecordSource()).hasNext();
    }

    @Override
    public Record next() {
        return mapRecordSource.next();
    }


    public static enum SampleBy {
        YEAR, MONTH, DAY, HOUR, MINUTE, SECOND
    }
}
