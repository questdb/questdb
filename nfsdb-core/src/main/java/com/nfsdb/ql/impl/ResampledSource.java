/*******************************************************************************
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
 ******************************************************************************/

package com.nfsdb.ql.impl;


import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.collections.ObjList;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.ql.*;
import com.nfsdb.ql.collections.MapRecordValueInterceptor;
import com.nfsdb.ql.collections.MapValues;
import com.nfsdb.ql.collections.MultiMap;
import com.nfsdb.utils.Dates;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
public class ResampledSource extends AbstractImmutableIterator<Record> implements RecordSource<Record>, RecordCursor<Record> {

    private final MultiMap map;
    private final RecordSource<? extends Record> recordSource;
    private final int[] keyIndices;
    private final int tsIndex;
    private final ObjList<AggregatorFunction> aggregators;
    private final SampleBy sampleBy;
    private RecordCursor<? extends Record> recordCursor;
    private RecordCursor<Record> mapRecordSource;
    private Record nextRecord = null;

    @SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
    public ResampledSource(
            RecordSource<? extends Record> recordSource,
            ObjList<ColumnMetadata> keyColumns,
            ObjList<AggregatorFunction> aggregators,
            ColumnMetadata timestampMetadata,
            SampleBy sampleBy
    ) {

        MultiMap.Builder builder = new MultiMap.Builder();
        int keyColumnsSize = keyColumns.size();
        this.keyIndices = new int[keyColumnsSize];
        // define key columns

        RecordMetadata rm = recordSource.getMetadata();
        this.tsIndex = rm.getColumnIndex(timestampMetadata.name);
        builder.keyColumn(timestampMetadata);
        for (int i = 0; i < keyColumnsSize; i++) {
            ColumnMetadata cm = keyColumns.getQuick(i);
            builder.keyColumn(cm);
            keyIndices[i] = rm.getColumnIndex(cm.name);
        }

        this.aggregators = aggregators;

        // take value columns from aggregator function
        int index = 0;
        for (int i = 0, sz = aggregators.size(); i < sz; i++) {
            AggregatorFunction func = aggregators.getQuick(i);

            func.prepareSource(recordSource);

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
        this.recordSource = recordSource;
        this.sampleBy = sampleBy;
    }

    @Override
    public Record getByRowId(long rowId) {
        return null;
    }

    @Override
    public StorageFacade getSymFacade() {
        return recordCursor.getSymFacade();
    }

    @Override
    public RecordMetadata getMetadata() {
        return map.getMetadata();
    }

    @Override
    public RecordCursor<Record> prepareCursor(JournalReaderFactory factory) throws JournalException {
        this.recordCursor = recordSource.prepareCursor(factory);
        return this;
    }

    @Override
    public void reset() {
        recordSource.reset();
        map.clear();
    }

    @Override
    public boolean supportsRowIdAccess() {
        return false;
    }

    @Override
    public boolean hasNext() {
        return mapRecordSource != null && mapRecordSource.hasNext() || buildMap();
    }

    @Override
    public Record next() {
        return mapRecordSource.next();
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
            if (!recordCursor.hasNext()) {
                return false;
            }
            rec = recordCursor.next();
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
            MultiMap.KeyWriter keyWriter = map.keyWriter();
            keyWriter.putLong(sample);
            for (int i = 0; i < keyIndices.length; i++) {
                switch (recordSource.getMetadata().getColumnQuick(i + 1).getType()) {
                    case LONG:
                        keyWriter.putLong(rec.getLong(keyIndices[i]));
                        break;
                    case INT:
                        keyWriter.putInt(rec.getInt(keyIndices[i]));
                        break;
                    case STRING:
                        keyWriter.putStr(rec.getStr(keyIndices[i]));
                        break;
                    case SYMBOL:
                        keyWriter.putInt(rec.getInt(keyIndices[i]));
                        break;
                    default:
                        throw new JournalRuntimeException("Unsupported type: " + recordSource.getMetadata().getColumn(i + 1).getType());
                }
            }
            MapValues values = map.getOrCreateValues(keyWriter);

            for (int i = 0, sz = aggregators.size(); i < sz; i++) {
                aggregators.getQuick(i).calculate(rec, values);
            }

            if (!recordCursor.hasNext()) {
                nextRecord = null;
                break;
            }

            rec = recordCursor.next();

        } while (true);

        return (mapRecordSource = map.getCursor()).hasNext();
    }


    public enum SampleBy {
        YEAR, MONTH, DAY, HOUR, MINUTE, SECOND
    }
}
