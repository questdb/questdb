/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.ql.impl.aggregation;


import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Misc;
import com.questdb.ql.*;
import com.questdb.ql.impl.map.*;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.*;

public class ResampledRecordSource extends AbstractCombinedRecordSource {
    private final DirectMap map;
    private final RecordSource recordSource;
    private final IntList keyIndices;
    private final int tsIndex;
    private final ObjList<AggregatorFunction> aggregators;
    private final TimestampSampler sampler;
    private final DirectMapMetadata metadata;
    private final DirectMapRecord record;
    private final DirectMapStorageFacade storageFacade;
    private final ObjList<MapRecordValueInterceptor> interceptors;
    private RecordCursor recordCursor;
    private DirectMapIterator mapCursor;
    private Record nextRecord = null;

    public ResampledRecordSource(
            RecordSource recordSource,
            int timestampColumnIndex,
            @Transient ObjHashSet<String> keyColumns,
            ObjList<AggregatorFunction> aggregators,
            TimestampSampler sampler,
            int pageSize
    ) {
        int keyColumnsSize = keyColumns.size();
        this.keyIndices = new IntList(keyColumnsSize);
        // define key columns

        ObjHashSet<String> keyCols = new ObjHashSet<>();

        RecordMetadata rm = recordSource.getMetadata();
        this.tsIndex = timestampColumnIndex;
        keyCols.add(rm.getColumnName(tsIndex));
        for (int i = 0; i < keyColumnsSize; i++) {
            keyCols.add(keyColumns.get(i));
            int index = rm.getColumnIndex(keyColumns.get(i));
            if (index != tsIndex) {
                keyIndices.add(index);
            }
        }

        this.aggregators = aggregators;
        this.sampler = sampler;

        ObjList<MapRecordValueInterceptor> interceptors = null;
        ObjList<RecordColumnMetadata> columns = AggregationUtils.TL_COLUMNS.get();
        columns.clear();

        // take value columns from aggregator function
        int index = 0;
        for (int i = 0, sz = aggregators.size(); i < sz; i++) {
            AggregatorFunction func = aggregators.getQuick(i);
            int n = columns.size();
            func.prepare(columns, index);
            index += columns.size() - n;
            if (func instanceof MapRecordValueInterceptor) {
                if (interceptors == null) {
                    interceptors = new ObjList<>();
                }
                interceptors.add((MapRecordValueInterceptor) func);
            }
        }

        this.interceptors = interceptors;
        // columns + 1 is because we intrinsically add "sample" as the first column to key writer
        this.storageFacade = new DirectMapStorageFacade(columns.size() + 1, keyIndices);
        this.metadata = new DirectMapMetadata(rm, keyCols, columns);
        this.record = new DirectMapRecord(this.storageFacade);
        this.map = new DirectMap(pageSize, keyCols.size(), AggregationUtils.toThreadLocalTypes(columns));
        this.recordSource = recordSource;
    }

    @Override
    public void close() {
        Misc.free(map);
        Misc.free(recordSource);
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor prepareCursor(JournalReaderFactory factory, CancellationHandler cancellationHandler) {
        map.clear();
        nextRecord = null;
        this.recordCursor = recordSource.prepareCursor(factory, cancellationHandler);
        this.storageFacade.prepare(this.recordCursor);
        return this;
    }

    @Override
    public StorageFacade getStorageFacade() {
        return storageFacade;
    }

    @Override
    public boolean hasNext() {
        return mapCursor != null && mapCursor.hasNext() || buildMap();
    }

    @Override
    public Record next() {
        DirectMapEntry entry = mapCursor.next();
        if (interceptors != null) {
            for (int i = 0, n = interceptors.size(); i < n; i++) {
                interceptors.getQuick(i).beforeRecord(entry.values());
            }
        }
        return record.of(entry);
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("ResampledRecordSource").put(',');
        sink.putQuoted("src").put(':').put(recordSource).put(',');
        sink.putQuoted("sampler").put(':').put(sampler);
        sink.put('}');
    }

    private boolean buildMap() {

        long current = 0;
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
            long sample = sampler.resample(rec.getLong(tsIndex));
            if (first) {
                current = sample;
                first = false;
            } else if (sample != current) {
                nextRecord = rec;
                break;
            }

            // we are inside of time window, compute aggregates
            DirectMap.KeyWriter kw = map.keyWriter();
            kw.putLong(sample);
            for (int i = 0, n = keyIndices.size(); i < n; i++) {
                int index;
                MapUtils.putRecord(kw, rec,
                        index = keyIndices.getQuick(i),
                        recordSource.getMetadata().getColumnQuick(index).getType());
            }

            DirectMapValues values = map.getOrCreateValues(kw);
            for (int i = 0, sz = aggregators.size(); i < sz; i++) {
                aggregators.getQuick(i).calculate(rec, values);
            }

            if (!recordCursor.hasNext()) {
                nextRecord = null;
                break;
            }

            rec = recordCursor.next();

        } while (true);

        mapCursor = map.iterator();
        return mapCursor.hasNext();
    }
}
