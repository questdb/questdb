/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.ql.aggregation;


import com.questdb.ql.AggregatorFunction;
import com.questdb.ql.CancellationHandler;
import com.questdb.ql.RecordSource;
import com.questdb.ql.map.*;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.*;
import com.questdb.std.str.CharSink;
import com.questdb.store.*;
import com.questdb.store.factory.ReaderFactory;

public class ResampledRecordSource extends AbstractCombinedRecordSource {
    private final static com.questdb.std.ThreadLocal<VirtualColumnTypeResolver> tlAggregationTypeResolver = new VirtualColumnTypeResolver.ResolverThreadLocal();
    private final static MetadataNameTypeResolver.MetadataNameTypeResolverThreadLocal tlMetadataTypeResolver = new MetadataNameTypeResolver.MetadataNameTypeResolverThreadLocal();
    private final DirectMap map;
    private final RecordSource recordSource;
    private final int tsIndex;
    private final ObjList<AggregatorFunction> aggregators;
    private final TimestampSampler sampler;
    private final DirectMapMetadata metadata;
    private final DirectMapRecord record;
    private final DirectMapStorageFacade storageFacade;
    private final ObjList<MapRecordValueInterceptor> interceptors;
    private final RecordKeyCopier copier;
    private RecordCursor recordCursor;
    private DirectMapIterator mapCursor;
    private Record nextRecord = null;

    public ResampledRecordSource(
            RecordSource recordSource,
            int timestampColumnIndex,
            @Transient ObjHashSet<String> keyColumns,
            ObjList<AggregatorFunction> aggregators,
            TimestampSampler sampler,
            int pageSize,
            RecordKeyCopierCompiler compiler
    ) {
        int keyColumnsSize = keyColumns.size();
        IntList keyIndices = new IntList(keyColumnsSize);
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
        this.copier = compiler.compile(rm, keyIndices);

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
        this.map = new DirectMap(pageSize, tlMetadataTypeResolver.get().of(rm, keyCols), tlAggregationTypeResolver.get().of(columns));
        this.recordSource = recordSource;
    }

    @Override
    public void close() {
        Misc.free(map);
        Misc.free(recordSource);
        for (int i = 0, n = aggregators.size(); i < n; i++) {
            Misc.free(aggregators.getQuick(i));
        }
        aggregators.clear();
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor prepareCursor(ReaderFactory factory, CancellationHandler cancellationHandler) {
        map.clear();
        nextRecord = null;
        mapCursor = null;
        this.recordCursor = recordSource.prepareCursor(factory, cancellationHandler);
        this.storageFacade.prepare(this.recordCursor);
        return this;
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public Record newRecord() {
        return new DirectMapRecord(this.storageFacade);
    }

    @Override
    public StorageFacade getStorageFacade() {
        return storageFacade;
    }

    @Override
    public void releaseCursor() {
        this.recordCursor.releaseCursor();
    }

    @Override
    public void toTop() {
        nextRecord = null;
        mapCursor = null;
        this.recordCursor.toTop();
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
        int sz = aggregators.size();

        map.clear();
        for (int i = 0; i < sz; i++) {
            aggregators.getQuick(i).clear();
        }

        Record rec;
        while ((rec = fetchNext()) != null) {

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
            copier.copy(rec, kw);
            DirectMapValues values = map.getOrCreateValues();
            for (int i = 0; i < sz; i++) {
                aggregators.getQuick(i).calculate(rec, values);
            }
        }

        mapCursor = map.iterator();
        return mapCursor.hasNext();
    }

    private Record fetchNext() {
        if (nextRecord != null) {
            Record r = nextRecord;
            nextRecord = null;
            return r;
        } else {
            if (!recordCursor.hasNext()) {
                return null;
            }
            return recordCursor.next();
        }
    }
}
