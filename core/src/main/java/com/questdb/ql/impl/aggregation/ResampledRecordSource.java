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


import com.questdb.ex.JournalException;
import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.ql.*;
import com.questdb.ql.impl.map.MapRecordValueInterceptor;
import com.questdb.ql.impl.map.MapValues;
import com.questdb.ql.impl.map.MultiMap;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.*;
import com.questdb.std.ThreadLocal;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
public class ResampledRecordSource extends AbstractCombinedRecordSource {
    private static final ThreadLocal<ObjList<RecordColumnMetadata>> tlColumns = new ThreadLocal<>(new ObjectFactory<ObjList<RecordColumnMetadata>>() {
        @Override
        public ObjList<RecordColumnMetadata> newInstance() {
            return new ObjList<>();
        }
    });

    private final MultiMap map;
    private final RecordSource recordSource;
    private final IntList keyIndices;
    private final int tsIndex;
    private final ObjList<AggregatorFunction> aggregators;
    private final TimestampSampler sampler;
    private RecordCursor recordCursor;
    private RecordCursor mapRecordSource;
    private Record nextRecord = null;

    @SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
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

        ObjList<MapRecordValueInterceptor> interceptors = new ObjList<>();
        ObjList<RecordColumnMetadata> columns = tlColumns.get();
        columns.clear();

        // take value columns from aggregator function
        int index = 0;
        for (int i = 0, sz = aggregators.size(); i < sz; i++) {
            AggregatorFunction func = aggregators.getQuick(i);
            int n = columns.size();
            func.prepare(columns, index);
            index += columns.size() - n;
            if (func instanceof MapRecordValueInterceptor) {
                interceptors.add((MapRecordValueInterceptor) func);
            }
        }

        this.map = new MultiMap(pageSize, rm, keyCols, columns, interceptors);
        this.recordSource = recordSource;
    }

    @Override
    public RecordMetadata getMetadata() {
        return map.getMetadata();
    }

    @Override
    public RecordCursor prepareCursor(JournalReaderFactory factory, CancellationHandler cancellationHandler) throws JournalException {
        this.recordCursor = recordSource.prepareCursor(factory, cancellationHandler);
        return this;
    }

    @Override
    public void reset() {
        recordSource.reset();
        map.clear();
        nextRecord = null;
    }

    @Override
    public boolean supportsRowIdAccess() {
        return false;
    }

    @Override
    public StorageFacade getStorageFacade() {
        return recordCursor.getStorageFacade();
    }

    @Override
    public Record newRecord() {
        return null;
    }

    @Override
    public Record recordAt(long rowId) {
        return null;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
    }

    @Override
    public boolean hasNext() {
        return mapRecordSource != null && mapRecordSource.hasNext() || buildMap();
    }

    @Override
    public Record next() {
        return mapRecordSource.next();
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
            MultiMap.KeyWriter kw = map.keyWriter();
            kw.putLong(sample);
            for (int i = 0, n = keyIndices.size(); i < n; i++) {
                int index;
                kw.put(
                        rec,
                        index = keyIndices.getQuick(i),
                        recordSource.getMetadata().getColumnQuick(index).getType()
                );
            }

            MapValues values = map.getOrCreateValues(kw);
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
}
