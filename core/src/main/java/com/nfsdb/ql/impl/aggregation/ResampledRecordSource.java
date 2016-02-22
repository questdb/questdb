/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

package com.nfsdb.ql.impl.aggregation;


import com.nfsdb.ex.JournalException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.ql.*;
import com.nfsdb.ql.impl.join.hash.KeyWriterHelper;
import com.nfsdb.ql.impl.map.MapRecordValueInterceptor;
import com.nfsdb.ql.impl.map.MapValues;
import com.nfsdb.ql.impl.map.MultiMap;
import com.nfsdb.ql.ops.AbstractRecordSource;
import com.nfsdb.std.*;
import com.nfsdb.std.ThreadLocal;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
public class ResampledRecordSource extends AbstractRecordSource {
    private static final com.nfsdb.std.ThreadLocal<ObjList<RecordColumnMetadata>> tlColumns = new ThreadLocal<>(new ObjectFactory<ObjList<RecordColumnMetadata>>() {
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
            @Transient ObjHashSet<String> keyColumns,
            ObjList<AggregatorFunction> aggregators,
            TimestampSampler sampler
    ) {
        int keyColumnsSize = keyColumns.size();
        this.keyIndices = new IntList(keyColumnsSize);
        // define key columns

        ObjHashSet<String> keyCols = new ObjHashSet<>();

        RecordMetadata rm = recordSource.getMetadata();
        this.tsIndex = rm.getTimestampIndex();
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

        this.map = new MultiMap(rm, keyCols, columns, interceptors);
        this.recordSource = recordSource;
    }

    @Override
    public Record getByRowId(long rowId) {
        return null;
    }

    @Override
    public StorageFacade getStorageFacade() {
        return recordCursor.getStorageFacade();
    }

    @Override
    public RecordMetadata getMetadata() {
        return map.getMetadata();
    }

    @Override
    public RecordCursor prepareCursor(JournalReaderFactory factory) throws JournalException {
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
                KeyWriterHelper.setKey(
                        kw,
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
