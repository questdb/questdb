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

package com.nfsdb.ql.impl.aggregation;


import com.nfsdb.ex.JournalException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.misc.Misc;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.ql.*;
import com.nfsdb.ql.impl.join.hash.KeyWriterHelper;
import com.nfsdb.ql.impl.map.MapRecordValueInterceptor;
import com.nfsdb.ql.impl.map.MapValues;
import com.nfsdb.ql.impl.map.MultiMap;
import com.nfsdb.std.AbstractImmutableIterator;
import com.nfsdb.std.ObjHashSet;
import com.nfsdb.std.ObjList;
import com.nfsdb.std.Transient;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Closeable;
import java.io.IOException;

@SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
public class AggregatedRecordSource extends AbstractImmutableIterator<Record> implements RecordSource<Record>, RecordCursor<Record>, Closeable {

    private final MultiMap map;
    private final RecordSource<? extends Record> recordSource;
    private final int[] keyIndices;
    private final ObjList<AggregatorFunction> aggregators;
    private RecordCursor<? extends Record> recordCursor;
    private RecordCursor<Record> mapRecordSource;

    @SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
    public AggregatedRecordSource(
            RecordSource<? extends Record> recordSource,
            @Transient ObjHashSet<String> keyColumns,
            ObjList<AggregatorFunction> aggregators
    ) {
        int keyColumnsSize = keyColumns.size();
        this.keyIndices = new int[keyColumnsSize];

        RecordMetadata rm = recordSource.getMetadata();
        for (int i = 0; i < keyColumnsSize; i++) {
            keyIndices[i] = rm.getColumnIndex(keyColumns.get(i));
        }

        this.aggregators = aggregators;

        ObjList<RecordColumnMetadata> valueCols = new ObjList<>();
        ObjList<MapRecordValueInterceptor> interceptors = new ObjList<>();

        // take value columns from aggregator function
        int index = 0;
        for (int i = 0, sz = aggregators.size(); i < sz; i++) {
            AggregatorFunction func = aggregators.getQuick(i);
            RecordColumnMetadata[] columns = func.getColumns();
            for (int k = 0, len = columns.length; k < len; k++) {
                valueCols.add(columns[k]);
                func.mapColumn(k, index++);
            }

            if (func instanceof MapRecordValueInterceptor) {
                interceptors.add((MapRecordValueInterceptor) func);
            }
        }
        this.map = new MultiMap(rm, keyColumns, valueCols, interceptors);
        this.recordSource = recordSource;
    }

    @Override
    public void close() throws IOException {
        Misc.free(this.map);
        Misc.free(recordSource);
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
    public RecordCursor<Record> prepareCursor(JournalReaderFactory factory) throws JournalException {
        this.recordCursor = recordSource.prepareCursor(factory);
        buildMap();
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
        return mapRecordSource.hasNext();
    }

    @Override
    public Record next() {
        return mapRecordSource.next();
    }

    private void buildMap() {

        while (recordCursor.hasNext()) {

            Record rec = recordCursor.next();

            // we are inside of time window, compute aggregates
            MultiMap.KeyWriter keyWriter = map.keyWriter();
            for (int i = 0; i < keyIndices.length; i++) {
                int index;
                KeyWriterHelper.setKey(
                        keyWriter,
                        rec,
                        index = Unsafe.arrayGet(keyIndices, i),
                        recordSource.getMetadata().getColumnQuick(index).getType()
                );
            }

            MapValues values = map.getOrCreateValues(keyWriter);

            for (int i = 0, sz = aggregators.size(); i < sz; i++) {
                aggregators.getQuick(i).calculate(rec, values);
            }
        }
        mapRecordSource = map.getCursor();
    }
}
