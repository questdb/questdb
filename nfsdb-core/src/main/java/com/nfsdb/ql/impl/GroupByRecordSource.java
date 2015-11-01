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
import com.nfsdb.collections.Transient;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.ql.*;
import com.nfsdb.ql.collections.MapRecordValueInterceptor;
import com.nfsdb.ql.collections.MapValues;
import com.nfsdb.ql.collections.MultiMap;
import com.nfsdb.utils.Misc;
import com.nfsdb.utils.Unsafe;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Closeable;
import java.io.IOException;

@SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
public class GroupByRecordSource extends AbstractImmutableIterator<Record> implements RecordSource<Record>, RecordCursor<Record>, Closeable {

    private final MultiMap map;
    private final RecordSource<? extends Record> recordSource;
    private final int[] keyIndices;
    private final ObjList<AggregatorFunction> aggregators;
    private RecordCursor<? extends Record> recordCursor;
    private RecordCursor<Record> mapRecordSource;

    @SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
    public GroupByRecordSource(
            RecordSource<? extends Record> recordSource,
            @Transient ObjList<CharSequence> keyColumns,
            ObjList<AggregatorFunction> aggregators
    ) {
        int keyColumnsSize = keyColumns.size();
        this.keyIndices = new int[keyColumnsSize];

        // define key columns
        ObjList<RecordColumnMetadata> keyCols = new ObjList<>();

        RecordMetadata rm = recordSource.getMetadata();
        for (int i = 0; i < keyColumnsSize; i++) {
            RecordColumnMetadata cm = rm.getColumn(keyColumns.getQuick(i));
            keyCols.add(cm);
            keyIndices[i] = rm.getColumnIndex(cm.getName());
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

        this.map = new MultiMap(valueCols, keyCols, interceptors);
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
                int index = Unsafe.arrayGet(keyIndices, i);
                switch (recordSource.getMetadata().getColumnQuick(index).getType()) {
                    case LONG:
                        keyWriter.putLong(rec.getLong(index));
                        break;
                    case INT:
                        keyWriter.putInt(rec.getInt(index));
                        break;
                    case STRING:
                        keyWriter.putStr(rec.getStr(index));
                        break;
                    case SYMBOL:
                        keyWriter.putInt(rec.getInt(index));
                        break;
                    default:
                        throw new JournalRuntimeException("Unsupported type: " + recordSource.getMetadata().getColumnQuick(index).getType());
                }
            }

            MapValues values = map.getOrCreateValues(keyWriter);

            for (int i = 0, sz = aggregators.size(); i < sz; i++) {
                aggregators.getQuick(i).calculate(rec, values);
            }
        }
        mapRecordSource = map.getCursor();
    }
}
