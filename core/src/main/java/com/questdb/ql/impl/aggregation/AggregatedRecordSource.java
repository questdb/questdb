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
import com.questdb.misc.Misc;
import com.questdb.ql.*;
import com.questdb.ql.impl.map.*;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.*;
import com.questdb.store.ColumnType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

@SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
public class AggregatedRecordSource extends AbstractCombinedRecordSource implements Closeable {

    private final DirectMap map;
    private final RecordSource recordSource;
    private final IntList keyIndices;
    private final ObjList<AggregatorFunction> aggregators;
    private final RecordMetadata metadata;
    private final DirectMapStorageFacade storageFacade;
    private final DirectMapRecord record;
    private final ObjList<MapRecordValueInterceptor> interceptors;
    private RecordCursor recordCursor;
    private Iterator<DirectMapEntry> mapCursor;

    @SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
    public AggregatedRecordSource(
            RecordSource recordSource,
            @Transient ObjHashSet<String> keyColumns,
            ObjList<AggregatorFunction> aggregators,
            int pageSize
    ) {
        int keyColumnsSize = keyColumns.size();
        this.keyIndices = new IntList(keyColumnsSize);
        this.aggregators = aggregators;

        RecordMetadata rm = recordSource.getMetadata();
        for (int i = 0; i < keyColumnsSize; i++) {
            keyIndices.add(rm.getColumnIndex(keyColumns.get(i)));
        }

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
        this.metadata = new DirectMapMetadata(rm, keyColumns, columns);
        this.storageFacade = new DirectMapStorageFacade(columns.size(), keyIndices);

        ObjList<ColumnType> types = AggregationUtils.TL_COLUMN_TYPES.get();
        types.clear();
        for (int i = 0, n = columns.size(); i < n; i++) {
            types.add(columns.getQuick(i).getType());
        }

        this.map = new DirectMap(pageSize, keyColumnsSize, types);
        this.recordSource = recordSource;
        this.record = new DirectMapRecord(this.metadata, storageFacade);
    }

    @Override
    public void close() throws IOException {
        Misc.free(this.map);
        Misc.free(recordSource);
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor prepareCursor(JournalReaderFactory factory, CancellationHandler cancellationHandler) throws JournalException {
        this.recordCursor = recordSource.prepareCursor(factory, cancellationHandler);
        this.storageFacade.prepare(this.recordCursor);
        buildMap(cancellationHandler);
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
        return mapCursor.hasNext();
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
        sink.putQuoted("op").put(':').putQuoted("AggregatedRecordSource").put(',');
        sink.putQuoted("src").put(':').put(recordSource);
        sink.put('}');
    }

    private void buildMap(CancellationHandler cancellationHandler) {

        while (recordCursor.hasNext()) {

            cancellationHandler.check();

            Record rec = recordCursor.next();

            // we are inside of time window, compute aggregates
            DirectMap.KeyWriter keyWriter = map.keyWriter();
            for (int i = 0; i < keyIndices.size(); i++) {
                int index;
                MapUtils.putRecord(keyWriter, rec, index = keyIndices.getQuick(i),
                        recordSource.getMetadata().getColumnQuick(index).getType());
            }

            DirectMapValues values = map.getOrCreateValues(keyWriter);
            for (int i = 0, sz = aggregators.size(); i < sz; i++) {
                aggregators.getQuick(i).calculate(rec, values);
            }
        }
        mapCursor = map.iterator();
    }
}
