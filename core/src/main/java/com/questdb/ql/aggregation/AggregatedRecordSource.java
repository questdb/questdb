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

import java.io.Closeable;
import java.util.Iterator;

public class AggregatedRecordSource extends AbstractCombinedRecordSource implements Closeable {

    private final static VirtualColumnTypeResolver.ResolverThreadLocal tlVirtualColumnTypeResolver = new VirtualColumnTypeResolver.ResolverThreadLocal();
    private final static MetadataTypeResolver.MetadataTypeResolverThreadLocal tlMetadataTypeResolver = new MetadataTypeResolver.MetadataTypeResolverThreadLocal();
    private final DirectMap map;
    private final RecordSource recordSource;
    private final ObjList<AggregatorFunction> aggregators;
    private final RecordMetadata metadata;
    private final DirectMapStorageFacade storageFacade;
    private final DirectMapRecord record;
    private final ObjList<MapRecordValueInterceptor> interceptors;
    private final RecordKeyCopier copier;
    private ObjList<MapRecordValueInterceptor> interceptorWorkingSet;
    private RecordCursor cursor;
    private Iterator<DirectMapEntry> mapCursor;

    public AggregatedRecordSource(
            RecordSource recordSource,
            @Transient ObjHashSet<String> keyColumns,
            ObjList<AggregatorFunction> aggregators,
            int pageSize,
            RecordKeyCopierCompiler compiler
    ) {
        int keyColumnsSize = keyColumns.size();
        IntList keyIndices = new IntList(keyColumnsSize);
        this.aggregators = aggregators;

        RecordMetadata rm = recordSource.getMetadata();
        for (int i = 0; i < keyColumnsSize; i++) {
            keyIndices.add(rm.getColumnIndex(keyColumns.get(i)));
        }

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
        this.metadata = new DirectMapMetadata(rm, keyColumns, columns);
        this.storageFacade = new DirectMapStorageFacade(columns.size(), keyIndices);
        this.map = new DirectMap(pageSize, tlMetadataTypeResolver.get().of(rm, keyIndices), tlVirtualColumnTypeResolver.get().of(columns));
        this.recordSource = recordSource;
        this.record = new DirectMapRecord(storageFacade);
    }

    @Override
    public void close() {
        Misc.free(this.map);
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
        this.interceptorWorkingSet = interceptors;
        map.clear();
        this.cursor = recordSource.prepareCursor(factory, cancellationHandler);
        this.storageFacade.prepare(this.cursor);
        buildMap(cancellationHandler);
        return this;
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public Record newRecord() {
        return new DirectMapRecord(storageFacade);
    }

    @Override
    public StorageFacade getStorageFacade() {
        return storageFacade;
    }

    @Override
    public void releaseCursor() {
        this.map.clear();
        this.cursor.releaseCursor();
    }

    @Override
    public void toTop() {
        interceptorWorkingSet = null;
        mapCursor = map.iterator();
    }

    @Override
    public boolean hasNext() {
        return mapCursor.hasNext();
    }

    @Override
    public Record next() {
        DirectMapEntry entry = mapCursor.next();
        if (interceptorWorkingSet != null) {
            notifyInterceptors(entry);
        }
        return record.of(entry);
    }

    @Override
    public Record recordAt(long rowId) {
        recordAt(record, rowId);
        return record;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        DirectMapEntry entry = map.entryAt(atRowId);
        if (interceptorWorkingSet != null) {
            notifyInterceptors(entry);
        }
        ((DirectMapRecord) record).of(entry);
    }

    @Override
    public boolean supportsRowIdAccess() {
        return true;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("AggregatedRecordSource").put(',');
        sink.putQuoted("src").put(':').put(recordSource);
        sink.put('}');
    }

    private void buildMap(CancellationHandler cancellationHandler) {

        int sz = aggregators.size();
        while (cursor.hasNext()) {
            cancellationHandler.check();
            Record r = cursor.next();
            map.locate(copier, r);
            DirectMapValues values = map.getOrCreateValues();
            for (int i = 0; i < sz; i++) {
                aggregators.getQuick(i).calculate(r, values);
            }
        }
        mapCursor = map.iterator();
    }

    private void notifyInterceptors(DirectMapEntry entry) {
        for (int i = 0, n = interceptorWorkingSet.size(); i < n; i++) {
            interceptors.getQuick(i).beforeRecord(entry.values());
        }
    }
}
