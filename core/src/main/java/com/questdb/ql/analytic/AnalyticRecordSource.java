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

package com.questdb.ql.analytic;

import com.questdb.ql.CancellationHandler;
import com.questdb.ql.CollectionRecordMetadata;
import com.questdb.ql.RecordSource;
import com.questdb.ql.SplitRecordMetadata;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.Misc;
import com.questdb.std.ObjList;
import com.questdb.std.str.CharSink;
import com.questdb.store.Record;
import com.questdb.store.RecordCursor;
import com.questdb.store.RecordMetadata;
import com.questdb.store.StorageFacade;
import com.questdb.store.factory.ReaderFactory;

public class AnalyticRecordSource extends AbstractCombinedRecordSource {

    private final RecordSource delegate;
    private final ObjList<AnalyticFunction> functions;
    private final RecordMetadata metadata;
    private final AnalyticRecord record;
    private final AnalyticRecordStorageFacade storageFacade;
    private final int split;
    private RecordCursor cursor;

    public AnalyticRecordSource(
            RecordSource delegate,
            ObjList<AnalyticFunction> functionGroups) {
        this.delegate = delegate;

        // create our metadata and also flatten functions for our record representation
        CollectionRecordMetadata funcMetadata = new CollectionRecordMetadata();
        this.functions = functionGroups;
        for (int j = 0; j < functionGroups.size(); j++) {
            funcMetadata.add(functionGroups.getQuick(j).getMetadata());
        }

        this.metadata = new SplitRecordMetadata(delegate.getMetadata(), funcMetadata);
        this.split = delegate.getMetadata().getColumnCount();
        this.record = new AnalyticRecord(split, functions);
        this.storageFacade = new AnalyticRecordStorageFacade(split, functions);
    }

    @Override
    public void close() {
        Misc.free(delegate);
        for (int i = 0, n = functions.size(); i < n; i++) {
            Misc.free(functions.getQuick(i));
        }
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor prepareCursor(ReaderFactory factory, CancellationHandler cancellationHandler) {
        this.cursor = delegate.prepareCursor(factory, cancellationHandler);
        this.storageFacade.prepare(cursor.getStorageFacade());
        prepareFunctions();
        return this;
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public Record newRecord() {
        return new AnalyticRecord(split, functions);
    }

    @Override
    public StorageFacade getStorageFacade() {
        return storageFacade;
    }

    @Override
    public void releaseCursor() {
        this.cursor.releaseCursor();
    }

    @Override
    public void toTop() {
        this.cursor.toTop();
        for (int i = 0, n = functions.size(); i < n; i++) {
            functions.getQuick(i).toTop();
        }
    }

    @Override
    public boolean hasNext() {
        if (cursor.hasNext()) {
            record.of(cursor.next());
            for (int i = 0, n = functions.size(); i < n; i++) {
                functions.getQuick(i).prepareFor(record);
            }
            return true;
        }
        return false;
    }

    @Override
    public Record next() {
        return record;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("AnalyticRecordSource").put(',');
        sink.putQuoted("functions").put(':').put(functions.size()).put(',');
        sink.putQuoted("src").put(':').put(delegate);
        sink.put('}');
    }

    private void prepareFunctions() {
        for (int i = 0, n = functions.size(); i < n; i++) {
            AnalyticFunction f = functions.getQuick(i);
            f.reset();
            f.prepare(cursor);
        }
    }

}
