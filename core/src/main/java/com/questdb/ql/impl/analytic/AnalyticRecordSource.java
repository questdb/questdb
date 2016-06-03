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

package com.questdb.ql.impl.analytic;

import com.questdb.ex.JournalException;
import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.ql.*;
import com.questdb.ql.impl.CollectionRecordMetadata;
import com.questdb.ql.impl.RecordList;
import com.questdb.ql.impl.SplitRecordMetadata;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.CharSink;
import com.questdb.std.ObjList;

public class AnalyticRecordSource extends AbstractCombinedRecordSource {
    private final RecordList records;
    private final RecordSource parentSource;
    private final ObjList<AnalyticFunction> functions;
    private final RecordMetadata metadata;
    private final AnalyticRecord record;

    public AnalyticRecordSource(int pageSize, RecordSource parentSource, ObjList<AnalyticFunction> functions) {
        this.parentSource = parentSource;
        this.records = new RecordList(parentSource.getMetadata(), pageSize);
        this.functions = functions;

        CollectionRecordMetadata funcMetadata = new CollectionRecordMetadata();
        for (int i = 0; i < functions.size(); i++) {
            funcMetadata.add(functions.getQuick(i).getMetadata());
        }
        this.metadata = new SplitRecordMetadata(parentSource.getMetadata(), funcMetadata);
        this.record = new AnalyticRecord(this.metadata, parentSource.getMetadata().getColumnCount(), functions);
    }

    @Override
    public Record getByRowId(long rowId) {
        return records.getByRowId(rowId);
    }

    @Override
    public StorageFacade getStorageFacade() {
        return null;
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor prepareCursor(JournalReaderFactory factory, CancellationHandler cancellationHandler) throws JournalException {
        RecordCursor cursor = this.parentSource.prepareCursor(factory, cancellationHandler);
        int n = functions.size();
        long rowid = -1;
        while (cursor.hasNext()) {
            Record record = cursor.next();
            rowid = records.append(record, rowid);
            for (int i = 0; i < n; i++) {
                functions.getQuick(i).addRecord(record, rowid);
            }
        }

        for (int i = 0; i < n; i++) {
            functions.getQuick(i).prepare(records);
        }

        records.toTop();
        return this;
    }

    @Override
    public void reset() {
        records.clear();
        parentSource.reset();
    }

    @Override
    public boolean supportsRowIdAccess() {
        return true;
    }

    @Override
    public boolean hasNext() {
        if (records.hasNext()) {
            for (int i = 0, n = functions.size(); i < n; i++) {
                functions.getQuick(i).scroll();
            }
            return true;
        }
        return false;
    }

    @Override
    public Record next() {
        record.of(records.next());
        return record;
    }

    @Override
    public void toSink(CharSink sink) {

    }
}
