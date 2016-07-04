/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/

package com.questdb.ql.impl.analytic.old;

import com.questdb.ex.JournalException;
import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Misc;
import com.questdb.ql.*;
import com.questdb.ql.impl.CollectionRecordMetadata;
import com.questdb.ql.impl.RecordList;
import com.questdb.ql.impl.SplitRecordMetadata;
import com.questdb.ql.impl.join.hash.FakeRecord;
import com.questdb.ql.impl.map.MapUtils;
import com.questdb.ql.impl.sort.RBTreeSortedRecordSource;
import com.questdb.ql.impl.sort.RecordComparator;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.CharSink;
import com.questdb.std.ObjList;
import com.questdb.std.Transient;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class OrderedRowIdAnalyticRecordSource extends AbstractCombinedRecordSource {
    private final RecordList records;
    private final RecordSource parentSource;
    private final ObjList<ObjList<AnalyticFunction>> functions;
    private final RecordMetadata metadata;
    private final AnalyticRecord record;
    private final AnalyticRecordStorageFacade storageFacade;
    private final ObjList<RBTreeSortedRecordSource> trees;
    private final ObjList<AnalyticFunction> flatFunctionList;
    private final FakeRecord fakeRecord = new FakeRecord();
    private Record parentRecord;
    private RecordCursor parentCursor;

    public OrderedRowIdAnalyticRecordSource(
            int pageSize,
            int keyPageSize,
            int valuePageSize,
            RecordSource parentSource,
            @Transient ObjList<RecordComparator> comparators,
            ObjList<ObjList<AnalyticFunction>> functions) {
        this.parentSource = parentSource;
        this.records = new RecordList(MapUtils.ROWID_RECORD_METADATA, pageSize);

        this.trees = new ObjList<>(comparators.size());
        for (int i = 0, n = comparators.size(); i < n; i++) {
            this.trees.add(new RBTreeSortedRecordSource(parentSource, comparators.getQuick(i), keyPageSize, valuePageSize));
        }

        this.functions = functions;
        this.flatFunctionList = new ObjList<>();

        CollectionRecordMetadata funcMetadata = new CollectionRecordMetadata();
        for (int i = 0; i < functions.size(); i++) {
            ObjList<AnalyticFunction> l = functions.getQuick(i);
            for (int j = 0; j < l.size(); j++) {
                AnalyticFunction f = l.getQuick(j);
                funcMetadata.add(f.getMetadata());
                flatFunctionList.add(f);
            }
        }

        this.metadata = new SplitRecordMetadata(parentSource.getMetadata(), funcMetadata);
        int split = parentSource.getMetadata().getColumnCount();
        this.record = new AnalyticRecord(split, flatFunctionList);
        this.storageFacade = new AnalyticRecordStorageFacade(split, flatFunctionList);
    }

    @Override
    public void close() {
        for (int i = 0, n = flatFunctionList.size(); i < n; i++) {
            Misc.free(flatFunctionList.getQuick(i));
        }
        Misc.free(parentSource);
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor prepareCursor(JournalReaderFactory factory, CancellationHandler cancellationHandler) throws JournalException {
        this.parentCursor = this.parentSource.prepareCursor(factory, cancellationHandler);
        final StorageFacade storageFacade = parentCursor.getStorageFacade();
        records.setStorageFacade(storageFacade);
        this.storageFacade.prepare(factory, storageFacade);
        this.parentRecord = parentCursor.newRecord();

        int n = trees.size();
        for (int i = 0; i < n; i++) {
            trees.getQuick(i).setSourceCursor(parentCursor);
        }

        // order parent record source
        long rowid = -1;
        while (parentCursor.hasNext()) {
            cancellationHandler.check();
            Record record = parentCursor.next();
            long rr = record.getRowId();
            for (int i = 0; i < n; i++) {
                trees.getQuick(i).put(rr);
            }
            rowid = records.append(fakeRecord.of(rr), rowid);
        }

        // apply analytic functions
        for (int i = 0; i < n; i++) {
            applyAnalyticFunctions(trees.getQuick(i).setupCursor(), functions.getQuick(i));
        }

        records.toTop();
        return this;
    }

    @Override
    public void reset() {
        records.clear();
        parentSource.reset();

        for (int i = 0, n = flatFunctionList.size(); i < n; i++) {
            flatFunctionList.getQuick(i).reset();
        }
    }

    @Override
    public StorageFacade getStorageFacade() {
        return storageFacade;
    }

    @Override
    public boolean hasNext() {
        if (records.hasNext()) {
            parentCursor.recordAt(parentRecord, records.next().getLong(0));
            record.of(parentRecord);
            for (int i = 0, n = flatFunctionList.size(); i < n; i++) {
                flatFunctionList.getQuick(i).scroll(record);
            }
            return true;
        }
        return false;
    }

    @SuppressFBWarnings("IT_NO_SUCH_ELEMENT")
    @Override
    public Record next() {
        return record;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("OrderedRowIdAnalyticRecordSource").put(',');
        sink.putQuoted("functions").put(':').put(functions.size()).put(',');
        sink.putQuoted("src").put(':').put(parentSource);
        sink.put('}');
    }

    private void applyAnalyticFunctions(RecordCursor treeCursor, ObjList<AnalyticFunction> functions) {
        final int m = functions.size();

        for (int i = 0; i < m; i++) {
            functions.getQuick(i).prepare(treeCursor);
        }

        while (treeCursor.hasNext()) {
            Record record = treeCursor.next();
            for (int i = 0; i < m; i++) {
                AnalyticFunction f = functions.getQuick(i);
                if (f instanceof TwoPassAnalyticFunction) {
                    ((TwoPassAnalyticFunction) f).addRecord(record, record.getRowId());
                }
            }
        }

        for (int i = 0; i < m; i++) {
            AnalyticFunction f = functions.getQuick(i);
            if (f instanceof TwoPassAnalyticFunction) {
                ((TwoPassAnalyticFunction) f).compute(treeCursor);
            }
        }
    }
}
