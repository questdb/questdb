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

package com.questdb.ql.impl.analytic;

import com.questdb.ex.JournalException;
import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Misc;
import com.questdb.ql.*;
import com.questdb.ql.impl.CollectionRecordMetadata;
import com.questdb.ql.impl.RecordList;
import com.questdb.ql.impl.SplitRecordMetadata;
import com.questdb.ql.impl.join.hash.FakeRecord;
import com.questdb.ql.impl.map.RedBlackTree;
import com.questdb.ql.impl.sort.RecordComparator;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.CharSink;
import com.questdb.std.ObjList;
import com.questdb.std.Transient;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class AnalyticRecordSource extends AbstractCombinedRecordSource {

    private final RecordList recordList;
    private final RecordSource recordSource;
    private final ObjList<RedBlackTree> orderedSources;
    private final int orderGroupCount;
    private final FakeRecord fakeRecord = new FakeRecord();
    private final ObjList<ObjList<AnalyticFunction>> functionGroups;
    private final ObjList<AnalyticFunction> functions;
    private final RecordMetadata metadata;
    private final AnalyticRecord record;
    private final AnalyticRecordStorageFacade storageFacade;
    private final int split;
    private RecordCursor cursor;

    public AnalyticRecordSource(
            int rowidPageSize,
            int keyPageSize,
            RecordSource recordSource,
            @Transient ObjList<RecordComparator> comparators,
            ObjList<ObjList<AnalyticFunction>> functionGroups) {
        this.recordSource = recordSource;
        this.orderGroupCount = comparators.size();
        assert orderGroupCount == functionGroups.size();
        this.orderedSources = new ObjList<>(orderGroupCount);
        this.functionGroups = functionGroups;

        RecordList list = null;
        // red&black trees, one for each comparator where comparator is not null
        for (int i = 0; i < orderGroupCount; i++) {
            RecordComparator cmp = comparators.getQuick(i);
            if (cmp != null) {
                // if there is comparator, then at least one function requires
                // different order on records, which makes us take a copy of
                // parent record source and walk data at least twice.
                if (list == null) {
//                    list = new RecordList(MapUtils.ROWID_RECORD_METADATA, rowidPageSize);
                    // todo: support rowid
                    list = new RecordList(recordSource.getMetadata(), rowidPageSize);
                }

                orderedSources.add(new RedBlackTree(new MyComparator(cmp, list), keyPageSize));
            } else {
                // even though there is no order we must check if there are
                // multi-pass functions
                if (list == null) {
                    ObjList<AnalyticFunction> functions = functionGroups.getQuick(i);
                    for (int j = 0, n = functions.size(); j < n; j++) {
                        if (functions.getQuick(i).getType() != AnalyticFunctionType.STREAM) {
//                            list = new RecordList(MapUtils.ROWID_RECORD_METADATA, rowidPageSize);
                            // todo: support rowid
                            list = new RecordList(recordSource.getMetadata(), rowidPageSize);
                            break;
                        }
                    }
                }
                orderedSources.add(null);
            }
        }

        this.recordList = list;

        // create our metadata and also flatten functions for our record representation
        CollectionRecordMetadata funcMetadata = new CollectionRecordMetadata();
        this.functions = new ObjList<>(orderGroupCount);
        for (int i = 0; i < orderGroupCount; i++) {
            ObjList<AnalyticFunction> l = functionGroups.getQuick(i);
            for (int j = 0; j < l.size(); j++) {
                AnalyticFunction f = l.getQuick(j);
                funcMetadata.add(f.getMetadata());
                functions.add(f);
            }
        }
        this.metadata = new SplitRecordMetadata(recordSource.getMetadata(), funcMetadata);
        this.split = recordSource.getMetadata().getColumnCount();
        this.record = new AnalyticRecord(split, functions);
        this.storageFacade = new AnalyticRecordStorageFacade(split, functions);
    }

    @Override
    public void close() {
        if (recordList != null) {
            recordList.close();
        }

        for (int i = 0; i < orderGroupCount; i++) {
            RedBlackTree tree = orderedSources.getQuick(i);
            if (tree != null) {
                tree.close();
            }
        }

        for (int i = 0, n = functions.size(); i < n; i++) {
            Misc.free(functions.getQuick(i));
        }
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor prepareCursor(JournalReaderFactory factory, CancellationHandler cancellationHandler) throws JournalException {
        RecordCursor cursor = recordSource.prepareCursor(factory, cancellationHandler);
        this.storageFacade.prepare(factory, cursor.getStorageFacade());


        // record list can be null if all functions are of type STREAM
        if (recordList == null) {
            setCursorAndPrepareFunctions(cursor);
            return this;
        }

        recordList.setStorageFacade(cursor.getStorageFacade());

        // step #1: store source cursor in record list
        // - add record list' row ids to all trees, which will put these row ids in necessary order
        // for this we will be using out comparator, which helps tree compare long values
        // based on record these values are addressing
        long rowid = -1;
        while (cursor.hasNext()) {

            cancellationHandler.check();

            Record record = cursor.next();
//            rowid = recordList.append(fakeRecord.of(record.getRowId()), rowid);
            rowid = recordList.append(record, rowid);
            for (int i = 0; i < orderGroupCount; i++) {
                RedBlackTree tree = orderedSources.getQuick(i);
                if (tree != null) {
                    tree.put(rowid);
                }
            }
        }

        for (int i = 0; i < orderGroupCount; i++) {
            RedBlackTree tree = orderedSources.getQuick(i);
            ObjList<AnalyticFunction> functions = functionGroups.getQuick(i);
            if (tree != null) {
                // step #2: populate all analytic functions with records in order of respective tree
                RedBlackTree.LongIterator iterator = tree.iterator();
                while (iterator.hasNext()) {

                    cancellationHandler.check();

                    Record record = recordList.recordAt(iterator.next());
                    for (int j = 0, n = functions.size(); j < n; j++) {
                        functions.getQuick(j).add(record);
                    }
                }
            } else {
                // step #2: alternatively run record list through two-pass functions
                for (int j = 0, n = functions.size(); j < n; j++) {
                    AnalyticFunction f = functions.getQuick(j);
                    if (f.getType() != AnalyticFunctionType.STREAM) {
                        recordList.toTop();
                        while (recordList.hasNext()) {
                            f.add(recordList.next());
                        }
                    }
                }
            }
        }

        recordList.toTop();
        setCursorAndPrepareFunctions(recordList);
        return this;
    }

    @Override
    public void reset() {
        if (recordList != null) {
            recordList.clear();
        }

        for (int i = 0; i < orderGroupCount; i++) {
            RedBlackTree tree = orderedSources.getQuick(i);
            if (tree != null) {
                tree.clear();
            }
        }

        for (int i = 0, n = functions.size(); i < n; i++) {
            functions.getQuick(i).reset();
        }
    }

    @Override
    public StorageFacade getStorageFacade() {
        return storageFacade;
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

    @SuppressFBWarnings("IT_NO_SUCH_ELEMENT")
    @Override
    public Record next() {
        return record;
    }

    @Override
    public Record newRecord() {
        return new AnalyticRecord(split, functions);
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("AnalyticRecordSource").put(',');
        sink.putQuoted("functions").put(':').put(functions.size()).put(',');
        sink.putQuoted("src").put(':').put(recordSource);
        sink.put('}');
    }

    private void setCursorAndPrepareFunctions(RecordCursor cursor) {
        for (int i = 0, n = functions.size(); i < n; i++) {
            functions.getQuick(i).prepare(cursor);
        }
        this.cursor = cursor;
    }

    private static class MyComparator implements RedBlackTree.LongComparator {
        private final RecordComparator delegate;
        private final RecordCursor cursor;

        public MyComparator(RecordComparator delegate, RecordCursor cursor) {
            this.delegate = delegate;
            this.cursor = cursor;
        }

        @Override
        public int compare(long right) {
            return delegate.compare(cursor.recordAt(right));
        }

        @Override
        public void setLeft(long left) {
            delegate.setLeft(cursor.recordAt(left));
        }
    }
}
