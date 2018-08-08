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

import com.questdb.ql.*;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.ql.sort.RecordComparator;
import com.questdb.std.Misc;
import com.questdb.std.ObjList;
import com.questdb.std.RedBlackTree;
import com.questdb.std.Transient;
import com.questdb.std.str.CharSink;
import com.questdb.store.Record;
import com.questdb.store.RecordCursor;
import com.questdb.store.RecordMetadata;
import com.questdb.store.StorageFacade;
import com.questdb.store.factory.ReaderFactory;

public class CachedAnalyticRecordSource extends AbstractCombinedRecordSource {

    private final RecordList recordList;
    private final RecordSource recordSource;
    private final ObjList<RedBlackTree> orderedSources;
    private final int orderGroupCount;
    private final ObjList<ObjList<AnalyticFunction>> functionGroups;
    private final ObjList<AnalyticFunction> functions;
    private final RecordMetadata metadata;
    private final AnalyticRecord record;
    private final AnalyticRecordStorageFacade storageFacade;
    private final int split;
    private RecordCursor cursor;
    private boolean closed = false;

    public CachedAnalyticRecordSource(
            int rowidPageSize,
            int keyPageSize,
            RecordSource delegate,
            @Transient ObjList<RecordComparator> comparators,
            ObjList<ObjList<AnalyticFunction>> functionGroups) {
        this.recordSource = delegate;
        this.orderGroupCount = comparators.size();
        assert orderGroupCount == functionGroups.size();
        this.orderedSources = new ObjList<>(orderGroupCount);
        this.functionGroups = functionGroups;

        RecordList list = new RecordList(delegate.getMetadata(), rowidPageSize);
        // red&black trees, one for each comparator where comparator is not null
        for (int i = 0; i < orderGroupCount; i++) {
            RecordComparator cmp = comparators.getQuick(i);
            orderedSources.add(cmp == null ? null : new RedBlackTree(new MyComparator(cmp, list), keyPageSize));
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
        this.metadata = new SplitRecordMetadata(delegate.getMetadata(), funcMetadata);
        this.split = delegate.getMetadata().getColumnCount();
        this.record = new AnalyticRecord(split, functions);
        this.storageFacade = new AnalyticRecordStorageFacade(split, functions);
        this.recordList.setStorageFacade(storageFacade);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        Misc.free(recordSource);
        Misc.free(recordList);
        for (int i = 0; i < orderGroupCount; i++) {
            Misc.free(orderedSources.getQuick(i));
        }

        for (int i = 0, n = functions.size(); i < n; i++) {
            Misc.free(functions.getQuick(i));
        }

        closed = true;
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor prepareCursor(ReaderFactory factory, CancellationHandler cancellationHandler) {
        recordList.clear();
        for (int i = 0; i < orderGroupCount; i++) {
            RedBlackTree tree = orderedSources.getQuick(i);
            if (tree != null) {
                tree.clear();
            }
        }

        for (int i = 0, n = functions.size(); i < n; i++) {
            functions.getQuick(i).reset();
        }

        final RecordCursor cursor = recordSource.prepareCursor(factory, cancellationHandler);
        try {
            this.storageFacade.prepare(cursor.getStorageFacade());
            // step #1: store source cursor in record list
            // - add record list' row ids to all trees, which will put these row ids in necessary order
            // for this we will be using out comparator, which helps tree compare long values
            // based on record these values are addressing
            long rowid = -1;
            while (cursor.hasNext()) {
                cancellationHandler.check();
                Record record = cursor.next();
                rowid = recordList.append(record, rowid);
                if (orderGroupCount > 0) {
                    for (int i = 0; i < orderGroupCount; i++) {
                        RedBlackTree tree = orderedSources.getQuick(i);
                        if (tree != null) {
                            tree.add(rowid);
                        }
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
                        if (f.getType() != AnalyticFunction.STREAM) {
                            recordList.toTop();
                            while (recordList.hasNext()) {
                                f.add(recordList.next());
                            }
                        }
                    }
                }
            }
        } finally {
            cursor.releaseCursor();
        }

        recordList.toTop();
        setCursorAndPrepareFunctions();
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
        cursor.releaseCursor();
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
        sink.putQuoted("op").put(':').putQuoted("CachedAnalyticRecordSource").put(',');
        sink.putQuoted("functions").put(':').put(functions.size()).put(',');
        sink.putQuoted("orderedSources").put(':').put(orderedSources.size()).put(',');
        sink.putQuoted("src").put(':').put(recordSource);
        sink.put('}');
    }

    private void setCursorAndPrepareFunctions() {
        for (int i = 0, n = functions.size(); i < n; i++) {
            functions.getQuick(i).prepare(recordList);
        }
        this.cursor = recordList;
    }

    private static class MyComparator implements RedBlackTree.LongComparator {
        private final RecordComparator delegate;
        private final RecordCursor cursor;
        private final Record left;
        private final Record right;

        public MyComparator(RecordComparator delegate, RecordCursor cursor) {
            this.delegate = delegate;
            this.cursor = cursor;
            this.left = cursor.newRecord();
            this.right = cursor.newRecord();
        }

        @Override
        public int compare(long right) {
            cursor.recordAt(this.right, right);
            return delegate.compare(this.right);
        }

        @Override
        public void setLeft(long left) {
            cursor.recordAt(this.left, left);
            delegate.setLeft(this.left);
        }
    }
}
