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

package io.questdb.griffin.engine.analytic;


import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.RecordChain;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.orderby.LongTreeChain;
import io.questdb.griffin.engine.orderby.RecordComparator;
import io.questdb.griffin.engine.orderby.RecordTreeChain;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class CachedAnalyticRecordCursorFactory implements RecordCursorFactory {
    private final RecordChain recordChain;
    private final RecordCursorFactory base;
    private final ObjList<RecordTreeChain> orderedSources;
    private final int orderGroupCount;
    private final ObjList<ObjList<AnalyticFunction>> functionGroups;
    private final ObjList<AnalyticFunction> functions;
    private final RecordMetadata metadata;
    private final AnalyticRecord record;
    private final AnalyticRecordStorageFacade storageFacade;
    private final int split;
    private RecordCursor cursor;
    private boolean closed = false;

    public CachedAnalyticRecordCursorFactory(
            int rowidPageSize,
            int keyPageSize,
            RecordCursorFactory base,
            @Transient ObjList<RecordComparator> comparators,
            ObjList<ObjList<AnalyticFunction>> functionGroups
    ) {
        this.base = base;
        this.orderGroupCount = comparators.size();
        assert orderGroupCount == functionGroups.size();
        this.orderedSources = new ObjList<>(orderGroupCount);
        this.functionGroups = functionGroups;

        // todo: compile sink and limit pages from the configuration
        RecordChain list = new RecordChain(base.getMetadata(), null, rowidPageSize, Integer.MAX_VALUE);
        // red&black trees, one for each comparator where comparator is not null
        for (int i = 0; i < orderGroupCount; i++) {
            RecordComparator cmp = comparators.getQuick(i);
            orderedSources.add(cmp == null ? null : new RecordTreeChain(new MyComparator(cmp, list), keyPageSize));
        }

        this.recordChain = list;

        // create our metadata and also flatten functions for our record representation
        GenericRecordMetadata funcMetadata = new GenericRecordMetadata();
        this.functions = new ObjList<>(orderGroupCount);
        for (int i = 0; i < orderGroupCount; i++) {
            ObjList<AnalyticFunction> l = functionGroups.getQuick(i);
            for (int j = 0; j < l.size(); j++) {
                AnalyticFunction f = l.getQuick(j);
                funcMetadata.add(f.getMetadata());
                functions.add(f);
            }
        }
        this.metadata = new SplitRecordMetadata(base.getMetadata(), funcMetadata);
        this.split = base.getMetadata().getColumnCount();
        this.record = new AnalyticRecord(split, functions);
        this.storageFacade = new AnalyticRecordStorageFacade(split, functions);
        this.recordChain.setStorageFacade(storageFacade);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        recordChain.clear();
        for (int i = 0; i < orderGroupCount; i++) {
            RecordTreeChain tree = orderedSources.getQuick(i);
            if (tree != null) {
                tree.clear();
            }
        }

        for (int i = 0, n = functions.size(); i < n; i++) {
            functions.getQuick(i).reset();
        }

        final RecordCursor baseCursor = base.getCursor(executionContext);
        this.storageFacade.prepare(baseCursor.getStorageFacade());
        // step #1: store source cursor in record list
        // - add record list' row ids to all trees, which will put these row ids in necessary order
        // for this we will be using out comparator, which helps tree compare long values
        // based on record these values are addressing
        long rowid = -1;
        final Record record = baseCursor.getRecord();
        while (baseCursor.hasNext()) {
            rowid = recordChain.put(record, rowid);
            if (orderGroupCount > 0) {
                for (int i = 0; i < orderGroupCount; i++) {
                    RecordTreeChain tree = orderedSources.getQuick(i);
                    if (tree != null) {
                        tree.add(rowid);
                    }
                }
            }
        }

        for (int i = 0; i < orderGroupCount; i++) {
            RecordTreeChain tree = orderedSources.getQuick(i);
            ObjList<AnalyticFunction> functions = functionGroups.getQuick(i);
            if (tree != null) {
                // step #2: populate all analytic functions with records in order of respective tree
                RedBlackTree.LongIterator iterator = tree.iterator();
                while (iterator.hasNext()) {

                    cancellationHandler.check();

                    Record record = recordChain.recordAt(iterator.next());
                    for (int j = 0, n = functions.size(); j < n; j++) {
                        functions.getQuick(j).add(record);
                    }
                }
            } else {
                // step #2: alternatively run record list through two-pass functions
                for (int j = 0, n = functions.size(); j < n; j++) {
                    AnalyticFunction f = functions.getQuick(j);
                    if (f.getType() != AnalyticFunction.STREAM) {
                        recordChain.toTop();
                        while (recordChain.hasNext()) {
                            f.add(recordChain.next());
                        }
                    }
                }
            }
        }

        recordChain.toTop();
        setCursorAndPrepareFunctions();
        return this;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        Misc.free(base);
        Misc.free(recordChain);
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

    private static class CachedAnalyticRecordCursor implements RecordCursor {

    }
    @Override
    public RecordCursor prepareCursor(JournalReaderFactory factory, CancellationHandler cancellationHandler) {
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
        sink.putQuoted("src").put(':').put(base);
        sink.put('}');
    }

    private void setCursorAndPrepareFunctions() {
        for (int i = 0, n = functions.size(); i < n; i++) {
            functions.getQuick(i).prepare(recordChain);
        }
        this.cursor = recordChain;
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
