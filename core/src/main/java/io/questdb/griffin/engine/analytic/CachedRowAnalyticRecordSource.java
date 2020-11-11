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

import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Misc;
import com.questdb.ql.*;
import com.questdb.ql.impl.CollectionRecordMetadata;
import com.questdb.ql.impl.RecordList;
import com.questdb.ql.impl.SplitRecordMetadata;
import com.questdb.ql.impl.join.hash.FakeRecord;
import com.questdb.ql.impl.map.MapUtils;
import com.questdb.ql.impl.sort.RecordComparator;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.ObjList;
import com.questdb.std.RedBlackTree;
import com.questdb.std.str.CharSink;

public class CachedRowAnalyticRecordSource extends AbstractCombinedRecordSource {
    private final RecordList recordList;
    private final RecordSource delegate;
    private final ObjList<RedBlackTree> orderedSources;
    private final int orderGroupCount;
    private final ObjList<ObjList<AnalyticFunction>> functionGroups;
    private final ObjList<AnalyticFunction> functions;
    private final RecordMetadata metadata;
    private final AnalyticRecord record;
    private final AnalyticRecordStorageFacade storageFacade;
    private final int split;
    private final FakeRecord fakeRecord = new FakeRecord();
    private RecordCursor parentCursor;

    public CachedRowAnalyticRecordSource(
            int pageSize,
            RecordSource delegate,
            ObjList<RecordComparator> comparators,
            ObjList<ObjList<AnalyticFunction>> functionGroups) {
        this.delegate = delegate;
        this.orderGroupCount = comparators.size();
        assert orderGroupCount == functionGroups.size();
        this.orderedSources = new ObjList<>(orderGroupCount);
        this.functionGroups = functionGroups;
        this.recordList = new RecordList(MapUtils.ROWID_RECORD_METADATA, pageSize);
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

        // red&black trees, one for each comparator where comparator is not null
        for (int i = 0; i < orderGroupCount; i++) {
            RecordComparator cmp = comparators.getQuick(i);
            if (cmp != null) {
                orderedSources.add(new RedBlackTree(new MyComparator(cmp), pageSize));
            } else {
                orderedSources.add(null);
            }
        }

    }

    @Override
    public void close() {
        Misc.free(delegate);
        Misc.free(recordList);
        for (int i = 0; i < orderGroupCount; i++) {
            Misc.free(orderedSources.getQuick(i));
        }
        orderedSources.clear();

        for (int i = 0, n = functions.size(); i < n; i++) {
            Misc.free(functions.getQuick(i));
        }
        functions.clear();

    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor prepareCursor(JournalReaderFactory factory, CancellationHandler cancellationHandler) {
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

        final RecordCursor cursor = delegate.prepareCursor(factory, cancellationHandler);
        this.parentCursor = cursor;
        this.storageFacade.prepare(cursor.getStorageFacade());

        // red&black trees, one for each comparator where comparator is not null
        for (int i = 0; i < orderGroupCount; i++) {
            RedBlackTree tree = orderedSources.getQuick(i);
            if (tree != null) {
                ((MyComparator) tree.getComparator()).setCursor(cursor);
            }
        }

        // step #1: store source cursor in record list
        // - add record list' row ids to all trees, which will put these row ids in necessary order
        // for this we will be using out comparator, which helps tree compare long values
        // based on record these values are addressing
        long rowid = -1;
        while (cursor.hasNext()) {
            cancellationHandler.check();
            Record record = cursor.next();
            long row = record.getRowId();
            rowid = recordList.append(fakeRecord.of(row), rowid);
            if (orderGroupCount > 0) {
                for (int i = 0; i < orderGroupCount; i++) {
                    RedBlackTree tree = orderedSources.getQuick(i);
                    if (tree != null) {
                        tree.add(row);
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

                    Record record = cursor.recordAt(iterator.next());
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
                            f.add(cursor.recordAt(recordList.next().getLong(0)));
                        }
                    }
                }
            }
        }

        recordList.toTop();
        for (int i = 0, n = functions.size(); i < n; i++) {
            functions.getQuick(i).prepare(cursor);
        }
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
    public void toTop() {
        this.recordList.toTop();
        for (int i = 0, n = functions.size(); i < n; i++) {
            functions.getQuick(i).toTop();
        }
    }

    @Override
    public boolean hasNext() {
        if (recordList.hasNext()) {
            long row = recordList.next().getLong(0);
            record.of(parentCursor.recordAt(row));
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
        sink.putQuoted("op").put(':').putQuoted("CachedRowAnalyticRecordSource").put(',');
        sink.putQuoted("functions").put(':').put(functions.size()).put(',');
        sink.putQuoted("orderedSources").put(':').put(orderedSources.size()).put(',');
        sink.putQuoted("src").put(':').put(delegate);
        sink.put('}');
    }

    private static class MyComparator implements RedBlackTree.LongComparator {
        private final RecordComparator delegate;
        private RecordCursor cursor;
        private Record left;
        private Record right;

        public MyComparator(RecordComparator delegate) {
            this.delegate = delegate;
        }

        @Override
        public int compare(long right) {
            assert right > -1;
            cursor.recordAt(this.right, right);
            assert this.right != null;
            return delegate.compare(this.right);
        }

        @Override
        public void setLeft(long left) {
            assert left > -1;
            cursor.recordAt(this.left, left);
            assert this.left != null;
            delegate.setLeft(this.left);
        }

        public void setCursor(RecordCursor cursor) {
            this.cursor = cursor;
            if (left == null || right == null) {
                this.left = cursor.newRecord();
                this.right = cursor.newRecord();
            }
        }
    }
}
