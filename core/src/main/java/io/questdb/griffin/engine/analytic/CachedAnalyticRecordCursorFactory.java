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
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class CachedAnalyticRecordCursorFactory implements RecordCursorFactory {
    private final RecordChain recordChain;
    private final RecordCursorFactory base;
    private final ObjList<LongTreeChain> orderedSources;
    private final int orderGroupCount;
    private final ObjList<ObjList<AnalyticFunction>> functionGroups;
    private final ObjList<AnalyticFunction> functions;
    private final GenericRecordMetadata metadata;
    private final AnalyticRecord record;
    private final CachedAnalyticRecordCursor cursor = new CachedAnalyticRecordCursor();
    private final ObjList<RecordComparator> comparators;
    private boolean closed = false;

    public CachedAnalyticRecordCursorFactory(
            int rowidPageSize,
            int keyPageSize,
            RecordCursorFactory base,
            ObjList<RecordComparator> comparators,
            ObjList<ObjList<AnalyticFunction>> functionGroups
    ) {
        this.base = base;
        this.orderGroupCount = comparators.size();
        assert orderGroupCount == functionGroups.size();
        this.orderedSources = new ObjList<>(orderGroupCount);
        this.functionGroups = functionGroups;
        this.comparators = comparators;

        // todo: compile sink and limit pages from the configuration
        this.recordChain = new RecordChain(base.getMetadata(), null, rowidPageSize, Integer.MAX_VALUE);
        // red&black trees, one for each comparator where comparator is not null
        for (int i = 0; i < orderGroupCount; i++) {
            RecordComparator cmp = comparators.getQuick(i);
            orderedSources.add(cmp == null ? null : new LongTreeChain(keyPageSize, Integer.MAX_VALUE, keyPageSize, Integer.MAX_VALUE));
        }

        // create our metadata and also flatten functions for our record representation
        this.metadata = new GenericRecordMetadata();
        GenericRecordMetadata.copyColumns(base.getMetadata(), metadata);
        this.functions = new ObjList<>(orderGroupCount);
//        for (int i = 0; i < orderGroupCount; i++) {
//            ObjList<AnalyticFunction> l = functionGroups.getQuick(i);
//            for (int j = 0; j < l.size(); j++) {
//                AnalyticFunction f = l.getQuick(j);
//                metadata.add(f.getMetadata());
//                functions.add(f);
//            }
//        }

        int split = base.getMetadata().getColumnCount();
        this.record = new AnalyticRecord(split, functions);
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
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        recordChain.clear();
        for (int i = 0; i < orderGroupCount; i++) {
            final LongTreeChain tree = orderedSources.getQuick(i);
            if (tree != null) {
                tree.clear();
            }
        }

        for (int i = 0, n = functions.size(); i < n; i++) {
            functions.getQuick(i).reset();
        }

        final RecordCursor baseCursor = base.getCursor(executionContext);
        // step #1: store source cursor in record list
        // - add record list' row ids to all trees, which will put these row ids in necessary order
        // for this we will be using out comparator, which helps tree compare long values
        // based on record these values are addressing
        long rowid = -1;
        final Record record = baseCursor.getRecord();
        final Record chainLeftRecord = recordChain.getRecord();
        final Record chainRightRecord = recordChain.getRecordB();
        while (baseCursor.hasNext()) {
            rowid = recordChain.put(record, rowid);
            recordChain.recordAt(chainLeftRecord, rowid);

            if (orderGroupCount > 0) {
                for (int i = 0; i < orderGroupCount; i++) {
                    LongTreeChain tree = orderedSources.getQuick(i);
                    if (tree != null) {
                        tree.put(
                                chainLeftRecord,
                                recordChain,
                                chainRightRecord,
                                comparators.getQuick(i)
                        );
                    }
                }
            }
        }

        for (int i = 0; i < orderGroupCount; i++) {
            LongTreeChain tree = orderedSources.getQuick(i);
            ObjList<AnalyticFunction> functions = functionGroups.getQuick(i);
            if (tree != null) {
                // step #2: populate all analytic functions with records in order of respective tree
                final LongTreeChain.TreeCursor cursor = tree.getCursor();
                while (cursor.hasNext()) {
//                    cancellationHandler.check();
                    recordChain.recordAt(record, cursor.next());
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
                        final Record rec = recordChain.getRecord();
                        while (recordChain.hasNext()) {
                            f.add(rec);
                        }
                    }
                }
            }
        }

        recordChain.toTop();
        for (int i = 0, n = functions.size(); i < n; i++) {
            functions.getQuick(i).prepare(recordChain);
        }
        this.cursor.of(recordChain);
        return cursor;
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    private class CachedAnalyticRecordCursor implements RecordCursor {
        private RecordCursor baseCursor;

        @Override
        public void close() {
            Misc.free(baseCursor);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (baseCursor.hasNext()) {
                for (int i = 0, n = functions.size(); i < n; i++) {
                    functions.getQuick(i).prepareFor(record);
                }
                return true;
            }
            return false;
        }

        @Override
        public Record getRecordB() {
            return null;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
        }

        @Override
        public void toTop() {
            this.baseCursor.toTop();
            for (int i = 0, n = functions.size(); i < n; i++) {
                functions.getQuick(i).toTop();
            }
        }

        @Override
        public long size() {
            return baseCursor.size();
        }

        void of(RecordCursor baseCursor) {
            this.baseCursor = baseCursor;
            record.of(baseCursor.getRecord());
        }
    }
}
