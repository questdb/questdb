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
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.orderby.LongTreeChain;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class CachedAnalyticRecordCursorFactory implements RecordCursorFactory {
    private final RecordChain recordChain;
    private final RecordCursorFactory base;
    private final ObjList<LongTreeChain> orderedSources;
    private final int orderGroupCount;
    private final ObjList<ObjList<AnalyticFunction>> functionGroups;
    private final ObjList<AnalyticFunction> analyticFunctions;
    private final VirtualRecord record;
    private final VirtualRecord recordB;
    private final CachedAnalyticRecordCursor cursor = new CachedAnalyticRecordCursor();
    private final ObjList<RecordComparator> comparators;
    private final GenericRecordMetadata metadata;
    private final Record recordChainRecord;
    private boolean closed = false;

    public CachedAnalyticRecordCursorFactory(
            int rowidPageSize,
            int keyPageSize,
            RecordCursorFactory base,
            RecordSink baseRecordSink,
            GenericRecordMetadata metadata,
            ObjList<RecordComparator> comparators,
            ObjList<ObjList<AnalyticFunction>> functionGroups,
            ObjList<Function> recordFunctions
    ) {
        this.base = base;
        this.orderGroupCount = comparators.size();
        assert orderGroupCount == functionGroups.size();
        this.orderedSources = new ObjList<>(orderGroupCount);
        this.functionGroups = functionGroups;
        this.comparators = comparators;
        this.recordChain = new RecordChain(base.getMetadata(), baseRecordSink, rowidPageSize, Integer.MAX_VALUE);
        // red&black trees, one for each comparator where comparator is not null
        for (int i = 0; i < orderGroupCount; i++) {
            final RecordComparator cmp = comparators.getQuick(i);
            orderedSources.add(cmp == null ? null : new LongTreeChain(keyPageSize, Integer.MAX_VALUE, keyPageSize, Integer.MAX_VALUE));
        }

        // todo: we will tidy up structures later
        //     for now copy functions over to dense list
        this.analyticFunctions = new ObjList<>();
        for (int i = 0, n = functionGroups.size(); i < n; i++) {
            analyticFunctions.addAll(functionGroups.getQuick(i));
        }

        // create our metadata and also flatten functions for our record representation
        this.metadata = metadata;
        this.record = new VirtualRecord(recordFunctions);
        this.recordB = new VirtualRecord(recordFunctions);
        this.recordChainRecord = recordChain.getRecord();
        this.record.of(recordChainRecord);
        this.recordB.of(recordChain.getRecordB());
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        Misc.free(base);
        Misc.free(recordChain);
        Misc.freeObjList(orderedSources);
        Misc.freeObjList(analyticFunctions);
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

        for (int i = 0, n = analyticFunctions.size(); i < n; i++) {
            analyticFunctions.getQuick(i).reset();
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
                    recordChain.recordAt(recordChainRecord, cursor.next());
                    for (int j = 0, n = functions.size(); j < n; j++) {
                        functions.getQuick(j).add(recordChainRecord);
                    }
                }
            } else {
                // step #2: alternatively run record list through two-pass functions
                for (int j = 0, n = functions.size(); j < n; j++) {
                    AnalyticFunction f = functions.getQuick(j);
                    if (f.getType() != AnalyticFunction.STREAM) {
                        recordChain.toTop();
                        while (recordChain.hasNext()) {
                            f.add(recordChainRecord);
                        }
                    }
                }
            }
        }

        recordChain.toTop();
        for (int i = 0, n = analyticFunctions.size(); i < n; i++) {
            analyticFunctions.getQuick(i).prepare(recordChain);
        }
        return cursor;
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    private class CachedAnalyticRecordCursor implements RecordCursor {

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public boolean hasNext() {
            if (recordChain.hasNext()) {
                for (int i = 0, n = analyticFunctions.size(); i < n; i++) {
                    analyticFunctions.getQuick(i).prepareFor(recordChainRecord);
                }
                return true;
            }
            return false;
        }

        @Override
        public Record getRecordB() {
            return recordB;
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            recordChain.recordAt(((VirtualRecord) record).getBaseRecord(), atRowId);
        }

        @Override
        public void toTop() {
            recordChain.toTop();
            GroupByUtils.toTop(analyticFunctions);
        }

        @Override
        public long size() {
            return recordChain.size();
        }
    }
}
