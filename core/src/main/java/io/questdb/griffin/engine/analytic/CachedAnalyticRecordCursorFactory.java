/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.analytic;


import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.orderby.LongTreeChain;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CachedAnalyticRecordCursorFactory extends AbstractRecordCursorFactory {
    private final ObjList<AnalyticFunction> allFunctions;
    private final RecordCursorFactory base;
    private final ObjList<RecordComparator> comparators;
    private final CachedAnalyticRecordCursor cursor;
    private final ObjList<ObjList<AnalyticFunction>> orderedFunctions;
    private final int orderedGroupCount;
    @Nullable
    private final ObjList<AnalyticFunction> unorderedFunctions;
    private boolean closed = false;

    public CachedAnalyticRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base,
            RecordSink recordSink,
            GenericRecordMetadata metadata,
            @Transient ColumnTypes chainMetadata,
            ObjList<RecordComparator> comparators,
            ObjList<ObjList<AnalyticFunction>> orderedFunctions,
            @Nullable ObjList<AnalyticFunction> unorderedFunctions,
            @NotNull IntList columnIndexes
    ) {
        super(metadata);
        this.base = base;
        this.orderedGroupCount = comparators.size();
        assert orderedGroupCount == orderedFunctions.size();
        this.orderedFunctions = orderedFunctions;
        this.comparators = comparators;
        RecordChain recordChain = new RecordChain(
                chainMetadata,
                recordSink,
                configuration.getSqlAnalyticStorePageSize(),
                configuration.getSqlAnalyticStoreMaxPages()
        );

        ObjList<LongTreeChain> orderedSources = new ObjList<>(orderedGroupCount);
        // red&black trees, one for each comparator where comparator is not null
        for (int i = 0; i < orderedGroupCount; i++) {
            orderedSources.add(
                    new LongTreeChain(
                            configuration.getSqlAnalyticTreeKeyPageSize(),
                            configuration.getSqlAnalyticTreeKeyMaxPages(),
                            configuration.getSqlAnalyticRowIdPageSize(),
                            configuration.getSqlAnalyticRowIdMaxPages()
                    )
            );
        }

        this.cursor = new CachedAnalyticRecordCursor(columnIndexes, recordChain, orderedSources);

        this.allFunctions = new ObjList<>();
        for (int i = 0, n = orderedFunctions.size(); i < n; i++) {
            allFunctions.addAll(orderedFunctions.getQuick(i));
        }
        if (unorderedFunctions != null) {
            allFunctions.addAll(unorderedFunctions);
        }

        this.unorderedFunctions = unorderedFunctions;
    }

    @Override
    public boolean followedOrderByAdvice() {
        return base.followedOrderByAdvice();
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        cursor.of(baseCursor, executionContext);
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("CachedAnalytic");
        sink.optAttr("functions", allFunctions);
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    private void resetFunctions() {
        for (int i = 0, n = allFunctions.size(); i < n; i++) {
            allFunctions.getQuick(i).reset();
        }
    }

    @Override
    protected void _close() {
        if (closed) {
            return;
        }
        Misc.free(base);
        Misc.free(cursor);
        Misc.freeObjList(allFunctions);
        closed = true;
    }

    class CachedAnalyticRecordCursor implements RecordCursor {

        private final IntList columnIndexes; // Used for symbol table lookups.
        private final ObjList<LongTreeChain> orderedSources;
        private final RecordChain recordChain;
        private RecordCursor base;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isOpen;
        private boolean isRecordChainBuilt;
        private long recordChainOffset;

        public CachedAnalyticRecordCursor(IntList columnIndexes, RecordChain recordChain, ObjList<LongTreeChain> orderedSources) {
            this.columnIndexes = columnIndexes;
            this.recordChain = recordChain;
            this.recordChain.setSymbolTableResolver(this);
            this.isOpen = true;
            this.orderedSources = orderedSources;
        }

        @Override
        public void close() {
            if (isOpen) {
                Misc.free(base);
                Misc.free(recordChain);
                for (int i = 0, n = orderedSources.size(); i < n; i++) {
                    Misc.free(orderedSources.getQuick(i));
                }
                resetFunctions(); // calls close on map within RowNumber
                isOpen = false;
            }
        }

        @Override
        public Record getRecord() {
            return recordChain.getRecord();
        }

        @Override
        public Record getRecordB() {
            return recordChain.getRecordB();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return base.getSymbolTable(columnIndexes.getQuick(columnIndex));
        }

        @Override
        public boolean hasNext() {
            if (!isRecordChainBuilt) {
                buildRecordChain();
            }
            isRecordChainBuilt = true;
            return recordChain.hasNext();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return base.newSymbolTable(columnIndexes.getQuick(columnIndex));
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            recordChain.recordAt(record, atRowId);
        }

        @Override
        public long size() {
            return recordChain.size();
        }

        @Override
        public void toTop() {
            recordChain.toTop();
        }

        private void buildRecordChain() {
            // step #1: store source cursor in record list
            // - add record list's row ids to all trees, which will put these row ids in necessary order
            // for this we will be using out comparator, which helps tree compare long values
            // based on record these values are addressing
            final Record record = base.getRecord();
            final Record chainRecord = recordChain.getRecord();
            final Record chainRightRecord = recordChain.getRecordB();
            if (orderedGroupCount > 0) {
                while (base.hasNext()) {
                    recordChainOffset = recordChain.put(record, recordChainOffset);
                    recordChain.recordAt(chainRecord, recordChainOffset);
                    for (int i = 0; i < orderedGroupCount; i++) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        orderedSources.getQuick(i).put(chainRecord, recordChain, chainRightRecord, comparators.getQuick(i));
                    }
                }
            } else {
                while (base.hasNext()) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    recordChainOffset = recordChain.put(record, recordChainOffset);
                }
            }

            // step #2: populate all analytic functions with records in order of respective tree
            // run pass1 for all ordered functions
            long offset;
            if (orderedGroupCount > 0) {
                for (int i = 0; i < orderedGroupCount; i++) {
                    final LongTreeChain tree = orderedSources.getQuick(i);
                    final ObjList<AnalyticFunction> functions = orderedFunctions.getQuick(i);
                    final LongTreeChain.TreeCursor cursor = tree.getCursor();
                    final int functionCount = functions.size();
                    while (cursor.hasNext()) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        offset = cursor.next();
                        recordChain.recordAt(chainRecord, offset);
                        for (int j = 0; j < functionCount; j++) {
                            functions.getQuick(j).pass1(chainRecord, offset, recordChain);
                        }
                    }
                }
            }

            // run pass1 for all unordered functions
            if (unorderedFunctions != null) {
                for (int j = 0, n = unorderedFunctions.size(); j < n; j++) {
                    final AnalyticFunction f = unorderedFunctions.getQuick(j);
                    recordChain.toTop();
                    while (recordChain.hasNext()) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        f.pass1(chainRecord, chainRecord.getRowId(), recordChain);
                    }
                }
            }

            recordChain.toTop();
        }

        private void of(RecordCursor base, SqlExecutionContext context) {
            this.base = base;
            isRecordChainBuilt = false;
            recordChainOffset = -1;
            circuitBreaker = context.getCircuitBreaker();
            if (!isOpen) {
                recordChain.reopen();
                recordChain.setSymbolTableResolver(this);
                reopenTrees();
                reopen(allFunctions);
                isOpen = true;
            }
        }

        private void reopen(ObjList<?> list) {
            for (int i = 0, n = list.size(); i < n; i++) {
                if (list.getQuick(i) instanceof Reopenable) {
                    ((Reopenable) list.getQuick(i)).reopen();
                }
            }
        }

        private void reopenTrees() {
            for (int i = 0; i < orderedGroupCount; i++) {
                orderedSources.getQuick(i).reopen();
            }
        }
    }
}
