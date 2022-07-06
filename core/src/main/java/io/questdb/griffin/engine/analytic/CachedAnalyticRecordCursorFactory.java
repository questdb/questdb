/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SymbolTable;
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
    private final CachedAnalyticRecordCursor cursor;
    private final RecordChain recordChain;
    private final RecordCursorFactory base;
    private final ObjList<LongTreeChain> orderedSources;
    private final int orderedGroupCount;
    private final ObjList<ObjList<AnalyticFunction>> orderedFunctions;
    @Nullable private final ObjList<AnalyticFunction> unorderedFunctions;
    private final ObjList<AnalyticFunction> allFunctions;
    private final ObjList<RecordComparator> comparators;
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
        this.orderedSources = new ObjList<>(orderedGroupCount);
        this.orderedFunctions = orderedFunctions;
        this.comparators = comparators;
        this.cursor = new CachedAnalyticRecordCursor(columnIndexes);
        this.recordChain = new RecordChain(
                chainMetadata,
                recordSink,
                configuration.getSqlAnalyticStorePageSize(),
                configuration.getSqlAnalyticStoreMaxPages()
        );
        recordChain.setSymbolTableResolver(cursor);

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
    protected void _close() {
        if (closed) {
            return;
        }
        Misc.free(base);
        Misc.free(cursor);
        Misc.free(recordChain);
        Misc.freeObjList(orderedSources);
        Misc.freeObjList(allFunctions);
        closed = true;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        recordChain.clear();
        clearTrees();
        resetFunctions();

        final RecordCursor baseCursor = base.getCursor(executionContext);
        cursor.of(baseCursor);
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
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

    private void clearTrees() {
        for (int i = 0; i < orderedGroupCount; i++) {
            orderedSources.getQuick(i).clear();
        }
    }

    public class CachedAnalyticRecordCursor implements RecordCursor {
        private RecordCursor base;
        private final IntList columnIndexes; // Used for symbol table lookups.

        public CachedAnalyticRecordCursor(IntList columnIndexes) {
            this.columnIndexes = columnIndexes;
        }

        private void of(RecordCursor base) {
            this.base = base;
            buildRecordChain();
        }

        private void buildRecordChain() {
            // step #1: store source cursor in record list
            // - add record list' row ids to all trees, which will put these row ids in necessary order
            // for this we will be using out comparator, which helps tree compare long values
            // based on record these values are addressing
            long offset = -1;
            final Record record = base.getRecord();
            final Record chainRecord = recordChain.getRecord();
            final Record chainRightRecord = recordChain.getRecordB();
            if (orderedGroupCount > 0) {
                while (base.hasNext()) {
                    offset = recordChain.put(record, offset);
                    recordChain.recordAt(chainRecord, offset);
                    for (int i = 0; i < orderedGroupCount; i++) {
                        orderedSources.getQuick(i).put(chainRecord, recordChain, chainRightRecord, comparators.getQuick(i));
                    }
                }
            } else {
                while (base.hasNext()) {
                    offset = recordChain.put(record, offset);
                }
            }

            if (orderedGroupCount > 0) {
                for (int i = 0; i < orderedGroupCount; i++) {
                    final LongTreeChain tree = orderedSources.getQuick(i);
                    final ObjList<AnalyticFunction> functions = orderedFunctions.getQuick(i);
                    // step #2: populate all analytic functions with records in order of respective tree
                    final LongTreeChain.TreeCursor cursor = tree.getCursor();
                    final int functionCount = functions.size();
                    while (cursor.hasNext()) {
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
                        f.pass1(chainRecord, chainRecord.getRowId(), recordChain);
                    }
                }
            }

            recordChain.toTop();
        }

        @Override
        public void close() {
            base.close();
        }

        @Override
        public Record getRecord() {
            return recordChain.getRecord();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return base.getSymbolTable(columnIndexes.getQuick(columnIndex));
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return base.newSymbolTable(columnIndexes.getQuick(columnIndex));
        }

        @Override
        public boolean hasNext() {
            return recordChain.hasNext();
        }

        @Override
        public long size() {
            return recordChain.size();
        }

        @Override
        public Record getRecordB() {
            return recordChain.getRecordB();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            recordChain.recordAt(record, atRowId);
        }

        @Override
        public void toTop() {
            recordChain.toTop();
        }
    }
}
