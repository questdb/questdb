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

package io.questdb.griffin.engine.window;


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

public class CachedWindowRecordCursorFactory extends AbstractRecordCursorFactory {
    private final ObjList<WindowFunction> allFunctions;
    private final RecordCursorFactory base;
    private final GenericRecordMetadata chainMetadata;
    private final ObjList<RecordComparator> comparators;
    private final CachedWindowRecordCursor cursor;
    private final ObjList<ObjList<WindowFunction>> ordered2PassFunctions;
    private final ObjList<ObjList<WindowFunction>> orderedFunctions;
    private final int orderedGroupCount;
    private final ObjList<IntList> sortKeys;
    private final ObjList<WindowFunction> unordered2PassFunctions;
    @Nullable
    private final ObjList<WindowFunction> unorderedFunctions;
    private boolean closed = false;

    public CachedWindowRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base,
            RecordSink recordSink,
            GenericRecordMetadata metadata,
            @Transient ColumnTypes chainTypes,
            ObjList<RecordComparator> comparators,
            ObjList<ObjList<WindowFunction>> orderedFunctions,
            @Nullable ObjList<WindowFunction> unorderedFunctions,
            @NotNull IntList columnIndexes,
            @NotNull final ObjList<IntList> sortKeys,
            @NotNull GenericRecordMetadata chainMetadata
    ) {
        super(metadata);
        this.base = base;
        this.orderedGroupCount = comparators.size();
        assert orderedGroupCount == orderedFunctions.size();
        this.orderedFunctions = orderedFunctions;
        this.comparators = comparators;
        RecordChain recordChain = new RecordChain(
                chainTypes,
                recordSink,
                configuration.getSqlWindowStorePageSize(),
                configuration.getSqlWindowStoreMaxPages()
        );
        this.sortKeys = sortKeys;
        this.chainMetadata = chainMetadata;

        ObjList<LongTreeChain> orderedSources = new ObjList<>(orderedGroupCount);
        // red&black trees, one for each comparator where comparator is not null
        for (int i = 0; i < orderedGroupCount; i++) {
            orderedSources.add(
                    new LongTreeChain(
                            configuration.getSqlWindowTreeKeyPageSize(),
                            configuration.getSqlWindowTreeKeyMaxPages(),
                            configuration.getSqlWindowRowIdPageSize(),
                            configuration.getSqlWindowRowIdMaxPages()
                    )
            );
        }

        this.cursor = new CachedWindowRecordCursor(columnIndexes, recordChain, orderedSources);
        this.allFunctions = new ObjList<>();

        ObjList<ObjList<WindowFunction>> orderedTmp = null;
        for (int i = 0, n = orderedFunctions.size(); i < n; i++) {
            ObjList<WindowFunction> functions = orderedFunctions.getQuick(i);
            allFunctions.addAll(functions);

            ObjList<WindowFunction> twoPassFunctions = null;
            for (int j = 0, k = functions.size(); j < k; j++) {
                WindowFunction function = functions.getQuick(j);
                if (function.getPassCount() > WindowFunction.ONE_PASS) {
                    if (twoPassFunctions == null) {
                        twoPassFunctions = new ObjList<WindowFunction>();
                    }
                    twoPassFunctions.add(function);
                }
            }
            if (twoPassFunctions != null) {
                if (orderedTmp == null) {
                    orderedTmp = new ObjList<ObjList<WindowFunction>>();
                }

                orderedTmp.extendAndSet(i, twoPassFunctions);
            }
        }

        ordered2PassFunctions = orderedTmp;

        ObjList<WindowFunction> unorderedTmp = null;
        if (unorderedFunctions != null) {
            allFunctions.addAll(unorderedFunctions);

            for (int i = 0, n = unorderedFunctions.size(); i < n; i++) {
                WindowFunction function = unorderedFunctions.getQuick(i);
                if (function.getPassCount() > WindowFunction.ONE_PASS) {
                    if (unorderedTmp == null) {
                        unorderedTmp = new ObjList<>();
                    }
                    unorderedTmp.add(function);
                }
            }
        }
        this.unordered2PassFunctions = unorderedTmp;

        this.unorderedFunctions = unorderedFunctions;
    }

    @Override
    public boolean followedOrderByAdvice() {
        return base.followedOrderByAdvice();
    }

    @Override
    public String getBaseColumnName(int idx) {
        return chainMetadata.getColumnName(idx);
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
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("CachedWindow");

        boolean oldVal = sink.getUseBaseMetadata();
        try {
            if (orderedFunctions.size() > 0) {
                sink.attr("orderedFunctions");
                sink.val("[");

                sink.useBaseMetadata(true);

                for (int i = 0, n = orderedFunctions.size(); i < n; i++) {
                    if (i > 0) {
                        sink.val(',');
                    }
                    sink.val('[');

                    addSortKeys(sink, sortKeys.getQuick(i));

                    sink.val("] => [");
                    ObjList<WindowFunction> functions = orderedFunctions.getQuick(i);
                    for (int j = 0, k = functions.size(); j < k; j++) {
                        if (j > 0) {
                            sink.val(',');
                        }
                        sink.val(functions.getQuick(j));
                    }

                    sink.val("]");
                }
                sink.val(']');
            }

            sink.optAttr("unorderedFunctions", unorderedFunctions, true);
        } finally {
            sink.useBaseMetadata(oldVal);
        }

        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    private void addSortKeys(PlanSink sink, IntList list) {
        for (int i = 0, n = list.size(); i < n; i++) {
            int colIdx = list.get(i);
            int col = (colIdx > 0 ? colIdx : -colIdx) - 1;
            if (i > 0) {
                sink.val(", ");
            }
            sink.val(chainMetadata.getColumnName(col));
            if (colIdx < 0) {
                sink.val(" ").val("desc");
            }
        }
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

    class CachedWindowRecordCursor implements RecordCursor {

        private final IntList columnIndexes; // Used for symbol table lookups.
        private final ObjList<LongTreeChain> orderedSources;
        private final RecordChain recordChain;
        private RecordCursor baseCursor;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isOpen;
        private boolean isRecordChainBuilt;
        private long recordChainOffset;

        public CachedWindowRecordCursor(IntList columnIndexes, RecordChain recordChain, ObjList<LongTreeChain> orderedSources) {
            this.columnIndexes = columnIndexes;
            this.recordChain = recordChain;
            this.recordChain.setSymbolTableResolver(this);
            this.isOpen = true;
            this.orderedSources = orderedSources;
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, RecordCursor.Counter counter) {
            if (!isRecordChainBuilt) {
                buildRecordChain();
            }
            recordChain.calculateSize(circuitBreaker, counter);
        }

        @Override
        public void close() {
            if (isOpen) {
                Misc.free(baseCursor);
                Misc.free(recordChain);
                for (int i = 0, n = orderedSources.size(); i < n; i++) {
                    Misc.free(orderedSources.getQuick(i));
                }
                resetFunctions();
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
            return baseCursor.getSymbolTable(columnIndexes.getQuick(columnIndex));
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
            return baseCursor.newSymbolTable(columnIndexes.getQuick(columnIndex));
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            recordChain.recordAt(record, atRowId);
        }

        @Override
        public long size() {
            return isRecordChainBuilt ? recordChain.size() : -1;// in case recordChain starts returning actual size
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
            final Record record = baseCursor.getRecord();
            final Record chainRecord = recordChain.getRecord();
            final Record chainRightRecord = recordChain.getRecordB();
            if (orderedGroupCount > 0) {
                while (baseCursor.hasNext()) {
                    recordChainOffset = recordChain.put(record, recordChainOffset);
                    recordChain.recordAt(chainRecord, recordChainOffset);
                    for (int i = 0; i < orderedGroupCount; i++) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        orderedSources.getQuick(i).put(chainRecord, recordChain, chainRightRecord, comparators.getQuick(i));
                    }
                }
            } else {
                while (baseCursor.hasNext()) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                    recordChainOffset = recordChain.put(record, recordChainOffset);
                }
            }

            // step #2: populate all window functions with records in order of respective tree
            // run pass1 for all ordered functions
            long offset;
            if (orderedGroupCount > 0) {
                for (int i = 0; i < orderedGroupCount; i++) {
                    final LongTreeChain tree = orderedSources.getQuick(i);
                    final ObjList<WindowFunction> functions = orderedFunctions.getQuick(i);
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
                    final WindowFunction f = unorderedFunctions.getQuick(j);
                    recordChain.toTop();
                    while (recordChain.hasNext()) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        f.pass1(chainRecord, chainRecord.getRowId(), recordChain);
                    }
                }
            }

            // prepare pass 2 for ordered functions
            if (ordered2PassFunctions != null) {
                for (int i = 0, n = ordered2PassFunctions.size(); i < n; i++) {
                    final ObjList<WindowFunction> functions = ordered2PassFunctions.getQuick(i);
                    if (functions == null) {
                        continue;
                    }
                    for (int j = 0, k = functions.size(); j < k; j++) {
                        functions.getQuick(j).preparePass2();
                    }
                }
            }
            // prepare pass 2 for unordered functions
            if (unordered2PassFunctions != null) {
                for (int j = 0, n = unordered2PassFunctions.size(); j < n; j++) {
                    unordered2PassFunctions.getQuick(j).preparePass2();
                }
            }

            // run pass2 for all ordered functions
            if (ordered2PassFunctions != null) {
                for (int i = 0, n = ordered2PassFunctions.size(); i < n; i++) {
                    final LongTreeChain tree = orderedSources.getQuick(i);
                    final ObjList<WindowFunction> functions = ordered2PassFunctions.getQuick(i);
                    if (functions == null) {
                        continue;
                    }
                    final LongTreeChain.TreeCursor cursor = tree.getCursor();
                    final int functionCount = functions.size();
                    while (cursor.hasNext()) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        offset = cursor.next();
                        recordChain.recordAt(chainRecord, offset);
                        for (int j = 0; j < functionCount; j++) {
                            functions.getQuick(j).pass2(chainRecord, offset, recordChain);
                        }
                    }
                }
            }

            // run pass2 for all unordered functions
            if (unordered2PassFunctions != null) {
                for (int j = 0, n = unordered2PassFunctions.size(); j < n; j++) {
                    final WindowFunction f = unordered2PassFunctions.getQuick(j);
                    recordChain.toTop();
                    while (recordChain.hasNext()) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        f.pass2(chainRecord, chainRecord.getRowId(), recordChain);
                    }
                }
            }

            recordChain.toTop();
        }

        private void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
            this.baseCursor = baseCursor;
            isRecordChainBuilt = false;
            recordChainOffset = -1;
            circuitBreaker = executionContext.getCircuitBreaker();
            if (!isOpen) {
                recordChain.reopen();
                recordChain.setSymbolTableResolver(this);
                reopenTrees();
                reopen(allFunctions);
                isOpen = true;
            }
            Function.init(allFunctions, this, executionContext);
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
