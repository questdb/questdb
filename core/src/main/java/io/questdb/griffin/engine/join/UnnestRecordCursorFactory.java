/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordFanOut;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.TestOnly;

public class UnnestRecordCursorFactory extends AbstractRecordCursorFactory implements RecordFanOut {
    private final ObjList<CharSequence> columnNames;
    private final boolean hasOrdinality;
    private RecordCursorFactory baseFactory;
    private UnnestRecordCursor cursor;
    private ObjList<Function> functions;
    private ObjList<UnnestSource> sources;

    public UnnestRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory baseFactory,
            ObjList<Function> functions,
            ObjList<UnnestSource> sources,
            int columnSplit,
            boolean hasOrdinality,
            ObjList<CharSequence> columnNames
    ) {
        super(metadata);
        this.baseFactory = baseFactory;
        this.functions = functions;
        this.sources = sources;
        this.hasOrdinality = hasOrdinality;
        this.columnNames = columnNames;
        if (hasOrdinality) {
            sources.add(new OrdinalityUnnestSource());
        }
        this.cursor = new UnnestRecordCursor(columnSplit, sources);
    }

    @Override
    public boolean followedOrderByAdvice() {
        return baseFactory.followedOrderByAdvice();
    }

    @Override
    public RecordCursor getCursor(
            SqlExecutionContext executionContext
    ) throws SqlException {
        RecordCursor baseCursor = baseFactory.getCursor(executionContext);
        try {
            Function.init(
                    functions, baseCursor, executionContext, null
            );
            cursor.of(
                    baseCursor,
                    executionContext.getCircuitBreaker()
            );
            return cursor;
        } catch (Throwable ex) {
            Misc.free(baseCursor);
            throw ex;
        }
    }

    /**
     * Returns the rows that UNNEST returns for a row of the scan below it: one for each element of its longest
     * array. The scan calls it ahead of the cursor, on its own rows, so the code generator hands UNNEST over
     * only when its expressions read the columns of the scan alone, see SqlCodeGenerator.attachUnnestFanOut().
     * The cursor binds the sources to each row again before it returns the row.
     */
    @Override
    public long getFanOut(Record scanRecord) {
        return initSources(sources, scanRecord);
    }

    @Override
    public int getScanDirection() {
        return baseFactory.getScanDirection();
    }

    /**
     * Returns the base factory, which the factory does not expose through getBaseFactory().
     */
    @TestOnly
    public RecordCursorFactory getUnnestBaseFactory() {
        return baseFactory;
    }

    /**
     * Returns true when every UNNEST expression returns the same array whenever it reads the same row, so
     * that a scan below can count the rows of UNNEST ahead of it, see getFanOut().
     */
    public boolean isFanOutStable() {
        for (int i = 0, n = functions.size(); i < n; i++) {
            if (!functions.getQuick(i).isStableWithinExecution()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Unnest");
        sink.attr("columns").val(columnNames);
        if (hasOrdinality) {
            sink.attr("ordinality").val(true);
        }
        sink.child(baseFactory);
    }

    // Wraps a single input without exposing it through getBaseFactory(), so the external-source
    // property is propagated explicitly. Guards against a null base during teardown.
    @Override
    public boolean usesExternalDataSource() {
        final RecordCursorFactory baseFactory = this.baseFactory;
        return baseFactory != null && baseFactory.usesExternalDataSource();
    }

    @Override
    protected void _close() {
        final RecordMetadata metadata = detachMetadata();
        final RecordCursorFactory baseFactory = this.baseFactory;
        this.baseFactory = null;
        final UnnestRecordCursor cursor = this.cursor;
        this.cursor = null;
        final ObjList<Function> functions = this.functions;
        this.functions = null;
        final ObjList<UnnestSource> sources = this.sources;
        this.sources = null;

        Throwable failure = Misc.freeIfCloseableBestEffort(null, metadata);
        failure = Misc.freeBestEffort(failure, cursor);
        failure = Misc.freeBestEffort(failure, baseFactory);
        failure = Misc.freeObjListBestEffort(failure, functions);
        failure = Misc.freeObjListIfCloseableBestEffort(failure, sources);
        CairoException.rethrowCleanupFailure(failure);
    }

    // Binds the sources to the base row and returns the length of the longest of them, 0 when every array
    // of the row is empty or NULL.
    private static int initSources(ObjList<UnnestSource> sources, Record baseRecord) {
        int maxArrayLen = 0;
        for (int i = 0, n = sources.size(); i < n; i++) {
            final int len = sources.getQuick(i).init(baseRecord);
            if (len > maxArrayLen) {
                maxArrayLen = len;
            }
        }
        return maxArrayLen;
    }

    private static class UnnestRecordCursor implements NoRandomAccessRecordCursor {
        private final UnnestRecord record;
        private final ObjList<UnnestSource> sources;
        private int arrayIndex;
        private RecordCursor baseCursor;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isMasterPending;
        private int maxArrayLen;

        public UnnestRecordCursor(
                int columnSplit,
                ObjList<UnnestSource> sources
        ) {
            this.sources = sources;
            this.record = new UnnestRecord(columnSplit, sources);
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            while (hasNext()) {
                counter.inc();
            }
        }

        @Override
        public void close() {
            baseCursor = Misc.free(baseCursor);
        }

        @Override
        public void expectLimitedIteration() {
            // UNNEST reads its base as it returns rows, as a join reads its master, and skips the rows of
            // an empty array: the cap limits how far the tasks of the base run ahead, never the rows it returns
            baseCursor.expectLimitedIteration();
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            if (columnIndex < record.getSplit()) {
                return baseCursor.getSymbolTable(columnIndex);
            }
            return null;
        }

        @Override
        public boolean hasNext() {
            while (true) {
                circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
                if (isMasterPending) {
                    if (!baseCursor.hasNext()) {
                        return false;
                    }
                    initSources();
                    isMasterPending = false;
                    arrayIndex = 0;
                    if (maxArrayLen == 0) {
                        isMasterPending = true;
                        continue;
                    }
                }
                if (arrayIndex < maxArrayLen) {
                    record.setArrayIndex(arrayIndex);
                    arrayIndex++;
                    return true;
                }
                isMasterPending = true;
            }
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            if (columnIndex < record.getSplit()) {
                return baseCursor.newSymbolTable(columnIndex);
            }
            return null;
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
            isMasterPending = true;
            arrayIndex = 0;
            maxArrayLen = 0;
        }

        private void initSources() {
            maxArrayLen = UnnestRecordCursorFactory.initSources(sources, baseCursor.getRecord());
        }

        void of(
                RecordCursor baseCursor,
                SqlExecutionCircuitBreaker circuitBreaker
        ) {
            this.baseCursor = baseCursor;
            this.circuitBreaker = circuitBreaker;
            this.isMasterPending = true;
            this.arrayIndex = 0;
            this.maxArrayLen = 0;
            Record baseRecord = baseCursor.getRecord();
            record.of(baseRecord);
        }
    }
}
