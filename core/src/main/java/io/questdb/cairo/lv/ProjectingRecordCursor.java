/*+*****************************************************************************
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

package io.questdb.cairo.lv;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.VirtualFunctionRecord;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.memoization.MemoizerFunction;
import io.questdb.std.ObjList;

/**
 * Evaluates a scalar projection over a base {@link RecordCursor}, one row at a time. The
 * refresh-path counterpart of the planner's {@code VirtualRecordCursorFactory}, which is
 * what an expression in the SELECT list compiles to.
 * <p>
 * A live view carries at most two of these, on either side of the window (see
 * {@link LiveViewCompiledPlan}):
 * <ul>
 *     <li><b>input</b> - {@code px * 2} feeding a window function's argument or its
 *     PARTITION BY key. Sits below the window, so the window sees the shape it was
 *     compiled against.</li>
 *     <li><b>output</b> - {@code px - avg(px) OVER (...)}. Sits above the window, and
 *     produces the view's own schema, which is what the copier writes and the in-memory
 *     tier stores.</li>
 * </ul>
 * Either way the projection is a pure function of the row it is handed, so it holds no
 * state across rows and adds nothing an O3 replay or a checkpoint restore has to
 * reproduce: replaying the same base rows through the same window state re-derives the
 * same projected values. That is what makes an expression over a window function
 * incrementally maintainable at all, and why the window state a checkpoint captures is
 * indexed by the window factory's own output positions rather than the view's columns.
 * <p>
 * The functions are the compiled factory's own, borrowed rather than owned: the factory
 * frees them, and {@link #close()} only drops the references. They are re-initialised on
 * every {@link #of} so bind variables and symbol-table caches reflect the current base
 * cursor.
 * <p>
 * Not to be confused with the table-package VirtualFunctionRecordCursor, which is tied to
 * a VirtualRecordCursorFactory lifecycle and rewinds its base on bind.
 */
final class ProjectingRecordCursor implements RecordCursor {
    private final ObjList<Function> functions;
    private final ObjList<MemoizerFunction> memoizers = new ObjList<>();
    private final VirtualFunctionRecord recordA;
    private final SplitSymbolTableSource symbolTableSource = new SplitSymbolTableSource();
    private RecordCursor base;

    /**
     * @param functions                  the projection's output functions, one per output column,
     *                                   owned by the compiled factory
     * @param virtualColumnReservedSlots where the projection's own outputs stop and its base's
     *                                   columns start in the function address space, which is what
     *                                   lets a projection reference a column it produced itself
     */
    ProjectingRecordCursor(ObjList<Function> functions, int virtualColumnReservedSlots) {
        this.functions = functions;
        this.recordA = new VirtualFunctionRecord(functions, virtualColumnReservedSlots);
        for (int i = 0, n = functions.size(); i < n; i++) {
            if (functions.getQuick(i) instanceof MemoizerFunction m) {
                memoizers.add(m);
            }
        }
        // `this` rather than the record: a self-referenced column resolves through
        // getSymbolTable(i), which is the projecting cursor's own function-backed answer.
        this.symbolTableSource.of(this, virtualColumnReservedSlots);
    }

    @Override
    public void close() {
        // The functions belong to the compiled factory, which frees them; dropping the
        // base reference is all this cursor owns.
        base = null;
        recordA.of(null);
        symbolTableSource.ofBase(null);
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public Record getRecordB() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return (SymbolTable) functions.getQuick(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (!base.hasNext()) {
            return false;
        }
        for (int i = 0, n = memoizers.size(); i < n; i++) {
            memoizers.getQuick(i).memoize(recordA.getInternalJoinRecord());
        }
        return true;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return ((SymbolFunction) functions.getQuick(columnIndex)).newSymbolTable();
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        base.recordAt(((VirtualFunctionRecord) record).getBaseRecord(), atRowId);
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public void toTop() {
        base.toTop();
        for (int i = 0, n = functions.size(); i < n; i++) {
            functions.getQuick(i).toTop();
        }
    }

    /**
     * Binds this cursor to {@code base} and re-initialises the projection's functions
     * against it. Deliberately does not rewind: the refresh path skips already-folded
     * rows on the cursor underneath it after the whole chain is built, and a rewind here
     * would undo that skip - and, above the window, would rewind a cursor whose window
     * state is mid-stream.
     */
    void of(RecordCursor base, SqlExecutionContext executionContext) throws SqlException {
        this.base = base;
        this.recordA.of(base.getRecord());
        this.symbolTableSource.ofBase(base);
        Function.init(functions, symbolTableSource, executionContext, null);
    }

    /**
     * Addresses the projection's own outputs below {@code virtualColumnReservedSlots} and
     * its base's columns above, mirroring how {@link VirtualFunctionRecord} splits its
     * internal join record. A projection function initialised against the bare base
     * cursor would resolve a self-referenced symbol column against the wrong side.
     */
    private static final class SplitSymbolTableSource implements SymbolTableSource {
        private RecordCursor base;
        private RecordCursor own;
        private int virtualColumnReservedSlots;

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            if (columnIndex < virtualColumnReservedSlots) {
                return own.getSymbolTable(columnIndex);
            }
            return base.getSymbolTable(columnIndex - virtualColumnReservedSlots);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            if (columnIndex < virtualColumnReservedSlots) {
                return own.newSymbolTable(columnIndex);
            }
            return base.newSymbolTable(columnIndex - virtualColumnReservedSlots);
        }

        void of(RecordCursor own, int virtualColumnReservedSlots) {
            this.own = own;
            this.virtualColumnReservedSlots = virtualColumnReservedSlots;
        }

        void ofBase(RecordCursor base) {
            this.base = base;
        }
    }
}
