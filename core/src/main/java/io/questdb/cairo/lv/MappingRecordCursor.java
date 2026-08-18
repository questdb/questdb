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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.engine.table.SelectedRecord;
import io.questdb.std.IntList;

/**
 * Re-maps a base {@link RecordCursor}'s columns through a cross index, so column
 * {@code i} of this cursor is column {@code crossIndex[i]} of the base. The refresh-path
 * counterpart of the planner's {@code SelectedRecordCursorFactory}, which is what an
 * alias, a column reorder or a dropped column between the base scan and the window
 * compiles to.
 * <p>
 * The live view refresh job uses this wrapper to feed WAL segment rows to the window in
 * the shape the window functions were compiled against, without reinvoking the compiled
 * mapping factory's getCursor() (which would open a new base cursor).
 * <p>
 * Pure re-indexing: it neither drops nor reorders rows, so it can sit anywhere in the
 * refresh chain a row-preserving link may sit. One instance belongs to one view - the
 * cross index is baked into {@link SelectedRecord} at construction.
 * <p>
 * Not to be confused with the table-package SelectedRecordCursor, which is tied to a
 * SelectedRecordCursorFactory lifecycle.
 *
 * @see LiveViewCompiledPlan
 */
final class MappingRecordCursor implements RecordCursor {
    private final IntList crossIndex;
    private final SelectedRecord recordA;
    private final SelectedRecord recordB;
    private RecordCursor base;

    MappingRecordCursor(IntList crossIndex) {
        this.crossIndex = crossIndex;
        this.recordA = new SelectedRecord(crossIndex);
        this.recordB = new SelectedRecord(crossIndex);
    }

    @Override
    public void close() {
        base = null;
        recordA.of(null);
        recordB.of(null);
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public Record getRecordB() {
        recordB.of(base.getRecordB());
        return recordB;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return base.getSymbolTable(crossIndex.getQuick(columnIndex));
    }

    @Override
    public boolean hasNext() {
        return base.hasNext();
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return base.newSymbolTable(crossIndex.getQuick(columnIndex));
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        base.recordAt(((SelectedRecord) record).getBaseRecord(), atRowId);
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public void toTop() {
        base.toTop();
    }

    /**
     * Binds this cursor to {@code base}. Deliberately does not rewind: the refresh path
     * skips already-folded rows on the cursor underneath it after the whole chain is
     * built, and a rewind here would undo that skip.
     */
    void of(RecordCursor base) {
        this.base = base;
        this.recordA.of(base.getRecord());
    }
}
