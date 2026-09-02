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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.RowCursorFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Returns rows from current page frame in table (physical) order:
 * - fetches first record index/row id per cursor into priority queue
 * - then returns record with the smallest available index and adds next
 * record from related cursor into queue until all cursors are exhausted.
 */
public class HeapRowCursorFactory implements RowCursorFactory {
    // @TestOnly observability for the loop bound in getCursor(). A row cursor opened past the live
    // key count never reaches the result set -- HeapRowCursor.of() seeds the heap only up to that
    // bound -- so only a counter can tell a wasted index seek from a necessary one. A plain static
    // boolean guards it: the JIT folds the always-false production branch away, and the single test
    // that flips it drives one query on the calling thread.
    @TestOnly
    public static boolean isRowCursorCounterEnabled = false;
    @TestOnly
    public static final AtomicLong testRowCursorsOpened = new AtomicLong();
    private final HeapRowCursor cursor;
    private final ObjList<? extends RowCursorFactory> cursorFactories;
    // used to skip some cursor factories if values repeat
    private final int[] cursorFactoriesIdx;
    private final ObjList<RowCursor> cursors;

    public HeapRowCursorFactory(ObjList<? extends RowCursorFactory> cursorFactories, int[] cursorFactoriesIdx) {
        this.cursorFactories = cursorFactories;
        this.cursors = new ObjList<>();
        this.cursor = new HeapRowCursor();
        this.cursorFactoriesIdx = cursorFactoriesIdx;
    }

    @Override
    public void close() {
        // Free the row cursors directly off the factory's own list. getCursor()
        // builds them into `cursors` and only later hands the list to the
        // HeapRowCursor via cursor.of(); if getCursor() throws after building
        // some cursors but before that hand-off (e.g. an OOM mid-build on the
        // very first page frame), HeapRowCursor.cursors is still null and
        // Misc.free(cursor) would never reach them. Draining `cursors` here
        // reclaims those stranded cursors; once cursor.of() has run it shares
        // this same list, so the HeapRowCursor's own close() drains it to empty
        // and this call is a no-op.
        Misc.freeObjListAndClear(cursors);
        Misc.free(cursor);
    }

    @Override
    public RowCursor getCursor(PageFrame pageFrame, PageFrameMemory pageFrameMemory) {
        Misc.freeObjListAndClear(cursors);
        // Bound by the live key count rather than by cursorFactories.size(): owners grow that list
        // monotonically across executions and re-arm only its first cursorFactoriesIdx[0] entries, so
        // a factory past that bound still carries a symbol key from an earlier execution. cursor.of()
        // seeds the heap up to the same bound, so opening the rest only spends an index seek per page
        // frame on rows nothing reads. SequentialRowCursorFactory.getCursor() already bounds itself
        // this way.
        final int activeCursors = cursorFactoriesIdx[0];
        for (int i = 0; i < activeCursors; i++) {
            cursors.extendAndSet(i, cursorFactories.getQuick(i).getCursor(pageFrame, pageFrameMemory));
            // Count each open individually, not activeCursors once: the counter has to observe the
            // loop bound, not restate it, or a regression that opened cursorFactories.size() cursors
            // (the stale-key seeks this bound exists to avoid) would still report activeCursors.
            if (isRowCursorCounterEnabled) {
                testRowCursorsOpened.incrementAndGet();
            }
        }
        cursor.of(cursors, activeCursors);
        return cursor;
    }

    @Override
    public void init(PageFrameCursor pageFrameCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
        RowCursorFactory.init(cursorFactories, pageFrameCursor, sqlExecutionContext);
    }

    @Override
    public boolean isEntity() {
        return false;
    }

    @Override
    public boolean isUsingIndex() {
        return true;
    }

    @Override
    public void prepareCursor(PageFrameCursor pageFrameCursor) {
        RowCursorFactory.prepareCursor(cursorFactories, pageFrameCursor);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Table-order scan");
        for (int i = 0, n = cursorFactories.size(); i < n; i++) {
            sink.child(cursorFactories.getQuick(i));
        }
    }
}
