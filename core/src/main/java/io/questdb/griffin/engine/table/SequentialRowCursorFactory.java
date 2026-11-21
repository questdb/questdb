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

import io.questdb.cairo.CairoException;
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
import org.jetbrains.annotations.Nullable;

/**
 * Returns rows from current page frame in order of cursors list:
 * - first fetches and returns all records from first cursor
 * - then from second cursor, third, ...
 * until all cursors are exhausted.
 */
public class SequentialRowCursorFactory implements RowCursorFactory {
    protected final int[] cursorFactoriesIdx;
    private final SequentialRowCursor cursor;
    private final ObjList<? extends RowCursorFactory> cursorFactories;
    private final ObjList<RowCursor> cursors;
    private int cursorLo;

    public SequentialRowCursorFactory(ObjList<? extends RowCursorFactory> cursorFactories, int[] cursorFactoriesIdx) {
        this.cursorFactories = cursorFactories;
        cursors = new ObjList<>();
        cursor = new SequentialRowCursor();
        this.cursorFactoriesIdx = cursorFactoriesIdx;
    }

    @Override
    public void close() {
        Misc.free(cursor);
    }

    @Override
    public RowCursor getCursor(PageFrame pageFrame, PageFrameMemory pageFrameMemory) {
        return getCursor(pageFrame, pageFrameMemory, 0, cursorFactoriesIdx[0]);
    }

    /**
     * The cursor over the rows in the page frame of the cursors at {@code cursorLo}, inclusive, to {@code cursorHi},
     * exclusive, one cursor after another.
     */
    public RowCursor getCursor(PageFrame pageFrame, PageFrameMemory pageFrameMemory, int cursorLo, int cursorHi) {
        return getCursor(null, pageFrame, pageFrameMemory, cursorLo, cursorHi);
    }

    /**
     * Opens the requested range of cursors, taking ownership of {@code firstCursor} when supplied.
     */
    protected RowCursor getCursor(@Nullable RowCursor firstCursor, PageFrame pageFrame, PageFrameMemory pageFrameMemory, int cursorLo, int cursorHi) {
        try {
            cursor.close();
            cursors.setPos(cursorHi - cursorLo);
            int i = cursorLo;
            if (firstCursor != null) {
                cursors.setQuick(0, firstCursor);
                firstCursor = null;
                i++;
            }
            for (; i < cursorHi; i++) {
                cursors.setQuick(i - cursorLo, cursorFactories.getQuick(i).getCursor(pageFrame, pageFrameMemory));
            }
            this.cursorLo = cursorLo;
            cursor.init();
            return cursor;
        } catch (Throwable th) {
            Misc.free(firstCursor, th);
            Misc.freeObjList(cursors, th);
            cursors.clear();
            throw th;
        }
    }

    /**
     * The index of the cursor whose rows the cursor of the page frame returns, among all the cursors of the factory.
     */
    protected int getCursorIndex() {
        return cursorLo + cursor.cursorIndex;
    }

    @Override
    public void init(PageFrameCursor pageFrameCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
        RowCursorFactory.init(cursorFactories, pageFrameCursor, sqlExecutionContext);
    }

    @Override
    public boolean isEntity() {
        return false;
    }

    public boolean isUsingIndex() {
        return true;
    }

    @Override
    public void prepareCursor(PageFrameCursor pageFrameCursor) {
        RowCursorFactory.prepareCursor(cursorFactories, pageFrameCursor);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Cursor-order scan"); // postgres uses 'Append' node
        for (int i = 0, n = cursorFactories.size(); i < n; i++) {
            sink.child(cursorFactories.getQuick(i));
        }
    }

    private class SequentialRowCursor implements RowCursor {
        private RowCursor currentCursor;
        private int cursorIndex = 0;

        @Override
        public void close() {
            final Throwable failure = Misc.freeObjListBestEffort(null, cursors);
            cursors.clear();
            currentCursor = null;
            CairoException.rethrowCleanupFailure(failure);
        }

        @Override
        public boolean hasNext() {
            // Guard against an empty key set (cursorFactoriesIdx[0] == 0): currentCursor is null.
            if (currentCursor == null) {
                return false;
            }
            boolean hasNext = currentCursor.hasNext();
            if (hasNext) {
                return true;
            }

            while (cursorIndex < cursors.size() - 1) {
                currentCursor = cursors.getQuick(++cursorIndex);
                if (currentCursor.hasNext()) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public long next() {
            return currentCursor.next();
        }

        private void init() {
            cursorIndex = 0;
            currentCursor = cursors.size() > 0 ? cursors.getQuick(0) : null;
        }
    }
}
