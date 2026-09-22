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

package io.questdb.griffin.engine.union;

import io.questdb.cairo.sql.RecordCursor;

/**
 * Symbol-source bookkeeping shared by the set cursors that a
 * {@link UnionSymbolCastRecordCursorFactory} projection can sit on. The projection caches a key
 * translation per source dictionary, so it has to know which leg feeds the current row.
 * <p>
 * {@link #bindSymbolSourceTracker} hands every leaf leg its own index and lets a leg that is
 * itself a union number its own leaves instead of taking an index; the subclass then reports each
 * side switch through {@link #updateSymbolSource()}. Deliberately a separate class rather than
 * part of {@link AbstractSetRecordCursor}, so EXCEPT and INTERSECT do not inherit machinery they
 * never use.
 */
abstract class AbstractUnionSymbolSourceCursor extends AbstractSetRecordCursor implements UnionSymbolSourceCursor {
    protected boolean isUsingCursorA;
    private int symbolSourceIndexA = -1;
    private int symbolSourceIndexB = -1;
    private SymbolSourceTracker symbolSourceTracker;

    @Override
    public int bindSymbolSourceTracker(SymbolSourceTracker tracker, int nextSourceIndex) {
        symbolSourceTracker = tracker;
        if (cursorA instanceof UnionSymbolSourceCursor sourceCursor) {
            nextSourceIndex = sourceCursor.bindSymbolSourceTracker(tracker, nextSourceIndex);
            symbolSourceIndexA = -1;
        } else {
            symbolSourceIndexA = nextSourceIndex++;
        }
        if (cursorB instanceof UnionSymbolSourceCursor sourceCursor) {
            nextSourceIndex = sourceCursor.bindSymbolSourceTracker(tracker, nextSourceIndex);
            symbolSourceIndexB = -1;
        } else {
            symbolSourceIndexB = nextSourceIndex++;
        }
        return nextSourceIndex;
    }

    @Override
    public boolean hasKeyValueSymbolTable(int columnIndex) {
        return UnionSymbolSourceCursor.hasKeyValueSymbolTable(cursorA, columnIndex)
                || UnionSymbolSourceCursor.hasKeyValueSymbolTable(cursorB, columnIndex);
    }

    @Override
    public void updateSymbolSource() {
        updateSymbolSource(isUsingCursorA ? cursorA : cursorB, isUsingCursorA ? symbolSourceIndexA : symbolSourceIndexB);
    }

    private void updateSymbolSource(RecordCursor cursor, int sourceIndex) {
        if (symbolSourceTracker != null) {
            if (cursor instanceof UnionSymbolSourceCursor sourceCursor) {
                sourceCursor.updateSymbolSource();
            } else {
                symbolSourceTracker.of(cursor, sourceIndex);
            }
        }
    }
}
