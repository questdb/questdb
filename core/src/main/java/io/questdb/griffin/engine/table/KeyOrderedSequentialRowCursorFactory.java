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
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.ObjList;

/**
 * The sequential row cursor factory of a scan ordered by its keys, each cursor of which reads one key.
 */
class KeyOrderedSequentialRowCursorFactory extends SequentialRowCursorFactory implements KeyOrderedRowCursorFactory {
    private final int indexDirection;
    private final ObjList<? extends FunctionBasedRowCursorFactory> keyCursorFactories;

    KeyOrderedSequentialRowCursorFactory(ObjList<? extends FunctionBasedRowCursorFactory> cursorFactories, int[] cursorFactoriesIdx, int indexDirection) {
        super(cursorFactories, cursorFactoriesIdx);
        this.keyCursorFactories = cursorFactories;
        this.indexDirection = indexDirection;
    }

    @Override
    public RowCursor getCursor(PageFrame pageFrame, PageFrameMemory pageFrameMemory, int keyLo, int keyHi, long firstRow) {
        return getCursor(keyCursorFactories.getQuick(keyLo).getCursor(pageFrame, pageFrameMemory, firstRow), pageFrame, pageFrameMemory, keyLo, keyHi);
    }

    @Override
    public int getIndexDirection() {
        return indexDirection;
    }

    @Override
    public int getKeyCount() {
        return cursorFactoriesIdx[0];
    }

    @Override
    public long getRow(PageFrame pageFrame, int keyIndex, long rowLo, long rowHi, long rowIndex) {
        return keyCursorFactories.getQuick(keyIndex).getRow(pageFrame, rowLo, rowHi, rowIndex);
    }

    @Override
    public long getRowCount(PageFrame pageFrame, int keyLo, int keyHi, long rowLo, long rowHi, long maxCount) {
        long count = 0;
        for (int i = keyLo; i < keyHi && count <= maxCount; i++) {
            final long keyRowCount = keyCursorFactories.getQuick(i).getRowCount(pageFrame, rowLo, rowHi, maxCount - count);
            if (keyRowCount < 0) {
                return -1;
            }
            count += keyRowCount;
        }
        return count;
    }
}
