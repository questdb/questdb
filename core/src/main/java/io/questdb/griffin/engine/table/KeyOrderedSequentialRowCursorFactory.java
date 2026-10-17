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
import io.questdb.cairo.sql.RowCursorFactory;
import io.questdb.std.ObjList;

/**
 * The sequential row cursor factory of a scan ordered by its keys, each cursor of which reads one key.
 */
class KeyOrderedSequentialRowCursorFactory extends SequentialRowCursorFactory implements KeyOrderedRowCursorFactory {

    KeyOrderedSequentialRowCursorFactory(ObjList<? extends RowCursorFactory> cursorFactories, int[] cursorFactoriesIdx) {
        super(cursorFactories, cursorFactoriesIdx);
    }

    @Override
    public int getKeyCount() {
        return cursorFactoriesIdx[0];
    }

    @Override
    public long getRowCount(PageFrame pageFrame, int keyLo, int keyHi, long rowLo, long rowHi) {
        return -1;
    }
}
