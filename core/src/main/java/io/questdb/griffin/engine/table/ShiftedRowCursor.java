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

import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.Misc;

/**
 * The rows of another cursor moved by a fixed number of rows. An index cursor numbers its rows from the low bound of
 * its range, so a cursor opened at a row inside a page frame numbers them from the start of the page frame through it.
 */
final class ShiftedRowCursor implements RowCursor {
    private RowCursor base;
    private long shift;

    @Override
    public void close() {
        base = Misc.free(base);
    }

    @Override
    public boolean hasNext() {
        return base.hasNext();
    }

    @Override
    public long next() {
        return base.next() + shift;
    }

    ShiftedRowCursor of(RowCursor base, long shift) {
        this.base = base;
        this.shift = shift;
        return this;
    }
}
