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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.DelegatingRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;

/**
 * The probe record a staged key sink reads from when the key holds SYMBOL columns. A
 * {@link RecordSink} stages a SYMBOL key column as {@code getInt()}, so intercepting that one
 * accessor on the key's symbol columns is enough to hand the sink build-domain keys, while the
 * projection and the filters keep reading the probe record itself and see the probe's own keys.
 * <p>
 * One instance per worker: each holds its own {@link SymbolKeyTranslator.View} per symbol key
 * column, and the views hold that worker's symbol tables. {@link #close()} releases the views,
 * which is what ends the execution's borrowing of those tables.
 */
public final class SymbolKeyTranslatingRecord extends DelegatingRecord implements QuietCloseable {
    // Probe column index -> index into views; -1 for every column the sink copies as it is.
    private final IntList columnToView = new IntList();
    private final ObjList<SymbolKeyTranslator.View> views = new ObjList<>();

    /** Symbol key columns are probe-record indexes; the count bounds every index the sink reads. */
    public SymbolKeyTranslatingRecord(int probeColumnCount, IntList symbolKeyColumns) {
        columnToView.setAll(probeColumnCount, -1);
        for (int i = 0, n = symbolKeyColumns.size(); i < n; i++) {
            final int column = symbolKeyColumns.getQuick(i);
            if (columnToView.getQuick(column) >= 0) {
                // The planner rejects such a key, since one column can carry only one translation.
                throw new IllegalArgumentException("hash join probe column translates twice");
            }
            columnToView.setQuick(column, i);
            views.add(new SymbolKeyTranslator.View());
        }
    }

    @Override
    public void close() {
        // The views go back to unbound, not away: the next execution rebinds them.
        Misc.freeObjListAndKeepObjects(views);
    }

    @Override
    public int getInt(int col) {
        final int view = columnToView.getQuick(col);
        return view < 0 ? base.getInt(col) : views.getQuick(view).translate(base.getInt(col));
    }

    /** The view of one symbol key column, in the order the constructor took the columns. */
    public SymbolKeyTranslator.View getView(int key) {
        return views.getQuick(key);
    }

    public void of(Record base) {
        this.base = base;
    }
}
