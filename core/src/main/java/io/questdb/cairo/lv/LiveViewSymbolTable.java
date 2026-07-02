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

import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;

/**
 * Symbol table the live-view read path exposes for a SYMBOL column while the
 * in-memory tier may serve the un-flushed lead. It overlays the LV table's
 * committed symbol table (the disk reader's) with {@link LiveViewSymbolCache}'s
 * eager-interned lead symbols:
 * <ul>
 *   <li>a committed id ({@code id < committed symbol count}) resolves against the
 *     disk table;</li>
 *   <li>an id assigned to a value new to the un-flushed lead resolves against the
 *     cache.</li>
 * </ul>
 * Because both bands share the LV-table id space, raw-int-key consumers (WHERE
 * filters, GROUP BY, static ORDER BY) and per-record {@code getSymA} consumers
 * (printing, HTTP / PGWire) both see correct values whether a row came from disk
 * or from the lead. The flush path reuses the same overlay to re-serialise the
 * lead's stored ids back to strings.
 * <p>
 * {@code ownsBase} distinguishes a cursor's shared {@code getSymbolTable} view
 * (does not own the disk table) from a cloned {@code newSymbolTable} view (owns
 * the cloned disk table and closes it).
 */
public class LiveViewSymbolTable implements StaticSymbolTable, QuietCloseable {
    private StaticSymbolTable base;
    private LiveViewSymbolCache cache;
    private int column;
    // Exclusive upper bound of the lead's new-symbol id band - the pinned slot's
    // symbol horizon, captured at the slot's publish. Bounds keyOf's cache scan and
    // sizes getSymbolCount() so a reader never resolves or indexes past the ids its
    // slot carries (see LiveViewSymbolCache threading note).
    private int maxNewIdExclusive;
    private boolean ownsBase;

    @Override
    public void close() {
        if (ownsBase) {
            base = (StaticSymbolTable) Misc.freeIfCloseable(base);
        } else {
            base = null;
        }
        cache = null;
    }

    @Override
    public boolean containsNullValue() {
        return base.containsNullValue();
    }

    @Override
    public int getSymbolCount() {
        return Math.max(base.getSymbolCount(), maxNewIdExclusive);
    }

    @Override
    public int keyOf(CharSequence value) {
        final int k = base.keyOf(value);
        if (k != SymbolTable.VALUE_NOT_FOUND) {
            return k;
        }
        return cache.newSymbolKeyOf(column, value, base.getSymbolCount(), maxNewIdExclusive);
    }

    /**
     * Binds this overlay to a disk symbol table for {@code column}.
     * {@code maxNewIdExclusive} is the pinned slot's symbol horizon
     * ({@link LiveViewInMemoryBuffer#newSymbolMaxId}), which bounds the cache scan
     * and the symbol count. {@code ownsBase} is {@code true} when {@code base} is a
     * freshly cloned table this overlay must close (the {@code newSymbolTable}
     * path), {@code false} when it is borrowed from a live cursor (the
     * {@code getSymbolTable} path).
     */
    public LiveViewSymbolTable of(StaticSymbolTable base, LiveViewSymbolCache cache, int column, int maxNewIdExclusive, boolean ownsBase) {
        this.base = base;
        this.cache = cache;
        this.column = column;
        this.maxNewIdExclusive = maxNewIdExclusive;
        this.ownsBase = ownsBase;
        return this;
    }

    @Override
    public CharSequence valueBOf(int key) {
        final CharSequence s = base.valueBOf(key);
        return s != null ? s : cache.newSymbolValueOf(column, key);
    }

    @Override
    public CharSequence valueOf(int key) {
        final CharSequence s = base.valueOf(key);
        return s != null ? s : cache.newSymbolValueOf(column, key);
    }
}
