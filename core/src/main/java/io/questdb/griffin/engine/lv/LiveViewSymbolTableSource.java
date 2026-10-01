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

package io.questdb.griffin.engine.lv;

import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.cairo.lv.LiveViewSymbolCache;
import io.questdb.cairo.lv.LiveViewSymbolTable;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;

/**
 * SYMBOL resolution for a live-view read that routes through the in-memory tier.
 * Wraps the disk scan's own symbol source and, for every SYMBOL column the read
 * projects, overlays it with the pinned slot's band of eager-interned lead symbols
 * (see {@link LiveViewSymbolCache}), so a value that exists only in the un-flushed
 * lead resolves from RAM. Both bands share one LV-table id space, so a per-record
 * {@code getSymA} read and a raw-int-key read (WHERE / GROUP BY / static ORDER BY)
 * agree whether the row came from disk or from the lead.
 * <p>
 * The source owns no tier state of its own: it holds the pin's slot and cache by
 * reference and stays valid exactly as long as its binder keeps the slot pinned.
 * Whoever the engine hands a read to - {@link LiveViewRecordCursor} or
 * {@link LiveViewPageFrameCursor} - binds one of these and answers
 * {@link SymbolTableSource} through it. Keeping the overlay here rather than inline
 * in the record cursor is what lets a frame consumer (a parallel filter worker
 * resolving symbols through the frame cursor rather than through a record) see the
 * lead at all.
 * <p>
 * Column indices are OUTPUT columns throughout, as {@link SymbolTableSource}
 * defines them. The cache and the slot's symbol horizon, however, key off the TIER
 * column, which a pruned or reordered projection no longer numbers the same - so
 * every cache-side lookup goes through {@code tierColumns} (see
 * {@link LiveViewRecordCursor#isTierAddressableProjection}). A mis-keyed probe does
 * not merely resolve the wrong string: {@link LiveViewSymbolCache#isSymbolColumn}
 * reports a SYMBOL column as plain, no overlay is built, and a lead-only value then
 * matches nothing at all.
 * <p>
 * Each overlay is bounded by its slot's own symbol horizon
 * ({@link LiveViewInMemoryBuffer#newSymbolMaxId}), stamped at publish, so a reader
 * never resolves or indexes past the ids its slot carries - including a worker that
 * clones a table through {@link #newSymbolTable} while a later refresh cycle interns
 * further values.
 */
public class LiveViewSymbolTableSource implements SymbolTableSource, QuietCloseable {
    // Per-OUTPUT-column overlays for the shared getSymbolTable view, created on
    // demand. They borrow the base's symbol tables rather than owning them, so this
    // list is a cache, not a resource ledger. Entries stay null for a column that
    // resolves straight from the base.
    private final ObjList<LiveViewSymbolTable> overlays = new ObjList<>();
    private SymbolTableSource base;
    // The pinned tier's eager-interning symbol cache, or null to resolve every column
    // from the base alone - which is what a read that does not route through the tier
    // binds (see of()).
    private LiveViewSymbolCache cache;
    private LiveViewInMemoryBuffer slot;
    private IntList tierColumns;

    @Override
    public void close() {
        // The shared overlays borrow the base's symbol tables (ownsBase=false), so
        // closing them only drops references - the base frees its own tables. The
        // overlays cloned out through newSymbolTable() are the caller's to free.
        Misc.freeObjListIfCloseable(overlays);
        overlays.clear();
        base = null;
        cache = null;
        slot = null;
        tierColumns = null;
    }

    /**
     * The shared, non-owning view of {@code columnIndex}'s symbol table: an overlay
     * over the lead's band while the column resolves through the tier, the base's own
     * table otherwise. Cached per output column, so consumers that must not share a
     * flyweight (recordA vs recordB, or two filter workers) take
     * {@link #newSymbolTable} instead.
     */
    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        if (!isTierResolved(columnIndex)) {
            return base.getSymbolTable(columnIndex);
        }
        LiveViewSymbolTable overlay = overlays.getQuiet(columnIndex);
        if (overlay == null) {
            final int tierColumn = tierColumns.getQuick(columnIndex);
            overlay = new LiveViewSymbolTable().of(
                    (StaticSymbolTable) base.getSymbolTable(columnIndex),
                    cache,
                    tierColumn,
                    slot.newSymbolMaxId(tierColumn),
                    slot.symbolHasNull(tierColumn),
                    false
            );
            overlays.extendAndSet(columnIndex, overlay);
        }
        return overlay;
    }

    /**
     * A fresh, independent symbol table for {@code columnIndex} that the caller owns
     * and closes. While the column resolves through the tier this is an overlay that
     * owns the base's freshly cloned table and closes it in turn, so a caller that
     * frees the returned table (parallel execution clones one per worker) does not
     * strand the clone underneath.
     */
    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        if (!isTierResolved(columnIndex)) {
            return base.newSymbolTable(columnIndex);
        }
        final int tierColumn = tierColumns.getQuick(columnIndex);
        return new LiveViewSymbolTable().of(
                (StaticSymbolTable) base.newSymbolTable(columnIndex),
                cache,
                tierColumn,
                slot.newSymbolMaxId(tierColumn),
                slot.symbolHasNull(tierColumn),
                true
        );
    }

    /**
     * Binds the source to a read. {@code base} is the disk scan's own symbol source,
     * which every column falls back to.
     * <p>
     * Pass a non-null {@code cache} (the pinned tier's) together with the pinned
     * {@code slot} and the read's output-to-tier {@code tierColumns} mapping only
     * while the read actually routes through the tier; the caller must keep the slot
     * pinned for as long as this source, or anything {@link #newSymbolTable} handed
     * out, can still be read. A null {@code cache} binds the source in pass-through
     * mode - every column resolves from {@code base} alone, and neither {@code slot}
     * nor {@code tierColumns} is touched - which is what a disk-only read wants: it
     * serves no lead rows, so it has no lead symbols to resolve.
     * <p>
     * Re-binding drops the previous read's overlays, which are stamped with its slot.
     */
    public LiveViewSymbolTableSource of(
            SymbolTableSource base,
            LiveViewSymbolCache cache,
            LiveViewInMemoryBuffer slot,
            IntList tierColumns
    ) {
        close();
        this.base = base;
        this.cache = cache;
        this.slot = slot;
        this.tierColumns = tierColumns;
        return this;
    }

    // True when columnIndex resolves through the tier's lead band: the read routes
    // through a pinned slot at all, and the TIER column it projects is a SYMBOL
    // column. Guarding on the cache first keeps a pass-through binding from reaching
    // for a tierColumns mapping it was never given.
    private boolean isTierResolved(int columnIndex) {
        return cache != null && cache.isSymbolColumn(tierColumns.getQuick(columnIndex));
    }
}
