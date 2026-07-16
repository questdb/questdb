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

package io.questdb.cairo;

import io.questdb.std.IntList;
import io.questdb.std.ObjList;

/**
 * Immutable, pure-logic descriptor derived from a {@link PartitionSpec} that answers: which
 * composite partition dimensions need a dedicated on-disk dictionary, in what dense slot order,
 * under what reserved file-name txn, and where the table-root cell registry slot sits relative to
 * them.
 * <p>
 * A dimension needs a dedicated dictionary iff its transform is {@link PartitionDimension#KIND_TRUNCATE}
 * or {@link PartitionDimension#KIND_EXPRESSION}: an {@link PartitionDimension#KIND_IDENTITY} dimension
 * reuses the source column's own (SYMBOL) dictionary, and a {@link PartitionDimension#KIND_HASH}
 * dimension is a pure function of the source value, so neither needs a dictionary of its own.
 * Dedicated dictionaries are assigned dense slots {@code 0, 1, 2, ...} in dimension order; the cell
 * registry always occupies the slot immediately after the last dedicated dictionary, i.e.
 * {@link #dedicatedCount()}.
 * <p>
 * Instances are snapshots: they copy every field they need out of the {@link PartitionSpec} at
 * {@link #of(PartitionSpec)} time and never retain a reference to it, so a later mutation of a
 * mutable spec (e.g. a {@code TableWriterMetadata} reload clearing and repopulating its spec in
 * place) cannot reach back and change an already-derived layout.
 */
public final class CompositeInternerLayout {

    /**
     * Reserved base for {@link #dictColumnNameTxn(int)}, chosen well clear of any real (small,
     * slowly-incrementing) {@code columnNameTxn} so dedicated-dictionary file names never collide
     * with an actual column's name-txn files. Assumes dimension count stays well below 1024
     * headroom (realistic tables have a handful of dimensions).
     */
    public static final long COMPOSITE_DICT_TXN_BASE = Long.MAX_VALUE - 1024;

    /**
     * Shared, always-empty layout returned by {@link #of(PartitionSpec)} for every non-composite
     * spec (plain tables): zero dedicated dictionaries, no cell registry slot.
     */
    public static final CompositeInternerLayout EMPTY = new CompositeInternerLayout();

    public static final String REGISTRY_NAME = "_cell";

    /**
     * Reserved txn for the cell registry file, distinct from every {@link #dictColumnNameTxn(int)}
     * (which is always {@code >= COMPOSITE_DICT_TXN_BASE}) and from {@link TableUtils#COLUMN_NAME_TXN_NONE}.
     */
    public static final long REGISTRY_TXN = COMPOSITE_DICT_TXN_BASE - 1;

    // per dimension index: dense dedicated-dict slot, or -1 if the dimension needs no dedicated dict
    private final IntList dedicatedSlots = new IntList();
    // per dimension index: the dimension's alias
    private final ObjList<String> dictNames = new ObjList<>();
    private int dedicatedCount = 0;
    private int registrySlot = -1;

    private CompositeInternerLayout() {
    }

    /**
     * Derives a layout from {@code spec}. Returns {@link #EMPTY} when {@code spec} has no
     * composite dimensions, keeping plain and cluster-only tables allocation-free.
     */
    public static CompositeInternerLayout of(PartitionSpec spec) {
        if (spec.getDimensionCount() == 0) {
            return EMPTY;
        }
        CompositeInternerLayout layout = new CompositeInternerLayout();
        int slot = 0;
        for (int i = 0, n = spec.getDimensionCount(); i < n; i++) {
            PartitionDimension dim = spec.getDimension(i);
            byte kind = dim.getKind();
            boolean needsDict = kind == PartitionDimension.KIND_TRUNCATE || kind == PartitionDimension.KIND_EXPRESSION;
            layout.dedicatedSlots.add(needsDict ? slot++ : -1);
            layout.dictNames.add(dim.getAlias());
        }
        layout.dedicatedCount = slot;
        layout.registrySlot = slot;
        return layout;
    }

    /**
     * Number of composite dimensions that need a dedicated dictionary (i.e. the dense slot count
     * occupied by dedicated dictionaries, before the cell registry slot).
     */
    public int dedicatedCount() {
        return dedicatedCount;
    }

    /**
     * The dense slot of dimension {@code dimIndex}'s dedicated dictionary, or -1 if it needs none.
     */
    public int dedicatedDictSlot(int dimIndex) {
        return dedicatedSlots.getQuick(dimIndex);
    }

    /**
     * Reserved, per-dimension-unique file-name txn for dimension {@code dimIndex}'s dedicated
     * dictionary. Distinguishes dimensions that share an alias (e.g. {@code truncate(symbol,3)} and
     * {@code truncate(symbol,5)} both aliased {@code symbol_trunc}) by giving each its own file.
     */
    public long dictColumnNameTxn(int dimIndex) {
        return COMPOSITE_DICT_TXN_BASE + dimIndex;
    }

    /**
     * Dimension {@code dimIndex}'s alias, i.e. the dictionary name used together with
     * {@link #dictColumnNameTxn(int)} to name its dedicated dictionary's files.
     */
    public String dictName(int dimIndex) {
        return dictNames.getQuick(dimIndex);
    }

    /**
     * True iff dimension {@code dimIndex}'s transform is {@code KIND_TRUNCATE} or
     * {@code KIND_EXPRESSION} and therefore needs a dedicated dictionary.
     */
    public boolean needsDedicatedDict(int dimIndex) {
        return dedicatedSlots.getQuick(dimIndex) != -1;
    }

    /**
     * The dense slot of the table-root cell registry: always immediately after the last dedicated
     * dictionary ({@link #dedicatedCount()}), or -1 for {@link #EMPTY} (no composite structures at
     * all, so no registry either).
     */
    public int registrySlot() {
        return registrySlot;
    }

    /**
     * True iff this layout has composite interners (i.e., is non-{@link #EMPTY}). Equivalent to
     * {@code registrySlot() >= 0} and to {@code spec.getDimensionCount() > 0} at layout derivation.
     */
    public boolean hasInterners() {
        return registrySlot >= 0;
    }
}
