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

package io.questdb.griffin;

import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.columns.BindableColumn;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

/**
 * Owns the executable roots the parser builds while binding, one slot per bound expression, and the constant arguments
 * and column leaves of calls the binder leaves unconstructed, until a consumer adopts, retargets or closes them.
 */
public final class PreparedFunctions implements Mutable {
    private final ObjectPool<Entry> entries;
    private final ObjList<Entry> prepared = new ObjList<>();
    private final ResourceScope resources = new ResourceScope();

    public PreparedFunctions(int maxRetainedEntries) {
        this.entries = new ObjectPool<>(Entry::new, 4, maxRetainedEntries);
    }

    /**
     * Whether every open column leaf of the entry is a column its description reads: the binder closes and removes
     * the leaves of the operands its folds drop.
     */
    public static boolean hasOnlyReadLeaves(Entry entry) {
        for (int i = 0, n = entry.leaves.size(); i < n; i++) {
            final BindableColumn leaf = entry.leaves.getQuick(i);
            if (leaf.isOpen() && !LogicalPlans.readsColumn(entry.expression, leaf.getColumnId())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Reserves the slot of the root the parser is about to build.
     */
    public Entry begin() {
        final Entry entry = entries.next();
        entry.slot = resources.reserve();
        prepared.add(entry);
        return entry;
    }

    @Override
    public void clear() {
        try {
            resources.clear();
        } finally {
            prepared.clear();
            entries.clear();
        }
    }

    /**
     * Closes the roots prepared since {@code mark} when their binding fails; the parser owns its partial roots and
     * the roots prepared earlier stay with their consumers.
     */
    public void closeOnFailure(int mark, @NotNull Throwable primary) {
        for (int i = mark, n = prepared.size(); i < n; i++) {
            final Entry entry = prepared.getQuick(i);
            if (entry.slot >= 0 && resources.isOwned(entry.slot)) {
                Misc.free(detach(entry), primary);
            }
        }
    }

    /**
     * Transfers the entry's root to the caller.
     */
    public Function detach(Entry entry) {
        final Function function = (Function) resources.detach(entry.slot);
        entry.slot = -1;
        return function;
    }

    /**
     * The first entry that still owns a root describing {@code expression}, or null.
     */
    public Entry findOwned(BoundExpression expression) {
        for (int i = 0, n = prepared.size(); i < n; i++) {
            final Entry entry = prepared.getQuick(i);
            if (entry.expression == expression && entry.slot >= 0 && resources.isOwned(entry.slot)) {
                return entry;
            }
        }
        return null;
    }

    /**
     * The first entry that still owns a root describing {@code expression} converted for {@code updateTargetType}
     * (-1 for an unconverted root), or null.
     */
    public Entry findOwned(BoundExpression expression, int updateTargetType) {
        for (int i = 0, n = prepared.size(); i < n; i++) {
            final Entry entry = prepared.getQuick(i);
            if (entry.expression == expression && entry.updateTargetType == updateTargetType
                    && entry.slot >= 0 && resources.isOwned(entry.slot)) {
                return entry;
            }
        }
        return null;
    }

    /**
     * The position of the next root, from which {@link #closeOnFailure} closes.
     */
    public int mark() {
        return prepared.size();
    }

    /**
     * Fills the entry's reserved slot with the built root.
     */
    public void own(Entry entry, Function root) {
        resources.own(entry.slot, root);
    }

    /**
     * Moves the entry to a slot reserved with {@link #reserve()} and fills it with a converted root.
     */
    public void own(Entry entry, int slot, Function root) {
        resources.own(slot, root);
        entry.slot = slot;
    }

    /**
     * Reserves a slot before building the root that fills it, so growth cannot orphan a built root.
     */
    public int reserve() {
        return resources.reserve();
    }

    /**
     * The entry's root, which stays owned here.
     */
    public Function root(Entry entry) {
        return resources.function(entry.slot);
    }

    /**
     * Closes every root nothing adopted and chains close failures onto {@code primary}.
     */
    Throwable closePrepared(Throwable primary) {
        return resources.closeOwned(primary);
    }

    @TestOnly
    int getEntryCapacity() {
        return entries.getCapacity();
    }

    /**
     * One prepared root: the description it implements, its relocatable column leaves and the slot that owns it.
     */
    public static final class Entry implements Mutable {
        public final ObjList<BindableColumn> leaves = new ObjList<>();
        public BoundExpression expression;
        public boolean isRebuildRequired;
        int slot = -1;
        public int updateTargetType = -1;

        @Override
        public void clear() {
            leaves.clear();
            expression = null;
            isRebuildRequired = false;
            slot = -1;
            updateTargetType = -1;
        }
    }
}
