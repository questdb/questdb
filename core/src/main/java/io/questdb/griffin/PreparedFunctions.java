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
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

/**
 * Owns the executable roots the parser builds while binding, one slot per bound expression, and the constant arguments
 * and column leaves of calls the binder leaves unconstructed, until a consumer adopts, retargets or closes them.
 */
final class PreparedFunctions implements Mutable {
    private final ObjectPool<Entry> entries = new ObjectPool<>(Entry::new, 4);
    private final ObjList<Entry> prepared = new ObjList<>();
    private final ResourceScope resources = new ResourceScope();

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
     * Reserves the slot of the root the parser is about to build.
     */
    Entry begin() {
        final Entry entry = entries.next();
        entry.slot = resources.reserve();
        prepared.add(entry);
        return entry;
    }

    /**
     * Closes every owned root when binding fails; the parser owns its partial roots.
     */
    void closeOnFailure(Throwable primary) {
        resources.closeOwned(primary);
    }

    /**
     * Closes every root nothing adopted and chains close failures onto {@code primary}.
     */
    Throwable closePrepared(Throwable primary) {
        return resources.closeOwned(-1, primary);
    }

    /**
     * Transfers the entry's root to the caller.
     */
    Function detach(Entry entry) {
        final Function function = resources.detachFunction(entry.slot);
        entry.slot = -1;
        return function;
    }

    /**
     * The first entry that still owns a root describing {@code expression}, or null.
     */
    Entry findOwned(BoundExpression expression) {
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
    Entry findOwned(BoundExpression expression, int updateTargetType) {
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
     * Whether every open column leaf of the entry is a column its description reads: the binder closes and removes
     * the leaves of the operands its folds drop.
     */
    static boolean hasOnlyReadLeaves(Entry entry) {
        for (int i = 0, n = entry.leaves.size(); i < n; i++) {
            final BindableColumn leaf = entry.leaves.getQuick(i);
            if (leaf.isOpen() && !BoundExpressionRewriter.references(entry.expression, leaf.getColumnId())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Fills the entry's reserved slot with the built root.
     */
    void own(Entry entry, Function root) {
        resources.own(entry.slot, root);
    }

    /**
     * Moves the entry to a slot reserved with {@link #reserve()} and fills it with a converted root.
     */
    void own(Entry entry, int slot, Function root) {
        resources.own(slot, root);
        entry.slot = slot;
    }

    /**
     * Reserves a slot before building the root that fills it, so growth cannot orphan a built root.
     */
    int reserve() {
        return resources.reserve();
    }

    /**
     * The entry's root, which stays owned here.
     */
    Function root(Entry entry) {
        return resources.function(entry.slot);
    }

    /**
     * One prepared root: the description it implements, its relocatable column leaves and the slot that owns it.
     */
    static final class Entry implements Mutable {
        final ObjList<BindableColumn> leaves = new ObjList<>();
        BoundExpression expression;
        boolean isRebuildRequired;
        int slot = -1;
        int updateTargetType = -1;

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
