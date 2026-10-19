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
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.columns.BindableColumn;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjIntHashMap;
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
    private final ObjIntHashMap<BoundExpression> firstEntries = new ObjIntHashMap<>();
    private final ObjList<Entry> prepared = new ObjList<>();
    private final ResourceScope resources = new ResourceScope();

    public PreparedFunctions(int minRetainedEntries) {
        this.entries = new ObjectPool<>(Entry::new, 4, minRetainedEntries);
    }

    /**
     * Whether every open column leaf of the entry is a column its description reads: the binder closes and removes
     * the leaves of the operands its folds drop.
     */
    public static boolean hasOnlyReadLeaves(Entry entry) {
        IntHashSet columnIds = null;
        for (int i = 0, n = entry.leaves.size(); i < n; i++) {
            final BindableColumn leaf = entry.leaves.getQuick(i);
            if (leaf.isOpen()) {
                if (n == 1) {
                    return LogicalPlans.readsColumn(entry.expression, leaf.getColumnId());
                }
                if (columnIds == null) {
                    final IntHashSet readColumnIds = new IntHashSet();
                    if (entry.expression != null) {
                        entry.expression.walk(expression -> {
                            if (expression instanceof ColumnExpression column) {
                                readColumnIds.add(column.getColumnId());
                            }
                            return TreeWalk.CONTINUE;
                        });
                    }
                    columnIds = readColumnIds;
                }
                if (!columnIds.contains(leaf.getColumnId())) {
                    return false;
                }
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
        entry.index = prepared.size();
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
            firstEntries.clear();
        }
    }

    /**
     * Closes the roots prepared since {@code mark} when their binding fails; the parser owns its partial roots and
     * the roots prepared earlier stay with their consumers.
     */
    public void closeOnFailure(int mark, @NotNull Throwable primary) {
        for (int i = mark, n = prepared.size(); i < n; i++) {
            final Entry entry = prepared.getQuick(i);
            if (isOwned(entry)) {
                Misc.free(detach(entry), primary);
            }
        }
    }

    /**
     * Makes the entry describe {@code expression}; the entries describing one expression are found in the order
     * {@link #begin} reserved them.
     */
    public void describe(Entry entry, BoundExpression expression) {
        if (entry.expression != null) {
            unlink(entry);
        }
        entry.expression = expression;
        final int keyIndex = firstEntries.keyIndex(expression);
        final int first = firstEntries.valueAt(keyIndex);
        if (first < 0 || first > entry.index) {
            entry.nextIndex = first;
            firstEntries.putAt(keyIndex, expression, entry.index);
            return;
        }
        Entry previous = prepared.getQuick(first);
        while (previous.nextIndex >= 0 && previous.nextIndex < entry.index) {
            previous = prepared.getQuick(previous.nextIndex);
        }
        entry.nextIndex = previous.nextIndex;
        previous.nextIndex = entry.index;
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
        for (int i = firstEntries.get(expression); i >= 0; ) {
            final Entry entry = prepared.getQuick(i);
            if (isOwned(entry)) {
                return entry;
            }
            i = entry.nextIndex;
        }
        return null;
    }

    /**
     * The first entry that still owns a root describing {@code expression} converted for {@code updateTargetType}
     * (-1 for an unconverted root), or null.
     */
    public Entry findOwned(BoundExpression expression, int updateTargetType) {
        for (int i = firstEntries.get(expression); i >= 0; ) {
            final Entry entry = prepared.getQuick(i);
            if (entry.updateTargetType == updateTargetType && isOwned(entry)) {
                return entry;
            }
            i = entry.nextIndex;
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
     * Makes the entry describe the replacement of its expression whose columns a column-only projection renames,
     * renaming its open leaves alike; a leaf whose static symbol table capability the projection's input does not
     * share requires the root to be rebuilt.
     */
    public void retarget(Entry entry, BoundExpression replacement, ProjectPlan projection) {
        assert hasOnlyReadLeaves(entry) : "prepared leaf is not read by its description";
        for (int i = 0, n = entry.leaves.size(); i < n; i++) {
            final BindableColumn leaf = entry.leaves.getQuick(i);
            if (leaf.isOpen()) {
                leaf.setColumnId(BoundExpressionRewriter.projectionColumn(projection, leaf.getColumnId(), leaf.getType()).getColumnId());
            }
        }
        describeRelocated(entry, replacement, projection.getChild().getOutput());
    }

    /**
     * Makes the entry describe the replacement of its expression that reads each column the map holds under its
     * mapped id over {@code input}, renaming its open leaves alike; a leaf whose static symbol table capability the
     * input does not share requires the root to be rebuilt.
     */
    public void retarget(Entry entry, BoundExpression replacement, IntIntHashMap columnIds, OutputSchema input) {
        assert hasOnlyReadLeaves(entry) : "prepared leaf is not read by its description";
        for (int i = 0, n = entry.leaves.size(); i < n; i++) {
            final BindableColumn leaf = entry.leaves.getQuick(i);
            if (leaf.isOpen()) {
                final int columnId = columnIds.get(leaf.getColumnId());
                if (columnId >= 0) {
                    leaf.setColumnId(columnId);
                }
            }
        }
        describeRelocated(entry, replacement, input);
    }

    /**
     * The entry's root, which stays owned here.
     */
    public Function root(Entry entry) {
        return resources.function(entry.slot);
    }

    private void describeRelocated(Entry entry, BoundExpression replacement, OutputSchema input) {
        describe(entry, replacement);
        if (!entry.isRelocatable(input, null)) {
            entry.isRebuildRequired = true;
        }
    }

    private boolean isOwned(Entry entry) {
        return entry.slot >= 0 && resources.isOwned(entry.slot);
    }

    private void unlink(Entry entry) {
        final BoundExpression expression = entry.expression;
        final int keyIndex = firstEntries.keyIndex(expression);
        if (firstEntries.valueAt(keyIndex) == entry.index) {
            firstEntries.putAt(keyIndex, expression, entry.nextIndex);
        } else {
            Entry previous = prepared.getQuick(firstEntries.valueAt(keyIndex));
            while (previous.nextIndex != entry.index) {
                previous = prepared.getQuick(previous.nextIndex);
            }
            previous.nextIndex = entry.nextIndex;
        }
        entry.nextIndex = -1;
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
        public boolean isRebuildRequired;
        public int updateTargetType = -1;
        BoundExpression expression;
        int index = -1;
        int nextIndex = -1;
        int slot = -1;

        /**
         * Registers a column function the root reads: a bindable leaf relocates with the root, any other column
         * requires the root to be rebuilt under the final layout.
         */
        public Function addColumn(Function column) {
            if (column instanceof BindableColumn leaf) {
                leaves.add(leaf);
            } else {
                isRebuildRequired = true;
            }
            return column;
        }

        @Override
        public void clear() {
            leaves.clear();
            expression = null;
            index = -1;
            isRebuildRequired = false;
            nextIndex = -1;
            slot = -1;
            updateTargetType = -1;
        }

        public BoundExpression getExpression() {
            return expression;
        }

        /**
         * Whether the root reads the input unchanged once {@link #relocate} assigns its leaves their positions:
         * false when it requires a rebuild, as it does when a SYMBOL leaf's static symbol table capability differs
         * from the input's. A leaf whose column the input lacks or retypes is an invalid layout change.
         */
        public boolean isRelocatable(OutputSchema input, RecordMetadata metadata) {
            if (isRebuildRequired) {
                return false;
            }
            assert hasOnlyReadLeaves(this) : "prepared leaf is not read by its description";
            for (int i = 0, n = leaves.size(); i < n; i++) {
                final BindableColumn leaf = leaves.getQuick(i);
                if (leaf.isOpen()) {
                    final int index = input.getColumnIndexById(leaf.getColumnId());
                    if (index < 0 || input.getColumnType(index) != leaf.getType()
                            || metadata != null && metadata.getColumnType(index) != leaf.getType()) {
                        throw new IllegalStateException("bound function input has changed");
                    }
                    if (leaf instanceof SymbolFunction symbol && symbol.isSymbolTableStatic()
                            != (metadata == null ? input.isSymbolTableStatic(index) : metadata.isSymbolTableStatic(index))) {
                        return false;
                    }
                }
            }
            return true;
        }

        /**
         * Assigns each open leaf its input position; {@link #isRelocatable} vets the input first.
         */
        public void relocate(OutputSchema input) {
            for (int i = 0, n = leaves.size(); i < n; i++) {
                final BindableColumn leaf = leaves.getQuick(i);
                if (leaf.isOpen()) {
                    leaf.setColumnIndex(input.getColumnIndexById(leaf.getColumnId()));
                }
            }
        }
    }
}
