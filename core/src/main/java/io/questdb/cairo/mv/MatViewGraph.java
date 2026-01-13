/*******************************************************************************
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

package io.questdb.cairo.mv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.view.ViewDependencyList;
import io.questdb.std.Chars;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.Mutable;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.ThreadLocal;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.ArrayDeque;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Holds mat view definitions and dependency lists, i.e. mat view graph.
 * This object is always in-use, even when mat views are disabled or the node is a read-only replica.
 */
public class MatViewGraph implements Mutable {
    private static final java.lang.ThreadLocal<MatViewDefinition> tlDefinitionTask = new java.lang.ThreadLocal<>();
    private static final ThreadLocal<LowerCaseCharSequenceHashSet> tlSeen = new ThreadLocal<>(LowerCaseCharSequenceHashSet::new);
    private static final ThreadLocal<ArrayDeque<CharSequence>> tlStack = new ThreadLocal<>(ArrayDeque::new);
    private final Function<CharSequence, ViewDependencyList> createDependencyList;
    private final ConcurrentHashMap<MatViewDefinition> definitionsByTableDirName = new ConcurrentHashMap<>();
    // Note: this map is grow-only, i.e. keys are never removed.
    private final ConcurrentHashMap<ViewDependencyList> dependentViewsByTableName = new ConcurrentHashMap<>(false);
    private final BiFunction<CharSequence, MatViewDefinition, MatViewDefinition> updateDefinitionRef;

    public MatViewGraph() {
        this.createDependencyList = name -> new ViewDependencyList();
        this.updateDefinitionRef = this::updateDefinition;
    }

    public boolean addView(MatViewDefinition viewDefinition) {
        final TableToken viewToken = viewDefinition.getMatViewToken();
        final MatViewDefinition prevDefinition = definitionsByTableDirName.putIfAbsent(viewToken.getDirName(), viewDefinition);
        // WAL table directories are unique, so we don't expect previous value
        if (prevDefinition != null) {
            return false;
        }

        synchronized (this) {
            if (hasDependencyLoop(viewDefinition.getBaseTableName(), viewToken)) {
                throw CairoException.critical(0)
                        .put("circular dependency detected for materialized view [view=").put(viewToken.getTableName())
                        .put(", baseTable=").put(viewDefinition.getBaseTableName())
                        .put(']');
            }
            final ViewDependencyList list = getOrCreateDependentViews(viewDefinition.getBaseTableName());
            final ObjList<TableToken> matViews = list.lockForWrite();
            try {
                matViews.add(viewToken);
            } finally {
                list.unlockAfterWrite();
            }
        }
        return true;
    }

    @TestOnly
    @Override
    public void clear() {
        definitionsByTableDirName.clear();
        dependentViewsByTableName.clear();
    }

    public void getDependentViews(TableToken baseTableToken, ObjList<TableToken> sink) {
        final ViewDependencyList list = getOrCreateDependentViews(baseTableToken.getTableName());
        final ReadOnlyObjList<TableToken> matViews = list.lockForRead();
        try {
            sink.addAll(matViews);
        } finally {
            list.unlockAfterRead();
        }
    }

    public MatViewDefinition getViewDefinition(TableToken matViewToken) {
        return definitionsByTableDirName.get(matViewToken.getDirName());
    }

    public void getViews(ObjList<TableToken> sink) {
        for (MatViewDefinition viewDefinition : definitionsByTableDirName.values()) {
            sink.add(viewDefinition.getMatViewToken());
        }
    }

    /**
     * Writes all table tokens to the destination list in order, so that dependent materialized views
     * go first followed by their base tables (or materialized views).
     * <p>
     * This is used for checkpoints: we want to first take a snapshot of a mat view and only then
     * take a snapshot its base table. That's to prevent situation when a checkpoint contains
     * mat view refreshed with "ghost" base table data that is newer than what's in the checkpoint.
     *
     * @param tables      source set of all table tokens
     * @param orderedSink destination list
     */
    public void orderByDependentViews(ObjHashSet<TableToken> tables, ObjList<TableToken> orderedSink) {
        orderedSink.clear();
        ObjHashSet<TableToken> seen = new ObjHashSet<>();
        ArrayDeque<TableToken> stack = new ArrayDeque<>();
        for (int i = 0, n = tables.size(); i < n; i++) {
            TableToken token = tables.get(i);
            if (!seen.contains(token)) {
                orderByDependentViews(token, seen, stack, orderedSink);
            }
        }
    }

    public void removeView(TableToken viewToken) {
        final MatViewDefinition viewDefinition = definitionsByTableDirName.remove(viewToken.getDirName());
        if (viewDefinition != null) {
            final CharSequence baseTableName = viewDefinition.getBaseTableName();
            final ViewDependencyList dependentViews = dependentViewsByTableName.get(baseTableName);
            if (dependentViews != null) {
                final ObjList<TableToken> matViews = dependentViews.lockForWrite();
                try {
                    for (int i = 0, n = matViews.size(); i < n; i++) {
                        final TableToken matView = matViews.get(i);
                        if (matView.equals(viewToken)) {
                            matViews.remove(i);
                            break;
                        }
                    }
                } finally {
                    dependentViews.unlockAfterWrite();
                }
            }
        }
    }

    public void updateToken(TableToken updatedToken) {
        final MatViewDefinition viewDefinition = definitionsByTableDirName.get(updatedToken.getDirName());
        if (viewDefinition != null) {
            viewDefinition.updateToken(updatedToken);
            ViewDependencyList viewList = dependentViewsByTableName.get(viewDefinition.getBaseTableName());
            if (viewList != null) {
                var matViews = viewList.lockForWrite();
                try {
                    for (int i = 0, n = matViews.size(); i < n; i++) {
                        if (Chars.equalsIgnoreCase(matViews.get(i).getDirName(), updatedToken.getDirName())) {
                            matViews.set(i, updatedToken);
                            return;
                        }
                    }
                } finally {
                    viewList.unlockAfterWrite();
                }
            }
        }
    }

    public void updateViewDefinition(@NotNull TableToken viewToken, @NotNull MatViewDefinition newDefinition) {
        tlDefinitionTask.set(newDefinition);
        if (definitionsByTableDirName.computeIfPresent(viewToken.getDirName(), updateDefinitionRef) == null) {
            throw CairoException.nonCritical().put("previous view definition was not found: ").put(viewToken.getTableName());
        }
    }

    @NotNull
    private ViewDependencyList getOrCreateDependentViews(CharSequence baseTableName) {
        return dependentViewsByTableName.computeIfAbsent(baseTableName, createDependencyList);
    }

    private boolean hasDependencyLoop(CharSequence baseTableName, TableToken newMatViewToken) {
        LowerCaseCharSequenceHashSet seen = tlSeen.get();
        ArrayDeque<CharSequence> stack = tlStack.get();

        seen.clear();
        stack.clear();

        if (Chars.equalsIgnoreCase(baseTableName, newMatViewToken.getTableName())) {
            return true; // Self-loop
        }

        stack.push(newMatViewToken.getTableName());

        while (!stack.isEmpty()) {
            CharSequence currentTableName = stack.pop();
            if (!seen.add(currentTableName)) {
                continue;
            }

            ViewDependencyList dependentViews = dependentViewsByTableName.get(currentTableName);
            if (dependentViews != null) {
                ReadOnlyObjList<TableToken> matViews = dependentViews.lockForRead();
                try {
                    for (int i = 0, n = matViews.size(); i < n; i++) {
                        TableToken matView = matViews.get(i);
                        if (Chars.equalsIgnoreCase(matView.getTableName(), baseTableName)) {
                            return true; // Cycle detected
                        }
                        stack.push(matView.getTableName());
                    }
                } finally {
                    dependentViews.unlockAfterRead();
                }
            }
        }
        return false;
    }

    private void orderByDependentViews(
            TableToken current,
            ObjHashSet<TableToken> seen,
            ArrayDeque<TableToken> stack,
            ObjList<TableToken> sink
    ) {
        stack.push(current);
        while (!stack.isEmpty()) {
            TableToken top = stack.peek();
            if (!seen.contains(top)) {
                ViewDependencyList list = dependentViewsByTableName.get(top.getTableName());
                if (list == null) {
                    sink.add(top);
                    seen.add(top);
                    stack.pop();
                } else {
                    boolean allDependentSeen = true;
                    ReadOnlyObjList<TableToken> views = list.lockForRead();
                    try {
                        for (int i = 0, n = views.size(); i < n; i++) {
                            TableToken view = views.get(i);
                            if (!seen.contains(view)) {
                                stack.push(view);
                                allDependentSeen = false;
                            }
                        }
                    } finally {
                        list.unlockAfterRead();
                    }
                    if (allDependentSeen) {
                        sink.add(top);
                        seen.add(top);
                        stack.pop();
                    }
                }
            } else {
                stack.pop();
            }
        }
    }

    private MatViewDefinition updateDefinition(CharSequence tableDirName, MatViewDefinition existingDefinition) {
        if (existingDefinition == null) {
            return null;
        }
        MatViewDefinition newDefinition = tlDefinitionTask.get();
        assert newDefinition != null;
        // no, this won't produce a mem leak since we don't spawn short-lived threads in runtime
        tlDefinitionTask.set(null);
        return newDefinition;
    }
}
