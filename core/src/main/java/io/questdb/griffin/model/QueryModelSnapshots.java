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
package io.questdb.griffin.model;

import io.questdb.std.BoolList;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

import java.util.IdentityHashMap;

/**
 * Snapshots of the state a physical generation consumes, for every model of a model tree, so
 * that the same models can be generated again as if for the first time.
 * <p>
 * A generation consumes its models: it takes the where clause apart (keeping only the residual
 * filter), clears LATEST BY once a factory implements it, marks the LIMIT it implemented, and so
 * on (see {@link QueryModelSelection}). Expressions are cloned with their sub-query models
 * shared, so a second generation of a model, or a second compile of a filter, also meets the
 * sub-queries the first one consumed. Every site that generates a model or compiles an
 * expression more than once takes a snapshot first, from the model or the expression, and
 * restores it before each later generation. A snapshot reaches every model the root references,
 * at any depth: nested, union, UPDATE and join models, and the sub-queries of every expression
 * the models hold, the same graph {@link QueryModelGenerationState} walks for shared models.
 * <p>
 * {@link #save} returns an id; {@link #restore} puts back the state of every model that snapshot
 * reached, as many times as needed; {@link #release} drops it. Snapshots are independent of each
 * other, so releasing one leaves those taken during its life. A snapshot that is not released,
 * such as the one a filter takes before its first compile for the per-worker compiles that may
 * follow much later, lives until {@link #clear()}, when the compilation ends.
 */
public final class QueryModelSnapshots implements Mutable {
    private final IdentityHashMap<ExpressionNode, ExpressionNode> copies = new IdentityHashMap<>();
    private final ObjList<Object> edges = new ObjList<>();
    private final ObjList<IQueryModel> models = new ObjList<>();
    private final ObjList<Object> pending = new ObjList<>();
    // per snapshot id: the index of its first and one past its last model
    private final IntList saveBounds = new IntList();
    // per snapshot id: whether it is still held
    private final BoolList saveLive = new BoolList();
    private final ObjList<QueryModelSelection> selections = new ObjList<>();
    private final IdentityHashMap<Object, Boolean> visited = new IdentityHashMap<>();
    private final ObjList<ExpressionNode> windowArguments = new ObjList<>();

    @Override
    public void clear() {
        models.clear();
        selections.clear();
        saveBounds.clear();
        saveLive.clear();
        visited.clear();
        copies.clear();
        pending.clear();
        edges.clear();
        windowArguments.clear();
    }

    /**
     * The number of models snapshot {@code id} saved.
     */
    public int getModelCount(int id) {
        return saveBounds.getQuick(2 * id + 1) - saveBounds.getQuick(2 * id);
    }

    /**
     * The {@code i}-th model snapshot {@code id} saved, a delegate for a shared model.
     */
    public IQueryModel getModel(int id, int i) {
        return models.getQuick(saveBounds.getQuick(2 * id) + i);
    }

    /**
     * Drops snapshot {@code id}. The memory of the snapshots at the top of the stack that are no
     * longer held is reclaimed.
     */
    public void release(int id) {
        if (id < 0 || id >= saveLive.size()) {
            return;
        }
        saveLive.setQuick(id, false);
        int n = saveLive.size();
        while (n > 0 && !saveLive.get(n - 1)) {
            n--;
        }
        if (n < saveLive.size()) {
            final int start = saveBounds.getQuick(2 * n);
            models.setPos(start);
            selections.setPos(start);
            saveBounds.setPos(2 * n);
            saveLive.setPos(n);
        }
    }

    /**
     * Puts back the state of every model snapshot {@code id} saved. The snapshot holds until it
     * is released, so it can be restored any number of times.
     */
    public void restore(int id, ObjectPool<ExpressionNode> pool) {
        final int start = saveBounds.getQuick(2 * id);
        final int end = saveBounds.getQuick(2 * id + 1);
        copies.clear();
        try {
            for (int i = start; i < end; i++) {
                selections.getQuick(i).apply(models.getQuick(i), copies, pool);
            }
        } finally {
            copies.clear();
        }
    }

    /**
     * Puts back, for every model that {@code scope} reaches, the state the latest held snapshot
     * that saved it recorded. This is how an expression compiled more than once, say once per worker,
     * gets its sub-queries back for each compile, from the snapshot taken before its first one.
     */
    public void restoreReachable(ExpressionNode scope, ObjectPool<ExpressionNode> pool) {
        if (scope == null || models.size() == 0) {
            return;
        }
        collect(scope);
        copies.clear();
        try {
            for (int i = 0, n = pending.size(); i < n; i++) {
                final IQueryModel model = (IQueryModel) pending.getQuick(i);
                for (int id = saveLive.size() - 1, found = 0; id > -1 && found == 0; id--) {
                    if (saveLive.get(id)) {
                        for (int j = saveBounds.getQuick(2 * id), end = saveBounds.getQuick(2 * id + 1); j < end; j++) {
                            if (models.getQuick(j) == model) {
                                selections.getQuick(j).apply(model, copies, pool);
                                found = 1;
                                break;
                            }
                        }
                    }
                }
            }
        } finally {
            pending.clear();
            copies.clear();
        }
    }

    /**
     * Saves the state of every model {@code root} reaches, a model or an expression, the model
     * itself included. Returns the snapshot's id.
     */
    public int save(Object root) {
        final int id = saveBounds.size() / 2;
        final int start = models.size();
        saveBounds.add(start);
        saveBounds.add(start);
        saveLive.add(true);
        if (root != null) {
            collect(root);
            copies.clear();
            try {
                for (int i = 0, n = pending.size(); i < n; i++) {
                    final IQueryModel model = (IQueryModel) pending.getQuick(i);
                    models.add(model);
                    selections.add(new QueryModelSelection(model, copies));
                }
            } catch (Throwable th) {
                models.setPos(start);
                selections.setPos(start);
                throw th;
            } finally {
                pending.clear();
                copies.clear();
            }
            saveBounds.setQuick(2 * id + 1, models.size());
        }
        return id;
    }

    /**
     * Leaves in {@link #pending} every model {@code root} reaches, each once.
     */
    private void collect(Object root) {
        visited.clear();
        pending.clear();
        edges.clear();
        // pending holds the models found; edges is the work list
        QueryModelGenerationState.add(edges, root);
        try {
            while (edges.size() > 0) {
                final Object node = edges.popLast();
                if (visited.put(node, Boolean.TRUE) != null) {
                    continue;
                }
                if (node instanceof IQueryModel model) {
                    pending.add(model);
                }
                QueryModelGenerationState.addEdges(node, edges, windowArguments);
            }
        } finally {
            visited.clear();
            edges.clear();
            windowArguments.clear();
        }
    }
}
