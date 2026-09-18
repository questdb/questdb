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

import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import org.jetbrains.annotations.TestOnly;

import java.util.IdentityHashMap;
import java.util.function.Consumer;

/**
 * Logical selection state for one physical-generation attempt. Snapshots never enter
 * codegen and own no factories. LIMIT/advice keep their optimizer-established identity;
 * only their implementation markers belong to a physical generation.
 */
public final class QueryModelGenerationState implements Mutable {
    private final IdentityHashMap<IQueryModel, Integer> active = new IdentityHashMap<>();
    private final IdentityHashMap<Object, ObjList<Object>> graph = new IdentityHashMap<>();
    private final IdentityHashMap<ExpressionNode, Boolean> markers = new IdentityHashMap<>();
    private final ObjList<IQueryModel> scopes = new ObjList<>();
    private final IdentityHashMap<IQueryModel, Selection> selections = new IdentityHashMap<>();
    private int discoveryCount;
    private boolean isReady;
    private int preparationCount;
    private Consumer<IQueryModel> preparationHook;
    private int workingCopyCount;

    public void begin(IQueryModel root, ObjectPool<ExpressionNode> pool) {
        clear();
        try {
            boolean hasSharing = discover(root);
            if (!hasSharing) {
                graph.clear();
                return;
            }
            IdentityHashMap<ExpressionNode, ExpressionNode> copies = new IdentityHashMap<>();
            for (Object node : graph.keySet()) {
                if (node instanceof IQueryModel model) {
                    selections.put(model, new Selection(model, copies));
                    saveMarker(model.getLimitLo());
                    saveMarker(model.getLimitHi());
                    saveMarker(model.getLimitAdviceLo());
                    saveMarker(model.getLimitAdviceHi());
                }
            }
            prepare(root, pool);
            scopes.add(unwrap(root));
            isReady = true;
        } catch (Throwable th) {
            clear();
            throw th;
        }
    }

    @Override
    public void clear() {
        active.clear();
        graph.clear();
        markers.clear();
        scopes.clear();
        selections.clear();
        isReady = false;
        discoveryCount = preparationCount = workingCopyCount = 0;
    }

    public void enterModel(IQueryModel model) {
        if (isReady) {
            model = unwrap(model);
            active.put(model, active.getOrDefault(model, 0) + 1);
        }
    }

    /**
     * Returns whether the caller must close a newly prepared scope.
     */
    public boolean enterRegion(IQueryModel root, ObjectPool<ExpressionNode> pool) {
        root = unwrap(root);
        if (!isReady || !selections.containsKey(root) || active.containsKey(root)
                || (scopes.size() > 0 && scopes.getLast() == root)) {
            return false;
        }
        prepare(root, pool);
        scopes.add(root);
        return true;
    }

    public void exitModel(IQueryModel model) {
        if (isReady) {
            model = unwrap(model);
            int count = active.get(model);
            if (count == 1) {
                active.remove(model);
            } else {
                active.put(model, count - 1);
            }
        }
    }

    public void exitRegion(boolean hasEntered) {
        if (hasEntered) {
            scopes.popLast();
        }
    }

    @TestOnly
    public int getDiscoveryCount() {
        return discoveryCount;
    }

    @TestOnly
    public int getPreparationCount() {
        return preparationCount;
    }

    @TestOnly
    public int getRetainedNodeCount() {
        return graph.size() + selections.size() + markers.size() + active.size() + scopes.size();
    }

    @TestOnly
    public int getWorkingCopyCount() {
        return workingCopyCount;
    }

    @TestOnly
    public void setPreparationHook(Consumer<IQueryModel> hook) {
        preparationHook = hook;
    }

    private static void add(ObjList<Object> edges, Object node) {
        if (node != null) {
            edges.add(node instanceof IQueryModel model ? unwrap(model) : node);
        }
    }

    private static void addColumns(ObjList<Object> edges, ObjList<QueryColumn> columns) {
        for (int i = 0; i < columns.size(); i++) {
            add(edges, columns.getQuick(i).getAst());
        }
    }

    private static void addExpressions(ObjList<Object> edges, ObjList<ExpressionNode> expressions) {
        if (expressions != null) {
            for (int i = 0; i < expressions.size(); i++) {
                add(edges, expressions.getQuick(i));
            }
        }
    }

    private static void addWindow(ObjList<Object> edges, WindowExpression window) {
        if (window != null) {
            addExpressions(edges, window.getPartitionBy());
            addExpressions(edges, window.getOrderBy());
            add(edges, window.getRowsLoExpr());
            add(edges, window.getRowsHiExpr());
            add(edges, window.getAnchorExpression());
            add(edges, window.getPendingSubsample());
        }
    }

    private static ExpressionNode copy(ExpressionNode node, IdentityHashMap<ExpressionNode, ExpressionNode> memo, ObjectPool<ExpressionNode> pool) {
        if (node == null) {
            return null;
        }
        ExpressionNode result = memo.get(node);
        if (result == null) {
            result = pool == null ? ExpressionNode.FACTORY.newInstance() : pool.next();
            result.copyFrom(node);
            result.implemented = node.implemented;
            memo.put(node, result);
            result.lhs = copy(node.lhs, memo, pool);
            result.rhs = copy(node.rhs, memo, pool);
            for (int i = 0; i < node.args.size(); i++) {
                result.args.setQuick(i, copy(node.args.getQuick(i), memo, pool));
            }
        }
        return result;
    }

    private static IQueryModel unwrap(IQueryModel model) {
        return model instanceof QueryModelWrapper wrapper ? wrapper.getDelegate() : model;
    }

    private boolean discover(IQueryModel root) {
        boolean hasSharing = root instanceof QueryModelWrapper;
        ObjList<Object> pending = new ObjList<>();
        add(pending, root);
        while (pending.size() > 0) {
            Object node = pending.popLast();
            if (graph.containsKey(node)) {
                continue;
            }
            discoveryCount++;
            ObjList<Object> edges = new ObjList<>();
            graph.put(node, edges);
            if (node instanceof IQueryModel model) {
                hasSharing |= model.hasSharedRefs()
                        || model.getNestedModel() instanceof QueryModelWrapper
                        || model.getUnionModel() instanceof QueryModelWrapper
                        || model.getUpdateTableModel() instanceof QueryModelWrapper;
                add(edges, model.getNestedModel());
                add(edges, model.getUnionModel());
                add(edges, model.getUpdateTableModel());
                for (int i = 1; i < model.getJoinModels().size(); i++) {
                    IQueryModel child = model.getJoinModels().getQuick(i);
                    hasSharing |= child instanceof QueryModelWrapper;
                    add(edges, child);
                }
                addColumns(edges, model.getBottomUpColumns());
                addColumns(edges, model.getTopDownColumns());
                addExpressions(edges, model.getExpressionModels());
                addExpressions(edges, model.getLatestBy());
                addExpressions(edges, model.getOrderBy());
                addExpressions(edges, model.getOrderByAdvice());
                addExpressions(edges, model.getGroupBy());
                addExpressions(edges, model.getJoinColumns());
                addExpressions(edges, model.getUpdateExpressions());
                addExpressions(edges, model.getUnnestExpressions());
                addExpressions(edges, model.getSampleByFill());
                addExpressions(edges, model.getFillValues());
                add(edges, model.getWhereClause());
                add(edges, model.getBackupWhereClause());
                add(edges, model.getPostJoinWhereClause());
                add(edges, model.getConstWhereClause());
                add(edges, model.getOuterJoinExpressionClause());
                add(edges, model.getJoinCriteria());
                add(edges, model.getLimitLo());
                add(edges, model.getLimitHi());
                add(edges, model.getLimitAdviceLo());
                add(edges, model.getLimitAdviceHi());
                add(edges, model.getTableNameExpr());
                add(edges, model.getTimestamp());
                add(edges, model.getSampleBy());
                add(edges, model.getSampleByFrom());
                add(edges, model.getSampleByTo());
                add(edges, model.getSampleByOffset());
                add(edges, model.getSampleByTimezoneName());
                add(edges, model.getSubsample());
                add(edges, model.getFillFrom());
                add(edges, model.getFillTo());
                add(edges, model.getFillStride());
                add(edges, model.getFillOffset());
                add(edges, model.getFillTimezoneName());
                add(edges, model.getAsOfJoinTolerance());
                add(edges, model.getLateralCountCoalesceGuard());
                WindowJoinContext windowJoin = model.getWindowJoinContext();
                if (windowJoin != null) {
                    add(edges, windowJoin.getLoExpr());
                    add(edges, windowJoin.getHiExpr());
                }
            } else if (node instanceof ExpressionNode expression) {
                hasSharing |= expression.queryModel instanceof QueryModelWrapper;
                add(edges, expression.queryModel);
                add(edges, expression.lhs);
                add(edges, expression.rhs);
                addExpressions(edges, expression.args);
                addWindow(edges, expression.windowExpression);
            }
            pending.addAll(edges);
        }
        return hasSharing;
    }

    private void prepare(IQueryModel root, ObjectPool<ExpressionNode> pool) {
        if (preparationHook != null) {
            preparationHook.accept(unwrap(root));
        }
        IdentityHashMap<Object, Boolean> visited = new IdentityHashMap<>();
        IdentityHashMap<ExpressionNode, ExpressionNode> copies = new IdentityHashMap<>();
        ObjList<Object> pending = new ObjList<>();
        add(pending, root);
        while (pending.size() > 0) {
            Object node = pending.popLast();
            if (visited.put(node, Boolean.TRUE) != null) {
                continue;
            }
            if (node instanceof IQueryModel model) {
                if (active.containsKey(model)) {
                    continue;
                }
                Selection selection = selections.get(model);
                if (selection != null) {
                    selection.apply(model, copies, pool);
                    resetMarker(model.getLimitLo());
                    resetMarker(model.getLimitHi());
                    resetMarker(model.getLimitAdviceLo());
                    resetMarker(model.getLimitAdviceHi());
                }
            }
            ObjList<Object> edges = graph.get(node);
            if (edges != null) {
                pending.addAll(edges);
            }
        }
        preparationCount += visited.size();
        workingCopyCount += copies.size();
    }

    private void resetMarker(ExpressionNode node) {
        Boolean value = markers.get(node);
        if (value != null) {
            node.implemented = value;
        }
    }

    private void saveMarker(ExpressionNode node) {
        if (node != null) {
            markers.put(node, node.implemented);
        }
    }

    private static final class Selection {
        private final ExpressionNode backupWhere;
        private final ExpressionNode constWhere;
        private final boolean isSkipped;
        private final ObjList<ExpressionNode> latestBy = new ObjList<>();
        private final ExpressionNode outerJoin;
        private final ExpressionNode postJoinWhere;
        private final ExpressionNode where;

        private Selection(IQueryModel model, IdentityHashMap<ExpressionNode, ExpressionNode> copies) {
            where = copy(model.getWhereClause(), copies, null);
            backupWhere = copy(model.getBackupWhereClause(), copies, null);
            constWhere = copy(model.getConstWhereClause(), copies, null);
            postJoinWhere = copy(model.getPostJoinWhereClause(), copies, null);
            outerJoin = copy(model.getOuterJoinExpressionClause(), copies, null);
            isSkipped = model.isSkipped();
            for (int i = 0; i < model.getLatestBy().size(); i++) {
                latestBy.add(copy(model.getLatestBy().getQuick(i), copies, null));
            }
        }

        private void apply(IQueryModel model, IdentityHashMap<ExpressionNode, ExpressionNode> copies, ObjectPool<ExpressionNode> pool) {
            model.setWhereClause(copy(where, copies, pool));
            model.setBackupWhereClause(copy(backupWhere, copies, pool));
            model.setConstWhereClause(copy(constWhere, copies, pool));
            model.setPostJoinWhereClause(copy(postJoinWhere, copies, pool));
            model.setOuterJoinExpressionClause(copy(outerJoin, copies, pool));
            model.setSkipped(isSkipped);
            model.getLatestBy().clear();
            for (int i = 0; i < latestBy.size(); i++) {
                model.addLatestBy(copy(latestBy.getQuick(i), copies, pool));
            }
        }
    }
}
