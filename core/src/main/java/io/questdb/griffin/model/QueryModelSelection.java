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

import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

import java.util.IdentityHashMap;

/**
 * The state of one model that a physical generation consumes or marks: its where clauses (the
 * filter, its backup, the constant and post-join filters, the outer-join filter), its LATEST BY
 * nodes, whether it was skipped, and the implementation markers of its LIMIT and LIMIT advice.
 * Generation clears or rewrites all of these; nothing else of a model changes when it is
 * generated. {@link #apply} puts them back as they were when the selection was taken, so that
 * the model can be generated again as if for the first time.
 * <p>
 * The saved expressions are copies, made through one memo per snapshot so that a node two
 * fields share stays shared. They keep the sub-query models they reference: those models' own
 * state is saved by their own selection.
 */
final class QueryModelSelection {
    private final ExpressionNode backupWhere;
    private final ExpressionNode constWhere;
    private final boolean isLimitAdviceHiImplemented;
    private final boolean isLimitAdviceLoImplemented;
    private final boolean isLimitHiImplemented;
    private final boolean isLimitLoImplemented;
    private final boolean isSkipped;
    private final ObjList<ExpressionNode> latestBy = new ObjList<>();
    private final ExpressionNode limitAdviceHi;
    private final ExpressionNode limitAdviceLo;
    private final ExpressionNode limitHi;
    private final ExpressionNode limitLo;
    private final ExpressionNode outerJoin;
    private final ExpressionNode postJoinWhere;
    private final ExpressionNode where;

    QueryModelSelection(IQueryModel model, IdentityHashMap<ExpressionNode, ExpressionNode> copies) {
        where = copy(model.getWhereClause(), copies, null);
        backupWhere = copy(model.getBackupWhereClause(), copies, null);
        constWhere = copy(model.getConstWhereClause(), copies, null);
        postJoinWhere = copy(model.getPostJoinWhereClause(), copies, null);
        outerJoin = copy(model.getOuterJoinExpressionClause(), copies, null);
        isSkipped = model.isSkipped();
        for (int i = 0; i < model.getLatestBy().size(); i++) {
            latestBy.add(copy(model.getLatestBy().getQuick(i), copies, null));
        }
        // LIMIT keeps its optimizer-established identity, only its marker belongs to a generation
        limitLo = model.getLimitLo();
        limitHi = model.getLimitHi();
        limitAdviceLo = model.getLimitAdviceLo();
        limitAdviceHi = model.getLimitAdviceHi();
        isLimitLoImplemented = limitLo != null && limitLo.implemented;
        isLimitHiImplemented = limitHi != null && limitHi.implemented;
        isLimitAdviceLoImplemented = limitAdviceLo != null && limitAdviceLo.implemented;
        isLimitAdviceHiImplemented = limitAdviceHi != null && limitAdviceHi.implemented;
    }

    static ExpressionNode copy(ExpressionNode node, IdentityHashMap<ExpressionNode, ExpressionNode> memo, ObjectPool<ExpressionNode> pool) {
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

    void apply(IQueryModel model, IdentityHashMap<ExpressionNode, ExpressionNode> copies, ObjectPool<ExpressionNode> pool) {
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
        resetMarker(limitLo, isLimitLoImplemented);
        resetMarker(limitHi, isLimitHiImplemented);
        resetMarker(limitAdviceLo, isLimitAdviceLoImplemented);
        resetMarker(limitAdviceHi, isLimitAdviceHiImplemented);
    }

    private static void resetMarker(ExpressionNode node, boolean implemented) {
        if (node != null) {
            node.implemented = implemented;
        }
    }
}
