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

package io.questdb.griffin;

import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.StringSink;

import java.util.ArrayDeque;

import static io.questdb.griffin.model.QueryModel.isLateralJoin;

class LateralJoinRewriter {

    private final ObjList<ExpressionNode> branchCorrelated = new ObjList<>();
    private final ObjList<ExpressionNode> branchGroupingCols = new ObjList<>();
    private final ObjList<ExpressionNode> branchNonCorrelated = new ObjList<>();
    private final CharacterStore characterStore;
    private final ObjList<ExpressionNode> correlatedPreds = new ObjList<>();
    private final ObjList<CharSequence> countColAliases = new ObjList<>();
    private final ObjectPool<ExpressionNode> expressionNodePool;
    private final FunctionParser functionParser;
    private final ObjList<ExpressionNode> groupingCols = new ObjList<>();
    private final ObjList<ExpressionNode> innerJoinCorrelated = new ObjList<>();
    private final ObjList<ExpressionNode> innerJoinNonCorrelated = new ObjList<>();
    private final ObjList<ExpressionNode> nonCorrelatedPreds = new ObjList<>();
    private final IntList orderByDirSave = new IntList();
    private final ObjList<ExpressionNode> orderBySave = new ObjList<>();
    private final ObjList<ExpressionNode> outerCols = new ObjList<>();

    private final StringSink outerRefColSink = new StringSink();
    private final LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias = new LowerCaseCharSequenceObjHashMap<>();
    private final ObjectPool<QueryColumn> queryColumnPool;
    private final ObjectPool<QueryModel> queryModelPool;
    private final ArrayDeque<ExpressionNode> sqlNodeStack;
    private final ObjList<CharSequence> subCountColAliases = new ObjList<>();
    private final ObjList<ExpressionNode> subGroupingCols = new ObjList<>();
    private final ObjectPool<WindowExpression> windowExpressionPool;
    private boolean foundCorrelation;
    private int outerRefId;

    LateralJoinRewriter(
            CharacterStore characterStore,
            ObjectPool<ExpressionNode> expressionNodePool,
            ObjectPool<QueryColumn> queryColumnPool,
            ObjectPool<QueryModel> queryModelPool,
            ObjectPool<WindowExpression> windowExpressionPool,
            ArrayDeque<ExpressionNode> sqlNodeStack,
            FunctionParser functionParser
    ) {
        this.characterStore = characterStore;
        this.expressionNodePool = expressionNodePool;
        this.queryColumnPool = queryColumnPool;
        this.queryModelPool = queryModelPool;
        this.windowExpressionPool = windowExpressionPool;
        this.sqlNodeStack = sqlNodeStack;
        this.functionParser = functionParser;
    }

    public void rewrite(QueryModel model) throws SqlException {
        outerRefId = 0;
        analyzeCorrelation(model, 0);
        decorrelate(model, 0);
    }

    private static LowerCaseCharSequenceHashSet ensureCorrelatedColumnSet(
            ObjList<LowerCaseCharSequenceHashSet> correlatedColumns,
            int index
    ) {
        if (correlatedColumns.size() <= index) {
            correlatedColumns.extendAndSet(index, new LowerCaseCharSequenceHashSet());
        }
        LowerCaseCharSequenceHashSet set = correlatedColumns.getQuick(index);
        if (set == null) {
            set = new LowerCaseCharSequenceHashSet();
            correlatedColumns.setQuick(index, set);
        }
        return set;
    }

    private static boolean hasWindowExpression(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        if (node.windowExpression != null) {
            return true;
        }
        if (hasWindowExpression(node.lhs) || hasWindowExpression(node.rhs)) {
            return true;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (hasWindowExpression(node.args.getQuick(i))) {
                return true;
            }
        }
        return false;
    }

    private static boolean isCountAggregate(ExpressionNode node) {
        return node != null
                && node.type == ExpressionNode.FUNCTION
                && Chars.equalsIgnoreCase(node.token, "count");
    }

    private static boolean isOuterRefEquality(ExpressionNode pred, CharSequence outerRefAlias) {
        if (pred.type != ExpressionNode.OPERATION || !Chars.equals(pred.token, "=")) {
            return false;
        }
        return isOuterRefToken(pred.lhs, outerRefAlias)
                || isOuterRefToken(pred.rhs, outerRefAlias);
    }

    private static boolean isOuterRefToken(ExpressionNode node, CharSequence outerRefAlias) {
        return node != null
                && node.type == ExpressionNode.LITERAL
                && Chars.startsWith(node.token, outerRefAlias)
                && node.token.length() > outerRefAlias.length()
                && node.token.charAt(outerRefAlias.length()) == '.';
    }

    private static boolean isSimpleColumnRef(ExpressionNode node) {
        return node != null && node.type == ExpressionNode.LITERAL;
    }

    private static boolean isWildcard(ObjList<QueryColumn> cols) {
        return cols.size() == 1 && cols.getQuick(1).getAst().isWildcard();
    }

    private static int toDegradedJoinType(int lateralJoinType) {
        return switch (lateralJoinType) {
            case QueryModel.JOIN_LATERAL_INNER -> QueryModel.JOIN_INNER;
            case QueryModel.JOIN_LATERAL_LEFT -> QueryModel.JOIN_LEFT_OUTER;
            case QueryModel.JOIN_LATERAL_CROSS -> QueryModel.JOIN_CROSS;
            default -> lateralJoinType;
        };
    }

    private void addColumnToOuterRefSelect(QueryModel outerRefSubquery, ExpressionNode outerCol) throws SqlException {
        ObjList<QueryColumn> cols = outerRefSubquery.getBottomUpColumns();
        for (int i = 0, n = cols.size(); i < n; i++) {
            if (Chars.equalsIgnoreCase(cols.getQuick(i).getAst().token, outerCol.token)) {
                return;
            }
        }
        int dotPos = Chars.indexOf(outerCol.token, '.');
        CharSequence colName = dotPos > 0
                ? outerCol.token.subSequence(dotPos + 1, outerCol.token.length())
                : outerCol.token;
        CharSequence alias = createColumnAlias(colName, outerRefSubquery);
        ExpressionNode ref = expressionNodePool.next().of(
                ExpressionNode.LITERAL, outerCol.token, 0, outerCol.position
        );
        QueryColumn qc = queryColumnPool.next().of(alias, ref);
        outerRefSubquery.addBottomUpColumn(qc);
    }

    private void addGroupingColsToEmbeddedWindows(
            ExpressionNode node,
            ObjList<ExpressionNode> groupingCols
    ) {
        if (node == null) {
            return;
        }
        if (node.windowExpression != null) {
            addGroupingColsToPartitionBy(node.windowExpression.getPartitionBy(), groupingCols);
        }
        addGroupingColsToEmbeddedWindows(node.lhs, groupingCols);
        addGroupingColsToEmbeddedWindows(node.rhs, groupingCols);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            addGroupingColsToEmbeddedWindows(node.args.getQuick(i), groupingCols);
        }
    }

    private void addGroupingColsToPartitionBy(
            ObjList<ExpressionNode> partitionBy,
            ObjList<ExpressionNode> groupingCols
    ) {
        for (int j = 0, m = groupingCols.size(); j < m; j++) {
            ExpressionNode gcol = groupingCols.getQuick(j);
            boolean found = false;
            for (int k = 0, p = partitionBy.size(); k < p; k++) {
                if (expressionsEqual(partitionBy.getQuick(k), gcol)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                partitionBy.add(ExpressionNode.deepClone(expressionNodePool, gcol));
            }
        }
    }


    private void addQualifiedAliasVariants(
            CharSequence token,
            CharSequence value,
            QueryModel outerModel,
            LowerCaseCharSequenceObjHashMap<CharSequence> map
    ) {
        int dotPos = Chars.indexOf(token, '.');
        if (dotPos < 0) {
            for (int j = 0, jn = outerModel.getJoinModels().size(); j < jn; j++) {
                QueryModel jm = outerModel.getJoinModels().getQuick(j);
                if (canResolveColumnForOuter(token, jm)) {
                    CharSequence modelName = jm.getName();
                    if (modelName != null) {
                        characterStore.newEntry();
                        characterStore.put(modelName).put('.').put(token);
                        CharSequence qualifiedKey = characterStore.toImmutable();
                        if (map.get(qualifiedKey) == null) {
                            map.put(qualifiedKey, value);
                        }
                    }
                }
            }
        }
    }

    private void analyzeCorrelation(QueryModel model, int lateralDepth) {
        if (model.getNestedModel() != null) {
            analyzeCorrelation(model.getNestedModel(), lateralDepth);
        }
        if (model.getUnionModel() != null) {
            analyzeCorrelation(model.getUnionModel(), lateralDepth);
        }

        for (int i = 1, n = model.getJoinModels().size(); i < n; i++) {
            QueryModel joinModel = model.getJoinModels().getQuick(i);
            if (isLateralJoin(joinModel.getJoinType())) {
                int newDepth = lateralDepth + 1;
                QueryModel topInner = joinModel.getNestedModel();
                assert topInner != null;
                collectCorrelatedRefs(topInner, model, i, newDepth);
                analyzeCorrelation(topInner, newDepth);
            } else if (joinModel.getNestedModel() != null) {
                analyzeCorrelation(joinModel.getNestedModel(), lateralDepth);
            }
        }
    }

    private void buildOuterColsFromCorrelatedColumns(
            QueryModel lateralJoinModel,
            QueryModel outerModel,
            ObjList<ExpressionNode> result
    ) {
        ObjList<LowerCaseCharSequenceHashSet> corrCols = lateralJoinModel.getCorrelatedColumns();
        for (int j = 0, m = corrCols.size(); j < m; j++) {
            LowerCaseCharSequenceHashSet colSet = corrCols.getQuick(j);
            if (colSet == null || colSet.size() == 0) {
                continue;
            }
            QueryModel outerJm = outerModel.getJoinModels().getQuick(j);
            CharSequence modelName = outerJm.getName();
            for (int k = 0, kn = colSet.getKeyCount(); k < kn; k++) {
                CharSequence colName = colSet.getKey(k);
                if (colName == null) {
                    continue;
                }
                if (modelName != null) {
                    characterStore.newEntry();
                    characterStore.put(modelName).put('.').put(colName);
                    result.add(expressionNodePool.next().of(
                            ExpressionNode.LITERAL, characterStore.toImmutable(), 0, 0
                    ));
                } else {
                    result.add(expressionNodePool.next().of(
                            ExpressionNode.LITERAL, colName, 0, 0
                    ));
                }
            }
        }
    }

    private boolean canPushThroughJoins(QueryModel model, int depth) {
        if (model.getNestedModel() == null) {
            return false;
        }
        for (int i = 1, n = model.getJoinModels().size(); i < n; i++) {
            QueryModel jm = model.getJoinModels().getQuick(i);
            if (isLateralJoin(jm.getJoinType())) {
                return false;
            }
            if (jm.getJoinCriteria() != null && hasCorrelatedExprAtDepth(jm.getJoinCriteria(), depth)) {
                return false;
            }
        }
        return model.getUnionModel() == null;
    }

    private boolean canResolveColumn(CharSequence columnName, QueryModel jm) {
        QueryModel nested = jm.getNestedModel();
        if (nested != null) {
            if (resolveColumnInChild(columnName, nested)) {
                return true;
            }
        } else if (jm.getTableNameExpr() != null) {
            if (jm.getAliasToColumnNameMap().contains(columnName)) {
                return true;
            }
        }
        return isLocalSelectAlias(columnName, jm);
    }

    private boolean canResolveColumnForOuter(CharSequence columnName, QueryModel jm) {
        if (jm.getAliasToColumnNameMap().contains(columnName)) {
            return true;
        }
        QueryModel nested = jm.getNestedModel();
        if (nested != null) {
            return resolveColumnInChild(columnName, nested);
        }
        return false;
    }

    private boolean cannotResolveAliasLocally(CharSequence alias, int end, QueryModel model) {
        QueryModel current = model;
        while (current != null) {
            if (current.getModelAliasIndex(alias, 0, end) >= 0) {
                return false;
            }
            if (current.getTableNameExpr() != null
                    || current.getJoinModels().size() > 1
                    || current.isNestedModelIsSubQuery()) {
                break;
            }
            current = current.getNestedModel();
        }
        return true;
    }

    private void collectCorrelatedRef(
            ExpressionNode node,
            QueryModel outerModel,
            int lateralJoinIndex,
            int lateralDepth,
            QueryModel innerModel,
            ObjList<LowerCaseCharSequenceHashSet> correlatedColumns
    ) {
        if (node == null) {
            return;
        }
        if (node.type == ExpressionNode.LITERAL) {
            int dotPos = Chars.indexOf(node.token, '.');
            if (dotPos > 0) {
                CharSequence tableAlias = node.token.subSequence(0, dotPos);
                int jmIndex = outerModel.getModelAliasIndex(tableAlias, 0, tableAlias.length());
                if (jmIndex >= 0 && jmIndex < lateralJoinIndex
                        && cannotResolveAliasLocally(tableAlias, tableAlias.length(), innerModel)) {
                    CharSequence colName = node.token.subSequence(dotPos + 1, node.token.length());
                    ensureCorrelatedColumnSet(correlatedColumns, jmIndex).add(colName);
                    node.lateralDepth = lateralDepth;
                    foundCorrelation = true;
                }
            } else if (!canResolveColumn(node.token, innerModel)) {
                for (int j = 0; j < lateralJoinIndex; j++) {
                    QueryModel outerJm = outerModel.getJoinModels().getQuick(j);
                    if (canResolveColumnForOuter(node.token, outerJm)) {
                        ensureCorrelatedColumnSet(correlatedColumns, j).add(node.token);
                        node.lateralDepth = lateralDepth;
                        foundCorrelation = true;
                        break;
                    }
                }
            }
            return;
        }
        if (node.type == ExpressionNode.QUERY && node.queryModel != null) {
            collectCorrelatedRefsInSubquery(node.queryModel, outerModel, lateralJoinIndex, lateralDepth, correlatedColumns);
            return;
        }

        collectCorrelatedRef(node.lhs, outerModel, lateralJoinIndex, lateralDepth, innerModel, correlatedColumns);
        collectCorrelatedRef(node.rhs, outerModel, lateralJoinIndex, lateralDepth, innerModel, correlatedColumns);

        for (int i = 0, n = node.args.size(); i < n; i++) {
            collectCorrelatedRef(node.args.getQuick(i), outerModel, lateralJoinIndex, lateralDepth, innerModel, correlatedColumns);
        }

        if (node.windowExpression != null) {
            ObjList<ExpressionNode> partitionBy = node.windowExpression.getPartitionBy();
            for (int i = 0, n = partitionBy.size(); i < n; i++) {
                collectCorrelatedRef(partitionBy.getQuick(i), outerModel, lateralJoinIndex, lateralDepth, innerModel, correlatedColumns);
            }
            ObjList<ExpressionNode> winOrderBy = node.windowExpression.getOrderBy();
            for (int i = 0, n = winOrderBy.size(); i < n; i++) {
                collectCorrelatedRef(winOrderBy.getQuick(i), outerModel, lateralJoinIndex, lateralDepth, innerModel, correlatedColumns);
            }
        }
    }

    private void collectCorrelatedRefs(
            QueryModel topInner,
            QueryModel outerModel,
            int lateralJoinIndex,
            int lateralDepth
    ) {
        ObjList<LowerCaseCharSequenceHashSet> correlatedColumns =
                outerModel.getJoinModels().getQuick(lateralJoinIndex).getCorrelatedColumns();
        QueryModel current = topInner;
        while (current != null) {
            foundCorrelation = false;
            ObjList<QueryColumn> cols = current.getBottomUpColumns();
            for (int i = 0, n = cols.size(); i < n; i++) {
                collectCorrelatedRef(cols.getQuick(i).getAst(), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);
            }

            collectCorrelatedRef(current.getWhereClause(), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);
            ObjList<ExpressionNode> orderBy = current.getOrderBy();
            for (int i = 0, n = orderBy.size(); i < n; i++) {
                collectCorrelatedRef(orderBy.getQuick(i), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);
            }

            ObjList<ExpressionNode> groupBy = current.getGroupBy();
            for (int i = 0, n = groupBy.size(); i < n; i++) {
                collectCorrelatedRef(groupBy.getQuick(i), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);
            }

            collectCorrelatedRef(current.getSampleBy(), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);
            ObjList<ExpressionNode> latestBy = current.getLatestBy();
            for (int i = 0, n = latestBy.size(); i < n; i++) {
                collectCorrelatedRef(latestBy.getQuick(i), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);
            }

            collectCorrelatedRef(current.getLimitLo(), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);
            collectCorrelatedRef(current.getLimitHi(), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);

            for (int i = 1, n = current.getJoinModels().size(); i < n; i++) {
                QueryModel jm = current.getJoinModels().getQuick(i);
                collectCorrelatedRef(jm.getJoinCriteria(), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);
            }

            if (foundCorrelation) {
                current.makeCorrelatedAtDepth(lateralDepth);
            }

            for (int i = 1, n = current.getJoinModels().size(); i < n; i++) {
                QueryModel jm = current.getJoinModels().getQuick(i);
                if (jm.getNestedModel() != null) {
                    collectCorrelatedRefs(jm.getNestedModel(), outerModel, lateralJoinIndex, lateralDepth);
                }
            }
            if (current.getUnionModel() != null) {
                collectCorrelatedRefs(current.getUnionModel(), outerModel, lateralJoinIndex, lateralDepth);
            }

            current = current.getNestedModel();
        }
    }

    private void collectCorrelatedRefsInSubquery(
            QueryModel subquery,
            QueryModel outerModel,
            int lateralJoinIndex,
            int lateralDepth,
            ObjList<LowerCaseCharSequenceHashSet> correlatedColumns
    ) {
        QueryModel current = subquery;
        while (current != null) {
            ObjList<QueryColumn> cols = current.getBottomUpColumns();
            for (int i = 0, n = cols.size(); i < n; i++) {
                collectCorrelatedRef(cols.getQuick(i).getAst(), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);
            }

            collectCorrelatedRef(current.getWhereClause(), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);
            ObjList<ExpressionNode> orderBy = current.getOrderBy();
            for (int i = 0, n = orderBy.size(); i < n; i++) {
                collectCorrelatedRef(orderBy.getQuick(i), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);
            }

            ObjList<ExpressionNode> groupBy = current.getGroupBy();
            for (int i = 0, n = groupBy.size(); i < n; i++) {
                collectCorrelatedRef(groupBy.getQuick(i), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);
            }

            collectCorrelatedRef(current.getSampleBy(), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);
            ObjList<ExpressionNode> latestBy = current.getLatestBy();
            for (int i = 0, n = latestBy.size(); i < n; i++) {
                collectCorrelatedRef(latestBy.getQuick(i), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);
            }

            collectCorrelatedRef(current.getLimitLo(), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);
            collectCorrelatedRef(current.getLimitHi(), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);

            if (current.getJoinModels().size() > 1) {
                for (int i = 1, n = current.getJoinModels().size(); i < n; i++) {
                    QueryModel jm = current.getJoinModels().getQuick(i);
                    collectCorrelatedRef(jm.getJoinCriteria(), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);
                    if (jm.getNestedModel() != null) {
                        collectCorrelatedRefsInSubquery(jm.getNestedModel(), outerModel, lateralJoinIndex, lateralDepth, correlatedColumns);
                    }
                }
            }

            if (current.getUnionModel() != null) {
                collectCorrelatedRefsInSubquery(current.getUnionModel(), outerModel, lateralJoinIndex, lateralDepth, correlatedColumns);
            }

            current = current.getNestedModel();
        }
    }


    private void compensateAggregate(
            QueryModel inner,
            ObjList<ExpressionNode> groupingCols,
            boolean isLeftJoin,
            ObjList<CharSequence> countColAliases
    ) throws SqlException {
        ObjList<ExpressionNode> groupBy = inner.getGroupBy();
        for (int i = 0, n = groupingCols.size(); i < n; i++) {
            ExpressionNode col = groupingCols.getQuick(i);
            boolean found = false;
            for (int j = 0, m = groupBy.size(); j < m; j++) {
                if (expressionsEqual(groupBy.getQuick(j), col)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                inner.addGroupBy(ExpressionNode.deepClone(expressionNodePool, col));
            }
            ensureColumnInSelect(inner, col, col.token);
        }
        if (isLeftJoin) {
            rewriteCountForLeftJoin(inner, countColAliases);
        }
    }

    private void compensateDistinct(
            QueryModel inner,
            ObjList<ExpressionNode> groupingCols
    ) throws SqlException {
        for (int j = 0, m = groupingCols.size(); j < m; j++) {
            ExpressionNode col = groupingCols.getQuick(j);
            ensureColumnInSelect(inner, col, col.token);
        }
    }

    private void compensateInnerJoins(
            QueryModel inner,
            QueryModel outer,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth
    ) {
        for (int i = 1, n = inner.getJoinModels().size(); i < n; i++) {
            QueryModel innerJoin = inner.getJoinModels().getQuick(i);
            ExpressionNode joinCriteria = innerJoin.getJoinCriteria();
            if (hasCorrelatedExprAtDepth(joinCriteria, depth)) {
                innerJoin.setJoinCriteria(
                        rewriteOuterRefs(joinCriteria, outer, inner, outerToInnerAlias)
                );
            }
        }
    }

    private void compensateLatestBy(
            QueryModel inner,
            ObjList<ExpressionNode> groupingCols
    ) throws SqlException {
        ObjList<ExpressionNode> latestBy = inner.getLatestBy();
        for (int i = 0, n = groupingCols.size(); i < n; i++) {
            ExpressionNode col = groupingCols.getQuick(i);
            boolean alreadyPresent = false;
            for (int j = 0, m = latestBy.size(); j < m; j++) {
                if (expressionsEqual(latestBy.getQuick(j), col)) {
                    alreadyPresent = true;
                    break;
                }
            }
            if (!alreadyPresent) {
                latestBy.add(ExpressionNode.deepClone(expressionNodePool, col));
            }
            ensureColumnInSelect(inner, col, col.token);
        }
    }

    private QueryModel compensateLimit(
            QueryModel current,
            ObjList<ExpressionNode> groupingCols,
            int depth
    ) throws SqlException {
        ExpressionNode limitHi = current.getLimitHi();
        ExpressionNode limitLo = current.getLimitLo();

        if (limitHi == null) {
            return current;
        }

        if (hasCorrelatedExprAtDepth(limitHi, depth)) {
            throw SqlException.position(limitHi.position)
                    .put("non-constant LIMIT in LATERAL join is not supported");
        }
        if (hasCorrelatedExprAtDepth(limitLo, depth)) {
            throw SqlException.position(limitLo.position)
                    .put("non-constant OFFSET in LATERAL join is not supported");
        }
        if (limitHi.type != ExpressionNode.CONSTANT) {
            throw SqlException.position(limitHi.position)
                    .put("non-constant LIMIT in LATERAL join is not supported");
        }
        if (limitLo != null && limitLo.type != ExpressionNode.CONSTANT) {
            throw SqlException.position(limitLo.position)
                    .put("non-constant OFFSET in LATERAL join is not supported");
        }

        orderBySave.clear();
        orderByDirSave.clear();
        QueryModel orderByModel = null;
        QueryModel cur = current;
        while (cur != null) {
            if (cur.getOrderBy().size() > 0) {
                orderByModel = cur;
                for (int i = 0, n = cur.getOrderBy().size(); i < n; i++) {
                    orderBySave.add(cur.getOrderBy().getQuick(i));
                    orderByDirSave.add(cur.getOrderByDirection().getQuick(i));
                }
                break;
            }
            if (cur.getTableNameExpr() != null || cur.getJoinModels().size() > 1) {
                break;
            }
            cur = cur.getNestedModel();
        }

        CharSequence rnAlias = createColumnAlias("_lateral_rn", current);
        ExpressionNode rnFunc = expressionNodePool.next().of(
                ExpressionNode.FUNCTION, "row_number", 0, 0
        );
        rnFunc.paramCount = 0;

        WindowExpression rnWindowExpr = windowExpressionPool.next();
        rnWindowExpr.of(rnAlias, rnFunc);

        for (int i = 0, n = groupingCols.size(); i < n; i++) {
            rnWindowExpr.getPartitionBy().add(
                    ExpressionNode.deepClone(expressionNodePool, groupingCols.getQuick(i))
            );
        }
        for (int i = 0, n = orderBySave.size(); i < n; i++) {
            rnWindowExpr.getOrderBy().add(
                    ExpressionNode.deepClone(expressionNodePool, orderBySave.getQuick(i))
            );
            rnWindowExpr.getOrderByDirection().add(orderByDirSave.getQuick(i));
        }

        current.addBottomUpColumn(rnWindowExpr);
        current.setLimit(null, null);
        if (orderByModel != null) {
            orderByModel.getOrderBy().clear();
            orderByModel.getOrderByDirection().clear();
        }
        current.getOrderBy().clear();
        current.getOrderByDirection().clear();

        QueryModel wrapper = queryModelPool.next();
        wrapper.setNestedModel(current);
        wrapper.setNestedModelIsSubQuery(true);

        ExpressionNode rnRef = expressionNodePool.next().of(
                ExpressionNode.LITERAL, rnAlias, 0, 0
        );

        ExpressionNode upperBound;
        if (limitLo != null) {
            ExpressionNode sum = createBinaryOp("+",
                    ExpressionNode.deepClone(expressionNodePool, limitLo),
                    ExpressionNode.deepClone(expressionNodePool, limitHi));
            upperBound = createBinaryOp("<=",
                    ExpressionNode.deepClone(expressionNodePool, rnRef), sum);
        } else {
            upperBound = createBinaryOp("<=",
                    ExpressionNode.deepClone(expressionNodePool, rnRef),
                    ExpressionNode.deepClone(expressionNodePool, limitHi));
        }

        if (limitLo != null) {
            ExpressionNode lowerBound = createBinaryOp(">",
                    ExpressionNode.deepClone(expressionNodePool, rnRef),
                    ExpressionNode.deepClone(expressionNodePool, limitLo));
            wrapper.setWhereClause(createBinaryOp("and", lowerBound, upperBound));
        } else {
            wrapper.setWhereClause(upperBound);
        }

        copyColumnsExcept(current, wrapper, rnAlias);
        return wrapper;
    }

    private void compensateSampleBy(
            QueryModel inner,
            ObjList<ExpressionNode> groupingCols
    ) throws SqlException {
        ObjList<ExpressionNode> groupBy = inner.getGroupBy();
        for (int i = 0, n = groupingCols.size(); i < n; i++) {
            ExpressionNode col = groupingCols.getQuick(i);
            ensureColumnInSelect(inner, col, col.token);
            boolean found = false;
            for (int j = 0, m = groupBy.size(); j < m; j++) {
                if (expressionsEqual(groupBy.getQuick(j), col)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                inner.addGroupBy(ExpressionNode.deepClone(expressionNodePool, col));
            }
        }
    }

    private void compensateSetOp(
            QueryModel inner,
            QueryModel outer,
            ObjList<ExpressionNode> groupingCols,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth
    ) throws SqlException {
        QueryModel outerRefJm = null;
        for (int j = 1, jn = inner.getJoinModels().size(); j < jn; j++) {
            QueryModel jm = inner.getJoinModels().getQuick(j);
            if (jm.getAlias() != null
                    && jm.getJoinType() == QueryModel.JOIN_CROSS
                    && Chars.startsWith(jm.getAlias().token, "__outer_ref")) {
                outerRefJm = jm;
                break;
            }
        }
        if (outerRefJm == null) {
            return;
        }

        int baseColumnCount = inner.getBottomUpColumns().size();
        QueryModel prev = inner;
        QueryModel current = inner.getUnionModel();
        while (current != null) {
            QueryModel branchOuterRef = queryModelPool.next();
            branchOuterRef.setNestedModel(outerRefJm.getNestedModel());
            branchOuterRef.setAlias(outerRefJm.getAlias());
            branchOuterRef.setJoinType(QueryModel.JOIN_CROSS);
            LowerCaseCharSequenceObjHashMap<CharSequence> srcMap = outerRefJm.getColumnNameToAliasMap();
            LowerCaseCharSequenceObjHashMap<CharSequence> dstMap = branchOuterRef.getColumnNameToAliasMap();
            ObjList<CharSequence> srcKeys = srcMap.keys();
            for (int k = 0, kn = srcKeys.size(); k < kn; k++) {
                CharSequence key = srcKeys.getQuick(k);
                dstMap.put(key, srcMap.get(key));
            }
            current.addJoinModel(branchOuterRef);

            branchCorrelated.clear();
            branchNonCorrelated.clear();
            extractCorrelatedPredicates(current.getWhereClause(), branchCorrelated, branchNonCorrelated, depth);

            branchGroupingCols.clear();
            for (int j = 0, m = groupingCols.size(); j < m; j++) {
                branchGroupingCols.add(ExpressionNode.deepClone(expressionNodePool, groupingCols.getQuick(j)));
            }

            for (int j = 0, m = branchCorrelated.size(); j < m; j++) {
                ExpressionNode pred = branchCorrelated.getQuick(j);
                pred = rewriteOuterRefs(pred, outer, current, outerToInnerAlias);
                branchNonCorrelated.add(pred);
            }
            current.setWhereClause(conjoin(branchNonCorrelated));

            rewriteSelectExpressions(current, outer, outerToInnerAlias, depth);
            rewriteOrderByExpressions(current, outer, outerToInnerAlias, depth);
            rewriteGroupByExpressions(current, outer, outerToInnerAlias, depth);
            if (current.getSampleBy() != null
                    && hasCorrelatedExprAtDepth(current.getSampleBy(), depth)) {
                current.setSampleBy(
                        rewriteOuterRefs(current.getSampleBy(), outer, current, outerToInnerAlias));
            }
            ObjList<ExpressionNode> branchLatestBy = current.getLatestBy();
            for (int j = 0, m = branchLatestBy.size(); j < m; j++) {
                ExpressionNode lb = branchLatestBy.getQuick(j);
                if (hasCorrelatedExprAtDepth(lb, depth)) {
                    branchLatestBy.setQuick(j,
                            rewriteOuterRefs(lb, outer, current, outerToInnerAlias));
                }
            }

            ObjList<ExpressionNode> branchGroupBy = current.getGroupBy();
            for (int j = 0, m = branchGroupingCols.size(); j < m; j++) {
                ExpressionNode col = branchGroupingCols.getQuick(j);
                ensureColumnInSelect(current, col, col.token);
                if (branchGroupBy.size() > 0) {
                    boolean found = false;
                    for (int k = 0, kn = branchGroupBy.size(); k < kn; k++) {
                        if (expressionsEqual(branchGroupBy.getQuick(k), col)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        current.addGroupBy(ExpressionNode.deepClone(expressionNodePool, col));
                    }
                }
            }

            if (hasWindowColumns(current)) {
                compensateWindow(current, branchGroupingCols);
            }
            if (current.getLatestBy().size() > 0) {
                compensateLatestBy(current, branchGroupingCols);
            }

            decorrelateJoinModelSubqueries(current, branchGroupingCols, outer, outerToInnerAlias, depth);

            // Recursively descend into branch nesting chain if deeper levels are correlated.
            // This mirrors pushDownOuterRefs step 5 but skips the unionModel check
            // (getUnionModel() here is the next UNION branch, not a nested UNION).
            if (current.getTableNameExpr() == null && current.getNestedModel() != null) {
                boolean canDescend = true;
                for (int ji = 1, jn = current.getJoinModels().size(); ji < jn && canDescend; ji++) {
                    QueryModel bjm = current.getJoinModels().getQuick(ji);
                    if ((bjm.getJoinCriteria() != null && hasCorrelatedExprAtDepth(bjm.getJoinCriteria(), depth))
                            || isLateralJoin(bjm.getJoinType())
                            || (bjm.getNestedModel() != null && bjm.getNestedModel().isCorrelatedAtDepth(depth))) {
                        canDescend = false;
                    }
                }
                if (canDescend) {
                    compensateInnerJoins(current, outer, outerToInnerAlias, depth);
                    deepRewriteOuterRefsInNestedLaterals(current, outer, outerToInnerAlias, depth);
                    QueryModel deepOuterRef = queryModelPool.next();
                    deepOuterRef.setNestedModel(branchOuterRef.getNestedModel());
                    deepOuterRef.setAlias(branchOuterRef.getAlias());
                    deepOuterRef.setJoinType(QueryModel.JOIN_CROSS);
                    LowerCaseCharSequenceObjHashMap<CharSequence> deepSrc = branchOuterRef.getColumnNameToAliasMap();
                    LowerCaseCharSequenceObjHashMap<CharSequence> deepDst = deepOuterRef.getColumnNameToAliasMap();
                    ObjList<CharSequence> deepKeys = deepSrc.keys();
                    for (int dk = 0, dkn = deepKeys.size(); dk < dkn; dk++) {
                        CharSequence dKey = deepKeys.getQuick(dk);
                        deepDst.put(dKey, deepSrc.get(dKey));
                    }
                    subCountColAliases.clear();
                    pushDownOuterRefs(
                            current, current.getNestedModel(), branchGroupingCols,
                            outerToInnerAlias, outer, false, subCountColAliases,
                            deepOuterRef, current, depth
                    );
                }
            }

            // LIMIT compensation for union branch
            if (current.getLimitHi() != null) {
                QueryModel wrapper = compensateLimit(current, branchGroupingCols, depth);
                if (wrapper != current) {
                    wrapper.setUnionModel(current.getUnionModel());
                    wrapper.setSetOperationType(current.getSetOperationType());
                    current.setUnionModel(null);
                    prev.setUnionModel(wrapper);
                    for (int j = 0, m = branchGroupingCols.size(); j < m; j++) {
                        ExpressionNode gcol = branchGroupingCols.getQuick(j);
                        ensureColumnInSelect(wrapper, gcol, gcol.token);
                    }
                    current = wrapper;
                }
            }

            if (current.getBottomUpColumns().size() != baseColumnCount) {
                throw SqlException.position(current.getModelPosition())
                        .put("set operation branches must have the same number of columns after decorrelation");
            }

            prev = current;
            current = current.getUnionModel();
        }
    }

    private void compensateWindow(
            QueryModel inner,
            ObjList<ExpressionNode> groupingCols
    ) throws SqlException {
        for (int i = 0, n = inner.getBottomUpColumns().size(); i < n; i++) {
            QueryColumn col = inner.getBottomUpColumns().getQuick(i);
            if (col instanceof WindowExpression) {
                addGroupingColsToPartitionBy(((WindowExpression) col).getPartitionBy(), groupingCols);
            } else {
                addGroupingColsToEmbeddedWindows(col.getAst(), groupingCols);
            }
        }
        for (int j = 0, m = groupingCols.size(); j < m; j++) {
            ExpressionNode gcol = groupingCols.getQuick(j);
            ensureColumnInSelect(inner, gcol, gcol.token);
        }
    }

    private ExpressionNode conjoin(ObjList<ExpressionNode> predicates) {
        if (predicates.size() == 0) {
            return null;
        }
        ExpressionNode result = predicates.getQuick(0);
        for (int i = 1, n = predicates.size(); i < n; i++) {
            result = createBinaryOp("and", result, predicates.getQuick(i));
        }
        return result;
    }

    private void copyColumnsExcept(
            QueryModel source,
            QueryModel target,
            CharSequence excludeAlias
    ) throws SqlException {
        ObjList<QueryColumn> cols = source.getBottomUpColumns();
        for (int i = 0, n = cols.size(); i < n; i++) {
            QueryColumn col = cols.getQuick(i);
            if (!Chars.equalsIgnoreCase(col.getAlias(), excludeAlias)) {
                ExpressionNode ref = expressionNodePool.next().of(
                        ExpressionNode.LITERAL, col.getAlias(), 0, 0
                );
                QueryColumn wrapperCol = queryColumnPool.next().of(col.getAlias(), ref);
                target.addBottomUpColumn(wrapperCol);
            }
        }
    }

    private ExpressionNode createBinaryOp(CharSequence op, ExpressionNode lhs, ExpressionNode rhs) {
        ExpressionNode node = expressionNodePool.next().of(
                ExpressionNode.OPERATION, op, 0, lhs != null ? lhs.position : 0
        );
        node.paramCount = 2;
        node.lhs = lhs;
        node.rhs = rhs;
        return node;
    }

    private CharSequence createColumnAlias(CharSequence name, QueryModel model) {
        return SqlUtil.createColumnAlias(
                characterStore,
                name,
                Chars.indexOfLastUnquoted(name, '.'),
                model.getAliasToColumnMap(),
                model.getAliasSequenceMap(),
                false
        );
    }

    private QueryModel createOuterRefBase(QueryModel outerJm) throws SqlException {
        QueryModel outerRefBase = queryModelPool.next();
        if (outerJm.getTableNameExpr() != null) {
            outerRefBase.setTableNameExpr(
                    ExpressionNode.deepClone(expressionNodePool, outerJm.getTableNameExpr())
            );
        } else if (outerJm.getNestedModel() != null) {
            outerRefBase.setNestedModel(outerJm.getNestedModel());
            outerRefBase.setNestedModelIsSubQuery(true);
        } else {
            throw SqlException.position(0)
                    .put("LATERAL decorrelation: cannot determine outer data source");
        }
        if (outerJm.getAlias() != null) {
            outerRefBase.setAlias(outerJm.getAlias());
        }
        return outerRefBase;
    }

    private void decorrelate(QueryModel model, int lateralDepth) throws SqlException {
        if (model.getNestedModel() != null) {
            decorrelate(model.getNestedModel(), lateralDepth);
        }
        if (model.getUnionModel() != null) {
            decorrelate(model.getUnionModel(), lateralDepth);
        }

        for (int i = 1, n = model.getJoinModels().size(); i < n; i++) {
            QueryModel joinModel = model.getJoinModels().getQuick(i);
            if (QueryModel.isLateralJoin(joinModel.getJoinType())) {
                QueryModel topInner = joinModel.getNestedModel();
                assert topInner != null;
                boolean isLeft = joinModel.getJoinType() == QueryModel.JOIN_LATERAL_LEFT;

                outerCols.clear();
                buildOuterColsFromCorrelatedColumns(joinModel, model, outerCols);
                if (outerCols.size() == 0) {
                    joinModel.setJoinType(toDegradedJoinType(joinModel.getJoinType()));
                    continue;
                }

                outerToInnerAlias.clear();
                QueryModel outerRefSubquery = queryModelPool.next();
                outerRefSubquery.setDistinct(true);
                characterStore.newEntry();
                characterStore.put("__outer_ref").put(outerRefId++);
                CharSequence outerRefAlias = characterStore.toImmutable();

                for (int j = 0, m = outerCols.size(); j < m; j++) {
                    addColumnToOuterRefSelect(outerRefSubquery, outerCols.getQuick(j));
                }

                setupOuterRefDataSource(outerRefSubquery, model, joinModel.getCorrelatedColumns());

                QueryModel outerRefJoinModel = queryModelPool.next();
                outerRefJoinModel.setJoinType(QueryModel.JOIN_CROSS);
                outerRefJoinModel.setNestedModel(outerRefSubquery);
                outerRefJoinModel.setNestedModelIsSubQuery(true);
                ExpressionNode outerRefAliasExpr = expressionNodePool.next().of(
                        ExpressionNode.LITERAL, outerRefAlias, 0, 0
                );
                outerRefJoinModel.setAlias(outerRefAliasExpr);

                ObjList<QueryColumn> outerRefCols = outerRefSubquery.getBottomUpColumns();
                for (int k = 0, kn = outerRefCols.size(); k < kn; k++) {
                    CharSequence colAlias = outerRefCols.getQuick(k).getAlias();
                    outerRefJoinModel.getColumnNameToAliasMap().put(colAlias, colAlias);
                    outerRefJoinModel.getAliasToColumnNameMap().put(colAlias, colAlias);
                }

                for (int j = 0, m = outerCols.size(); j < m; j++) {
                    ExpressionNode outerCol = outerCols.getQuick(j);
                    CharSequence outerRefColAlias = getOuterRefColumnAlias(outerRefAlias, outerCol, outerRefSubquery);
                    outerToInnerAlias.put(outerCol.token, outerRefColAlias);
                    addQualifiedAliasVariants(outerCol.token, outerRefColAlias, model, outerToInnerAlias);
                }

                // Build groupingCols
                groupingCols.clear();
                for (int j = 0, m = outerCols.size(); j < m; j++) {
                    ExpressionNode outerCol = outerCols.getQuick(j);
                    groupingCols.add(
                            expressionNodePool.next().of(
                                    ExpressionNode.LITERAL, outerToInnerAlias.get(outerCol.token), 0, outerCol.position
                            )
                    );
                }

                // Push down outer refs
                int depth = lateralDepth + 1;
                countColAliases.clear();
                pushDownOuterRefs(
                        null, topInner, groupingCols, outerToInnerAlias, model,
                        isLeft, countColAliases, outerRefJoinModel,
                        joinModel, depth
                );

                topInner = joinModel.getNestedModel();
                ExpressionNode joinCriteria = null;
                for (int j = 0, m = outerCols.size(); j < m; j++) {
                    ExpressionNode outerCol = outerCols.getQuick(j);
                    CharSequence outerRefColAlias = outerToInnerAlias.get(outerCol.token);
                    ExpressionNode outerRefNode = expressionNodePool.next().of(
                            ExpressionNode.LITERAL, outerRefColAlias, 0, 0
                    );
                    CharSequence selectAlias = ensureColumnInSelect(topInner, outerRefNode, outerRefColAlias);
                    ExpressionNode innerRef = expressionNodePool.next().of(
                            ExpressionNode.LITERAL, selectAlias, 0, 0
                    );
                    ExpressionNode outerRef = ExpressionNode.deepClone(expressionNodePool, outerCol);
                    ExpressionNode eq = createBinaryOp("=", innerRef, outerRef);
                    joinCriteria = joinCriteria == null ? eq : createBinaryOp("and", joinCriteria, eq);
                }
                joinModel.setJoinCriteria(joinCriteria);

                // Try eliminateOuterRef
                if (!isLeft) {
                    eliminateOuterRef(topInner, model, joinModel);
                }

                // Degrade join type
                joinModel.setJoinType(toDegradedJoinType(joinModel.getJoinType()));

                // LEFT JOIN count coalesce
                if (isLeft && countColAliases.size() > 0) {
                    wrapCountColumnsWithCoalesce(model, joinModel, countColAliases);
                }
            } else if (joinModel.getNestedModel() != null) {
                decorrelate(joinModel.getNestedModel(), lateralDepth);
            }
        }
    }

    private void decorrelateJoinModelSubqueries(
            QueryModel joinLayer,
            ObjList<ExpressionNode> groupingCols,
            QueryModel outer,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth
    ) throws SqlException {
        QueryModel outerRefJm = null;
        for (int j = 1, jn = joinLayer.getJoinModels().size(); j < jn; j++) {
            QueryModel jm = joinLayer.getJoinModels().getQuick(j);
            if (jm.getAlias() != null
                    && jm.getJoinType() == QueryModel.JOIN_CROSS
                    && Chars.startsWith(jm.getAlias().token, "__outer_ref")) {
                outerRefJm = jm;
                break;
            }
        }
        if (outerRefJm == null) {
            return;
        }

        // Process each join model subquery that has correlated refs
        for (int i = 1, n = joinLayer.getJoinModels().size(); i < n; i++) {
            QueryModel jm = joinLayer.getJoinModels().getQuick(i);
            if (jm == outerRefJm || isLateralJoin(jm.getJoinType())) {
                continue;
            }
            QueryModel nested = jm.getNestedModel();
            if (nested == null || !nested.isCorrelatedAtDepth(depth)) {
                continue;
            }

            // Clone __outer_ref for this subquery
            QueryModel clonedOuterRef = queryModelPool.next();
            clonedOuterRef.setNestedModel(outerRefJm.getNestedModel());
            clonedOuterRef.setAlias(outerRefJm.getAlias());
            clonedOuterRef.setJoinType(QueryModel.JOIN_CROSS);
            LowerCaseCharSequenceObjHashMap<CharSequence> srcMap = outerRefJm.getColumnNameToAliasMap();
            LowerCaseCharSequenceObjHashMap<CharSequence> dstMap = clonedOuterRef.getColumnNameToAliasMap();
            ObjList<CharSequence> srcKeys = srcMap.keys();
            for (int k = 0, kn = srcKeys.size(); k < kn; k++) {
                CharSequence key = srcKeys.getQuick(k);
                dstMap.put(key, srcMap.get(key));
            }

            // Clone groupingCols for this subquery
            subGroupingCols.clear();
            for (int j = 0, m = groupingCols.size(); j < m; j++) {
                subGroupingCols.add(ExpressionNode.deepClone(expressionNodePool, groupingCols.getQuick(j)));
            }

            // Use pushDownOuterRefs to decorrelate the subquery
            subCountColAliases.clear();
            pushDownOuterRefs(
                    null, nested, subGroupingCols, outerToInnerAlias,
                    outer, false, subCountColAliases, clonedOuterRef,
                    jm, depth
            );
        }
    }

    private void deepRewriteOuterRefsInNestedLaterals(
            QueryModel innerModel,
            QueryModel outerModel,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth
    ) {
        for (int i = 1, n = innerModel.getJoinModels().size(); i < n; i++) {
            QueryModel jm = innerModel.getJoinModels().getQuick(i);
            if (isLateralJoin(jm.getJoinType())) {
                QueryModel nestedLateral = jm.getNestedModel();
                if (nestedLateral != null) {
                    rewriteSelectExpressions(nestedLateral, outerModel, outerToInnerAlias, depth);
                    rewriteOrderByExpressions(nestedLateral, outerModel, outerToInnerAlias, depth);
                    rewriteGroupByExpressions(nestedLateral, outerModel, outerToInnerAlias, depth);

                    if (nestedLateral.getWhereClause() != null
                            && hasCorrelatedExprAtDepth(nestedLateral.getWhereClause(), depth)) {
                        nestedLateral.setWhereClause(
                                rewriteOuterRefs(nestedLateral.getWhereClause(), outerModel, nestedLateral, outerToInnerAlias)
                        );
                    }

                    if (nestedLateral.getLimitLo() != null
                            && hasCorrelatedExprAtDepth(nestedLateral.getLimitLo(), depth)) {
                        nestedLateral.setLimit(
                                rewriteOuterRefs(nestedLateral.getLimitLo(), outerModel, nestedLateral, outerToInnerAlias),
                                nestedLateral.getLimitHi()
                        );
                    }
                    if (nestedLateral.getLimitHi() != null
                            && hasCorrelatedExprAtDepth(nestedLateral.getLimitHi(), depth)) {
                        nestedLateral.setLimit(
                                nestedLateral.getLimitLo(),
                                rewriteOuterRefs(nestedLateral.getLimitHi(), outerModel, nestedLateral, outerToInnerAlias)
                        );
                    }

                    if (nestedLateral.getJoinCriteria() != null
                            && hasCorrelatedExprAtDepth(nestedLateral.getJoinCriteria(), depth)) {
                        nestedLateral.setJoinCriteria(
                                rewriteOuterRefs(nestedLateral.getJoinCriteria(), outerModel, nestedLateral, outerToInnerAlias)
                        );
                    }

                    if (nestedLateral.getSampleBy() != null
                            && hasCorrelatedExprAtDepth(nestedLateral.getSampleBy(), depth)) {
                        nestedLateral.setSampleBy(
                                rewriteOuterRefs(nestedLateral.getSampleBy(), outerModel, nestedLateral, outerToInnerAlias)
                        );
                    }
                    ObjList<ExpressionNode> latestBy = nestedLateral.getLatestBy();
                    for (int j = 0, m = latestBy.size(); j < m; j++) {
                        ExpressionNode lb = latestBy.getQuick(j);
                        if (hasCorrelatedExprAtDepth(lb, depth)) {
                            latestBy.setQuick(j,
                                    rewriteOuterRefs(lb, outerModel, nestedLateral, outerToInnerAlias));
                        }
                    }

                    for (int k = 1, kn = nestedLateral.getJoinModels().size(); k < kn; k++) {
                        QueryModel innerJm = nestedLateral.getJoinModels().getQuick(k);
                        if (!isLateralJoin(innerJm.getJoinType())) {
                            if (innerJm.getJoinCriteria() != null
                                    && hasCorrelatedExprAtDepth(innerJm.getJoinCriteria(), depth)) {
                                innerJm.setJoinCriteria(
                                        rewriteOuterRefs(innerJm.getJoinCriteria(), outerModel, nestedLateral, outerToInnerAlias));
                            }
                        }
                    }

                    deepRewriteOuterRefsInNestedLaterals(nestedLateral, outerModel, outerToInnerAlias, depth);
                }
            }
        }
        QueryModel unionBranch = innerModel.getUnionModel();
        while (unionBranch != null) {
            deepRewriteOuterRefsInNestedLaterals(unionBranch, outerModel, outerToInnerAlias, depth);
            unionBranch = unionBranch.getUnionModel();
        }
    }

    private void eliminateOuterRef(
            QueryModel topInner,
            QueryModel outerModel,
            QueryModel joinModel
    ) throws SqlException {
        // 1. Find __outer_ref cross join
        QueryModel dataSourceLayer = null;
        int outerRefJmIndex = -1;
        CharSequence outerRefAlias = null;
        QueryModel current = topInner;
        while (current != null) {
            for (int j = 1, jn = current.getJoinModels().size(); j < jn; j++) {
                QueryModel jm = current.getJoinModels().getQuick(j);
                if (jm.getAlias() != null
                        && jm.getJoinType() == QueryModel.JOIN_CROSS
                        && Chars.startsWith(jm.getAlias().token, "__outer_ref")) {
                    dataSourceLayer = current;
                    outerRefJmIndex = j;
                    outerRefAlias = jm.getAlias().token;
                    break;
                }
            }
            if (dataSourceLayer != null) {
                break;
            }
            if (current.getTableNameExpr() != null || current.getJoinModels().size() > 1) {
                break;
            }
            current = current.getNestedModel();
        }
        if (outerRefJmIndex < 0) {
            return;
        }

        // Guard: bail out if any layer has UNION or nested lateral joins
        current = topInner;
        while (current != null) {
            if (current.getUnionModel() != null) {
                return;
            }
            for (int i = 1, n = current.getJoinModels().size(); i < n; i++) {
                if (isLateralJoin(current.getJoinModels().getQuick(i).getJoinType())) {
                    return;
                }
            }
            if (current == dataSourceLayer) {
                break;
            }
            current = current.getNestedModel();
        }

        QueryModel outerRefSubquery = dataSourceLayer.getJoinModels().getQuick(outerRefJmIndex).getNestedModel();

        // 2. Scan injection layer WHERE for equalities
        outerToInnerAlias.clear();
        ExpressionNode where = dataSourceLayer.getWhereClause();
        if (where == null) {
            return;
        }
        scanWhereForOuterRefEqualities(where, outerRefAlias, outerToInnerAlias);

        // 3. Check if every __outer_ref column has a matching equality
        ObjList<QueryColumn> outerRefCols = outerRefSubquery.getBottomUpColumns();
        for (int j = 0, m = outerRefCols.size(); j < m; j++) {
            CharSequence colAlias = outerRefCols.getQuick(j).getAlias();
            outerRefColSink.clear();
            outerRefColSink.put(outerRefAlias).put('.').put(colAlias);
            if (outerToInnerAlias.get(outerRefColSink) == null) {
                return;
            }
        }

        // 4. Top-down scan all layers, remove/rewrite __outer_ref columns
        current = topInner;
        while (current != null) {
            ObjList<QueryColumn> cols = current.getBottomUpColumns();
            for (int j = cols.size() - 1; j >= 0; j--) {
                QueryColumn col = cols.getQuick(j);
                ExpressionNode ast = col.getAst();
                if (ast != null
                        && ast.type == ExpressionNode.LITERAL
                        && Chars.startsWith(ast.token, outerRefAlias)
                        && ast.token.length() > outerRefAlias.length()
                        && ast.token.charAt(outerRefAlias.length()) == '.') {
                    current.removeColumn(j);
                } else {
                    ExpressionNode rewritten = rewriteOuterRefs(ast, outerModel, dataSourceLayer, outerToInnerAlias);
                    if (rewritten != ast) {
                        col.of(col.getAlias(), rewritten);
                    }
                }
            }
            if (current == dataSourceLayer) {
                break;
            }
            current = current.getNestedModel();
        }

        // 5. Rewrite WHERE, ORDER BY, GROUP BY, etc. in injection layer
        nonCorrelatedPreds.clear();
        splitWhereForElimination(where, outerRefAlias, outerModel, dataSourceLayer, nonCorrelatedPreds);
        dataSourceLayer.setWhereClause(conjoin(nonCorrelatedPreds));

        ObjList<ExpressionNode> orderBy = dataSourceLayer.getOrderBy();
        for (int j = 0, m = orderBy.size(); j < m; j++) {
            orderBy.setQuick(j,
                    rewriteOuterRefs(orderBy.getQuick(j), outerModel, dataSourceLayer, outerToInnerAlias));
        }
        ObjList<ExpressionNode> groupBy = dataSourceLayer.getGroupBy();
        for (int j = 0, m = groupBy.size(); j < m; j++) {
            groupBy.setQuick(j,
                    rewriteOuterRefs(groupBy.getQuick(j), outerModel, dataSourceLayer, outerToInnerAlias));
        }
        if (dataSourceLayer.getSampleBy() != null) {
            dataSourceLayer.setSampleBy(
                    rewriteOuterRefs(dataSourceLayer.getSampleBy(), outerModel, dataSourceLayer, outerToInnerAlias));
        }
        ObjList<ExpressionNode> latestBy = dataSourceLayer.getLatestBy();
        for (int j = 0, m = latestBy.size(); j < m; j++) {
            latestBy.setQuick(j,
                    rewriteOuterRefs(latestBy.getQuick(j), outerModel, dataSourceLayer, outerToInnerAlias));
        }

        // 6. Rebuild joinCriteria
        ExpressionNode newCriteria = null;
        ObjList<QueryColumn> outerRefSelectCols = outerRefSubquery.getBottomUpColumns();
        for (int j = 0, m = outerRefSelectCols.size(); j < m; j++) {
            QueryColumn refCol = outerRefSelectCols.getQuick(j);
            outerRefColSink.clear();
            outerRefColSink.put(outerRefAlias).put('.').put(refCol.getAlias());
            CharSequence innerCol = outerToInnerAlias.get(outerRefColSink);
            if (innerCol != null) {
                ExpressionNode innerNode = expressionNodePool.next().of(
                        ExpressionNode.LITERAL, innerCol, 0, 0
                );
                ensureColumnInSelect(topInner, innerNode, innerCol);
                ExpressionNode outerNode = ExpressionNode.deepClone(expressionNodePool, refCol.getAst());
                ExpressionNode eq = createBinaryOp("=", innerNode, outerNode);
                newCriteria = newCriteria == null ? eq : createBinaryOp("and", newCriteria, eq);
            }
        }
        joinModel.setJoinCriteria(newCriteria);

        // 7. Remove __outer_ref cross join
        dataSourceLayer.getJoinModels().remove(outerRefJmIndex);
    }

    private CharSequence ensureColumnInSelect(
            QueryModel model,
            ExpressionNode colExpr,
            CharSequence preferredAlias
    ) throws SqlException {
        ObjList<QueryColumn> cols = model.getBottomUpColumns();
        for (int i = 0, n = cols.size(); i < n; i++) {
            QueryColumn existing = cols.getQuick(i);
            if (expressionsEqual(existing.getAst(), colExpr)) {
                return existing.getAlias();
            }
            if (colExpr.type == ExpressionNode.LITERAL
                    && Chars.equalsIgnoreCase(existing.getAlias(), colExpr.token)) {
                return existing.getAlias();
            }
        }
        CharSequence alias = createColumnAlias(preferredAlias, model);
        QueryColumn qc = queryColumnPool.next().of(alias, colExpr);
        model.addBottomUpColumn(qc);
        return alias;
    }

    private boolean expressionsEqual(ExpressionNode a, ExpressionNode b) {
        if (a == b) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }
        if (a.type != b.type) {
            return false;
        }
        if (!Chars.equalsIgnoreCase(a.token, b.token)) {
            return false;
        }
        if (a.type == ExpressionNode.LITERAL) {
            return true;
        }
        if (!expressionsEqual(a.lhs, b.lhs)) {
            return false;
        }
        if (!expressionsEqual(a.rhs, b.rhs)) {
            return false;
        }
        if (a.args.size() != b.args.size()) {
            return false;
        }
        for (int i = 0, n = a.args.size(); i < n; i++) {
            if (!expressionsEqual(a.args.getQuick(i), b.args.getQuick(i))) {
                return false;
            }
        }
        return ExpressionNode.compareWindowExpressions(a.windowExpression, b.windowExpression);
    }

    private void extractCorrelatedFromInnerJoins(
            QueryModel inner,
            ObjList<ExpressionNode> correlated,
            int depth
    ) {
        for (int i = 1, n = inner.getJoinModels().size(); i < n; i++) {
            QueryModel jm = inner.getJoinModels().getQuick(i);
            ExpressionNode joinCriteria = jm.getJoinCriteria();
            if (joinCriteria != null
                    && (jm.getJoinType() == QueryModel.JOIN_INNER
                    || jm.getJoinType() == QueryModel.JOIN_CROSS)) {
                innerJoinCorrelated.clear();
                innerJoinNonCorrelated.clear();
                extractCorrelatedPredicates(joinCriteria, innerJoinCorrelated, innerJoinNonCorrelated, depth);

                if (innerJoinCorrelated.size() > 0) {
                    correlated.addAll(innerJoinCorrelated);
                    jm.setJoinCriteria(innerJoinNonCorrelated.size() > 0
                            ? conjoin(innerJoinNonCorrelated) : null);
                }
            }
        }
    }

    private void extractCorrelatedPredicates(
            ExpressionNode expr,
            ObjList<ExpressionNode> correlated,
            ObjList<ExpressionNode> nonCorrelated,
            int depth
    ) {
        if (expr == null) {
            return;
        }
        sqlNodeStack.clear();
        ExpressionNode node = expr;
        while (node != null || !sqlNodeStack.isEmpty()) {
            if (node != null && SqlKeywords.isAndKeyword(node.token)) {
                if (node.rhs != null) {
                    sqlNodeStack.push(node.rhs);
                }
                node = node.lhs;
            } else {
                if (node != null) {
                    if (hasCorrelatedExprAtDepth(node, depth)) {
                        correlated.add(node);
                    } else {
                        nonCorrelated.add(node);
                    }
                }
                node = sqlNodeStack.isEmpty() ? null : sqlNodeStack.pop();
            }
        }
    }

    private CharSequence getOuterRefColumnAlias(
            CharSequence outerRefAlias,
            ExpressionNode outerCol,
            QueryModel outerRefSubquery
    ) {
        ObjList<QueryColumn> cols = outerRefSubquery.getBottomUpColumns();
        for (int i = 0, n = cols.size(); i < n; i++) {
            QueryColumn col = cols.getQuick(i);
            if (Chars.equalsIgnoreCase(col.getAst().token, outerCol.token)) {
                characterStore.newEntry();
                characterStore.put(outerRefAlias).put('.').put(col.getAlias());
                return characterStore.toImmutable();
            }
        }
        int dotPos = Chars.indexOf(outerCol.token, '.');
        characterStore.newEntry();
        characterStore.put(outerRefAlias).put('.');
        if (dotPos > 0) {
            characterStore.put(outerCol.token, dotPos + 1, outerCol.token.length() - dotPos - 1);
        } else {
            characterStore.put(outerCol.token);
        }
        return characterStore.toImmutable();
    }

    private boolean hasAggregateFunctions(QueryModel model) {
        ObjList<QueryColumn> cols = model.getBottomUpColumns();
        for (int i = 0, n = cols.size(); i < n; i++) {
            QueryColumn col = cols.getQuick(i);
            if (col instanceof WindowExpression) {
                continue;
            }
            ExpressionNode ast = col.getAst();
            if (ast != null && SqlOptimiser.hasGroupByFunc(
                    sqlNodeStack, functionParser.getFunctionFactoryCache(), ast)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasCorrelatedExprAtDepth(ExpressionNode node, int depth) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.LITERAL) {
            return node.lateralDepth == depth;
        }
        if (node.type == ExpressionNode.QUERY) {
            return false;
        }
        if (hasCorrelatedExprAtDepth(node.lhs, depth) || hasCorrelatedExprAtDepth(node.rhs, depth)) {
            return true;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (hasCorrelatedExprAtDepth(node.args.getQuick(i), depth)) {
                return true;
            }
        }
        if (node.windowExpression != null) {
            ObjList<ExpressionNode> partitionBy = node.windowExpression.getPartitionBy();
            for (int i = 0, n = partitionBy.size(); i < n; i++) {
                if (hasCorrelatedExprAtDepth(partitionBy.getQuick(i), depth)) {
                    return true;
                }
            }
            ObjList<ExpressionNode> winOrderBy = node.windowExpression.getOrderBy();
            for (int i = 0, n = winOrderBy.size(); i < n; i++) {
                if (hasCorrelatedExprAtDepth(winOrderBy.getQuick(i), depth)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean hasWindowColumns(QueryModel model) {
        ObjList<QueryColumn> cols = model.getBottomUpColumns();
        for (int i = 0, n = cols.size(); i < n; i++) {
            QueryColumn col = cols.getQuick(i);
            if (col instanceof WindowExpression) {
                return true;
            }
            if (hasWindowExpression(col.getAst())) {
                return true;
            }
        }
        return false;
    }

    private boolean isLocalSelectAlias(CharSequence columnName, QueryModel jm) {
        ObjList<QueryColumn> cols = jm.getBottomUpColumns();
        for (int i = 0, n = cols.size(); i < n; i++) {
            QueryColumn col = cols.getQuick(i);
            if (Chars.equalsIgnoreCase(col.getAlias(), columnName)) {
                ExpressionNode ast = col.getAst();
                if (ast.type != ExpressionNode.LITERAL) {
                    return true;
                }
                return !Chars.equalsIgnoreCase(ast.token, columnName);
            }
        }
        return false;
    }

    private void pushDownOuterRefs(
            QueryModel parent,
            QueryModel current,
            ObjList<ExpressionNode> groupingCols,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            QueryModel outer,
            boolean isLeftJoin,
            ObjList<CharSequence> countColAliases,
            QueryModel outerRefJoinModel,
            QueryModel lateralJoinModel,
            int depth
    ) throws SqlException {
        // 1. LIMIT compensation
        if (current.getLimitHi() != null) {
            QueryModel wrapper = compensateLimit(current, groupingCols, depth);
            if (wrapper != current) {
                if (parent != null) {
                    parent.setNestedModel(wrapper);
                } else {
                    lateralJoinModel.setNestedModel(wrapper);
                }
                for (int j = 0, m = groupingCols.size(); j < m; j++) {
                    ExpressionNode gcol = groupingCols.getQuick(j);
                    ensureColumnInSelect(wrapper, gcol, gcol.token);
                }
            }
        }

        // 2. Add groupingCols to current layer SELECT
        if (current.getBottomUpColumns().size() > 0) {
            for (int j = 0, m = groupingCols.size(); j < m; j++) {
                ExpressionNode gcol = groupingCols.getQuick(j);
                ensureColumnInSelect(current, gcol, gcol.token);
            }
        }

        // 3. Compensate current layer operators
        boolean hasGroupBy = current.getGroupBy().size() > 0;
        boolean hasAggregates = hasAggregateFunctions(current);
        if (current.getSampleBy() != null) {
            compensateSampleBy(current, groupingCols);
        }
        if (hasGroupBy || hasAggregates) {
            compensateAggregate(current, groupingCols, isLeftJoin, countColAliases);
        }
        if (hasWindowColumns(current)) {
            compensateWindow(current, groupingCols);
        }
        if (current.isDistinct()) {
            compensateDistinct(current, groupingCols);
        }
        if (current.getLatestBy().size() > 0) {
            compensateLatestBy(current, groupingCols);
        }

        // 4. Rewrite current layer correlated references
        rewriteSelectExpressions(current, outer, outerToInnerAlias, depth);
        rewriteOrderByExpressions(current, outer, outerToInnerAlias, depth);
        rewriteGroupByExpressions(current, outer, outerToInnerAlias, depth);
        if (current.getJoinCriteria() != null
                && hasCorrelatedExprAtDepth(current.getJoinCriteria(), depth)) {
            current.setJoinCriteria(
                    rewriteOuterRefs(current.getJoinCriteria(), outer, current, outerToInnerAlias));
        }
        if (current.getSampleBy() != null
                && hasCorrelatedExprAtDepth(current.getSampleBy(), depth)) {
            current.setSampleBy(
                    rewriteOuterRefs(current.getSampleBy(), outer, current, outerToInnerAlias));
        }
        ObjList<ExpressionNode> latestBy = current.getLatestBy();
        for (int li = 0, ln = latestBy.size(); li < ln; li++) {
            ExpressionNode lb = latestBy.getQuick(li);
            if (hasCorrelatedExprAtDepth(lb, depth)) {
                latestBy.setQuick(li,
                        rewriteOuterRefs(lb, outer, current, outerToInnerAlias));
            }
        }

        if (current.getWhereClause() != null
                && hasCorrelatedExprAtDepth(current.getWhereClause(), depth)) {
            current.setWhereClause(
                    rewriteOuterRefs(current.getWhereClause(), outer, current, outerToInnerAlias));
        }

        // 5. Terminate or recurse
        if (current.getTableNameExpr() != null) {
            terminateHere(current, groupingCols, outerRefJoinModel, outer, outerToInnerAlias, depth);
        } else if (current.getJoinModels().size() > 1) {
            if (canPushThroughJoins(current, depth)) {
                // Neumann-style push: handle each join model independently
                compensateInnerJoins(current, outer, outerToInnerAlias, depth);
                deepRewriteOuterRefsInNestedLaterals(current, outer, outerToInnerAlias, depth);

                // Push delim into correlated join model subqueries (index > 0)
                for (int ji = 1, jn = current.getJoinModels().size(); ji < jn; ji++) {
                    QueryModel jm = current.getJoinModels().getQuick(ji);
                    if (isLateralJoin(jm.getJoinType()) || jm.getNestedModel() == null) {
                        continue;
                    }
                    boolean jmCorrelated = jm.getNestedModel().isCorrelatedAtDepth(depth);
                    boolean needsDelimForJoinType = jm.getJoinType() == QueryModel.JOIN_LEFT_OUTER
                            || jm.getJoinType() == QueryModel.JOIN_FULL_OUTER;

                    if (jmCorrelated || needsDelimForJoinType) {
                        // Clone __outer_ref and push into this subquery
                        QueryModel clonedOuterRef = queryModelPool.next();
                        clonedOuterRef.setNestedModel(outerRefJoinModel.getNestedModel());
                        clonedOuterRef.setAlias(outerRefJoinModel.getAlias());
                        clonedOuterRef.setJoinType(QueryModel.JOIN_CROSS);
                        LowerCaseCharSequenceObjHashMap<CharSequence> srcMap = outerRefJoinModel.getColumnNameToAliasMap();
                        LowerCaseCharSequenceObjHashMap<CharSequence> dstMap = clonedOuterRef.getColumnNameToAliasMap();
                        ObjList<CharSequence> srcKeys = srcMap.keys();
                        for (int sk = 0, skn = srcKeys.size(); sk < skn; sk++) {
                            CharSequence key = srcKeys.getQuick(sk);
                            dstMap.put(key, srcMap.get(key));
                        }
                        subGroupingCols.clear();
                        for (int sg = 0, sgn = groupingCols.size(); sg < sgn; sg++) {
                            subGroupingCols.add(ExpressionNode.deepClone(expressionNodePool, groupingCols.getQuick(sg)));
                        }
                        subCountColAliases.clear();
                        pushDownOuterRefs(
                                null, jm.getNestedModel(), subGroupingCols, outerToInnerAlias,
                                outer, false, subCountColAliases, clonedOuterRef,
                                jm, depth
                        );

                        // Add delim column alignment to join criteria.
                        // Step 2 already added groupingCols to current's SELECT,
                        // so we use those aliases for the main chain side.
                        ExpressionNode alignCriteria = jm.getJoinCriteria();
                        QueryModel jmTop = jm.getNestedModel();
                        CharSequence jmAlias = jm.getAlias() != null ? jm.getAlias().token : null;
                        for (int gc = 0, gcn = groupingCols.size(); gc < gcn; gc++) {
                            ExpressionNode gcol = groupingCols.getQuick(gc);
                            ExpressionNode gcolForSub = ExpressionNode.deepClone(expressionNodePool, gcol);
                            CharSequence subAlias = ensureColumnInSelect(jmTop, gcolForSub, gcol.token);
                            ExpressionNode gcolForMain = ExpressionNode.deepClone(expressionNodePool, gcol);
                            CharSequence mainAlias = ensureColumnInSelect(current, gcolForMain, gcol.token);

                            // Build: jm_alias.subAlias = mainAlias
                            CharSequence qualifiedSubCol;
                            if (jmAlias != null) {
                                characterStore.newEntry();
                                characterStore.put(jmAlias).put('.').put(subAlias);
                                qualifiedSubCol = characterStore.toImmutable();
                            } else {
                                qualifiedSubCol = subAlias;
                            }
                            ExpressionNode subRef = expressionNodePool.next().of(
                                    ExpressionNode.LITERAL, qualifiedSubCol, 0, 0
                            );
                            ExpressionNode mainRef = expressionNodePool.next().of(
                                    ExpressionNode.LITERAL, mainAlias, 0, 0
                            );
                            ExpressionNode eq = createBinaryOp("=", subRef, mainRef);
                            alignCriteria = alignCriteria == null ? eq : createBinaryOp("and", alignCriteria, eq);
                        }
                        jm.setJoinCriteria(alignCriteria);

                        // If join had no criteria before, upgrade CROSS to INNER
                        if (jm.getJoinType() == QueryModel.JOIN_CROSS) {
                            jm.setJoinType(QueryModel.JOIN_INNER);
                        }
                    }
                }

                // Push delim into main chain (index 0)
                pushDownOuterRefs(
                        current, current.getNestedModel(), groupingCols, outerToInnerAlias,
                        outer, isLeftJoin, countColAliases, outerRefJoinModel,
                        lateralJoinModel, depth
                );
            } else {
                terminateHere(current, groupingCols, outerRefJoinModel, outer, outerToInnerAlias, depth);
            }
        } else if (current.getNestedModel() != null) {
            if (current.getUnionModel() != null) {
                terminateHere(current, groupingCols, outerRefJoinModel, outer, outerToInnerAlias, depth);
            } else if (current.getNestedModel().isCorrelatedAtDepth(depth)) {
                pushDownOuterRefs(
                        current, current.getNestedModel(), groupingCols, outerToInnerAlias,
                        outer, isLeftJoin, countColAliases, outerRefJoinModel,
                        lateralJoinModel, depth
                );
            } else {
                terminateHere(current, groupingCols, outerRefJoinModel, outer, outerToInnerAlias, depth);
            }
        }
    }

    private boolean resolveColumnInChild(CharSequence columnName, QueryModel model) {
        for (int i = 0, n = model.getJoinModels().size(); i < n; i++) {
            QueryModel jm = model.getJoinModels().getQuick(i);
            if (jm.getAliasToColumnNameMap().size() > 0 && !isWildcard(jm.getBottomUpColumns())) {
                if (jm.getAliasToColumnNameMap().contains(columnName)) {
                    return true;
                }
            } else {
                QueryModel nested = jm.getNestedModel();
                if (nested != null && resolveColumnInChild(columnName, nested)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void rewriteCountForLeftJoin(
            QueryModel inner,
            ObjList<CharSequence> countColAliases
    ) {
        for (int i = 0, n = inner.getBottomUpColumns().size(); i < n; i++) {
            QueryColumn col = inner.getBottomUpColumns().getQuick(i);
            if (isCountAggregate(col.getAst())) {
                countColAliases.add(col.getAlias());
            }
        }
    }

    private void rewriteGroupByExpressions(
            QueryModel inner,
            QueryModel outer,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth
    ) {
        ObjList<ExpressionNode> groupBy = inner.getGroupBy();
        for (int i = 0, n = groupBy.size(); i < n; i++) {
            ExpressionNode expr = groupBy.getQuick(i);
            if (hasCorrelatedExprAtDepth(expr, depth)) {
                groupBy.setQuick(i,
                        rewriteOuterRefs(expr, outer, inner, outerToInnerAlias));
            }
        }
    }

    private void rewriteOrderByExpressions(
            QueryModel inner,
            QueryModel outer,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth
    ) {
        ObjList<ExpressionNode> orderBy = inner.getOrderBy();
        for (int i = 0, n = orderBy.size(); i < n; i++) {
            ExpressionNode expr = orderBy.getQuick(i);
            if (hasCorrelatedExprAtDepth(expr, depth)) {
                orderBy.setQuick(i,
                        rewriteOuterRefs(expr, outer, inner, outerToInnerAlias));
            }
        }
    }

    private ExpressionNode rewriteOuterRefs(
            ExpressionNode node,
            QueryModel outerModel,
            QueryModel innerModel,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias
    ) {
        if (node == null) {
            return null;
        }
        if (node.type == ExpressionNode.QUERY && node.queryModel != null) {
            rewriteOuterRefsInSubquery(node.queryModel, outerModel, innerModel, outerToInnerAlias);
            return node;
        }
        if (node.type == ExpressionNode.LITERAL) {
            CharSequence mapped = outerToInnerAlias.get(node.token);
            if (mapped != null) {
                return expressionNodePool.next().of(
                        ExpressionNode.LITERAL, mapped, node.precedence, node.position
                );
            }
            int dotPos = Chars.indexOf(node.token, '.');
            if (dotPos > 0) {
                CharSequence tableAlias = node.token.subSequence(0, dotPos);
                if (outerModel.getModelAliasIndex(tableAlias, 0, tableAlias.length()) >= 0
                        && innerModel.getModelAliasIndex(tableAlias, 0, tableAlias.length()) < 0) {
                    CharSequence colOnly = node.token.subSequence(dotPos + 1, node.token.length());
                    mapped = outerToInnerAlias.get(colOnly);
                    if (mapped != null) {
                        return expressionNodePool.next().of(
                                ExpressionNode.LITERAL, mapped, node.precedence, node.position
                        );
                    }
                }
            }
            return node;
        }
        node.lhs = rewriteOuterRefs(node.lhs, outerModel, innerModel, outerToInnerAlias);
        node.rhs = rewriteOuterRefs(node.rhs, outerModel, innerModel, outerToInnerAlias);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            node.args.setQuick(i, rewriteOuterRefs(
                    node.args.getQuick(i), outerModel, innerModel, outerToInnerAlias));
        }
        if (node.windowExpression != null) {
            ObjList<ExpressionNode> partitionBy = node.windowExpression.getPartitionBy();
            for (int i = 0, n = partitionBy.size(); i < n; i++) {
                partitionBy.setQuick(i, rewriteOuterRefs(
                        partitionBy.getQuick(i), outerModel, innerModel, outerToInnerAlias));
            }
            ObjList<ExpressionNode> winOrderBy = node.windowExpression.getOrderBy();
            for (int i = 0, n = winOrderBy.size(); i < n; i++) {
                winOrderBy.setQuick(i, rewriteOuterRefs(
                        winOrderBy.getQuick(i), outerModel, innerModel, outerToInnerAlias));
            }
        }
        return node;
    }

    private void rewriteOuterRefsInSubquery(
            QueryModel subquery,
            QueryModel outerModel,
            QueryModel innerModel,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias
    ) {
        ObjList<QueryColumn> cols = subquery.getBottomUpColumns();
        for (int i = 0, n = cols.size(); i < n; i++) {
            QueryColumn col = cols.getQuick(i);
            ExpressionNode ast = col.getAst();
            ExpressionNode rewritten = rewriteOuterRefs(ast, outerModel, innerModel, outerToInnerAlias);
            if (rewritten != ast) {
                col.of(col.getAlias(), rewritten);
            }
        }
        if (subquery.getWhereClause() != null) {
            subquery.setWhereClause(
                    rewriteOuterRefs(subquery.getWhereClause(), outerModel, innerModel, outerToInnerAlias)
            );
        }
        ObjList<ExpressionNode> orderBy = subquery.getOrderBy();
        for (int i = 0, n = orderBy.size(); i < n; i++) {
            orderBy.setQuick(i,
                    rewriteOuterRefs(orderBy.getQuick(i), outerModel, innerModel, outerToInnerAlias));
        }
        ObjList<ExpressionNode> groupBy = subquery.getGroupBy();
        for (int i = 0, n = groupBy.size(); i < n; i++) {
            groupBy.setQuick(i,
                    rewriteOuterRefs(groupBy.getQuick(i), outerModel, innerModel, outerToInnerAlias));
        }
        if (subquery.getSampleBy() != null) {
            subquery.setSampleBy(
                    rewriteOuterRefs(subquery.getSampleBy(), outerModel, innerModel, outerToInnerAlias));
        }
        ObjList<ExpressionNode> latestBy = subquery.getLatestBy();
        for (int i = 0, n = latestBy.size(); i < n; i++) {
            latestBy.setQuick(i,
                    rewriteOuterRefs(latestBy.getQuick(i), outerModel, innerModel, outerToInnerAlias));
        }
        if (subquery.getLimitLo() != null) {
            subquery.setLimit(
                    rewriteOuterRefs(subquery.getLimitLo(), outerModel, innerModel, outerToInnerAlias),
                    subquery.getLimitHi() != null
                            ? rewriteOuterRefs(subquery.getLimitHi(), outerModel, innerModel, outerToInnerAlias)
                            : null
            );
        }
        for (int i = 1, n = subquery.getJoinModels().size(); i < n; i++) {
            QueryModel jm = subquery.getJoinModels().getQuick(i);
            if (jm.getJoinCriteria() != null) {
                jm.setJoinCriteria(
                        rewriteOuterRefs(jm.getJoinCriteria(), outerModel, innerModel, outerToInnerAlias)
                );
            }
        }
        if (subquery.getNestedModel() != null) {
            rewriteOuterRefsInSubquery(subquery.getNestedModel(), outerModel, innerModel, outerToInnerAlias);
        }
        if (subquery.getUnionModel() != null) {
            rewriteOuterRefsInSubquery(subquery.getUnionModel(), outerModel, innerModel, outerToInnerAlias);
        }
    }

    private void rewriteSelectExpressions(
            QueryModel inner,
            QueryModel outer,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth
    ) {
        for (int i = 0, n = inner.getBottomUpColumns().size(); i < n; i++) {
            QueryColumn col = inner.getBottomUpColumns().getQuick(i);
            ExpressionNode ast = col.getAst();
            if (hasCorrelatedExprAtDepth(ast, depth)) {
                col.of(col.getAlias(),
                        rewriteOuterRefs(ast, outer, inner, outerToInnerAlias));
            }
        }
    }

    private void scanWhereForOuterRefEqualities(
            ExpressionNode expr,
            CharSequence outerRefAlias,
            LowerCaseCharSequenceObjHashMap<CharSequence> result
    ) {
        sqlNodeStack.clear();
        ExpressionNode node = expr;
        while (node != null || !sqlNodeStack.isEmpty()) {
            if (node != null && SqlKeywords.isAndKeyword(node.token)) {
                if (node.rhs != null) {
                    sqlNodeStack.push(node.rhs);
                }
                node = node.lhs;
            } else {
                if (node != null
                        && node.type == ExpressionNode.OPERATION
                        && Chars.equals(node.token, "=")) {
                    if (isOuterRefToken(node.lhs, outerRefAlias) && isSimpleColumnRef(node.rhs)) {
                        result.put(node.lhs.token, node.rhs.token);
                    } else if (isOuterRefToken(node.rhs, outerRefAlias) && isSimpleColumnRef(node.lhs)) {
                        result.put(node.rhs.token, node.lhs.token);
                    }
                }
                node = sqlNodeStack.isEmpty() ? null : sqlNodeStack.pop();
            }
        }
    }

    private void setupOuterRefDataSource(
            QueryModel outerRefSubquery,
            QueryModel outerModel,
            ObjList<LowerCaseCharSequenceHashSet> corrCols
    ) throws SqlException {
        int maxIndex = -1;
        int refCount = 0;
        for (int j = 0, m = corrCols.size(); j < m; j++) {
            LowerCaseCharSequenceHashSet colSet = corrCols.getQuick(j);
            if (colSet != null && colSet.size() > 0) {
                maxIndex = j;
                refCount++;
            }
        }
        if (maxIndex < 0) {
            return;
        }

        if (refCount == 1) {
            QueryModel outerJm = outerModel.getJoinModels().getQuick(maxIndex);
            QueryModel outerRefBase = createOuterRefBase(outerJm);
            outerRefSubquery.setNestedModel(outerRefBase);
            return;
        }

        for (int i = 0, n = outerModel.getJoinModels().size(); i <= maxIndex && i < n; i++) {
            QueryModel outerJm = outerModel.getJoinModels().getQuick(i);
            QueryModel outerRefBase = createOuterRefBase(outerJm);
            if (outerRefSubquery.getNestedModel() == null) {
                outerRefSubquery.setNestedModel(outerRefBase);
            } else {
                if (outerJm.getJoinCriteria() != null) {
                    outerRefBase.setJoinType(outerJm.getJoinType());
                    outerRefBase.setJoinCriteria(
                            ExpressionNode.deepClone(expressionNodePool, outerJm.getJoinCriteria())
                    );
                } else {
                    outerRefBase.setJoinType(QueryModel.JOIN_CROSS);
                }
                outerRefSubquery.getJoinModels().add(outerRefBase);
            }
        }
    }

    private void splitWhereForElimination(
            ExpressionNode expr,
            CharSequence outerRefAlias,
            QueryModel outerModel,
            QueryModel inner,
            ObjList<ExpressionNode> result
    ) {
        sqlNodeStack.clear();
        ExpressionNode node = expr;
        while (node != null || !sqlNodeStack.isEmpty()) {
            if (node != null && SqlKeywords.isAndKeyword(node.token)) {
                if (node.rhs != null) {
                    sqlNodeStack.push(node.rhs);
                }
                node = node.lhs;
            } else {
                if (node != null && !isOuterRefEquality(node, outerRefAlias)) {
                    result.add(rewriteOuterRefs(node, outerModel, inner, outerToInnerAlias));
                }
                node = sqlNodeStack.isEmpty() ? null : sqlNodeStack.pop();
            }
        }
    }

    private void terminateHere(
            QueryModel current,
            ObjList<ExpressionNode> groupingCols,
            QueryModel outerRefJoinModel,
            QueryModel outer,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth
    ) throws SqlException {
        current.getJoinModels().add(outerRefJoinModel);
        correlatedPreds.clear();
        extractCorrelatedFromInnerJoins(current, correlatedPreds, depth);
        for (int j = 0, m = correlatedPreds.size(); j < m; j++) {
            ExpressionNode rewritten = rewriteOuterRefs(
                    correlatedPreds.getQuick(j), outer, current, outerToInnerAlias);
            ExpressionNode w = current.getWhereClause();
            current.setWhereClause(w != null ? createBinaryOp("and", w, rewritten) : rewritten);
        }

        if (current.getUnionModel() != null) {
            compensateSetOp(current, outer, groupingCols, outerToInnerAlias, depth);
        }

        compensateInnerJoins(current, outer, outerToInnerAlias, depth);
        deepRewriteOuterRefsInNestedLaterals(current, outer, outerToInnerAlias, depth);
        decorrelateJoinModelSubqueries(current, groupingCols, outer, outerToInnerAlias, depth);
    }

    private void wrapCountColumnsWithCoalesce(
            QueryModel parentModel,
            QueryModel joinModel,
            ObjList<CharSequence> countColAliases
    ) {
        CharSequence joinAlias = joinModel.getAlias() != null
                ? joinModel.getAlias().token : null;
        ObjList<QueryColumn> parentCols = parentModel.getBottomUpColumns();
        if (isWildcard(parentCols)) {
            ObjList<CharSequence> deferred = parentModel.getLateralCountColumns();
            for (int i = 0, n = countColAliases.size(); i < n; i++) {
                deferred.add(countColAliases.getQuick(i));
            }
            return;
        }
        for (int i = 0, n = parentCols.size(); i < n; i++) {
            QueryColumn pc = parentCols.getQuick(i);
            ExpressionNode ast = pc.getAst();
            if (ast == null || ast.type != ExpressionNode.LITERAL) {
                continue;
            }
            CharSequence colRef = ast.token;
            for (int j = 0, m = countColAliases.size(); j < m; j++) {
                CharSequence countAlias = countColAliases.getQuick(j);
                boolean isMatch = false;
                if (joinAlias != null) {
                    int dotPos = Chars.indexOf(colRef, '.');
                    if (dotPos > 0) {
                        CharSequence prefix = colRef.subSequence(0, dotPos);
                        CharSequence suffix = colRef.subSequence(dotPos + 1, colRef.length());
                        if (Chars.equalsIgnoreCase(prefix, joinAlias)
                                && Chars.equalsIgnoreCase(suffix, countAlias)) {
                            isMatch = true;
                        }
                    }
                }
                if (!isMatch && Chars.equalsIgnoreCase(colRef, countAlias)) {
                    isMatch = true;
                }
                if (isMatch) {
                    ExpressionNode coalesce = expressionNodePool.next().of(
                            ExpressionNode.FUNCTION, "coalesce", 0, ast.position
                    );
                    coalesce.paramCount = 2;
                    coalesce.args.clear();
                    coalesce.args.add(expressionNodePool.next().of(
                            ExpressionNode.CONSTANT, "0", 0, ast.position
                    ));
                    coalesce.args.add(ast);
                    pc.of(pc.getAlias(), coalesce);
                    break;
                }
            }
        }
    }
}
