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
    private final IntList outerJmIndexes = new IntList();
    private final StringSink outerRefColSink = new StringSink();
    private final LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias = new LowerCaseCharSequenceObjHashMap<>();
    private final ObjectPool<QueryColumn> queryColumnPool;
    private final ObjectPool<QueryModel> queryModelPool;
    private final ArrayDeque<ExpressionNode> sqlNodeStack;
    private final ArrayDeque<ExpressionNode> sqlNodeStack2;
    private final ObjectPool<WindowExpression> windowExpressionPool;
    private final LowerCaseCharSequenceObjHashMap<CharSequence> wrapperOuterToInnerAlias = new LowerCaseCharSequenceObjHashMap<>();

    LateralJoinRewriter(
            CharacterStore characterStore,
            ObjectPool<ExpressionNode> expressionNodePool,
            ObjectPool<QueryColumn> queryColumnPool,
            ObjectPool<QueryModel> queryModelPool,
            ObjectPool<WindowExpression> windowExpressionPool,
            ArrayDeque<ExpressionNode> sqlNodeStack,
            ArrayDeque<ExpressionNode> sqlNodeStack2,
            FunctionParser functionParser
    ) {
        this.characterStore = characterStore;
        this.expressionNodePool = expressionNodePool;
        this.queryColumnPool = queryColumnPool;
        this.queryModelPool = queryModelPool;
        this.windowExpressionPool = windowExpressionPool;
        this.sqlNodeStack = sqlNodeStack;
        this.sqlNodeStack2 = sqlNodeStack2;
        this.functionParser = functionParser;
    }

    private static boolean hasLimitInChain(QueryModel top, QueryModel content) {
        QueryModel cur = top;
        while (cur != null) {
            if (cur.getLimitHi() != null) {
                return true;
            }
            if (cur == content) {
                break;
            }
            cur = cur.getNestedModel();
        }
        return false;
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
        return cols.size() == 1 && Chars.equals(cols.getQuick(0).getAlias(), "*");
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

    private void addOuterJmIndex(QueryModel outerModel, ExpressionNode outerCol, IntList result) {
        int dotPos = Chars.indexOf(outerCol.token, '.');
        if (dotPos > 0) {
            CharSequence tableAlias = outerCol.token.subSequence(0, dotPos);
            int idx = outerModel.getModelAliasIndex(tableAlias, 0, tableAlias.length());
            if (idx >= 0 && !result.contains(idx)) {
                result.add(idx);
            }
        } else {
            for (int j = 0, jn = outerModel.getJoinModels().size(); j < jn; j++) {
                QueryModel jm = outerModel.getJoinModels().getQuick(j);
                if (jm.getColumnNameToAliasMap().contains(outerCol.token)
                        && !result.contains(j)) {
                    result.add(j);
                    break;
                }
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
                if (jm.getColumnNameToAliasMap().contains(token)) {
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

    private void buildWrapperAliasMap(
            LowerCaseCharSequenceObjHashMap<CharSequence> contentAliasMap,
            QueryModel content,
            LowerCaseCharSequenceObjHashMap<CharSequence> result
    ) {
        result.clear();
        ObjList<CharSequence> keys = contentAliasMap.keys();
        ObjList<QueryColumn> cols = content.getBottomUpColumns();
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence outerToken = keys.getQuick(i);
            CharSequence innerRef = contentAliasMap.get(outerToken);
            boolean found = false;
            for (int j = 0, m = cols.size(); j < m; j++) {
                QueryColumn col = cols.getQuick(j);
                if (col.getAst() != null
                        && Chars.equalsIgnoreCase(col.getAst().token, innerRef)) {
                    result.put(outerToken, col.getAlias());
                    found = true;
                    break;
                }
            }
            if (!found) {
                int refDotPos = Chars.indexOf(innerRef, '.');
                CharSequence stripped = refDotPos > 0
                        ? innerRef.subSequence(refDotPos + 1, innerRef.length())
                        : innerRef;
                for (int j = 0, m = cols.size(); j < m; j++) {
                    if (Chars.equalsIgnoreCase(cols.getQuick(j).getAlias(), stripped)) {
                        result.put(outerToken, cols.getQuick(j).getAlias());
                        break;
                    }
                }
            }
        }
    }

    private boolean canResolveInModel(CharSequence columnName, QueryModel model) {
        for (int i = 0, n = model.getJoinModels().size(); i < n; i++) {
            QueryModel jm = model.getJoinModels().getQuick(i);
            if (jm.getColumnNameToAliasMap().contains(columnName)) {
                return true;
            }
        }
        return false;
    }

    private void collectOuterJoinModelIndexes(
            QueryModel outerModel,
            ObjList<ExpressionNode> outerCols,
            IntList result
    ) {
        for (int i = 0, n = outerCols.size(); i < n; i++) {
            addOuterJmIndex(outerModel, outerCols.getQuick(i), result);
        }
        if (result.size() == 0) {
            result.add(0);
        }
    }

    private void collectOuterRefsFromWrapperChain(
            QueryModel top,
            QueryModel content,
            QueryModel outer,
            LowerCaseCharSequenceObjHashMap<CharSequence> map,
            ObjList<ExpressionNode> extraOuterCols
    ) {
        QueryModel current = top;
        while (current != content) {
            // Use content model for correlation checks: wrapper models
            // may have empty columnNameToAliasMap because enumerateTableColumns
            // does not populate it for pass-through models. Columns in
            // wrapper layers resolve through the content model underneath.
            ObjList<QueryColumn> cols = current.getBottomUpColumns();
            for (int i = 0, n = cols.size(); i < n; i++) {
                collectUnmappedOuterColumnsFromExpr(cols.getQuick(i).getAst(), content, outer, map, extraOuterCols);
            }
            ObjList<ExpressionNode> orderBy = current.getOrderBy();
            for (int i = 0, n = orderBy.size(); i < n; i++) {
                collectUnmappedOuterColumnsFromExpr(orderBy.getQuick(i), content, outer, map, extraOuterCols);
            }
            ObjList<ExpressionNode> groupBy = current.getGroupBy();
            for (int i = 0, n = groupBy.size(); i < n; i++) {
                collectUnmappedOuterColumnsFromExpr(groupBy.getQuick(i), content, outer, map, extraOuterCols);
            }
            if (current.getSampleBy() != null) {
                collectUnmappedOuterColumnsFromExpr(current.getSampleBy(), content, outer, map, extraOuterCols);
            }
            ObjList<ExpressionNode> latestBy = current.getLatestBy();
            for (int i = 0, n = latestBy.size(); i < n; i++) {
                collectUnmappedOuterColumnsFromExpr(latestBy.getQuick(i), content, outer, map, extraOuterCols);
            }
            if (current.getWhereClause() != null) {
                collectUnmappedOuterColumnsFromExpr(
                        current.getWhereClause(), content, outer, map, extraOuterCols
                );
            }
            if (current.getLimitLo() != null) {
                collectUnmappedOuterColumnsFromExpr(
                        current.getLimitLo(), content, outer, map, extraOuterCols
                );
            }
            if (current.getLimitHi() != null) {
                collectUnmappedOuterColumnsFromExpr(
                        current.getLimitHi(), content, outer, map, extraOuterCols
                );
            }
            current = current.getNestedModel();
            if (current == null) {
                break;
            }
        }
    }

    private void collectUnmappedOuterColumnsFromExpr(
            ExpressionNode node,
            QueryModel innerModel,
            QueryModel outerModel,
            LowerCaseCharSequenceObjHashMap<CharSequence> map,
            ObjList<ExpressionNode> result
    ) {
        if (node == null) {
            return;
        }
        if (node.type == ExpressionNode.LITERAL) {
            boolean isOuterRef = hasCorrelatedRef(node, innerModel, outerModel);
            if (isOuterRef && map.get(node.token) == null) {
                boolean dup = false;
                for (int j = 0, m = result.size(); j < m; j++) {
                    if (Chars.equalsIgnoreCase(result.getQuick(j).token, node.token)) {
                        dup = true;
                        break;
                    }
                }
                if (!dup) {
                    result.add(node);
                }
            }
            return;
        }
        if (node.type == ExpressionNode.QUERY && node.queryModel != null) {
            collectUnmappedOuterColumnsFromSubquery(node.queryModel, innerModel, outerModel, map, result);
            return;
        }
        collectUnmappedOuterColumnsFromExpr(node.lhs, innerModel, outerModel, map, result);
        collectUnmappedOuterColumnsFromExpr(node.rhs, innerModel, outerModel, map, result);
        for (int j = 0, m = node.args.size(); j < m; j++) {
            collectUnmappedOuterColumnsFromExpr(node.args.getQuick(j), innerModel, outerModel, map, result);
        }
        if (node.windowExpression != null) {
            ObjList<ExpressionNode> partitionBy = node.windowExpression.getPartitionBy();
            for (int j = 0, m = partitionBy.size(); j < m; j++) {
                collectUnmappedOuterColumnsFromExpr(partitionBy.getQuick(j), innerModel, outerModel, map, result);
            }
            ObjList<ExpressionNode> winOrderBy = node.windowExpression.getOrderBy();
            for (int j = 0, m = winOrderBy.size(); j < m; j++) {
                collectUnmappedOuterColumnsFromExpr(winOrderBy.getQuick(j), innerModel, outerModel, map, result);
            }
        }
    }

    private void collectUnmappedOuterColumnsFromInnerJoinCriteria(
            QueryModel innerModel,
            QueryModel outerModel,
            LowerCaseCharSequenceObjHashMap<CharSequence> map,
            ObjList<ExpressionNode> result
    ) {
        for (int i = 1, n = innerModel.getJoinModels().size(); i < n; i++) {
            QueryModel jm = innerModel.getJoinModels().getQuick(i);
            ExpressionNode joinCriteria = jm.getJoinCriteria();
            if (joinCriteria != null) {
                collectUnmappedOuterColumnsFromExpr(joinCriteria, innerModel, outerModel, map, result);
            }
        }
    }

    private void collectUnmappedOuterColumnsFromNestedLaterals(
            QueryModel innerModel,
            QueryModel outerModel,
            LowerCaseCharSequenceObjHashMap<CharSequence> map,
            ObjList<ExpressionNode> result
    ) {
        for (int i = 1, n = innerModel.getJoinModels().size(); i < n; i++) {
            QueryModel jm = innerModel.getJoinModels().getQuick(i);
            if (isLateralJoin(jm.getJoinType())) {
                QueryModel topNested = jm.getNestedModel();
                if (topNested != null) {
                    QueryModel nestedContent = findContentModel(topNested);
                    collectOuterRefsFromWrapperChain(
                            topNested, nestedContent, outerModel, map, result
                    );
                    collectUnmappedOuterColumnsFromSelectAndClauses(
                            nestedContent, outerModel, map, result
                    );
                    if (nestedContent.getWhereClause() != null) {
                        collectUnmappedOuterColumnsFromExpr(
                                nestedContent.getWhereClause(), nestedContent, outerModel, map, result
                        );
                    }
                    for (int k = 1, kn = nestedContent.getJoinModels().size(); k < kn; k++) {
                        QueryModel innerJm = nestedContent.getJoinModels().getQuick(k);
                        if (!isLateralJoin(innerJm.getJoinType())) {
                            if (innerJm.getJoinCriteria() != null) {
                                collectUnmappedOuterColumnsFromExpr(
                                        innerJm.getJoinCriteria(), nestedContent, outerModel, map, result
                                );
                            }
                        }
                    }
                    collectUnmappedOuterColumnsFromNestedLaterals(
                            nestedContent, outerModel, map, result
                    );
                }
            }
        }
        QueryModel unionBranch = innerModel.getUnionModel();
        while (unionBranch != null) {
            collectUnmappedOuterColumnsFromNestedLaterals(unionBranch, outerModel, map, result);
            unionBranch = unionBranch.getUnionModel();
        }
    }

    private void collectUnmappedOuterColumnsFromSelectAndClauses(
            QueryModel innerModel,
            QueryModel outerModel,
            LowerCaseCharSequenceObjHashMap<CharSequence> map,
            ObjList<ExpressionNode> result
    ) {
        ObjList<QueryColumn> cols = innerModel.getBottomUpColumns();
        for (int i = 0, n = cols.size(); i < n; i++) {
            collectUnmappedOuterColumnsFromExpr(cols.getQuick(i).getAst(), innerModel, outerModel, map, result);
        }
        ObjList<ExpressionNode> orderBy = innerModel.getOrderBy();
        for (int i = 0, n = orderBy.size(); i < n; i++) {
            collectUnmappedOuterColumnsFromExpr(orderBy.getQuick(i), innerModel, outerModel, map, result);
        }
        ObjList<ExpressionNode> groupBy = innerModel.getGroupBy();
        for (int i = 0, n = groupBy.size(); i < n; i++) {
            collectUnmappedOuterColumnsFromExpr(groupBy.getQuick(i), innerModel, outerModel, map, result);
        }
        if (innerModel.getSampleBy() != null) {
            collectUnmappedOuterColumnsFromExpr(innerModel.getSampleBy(), innerModel, outerModel, map, result);
        }
        ObjList<ExpressionNode> latestBy = innerModel.getLatestBy();
        for (int i = 0, n = latestBy.size(); i < n; i++) {
            collectUnmappedOuterColumnsFromExpr(latestBy.getQuick(i), innerModel, outerModel, map, result);
        }
    }

    private void collectUnmappedOuterColumnsFromSubquery(
            QueryModel subquery,
            QueryModel innerModel,
            QueryModel outerModel,
            LowerCaseCharSequenceObjHashMap<CharSequence> map,
            ObjList<ExpressionNode> result
    ) {
        ObjList<QueryColumn> cols = subquery.getBottomUpColumns();
        for (int i = 0, n = cols.size(); i < n; i++) {
            collectUnmappedOuterColumnsFromExpr(cols.getQuick(i).getAst(), innerModel, outerModel, map, result);
        }
        if (subquery.getWhereClause() != null) {
            collectUnmappedOuterColumnsFromExpr(subquery.getWhereClause(), innerModel, outerModel, map, result);
        }
        ObjList<ExpressionNode> orderBy = subquery.getOrderBy();
        for (int i = 0, n = orderBy.size(); i < n; i++) {
            collectUnmappedOuterColumnsFromExpr(orderBy.getQuick(i), innerModel, outerModel, map, result);
        }
        ObjList<ExpressionNode> groupBy = subquery.getGroupBy();
        for (int i = 0, n = groupBy.size(); i < n; i++) {
            collectUnmappedOuterColumnsFromExpr(groupBy.getQuick(i), innerModel, outerModel, map, result);
        }
        if (subquery.getSampleBy() != null) {
            collectUnmappedOuterColumnsFromExpr(subquery.getSampleBy(), innerModel, outerModel, map, result);
        }
        ObjList<ExpressionNode> latestBy = subquery.getLatestBy();
        for (int i = 0, n = latestBy.size(); i < n; i++) {
            collectUnmappedOuterColumnsFromExpr(latestBy.getQuick(i), innerModel, outerModel, map, result);
        }
        for (int i = 1, n = subquery.getJoinModels().size(); i < n; i++) {
            QueryModel jm = subquery.getJoinModels().getQuick(i);
            if (jm.getJoinCriteria() != null) {
                collectUnmappedOuterColumnsFromExpr(jm.getJoinCriteria(), innerModel, outerModel, map, result);
            }
        }
        if (subquery.getNestedModel() != null) {
            collectUnmappedOuterColumnsFromSubquery(subquery.getNestedModel(), innerModel, outerModel, map, result);
        }
        if (subquery.getUnionModel() != null) {
            collectUnmappedOuterColumnsFromSubquery(subquery.getUnionModel(), innerModel, outerModel, map, result);
        }
    }

    private void collectUnmappedOuterColumnsFromUnionBranches(
            QueryModel inner,
            QueryModel outerModel,
            LowerCaseCharSequenceObjHashMap<CharSequence> map,
            ObjList<ExpressionNode> result
    ) {
        QueryModel branch = inner.getUnionModel();
        while (branch != null) {
            collectUnmappedOuterColumnsFromSelectAndClauses(branch, outerModel, map, result);
            if (branch.getWhereClause() != null) {
                collectUnmappedOuterColumnsFromExpr(
                        branch.getWhereClause(), branch, outerModel, map, result
                );
            }
            collectUnmappedOuterColumnsFromInnerJoinCriteria(branch, outerModel, map, result);
            branch = branch.getUnionModel();
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
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias
    ) {
        for (int i = 1, n = inner.getJoinModels().size(); i < n; i++) {
            QueryModel innerJoin = inner.getJoinModels().getQuick(i);
            ExpressionNode joinCriteria = innerJoin.getJoinCriteria();
            if (hasCorrelatedRef(joinCriteria, inner, outer)) {
                innerJoin.setJoinCriteria(
                        rewriteOuterRefs(joinCriteria, outer, inner, outerToInnerAlias)
                );
            }
        }
    }

    private QueryModel compensateInnerOperators(
            QueryModel inner,
            QueryModel topInner,
            QueryModel outer,
            ObjList<ExpressionNode> groupingCols,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            boolean isLeftJoin,
            ObjList<CharSequence> countColAliases
    ) throws SqlException {
        QueryModel effectiveInner = inner;

        boolean hasGroupBy = inner.getGroupBy().size() > 0;
        boolean hasAggregates = hasAggregateFunctions(inner);
        boolean hasWindow = hasWindowColumns(inner);
        boolean hasLimit = hasLimitInChain(topInner, inner);
        boolean hasDistinct = inner.isDistinct();
        boolean hasUnion = inner.getUnionModel() != null;
        boolean hasInnerJoins = inner.getJoinModels().size() > 1;
        boolean hasSampleBy = inner.getSampleBy() != null;
        boolean hasLatestBy = inner.getLatestBy().size() > 0;

        if (hasSampleBy) {
            compensateSampleBy(inner, groupingCols);
        }

        if (hasGroupBy || hasAggregates) {
            compensateAggregate(inner, groupingCols, isLeftJoin, countColAliases);
        }

        if (hasWindow) {
            compensateWindow(inner, groupingCols);
        }

        if (hasDistinct) {
            compensateDistinct(inner, groupingCols);
        }

        if (hasLimit) {
            effectiveInner = compensateLimit(inner, topInner, groupingCols, outer);
        }

        if (hasUnion) {
            compensateSetOp(inner, outer, groupingCols, outerToInnerAlias);
        }

        if (hasInnerJoins) {
            compensateInnerJoins(inner, outer, outerToInnerAlias);
        }

        if (hasLatestBy) {
            compensateLatestBy(inner, groupingCols);
        }

        rewriteSelectExpressions(inner, outer, outerToInnerAlias);

        if (inner.getJoinCriteria() != null) {
            inner.setJoinCriteria(
                    rewriteOuterRefs(inner.getJoinCriteria(), outer, inner, outerToInnerAlias)
            );
        }

        rewriteOrderByExpressions(inner, outer, outerToInnerAlias);
        rewriteGroupByExpressions(inner, outer, outerToInnerAlias);

        if (inner.getSampleBy() != null
                && hasCorrelatedRef(inner.getSampleBy(), inner, outer)) {
            inner.setSampleBy(
                    rewriteOuterRefs(inner.getSampleBy(), outer, inner, outerToInnerAlias));
        }
        ObjList<ExpressionNode> latestBy = inner.getLatestBy();
        for (int li = 0, ln = latestBy.size(); li < ln; li++) {
            ExpressionNode lb = latestBy.getQuick(li);
            if (hasCorrelatedRef(lb, inner, outer)) {
                latestBy.setQuick(li,
                        rewriteOuterRefs(lb, outer, inner, outerToInnerAlias));
            }
        }

        deepRewriteOuterRefsInNestedLaterals(inner, outer, outerToInnerAlias);

        return effectiveInner;
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
            QueryModel inner,
            QueryModel topInner,
            ObjList<ExpressionNode> groupingCols,
            QueryModel outer
    ) throws SqlException {
        ExpressionNode limitHi = null;
        ExpressionNode limitLo = null;
        QueryModel limitModel = null;
        QueryModel cur = topInner;
        while (cur != null) {
            if (cur.getLimitHi() != null) {
                limitHi = cur.getLimitHi();
                limitLo = cur.getLimitLo();
                limitModel = cur;
                break;
            }
            if (cur == inner) {
                break;
            }
            cur = cur.getNestedModel();
        }

        if (limitHi == null) {
            return inner;
        }

        if (hasCorrelatedRef(limitHi, inner, outer)) {
            throw SqlException.position(limitHi.position)
                    .put("non-constant LIMIT in LATERAL join is not supported");
        }
        if (hasCorrelatedRef(limitLo, inner, outer)) {
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
        cur = topInner;
        while (cur != null) {
            if (cur.getOrderBy().size() > 0) {
                orderByModel = cur;
                for (int i = 0, n = cur.getOrderBy().size(); i < n; i++) {
                    ExpressionNode ob = cur.getOrderBy().getQuick(i);
                    if (cur != inner) {
                        ob = resolveExprThroughWrappers(ob, cur, inner);
                    }
                    orderBySave.add(ob);
                    orderByDirSave.add(cur.getOrderByDirection().getQuick(i));
                }
                break;
            }
            if (cur == inner) {
                break;
            }
            cur = cur.getNestedModel();
        }

        CharSequence rnAlias = createColumnAlias("_lateral_rn", inner);
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

        inner.addBottomUpColumn(rnWindowExpr);
        // Clear LIMIT and ORDER BY from their original locations
        limitModel.setLimit(null, null);
        if (orderByModel != null) {
            orderByModel.getOrderBy().clear();
            orderByModel.getOrderByDirection().clear();
        }
        // Also clear from inner in case they were on inner directly
        inner.setLimit(null, null);
        inner.getOrderBy().clear();
        inner.getOrderByDirection().clear();

        QueryModel wrapper = queryModelPool.next();
        wrapper.setNestedModel(inner);
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

        copyColumnsExcept(inner, wrapper, rnAlias);
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
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias
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
            extractCorrelatedPredicates(current.getWhereClause(), current, outer, branchCorrelated, branchNonCorrelated);

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

            rewriteSelectExpressions(current, outer, outerToInnerAlias);
            rewriteOrderByExpressions(current, outer, outerToInnerAlias);
            rewriteGroupByExpressions(current, outer, outerToInnerAlias);
            if (current.getSampleBy() != null
                    && hasCorrelatedRef(current.getSampleBy(), current, outer)) {
                current.setSampleBy(
                        rewriteOuterRefs(current.getSampleBy(), outer, current, outerToInnerAlias));
            }
            ObjList<ExpressionNode> branchLatestBy = current.getLatestBy();
            for (int j = 0, m = branchLatestBy.size(); j < m; j++) {
                ExpressionNode lb = branchLatestBy.getQuick(j);
                if (hasCorrelatedRef(lb, current, outer)) {
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

            if (current.getBottomUpColumns().size() != baseColumnCount) {
                throw SqlException.position(current.getModelPosition())
                        .put("set operation branches must have the same number of columns after decorrelation");
            }

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

    private void deepRewriteOuterRefsInNestedLaterals(
            QueryModel innerModel,
            QueryModel outerModel,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias
    ) {
        for (int i = 1, n = innerModel.getJoinModels().size(); i < n; i++) {
            QueryModel jm = innerModel.getJoinModels().getQuick(i);
            if (isLateralJoin(jm.getJoinType())) {
                QueryModel nestedLateral = jm.getNestedModel();
                if (nestedLateral != null) {
                    rewriteSelectExpressions(nestedLateral, outerModel, outerToInnerAlias);
                    rewriteOrderByExpressions(nestedLateral, outerModel, outerToInnerAlias);
                    rewriteGroupByExpressions(nestedLateral, outerModel, outerToInnerAlias);

                    if (nestedLateral.getWhereClause() != null
                            && hasCorrelatedRef(nestedLateral.getWhereClause(), nestedLateral, outerModel)) {
                        nestedLateral.setWhereClause(
                                rewriteOuterRefs(nestedLateral.getWhereClause(), outerModel, nestedLateral, outerToInnerAlias)
                        );
                    }

                    if (nestedLateral.getLimitLo() != null
                            && hasCorrelatedRef(nestedLateral.getLimitLo(), nestedLateral, outerModel)) {
                        nestedLateral.setLimit(
                                rewriteOuterRefs(nestedLateral.getLimitLo(), outerModel, nestedLateral, outerToInnerAlias),
                                nestedLateral.getLimitHi()
                        );
                    }
                    if (nestedLateral.getLimitHi() != null
                            && hasCorrelatedRef(nestedLateral.getLimitHi(), nestedLateral, outerModel)) {
                        nestedLateral.setLimit(
                                nestedLateral.getLimitLo(),
                                rewriteOuterRefs(nestedLateral.getLimitHi(), outerModel, nestedLateral, outerToInnerAlias)
                        );
                    }

                    if (nestedLateral.getJoinCriteria() != null
                            && hasCorrelatedRef(nestedLateral.getJoinCriteria(), nestedLateral, outerModel)) {
                        nestedLateral.setJoinCriteria(
                                rewriteOuterRefs(nestedLateral.getJoinCriteria(), outerModel, nestedLateral, outerToInnerAlias)
                        );
                    }

                    if (nestedLateral.getSampleBy() != null
                            && hasCorrelatedRef(nestedLateral.getSampleBy(), nestedLateral, outerModel)) {
                        nestedLateral.setSampleBy(
                                rewriteOuterRefs(nestedLateral.getSampleBy(), outerModel, nestedLateral, outerToInnerAlias)
                        );
                    }
                    ObjList<ExpressionNode> latestBy = nestedLateral.getLatestBy();
                    for (int j = 0, m = latestBy.size(); j < m; j++) {
                        ExpressionNode lb = latestBy.getQuick(j);
                        if (hasCorrelatedRef(lb, nestedLateral, outerModel)) {
                            latestBy.setQuick(j,
                                    rewriteOuterRefs(lb, outerModel, nestedLateral, outerToInnerAlias));
                        }
                    }

                    for (int k = 1, kn = nestedLateral.getJoinModels().size(); k < kn; k++) {
                        QueryModel innerJm = nestedLateral.getJoinModels().getQuick(k);
                        if (!isLateralJoin(innerJm.getJoinType())) {
                            if (innerJm.getJoinCriteria() != null
                                    && hasCorrelatedRef(innerJm.getJoinCriteria(), nestedLateral, outerModel)) {
                                innerJm.setJoinCriteria(
                                        rewriteOuterRefs(innerJm.getJoinCriteria(), outerModel, nestedLateral, outerToInnerAlias));
                            }
                        }
                    }

                    deepRewriteOuterRefsInNestedLaterals(nestedLateral, outerModel, outerToInnerAlias);
                }
            }
        }
        QueryModel unionBranch = innerModel.getUnionModel();
        while (unionBranch != null) {
            deepRewriteOuterRefsInNestedLaterals(unionBranch, outerModel, outerToInnerAlias);
            unionBranch = unionBranch.getUnionModel();
        }
    }

    private void eliminateOuterRef(
            QueryModel inner,
            QueryModel selectModel,
            QueryModel outerModel,
            QueryModel joinModel
    ) throws SqlException {
        int outerRefJmIndex = -1;
        CharSequence outerRefAlias = null;
        for (int j = 1, jn = inner.getJoinModels().size(); j < jn; j++) {
            QueryModel jm = inner.getJoinModels().getQuick(j);
            if (jm.getAlias() != null
                    && jm.getJoinType() == QueryModel.JOIN_CROSS
                    && Chars.startsWith(jm.getAlias().token, "__outer_ref")) {
                outerRefJmIndex = j;
                outerRefAlias = jm.getAlias().token;
                break;
            }
        }
        if (outerRefJmIndex < 0) {
            return;
        }

        QueryModel outerRefSubquery = inner.getJoinModels().getQuick(outerRefJmIndex).getNestedModel();

        // Scan WHERE for equalities of the form: simple_col = __outer_ref.xxx
        outerToInnerAlias.clear();
        ExpressionNode where = inner.getWhereClause();
        if (where == null) {
            return;
        }
        scanWhereForOuterRefEqualities(where, outerRefAlias, outerToInnerAlias);

        // Check if every __outer_ref column has a matching equality
        ObjList<QueryColumn> outerRefCols = outerRefSubquery.getBottomUpColumns();
        for (int j = 0, m = outerRefCols.size(); j < m; j++) {
            CharSequence colAlias = outerRefCols.getQuick(j).getAlias();
            outerRefColSink.clear();
            outerRefColSink.put(outerRefAlias).put('.').put(colAlias);
            if (outerToInnerAlias.get(outerRefColSink) == null) {
                return;
            }
        }

        // All covered — elimination is possible
        // Remove/rewrite __outer_ref columns from selectModel (where they were added
        // by ensureColumnInSelect in materializeOuterRefs)
        ObjList<QueryColumn> cols = selectModel.getBottomUpColumns();
        for (int j = cols.size() - 1; j >= 0; j--) {
            QueryColumn col = cols.getQuick(j);
            ExpressionNode ast = col.getAst();
            if (ast != null
                    && ast.type == ExpressionNode.LITERAL
                    && Chars.startsWith(ast.token, outerRefAlias)
                    && ast.token.length() > outerRefAlias.length()
                    && ast.token.charAt(outerRefAlias.length()) == '.') {
                selectModel.removeColumn(j);
            } else {
                ExpressionNode rewritten = rewriteOuterRefs(ast, outerModel, inner, outerToInnerAlias);
                if (rewritten != ast) {
                    col.of(col.getAlias(), rewritten);
                }
            }
        }

        nonCorrelatedPreds.clear();
        splitWhereForElimination(where, outerRefAlias, outerModel, inner, nonCorrelatedPreds);
        inner.setWhereClause(conjoin(nonCorrelatedPreds));

        ObjList<ExpressionNode> orderBy = inner.getOrderBy();
        for (int j = 0, m = orderBy.size(); j < m; j++) {
            orderBy.setQuick(j,
                    rewriteOuterRefs(orderBy.getQuick(j), outerModel, inner, outerToInnerAlias));
        }
        ObjList<ExpressionNode> groupBy = inner.getGroupBy();
        for (int j = 0, m = groupBy.size(); j < m; j++) {
            groupBy.setQuick(j,
                    rewriteOuterRefs(groupBy.getQuick(j), outerModel, inner, outerToInnerAlias));
        }
        if (inner.getSampleBy() != null) {
            inner.setSampleBy(
                    rewriteOuterRefs(inner.getSampleBy(), outerModel, inner, outerToInnerAlias));
        }
        ObjList<ExpressionNode> latestBy = inner.getLatestBy();
        for (int j = 0, m = latestBy.size(); j < m; j++) {
            latestBy.setQuick(j,
                    rewriteOuterRefs(latestBy.getQuick(j), outerModel, inner, outerToInnerAlias));
        }

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
                ensureColumnInSelect(selectModel, innerNode, innerCol);
                ExpressionNode outerNode = ExpressionNode.deepClone(expressionNodePool, refCol.getAst());
                ExpressionNode eq = createBinaryOp("=", innerNode, outerNode);
                newCriteria = newCriteria == null ? eq : createBinaryOp("and", newCriteria, eq);
            }
        }
        joinModel.setJoinCriteria(newCriteria);
        inner.getJoinModels().remove(outerRefJmIndex);
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
            QueryModel outer,
            ObjList<ExpressionNode> correlated
    ) {
        for (int i = 1, n = inner.getJoinModels().size(); i < n; i++) {
            QueryModel jm = inner.getJoinModels().getQuick(i);
            ExpressionNode joinCriteria = jm.getJoinCriteria();
            if (joinCriteria != null && jm.getJoinType() == QueryModel.JOIN_INNER) {
                innerJoinCorrelated.clear();
                innerJoinNonCorrelated.clear();
                extractCorrelatedPredicates(joinCriteria, inner, outer, innerJoinCorrelated, innerJoinNonCorrelated);

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
            QueryModel inner,
            QueryModel outer,
            ObjList<ExpressionNode> correlated,
            ObjList<ExpressionNode> nonCorrelated
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
                    if (hasCorrelatedRef(node, inner, outer)) {
                        correlated.add(node);
                    } else {
                        nonCorrelated.add(node);
                    }
                }
                node = sqlNodeStack.isEmpty() ? null : sqlNodeStack.pop();
            }
        }
    }

    private QueryModel findContentModel(QueryModel model) {
        QueryModel current = model;
        while (current.getNestedModel() != null
                && current.getTableNameExpr() == null
                && current.getJoinModels().size() <= 1) {
            current = current.getNestedModel();
        }
        return current;
    }

    private QueryModel findWrapperParent(QueryModel top, QueryModel target) {
        QueryModel current = top;
        while (current != null && current.getNestedModel() != target) {
            current = current.getNestedModel();
        }
        return current;
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

    private boolean hasCorrelatedModelRefs(QueryModel inner, QueryModel outer) {
        return hasCorrelatedModelRefs(inner, inner, outer);
    }

    private boolean hasCorrelatedModelRefs(QueryModel inner, QueryModel resolveModel, QueryModel outer) {
        ObjList<QueryColumn> cols = inner.getBottomUpColumns();
        for (int i = 0, n = cols.size(); i < n; i++) {
            if (hasCorrelatedRef(cols.getQuick(i).getAst(), resolveModel, outer)) {
                return true;
            }
        }
        ObjList<ExpressionNode> orderBy = inner.getOrderBy();
        for (int i = 0, n = orderBy.size(); i < n; i++) {
            if (hasCorrelatedRef(orderBy.getQuick(i), resolveModel, outer)) {
                return true;
            }
        }
        if (inner.getLimitHi() != null && hasCorrelatedRef(inner.getLimitHi(), resolveModel, outer)) {
            return true;
        }
        if (inner.getLimitLo() != null && hasCorrelatedRef(inner.getLimitLo(), resolveModel, outer)) {
            return true;
        }
        ObjList<ExpressionNode> groupBy = inner.getGroupBy();
        for (int i = 0, n = groupBy.size(); i < n; i++) {
            if (hasCorrelatedRef(groupBy.getQuick(i), resolveModel, outer)) {
                return true;
            }
        }
        if (inner.getSampleBy() != null && hasCorrelatedRef(inner.getSampleBy(), resolveModel, outer)) {
            return true;
        }
        ObjList<ExpressionNode> latestBy = inner.getLatestBy();
        for (int i = 0, n = latestBy.size(); i < n; i++) {
            if (hasCorrelatedRef(latestBy.getQuick(i), resolveModel, outer)) {
                return true;
            }
        }
        for (int i = 1, n = inner.getJoinModels().size(); i < n; i++) {
            QueryModel jm = inner.getJoinModels().getQuick(i);
            if (jm.getJoinCriteria() != null
                    && hasCorrelatedRef(jm.getJoinCriteria(), resolveModel, outer)) {
                return true;
            }
            if (jm.getWhereClause() != null
                    && hasCorrelatedRef(jm.getWhereClause(), resolveModel, outer)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasCorrelatedRef(
            ExpressionNode node,
            QueryModel innerModel,
            QueryModel outerModel
    ) {
        if (node == null) {
            return false;
        }
        sqlNodeStack2.clear();
        sqlNodeStack2.push(node);
        while (!sqlNodeStack2.isEmpty()) {
            ExpressionNode current = sqlNodeStack2.pop();
            if (current.type == ExpressionNode.LITERAL) {
                int dotPos = Chars.indexOf(current.token, '.');
                if (dotPos > 0) {
                    CharSequence tableAlias = current.token.subSequence(0, dotPos);
                    if (outerModel.getModelAliasIndex(tableAlias, 0, tableAlias.length()) >= 0
                            && innerModel.getModelAliasIndex(tableAlias, 0, tableAlias.length()) < 0) {
                        return true;
                    }
                } else if (!canResolveInModel(current.token, innerModel)
                        && canResolveInModel(current.token, outerModel)) {
                    return true;
                }
                continue;
            }
            if (current.type == ExpressionNode.QUERY) {
                if (hasCorrelatedRefInSubquery(current.queryModel, innerModel, outerModel)) {
                    return true;
                }
                continue;
            }
            if (current.lhs != null) {
                sqlNodeStack2.push(current.lhs);
            }
            if (current.rhs != null) {
                sqlNodeStack2.push(current.rhs);
            }
            for (int i = 0, n = current.args.size(); i < n; i++) {
                sqlNodeStack2.push(current.args.getQuick(i));
            }
            if (current.windowExpression != null) {
                ObjList<ExpressionNode> partitionBy = current.windowExpression.getPartitionBy();
                for (int i = 0, n = partitionBy.size(); i < n; i++) {
                    sqlNodeStack2.push(partitionBy.getQuick(i));
                }
                ObjList<ExpressionNode> winOrderBy = current.windowExpression.getOrderBy();
                for (int i = 0, n = winOrderBy.size(); i < n; i++) {
                    sqlNodeStack2.push(winOrderBy.getQuick(i));
                }
            }
        }
        return false;
    }

    private boolean hasCorrelatedRefInSubquery(
            QueryModel subquery,
            QueryModel innerModel,
            QueryModel outerModel
    ) {
        QueryModel current = subquery;
        while (current != null) {
            ObjList<QueryColumn> cols = current.getBottomUpColumns();
            for (int i = 0, n = cols.size(); i < n; i++) {
                if (hasCorrelatedRef(cols.getQuick(i).getAst(), innerModel, outerModel)) {
                    return true;
                }
            }
            if (hasCorrelatedRef(current.getWhereClause(), innerModel, outerModel)) {
                return true;
            }
            ObjList<ExpressionNode> orderBy = current.getOrderBy();
            for (int i = 0, n = orderBy.size(); i < n; i++) {
                if (hasCorrelatedRef(orderBy.getQuick(i), innerModel, outerModel)) {
                    return true;
                }
            }
            ObjList<ExpressionNode> groupBy = current.getGroupBy();
            for (int i = 0, n = groupBy.size(); i < n; i++) {
                if (hasCorrelatedRef(groupBy.getQuick(i), innerModel, outerModel)) {
                    return true;
                }
            }
            if (hasCorrelatedRef(current.getSampleBy(), innerModel, outerModel)) {
                return true;
            }
            ObjList<ExpressionNode> latestBy = current.getLatestBy();
            for (int i = 0, n = latestBy.size(); i < n; i++) {
                if (hasCorrelatedRef(latestBy.getQuick(i), innerModel, outerModel)) {
                    return true;
                }
            }
            if (hasCorrelatedRef(current.getLimitLo(), innerModel, outerModel)) {
                return true;
            }
            if (hasCorrelatedRef(current.getLimitHi(), innerModel, outerModel)) {
                return true;
            }
            for (int i = 1, n = current.getJoinModels().size(); i < n; i++) {
                QueryModel jm = current.getJoinModels().getQuick(i);
                if (jm.getJoinCriteria() != null
                        && hasCorrelatedRef(jm.getJoinCriteria(), innerModel, outerModel)) {
                    return true;
                }
            }
            // Check union branches before descending into nested model
            if (current.getUnionModel() != null
                    && hasCorrelatedRefInSubquery(current.getUnionModel(), innerModel, outerModel)) {
                return true;
            }
            current = current.getNestedModel();
        }
        return false;
    }

    private boolean hasCorrelatedRefsInWrapperChain(QueryModel top, QueryModel content, QueryModel outer) {
        QueryModel current = top;
        while (current != content) {
            if (isCorrelatedWithOuter(current, content, outer)) {
                return true;
            }
            current = current.getNestedModel();
            if (current == null) {
                break;
            }
        }
        return false;
    }

    // Checks whether nested lateral joins within innerModel contain correlated
    // references to outerModel. Only lateral joins are checked because non-lateral
    // subqueries form a scope boundary — their content cannot reference outer tables.
    //
    // Example: SELECT * FROM o LATERAL JOIN (
    //            SELECT * FROM t1
    //            LATERAL JOIN (SELECT * FROM t2 WHERE t2.x = o.id) sub2
    //          ) sub
    // innerModel = content of sub (has t1 + lateral join sub2)
    // outerModel = o
    // → finds o.id in sub2's WHERE → returns true
    private boolean hasDeepCorrelatedRefs(QueryModel innerModel, QueryModel outerModel) {
        for (int i = 1, n = innerModel.getJoinModels().size(); i < n; i++) {
            QueryModel jm = innerModel.getJoinModels().getQuick(i);
            if (isLateralJoin(jm.getJoinType())) {
                QueryModel topNested = jm.getNestedModel();
                assert topNested != null;
                QueryModel nestedContent = findContentModel(topNested);
                // Check wrapper layers (SELECT/ORDER BY/LIMIT that wrap the content)
                if (hasCorrelatedRefsInWrapperChain(topNested, nestedContent, outerModel)) {
                    return true;
                }
                // Check content model's SELECT/ORDER BY/GROUP BY/join criteria/etc.
                if (hasCorrelatedModelRefs(nestedContent, outerModel)) {
                    return true;
                }
                // Check content model's WHERE
                if (nestedContent.getWhereClause() != null
                        && hasCorrelatedRef(nestedContent.getWhereClause(), nestedContent, outerModel)) {
                    return true;
                }
                // Recurse: content model may itself contain nested lateral joins
                if (hasDeepCorrelatedRefs(nestedContent, outerModel)) {
                    return true;
                }
            }
        }
        // Union branches may also contain nested lateral joins referencing outerModel
        QueryModel unionBranch = innerModel.getUnionModel();
        while (unionBranch != null) {
            if (hasDeepCorrelatedRefs(unionBranch, outerModel)) {
                return true;
            }
            unionBranch = unionBranch.getUnionModel();
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

    /**
     * Checks whether expressions in {@code model} contain correlated references
     * to {@code outer}, using {@code resolveModel} for column resolution. This
     * overload exists for wrapper chain models whose columnNameToAliasMap may
     * be empty — pass the content model as {@code resolveModel} so that columns
     * resolvable through the wrapper chain are not falsely classified as outer refs.
     */
    private boolean isCorrelatedWithOuter(QueryModel model, QueryModel resolveModel, QueryModel outer) {
        if (model.getWhereClause() != null
                && hasCorrelatedRef(model.getWhereClause(), resolveModel, outer)) {
            return true;
        }
        return hasCorrelatedModelRefs(model, resolveModel, outer);
    }

    private void materializeOuterRefs(
            QueryModel inner,
            QueryModel selectModel,
            QueryModel outerModel,
            QueryModel joinModel,
            ObjList<ExpressionNode> outerCols,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias
    ) throws SqlException {
        QueryModel outerRefSubquery = queryModelPool.next();
        outerRefSubquery.setDistinct(true);
        CharSequence outerRefAlias = createColumnAlias("__outer_ref", inner);

        for (int i = 0, n = outerCols.size(); i < n; i++) {
            addColumnToOuterRefSelect(outerRefSubquery, outerCols.getQuick(i));
        }

        setupOuterRefDataSource(outerRefSubquery, outerModel, outerCols);
        QueryModel outerRefJoinModel = queryModelPool.next();
        outerRefJoinModel.setJoinType(QueryModel.JOIN_CROSS);
        outerRefJoinModel.setNestedModel(outerRefSubquery);
        outerRefJoinModel.setNestedModelIsSubQuery(true);
        ExpressionNode outerRefAliasExpr = expressionNodePool.next().of(
                ExpressionNode.LITERAL, outerRefAlias, 0, 0
        );
        outerRefJoinModel.setAlias(outerRefAliasExpr);
        inner.getJoinModels().add(outerRefJoinModel);

        ObjList<QueryColumn> outerRefCols = outerRefSubquery.getBottomUpColumns();
        for (int k = 0, kn = outerRefCols.size(); k < kn; k++) {
            QueryColumn dc = outerRefCols.getQuick(k);
            outerRefJoinModel.getColumnNameToAliasMap().put(dc.getAlias(), dc.getAlias());
        }

        ExpressionNode joinCriteria = null;
        for (int i = 0, n = outerCols.size(); i < n; i++) {
            ExpressionNode outerCol = outerCols.getQuick(i);
            CharSequence outerRefColAlias = getOuterRefColumnAlias(outerRefAlias, outerCol, outerRefSubquery);
            outerToInnerAlias.put(outerCol.token, outerRefColAlias);
            addQualifiedAliasVariants(outerCol.token, outerRefColAlias, outerModel, outerToInnerAlias);
            ExpressionNode outerRefNode = expressionNodePool.next().of(
                    ExpressionNode.LITERAL, outerRefColAlias, 0, 0
            );
            CharSequence selectAlias = ensureColumnInSelect(selectModel, outerRefNode, outerRefColAlias);
            // Build ON clause like the parser does: selectAlias = outerCol
            // optimiseJoins reads joinCriteria to build JoinContext and orderedJoinModels
            ExpressionNode innerRef = expressionNodePool.next().of(
                    ExpressionNode.LITERAL, selectAlias, 0, 0
            );
            ExpressionNode outerRef = ExpressionNode.deepClone(expressionNodePool, outerCol);
            ExpressionNode eq = createBinaryOp("=", innerRef, outerRef);
            joinCriteria = joinCriteria == null ? eq : createBinaryOp("and", joinCriteria, eq);
        }
        joinModel.setJoinCriteria(joinCriteria);
    }

    private void propagateContentColumns(QueryModel top, QueryModel content) throws SqlException {
        ObjList<QueryColumn> contentCols = content.getBottomUpColumns();
        int n = contentCols.size();
        if (n == 0) {
            return;
        }
        QueryModel current = top;
        while (current != content) {
            ObjList<QueryColumn> wrapperCols = current.getBottomUpColumns();
            if (wrapperCols.size() > 0 && !isWildcard(wrapperCols)) {
                boolean hasGroupBy = current.getGroupBy().size() > 0;
                for (int i = 0; i < n; i++) {
                    CharSequence alias = contentCols.getQuick(i).getAlias();
                    ExpressionNode ref = expressionNodePool.next().of(
                            ExpressionNode.LITERAL, alias, 0, 0
                    );
                    current.addBottomUpColumn(queryColumnPool.next().of(alias, ref));
                    if (hasGroupBy) {
                        current.addGroupBy(
                                expressionNodePool.next().of(ExpressionNode.LITERAL, alias, 0, 0)
                        );
                    }
                }
            }
            current = current.getNestedModel();
        }
    }

    private ExpressionNode resolveExprThroughWrappers(
            ExpressionNode expr,
            QueryModel from,
            QueryModel content
    ) {
        if (expr == null || expr.type != ExpressionNode.LITERAL) {
            return expr;
        }
        QueryModel cur = from.getNestedModel();
        while (cur != null) {
            ObjList<QueryColumn> cols = cur.getBottomUpColumns();
            for (int i = 0, n = cols.size(); i < n; i++) {
                QueryColumn col = cols.getQuick(i);
                if (Chars.equalsIgnoreCase(col.getAlias(), expr.token)
                        && col.getAst() != null
                        && !Chars.equalsIgnoreCase(col.getAst().token, expr.token)) {
                    return resolveExprThroughWrappers(
                            ExpressionNode.deepClone(expressionNodePool, col.getAst()),
                            cur,
                            content
                    );
                }
            }
            if (cur == content) {
                break;
            }
            cur = cur.getNestedModel();
        }
        return expr;
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
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias
    ) {
        ObjList<ExpressionNode> groupBy = inner.getGroupBy();
        for (int i = 0, n = groupBy.size(); i < n; i++) {
            ExpressionNode expr = groupBy.getQuick(i);
            if (hasCorrelatedRef(expr, inner, outer)) {
                groupBy.setQuick(i,
                        rewriteOuterRefs(expr, outer, inner, outerToInnerAlias));
            }
        }
    }

    private void rewriteOrderByExpressions(
            QueryModel inner,
            QueryModel outer,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias
    ) {
        ObjList<ExpressionNode> orderBy = inner.getOrderBy();
        for (int i = 0, n = orderBy.size(); i < n; i++) {
            ExpressionNode expr = orderBy.getQuick(i);
            if (hasCorrelatedRef(expr, inner, outer)) {
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
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias
    ) {
        for (int i = 0, n = inner.getBottomUpColumns().size(); i < n; i++) {
            QueryColumn col = inner.getBottomUpColumns().getQuick(i);
            ExpressionNode ast = col.getAst();
            if (hasCorrelatedRef(ast, inner, outer)) {
                col.of(col.getAlias(),
                        rewriteOuterRefs(ast, outer, inner, outerToInnerAlias));
            }
        }
    }

    private void rewriteWrapperChain(
            QueryModel top,
            QueryModel content,
            QueryModel outer,
            LowerCaseCharSequenceObjHashMap<CharSequence> aliasMap
    ) {
        QueryModel current = top;
        while (current != content) {
            rewriteSelectExpressions(current, outer, aliasMap);
            rewriteOrderByExpressions(current, outer, aliasMap);
            rewriteGroupByExpressions(current, outer, aliasMap);
            if (current.getWhereClause() != null
                    && hasCorrelatedRef(current.getWhereClause(), current, outer)) {
                current.setWhereClause(
                        rewriteOuterRefs(current.getWhereClause(), outer, current, aliasMap));
            }
            if (current.getLimitLo() != null
                    && hasCorrelatedRef(current.getLimitLo(), current, outer)) {
                current.setLimit(
                        rewriteOuterRefs(current.getLimitLo(), outer, current, aliasMap),
                        current.getLimitHi());
            }
            if (current.getLimitHi() != null
                    && hasCorrelatedRef(current.getLimitHi(), current, outer)) {
                current.setLimit(
                        current.getLimitLo(),
                        rewriteOuterRefs(current.getLimitHi(), outer, current, aliasMap));
            }
            if (current.getSampleBy() != null
                    && hasCorrelatedRef(current.getSampleBy(), current, outer)) {
                current.setSampleBy(
                        rewriteOuterRefs(current.getSampleBy(), outer, current, aliasMap));
            }
            ObjList<ExpressionNode> latestBy = current.getLatestBy();
            for (int j = 0, m = latestBy.size(); j < m; j++) {
                ExpressionNode lb = latestBy.getQuick(j);
                if (hasCorrelatedRef(lb, current, outer)) {
                    latestBy.setQuick(j,
                            rewriteOuterRefs(lb, outer, current, aliasMap));
                }
            }
            current = current.getNestedModel();
            if (current == null) {
                break;
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
            ObjList<ExpressionNode> outerCols
    ) throws SqlException {
        outerJmIndexes.clear();
        collectOuterJoinModelIndexes(outerModel, outerCols, outerJmIndexes);

        if (outerJmIndexes.size() <= 1) {
            QueryModel outerJm = outerModel.getJoinModels().getQuick(outerJmIndexes.getQuick(0));
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
            outerRefSubquery.setNestedModel(outerRefBase);
            return;
        }

        // Multiple outer tables — include all models from 0 to max(outerJmIndexes)
        // to preserve the join chain between them. Example:
        //   FROM orders o JOIN customers c ON c.id = o.customer_id
        //   outerJmIndexes = [0, 1] → include both with original JOIN condition
        //   Result: (SELECT DISTINCT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id)
        int maxIndex = 0;
        for (int i = 0, n = outerJmIndexes.size(); i < n; i++) {
            maxIndex = Math.max(maxIndex, outerJmIndexes.getQuick(i));
        }

        // If any lateral joins appear in the range and must be skipped, subsequent
        // join criteria may reference the skipped model. Fall back to CROSS JOIN
        // for safety in that case.
        boolean hasLateralInRange = false;
        for (int i = 1, n = outerModel.getJoinModels().size(); i <= maxIndex && i < n; i++) {
            if (isLateralJoin(outerModel.getJoinModels().getQuick(i).getJoinType())) {
                hasLateralInRange = true;
                break;
            }
        }

        for (int i = 0, n = outerModel.getJoinModels().size(); i <= maxIndex && i < n; i++) {
            QueryModel outerJm = outerModel.getJoinModels().getQuick(i);
            if (i > 0 && isLateralJoin(outerJm.getJoinType())) {
                continue;
            }
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
            if (outerRefSubquery.getNestedModel() == null) {
                outerRefSubquery.setNestedModel(outerRefBase);
            } else {
                if (!hasLateralInRange && outerJm.getJoinCriteria() != null) {
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

    void rewrite(QueryModel model) throws SqlException {
        if (model.getNestedModel() != null) {
            rewrite(model.getNestedModel());
        }
        if (model.getUnionModel() != null) {
            rewrite(model.getUnionModel());
        }

        for (int i = 1, n = model.getJoinModels().size(); i < n; i++) {
            QueryModel joinModel = model.getJoinModels().getQuick(i);
            if (QueryModel.isLateralJoin(joinModel.getJoinType())) {
                QueryModel topInner = joinModel.getNestedModel();
                assert topInner != null;
                // Descends through pass-through wrapper layers (SELECT/ORDER BY/LIMIT projections
                // with no table and no joins) to find the content model that owns the actual data
                // sources. Decorrelation operates on the content model because that is where
                // __outer_ref must be cross-joined and WHERE predicates must be rewritten.
                // Example: SELECT * FROM (SELECT * FROM t1 JOIN t2 ON ... WHERE ...)
                //   wrapper: outer SELECT *
                //   content: inner SELECT * FROM t1 JOIN t2 ...
                QueryModel inner = findContentModel(topInner);
                // When inner is a flat table model (no bottomUpColumns, has tableNameExpr),
                // columns must be added to the parent SELECT model instead. Adding columns
                // to a flat table model breaks rewriteSelectClause which asserts nestedModel != null
                // for non-flat models. When inner already has columns (topInner == inner), it is
                // safe to add directly.
                QueryModel selectModel = (inner != topInner
                        && inner.getBottomUpColumns().size() == 0
                        && inner.getTableNameExpr() != null)
                        ? findWrapperParent(topInner, inner) : inner;
                if (selectModel == null) {
                    selectModel = inner;
                }
                if (QueryModel.isWindowJoin(inner)) {
                    throw SqlException.position(joinModel.getModelPosition())
                            .put("LATERAL decorrelation with WINDOW JOIN is not supported");
                }
                if (QueryModel.isHorizonJoin(inner)) {
                    throw SqlException.position(joinModel.getModelPosition())
                            .put("LATERAL decorrelation with HORIZON JOIN is not supported");
                }

                boolean isLeft = joinModel.getJoinType() == QueryModel.JOIN_LATERAL_LEFT;
                correlatedPreds.clear();
                nonCorrelatedPreds.clear();

                // Split WHERE into correlated (references outer) and non-correlated predicates.
                // Correlated predicates are separated so they can be materialized into
                // __outer_ref and rewritten; non-correlated predicates stay in WHERE as-is.
                // Example: WHERE li.order_id = o.id AND li.qty > o.min_qty AND li.status = 'active'
                //   correlated:    [li.order_id = o.id, li.qty > o.min_qty]
                //   nonCorrelated: [li.status = 'active']
                extractCorrelatedPredicates(inner.getWhereClause(), inner, model, correlatedPreds, nonCorrelatedPreds);

                // Extract correlated predicates from INNER JOIN ON clauses into the
                // correlated list so they end up in WHERE after rewriting. This enables
                // eliminateOuterRef to discover them (it only scans WHERE). Without
                // extraction, compensateInnerJoins would still rewrite them in place —
                // correct but __outer_ref can never be eliminated, hurting performance.
                // Only INNER JOINs are safe to extract from; extracting from LEFT JOIN
                // ON would change semantics (NULL rows filtered out).
                // Example: ... FROM t1 INNER JOIN t2 ON t2.x = t1.y AND t2.z = o.id
                //   → correlated gets [t2.z = o.id], ON becomes [t2.x = t1.y]
                extractCorrelatedFromInnerJoins(inner, model, correlatedPreds);

                // Collect ALL outer ref columns from every location.
                outerToInnerAlias.clear();
                outerCols.clear();
                for (int j = 0, m = correlatedPreds.size(); j < m; j++) {
                    collectUnmappedOuterColumnsFromExpr(
                            correlatedPreds.getQuick(j), inner, model, outerToInnerAlias, outerCols
                    );
                }
                collectUnmappedOuterColumnsFromSelectAndClauses(
                        inner, model, outerToInnerAlias, outerCols
                );
                collectUnmappedOuterColumnsFromInnerJoinCriteria(
                        inner, model, outerToInnerAlias, outerCols
                );
                collectUnmappedOuterColumnsFromNestedLaterals(
                        inner, model, outerToInnerAlias, outerCols
                );
                collectOuterRefsFromWrapperChain(
                        topInner, inner, model, outerToInnerAlias, outerCols
                );
                collectUnmappedOuterColumnsFromUnionBranches(
                        inner, model, outerToInnerAlias, outerCols
                );

                if (outerCols.size() == 0) {
                    joinModel.setJoinType(toDegradedJoinType(joinModel.getJoinType()));
                    continue;
                }

                outerToInnerAlias.clear();
                materializeOuterRefs(inner, selectModel, model, joinModel, outerCols, outerToInnerAlias);

                // Rewrite outer refs in correlated predicates to use __outer_ref aliases,
                // then merge them into nonCorrelatedPreds. After rewriting, they no longer
                // reference the outer model and become regular inner-model predicates.
                // Example: [order_id = o.id, qty > o.min_qty]
                //   → [order_id = __outer_ref.id, qty > __outer_ref.min_qty]
                for (int j = 0, m = correlatedPreds.size(); j < m; j++) {
                    nonCorrelatedPreds.add(rewriteOuterRefs(
                            correlatedPreds.getQuick(j), model, inner, outerToInnerAlias));
                }

                groupingCols.clear();
                for (int j = 0, m = outerCols.size(); j < m; j++) {
                    ExpressionNode outerCol = outerCols.getQuick(j);
                    CharSequence outerRefCol = outerToInnerAlias.get(outerCol.token);
                    if (outerRefCol != null) {
                        groupingCols.add(
                                expressionNodePool.next().of(
                                        ExpressionNode.LITERAL, outerRefCol, 0, outerCol.position
                                )
                        );
                    }
                }

                countColAliases.clear();
                QueryModel effectiveInner = compensateInnerOperators(
                        inner,
                        topInner,
                        model,
                        groupingCols,
                        outerToInnerAlias,
                        isLeft,
                        countColAliases
                );

                if (effectiveInner != inner) {
                    QueryModel parent = findWrapperParent(topInner, inner);
                    if (parent != null) {
                        parent.setNestedModel(effectiveInner);
                    } else {
                        joinModel.setNestedModel(effectiveInner);
                        topInner = effectiveInner;
                    }
                }

                boolean hasWrapperRefs = hasCorrelatedRefsInWrapperChain(topInner, inner, model);
                if (hasWrapperRefs) {
                    buildWrapperAliasMap(outerToInnerAlias, inner, wrapperOuterToInnerAlias);
                    rewriteWrapperChain(topInner, inner, model, wrapperOuterToInnerAlias);
                }

                inner.setWhereClause(conjoin(nonCorrelatedPreds));

                // Try to eliminate __outer_ref (post-hoc check).
                // Cannot eliminate when wrapper/deep refs depend on __outer_ref columns,
                // because eliminateOuterRef only rewrites SELECT/WHERE/ORDER BY/GROUP BY
                // of the content model — not wrapper chain or nested lateral expressions.
                if (!isLeft
                        && effectiveInner == inner
                        && !hasWrapperRefs
                        && !hasDeepCorrelatedRefs(inner, model)
                        && inner.getUnionModel() == null) {
                    eliminateOuterRef(inner, selectModel, model, joinModel);
                }

                propagateContentColumns(topInner, inner);
                joinModel.setJoinType(toDegradedJoinType(joinModel.getJoinType()));

                if (isLeft && countColAliases.size() > 0) {
                    wrapCountColumnsWithCoalesce(model, joinModel, countColAliases);
                }
            }
        }

        for (int i = 0, n = model.getJoinModels().size(); i < n; i++) {
            QueryModel jm = model.getJoinModels().getQuick(i);
            if (jm != model && jm.getNestedModel() != null) {
                rewrite(jm.getNestedModel());
            }
        }
    }
}
