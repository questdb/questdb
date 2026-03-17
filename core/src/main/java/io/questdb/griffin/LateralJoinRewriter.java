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
import io.questdb.griffin.model.JoinContext;
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
    private final ObjectPool<JoinContext> contextPool;
    private final ObjList<ExpressionNode> correlated = new ObjList<>();
    private final ObjList<CharSequence> countColAliases = new ObjList<>();
    private final ObjList<ExpressionNode> eqPreds = new ObjList<>();
    private final ObjectPool<ExpressionNode> expressionNodePool;
    private final ObjList<ExpressionNode> extraOuterCols = new ObjList<>();
    private final FunctionParser functionParser;
    private final ObjList<ExpressionNode> groupingCols = new ObjList<>();
    private final ObjList<ExpressionNode> innerJoinCorrelated = new ObjList<>();
    private final ObjList<ExpressionNode> innerJoinNonCorrelated = new ObjList<>();
    private final ObjList<ExpressionNode> nonCorrelated = new ObjList<>();
    private final ObjList<ExpressionNode> nonEqPreds = new ObjList<>();
    private final ObjList<ExpressionNode> nonRewritableNonEq = new ObjList<>();
    private final IntList orderByDirSave = new IntList();
    private final ObjList<ExpressionNode> orderBySave = new ObjList<>();
    private final IntList outerJmIndexes = new IntList();
    private final StringSink outerRefColSink = new StringSink();
    private final LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias = new LowerCaseCharSequenceObjHashMap<>();
    private final ObjectPool<QueryColumn> queryColumnPool;
    private final ObjectPool<QueryModel> queryModelPool;
    private final ObjList<ExpressionNode> rewritableNonEq = new ObjList<>();
    private final ArrayDeque<ExpressionNode> sqlNodeStack;
    private final ObjectPool<WindowExpression> windowExpressionPool;
    private final LowerCaseCharSequenceObjHashMap<CharSequence> wrapperOuterToInnerAlias = new LowerCaseCharSequenceObjHashMap<>();

    LateralJoinRewriter(
            CharacterStore characterStore,
            ObjectPool<ExpressionNode> expressionNodePool,
            ObjectPool<QueryColumn> queryColumnPool,
            ObjectPool<QueryModel> queryModelPool,
            ObjectPool<JoinContext> contextPool,
            ObjectPool<WindowExpression> windowExpressionPool,
            ArrayDeque<ExpressionNode> sqlNodeStack,
            FunctionParser functionParser
    ) {
        this.characterStore = characterStore;
        this.expressionNodePool = expressionNodePool;
        this.queryColumnPool = queryColumnPool;
        this.queryModelPool = queryModelPool;
        this.contextPool = contextPool;
        this.windowExpressionPool = windowExpressionPool;
        this.sqlNodeStack = sqlNodeStack;
        this.functionParser = functionParser;
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

    private void addEqualityToJoinContext(
            QueryModel parent,
            QueryModel joinModel,
            int joinIndex,
            ExpressionNode innerCol,
            ExpressionNode outerCol
    ) {
        JoinContext jc = joinModel.getJoinContext();
        if (jc == null) {
            jc = contextPool.next();
            jc.slaveIndex = joinIndex;
            joinModel.setContext(jc);
        }

        int outerIndex = 0;
        int dotPos = Chars.indexOf(outerCol.token, '.');
        if (dotPos > 0) {
            CharSequence tableAlias = outerCol.token.subSequence(0, dotPos);
            int idx = parent.getModelAliasIndex(tableAlias, 0, tableAlias.length());
            if (idx >= 0) {
                outerIndex = idx;
            }
        } else {
            for (int j = 0, jn = parent.getJoinModels().size(); j < jn; j++) {
                QueryModel jm = parent.getJoinModels().getQuick(j);
                if (jm.getColumnNameToAliasMap().contains(outerCol.token)) {
                    outerIndex = j;
                    break;
                }
            }
        }

        jc.aNodes.add(innerCol);
        jc.aIndexes.add(joinIndex);
        jc.aNames.add(innerCol.token);
        jc.bNodes.add(outerCol);
        jc.bIndexes.add(outerIndex);
        jc.bNames.add(outerCol.token);
        jc.parents.add(outerIndex);
        jc.inCount++;
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
                        outerRefColSink.clear();
                        outerRefColSink.put(modelName).put('.').put(token);
                        CharSequence qualifiedKey = Chars.toString(outerRefColSink);
                        if (map.get(qualifiedKey) == null) {
                            map.put(qualifiedKey, value);
                        }
                    }
                }
            }
        }
    }

    private boolean allOuterRefsInMap(
            ExpressionNode node,
            QueryModel innerModel,
            QueryModel outerModel,
            LowerCaseCharSequenceObjHashMap<CharSequence> map
    ) {
        if (node == null) {
            return true;
        }
        if (node.type == ExpressionNode.LITERAL) {
            boolean isOuterRef = !canResolveInModel(node.token, innerModel)
                    && canResolveInModel(node.token, outerModel);
            if (isOuterRef) {
                return map.get(node.token) != null;
            }
            int dotPos = Chars.indexOf(node.token, '.');
            if (dotPos > 0) {
                CharSequence tableAlias = node.token.subSequence(0, dotPos);
                if (outerModel.getModelAliasIndex(tableAlias, 0, tableAlias.length()) >= 0
                        && innerModel.getModelAliasIndex(tableAlias, 0, tableAlias.length()) < 0) {
                    return map.get(node.token) != null;
                }
            }
            return true;
        }
        boolean result = allOuterRefsInMap(node.lhs, innerModel, outerModel, map)
                && allOuterRefsInMap(node.rhs, innerModel, outerModel, map);
        for (int j = 0, m = node.args.size(); j < m; j++) {
            result &= allOuterRefsInMap(node.args.getQuick(j), innerModel, outerModel, map);
        }
        return result;
    }

    private void buildOuterToInnerAliasMap(
            ObjList<ExpressionNode> equalities,
            QueryModel innerModel,
            LowerCaseCharSequenceObjHashMap<CharSequence> map
    ) throws SqlException {
        for (int i = 0, n = equalities.size(); i < n; i++) {
            ExpressionNode eq = equalities.getQuick(i);
            CharSequence innerAlias = ensureColumnInSelect(innerModel, eq.lhs, eq.lhs.token);
            map.put(eq.rhs.token, innerAlias);
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

    private void classifyNonEqPredicates(
            ObjList<ExpressionNode> nonEqPreds,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            QueryModel innerModel,
            QueryModel outerModel,
            ObjList<ExpressionNode> rewritable,
            ObjList<ExpressionNode> nonRewritable
    ) {
        for (int i = 0, n = nonEqPreds.size(); i < n; i++) {
            ExpressionNode pred = nonEqPreds.getQuick(i);
            if (allOuterRefsInMap(pred, innerModel, outerModel, outerToInnerAlias)) {
                rewritable.add(pred);
            } else {
                nonRewritable.add(pred);
            }
        }
    }

    private void collectOuterJoinModelIndexes(
            QueryModel outerModel,
            ObjList<ExpressionNode> eqPreds,
            ObjList<ExpressionNode> extraOuterCols,
            IntList result
    ) {
        for (int i = 0, n = eqPreds.size(); i < n; i++) {
            addOuterJmIndex(outerModel, eqPreds.getQuick(i).rhs, result);
        }
        for (int i = 0, n = extraOuterCols.size(); i < n; i++) {
            addOuterJmIndex(outerModel, extraOuterCols.getQuick(i), result);
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

    private void collectUnmappedOuterColumns(
            ObjList<ExpressionNode> preds,
            QueryModel innerModel,
            QueryModel outerModel,
            LowerCaseCharSequenceObjHashMap<CharSequence> map,
            ObjList<ExpressionNode> result
    ) {
        for (int i = 0, n = preds.size(); i < n; i++) {
            collectUnmappedOuterColumnsFromExpr(preds.getQuick(i), innerModel, outerModel, map, result);
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
                QueryModel nestedLateral = jm.getNestedModel();
                if (nestedLateral != null) {
                    collectUnmappedOuterColumnsFromSelectAndClauses(
                            nestedLateral, outerModel, map, result
                    );
                    ObjList<ExpressionNode> parsedWhere = nestedLateral.getParsedWhere();
                    for (int j = 0, m = parsedWhere.size(); j < m; j++) {
                        collectUnmappedOuterColumnsFromExpr(
                                parsedWhere.getQuick(j), nestedLateral, outerModel, map, result
                        );
                    }
                    for (int k = 1, kn = nestedLateral.getJoinModels().size(); k < kn; k++) {
                        QueryModel innerJm = nestedLateral.getJoinModels().getQuick(k);
                        if (!isLateralJoin(innerJm.getJoinType())) {
                            if (innerJm.getJoinCriteria() != null) {
                                collectUnmappedOuterColumnsFromExpr(
                                        innerJm.getJoinCriteria(), nestedLateral, outerModel, map, result
                                );
                            }
                            if (innerJm.getWhereClause() != null) {
                                collectUnmappedOuterColumnsFromExpr(
                                        innerJm.getWhereClause(), nestedLateral, outerModel, map, result
                                );
                            }
                        }
                    }
                    collectUnmappedOuterColumnsFromNestedLaterals(
                            nestedLateral, outerModel, map, result
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
            if (innerJoin.getWhereClause() != null
                    && hasCorrelatedRef(innerJoin.getWhereClause(), inner, outer)) {
                innerJoin.setWhereClause(
                        rewriteOuterRefs(innerJoin.getWhereClause(), outer, inner, outerToInnerAlias)
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
            QueryModel inner,
            ObjList<ExpressionNode> groupingCols,
            QueryModel outer
    ) throws SqlException {
        ExpressionNode limitHi = inner.getLimitHi();
        ExpressionNode limitLo = inner.getLimitLo();

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
        if (inner.getOrderBy().size() > 0) {
            orderBySave.addAll(inner.getOrderBy());
            orderByDirSave.addAll(inner.getOrderByDirection());
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
            int eqPredsCount,
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
            splitCorrelatedPredicates(current, outer, branchCorrelated, branchNonCorrelated);

            branchGroupingCols.clear();
            for (int j = 0, m = branchCorrelated.size(); j < m; j++) {
                ExpressionNode pred = branchCorrelated.getQuick(j);
                if (pred.type == ExpressionNode.OPERATION && Chars.equals(pred.token, "=")) {
                    boolean lhsCorr = hasCorrelatedRef(pred.lhs, current, outer);
                    boolean rhsCorr = hasCorrelatedRef(pred.rhs, current, outer);
                    if (lhsCorr != rhsCorr) {
                        ExpressionNode innerSide = lhsCorr ? pred.rhs : pred.lhs;
                        if (isSimpleColumnRef(innerSide)) {
                            branchGroupingCols.add(innerSide);
                        }
                    }
                }
            }
            for (int j = eqPredsCount, m = groupingCols.size(); j < m; j++) {
                branchGroupingCols.add(ExpressionNode.deepClone(expressionNodePool, groupingCols.getQuick(j)));
            }

            for (int j = 0, m = branchCorrelated.size(); j < m; j++) {
                ExpressionNode pred = branchCorrelated.getQuick(j);
                pred = rewriteOuterRefs(pred, outer, current, outerToInnerAlias);
                branchNonCorrelated.add(pred);
            }
            current.setWhereClause(conjoin(branchNonCorrelated));
            current.getParsedWhere().clear();
            current.getParsedWhere().addAll(branchNonCorrelated);

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

    private QueryModel decorrelateInner(
            QueryModel inner,
            QueryModel outer,
            ObjList<ExpressionNode> groupingCols,
            int eqPredsCount,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            boolean isLeftJoin,
            ObjList<CharSequence> countColAliases
    ) throws SqlException {
        QueryModel effectiveInner = inner;

        boolean hasGroupBy = inner.getGroupBy().size() > 0;
        boolean hasAggregates = hasAggregateFunctions(inner);
        boolean hasWindow = hasWindowColumns(inner);
        boolean hasLimit = inner.getLimitHi() != null;
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
            effectiveInner = compensateLimit(inner, groupingCols, outer);
        }

        if (hasUnion) {
            compensateSetOp(inner, outer, groupingCols, eqPredsCount, outerToInnerAlias);
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
                        ObjList<ExpressionNode> parsedWhere = nestedLateral.getParsedWhere();
                        for (int j = 0, m = parsedWhere.size(); j < m; j++) {
                            ExpressionNode pred = parsedWhere.getQuick(j);
                            if (hasCorrelatedRef(pred, nestedLateral, outerModel)) {
                                parsedWhere.setQuick(j,
                                        rewriteOuterRefs(pred, outerModel, nestedLateral, outerToInnerAlias));
                            }
                        }
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
                            if (innerJm.getWhereClause() != null
                                    && hasCorrelatedRef(innerJm.getWhereClause(), nestedLateral, outerModel)) {
                                innerJm.setWhereClause(
                                        rewriteOuterRefs(innerJm.getWhereClause(), outerModel, nestedLateral, outerToInnerAlias));
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
            QueryModel outerModel,
            QueryModel joinModel,
            ObjList<ExpressionNode> eqPreds
    ) {
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
        outerToInnerAlias.clear();
        for (int j = 0, m = eqPreds.size(); j < m; j++) {
            ExpressionNode innerCol = eqPreds.getQuick(j).lhs;
            ExpressionNode outerCol = eqPreds.getQuick(j).rhs;
            CharSequence outerRefColAlias = getOuterRefColumnAlias(outerRefAlias, outerCol, outerRefSubquery);
            outerToInnerAlias.put(outerRefColAlias, innerCol.token);
        }

        ObjList<QueryColumn> cols = inner.getBottomUpColumns();
        for (int j = cols.size() - 1; j >= 0; j--) {
            QueryColumn col = cols.getQuick(j);
            ExpressionNode ast = col.getAst();
            if (ast != null
                    && ast.type == ExpressionNode.LITERAL
                    && Chars.startsWith(ast.token, outerRefAlias)
                    && ast.token.length() > outerRefAlias.length()
                    && ast.token.charAt(outerRefAlias.length()) == '.') {
                inner.removeColumn(j);
            } else {
                ExpressionNode rewritten = rewriteOuterRefs(ast, outerModel, inner, outerToInnerAlias);
                if (rewritten != ast) {
                    col.of(col.getAlias(), rewritten);
                }
            }
        }

        ObjList<ExpressionNode> parsedWhere = inner.getParsedWhere();
        nonCorrelated.clear();
        for (int j = 0, m = parsedWhere.size(); j < m; j++) {
            ExpressionNode pred = parsedWhere.getQuick(j);
            if (isOuterRefEquality(pred, outerRefAlias)) {
                continue;
            }
            pred = rewriteOuterRefs(pred, outerModel, inner, outerToInnerAlias);
            nonCorrelated.add(pred);
        }
        inner.setWhereClause(conjoin(nonCorrelated));
        inner.getParsedWhere().clear();
        inner.getParsedWhere().addAll(nonCorrelated);

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

        JoinContext jc = joinModel.getJoinContext();
        if (jc != null) {
            for (int j = 0, m = jc.aNodes.size(); j < m; j++) {
                ExpressionNode aNode = jc.aNodes.getQuick(j);
                CharSequence mapped = outerToInnerAlias.get(aNode.token);
                if (mapped != null) {
                    jc.aNodes.setQuick(j,
                            expressionNodePool.next().of(
                                    ExpressionNode.LITERAL, mapped, aNode.precedence, aNode.position
                            ));
                    jc.aNames.setQuick(j, mapped);
                }
            }
        }

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
            int joinType = jm.getJoinType();
            if (joinCriteria != null
                    && (joinType == QueryModel.JOIN_INNER || joinType == QueryModel.JOIN_CROSS)) {
                innerJoinCorrelated.clear();
                innerJoinNonCorrelated.clear();
                splitAndChain(joinCriteria, inner, outer, innerJoinCorrelated, innerJoinNonCorrelated);
                if (innerJoinCorrelated.size() > 0) {
                    correlated.addAll(innerJoinCorrelated);
                    jm.setJoinCriteria(innerJoinNonCorrelated.size() > 0
                            ? conjoin(innerJoinNonCorrelated) : null);
                }
            }

            ExpressionNode jmWhere = jm.getWhereClause();
            if (hasCorrelatedRef(jmWhere, inner, outer)) {
                innerJoinCorrelated.clear();
                innerJoinNonCorrelated.clear();
                splitAndChain(jmWhere, inner, outer, innerJoinCorrelated, innerJoinNonCorrelated);
                if (innerJoinCorrelated.size() > 0) {
                    correlated.addAll(innerJoinCorrelated);
                    jm.setWhereClause(innerJoinNonCorrelated.size() > 0
                            ? conjoin(innerJoinNonCorrelated) : null);
                }
            }
        }
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
                outerRefColSink.clear();
                outerRefColSink.put(outerRefAlias).put('.').put(col.getAlias());
                return Chars.toString(outerRefColSink);
            }
        }
        int dotPos = Chars.indexOf(outerCol.token, '.');
        CharSequence colName = dotPos > 0
                ? outerCol.token.subSequence(dotPos + 1, outerCol.token.length())
                : outerCol.token;
        outerRefColSink.clear();
        outerRefColSink.put(outerRefAlias).put('.').put(colName);
        return Chars.toString(outerRefColSink);
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

    private boolean hasCorrelatedNonWhereRefs(QueryModel inner, QueryModel outer) {
        return hasCorrelatedNonWhereRefs(inner, inner, outer);
    }

    private boolean hasCorrelatedNonWhereRefs(QueryModel inner, QueryModel resolveModel, QueryModel outer) {
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
        if (node.type == ExpressionNode.LITERAL) {
            CharSequence token = node.token;
            int dotPos = Chars.indexOf(token, '.');
            if (dotPos > 0) {
                CharSequence tableAlias = token.subSequence(0, dotPos);
                if (outerModel.getModelAliasIndex(tableAlias, 0, tableAlias.length()) >= 0
                        && innerModel.getModelAliasIndex(tableAlias, 0, tableAlias.length()) < 0) {
                    return true;
                }
            }
            return !canResolveInModel(token, innerModel)
                    && canResolveInModel(token, outerModel);
        }
        if (node.type == ExpressionNode.QUERY) {
            return hasCorrelatedRefInSubquery(node.queryModel, innerModel, outerModel);
        }
        boolean result = hasCorrelatedRef(node.lhs, innerModel, outerModel)
                || hasCorrelatedRef(node.rhs, innerModel, outerModel);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            result |= hasCorrelatedRef(node.args.getQuick(i), innerModel, outerModel);
        }
        if (node.windowExpression != null) {
            ObjList<ExpressionNode> partitionBy = node.windowExpression.getPartitionBy();
            for (int i = 0, n = partitionBy.size(); i < n; i++) {
                result |= hasCorrelatedRef(partitionBy.getQuick(i), innerModel, outerModel);
            }
            ObjList<ExpressionNode> winOrderBy = node.windowExpression.getOrderBy();
            for (int i = 0, n = winOrderBy.size(); i < n; i++) {
                result |= hasCorrelatedRef(winOrderBy.getQuick(i), innerModel, outerModel);
            }
        }
        return result;
    }

    private boolean hasCorrelatedRefInSubquery(
            QueryModel subquery,
            QueryModel innerModel,
            QueryModel outerModel
    ) {
        if (subquery == null) {
            return false;
        }
        ObjList<QueryColumn> cols = subquery.getBottomUpColumns();
        for (int i = 0, n = cols.size(); i < n; i++) {
            if (hasCorrelatedRef(cols.getQuick(i).getAst(), innerModel, outerModel)) {
                return true;
            }
        }
        ExpressionNode where = subquery.getWhereClause();
        if (hasCorrelatedRef(where, innerModel, outerModel)) {
            return true;
        }
        ObjList<ExpressionNode> orderBy = subquery.getOrderBy();
        for (int i = 0, n = orderBy.size(); i < n; i++) {
            if (hasCorrelatedRef(orderBy.getQuick(i), innerModel, outerModel)) {
                return true;
            }
        }
        ObjList<ExpressionNode> groupBy = subquery.getGroupBy();
        for (int i = 0, n = groupBy.size(); i < n; i++) {
            if (hasCorrelatedRef(groupBy.getQuick(i), innerModel, outerModel)) {
                return true;
            }
        }
        if (subquery.getSampleBy() != null && hasCorrelatedRef(subquery.getSampleBy(), innerModel, outerModel)) {
            return true;
        }
        ObjList<ExpressionNode> latestBy = subquery.getLatestBy();
        for (int i = 0, n = latestBy.size(); i < n; i++) {
            if (hasCorrelatedRef(latestBy.getQuick(i), innerModel, outerModel)) {
                return true;
            }
        }
        if (subquery.getLimitLo() != null && hasCorrelatedRef(subquery.getLimitLo(), innerModel, outerModel)) {
            return true;
        }
        if (subquery.getLimitHi() != null && hasCorrelatedRef(subquery.getLimitHi(), innerModel, outerModel)) {
            return true;
        }
        for (int i = 1, n = subquery.getJoinModels().size(); i < n; i++) {
            QueryModel jm = subquery.getJoinModels().getQuick(i);
            if (jm.getJoinCriteria() != null
                    && hasCorrelatedRef(jm.getJoinCriteria(), innerModel, outerModel)) {
                return true;
            }
        }
        if (subquery.getNestedModel() != null
                && hasCorrelatedRefInSubquery(subquery.getNestedModel(), innerModel, outerModel)) {
            return true;
        }
        return subquery.getUnionModel() != null
                && hasCorrelatedRefInSubquery(subquery.getUnionModel(), innerModel, outerModel);
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

    private boolean hasCorrelatedSelectExprs(QueryModel inner, QueryModel outer) {
        return hasCorrelatedSelectExprs(inner, inner, outer);
    }

    private boolean hasCorrelatedSelectExprs(QueryModel inner, QueryModel resolveModel, QueryModel outer) {
        ObjList<QueryColumn> cols = inner.getBottomUpColumns();
        for (int i = 0, n = cols.size(); i < n; i++) {
            if (hasCorrelatedRef(cols.getQuick(i).getAst(), resolveModel, outer)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasDeepCorrelatedRefs(QueryModel innerModel, QueryModel outerModel) {
        for (int i = 1, n = innerModel.getJoinModels().size(); i < n; i++) {
            QueryModel jm = innerModel.getJoinModels().getQuick(i);
            if (isLateralJoin(jm.getJoinType())) {
                QueryModel nestedLateral = jm.getNestedModel();
                if (nestedLateral != null) {
                    if (hasCorrelatedSelectExprs(nestedLateral, outerModel)
                            || hasCorrelatedNonWhereRefs(nestedLateral, outerModel)) {
                        return true;
                    }
                    ObjList<ExpressionNode> parsedWhere = nestedLateral.getParsedWhere();
                    for (int j = 0, m = parsedWhere.size(); j < m; j++) {
                        if (hasCorrelatedRef(parsedWhere.getQuick(j), nestedLateral, outerModel)) {
                            return true;
                        }
                    }
                    if (hasDeepCorrelatedRefs(nestedLateral, outerModel)) {
                        return true;
                    }
                }
            }
        }
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
        ObjList<ExpressionNode> parsedWhere = model.getParsedWhere();
        for (int i = 0, n = parsedWhere.size(); i < n; i++) {
            if (hasCorrelatedRef(parsedWhere.getQuick(i), resolveModel, outer)) {
                return true;
            }
        }
        return hasCorrelatedSelectExprs(model, resolveModel, outer)
                || hasCorrelatedNonWhereRefs(model, resolveModel, outer);
    }

    private void materializeOuterRefs(
            QueryModel inner,
            QueryModel outerModel,
            QueryModel joinModel,
            int joinIndex,
            ObjList<ExpressionNode> nonRewritablePreds,
            ObjList<ExpressionNode> extraOuterCols,
            ObjList<ExpressionNode> eqPreds,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            ObjList<ExpressionNode> nonCorrelated
    ) throws SqlException {
        QueryModel outerRefSubquery = queryModelPool.next();
        outerRefSubquery.setDistinct(true);
        CharSequence outerRefAlias = createColumnAlias("__outer_ref", inner);

        for (int i = 0, n = eqPreds.size(); i < n; i++) {
            ExpressionNode outerCol = eqPreds.getQuick(i).rhs;
            addColumnToOuterRefSelect(outerRefSubquery, outerCol);
        }
        for (int i = 0, n = extraOuterCols.size(); i < n; i++) {
            addColumnToOuterRefSelect(outerRefSubquery, extraOuterCols.getQuick(i));
        }

        setupOuterRefDataSource(outerRefSubquery, outerModel, eqPreds, extraOuterCols);

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

        for (int i = 0, n = eqPreds.size(); i < n; i++) {
            ExpressionNode outerCol = eqPreds.getQuick(i).rhs;
            ExpressionNode innerCol = eqPreds.getQuick(i).lhs;
            CharSequence outerRefColAlias = getOuterRefColumnAlias(outerRefAlias, outerCol, outerRefSubquery);
            outerToInnerAlias.put(outerCol.token, outerRefColAlias);
            addQualifiedAliasVariants(outerCol.token, outerRefColAlias, outerModel, outerToInnerAlias);
            ExpressionNode outerRefNode = expressionNodePool.next().of(
                    ExpressionNode.LITERAL, outerRefColAlias, 0, outerCol.position
            );
            ExpressionNode eqInWhere = createBinaryOp("=",
                    ExpressionNode.deepClone(expressionNodePool, innerCol), outerRefNode);
            nonCorrelated.add(eqInWhere);
        }

        for (int i = 0, n = extraOuterCols.size(); i < n; i++) {
            ExpressionNode outerCol = extraOuterCols.getQuick(i);
            CharSequence outerRefColAlias = getOuterRefColumnAlias(outerRefAlias, outerCol, outerRefSubquery);
            outerToInnerAlias.put(outerCol.token, outerRefColAlias);
            addQualifiedAliasVariants(outerCol.token, outerRefColAlias, outerModel, outerToInnerAlias);
        }

        for (int i = 0, n = nonRewritablePreds.size(); i < n; i++) {
            ExpressionNode pred = nonRewritablePreds.getQuick(i);
            pred = rewriteOuterRefs(pred, outerModel, inner, outerToInnerAlias);
            nonCorrelated.add(pred);
        }

        for (int i = 0, n = eqPreds.size(); i < n; i++) {
            ExpressionNode outerCol = eqPreds.getQuick(i).rhs;
            CharSequence outerRefColAlias = getOuterRefColumnAlias(outerRefAlias, outerCol, outerRefSubquery);
            ExpressionNode outerRefNode = expressionNodePool.next().of(
                    ExpressionNode.LITERAL, outerRefColAlias, 0, 0
            );
            ensureColumnInSelect(inner, outerRefNode, outerRefColAlias);
            addEqualityToJoinContext(outerModel, joinModel, joinIndex, outerRefNode, outerCol);
        }
        for (int i = 0, n = extraOuterCols.size(); i < n; i++) {
            ExpressionNode outerCol = extraOuterCols.getQuick(i);
            CharSequence outerRefColAlias = getOuterRefColumnAlias(outerRefAlias, outerCol, outerRefSubquery);
            ExpressionNode outerRefNode = expressionNodePool.next().of(
                    ExpressionNode.LITERAL, outerRefColAlias, 0, 0
            );
            ensureColumnInSelect(inner, outerRefNode, outerRefColAlias);
            addEqualityToJoinContext(outerModel, joinModel, joinIndex, outerRefNode, outerCol);
        }
    }

    private void propagateNewColumns(QueryModel top, QueryModel content, int colCountBefore) throws SqlException {
        ObjList<QueryColumn> contentCols = content.getBottomUpColumns();
        int colCountAfter = contentCols.size();
        if (colCountAfter <= colCountBefore) {
            return;
        }
        QueryModel current = top;
        while (current != content) {
            ObjList<QueryColumn> wrapperCols = current.getBottomUpColumns();
            if (wrapperCols.size() > 0 && !isWildcard(wrapperCols)) {
                boolean hasGroupBy = current.getGroupBy().size() > 0;
                for (int i = colCountBefore; i < colCountAfter; i++) {
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
            ObjList<ExpressionNode> parsedWhere = subquery.getParsedWhere();
            for (int i = 0, n = parsedWhere.size(); i < n; i++) {
                parsedWhere.setQuick(i,
                        rewriteOuterRefs(parsedWhere.getQuick(i), outerModel, innerModel, outerToInnerAlias));
            }
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
                ObjList<ExpressionNode> parsedWhere = current.getParsedWhere();
                for (int j = 0, m = parsedWhere.size(); j < m; j++) {
                    ExpressionNode pred = parsedWhere.getQuick(j);
                    if (hasCorrelatedRef(pred, current, outer)) {
                        parsedWhere.setQuick(j,
                                rewriteOuterRefs(pred, outer, current, aliasMap));
                    }
                }
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

    private void setupOuterRefDataSource(
            QueryModel outerRefSubquery,
            QueryModel outerModel,
            ObjList<ExpressionNode> eqPreds,
            ObjList<ExpressionNode> extraOuterCols
    ) throws SqlException {
        outerJmIndexes.clear();
        collectOuterJoinModelIndexes(outerModel, eqPreds, extraOuterCols, outerJmIndexes);

        for (int i = 0, n = outerJmIndexes.size(); i < n; i++) {
            QueryModel outerJm = outerModel.getJoinModels().getQuick(outerJmIndexes.getQuick(i));
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
                        .put("LATERAL non-equality correlation: cannot determine outer data source");
            }
            if (i == 0) {
                outerRefSubquery.setNestedModel(outerRefBase);
            } else {
                outerRefBase.setJoinType(QueryModel.JOIN_CROSS);
                outerRefSubquery.getJoinModels().add(outerRefBase);
            }
        }
    }

    private void splitAndChain(
            ExpressionNode expr,
            QueryModel inner,
            QueryModel outer,
            ObjList<ExpressionNode> correlated,
            ObjList<ExpressionNode> nonCorrelated
    ) {
        if (expr.type == ExpressionNode.OPERATION
                && Chars.equalsIgnoreCase(expr.token, "and")
                && expr.paramCount == 2) {
            splitAndChain(expr.lhs, inner, outer, correlated, nonCorrelated);
            splitAndChain(expr.rhs, inner, outer, correlated, nonCorrelated);
        } else {
            if (hasCorrelatedRef(expr, inner, outer)) {
                correlated.add(expr);
            } else {
                nonCorrelated.add(expr);
            }
        }
    }

    private void splitCorrelatedPredicates(
            QueryModel innerModel,
            QueryModel outerModel,
            ObjList<ExpressionNode> correlated,
            ObjList<ExpressionNode> nonCorrelated
    ) {
        ObjList<ExpressionNode> parsedWhere = innerModel.getParsedWhere();
        for (int i = 0, n = parsedWhere.size(); i < n; i++) {
            ExpressionNode pred = parsedWhere.getQuick(i);
            if (hasCorrelatedRef(pred, innerModel, outerModel)) {
                correlated.add(pred);
            } else {
                nonCorrelated.add(pred);
            }
        }
    }

    private void splitEqualityPredicates(
            ObjList<ExpressionNode> correlated,
            QueryModel innerModel,
            QueryModel outerModel,
            ObjList<ExpressionNode> equalities,
            ObjList<ExpressionNode> nonEqualities
    ) {
        for (int i = 0, n = correlated.size(); i < n; i++) {
            ExpressionNode pred = correlated.getQuick(i);
            if (pred.type == ExpressionNode.OPERATION
                    && Chars.equals(pred.token, "=")) {
                boolean lhsCorr = hasCorrelatedRef(pred.lhs, innerModel, outerModel);
                boolean rhsCorr = hasCorrelatedRef(pred.rhs, innerModel, outerModel);
                if (lhsCorr != rhsCorr) {
                    ExpressionNode innerSide = lhsCorr ? pred.rhs : pred.lhs;
                    ExpressionNode outerSide = lhsCorr ? pred.lhs : pred.rhs;
                    if (isSimpleColumnRef(innerSide) && isSimpleColumnRef(outerSide)) {
                        if (lhsCorr) {
                            ExpressionNode tmp = pred.lhs;
                            pred.lhs = pred.rhs;
                            pred.rhs = tmp;
                        }
                        equalities.add(pred);
                        continue;
                    }
                }
            }
            nonEqualities.add(pred);
        }
    }

    private QueryModel unwrapToContent(QueryModel model) {
        QueryModel current = model;
        while (current.getNestedModel() != null
                && current.getTableNameExpr() == null
                && current.getJoinModels().size() <= 1) {
            current = current.getNestedModel();
        }
        return current;
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
                QueryModel inner = unwrapToContent(topInner);
                boolean isWrapped = (topInner != inner);
                int colCountBefore = isWrapped ? inner.getBottomUpColumns().size() : 0;
                boolean isLeft = joinModel.getJoinType() == QueryModel.JOIN_LATERAL_LEFT;

                correlated.clear();
                nonCorrelated.clear();
                splitCorrelatedPredicates(inner, model, correlated, nonCorrelated);
                extractCorrelatedFromInnerJoins(inner, model, correlated);

                eqPreds.clear();
                nonEqPreds.clear();
                splitEqualityPredicates(correlated, inner, model, eqPreds, nonEqPreds);

                boolean hasDeepRefs = hasDeepCorrelatedRefs(inner, model);
                boolean hasWrapperRefs = isWrapped
                        && hasCorrelatedRefsInWrapperChain(topInner, inner, model);
                boolean hasAnyCorrelation = correlated.size() > 0
                        || hasCorrelatedSelectExprs(inner, model)
                        || hasCorrelatedNonWhereRefs(inner, model)
                        || hasDeepRefs
                        || hasWrapperRefs;

                if (!hasAnyCorrelation) {
                    joinModel.setJoinType(toDegradedJoinType(joinModel.getJoinType()));
                    continue;
                }

                if (QueryModel.isWindowJoin(inner)) {
                    throw SqlException.position(joinModel.getModelPosition())
                            .put("LATERAL decorrelation with WINDOW JOIN is not supported");
                }
                if (QueryModel.isHorizonJoin(inner)) {
                    throw SqlException.position(joinModel.getModelPosition())
                            .put("LATERAL decorrelation with HORIZON JOIN is not supported");
                }

                outerToInnerAlias.clear();
                buildOuterToInnerAliasMap(eqPreds, inner, outerToInnerAlias);

                rewritableNonEq.clear();
                nonRewritableNonEq.clear();
                extraOuterCols.clear();

                if (nonEqPreds.size() > 0) {
                    classifyNonEqPredicates(
                            nonEqPreds, outerToInnerAlias,
                            inner, model, rewritableNonEq, nonRewritableNonEq
                    );

                    for (int j = 0, m = rewritableNonEq.size(); j < m; j++) {
                        ExpressionNode pred = rewritableNonEq.getQuick(j);
                        pred = rewriteOuterRefs(pred, model, inner, outerToInnerAlias);
                        nonCorrelated.add(pred);
                    }

                    if (nonRewritableNonEq.size() > 0) {
                        collectUnmappedOuterColumns(
                                nonRewritableNonEq, inner, model,
                                outerToInnerAlias, extraOuterCols
                        );
                    }
                }

                collectUnmappedOuterColumnsFromSelectAndClauses(
                        inner, model, outerToInnerAlias, extraOuterCols
                );
                collectUnmappedOuterColumnsFromInnerJoinCriteria(
                        inner, model, outerToInnerAlias, extraOuterCols
                );
                collectUnmappedOuterColumnsFromNestedLaterals(
                        inner, model, outerToInnerAlias, extraOuterCols
                );
                if (isWrapped) {
                    collectOuterRefsFromWrapperChain(
                            topInner, inner, model, outerToInnerAlias, extraOuterCols
                    );
                }

                materializeOuterRefs(
                        inner, model, joinModel, i,
                        nonRewritableNonEq, extraOuterCols,
                        eqPreds, outerToInnerAlias,
                        nonCorrelated
                );

                groupingCols.clear();
                for (int j = 0, m = eqPreds.size(); j < m; j++) {
                    groupingCols.add(eqPreds.getQuick(j).lhs);
                }
                for (int j = 0, m = extraOuterCols.size(); j < m; j++) {
                    ExpressionNode outerCol = extraOuterCols.getQuick(j);
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
                QueryModel effectiveInner = decorrelateInner(
                        inner,
                        model,
                        groupingCols,
                        eqPreds.size(),
                        outerToInnerAlias,
                        isLeft,
                        countColAliases
                );

                if (effectiveInner != inner) {
                    if (isWrapped) {
                        QueryModel parent = findWrapperParent(topInner, inner);
                        if (parent != null) {
                            parent.setNestedModel(effectiveInner);
                        }
                    } else {
                        joinModel.setNestedModel(effectiveInner);
                    }
                }

                if (isWrapped) {
                    propagateNewColumns(topInner, inner, colCountBefore);
                    if (hasWrapperRefs) {
                        buildWrapperAliasMap(outerToInnerAlias, inner, wrapperOuterToInnerAlias);
                        rewriteWrapperChain(topInner, inner, model, wrapperOuterToInnerAlias);
                    }
                }

                inner.setWhereClause(conjoin(nonCorrelated));
                inner.getParsedWhere().clear();
                inner.getParsedWhere().addAll(nonCorrelated);

                joinModel.setJoinType(toDegradedJoinType(joinModel.getJoinType()));

                if (isLeft && countColAliases.size() > 0) {
                    wrapCountColumnsWithCoalesce(model, joinModel, countColAliases);
                }

                if (!isLeft
                        && effectiveInner == inner
                        && !isWrapped
                        && !hasDeepRefs
                        && inner.getUnionModel() == null
                        && nonRewritableNonEq.size() == 0
                        && extraOuterCols.size() == 0
                        && eqPreds.size() > 0) {
                    eliminateOuterRef(inner, model, joinModel, eqPreds);
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
