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

import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.StringSink;

import java.util.ArrayDeque;

import static io.questdb.griffin.model.QueryModel.isLateralJoin;

class LateralJoinRewriter {

    private final ObjList<ExpressionNode> branchCorrelated = new ObjList<>();
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

    private static boolean allLiteralsBelongTo(
            ExpressionNode node,
            CharSequence alias,
            LowerCaseCharSequenceObjHashMap<QueryColumn> columnMap
    ) {
        if (node == null) {
            return true;
        }
        if (node.type == ExpressionNode.LITERAL) {
            int dot = Chars.indexOf(node.token, '.');
            if (dot > 0) {
                return Chars.startsWith(node.token, alias)
                        && dot == alias.length();
            }
            // Unqualified: accept only if columnMap is provided (single source)
            return columnMap != null && columnMap.contains(node.token);
        }
        if (!allLiteralsBelongTo(node.lhs, alias, columnMap)
                || !allLiteralsBelongTo(node.rhs, alias, columnMap)) {
            return false;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (!allLiteralsBelongTo(node.args.getQuick(i), alias, columnMap)) {
                return false;
            }
        }
        return true;
    }

    private static LowerCaseCharSequenceIntHashMap ensureCorrelatedColumnSet(
            ObjList<LowerCaseCharSequenceIntHashMap> correlatedColumns,
            int index
    ) {
        if (correlatedColumns.size() <= index) {
            correlatedColumns.extendAndSet(index, new LowerCaseCharSequenceIntHashMap());
        }
        LowerCaseCharSequenceIntHashMap set = correlatedColumns.getQuick(index);
        if (set == null) {
            set = new LowerCaseCharSequenceIntHashMap();
            correlatedColumns.setQuick(index, set);
        }
        return set;
    }

    private static boolean hasOuterRefLiteral(ExpressionNode node, CharSequence outerRefAlias) {
        if (node == null) {
            return false;
        }
        if (node.type == ExpressionNode.LITERAL) {
            return Chars.startsWith(node.token, outerRefAlias)
                    && node.token.length() > outerRefAlias.length()
                    && node.token.charAt(outerRefAlias.length()) == '.';
        }
        if (hasOuterRefLiteral(node.lhs, outerRefAlias) || hasOuterRefLiteral(node.rhs, outerRefAlias)) {
            return true;
        }
        for (int i = 0, n = node.args.size(); i < n; i++) {
            if (hasOuterRefLiteral(node.args.getQuick(i), outerRefAlias)) {
                return true;
            }
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
        return cols.size() == 1 && cols.getQuick(0).getAst().isWildcard();
    }

    private static void registerDataSourceAlias(QueryModel parent, QueryModel dataSource, int index) {
        ExpressionNode alias = dataSource.getAlias();
        if (alias == null) {
            alias = dataSource.getTableNameExpr();
        }
        if (alias != null) {
            parent.addModelAliasIndex(alias, index);
        }
    }

    private static int toDegradedJoinType(int lateralJoinType) {
        return switch (lateralJoinType) {
            case QueryModel.JOIN_LATERAL_INNER -> QueryModel.JOIN_INNER;
            case QueryModel.JOIN_LATERAL_LEFT -> QueryModel.JOIN_LEFT_OUTER;
            case QueryModel.JOIN_LATERAL_CROSS -> QueryModel.JOIN_CROSS;
            default -> lateralJoinType;
        };
    }

    private void addColumnToOuterRefSelect(QueryModel outerRefSubquery, ExpressionNode outerCol) {
        int dotPos = Chars.indexOf(outerCol.token, '.');
        CharSequence colName = dotPos > 0
                ? outerCol.token.subSequence(dotPos + 1, outerCol.token.length())
                : outerCol.token;
        CharSequence alias = createColumnAlias(colName, outerRefSubquery);
        ExpressionNode ref = expressionNodePool.next().of(
                ExpressionNode.LITERAL, outerCol.token, 0, outerCol.position
        );
        QueryColumn qc = queryColumnPool.next().of(alias, ref);
        outerRefSubquery.addBottomUpColumnIfNotExists(qc);
    }

    private void addGroupingColsToEmbeddedWindows(ExpressionNode node) {
        if (node == null) {
            return;
        }
        if (node.windowExpression != null) {
            addGroupingColsToPartitionBy(node.windowExpression.getPartitionBy());
        }
        addGroupingColsToEmbeddedWindows(node.lhs);
        addGroupingColsToEmbeddedWindows(node.rhs);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            addGroupingColsToEmbeddedWindows(node.args.getQuick(i));
        }
    }

    private void addGroupingColsToPartitionBy(ObjList<ExpressionNode> partitionBy) {
        for (int j = 0, m = groupingCols.size(); j < m; j++) {
            ExpressionNode gcol = groupingCols.getQuick(j);
            boolean isFound = false;
            for (int k = 0, p = partitionBy.size(); k < p; k++) {
                if (expressionsEqual(partitionBy.getQuick(k), gcol)) {
                    isFound = true;
                    break;
                }
            }
            if (!isFound) {
                partitionBy.add(ExpressionNode.deepClone(expressionNodePool, gcol));
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

    private ExpressionNode buildLateralOnFromOuterRefCriteria(
            ExpressionNode criteria,
            CharSequence outerRefAlias,
            ObjList<QueryColumn> outerRefCols,
            QueryModel topInner,
            CharSequence lateralAlias
    ) throws SqlException {
        splitAndPredicates(criteria, nonCorrelatedPreds);

        ExpressionNode result = null;
        for (int j = 0, m = nonCorrelatedPreds.size(); j < m; j++) {
            ExpressionNode pred = nonCorrelatedPreds.getQuick(j);
            if (pred.type != ExpressionNode.OPERATION || pred.lhs == null || pred.rhs == null) {
                continue;
            }

            ExpressionNode outerRefSide;
            ExpressionNode innerSide;
            boolean isOuterRefLhs;
            if (isOuterRefToken(pred.lhs, outerRefAlias)) {
                outerRefSide = pred.lhs;
                innerSide = pred.rhs;
                isOuterRefLhs = true;
            } else if (isOuterRefToken(pred.rhs, outerRefAlias)) {
                outerRefSide = pred.rhs;
                innerSide = pred.lhs;
                isOuterRefLhs = false;
            } else {
                continue;
            }

            // Find original outer reference from __outer_ref SELECT
            int dotPos = Chars.indexOf(outerRefSide.token, '.');
            CharSequence colAlias = outerRefSide.token.subSequence(dotPos + 1, outerRefSide.token.length());
            ExpressionNode originalOuterRef = null;
            for (int k = 0, kn = outerRefCols.size(); k < kn; k++) {
                if (Chars.equalsIgnoreCase(outerRefCols.getQuick(k).getAlias(), colAlias)) {
                    originalOuterRef = outerRefCols.getQuick(k).getAst();
                    break;
                }
            }
            if (originalOuterRef == null) {
                continue;
            }

            // Ensure inner column in topInner's SELECT and qualify with lateral alias
            ExpressionNode innerNode = expressionNodePool.next().of(
                    ExpressionNode.LITERAL, innerSide.token, 0, innerSide.position
            );
            CharSequence selectAlias = ensureColumnInSelect(topInner, innerNode, innerSide.token);
            CharSequence qualifiedInner = qualifyWithAlias(lateralAlias, selectAlias);
            ExpressionNode qualifiedInnerNode = expressionNodePool.next().of(
                    ExpressionNode.LITERAL, qualifiedInner, 0, innerSide.position
            );
            ExpressionNode outerNode = ExpressionNode.deepClone(expressionNodePool, originalOuterRef);

            // Build condition preserving operand order
            ExpressionNode cond;
            if (isOuterRefLhs) {
                cond = createBinaryOp(pred.token, outerNode, qualifiedInnerNode);
            } else {
                cond = createBinaryOp(pred.token, qualifiedInnerNode, outerNode);
            }
            result = result == null ? cond : createBinaryOp("and", result, cond);
        }
        return result;
    }

    private void buildOuterColsFromCorrelatedColumns(
            QueryModel lateralJoinModel,
            QueryModel outerModel,
            ObjList<ExpressionNode> result
    ) {
        ObjList<LowerCaseCharSequenceIntHashMap> corrCols = lateralJoinModel.getCorrelatedColumns();
        for (int j = 0, m = corrCols.size(); j < m; j++) {
            LowerCaseCharSequenceIntHashMap colSet = corrCols.getQuick(j);
            if (colSet == null || colSet.size() == 0) {
                continue;
            }
            QueryModel outerJm = outerModel.getJoinModels().getQuick(j);
            CharSequence modelName = outerJm.getName();
            ObjList<CharSequence> colNames = colSet.keys();
            for (int k = 0, kn = colNames.size(); k < kn; k++) {
                CharSequence colName = colNames.getQuick(k);
                if (modelName != null) {
                    characterStore.newEntry();
                    characterStore.put(modelName).put('.').put(colName);
                    result.add(expressionNodePool.next().of(
                            ExpressionNode.LITERAL, characterStore.toImmutable(), 0, colSet.get(colName)
                    ));
                } else {
                    result.add(expressionNodePool.next().of(
                            ExpressionNode.LITERAL, colName, 0, colSet.get(colName)
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
            ObjList<LowerCaseCharSequenceIntHashMap> correlatedColumns
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
                    ensureCorrelatedColumnSet(correlatedColumns, jmIndex).put(colName, node.position);
                    node.lateralDepth = lateralDepth;
                    foundCorrelation = true;
                }
            } else if (!canResolveColumn(node.token, innerModel)) {
                for (int j = 0; j < lateralJoinIndex; j++) {
                    QueryModel outerJm = outerModel.getJoinModels().getQuick(j);
                    if (canResolveColumnForOuter(node.token, outerJm)) {
                        ensureCorrelatedColumnSet(correlatedColumns, j).put(node.token, node.position);
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
        ObjList<LowerCaseCharSequenceIntHashMap> correlatedColumns =
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
            ObjList<LowerCaseCharSequenceIntHashMap> correlatedColumns
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
            boolean isLeftJoin,
            ObjList<CharSequence> countColAliases
    ) throws SqlException {
        ObjList<ExpressionNode> groupBy = inner.getGroupBy();
        if (groupBy != null && groupBy.size() > 0) {
            for (int i = 0, n = groupingCols.size(); i < n; i++) {
                ExpressionNode col = groupingCols.getQuick(i);
                boolean isFound = false;
                for (int j = 0, m = groupBy.size(); j < m; j++) {
                    if (expressionsEqual(groupBy.getQuick(j), col)) {
                        isFound = true;
                        break;
                    }
                }
                if (!isFound) {
                    inner.addGroupBy(ExpressionNode.deepClone(expressionNodePool, col));
                }
            }

            for (int i = 0, n = groupingCols.size(); i < n; i++) {
                ExpressionNode col = groupingCols.getQuick(i);
                ensureColumnInSelect(inner, col, col.token);
            }
        }

        if (isLeftJoin) {
            rewriteCountForLeftJoin(inner, countColAliases);
        }
    }

    private void compensateDistinct(QueryModel inner) throws SqlException {
        for (int j = 0, m = groupingCols.size(); j < m; j++) {
            ExpressionNode col = groupingCols.getQuick(j);
            ensureColumnInSelect(inner, col, col.token);
        }
    }

    private void compensateInnerJoins(
            QueryModel inner,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth
    ) {
        for (int i = 1, n = inner.getJoinModels().size(); i < n; i++) {
            QueryModel innerJoin = inner.getJoinModels().getQuick(i);
            ExpressionNode joinCriteria = innerJoin.getJoinCriteria();
            if (hasCorrelatedExprAtDepth(joinCriteria, depth)) {
                innerJoin.setJoinCriteria(
                        rewriteOuterRefs(joinCriteria, outerToInnerAlias)
                );
            }
        }
    }

    private void compensateLatestBy(QueryModel inner) throws SqlException {
        ObjList<ExpressionNode> latestBy = inner.getLatestBy();
        for (int i = 0, n = groupingCols.size(); i < n; i++) {
            ExpressionNode col = groupingCols.getQuick(i);
            boolean isAlreadyPresent = false;
            for (int j = 0, m = latestBy.size(); j < m; j++) {
                if (expressionsEqual(latestBy.getQuick(j), col)) {
                    isAlreadyPresent = true;
                    break;
                }
            }
            if (!isAlreadyPresent) {
                latestBy.add(ExpressionNode.deepClone(expressionNodePool, col));
            }
            ensureColumnInSelect(inner, col, col.token);
        }
    }

    private QueryModel compensateLimit(
            QueryModel current,
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

    private void compensateSampleBy(QueryModel inner) throws SqlException {
        ObjList<ExpressionNode> groupBy = inner.getGroupBy();
        for (int i = 0, n = groupingCols.size(); i < n; i++) {
            ExpressionNode col = groupingCols.getQuick(i);
            ensureColumnInSelect(inner, col, col.token);
            boolean isFound = false;
            for (int j = 0, m = groupBy.size(); j < m; j++) {
                if (expressionsEqual(groupBy.getQuick(j), col)) {
                    isFound = true;
                    break;
                }
            }
            if (!isFound) {
                inner.addGroupBy(ExpressionNode.deepClone(expressionNodePool, col));
            }
        }
    }

    private void compensateSetOp(
            QueryModel inner,
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
            if (branchOuterRef.getAlias() != null) {
                current.addModelAliasIndex(branchOuterRef.getAlias(), current.getJoinModels().size() - 1);
            }

            branchCorrelated.clear();
            branchNonCorrelated.clear();
            extractCorrelatedPredicates(current.getWhereClause(), branchCorrelated, branchNonCorrelated, depth);

            for (int j = 0, m = branchCorrelated.size(); j < m; j++) {
                ExpressionNode pred = branchCorrelated.getQuick(j);
                pred = rewriteOuterRefs(pred, outerToInnerAlias);
                branchNonCorrelated.add(pred);
            }
            current.setWhereClause(conjoin(branchNonCorrelated));

            rewriteSelectExpressions(current, outerToInnerAlias, depth);
            rewriteOrderByExpressions(current, outerToInnerAlias, depth);
            rewriteGroupByExpressions(current, outerToInnerAlias, depth);
            if (current.getSampleBy() != null
                    && hasCorrelatedExprAtDepth(current.getSampleBy(), depth)) {
                current.setSampleBy(
                        rewriteOuterRefs(current.getSampleBy(), outerToInnerAlias));
            }
            ObjList<ExpressionNode> branchLatestBy = current.getLatestBy();
            for (int j = 0, m = branchLatestBy.size(); j < m; j++) {
                ExpressionNode lb = branchLatestBy.getQuick(j);
                if (hasCorrelatedExprAtDepth(lb, depth)) {
                    branchLatestBy.setQuick(j,
                            rewriteOuterRefs(lb, outerToInnerAlias));
                }
            }

            ObjList<ExpressionNode> branchGroupBy = current.getGroupBy();
            for (int j = 0, m = groupingCols.size(); j < m; j++) {
                ExpressionNode col = groupingCols.getQuick(j);
                ensureColumnInSelect(current, col, col.token);
                if (branchGroupBy.size() > 0) {
                    boolean isFound = false;
                    for (int k = 0, kn = branchGroupBy.size(); k < kn; k++) {
                        if (expressionsEqual(branchGroupBy.getQuick(k), col)) {
                            isFound = true;
                            break;
                        }
                    }
                    if (!isFound) {
                        current.addGroupBy(ExpressionNode.deepClone(expressionNodePool, col));
                    }
                }
            }

            if (hasWindowColumns(current)) {
                compensateWindow(current);
            }
            if (current.getLatestBy().size() > 0) {
                compensateLatestBy(current);
            }

            decorrelateJoinModelSubqueries(current, outerToInnerAlias, depth);
            if (current.getTableNameExpr() == null && current.getNestedModel() != null) {
                boolean isDescendable = true;
                for (int ji = 1, jn = current.getJoinModels().size(); ji < jn && isDescendable; ji++) {
                    QueryModel bjm = current.getJoinModels().getQuick(ji);
                    if ((bjm.getJoinCriteria() != null && hasCorrelatedExprAtDepth(bjm.getJoinCriteria(), depth))
                            || isLateralJoin(bjm.getJoinType())
                            || (bjm.getNestedModel() != null && bjm.getNestedModel().isCorrelatedAtDepth(depth))) {
                        isDescendable = false;
                    }
                }
                if (isDescendable) {
                    compensateInnerJoins(current, outerToInnerAlias, depth);
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
                            current, current.getNestedModel(), outerToInnerAlias,
                            false, subCountColAliases, deepOuterRef, current, depth
                    );
                }
            }

            // LIMIT compensation for union branch
            if (current.getLimitHi() != null) {
                QueryModel wrapper = compensateLimit(current, depth);
                if (wrapper != current) {
                    wrapper.setUnionModel(current.getUnionModel());
                    wrapper.setSetOperationType(current.getSetOperationType());
                    current.setUnionModel(null);
                    prev.setUnionModel(wrapper);
                    for (int j = 0, m = groupingCols.size(); j < m; j++) {
                        ExpressionNode gcol = groupingCols.getQuick(j);
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

    private void compensateWindow(QueryModel inner) throws SqlException {
        for (int i = 0, n = inner.getBottomUpColumns().size(); i < n; i++) {
            QueryColumn col = inner.getBottomUpColumns().getQuick(i);
            if (col instanceof WindowExpression we) {
                addGroupingColsToPartitionBy(we.getPartitionBy());
            } else {
                addGroupingColsToEmbeddedWindows(col.getAst());
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

    // Creates a data source model for the __outer_ref subquery from an outer join model.
    // For tables: deep clones the tableNameExpr (safe, independent copy).
    // For subqueries: SHARES the nestedModel reference with the outer query. A full QueryModel
    // deep clone would be needed to break the sharing but is prohibitively expensive.
    private QueryModel createOuterRefBase(QueryModel outerJm) throws SqlException {
        QueryModel outerRefBase = queryModelPool.next();
        if (outerJm.getTableNameExpr() != null) {
            outerRefBase.setTableNameExpr(
                    ExpressionNode.deepClone(expressionNodePool, outerJm.getTableNameExpr())
            );
        } else if (outerJm.getNestedModel() != null) {
            outerRefBase.setNestedModel(outerJm.getNestedModel());
            outerRefBase.setNestedModelIsSubQuery(outerJm.isNestedModelIsSubQuery());
        } else {
            throw SqlException.position(0)
                    .put("LATERAL decorrelation: cannot determine outer data source");
        }
        if (outerJm.getAlias() != null) {
            outerRefBase.setAlias(outerJm.getAlias());
        }
        ExpressionNode alias = outerRefBase.getAlias();
        if (alias == null) {
            alias = outerRefBase.getTableNameExpr();
        }
        if (alias != null) {
            outerRefBase.addModelAliasIndex(alias, 0);
        }
        ObjList<CharSequence> srcKeys = outerJm.getAliasToColumnMap().keys();
        for (int i = 0, n = srcKeys.size(); i < n; i++) {
            outerRefBase.addField(outerJm.getAliasToColumnMap().get(srcKeys.getQuick(i)));
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
                pushOuterWhereToRefBases(model, outerRefSubquery);

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
                }

                // Push down outer refs
                int depth = lateralDepth + 1;
                countColAliases.clear();
                pushDownOuterRefs(
                        null, topInner, outerToInnerAlias, isLeft,
                        countColAliases, outerRefJoinModel, joinModel, depth
                );

                topInner = joinModel.getNestedModel();
                CharSequence lateralAlias = joinModel.getAlias() != null
                        ? joinModel.getAlias().token : null;
                ExpressionNode joinCriteria = null;
                for (int j = 0, m = outerCols.size(); j < m; j++) {
                    ExpressionNode outerCol = outerCols.getQuick(j);
                    CharSequence outerRefColAlias = outerToInnerAlias.get(outerCol.token);
                    ExpressionNode outerRefNode = expressionNodePool.next().of(
                            ExpressionNode.LITERAL, outerRefColAlias, 0, 0
                    );
                    CharSequence selectAlias = ensureColumnInSelect(topInner, outerRefNode, outerRefColAlias);
                    CharSequence qualifiedInnerCol;
                    if (lateralAlias != null) {
                        characterStore.newEntry();
                        characterStore.put(lateralAlias).put('.').put(selectAlias);
                        qualifiedInnerCol = characterStore.toImmutable();
                    } else {
                        qualifiedInnerCol = selectAlias;
                    }
                    ExpressionNode innerRef = expressionNodePool.next().of(
                            ExpressionNode.LITERAL, qualifiedInnerCol, 0, 0
                    );
                    ExpressionNode outerRef = ExpressionNode.deepClone(expressionNodePool, outerCol);
                    ExpressionNode eq = createBinaryOp("=", innerRef, outerRef);
                    joinCriteria = joinCriteria == null ? eq : createBinaryOp("and", joinCriteria, eq);
                }
                joinModel.setJoinCriteria(joinCriteria);

                // Try eliminateOuterRef
                eliminateOuterRef(topInner, joinModel);

                // Degrade join type
                joinModel.setJoinType(toDegradedJoinType(joinModel.getJoinType()));

                // LEFT JOIN count coalesce
                if (isLeft && countColAliases.size() > 0) {
                    wrapCountColumnsWithCoalesce(model, joinModel, countColAliases);
                }

                topInner = joinModel.getNestedModel();
                if (topInner != null) {
                    decorrelate(topInner, lateralDepth + 1);
                }
            } else if (joinModel.getNestedModel() != null) {
                decorrelate(joinModel.getNestedModel(), lateralDepth);
            }
        }
    }

    private void decorrelateJoinModelSubqueries(
            QueryModel joinLayer,
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
            if (jm == outerRefJm) {
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

            // Use pushDownOuterRefs to decorrelate the subquery
            subCountColAliases.clear();
            pushDownOuterRefs(
                    null, nested, outerToInnerAlias, false,
                    subCountColAliases, clonedOuterRef, jm, depth
            );
        }
    }

    private void eliminateOuterRef(
            QueryModel topInner,
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

        // 2. Scan __outer_ref's join criteria for equalities
        QueryModel outerRefJm = dataSourceLayer.getJoinModels().getQuick(outerRefJmIndex);
        outerToInnerAlias.clear();
        ExpressionNode joinCrit = outerRefJm.getJoinCriteria();
        if (joinCrit == null) {
            return;
        }
        scanWhereForOuterRefEqualities(joinCrit, outerRefAlias, outerToInnerAlias);

        // 3. Check if every __outer_ref column has a matching equality
        ObjList<QueryColumn> outerRefCols = outerRefSubquery.getBottomUpColumns();
        boolean isAllEqualities = true;
        for (int j = 0, m = outerRefCols.size(); j < m; j++) {
            CharSequence colAlias = outerRefCols.getQuick(j).getAlias();
            outerRefColSink.clear();
            outerRefColSink.put(outerRefAlias).put('.').put(colAlias);
            if (outerToInnerAlias.get(outerRefColSink) == null) {
                isAllEqualities = false;
                break;
            }
        }

        CharSequence lateralAlias = joinModel.getAlias() != null
                ? joinModel.getAlias().token : null;
        ExpressionNode simpleChainCriteria = null;
        if (!isAllEqualities) {
            if (!isSimpleInnerChain(topInner, dataSourceLayer)) {
                return;
            }
            simpleChainCriteria = buildLateralOnFromOuterRefCriteria(
                    outerRefJm.getJoinCriteria(), outerRefAlias, outerRefCols,
                    topInner, lateralAlias
            );
            if (simpleChainCriteria == null) {
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
                    ExpressionNode rewritten = rewriteOuterRefs(ast, outerToInnerAlias);
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

        // 5. Rewrite ORDER BY, GROUP BY (for equality-mapped columns)
        ObjList<ExpressionNode> orderBy = dataSourceLayer.getOrderBy();
        for (int j = 0, m = orderBy.size(); j < m; j++) {
            orderBy.setQuick(j,
                    rewriteOuterRefs(orderBy.getQuick(j), outerToInnerAlias));
        }
        ObjList<ExpressionNode> groupBy = dataSourceLayer.getGroupBy();
        for (int j = 0, m = groupBy.size(); j < m; j++) {
            groupBy.setQuick(j,
                    rewriteOuterRefs(groupBy.getQuick(j), outerToInnerAlias));
        }
        if (dataSourceLayer.getSampleBy() != null) {
            dataSourceLayer.setSampleBy(
                    rewriteOuterRefs(dataSourceLayer.getSampleBy(), outerToInnerAlias));
        }
        ObjList<ExpressionNode> latestBy = dataSourceLayer.getLatestBy();
        for (int j = 0, m = latestBy.size(); j < m; j++) {
            latestBy.setQuick(j,
                    rewriteOuterRefs(latestBy.getQuick(j), outerToInnerAlias));
        }

        // 6. Rebuild lateral join's ON criteria
        ExpressionNode newCriteria;
        if (isAllEqualities) {
            newCriteria = null;
            for (int j = 0, m = outerRefCols.size(); j < m; j++) {
                QueryColumn refCol = outerRefCols.getQuick(j);
                outerRefColSink.clear();
                outerRefColSink.put(outerRefAlias).put('.').put(refCol.getAlias());
                CharSequence innerCol = outerToInnerAlias.get(outerRefColSink);
                if (innerCol != null) {
                    ExpressionNode innerNode = expressionNodePool.next().of(
                            ExpressionNode.LITERAL, innerCol, 0, 0
                    );
                    CharSequence selectAlias = ensureColumnInSelect(topInner, innerNode, innerCol);
                    CharSequence qualifiedInner = qualifyWithAlias(lateralAlias, selectAlias);
                    ExpressionNode qualifiedInnerNode = expressionNodePool.next().of(
                            ExpressionNode.LITERAL, qualifiedInner, 0, 0
                    );
                    ExpressionNode outerNode = ExpressionNode.deepClone(expressionNodePool, refCol.getAst());
                    ExpressionNode eq = createBinaryOp("=", qualifiedInnerNode, outerNode);
                    newCriteria = newCriteria == null ? eq : createBinaryOp("and", newCriteria, eq);
                }
            }
        } else {
            newCriteria = simpleChainCriteria;
        }
        joinModel.setJoinCriteria(newCriteria);

        // 7. Remove __outer_ref join
        dataSourceLayer.getJoinModels().remove(outerRefJmIndex);
        dataSourceLayer.getModelAliasIndexes().remove(outerRefAlias);
    }

    private CharSequence ensureColumnInSelect(
            QueryModel model,
            ExpressionNode colExpr,
            CharSequence preferredAlias
    ) throws SqlException {
        ObjList<QueryColumn> cols = model.getBottomUpColumns();
        if (cols.size() == 0 || isWildcard(cols)) {
            return preferredAlias;
        }
        for (int i = 0, n = cols.size(); i < n; i++) {
            QueryColumn existing = cols.getQuick(i);
            if (expressionsEqual(existing.getAst(), colExpr)) {
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
        // splitAndPredicates uses nonCorrelatedPreds as temp; safe because nonCorrelated
        // is a different list passed by the caller (innerJoinNonCorrelated or similar)
        splitAndPredicates(expr, nonCorrelatedPreds);
        for (int i = 0, n = nonCorrelatedPreds.size(); i < n; i++) {
            ExpressionNode pred = nonCorrelatedPreds.getQuick(i);
            if (hasCorrelatedExprAtDepth(pred, depth)) {
                correlated.add(pred);
            } else {
                nonCorrelated.add(pred);
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
            characterStore.put(outerCol.token, dotPos + 1, outerCol.token.length());
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

    private boolean isSimpleInnerChain(QueryModel topInner, QueryModel dataSourceLayer) {
        QueryModel m = topInner;
        while (m != null) {
            if (m.getGroupBy().size() > 0
                    || m.getSampleBy() != null
                    || m.isDistinct()
                    || m.getLimitHi() != null
                    || m.getLatestBy().size() > 0
                    || m.getUnionModel() != null
                    || hasAggregateFunctions(m)
                    || hasWindowColumns(m)) {
                return false;
            }
            if (m == dataSourceLayer) {
                break;
            }
            m = m.getNestedModel();
        }
        return true;
    }

    private void moveOuterRefPredsToJoinCriteria(QueryModel current, QueryModel outerRefJoinModel) {
        ExpressionNode where = current.getWhereClause();
        if (where == null || outerRefJoinModel.getAlias() == null) {
            return;
        }
        CharSequence outerRefAlias = outerRefJoinModel.getAlias().token;
        splitAndPredicates(where, innerJoinCorrelated);
        correlatedPreds.clear();
        nonCorrelatedPreds.clear();
        for (int i = 0, n = innerJoinCorrelated.size(); i < n; i++) {
            ExpressionNode pred = innerJoinCorrelated.getQuick(i);
            if (hasOuterRefLiteral(pred, outerRefAlias)) {
                correlatedPreds.add(pred);
            } else {
                nonCorrelatedPreds.add(pred);
            }
        }
        if (correlatedPreds.size() > 0) {
            outerRefJoinModel.setJoinCriteria(conjoin(correlatedPreds));
            outerRefJoinModel.setJoinType(QueryModel.JOIN_INNER);
            current.setWhereClause(conjoin(nonCorrelatedPreds));
        }
    }

    private void pushDownOuterRefs(
            QueryModel parent,
            QueryModel current,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            boolean isLeftJoin,
            ObjList<CharSequence> countColAliases,
            QueryModel outerRefJoinModel,
            QueryModel lateralJoinModel,
            int depth
    ) throws SqlException {
        if (parent == null) {
            groupingCols.clear();
            ObjList<CharSequence> mapKeys = outerToInnerAlias.keys();
            for (int ki = 0, kn = mapKeys.size(); ki < kn; ki++) {
                CharSequence key = mapKeys.getQuick(ki);
                if (key != null) {
                    CharSequence value = outerToInnerAlias.get(key);
                    if (value != null) {
                        groupingCols.add(expressionNodePool.next().of(
                                ExpressionNode.LITERAL, value, 0, 0
                        ));
                    }
                }
            }
        }

        // 1. LIMIT compensation
        if (current.getLimitHi() != null) {
            QueryModel wrapper = compensateLimit(current, depth);
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
            compensateSampleBy(current);
        }
        if (hasGroupBy || hasAggregates) {
            compensateAggregate(current, isLeftJoin, countColAliases);
        }
        if (hasWindowColumns(current)) {
            compensateWindow(current);
        }
        if (current.isDistinct()) {
            compensateDistinct(current);
        }
        if (current.getLatestBy().size() > 0) {
            compensateLatestBy(current);
        }

        // 4. Rewrite current layer correlated references
        rewriteSelectExpressions(current, outerToInnerAlias, depth);
        rewriteOrderByExpressions(current, outerToInnerAlias, depth);
        rewriteGroupByExpressions(current, outerToInnerAlias, depth);
        if (current.getSampleBy() != null
                && hasCorrelatedExprAtDepth(current.getSampleBy(), depth)) {
            current.setSampleBy(
                    rewriteOuterRefs(current.getSampleBy(), outerToInnerAlias));
        }
        ObjList<ExpressionNode> latestBy = current.getLatestBy();
        for (int li = 0, ln = latestBy.size(); li < ln; li++) {
            ExpressionNode lb = latestBy.getQuick(li);
            if (hasCorrelatedExprAtDepth(lb, depth)) {
                latestBy.setQuick(li,
                        rewriteOuterRefs(lb, outerToInnerAlias));
            }
        }

        if (current.getWhereClause() != null
                && hasCorrelatedExprAtDepth(current.getWhereClause(), depth)) {
            current.setWhereClause(
                    rewriteOuterRefs(current.getWhereClause(), outerToInnerAlias));
        }

        // 5. Terminate or recurse
        if (current.getTableNameExpr() != null) {
            terminateHere(current, outerRefJoinModel, outerToInnerAlias, depth);
        } else if (current.getJoinModels().size() > 1) {
            if (canPushThroughJoins(current, depth)) {
                // Neumann-style push: handle each join model independently
                for (int ji = 1, jn = current.getJoinModels().size(); ji < jn; ji++) {
                    QueryModel jm = current.getJoinModels().getQuick(ji);
                    if (isLateralJoin(jm.getJoinType()) || jm.getNestedModel() == null) {
                        continue;
                    }
                    boolean isJmCorrelated = jm.getNestedModel().isCorrelatedAtDepth(depth);
                    boolean isDelimRequiredForJoinType = jm.getJoinType() == QueryModel.JOIN_RIGHT_OUTER
                            || jm.getJoinType() == QueryModel.JOIN_FULL_OUTER;

                    if (isJmCorrelated || isDelimRequiredForJoinType) {
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
                        subCountColAliases.clear();
                        pushDownOuterRefs(
                                null, jm.getNestedModel(), outerToInnerAlias, false,
                                subCountColAliases, clonedOuterRef, jm, depth
                        );

                        ExpressionNode alignCriteria = jm.getJoinCriteria();
                        QueryModel jmTop = jm.getNestedModel();
                        CharSequence jmAlias = jm.getAlias() != null ? jm.getAlias().token : null;
                        for (int gc = 0, gcn = groupingCols.size(); gc < gcn; gc++) {
                            ExpressionNode gcol = groupingCols.getQuick(gc);
                            ExpressionNode gcolForSub = ExpressionNode.deepClone(expressionNodePool, gcol);
                            CharSequence subAlias = ensureColumnInSelect(jmTop, gcolForSub, gcol.token);
                            ExpressionNode gcolForMain = ExpressionNode.deepClone(expressionNodePool, gcol);
                            CharSequence mainAlias = ensureColumnInSelect(current, gcolForMain, gcol.token);

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

                        if (jm.getJoinType() == QueryModel.JOIN_CROSS) {
                            jm.setJoinType(QueryModel.JOIN_INNER);
                        }
                    }
                }

                pushDownOuterRefs(
                        current, current.getNestedModel(), outerToInnerAlias, isLeftJoin,
                        countColAliases, outerRefJoinModel, lateralJoinModel, depth
                );
            } else {
                terminateHere(current, outerRefJoinModel, outerToInnerAlias, depth);
            }
        } else if (current.getNestedModel() != null) {
            if (current.getUnionModel() != null) {
                terminateHere(current, outerRefJoinModel, outerToInnerAlias, depth);
            } else if (current.getNestedModel().isCorrelatedAtDepth(depth)) {
                pushDownOuterRefs(
                        current, current.getNestedModel(), outerToInnerAlias, isLeftJoin,
                        countColAliases, outerRefJoinModel, lateralJoinModel, depth
                );
            } else {
                terminateHere(current, outerRefJoinModel, outerToInnerAlias, depth);
            }
        }
    }

    private void pushMatchingPredicates(
            QueryModel outerRefBase,
            ObjList<ExpressionNode> predicates,
            boolean isSingleSource
    ) {
        if (outerRefBase == null) {
            return;
        }
        CharSequence baseAlias = outerRefBase.getAlias() != null
                ? outerRefBase.getAlias().token
                : (outerRefBase.getTableNameExpr() != null ? outerRefBase.getTableNameExpr().token : null);
        if (baseAlias == null) {
            return;
        }

        LowerCaseCharSequenceObjHashMap<QueryColumn> columnMap =
                isSingleSource ? outerRefBase.getAliasToColumnMap() : null;
        for (int i = 0, n = predicates.size(); i < n; i++) {
            ExpressionNode pred = predicates.getQuick(i);
            if (allLiteralsBelongTo(pred, baseAlias, columnMap)) {
                ExpressionNode cloned = ExpressionNode.deepClone(expressionNodePool, pred);
                ExpressionNode existing = outerRefBase.getWhereClause();
                outerRefBase.setWhereClause(
                        existing != null ? createBinaryOp("and", existing, cloned) : cloned
                );
            }
        }
    }

    private void pushOuterWhereToRefBases(QueryModel outerModel, QueryModel outerRefSubquery) {
        ExpressionNode where = outerModel.getWhereClause();
        if (where == null) {
            return;
        }

        splitAndPredicates(where, nonCorrelatedPreds);

        boolean isSingleSource = outerRefSubquery.getJoinModels().size() == 1;
        pushMatchingPredicates(outerRefSubquery.getNestedModel(), nonCorrelatedPreds, isSingleSource);
        for (int i = 1, n = outerRefSubquery.getJoinModels().size(); i < n; i++) {
            pushMatchingPredicates(outerRefSubquery.getJoinModels().getQuick(i), nonCorrelatedPreds, false);
        }
    }

    private CharSequence qualifyWithAlias(CharSequence alias, CharSequence column) {
        if (alias != null) {
            characterStore.newEntry();
            characterStore.put(alias).put('.').put(column);
            return characterStore.toImmutable();
        }
        return column;
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
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth
    ) {
        ObjList<ExpressionNode> groupBy = inner.getGroupBy();
        for (int i = 0, n = groupBy.size(); i < n; i++) {
            ExpressionNode expr = groupBy.getQuick(i);
            if (hasCorrelatedExprAtDepth(expr, depth)) {
                groupBy.setQuick(i,
                        rewriteOuterRefs(expr, outerToInnerAlias));
            }
        }
    }

    private void rewriteOrderByExpressions(
            QueryModel inner,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth
    ) {
        ObjList<ExpressionNode> orderBy = inner.getOrderBy();
        for (int i = 0, n = orderBy.size(); i < n; i++) {
            ExpressionNode expr = orderBy.getQuick(i);
            if (hasCorrelatedExprAtDepth(expr, depth)) {
                orderBy.setQuick(i,
                        rewriteOuterRefs(expr, outerToInnerAlias));
            }
        }
    }

    private ExpressionNode rewriteOuterRefs(
            ExpressionNode node,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias
    ) {
        if (node == null) {
            return null;
        }
        if (node.type == ExpressionNode.QUERY && node.queryModel != null) {
            rewriteOuterRefsInSubquery(node.queryModel, outerToInnerAlias);
            return node;
        }
        if (node.type == ExpressionNode.LITERAL) {
            CharSequence mapped = outerToInnerAlias.get(node.token);
            if (mapped != null) {
                return expressionNodePool.next().of(
                        ExpressionNode.LITERAL, mapped, node.precedence, node.position
                );
            }
            // Unqualified token (e.g., "x"): find matching qualified key ("t1.x")
            if (Chars.indexOf(node.token, '.') < 0) {
                ObjList<CharSequence> keys = outerToInnerAlias.keys();
                for (int i = 0, n = keys.size(); i < n; i++) {
                    CharSequence key = keys.getQuick(i);
                    if (key != null) {
                        int kDot = Chars.indexOf(key, '.');
                        if (kDot > 0 && Chars.equalsIgnoreCase(
                                node.token,
                                key.subSequence(kDot + 1, key.length()))) {
                            mapped = outerToInnerAlias.get(key);
                            if (mapped != null) {
                                return expressionNodePool.next().of(
                                        ExpressionNode.LITERAL, mapped, node.precedence, node.position
                                );
                            }
                        }
                    }
                }
            }
            return node;
        }
        node.lhs = rewriteOuterRefs(node.lhs, outerToInnerAlias);
        node.rhs = rewriteOuterRefs(node.rhs, outerToInnerAlias);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            node.args.setQuick(i, rewriteOuterRefs(
                    node.args.getQuick(i), outerToInnerAlias));
        }
        if (node.windowExpression != null) {
            ObjList<ExpressionNode> partitionBy = node.windowExpression.getPartitionBy();
            for (int i = 0, n = partitionBy.size(); i < n; i++) {
                partitionBy.setQuick(i, rewriteOuterRefs(
                        partitionBy.getQuick(i), outerToInnerAlias));
            }
            ObjList<ExpressionNode> winOrderBy = node.windowExpression.getOrderBy();
            for (int i = 0, n = winOrderBy.size(); i < n; i++) {
                winOrderBy.setQuick(i, rewriteOuterRefs(
                        winOrderBy.getQuick(i), outerToInnerAlias));
            }
        }
        return node;
    }

    private void rewriteOuterRefsInSubquery(
            QueryModel subquery,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias
    ) {
        ObjList<QueryColumn> cols = subquery.getBottomUpColumns();
        for (int i = 0, n = cols.size(); i < n; i++) {
            QueryColumn col = cols.getQuick(i);
            ExpressionNode ast = col.getAst();
            ExpressionNode rewritten = rewriteOuterRefs(ast, outerToInnerAlias);
            if (rewritten != ast) {
                col.of(col.getAlias(), rewritten);
            }
        }
        if (subquery.getWhereClause() != null) {
            subquery.setWhereClause(
                    rewriteOuterRefs(subquery.getWhereClause(), outerToInnerAlias)
            );
        }
        ObjList<ExpressionNode> orderBy = subquery.getOrderBy();
        for (int i = 0, n = orderBy.size(); i < n; i++) {
            orderBy.setQuick(i,
                    rewriteOuterRefs(orderBy.getQuick(i), outerToInnerAlias));
        }
        ObjList<ExpressionNode> groupBy = subquery.getGroupBy();
        for (int i = 0, n = groupBy.size(); i < n; i++) {
            groupBy.setQuick(i,
                    rewriteOuterRefs(groupBy.getQuick(i), outerToInnerAlias));
        }
        if (subquery.getSampleBy() != null) {
            subquery.setSampleBy(
                    rewriteOuterRefs(subquery.getSampleBy(), outerToInnerAlias));
        }
        ObjList<ExpressionNode> latestBy = subquery.getLatestBy();
        for (int i = 0, n = latestBy.size(); i < n; i++) {
            latestBy.setQuick(i,
                    rewriteOuterRefs(latestBy.getQuick(i), outerToInnerAlias));
        }
        if (subquery.getLimitLo() != null) {
            subquery.setLimit(
                    rewriteOuterRefs(subquery.getLimitLo(), outerToInnerAlias),
                    subquery.getLimitHi() != null
                            ? rewriteOuterRefs(subquery.getLimitHi(), outerToInnerAlias)
                            : null
            );
        }
        for (int i = 1, n = subquery.getJoinModels().size(); i < n; i++) {
            QueryModel jm = subquery.getJoinModels().getQuick(i);
            if (jm.getJoinCriteria() != null) {
                jm.setJoinCriteria(
                        rewriteOuterRefs(jm.getJoinCriteria(), outerToInnerAlias)
                );
            }
        }
        if (subquery.getNestedModel() != null) {
            rewriteOuterRefsInSubquery(subquery.getNestedModel(), outerToInnerAlias);
        }
        if (subquery.getUnionModel() != null) {
            rewriteOuterRefsInSubquery(subquery.getUnionModel(), outerToInnerAlias);
        }
    }

    private void rewriteSelectExpressions(
            QueryModel inner,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth
    ) {
        for (int i = 0, n = inner.getBottomUpColumns().size(); i < n; i++) {
            QueryColumn col = inner.getBottomUpColumns().getQuick(i);
            ExpressionNode ast = col.getAst();
            if (hasCorrelatedExprAtDepth(ast, depth)) {
                col.of(col.getAlias(),
                        rewriteOuterRefs(ast, outerToInnerAlias));
            }
        }
    }

    private void scanWhereForOuterRefEqualities(
            ExpressionNode expr,
            CharSequence outerRefAlias,
            LowerCaseCharSequenceObjHashMap<CharSequence> result
    ) {
        splitAndPredicates(expr, innerJoinNonCorrelated);
        for (int i = 0, n = innerJoinNonCorrelated.size(); i < n; i++) {
            ExpressionNode node = innerJoinNonCorrelated.getQuick(i);
            if (node.type == ExpressionNode.OPERATION
                    && Chars.equals(node.token, "=")) {
                if (isOuterRefToken(node.lhs, outerRefAlias) && isSimpleColumnRef(node.rhs)) {
                    result.put(node.lhs.token, node.rhs.token);
                } else if (isOuterRefToken(node.rhs, outerRefAlias) && isSimpleColumnRef(node.lhs)) {
                    result.put(node.rhs.token, node.lhs.token);
                }
            }
        }
    }

    private void setupOuterRefDataSource(
            QueryModel outerRefSubquery,
            QueryModel outerModel,
            ObjList<LowerCaseCharSequenceIntHashMap> corrCols
    ) throws SqlException {
        int maxIndex = -1;
        int refCount = 0;
        for (int j = 0, m = corrCols.size(); j < m; j++) {
            LowerCaseCharSequenceIntHashMap colSet = corrCols.getQuick(j);
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
            registerDataSourceAlias(outerRefSubquery, outerRefBase, 0);
            return;
        }

        for (int i = 0, n = outerModel.getJoinModels().size(); i <= maxIndex && i < n; i++) {
            QueryModel outerJm = outerModel.getJoinModels().getQuick(i);
            QueryModel outerRefBase = createOuterRefBase(outerJm);
            if (outerRefSubquery.getNestedModel() == null) {
                outerRefSubquery.setNestedModel(outerRefBase);
                registerDataSourceAlias(outerRefSubquery, outerRefBase, 0);
            } else {
                if (outerJm.getJoinCriteria() != null) {
                    outerRefBase.setJoinType(outerJm.getJoinType());
                    outerRefBase.setJoinCriteria(
                            ExpressionNode.deepClone(expressionNodePool, outerJm.getJoinCriteria())
                    );
                } else {
                    outerRefBase.setJoinType(QueryModel.JOIN_CROSS);
                }
                int jmIndex = outerRefSubquery.getJoinModels().size();
                outerRefSubquery.getJoinModels().add(outerRefBase);
                registerDataSourceAlias(outerRefSubquery, outerRefBase, jmIndex);
            }
        }
    }

    private void splitAndPredicates(ExpressionNode expr, ObjList<ExpressionNode> result) {
        result.clear();
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
                    result.add(node);
                }
                node = sqlNodeStack.isEmpty() ? null : sqlNodeStack.pop();
            }
        }
    }

    private void terminateHere(
            QueryModel current,
            QueryModel outerRefJoinModel,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth
    ) throws SqlException {
        current.getJoinModels().add(outerRefJoinModel);
        if (outerRefJoinModel.getAlias() != null) {
            current.addModelAliasIndex(outerRefJoinModel.getAlias(), current.getJoinModels().size() - 1);
        }
        correlatedPreds.clear();
        extractCorrelatedFromInnerJoins(current, correlatedPreds, depth);
        for (int j = 0, m = correlatedPreds.size(); j < m; j++) {
            ExpressionNode rewritten = rewriteOuterRefs(
                    correlatedPreds.getQuick(j), outerToInnerAlias);
            ExpressionNode w = current.getWhereClause();
            current.setWhereClause(w != null ? createBinaryOp("and", w, rewritten) : rewritten);
        }

        moveOuterRefPredsToJoinCriteria(current, outerRefJoinModel);
        if (current.getUnionModel() != null) {
            compensateSetOp(current, outerToInnerAlias, depth);
        }

        compensateInnerJoins(current, outerToInnerAlias, depth);
        decorrelateJoinModelSubqueries(current, outerToInnerAlias, depth);
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
