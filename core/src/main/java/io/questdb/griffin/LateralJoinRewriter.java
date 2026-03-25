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

import java.util.ArrayDeque;
import java.util.Objects;

import static io.questdb.griffin.model.QueryModel.isLateralJoin;

class LateralJoinRewriter {

    private static final String OUTER_REF_PREFIX = "__qdb_outer_ref__";
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
        System.err.println("=== BEFORE ===");
        dumpModel(model, 0, "PRE");
        analyzeCorrelation(model, 0);
        decorrelate(model, 0);
        tryEliminateOuterRefs(model);
        System.err.println("=== AFTER ===");
        dumpModel(model, 0, "ROOT");
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
            return Chars.startsWith(node.token, outerRefAlias);
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
                && Chars.startsWith(node.token, outerRefAlias);
    }

    private static boolean isSimpleColumnRef(ExpressionNode node) {
        return node != null && node.type == ExpressionNode.LITERAL;
    }

    private static boolean isWildcard(ObjList<QueryColumn> cols) {
        for (int i = 0, n = cols.size(); i < n; i++) {
            if (cols.getQuick(i).getAst().isWildcard()) {
                return true;
            }
        }
        return false;
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

    private static CharSequence unqualify(CharSequence token) {
        int dotPos = Chars.indexOf(token, '.');
        return dotPos > 0 ? token.subSequence(dotPos + 1, token.length()) : token;
    }

    private void addColumnToOuterRefSelect(CharSequence outerRefAlias, QueryModel outerRefSubquery, ExpressionNode outerCol) {
        int dotPos = Chars.indexOf(outerCol.token, '.');
        CharSequence colName = dotPos > 0
                ? outerCol.token.subSequence(dotPos + 1, outerCol.token.length())
                : outerCol.token;
        final CharacterStoreEntry characterStoreEntry = characterStore.newEntry();
        characterStoreEntry.put(outerRefAlias).put("_").put(colName);
        CharSequence alias = createColumnAlias(characterStoreEntry.toImmutable(), outerRefSubquery);
        ExpressionNode ref = expressionNodePool.next().of(
                ExpressionNode.LITERAL, outerCol.token, 0, outerCol.position
        );
        QueryColumn qc = queryColumnPool.next().of(alias, ref);
        outerToInnerAlias.put(outerCol.token, alias);
        outerRefSubquery.addBottomUpColumnIfNotExists(qc);
    }

    private void addGroupingColsToEmbeddedWindows(ExpressionNode node, CharSequence outerRefAlias) {
        if (node == null) {
            return;
        }
        if (node.windowExpression != null) {
            addGroupingColsToPartitionBy(node.windowExpression.getPartitionBy(), outerRefAlias);
        }
        addGroupingColsToEmbeddedWindows(node.lhs, outerRefAlias);
        addGroupingColsToEmbeddedWindows(node.rhs, outerRefAlias);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            addGroupingColsToEmbeddedWindows(node.args.getQuick(i), outerRefAlias);
        }
    }

    private void addGroupingColsToPartitionBy(ObjList<ExpressionNode> partitionBy, CharSequence outerRefAlias) {
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
                ExpressionNode cloned = ExpressionNode.deepClone(expressionNodePool, gcol);
                if (outerRefAlias != null) {
                    CharacterStoreEntry cse = characterStore.newEntry();
                    cse.put(outerRefAlias).put('.').put(gcol.token);
                    cloned.token = cse.toImmutable();
                }
                partitionBy.add(cloned);
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

    private boolean canEliminateOuterRefInBranch(QueryModel branchTop, CharSequence targetAlias) {
        QueryModel dataSourceLayer = null;
        QueryModel current = branchTop;
        while (current != null) {
            for (int j = 1, jn = current.getJoinModels().size(); j < jn; j++) {
                QueryModel jm = current.getJoinModels().getQuick(j);
                if (jm.getAlias() != null
                        && Chars.equalsIgnoreCase(jm.getAlias().token, targetAlias)) {
                    dataSourceLayer = current;
                    break;
                }
            }
            if (dataSourceLayer != null) {
                break;
            }
            current = current.getNestedModel();
        }
        if (dataSourceLayer == null) {
            return true;
        }

        if (dataSourceLayer.getBottomUpColumns().size() == 0
                && dataSourceLayer != branchTop) {
            return false;
        }

        QueryModel outerRefJm = dataSourceLayer.getJoinModels().getQuick(
                dataSourceLayer.getModelAliasIndex(targetAlias, 0, targetAlias.length())
        );

        ExpressionNode joinCrit = outerRefJm.getJoinCriteria();
        if (joinCrit == null) {
            return true;
        }

        outerToInnerAlias.clear();
        scanWhereForOuterRefEqualities(joinCrit, targetAlias, outerToInnerAlias);
        ObjList<QueryColumn> outerRefCols = outerRefJm.getNestedModel().getBottomUpColumns();
        for (int j = 0, sz = outerRefCols.size(); j < sz; j++) {
            if (outerToInnerAlias.get(outerRefCols.getQuick(j).getAlias()) == null) {
                return isSimpleChain(branchTop, dataSourceLayer);
            }
        }
        return true;
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

    private void collectOuterRefAliases(QueryModel model, ObjList<ExpressionNode> result) {
        QueryModel m = model;
        while (m != null) {
            for (int j = 1, n = m.getJoinModels().size(); j < n; j++) {
                QueryModel jm = m.getJoinModels().getQuick(j);
                if (jm.getAlias() != null
                        && Chars.startsWith(jm.getAlias().token, OUTER_REF_PREFIX)) {
                    result.add(jm.getAlias());
                }
            }
            m = m.getNestedModel();
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
        if (limitHi == null && limitLo == null) {
            return current;
        }

        if (hasCorrelatedExprAtDepth(limitHi, depth)) {
            throw SqlException.position(limitHi.position)
                    .put("non-constant LIMIT in LATERAL join is not supported");
        }
        if (hasCorrelatedExprAtDepth(limitLo, depth)) {
            throw SqlException.position(limitLo.position)
                    .put("non-constant LIMIT in LATERAL join is not supported");
        }
        if (limitHi != null && limitHi.type != ExpressionNode.CONSTANT) {
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

        for (int i = 0, n = orderBySave.size(); i < n; i++) {
            rnWindowExpr.getOrderBy().add(
                    ExpressionNode.deepClone(expressionNodePool, orderBySave.getQuick(i)));
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

        ExpressionNode rnRef = expressionNodePool.next().of(
                ExpressionNode.LITERAL, rnAlias, 0, 0
        );

        ExpressionNode rnFilter;
        if (limitHi != null && limitLo != null) {
            ExpressionNode sum = createBinaryOp("+",
                    ExpressionNode.deepClone(expressionNodePool, limitLo),
                    ExpressionNode.deepClone(expressionNodePool, limitHi));
            ExpressionNode upperBound = createBinaryOp("<=",
                    ExpressionNode.deepClone(expressionNodePool, rnRef), sum);
            ExpressionNode lowerBound = createBinaryOp(">",
                    ExpressionNode.deepClone(expressionNodePool, rnRef),
                    ExpressionNode.deepClone(expressionNodePool, limitLo));
            rnFilter = createBinaryOp("and", lowerBound, upperBound);
        } else {
            rnFilter = createBinaryOp("<=",
                    ExpressionNode.deepClone(expressionNodePool, rnRef),
                    ExpressionNode.deepClone(expressionNodePool, Objects.requireNonNullElse(limitHi, limitLo)));
        }

        QueryModel filterModel = queryModelPool.next();
        filterModel.setNestedModel(current);
        filterModel.setNestedModelIsSubQuery(true);
        filterModel.setWhereClause(rnFilter);

        QueryModel outerSelect = queryModelPool.next();
        outerSelect.setNestedModel(filterModel);
        outerSelect.setNestedModelIsSubQuery(true);
        copyColumnsExcept(current, outerSelect, rnAlias);

        return outerSelect;
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
            int depth,
            QueryModel outerRefJm
    ) throws SqlException {

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
                branchCorrelated.setQuick(j, rewriteOuterRefs(branchCorrelated.getQuick(j), outerToInnerAlias));
            }
            current.setWhereClause(conjoin(branchNonCorrelated));
            if (branchCorrelated.size() > 0) {
                branchOuterRef.setJoinCriteria(conjoin(branchCorrelated));
                branchOuterRef.setJoinType(QueryModel.JOIN_INNER);
            }

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
                compensateWindow(current, branchOuterRef.getAlias().token);
            }
            if (current.getLatestBy().size() > 0) {
                compensateLatestBy(current);
            }

            decorrelateJoinModelSubqueries(current, outerToInnerAlias, depth, branchOuterRef);
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
                    if (branchOuterRef.getJoinType() == QueryModel.JOIN_CROSS) {
                        current.getJoinModels().remove(branchOuterRef);
                        if (branchOuterRef.getAlias() != null) {
                            current.getModelAliasIndexes().removeEntry(branchOuterRef.getAlias().token);
                        }
                    }
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
            if (current.getLimitHi() != null || current.getLimitLo() != null) {
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

    private void compensateWindow(QueryModel inner, CharSequence outerRefAlias) throws SqlException {
        for (int i = 0, n = inner.getBottomUpColumns().size(); i < n; i++) {
            QueryColumn col = inner.getBottomUpColumns().getQuick(i);
            if (col instanceof WindowExpression we) {
                addGroupingColsToPartitionBy(we.getPartitionBy(), outerRefAlias);
            } else {
                addGroupingColsToEmbeddedWindows(col.getAst(), outerRefAlias);
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
                QueryColumn wrapperCol = queryColumnPool.next().of(col.getAlias(), ref, col.isIncludeIntoWildcard());
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

                // bottom up
                decorrelate(topInner, lateralDepth + 1);

                boolean isLeft = joinModel.getJoinType() == QueryModel.JOIN_LATERAL_LEFT;

                outerCols.clear();
                buildOuterColsFromCorrelatedColumns(joinModel, model, outerCols);
                if (outerCols.size() == 0) {
                    joinModel.setJoinType(toDegradedJoinType(joinModel.getJoinType()));
                    continue;
                }

                int depth = lateralDepth + 1;
                outerToInnerAlias.clear();
                QueryModel outerRefSubquery = queryModelPool.next();
                outerRefSubquery.setDistinct(true);
                characterStore.newEntry();
                characterStore.put(OUTER_REF_PREFIX).put(outerRefId++);
                CharSequence outerRefAlias = characterStore.toImmutable();

                for (int j = 0, m = outerCols.size(); j < m; j++) {
                    addColumnToOuterRefSelect(outerRefAlias, outerRefSubquery, outerCols.getQuick(j));
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

                // Push down outer refs
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

                // Degrade join type
                joinModel.setJoinType(toDegradedJoinType(joinModel.getJoinType()));

                ObjList<QueryColumn> topCols = topInner.getBottomUpColumns();
                for (int j = 0, m = topCols.size(); j < m; j++) {
                    QueryColumn tc = topCols.getQuick(j);
                    if (Chars.startsWith(tc.getAlias(), OUTER_REF_PREFIX)) {
                        tc.setIncludeIntoWildcard(false);
                    }
                }

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
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth,
            QueryModel outerRefJm
    ) throws SqlException {

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

    private void dumpModel(QueryModel m, int indent, String label) {
        if (m == null) return;
        String pad = " ".repeat(indent);
        System.err.println(pad + label + " @" + System.identityHashCode(m)
                + " table=" + (m.getTableNameExpr() != null ? m.getTableNameExpr().token : "null")
                + " alias=" + (m.getAlias() != null ? m.getAlias().token : "null")
                + " cols=" + m.getBottomUpColumns().size()
                + " joins=" + m.getJoinModels().size()
                + " where=" + (m.getWhereClause() != null ? m.getWhereClause() : "null")
                + (m.getLimitHi() != null ? " LIMIT=" + m.getLimitHi() : "")
                + " subQ=" + m.isNestedModelIsSubQuery());
        for (int i = 0, n = m.getBottomUpColumns().size(); i < n; i++) {
            QueryColumn c = m.getBottomUpColumns().getQuick(i);
            String extra = "";
            if (c instanceof WindowExpression we) {
                extra = " WIN(partBy=" + we.getPartitionBy().size() + " orderBy=" + we.getOrderBy().size() + ")";
            }
            System.err.println(pad + "  col[" + i + "] alias=" + c.getAlias() + " ast=" + c.getAst().token + extra);
        }
        for (int i = 1, n = m.getJoinModels().size(); i < n; i++) {
            QueryModel jm = m.getJoinModels().getQuick(i);
            System.err.println(pad + "  jm[" + i + "] type=" + jm.getJoinType() + " alias=" + (jm.getAlias() != null ? jm.getAlias().token : "null")
                    + " crit=" + (jm.getJoinCriteria() != null ? jm.getJoinCriteria() : "null"));
            dumpModel(jm.getNestedModel(), indent + 4, "JM");
        }
        dumpModel(m.getNestedModel(), indent + 2, "N");
        dumpModel(m.getUnionModel(), indent + 2, "U");
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

        if (colExpr.type == ExpressionNode.LITERAL) {
            CharSequence unqualifiedName = unqualify(colExpr.token);
            for (int i = 0, n = cols.size(); i < n; i++) {
                QueryColumn existing = cols.getQuick(i);
                ExpressionNode ast = existing.getAst();
                if (ast != null
                        && ast.type == ExpressionNode.LITERAL
                        && Chars.equalsIgnoreCase(unqualify(ast.token), unqualifiedName)) {
                    return existing.getAlias();
                }
            }
        }
        ExpressionNode cloned = ExpressionNode.deepClone(expressionNodePool, colExpr);
        CharSequence alias = createColumnAlias(preferredAlias, model);
        QueryColumn qc = queryColumnPool.next().of(alias, cloned);
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

    private ExpressionNode findOriginalOuterRef(
            CharSequence outerRefToken,
            ObjList<QueryColumn> outerRefCols
    ) {
        for (int k = 0, kn = outerRefCols.size(); k < kn; k++) {
            QueryColumn refCol = outerRefCols.getQuick(k);
            if (Chars.equalsIgnoreCase(refCol.getAlias(), outerRefToken)) {
                return refCol.getAst();
            }
        }
        return null;
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

    private boolean isSimpleChain(QueryModel branchTop, QueryModel dataSourceLayer) {
        QueryModel m = branchTop;
        while (m != null) {
            if (m.getGroupBy().size() > 0
                    || m.getSampleBy() != null
                    || m.isDistinct()
                    || m.getLimitHi() != null || m.getLimitLo() != null
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

    private ExpressionNode liftExpression(
            ExpressionNode node,
            CharSequence outerRefAlias,
            ObjList<QueryColumn> outerRefCols,
            CharSequence lateralAlias,
            QueryModel branchTop,
            QueryModel dataSourceLayer
    ) throws SqlException {
        if (node == null) {
            return null;
        }
        if (node.type == ExpressionNode.LITERAL) {
            if (Chars.startsWith(node.token, outerRefAlias)
                    && node.token.length() > outerRefAlias.length()) {
                ExpressionNode original = findOriginalOuterRef(node.token, outerRefCols);
                return ExpressionNode.deepClone(expressionNodePool, Objects.requireNonNullElse(original, node));
            }
            CharSequence colName = node.token;
            int dotPos = Chars.indexOf(colName, '.');
            if (dotPos > 0) {
                colName = colName.subSequence(dotPos + 1, colName.length());
            }
            CharSequence selectAlias;
            if (dataSourceLayer != null) {
                selectAlias = propagateColumnUp(colName, branchTop, dataSourceLayer);
            } else {
                ExpressionNode innerRef = expressionNodePool.next().of(
                        ExpressionNode.LITERAL, colName, node.precedence, node.position
                );
                selectAlias = ensureColumnInSelect(branchTop, innerRef, colName);
            }
            CharSequence qualified = qualifyWithAlias(lateralAlias, selectAlias);
            return expressionNodePool.next().of(
                    ExpressionNode.LITERAL, qualified, node.precedence, node.position
            );
        }
        ExpressionNode clone = expressionNodePool.next().of(
                node.type, node.token, node.precedence, node.position
        );
        clone.paramCount = node.paramCount;
        clone.lhs = liftExpression(node.lhs, outerRefAlias, outerRefCols, lateralAlias, branchTop, dataSourceLayer);
        clone.rhs = liftExpression(node.rhs, outerRefAlias, outerRefCols, lateralAlias, branchTop, dataSourceLayer);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            clone.args.add(liftExpression(node.args.getQuick(i), outerRefAlias, outerRefCols, lateralAlias, branchTop, dataSourceLayer));
        }
        return clone;
    }

    private void liftOuterRefExpressions(
            QueryModel branchTop,
            QueryModel selectModel,
            QueryModel joinLayer,
            CharSequence outerRefAlias,
            ObjList<QueryColumn> outerRefCols,
            CharSequence lateralAlias
    ) throws SqlException {
        ObjList<QueryColumn> innerCols = branchTop.getBottomUpColumns();
        boolean isOuterWildcard = isWildcard(selectModel.getBottomUpColumns());
        for (int j = innerCols.size() - 1; j >= 0; j--) {
            QueryColumn col = innerCols.getQuick(j);
            ExpressionNode ast = col.getAst();
            if (!hasOuterRefLiteral(ast, outerRefAlias)) {
                continue;
            }

            if (ast.type == ExpressionNode.LITERAL
                    && Chars.startsWith(col.getAlias(), OUTER_REF_PREFIX)) {
                continue;
            }
            ExpressionNode lifted = liftExpression(ast, outerRefAlias, outerRefCols, lateralAlias, branchTop, null);

            if (isOuterWildcard) {
                selectModel.addBottomUpColumnIfNotExists(
                        queryColumnPool.next().of(col.getAlias(), lifted)
                );
            } else if (lateralAlias != null) {
                CharSequence qualifiedRef = qualifyWithAlias(lateralAlias, col.getAlias());
                replaceColumnRefInModel(selectModel, qualifiedRef, lifted);
                if (joinLayer != null && joinLayer != selectModel) {
                    ExpressionNode aliasRef = expressionNodePool.next().of(
                            ExpressionNode.LITERAL, col.getAlias(), 0, 0
                    );
                    replaceColumnRefInModel(joinLayer, qualifiedRef, aliasRef);
                }
            }
            branchTop.removeColumn(j);
        }
    }

    private CharSequence lookupOuterRefAlias(
            CharSequence token,
            CharSequence outerRefAlias,
            LowerCaseCharSequenceObjHashMap<CharSequence> aliasMap
    ) {
        CharSequence result = aliasMap.get(token);
        if (result != null) {
            return result;
        }
        int dot = Chars.indexOf(token, '.');
        if (dot > 0 && Chars.startsWith(token, outerRefAlias)) {
            CharSequence unqualified = token.subSequence(dot + 1, token.length());
            return aliasMap.get(unqualified);
        }
        return null;
    }

    private CharSequence propagateColumnUp(
            CharSequence columnName,
            QueryModel current,
            QueryModel dataSourceLayer
    ) throws SqlException {
        if (current == dataSourceLayer || current == null) {
            return columnName;
        }

        CharSequence alias = propagateColumnUp(columnName, current.getNestedModel(), dataSourceLayer);
        ExpressionNode colNode = expressionNodePool.next().of(
                ExpressionNode.LITERAL, alias, 0, 0
        );
        return ensureColumnInSelect(current, colNode, alias);
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
        if (current.getLimitHi() != null || current.getLimitLo() != null) {
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
            compensateWindow(current, outerRefJoinModel.getAlias().token);
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

        // 5. Neumann-style push: handle each branch independently
        boolean hasJoins = current.getJoinModels().size() > 1;
        for (int ji = 0, jn = current.getJoinModels().size(); ji < jn; ji++) {
            QueryModel jm = current.getJoinModels().getQuick(ji);
            QueryModel jmNested = ji == 0 ? current.getNestedModel() : jm.getNestedModel();

            if (ji == 0 && !hasJoins) {
                if (current.getNestedModel() != null) {
                    pushDownOuterRefs(
                            current, current.getNestedModel(), outerToInnerAlias, isLeftJoin,
                            countColAliases, outerRefJoinModel, lateralJoinModel, depth
                    );
                } else {
                    terminateHere(current, outerRefJoinModel, outerToInnerAlias, depth);
                }
                if (current.getUnionModel() != null) {
                    compensateSetOp(current, outerToInnerAlias, depth, outerRefJoinModel);
                }
                break;
            }

            if (ji == 0) {
                // Main chain: use original outer ref
                if (current.getNestedModel() != null) {
                    pushDownOuterRefs(
                            current, current.getNestedModel(), outerToInnerAlias, isLeftJoin,
                            countColAliases, outerRefJoinModel, lateralJoinModel, depth
                    );
                } else {
                    terminateHere(current, outerRefJoinModel, outerToInnerAlias, depth);
                    jn = current.getJoinModels().size();
                }
                if (current.getUnionModel() != null) {
                    compensateSetOp(current, outerToInnerAlias, depth, outerRefJoinModel);
                }
            } else {
                // Join branch (ji > 0)
                boolean hasCorrelatedCriteria = jm.getJoinCriteria() != null
                        && hasCorrelatedExprAtDepth(jm.getJoinCriteria(), depth);

                if (hasCorrelatedCriteria) {
                    ExpressionNode jmCrit = jm.getJoinCriteria();
                    splitAndPredicates(jmCrit, innerJoinCorrelated);
                    ExpressionNode remainingJoinCrit = null;
                    for (int ci = 0, cn = innerJoinCorrelated.size(); ci < cn; ci++) {
                        ExpressionNode pred = innerJoinCorrelated.getQuick(ci);
                        if (hasCorrelatedExprAtDepth(pred, depth)) {
                            ExpressionNode rewritten = rewriteOuterRefs(pred, outerToInnerAlias);
                            if (jmNested != null) {
                                ExpressionNode w = jmNested.getWhereClause();
                                jmNested.setWhereClause(w != null ? createBinaryOp("and", w, rewritten) : rewritten);
                            } else {
                                remainingJoinCrit = remainingJoinCrit == null
                                        ? rewritten : createBinaryOp("and", remainingJoinCrit, rewritten);
                            }
                        } else {
                            remainingJoinCrit = remainingJoinCrit == null
                                    ? pred : createBinaryOp("and", remainingJoinCrit, pred);
                        }
                    }
                    jm.setJoinCriteria(remainingJoinCrit);
                }

                if (jmNested == null) {
                    continue;
                }
                boolean isJmCorrelated = jmNested.isCorrelatedAtDepth(depth);
                boolean isDelimRequiredForJoinType = jm.getJoinType() == QueryModel.JOIN_RIGHT_OUTER
                        || jm.getJoinType() == QueryModel.JOIN_FULL_OUTER;
                if (!(isJmCorrelated || isDelimRequiredForJoinType || hasCorrelatedCriteria)) {
                    continue;
                }

                characterStore.newEntry();
                characterStore.put(OUTER_REF_PREFIX).put(outerRefId++);
                CharSequence cloneAlias = characterStore.toImmutable();

                QueryModel cloneSubquery = queryModelPool.next();
                cloneSubquery.setDistinct(true);
                QueryModel origSubquery = outerRefJoinModel.getNestedModel();
                cloneSubquery.setNestedModel(origSubquery.getNestedModel());
                LowerCaseCharSequenceObjHashMap<CharSequence> origColNameAlias = origSubquery.getColumnNameToAliasMap();
                ObjList<CharSequence> origColKeys = origColNameAlias.keys();
                for (int ck = 0, ckn = origColKeys.size(); ck < ckn; ck++) {
                    CharSequence key = origColKeys.getQuick(ck);
                    cloneSubquery.getColumnNameToAliasMap().put(key, origColNameAlias.get(key));
                }
                ObjList<QueryColumn> origCols = origSubquery.getBottomUpColumns();
                for (int oc = 0, ocn = origCols.size(); oc < ocn; oc++) {
                    QueryColumn origCol = origCols.getQuick(oc);
                    ExpressionNode origAst = origCol.getAst();
                    CharSequence colName = unqualify(origAst.token);
                    final CharacterStoreEntry cse = characterStore.newEntry();
                    cse.put(cloneAlias).put("_").put(colName);
                    CharSequence newColAlias = cse.toImmutable();
                    ExpressionNode ref = expressionNodePool.next().of(
                            ExpressionNode.LITERAL, origAst.token, 0, origAst.position
                    );
                    cloneSubquery.addBottomUpColumnIfNotExists(queryColumnPool.next().of(newColAlias, ref));
                }

                QueryModel clonedOuterRef = queryModelPool.next();
                clonedOuterRef.setNestedModel(cloneSubquery);
                clonedOuterRef.setNestedModelIsSubQuery(true);
                ExpressionNode cloneAliasExpr = expressionNodePool.next().of(
                        ExpressionNode.LITERAL, cloneAlias, 0, 0
                );
                clonedOuterRef.setAlias(cloneAliasExpr);
                clonedOuterRef.setJoinType(QueryModel.JOIN_CROSS);

                // Save outerToInnerAlias values, replace with clone aliases (zero GC: reuse subCountColAliases)
                subCountColAliases.clear();
                ObjList<CharSequence> oKeys = outerToInnerAlias.keys();
                for (int ok = 0, okn = oKeys.size(); ok < okn; ok++) {
                    CharSequence key = oKeys.getQuick(ok);
                    if (key == null) continue;
                    CharSequence origValue = outerToInnerAlias.get(key);
                    subCountColAliases.add(origValue);
                    CharSequence colName = unqualify(key);
                    final CharacterStoreEntry cse = characterStore.newEntry();
                    cse.put(cloneAlias).put("_").put(colName);
                    outerToInnerAlias.put(key, cse.toImmutable());
                }

                pushDownOuterRefs(
                        null, jmNested, outerToInnerAlias, false,
                        countColAliases, clonedOuterRef, jm, depth
                );

                ExpressionNode alignCriteria = jm.getJoinCriteria();
                QueryModel jmTop = jm.getNestedModel();
                CharSequence jmAlias = jm.getAlias() != null ? jm.getAlias().token : null;
                int savedIdx = 0;
                for (int ok = 0, okn = oKeys.size(); ok < okn; ok++) {
                    CharSequence key = oKeys.getQuick(ok);
                    if (key == null) {
                        continue;
                    }

                    CharSequence cloneColAlias = outerToInnerAlias.get(key);
                    CharSequence mainColAlias = subCountColAliases.getQuick(savedIdx);

                    ExpressionNode cloneColNode = expressionNodePool.next().of(
                            ExpressionNode.LITERAL, cloneColAlias, 0, 0
                    );
                    CharSequence subAlias = ensureColumnInSelect(jmTop, cloneColNode, cloneColAlias);
                    CharSequence qualifiedSubCol;
                    if (jmAlias != null) {
                        characterStore.newEntry();
                        characterStore.put(jmAlias).put('.').put(subAlias);
                        qualifiedSubCol = characterStore.toImmutable();
                    } else {
                        qualifiedSubCol = subAlias;
                    }
                    ExpressionNode mainColNode = expressionNodePool.next().of(
                            ExpressionNode.LITERAL, mainColAlias, 0, 0
                    );
                    CharSequence mainAlias = ensureColumnInSelect(current, mainColNode, mainColAlias);
                    ExpressionNode subRef = expressionNodePool.next().of(
                            ExpressionNode.LITERAL, qualifiedSubCol, 0, 0
                    );
                    ExpressionNode mainRef = expressionNodePool.next().of(
                            ExpressionNode.LITERAL, mainAlias, 0, 0
                    );
                    ExpressionNode eq = createBinaryOp("=", subRef, mainRef);
                    alignCriteria = alignCriteria == null ? eq : createBinaryOp("and", alignCriteria, eq);
                    savedIdx++;
                }
                jm.setJoinCriteria(alignCriteria);
                if (jm.getJoinType() == QueryModel.JOIN_CROSS) {
                    jm.setJoinType(QueryModel.JOIN_INNER);
                }

                // Restore outerToInnerAlias values
                savedIdx = 0;
                for (int ok = 0, okn = oKeys.size(); ok < okn; ok++) {
                    CharSequence key = oKeys.getQuick(ok);
                    if (key == null) continue;
                    outerToInnerAlias.put(key, subCountColAliases.getQuick(savedIdx));
                    savedIdx++;
                }
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

    private ExpressionNode rebuildOnFromOuterRefCriteria(
            ObjList<ExpressionNode> predicates,
            CharSequence outerRefAlias,
            ObjList<QueryColumn> outerRefCols,
            QueryModel branchTop,
            QueryModel dataSourceLayer,
            CharSequence lateralAlias
    ) throws SqlException {
        ExpressionNode result = null;
        for (int j = 0, sz = predicates.size(); j < sz; j++) {
            ExpressionNode lifted = liftExpression(
                    predicates.getQuick(j), outerRefAlias, outerRefCols, lateralAlias, branchTop, dataSourceLayer
            );
            result = result == null ? lifted : createBinaryOp("and", result, lifted);
        }
        return result;
    }

    private ExpressionNode replaceColumnRef(
            ExpressionNode node,
            CharSequence qualifiedRef,
            ExpressionNode replacement
    ) {
        if (node == null) {
            return null;
        }
        if (node.type == ExpressionNode.LITERAL
                && Chars.equalsIgnoreCase(node.token, qualifiedRef)) {
            return ExpressionNode.deepClone(expressionNodePool, replacement);
        }
        ExpressionNode newLhs = replaceColumnRef(node.lhs, qualifiedRef, replacement);
        ExpressionNode newRhs = replaceColumnRef(node.rhs, qualifiedRef, replacement);
        if (newLhs == node.lhs && newRhs == node.rhs) {
            boolean isArgsChanged = false;
            for (int i = 0, n = node.args.size(); i < n; i++) {
                if (replaceColumnRef(node.args.getQuick(i), qualifiedRef, replacement) != node.args.getQuick(i)) {
                    isArgsChanged = true;
                    break;
                }
            }
            if (!isArgsChanged) {
                return node;
            }
        }
        ExpressionNode clone = expressionNodePool.next().of(
                node.type, node.token, node.precedence, node.position
        );
        clone.paramCount = node.paramCount;
        clone.lhs = newLhs;
        clone.rhs = newRhs;
        for (int i = 0, n = node.args.size(); i < n; i++) {
            clone.args.add(replaceColumnRef(node.args.getQuick(i), qualifiedRef, replacement));
        }
        return clone;
    }

    private void replaceColumnRefInModel(
            QueryModel outerModel,
            CharSequence qualifiedRef,
            ExpressionNode replacement
    ) {
        ObjList<QueryColumn> outerCols = outerModel.getBottomUpColumns();
        for (int i = 0, n = outerCols.size(); i < n; i++) {
            QueryColumn col = outerCols.getQuick(i);
            ExpressionNode rewritten = replaceColumnRef(col.getAst(), qualifiedRef, replacement);
            if (rewritten != col.getAst()) {
                col.of(col.getAlias(), rewritten);
            }
        }
        ObjList<ExpressionNode> orderBy = outerModel.getOrderBy();
        for (int i = 0, n = orderBy.size(); i < n; i++) {
            ExpressionNode rewritten = replaceColumnRef(orderBy.getQuick(i), qualifiedRef, replacement);
            if (rewritten != orderBy.getQuick(i)) {
                orderBy.setQuick(i, rewritten);
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

    private void rewriteOuterRefColumns(
            QueryModel model,
            CharSequence outerRefAlias,
            LowerCaseCharSequenceObjHashMap<CharSequence> aliasMap
    ) {
        ObjList<QueryColumn> cols = model.getBottomUpColumns();
        if (cols.size() == 0) {
            return;
        }
        for (int j = cols.size() - 1; j >= 0; j--) {
            QueryColumn col = cols.getQuick(j);
            if (col.getAst().isWildcard()) {
                continue;
            }
            ExpressionNode ast = col.getAst();
            if (ast != null
                    && ast.type == ExpressionNode.LITERAL
                    && Chars.startsWith(ast.token, outerRefAlias)) {
                CharSequence innerEquiv = lookupOuterRefAlias(ast.token, outerRefAlias, aliasMap);
                if (innerEquiv != null) {
                    col.of(col.getAlias(), expressionNodePool.next().of(
                            ExpressionNode.LITERAL, innerEquiv, ast.precedence, ast.position
                    ), col.isIncludeIntoWildcard());
                } else {
                    model.removeColumn(j);
                }
            } else {
                ExpressionNode rewritten = rewriteOuterRefs(ast, aliasMap);
                if (rewritten != ast) {
                    col.of(col.getAlias(), rewritten);
                }
            }

            if (col instanceof WindowExpression we) {
                rewriteOuterRefExprs(we.getPartitionBy(), outerRefAlias, aliasMap);
            }
        }
    }

    private void rewriteOuterRefExprs(
            ObjList<ExpressionNode> exprs,
            CharSequence outerRefAlias,
            LowerCaseCharSequenceObjHashMap<CharSequence> aliasMap
    ) {
        for (int i = 0, n = exprs.size(); i < n; i++) {
            ExpressionNode node = exprs.getQuick(i);
            if (node != null && node.type == ExpressionNode.LITERAL
                    && Chars.startsWith(node.token, outerRefAlias)) {
                CharSequence innerEquiv = lookupOuterRefAlias(node.token, outerRefAlias, aliasMap);
                if (innerEquiv != null) {
                    node.token = innerEquiv;
                }
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

            if (Chars.indexOf(node.token, '.') < 0 && node.lateralDepth > 0) {
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

    private void rewriteOuterRefsInModelClauses(
            QueryModel model,
            LowerCaseCharSequenceObjHashMap<CharSequence> aliasMap
    ) {
        ExpressionNode where = model.getWhereClause();
        if (where != null) {
            ExpressionNode rewritten = rewriteOuterRefs(where, aliasMap);
            if (rewritten != where) {
                model.setWhereClause(rewritten);
            }
        }
        ObjList<ExpressionNode> groupBy = model.getGroupBy();
        for (int j = 0, sz = groupBy.size(); j < sz; j++) {
            groupBy.setQuick(j, rewriteOuterRefs(groupBy.getQuick(j), aliasMap));
        }
        ObjList<ExpressionNode> orderBy = model.getOrderBy();
        for (int j = 0, sz = orderBy.size(); j < sz; j++) {
            orderBy.setQuick(j, rewriteOuterRefs(orderBy.getQuick(j), aliasMap));
        }
        if (model.getSampleBy() != null) {
            model.setSampleBy(rewriteOuterRefs(model.getSampleBy(), aliasMap));
        }
        ObjList<ExpressionNode> latestBy = model.getLatestBy();
        for (int j = 0, sz = latestBy.size(); j < sz; j++) {
            latestBy.setQuick(j, rewriteOuterRefs(latestBy.getQuick(j), aliasMap));
        }
        for (int j = 1, sz = model.getJoinModels().size(); j < sz; j++) {
            QueryModel jm = model.getJoinModels().getQuick(j);
            ExpressionNode jc = jm.getJoinCriteria();
            if (jc != null) {
                ExpressionNode rewritten = rewriteOuterRefs(jc, aliasMap);
                if (rewritten != jc) {
                    jm.setJoinCriteria(rewritten);
                }
            }
        }
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
        ObjList<QueryModel> joinModels = current.getJoinModels();
        int insertPos = 1;
        joinModels.add(outerRefJoinModel);
        for (int si = joinModels.size() - 1; si > insertPos; si--) {
            joinModels.setQuick(si, joinModels.getQuick(si - 1));
        }
        joinModels.setQuick(insertPos, outerRefJoinModel);
        if (outerRefJoinModel.getAlias() != null) {
            current.addModelAliasIndex(outerRefJoinModel.getAlias(), insertPos);
        }
        for (int si = insertPos + 1, sn = joinModels.size(); si < sn; si++) {
            QueryModel shifted = joinModels.getQuick(si);
            ExpressionNode shiftedAlias = shifted.getAlias() != null ? shifted.getAlias() : shifted.getTableNameExpr();
            if (shiftedAlias != null) {
                current.getModelAliasIndexes().put(shiftedAlias.token, si);
            }
        }
        correlatedPreds.clear();
        extractCorrelatedFromInnerJoins(current, correlatedPreds, depth);
        ExpressionNode joinCrit = null;
        for (int j = 0, m = correlatedPreds.size(); j < m; j++) {
            ExpressionNode rewritten = rewriteOuterRefs(
                    correlatedPreds.getQuick(j), outerToInnerAlias);
            joinCrit = joinCrit == null ? rewritten : createBinaryOp("and", joinCrit, rewritten);
        }

        ExpressionNode where = current.getWhereClause();
        if (where != null) {
            splitAndPredicates(where, innerJoinCorrelated);
            nonCorrelatedPreds.clear();
            CharSequence outerRefAlias = outerRefJoinModel.getAlias().token;
            for (int i = 0, n = innerJoinCorrelated.size(); i < n; i++) {
                ExpressionNode pred = innerJoinCorrelated.getQuick(i);
                if (hasOuterRefLiteral(pred, outerRefAlias)) {
                    joinCrit = joinCrit == null ? pred : createBinaryOp("and", joinCrit, pred);
                } else {
                    nonCorrelatedPreds.add(pred);
                }
            }
            current.setWhereClause(conjoin(nonCorrelatedPreds));
        }

        if (joinCrit != null) {
            outerRefJoinModel.setJoinCriteria(joinCrit);
            outerRefJoinModel.setJoinType(QueryModel.JOIN_INNER);
        }
        if (current.getUnionModel() != null) {
            compensateSetOp(current, outerToInnerAlias, depth, outerRefJoinModel);
        }
    }

    private void tryEliminateOuterRef(
            QueryModel selectModel,
            QueryModel joinLayer,
            QueryModel joinModel,
            CharSequence outerRefAlias
    ) throws SqlException {
        QueryModel topInner = joinModel.getNestedModel();
        CharSequence lateralAlias = joinModel.getAlias() != null
                ? joinModel.getAlias().token : null;

        boolean hasUnion = false;
        QueryModel check = topInner;
        while (check != null && !hasUnion) {
            if (check.getUnionModel() != null) {
                hasUnion = true;
            }
            check = check.getNestedModel();
        }

        if (hasUnion) {
            if (!canEliminateOuterRefInBranch(topInner, outerRefAlias)) {
                return;
            }
            check = topInner;
            while (check != null) {
                QueryModel ub = check.getUnionModel();
                while (ub != null) {
                    if (!canEliminateOuterRefInBranch(ub, outerRefAlias)) {
                        return;
                    }
                    ub = ub.getUnionModel();
                }
                check = check.getNestedModel();
            }
        }

        if (!tryEliminateOuterRefInBranch(topInner, selectModel, joinLayer, joinModel, lateralAlias, outerRefAlias)) {
            return;
        }

        QueryModel m = topInner;
        while (m != null) {
            QueryModel unionBranch = m.getUnionModel();
            while (unionBranch != null) {
                tryEliminateOuterRefInBranch(unionBranch, null, null, null, null, outerRefAlias);
                unionBranch = unionBranch.getUnionModel();
            }
            m = m.getNestedModel();
        }
    }

    private boolean tryEliminateOuterRefInBranch(
            QueryModel branchTop,
            QueryModel selectModel,
            QueryModel joinLayer,
            QueryModel joinModel,
            CharSequence lateralAlias,
            CharSequence outerRefAlias
    ) throws SqlException {
        QueryModel dataSourceLayer = null;
        int outerRefJmIndex = -1;
        QueryModel current = branchTop;
        while (current != null) {
            for (int j = 1, jn = current.getJoinModels().size(); j < jn; j++) {
                QueryModel jm = current.getJoinModels().getQuick(j);
                if (jm.getAlias() != null
                        && Chars.equalsIgnoreCase(jm.getAlias().token, outerRefAlias)) {
                    dataSourceLayer = current;
                    outerRefJmIndex = j;
                    break;
                }
            }
            if (dataSourceLayer != null) {
                break;
            }
            current = current.getNestedModel();
        }
        if (outerRefJmIndex < 0) {
            return false;
        }

        QueryModel outerRefJm = dataSourceLayer.getJoinModels().getQuick(outerRefJmIndex);
        outerToInnerAlias.clear();
        ExpressionNode joinCrit = outerRefJm.getJoinCriteria();

        if (joinCrit != null) {
            scanWhereForOuterRefEqualities(joinCrit, outerRefAlias, outerToInnerAlias);
        }
        current = branchTop;
        while (current != null) {
            ExpressionNode where = current.getWhereClause();
            if (where != null) {
                scanWhereForOuterRefEqualities(where, outerRefAlias, outerToInnerAlias);
            }
            if (current == dataSourceLayer) {
                break;
            }
            current = current.getNestedModel();
        }
        if (outerToInnerAlias.size() == 0 && joinModel != null) {
            return false;
        }
        QueryModel outerRefSubquery = outerRefJm.getNestedModel();
        ObjList<QueryColumn> outerRefCols = outerRefSubquery.getBottomUpColumns();
        boolean isAllEqualities = true;
        for (int j = 0, sz = outerRefCols.size(); j < sz; j++) {
            if (outerToInnerAlias.get(outerRefCols.getQuick(j).getAlias()) == null) {
                isAllEqualities = false;
                break;
            }
        }

        ExpressionNode simpleChainCriteria = null;
        if (!isAllEqualities) {
            if (!isSimpleChain(branchTop, dataSourceLayer)) {
                return false;
            }
            if (joinModel != null) {
                correlatedPreds.clear();
                if (joinCrit != null) {
                    splitAndPredicates(joinCrit, correlatedPreds);
                }
                current = branchTop;
                while (current != null && current != dataSourceLayer) {
                    ExpressionNode where = current.getWhereClause();
                    if (where != null) {
                        splitAndPredicates(where, innerJoinNonCorrelated);
                        ExpressionNode remaining = null;
                        for (int j = 0, sz = innerJoinNonCorrelated.size(); j < sz; j++) {
                            ExpressionNode pred = innerJoinNonCorrelated.getQuick(j);
                            if (hasOuterRefLiteral(pred, outerRefAlias)) {
                                correlatedPreds.add(pred);
                            } else {
                                remaining = remaining == null ? pred : createBinaryOp("and", remaining, pred);
                            }
                        }
                        current.setWhereClause(remaining);
                    }
                    current = current.getNestedModel();
                }
                simpleChainCriteria = rebuildOnFromOuterRefCriteria(
                        correlatedPreds, outerRefAlias, outerRefCols, branchTop, dataSourceLayer, lateralAlias
                );
            }
            if (selectModel != null) {
                liftOuterRefExpressions(branchTop, selectModel, joinLayer, outerRefAlias, outerRefCols, lateralAlias);
            }
        }

        QueryModel levelAboveDS = null;
        current = branchTop;
        while (current != null && current != dataSourceLayer) {
            if (current.getNestedModel() == dataSourceLayer) {
                levelAboveDS = current;
            }
            current = current.getNestedModel();
        }

        rewriteOuterRefColumns(dataSourceLayer, outerRefAlias, outerToInnerAlias);
        rewriteOuterRefsInModelClauses(dataSourceLayer, outerToInnerAlias);
        if (levelAboveDS != null) {
            rewriteOuterRefColumns(levelAboveDS, outerRefAlias, outerToInnerAlias);
            rewriteOuterRefsInModelClauses(levelAboveDS, outerToInnerAlias);
        }

        current = branchTop;
        while (current != null && current != levelAboveDS && current != dataSourceLayer) {
            ObjList<QueryColumn> cols = current.getBottomUpColumns();
            for (int j = cols.size() - 1; j >= 0; j--) {
                QueryColumn col = cols.getQuick(j);
                ExpressionNode ast = col.getAst();
                if (ast != null
                        && ast.type == ExpressionNode.LITERAL
                        && Chars.startsWith(ast.token, outerRefAlias)
                        && outerToInnerAlias.get(ast.token) == null) {
                    current.removeColumn(j);
                }
            }
            current = current.getNestedModel();
        }

        if (joinModel != null) {
            if (isAllEqualities) {
                ExpressionNode existingCrit = joinModel.getJoinCriteria();
                if (existingCrit != null) {
                    joinModel.setJoinCriteria(rewriteOuterRefs(existingCrit, outerToInnerAlias));
                }
            } else {
                joinModel.setJoinCriteria(simpleChainCriteria);
            }
        }

        ObjList<QueryColumn> branchTopCols = branchTop.getBottomUpColumns();
        if (branchTopCols.size() > 0 && isWildcard(branchTopCols)) {
            for (int j = 0, sz = outerRefCols.size(); j < sz; j++) {
                CharSequence colAlias = outerRefCols.getQuick(j).getAlias();
                if (branchTop.getAliasToColumnMap().excludes(colAlias)) {
                    ExpressionNode ref = expressionNodePool.next().of(
                            ExpressionNode.LITERAL, colAlias, 0, 0
                    );
                    branchTop.addBottomUpColumn(queryColumnPool.next().of(colAlias, ref, false));
                }
            }
        }

        dataSourceLayer.getJoinModels().remove(outerRefJmIndex);
        dataSourceLayer.getModelAliasIndexes().remove(outerRefAlias);
        // Update alias indices for models shifted down by the removal
        ObjList<QueryModel> remainingJoins = dataSourceLayer.getJoinModels();
        LowerCaseCharSequenceIntHashMap aliasIdx = dataSourceLayer.getModelAliasIndexes();
        for (int si = outerRefJmIndex, sn = remainingJoins.size(); si < sn; si++) {
            QueryModel shifted = remainingJoins.getQuick(si);
            ExpressionNode shiftedAlias = shifted.getAlias() != null ? shifted.getAlias() : shifted.getTableNameExpr();
            if (shiftedAlias != null) {
                aliasIdx.put(shiftedAlias.token, si);
            }
        }
        return true;
    }

    private void tryEliminateOuterRefs(QueryModel model) throws SqlException {
        tryEliminateOuterRefs(model, null);
    }

    private void tryEliminateOuterRefs(QueryModel model, QueryModel parent) throws SqlException {
        if (model == null) {
            return;
        }
        tryEliminateOuterRefs(model.getUnionModel(), null);
        for (int i = 0, n = model.getJoinModels().size(); i < n; i++) {
            QueryModel jm = model.getJoinModels().getQuick(i);
            QueryModel nested = jm.getNestedModel();
            if (nested != null) {
                tryEliminateOuterRefs(nested, i == 0 ? model : null);
                if (i > 0) {
                    outerCols.clear();
                    collectOuterRefAliases(nested, outerCols);
                    for (int oi = 0, on = outerCols.size(); oi < on; oi++) {
                        CharSequence outerRefAlias = outerCols.getQuick(oi).token;
                        QueryModel selectModel = (model.getBottomUpColumns().size() > 0 || parent == null)
                                ? model : parent;
                        tryEliminateOuterRef(selectModel, model, jm, outerRefAlias);
                    }
                }
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
}
