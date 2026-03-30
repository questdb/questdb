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
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

import java.util.ArrayDeque;

import static io.questdb.griffin.SqlOptimiser.checkForChildWindowFunctions;
import static io.questdb.griffin.model.QueryModel.isLateralJoin;

/**
 * Decorrelates lateral joins into standard hash joins,
 * based on the Neumann-Kemper "Unnesting Arbitrary Queries" technique.
 *
 * @see <a href="https://btw-2015.informatik.uni-hamburg.de/res/proceedings/Hauptband/Wiss/Neumann-Unnesting_Arbitrary_Querie.pdf">
 * Neumann, Kemper — Unnesting Arbitrary Queries (BTW 2015)</a>
 */
class LateralJoinRewriter implements Mutable {

    private static final String OUTER_REF_PREFIX = "__qdb_outer_ref__";
    private final CharacterStore characterStore;
    private final ObjList<ExpressionNode> correlatedPreds = new ObjList<>();
    private final ObjList<CharSequence> countColAliases;
    private final ObjectPool<ExpressionNode> expressionNodePool;
    private final FunctionParser functionParser;
    private final ObjList<ExpressionNode> groupingCols;
    private final ObjList<ExpressionNode> innerJoinCorrelated = new ObjList<>();
    private final ObjList<ExpressionNode> innerJoinNonCorrelated = new ObjList<>();
    private final ObjList<ExpressionNode> nonCorrelatedPreds = new ObjList<>();
    private final IntList orderByDirSave;
    private final ObjList<ExpressionNode> orderBySave;
    private final ObjList<CharSequence> outerAliasSaveStack;
    private final ObjList<ExpressionNode> outerCols;
    private final LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias;
    private final ObjectPool<QueryColumn> queryColumnPool;
    private final ObjectPool<QueryModel> queryModelPool;
    private final ArrayDeque<ExpressionNode> sqlNodeStack;
    private final ObjList<CharSequence> subCountColAliases;
    private final ObjectPool<WindowExpression> windowExpressionPool;
    private boolean hasCorrelation;
    private int outerRefId;

    LateralJoinRewriter(
            CharacterStore characterStore,
            ObjectPool<ExpressionNode> expressionNodePool,
            ObjectPool<QueryColumn> queryColumnPool,
            ObjectPool<QueryModel> queryModelPool,
            ObjectPool<WindowExpression> windowExpressionPool,
            ArrayDeque<ExpressionNode> sqlNodeStack,
            FunctionParser functionParser,
            ObjList<ExpressionNode> tempGroupingCols,
            ObjList<ExpressionNode> tempOuterCols,
            ObjList<ExpressionNode> tempOrderBySave,
            IntList tempOrderByDirSave,
            LowerCaseCharSequenceObjHashMap<CharSequence> tempOuterToInnerAlias,
            ObjList<CharSequence> tempCountColAliases,
            ObjList<CharSequence> tempSubCountColAliases,
            ObjList<CharSequence> tempOuterAliasSaveStack
    ) {
        this.characterStore = characterStore;
        this.expressionNodePool = expressionNodePool;
        this.queryColumnPool = queryColumnPool;
        this.queryModelPool = queryModelPool;
        this.windowExpressionPool = windowExpressionPool;
        this.sqlNodeStack = sqlNodeStack;
        this.functionParser = functionParser;
        this.groupingCols = tempGroupingCols;
        this.outerCols = tempOuterCols;
        this.orderBySave = tempOrderBySave;
        this.orderByDirSave = tempOrderByDirSave;
        this.outerToInnerAlias = tempOuterToInnerAlias;
        this.countColAliases = tempCountColAliases;
        this.subCountColAliases = tempSubCountColAliases;
        this.outerAliasSaveStack = tempOuterAliasSaveStack;
    }

    @Override
    public void clear() {
        correlatedPreds.clear();
        innerJoinCorrelated.clear();
        innerJoinNonCorrelated.clear();
        nonCorrelatedPreds.clear();
        hasCorrelation = false;
        outerRefId = 0;
    }

    public void rewrite(QueryModel model) throws SqlException {
        // Pass 1 (top-down): tag correlated refs with lateralDepth
        // and collect correlated columns per lateral join model
        if (analyzeCorrelation(model, 0)) {
            outerRefId = 0;
            // Pass 2 (bottom-up): decorrelate each lateral join.
            // Input:
            //   SELECT o.id, sub.cnt FROM orders o
            //   JOIN LATERAL (SELECT count(*) AS cnt FROM trades
            //                 WHERE order_id = o.id) sub
            // Output:
            //   SELECT o.id, sub.cnt FROM orders o
            //   JOIN (SELECT __qdb_outer_ref__0_id, count(*) AS cnt
            //         FROM trades
            //         CROSS JOIN (SELECT DISTINCT id FROM orders) __qdb_outer_ref__0
            //         WHERE order_id = __qdb_outer_ref__0.__qdb_outer_ref__0_id
            //         GROUP BY __qdb_outer_ref__0_id) sub
            //     ON sub.__qdb_outer_ref__0_id = o.id
            decorrelate(model, 0, null);
            // Pass 3 (top-down): eliminate __qdb_outer_ref__ where possible.
            // From the alignment criterion sub.__qdb_outer_ref__0_id = o.id,
            // derives __qdb_outer_ref__0_id ≡ order_id, rewrites all refs,
            // and removes the __qdb_outer_ref__0 join model:
            //   SELECT o.id, sub.cnt FROM orders o
            //   JOIN (SELECT order_id, count(*) AS cnt
            //         FROM trades
            //         WHERE order_id = order_id  -- tautology, removed later
            //         GROUP BY order_id) sub
            //     ON sub.order_id = o.id
            tryEliminateOuterRefs(model, null);
        }
    }

    private static void copyColumnNameToAliasMap(QueryModel src, QueryModel dst) {
        LowerCaseCharSequenceObjHashMap<CharSequence> srcMap = src.getColumnNameToAliasMap();
        LowerCaseCharSequenceObjHashMap<CharSequence> dstMap = dst.getColumnNameToAliasMap();
        ObjList<CharSequence> keys = srcMap.keys();
        for (int k = 0, kn = keys.size(); k < kn; k++) {
            CharSequence key = keys.getQuick(k);
            dstMap.put(key, srcMap.get(key));
        }
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

    private static boolean isCountAggregate(ExpressionNode node) {
        return node != null
                && node.type == ExpressionNode.FUNCTION
                && Chars.equalsIgnoreCase(node.token, "count");
    }

    private static boolean isOuterRefToken(ExpressionNode node, CharSequence outerRefAlias) {
        return node != null
                && node.type == ExpressionNode.LITERAL
                && matchesOuterRefAlias(node.token, outerRefAlias);
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

    private static boolean matchesOuterRefAlias(CharSequence token, CharSequence outerRefAlias) {
        return Chars.startsWith(token, outerRefAlias)
                && (token.length() == outerRefAlias.length()
                || !Character.isDigit(token.charAt(outerRefAlias.length())));
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
        sqlNodeStack.clear();
        while (node != null) {
            if (node.windowExpression != null) {
                addGroupingColsToPartitionBy(node.windowExpression.getPartitionBy(), outerRefAlias);
            }
            if (node.rhs != null) {
                sqlNodeStack.push(node.rhs);
            }
            for (int i = 0, n = node.args.size(); i < n; i++) {
                sqlNodeStack.push(node.args.getQuick(i));
            }
            node = node.lhs != null ? node.lhs : (sqlNodeStack.poll());
        }
    }

    private void addGroupingColsToPartitionBy(ObjList<ExpressionNode> partitionBy, CharSequence outerRefAlias) {
        for (int j = 0, m = groupingCols.size(); j < m; j++) {
            ExpressionNode gcol = groupingCols.getQuick(j);
            boolean isFound = false;
            for (int k = 0, p = partitionBy.size(); k < p; k++) {
                if (ExpressionNode.compareNodesExact(partitionBy.getQuick(k), gcol)) {
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

    private boolean allLiteralsAreCorrelated(ExpressionNode node, int depth) {
        sqlNodeStack.clear();
        ExpressionNode current = node;
        while (current != null) {
            if (current.type == ExpressionNode.LITERAL) {
                if (current.lateralDepth != depth) {
                    return false;
                }
            } else {
                if (current.rhs != null) {
                    sqlNodeStack.push(current.rhs);
                }
                for (int i = 0, n = current.args.size(); i < n; i++) {
                    sqlNodeStack.push(current.args.getQuick(i));
                }
                if (current.lhs != null) {
                    sqlNodeStack.push(current.lhs);
                }
            }
            current = sqlNodeStack.poll();
        }
        return true;
    }

    private boolean allLiteralsBelongTo(
            ExpressionNode node,
            CharSequence alias,
            QueryModel outerRefBase
    ) {
        sqlNodeStack.clear();
        ExpressionNode current = node;
        while (current != null) {
            if (current.type == ExpressionNode.LITERAL) {
                int dot = Chars.indexOf(current.token, '.');
                if (dot > 0) {
                    if (!Chars.startsWith(current.token, alias) || dot != alias.length()) {
                        return false;
                    }
                } else if (!canResolveColumnForOuter(current.token, outerRefBase)) {
                    return false;
                }
            } else {
                if (current.rhs != null) {
                    sqlNodeStack.push(current.rhs);
                }
                for (int i = 0, n = current.args.size(); i < n; i++) {
                    sqlNodeStack.push(current.args.getQuick(i));
                }
                if (current.lhs != null) {
                    sqlNodeStack.push(current.lhs);
                }
            }
            current = sqlNodeStack.poll();
        }
        return true;
    }

    // Returns true if any lateral join was found in the model tree.
    private boolean analyzeCorrelation(QueryModel model, int lateralDepth) {
        boolean hasLateral = false;
        if (model.getNestedModel() != null) {
            hasLateral = analyzeCorrelation(model.getNestedModel(), lateralDepth);
        }
        if (model.getUnionModel() != null) {
            hasLateral |= analyzeCorrelation(model.getUnionModel(), lateralDepth);
        }

        for (int i = 1, n = model.getJoinModels().size(); i < n; i++) {
            QueryModel joinModel = model.getJoinModels().getQuick(i);
            if (isLateralJoin(joinModel.getJoinType())) {
                hasLateral = true;
                int newDepth = lateralDepth + 1;
                QueryModel topInner = joinModel.getNestedModel();
                assert topInner != null;
                collectCorrelatedRefs(topInner, model, i, newDepth);
                analyzeCorrelation(topInner, newDepth);
            } else if (joinModel.getNestedModel() != null) {
                hasLateral |= analyzeCorrelation(joinModel.getNestedModel(), lateralDepth);
            }
        }
        return hasLateral;
    }

    private ExpressionNode assembleCoalesce(ExpressionNode node) {
        ExpressionNode coalesce = expressionNodePool.next().of(
                ExpressionNode.FUNCTION, "coalesce", 0, node.position
        );
        coalesce.paramCount = 2;
        coalesce.rhs = expressionNodePool.next().of(
                ExpressionNode.CONSTANT, "0", 0, node.position
        );
        coalesce.lhs = node;
        return coalesce;
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
                return isSimpleChain(branchTop, dataSourceLayer, targetAlias, outerToInnerAlias);
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
                int jmIndex = outerModel.getModelAliasIndex(node.token, 0, dotPos);
                if (jmIndex >= 0 && jmIndex < lateralJoinIndex
                        && cannotResolveAliasLocally(node.token, dotPos, innerModel)) {
                    ensureCorrelatedColumnSet(correlatedColumns, jmIndex).put(node.token, node.position, dotPos + 1, node.token.length());
                    node.lateralDepth = lateralDepth;
                    hasCorrelation = true;
                }
            } else if (!canResolveColumn(node.token, innerModel)) {
                for (int j = 0; j < lateralJoinIndex; j++) {
                    QueryModel outerJm = outerModel.getJoinModels().getQuick(j);
                    if (canResolveColumnForOuter(node.token, outerJm)) {
                        ensureCorrelatedColumnSet(correlatedColumns, j).put(node.token, node.position);
                        node.lateralDepth = lateralDepth;
                        hasCorrelation = true;
                        break;
                    }
                }
            }
        } else if (node.type == ExpressionNode.QUERY && node.queryModel != null) {
            collectCorrelatedRefs(node.queryModel, outerModel, lateralJoinIndex, lateralDepth, correlatedColumns, false);
        } else {
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
    }

    private void collectCorrelatedRefs(
            QueryModel topInner,
            QueryModel outerModel,
            int lateralJoinIndex,
            int lateralDepth
    ) {
        ObjList<LowerCaseCharSequenceIntHashMap> correlatedColumns =
                outerModel.getJoinModels().getQuick(lateralJoinIndex).getCorrelatedColumns();
        collectCorrelatedRefs(topInner, outerModel, lateralJoinIndex, lateralDepth, correlatedColumns, true);
    }

    private void collectCorrelatedRefs(
            QueryModel model,
            QueryModel outerModel,
            int lateralJoinIndex,
            int lateralDepth,
            ObjList<LowerCaseCharSequenceIntHashMap> correlatedColumns,
            boolean trackDepth
    ) {
        QueryModel current = model;
        while (current != null) {
            if (trackDepth) {
                hasCorrelation = false;
            }

            scanModelForCorrelatedRefs(current, outerModel, lateralJoinIndex, lateralDepth, correlatedColumns);

            if (trackDepth && hasCorrelation) {
                current.makeCorrelatedAtDepth(lateralDepth);
            }

            for (int i = 1, n = current.getJoinModels().size(); i < n; i++) {
                QueryModel jm = current.getJoinModels().getQuick(i);
                if (jm.getNestedModel() != null) {
                    collectCorrelatedRefs(jm.getNestedModel(), outerModel, lateralJoinIndex, lateralDepth, correlatedColumns, trackDepth);
                }
            }
            if (current.getUnionModel() != null) {
                collectCorrelatedRefs(current.getUnionModel(), outerModel, lateralJoinIndex, lateralDepth, correlatedColumns, trackDepth);
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

    // Adds groupingCols (outer ref columns) to GROUP BY and SELECT.
    // Without this, the aggregate collapses all outer-ref groups into one row.
    //   Before: SELECT count(*) FROM trades WHERE order_id = __outer_ref__.id
    //   After:  SELECT __outer_ref__.id, count(*) FROM trades ... GROUP BY __outer_ref__.id
    private void compensateAggregate(QueryModel inner) throws SqlException {
        ObjList<ExpressionNode> groupBy = inner.getGroupBy();
        if (groupBy != null && groupBy.size() > 0) {
            for (int i = 0, n = groupingCols.size(); i < n; i++) {
                ExpressionNode col = groupingCols.getQuick(i);
                boolean isFound = false;
                for (int j = 0, m = groupBy.size(); j < m; j++) {
                    if (ExpressionNode.compareNodesExact(groupBy.getQuick(j), col)) {
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
    }

    // Adds groupingCols to SELECT so DISTINCT preserves per-outer-row identity.
    // GROUP BY is not needed — DISTINCT already deduplicates across all columns.
    //   Before: SELECT DISTINCT category FROM trades WHERE order_id = __outer_ref__.id
    //   After:  SELECT DISTINCT category, __outer_ref__.id FROM trades ...
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
                        rewriteOuterRefs(joinCriteria, outerToInnerAlias, depth)
                );
            }
        }
    }

    // Compensates LATEST BY inside a lateral subquery to ensure per-outer-row computation.
    //
    // LATEST BY operates at the physical table scan level, BEFORE the WHERE filter.
    // But original LATERAL semantics require WHERE FIRST, then LATEST BY.
    // This difference is safe only when ALL correlations are equalities.
    //
    // Fast path — add inner physical column to PARTITION BY (native LATEST BY preserved):
    //   Condition: ALL correlations are equalities (no non-eq correlation at all).
    //             Each groupingCol maps to an inner physical column via WHERE equality.
    //   Example:
    //     SELECT * FROM orders o JOIN LATERAL (
    //         SELECT category, qty FROM trades
    //         WHERE order_id = o.id
    //         LATEST ON ts PARTITION BY category
    //     ) t
    //   After compensation:
    //     LATEST ON ts PARTITION BY category, order_id
    //   This is safe because LATEST BY groups by order_id (physical column),
    //   and the equality WHERE becomes a hash join — execution order doesn't matter.
    //
    // Fallback — convert to row_number() OVER (...) = 1 (window function replacement):
    //   Condition: ANY non-equality correlation exists (e.g., ts > o.start_ts, price > o.min_price).
    //   Why: LATEST BY runs before WHERE at table scan level. If non-eq WHERE filters
    //        affect which row is "latest", the semantics change. Example:
    //          trades: (cat=A, price=50, ts=00:20), (cat=A, price=100, ts=00:10)
    //          WHERE price > 60 LATEST ON ts PARTITION BY category
    //        LATERAL: WHERE first → {price=100} → latest = price=100 ✓
    //        Fast path: LATEST first → price=50 (highest ts) → WHERE price>60 → filtered out ✗
    //   Example:
    //     WHERE order_id = o.id AND ts > o.start_ts   ← has non-eq → fallback
    //   After conversion:
    //     filterModel (WHERE _latest_rn = 1)
    //       → windowLayer (row_number() OVER (PARTITION BY category, groupingCols
    //                       ORDER BY ts DESC) AS _latest_rn)
    //         → trades CROSS JOIN __qdb_outer_ref__0
    //           WHERE order_id = __qdb_outer_ref__0_id AND ts > __qdb_outer_ref__0_start_ts
    private QueryModel compensateLatestBy(
            QueryModel inner,
            QueryModel parent,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth
    ) throws SqlException {
        ObjList<ExpressionNode> latestBy = inner.getLatestBy();

        boolean canUseNativeLatestBy = !hasNonEqualityCorrelation(inner.getWhereClause(), depth)
                && groupingCols.size() > 0;

        if (canUseNativeLatestBy) {
            for (int i = 0, n = groupingCols.size(); i < n; i++) {
                CharSequence outerRefAlias = groupingCols.getQuick(i).token;
                CharSequence innerCol = findInnerPhysicalColumn(inner.getWhereClause(), outerRefAlias, outerToInnerAlias);
                if (innerCol == null) {
                    canUseNativeLatestBy = false;
                    break;
                }
            }
        }

        if (canUseNativeLatestBy) {
            for (int i = 0, n = groupingCols.size(); i < n; i++) {
                CharSequence outerRefAlias = groupingCols.getQuick(i).token;
                CharSequence innerCol = findInnerPhysicalColumn(inner.getWhereClause(), outerRefAlias, outerToInnerAlias);
                boolean isFound = false;
                for (int j = 0, m = latestBy.size(); j < m; j++) {
                    if (Chars.equalsIgnoreCase(latestBy.getQuick(j).token, innerCol)) {
                        isFound = true;
                        break;
                    }
                }
                if (!isFound) {
                    latestBy.add(expressionNodePool.next().of(
                            ExpressionNode.LITERAL, innerCol, 0, 0
                    ));
                }
                ensureColumnInSelect(inner, groupingCols.getQuick(i), groupingCols.getQuick(i).token);
            }
            return inner;
        }

        // Fallback: convert LATEST BY to row_number() OVER (...) = 1
        return convertLatestByToWindowFunction(inner, parent, outerToInnerAlias, depth);
    }

    // Converts LIMIT N to row_number() OVER (PARTITION BY groupingCols ORDER BY ...) <= N.
    // A plain LIMIT applies globally after decorrelation, but lateral semantics require
    // per-outer-row limiting.
    //   Before: SELECT * FROM trades WHERE order_id = __outer_ref__.id ORDER BY ts LIMIT 3
    //   After:  SELECT * FROM (
    //             SELECT *, row_number() OVER (PARTITION BY __outer_ref__.id ORDER BY ts) AS _lateral_rn
    //             FROM trades ...
    //           ) WHERE _lateral_rn <= 3
    private QueryModel compensateLimit(
            QueryModel current,
            int depth
    ) throws SqlException {
        ExpressionNode limitHi = current.getLimitHi();
        ExpressionNode limitLo = current.getLimitLo();
        if (limitHi == null && limitLo == null) {
            return current;
        }

        if (limitHi != null && hasCorrelatedExprAtDepth(limitHi, depth)) {
            throw SqlException.position(limitHi.position)
                    .put("non-constant LIMIT in LATERAL join is not supported");
        }
        if (limitLo != null && hasCorrelatedExprAtDepth(limitLo, depth)) {
            throw SqlException.position(limitLo.position)
                    .put("non-constant LIMIT in LATERAL join is not supported");
        }
        if (limitHi != null && limitHi.type != ExpressionNode.CONSTANT) {
            throw SqlException.position(limitHi.position)
                    .put("non-constant LIMIT in LATERAL join is not supported");
        }
        if (limitLo != null && limitLo.type != ExpressionNode.CONSTANT) {
            throw SqlException.position(limitLo.position)
                    .put("non-constant LIMIT in LATERAL join is not supported");
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

        boolean isWrappingNeeded = current.getGroupBy().size() > 0
                || (current.getNestedModel() != null && current.getNestedModel().getGroupBy().size() > 0)
                || hasAggregateFunctions(current)
                || current.isDistinct();
        QueryModel windowLayer = null;
        if (isWrappingNeeded) {
            for (int i = 0, n = groupingCols.size(); i < n; i++) {
                rnWindowExpr.getPartitionBy().add(
                        ExpressionNode.deepClone(expressionNodePool, groupingCols.getQuick(i)));
            }

            QueryModel flatWrapper = queryModelPool.next();
            flatWrapper.setNestedModel(current);
            flatWrapper.setNestedModelIsSubQuery(true);

            windowLayer = queryModelPool.next();
            windowLayer.setNestedModel(flatWrapper);
            windowLayer.setNestedModelIsSubQuery(true);

            ObjList<QueryColumn> curCols = current.getBottomUpColumns();
            for (int i = 0, n = curCols.size(); i < n; i++) {
                QueryColumn col = curCols.getQuick(i);
                ExpressionNode ref = expressionNodePool.next().of(
                        ExpressionNode.LITERAL, col.getAlias(), 0, 0
                );
                windowLayer.addBottomUpColumn(queryColumnPool.next().of(col.getAlias(), ref));
            }
            for (int i = 0, n = groupingCols.size(); i < n; i++) {
                ExpressionNode gcol = groupingCols.getQuick(i);
                ExpressionNode ref = ExpressionNode.deepClone(expressionNodePool, gcol);
                CharSequence alias = createColumnAlias(gcol.token, windowLayer);
                windowLayer.addBottomUpColumn(queryColumnPool.next().of(alias, ref));
            }
            windowLayer.addBottomUpColumn(rnWindowExpr);
        } else {
            current.addBottomUpColumn(rnWindowExpr);
        }

        current.setLimit(null, null);
        if (orderByModel != null) {
            orderByModel.getOrderBy().clear();
            orderByModel.getOrderByDirection().clear();
        }
        current.getOrderBy().clear();
        current.getOrderByDirection().clear();

        if (isWrappingNeeded) {
            current = windowLayer;
        }

        ExpressionNode rnRef = expressionNodePool.next().of(
                ExpressionNode.LITERAL, rnAlias, 0, 0
        );

        ExpressionNode rnFilter;
        if (limitHi != null && limitLo != null) {
            ExpressionNode upperBound = createBinaryOp("<=",
                    ExpressionNode.deepClone(expressionNodePool, rnRef),
                    ExpressionNode.deepClone(expressionNodePool, limitHi));
            ExpressionNode lowerBound = createBinaryOp(">",
                    ExpressionNode.deepClone(expressionNodePool, rnRef),
                    ExpressionNode.deepClone(expressionNodePool, limitLo));
            rnFilter = createBinaryOp("and", lowerBound, upperBound);
        } else {
            rnFilter = createBinaryOp("<=",
                    ExpressionNode.deepClone(expressionNodePool, rnRef),
                    ExpressionNode.deepClone(expressionNodePool, limitHi != null ? limitHi : limitLo));
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

    // Adds groupingCols to SELECT so SAMPLE BY results are partitioned per outer row.
    // SAMPLE BY already acts as an implicit GROUP BY on the time bucket, so we only
    // need the outer ref column visible in SELECT for the alignment join.
    //   Before: SELECT ts, count(*) FROM trades WHERE ... SAMPLE BY 1h
    //   After:  SELECT ts, count(*), __outer_ref__.id FROM trades WHERE ... SAMPLE BY 1h
    private void compensateSampleBy(QueryModel inner) throws SqlException {
        for (int i = 0, n = groupingCols.size(); i < n; i++) {
            ExpressionNode col = groupingCols.getQuick(i);
            ensureColumnInSelect(inner, col, col.token);
        }
    }

    // Decorrelates each UNION/INTERSECT/EXCEPT branch independently: clones the
    // __qdb_outer_ref__ data source into each branch, extracts correlated predicates,
    // rewrites outer refs, and compensates GROUP BY / window / LATEST BY / LIMIT.
    //   Before: (SELECT sum(qty) FROM trades WHERE order_id = o.id)
    //           UNION ALL
    //           (SELECT sum(qty) FROM returns WHERE order_id = o.id)
    //   After:  (SELECT __outer_ref__.id, sum(qty) FROM trades
    //            CROSS JOIN __outer_ref__ WHERE order_id = __outer_ref__.id
    //            GROUP BY __outer_ref__.id)
    //           UNION ALL
    //           (SELECT __outer_ref__.id, sum(qty) FROM returns
    //            CROSS JOIN __outer_ref__ WHERE order_id = __outer_ref__.id
    //            GROUP BY __outer_ref__.id)
    private void compensateSetOp(
            QueryModel inner,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth,
            QueryModel outerRefJm
    ) throws SqlException {

        QueryModel prev = inner;
        QueryModel current = inner.getUnionModel();
        while (current != null) {
            // Each UNION branch needs its own __qdb_outer_ref__ copy
            QueryModel branchOuterRef = queryModelPool.next();
            branchOuterRef.setNestedModel(outerRefJm.getNestedModel().deepClone(queryModelPool, queryColumnPool, expressionNodePool, windowExpressionPool));
            branchOuterRef.setAlias(outerRefJm.getAlias());
            branchOuterRef.setJoinType(QueryModel.JOIN_CROSS);
            copyColumnNameToAliasMap(outerRefJm, branchOuterRef);
            current.addJoinModel(branchOuterRef);
            if (branchOuterRef.getAlias() != null) {
                current.addModelAliasIndex(branchOuterRef.getAlias(), current.getJoinModels().size() - 1);
            }

            correlatedPreds.clear();
            nonCorrelatedPreds.clear();
            extractCorrelatedPredicates(current.getWhereClause(), correlatedPreds, nonCorrelatedPreds, depth);

            for (int j = 0, m = correlatedPreds.size(); j < m; j++) {
                correlatedPreds.setQuick(j, rewriteOuterRefs(correlatedPreds.getQuick(j), outerToInnerAlias, depth));
            }
            current.setWhereClause(conjoin(nonCorrelatedPreds));
            if (correlatedPreds.size() > 0) {
                branchOuterRef.setJoinCriteria(conjoin(correlatedPreds));
                branchOuterRef.setJoinType(QueryModel.JOIN_INNER);
            }

            rewriteSelectExpressions(current, outerToInnerAlias, depth);
            rewriteExpressionList(current.getOrderBy(), outerToInnerAlias, depth);
            rewriteExpressionList(current.getGroupBy(), outerToInnerAlias, depth);
            if (current.getSampleBy() != null
                    && hasCorrelatedExprAtDepth(current.getSampleBy(), depth)) {
                current.setSampleBy(
                        rewriteOuterRefs(current.getSampleBy(), outerToInnerAlias, depth));
            }
            ObjList<ExpressionNode> branchLatestBy = current.getLatestBy();
            for (int j = 0, m = branchLatestBy.size(); j < m; j++) {
                ExpressionNode lb = branchLatestBy.getQuick(j);
                if (hasCorrelatedExprAtDepth(lb, depth)) {
                    branchLatestBy.setQuick(j,
                            rewriteOuterRefs(lb, outerToInnerAlias, depth));
                }
            }

            ObjList<ExpressionNode> branchGroupBy = current.getGroupBy();
            for (int j = 0, m = groupingCols.size(); j < m; j++) {
                ExpressionNode col = groupingCols.getQuick(j);
                ensureColumnInSelect(current, col, col.token);
                if (branchGroupBy.size() > 0) {
                    boolean isFound = false;
                    for (int k = 0, kn = branchGroupBy.size(); k < kn; k++) {
                        if (ExpressionNode.compareNodesExact(branchGroupBy.getQuick(k), col)) {
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
                QueryModel latestByWrapper = compensateLatestBy(current, prev, outerToInnerAlias, depth);
                if (latestByWrapper != current) {
                    latestByWrapper.setUnionModel(current.getUnionModel());
                    current.setUnionModel(null);
                    prev.setUnionModel(latestByWrapper);
                    current = latestByWrapper;
                }
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
                    // Clone for nested pushDownOuterRefs
                    QueryModel deepOuterRef = queryModelPool.next();
                    deepOuterRef.setNestedModel(branchOuterRef.getNestedModel().deepClone(queryModelPool, queryColumnPool, expressionNodePool, windowExpressionPool));
                    deepOuterRef.setAlias(branchOuterRef.getAlias());
                    deepOuterRef.setJoinType(QueryModel.JOIN_CROSS);
                    copyColumnNameToAliasMap(branchOuterRef, deepOuterRef);
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

            if (current.getBottomUpColumns().size() != prev.getBottomUpColumns().size()) {
                throw SqlException.position(current.getModelPosition())
                        .put("set operation branches must have the same number of columns after decorrelation");
            }

            prev = current;
            current = current.getUnionModel();
        }
    }

    // Adds groupingCols to every window function's PARTITION BY so the window
    // computes per outer row instead of across all rows globally.
    //   Before: SELECT qty, sum(qty) OVER (ORDER BY ts) FROM trades WHERE ...
    //   After:  SELECT qty, sum(qty) OVER (PARTITION BY __outer_ref__.id ORDER BY ts)
    //           , __outer_ref__.id FROM trades ...
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

    private QueryModel convertLatestByToWindowFunction(
            QueryModel inner,
            QueryModel parent,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth
    ) throws SqlException {
        ObjList<ExpressionNode> latestBy = inner.getLatestBy();
        CharSequence rnAlias = createColumnAlias("_latest_rn", inner);
        ExpressionNode rnFunc = expressionNodePool.next().of(
                ExpressionNode.FUNCTION, "row_number", 0, 0
        );
        rnFunc.paramCount = 0;

        WindowExpression rnWindowExpr = windowExpressionPool.next();
        rnWindowExpr.of(rnAlias, rnFunc);

        for (int i = 0, n = latestBy.size(); i < n; i++) {
            ExpressionNode cloned = ExpressionNode.deepClone(expressionNodePool, latestBy.getQuick(i));
            if (hasCorrelatedExprAtDepth(cloned, depth)) {
                cloned = rewriteOuterRefs(cloned, outerToInnerAlias, depth);
            }
            rnWindowExpr.getPartitionBy().add(cloned);
        }
        for (int i = 0, n = groupingCols.size(); i < n; i++) {
            rnWindowExpr.getPartitionBy().add(
                    ExpressionNode.deepClone(expressionNodePool, groupingCols.getQuick(i)));
        }

        ExpressionNode tsExpr = inner.getTimestamp();
        if (tsExpr != null) {
            rnWindowExpr.getOrderBy().add(
                    ExpressionNode.deepClone(expressionNodePool, tsExpr));
            rnWindowExpr.getOrderByDirection().add(QueryModel.ORDER_DIRECTION_DESCENDING);
        }

        latestBy.clear();

        QueryModel windowLayer = queryModelPool.next();
        windowLayer.setNestedModel(inner);
        if (parent != null && parent.getBottomUpColumns().size() > 0) {
            ObjList<QueryColumn> parentCols = parent.getBottomUpColumns();
            for (int i = 0, n = parentCols.size(); i < n; i++) {
                QueryColumn col = parentCols.getQuick(i);
                ExpressionNode ref = expressionNodePool.next().of(
                        ExpressionNode.LITERAL, col.getAlias(), 0, 0
                );
                windowLayer.addBottomUpColumn(queryColumnPool.next().of(col.getAlias(), ref));
            }
        }
        for (int i = 0, n = groupingCols.size(); i < n; i++) {
            ExpressionNode gcol = groupingCols.getQuick(i);
            ensureColumnInSelect(windowLayer, gcol, gcol.token);
        }
        windowLayer.addBottomUpColumn(rnWindowExpr);
        ExpressionNode rnRef = expressionNodePool.next().of(
                ExpressionNode.LITERAL, rnAlias, 0, 0
        );
        ExpressionNode one = expressionNodePool.next().of(
                ExpressionNode.CONSTANT, "1", 0, 0
        );

        QueryModel filterModel = queryModelPool.next();
        filterModel.setNestedModel(windowLayer);
        filterModel.setNestedModelIsSubQuery(true);
        filterModel.setWhereClause(createBinaryOp("=", rnRef, one));
        return filterModel;
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
            // Deep clone the nested model tree to avoid shared references.
            // Without cloning, the same model objects are shared between the main tree
            // and the outer ref subquery. rewriteSelectClause0 modifies models in place,
            // so processing the shared model twice causes corruption.
            outerRefBase.setNestedModel(outerJm.getNestedModel().deepClone(queryModelPool, queryColumnPool, expressionNodePool, windowExpressionPool));
            outerRefBase.setNestedModelIsSubQuery(outerJm.isNestedModelIsSubQuery());
        } else {
            throw SqlException.position(outerJm.getModelPosition())
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

    // Pass 2: bottom-up traversal. Recurses into nested/union models first, then
    // processes lateral joins at the current level. For each lateral join it:
    //  1. Builds a DISTINCT __qdb_outer_ref__ subquery from correlated columns
    //  2. Calls pushDownOuterRefs to extract correlated predicates from WHERE,
    //     compensate GROUP BY / DISTINCT / SAMPLE BY / LATEST BY / window functions
    //  3. Rewrites correlated column references to use __qdb_outer_ref__ alias
    //  4. Creates alignment join criteria (lateral.col = outer.col)
    //  5. Degrades the lateral join type to a regular join
    // Bottom-up order ensures nested laterals are decorrelated before their parents.
    private void decorrelate(QueryModel model, int lateralDepth, QueryModel parent) throws SqlException {
        if (model.getNestedModel() != null) {
            decorrelate(model.getNestedModel(), lateralDepth, model);
        }
        if (model.getUnionModel() != null) {
            decorrelate(model.getUnionModel(), lateralDepth, parent);
        }

        for (int i = 1, n = model.getJoinModels().size(); i < n; i++) {
            QueryModel joinModel = model.getJoinModels().getQuick(i);
            if (QueryModel.isLateralJoin(joinModel.getJoinType())) {
                QueryModel topInner = joinModel.getNestedModel();
                assert topInner != null;

                // bottom up
                decorrelate(topInner, lateralDepth + 1, null);

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

                ExpressionNode originalOnCondition = joinModel.getJoinCriteria();

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

                if (originalOnCondition != null) {
                    if (hasCorrelatedExprAtDepth(originalOnCondition, depth)) {
                        originalOnCondition = rewriteOuterRefs(originalOnCondition, outerToInnerAlias, depth);
                    }
                    joinCriteria = joinCriteria == null
                            ? originalOnCondition
                            : createBinaryOp("and", joinCriteria, originalOnCondition);
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

                if (isLeft && countColAliases.size() > 0) {
                    QueryModel selectModel = (model.getBottomUpColumns().size() > 0 || parent == null)
                            ? model : parent;
                    wrapCountColumnsWithCoalesce(selectModel, joinModel, countColAliases);
                }
            } else if (joinModel.getNestedModel() != null) {
                decorrelate(joinModel.getNestedModel(), lateralDepth, null);
            }
        }
    }

    private void decorrelateJoinModelSubqueries(
            QueryModel joinLayer,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth,
            QueryModel outerRefJm
    ) throws SqlException {
        for (int i = 1, n = joinLayer.getJoinModels().size(); i < n; i++) {
            QueryModel jm = joinLayer.getJoinModels().getQuick(i);
            if (jm == outerRefJm) {
                continue;
            }
            QueryModel nested = jm.getNestedModel();
            if (nested == null || !nested.isCorrelatedAtDepth(depth)) {
                continue;
            }

            // Each correlated join-model subquery needs its own __qdb_outer_ref__
            QueryModel clonedOuterRef = queryModelPool.next();
            clonedOuterRef.setNestedModel(outerRefJm.getNestedModel().deepClone(queryModelPool, queryColumnPool, expressionNodePool, windowExpressionPool));
            clonedOuterRef.setAlias(outerRefJm.getAlias());
            clonedOuterRef.setJoinType(QueryModel.JOIN_CROSS);
            copyColumnNameToAliasMap(outerRefJm, clonedOuterRef);

            subCountColAliases.clear();
            pushDownOuterRefs(
                    null, nested, outerToInnerAlias, false,
                    subCountColAliases, clonedOuterRef, jm, depth
            );
        }
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
            if (ExpressionNode.compareNodesExact(existing.getAst(), colExpr)) {
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

    private CharSequence ensureColumnInSelectAtFront(
            QueryModel model,
            ExpressionNode colExpr,
            CharSequence preferredAlias
    ) {
        ObjList<QueryColumn> cols = model.getBottomUpColumns();
        if (cols.size() == 0 || isWildcard(cols)) {
            return preferredAlias;
        }
        for (int i = 0, n = cols.size(); i < n; i++) {
            QueryColumn col = cols.getQuick(i);
            if (ExpressionNode.compareNodesExact(col.getAst(), colExpr)) {
                return col.getAlias();
            }
        }
        CharSequence alias = createColumnAlias(preferredAlias, model);
        QueryColumn qc = queryColumnPool.next().of(alias, colExpr, false);
        if (model.addField(qc)) {
            cols.insert(0, 1, null);
            cols.setQuick(0, qc);
        }
        return alias;
    }

    private void extractCorrelatedFromInnerJoins(
            QueryModel inner,
            ObjList<ExpressionNode> correlated,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
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
                    for (int ci = 0, cn = innerJoinCorrelated.size(); ci < cn; ci++) {
                        ExpressionNode pred = innerJoinCorrelated.getQuick(ci);
                        if (allLiteralsAreCorrelated(pred, depth)) {
                            correlated.add(pred);
                        } else {
                            ExpressionNode rewritten = rewriteOuterRefs(pred, outerToInnerAlias, depth);
                            innerJoinNonCorrelated.add(rewritten);
                        }
                    }
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
        splitAndPredicates(expr, nonCorrelated);
        for (int i = nonCorrelated.size() - 1; i >= 0; i--) {
            ExpressionNode pred = nonCorrelated.getQuick(i);
            if (hasCorrelatedExprAtDepth(pred, depth)) {
                correlated.add(pred);
                nonCorrelated.remove(i);
            }
        }
    }

    private CharSequence findEqualityPartner(ExpressionNode node, CharSequence target) {
        sqlNodeStack.clear();
        while (node != null) {
            if (SqlKeywords.isAndKeyword(node.token)) {
                if (node.rhs != null) {
                    sqlNodeStack.push(node.rhs);
                }
                node = node.lhs;
            } else {
                if (Chars.equals(node.token, "=") && node.lhs != null && node.rhs != null) {
                    if (node.lhs.type == ExpressionNode.LITERAL
                            && Chars.equalsIgnoreCase(node.lhs.token, target)
                            && node.rhs.type == ExpressionNode.LITERAL) {
                        return node.rhs.token;
                    }
                    if (node.rhs.type == ExpressionNode.LITERAL
                            && Chars.equalsIgnoreCase(node.rhs.token, target)
                            && node.lhs.type == ExpressionNode.LITERAL) {
                        return node.lhs.token;
                    }
                }
                node = sqlNodeStack.poll();
            }
        }
        return null;
    }

    private CharSequence findInnerPhysicalColumn(
            ExpressionNode where,
            CharSequence outerRefAlias,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias
    ) {
        if (where == null) {
            return null;
        }
        ObjList<CharSequence> keys = outerToInnerAlias.keys();
        CharSequence outerColToken = null;
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence key = keys.getQuick(i);
            if (key != null && Chars.equalsIgnoreCase(outerToInnerAlias.get(key), outerRefAlias)) {
                outerColToken = key;
                break;
            }
        }
        if (outerColToken == null) {
            return null;
        }
        return findEqualityPartner(where, outerColToken);
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
        sqlNodeStack.clear();
        while (node != null) {
            if (node.type == ExpressionNode.LITERAL) {
                if (node.lateralDepth == depth) {
                    return true;
                }
            } else if (node.type != ExpressionNode.QUERY) {
                if (node.rhs != null) {
                    sqlNodeStack.push(node.rhs);
                }
                for (int i = 0, n = node.args.size(); i < n; i++) {
                    sqlNodeStack.push(node.args.getQuick(i));
                }
                if (node.windowExpression != null) {
                    ObjList<ExpressionNode> partitionBy = node.windowExpression.getPartitionBy();
                    for (int i = 0, n = partitionBy.size(); i < n; i++) {
                        sqlNodeStack.push(partitionBy.getQuick(i));
                    }
                    ObjList<ExpressionNode> winOrderBy = node.windowExpression.getOrderBy();
                    for (int i = 0, n = winOrderBy.size(); i < n; i++) {
                        sqlNodeStack.push(winOrderBy.getQuick(i));
                    }
                }
            }
            node = node.lhs != null ? node.lhs : (sqlNodeStack.poll());
        }
        return false;
    }


    private boolean hasNonEqualityCorrelation(ExpressionNode where, int depth) {
        if (where == null) {
            return false;
        }
        if (SqlKeywords.isAndKeyword(where.token)) {
            return hasNonEqualityCorrelation(where.lhs, depth) || hasNonEqualityCorrelation(where.rhs, depth);
        }
        boolean hasCorrelatedChild = hasCorrelatedExprAtDepth(where, depth);
        if (hasCorrelatedChild) {
            return !Chars.equals(where.token, "=");
        }
        return false;
    }

    private boolean hasOuterRefLiteral(ExpressionNode node, CharSequence outerRefAlias) {
        sqlNodeStack.clear();
        while (node != null) {
            if (node.type == ExpressionNode.LITERAL) {
                if (matchesOuterRefAlias(node.token, outerRefAlias)) {
                    return true;
                }
            } else {
                if (node.rhs != null) {
                    sqlNodeStack.push(node.rhs);
                }
                for (int i = 0, n = node.args.size(); i < n; i++) {
                    sqlNodeStack.push(node.args.getQuick(i));
                }
            }
            node = node.lhs != null ? node.lhs : (sqlNodeStack.poll());
        }
        return false;
    }

    private boolean hasUnmappedOuterRefLiteral(
            ExpressionNode node,
            CharSequence outerRefAlias,
            LowerCaseCharSequenceObjHashMap<CharSequence> aliasMap
    ) {
        sqlNodeStack.clear();
        while (node != null) {
            if (node.type == ExpressionNode.LITERAL) {
                if (matchesOuterRefAlias(node.token, outerRefAlias)
                        && lookupOuterRefAlias(node.token, outerRefAlias, aliasMap) == null) {
                    return true;
                }
            } else {
                if (node.rhs != null) {
                    sqlNodeStack.push(node.rhs);
                }
                for (int i = 0, n = node.args.size(); i < n; i++) {
                    sqlNodeStack.push(node.args.getQuick(i));
                }
            }
            node = node.lhs != null ? node.lhs : (sqlNodeStack.poll());
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
            if (checkForChildWindowFunctions(sqlNodeStack, col.getAst())) {
                return true;
            }
        }
        return false;
    }

    private boolean isCountLiteralMatch(
            ExpressionNode node,
            CharSequence joinAlias,
            ObjList<CharSequence> countColAliases
    ) {
        if (node == null || node.type != ExpressionNode.LITERAL) {
            return false;
        }
        for (int j = 0, m = countColAliases.size(); j < m; j++) {
            CharSequence countAlias = countColAliases.getQuick(j);
            if (joinAlias != null) {
                int dotPos = Chars.indexOf(node.token, '.');
                if (dotPos > 0
                        && Chars.equalsIgnoreCase(joinAlias, node.token, 0, dotPos)
                        && Chars.equalsIgnoreCase(countAlias, node.token, dotPos + 1, node.token.length())) {
                    return true;
                }
            }
            if (Chars.equalsIgnoreCase(node.token, countAlias)) {
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

    private boolean isSimpleChain(
            QueryModel branchTop,
            QueryModel dataSourceLayer,
            CharSequence outerRefAlias,
            LowerCaseCharSequenceObjHashMap<CharSequence> aliasMap
    ) {
        QueryModel m = branchTop;
        QueryModel parent = null;
        while (m != null) {
            if (m.getGroupBy().size() > 0
                    || m.getSampleBy() != null
                    || m.isDistinct()
                    || m.getLimitHi() != null || m.getLimitLo() != null
                    || m.getLatestBy().size() > 0
                    || m.getUnionModel() != null
                    || hasAggregateFunctions(m)
                    || hasWindowColumns(m)
                    || (m != dataSourceLayer && m.getJoinModels().size() > 1)) {
                return false;
            }

            if (m.getOrderBy().size() > 0 && parent != null) {
                for (int i = 0, n = m.getOrderBy().size(); i < n; i++) {
                    QueryColumn column = parent.getAliasToColumnMap().get(m.getOrderBy().getQuick(i).token);
                    if (column != null && hasUnmappedOuterRefLiteral(column.getAst(), outerRefAlias, aliasMap)) {
                        return false;
                    }
                }
            }

            if (m == dataSourceLayer) {
                break;
            }
            parent = m;
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
            if (matchesOuterRefAlias(node.token, outerRefAlias)
                    && node.token.length() > outerRefAlias.length()) {
                ExpressionNode original = findOriginalOuterRef(node.token, outerRefCols);
                return ExpressionNode.deepClone(expressionNodePool, original != null ? original : node);
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
        if (dot > 0 && matchesOuterRefAlias(token, outerRefAlias)) {
            return aliasMap.get(token, dot + 1, token.length());
        }
        return null;
    }

    private CharSequence propagateColumnUp(
            CharSequence columnName,
            QueryModel current,
            QueryModel dataSourceLayer
    ) {
        if (current == dataSourceLayer || current == null) {
            return columnName;
        }

        CharSequence alias = propagateColumnUp(columnName, current.getNestedModel(), dataSourceLayer);
        ExpressionNode colNode = expressionNodePool.next().of(
                ExpressionNode.LITERAL, alias, 0, 0
        );
        return ensureColumnInSelectAtFront(current, colNode, alias);
    }

    // Walks the lateral subquery's nestedModel chain top-down, at each layer:
    //   1. compensateLimit — convert LIMIT to row_number() window
    //   2. add groupingCols to SELECT
    //   3. compensate operators (SAMPLE BY / GROUP BY / window / DISTINCT / LATEST BY)
    //   4. rewrite correlated refs (o.id → __qdb_outer_ref__0_id)
    //   5. descend or terminate:
    //      - no joins: recurse into nestedModel or terminateHere
    //      - has joins: recurse main chain (ji==0), delegate each correlated join
    //        branch (ji>0) to pushDownOuterRefsForJoinBranch
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
        } else if (hasGroupBy || hasAggregates) {
            compensateAggregate(current);
        }
        if (isLeftJoin && hasAggregateFunctions(current)) {
            rewriteCountForLeftJoin(current, countColAliases);
        }
        if (hasWindowColumns(current)) {
            compensateWindow(current, outerRefJoinModel.getAlias().token);
        }
        if (current.isDistinct()) {
            compensateDistinct(current);
        }
        if (current.getLatestBy().size() > 0) {
            QueryModel latestByWrapper = compensateLatestBy(current, parent, outerToInnerAlias, depth);
            if (latestByWrapper != current) {
                if (parent != null) {
                    parent.setNestedModel(latestByWrapper);
                } else {
                    lateralJoinModel.setNestedModel(latestByWrapper);
                }
            }
        }

        // 4. Rewrite current layer correlated references
        rewriteSelectExpressions(current, outerToInnerAlias, depth);
        rewriteExpressionList(current.getOrderBy(), outerToInnerAlias, depth);
        rewriteExpressionList(current.getGroupBy(), outerToInnerAlias, depth);
        if (current.getSampleBy() != null
                && hasCorrelatedExprAtDepth(current.getSampleBy(), depth)) {
            current.setSampleBy(
                    rewriteOuterRefs(current.getSampleBy(), outerToInnerAlias, depth));
        }
        ObjList<ExpressionNode> latestBy = current.getLatestBy();
        for (int li = 0, ln = latestBy.size(); li < ln; li++) {
            ExpressionNode lb = latestBy.getQuick(li);
            if (hasCorrelatedExprAtDepth(lb, depth)) {
                latestBy.setQuick(li,
                        rewriteOuterRefs(lb, outerToInnerAlias, depth));
            }
        }

        if (current.getWhereClause() != null
                && hasCorrelatedExprAtDepth(current.getWhereClause(), depth)) {
            current.setWhereClause(
                    rewriteOuterRefs(current.getWhereClause(), outerToInnerAlias, depth));
        }

        // 5. Descend or terminate: handle main chain and join branches
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
                    // shifting existing branches right. Update jn and skip
                    // the inserted model (it has no correlated refs).
                    jn = current.getJoinModels().size();
                    ji = 1;
                }
                if (current.getUnionModel() != null) {
                    compensateSetOp(current, outerToInnerAlias, depth, outerRefJoinModel);
                }
            } else {
                pushDownOuterRefsForJoinBranch(
                        current, jm, jmNested, outerToInnerAlias,
                        countColAliases, outerRefJoinModel, depth
                );
            }
        }
    }

    // Handles a single join branch (ji > 0) inside the lateral subquery.
    // Each correlated branch gets its own cloned __qdb_outer_ref__ (with a fresh
    // alias like __qdb_outer_ref__1) and is recursively pushed down independently.
    // Correlated ON predicates are moved to the branch's WHERE and rewritten.
    // After recursion, alignment join criteria are built for the branch.
    //
    // Example:
    //   LATERAL (SELECT ... FROM t1 JOIN t2 ON t2.x = o.id WHERE t1.id = o.id)
    //   Main chain (t1): gets __qdb_outer_ref__0 via terminateHere
    //   Join branch (t2): gets cloned __qdb_outer_ref__1, ON t2.x = o.id is
    //     rewritten to WHERE t2.x = __qdb_outer_ref__1.id inside t2's subquery
    //
    // TODO: the Neumann-Kemper paper prescribes different strategies per join type:
    //  - INNER: current approach (move ON to WHERE) is semantically equivalent
    //  - LEFT: moving correlated ON to WHERE changes LEFT JOIN semantics (unmatched
    //    rows that should produce NULLs may be filtered out). Currently safe because
    //    the correlated predicate filters the data source, and the alignment ON
    //    preserves LEFT JOIN behavior at the outer level. But edge cases with
    //    non-equality correlations in LEFT JOIN ON may need special handling.
    //  - RIGHT/FULL OUTER: forced to process even without correlation
    //    (isDelimRequiredForJoinType) but uses the same approach as INNER.
    private void pushDownOuterRefsForJoinBranch(
            QueryModel current,
            QueryModel jm,
            QueryModel jmNested,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            ObjList<CharSequence> countColAliases,
            QueryModel outerRefJoinModel,
            int depth
    ) throws SqlException {
        boolean hasCorrelatedCriteria = jm.getJoinCriteria() != null
                && hasCorrelatedExprAtDepth(jm.getJoinCriteria(), depth);

        if (hasCorrelatedCriteria) {
            ExpressionNode jmCrit = jm.getJoinCriteria();
            splitAndPredicates(jmCrit, innerJoinCorrelated);
            ExpressionNode remainingJoinCrit = null;
            for (int ci = 0, cn = innerJoinCorrelated.size(); ci < cn; ci++) {
                ExpressionNode pred = innerJoinCorrelated.getQuick(ci);
                if (hasCorrelatedExprAtDepth(pred, depth)) {
                    ExpressionNode rewritten = rewriteOuterRefs(pred, outerToInnerAlias, depth);
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
            return;
        }
        boolean isJmCorrelated = jmNested.isCorrelatedAtDepth(depth);
        boolean isDelimRequiredForJoinType = jm.getJoinType() == QueryModel.JOIN_RIGHT_OUTER
                || jm.getJoinType() == QueryModel.JOIN_FULL_OUTER;
        if (!(isJmCorrelated || isDelimRequiredForJoinType || hasCorrelatedCriteria)) {
            return;
        }

        characterStore.newEntry();
        characterStore.put(OUTER_REF_PREFIX).put(outerRefId++);
        CharSequence cloneAlias = characterStore.toImmutable();

        QueryModel cloneSubquery = queryModelPool.next();
        cloneSubquery.setDistinct(true);
        QueryModel origSubquery = outerRefJoinModel.getNestedModel();
        cloneSubquery.setNestedModel(origSubquery.getNestedModel()
                .deepClone(queryModelPool, queryColumnPool, expressionNodePool, windowExpressionPool));

        copyColumnNameToAliasMap(origSubquery, cloneSubquery);

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

        int aliasSaveBase = outerAliasSaveStack.size();
        ObjList<CharSequence> oKeys = outerToInnerAlias.keys();
        for (int ok = 0, okn = oKeys.size(); ok < okn; ok++) {
            CharSequence key = oKeys.getQuick(ok);
            if (key == null) continue;
            CharSequence origValue = outerToInnerAlias.get(key);
            outerAliasSaveStack.add(origValue);
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
        int savedIdx = aliasSaveBase;
        for (int ok = 0, okn = oKeys.size(); ok < okn; ok++) {
            CharSequence key = oKeys.getQuick(ok);
            if (key == null) {
                continue;
            }

            CharSequence cloneColAlias = outerToInnerAlias.get(key);
            CharSequence mainColAlias = outerAliasSaveStack.getQuick(savedIdx);

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
        savedIdx = aliasSaveBase;
        for (int ok = 0, okn = oKeys.size(); ok < okn; ok++) {
            CharSequence key = oKeys.getQuick(ok);
            if (key == null) continue;
            outerToInnerAlias.put(key, outerAliasSaveStack.getQuick(savedIdx));
            savedIdx++;
        }
        outerAliasSaveStack.setPos(aliasSaveBase);
    }

    private void pushMatchingPredicates(
            QueryModel outerRefBase,
            ObjList<ExpressionNode> predicates
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

        for (int i = 0, n = predicates.size(); i < n; i++) {
            ExpressionNode pred = predicates.getQuick(i);
            if (allLiteralsBelongTo(pred, baseAlias, outerRefBase)) {
                ExpressionNode cloned = ExpressionNode.deepClone(expressionNodePool, pred);
                ExpressionNode existing = outerRefBase.getWhereClause();
                outerRefBase.setWhereClause(
                        existing != null ? createBinaryOp("and", existing, cloned) : cloned
                );
            }
        }
    }

    // Copies outer WHERE predicates that reference a single outer table into
    // the __qdb_outer_ref__ base model, so the DISTINCT subquery filters early.
    // E.g. for: SELECT * FROM orders o ... WHERE o.status = 'ACTIVE'
    //   the __qdb_outer_ref__ base becomes:
    //   (SELECT DISTINCT id FROM orders WHERE status = 'ACTIVE') __qdb_outer_ref__0
    //   instead of scanning the full orders table.
    private void pushOuterWhereToRefBases(QueryModel outerModel, QueryModel outerRefSubquery) {
        ExpressionNode where = outerModel.getWhereClause();
        if (where == null) {
            return;
        }

        splitAndPredicates(where, nonCorrelatedPreds);

        pushMatchingPredicates(outerRefSubquery.getNestedModel(), nonCorrelatedPreds);
        for (int i = 1, n = outerRefSubquery.getJoinModels().size(); i < n; i++) {
            pushMatchingPredicates(outerRefSubquery.getJoinModels().getQuick(i), nonCorrelatedPreds);
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

    // Collects aliases of columns whose top-level expression is count().
    // These columns are later wrapped with coalesce(x, 0) so that LEFT
    // LATERAL no-match rows produce 0 instead of NULL.
    //
    // Limitation: only bare count() are detected. Expressions
    // that embed count (e.g., count(*) + 2) are NOT detected — after decorrelation
    // the GROUP BY eliminates empty groups entirely, so the outer LEFT JOIN
    // NULL-fills the whole row. Wrapping the outer reference with coalesce(x, 0)
    // would produce 0, not the correct value (2). Fixing this requires extracting
    // count into a separate projected column inside the lateral body and rebuilding
    // the expression at the parent level with coalesce applied only to the count
    // part.
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

    private void rewriteExpressionList(
            ObjList<ExpressionNode> list,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth
    ) {
        for (int i = 0, n = list.size(); i < n; i++) {
            ExpressionNode expr = list.getQuick(i);
            if (hasCorrelatedExprAtDepth(expr, depth)) {
                list.setQuick(i,
                        rewriteOuterRefs(expr, outerToInnerAlias, depth));
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
                    && matchesOuterRefAlias(ast.token, outerRefAlias)) {
                CharSequence innerEquiv = lookupOuterRefAlias(ast.token, outerRefAlias, aliasMap);
                if (innerEquiv != null) {
                    col.of(col.getAlias(), expressionNodePool.next().of(
                            ExpressionNode.LITERAL, innerEquiv, ast.precedence, ast.position
                    ), col.isIncludeIntoWildcard());
                } else {
                    model.removeColumn(j);
                    continue;
                }
            } else {
                ExpressionNode rewritten = rewriteOuterRefs(ast, aliasMap, 0);
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
                    && matchesOuterRefAlias(node.token, outerRefAlias)) {
                CharSequence innerEquiv = lookupOuterRefAlias(node.token, outerRefAlias, aliasMap);
                if (innerEquiv != null) {
                    node.token = innerEquiv;
                }
            }
        }
    }

    private ExpressionNode rewriteOuterRefs(
            ExpressionNode node,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth
    ) {
        if (node == null) {
            return null;
        }
        if (node.type == ExpressionNode.QUERY && node.queryModel != null) {
            rewriteOuterRefsInSubquery(node.queryModel, outerToInnerAlias, depth);
            return node;
        }
        if (node.type == ExpressionNode.LITERAL) {
            CharSequence mapped = outerToInnerAlias.get(node.token);
            if (mapped != null) {
                return expressionNodePool.next().of(
                        ExpressionNode.LITERAL, mapped, node.precedence, node.position
                );
            }

            int dot = Chars.indexOf(node.token, '.');
            if (dot > 0 && Chars.startsWith(node.token, OUTER_REF_PREFIX)) {
                mapped = outerToInnerAlias.get(node.token, dot + 1, node.token.length());
                if (mapped != null) {
                    return expressionNodePool.next().of(
                            ExpressionNode.LITERAL, mapped, node.precedence, node.position
                    );
                }
            }

            if (Chars.indexOf(node.token, '.') < 0 && node.lateralDepth == depth) {
                ObjList<CharSequence> keys = outerToInnerAlias.keys();
                for (int i = 0, n = keys.size(); i < n; i++) {
                    CharSequence key = keys.getQuick(i);
                    if (key != null) {
                        int kDot = Chars.indexOf(key, '.');
                        if (kDot > 0 && Chars.equalsIgnoreCase(
                                node.token, key, kDot + 1, key.length())) {
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
        node.lhs = rewriteOuterRefs(node.lhs, outerToInnerAlias, depth);
        node.rhs = rewriteOuterRefs(node.rhs, outerToInnerAlias, depth);
        for (int i = 0, n = node.args.size(); i < n; i++) {
            node.args.setQuick(i, rewriteOuterRefs(
                    node.args.getQuick(i), outerToInnerAlias, depth));
        }
        if (node.windowExpression != null) {
            ObjList<ExpressionNode> partitionBy = node.windowExpression.getPartitionBy();
            for (int i = 0, n = partitionBy.size(); i < n; i++) {
                partitionBy.setQuick(i, rewriteOuterRefs(
                        partitionBy.getQuick(i), outerToInnerAlias, depth));
            }
            ObjList<ExpressionNode> winOrderBy = node.windowExpression.getOrderBy();
            for (int i = 0, n = winOrderBy.size(); i < n; i++) {
                winOrderBy.setQuick(i, rewriteOuterRefs(
                        winOrderBy.getQuick(i), outerToInnerAlias, depth));
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
            ExpressionNode rewritten = rewriteOuterRefs(where, aliasMap, 0);
            if (rewritten != where) {
                model.setWhereClause(rewritten);
            }
        }
        ObjList<ExpressionNode> groupBy = model.getGroupBy();
        for (int j = 0, sz = groupBy.size(); j < sz; j++) {
            groupBy.setQuick(j, rewriteOuterRefs(groupBy.getQuick(j), aliasMap, 0));
        }
        ObjList<ExpressionNode> orderBy = model.getOrderBy();
        for (int j = 0, sz = orderBy.size(); j < sz; j++) {
            orderBy.setQuick(j, rewriteOuterRefs(orderBy.getQuick(j), aliasMap, 0));
        }
        if (model.getSampleBy() != null) {
            model.setSampleBy(rewriteOuterRefs(model.getSampleBy(), aliasMap, 0));
        }
        ObjList<ExpressionNode> latestBy = model.getLatestBy();
        for (int j = 0, sz = latestBy.size(); j < sz; j++) {
            latestBy.setQuick(j, rewriteOuterRefs(latestBy.getQuick(j), aliasMap, 0));
        }
        for (int j = 1, sz = model.getJoinModels().size(); j < sz; j++) {
            QueryModel jm = model.getJoinModels().getQuick(j);
            ExpressionNode jc = jm.getJoinCriteria();
            if (jc != null) {
                ExpressionNode rewritten = rewriteOuterRefs(jc, aliasMap, 0);
                if (rewritten != jc) {
                    jm.setJoinCriteria(rewritten);
                }
            }
        }
    }

    private void rewriteOuterRefsInSubquery(
            QueryModel subquery,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth
    ) {
        ObjList<QueryColumn> cols = subquery.getBottomUpColumns();
        for (int i = 0, n = cols.size(); i < n; i++) {
            QueryColumn col = cols.getQuick(i);
            ExpressionNode ast = col.getAst();
            ExpressionNode rewritten = rewriteOuterRefs(ast, outerToInnerAlias, depth);
            if (rewritten != ast) {
                col.of(col.getAlias(), rewritten);
            }
        }
        if (subquery.getWhereClause() != null) {
            subquery.setWhereClause(
                    rewriteOuterRefs(subquery.getWhereClause(), outerToInnerAlias, depth)
            );
        }
        ObjList<ExpressionNode> orderBy = subquery.getOrderBy();
        for (int i = 0, n = orderBy.size(); i < n; i++) {
            orderBy.setQuick(i,
                    rewriteOuterRefs(orderBy.getQuick(i), outerToInnerAlias, depth));
        }
        ObjList<ExpressionNode> groupBy = subquery.getGroupBy();
        for (int i = 0, n = groupBy.size(); i < n; i++) {
            groupBy.setQuick(i,
                    rewriteOuterRefs(groupBy.getQuick(i), outerToInnerAlias, depth));
        }
        if (subquery.getSampleBy() != null) {
            subquery.setSampleBy(
                    rewriteOuterRefs(subquery.getSampleBy(), outerToInnerAlias, depth));
        }
        ObjList<ExpressionNode> latestBy = subquery.getLatestBy();
        for (int i = 0, n = latestBy.size(); i < n; i++) {
            latestBy.setQuick(i,
                    rewriteOuterRefs(latestBy.getQuick(i), outerToInnerAlias, depth));
        }
        if (subquery.getLimitLo() != null) {
            subquery.setLimit(
                    rewriteOuterRefs(subquery.getLimitLo(), outerToInnerAlias, depth),
                    subquery.getLimitHi() != null
                            ? rewriteOuterRefs(subquery.getLimitHi(), outerToInnerAlias, depth)
                            : null
            );
        }
        for (int i = 1, n = subquery.getJoinModels().size(); i < n; i++) {
            QueryModel jm = subquery.getJoinModels().getQuick(i);
            if (jm.getJoinCriteria() != null) {
                jm.setJoinCriteria(
                        rewriteOuterRefs(jm.getJoinCriteria(), outerToInnerAlias, depth)
                );
            }
        }
        if (subquery.getNestedModel() != null) {
            rewriteOuterRefsInSubquery(subquery.getNestedModel(), outerToInnerAlias, depth);
        }
        if (subquery.getUnionModel() != null) {
            rewriteOuterRefsInSubquery(subquery.getUnionModel(), outerToInnerAlias, depth);
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
                        rewriteOuterRefs(ast, outerToInnerAlias, depth));
            }
        }
    }

    private void scanModelForCorrelatedRefs(
            QueryModel current,
            QueryModel outerModel,
            int lateralJoinIndex,
            int lateralDepth,
            ObjList<LowerCaseCharSequenceIntHashMap> correlatedColumns
    ) {
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
    }

    private void scanWhereForOuterRefEqualities(
            ExpressionNode node,
            CharSequence outerRefAlias,
            LowerCaseCharSequenceObjHashMap<CharSequence> result
    ) {
        sqlNodeStack.clear();
        while (node != null) {
            if (SqlKeywords.isAndKeyword(node.token)) {
                if (node.rhs != null) {
                    sqlNodeStack.push(node.rhs);
                }
                node = node.lhs;
            } else {
                if (Chars.equals(node.token, "=")) {
                    if (isOuterRefToken(node.lhs, outerRefAlias) && isSimpleColumnRef(node.rhs)) {
                        result.put(node.lhs.token, node.rhs.token);
                    } else if (isOuterRefToken(node.rhs, outerRefAlias) && isSimpleColumnRef(node.lhs)) {
                        result.put(node.rhs.token, node.lhs.token);
                    }
                }
                node = sqlNodeStack.poll();
            }
        }
    }

    // Builds the data source for __qdb_outer_ref__ by cloning the outer model(s)
    // that provide correlated columns. The caller wraps this with DISTINCT and
    // explicit SELECT of only the correlated columns, e.g.:
    //
    //   SELECT DISTINCT o.id AS __qdb_outer_ref__0_id FROM orders o
    //
    // When correlated columns come from multiple outer join models (e.g. t1.a
    // and t2.b), this method reconstructs the join structure so the DISTINCT
    // subquery sees all required sources.
    //
    // TODO: when the lateral subquery has multiple join/union branches, each
    //  branch gets its own copy of __qdb_outer_ref__ via terminateHere, causing
    //  the DISTINCT subquery to execute multiple times. Like CTE, this needs a
    //  materializing RecordCursorFactory that executes once and serves multiple
    //  cursors from the cached result.
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
                QueryModel nestedBase = outerRefSubquery.getNestedModel();
                if (outerJm.getJoinCriteria() != null) {
                    outerRefBase.setJoinType(outerJm.getJoinType());
                    outerRefBase.setJoinCriteria(
                            ExpressionNode.deepClone(expressionNodePool, outerJm.getJoinCriteria())
                    );
                } else {
                    outerRefBase.setJoinType(QueryModel.JOIN_CROSS);
                }
                if (outerJm.getWhereClause() != null) {
                    outerRefBase.setWhereClause(
                            ExpressionNode.deepClone(expressionNodePool, outerJm.getWhereClause())
                    );
                }
                int jmIndex = nestedBase.getJoinModels().size();
                nestedBase.getJoinModels().add(outerRefBase);
                registerDataSourceAlias(nestedBase, outerRefBase, jmIndex);
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
                node = sqlNodeStack.poll();
            }
        }
    }

    // Inserts the __qdb_outer_ref__ join model at the bottom-most data source
    // layer of the lateral subquery's nestedModel chain. pushDownOuterRefs walks
    // down the chain and calls terminateHere when it reaches a node that has a
    // table, joins, or cannot descend further. This is where the CROSS JOIN to
    // __qdb_outer_ref__ is physically attached so that correlated predicates
    // (rewritten to reference __qdb_outer_ref__ columns) can be evaluated.
    //
    //   QueryModel chain:    SELECT → GROUP BY → [table: trades]
    //                                                ↑ terminateHere inserts here
    //   Result:              SELECT → GROUP BY → trades CROSS JOIN __qdb_outer_ref__0
    //                                            WHERE order_id = __qdb_outer_ref__0.id
    private void terminateHere(
            QueryModel current,
            QueryModel outerRefJoinModel,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth
    ) throws SqlException {
        ObjList<QueryModel> joinModels = current.getJoinModels();
        final int insertPos = 1;
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
        extractCorrelatedFromInnerJoins(current, correlatedPreds, outerToInnerAlias, depth);
        ExpressionNode joinCrit = null;
        for (int j = 0, m = correlatedPreds.size(); j < m; j++) {
            ExpressionNode rewritten = rewriteOuterRefs(
                    correlatedPreds.getQuick(j), outerToInnerAlias, depth);
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
            if (!isSimpleChain(branchTop, dataSourceLayer, outerRefAlias, outerToInnerAlias)) {
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
                        && matchesOuterRefAlias(ast.token, outerRefAlias)
                        && outerToInnerAlias.get(ast.token) == null) {
                    current.removeColumn(j);
                }
            }
            current = current.getNestedModel();
        }
        if (joinModel != null && !isAllEqualities) {
            joinModel.setJoinCriteria(simpleChainCriteria);
        }

        ObjList<QueryColumn> branchTopCols = branchTop.getBottomUpColumns();
        if (branchTopCols.size() > 0 && isWildcard(branchTopCols)) {
            for (int j = 0, sz = outerRefCols.size(); j < sz; j++) {
                CharSequence colAlias = outerRefCols.getQuick(j).getAlias();
                if (branchTop.getAliasToColumnMap().excludes(colAlias)) {
                    CharSequence innerEquiv = outerToInnerAlias.get(colAlias);
                    if (innerEquiv == null) {
                        continue;
                    }
                    CharSequence astToken = branchTop.getNestedModel() != dataSourceLayer ? colAlias : innerEquiv;
                    ExpressionNode ref = expressionNodePool.next().of(
                            ExpressionNode.LITERAL, astToken, 0, 0
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

    // Pass 3: top-down traversal. Scans join models for __qdb_outer_ref__ data
    // sources and attempts to eliminate them. Uses the alignment join criteria
    // (e.g. sub.order_id = o.id) to build an alias mapping, then replaces all
    // __qdb_outer_ref__ column references with their inner equivalents and removes
    // the __qdb_outer_ref__ join model from the tree. This converts the decorrelated
    // plan back to a simple join without the synthetic outer-ref data source.
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
                if (joinAlias != null) {
                    characterStore.newEntry();
                    characterStore.put(joinAlias).put('.').put(countColAliases.getQuick(i));
                    deferred.add(characterStore.toImmutable());
                } else {
                    deferred.add(countColAliases.getQuick(i));
                }
            }
            return;
        }
        for (int i = 0, n = parentCols.size(); i < n; i++) {
            QueryColumn pc = parentCols.getQuick(i);
            ExpressionNode ast = pc.getAst();
            if (ast == null) {
                continue;
            }
            ExpressionNode rewritten = wrapCountRefsWithCoalesce(ast, joinAlias, countColAliases);
            if (rewritten != ast) {
                pc.of(pc.getAlias(), rewritten, pc.isIncludeIntoWildcard());
            }
        }
    }

    private ExpressionNode wrapCountRefsWithCoalesce(
            ExpressionNode node,
            CharSequence joinAlias,
            ObjList<CharSequence> countColAliases
    ) {
        if (node == null) {
            return null;
        }
        if (isCountLiteralMatch(node, joinAlias, countColAliases)) {
            return assembleCoalesce(node);
        }
        sqlNodeStack.clear();
        sqlNodeStack.push(node);
        while (!sqlNodeStack.isEmpty()) {
            ExpressionNode current = sqlNodeStack.poll();
            if (current.lhs != null) {
                if (isCountLiteralMatch(current.lhs, joinAlias, countColAliases)) {
                    current.lhs = assembleCoalesce(current.lhs);
                } else {
                    sqlNodeStack.push(current.lhs);
                }
            }
            if (current.rhs != null) {
                if (isCountLiteralMatch(current.rhs, joinAlias, countColAliases)) {
                    current.rhs = assembleCoalesce(current.rhs);
                } else {
                    sqlNodeStack.push(current.rhs);
                }
            }
            for (int i = 0, n = current.args.size(); i < n; i++) {
                ExpressionNode arg = current.args.getQuick(i);
                if (arg != null) {
                    if (isCountLiteralMatch(arg, joinAlias, countColAliases)) {
                        current.args.setQuick(i, assembleCoalesce(arg));
                    } else {
                        sqlNodeStack.push(arg);
                    }
                }
            }
        }
        return node;
    }
}
