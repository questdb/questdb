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
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.model.QueryModelWrapper;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.LowerCaseCharSequenceObjHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;

import java.util.ArrayDeque;

import static io.questdb.griffin.SqlOptimiser.checkForChildWindowFunctions;
import static io.questdb.griffin.SqlOptimiser.replaceAndTransferDependents;
import static io.questdb.griffin.model.IQueryModel.isLateralJoin;

/**
 * Decorrelates lateral joins into standard hash joins,
 * based on the Neumann-Kemper "Unnesting Arbitrary Queries" technique.
 *
 * @see <a href="https://btw-2015.informatik.uni-hamburg.de/res/proceedings/Hauptband/Wiss/Neumann-Unnesting_Arbitrary_Querie.pdf">
 * Neumann, Kemper — Unnesting Arbitrary Queries (BTW 2015)</a>
 */
class LateralJoinRewriter implements Mutable {
    private static final int CORRELATED_WHERE = 1;
    private static final int CORRELATED_PROJECTION = CORRELATED_WHERE << 1;
    private static final int CORRELATED_ORDER_BY = CORRELATED_PROJECTION << 1;
    private static final int CORRELATED_GROUP_BY = CORRELATED_ORDER_BY << 1;
    private static final int CORRELATED_SAMPLE_BY = CORRELATED_GROUP_BY << 1;
    private static final int CORRELATED_LATEST_BY = CORRELATED_SAMPLE_BY << 1;
    private static final int CORRELATED_LIMIT = CORRELATED_LATEST_BY << 1;
    private static final int CORRELATED_JOIN_ON = CORRELATED_LIMIT << 1;

    private static final String COUNT_DRIVER_PREFIX = "__qdb_count_driver__";
    // sentinel for a LIMIT term that is not a compile-time constant; a bare
    // CONSTANT token is unsigned so a real limit can never collide with it
    private static final long LIMIT_NOT_CONSTANT = Long.MIN_VALUE;
    // Verdict on what one lateral body layer does to the single row a scalar
    // count emits: the row provably survives, it provably goes, or it cannot be
    // decided at compile time. Introduced for LIMIT, now applied to every layer
    // that could remove the aggregate row.
    private static final int ROW_DROPPED = 1;
    private static final int ROW_KEPT = 0;
    private static final int ROW_UNPROVABLE = 2;
    private static final int CMP_EQ = 0;
    private static final int CMP_GE = 5;
    private static final int CMP_GT = 4;
    private static final int CMP_LE = 3;
    private static final int CMP_LT = 2;
    private static final int CMP_NE = 1;
    private static final int CMP_NONE = -1;
    private static final String OUTER_REF_PREFIX = "__qdb_outer_ref__";
    private static final byte TERMINATE_AT_NESTED = 2;
    private static final byte TERMINATE_DESCEND = 3;
    private static final byte TERMINATE_HERE = 1;
    private static final byte TERMINATE_SKIP = 0;
    // truth interval of the wrapper predicate currently being analysed, over the
    // count column's domain; written by truthIntervalOf
    private long truthLo;
    private long truthHi;
    private final CharacterStore characterStore;
    private final ObjList<ExpressionNode> correlatedPreds = new ObjList<>();
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
    private final ObjectPool<QueryModelWrapper> queryModelWrapperPool;
    private final ObjHashSet<QueryModel> sharedModels = new ObjHashSet<>();
    private final ArrayDeque<ExpressionNode> sqlNodeStack;
    private final ArrayDeque<ExpressionNode> sqlNodeStack2;
    private final ObjectPool<WindowExpression> windowExpressionPool;
    private final ObjList<IQueryModel> wrapperKeyLayers = new ObjList<>();
    // guard for the lateral body currently being decorrelated; see accumulateScalarCountGuard
    private ExpressionNode scalarCountGuard;
    // set when a run-time LIMIT reads an outer column: such a guard cannot be
    // evaluated in the outer projection, so a scalar-count body must be rejected
    private ExpressionNode scalarCountGuardBlocker;
    // set once the push-down descent crosses an aggregation layer: a LIMIT below
    // the aggregate only caps its input and can never drop the aggregate row, so
    // it must not enter the guard (a run-time 0 would masquerade as NULL)
    private boolean scalarCountGuardDisarmed;
    private boolean hasCorrelation;
    private int outerRefId;

    LateralJoinRewriter(
            CharacterStore characterStore,
            ObjectPool<ExpressionNode> expressionNodePool,
            ObjectPool<QueryColumn> queryColumnPool,
            ObjectPool<QueryModel> queryModelPool,
            ObjectPool<QueryModelWrapper> queryModelWrapperPool,
            ObjectPool<WindowExpression> windowExpressionPool,
            ArrayDeque<ExpressionNode> sqlNodeStack,
            ArrayDeque<ExpressionNode> sqlNodeStack2,
            FunctionParser functionParser,
            ObjList<ExpressionNode> tempGroupingCols,
            ObjList<ExpressionNode> tempOuterCols,
            ObjList<ExpressionNode> tempOrderBySave,
            IntList tempOrderByDirSave,
            LowerCaseCharSequenceObjHashMap<CharSequence> tempOuterToInnerAlias,
            ObjList<CharSequence> tempOuterAliasSaveStack
    ) {
        this.characterStore = characterStore;
        this.expressionNodePool = expressionNodePool;
        this.queryColumnPool = queryColumnPool;
        this.queryModelPool = queryModelPool;
        this.queryModelWrapperPool = queryModelWrapperPool;
        this.windowExpressionPool = windowExpressionPool;
        this.sqlNodeStack = sqlNodeStack;
        this.sqlNodeStack2 = sqlNodeStack2;
        this.functionParser = functionParser;
        this.groupingCols = tempGroupingCols;
        this.outerCols = tempOuterCols;
        this.orderBySave = tempOrderBySave;
        this.orderByDirSave = tempOrderByDirSave;
        this.outerToInnerAlias = tempOuterToInnerAlias;
        this.outerAliasSaveStack = tempOuterAliasSaveStack;
    }

    @Override
    public void clear() {
        scalarCountGuard = null;
        scalarCountGuardBlocker = null;
        scalarCountGuardDisarmed = false;
        correlatedPreds.clear();
        innerJoinCorrelated.clear();
        innerJoinNonCorrelated.clear();
        nonCorrelatedPreds.clear();
        hasCorrelation = false;
        outerRefId = 0;
        sharedModels.clear();
    }

    public void rewrite(IQueryModel model) throws SqlException {
        if (!model.isOptimisable()) {
            return;
        }

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

    static void excludeGeneratedColumnsFromWildcard(IQueryModel model) {
        ObjList<QueryColumn> columns = model.getBottomUpColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            QueryColumn column = columns.getQuick(i);
            if (column.isGenerated()) {
                column.setIncludeIntoWildcard(false);
            }
        }

        ObjList<CharSequence> aliases = model.getAliasToColumnMap().keys();
        for (int i = 0, n = aliases.size(); i < n; i++) {
            CharSequence alias = aliases.getQuick(i);
            if (alias != null) {
                QueryColumn column = model.getAliasToColumnMap().get(alias);
                if (column != null && column.isGenerated()) {
                    column.setIncludeIntoWildcard(false);
                }
            }
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

    private static boolean hasBareWildcard(ObjList<QueryColumn> columns) {
        for (int i = 0, n = columns.size(); i < n; i++) {
            ExpressionNode ast = columns.getQuick(i).getAst();
            if (ast != null && ast.isWildcard() && Chars.equals(ast.token, "*")) {
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

    private static void registerDataSourceAlias(IQueryModel parent, IQueryModel dataSource, int index) {
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
            case IQueryModel.JOIN_LATERAL_INNER -> IQueryModel.JOIN_INNER;
            case IQueryModel.JOIN_LATERAL_LEFT -> IQueryModel.JOIN_LEFT_OUTER;
            case IQueryModel.JOIN_LATERAL_CROSS -> IQueryModel.JOIN_CROSS;
            default -> throw new AssertionError("unexpected lateral join type: " + lateralJoinType);
        };
    }

    private static CharSequence unqualify(CharSequence token) {
        int dotPos = Chars.indexOfLastUnquoted(token, '.');
        return dotPos > 0 ? token.subSequence(dotPos + 1, token.length()) : token;
    }

    private void addColumnToOuterRefSelect(CharSequence outerRefAlias, IQueryModel outerRefSubquery, ExpressionNode outerCol) {
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
        qc.setGenerated(true);
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
            IQueryModel outerRefBase
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
    private boolean analyzeCorrelation(IQueryModel model, int lateralDepth) {
        if (!model.isOptimisable()) {
            return false;
        }
        boolean hasLateral = false;
        if (model.getNestedModel() != null) {
            hasLateral = analyzeCorrelation(model.getNestedModel(), lateralDepth);
        }
        if (model.getUnionModel() != null) {
            hasLateral |= analyzeCorrelation(model.getUnionModel(), lateralDepth);
        }

        for (int i = 1, n = model.getJoinModels().size(); i < n; i++) {
            IQueryModel joinModel = model.getJoinModels().getQuick(i);
            if (isLateralJoin(joinModel.getJoinType())) {
                hasLateral = true;
                int newDepth = lateralDepth + 1;
                IQueryModel topInner = joinModel.getNestedModel();
                assert topInner != null;
                collectCorrelatedRefs(topInner, model, i, newDepth);
                analyzeCorrelation(topInner, newDepth);
            } else if (joinModel.getNestedModel() != null) {
                hasLateral |= analyzeCorrelation(joinModel.getNestedModel(), lateralDepth);
            }
        }
        return hasLateral;
    }

    private void buildOuterColsFromCorrelatedColumns(
            IQueryModel lateralJoinModel,
            IQueryModel outerModel,
            ObjList<ExpressionNode> result
    ) {
        ObjList<LowerCaseCharSequenceIntHashMap> corrCols = lateralJoinModel.getCorrelatedColumns();
        for (int j = 0, m = corrCols.size(); j < m; j++) {
            LowerCaseCharSequenceIntHashMap colSet = corrCols.getQuick(j);
            if (colSet == null || colSet.size() == 0) {
                continue;
            }
            IQueryModel outerJm = outerModel.getJoinModels().getQuick(j);
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

    // Returns true if per-side push optimization is possible:
    // - Main chain (nestedModel chain) has no correlated expressions
    // - All branches at terminateHere level are INNER/CROSS/RIGHT
    // - Every branch with correlated ON has a correlated nested model
    //   (so a clone can be pushed into it)
    // Per-side push: skip main chain CROSS JOIN (Neumann-Kemper transform
    // R ⋈ (D ⋈ S) instead of (R × D) ⋈ S). Conditions:
    //  1. Main chain has no own correlated expressions (R doesn't need D)
    //  2. All branches are INNER/CROSS/RIGHT (no LEFT/FULL row preservation)
    //  3. Table-model branches have no correlated ON (can't create clone)
    //  4. At least one correlated branch exists (alignment needs a source)
    //  5. No main chain layer carries a LIMIT. Lateral semantics apply a body
    //     LIMIT per outer row, but per-side push keeps the chain unkeyed, so
    //     the LIMIT would run globally over the decorrelated result, and the
    //     scalar-count guard would never be built (compensateLimit is not
    //     reached for chain layers on this path). The general path partitions
    //     the LIMIT per outer row and guards the count compensation.
    private boolean canPerSidePush(IQueryModel model, int depth) {
        IQueryModel m = model;
        while (m != null) {
            if (m.getLimitLo() != null || m.getLimitHi() != null) {
                return false;
            }
            if (m.isOwnCorrelatedAtDepth(depth, ~CORRELATED_JOIN_ON)) {
                return false;
            }
            IQueryModel nested = m.getNestedModel();
            if (nested == null || !nested.isCorrelatedAtDepth(depth)) {
                boolean hasCorrelatedBranch = false;
                for (int i = 1, n = m.getJoinModels().size(); i < n; i++) {
                    IQueryModel branch = m.getJoinModels().getQuick(i);
                    int jt = branch.getJoinType();
                    if (jt != IQueryModel.JOIN_INNER && jt != IQueryModel.JOIN_CROSS
                            && jt != IQueryModel.JOIN_RIGHT_OUTER) {
                        return false;
                    }
                    boolean hasCorrelatedOn = branch.getJoinCriteria() != null
                            && hasCorrelatedExprAtDepth(branch.getJoinCriteria(), depth);
                    if (hasCorrelatedOn && branch.getNestedModel() == null) {
                        return false;
                    }
                    if (hasCorrelatedOn || (branch.getNestedModel() != null
                            && branch.getNestedModel().isCorrelatedAtDepth(depth))) {
                        hasCorrelatedBranch = true;
                    }
                }
                return hasCorrelatedBranch;
            }
            m = nested;
        }
        return false;
    }

    private boolean canResolveColumn(CharSequence columnName, IQueryModel jm) {
        IQueryModel nested = jm.getNestedModel();
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

    private boolean canResolveColumnForOuter(CharSequence columnName, IQueryModel jm) {
        if (jm.getAliasToColumnNameMap().contains(columnName)) {
            return true;
        }
        IQueryModel nested = jm.getNestedModel();
        if (nested != null) {
            return resolveColumnInChild(columnName, nested);
        }
        return false;
    }

    private boolean cannotResolveAliasLocally(CharSequence alias, int end, IQueryModel model) {
        IQueryModel current = model;
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

    private IQueryModel cloneOuterRef(IQueryModel outerRefJoinModel) {
        return cloneOuterRef(outerRefJoinModel, OUTER_REF_PREFIX);
    }

    private IQueryModel cloneOuterRef(IQueryModel outerRefJoinModel, CharSequence aliasPrefix) {
        return cloneOuterRef(outerRefJoinModel, aliasPrefix, null);
    }

    private IQueryModel cloneOuterRef(
            IQueryModel outerRefJoinModel,
            CharSequence aliasPrefix,
            IQueryModel aliasOwner
    ) {
        CharSequence cloneAlias;
        do {
            characterStore.newEntry();
            characterStore.put(aliasPrefix).put(outerRefId++);
            cloneAlias = characterStore.toImmutable();
        } while (aliasOwner != null && aliasOwner.getModelAliasIndex(cloneAlias, 0, cloneAlias.length()) >= 0);

        IQueryModel origSubquery = outerRefJoinModel.getNestedModel();
        assert origSubquery instanceof QueryModel;
        IQueryModel sharedDistinct = createSharedRef((QueryModel) origSubquery);
        IQueryModel renamingLayer = queryModelPool.next();
        IQueryModel emptyLayer = queryModelPool.next();
        emptyLayer.setNestedModel(sharedDistinct);
        renamingLayer.setNestedModel(emptyLayer);

        ObjList<QueryColumn> origCols = origSubquery.getBottomUpColumns();
        for (int oc = 0, ocn = origCols.size(); oc < ocn; oc++) {
            QueryColumn origCol = origCols.getQuick(oc);
            CharSequence colName = unqualify(origCol.getAst().token);
            CharacterStoreEntry cse = characterStore.newEntry();
            cse.put(cloneAlias).put("_").put(colName);
            CharSequence newColAlias = cse.toImmutable();
            ExpressionNode ref = expressionNodePool.next().of(
                    ExpressionNode.LITERAL, origCol.getAlias(), 0, 0
            );
            QueryColumn generatedColumn = queryColumnPool.next().of(newColAlias, ref);
            generatedColumn.setGenerated(true);
            renamingLayer.addBottomUpColumnIfNotExists(generatedColumn);
        }

        IQueryModel clonedOuterRef = queryModelPool.next();
        clonedOuterRef.setNestedModel(renamingLayer);
        clonedOuterRef.setNestedModelIsSubQuery(true);
        ExpressionNode cloneAliasExpr = expressionNodePool.next().of(
                ExpressionNode.LITERAL, cloneAlias, 0, 0
        );
        clonedOuterRef.setAlias(cloneAliasExpr);
        clonedOuterRef.setJoinType(IQueryModel.JOIN_CROSS);
        return clonedOuterRef;
    }

    private void collectCorrelatedRef(
            ExpressionNode node,
            IQueryModel outerModel,
            int lateralJoinIndex,
            int lateralDepth,
            IQueryModel innerModel,
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
                    ensureCorrelatedColumnSet(correlatedColumns, jmIndex).put(
                            node.token,
                            node.position,
                            dotPos + 1,
                            node.token.length()
                    );
                    node.lateralDepth = lateralDepth;
                    hasCorrelation = true;
                }
            } else if (!canResolveColumn(node.token, innerModel)) {
                for (int j = 0; j < lateralJoinIndex; j++) {
                    IQueryModel outerJm = outerModel.getJoinModels().getQuick(j);
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
            IQueryModel topInner,
            IQueryModel outerModel,
            int lateralJoinIndex,
            int lateralDepth
    ) {
        ObjList<LowerCaseCharSequenceIntHashMap> correlatedColumns =
                outerModel.getJoinModels().getQuick(lateralJoinIndex).getCorrelatedColumns();
        collectCorrelatedRefs(topInner, outerModel, lateralJoinIndex, lateralDepth, correlatedColumns, true);
    }

    private void collectCorrelatedRefs(
            IQueryModel model,
            IQueryModel outerModel,
            int lateralJoinIndex,
            int lateralDepth,
            ObjList<LowerCaseCharSequenceIntHashMap> correlatedColumns,
            boolean isTrackingDepth
    ) {
        IQueryModel current = model;
        while (current != null) {
            if (isTrackingDepth) {
                hasCorrelation = false;
            }
            int correlationFlags = scanModelForCorrelatedRefs(current, outerModel, lateralJoinIndex, lateralDepth, correlatedColumns);

            if (isTrackingDepth && correlationFlags != 0) {
                current.makeCorrelatedAtDepth(lateralDepth, correlationFlags);
            }

            for (int i = 1, n = current.getJoinModels().size(); i < n; i++) {
                IQueryModel jm = current.getJoinModels().getQuick(i);
                if (jm.getNestedModel() != null) {
                    collectCorrelatedRefs(jm.getNestedModel(), outerModel, lateralJoinIndex, lateralDepth, correlatedColumns, isTrackingDepth);
                }
            }
            if (current.getUnionModel() != null) {
                collectCorrelatedRefs(current.getUnionModel(), outerModel, lateralJoinIndex, lateralDepth, correlatedColumns, isTrackingDepth);
            }

            current = current.getNestedModel();
        }
    }

    private void collectOuterRefAliases(IQueryModel model, ObjList<ExpressionNode> result) {
        IQueryModel m = model;
        while (m != null) {
            for (int j = 1, n = m.getJoinModels().size(); j < n; j++) {
                IQueryModel jm = m.getJoinModels().getQuick(j);
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
    private void compensateAggregate(IQueryModel inner) throws SqlException {
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
    private void compensateDistinct(IQueryModel inner) throws SqlException {
        for (int j = 0, m = groupingCols.size(); j < m; j++) {
            ExpressionNode col = groupingCols.getQuick(j);
            ensureColumnInSelect(inner, col, col.token);
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
    private IQueryModel compensateLatestBy(
            IQueryModel inner,
            IQueryModel parent,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth
    ) throws SqlException {
        ObjList<ExpressionNode> latestBy = inner.getLatestBy();

        boolean isNativeLatestByUsable = !hasNonEqualityCorrelation(inner.getWhereClause(), depth)
                && groupingCols.size() > 0;

        if (isNativeLatestByUsable) {
            for (int i = 0, n = groupingCols.size(); i < n; i++) {
                CharSequence outerRefAlias = groupingCols.getQuick(i).token;
                CharSequence innerCol = findInnerPhysicalColumn(inner.getWhereClause(), outerRefAlias, outerToInnerAlias);
                if (innerCol == null) {
                    isNativeLatestByUsable = false;
                    break;
                }
            }
        }

        if (isNativeLatestByUsable) {
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
    //             SELECT *, row_number() OVER (PARTITION BY __outer_ref__.id ORDER BY ts) AS __lateral_rn
    //             FROM trades ...
    //           ) WHERE __lateral_rn <= 3
    private IQueryModel compensateLimit(
            IQueryModel current,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth
    ) throws SqlException {
        final IQueryModel originalCurrent = current;
        ExpressionNode limitHi = current.getLimitHi();
        ExpressionNode limitLo = current.getLimitLo();
        if (limitHi == null && limitLo == null) {
            return current;
        }

        // compensateLimit turns LIMIT into `__lateral_rn <= limit`, which is a
        // contradiction for a negative limit. QuestDB's negative-limit semantics
        // (last |N| rows) cannot be expressed by that filter, so reject instead of
        // silently emptying the body.
        rejectNegativeLateralLimit(limitLo);
        rejectNegativeLateralLimit(limitHi);

        // Captured before rewriteOuterRefs turns outer refs into body-local aliases.
        // It must be a deep copy: rewriteOuterRefs rewrites operator nodes IN PLACE, so
        // holding the reference alone would hand the guard a rewritten alias.
        final ExpressionNode originalLimitLo = limitLo == null
                ? null : ExpressionNode.deepClone(expressionNodePool, limitLo);
        final ExpressionNode originalLimitHi = limitHi == null
                ? null : ExpressionNode.deepClone(expressionNodePool, limitHi);

        if (limitHi != null && hasCorrelatedExprAtDepth(limitHi, depth)) {
            limitHi = rewriteOuterRefs(limitHi, outerToInnerAlias, depth);
        }
        if (limitLo != null && hasCorrelatedExprAtDepth(limitLo, depth)) {
            limitLo = rewriteOuterRefs(limitLo, outerToInnerAlias, depth);
        }

        orderBySave.clear();
        orderByDirSave.clear();
        IQueryModel orderByModel = null;
        IQueryModel cur = current;
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

        CharSequence rnAlias = createColumnAlias("__lateral_rn", current);
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
        IQueryModel windowLayer = null;
        if (isWrappingNeeded) {
            for (int i = 0, n = groupingCols.size(); i < n; i++) {
                rnWindowExpr.getPartitionBy().add(
                        ExpressionNode.deepClone(expressionNodePool, groupingCols.getQuick(i)));
            }

            IQueryModel flatWrapper = queryModelPool.next();
            flatWrapper.setNestedModel(current);
            flatWrapper.setNestedModelIsSubQuery(true);

            windowLayer = queryModelPool.next();
            windowLayer.setNestedModel(flatWrapper);
            windowLayer.setNestedModelIsSubQuery(true);

            ObjList<QueryColumn> curCols = current.getBottomUpColumns();
            for (int i = 0, n = curCols.size(); i < n; i++) {
                copyColumn(windowLayer, curCols.getQuick(i));
            }
            for (int i = 0, n = groupingCols.size(); i < n; i++) {
                ExpressionNode gcol = groupingCols.getQuick(i);
                ExpressionNode ref = ExpressionNode.deepClone(expressionNodePool, gcol);
                CharSequence alias = createColumnAlias(gcol.token, windowLayer);
                QueryColumn groupingColumn = queryColumnPool.next().of(alias, ref);
                groupingColumn.setGenerated(true);
                windowLayer.addBottomUpColumn(groupingColumn);
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
            ExpressionNode upperBound = createBinaryOp("<=", rnRef, limitHi);
            ExpressionNode lowerBound = createBinaryOp(">",
                    ExpressionNode.deepClone(expressionNodePool, rnRef), limitLo);
            rnFilter = createBinaryOp("and", lowerBound, upperBound);
        } else {
            rnFilter = createBinaryOp("<=", rnRef, limitHi != null ? limitHi : limitLo);
        }

        // A scalar-count body emits exactly one row per group, so __lateral_rn is
        // always 1 there. Evaluating this very filter at row 1 therefore answers
        // "did the LIMIT keep the aggregate row?", which is exactly the condition
        // under which coalesce(count, 0) may be applied. Deriving the guard from the
        // filter itself means the two can never disagree, for any LIMIT form.
        accumulateScalarCountGuard(
                originalLimitLo,
                originalLimitHi,
                hasColumnRef(originalLimitLo) || hasColumnRef(originalLimitHi)
        );

        IQueryModel filterModel = queryModelPool.next();
        filterModel.setNestedModel(current);
        filterModel.setNestedModelIsSubQuery(true);
        filterModel.setWhereClause(rnFilter);

        IQueryModel outerSelect = queryModelPool.next();
        outerSelect.setNestedModel(filterModel);
        outerSelect.setNestedModelIsSubQuery(true);
        copyColumnsExcept(current, outerSelect, rnAlias);

        return replaceAndTransferDependents(originalCurrent, outerSelect);
    }

    // Adds groupingCols to SELECT so SAMPLE BY results are partitioned per outer row.
    // SAMPLE BY already acts as an implicit GROUP BY on the time bucket, so we only
    // need the outer ref column visible in SELECT for the alignment join.
    //   Before: SELECT ts, count(*) FROM trades WHERE ... SAMPLE BY 1h
    //   After:  SELECT ts, count(*), __outer_ref__.id FROM trades WHERE ... SAMPLE BY 1h
    private void compensateSampleBy(IQueryModel inner) throws SqlException {
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
            IQueryModel inner,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth,
            IQueryModel outerRefJm
    ) throws SqlException {
        IQueryModel current = inner.getUnionModel();
        while (current != null) {
            IQueryModel branchOuterRef = cloneOuterRef(outerRefJm);
            CharSequence cloneAlias = branchOuterRef.getAlias().token;
            int aliasSaveBase = saveAndRemapOuterToInnerAlias(cloneAlias);
            rebuildGroupingCols();
            boolean isPushDeep = current.getTableNameExpr() == null && current.getNestedModel() != null;
            if (!isPushDeep) {
                current.addJoinModel(branchOuterRef);
                current.addModelAliasIndex(branchOuterRef.getAlias(), current.getJoinModels().size() - 1);
            }

            rewriteSelectExpressions(current, outerToInnerAlias, depth);
            rewriteExpressionList(current.getOrderBy(), outerToInnerAlias, depth);

            for (int j = 0, m = groupingCols.size(); j < m; j++) {
                ensureColumnInSelect(current, groupingCols.getQuick(j), groupingCols.getQuick(j).token);
            }

            if (hasWindowColumns(current)) {
                compensateWindow(current, cloneAlias);
            }

            if (isPushDeep) {
                IQueryModel deepOuterRef = queryModelPool.next();
                deepOuterRef.setNestedModel(branchOuterRef.getNestedModel());
                deepOuterRef.setAlias(branchOuterRef.getAlias());
                deepOuterRef.setJoinType(IQueryModel.JOIN_CROSS);
                pushDownOuterRefs(
                        current, current.getNestedModel(), outerToInnerAlias,
                        false, deepOuterRef, current, depth
                );
            }

            restoreOuterToInnerAlias(aliasSaveBase);
            current = current.getUnionModel();
        }
    }

    // Adds groupingCols to every window function's PARTITION BY so the window
    // computes per outer row instead of across all rows globally.
    //   Before: SELECT qty, sum(qty) OVER (ORDER BY ts) FROM trades WHERE ...
    //   After:  SELECT qty, sum(qty) OVER (PARTITION BY __outer_ref__.id ORDER BY ts)
    //           , __outer_ref__.id FROM trades ...
    private void compensateWindow(IQueryModel inner, CharSequence outerRefAlias) throws SqlException {
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

    private IQueryModel convertLatestByToWindowFunction(
            final IQueryModel inner,
            IQueryModel parent,
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
            ExpressionNode cloned = latestBy.getQuick(i);
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
            rnWindowExpr.getOrderByDirection().add(IQueryModel.ORDER_DIRECTION_DESCENDING);
        }

        latestBy.clear();

        IQueryModel windowLayer = queryModelPool.next();
        windowLayer.setNestedModel(inner);
        if (parent != null && parent.getBottomUpColumns().size() > 0) {
            ObjList<QueryColumn> parentCols = parent.getBottomUpColumns();
            for (int i = 0, n = parentCols.size(); i < n; i++) {
                copyColumn(windowLayer, parentCols.getQuick(i));
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

        IQueryModel filterModel = queryModelPool.next();
        filterModel.setNestedModel(windowLayer);
        filterModel.setNestedModelIsSubQuery(true);
        filterModel.setWhereClause(createBinaryOp("=", rnRef, one));
        return replaceAndTransferDependents(inner, filterModel);
    }

    private void copyColumn(IQueryModel target, QueryColumn column) throws SqlException {
        ExpressionNode ref = expressionNodePool.next().of(
                ExpressionNode.LITERAL, column.getAlias(), 0, 0
        );
        QueryColumn wrapperColumn = queryColumnPool.next().of(
                column.getAlias(), ref, column.isIncludeIntoWildcard()
        );
        wrapperColumn.setGenerated(column.isGenerated());
        wrapperColumn.setLateralScalarCount(column.isLateralScalarCount());
        target.addBottomUpColumn(wrapperColumn);
    }

    private void copyColumnsExcept(
            IQueryModel source,
            IQueryModel target,
            CharSequence excludeAlias
    ) throws SqlException {
        ObjList<QueryColumn> cols = source.getBottomUpColumns();
        for (int i = 0, n = cols.size(); i < n; i++) {
            QueryColumn col = cols.getQuick(i);
            if (!Chars.equalsIgnoreCase(col.getAlias(), excludeAlias)) {
                copyColumn(target, col);
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

    private CharSequence createColumnAlias(CharSequence name, IQueryModel model) {
        return SqlUtil.createColumnAlias(
                characterStore,
                name,
                Chars.indexOfLastUnquoted(name, '.'),
                model.getAliasToColumnMap(),
                model.getAliasSequenceMap(),
                false
        );
    }

    private IQueryModel createOuterRefBase(IQueryModel outerJm) throws SqlException {
        IQueryModel outerRefBase = queryModelPool.next();
        if (outerJm.getTableNameExpr() != null) {
            outerRefBase.setTableNameExpr(
                    ExpressionNode.deepClone(expressionNodePool, outerJm.getTableNameExpr())
            );
        } else if (outerJm.getNestedModel() != null) {
            IQueryModel iNestModel = outerJm.getNestedModel();
            QueryModel nestModel = iNestModel instanceof QueryModelWrapper ? ((QueryModelWrapper) iNestModel).getDelegate() : (QueryModel) iNestModel;
            QueryModelWrapper wrapper = queryModelWrapperPool.next();
            nestModel.getSharedRefs().add(wrapper);
            wrapper.of(nestModel, nestModel.getSharedRefs().size());
            outerRefBase.setNestedModel(wrapper);
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

    private IQueryModel createSharedRef(QueryModel delegate) {
        if (sharedModels.add(delegate)) {
            return delegate;
        }
        QueryModelWrapper wrapper = queryModelWrapperPool.next();
        delegate.getSharedRefs().add(wrapper);
        wrapper.of(delegate, delegate.getSharedRefs().size());
        return wrapper;
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
    private void decorrelate(IQueryModel model, int lateralDepth, IQueryModel parent) throws SqlException {
        if (!model.isOptimisable()) {
            return;
        }
        if (model.getNestedModel() != null) {
            decorrelate(model.getNestedModel(), lateralDepth, model);
        }
        if (model.getUnionModel() != null) {
            decorrelate(model.getUnionModel(), lateralDepth, parent);
        }

        for (int i = 1, n = model.getJoinModels().size(); i < n; i++) {
            IQueryModel joinModel = model.getJoinModels().getQuick(i);
            if (IQueryModel.isLateralJoin(joinModel.getJoinType())) {
                IQueryModel topInner = joinModel.getNestedModel();
                assert topInner != null;

                // bottom up
                decorrelate(topInner, lateralDepth + 1, null);

                boolean isLeft = joinModel.getJoinType() == IQueryModel.JOIN_LATERAL_LEFT;

                outerCols.clear();
                buildOuterColsFromCorrelatedColumns(joinModel, model, outerCols);
                if (outerCols.size() == 0) {
                    joinModel.setJoinType(toDegradedJoinType(joinModel.getJoinType()));
                    continue;
                }

                int depth = lateralDepth + 1;
                outerToInnerAlias.clear();
                IQueryModel outerRefSubquery = queryModelPool.next();
                outerRefSubquery.setDistinct(true);
                characterStore.newEntry();
                characterStore.put(OUTER_REF_PREFIX).put(outerRefId++);
                CharSequence outerRefAlias = characterStore.toImmutable();

                for (int j = 0, m = outerCols.size(); j < m; j++) {
                    addColumnToOuterRefSelect(outerRefAlias, outerRefSubquery, outerCols.getQuick(j));
                }

                setupOuterRefDataSource(outerRefSubquery, model, joinModel.getCorrelatedColumns());
                pushOuterWhereToRefBases(model, outerRefSubquery);

                IQueryModel outerRefJoinModel = queryModelPool.next();
                outerRefJoinModel.setJoinType(IQueryModel.JOIN_CROSS);
                outerRefJoinModel.setNestedModel(outerRefSubquery);
                outerRefJoinModel.setNestedModelIsSubQuery(true);
                ExpressionNode outerRefAliasExpr = expressionNodePool.next().of(
                        ExpressionNode.LITERAL, outerRefAlias, 0, 0
                );
                outerRefJoinModel.setAlias(outerRefAliasExpr);

                // Push down outer refs
                boolean isPerSidePush = canPerSidePush(topInner, depth);
                boolean isScalarCountBody = isLeft && hasScalarCountBody(topInner);
                scalarCountGuard = null;
                scalarCountGuardBlocker = null;
                scalarCountGuardDisarmed = false;
                CharSequence perSideCloneAlias = null;
                if (isPerSidePush) {
                    perSideCloneAlias = pushDownPerSidePush(
                            topInner, outerToInnerAlias, outerRefJoinModel, isLeft, depth
                    );
                } else {
                    pushDownOuterRefs(
                            null, topInner, outerToInnerAlias, isLeft,
                            outerRefJoinModel, joinModel, depth
                    );
                }

                ExpressionNode originalOnCondition = joinModel.getJoinCriteria();

                topInner = joinModel.getNestedModel();
                CharSequence lateralAlias = joinModel.getAlias() != null
                        ? joinModel.getAlias().token : null;
                ExpressionNode joinCriteria = null;
                for (int j = 0, m = outerCols.size(); j < m; j++) {
                    ExpressionNode outerCol = outerCols.getQuick(j);
                    CharSequence outerRefColAlias = outerToInnerAlias.get(outerCol.token);
                    CharSequence selectAlias;
                    if (perSideCloneAlias != null) {
                        // pushDownPerSidePush already chained the key through
                        // every projection layer and recorded the topInner-visible
                        // alias; re-resolving the bare name here could capture a
                        // same-named user column
                        selectAlias = outerRefColAlias;
                    } else {
                        ExpressionNode outerRefNode = expressionNodePool.next().of(
                                ExpressionNode.LITERAL, outerRefColAlias, 0, 0
                        );
                        selectAlias = ensureColumnInSelect(topInner, outerRefNode, outerRefColAlias);
                    }
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

                excludeGeneratedColumnsFromWildcard(topInner);
                topInner.setOuterRefWildcardExcluded(true);

                if (isScalarCountBody && markScalarCountColumns(topInner)) {
                    IQueryModel selectModel = (model.getBottomUpColumns().size() > 0 || parent == null)
                            ? model : parent;
                    rejectCorrelatedScalarCountLimit();
                    selectModel.setLateralCountCoalesceRequired(true);
                    selectModel.setLateralCountCoalesceGuard(scalarCountGuard);
                }
            } else if (joinModel.getNestedModel() != null) {
                decorrelate(joinModel.getNestedModel(), lateralDepth, null);
            }
        }
    }

    private CharSequence ensureColumnInSelect(
            IQueryModel model,
            ExpressionNode colExpr,
            CharSequence preferredAlias
    ) throws SqlException {
        ObjList<QueryColumn> cols = model.getBottomUpColumns();
        if (cols.size() == 0 || hasBareWildcard(cols)) {
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
        qc.setGenerated(true);
        model.addBottomUpColumn(qc);
        return alias;
    }

    private CharSequence ensureColumnInSelectAtFront(
            IQueryModel model,
            ExpressionNode colExpr,
            CharSequence preferredAlias
    ) {
        ObjList<QueryColumn> cols = model.getBottomUpColumns();
        if (cols.size() == 0 || hasBareWildcard(cols)) {
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

    // Inserts a synthetic key column by identity: existing columns match only
    // when they are themselves generated and their AST is exactly equal. There
    // is deliberately no basename fallback against user columns, so no user
    // column of any spelling can capture the key. The returned alias is the
    // one actually registered (post-dedupe) and must be chained by callers.
    private CharSequence ensureGeneratedKeyColumn(
            IQueryModel model,
            ExpressionNode colExpr,
            CharSequence preferredAlias
    ) throws SqlException {
        ObjList<QueryColumn> cols = model.getBottomUpColumns();
        if (cols.size() == 0 || hasBareWildcard(cols)) {
            return preferredAlias;
        }
        for (int i = 0, n = cols.size(); i < n; i++) {
            QueryColumn existing = cols.getQuick(i);
            if (existing.isGenerated() && ExpressionNode.compareNodesExact(existing.getAst(), colExpr)) {
                return existing.getAlias();
            }
        }
        CharSequence alias = createColumnAlias(preferredAlias, model);
        QueryColumn qc = queryColumnPool.next().of(alias, colExpr);
        qc.setGenerated(true);
        model.addBottomUpColumn(qc);
        return alias;
    }

    private void extractCorrelatedFromInnerJoins(
            IQueryModel inner,
            ObjList<ExpressionNode> correlated,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth
    ) {
        for (int i = 1, n = inner.getJoinModels().size(); i < n; i++) {
            IQueryModel jm = inner.getJoinModels().getQuick(i);
            ExpressionNode joinCriteria = jm.getJoinCriteria();
            if (joinCriteria != null && hasCorrelatedExprAtDepth(joinCriteria, depth)) {
                int jt = jm.getJoinType();
                if (jt == IQueryModel.JOIN_INNER || jt == IQueryModel.JOIN_CROSS) {
                    // INNER/CROSS: split ON into correlated/non-correlated, then:
                    //  - pure outer predicates (allLiteralsAreCorrelated) → extract
                    //    to __qdb_outer_ref__0 criteria for early filtering
                    //  - mixed predicates (inner + outer cols) → rewrite in place
                    // Example ON: a.order_id = o.id AND o.status = 'ACTIVE'
                    //  → mixed: a.order_id = __qdb_outer_ref__0_id (stays in ON)
                    //  → pure:  o.status = 'ACTIVE' (→ __qdb_outer_ref__0 criteria)
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
                } else {
                    jm.setJoinCriteria(rewriteOuterRefs(joinCriteria, outerToInnerAlias, depth));
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

    private boolean hasAggregateFunctions(IQueryModel model) {
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
        ExpressionNode node = where;
        while (node != null) {
            if (SqlKeywords.isAndKeyword(node.token)) {
                if (hasNonEqualityCorrelation(node.rhs, depth)) {
                    return true;
                }
                node = node.lhs;
            } else {
                return hasCorrelatedExprAtDepth(node, depth) && !Chars.equals(node.token, "=");
            }
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

    private boolean hasScalarCountBody(IQueryModel model) throws SqlException {
        IQueryModel current = model;
        while (current != null) {
            // The compensation can only ever produce one row, so a set operation is
            // admissible only while every other branch is empty for the zero row.
            // Then SQL's answer for an unmatched outer row is exactly this branch's
            // aggregate row, which is what the coalesce reproduces.
            if (current.getUnionModel() != null && !unionBranchesRejectZeroRow(current)) {
                return false;
            }
            ExpressionNode limitHi = current.getLimitHi();
            ExpressionNode limitLo = current.getLimitLo();
            // report a provable negative with the specific message, ahead of the
            // generic unprovable one - compensateLimit would reject it regardless
            rejectNegativeLateralLimit(limitLo);
            rejectNegativeLateralLimit(limitHi);
            switch (classifyLateralLimit(limitLo, limitHi)) {
                case ROW_DROPPED:
                    // body provably yields no rows: NULL fill is correct, no coalesce
                    return false;
                default:
                    // ROW_UNPROVABLE is decided at execution time by the guard
                    break;
            }
            if (current.getSampleBy() != null || current.getLatestBy().size() > 0) {
                return false;
            }
            // A GROUP BY on the aggregate's own layer partitions the counted input,
            // so the empty group really is gone and NULL is right. A GROUP BY above
            // the aggregate instead groups the single count row by a value that row
            // determines: one group in, one group out, so the row survives.
            if (current.getGroupBy().size() > 0
                    && (hasAggregateFunctions(current)
                    || classifyGroupByOnZeroCountRow(current) != ROW_KEPT)) {
                return false;
            }
            if (hasAggregateFunctions(current)) {
                // a run-time valued LIMIT no longer disqualifies the body: the
                // compensation is guarded by accumulateScalarCountGuard instead
                IQueryModel input = current.getNestedModel();
                while (input != null) {
                    if (input.isNestedModelIsSubQuery()) {
                        break;
                    }
                    if (input.getUnionModel() != null
                            || input.getGroupBy().size() > 0
                            || input.getSampleBy() != null
                            || input.getLatestBy().size() > 0) {
                        return false;
                    }
                    input = input.getNestedModel();
                }
                ObjList<QueryColumn> columns = current.getBottomUpColumns();
                for (int i = 0, n = columns.size(); i < n; i++) {
                    if (isCountAggregate(columns.getQuick(i).getAst())) {
                        return true;
                    }
                }
                return false;
            }
            // A join above the aggregate keeps the count row exactly once only when
            // every other operand yields exactly one row. An empty operand removes
            // the row, and an N-row operand repeats it N times, while the coalesce
            // above can only ever produce a single compensated row - the general
            // path has no way to reproduce either. The per-join-model path handles
            // arbitrary cardinality and is reached when the count body is not the
            // first operand; this arm only has to be sound, not complete.
            if (current.getJoinModels().size() > 1 && !joinKeepsSingleRow(current)) {
                return false;
            }
            // A filter above the aggregate must accept every count, not merely the
            // zero one. coalesce(cnt, 0) cannot tell a group the filter rejected
            // from a group that never existed - both reach the outer projection as
            // NULL - so compensating under a filter that rejects any count would
            // turn a legitimate NULL into 0. Presence of a WHERE still proves
            // nothing: cnt >= 0 accepts everything, cnt < 100 does not.
            if (!filterAcceptsEveryCount(current.getWhereClause(), current)
                    || !filterAcceptsEveryCount(current.getPostJoinWhereClause(), current)) {
                return false;
            }
            current = current.getNestedModel();
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

    private boolean hasWindowColumns(IQueryModel model) {
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

    private boolean isComplexChain(
            IQueryModel branchTop,
            IQueryModel dataSourceLayer,
            CharSequence outerRefAlias,
            LowerCaseCharSequenceObjHashMap<CharSequence> aliasMap
    ) {
        IQueryModel m = branchTop;
        IQueryModel parent = null;
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
                return true;
            }

            if (m.getOrderBy().size() > 0 && parent != null) {
                for (int i = 0, n = m.getOrderBy().size(); i < n; i++) {
                    QueryColumn column = parent.getAliasToColumnMap().get(m.getOrderBy().getQuick(i).token);
                    if (column != null && hasUnmappedOuterRefLiteral(column.getAst(), outerRefAlias, aliasMap)) {
                        return true;
                    }
                }
            }

            if (m == dataSourceLayer) {
                for (int ji = 1, jn = m.getJoinModels().size(); ji < jn; ji++) {
                    IQueryModel bjm = m.getJoinModels().getQuick(ji);
                    if (bjm.getAlias() != null
                            && Chars.equalsIgnoreCase(bjm.getAlias().token, outerRefAlias)) {
                        continue;
                    }
                    ExpressionNode jmCrit = bjm.getJoinCriteria();
                    if (jmCrit != null && hasUnmappedOuterRefLiteral(jmCrit, outerRefAlias, aliasMap)) {
                        return true;
                    }
                }
                break;
            }
            parent = m;
            m = m.getNestedModel();
        }
        return false;
    }

    private boolean isLocalSelectAlias(CharSequence columnName, IQueryModel jm) {
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

    // Classifies what a LIMIT does to a body that produces exactly one row
    // (a bare scalar count). The single row sits at position 1, and QuestDB's
    // LIMIT lo,hi selects the half-open row range (lo, hi].
    private static int classifyLateralLimit(ExpressionNode limitLo, ExpressionNode limitHi) {
        if (limitLo == null && limitHi == null) {
            return ROW_KEPT;
        }
        final long lo = constLimitValue(limitLo);
        final long hi = constLimitValue(limitHi);
        if (lo == LIMIT_NOT_CONSTANT || hi == LIMIT_NOT_CONSTANT) {
            return ROW_UNPROVABLE;
        }
        if (limitHi == null) {
            // negative limit keeps the last |lo| rows, which for a one-row body is the row
            return lo == 0 ? ROW_DROPPED : ROW_KEPT;
        }
        if (limitLo == null) {
            return hi == 0 ? ROW_DROPPED : ROW_KEPT;
        }
        if (lo < 0 || hi < 0) {
            // negative two-sided windows are rejected by compensateLimit
            return ROW_UNPROVABLE;
        }
        return (lo <= 0 && hi >= 1) ? ROW_KEPT : ROW_DROPPED;
    }

    // QuestDB parses `LIMIT -N` as unary minus applied to a CONSTANT, never as a
    // CONSTANT carrying a signed token, so the sign must be folded explicitly.
    // Same node shape SqlOptimiser relies on for its negative-limit rewrite.
    private static long constLimitValue(ExpressionNode limit) {
        if (limit == null) {
            return 0;
        }
        if (limit.type == ExpressionNode.OPERATION
                && limit.paramCount == 1
                && limit.lhs == null
                && limit.rhs != null
                && Chars.equals(limit.token, '-')) {
            final long v = constLimitValue(limit.rhs);
            return v == LIMIT_NOT_CONSTANT ? LIMIT_NOT_CONSTANT : -v;
        }
        if (limit.type != ExpressionNode.CONSTANT) {
            return LIMIT_NOT_CONSTANT;
        }
        try {
            // a bare CONSTANT token is unsigned, so this can never return the sentinel
            return Numbers.parseLong(limit.token);
        } catch (NumericException ignored) {
            return LIMIT_NOT_CONSTANT;
        }
    }

    // Classifies what a GROUP BY sitting above the aggregate does to the single
    // count row. Grouping by a value that row functionally determines yields
    // exactly one group, so the row survives; grouping by anything this method
    // cannot tie back to the count stays unprovable.
    // Every branch after this one must yield nothing when the count is zero. Only
    // UNION ALL qualifies: the deduplicating set operations decide row identity
    // across branches, which this analysis says nothing about.
    private boolean unionBranchesRejectZeroRow(IQueryModel model) {
        IQueryModel branch = model.getUnionModel();
        int guard = 0;
        while (branch != null && guard++ < 16) {
            if (branch.getSetOperationType() != IQueryModel.SET_OPERATION_UNION_ALL) {
                return false;
            }
            if (!branchRejectsZeroCountRow(branch)) {
                return false;
            }
            branch = branch.getUnionModel();
        }
        return true;
    }

    // True when some filter in the branch provably excludes a count of zero, so
    // the branch contributes no row for an unmatched outer row.
    private boolean branchRejectsZeroCountRow(IQueryModel branch) {
        IQueryModel m = branch;
        int guard = 0;
        while (m != null && guard++ < 16) {
            final ExpressionNode where = m.getWhereClause();
            if (where != null
                    && truthIntervalOf(where, m, 0, false)
                    && (truthLo > 0 || truthHi < 0)) {
                return true;
            }
            m = m.getNestedModel();
        }
        return false;
    }

    private boolean joinKeepsSingleRow(IQueryModel layer) {
        final ObjList<IQueryModel> joinModels = layer.getJoinModels();
        for (int i = 1, n = joinModels.size(); i < n; i++) {
            final IQueryModel jm = joinModels.getQuick(i);
            final int joinType = jm.getJoinType();
            if (joinType != IQueryModel.JOIN_CROSS && joinType != IQueryModel.JOIN_INNER) {
                return false;
            }
            if (jm.getJoinCriteria() != null) {
                // a predicate can reject the pairing, so the row is no longer certain
                return false;
            }
            if (!isProvablyOneRowRelation(jm)) {
                return false;
            }
        }
        return true;
    }

    // True only for a constant projection with no source, which yields exactly one
    // row by construction. Anything reading a table could be empty or hold many
    // rows, and neither is knowable here.
    private boolean isProvablyOneRowRelation(IQueryModel jm) {
        IQueryModel m = jm;
        int depth = 0;
        while (m != null && depth++ < 16) {
            // a projection cannot change the row count, but any of these can
            if (m.getWhereClause() != null
                    || m.getPostJoinWhereClause() != null
                    || m.getLimitLo() != null
                    || m.getLimitHi() != null
                    || m.getJoinModels().size() > 1
                    || m.getGroupBy().size() > 0
                    || m.getSampleBy() != null
                    || m.getLatestBy().size() > 0
                    || m.getUnionModel() != null
                    || m.isDistinct()) {
                return false;
            }
            final ExpressionNode tableNameExpr = m.getTableNameExpr();
            if (tableNameExpr != null) {
                return isSingleRowGenerator(tableNameExpr);
            }
            final IQueryModel nested = m.getNestedModel();
            if (nested == null) {
                return false;
            }
            m = nested;
        }
        return false;
    }

    // A constant SELECT with no FROM is parsed as long_sequence(1), the only
    // source whose cardinality is known here. A table could be empty or hold any
    // number of rows, and long_sequence(n) for any other n is not one row.
    private static boolean isSingleRowGenerator(ExpressionNode tableNameExpr) {
        return tableNameExpr.type == ExpressionNode.FUNCTION
                && Chars.equalsIgnoreCase(tableNameExpr.token, "long_sequence")
                && tableNameExpr.paramCount == 1
                && tableNameExpr.rhs != null
                && tableNameExpr.rhs.type == ExpressionNode.CONSTANT
                && constLimitValue(tableNameExpr.rhs) == 1;
    }

    private int classifyGroupByOnZeroCountRow(IQueryModel layer) {
        final ObjList<ExpressionNode> groupBy = layer.getGroupBy();
        for (int i = 0, n = groupBy.size(); i < n; i++) {
            final ExpressionNode g = groupBy.getQuick(i);
            if (g == null
                    || g.type != ExpressionNode.LITERAL
                    || !resolvesToScalarCount(g.token, layer, 0)) {
                return ROW_UNPROVABLE;
            }
        }
        return ROW_KEPT;
    }

    // True when a filter above the aggregate provably accepts the count column's
    // whole domain, so it can never remove a row and the compensation below it
    // stays sound. Anything this method cannot describe answers false and the
    // caller keeps suppressing, which is always safe: suppressing only ever
    // leaves a NULL where a 0 was wanted, never a 0 where a NULL was wanted.
    private boolean filterAcceptsEveryCount(ExpressionNode filter, IQueryModel layer) {
        if (filter == null) {
            return true;
        }
        if (!truthIntervalOf(filter, layer, 0, false)) {
            return false;
        }
        // count() is non-negative, so covering [0, Long.MAX_VALUE] covers the domain
        return truthLo <= 0 && truthHi == Long.MAX_VALUE;
    }

    // Under-approximates the set of count values for which a predicate holds, as
    // a single interval left in truthLo/truthHi. Returns false when the truth set
    // cannot be described that way. count() never produces NULL, so two-valued
    // logic is sound here; any operand that is not the count or an integer
    // constant ends the analysis rather than being guessed at. `negated` carries
    // an enclosing NOT down by De Morgan instead of complementing intervals.
    private boolean truthIntervalOf(ExpressionNode pred, IQueryModel layer, int depth, boolean negated) {
        if (pred == null || depth > 24) {
            return false;
        }
        final CharSequence token = pred.token;
        final boolean isAnd = SqlKeywords.isAndKeyword(token);
        if (isAnd || SqlKeywords.isOrKeyword(token)) {
            if (!truthIntervalOf(pred.lhs, layer, depth + 1, negated)) {
                return false;
            }
            final long aLo = truthLo;
            final long aHi = truthHi;
            if (!truthIntervalOf(pred.rhs, layer, depth + 1, negated)) {
                return false;
            }
            // negation swaps the connective, so AND under a NOT unions instead
            if (isAnd != negated) {
                truthLo = Math.max(aLo, truthLo);
                truthHi = Math.min(aHi, truthHi);
                return true;
            }
            return unionInterval(aLo, aHi, truthLo, truthHi);
        }
        if (SqlKeywords.isNotKeyword(token)) {
            return truthIntervalOf(pred.rhs != null ? pred.rhs : pred.lhs, layer, depth + 1, !negated);
        }
        if (pred.type == ExpressionNode.CONSTANT) {
            if (SqlKeywords.isTrueKeyword(token)) {
                return constantTruth(!negated);
            }
            return SqlKeywords.isFalseKeyword(token) && constantTruth(negated);
        }

        int op = comparisonOp(token);
        if (op == CMP_NONE) {
            return false;
        }
        final boolean lhsIsCount = isScalarCountRef(pred.lhs, layer);
        final boolean rhsIsCount = isScalarCountRef(pred.rhs, layer);
        final long lhsConst = constOperand(pred.lhs);
        final long rhsConst = constOperand(pred.rhs);
        if (negated) {
            op = negateComparison(op);
        }
        if (lhsIsCount == rhsIsCount) {
            // both sides constant folds to a verdict; anything else is undecidable
            if (lhsIsCount || lhsConst == LIMIT_NOT_CONSTANT || rhsConst == LIMIT_NOT_CONSTANT) {
                return false;
            }
            return constantTruth(compare(op, lhsConst, rhsConst));
        }
        final long k = lhsIsCount ? rhsConst : lhsConst;
        if (k == LIMIT_NOT_CONSTANT) {
            return false;
        }
        if (rhsIsCount) {
            // normalise to `count OP constant`
            op = flipComparison(op);
        }
        return countIntervalFor(op, k);
    }

    private boolean countIntervalFor(int op, long k) {
        switch (op) {
            case CMP_EQ:
                truthLo = k;
                truthHi = k;
                return true;
            case CMP_NE:
                // the complement of a point is two intervals; only a negative k,
                // which no count can equal, collapses back to the whole domain
                if (k >= 0) {
                    return false;
                }
                truthLo = Long.MIN_VALUE;
                truthHi = Long.MAX_VALUE;
                return true;
            case CMP_LT:
                if (k == Long.MIN_VALUE) {
                    return constantTruth(false);
                }
                truthLo = Long.MIN_VALUE;
                truthHi = k - 1;
                return true;
            case CMP_LE:
                truthLo = Long.MIN_VALUE;
                truthHi = k;
                return true;
            case CMP_GT:
                if (k == Long.MAX_VALUE) {
                    return constantTruth(false);
                }
                truthLo = k + 1;
                truthHi = Long.MAX_VALUE;
                return true;
            default:
                truthLo = k;
                truthHi = Long.MAX_VALUE;
                return true;
        }
    }

    private boolean constantTruth(boolean holds) {
        if (holds) {
            truthLo = Long.MIN_VALUE;
            truthHi = Long.MAX_VALUE;
        } else {
            // empty interval
            truthLo = Long.MAX_VALUE;
            truthHi = Long.MIN_VALUE;
        }
        return true;
    }

    // Two intervals only union back into one when they overlap or abut; a gap
    // between them cannot be described here and ends the analysis.
    private boolean unionInterval(long aLo, long aHi, long bLo, long bHi) {
        if (aLo > aHi) {
            truthLo = bLo;
            truthHi = bHi;
            return true;
        }
        if (bLo > bHi) {
            truthLo = aLo;
            truthHi = aHi;
            return true;
        }
        if (aLo > bLo) {
            // order so the lower-starting interval is first
            final long tLo = aLo;
            final long tHi = aHi;
            aLo = bLo;
            aHi = bHi;
            bLo = tLo;
            bHi = tHi;
        }
        if (aHi != Long.MAX_VALUE && aHi + 1 < bLo) {
            return false;
        }
        truthLo = aLo;
        truthHi = aHi >= bHi ? aHi : bHi;
        return true;
    }

    private boolean isScalarCountRef(ExpressionNode node, IQueryModel layer) {
        return node != null
                && node.type == ExpressionNode.LITERAL
                && resolvesToScalarCount(node.token, layer, 0);
    }

    private static long constOperand(ExpressionNode node) {
        if (node == null) {
            return LIMIT_NOT_CONSTANT;
        }
        if (node.type != ExpressionNode.CONSTANT && !isUnaryMinusConstant(node)) {
            return LIMIT_NOT_CONSTANT;
        }
        return constLimitValue(node);
    }

    private static boolean compare(int op, long l, long r) {
        switch (op) {
            case CMP_EQ:
                return l == r;
            case CMP_NE:
                return l != r;
            case CMP_LT:
                return l < r;
            case CMP_LE:
                return l <= r;
            case CMP_GT:
                return l > r;
            default:
                return l >= r;
        }
    }

    private static int comparisonOp(CharSequence token) {
        if (Chars.equals(token, "=")) {
            return CMP_EQ;
        }
        if (Chars.equals(token, "!=") || Chars.equals(token, "<>")) {
            return CMP_NE;
        }
        if (Chars.equals(token, "<")) {
            return CMP_LT;
        }
        if (Chars.equals(token, "<=")) {
            return CMP_LE;
        }
        if (Chars.equals(token, ">")) {
            return CMP_GT;
        }
        return Chars.equals(token, ">=") ? CMP_GE : CMP_NONE;
    }

    // operand swap: `k < count` becomes `count > k`
    private static int flipComparison(int op) {
        switch (op) {
            case CMP_LT:
                return CMP_GT;
            case CMP_LE:
                return CMP_GE;
            case CMP_GT:
                return CMP_LT;
            case CMP_GE:
                return CMP_LE;
            default:
                return op;
        }
    }

    private static int negateComparison(int op) {
        switch (op) {
            case CMP_EQ:
                return CMP_NE;
            case CMP_NE:
                return CMP_EQ;
            case CMP_LT:
                return CMP_GE;
            case CMP_LE:
                return CMP_GT;
            case CMP_GT:
                return CMP_LE;
            default:
                return CMP_LT;
        }
    }

    private static boolean isUnaryMinusConstant(ExpressionNode node) {
        return node.type == ExpressionNode.OPERATION
                && node.paramCount == 1
                && node.lhs == null
                && node.rhs != null
                && Chars.equals(node.token, '-');
    }

    // Resolves a column referenced by a layer above the aggregate to the scalar
    // count below it, following pure renames down the projection chain. Anything
    // that is not a straight rename of the count breaks the chain and the caller
    // treats the reference as unknown.
    private boolean resolvesToScalarCount(CharSequence token, IQueryModel layer, int depth) {
        if (token == null || layer == null || depth > 16) {
            return false;
        }
        final IQueryModel below = layer.getNestedModel();
        if (below == null) {
            return false;
        }
        CharSequence name = token;
        final int dot = Chars.indexOfLastUnquoted(name, '.');
        if (dot >= 0) {
            name = name.subSequence(dot + 1, name.length());
        }
        final ObjList<QueryColumn> cols = below.getBottomUpColumns();
        if (cols.size() == 0) {
            // a layer that projects nothing of its own passes the name straight down
            return resolvesToScalarCount(token, below, depth + 1);
        }
        for (int i = 0, n = cols.size(); i < n; i++) {
            final QueryColumn col = cols.getQuick(i);
            if (Chars.equalsIgnoreCase(col.getAlias(), name)) {
                final ExpressionNode ast = col.getAst();
                if (isCountAggregate(ast)) {
                    return true;
                }
                if (ast != null && ast.type == ExpressionNode.LITERAL) {
                    return resolvesToScalarCount(ast.token, below, depth + 1);
                }
                return false;
            }
        }
        return false;
    }

    // Builds the row-1 evaluation of the row_number filter for one body layer and
    // ANDs it into the guard for the lateral body currently being rewritten.
    private void accumulateScalarCountGuard(ExpressionNode limitLo, ExpressionNode limitHi, boolean readsColumn) {
        if (limitLo == null && limitHi == null) {
            return;
        }
        if (scalarCountGuardDisarmed) {
            // below an aggregation layer: this LIMIT caps the aggregate's input,
            // not the aggregate row itself - the coalesce compensation stays valid
            // with no guard (and an outer-column LIMIT here needs no blocker)
            return;
        }
        if (classifyLateralLimit(limitLo, limitHi) != ROW_UNPROVABLE) {
            // compile-time decidable: hasScalarCountBody already folded it, no guard needed
            return;
        }
        if (readsColumn) {
            // the guard lives in the outer projection, which carries only the query's
            // own columns; a LIMIT reading any column cannot be evaluated there
            if (scalarCountGuardBlocker == null) {
                scalarCountGuardBlocker = limitLo != null ? limitLo : limitHi;
            }
            return;
        }
        ExpressionNode layerGuard;
        if (limitHi != null && limitLo != null) {
            layerGuard = createBinaryOp("and",
                    createBinaryOp(">", rowOneConstant(), ExpressionNode.deepClone(expressionNodePool, limitLo)),
                    createBinaryOp("<=", rowOneConstant(), ExpressionNode.deepClone(expressionNodePool, limitHi)));
        } else {
            layerGuard = createBinaryOp("<=", rowOneConstant(),
                    ExpressionNode.deepClone(expressionNodePool, limitHi != null ? limitHi : limitLo));
        }
        scalarCountGuard = scalarCountGuard == null
                ? layerGuard
                : createBinaryOp("and", scalarCountGuard, layerGuard);
    }

    private void rejectCorrelatedScalarCountLimit() throws SqlException {
        if (scalarCountGuardBlocker != null) {
            throw SqlException.position(scalarCountGuardBlocker.position)
                    .put("LIMIT referencing an outer column is not supported over a scalar count ")
                    .put("in a correlated lateral sub-query; use a constant or bind variable");
        }
    }

    // True when the expression reads a column. Constants, bind variables and
    // functions/operators over them evaluate identically in the outer projection;
    // anything naming a column does not, so such a LIMIT cannot be guarded there.
    private boolean hasColumnRef(ExpressionNode node) {
        if (node == null) {
            return false;
        }
        sqlNodeStack.clear();
        sqlNodeStack.push(node);
        while (!sqlNodeStack.isEmpty()) {
            ExpressionNode n = sqlNodeStack.pop();
            if (n.type == ExpressionNode.LITERAL) {
                return true;
            }
            if (n.lhs != null) {
                sqlNodeStack.push(n.lhs);
            }
            if (n.rhs != null) {
                sqlNodeStack.push(n.rhs);
            }
            for (int i = 0, m = n.args.size(); i < m; i++) {
                sqlNodeStack.push(n.args.getQuick(i));
            }
        }
        return false;
    }

    private ExpressionNode rowOneConstant() {
        return expressionNodePool.next().of(ExpressionNode.CONSTANT, "1", 0, 0);
    }

    private static void rejectNegativeLateralLimit(ExpressionNode limit) throws SqlException {
        final long v = constLimitValue(limit);
        if (v != LIMIT_NOT_CONSTANT && v < 0) {
            throw SqlException.position(limit.position)
                    .put("negative LIMIT is not supported in a correlated lateral sub-query");
        }
    }

    private ExpressionNode liftExpression(
            ExpressionNode node,
            CharSequence outerRefAlias,
            ObjList<QueryColumn> outerRefCols,
            CharSequence lateralAlias,
            IQueryModel branchTop,
            IQueryModel dataSourceLayer
    ) throws SqlException {
        if (node == null) {
            return null;
        }

        ExpressionNode rootResult = liftLeaf(node, outerRefAlias, outerRefCols, lateralAlias, branchTop, dataSourceLayer);
        if (rootResult != null) {
            return rootResult;
        }

        ExpressionNode rootClone = expressionNodePool.next().of(
                node.type, node.token, node.precedence, node.position
        );
        rootClone.paramCount = node.paramCount;
        sqlNodeStack.clear();
        sqlNodeStack2.clear();
        sqlNodeStack.push(node);
        sqlNodeStack2.push(rootClone);

        while (!sqlNodeStack.isEmpty()) {
            ExpressionNode orig = sqlNodeStack.pop();
            ExpressionNode clone = sqlNodeStack2.pop();
            if (orig.lhs != null) {
                ExpressionNode leafResult = liftLeaf(orig.lhs, outerRefAlias, outerRefCols, lateralAlias, branchTop, dataSourceLayer);
                if (leafResult != null) {
                    clone.lhs = leafResult;
                } else {
                    clone.lhs = expressionNodePool.next().of(
                            orig.lhs.type, orig.lhs.token, orig.lhs.precedence, orig.lhs.position
                    );
                    clone.lhs.paramCount = orig.lhs.paramCount;
                    sqlNodeStack.push(orig.lhs);
                    sqlNodeStack2.push(clone.lhs);
                }
            }
            if (orig.rhs != null) {
                ExpressionNode leafResult = liftLeaf(orig.rhs, outerRefAlias, outerRefCols, lateralAlias, branchTop, dataSourceLayer);
                if (leafResult != null) {
                    clone.rhs = leafResult;
                } else {
                    clone.rhs = expressionNodePool.next().of(
                            orig.rhs.type, orig.rhs.token, orig.rhs.precedence, orig.rhs.position
                    );
                    clone.rhs.paramCount = orig.rhs.paramCount;
                    sqlNodeStack.push(orig.rhs);
                    sqlNodeStack2.push(clone.rhs);
                }
            }
            for (int i = 0, n = orig.args.size(); i < n; i++) {
                ExpressionNode argOrig = orig.args.getQuick(i);
                ExpressionNode leafResult = liftLeaf(argOrig, outerRefAlias, outerRefCols, lateralAlias, branchTop, dataSourceLayer);
                if (leafResult != null) {
                    clone.args.add(leafResult);
                } else {
                    ExpressionNode argClone = expressionNodePool.next().of(
                            argOrig.type, argOrig.token, argOrig.precedence, argOrig.position
                    );
                    argClone.paramCount = argOrig.paramCount;
                    clone.args.add(argClone);
                    sqlNodeStack.push(argOrig);
                    sqlNodeStack2.push(argClone);
                }
            }
        }
        return rootClone;
    }

    // Returns transformed node if node is a LITERAL (leaf), null if non-leaf.
    private ExpressionNode liftLeaf(
            ExpressionNode node,
            CharSequence outerRefAlias,
            ObjList<QueryColumn> outerRefCols,
            CharSequence lateralAlias,
            IQueryModel branchTop,
            IQueryModel dataSourceLayer
    ) throws SqlException {
        if (node.type != ExpressionNode.LITERAL) {
            return null;
        }
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

    private void liftOuterRefExpressions(
            IQueryModel branchTop,
            IQueryModel selectModel,
            IQueryModel joinLayer,
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

            if (ast.type == ExpressionNode.LITERAL && col.isGenerated()) {
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

    private boolean markScalarCountColumns(IQueryModel model) {
        if (model == null) {
            return false;
        }
        if (hasAggregateFunctions(model)) {
            boolean hasMarkedColumn = false;
            ObjList<QueryColumn> columns = model.getBottomUpColumns();
            for (int i = 0, n = columns.size(); i < n; i++) {
                QueryColumn column = columns.getQuick(i);
                if (isCountAggregate(column.getAst())) {
                    column.setLateralScalarCount(true);
                    hasMarkedColumn = true;
                }
            }
            return hasMarkedColumn;
        }
        if (!markScalarCountColumns(model.getNestedModel())) {
            return false;
        }
        // Carry the mark up through this layer. Downstream the mark is copied by
        // hand wherever a column is derived from another, and the GROUP BY rewrite
        // builds its output column through a path that does not copy it, so the
        // mark would otherwise die between the aggregate and the body's top
        // projection. A column that is a plain reference to the scalar count below
        // IS that same scalar count, which resolvesToScalarCount proves, so this
        // cannot widen the compensation to an unrelated column.
        ObjList<QueryColumn> columns = model.getBottomUpColumns();
        for (int i = 0, n = columns.size(); i < n; i++) {
            QueryColumn column = columns.getQuick(i);
            ExpressionNode ast = column.getAst();
            if (ast != null
                    && ast.type == ExpressionNode.LITERAL
                    && resolvesToScalarCount(ast.token, model, 0)) {
                column.setLateralScalarCount(true);
            }
        }
        return true;
    }

    private CharSequence propagateColumnUp(
            CharSequence columnName,
            IQueryModel current,
            IQueryModel dataSourceLayer
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
            IQueryModel parent,
            IQueryModel current,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            boolean isLeftJoin,
            IQueryModel outerRefJoinModel,
            IQueryModel lateralJoinModel,
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

        byte terminate;
        IQueryModel nestModel = current.getNestedModel();

        if (!current.isCorrelatedAtDepth(depth)) {
            terminate = TERMINATE_SKIP;
        } else if (nestModel == null) {
            terminate = TERMINATE_HERE;
        } else if (nestModel.isCorrelatedAtDepth(depth)) {
            terminate = TERMINATE_DESCEND;
        } else {
            terminate = TERMINATE_HERE;
            if (current.isOwnCorrelatedAtDepth(depth, CORRELATED_PROJECTION)) {
                terminate = TERMINATE_AT_NESTED;
            }
        }

        if (terminate == TERMINATE_SKIP) {
            terminateHere(current, outerRefJoinModel, outerToInnerAlias, depth);
            return;
        }

        // 1. LIMIT compensation
        if (current.getLimitHi() != null || current.getLimitLo() != null) {
            IQueryModel wrapper = compensateLimit(current, outerToInnerAlias, depth);
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
        if (current.getSampleBy() != null || hasGroupBy || hasAggregates) {
            // deeper layers feed this aggregation: their LIMITs cap its input and
            // cannot drop the aggregate row, so they stay out of the guard
            scalarCountGuardDisarmed = true;
        }
        if (hasWindowColumns(current)) {
            compensateWindow(current, outerRefJoinModel.getAlias().token);
        }
        if (current.isDistinct()) {
            compensateDistinct(current);
        }
        if (current.getLatestBy().size() > 0) {
            IQueryModel latestByWrapper = compensateLatestBy(current, parent, outerToInnerAlias, depth);
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
                && current.isOwnCorrelatedAtDepth(depth, CORRELATED_SAMPLE_BY)) {
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
                && current.isOwnCorrelatedAtDepth(depth, CORRELATED_WHERE)) {
            current.setWhereClause(
                    rewriteOuterRefs(current.getWhereClause(), outerToInnerAlias, depth));
        }

        // 5. Descend or terminate: handle main chain and join branches
        boolean hasJoins = current.getJoinModels().size() > 1;
        if (!hasJoins) {
            switch (terminate) {
                case TERMINATE_HERE -> terminateHere(current, outerRefJoinModel, outerToInnerAlias, depth);
                case TERMINATE_AT_NESTED -> terminateHere(nestModel, outerRefJoinModel, outerToInnerAlias, depth);
                case TERMINATE_DESCEND -> pushDownOuterRefs(
                        current, nestModel, outerToInnerAlias, isLeftJoin,
                        outerRefJoinModel, lateralJoinModel, depth
                );
            }

            if (current.getUnionModel() != null) {
                compensateSetOp(current, outerToInnerAlias, depth, outerRefJoinModel);
            }
        } else {
            for (int ji = 0, jn = current.getJoinModels().size(); ji < jn; ji++) {
                IQueryModel jm = current.getJoinModels().getQuick(ji);
                IQueryModel jmNested = ji == 0 ? current.getNestedModel() : jm.getNestedModel();
                if (ji == 0) {
                    switch (terminate) {
                        case TERMINATE_HERE -> {
                            terminateHere(current, outerRefJoinModel, outerToInnerAlias, depth);
                            jn = current.getJoinModels().size();
                            ji = 1;
                        }
                        case TERMINATE_AT_NESTED ->
                                terminateHere(nestModel, outerRefJoinModel, outerToInnerAlias, depth);
                        case TERMINATE_DESCEND -> pushDownOuterRefs(
                                current, nestModel, outerToInnerAlias, isLeftJoin,
                                outerRefJoinModel, lateralJoinModel, depth
                        );
                    }

                    if (current.getUnionModel() != null) {
                        compensateSetOp(current, outerToInnerAlias, depth, outerRefJoinModel);
                    }
                } else {
                    pushDownOuterRefsForJoinBranch(
                            current, jm, jmNested, outerToInnerAlias, isLeftJoin,
                            lateralJoinModel.getNestedModel(), outerRefJoinModel, depth
                    );
                }
            }
        }
    }

    // Handles a single join branch (ji > 0) inside the lateral subquery.
    //
    // 1. Correlated ON is always rewritten in place using main chain aliases.
    //    Main delim is at the join level, visible from ON.
    //
    // 2. Table-model branches (jmNested == null): return after ON rewrite.
    //
    // 3. Subquery branches with non-correlated nested model: return after ON
    //    rewrite. No clone needed — no nested compensation required.
    //
    // 4. Subquery branches with correlated nested model: create cloned
    //    __qdb_outer_ref__, push inside for compensation, build alignment
    //    criteria (clone_col = main_col) appended to branch ON.
    //
    // hasCorrelatedCriteria is false when extractCorrelatedFromInnerJoins
    // already handled ON (terminate=1); true at levels above terminateHere.
    private void pushDownOuterRefsForJoinBranch(
            IQueryModel current,
            IQueryModel jm,
            IQueryModel jmNested,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            boolean isLeftJoin,
            IQueryModel localCountModel,
            IQueryModel outerRefJoinModel,
            int depth
    ) throws SqlException {
        boolean hasCorrelatedCriteria = jm.getJoinCriteria() != null
                && hasCorrelatedExprAtDepth(jm.getJoinCriteria(), depth);
        if (hasCorrelatedCriteria) {
            jm.setJoinCriteria(rewriteOuterRefs(jm.getJoinCriteria(), outerToInnerAlias, depth));
        }

        if (jmNested == null || !jmNested.isCorrelatedAtDepth(depth)) {
            return;
        }

        IQueryModel clonedOuterRef = cloneOuterRef(outerRefJoinModel);
        CharSequence cloneAlias = clonedOuterRef.getAlias().token;
        int aliasSaveBase = saveAndRemapOuterToInnerAlias(cloneAlias);

        boolean isScalarCountBody = isLeftJoin && hasScalarCountBody(jmNested);
        // Branch-local guard scope: consumed below onto localCountModel; the caller's
        // guard state must be restored, or this branch's guard would leak onto the
        // outer model (and a leftover blocker would spuriously reject valid queries)
        final ExpressionNode savedScalarCountGuard = scalarCountGuard;
        final ExpressionNode savedScalarCountGuardBlocker = scalarCountGuardBlocker;
        final boolean savedScalarCountGuardDisarmed = scalarCountGuardDisarmed;
        scalarCountGuard = null;
        scalarCountGuardBlocker = null;
        scalarCountGuardDisarmed = false;
        pushDownOuterRefs(
                null, jmNested, outerToInnerAlias, isLeftJoin,
                clonedOuterRef, jm, depth
        );
        if (isScalarCountBody && markScalarCountColumns(jmNested)) {
            rejectCorrelatedScalarCountLimit();
            localCountModel.setLateralCountCoalesceRequired(true);
            localCountModel.setLateralCountCoalesceGuard(scalarCountGuard);
            if (jm.getJoinType() == IQueryModel.JOIN_CROSS || jm.getJoinType() == IQueryModel.JOIN_INNER) {
                jm.setJoinType(IQueryModel.JOIN_LEFT_OUTER);
            }
        }
        scalarCountGuard = savedScalarCountGuard;
        scalarCountGuardBlocker = savedScalarCountGuardBlocker;
        scalarCountGuardDisarmed = savedScalarCountGuardDisarmed;

        ExpressionNode alignCriteria = jm.getJoinCriteria();
        IQueryModel jmTop = jm.getNestedModel();
        CharSequence jmAlias = jm.getAlias() != null ? jm.getAlias().token : null;
        ObjList<CharSequence> oKeys = outerToInnerAlias.keys();
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
        if (jm.getJoinType() == IQueryModel.JOIN_CROSS) {
            jm.setJoinType(IQueryModel.JOIN_INNER);
        }

        restoreOuterToInnerAlias(aliasSaveBase);
    }

    private CharSequence pushDownPerSidePush(
            IQueryModel topInner,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            IQueryModel outerRefJoinModel,
            boolean isLeftJoin,
            int depth
    ) throws SqlException {
        IQueryModel terminateLevel = topInner;
        while (terminateLevel != null) {
            IQueryModel nested = terminateLevel.getNestedModel();
            if (nested == null || !nested.isCorrelatedAtDepth(depth)) {
                break;
            }
            terminateLevel = nested;
        }
        assert terminateLevel != null;

        IQueryModel scalarCountDriver = null;
        ObjList<IQueryModel> terminateJoins = terminateLevel.getJoinModels();
        for (int bi = 1, bn = terminateJoins.size(); bi < bn; bi++) {
            IQueryModel branch = terminateJoins.getQuick(bi);
            IQueryModel branchNested = branch.getNestedModel();
            if (isLeftJoin
                    && branchNested != null
                    && branchNested.isCorrelatedAtDepth(depth)
                    && hasScalarCountBody(branchNested)) {
                scalarCountDriver = cloneOuterRef(outerRefJoinModel, COUNT_DRIVER_PREFIX, terminateLevel);
                terminateJoins.insert(1, 1, null);
                terminateJoins.setQuick(1, scalarCountDriver);
                for (int i = 1, n = terminateJoins.size(); i < n; i++) {
                    registerDataSourceAlias(terminateLevel, terminateJoins.getQuick(i), i);
                }
                break;
            }
        }

        CharSequence firstCloneAlias = scalarCountDriver != null ? scalarCountDriver.getAlias().token : null;
        // Branch-local guards are consumed inside the loop; the caller's guard state
        // must survive it, or the last branch's guard would leak onto the outer model
        final ExpressionNode savedScalarCountGuard = scalarCountGuard;
        final ExpressionNode savedScalarCountGuardBlocker = scalarCountGuardBlocker;
        final boolean savedScalarCountGuardDisarmed = scalarCountGuardDisarmed;
        for (int bi = 1, bn = terminateJoins.size(); bi < bn; bi++) {
            IQueryModel branch = terminateLevel.getJoinModels().getQuick(bi);
            IQueryModel branchNested = branch.getNestedModel();

            boolean hasCorrelatedOn = branch.getJoinCriteria() != null
                    && hasCorrelatedExprAtDepth(branch.getJoinCriteria(), depth);
            boolean isNestedCorrelated = branchNested != null
                    && branchNested.isCorrelatedAtDepth(depth);

            if (!hasCorrelatedOn && !isNestedCorrelated) {
                continue;
            }
            if (branchNested == null) {
                continue;
            }

            IQueryModel clonedOuterRef = cloneOuterRef(outerRefJoinModel);
            CharSequence cloneAlias = clonedOuterRef.getAlias().token;
            if (firstCloneAlias == null) {
                firstCloneAlias = cloneAlias;
            }

            int aliasSaveBase = saveAndRemapOuterToInnerAlias(cloneAlias);

            if (hasCorrelatedOn) {
                branch.setJoinCriteria(rewriteOuterRefs(branch.getJoinCriteria(), outerToInnerAlias, depth));
            }

            boolean isScalarCountBody = isLeftJoin && hasScalarCountBody(branchNested);
            scalarCountGuard = null;
            scalarCountGuardBlocker = null;
            scalarCountGuardDisarmed = false;
            pushDownOuterRefs(
                    null, branchNested, outerToInnerAlias, isLeftJoin,
                    clonedOuterRef, branch, depth
            );
            if (scalarCountDriver != null) {
                ExpressionNode branchCriteria = branch.getJoinCriteria();
                CharSequence branchAlias = branch.getAlias() != null ? branch.getAlias().token : null;
                CharSequence driverAlias = scalarCountDriver.getAlias().token;
                ObjList<CharSequence> outerKeys = outerToInnerAlias.keys();
                for (int i = 0, n = outerKeys.size(); i < n; i++) {
                    CharSequence outerKey = outerKeys.getQuick(i);
                    if (outerKey == null) {
                        continue;
                    }
                    CharSequence branchKey = outerToInnerAlias.get(outerKey);
                    characterStore.newEntry();
                    characterStore.put(driverAlias).put('_').put(unqualify(outerKey));
                    CharSequence driverKey = characterStore.toImmutable();
                    ExpressionNode branchRef = expressionNodePool.next().of(
                            ExpressionNode.LITERAL, qualifyWithAlias(branchAlias, branchKey), 0, 0
                    );
                    ExpressionNode driverRef = expressionNodePool.next().of(
                            ExpressionNode.LITERAL, qualifyWithAlias(driverAlias, driverKey), 0, 0
                    );
                    ExpressionNode equality = createBinaryOp("=", branchRef, driverRef);
                    branchCriteria = branchCriteria == null
                            ? equality
                            : createBinaryOp("and", branchCriteria, equality);
                }
                branch.setJoinCriteria(branchCriteria);
                if (branch.getJoinType() == IQueryModel.JOIN_CROSS) {
                    branch.setJoinType(IQueryModel.JOIN_INNER);
                }
            }
            if (isScalarCountBody
                    && scalarCountDriver != null
                    && markScalarCountColumns(branchNested)) {
                if (branch.getJoinType() == IQueryModel.JOIN_CROSS
                        || branch.getJoinType() == IQueryModel.JOIN_INNER) {
                    branch.setJoinType(IQueryModel.JOIN_LEFT_OUTER);
                }
                rejectCorrelatedScalarCountLimit();
                topInner.setLateralCountCoalesceRequired(true);
                topInner.setLateralCountCoalesceGuard(scalarCountGuard);
            }
            IQueryModel branchTop = branch.getNestedModel();
            ObjList<CharSequence> oKeys = outerToInnerAlias.keys();
            for (int ok = 0, okn = oKeys.size(); ok < okn; ok++) {
                CharSequence key = oKeys.getQuick(ok);
                if (key == null) continue;
                CharSequence cloneColAlias = outerToInnerAlias.get(key);
                ExpressionNode cloneColNode = expressionNodePool.next().of(
                        ExpressionNode.LITERAL, cloneColAlias, 0, 0);
                ensureColumnInSelect(branchTop, cloneColNode, cloneColAlias);
            }

            restoreOuterToInnerAlias(aliasSaveBase);
        }
        scalarCountGuard = savedScalarCountGuard;
        scalarCountGuardBlocker = savedScalarCountGuardBlocker;
        scalarCountGuardDisarmed = savedScalarCountGuardDisarmed;

        if (firstCloneAlias != null) {
            // Identity-tracked propagation: walk the projection layers between
            // topInner and terminateLevel deepest-first and chain the alias each
            // layer actually registers into the layer above. The deepest layer
            // sees the driver key qualified with the collision-probed driver
            // alias, so a same-named user column can never be captured. The
            // ordinary (non-driver) path keeps the bare clone token, preserving
            // existing names when no collision exists.
            wrapperKeyLayers.clear();
            for (IQueryModel layer = topInner; layer != terminateLevel; layer = layer.getNestedModel()) {
                wrapperKeyLayers.add(layer);
            }
            if (wrapperKeyLayers.size() == 0) {
                wrapperKeyLayers.add(topInner);
            }
            int deepest = wrapperKeyLayers.size() - 1;
            ObjList<CharSequence> mapKeys = outerToInnerAlias.keys();
            for (int ki = 0, kn = mapKeys.size(); ki < kn; ki++) {
                CharSequence key = mapKeys.getQuick(ki);
                if (key == null) {
                    continue;
                }
                CharSequence cn = unqualify(key);
                characterStore.newEntry();
                characterStore.put(firstCloneAlias).put("_").put(cn);
                CharSequence cloneColAlias = characterStore.toImmutable();
                CharSequence seedToken = scalarCountDriver != null
                        ? qualifyWithAlias(firstCloneAlias, cloneColAlias)
                        : cloneColAlias;
                CharSequence layerAlias = cloneColAlias;
                for (int li = deepest; li >= 0; li--) {
                    CharSequence token = li == deepest ? seedToken : layerAlias;
                    ExpressionNode colNode = expressionNodePool.next().of(
                            ExpressionNode.LITERAL, token, 0, 0);
                    layerAlias = ensureGeneratedKeyColumn(wrapperKeyLayers.getQuick(li), colNode, layerAlias);
                }
                outerToInnerAlias.put(key, layerAlias);
            }
        }
        return firstCloneAlias;
    }

    private void pushMatchingPredicates(
            IQueryModel outerRefBase,
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
    private void pushOuterWhereToRefBases(IQueryModel outerModel, IQueryModel outerRefSubquery) {
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

    private void rebuildGroupingCols() {
        groupingCols.clear();
        ObjList<CharSequence> keys = outerToInnerAlias.keys();
        for (int ki = 0, kn = keys.size(); ki < kn; ki++) {
            CharSequence key = keys.getQuick(ki);
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

    private ExpressionNode rebuildOnFromOuterRefCriteria(
            ObjList<ExpressionNode> predicates,
            CharSequence outerRefAlias,
            ObjList<QueryColumn> outerRefCols,
            IQueryModel branchTop,
            IQueryModel dataSourceLayer,
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

    // Iterative copy-on-write leaf replacement using post-order two-stack traversal.
    private ExpressionNode replaceColumnRef(
            ExpressionNode node,
            CharSequence qualifiedRef,
            ExpressionNode replacement
    ) {
        if (node == null) {
            return null;
        }

        sqlNodeStack.clear();
        sqlNodeStack2.clear();
        sqlNodeStack.push(node);
        while (!sqlNodeStack.isEmpty()) {
            ExpressionNode current = sqlNodeStack.pop();
            sqlNodeStack2.push(current);
            if (current.type != ExpressionNode.LITERAL && current.type != ExpressionNode.CONSTANT) {
                if (current.lhs != null) {
                    sqlNodeStack.push(current.lhs);
                }
                if (current.rhs != null) {
                    sqlNodeStack.push(current.rhs);
                }
            }
        }

        while (!sqlNodeStack2.isEmpty()) {
            ExpressionNode orig = sqlNodeStack2.pop();
            if (orig.type == ExpressionNode.LITERAL) {
                sqlNodeStack.push(Chars.equalsIgnoreCase(orig.token, qualifiedRef)
                        ? ExpressionNode.deepClone(expressionNodePool, replacement)
                        : orig);
            } else if (orig.type == ExpressionNode.CONSTANT) {
                sqlNodeStack.push(orig);
            } else {
                ExpressionNode newRhs = orig.rhs != null ? sqlNodeStack.pop() : null;
                ExpressionNode newLhs = orig.lhs != null ? sqlNodeStack.pop() : null;
                if (newLhs == orig.lhs && newRhs == orig.rhs) {
                    sqlNodeStack.push(orig);
                } else {
                    ExpressionNode clone = expressionNodePool.next().of(
                            orig.type, orig.token, orig.precedence, orig.position
                    );
                    clone.paramCount = orig.paramCount;
                    clone.lhs = newLhs;
                    clone.rhs = newRhs;
                    sqlNodeStack.push(clone);
                }
            }
        }
        return sqlNodeStack.pop();
    }

    private void replaceColumnRefInModel(
            IQueryModel outerModel,
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

    private boolean resolveColumnInChild(CharSequence columnName, IQueryModel model) {
        for (int i = 0, n = model.getJoinModels().size(); i < n; i++) {
            IQueryModel jm = model.getJoinModels().getQuick(i);
            if (jm.getAliasToColumnNameMap().size() > 0 && !isWildcard(jm.getBottomUpColumns())) {
                if (jm.getAliasToColumnNameMap().contains(columnName)) {
                    return true;
                }
            } else {
                IQueryModel nested = jm.getNestedModel();
                if (nested != null && resolveColumnInChild(columnName, nested)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void restoreOuterToInnerAlias(int aliasSaveBase) {
        ObjList<CharSequence> oKeys = outerToInnerAlias.keys();
        int savedIdx = aliasSaveBase;
        for (int ok = 0, okn = oKeys.size(); ok < okn; ok++) {
            CharSequence key = oKeys.getQuick(ok);
            if (key == null) continue;
            outerToInnerAlias.put(key, outerAliasSaveStack.getQuick(savedIdx));
            savedIdx++;
        }
        outerAliasSaveStack.setPos(aliasSaveBase);

        rebuildGroupingCols();
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
            IQueryModel model,
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
            IQueryModel model,
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
            IQueryModel jm = model.getJoinModels().getQuick(j);
            ExpressionNode jc = jm.getJoinCriteria();
            if (jc != null) {
                ExpressionNode rewritten = rewriteOuterRefs(jc, aliasMap, 0);
                if (rewritten != jc) {
                    jm.setJoinCriteria(rewritten);
                }
            }
        }
    }

    // Rewrites outer-ref aliases inside subquery expressions (e.g., IN (SELECT ...)).
    // Defensive programming
    private void rewriteOuterRefsInSubquery(
            IQueryModel subquery,
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
            IQueryModel jm = subquery.getJoinModels().getQuick(i);
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
            IQueryModel inner,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth
    ) {
        if (inner.isOwnCorrelatedAtDepth(depth, CORRELATED_PROJECTION)) {
            for (int i = 0, n = inner.getBottomUpColumns().size(); i < n; i++) {
                QueryColumn col = inner.getBottomUpColumns().getQuick(i);
                ExpressionNode ast = col.getAst();
                if (hasCorrelatedExprAtDepth(ast, depth)) {
                    col.of(col.getAlias(),
                            rewriteOuterRefs(ast, outerToInnerAlias, depth));
                }
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
    private int saveAndRemapOuterToInnerAlias(CharSequence cloneAlias) {
        int aliasSaveBase = outerAliasSaveStack.size();
        ObjList<CharSequence> oKeys = outerToInnerAlias.keys();
        for (int ok = 0, okn = oKeys.size(); ok < okn; ok++) {
            CharSequence key = oKeys.getQuick(ok);
            if (key == null) continue;
            outerAliasSaveStack.add(outerToInnerAlias.get(key));
            CharSequence colName = unqualify(key);
            CharacterStoreEntry cse = characterStore.newEntry();
            cse.put(cloneAlias).put("_").put(colName);
            outerToInnerAlias.put(key, cse.toImmutable());
        }
        return aliasSaveBase;
    }

    private int scanModelForCorrelatedRefs(
            IQueryModel current,
            IQueryModel outerModel,
            int lateralJoinIndex,
            int lateralDepth,
            ObjList<LowerCaseCharSequenceIntHashMap> correlatedColumns
    ) {
        int flags = 0;
        boolean baseline = hasCorrelation;
        ObjList<QueryColumn> cols = current.getBottomUpColumns();
        for (int i = 0, n = cols.size(); i < n; i++) {
            collectCorrelatedRef(cols.getQuick(i).getAst(), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);
        }
        if (!baseline && hasCorrelation) {
            flags |= CORRELATED_PROJECTION;
        }
        hasCorrelation = baseline;

        collectCorrelatedRef(current.getWhereClause(), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);
        if (!baseline && hasCorrelation) {
            flags |= CORRELATED_WHERE;
        }
        hasCorrelation = baseline;

        ObjList<ExpressionNode> orderBy = current.getOrderBy();
        for (int i = 0, n = orderBy.size(); i < n; i++) {
            collectCorrelatedRef(orderBy.getQuick(i), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);
        }
        if (!baseline && hasCorrelation) {
            flags |= CORRELATED_ORDER_BY;
        }
        hasCorrelation = baseline;

        ObjList<ExpressionNode> groupBy = current.getGroupBy();
        for (int i = 0, n = groupBy.size(); i < n; i++) {
            collectCorrelatedRef(groupBy.getQuick(i), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);
        }
        if (!baseline && hasCorrelation) {
            flags |= CORRELATED_GROUP_BY;
        }
        hasCorrelation = baseline;

        collectCorrelatedRef(current.getSampleBy(), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);
        if (!baseline && hasCorrelation) {
            flags |= CORRELATED_SAMPLE_BY;
        }
        hasCorrelation = baseline;

        ObjList<ExpressionNode> latestBy = current.getLatestBy();
        for (int i = 0, n = latestBy.size(); i < n; i++) {
            collectCorrelatedRef(latestBy.getQuick(i), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);
        }
        if (!baseline && hasCorrelation) {
            flags |= CORRELATED_LATEST_BY;
        }
        hasCorrelation = baseline;

        collectCorrelatedRef(current.getLimitLo(), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);
        collectCorrelatedRef(current.getLimitHi(), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);
        if (!baseline && hasCorrelation) {
            flags |= CORRELATED_LIMIT;
        }
        hasCorrelation = baseline;

        for (int i = 1, n = current.getJoinModels().size(); i < n; i++) {
            IQueryModel jm = current.getJoinModels().getQuick(i);
            collectCorrelatedRef(jm.getJoinCriteria(), outerModel, lateralJoinIndex, lateralDepth, current, correlatedColumns);
        }
        if (!baseline && hasCorrelation) {
            flags |= CORRELATED_JOIN_ON;
        }

        hasCorrelation = flags != 0 || baseline;
        return flags;
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

    // TODO: when the lateral subquery has multiple join/union branches, each
    //  branch gets its own copy of __qdb_outer_ref__ via terminateHere, causing
    //  the DISTINCT subquery to execute multiple times. Like CTE, this needs a
    //  materializing RecordCursorFactory that executes once and serves multiple
    //  cursors from the cached result.
    private void setupOuterRefDataSource(
            IQueryModel outerRefSubquery,
            IQueryModel outerModel,
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
            IQueryModel outerJm = outerModel.getJoinModels().getQuick(maxIndex);
            IQueryModel outerRefBase = createOuterRefBase(outerJm);
            outerRefSubquery.setNestedModel(outerRefBase);
            registerDataSourceAlias(outerRefSubquery, outerRefBase, 0);
            return;
        }

        for (int i = 0, n = outerModel.getJoinModels().size(); i <= maxIndex && i < n; i++) {
            IQueryModel outerJm = outerModel.getJoinModels().getQuick(i);
            IQueryModel outerRefBase = createOuterRefBase(outerJm);
            if (outerRefSubquery.getNestedModel() == null) {
                outerRefSubquery.setNestedModel(outerRefBase);
                registerDataSourceAlias(outerRefSubquery, outerRefBase, 0);
            } else {
                IQueryModel nestedBase = outerRefSubquery.getNestedModel();
                if (outerJm.getJoinCriteria() != null) {
                    outerRefBase.setJoinType(outerJm.getJoinType());
                    outerRefBase.setJoinCriteria(
                            ExpressionNode.deepClone(expressionNodePool, outerJm.getJoinCriteria())
                    );
                } else {
                    outerRefBase.setJoinType(IQueryModel.JOIN_CROSS);
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
    //   IQueryModel chain:    SELECT → GROUP BY → [table: trades]
    //                                                ↑ terminateHere inserts here
    //   Result:              SELECT → GROUP BY → trades CROSS JOIN __qdb_outer_ref__0
    //                                            WHERE order_id = __qdb_outer_ref__0.id
    private void terminateHere(
            IQueryModel current,
            IQueryModel outerRefJoinModel,
            LowerCaseCharSequenceObjHashMap<CharSequence> outerToInnerAlias,
            int depth
    ) {
        ObjList<IQueryModel> joinModels = current.getJoinModels();
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
            IQueryModel shifted = joinModels.getQuick(si);
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
            outerRefJoinModel.setJoinType(IQueryModel.JOIN_INNER);
        }
        assert outerRefJoinModel.getNestedModel() instanceof QueryModel;
        sharedModels.add((QueryModel) outerRefJoinModel.getNestedModel());
    }

    private void tryEliminateOuterRef(
            IQueryModel selectModel,
            IQueryModel joinLayer,
            IQueryModel joinModel,
            CharSequence outerRefAlias
    ) throws SqlException {
        IQueryModel topInner = joinModel.getNestedModel();
        CharSequence lateralAlias = joinModel.getAlias() != null
                ? joinModel.getAlias().token : null;
        IQueryModel check = topInner;
        while (check != null) {
            if (check.getUnionModel() != null) {
                return;
            }
            check = check.getNestedModel();
        }

        tryEliminateOuterRefInBranch(topInner, selectModel, joinLayer, joinModel, lateralAlias, outerRefAlias);
    }

    private void tryEliminateOuterRefInBranch(
            IQueryModel branchTop,
            IQueryModel selectModel,
            IQueryModel joinLayer,
            IQueryModel joinModel,
            CharSequence lateralAlias,
            CharSequence outerRefAlias
    ) throws SqlException {
        IQueryModel dataSourceLayer = null;
        int outerRefJmIndex = -1;
        IQueryModel current = branchTop;
        while (current != null) {
            for (int j = 1, jn = current.getJoinModels().size(); j < jn; j++) {
                IQueryModel jm = current.getJoinModels().getQuick(j);
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
            return;
        }

        IQueryModel outerRefJm = dataSourceLayer.getJoinModels().getQuick(outerRefJmIndex);
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
            return;
        }
        IQueryModel outerRefSubquery = outerRefJm.getNestedModel();
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
            if (isComplexChain(branchTop, dataSourceLayer, outerRefAlias, outerToInnerAlias)) {
                return;
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

        IQueryModel levelAboveDS = null;
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
        ObjList<IQueryModel> remainingJoins = dataSourceLayer.getJoinModels();
        LowerCaseCharSequenceIntHashMap aliasIdx = dataSourceLayer.getModelAliasIndexes();
        for (int si = outerRefJmIndex, sn = remainingJoins.size(); si < sn; si++) {
            IQueryModel shifted = remainingJoins.getQuick(si);
            ExpressionNode shiftedAlias = shifted.getAlias() != null ? shifted.getAlias() : shifted.getTableNameExpr();
            if (shiftedAlias != null) {
                aliasIdx.put(shiftedAlias.token, si);
            }
        }
    }

    // Pass 3: top-down traversal. Scans join models for __qdb_outer_ref__ data
    // sources and attempts to eliminate them. Uses the alignment join criteria
    // (e.g. sub.order_id = o.id) to build an alias mapping, then replaces all
    // __qdb_outer_ref__ column references with their inner equivalents and removes
    // the __qdb_outer_ref__ join model from the tree. This converts the decorrelated
    // plan back to a simple join without the synthetic outer-ref data source.
    private void tryEliminateOuterRefs(IQueryModel model, IQueryModel parent) throws SqlException {
        if (model == null || !model.isOptimisable()) {
            return;
        }
        tryEliminateOuterRefs(model.getUnionModel(), null);
        for (int i = 0, n = model.getJoinModels().size(); i < n; i++) {
            IQueryModel jm = model.getJoinModels().getQuick(i);
            IQueryModel nested = jm.getNestedModel();
            if (nested != null) {
                tryEliminateOuterRefs(nested, i == 0 ? model : null);
                if (i > 0) {
                    outerCols.clear();
                    collectOuterRefAliases(nested, outerCols);
                    for (int oi = 0, on = outerCols.size(); oi < on; oi++) {
                        CharSequence outerRefAlias = outerCols.getQuick(oi).token;
                        IQueryModel selectModel = (model.getBottomUpColumns().size() > 0 || parent == null)
                                ? model : parent;
                        tryEliminateOuterRef(selectModel, model, jm, outerRefAlias);
                    }
                }
            }
        }
    }

}
