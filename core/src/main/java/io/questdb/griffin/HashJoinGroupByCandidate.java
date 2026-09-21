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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.groupby.HashJoinGroupByAggregates;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.JoinContext;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * Pre-construction eligibility contract for the fused parallel hash join GROUP BY. This descriptor borrows optimized models:
 * use it before ordinary join generation mutates them, and never retain it in a cursor factory.
 * Analysis does not move filters, swap models, initialize functions, or adopt child factories.
 * The planner selects only after verifying the compiled children and aggregate functions.
 */
public final class HashJoinGroupByCandidate {
    private final IntList baseColumnIndexes;
    private final int buildIndex;
    private final ExpressionNode buildOnFilter;
    private final IntList columnSources;
    private final LowerCaseCharSequenceIntHashMap[] inputColumns;
    private final boolean isInputSwapped;
    private final IQueryModel joinModel;
    private final HashJoinGroupByKeys keys;
    private final int logicalJoinType;
    private final IntList postJoinFilterSources;
    private final RecordMetadata probeBaseMetadata;
    private final ExpressionNode resolvedBuildOnFilter;
    private final IntList requiredBuildColumns;
    private final ObjList<QueryColumn> resolvedColumns;
    private final RecordMetadata resolvedMetadata;
    private final ObjList<ExpressionNode> resolvedPostJoinFilters;

    private HashJoinGroupByCandidate(Analyzer analyzer, HashJoinGroupByKeys keys, boolean isInputSwapped) {
        this.probeBaseMetadata = GenericRecordMetadata.copyOf(analyzer.sources[1 - analyzer.buildIndex]);
        this.postJoinFilterSources = analyzer.postJoinFilterSources;
        this.inputColumns = analyzer.inputColumns;
        this.resolvedBuildOnFilter = analyzer.resolvedBuildOnFilter;
        this.baseColumnIndexes = analyzer.columnIndexes;
        this.columnSources = analyzer.columnSources;
        this.resolvedColumns = analyzer.resolvedColumns;
        this.resolvedMetadata = analyzer.metadata;
        this.resolvedPostJoinFilters = analyzer.resolvedPostJoinFilters;
        this.buildIndex = analyzer.buildIndex;
        this.keys = keys;
        this.isInputSwapped = isInputSwapped;
        this.buildOnFilter = analyzer.buildOnFilter;
        this.joinModel = analyzer.join;
        this.logicalJoinType = analyzer.joinType;
        this.requiredBuildColumns = analyzer.requiredBuildColumns;
    }

    public IQueryModel getBuildModel() {
        return joinModel.getJoinModels().getQuick(buildIndex);
    }

    /** Borrowed ON predicate, safe to apply only to the physical build input. */
    @Nullable
    public ExpressionNode getBuildOnFilter() {
        return buildOnFilter;
    }

    /** Reconciled equality keys in sink order, with base-table column indexes. */
    public HashJoinGroupByKeys getKeys() {
        return keys;
    }

    public int getLogicalJoinType() {
        return logicalJoinType;
    }

    public int getPhysicalJoinType() {
        return logicalJoinType == IQueryModel.JOIN_INNER ? IQueryModel.JOIN_INNER : IQueryModel.JOIN_LEFT_OUTER;
    }

    public IQueryModel getProbeModel() {
        return joinModel.getJoinModels().getQuick(1 - buildIndex);
    }

    /** Base-table indexes, including columns used only by post-join filters. */
    public IntList getRequiredBuildColumns() {
        return requiredBuildColumns;
    }

    /** True when the build is the first input in join order: every RIGHT join, and an INNER join whose first table is smaller. */
    public boolean isInputSwapped() {
        return isInputSwapped;
    }

    /** Exact implementations from HashJoinGroupByAggregates, not SQL names or supportsParallelism() alone. */
    public static boolean supportsAggregate(Function function) {
        if (!isParallelSafe(function)) {
            return false;
        }
        Class<?> type = function.getClass();
        if (function instanceof UnaryFunction unary) {
            Function arg = unary.getArg();
            return isParallelSafe(arg) && HashJoinGroupByAggregates.isSupportedUnary(type, arg.getType());
        }
        if (function instanceof BinaryFunction binary) {
            return isParallelSafe(binary.getLeft()) && isParallelSafe(binary.getRight())
                    && HashJoinGroupByAggregates.isSupportedBinary(type);
        }
        return HashJoinGroupByAggregates.isSupportedNullary(type);
    }

    /** The fused scan reads page frames directly and does not support covering-index frame descriptors or their decode caches. */
    public static boolean supportsInputFactory(RecordCursorFactory factory) {
        for (RecordCursorFactory current = factory; current != null; current = current.getBaseFactory()) {
            if (current instanceof CoveringIndexRecordCursorFactory) {
                return false;
            }
        }
        return true;
    }

    /**
     * Compile-time frame capability only: the probe must expose page frames, either directly or under a
     * stealable, parallel-safe filter. Execution reads typed logical frame values, including Parquet columns.
     * This check borrows the filter. Transfer follows all capability checks and worker compilation.
     */
    public static boolean supportsProbeFactory(RecordCursorFactory factory) {
        if (!supportsInputFactory(factory)) {
            return false;
        }
        if (factory.supportsPageFrameCursor()) {
            return true;
        }
        return factory.supportsFilterStealing()
                && factory.getBaseFactory() != null
                && factory.getBaseFactory().supportsPageFrameCursor()
                && factory.getFilter() != null
                && factory.getFilter().supportsParallelism()
                && factory.getFilter().isStableWithinExecution();
    }

    /**
     * Build payload types: the row heap copies a build column, so only a build column answers to
     * this set. A probe column reaches the joined record straight off its page frame and takes no
     * type check at all, and a key column and a grouping expression answer to
     * {@link HashJoinGroupByKeys#supportsKeyType(int)} instead.
     */
    public static boolean supportsValueType(int type) {
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.SHORT, ColumnType.CHAR,
                 ColumnType.INT, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP,
                 ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.SYMBOL, ColumnType.IPv4,
                 ColumnType.UUID, ColumnType.LONG256, ColumnType.GEOBYTE, ColumnType.GEOSHORT,
                 ColumnType.GEOINT, ColumnType.GEOLONG, ColumnType.DECIMAL8, ColumnType.DECIMAL16,
                 ColumnType.DECIMAL32, ColumnType.DECIMAL64, ColumnType.DECIMAL128,
                 ColumnType.DECIMAL256 -> true;
            // STRING, VARCHAR, BINARY and ARRAY are variable-size, so the row heap cannot copy
            // them. INTERVAL is not persisted, so no base-table column carries one.
            default -> false;
        };
    }

    /**
     * Returns null for a shape the fused plan does not support, and when analysis throws SqlException,
     * so that the ordinary compile reports its own error. Analysis parses an expression only when each
     * of its bind variables has a type that FunctionParser does not refine (not ColumnType.isUndefined()),
     * so analysis leaves every bind variable type unchanged.
     */
    @Nullable
    static HashJoinGroupByCandidate analyse(
            IQueryModel groupBy,
            FunctionParser parser,
            SqlExecutionContext executionContext
    ) {
        return analyse(groupBy, parser, executionContext, false);
    }

    /**
     * With {@code isBuildFlipped}, analyses the orientation that builds the other input of an INNER
     * join, the one the table sizes do not pick, and returns null for an outer join, whose build is
     * fixed. See {@link io.questdb.griffin.engine.table.HashJoinGroupByBuildChoiceRecordCursorFactory}.
     */
    @Nullable
    static HashJoinGroupByCandidate analyse(
            IQueryModel groupBy,
            FunctionParser parser,
            SqlExecutionContext executionContext,
            boolean isBuildFlipped
    ) {
        if (groupBy.getSelectModelType() != IQueryModel.SELECT_MODEL_GROUP_BY || groupBy.getSampleBy() != null
                || !groupBy.isOptimisable() || groupBy.getSharedRefCount() > 0 || hasFill(groupBy)) {
            return null;
        }
        // The fused factory replaces the generation of every model down to the join, so it must
        // reject the clauses that generateLatestBy() and generateJoins() apply to their output.
        IQueryModel join = groupBy.getNestedModel();
        while (join != null && join.getJoinModels().size() == 1) {
            if (!isProjection(join) || join.getLatestBy().size() > 0) {
                return null;
            }
            join = join.getNestedModel();
        }
        if (join == null || join.getJoinModels().size() != 2 || hasBarrier(join)
                || join.getLatestBy().size() > 0 || join.getConstWhereClause() != null) {
            return null;
        }
        IntList order = join.getOrderedJoinModels();
        if (order.size() != 2) {
            return null;
        }
        IQueryModel slave = join.getJoinModels().getQuick(order.getQuick(1));
        int joinType = slave.getJoinType();
        if (joinType != IQueryModel.JOIN_INNER && joinType != IQueryModel.JOIN_LEFT_OUTER
                && joinType != IQueryModel.JOIN_RIGHT_OUTER) {
            return null;
        }
        // A residual may be extracted only when it depends on the physical build alone.
        JoinContext joinContext = slave.getJoinContext();
        if (joinContext == null || joinContext.aNames.size() == 0) {
            return null;
        }
        for (int i = 0, n = joinContext.aNames.size(); i < n; i++) {
            // These are join-model indexes, so the guard rejects an equality of two columns of one
            // input (ON p.a = p.b) and admits a self-join, whose two names are two models.
            if (joinContext.aIndexes.getQuick(i) == joinContext.bIndexes.getQuick(i)) {
                return null;
            }
        }
        IQueryModel left = baseTable(join.getJoinModels().getQuick(0), join);
        IQueryModel right = baseTable(join.getJoinModels().getQuick(1), join);
        if (left == null || right == null) {
            return null;
        }
        try (
                TableReader leftReader = executionContext.getReader(executionContext.getTableToken(left.getTableName()), left.getMetadataVersion());
                TableReader rightReader = executionContext.getReader(executionContext.getTableToken(right.getTableName()), right.getMetadataVersion())
        ) {
            int buildIndex = selectBuildIndex(joinType, order, leftReader.size(), rightReader.size());
            if (isBuildFlipped) {
                if (joinType != IQueryModel.JOIN_INNER) {
                    return null;
                }
                buildIndex = buildIndex == order.getQuick(0) ? order.getQuick(1) : order.getQuick(0);
            }
            Analyzer analyzer = new Analyzer(join, joinType, buildIndex,
                    leftReader.getMetadata(), rightReader.getMetadata(), parser, executionContext);
            final HashJoinGroupByKeys keys = analyzer.resolveKeys(joinContext);
            if (keys == null) {
                return null;
            }
            // A key column is not a payload column: the key table or the map holds it.
            analyzer.requiredBuildColumns.clear();
            if (!analyzer.checkBuildOnFilter(slave.getOuterJoinExpressionClause())) {
                return null;
            }
            boolean hasAggregate = false;
            for (int i = 0; i < groupBy.getColumns().size(); i++) {
                ExpressionNode expression = analyzer.resolve(groupBy.getColumns().getQuick(i).getAst(), groupBy.getNestedModel(), -1, 0);
                if (expression == null) {
                    return null;
                }
                analyzer.resolvedColumns.add(QueryColumn.FACTORY.newInstance().of(
                        Chars.toString(groupBy.getColumns().getQuick(i).getName()), expression));
                try (Function function = parser.parseFunction(expression, analyzer.metadata, executionContext)) {
                    if (function instanceof GroupByFunction) {
                        if (!supportsAggregate(function)) {
                            return null;
                        }
                        hasAggregate = true;
                    } else if (!HashJoinGroupByKeys.supportsKeyType(function.getType())
                            || !function.supportsParallelism() || !function.isStableWithinExecution()) {
                        return null;
                    }
                }
            }
            if (!hasAggregate || !analyzer.checkFilters(groupBy.getNestedModel(), -1)) {
                return null;
            }
            for (int i = 0; i < 2; i++) {
                if (!analyzer.checkFilters(join.getJoinModels().getQuick(i), i)) {
                    return null;
                }
            }
            analyzer.captureInputColumns();
            // captureInputColumns() tolerates unresolvable input columns, so check the flag after it.
            if (analyzer.hasUndefinedBindVariable) {
                return null;
            }
            return new HashJoinGroupByCandidate(analyzer, keys, buildIndex == order.getQuick(0));
        } catch (SqlException e) {
            // The ordinary plan reports errors in its own compile order, and only its interval extraction
            // and generateFilter() compile optimiser-internal nodes such as and_offset. A failed
            // parseFunction() releases only its own functions, so when this GROUP BY is a sub-query
            // operand, the enclosing expression's parse keeps its pending operands.
            return null;
        }
    }

    int getBaseColumnIndex(int resolvedIndex) {
        return baseColumnIndexes.getQuick(resolvedIndex);
    }

    IntList getInputColumns(RecordMetadata metadata, boolean build) {
        LowerCaseCharSequenceIntHashMap indexes = inputColumns[build ? buildIndex : 1 - buildIndex];
        IntList result = new IntList(metadata.getColumnCount());
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            result.add(indexes.get(metadata.getColumnName(i)));
        }
        return result;
    }

    ExpressionNode getResolvedBuildOnFilter() {
        return resolvedBuildOnFilter;
    }

    ObjList<QueryColumn> getResolvedColumns() {
        return resolvedColumns;
    }

    RecordMetadata getResolvedMetadata() {
        return resolvedMetadata;
    }

    ObjList<ExpressionNode> getResolvedPostJoinFilters() {
        return resolvedPostJoinFilters;
    }

    boolean isBuildColumn(int resolvedIndex) {
        return columnSources.getQuick(resolvedIndex) == buildIndex;
    }

    /** Move preserved-probe WHERE conjuncts before child compilation, retaining interval extraction. */
    void pushProbePostJoinFilters() {
        IQueryModel table = baseTable(getProbeModel(), joinModel);
        for (int i = resolvedPostJoinFilters.size() - 1; i >= 0; i--) {
            if (postJoinFilterSources.getQuick(i) == (1 << (1 - buildIndex))) {
                ExpressionNode filter = remapProbeFilter(resolvedPostJoinFilters.getQuick(i));
                ExpressionNode existing = table.getWhereClause();
                if (existing != null) {
                    ExpressionNode and = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.OPERATION, "and", 0, filter.position);
                    and.paramCount = 2;
                    and.lhs = existing;
                    and.rhs = filter;
                    filter = and;
                }
                table.setWhereClause(filter);
                resolvedPostJoinFilters.remove(i);
            }
        }
    }

    private static IQueryModel baseTable(IQueryModel input, IQueryModel join) {
        IQueryModel current = input;
        while (current != null) {
            if (hasBarrier(current) || (current != join && current.getJoinModels().size() != 1)) {
                return null;
            }
            if (current.getTableName() != null) {
                return current.getTableNameFunction() == null && current.getLatestBy().size() == 0 ? current : null;
            }
            if (!isProjection(current)) {
                return null;
            }
            current = current.getNestedModel();
        }
        return null;
    }

    private static boolean hasBarrier(IQueryModel model) {
        return !model.isOptimisable() || model.getSharedRefCount() > 0
                || model.getLimitLo() != null || model.getLimitHi() != null || model.getUnionModel() != null
                || model.getSelectModelType() == IQueryModel.SELECT_MODEL_DISTINCT
                || model.getSampleBy() != null;
    }

    /**
     * Mirrors generateFill(): the first fill stride on the nested chain wraps the GROUP BY, or
     * fails compilation, unless its value list is empty or a lone NONE.
     */
    private static boolean hasFill(IQueryModel groupBy) {
        for (IQueryModel model = groupBy; model != null; model = model.getNestedModel()) {
            if (model.getFillStride() != null) {
                ObjList<ExpressionNode> values = model.getFillValues();
                return values == null || (values.size() > 0
                        && !(values.size() == 1 && SqlKeywords.isNoneKeyword(values.getQuick(0).token)));
            }
        }
        return false;
    }

    private static boolean isParallelSafe(Function function) {
        return function.supportsParallelism() && function.isStableWithinExecution();
    }

    private static boolean isProjection(IQueryModel model) {
        return !hasBarrier(model) && (model.getSelectModelType() == IQueryModel.SELECT_MODEL_CHOOSE
                || model.getSelectModelType() == IQueryModel.SELECT_MODEL_VIRTUAL
                || model.getSelectModelType() == IQueryModel.SELECT_MODEL_NONE);
    }

    /**
     * The probe preserves its unmatched rows, so an outer join fixes the build: the table after
     * the join for LEFT, the one before it for RIGHT. An INNER join builds the table with fewer
     * rows and keeps the join order on a tie. Row counts ignore filters and bind values on
     * purpose: one cached factory serves every bind value, and the unfiltered count bounds the
     * build at min(|left|, |right|) rows whatever the filters select.
     */
    private static int selectBuildIndex(int joinType, IntList order, long leftSize, long rightSize) {
        final int first = order.getQuick(0);
        final int second = order.getQuick(1);
        return switch (joinType) {
            case IQueryModel.JOIN_RIGHT_OUTER -> first;
            case IQueryModel.JOIN_LEFT_OUTER -> second;
            // The readers follow model indexes: leftSize is model 0, rightSize is model 1.
            default -> (first == 0 ? leftSize : rightSize) < (second == 0 ? leftSize : rightSize) ? first : second;
        };
    }

    private ExpressionNode remapProbeFilter(ExpressionNode node) {
        if (node == null) {
            return null;
        }
        CharSequence token = node.token;
        if (node.type == ExpressionNode.LITERAL) {
            token = probeBaseMetadata.getColumnName(baseColumnIndexes.getQuick(resolvedMetadata.getColumnIndex(token)));
        }
        ExpressionNode copy = ExpressionNode.FACTORY.newInstance().of(node.type, token, node.precedence, node.position);
        copy.paramCount = node.paramCount;
        copy.lhs = remapProbeFilter(node.lhs);
        copy.rhs = remapProbeFilter(node.rhs);
        for (int i = 0; i < node.args.size(); i++) {
            copy.args.add(remapProbeFilter(node.args.getQuick(i)));
        }
        return copy;
    }

    private static final class Analyzer {
        private final int buildIndex;
        private final IntList columnIndexes = new IntList();
        private final IntList columnSources = new IntList();
        private final SqlExecutionContext executionContext;
        private final LowerCaseCharSequenceIntHashMap[] inputColumns = {new LowerCaseCharSequenceIntHashMap(), new LowerCaseCharSequenceIntHashMap()};
        private final IQueryModel join;
        private final int joinType;
        private final GenericRecordMetadata metadata = new GenericRecordMetadata();
        private final FunctionParser parser;
        private final IntList postJoinFilterSources = new IntList();
        private final IntList requiredBuildColumns = new IntList();
        private final ObjList<QueryColumn> resolvedColumns = new ObjList<>();
        private final ObjList<ExpressionNode> resolvedPostJoinFilters = new ObjList<>();
        private final RecordMetadata[] sources;
        private ExpressionNode buildOnFilter;
        private boolean hasUndefinedBindVariable;
        // An input column reference is a key candidate or a name the input mapping captures, so it
        // takes no type check: HashJoinGroupByKeys.add() gates the key types, and the mapping must
        // name every input column that an admitted expression can later reach.
        private boolean isResolvingInputColumn;
        private ExpressionNode resolvedBuildOnFilter;
        private int usedSources;

        private Analyzer(
                IQueryModel join,
                int joinType,
                int buildIndex,
                RecordMetadata left,
                RecordMetadata right,
                FunctionParser parser,
                SqlExecutionContext executionContext
        ) {
            this.join = join;
            this.joinType = joinType;
            this.buildIndex = buildIndex;
            this.sources = new RecordMetadata[]{left, right};
            this.parser = parser;
            this.executionContext = executionContext;
            for (int source = 0; source < 2; source++) {
                RecordMetadata sourceMetadata = sources[source];
                for (int i = 0; i < sourceMetadata.getColumnCount(); i++) {
                    metadata.add(new TableColumnMetadata("c" + metadata.getColumnCount(), sourceMetadata.getColumnType(i), IndexType.NONE, 0, true, null));
                    columnIndexes.add(i);
                    columnSources.add(source);
                }
            }
        }

        private void captureInputColumns() {
            int payloadSize = requiredBuildColumns.size();
            isResolvingInputColumn = true;
            try {
                for (int source = 0; source < 2; source++) {
                    IQueryModel input = join.getJoinModels().getQuick(source);
                    ObjList<QueryColumn> columns = input.getColumns();
                    int count = columns.size() > 0 ? columns.size() : sources[source].getColumnCount();
                    for (int i = 0; i < count; i++) {
                        CharSequence name = columns.size() > 0 ? columns.getQuick(i).getName() : sources[source].getColumnName(i);
                        int resolved = resolveInput(source, name, 0);
                        if (resolved >= 0) {
                            inputColumns[source].put(Chars.toString(name), columnIndexes.getQuick(resolved));
                        }
                    }
                }
            } finally {
                isResolvingInputColumn = false;
            }
            requiredBuildColumns.setPos(payloadSize);
        }

        private boolean checkBuildOnFilter(ExpressionNode node) throws SqlException {
            if (node == null) {
                return true;
            }
            if (!checkFilter(node, join, -1, false) || (usedSources != 0 && usedSources != (1 << buildIndex))) {
                return false;
            }
            buildOnFilter = node;
            int payloadSize = requiredBuildColumns.size();
            resolvedBuildOnFilter = resolve(node, join, -1, 0);
            requiredBuildColumns.setPos(payloadSize);
            return true;
        }

        private boolean checkFilter(ExpressionNode node, IQueryModel model, int source, boolean postJoin) throws SqlException {
            if (node == null) {
                return true;
            }
            // Independently single-input conjuncts are legal; a cross-input OR is not.
            // A sub-query predicate is a QUERY node without a token; resolve() rejects it below.
            if (postJoin && node.type == ExpressionNode.OPERATION && Chars.equalsIgnoreCase(node.token, "and")) {
                return checkFilter(node.lhs, model, source, true) && checkFilter(node.rhs, model, source, true);
            }
            int payloadSize = requiredBuildColumns.size();
            usedSources = 0;
            ExpressionNode expression = resolve(node, model, source, 0);
            if (!postJoin) {
                requiredBuildColumns.setPos(payloadSize);
            }
            if (expression == null || (postJoin && usedSources == 3)) {
                return false;
            }
            if (postJoin) {
                resolvedPostJoinFilters.add(expression);
                postJoinFilterSources.add(usedSources);
            }
            try (Function function = parser.parseFunction(expression, metadata, executionContext)) {
                return function.getType() == ColumnType.BOOLEAN && function.supportsParallelism() && function.isStableWithinExecution();
            }
        }

        private boolean checkFilters(IQueryModel model, int source) throws SqlException {
            while (model != null) {
                if (model == join && source == -1) {
                    return true;
                }
                if (!checkFilter(model.getWhereClause(), model.getNestedModel() != null ? model.getNestedModel() : model, source, source == -1)
                        || !checkFilter(model.getConstWhereClause(), model, source, source == -1)
                        || !checkFilter(model.getPostJoinWhereClause(), join, -1, true)) {
                    return false;
                }
                model = model.getNestedModel();
            }
            return true;
        }

        private ExpressionNode column(int index, int position) {
            if (index < 0) {
                return null;
            }
            final int source = columnSources.getQuick(index);
            // The row heap copies a build column, so only a build column takes the payload type set.
            // A probe column of any type reaches the joined record straight off its page frame.
            if (!isResolvingInputColumn && source == buildIndex && !supportsValueType(metadata.getColumnType(index))) {
                return null;
            }
            usedSources |= 1 << source;
            if (source == buildIndex && !requiredBuildColumns.contains(columnIndexes.getQuick(index))) {
                requiredBuildColumns.add(columnIndexes.getQuick(index));
            }
            return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, metadata.getColumnName(index), 0, position);
        }

        private boolean isBindVariableTypeDefined(CharSequence token) {
            final BindVariableService bindVariables = executionContext.getBindVariableService();
            if (bindVariables == null || token.length() < 2) {
                return false;
            }
            final Function variable;
            if (token.charAt(0) == ':') {
                variable = bindVariables.getFunction(token);
            } else {
                try {
                    final int index = Numbers.parseInt(token, 1, token.length());
                    variable = index > 0 ? bindVariables.getFunction(index - 1) : null;
                } catch (NumericException e) {
                    return false;
                }
            }
            // FunctionParser refines exactly the types ColumnType.isUndefined() accepts, which includes
            // the weak-dimension arrays that PG Parse defines for array parameters.
            return variable != null && !ColumnType.isUndefined(variable.getType());
        }

        private ExpressionNode resolve(ExpressionNode node, IQueryModel model, int source, int depth) {
            if (node == null || depth > 128) {
                return null;
            }
            if (node.type == ExpressionNode.LITERAL) {
                if (model == join && source == -1) {
                    int dot = Chars.lastIndexOf(node.token, 0, node.token.length(), '.');
                    int found = -1;
                    for (int i = 0; i < 2; i++) {
                        IQueryModel input = join.getJoinModels().getQuick(i);
                        if (dot >= 0 && !Chars.equalsIgnoreCase(node.token.subSequence(0, dot), input.getName())) {
                            continue;
                        }
                        int index = resolveInput(i, node.token.subSequence(dot + 1, node.token.length()), depth + 1);
                        if (index >= 0) {
                            if (found >= 0) {
                                return null;
                            }
                            found = index;
                        }
                    }
                    return column(found, node.position);
                }
                if (model.getTableName() != null && source >= 0) {
                    int dot = Chars.lastIndexOf(node.token, 0, node.token.length(), '.');
                    int index = sources[source].getColumnIndexQuiet(node.token, dot + 1, node.token.length());
                    return column(index < 0 ? -1 : index + (source == 0 ? 0 : sources[0].getColumnCount()), node.position);
                }
                if (model.getSelectModelType() == IQueryModel.SELECT_MODEL_NONE && model.getNestedModel() != null) {
                    return resolve(node, model.getNestedModel(), source, depth + 1);
                }
                QueryColumn alias = model.getAliasToColumnMap().get(node.token);
                if (alias == null || model.getNestedModel() == null) {
                    return null;
                }
                return resolve(alias.getAst(), model.getNestedModel(), source, depth + 1);
            }
            if (node.type != ExpressionNode.CONSTANT && node.type != ExpressionNode.BIND_VARIABLE
                    && node.type != ExpressionNode.FUNCTION && node.type != ExpressionNode.OPERATION
                    && node.type != ExpressionNode.SET_OPERATION) {
                return null;
            }
            // Scalar subqueries and array/record access need separate ownership and storage contracts.
            if (node.queryModel != null || node.windowExpression != null) {
                return null;
            }
            // Type inference depends on parse order, and the fused plan parses its WHERE, ON and
            // aggregate expressions in a different order than the ordinary plan. Resolving before
            // parsing also keeps this analysis from defining the type itself.
            if (node.type == ExpressionNode.BIND_VARIABLE && !isBindVariableTypeDefined(node.token)) {
                hasUndefinedBindVariable = true;
                return null;
            }
            ExpressionNode copy = ExpressionNode.FACTORY.newInstance().of(node.type, node.token, node.precedence, node.position);
            copy.paramCount = node.paramCount;
            if (node.lhs != null && (copy.lhs = resolve(node.lhs, model, source, depth + 1)) == null) {
                return null;
            }
            if (node.rhs != null && (copy.rhs = resolve(node.rhs, model, source, depth + 1)) == null) {
                return null;
            }
            for (int i = 0; i < node.args.size(); i++) {
                ExpressionNode arg = resolve(node.args.getQuick(i), model, source, depth + 1);
                if (arg == null) {
                    return null;
                }
                copy.args.add(arg);
            }
            return copy;
        }

        private int resolveInput(int source, CharSequence name, int depth) {
            if (source < 0 || source > 1) {
                return -1;
            }
            ExpressionNode column = resolve(ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, name, 0, 0),
                    join.getJoinModels().getQuick(source), source, depth + 1);
            return column != null && column.type == ExpressionNode.LITERAL ? metadata.getColumnIndexQuiet(column.token) : -1;
        }

        /**
         * Resolves and reconciles every equality of the join context, in its order. Returns null
         * for a key the fused plan does not support and for a pair the ordinary plan rejects, so
         * that the ordinary compile reports the mismatch itself.
         */
        private HashJoinGroupByKeys resolveKeys(JoinContext context) {
            final HashJoinGroupByKeys keys = new HashJoinGroupByKeys();
            final int count = context.aNames.size();
            isResolvingInputColumn = true;
            try {
                for (int i = 0; i < count; i++) {
                    final int a = resolveInput(context.aIndexes.getQuick(i), context.aNames.getQuick(i), 0);
                    final int b = resolveInput(context.bIndexes.getQuick(i), context.bNames.getQuick(i), 0);
                    if (a < 0 || b < 0) {
                        return null;
                    }
                    final int build = context.aIndexes.getQuick(i) == buildIndex ? a : b;
                    final int probe = build == a ? b : a;
                    if (!keys.add(columnIndexes.getQuick(probe), metadata.getColumnType(probe),
                            columnIndexes.getQuick(build), metadata.getColumnType(build))) {
                        return null;
                    }
                }
            } finally {
                isResolvingInputColumn = false;
            }
            return keys;
        }
    }
}
