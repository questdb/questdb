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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.groupby.AvgDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountDoubleGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountIntGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountLongConstGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountLongGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountSymbolGroupByFunction;
import io.questdb.griffin.engine.functions.groupby.SumDoubleGroupByFunction;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.JoinContext;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * Pre-construction eligibility contract for RFC 130. This descriptor borrows optimized models:
 * use it before ordinary join generation mutates them, and never retain it in a cursor factory.
 * Analysis does not move filters, swap models, initialize functions, or adopt child factories.
 * The planner selects only after verifying the compiled children and aggregate functions.
 */
public final class HashJoinGroupByCandidate {
    private final IntList baseColumnIndexes;
    private final int buildIndex;
    private final int buildKeyColumn;
    private final ExpressionNode buildOnFilter;
    private final IntList columnSources;
    private final LowerCaseCharSequenceIntHashMap[] inputColumns;
    private final IQueryModel joinModel;
    private final int logicalJoinType;
    private final IntList postJoinFilterSources;
    private final RecordMetadata probeBaseMetadata;
    private final int probeKeyColumn;
    private final ExpressionNode resolvedBuildOnFilter;
    private final IntList requiredBuildColumns;
    private final ObjList<QueryColumn> resolvedColumns;
    private final RecordMetadata resolvedMetadata;
    private final ObjList<ExpressionNode> resolvedPostJoinFilters;

    private HashJoinGroupByCandidate(Analyzer analyzer, int probeKeyColumn, int buildKeyColumn) {
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
        this.buildKeyColumn = buildKeyColumn;
        this.buildOnFilter = analyzer.buildOnFilter;
        this.joinModel = analyzer.join;
        this.logicalJoinType = analyzer.joinType;
        this.probeKeyColumn = probeKeyColumn;
        this.requiredBuildColumns = analyzer.requiredBuildColumns;
    }

    public int getBuildKeyColumn() {
        return buildKeyColumn;
    }

    public IQueryModel getBuildModel() {
        return joinModel.getJoinModels().getQuick(buildIndex);
    }

    /** Borrowed ON predicate, safe to apply only to the physical build input. */
    @Nullable
    public ExpressionNode getBuildOnFilter() {
        return buildOnFilter;
    }

    public int getLogicalJoinType() {
        return logicalJoinType;
    }

    public int getPhysicalJoinType() {
        return logicalJoinType == IQueryModel.JOIN_INNER ? IQueryModel.JOIN_INNER : IQueryModel.JOIN_LEFT_OUTER;
    }

    public int getProbeKeyColumn() {
        return probeKeyColumn;
    }

    public IQueryModel getProbeModel() {
        return joinModel.getJoinModels().getQuick(1 - buildIndex);
    }

    /** Base-table indexes, including columns used only by post-join filters. */
    public IntList getRequiredBuildColumns() {
        return requiredBuildColumns;
    }

    public boolean isInputSwapped() {
        return logicalJoinType == IQueryModel.JOIN_RIGHT_OUTER;
    }

    /** Exact implementations, not SQL names or supportsParallelism() alone. */
    public static boolean supportsAggregate(Function function) {
        if (!function.supportsParallelism() || !function.isStableWithinExecution()) {
            return false;
        }
        Class<?> type = function.getClass();
        if (type == CountLongConstGroupByFunction.class) {
            return true;
        }
        if (!(function instanceof UnaryFunction unary)) {
            return false;
        }
        Function arg = unary.getArg();
        if (!arg.supportsParallelism() || !arg.isStableWithinExecution()) {
            return false;
        }
        int argType = ColumnType.tagOf(arg.getType());
        return ((type == SumDoubleGroupByFunction.class || type == AvgDoubleGroupByFunction.class
                || type == CountDoubleGroupByFunction.class) && argType == ColumnType.DOUBLE)
                || (type == CountIntGroupByFunction.class && argType == ColumnType.INT)
                || (type == CountLongGroupByFunction.class && argType == ColumnType.LONG)
                || (type == CountSymbolGroupByFunction.class && argType == ColumnType.SYMBOL);
    }

    /**
     * Compile-time frame capability only. Execution must use logical typed frame reads and
     * qualify native/Parquet/conversion paths before selection is enabled; see the RFC contract.
     * This check borrows the filter. Transfer follows all capability checks and worker compilation.
     */
    public static boolean supportsProbeFactory(RecordCursorFactory factory) {
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

    public static boolean supportsValueType(int type) {
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.SHORT, ColumnType.CHAR,
                 ColumnType.INT, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP,
                 ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.SYMBOL -> true;
            default -> false;
        };
    }

    @Nullable
    static HashJoinGroupByCandidate analyse(
            IQueryModel groupBy,
            FunctionParser parser,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (groupBy.getSelectModelType() != IQueryModel.SELECT_MODEL_GROUP_BY || groupBy.getSampleBy() != null
                || !groupBy.isOptimisable() || groupBy.getSharedRefCount() > 0) {
            return null;
        }
        IQueryModel join = groupBy.getNestedModel();
        while (join != null && join.getJoinModels().size() == 1) {
            if (!isProjection(join)) {
                return null;
            }
            join = join.getNestedModel();
        }
        if (join == null || join.getJoinModels().size() != 2 || hasBarrier(join)) {
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
        JoinContext keys = slave.getJoinContext();
        if (keys == null || keys.aNames.size() != 1
                || keys.aIndexes.getQuick(0) == keys.bIndexes.getQuick(0)) {
            return null;
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
            Analyzer analyzer = new Analyzer(join, joinType,
                    joinType == IQueryModel.JOIN_RIGHT_OUTER ? order.getQuick(0) : order.getQuick(1),
                    leftReader.getMetadata(), rightReader.getMetadata(), parser, executionContext);
            int a = analyzer.resolveInput(keys.aIndexes.getQuick(0), keys.aNames.getQuick(0), 0);
            int b = analyzer.resolveInput(keys.bIndexes.getQuick(0), keys.bNames.getQuick(0), 0);
            if (a < 0 || b < 0 || analyzer.metadata.getColumnType(a) != ColumnType.INT
                    || analyzer.metadata.getColumnType(b) != ColumnType.INT) {
                return null;
            }
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
                    } else if (!supportsValueType(function.getType()) || !function.supportsParallelism() || !function.isStableWithinExecution()) {
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
            int buildKey = keys.aIndexes.getQuick(0) == analyzer.buildIndex ? a : b;
            int probeKey = buildKey == a ? b : a;
            return new HashJoinGroupByCandidate(analyzer, analyzer.columnIndexes.getQuick(probeKey), analyzer.columnIndexes.getQuick(buildKey));
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

    private static boolean isProjection(IQueryModel model) {
        return !hasBarrier(model) && (model.getSelectModelType() == IQueryModel.SELECT_MODEL_CHOOSE
                || model.getSelectModelType() == IQueryModel.SELECT_MODEL_VIRTUAL
                || model.getSelectModelType() == IQueryModel.SELECT_MODEL_NONE);
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
            if (postJoin && Chars.equalsIgnoreCase(node.token, "and")) {
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
            if (index < 0 || !supportsValueType(metadata.getColumnType(index))) {
                return null;
            }
            int source = columnSources.getQuick(index);
            usedSources |= 1 << source;
            if (source == buildIndex && !requiredBuildColumns.contains(columnIndexes.getQuick(index))) {
                requiredBuildColumns.add(columnIndexes.getQuick(index));
            }
            return ExpressionNode.FACTORY.newInstance().of(ExpressionNode.LITERAL, metadata.getColumnName(index), 0, position);
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
    }
}
