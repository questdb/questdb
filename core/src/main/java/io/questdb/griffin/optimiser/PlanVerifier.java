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

package io.questdb.griffin.optimiser;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.DistinctPlan;
import io.questdb.griffin.plan.logical.ExpressionVisitor;
import io.questdb.griffin.plan.logical.FillPlan;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.ForwardingPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.FunctionSourcePlan;
import io.questdb.griffin.plan.logical.GroupingPlan;
import io.questdb.griffin.plan.logical.HorizonJoinPlan;
import io.questdb.griffin.plan.logical.HorizonJoinSlave;
import io.questdb.griffin.plan.logical.JoinDependency;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.LogicalPlanPrinter;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PlanExpressionVisitor;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationKind;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.griffin.plan.logical.SortKeys;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.griffin.plan.logical.TreeWalk;
import io.questdb.griffin.plan.logical.UnnestSpec;
import io.questdb.griffin.plan.logical.WindowJoinPlan;
import io.questdb.griffin.plan.logical.WindowJoinStep;
import io.questdb.griffin.plan.logical.WindowPlan;
import io.questdb.griffin.plan.logical.WindowSpec;
import io.questdb.std.Chars;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.TestOnly;

/**
 * Checks the structural invariants of a logical plan tree after binding and after optimisation; it runs only under
 * {@code assert} and fails with the violated invariant, the node and the stage that produced the plan.
 */
public final class PlanVerifier {
    public static final String ACCESS_PATH = "scan access path facts contradict its kind";
    public static final String ACCESS_PATH_UNPLANNED = "scan has no access path after access path planning";
    public static final String AGGREGATE_ALGORITHM = "aggregate algorithm contradicts its kind, keys or input";
    public static final String AGGREGATE_CALL = "aggregate value is not an aggregate call";
    public static final String AGGREGATE_SHAPE = "aggregate output is not its keys followed by its aggregates";
    public static final String AGGREGATE_TYPE = "aggregate output type differs from its expression";
    public static final String CHOICE_UNPLANNED = "physical choice is not recorded after access path planning";
    public static final String COLUMN_TYPE = "column expression type differs from the column it reads";
    public static final String CURSOR_PLAN = "sub-query cursor has no plan";
    public static final String DEPENDENT_STEP = "dependent join step survives decorrelation";
    public static final String DUPLICATE_COLUMN_ID = "output lists a column id twice";
    public static final String EXPRESSION_NULL = "expression is null";
    public static final String EXPRESSION_SHARED = "expression node appears twice in one expression tree";
    public static final String FILL_ALGORITHM = "fill reads the rows of an input other than a SAMPLE BY cursor without a fill";
    public static final String FILL_SHAPE = "fill lists differ in length";
    public static final String FILL_TIMESTAMP = "fill does not designate its bucket timestamp";
    public static final String FILTER_ALGORITHM = "filter algorithm recorded where the generator chooses none";
    public static final String FUNCTION_OVERLOAD = "function call has no overload";
    public static final String HORIZON_JOIN_SHAPE = "horizon join output does not start with the master columns";
    public static final String INPUT_NULL = "input is null";
    public static final String JOIN_INPUT_SOURCE = "join input has neither a plan nor an UNNEST, or both";
    public static final String JOIN_ALGORITHM = "join step algorithm contradicts its kind, keys or master side";
    public static final String JOIN_FILTER_ORIGINS = "join filter conjuncts and their origins differ in length";
    public static final String JOIN_KEYS = "join key lists differ in length";
    public static final String JOIN_ORDER = "ordered join inputs are not a permutation of the inputs";
    public static final String JOIN_OUTPUT = "join output does not list every input column exactly once";
    public static final String JOIN_SCOPE = "join step scope differs from its ordered prefix";
    public static final String JOIN_TIMESTAMP = "join designated timestamp is not the leading input's";
    public static final String LATEST_BY_ALGORITHM = "LATEST BY over a table scan has an algorithm for a derived input";
    public static final String LIMIT_APPLICATION = "LIMIT application contradicts the sort under it";
    public static final String NODE_SHARED = "node is reached through two paths";
    public static final String OPERAND_COLUMN = "LIMIT, SAMPLE BY or FILL operand reads a column";
    public static final String ORDER_REQUEST = "requested order names a column the node does not output";
    public static final String OUTER_COLUMN_SCOPE = "outer column outside a dependent join step";
    public static final String OUTER_COLUMN_UNRESOLVED = "outer column is not a column of a preceding join input";
    public static final String OUTPUT_FORWARDING = "output does not forward the input columns";
    public static final String OUTPUT_TIMESTAMP = "designated timestamp differs from the input's";
    public static final String PREDICATE_CONSTANT = "constant predicate is not folded to a literal";
    public static final String PREDICATE_TYPE = "predicate is not BOOLEAN";
    public static final String PROJECT_SHAPE = "projection expression count differs from its output";
    public static final String PROJECT_TIMESTAMP_DROP = "projection drops the timestamp outside a computing projection over a window join";
    public static final String PROJECT_TYPE = "projection output type differs from its expression";
    public static final String READ_UNCHECKED = "node reads an expression or column id its checks do not cover";
    public static final String SAMPLE_BY_ALGORITHM = "SAMPLE BY algorithm contradicts its fill";
    public static final String SCAN_SHAPE = "scan source column indexes differ in length from its output";
    public static final String SET_OPERATION_MERGE = "merge facts of a set operation other than UNION ALL";
    public static final String SET_OPERATION_SHAPE = "set operation branches and output differ in column count";
    public static final String SET_OPERATION_SYMBOLS = "set operation symbol columns are not ascending output indexes";
    public static final String SHARED_SOURCE_IDS = "shared source input and source id lists differ in length";
    public static final String SORT_ALGORITHM = "sort algorithm contradicts the sort or its input";
    public static final String SORT_KEYS = "sort has no keys or keys and directions differ in length";
    public static final String SORT_TIMESTAMP = "sort designates a timestamp other than its first key";
    public static final String TIMESTAMP_TYPE = "designated timestamp is not a timestamp column";
    public static final String UNRESOLVED_COLUMN = "column id is not in scope";
    public static final String WINDOW_ALGORITHM = "window streams a window that orders its own rows or needs more than one pass";
    public static final String WINDOW_CALL = "window function is not a window call";
    public static final String WINDOW_JOIN_ALGORITHM = "window join step algorithm contradicts its filter or master";
    public static final String WINDOW_JOIN_SCOPE = "window join step scope differs from its master and slave columns";
    public static final String WINDOW_JOIN_SHAPE = "window join output is not the master columns followed by the step aggregates";
    public static final String WINDOW_OUTPUT = "window output is not input columns followed by its function columns";
    public static final String WINDOW_SHAPE = "window functions, specs and column ids differ in length";
    private final ObjList<BoundExpression> checkedExpressions = new ObjList<>();
    private final IntList checkedReadIds = new IntList();
    private final IntHashSet columnIds;
    private final OutputSchema joinScope;
    private final ObjList<JoinInput> outerInputs;
    private final ExpressionVisitor expressionNodes = this::expressionNode;
    private final ReadEnumerator readEnumerator = new ReadEnumerator();
    private final ObjList<LogicalPlan> visited;
    private OutputSchema aliasScope;
    private BoundExpression expressionRoot;
    private boolean isAccessPathRequired;
    private boolean isDependentStepAllowed;
    private boolean isJoinUnordered;
    private LogicalPlan node;
    private String pass;
    private LogicalPlan root;
    private OutputSchema scope;
    private String site;

    /**
     * Borrows temporary lists from the optimiser: the verifier runs between stages and leaves every structure empty.
     */
    PlanVerifier(ObjList<LogicalPlan> visited, OutputSchema joinScope, IntHashSet columnIds, ObjList<JoinInput> outerInputs) {
        this.visited = visited;
        this.joinScope = joinScope;
        this.columnIds = columnIds;
        this.outerInputs = outerInputs;
    }

    /**
     * A verifier over its own temporary lists, for checking hand-built plans.
     */
    @TestOnly
    public static PlanVerifier newStandalone() {
        return new PlanVerifier(new ObjList<>(), new OutputSchema(), new IntHashSet(), new ObjList<>());
    }

    /**
     * Verifies the plan a pass produced; dependent join steps and outer columns must be gone.
     */
    public boolean verify(LogicalPlan root, String pass) {
        return check(root, pass, false);
    }

    /**
     * Verifies the plan access path planning produced; every scan the generator builds has an access path, and every
     * sort, LIMIT, filter, GROUP BY, LATEST BY over a derived input, join step and window join step the generator
     * builds has the physical choice order planning records for it.
     */
    public boolean verifyAccessPaths(LogicalPlan root) {
        isAccessPathRequired = true;
        try {
            return check(root, "access path planning", false);
        } finally {
            isAccessPathRequired = false;
        }
    }

    /**
     * Verifies the binder's output, where a dependent join step still reads the inputs before it through outer columns.
     */
    public boolean verifyBound(LogicalPlan root) {
        return verifyBound(root, "SqlBinder.bind");
    }

    /**
     * Verifies the output of a pass that runs before decorrelation, where a dependent join step still reads the inputs
     * before it through outer columns.
     */
    public boolean verifyBound(LogicalPlan root, String pass) {
        return check(root, pass, true);
    }

    private static boolean isBounded(SortPlan.Algorithm algorithm) {
        return algorithm == SortPlan.Algorithm.LIMITED || algorithm == SortPlan.Algorithm.PRESORTED_LIMITED || algorithm == SortPlan.Algorithm.LONG_TOP_K
                || algorithm == SortPlan.Algorithm.PARALLEL_FILTERED_TOP_K || algorithm == SortPlan.Algorithm.PARALLEL_TOP_K;
    }

    private static int occurrences(BoundExpression tree, BoundExpression node) {
        if (tree == node) {
            return 1;
        }
        int count = 0;
        if (tree instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                count += occurrences(call.argumentAt(i), node);
            }
        }
        return count;
    }

    private static boolean sameText(CharSequence text, CharSequence other) {
        return text == null ? other == null : other != null && Chars.equals(text, other);
    }

    private void accessPath(ScanPlan scan) {
        scope = scan.getOutput();
        site = "scan access path";
        final int indexColumnId = scan.getIndexColumnId();
        if (indexColumnId >= 0) {
            readColumn(indexColumnId);
        }
        if (isAccessPathRequired && scan.getAccessPath() == null) {
            fail(ACCESS_PATH_UNPLANNED);
        }
        if (scan.getIndexRead() != ScanPlan.IndexRead.NONE && indexColumnId < 0
                || scan.getAccessPath() == null && (indexColumnId >= 0 || scan.getResidual() != null || scan.getIndexKeys().size() > 0)) {
            fail(ACCESS_PATH);
        }
        predicate(scan.getResidual());
        final boolean hasChoice = OrderPlanning.hasResidualFilterChoice(scan);
        if (scan.getResidualAlgorithm() != null && !hasChoice) {
            fail(FILTER_ALGORITHM);
        }
        choice(!hasChoice || scan.getResidualAlgorithm() != null);
        for (int i = 0, n = scan.getIndexKeys().size(); i < n; i++) {
            expression(scan.getIndexKeys().getQuick(i));
        }
        for (int i = 0, n = scan.getExcludedKeys().size(); i < n; i++) {
            expression(scan.getExcludedKeys().getQuick(i));
        }
        if (scan.getKeySubquery() != null) {
            expression(scan.getKeySubquery());
        }
        predicate(scan.getKeyPattern());
        predicate(scan.getWithin());
    }

    private boolean check(LogicalPlan root, String pass, boolean isDependentStepAllowed) {
        this.root = root;
        this.pass = pass;
        this.isDependentStepAllowed = isDependentStepAllowed;
        clear();
        try {
            plan(root);
        } finally {
            clear();
            this.root = null;
            this.pass = null;
            this.isDependentStepAllowed = false;
            aliasScope = null;
            node = null;
            scope = null;
            site = null;
        }
        return true;
    }

    /**
     * Fails access path planning's output when a node lacks the physical choice the generator builds it by.
     */
    private void choice(boolean isRecorded) {
        if (isAccessPathRequired && !isRecorded) {
            fail(CHOICE_UNPLANNED);
        }
    }

    private void clear() {
        columnIds.clear();
        joinScope.clear();
        outerInputs.clear();
        visited.clear();
    }

    private void column(int columnId, int dataType, boolean isCast) {
        final int type = typeOf(columnId);
        if (!isCast && type != dataType) {
            fail(COLUMN_TYPE, columnId);
        }
    }

    private AssertionError error(String invariant, int columnId) {
        final StringBuilder message = new StringBuilder(invariant);
        if (columnId >= 0) {
            message.append(" [column id ").append(columnId).append(']');
        }
        if (site != null) {
            message.append(" in ").append(site);
        }
        message.append(" at ").append(node.getClass().getSimpleName()).append(" position ").append(node.getPosition())
                .append(" after ").append(pass);
        try {
            message.append('\n').append(new LogicalPlanPrinter().print(root));
        } catch (Throwable ignore) {
            // the plan is being reported as broken; a print failure must not hide the invariant
        }
        return new AssertionError(message.toString());
    }

    /**
     * Checks one expression tree. Its nodes are distinct objects: the JIT serializer keys per-position state on
     * node identity. A sub-query cursor is the exception: the instantiator keys sub-query reuse across its positions
     * on its identity. The identity hashes only pre-filter; a repeated hash counts the node's positions exactly.
     */
    private void expression(BoundExpression root) {
        checkedExpressions.add(root);
        expressionRoot = root;
        try {
            if (root == null) {
                fail(EXPRESSION_NULL);
            }
            root.walk(expressionNodes);
        } finally {
            expressionRoot = null;
            columnIds.clear();
        }
    }

    private int expressionNode(BoundExpression expression) {
        if (!(expression instanceof CursorExpression) && !columnIds.add(System.identityHashCode(expression))
                && occurrences(expressionRoot, expression) > 1) {
            fail(EXPRESSION_SHARED);
        }
        switch (expression) {
            case ColumnExpression column -> column(column.getColumnId(), column.getDataType(), column.isCast());
            case OuterColumnExpression outer -> outerColumn(outer);
            case FunctionExpression call -> {
                if (call.getOverload() == null) {
                    fail(FUNCTION_OVERLOAD);
                }
                for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                    if (call.argumentAt(i) == null) {
                        fail(EXPRESSION_NULL);
                    }
                }
            }
            case CursorExpression cursor -> {
                if (cursor.getPlan() == null) {
                    fail(CURSOR_PLAN);
                }
            }
            default -> {
            }
        }
        return TreeWalk.CONTINUE;
    }

    private void expressions(ObjList<? extends BoundExpression> expressions) {
        for (int i = 0, n = expressions.size(); i < n; i++) {
            expression(expressions.getQuick(i));
        }
    }

    private void fail(String invariant) {
        throw error(invariant, -1);
    }

    private void fail(String invariant, int columnId) {
        throw error(invariant, columnId);
    }

    private void fill(FillPlan fill) {
        forwards(fill, FILL_TIMESTAMP);
        unplanned(fill.getAlgorithm());
        if (fill.getAlgorithm() == FillPlan.Algorithm.SAMPLE_BY_ROWS
                && !(fill.getInput() instanceof SampleByPlan sample && sample.getAlgorithm() == SampleByPlan.Algorithm.FILL_NONE)) {
            fail(FILL_ALGORITHM);
        }
        final int count = fill.getTargetColumnIds().size();
        if (fill.getModes().size() != count || fill.getValues().size() != count) {
            fail(FILL_SHAPE);
        }
        scope = fill.getInput().getOutput();
        site = "fill targets";
        resolveAll(fill.getTargetColumnIds());
        site = "fill sources";
        final IntList sources = fill.getSourceColumnIds();
        for (int i = 0, n = sources.size(); i < n; i++) {
            if (sources.getQuick(i) >= 0) {
                readColumn(sources.getQuick(i));
            }
        }
        site = "fill timestamp";
        timestampColumn(fill.getTimestampColumnId());
        for (int i = 0; i < count; i++) {
            noColumnReads(fill.getValues().getQuick(i));
        }
        noColumnReads(fill.getFrom());
        noColumnReads(fill.getTo());
        noColumnReads(fill.getOffset());
        noColumnReads(fill.getTimezone());
    }

    /**
     * The output is what {@link ForwardingPlan#deriveOutput()} lays out: the input's columns, attribute for
     * attribute, under the node's own timestamp designation.
     */
    private void forwards(ForwardingPlan plan, String timestampInvariant) {
        final OutputSchema output = plan.getOutput();
        final OutputSchema input = plan.getInput().getOutput();
        final int n = output.getColumnCount();
        if (n != input.getColumnCount()) {
            fail(OUTPUT_FORWARDING);
        }
        for (int i = 0; i < n; i++) {
            if (output.getColumnId(i) != input.getColumnId(i) || output.getColumnType(i) != input.getColumnType(i)
                    || !Chars.equals(output.getColumnName(i), input.getColumnName(i))
                    || !sameText(output.getColumnQualifier(i), input.getColumnQualifier(i))
                    || output.getMetadata(i) != input.getMetadata(i)
                    || output.isVisible(i) != input.isVisible(i)
                    || output.isSymbolTableStatic(i) != input.isSymbolTableStatic(i)
                    || output.isNameProtected(i) != input.isNameProtected(i)) {
                fail(OUTPUT_FORWARDING, output.getColumnId(i));
            }
        }
        if (output.getTimestampIndex() != plan.derivedTimestampIndex()) {
            fail(timestampInvariant);
        }
    }

    private void grouping(GroupingPlan aggregate) {
        final OutputSchema output = aggregate.getOutput();
        final ObjList<BoundExpression> keys = aggregate.getGroupingExpressions();
        final ObjList<FunctionExpression> values = aggregate.getAggregates();
        final int keyCount = keys.size();
        if (keyCount + values.size() != output.getColumnCount()) {
            fail(AGGREGATE_SHAPE);
        }
        scope = aggregate.getInput().getOutput();
        site = "grouping keys";
        for (int i = 0; i < keyCount; i++) {
            final BoundExpression key = keys.getQuick(i);
            expression(key);
            if (key.getDataType() != output.getColumnType(i)) {
                fail(AGGREGATE_TYPE, output.getColumnId(i));
            }
        }
        site = "aggregates";
        for (int i = 0, n = values.size(); i < n; i++) {
            final FunctionExpression value = values.getQuick(i);
            expression(value);
            if (!value.isAggregate()) {
                fail(AGGREGATE_CALL, output.getColumnId(keyCount + i));
            }
            if (value.getDataType() != output.getColumnType(keyCount + i)) {
                fail(AGGREGATE_TYPE, output.getColumnId(keyCount + i));
            }
        }
        if (aggregate instanceof AggregatePlan plain) {
            if (plain.getSharedInputIds().size() != plain.getSharedSourceIds().size()) {
                fail(SHARED_SOURCE_IDS);
            }
            final AggregatePlan.Algorithm algorithm = plain.getAlgorithm();
            final boolean isHorizon = plain.getInput() instanceof HorizonJoinPlan;
            final boolean isGroupBy = !LogicalPlans.isCount(plain)
                    && !(LogicalPlans.skipFilters(plain.getInput()) instanceof ScanPlan scan && scan.getAccessPath() == ScanPlan.AccessPath.POSTING_DISTINCT);
            choice(!isGroupBy || algorithm != null);
            if (algorithm != null && (!isGroupBy
                    || algorithm == AggregatePlan.Algorithm.VECTORISED && (keyCount != 1 || isHorizon)
                    || algorithm == AggregatePlan.Algorithm.PARALLEL_STOLEN_FILTER
                    && LogicalPlans.firstFilter(isHorizon ? ((HorizonJoinPlan) plain.getInput()).getMaster() : plain.getInput()) == null)) {
                fail(AGGREGATE_ALGORITHM);
            }
        }
        if (aggregate instanceof SampleByPlan sample) {
            final SampleByPlan.Algorithm algorithm = sample.getAlgorithm();
            unplanned(algorithm);
            final ObjList<CharSequence> fill = sample.getFillTokens();
            if (algorithm != null && ((algorithm == SampleByPlan.Algorithm.INTERPOLATE) != (fill.size() == 1 && SqlKeywords.isLinearKeyword(fill.getQuick(0)))
                    || algorithm == SampleByPlan.Algorithm.FIRST_LAST_INDEX && sample.getFillMode() != SampleByPlan.FILL_NONE)) {
                fail(SAMPLE_BY_ALGORITHM);
            }
            site = "SAMPLE BY timestamp";
            timestampColumn(sample.getTimestampColumnId());
            noColumnReads(sample.getPeriod());
            noColumnReads(sample.getFrom());
            noColumnReads(sample.getTo());
            noColumnReads(sample.getOffset());
            noColumnReads(sample.getTimezone());
            for (int i = 0, n = sample.getFillValues().size(); i < n; i++) {
                noColumnReads(sample.getFillValues().getQuick(i));
            }
        }
    }

    private void horizonJoin(HorizonJoinPlan horizon) {
        final OutputSchema master = horizon.getMaster().getOutput();
        final OutputSchema output = horizon.getOutput();
        if (output.getColumnCount() < master.getColumnCount()) {
            fail(HORIZON_JOIN_SHAPE);
        }
        for (int i = 0, n = master.getColumnCount(); i < n; i++) {
            if (output.getColumnId(i) != master.getColumnId(i) || output.getColumnType(i) != master.getColumnType(i)) {
                fail(HORIZON_JOIN_SHAPE, master.getColumnId(i));
            }
        }
        for (int i = 0, n = horizon.getSlaves().size(); i < n; i++) {
            final HorizonJoinSlave slave = horizon.getSlaves().getQuick(i);
            final int keyCount = slave.getMasterKeyColumnIds().size();
            if (slave.getSlaveKeyColumnIds().size() != keyCount || slave.getKeyPositions().size() != keyCount) {
                fail(JOIN_KEYS);
            }
            scope = master;
            site = "master keys";
            resolveAll(slave.getMasterKeyColumnIds());
            scope = slave.getInput().getOutput();
            site = "slave keys";
            resolveAll(slave.getSlaveKeyColumnIds());
        }
    }

    private void join(JoinPlan join) {
        final ObjList<JoinInput> inputs = join.getInputs();
        final ObjList<JoinInput> ordered = join.getOrderedInputs();
        if (ordered.size() > 0) {
            if (ordered.size() != inputs.size()) {
                fail(JOIN_ORDER);
            }
            for (int i = 0, n = inputs.size(); i < n; i++) {
                if (ordered.indexOf(inputs.getQuick(i)) < 0) {
                    fail(JOIN_ORDER);
                }
            }
        }
        for (int i = 1, n = ordered.size(); i < n; i++) {
            final JoinInput step = ordered.getQuick(i);
            final JoinInput.Algorithm algorithm = step.getAlgorithm();
            choice(algorithm != null && (step.getMasterSide() != null
                    || algorithm != JoinInput.Algorithm.HASH && algorithm != JoinInput.Algorithm.LIGHT_HASH));
        }
        final ObjList<JoinInput> steps = ordered.size() > 0 ? ordered : inputs;
        joinScope.clear();
        isJoinUnordered = join.getGraph() != null;
        for (int i = 0, n = steps.size(); i < n; i++) {
            joinStep(steps.getQuick(i));
        }
        if (isJoinUnordered) {
            isJoinUnordered = false;
            scope = joinScope;
            for (int i = 0, n = steps.size(); i < n; i++) {
                stepPredicates(steps.getQuick(i));
            }
            site = "join graph conjuncts";
            final ObjList<BoundExpression> residuals = join.getGraph().getResiduals();
            for (int i = 0, n = residuals.size(); i < n; i++) {
                predicate(residuals.getQuick(i));
            }
            predicate(join.getGraph().getConstantFilter());
            site = "join graph keys";
            final ObjList<JoinDependency> dependencies = join.getGraph().getDependencies();
            for (int i = 0, n = dependencies.size(); i < n; i++) {
                final JoinDependency dependency = dependencies.getQuick(i);
                for (int k = 0, count = dependency == null ? 0 : dependency.getKeys().size(); k < count; k++) {
                    readColumn(dependency.getKeys().getQuick(k).getLeftColumnId());
                    readColumn(dependency.getKeys().getQuick(k).getRightColumnId());
                }
            }
        }
        site = null;
        final OutputSchema output = join.getOutput();
        if (output.getColumnCount() != joinScope.getColumnCount()) {
            fail(JOIN_OUTPUT);
        }
        for (int i = 0, n = joinScope.getColumnCount(); i < n; i++) {
            final int index = output.getColumnIndexById(joinScope.getColumnId(i));
            if (index < 0 || output.getColumnType(index) != joinScope.getColumnType(i)) {
                fail(JOIN_OUTPUT, joinScope.getColumnId(i));
            }
        }
        if (output.getTimestampIndex() >= 0
                && output.getTimestampColumnId() != steps.getQuick(0).getSourceOutput().getTimestampColumnId()) {
            fail(JOIN_TIMESTAMP, output.getTimestampColumnId());
        }
        if (join.getFilterConjunctOrigins().size() != join.getFilterConjuncts().size()) {
            fail(JOIN_FILTER_ORIGINS);
        }
        scope = output;
        site = "filter conjuncts";
        final ObjList<BoundExpression> conjuncts = join.getFilterConjuncts();
        for (int i = 0, n = conjuncts.size(); i < n; i++) {
            expression(conjuncts.getQuick(i));
        }
        joinScope.clear();
    }

    private void joinAlgorithm(JoinInput step) {
        final JoinInput.Algorithm algorithm = step.getAlgorithm();
        if (algorithm == null) {
            if (step.getMasterSide() != null) {
                fail(JOIN_ALGORITHM);
            }
            return;
        }
        final JoinKind joinType = step.getJoinType();
        final boolean isEquiJoin = !joinType.isTemporal() && joinType != JoinKind.UNNEST;
        final boolean isKeyed = step.getMasterKeyColumnIds().size() > 0;
        final boolean isValid = switch (algorithm) {
            case UNNEST -> joinType == JoinKind.UNNEST;
            case TEMPORAL, FULL_FAT_TEMPORAL -> joinType == JoinKind.ASOF || joinType == JoinKind.LT;
            case TEMPORAL_TIME_FRAME ->
                    (joinType == JoinKind.ASOF || joinType == JoinKind.LT) && (step.getHints() & JoinInput.HINT_ASOF_LINEAR) == 0;
            case TEMPORAL_STOLEN_FILTER ->
                    joinType == JoinKind.ASOF && LogicalPlans.firstFilter(step.getInput()) != null;
            case SPLICE, FULL_FAT_SPLICE -> joinType == JoinKind.SPLICE;
            case MARKOUT -> isEquiJoin && step.getMarkoutTimestampColumnId() >= 0;
            case NESTED_LOOP -> isEquiJoin && !isKeyed;
            case HASH, LIGHT_HASH -> isEquiJoin && isKeyed;
        };
        if (!isValid || step.getMasterSide() == JoinInput.MasterSide.SMALLER
                && (algorithm != JoinInput.Algorithm.LIGHT_HASH || joinType != JoinKind.INNER)) {
            fail(JOIN_ALGORITHM);
        }
    }

    private void joinInputs(JoinPlan join) {
        final ObjList<JoinInput> inputs = join.getInputs();
        for (int i = 0, n = inputs.size(); i < n; i++) {
            final JoinInput step = inputs.getQuick(i);
            node = join;
            if ((step.getInput() == null) == (step.getUnnest() == null)) {
                fail(JOIN_INPUT_SOURCE);
            }
            if (step.getInput() == null) {
                continue;
            }
            if (!step.isDependent()) {
                plan(step.getInput());
                continue;
            }
            if (!isDependentStepAllowed) {
                fail(DEPENDENT_STEP);
            }
            final int base = outerInputs.size();
            for (int k = 0; k < i; k++) {
                outerInputs.add(inputs.getQuick(k));
            }
            plan(step.getInput());
            outerInputs.setPos(base);
        }
    }

    private void joinStep(JoinInput step) {
        joinAlgorithm(step);
        final OutputSchema source = step.getSourceOutput();
        final UnnestSpec unnest = step.getUnnest();
        scope = joinScope;
        if (unnest != null) {
            site = "UNNEST expressions";
            expressions(unnest.getExpressions());
            if (unnest.isStandalone()) {
                joinScope.clear();
            }
        }
        final int keyCount = step.getMasterKeyColumnIds().size();
        if (step.getSlaveKeyColumnIds().size() != keyCount || step.getKeyPositions().size() != keyCount
                || step.getMasterKeyNames().size() != keyCount || step.getSlaveKeyNames().size() != keyCount) {
            fail(JOIN_KEYS);
        }
        site = "master keys";
        resolveAll(step.getMasterKeyColumnIds());
        if (step.getMarkoutTimestampColumnId() >= 0) {
            site = "markout timestamp";
            timestampColumn(step.getMarkoutTimestampColumnId());
        }
        scope = source;
        site = "slave keys";
        resolveAll(step.getSlaveKeyColumnIds());
        if (step.getMarkoutSequenceColumnId() >= 0) {
            site = "markout sequence";
            readColumn(step.getMarkoutSequenceColumnId());
        }
        joinScope.addColumnsFrom(source);
        scope = joinScope;
        if (!isJoinUnordered) {
            stepPredicates(step);
        }
        if (step.getOutput().getColumnCount() > 0 && !sameColumns(step.getOutput(), joinScope)) {
            fail(JOIN_SCOPE);
        }
    }

    private void latestBy(LatestByPlan latest) {
        forwards(latest, OUTPUT_TIMESTAMP);
        final LogicalPlan input = latest.getInput();
        final boolean isScanned = (input instanceof FilterPlan filter ? filter.getInput() : input) instanceof ScanPlan;
        if (latest.getAlgorithm() != null && isScanned) {
            fail(LATEST_BY_ALGORITHM);
        }
        choice(isScanned || latest.getAlgorithm() != null);
        scope = input.getOutput();
        site = "LATEST BY keys";
        resolveAll(latest.getKeyColumnIds());
        final int timestampId = latest.getTimestampColumnId();
        site = "LATEST BY timestamp";
        if (timestampId < 0 || scope.getColumnIndexById(timestampId) >= 0) {
            if (timestampId >= 0) {
                timestampColumn(timestampId);
            }
            return;
        }
        // LATEST BY over a table reads the table's designated timestamp whether or not the scan projects it
        checkedReadIds.add(timestampId);
        final LogicalPlan source = input instanceof FilterPlan filter ? filter.getInput() : input;
        if (!(source instanceof ScanPlan scan) || scan.getNativeTimestampColumnId() != timestampId) {
            fail(UNRESOLVED_COLUMN, timestampId);
        }
    }

    private void limit(LimitPlan limit) {
        forwards(limit, OUTPUT_TIMESTAMP);
        final LimitPlan.Application application = limit.getApplication();
        choice(application != null);
        if (application != null) {
            final boolean isBounded = LogicalPlans.hasSortUnderStableProjects(limit.getInput())
                    && isBounded(((SortPlan) LogicalPlans.skipProjects(limit.getInput())).getAlgorithm());
            if ((application == LimitPlan.Application.SORT) != isBounded) {
                fail(LIMIT_APPLICATION);
            }
        }
        if (limit.getLo() == null) {
            fail(EXPRESSION_NULL);
        }
        noColumnReads(limit.getLo());
        noColumnReads(limit.getHi());
    }

    private void noColumnReads(BoundExpression expression) {
        if (expression == null) {
            return;
        }
        if (LogicalPlans.readsColumn(expression)) {
            fail(OPERAND_COLUMN);
        }
        // a correlated LIMIT reads outer columns until decorrelation ranks it
        expression(expression);
    }

    private void outerColumn(OuterColumnExpression outer) {
        if (outerInputs.size() == 0) {
            fail(OUTER_COLUMN_SCOPE, outer.getColumnId());
        }
        for (int i = outerInputs.size() - 1; i >= 0; i--) {
            final OutputSchema source = outerInputs.getQuick(i).getSourceOutput();
            final int index = source.getColumnIndexById(outer.getColumnId());
            if (index >= 0) {
                if (source.getColumnType(index) != outer.getDataType()) {
                    fail(COLUMN_TYPE, outer.getColumnId());
                }
                return;
            }
        }
        fail(OUTER_COLUMN_UNRESOLVED, outer.getColumnId());
    }

    private void plan(LogicalPlan plan) {
        node = plan;
        if (visited.indexOf(plan) >= 0) {
            fail(NODE_SHARED);
        }
        visited.add(plan);
        if (plan instanceof JoinPlan join) {
            joinInputs(join);
        } else {
            for (int i = 0, n = plan.inputCount(); i < n; i++) {
                final LogicalPlan input = plan.inputAt(i);
                if (input == null) {
                    node = plan;
                    fail(INPUT_NULL);
                }
                plan(input);
            }
        }
        node = plan;
        site = null;
        checkedExpressions.clear();
        checkedReadIds.clear();
        schema(plan.getOutput());
        switch (plan) {
            case ScanPlan scan -> {
                if (scan.getSourceColumnIndexes().size() != scan.getOutput().getColumnCount()) {
                    fail(SCAN_SHAPE);
                }
                if (scan.getLimitLo() == null && scan.getLimitHi() != null) {
                    fail(EXPRESSION_NULL);
                }
                site = "scan LIMIT";
                noColumnReads(scan.getLimitLo());
                noColumnReads(scan.getLimitHi());
                requestedOrder(scan.getRequestedOrder(), scan.getOutput());
                accessPath(scan);
            }
            case FunctionSourcePlan _ -> {
            }
            case FilterPlan filter -> {
                forwards(filter, OUTPUT_TIMESTAMP);
                scope = filter.getInput().getOutput();
                site = "filter predicate";
                if (filter.getPredicate() == null) {
                    fail(EXPRESSION_NULL);
                }
                predicate(filter.getPredicate());
                final boolean hasChoice = !LogicalPlans.isFusedFilter(filter) && OrderPlanning.isFiltering(filter.getPredicate());
                if (filter.getAlgorithm() != null && !hasChoice) {
                    fail(FILTER_ALGORITHM);
                }
                choice(!hasChoice || filter.getAlgorithm() != null);
            }
            case ProjectPlan project -> project(project);
            case GroupingPlan grouping -> grouping(grouping);
            case DistinctPlan distinct -> forwards(distinct, OUTPUT_TIMESTAMP);
            case FillPlan fill -> fill(fill);
            case WindowPlan window -> window(window);
            case JoinPlan join -> join(join);
            case WindowJoinPlan windowJoin -> windowJoin(windowJoin);
            case HorizonJoinPlan horizon -> horizonJoin(horizon);
            case LatestByPlan latest -> latestBy(latest);
            case SetOperationPlan operation -> setOperation(operation);
            case SortPlan sort -> sort(sort);
            case LimitPlan limit -> limit(limit);
            default -> {
            }
        }
        site = null;
        reads(plan);
    }

    private void predicate(BoundExpression predicate) {
        if (predicate == null) {
            return;
        }
        expression(predicate);
        if (predicate.getDataType() != ColumnType.BOOLEAN) {
            fail(PREDICATE_TYPE);
        }
        if (LogicalPlans.isConstant(predicate) && !(predicate instanceof ConstantExpression)) {
            fail(PREDICATE_CONSTANT);
        }
    }

    private void project(ProjectPlan project) {
        final OutputSchema output = project.getOutput();
        final ObjList<BoundExpression> expressions = project.getExpressions();
        if (expressions.size() != output.getColumnCount()) {
            fail(PROJECT_SHAPE);
        }
        scope = project.getInput().getOutput();
        // a column may read another column of the same projection by alias
        aliasScope = output;
        for (int i = 0, n = expressions.size(); i < n; i++) {
            final BoundExpression expression = expressions.getQuick(i);
            expression(expression);
            if (!project.hasUpdateConversions() && expression.getDataType() != output.getColumnType(i)) {
                fail(PROJECT_TYPE, output.getColumnId(i));
            }
        }
        aliasScope = null;
        requestedColumn(project.getRequestedOrderColumnId(), output);
        requestedOrder(project.getRequestedOrder(), output);
        if (project.isTimestampDropped() && (!(project.getInput() instanceof WindowJoinPlan) || LogicalPlans.isColumnOnlyProjection(project))) {
            fail(PROJECT_TIMESTAMP_DROP);
        }
    }

    private void readColumn(int columnId) {
        checkedReadIds.add(columnId);
        typeOf(columnId);
    }

    /**
     * Every expression and column id the node hands to {@link LogicalPlan#visitReads} is one its checks covered, and
     * the reverse, so a read a node adds without exposing it fails here.
     */
    private void reads(LogicalPlan plan) {
        readEnumerator.expressionCount = 0;
        readEnumerator.readIds.clear();
        plan.visitReads(readEnumerator);
        if (readEnumerator.expressionCount != checkedExpressions.size()) {
            fail(READ_UNCHECKED);
        }
        final IntList readIds = readEnumerator.readIds;
        if (readIds.size() != checkedReadIds.size()) {
            fail(READ_UNCHECKED);
        }
        readIds.sortGroups(1);
        checkedReadIds.sortGroups(1);
        for (int i = 0, n = readIds.size(); i < n; i++) {
            if (readIds.getQuick(i) != checkedReadIds.getQuick(i)) {
                fail(READ_UNCHECKED, readIds.getQuick(i));
            }
        }
        checkedExpressions.clear();
        checkedReadIds.clear();
        readIds.clear();
    }

    private void requestedColumn(int columnId, OutputSchema output) {
        if (columnId >= 0 && output.getColumnIndexById(columnId) < 0) {
            fail(ORDER_REQUEST, columnId);
        }
    }

    private void requestedOrder(SortKeys order, OutputSchema output) {
        if (order.getColumnIds().size() != order.getDirections().size()) {
            fail(SORT_KEYS);
        }
        for (int i = 0, n = order.size(); i < n; i++) {
            requestedColumn(order.getColumnIds().getQuick(i), output);
        }
    }

    private void resolveAll(IntList columnIds) {
        for (int i = 0, n = columnIds.size(); i < n; i++) {
            readColumn(columnIds.getQuick(i));
        }
    }

    private boolean sameColumns(OutputSchema output, OutputSchema input) {
        final int n = output.getColumnCount();
        if (n != input.getColumnCount()) {
            return false;
        }
        for (int i = 0; i < n; i++) {
            if (output.getColumnId(i) != input.getColumnId(i) || output.getColumnType(i) != input.getColumnType(i)) {
                return false;
            }
        }
        return true;
    }

    private void schema(OutputSchema output) {
        columnIds.clear();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            if (!columnIds.add(output.getColumnId(i))) {
                fail(DUPLICATE_COLUMN_ID, output.getColumnId(i));
            }
        }
        columnIds.clear();
        final int timestampIndex = output.getTimestampIndex();
        if (timestampIndex >= 0 && (timestampIndex >= output.getColumnCount() || !ColumnType.isTimestamp(output.getColumnType(timestampIndex)))) {
            fail(TIMESTAMP_TYPE);
        }
    }

    private void setOperation(SetOperationPlan operation) {
        final int count = operation.getOutput().getColumnCount();
        if (operation.getLeft().getOutput().getColumnCount() != count || operation.getRight().getOutput().getColumnCount() != count) {
            fail(SET_OPERATION_SHAPE);
        }
        final IntList symbols = operation.getSymbolColumns();
        for (int i = 0, n = symbols.size(); i < n; i++) {
            final int index = symbols.getQuick(i);
            if (index < 0 || index >= count || i > 0 && index <= symbols.getQuick(i - 1)) {
                fail(SET_OPERATION_SYMBOLS);
            }
        }
        requestedColumn(operation.getRequestedOrderColumnId(), operation.getOutput());
        final SortPlan.Algorithm rightSort = operation.getRightBranchSort();
        if ((operation.isMerged() || rightSort != null) && operation.getOperation() != SetOperationKind.UNION_ALL
                || rightSort != null && rightSort != SortPlan.Algorithm.LIGHT && rightSort != SortPlan.Algorithm.MATERIALIZED) {
            fail(SET_OPERATION_MERGE);
        }
    }

    private void sort(SortPlan sort) {
        final IntList keys = sort.getColumnIds();
        if (keys.size() == 0 || keys.size() != sort.getDirections().size()) {
            fail(SORT_KEYS);
        }
        scope = sort.getOutput();
        site = "sort keys";
        resolveAll(keys);
        forwards(sort, SORT_TIMESTAMP);
        choice(sort.getAlgorithm() != null);
        if (sort.getAlgorithm() == SortPlan.Algorithm.TIMESTAMP_DECLARATION && !sort.isMarkoutHorizon()
                || isBounded(sort.getAlgorithm()) && !sort.isLimited()
                || sort.getAlgorithm() == SortPlan.Algorithm.PARALLEL_FILTERED_TOP_K && LogicalPlans.firstFilter(sort.getInput()) == null) {
            fail(SORT_ALGORITHM);
        }
    }

    private void stepPredicates(JoinInput step) {
        site = "ON residual";
        predicate(step.getOnResidual());
        site = "post-join filter";
        predicate(step.getPostJoinFilter());
        site = "key filter";
        predicate(step.getKeyFilter());
    }

    private void timestampColumn(int columnId) {
        checkedReadIds.add(columnId);
        if (!ColumnType.isTimestamp(typeOf(columnId))) {
            fail(TIMESTAMP_TYPE, columnId);
        }
    }

    private int typeOf(int columnId) {
        int index = scope.getColumnIndexById(columnId);
        if (index >= 0) {
            return scope.getColumnType(index);
        }
        if (aliasScope != null) {
            index = aliasScope.getColumnIndexById(columnId);
            if (index >= 0) {
                return aliasScope.getColumnType(index);
            }
        }
        throw error(UNRESOLVED_COLUMN, columnId);
    }

    private void unplanned(Enum<?> algorithm) {
        if (isAccessPathRequired && algorithm == null) {
            fail(CHOICE_UNPLANNED);
        }
    }

    private void window(WindowPlan window) {
        final ObjList<FunctionExpression> functions = window.getFunctions();
        final int count = functions.size();
        if (window.getSpecs().size() != count || window.getFunctionColumnIds().size() != count) {
            fail(WINDOW_SHAPE);
        }
        final OutputSchema input = window.getInput().getOutput();
        scope = input;
        for (int i = 0; i < count; i++) {
            final FunctionExpression call = functions.getQuick(i);
            site = "window functions";
            expression(call);
            if (!call.isWindow()) {
                fail(WINDOW_CALL, window.getFunctionColumnIds().getQuick(i));
            }
            final WindowSpec spec = window.getSpecs().getQuick(i);
            site = "PARTITION BY";
            expressions(spec.getPartitionBy());
            if (spec.getOrderByColumnIds().size() != spec.getOrderByDirections().size()) {
                fail(SORT_KEYS);
            }
            site = "window ORDER BY";
            resolveAll(spec.getOrderByColumnIds());
            if (window.getAlgorithm() == WindowPlan.Algorithm.STREAMING && ((call.getFunctionFlags() & BoundExpression.MULTI_PASS) != 0
                    || spec.getOrderByColumnIds().size() > 0 && !spec.isOrderDelivered())) {
                fail(WINDOW_ALGORITHM, window.getFunctionColumnIds().getQuick(i));
            }
        }
        unplanned(window.getAlgorithm());
        site = null;
        final OutputSchema output = window.getOutput();
        final int inputColumns = output.getColumnCount() - count;
        if (inputColumns < 0) {
            fail(WINDOW_OUTPUT);
        }
        for (int i = 0; i < inputColumns; i++) {
            final int index = input.getColumnIndexById(output.getColumnId(i));
            if (index < 0 || input.getColumnType(index) != output.getColumnType(i)) {
                fail(WINDOW_OUTPUT, output.getColumnId(i));
            }
        }
        for (int i = 0; i < count; i++) {
            final int columnId = window.getFunctionColumnIds().getQuick(i);
            if (output.getColumnId(inputColumns + i) != columnId || output.getColumnType(inputColumns + i) != functions.getQuick(i).getDataType()) {
                fail(WINDOW_OUTPUT, columnId);
            }
        }
        requestedOrder(window.getQueryOrder(), output);
    }

    private void windowJoin(WindowJoinPlan windowJoin) {
        final OutputSchema master = windowJoin.getMaster().getOutput();
        final OutputSchema output = windowJoin.getOutput();
        final ObjList<WindowJoinStep> steps = windowJoin.getSteps();
        int outputIndex = master.getColumnCount();
        for (int i = 0; i < outputIndex; i++) {
            if (i >= output.getColumnCount() || output.getColumnId(i) != master.getColumnId(i) || output.getColumnType(i) != master.getColumnType(i)) {
                fail(WINDOW_JOIN_SHAPE, master.getColumnId(i));
            }
        }
        for (int s = 0, m = steps.size(); s < m; s++) {
            final WindowJoinStep step = steps.getQuick(s);
            joinScope.clear();
            joinScope.addColumnsFrom(master);
            for (int t = 0; t < s; t++) {
                final WindowJoinStep earlier = steps.getQuick(t);
                for (int i = 0, n = earlier.getAggregates().size(); i < n; i++) {
                    joinScope.add(earlier.getAggregateColumnIds().getQuick(i), "", earlier.getAggregates().getQuick(i).getDataType(), false);
                }
            }
            if (step.getMasterScope().getColumnCount() > 0 && !sameColumns(step.getMasterScope(), joinScope)) {
                fail(WINDOW_JOIN_SCOPE);
            }
            scope = joinScope;
            site = "window bounds";
            if (step.getLoExpression() != null) {
                expression(step.getLoExpression());
            }
            if (step.getHiExpression() != null) {
                expression(step.getHiExpression());
            }
            joinScope.addColumnsFrom(step.getSlave().getOutput());
            if (step.getScope().getColumnCount() > 0 && !sameColumns(step.getScope(), joinScope)) {
                fail(WINDOW_JOIN_SCOPE);
            }
            site = "window join filter";
            predicate(step.getFilter());
            choice(step.getAlgorithm() != null || step.getFilter() instanceof ConstantExpression constant && constant.getLongValue() == 0);
            if (step.getAlgorithm() != null && step.getFilter() instanceof ConstantExpression constant && constant.getLongValue() == 0
                    || step.getAlgorithm() == WindowJoinStep.Algorithm.PARALLEL_STOLEN_FILTER
                    && (s > 0 || LogicalPlans.firstFilter(windowJoin.getMaster()) == null)) {
                fail(WINDOW_JOIN_ALGORITHM);
            }
            final ObjList<FunctionExpression> aggregates = step.getAggregates();
            if (step.getAggregateColumnIds().size() != aggregates.size()) {
                fail(WINDOW_JOIN_SHAPE);
            }
            for (int i = 0, n = aggregates.size(); i < n; i++) {
                final FunctionExpression call = aggregates.getQuick(i);
                site = "window join aggregates";
                expression(call);
                if (!call.isAggregate()) {
                    fail(AGGREGATE_CALL, step.getAggregateColumnIds().getQuick(i));
                }
                final int columnId = step.getAggregateColumnIds().getQuick(i);
                site = null;
                if (outputIndex >= output.getColumnCount() || output.getColumnId(outputIndex) != columnId
                        || output.getColumnType(outputIndex) != call.getDataType()) {
                    fail(WINDOW_JOIN_SHAPE, columnId);
                }
                outputIndex++;
            }
        }
        site = null;
        if (outputIndex != output.getColumnCount()) {
            fail(WINDOW_JOIN_SHAPE);
        }
        joinScope.clear();
    }

    private final class ReadEnumerator implements PlanExpressionVisitor {
        private final IntList readIds = new IntList();
        private int expressionCount;

        @Override
        public int visitColumnId(int columnId, int position) {
            readIds.add(columnId);
            return columnId;
        }

        @Override
        public BoundExpression visitExpression(BoundExpression expression) {
            boolean isChecked = false;
            for (int i = 0, n = checkedExpressions.size(); i < n && !isChecked; i++) {
                isChecked = checkedExpressions.getQuick(i) == expression;
            }
            if (!isChecked) {
                fail(READ_UNCHECKED);
            }
            expressionCount++;
            return expression;
        }
    }
}
