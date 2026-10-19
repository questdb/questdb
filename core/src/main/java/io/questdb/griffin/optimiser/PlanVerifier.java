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
import io.questdb.griffin.plan.logical.JoinKey;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LatestByPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.LogicalPlanPrinter;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.PlanExpressionVisitor;
import io.questdb.griffin.plan.logical.PlanToFactoryRules;
import io.questdb.griffin.plan.logical.PlanVisitor;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.SampleByPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
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
 * Checks the invariants of a logical plan tree after binding, after each optimisation pass and after access path
 * planning, when {@code PLAN_PARANOIA_MODE} is on: its structure, its column scopes and the reads each node exposes
 * here, the physical choices operator planning records through {@link PhysicalChoiceVerifier}, and decorrelation's
 * output through {@link DecorrelationVerifier}. A violation fails with the invariant, the node and the stage that
 * produced the plan.
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
    public static final String DECORRELATION_CARRIER = "decorrelation carrier reads a column that a step matching per outer row null-extends";
    public static final String DECORRELATION_CARRIER_UNRESOLVED = "decorrelation carrier is a column of neither its step nor a step before it";
    public static final String DECORRELATION_STEP_LOST = "join step that does not commute is missing from the plan after decorrelation";
    public static final String DEPENDENT_STEP = "dependent join step survives decorrelation";
    public static final String DUPLICATE_COLUMN_ID = "output lists a column id twice";
    public static final String EXPRESSION_NULL = "expression is null";
    public static final String EXPRESSION_SHARED = "expression node appears twice in one expression tree";
    public static final String FILL_ALGORITHM = "fill algorithm contradicts whether its input is a SAMPLE BY cursor without a fill";
    public static final String FILL_SHAPE = "fill lists differ in length";
    public static final String FILL_TIMESTAMP = "fill does not designate its bucket timestamp";
    public static final String FILTER_ALGORITHM = "filter algorithm contradicts the filter the generator builds";
    public static final String FUNCTION_OVERLOAD = "function call has no overload";
    public static final String HORIZON_JOIN_SHAPE = "horizon join output does not start with the master columns";
    public static final String INPUT_NULL = "input is null";
    public static final String INPUT_RANDOM_ACCESS = "physical choice reads an input by row id that lacks random access";
    public static final String JOIN_ALGORITHM = "join step algorithm contradicts its kind, keys or hints";
    public static final String JOIN_FILTER_ORIGINS = "join filter conjuncts and their origins differ in length";
    public static final String JOIN_INPUT_SOURCE = "join input has neither a plan nor an UNNEST, or both";
    public static final String JOIN_ON_KEPT = "join step that does not commute lost an ON condition fact in decorrelation";
    public static final String JOIN_ORDER = "ordered join inputs are not a permutation of the inputs";
    public static final String JOIN_OUTPUT = "join output does not list every input column exactly once";
    public static final String JOIN_SCOPE = "join step scope differs from its ordered prefix";
    public static final String JOIN_TIMESTAMP = "join designated timestamp is not the one its steps designate";
    public static final String LATEST_BY_ALGORITHM = "LATEST BY over a table scan has an algorithm for a derived input";
    public static final String LIMIT_APPLICATION = "LIMIT application contradicts the sort or the scan under it";
    public static final String NODE_SHARED = "node is reached through two paths";
    public static final String OPERAND_COLUMN = "LIMIT, SAMPLE BY or FILL operand reads a column";
    public static final String ORDER_KEYS = "order keys and directions differ in length";
    public static final String ORDER_REQUEST = "requested order names a column the node does not output";
    public static final String OUTER_COLUMN_SCOPE = "outer column outside a dependent join step";
    public static final String OUTER_COLUMN_UNRESOLVED = "outer column is not a column of a preceding join input";
    public static final String OUTPUT_FORWARDING = "output does not forward the input columns";
    public static final String OUTPUT_TIMESTAMP = "designated timestamp differs from the input's";
    public static final String PREDICATE_CONSTANT = "constant predicate is not folded to a literal";
    public static final String PREDICATE_TYPE = "predicate is not BOOLEAN";
    public static final String PROJECT_COUNTS = "projection column reference counts are stale";
    public static final String PROJECT_SHAPE = "projection expression count differs from its output";
    public static final String PROJECT_TIMESTAMP_DROP = "projection drops the timestamp outside a computing projection over a window join";
    public static final String PROJECT_TYPE = "projection output type differs from its expression";
    public static final String READ_UNCHECKED = "node reads an expression or column id its checks do not cover";
    public static final String SAMPLE_BY_ALGORITHM = "SAMPLE BY algorithm contradicts its fill";
    public static final String SCAN_LIMIT = "scan has a LIMIT hi without a lo";
    public static final String SCAN_SHAPE = "scan source column indexes differ in length from its output";
    public static final String SET_OPERATION_MERGE = "merge facts of a set operation other than UNION ALL";
    public static final String SET_OPERATION_SHAPE = "set operation branches and output differ in column count";
    public static final String SET_OPERATION_SYMBOLS = "set operation symbol columns are not ascending output indexes";
    public static final String SHARED_READ = "decorrelation domain reads its input in a way its shared source does not serve";
    public static final String SHARED_SOURCE_IDS = "shared source input and source id lists differ in length";
    public static final String SHARED_SOURCE_UNRECORDED = "decorrelation domain shares a join input the statement does not record";
    public static final String SORT_ALGORITHM = "sort algorithm contradicts the sort or its input";
    public static final String SORT_KEYS = "sort has no keys or keys and directions differ in length";
    public static final String SORT_TIMESTAMP = "sort designates a timestamp other than its first key";
    public static final String TIMESTAMP_TYPE = "designated timestamp is not a timestamp column";
    public static final String UNRESOLVED_COLUMN = "column id is not in scope";
    public static final String WINDOW_ALGORITHM = "window algorithm contradicts whether every window function streams";
    public static final String WINDOW_CALL = "window function is not a window call";
    public static final String WINDOW_JOIN_ALGORITHM = "window join step algorithm contradicts its filter or master";
    public static final String WINDOW_JOIN_SCOPE = "window join step scope differs from its master and slave columns";
    public static final String WINDOW_JOIN_SHAPE = "window join output is not the master columns followed by the step aggregates";
    public static final String WINDOW_OUTPUT = "window output is not input columns followed by its function columns";
    public static final String WINDOW_SHAPE = "window functions, specs and column ids differ in length";
    private final ObjList<BoundExpression> checkedExpressions;
    private final IntList checkedReadIds;
    private final IntHashSet columnIds;
    private final DecorrelationVerifier decorrelation;
    private final IntHashSet expressionIdentities;
    private final OutputSchema joinScope;
    private final OccurrenceCounter occurrences = new OccurrenceCounter();
    private final ObjList<JoinInput> outerInputs;
    private final IntList readIds = new IntList();
    private final ReadEnumerator readEnumerator = new ReadEnumerator();
    private final ObjList<LogicalPlan> visited;
    private int aliasCount;
    private OutputSchema aliasScope;
    private BoundExpression expressionRoot;
    private OutputSchema expressionScope;
    private boolean isAccessPathRequired;
    private boolean isDependentStepAllowed;
    private LogicalPlan node;
    private String pass;
    private LogicalPlan root;
    private String site;
    private final ExpressionVisitor expressionNodes = this::expressionNode;
    private final PlanVisitor sharedSources = this::sharedSource;

    /**
     * Borrows temporary lists from the optimiser: the verifier runs between stages and leaves every structure empty.
     */
    PlanVerifier(
            ObjList<LogicalPlan> visited,
            OutputSchema joinScope,
            IntHashSet columnIds,
            ObjList<JoinInput> outerInputs,
            ObjList<BoundExpression> checkedExpressions,
            IntList checkedReadIds
    ) {
        this.visited = visited;
        this.joinScope = joinScope;
        this.columnIds = columnIds;
        this.expressionIdentities = columnIds;
        this.outerInputs = outerInputs;
        this.checkedExpressions = checkedExpressions;
        this.checkedReadIds = checkedReadIds;
        this.decorrelation = new DecorrelationVerifier(outerInputs, checkedReadIds, readIds);
    }

    /**
     * A verifier over its own temporary lists, for checking hand-built plans.
     */
    @TestOnly
    public static PlanVerifier newStandalone() {
        return new PlanVerifier(new ObjList<>(), new OutputSchema(), new IntHashSet(), new ObjList<>(), new ObjList<>(), new IntList());
    }

    /**
     * Records the ON condition facts of every join step of the plan that does not commute, which
     * {@link #verifyDecorrelation} compares decorrelation's output with.
     */
    public void recordBarrierJoins(LogicalPlan root) {
        decorrelation.record(root);
    }

    /**
     * Verifies the plan a pass produced; dependent join steps and outer columns must be gone.
     */
    public void verify(LogicalPlan root, String pass) {
        check(root, pass, false);
    }

    /**
     * Verifies the plan access path planning produced; every scan the generator builds has an access path, and every
     * sort, LIMIT, filter, GROUP BY, LATEST BY over a derived input, join step and window join step the generator
     * builds has the physical choice operator planning records for it.
     */
    public void verifyAccessPaths(LogicalPlan root) {
        isAccessPathRequired = true;
        try {
            check(root, "access path planning", false);
        } finally {
            isAccessPathRequired = false;
        }
    }

    /**
     * Verifies the binder's output, where a dependent join step still reads the inputs before it through outer columns.
     */
    public void verifyBound(LogicalPlan root) {
        verifyBound(root, "SqlBinder.bind");
    }

    /**
     * Verifies the output of a pass that runs before decorrelation, where a dependent join step still reads the inputs
     * before it through outer columns.
     */
    public void verifyBound(LogicalPlan root, String pass) {
        check(root, pass, true);
    }

    /**
     * Verifies decorrelation's output against the facts {@link #recordBarrierJoins} recorded, see
     * {@link DecorrelationVerifier}.
     */
    public void verifyDecorrelation(LogicalPlan root, String pass, DecorrelationCarriers carriers) {
        decorrelation.verify(root, pass, carriers);
    }

    /**
     * Verifies that the plan has no decorrelation domain that re-reads a join input when its statement records none,
     * {@code hasSharedSources}: operator planning counts the consumers of shared sources only for a statement that
     * records one, see {@link OperatorPlanning#countSharedConsumers}.
     */
    public void verifySharedSources(LogicalPlan root, boolean hasSharedSources) {
        if (hasSharedSources) {
            return;
        }
        this.root = root;
        pass = "access path planning";
        try {
            root.walkTopDown(sharedSources);
        } finally {
            this.root = null;
            pass = null;
            node = null;
        }
    }

    private static boolean sameColumns(OutputSchema output, OutputSchema input) {
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

    private static boolean sameText(CharSequence text, CharSequence other) {
        return text == null ? other == null : other != null && Chars.equals(text, other);
    }

    private void accessPath(ScanPlan scan) {
        final OutputSchema output = scan.getOutput();
        site = "scan access path";
        final int indexColumnId = scan.getIndexColumnId();
        if (indexColumnId >= 0) {
            readColumn(output, indexColumnId);
        }
        predicate(output, scan.getResidual());
        for (int i = 0, n = scan.getIndexKeys().size(); i < n; i++) {
            expression(output, scan.getIndexKeys().getQuick(i));
        }
        for (int i = 0, n = scan.getExcludedKeys().size(); i < n; i++) {
            expression(output, scan.getExcludedKeys().getQuick(i));
        }
        if (scan.getKeySubquery() != null) {
            expression(output, scan.getKeySubquery());
        }
        predicate(output, scan.getKeyPattern());
        predicate(output, scan.getWithin());
        failIfViolated(PhysicalChoiceVerifier.scan(scan, isAccessPathRequired));
    }

    private void check(LogicalPlan root, String pass, boolean isDependentStepAllowed) {
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
            node = null;
            site = null;
        }
    }

    private void clear() {
        columnIds.clear();
        joinScope.clear();
        outerInputs.clear();
        visited.clear();
    }

    private void column(int columnId, int dataType, boolean isCast) {
        int index = expressionScope.getColumnIndexById(columnId);
        final int type;
        if (index >= 0) {
            type = expressionScope.getColumnType(index);
        } else {
            index = aliasScope == null ? -1 : aliasScope.getColumnIndexById(columnId);
            if (index < 0 || index >= aliasCount) {
                fail(UNRESOLVED_COLUMN, columnId);
            }
            type = aliasScope.getColumnType(index);
        }
        if (!isCast && type != dataType) {
            fail(COLUMN_TYPE, columnId);
        }
    }

    private void expression(OutputSchema scope, BoundExpression root) {
        expression(scope, null, 0, root);
    }

    /**
     * Checks one expression tree, which reads the columns of {@code scope} and, by alias, the first
     * {@code aliasCount} columns of {@code aliases}. Its nodes are distinct objects: the JIT serializer keys
     * per-position state on node identity. A sub-query cursor is the exception: the instantiator keys sub-query reuse
     * across its positions on its identity. The identity hashes only pre-filter; a repeated hash counts the node's
     * positions exactly.
     */
    private void expression(OutputSchema scope, OutputSchema aliases, int aliasCount, BoundExpression root) {
        checkedExpressions.add(root);
        if (root == null) {
            fail(EXPRESSION_NULL);
        }
        expressionRoot = root;
        expressionScope = scope;
        aliasScope = aliases;
        this.aliasCount = aliasCount;
        try {
            root.walk(expressionNodes);
        } finally {
            expressionRoot = null;
            expressionScope = null;
            aliasScope = null;
            this.aliasCount = 0;
            expressionIdentities.clear();
        }
    }

    private int expressionNode(BoundExpression expression) {
        if (!(expression instanceof CursorExpression) && !expressionIdentities.add(System.identityHashCode(expression))
                && occurrences.count(expressionRoot, expression) > 1) {
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

    private void expressions(OutputSchema scope, ObjList<? extends BoundExpression> expressions) {
        for (int i = 0, n = expressions.size(); i < n; i++) {
            expression(scope, expressions.getQuick(i));
        }
    }

    private void fail(String invariant) {
        throw failure(invariant, -1, site, node, pass, root);
    }

    private void fail(String invariant, int columnId) {
        throw failure(invariant, columnId, site, node, pass, root);
    }

    private void failIfViolated(String invariant) {
        if (invariant != null) {
            fail(invariant);
        }
    }

    private void fill(FillPlan fill) {
        forwards(fill, FILL_TIMESTAMP);
        failIfViolated(PhysicalChoiceVerifier.fill(fill, isAccessPathRequired));
        final int count = fill.getTargetColumnIds().size();
        if (fill.getModes().size() != count || fill.getValues().size() != count) {
            fail(FILL_SHAPE);
        }
        final OutputSchema input = fill.getChild().getOutput();
        site = "fill targets";
        resolveAll(input, fill.getTargetColumnIds());
        site = "fill sources";
        final IntList sources = fill.getSourceColumnIds();
        for (int i = 0, n = sources.size(); i < n; i++) {
            if (sources.getQuick(i) >= 0) {
                readColumn(input, sources.getQuick(i));
            }
        }
        site = "fill timestamp";
        timestampColumn(input, fill.getTimestampColumnId());
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
        final OutputSchema input = plan.getChild().getOutput();
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
        final OutputSchema input = aggregate.getChild().getOutput();
        site = "grouping keys";
        for (int i = 0; i < keyCount; i++) {
            final BoundExpression key = keys.getQuick(i);
            expression(input, key);
            if (key.getDataType() != output.getColumnType(i)) {
                fail(AGGREGATE_TYPE, output.getColumnId(i));
            }
        }
        site = "aggregates";
        for (int i = 0, n = values.size(); i < n; i++) {
            final FunctionExpression value = values.getQuick(i);
            expression(input, value);
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
            failIfViolated(PhysicalChoiceVerifier.aggregate(plain, isAccessPathRequired));
        }
        if (aggregate instanceof SampleByPlan sample) {
            failIfViolated(PhysicalChoiceVerifier.sampleBy(sample, isAccessPathRequired));
            site = "SAMPLE BY timestamp";
            timestampColumn(input, sample.getTimestampColumnId());
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
            masterKeys(master, slave.getKeys());
            slaveKeys(slave.getInput().getOutput(), slave.getKeys());
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
        final ObjList<JoinInput> steps = ordered.size() > 0 ? ordered : inputs;
        final boolean isUnordered = join.getGraph() != null;
        joinScope.clear();
        for (int i = 0, n = steps.size(); i < n; i++) {
            final JoinInput step = steps.getQuick(i);
            failIfViolated(PhysicalChoiceVerifier.joinStep(join, ordered.size() > 0 ? i : -1, step, isAccessPathRequired));
            joinStep(step, isUnordered);
        }
        if (isUnordered) {
            for (int i = 0, n = steps.size(); i < n; i++) {
                stepPredicates(steps.getQuick(i));
            }
            site = "join graph conjuncts";
            final ObjList<BoundExpression> residuals = join.getGraph().getResiduals();
            for (int i = 0, n = residuals.size(); i < n; i++) {
                predicate(joinScope, residuals.getQuick(i));
            }
            predicate(joinScope, join.getGraph().getConstantFilter());
            site = "join graph keys";
            final ObjList<JoinDependency> dependencies = join.getGraph().getDependencies();
            for (int i = 0, n = dependencies.size(); i < n; i++) {
                final JoinDependency dependency = dependencies.getQuick(i);
                for (int k = 0, count = dependency == null ? 0 : dependency.getKeys().size(); k < count; k++) {
                    readColumn(joinScope, dependency.getKeys().getQuick(k).getLeftColumnId());
                    readColumn(joinScope, dependency.getKeys().getQuick(k).getRightColumnId());
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
        if (output.getTimestampColumnId() != LogicalPlans.joinTimestampColumnId(join, steps.size())) {
            fail(JOIN_TIMESTAMP, output.getTimestampColumnId());
        }
        if (join.getFilterConjunctOrigins().size() != join.getFilterConjuncts().size()) {
            fail(JOIN_FILTER_ORIGINS);
        }
        site = "filter conjuncts";
        expressions(output, join.getFilterConjuncts());
        joinScope.clear();
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

    private void joinStep(JoinInput step, boolean isUnordered) {
        final OutputSchema source = step.getSourceOutput();
        final UnnestSpec unnest = step.getUnnest();
        if (unnest != null) {
            site = "UNNEST expressions";
            expressions(joinScope, unnest.getExpressions());
            if (unnest.isStandalone()) {
                joinScope.clear();
            }
        }
        masterKeys(joinScope, step.getKeys());
        if (step.getMarkoutTimestampColumnId() >= 0) {
            site = "markout timestamp";
            timestampColumn(joinScope, step.getMarkoutTimestampColumnId());
        }
        slaveKeys(source, step.getKeys());
        if (step.getMarkoutSequenceColumnId() >= 0) {
            site = "markout sequence";
            readColumn(source, step.getMarkoutSequenceColumnId());
        }
        joinScope.addColumnsFrom(source);
        if (!isUnordered) {
            stepPredicates(step);
        }
        if (step.getOutput().getColumnCount() > 0 && !sameColumns(step.getOutput(), joinScope)) {
            fail(JOIN_SCOPE);
        }
    }

    private void latestBy(LatestByPlan latest) {
        forwards(latest, OUTPUT_TIMESTAMP);
        failIfViolated(PhysicalChoiceVerifier.latestBy(latest, isAccessPathRequired));
        final OutputSchema input = latest.getChild().getOutput();
        site = "LATEST BY keys";
        resolveAll(input, latest.getKeyColumnIds());
        final int timestampId = latest.getTimestampColumnId();
        site = "LATEST BY timestamp";
        if (timestampId < 0 || input.getColumnIndexById(timestampId) >= 0) {
            if (timestampId >= 0) {
                timestampColumn(input, timestampId);
            }
            return;
        }
        // LATEST BY over a table reads the table's designated timestamp whether or not the scan projects it
        checkedReadIds.add(timestampId);
        final ScanPlan scan = PlanToFactoryRules.latestByScan(latest);
        if (scan == null || scan.getNativeTimestampColumnId() != timestampId) {
            fail(UNRESOLVED_COLUMN, timestampId);
        }
    }

    private void limit(LimitPlan limit) {
        forwards(limit, OUTPUT_TIMESTAMP);
        if (limit.getLo() == null) {
            fail(EXPRESSION_NULL);
        }
        noColumnReads(limit.getLo());
        noColumnReads(limit.getHi());
        failIfViolated(PhysicalChoiceVerifier.limit(limit, isAccessPathRequired));
    }

    private void masterKeys(OutputSchema scope, ObjList<JoinKey> keys) {
        site = "master keys";
        for (int i = 0, n = keys.size(); i < n; i++) {
            readColumn(scope, keys.getQuick(i).getMasterColumnId());
        }
    }

    private void noColumnReads(BoundExpression expression) {
        if (expression == null) {
            return;
        }
        if (LogicalPlans.readsColumn(expression)) {
            fail(OPERAND_COLUMN);
        }
        // a correlated LIMIT reads outer columns until decorrelation ranks it
        expression(null, expression);
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
            case ScanPlan scan -> scan(scan);
            case FunctionSourcePlan _ -> {
            }
            case FilterPlan filter -> {
                forwards(filter, OUTPUT_TIMESTAMP);
                site = "filter predicate";
                if (filter.getPredicate() == null) {
                    fail(EXPRESSION_NULL);
                }
                predicate(filter.getChild().getOutput(), filter.getPredicate());
                failIfViolated(PhysicalChoiceVerifier.filter(filter, isAccessPathRequired));
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

    private void predicate(OutputSchema scope, BoundExpression predicate) {
        if (predicate == null) {
            return;
        }
        expression(scope, predicate);
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
        final OutputSchema input = project.getChild().getOutput();
        final boolean hasUpdateConversions = project.hasUpdateConversions();
        // a column may read an earlier column of the same projection by alias, unless the projection converts UPDATE columns
        for (int i = 0, n = expressions.size(); i < n; i++) {
            final BoundExpression expression = expressions.getQuick(i);
            expression(input, output, hasUpdateConversions ? 0 : i, expression);
            if (!hasUpdateConversions && expression.getDataType() != output.getColumnType(i)) {
                fail(PROJECT_TYPE, output.getColumnId(i));
            }
        }
        requestedColumn(project.getRequestedOrderColumnId(), output);
        requestedOrder(project.getRequestedOrder(), output);
        final int distinctColumnCount = project.getDistinctColumnCount();
        if (distinctColumnCount >= 0) {
            final int distinctWindowColumnCount = project.getDistinctWindowColumnCount();
            project.deriveColumnReferenceCounts(columnIds);
            if (project.getDistinctColumnCount() != distinctColumnCount || project.getDistinctWindowColumnCount() != distinctWindowColumnCount) {
                fail(PROJECT_COUNTS);
            }
        }
        if (project.isTimestampDropped() && !PlanToFactoryRules.isWindowJoinTimestampDroppable(project)) {
            fail(PROJECT_TIMESTAMP_DROP);
        }
    }

    private void readColumn(OutputSchema scope, int columnId) {
        checkedReadIds.add(columnId);
        typeOf(scope, columnId);
    }

    /**
     * Every expression and column id the node hands to {@link LogicalPlan#visitReads} is one its checks covered, and
     * the reverse, so a read a node adds without exposing it fails here.
     */
    private void reads(LogicalPlan plan) {
        readEnumerator.expressionCount = 0;
        readIds.clear();
        plan.visitReads(readEnumerator);
        if (readEnumerator.expressionCount != checkedExpressions.size()) {
            fail(READ_UNCHECKED);
        }
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
            fail(ORDER_KEYS);
        }
        for (int i = 0, n = order.size(); i < n; i++) {
            requestedColumn(order.getColumnIds().getQuick(i), output);
        }
    }

    private void resolveAll(OutputSchema scope, IntList columnIds) {
        for (int i = 0, n = columnIds.size(); i < n; i++) {
            readColumn(scope, columnIds.getQuick(i));
        }
    }

    private void scan(ScanPlan scan) {
        if (scan.getSourceColumnIndexes().size() != scan.getOutput().getColumnCount()) {
            fail(SCAN_SHAPE);
        }
        if (scan.getLimitLo() == null && scan.getLimitHi() != null) {
            fail(SCAN_LIMIT);
        }
        site = "scan LIMIT";
        noColumnReads(scan.getLimitLo());
        noColumnReads(scan.getLimitHi());
        requestedOrder(scan.getRequestedOrder(), scan.getOutput());
        accessPath(scan);
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
        failIfViolated(PhysicalChoiceVerifier.setOperation(operation, isAccessPathRequired));
    }

    private int sharedSource(LogicalPlan plan) {
        if (plan instanceof AggregatePlan aggregate && aggregate.getSharedSource() != null) {
            node = plan;
            fail(SHARED_SOURCE_UNRECORDED);
        }
        return TreeWalk.CONTINUE;
    }

    private void slaveKeys(OutputSchema scope, ObjList<JoinKey> keys) {
        site = "slave keys";
        for (int i = 0, n = keys.size(); i < n; i++) {
            readColumn(scope, keys.getQuick(i).getSlaveColumnId());
        }
    }

    private void sort(SortPlan sort) {
        final IntList keys = sort.getColumnIds();
        if (keys.size() == 0 || keys.size() != sort.getDirections().size()) {
            fail(SORT_KEYS);
        }
        site = "sort keys";
        resolveAll(sort.getOutput(), keys);
        forwards(sort, SORT_TIMESTAMP);
        failIfViolated(PhysicalChoiceVerifier.sort(sort, isAccessPathRequired));
    }

    private void stepPredicates(JoinInput step) {
        site = "ON residual";
        predicate(joinScope, step.getOnResidual());
        site = "post-join filter";
        predicate(joinScope, step.getPostJoinFilter());
        site = "key filter";
        predicate(joinScope, step.getKeyFilter());
    }

    private void timestampColumn(OutputSchema scope, int columnId) {
        checkedReadIds.add(columnId);
        if (!ColumnType.isTimestamp(typeOf(scope, columnId))) {
            fail(TIMESTAMP_TYPE, columnId);
        }
    }

    private int typeOf(OutputSchema scope, int columnId) {
        final int index = scope.getColumnIndexById(columnId);
        if (index < 0) {
            fail(UNRESOLVED_COLUMN, columnId);
        }
        return scope.getColumnType(index);
    }

    private void window(WindowPlan window) {
        final ObjList<FunctionExpression> functions = window.getFunctions();
        final int count = functions.size();
        if (window.getSpecs().size() != count || window.getFunctionColumnIds().size() != count) {
            fail(WINDOW_SHAPE);
        }
        final OutputSchema input = window.getChild().getOutput();
        for (int i = 0; i < count; i++) {
            final FunctionExpression call = functions.getQuick(i);
            site = "window functions";
            expression(input, call);
            if (!call.isWindow()) {
                fail(WINDOW_CALL, window.getFunctionColumnIds().getQuick(i));
            }
            final WindowSpec spec = window.getSpecs().getQuick(i);
            site = "PARTITION BY";
            expressions(input, spec.getPartitionBy());
            if (spec.getOrderByColumnIds().size() != spec.getOrderByDirections().size()) {
                fail(ORDER_KEYS);
            }
            site = "window ORDER BY";
            resolveAll(input, spec.getOrderByColumnIds());
        }
        site = null;
        failIfViolated(PhysicalChoiceVerifier.window(window, isAccessPathRequired));
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
            site = "window bounds";
            if (step.getLoExpression() != null) {
                expression(joinScope, step.getLoExpression());
            }
            if (step.getHiExpression() != null) {
                expression(joinScope, step.getHiExpression());
            }
            joinScope.addColumnsFrom(step.getSlave().getOutput());
            if (step.getScope().getColumnCount() > 0 && !sameColumns(step.getScope(), joinScope)) {
                fail(WINDOW_JOIN_SCOPE);
            }
            site = "window join filter";
            predicate(joinScope, step.getFilter());
            failIfViolated(PhysicalChoiceVerifier.windowJoinStep(windowJoin, s, step, isAccessPathRequired));
            final ObjList<FunctionExpression> aggregates = step.getAggregates();
            if (step.getAggregateColumnIds().size() != aggregates.size()) {
                fail(WINDOW_JOIN_SHAPE);
            }
            for (int i = 0, n = aggregates.size(); i < n; i++) {
                final FunctionExpression call = aggregates.getQuick(i);
                site = "window join aggregates";
                expression(joinScope, call);
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

    /**
     * The failure of an invariant at {@code node}, with the plan {@code root} printed after it.
     */
    static AssertionError failure(String invariant, int columnId, String site, LogicalPlan node, String pass, LogicalPlan root) {
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

    private static final class OccurrenceCounter implements ExpressionVisitor {
        private int matchCount;
        private BoundExpression target;

        @Override
        public int visit(BoundExpression expression) {
            if (expression == target) {
                matchCount++;
                return TreeWalk.SKIP_CHILDREN;
            }
            return TreeWalk.CONTINUE;
        }

        private int count(BoundExpression tree, BoundExpression target) {
            this.target = target;
            matchCount = 0;
            tree.walk(this);
            this.target = null;
            return matchCount;
        }
    }

    private final class ReadEnumerator implements PlanExpressionVisitor {
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
