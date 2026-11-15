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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.HashJoinKeyLayout;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * One join of a parallel hash join, as the planner compiled it: the build input that the join
 * hashes, its equality keys over the probe and the build input, the join type and the condition
 * that EXPLAIN prints. The stage reads every column of its build input as payload, the key columns
 * included, since the joined row reads them all and a probe key cannot stand in for a build key:
 * SYMBOL dictionaries, TIMESTAMP units and STRING and VARCHAR types may differ between the two.
 * <p>
 * A LEFT join may carry an ON residual, the part of its ON condition beyond the keys, and any join
 * may carry a post-join filter, the WHERE conjuncts that the join's rows make resolvable. Both read
 * the joined row and run on the workers, for each pair of a probe row and one of its matches, with
 * a copy of their own on every worker when they are not thread safe. A probe row of a LEFT join
 * whose pairs all fail the residual yields one row with NULL build columns, as the serial join's,
 * and the post-join filter runs over that row too. A stage whose pairs run either filter in an
 * execution walks its matches on the workers, and so does every stage before it, whose columns the
 * filters may read; see {@link HashJoinStageStepper} and {@link #hasPairFilter(boolean)}. An
 * execution whose build ran a LEFT join's whole residual over the build's rows, see
 * {@link BuildOnFilter}, leaves the stage's pairs no residual, so that the stage hands its chains on,
 * as a stage without a residual does, unless a filter of its own or of a later stage reads its
 * matches. The filters of a later join of a stack compiled against that join's
 * metadata, whose leading columns are those of the stages before it, as the operator's joined row
 * lists them.
 * <p>
 * The first join of a parallel hash join may swap its inputs at execution, as the serial hash join
 * does, see {@link AsyncHashJoinSpec#isSwapCandidate()}: it then hashes its left input and streams
 * its build input. Such a stage also holds its keys with the inputs' roles swapped, its swap key
 * layout, and a stepper of its own for that orientation. A later join of a stack over such a first
 * join reads its keys off the left input's columns, which a swapped execution holds in the first
 * stage's build rather than in the probe scan: the later stage then also holds its keys as they read
 * the first stage's payload, its swap key source layout, whose probe side lays the left input's
 * columns out as the payload does, in the order the left input as compiled lists them.
 * <p>
 * The stage owns its build input, its filters with their worker copies, and its steppers, the
 * hidden-class copies of {@link HashJoinStageStepperTemplate} that probe the stage's build on the
 * workers, so that every stage's lookups profile its own key and probe classes alone. A hidden copy
 * starts cold, so the stage also keeps an instance of the shared template, which every query's
 * stages share the profile of and which the JVM has long compiled: the operator runs those until it
 * has probed enough rows to repay the copies' warm-up; see {@link AsyncHashJoinAtom}.
 * <p>
 * The planner also records which columns of the joined row each filter reads, so that a probe task
 * decodes the probe columns that the filters read before it joins a Parquet frame; see
 * {@link AsyncHashJoinSpec#newProbeTaskColumns(boolean)}. A filter whose columns the planner did not
 * record reads, as far as the join knows, every column.
 * <p>
 * The leading conjuncts of a LEFT join's ON residual that read the build's columns alone may filter
 * the build's rows instead of the pairs; see {@link BuildOnFilter}. EXPLAIN then prints them as
 * {@code buildOnFilter}, against the build scan's columns, as the fused GROUP BY prints its ON
 * condition on the build alone, while {@code onFilter} keeps printing the whole residual, as the
 * serial join prints its filter.
 */
public final class HashJoinStageSpec implements Plannable, QuietCloseable {
    private final HashJoinInput buildInput;
    // The leading conjuncts of the ON residual that an execution may run over the build's rows, and
    // the rest of the residual; null when the residual leads with no conjunct of the build alone.
    @Nullable
    private BuildOnFilter buildOnFilter;
    // The keys as the serial plan's EXPLAIN prints them.
    private final String condition;
    private final boolean isOuter;
    // The joined rows' metadata, which the filters compiled against and print with; borrowed.
    private final RecordMetadata joinMetadata;
    private final HashJoinKeyLayout keyLayout;
    // Every build column, in build order.
    private final IntList payloadColumns = new IntList();
    private final HashJoinStageStepper stepper = HashJoinStageStepper.newInstance();
    // The keys with the inputs' roles swapped, which a first join that may swap its inputs builds its
    // left input by and probes its build input with, and the stepper of that orientation; both null
    // for a stage that never swaps.
    @Nullable
    private final HashJoinKeyLayout swapKeyLayout;
    // The keys of a later join of a stack whose first join may swap its inputs, as a swapped execution
    // reads them: from the first stage's payload, which then holds the left input's rows; null for a
    // stage that never reads its keys there.
    @Nullable
    private final HashJoinKeyLayout swapKeySourceLayout;
    @Nullable
    private final HashJoinStageStepper swapStepper;
    // The shared template's instance that the swapped orientation runs while the operator warms up;
    // null for a stage that never swaps.
    @Nullable
    private final HashJoinStageStepper swapTemplateStepper;
    // The shared template's instance that the query's orientation runs while the operator warms up.
    private final HashJoinStageStepper templateStepper = new HashJoinStageStepperTemplate();
    // True while the stage, the first join of a swap candidate, may still swap its inputs; see
    // AsyncHashJoinSpec.isInputSwapPossible(), which keeps it.
    private boolean isInputSwapPossible;
    private boolean isPayloadCopyable;
    private Function onFilter;
    // The joined row's columns that the ON residual reads; null while unknown.
    @Nullable
    private IntHashSet onFilterColumns;
    private Function postFilter;
    // The joined row's columns that the post-join filter reads; null while unknown.
    @Nullable
    private IntHashSet postFilterColumns;
    private ObjList<Function> workerOnFilters;
    private ObjList<Function> workerPostFilters;

    /**
     * Takes ownership of the build input and of the ON residual with its worker copies. The key
     * layout borrows the metadata of both inputs to make key sinks, so construct the join's runtime
     * stages while the inputs are open.
     *
     * @param buildInput      the input the stage hashes
     * @param keyLayout       the equality keys over the probe and the build input
     * @param isOuter         true for a LEFT OUTER join, false for an INNER one
     * @param condition       the keys as EXPLAIN prints them
     * @param joinMetadata    the joined rows' metadata, which the ON residual and the post-join
     *                        filter compile against; borrowed, null without either filter
     * @param onFilter        a LEFT join's ON residual, null without one
     * @param workerOnFilters each worker's copy of the ON residual, null when it is thread safe or
     *                        absent
     */
    public HashJoinStageSpec(
            HashJoinInput buildInput,
            HashJoinKeyLayout keyLayout,
            boolean isOuter,
            String condition,
            @Nullable RecordMetadata joinMetadata,
            @Nullable Function onFilter,
            @Nullable ObjList<Function> workerOnFilters
    ) {
        this(buildInput, keyLayout, null, null, isOuter, condition, joinMetadata, onFilter, workerOnFilters);
    }

    /**
     * A later join of a stack, as
     * {@link #HashJoinStageSpec(HashJoinInput, HashJoinKeyLayout, boolean, String, RecordMetadata, Function, ObjList)}
     * takes its arguments, with its keys as an execution that swaps the first join's inputs reads
     * them too.
     *
     * @param swapKeySourceLayout the equality keys with the probe side read from the first stage's
     *                            payload, which holds the left input's columns in the order the left
     *                            input as compiled lists them; null for a stack whose first join never
     *                            swaps its inputs
     */
    public HashJoinStageSpec(
            HashJoinInput buildInput,
            HashJoinKeyLayout keyLayout,
            boolean isOuter,
            String condition,
            @Nullable RecordMetadata joinMetadata,
            @Nullable Function onFilter,
            @Nullable ObjList<Function> workerOnFilters,
            @Nullable HashJoinKeyLayout swapKeySourceLayout
    ) {
        this(buildInput, keyLayout, null, swapKeySourceLayout, isOuter, condition, joinMetadata, onFilter, workerOnFilters);
    }

    /**
     * The first join of a parallel hash join that may swap its inputs at execution, as
     * {@link #HashJoinStageSpec(HashJoinInput, HashJoinKeyLayout, boolean, String, RecordMetadata, Function, ObjList)}
     * takes its arguments, with its keys in the swapped orientation too.
     *
     * @param swapKeyLayout the equality keys with the probe on the build input and the build on the
     *                      join's left input; null for a stage that never swaps. Only an INNER join
     *                      swaps
     */
    public HashJoinStageSpec(
            HashJoinInput buildInput,
            HashJoinKeyLayout keyLayout,
            @Nullable HashJoinKeyLayout swapKeyLayout,
            boolean isOuter,
            String condition,
            @Nullable RecordMetadata joinMetadata,
            @Nullable Function onFilter,
            @Nullable ObjList<Function> workerOnFilters
    ) {
        this(buildInput, keyLayout, swapKeyLayout, null, isOuter, condition, joinMetadata, onFilter, workerOnFilters);
    }

    private HashJoinStageSpec(
            HashJoinInput buildInput,
            HashJoinKeyLayout keyLayout,
            @Nullable HashJoinKeyLayout swapKeyLayout,
            @Nullable HashJoinKeyLayout swapKeySourceLayout,
            boolean isOuter,
            String condition,
            @Nullable RecordMetadata joinMetadata,
            @Nullable Function onFilter,
            @Nullable ObjList<Function> workerOnFilters
    ) {
        assert onFilter == null || isOuter : "an INNER join has no ON residual";
        assert onFilter == null || joinMetadata != null;
        assert swapKeyLayout == null || !isOuter : "only an INNER join swaps its inputs";
        assert swapKeyLayout == null || swapKeySourceLayout == null : "the first join reads its keys off its inputs";
        this.buildInput = buildInput;
        this.keyLayout = keyLayout;
        this.swapKeyLayout = swapKeyLayout;
        this.swapKeySourceLayout = swapKeySourceLayout;
        this.swapStepper = swapKeyLayout != null ? HashJoinStageStepper.newInstance() : null;
        this.swapTemplateStepper = swapKeyLayout != null ? new HashJoinStageStepperTemplate() : null;
        this.isOuter = isOuter;
        this.condition = condition;
        this.joinMetadata = joinMetadata;
        this.onFilter = onFilter;
        this.workerOnFilters = workerOnFilters;
        payloadColumns.addAll(buildInput.getColumnMap());
    }

    /**
     * Adds to the sink the joined row's columns that the stage's ON residual and post-join filter
     * read, by their index in the joined rows' metadata, and returns true; returns false, and leaves
     * the sink in no defined state, when the stage has a filter whose columns the planner did not
     * record.
     */
    public boolean addFilterColumns(IntHashSet sink) {
        if (onFilter != null) {
            if (onFilterColumns == null) {
                return false;
            }
            sink.addAll(onFilterColumns);
        }
        if (postFilter != null) {
            if (postFilterColumns == null) {
                return false;
            }
            sink.addAll(postFilterColumns);
        }
        return true;
    }

    @Override
    public void close() {
        Throwable failure = Misc.freeBestEffort(null, buildOnFilter);
        buildOnFilter = null;
        failure = Misc.freeObjListBestEffort(failure, workerOnFilters);
        workerOnFilters = null;
        failure = Misc.freeBestEffort(failure, onFilter);
        onFilter = null;
        failure = Misc.freeObjListBestEffort(failure, workerPostFilters);
        workerPostFilters = null;
        failure = Misc.freeBestEffort(failure, postFilter);
        postFilter = null;
        failure = Misc.freeBestEffort(failure, buildInput);
        CairoException.rethrowCleanupFailure(failure);
    }

    public HashJoinInput getBuildInput() {
        return buildInput;
    }

    /**
     * The leading conjuncts of the ON residual that read the build's columns alone, compiled against
     * the build scan's columns, which an execution may run over the build's rows; see
     * {@link BuildOnFilter}. Null without such conjuncts.
     */
    @Nullable
    public Function getBuildOnFilter() {
        return buildOnFilter != null ? buildOnFilter.filter : null;
    }

    public HashJoinKeyLayout getKeyLayout() {
        return keyLayout;
    }

    /**
     * The ON residual that a slot runs: the slot's worker copy, or the owner's residual for the
     * owner's slot, -1, and for a residual without worker copies; null without a residual.
     */
    @Nullable
    public Function getOnFilter(int slotId) {
        return slotId < 0 || workerOnFilters == null ? onFilter : workerOnFilters.getQuick(slotId);
    }

    /**
     * The build columns that the joined row reads, in the order the joined row reads them: every
     * column of the build input as compiled, as the frame columns that hold them.
     */
    public IntList getPayloadColumns() {
        return payloadColumns;
    }

    /**
     * The post-join filter that a slot runs, as {@link #getOnFilter(int)} picks the residual; null
     * without a post-join filter.
     */
    @Nullable
    public Function getPostFilter(int slotId) {
        return slotId < 0 || workerPostFilters == null ? postFilter : workerPostFilters.getQuick(slotId);
    }

    /**
     * The part of the ON residual that a slot runs over the pairs of an execution whose build ran
     * {@link #getBuildOnFilter()} over its rows: the conjuncts after the leading ones of the build
     * alone, picked per slot as {@link #getOnFilter(int)} picks the residual; null when those
     * conjuncts make up the whole residual, or when the stage has none of them.
     */
    @Nullable
    public Function getRemainingOnFilter(int slotId) {
        if (buildOnFilter == null) {
            return null;
        }
        final ObjList<Function> workerRemainders = buildOnFilter.workerRemainders;
        return slotId < 0 || workerRemainders == null ? buildOnFilter.remainder : workerRemainders.getQuick(slotId);
    }

    /**
     * True when the stage's pairs run a filter on the workers in an execution, so that the workers
     * walk the stage's matches rather than leaving them to the consumer: the post-join filter, or the
     * ON residual, or, in an execution whose build ran {@link #getBuildOnFilter()} over its rows, the
     * rest of the residual, which is null when those conjuncts make up the whole residual.
     *
     * @param isBuildOnFilterApplied true when the execution's build ran the build ON filter over its
     *                               rows; see {@link HashJoinBuildStage#isBuildOnFilterApplied()}
     */
    public boolean hasPairFilter(boolean isBuildOnFilterApplied) {
        assert !isBuildOnFilterApplied || buildOnFilter != null;
        if (postFilter != null) {
            return true;
        }
        return isBuildOnFilterApplied ? buildOnFilter.remainder != null : onFilter != null;
    }

    public boolean hasPostFilter() {
        return postFilter != null;
    }

    /**
     * The stage's keys with the inputs' roles swapped, for a first join that may swap its inputs;
     * null for a stage that never swaps.
     */
    @Nullable
    public HashJoinKeyLayout getSwapKeyLayout() {
        return swapKeyLayout;
    }

    /**
     * A later join's keys as an execution that swaps the first join's inputs reads them: from the
     * first stage's payload, which then holds the left input's rows; null for a stage that never reads
     * its keys there.
     */
    @Nullable
    public HashJoinKeyLayout getSwapKeySourceLayout() {
        return swapKeySourceLayout;
    }

    /**
     * True when the leading conjuncts of the ON residual that read the build alone run over the
     * build's rows only in an execution whose build table holds no more rows than the probe table,
     * since one of them can cost far more on a build row than the hash insert it spares; see
     * {@link BuildOnFilter}. False for a stage without such conjuncts.
     */
    public boolean isBuildOnFilterSized() {
        return buildOnFilter != null && buildOnFilter.isSized;
    }

    public boolean isOuter() {
        return isOuter;
    }

    /**
     * Takes the leading conjuncts of the ON residual that read the build alone, with the rest of the
     * residual and the worker copies of both, which the planner compiled; see {@link BuildOnFilter}.
     * The stage frees them from here on, with its other filters.
     */
    public void ofBuildOnFilter(BuildOnFilter buildOnFilter) {
        assert this.buildOnFilter == null && onFilter != null && isOuter;
        this.buildOnFilter = buildOnFilter;
    }

    /**
     * Takes the post-join filter and its worker copies, which the planner compiled against the
     * joined rows' metadata, or, given nulls, lets go of the ones it took without freeing them, so
     * that a caller whose rebuild of the join failed frees them itself.
     *
     * @param postFilter        the WHERE conjuncts that the stage's joined rows make resolvable
     * @param workerPostFilters each worker's copy of the filter, null when it is thread safe
     */
    public void ofPostFilter(@Nullable Function postFilter, @Nullable ObjList<Function> workerPostFilters) {
        ofPostFilter(postFilter, workerPostFilters, null);
    }

    /**
     * Takes the post-join filter and its worker copies as {@link #ofPostFilter(Function, ObjList)}
     * does, with the joined row's columns that the filter reads.
     *
     * @param postFilterColumns the joined row's columns that the filter reads, by their index in the
     *                          joined rows' metadata; null when unknown
     */
    public void ofPostFilter(
            @Nullable Function postFilter,
            @Nullable ObjList<Function> workerPostFilters,
            @Nullable IntHashSet postFilterColumns
    ) {
        assert postFilter == null ? workerPostFilters == null : this.postFilter == null && joinMetadata != null;
        this.postFilter = postFilter;
        this.workerPostFilters = workerPostFilters;
        this.postFilterColumns = postFilter != null ? postFilterColumns : null;
    }

    /**
     * Records the joined row's columns that the ON residual reads, by their index in the joined rows'
     * metadata, which the planner collects from the residual's expression.
     */
    public void setOnFilterColumns(@Nullable IntHashSet onFilterColumns) {
        this.onFilterColumns = onFilterColumns;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Build");
        sink.attr("joinType").val(isOuter ? "left outer" : "inner");
        sink.attr("condition").val(condition);
        // The plan shows the inputs in the query's roles, and a first join that may still swap them
        // at execution the rule it swaps by.
        if (isInputSwapPossible) {
            sink.attr("inputSwap").val("when the left input is smaller");
        }
        if (keyLayout.getSymbolKeyProbeColumns().size() > 0) {
            sink.attr("symbolKeyJoin").val(true);
        }
        // The copy is chosen per execution, from the build's and the probe's row counts, so the plan
        // names the rule rather than the outcome.
        if (isPayloadCopyable) {
            sink.attr("buildPayload").val("copied when the probe is large enough");
        }
        final RecordCursorFactory buildFactory = buildInput.getFactory();
        // The ON residual and the post-join filter read the joined row, as the serial join's filter
        // and the filter over it do, and print its columns. The build scan's stolen filter reads the
        // scan's columns. A compiled filter runs as JIT code, so its attribute says so; the
        // operator's own name speaks for the probe filter only.
        final AsyncFilterContext buildFilterContext = buildInput.getFilterContext();
        final RecordMetadata callerMetadata = sink.getMetadata();
        try {
            sink.setMetadata(joinMetadata);
            sink.optAttr("onFilter", onFilter);
            sink.optAttr("postJoinFilter", postFilter);
            sink.setMetadata(buildFactory.getMetadata());
            sink.optAttr(buildInput.isFilterCompiled() ? "buildJitFilter" : "buildFilter", buildFilterContext.getFilter(-1));
            // The leading conjuncts of the residual that read the build alone, against the build
            // scan's columns, and, when an execution runs them over the build's rows only for a build
            // table no larger than the probe table, the rule by which it decides.
            if (buildOnFilter != null) {
                sink.attr("buildOnFilter").val(buildOnFilter.filter);
                if (buildOnFilter.isSized) {
                    sink.attr("buildOnFilterPushdown").val("when the build table holds no more rows than the probe table");
                }
            }
        } finally {
            sink.setMetadata(callerMetadata);
        }
        sink.child(buildFactory);
    }

    HashJoinStageStepper getStepper() {
        return stepper;
    }

    // The stepper of the swapped orientation; null for a stage that never swaps.
    @Nullable
    HashJoinStageStepper getSwapStepper() {
        return swapStepper;
    }

    // The shared template's instance of the swapped orientation; null for a stage that never swaps.
    @Nullable
    HashJoinStageStepper getSwapTemplateStepper() {
        return swapTemplateStepper;
    }

    // The shared template's instance of the query's orientation.
    HashJoinStageStepper getTemplateStepper() {
        return templateStepper;
    }

    // Each worker's copy of the build ON filter, null when it is thread safe or absent.
    @Nullable
    ObjList<Function> getWorkerBuildOnFilters() {
        return buildOnFilter != null ? buildOnFilter.workerFilters : null;
    }

    // Each worker's copy of the ON residual, null when the residual is thread safe or absent.
    @Nullable
    ObjList<Function> getWorkerOnFilters() {
        return workerOnFilters;
    }

    // Each worker's copy of the post-join filter, null when the filter is thread safe or absent.
    @Nullable
    ObjList<Function> getWorkerPostFilters() {
        return workerPostFilters;
    }

    // Each worker's copy of the rest of the residual beyond the build ON filter, null when it is
    // thread safe or absent.
    @Nullable
    ObjList<Function> getWorkerRemainingOnFilters() {
        return buildOnFilter != null ? buildOnFilter.workerRemainders : null;
    }

    /**
     * Records whether the stage, the first join of a swap candidate, may still swap its inputs, as
     * EXPLAIN reports; see {@link AsyncHashJoinSpec#isInputSwapPossible()}.
     */
    void setInputSwapPossible(boolean isInputSwapPossible) {
        assert !isInputSwapPossible || swapKeyLayout != null;
        this.isInputSwapPossible = isInputSwapPossible;
    }

    /**
     * Records whether some execution may copy the build's payload columns, as EXPLAIN reports; the
     * join's runtime stage knows it from the payload's types.
     */
    void setPayloadCopyable(boolean isPayloadCopyable) {
        this.isPayloadCopyable = isPayloadCopyable;
    }

    /**
     * The leading conjuncts of a LEFT join's ON residual that read the build's columns alone, which
     * an execution runs over the build's rows rather than over the pairs, so that a build row that
     * fails them never enters the build, and the rest of the residual, which the pairs run then. A
     * probe row whose matches the conjuncts all drop has no match left, and reads NULLs, as the
     * serial join's row whose pairs all fail the residual.
     * <p>
     * The conjuncts are the residual's AND chain in the order the residual runs them, from its first
     * conjunct to the last before the first that reads anything else, and only those: the serial join
     * runs the chain over each pair from left to right, stops at the first conjunct that fails, and
     * fails the query at the first that throws. So no conjunct of a pair whose build row a leading
     * conjunct fails runs in the serial join after that one, and dropping the row spares nothing
     * that it would have run; a conjunct of the build alone after one that reads the probe row could
     * drop a row whose pairs throw in that earlier conjunct.
     * <p>
     * The serial join never runs the conjuncts over a build row without a match, though, so a build
     * whose conjuncts throw on one of its rows gives them up for the execution: it keeps that row
     * and every later one of its frame, and its later frames run no conjunct, while the pairs run the
     * whole residual, which throws for exactly the pairs whose serial join throws. A row that the
     * conjuncts dropped before failed one of them, without a throw, so its pairs fail the whole
     * residual too.
     * <p>
     * An execution whose build ran conjuncts that make up the whole residual leaves the pairs no
     * residual: a pair passes it exactly when its build row did, and the build kept those rows alone,
     * in its scan's order. The stage then hands its chains on, as a stage without a residual does,
     * unless its post-join filter or a later stage's filter reads its matches, and any other execution
     * of the same factory walks them; see {@link HashJoinStageSpec#hasPairFilter(boolean)}.
     * <p>
     * The conjuncts run over every build row, where the serial join runs them over the pairs alone,
     * at most one per probe row for a build of unique keys, so a conjunct that can cost far more on a
     * row than the hash insert it spares, by the fused GROUP BY's rule, see
     * {@link io.questdb.griffin.HashJoinGroupByCandidate}, sizes the pushdown: an execution runs the
     * conjuncts over the build's rows only when the build table holds no more rows than the probe
     * table, as the open scans' table readers count them, the same sizes that decide whether the
     * build's key table is presized, see {@link HashJoinBuildStage#isKeyCapacityPresizedFor}, and
     * otherwise runs the whole residual over the pairs. A scan without a table, such as
     * {@code read_parquet()}, fails the rule. The decision never changes a result, so a factory that
     * runs again after the tables changed size decides again, with no recompile.
     * <p>
     * Takes ownership of what it is given, and frees it on close.
     */
    public static final class BuildOnFilter implements QuietCloseable {
        // The leading conjuncts, compiled against the build scan's columns.
        private final Function filter;
        private final boolean isSized;
        // The rest of the residual, compiled against the joined rows' metadata; null when the leading
        // conjuncts make up the whole residual.
        @Nullable
        private Function remainder;
        // Each worker's copy of the leading conjuncts and of the rest; null when thread safe or absent.
        @Nullable
        private ObjList<Function> workerFilters;
        @Nullable
        private ObjList<Function> workerRemainders;

        /**
         * @param filter  the leading conjuncts of the residual that read the build alone, compiled
         *                against the build scan's columns
         * @param isSized true when one of the conjuncts can cost far more on a build row than the
         *                hash insert it spares, so that an execution runs them over the build's rows
         *                only for a build table no larger than the probe table
         */
        public BuildOnFilter(Function filter, boolean isSized) {
            this.filter = filter;
            this.isSized = isSized;
        }

        @Override
        public void close() {
            Throwable failure = Misc.freeObjListBestEffort(null, workerFilters);
            workerFilters = null;
            failure = Misc.freeBestEffort(failure, filter);
            failure = Misc.freeObjListBestEffort(failure, workerRemainders);
            workerRemainders = null;
            failure = Misc.freeBestEffort(failure, remainder);
            remainder = null;
            CairoException.rethrowCleanupFailure(failure);
        }

        /**
         * Takes the rest of the residual, after the leading conjuncts, compiled against the joined
         * rows' metadata; null when the conjuncts make up the whole residual.
         */
        public void ofRemainder(@Nullable Function remainder) {
            assert this.remainder == null;
            this.remainder = remainder;
        }

        /**
         * Takes each worker's copy of the leading conjuncts; null when they are thread safe.
         */
        public void ofWorkerFilters(@Nullable ObjList<Function> workerFilters) {
            assert this.workerFilters == null;
            this.workerFilters = workerFilters;
        }

        /**
         * Takes each worker's copy of the rest of the residual; null when it is thread safe or absent.
         */
        public void ofWorkerRemainders(@Nullable ObjList<Function> workerRemainders) {
            assert this.workerRemainders == null && (workerRemainders == null || remainder != null);
            this.workerRemainders = workerRemainders;
        }
    }
}
