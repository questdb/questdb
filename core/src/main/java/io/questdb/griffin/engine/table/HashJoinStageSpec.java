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
 * and the post-join filter runs over that row too. A stage with either filter walks its matches on
 * the workers, and so does every stage before it, whose columns the filters may read; see
 * {@link HashJoinStageStepper}. The filters of a later join of a stack compiled against that join's
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
 * workers, so that every stage's lookups profile its own key and probe classes alone.
 */
public final class HashJoinStageSpec implements Plannable, QuietCloseable {
    private final HashJoinInput buildInput;
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
    // True while the stage, the first join of a swap candidate, may still swap its inputs; see
    // AsyncHashJoinSpec.isInputSwapPossible(), which keeps it.
    private boolean isInputSwapPossible;
    private boolean isPayloadCopyable;
    private Function onFilter;
    private Function postFilter;
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
        this.isOuter = isOuter;
        this.condition = condition;
        this.joinMetadata = joinMetadata;
        this.onFilter = onFilter;
        this.workerOnFilters = workerOnFilters;
        payloadColumns.addAll(buildInput.getColumnMap());
    }

    @Override
    public void close() {
        Throwable failure = Misc.freeObjListBestEffort(null, workerOnFilters);
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
     * True when the stage's pairs run a filter on the workers, an ON residual or a post-join filter,
     * so that the workers walk the stage's matches rather than leaving them to the consumer.
     */
    public boolean hasPairFilter() {
        return onFilter != null || postFilter != null;
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

    public boolean isOuter() {
        return isOuter;
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
        assert postFilter == null ? workerPostFilters == null : this.postFilter == null && joinMetadata != null;
        this.postFilter = postFilter;
        this.workerPostFilters = workerPostFilters;
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
}
