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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.HashJoinKeyLayout;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.std.IntList;
import io.questdb.std.QuietCloseable;

/**
 * One join of a parallel hash join, as the planner compiled it: the build input that the join
 * hashes, its equality keys over the probe and the build input, the join type and the condition
 * that EXPLAIN prints. The stage reads every column of its build input as payload, the key columns
 * included, since the joined row reads them all and a probe key cannot stand in for a build key:
 * SYMBOL dictionaries, TIMESTAMP units and STRING and VARCHAR types may differ between the two.
 * <p>
 * The stage owns its build input, and its stepper, the hidden-class copy of
 * {@link HashJoinStageStepperTemplate} that probes the stage's build on the workers, so that every
 * stage's lookups profile its own key and probe classes alone.
 */
public final class HashJoinStageSpec implements Plannable, QuietCloseable {
    private final HashJoinInput buildInput;
    // The keys as the serial plan's EXPLAIN prints them.
    private final String condition;
    private final boolean isOuter;
    private final HashJoinKeyLayout keyLayout;
    // Every build column, in build order.
    private final IntList payloadColumns = new IntList();
    private final HashJoinStageStepper stepper = HashJoinStageStepper.newInstance();
    private boolean isPayloadCopyable;

    /**
     * Takes ownership of the build input. The key layout borrows the metadata of both inputs to make
     * key sinks, so construct the join's runtime stages while the inputs are open.
     *
     * @param buildInput the input the stage hashes
     * @param keyLayout  the equality keys over the probe and the build input
     * @param isOuter    true for a LEFT OUTER join, false for an INNER one
     * @param condition  the keys as EXPLAIN prints them
     */
    public HashJoinStageSpec(HashJoinInput buildInput, HashJoinKeyLayout keyLayout, boolean isOuter, String condition) {
        this.buildInput = buildInput;
        this.keyLayout = keyLayout;
        this.isOuter = isOuter;
        this.condition = condition;
        payloadColumns.addAll(buildInput.getColumnMap());
    }

    @Override
    public void close() {
        buildInput.close();
    }

    public HashJoinInput getBuildInput() {
        return buildInput;
    }

    public HashJoinKeyLayout getKeyLayout() {
        return keyLayout;
    }

    /**
     * The build columns that the joined row reads, in the order the joined row reads them: every
     * column of the build input as compiled, as the frame columns that hold them.
     */
    public IntList getPayloadColumns() {
        return payloadColumns;
    }

    public boolean isOuter() {
        return isOuter;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Build");
        sink.attr("joinType").val(isOuter ? "left outer" : "inner");
        sink.attr("condition").val(condition);
        if (keyLayout.getSymbolKeyProbeColumns().size() > 0) {
            sink.attr("symbolKeyJoin").val(true);
        }
        // The copy is chosen per execution, from the build's and the probe's row counts, so the plan
        // names the rule rather than the outcome.
        if (isPayloadCopyable) {
            sink.attr("buildPayload").val("copied when the probe is large enough");
        }
        final RecordCursorFactory buildFactory = buildInput.getFactory();
        // The build scan's stolen filter reads the scan's columns. A compiled filter runs as JIT
        // code, so its attribute says so; the operator's own name speaks for the probe filter only.
        final AsyncFilterContext buildFilterContext = buildInput.getFilterContext();
        final RecordMetadata callerMetadata = sink.getMetadata();
        sink.setMetadata(buildFactory.getMetadata());
        try {
            sink.optAttr(buildInput.isFilterCompiled() ? "buildJitFilter" : "buildFilter", buildFilterContext.getFilter(-1));
        } finally {
            sink.setMetadata(callerMetadata);
        }
        sink.child(buildFactory);
    }

    HashJoinStageStepper getStepper() {
        return stepper;
    }

    /**
     * Records whether some execution may copy the build's payload columns, as EXPLAIN reports; the
     * join's runtime stage knows it from the payload's types.
     */
    void setPayloadCopyable(boolean isPayloadCopyable) {
        this.isPayloadCopyable = isPayloadCopyable;
    }
}
