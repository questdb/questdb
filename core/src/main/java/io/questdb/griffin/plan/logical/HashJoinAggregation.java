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

package io.questdb.griffin.plan.logical;

import io.questdb.griffin.HashJoinGroupByKeys;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjectFactory;

/**
 * How the GROUP BY of an aggregate fuses into the two-input hash join under it, which operator planning records with
 * {@link AggregatePlan.Algorithm#HASH_JOIN}: which input builds, how the generator reads each input, the join keys,
 * the aggregate over the columns of the inputs, the build columns the joined rows copy, and where each filter runs.
 * The build applies an outer join's ON condition on the build alone, the joined rows pass the post-join filter, and
 * the gate, a runtime constant the ordinary plan evaluates once over the join, decides per execution whether the
 * inputs serve their rows. Every expression reads the columns of the frame sources, the inputs the generator builds
 * the fused operator over: {@link #getProbeInput()} and {@link #getBuildInput()}.
 */
public final class HashJoinAggregation implements Mutable {
    public static final ObjectFactory<HashJoinAggregation> FACTORY = HashJoinAggregation::new;
    private final HashJoinGroupByKeys keys = new HashJoinGroupByKeys();
    private final IntList requiredBuildColumns = new IntList();
    private AggregatePlan aggregate;
    private HashJoinAggregation alternate;
    private CharSequence buildAlias;
    private LogicalPlan buildInput;
    private BoundExpression buildOnFilter;
    private OutputSchema buildOutput;
    private InputRead buildRead;
    private JoinInput.BuildSide buildSide;
    private BoundExpression gate;
    private boolean isKeyCapacityPresized;
    private JoinKind joinType;
    private BoundExpression postJoinFilter;
    private CharSequence probeAlias;
    private LogicalPlan probeInput;
    private OutputSchema probeOutput;
    private InputRead probeRead;

    /**
     * The output of a frame source: a filter forwards the columns of the scan under it.
     */
    public static OutputSchema frameOutput(LogicalPlan source) {
        return source instanceof FilterPlan filter ? filter.getChild().getOutput() : source.getOutput();
    }

    @Override
    public void clear() {
        keys.clear();
        requiredBuildColumns.clear();
        aggregate = null;
        alternate = null;
        buildAlias = null;
        buildInput = null;
        buildOnFilter = null;
        buildOutput = null;
        buildRead = null;
        buildSide = null;
        gate = null;
        isKeyCapacityPresized = false;
        joinType = null;
        postJoinFilter = null;
        probeAlias = null;
        probeInput = null;
        probeOutput = null;
        probeRead = null;
    }

    /**
     * The keys and aggregates of the fused GROUP BY over the columns of the frame sources, under the output of the
     * aggregate it replaces.
     */
    public AggregatePlan getAggregate() {
        return aggregate;
    }

    /**
     * The same fusion building the other input of an INNER join, which each execution picks when the interval scan of
     * an input leaves the other input with fewer rows; null when the build is fixed.
     */
    public HashJoinAggregation getAlternate() {
        return alternate;
    }

    public CharSequence getBuildAlias() {
        return buildAlias;
    }

    /**
     * The frame source of the input the operator builds: a table scan, or the filter over it.
     */
    public LogicalPlan getBuildInput() {
        return buildInput;
    }

    /**
     * The ON condition of an outer join on the build alone, which the build applies to its rows; null for none.
     */
    public BoundExpression getBuildOnFilter() {
        return buildOnFilter;
    }

    public OutputSchema getBuildOutput() {
        return buildOutput;
    }

    public InputRead getBuildRead() {
        return buildRead;
    }

    public JoinInput.BuildSide getBuildSide() {
        return buildSide;
    }

    /**
     * The runtime constant that gates both inputs once per execution, as the ordinary plan gates the join; null for
     * none.
     */
    public BoundExpression getGate() {
        return gate;
    }

    public JoinKind getJoinType() {
        return joinType;
    }

    /**
     * The equality keys, whose probe and build columns are columns of the probe and build frame sources.
     */
    public HashJoinGroupByKeys getKeys() {
        return keys;
    }

    /**
     * The filter the joined rows pass before they aggregate; null for none.
     */
    public BoundExpression getPostJoinFilter() {
        return postJoinFilter;
    }

    public CharSequence getProbeAlias() {
        return probeAlias;
    }

    /**
     * The frame source of the input the operator probes: a table scan, or the filter over it.
     */
    public LogicalPlan getProbeInput() {
        return probeInput;
    }

    public OutputSchema getProbeOutput() {
        return probeOutput;
    }

    public InputRead getProbeRead() {
        return probeRead;
    }

    /**
     * The ids of the build columns the joined rows read, which the build copies with its rows.
     */
    public IntList getRequiredBuildColumns() {
        return requiredBuildColumns;
    }

    /**
     * Whether the build may size its key table by its row count: the build table holds no more rows than the probe
     * table.
     */
    public boolean isKeyCapacityPresized() {
        return isKeyCapacityPresized;
    }

    public HashJoinAggregation of(
            AggregatePlan aggregate,
            JoinKind joinType,
            JoinInput.BuildSide buildSide,
            LogicalPlan probeInput,
            InputRead probeRead,
            CharSequence probeAlias,
            LogicalPlan buildInput,
            InputRead buildRead,
            CharSequence buildAlias
    ) {
        this.aggregate = aggregate;
        this.joinType = joinType;
        this.buildSide = buildSide;
        this.probeInput = probeInput;
        this.probeRead = probeRead;
        this.probeAlias = probeAlias;
        this.probeOutput = frameOutput(probeInput);
        this.buildInput = buildInput;
        this.buildRead = buildRead;
        this.buildAlias = buildAlias;
        this.buildOutput = frameOutput(buildInput);
        return this;
    }

    public void setAlternate(HashJoinAggregation alternate) {
        this.alternate = alternate;
    }

    public void setFilters(BoundExpression buildOnFilter, BoundExpression postJoinFilter, boolean isKeyCapacityPresized) {
        this.buildOnFilter = buildOnFilter;
        this.postJoinFilter = postJoinFilter;
        this.isKeyCapacityPresized = isKeyCapacityPresized;
    }

    public void setGate(BoundExpression gate) {
        this.gate = gate;
    }

    /**
     * How the generator reads the page frames of a frame source.
     */
    public enum InputRead {
        /**
         * Reads the page frames of the factory it builds for the source: a table scan, under a folded or gating
         * filter.
         */
        PAGE_FRAMES,
        /**
         * Steals the parallel filter of the source and runs it over the page frames of the scan under it.
         */
        STOLEN_FILTER
    }
}
