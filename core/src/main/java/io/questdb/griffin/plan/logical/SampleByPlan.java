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

import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;

/**
 * A SAMPLE BY grouping the SAMPLE BY cursor executes: the bucketed timestamp and the keys on the grid the grouping
 * records, with the range and fill the clause states. The cursor reads the grid's time zone and offset itself.
 */
public final class SampleByPlan extends GroupingPlan {
    public static final ObjectFactory<SampleByPlan> FACTORY = SampleByPlan::new;
    private final ObjList<FillMode> fillModes = new ObjList<>();
    private final ObjList<BoundExpression> fillValues = new ObjList<>();
    private Algorithm algorithm;
    private BoundExpression bucket;
    private BoundExpression from;
    private boolean isBaseCopied;
    private boolean isFillInput;
    private boolean isJoinInput;
    private boolean isTimestampRequired = true;
    private AggregatePlan.Algorithm preAggregation;
    private BoundExpression to;

    @Override
    public void clear() {
        super.clear();
        fillModes.clear();
        fillValues.clear();
        algorithm = null;
        bucket = null;
        from = null;
        isBaseCopied = false;
        isFillInput = false;
        isJoinInput = false;
        isTimestampRequired = true;
        preAggregation = null;
        to = null;
    }

    /**
     * The SAMPLE BY factory operator planning records for the node; null before planning.
     */
    public Algorithm getAlgorithm() {
        checkPhysicalRead("SampleByPlan.algorithm");
        return algorithm;
    }

    /**
     * The bucket a stream GROUP BY under a FILL(LINEAR) groups the rows by after the keys, see
     * {@link #getPreAggregation()}: timestamp_floor of the stride over the timestamp, which the binder binds where the
     * clause samples by a literal stride of a fixed width on the UTC calendar at no offset, without FROM or TO, so the
     * floor puts every row in the bucket the interpolation puts it in; null otherwise.
     */
    public BoundExpression getBucket() {
        return bucket;
    }

    /**
     * The fill of each aggregate a FILL with a {@link FillMode#LINEAR} value states, which the SAMPLE BY performs
     * itself; empty when a fill node above fills its buckets or nothing does.
     */
    public ObjList<FillMode> getFillModes() {
        return fillModes;
    }

    /**
     * The bound constant of each FILL value, aligned with {@link #getFillModes()}; null for a keyword.
     */
    public ObjList<BoundExpression> getFillValues() {
        return fillValues;
    }

    public BoundExpression getFrom() {
        return from;
    }

    /**
     * How the stream GROUP BY that aggregates the buckets of a FILL(LINEAR) runs, which operator planning records when
     * one does, see {@link #getBucket()}: serially, or in parallel over the page frames of the base, stealing the filter
     * over them or not; null when the interpolating factory aggregates the rows itself.
     */
    public AggregatePlan.Algorithm getPreAggregation() {
        checkPhysicalRead("SampleByPlan.preAggregation");
        return preAggregation;
    }

    public int getFromPosition() {
        return from == null ? 0 : from.getPosition();
    }

    public BoundExpression getTo() {
        return to;
    }

    public int getToPosition() {
        return to == null ? 0 : to.getPosition();
    }

    /**
     * True when the generator copies the rows of the base: a fill that reads the rows of the keyed SAMPLE BY cursor
     * scans their keys first and then rewinds the cursor, which rewinds the base and would run a stream GROUP BY under
     * it again. The copy replays the rows while they fit the query memory limit; past it, the base computes them again.
     */
    public boolean isBaseCopied() {
        checkPhysicalRead("SampleByPlan.isBaseCopied");
        return isBaseCopied;
    }

    /**
     * True when a FILL the clause states fills the buckets this node emits.
     */
    public boolean isFillInput() {
        return isFillInput;
    }

    public boolean isJoinInput() {
        return isJoinInput;
    }

    public boolean isTimestampRequired() {
        return isTimestampRequired;
    }

    @Override
    public SampleByPlan of(LogicalPlan input, int position) {
        configure(input, position);
        return this;
    }

    public void setAlgorithm(Algorithm algorithm) {
        this.algorithm = algorithm;
    }

    public void setBucket(BoundExpression bucket) {
        this.bucket = bucket;
    }

    public void setBaseCopied(boolean isBaseCopied) {
        this.isBaseCopied = isBaseCopied;
    }

    public void setFrom(BoundExpression from) {
        this.from = from;
    }

    public void setFillInput(boolean isFillInput) {
        this.isFillInput = isFillInput;
    }

    public void setJoinInput(boolean isJoinInput) {
        this.isJoinInput = isJoinInput;
    }

    public void setPreAggregation(AggregatePlan.Algorithm preAggregation) {
        assert preAggregation == null || preAggregation == AggregatePlan.Algorithm.SERIAL || preAggregation == AggregatePlan.Algorithm.PARALLEL
                || preAggregation == AggregatePlan.Algorithm.PARALLEL_STOLEN_FILTER;
        this.preAggregation = preAggregation;
    }

    public void setTimestampRequired(boolean isTimestampRequired) {
        this.isTimestampRequired = isTimestampRequired;
    }

    public void setTo(BoundExpression to) {
        this.to = to;
    }

    @Override
    public void visitFacts(PlanFactRecorder visitor) {
        super.visitFacts(visitor);
        visitor.facts("fillModes", fillModes);
        visitor.fact("algorithm", algorithm);
        visitor.fact("isBaseCopied", isBaseCopied);
        visitor.fact("isFillInput", isFillInput);
        visitor.fact("isJoinInput", isJoinInput);
        visitor.fact("isTimestampRequired", isTimestampRequired);
        visitor.fact("preAggregation", preAggregation);
    }

    @Override
    public void visitReads(PlanExpressionVisitor visitor) {
        super.visitReads(visitor);
        PlanReads.expressions(fillValues, visitor);
        bucket = PlanReads.expression(bucket, visitor);
        from = PlanReads.expression(from, visitor);
        to = PlanReads.expression(to, visitor);
    }

    /**
     * How the generator samples: interpolating a FILL(LINEAR); reading the first and last values of each bucket
     * straight from the bitmap index of the single symbol key a table scan filters on, which operator planning never
     * picks over a table whose parquet partitions store a column under a converted type; or aggregating each bucket
     * in the SAMPLE BY cursor, without a fill or filling keyless buckets with constants.
     */
    public enum Algorithm {
        FILL_NONE, FILL_VALUE, FIRST_LAST_INDEX, INTERPOLATE
    }
}
