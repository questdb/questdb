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

import io.questdb.std.IntList;
import io.questdb.std.ObjectFactory;

/**
 * A GROUP BY grouping, or a decorrelation domain that may re-read a join input it shares with its master.
 */
public final class AggregatePlan extends GroupingPlan {
    public static final ObjectFactory<AggregatePlan> FACTORY = AggregatePlan::new;
    private final IntList sharedInputIds = new IntList();
    private final IntList sharedSourceIds = new IntList();
    private Algorithm algorithm;
    private HashJoinAggregation hashJoin;
    private int readerCount;
    private int sharedConsumerCount;
    private LogicalPlan sharedHead;
    private SetOperationPlan sharedOperation;
    private SharedRead sharedRead;
    private JoinInput sharedSource;

    @Override
    public void clear() {
        super.clear();
        sharedInputIds.clear();
        sharedSourceIds.clear();
        algorithm = null;
        hashJoin = null;
        readerCount = 0;
        sharedConsumerCount = 0;
        sharedHead = null;
        sharedOperation = null;
        sharedRead = null;
        sharedSource = null;
    }

    /**
     * Records one more domain read of the decorrelation domain, see {@link #getReaderCount()}.
     */
    public void addReader() {
        readerCount++;
    }

    /**
     * The GROUP BY execution operator planning records for the aggregate; null for a count, a timestamp endpoint, a
     * posting-index distinct and a horizon join, which build no GROUP BY. A count fused into the hash join under it,
     * {@link Algorithm#HASH_JOIN}, builds one.
     */
    public Algorithm getAlgorithm() {
        return algorithm;
    }

    /**
     * How the GROUP BY fuses into the hash join under it, recorded with {@link Algorithm#HASH_JOIN}; null otherwise.
     */
    public HashJoinAggregation getHashJoin() {
        return hashJoin;
    }

    /**
     * The number of domain reads of the decorrelation domain, see {@link DomainReadPlan}, which decorrelation and the
     * plan copier count. Filter placement pushes no filter into a domain other consumers read.
     */
    public int getReaderCount() {
        return readerCount;
    }

    /**
     * The number of consumers that read the rows the generator computes once for the aggregate through shared
     * cursors, which {@code OperatorPlanning} counts: the decorrelation domains of the query level whose shared source
     * re-reads the aggregate, and the domain reads of a decorrelation domain that read it through a shared cursor,
     * see {@link DomainReadPlan.Algorithm#SHARED}. A domain read that builds the domain's plan again reads none.
     */
    public int getSharedConsumerCount() {
        return sharedConsumerCount;
    }

    /**
     * The leading branch of the input's set operation, under column projections, that the generator reads from the head
     * of {@link #getSharedOperation()} when the aggregate reads its input that way, see {@link SharedRead#SET_OPERATION_HEAD}.
     */
    public LogicalPlan getSharedHead() {
        return sharedHead;
    }

    /**
     * Input column ids of a relation that re-reads {@link #getSharedSource()}, paired with
     * {@link #getSharedSourceIds()}, so the generator can read the source's factory instead. The pairs map columns
     * rather than read them: a pass that drops an input column may leave its pair. Operator planning replaces them
     * with one pair per input column, in input order, when the aggregate reads the source's factory, see
     * {@link SharedRead#SOURCE}.
     */
    public IntList getSharedInputIds() {
        return sharedInputIds;
    }

    /**
     * The set operation under the column projections of the shared source's input, whose head the generator reads the
     * leading branch of the aggregate's input from, see {@link #getSharedHead()}.
     */
    public SetOperationPlan getSharedOperation() {
        return sharedOperation;
    }

    /**
     * How the generator reads the input of a decorrelation domain that re-reads {@link #getSharedSource()}, which
     * operator planning records; null for an aggregate without a shared source, and before planning.
     */
    public SharedRead getSharedRead() {
        return sharedRead;
    }

    public JoinInput getSharedSource() {
        return sharedSource;
    }

    public IntList getSharedSourceIds() {
        return sharedSourceIds;
    }

    /**
     * Whether other consumers read the aggregate's rows: decorrelation domains that re-read it through their shared
     * source, see {@link #getSharedConsumerCount()}, or domain reads of the decorrelation domain, see
     * {@link #getReaderCount()}, which read it through shared cursors whenever its GROUP BY serves them.
     */
    public boolean hasSharedConsumers() {
        return sharedConsumerCount > 0 || readerCount > 0;
    }

    public AggregatePlan of(LogicalPlan input, int position) {
        configure(input, position);
        return this;
    }

    /**
     * Records a GROUP BY that reads the aggregate's input; {@link #setHashJoin} records the fused one.
     */
    public void setAlgorithm(Algorithm algorithm) {
        assert algorithm != Algorithm.HASH_JOIN;
        this.algorithm = algorithm;
        hashJoin = null;
    }

    public void setHashJoin(HashJoinAggregation hashJoin) {
        algorithm = Algorithm.HASH_JOIN;
        this.hashJoin = hashJoin;
    }

    public void setSharedConsumerCount(int sharedConsumerCount) {
        this.sharedConsumerCount = sharedConsumerCount;
    }

    /**
     * Reads the leading branch of the input, {@code head}, from the head of {@code operation}, see
     * {@link SharedRead#SET_OPERATION_HEAD}.
     */
    public void setSharedHead(SetOperationPlan operation, LogicalPlan head) {
        sharedRead = SharedRead.SET_OPERATION_HEAD;
        sharedOperation = operation;
        sharedHead = head;
    }

    public void setSharedRead(SharedRead sharedRead) {
        this.sharedRead = sharedRead;
        sharedOperation = null;
        sharedHead = null;
    }

    public void setSharedSource(JoinInput sharedSource) {
        this.sharedSource = sharedSource;
    }

    /**
     * How the generator executes the GROUP BY: on one thread, in parallel over the page frames of its input or over
     * the frames under a filter it steals from its input, in the vectorised GROUP BY of a single key, which order
     * planning never picks over a table whose parquet partitions store a column under a converted type, or fused into
     * the two-input hash join under it as {@link #getHashJoin()} records.
     */
    public enum Algorithm {
        HASH_JOIN, PARALLEL, PARALLEL_STOLEN_FILTER, SERIAL, VECTORISED
    }

    /**
     * How the generator reads the input of a decorrelation domain that re-reads its shared source, see
     * {@link #getSharedRead()}.
     */
    public enum SharedRead {
        /**
         * Through the factory the generator builds for the shared source, which serves shared cursors, by the columns
         * {@link #getSharedInputIds()} and {@link #getSharedSourceIds()} pair.
         */
        SOURCE,
        /**
         * Generates the input, reading its leading branch from the head of the shared source's set operation, which
         * serves shared cursors.
         */
        SET_OPERATION_HEAD,
        /**
         * Generates the input.
         */
        NONE
    }
}
