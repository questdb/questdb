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
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;

/**
 * A GROUP BY grouping, or a decorrelation domain that may re-read a join input it shares with its master.
 */
public final class AggregatePlan extends GroupingPlan {
    public static final ObjectFactory<AggregatePlan> FACTORY = AggregatePlan::new;
    private final ObjList<IntList> groupingSets = new ObjList<>();
    private final IntList sharedInputIds = new IntList();
    private final IntList sharedSourceIds = new IntList();
    private Algorithm algorithm;
    private HashJoinAggregation hashJoin;
    private boolean isRandomAccessRequired;
    private boolean isRewound;
    private boolean isStreamBackward;
    private OutputCopy outputCopy = OutputCopy.NONE;
    private int readerCount;
    private OutputCopy requestedCopy = OutputCopy.NONE;
    private SortDirection requestedDirection;
    private int requestedOrderColumnId = -1;
    private LimitPlan requestedTopK;
    private int requestedTopKColumnId = -1;
    private int sharedConsumerCount;
    private LogicalPlan sharedHead;
    private SetOperationPlan sharedOperation;
    private SharedRead sharedRead;
    private JoinInput sharedSource;
    private int streamKeyIndex = -1;

    @Override
    public void clear() {
        super.clear();
        groupingSets.clear();
        sharedInputIds.clear();
        sharedSourceIds.clear();
        algorithm = null;
        hashJoin = null;
        isRandomAccessRequired = false;
        isRewound = false;
        isStreamBackward = false;
        outputCopy = OutputCopy.NONE;
        readerCount = 0;
        requestedCopy = OutputCopy.NONE;
        requestedDirection = null;
        requestedOrderColumnId = -1;
        requestedTopK = null;
        requestedTopKColumnId = -1;
        sharedConsumerCount = 0;
        sharedHead = null;
        sharedOperation = null;
        sharedRead = null;
        sharedSource = null;
        streamKeyIndex = -1;
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
        checkPhysicalRead("AggregatePlan.algorithm");
        return algorithm;
    }

    /**
     * The grouping sets of a ROLLUP, CUBE or GROUPING SETS: each lists the indexes of the grouping keys the set groups
     * by, and the rows of the set read every other key as NULL. Empty for any other aggregate. A key outside some set
     * is a plain input column that no other key reads, so rolling it up nulls that key alone.
     */
    public ObjList<IntList> getGroupingSets() {
        return groupingSets;
    }

    /**
     * How the GROUP BY fuses into the hash join under it, recorded with {@link Algorithm#HASH_JOIN}; null otherwise.
     */
    public HashJoinAggregation getHashJoin() {
        checkPhysicalRead("AggregatePlan.hashJoin");
        return hashJoin;
    }

    /**
     * The number of domain reads of the decorrelation domain, see {@link DomainReadPlan}, which decorrelation and the
     * plan copier count. Filter placement pushes no filter into a domain other consumers read.
     */
    /**
     * How the generator copies the rows of a stream GROUP BY for a consumer that reads them again or by row id, see
     * {@link OutputCopy}; {@link OutputCopy#NONE} for a GROUP BY that does not stream.
     */
    public OutputCopy getOutputCopy() {
        checkPhysicalRead("AggregatePlan.outputCopy");
        return outputCopy;
    }

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
        checkPhysicalRead("AggregatePlan.sharedConsumerCount");
        return sharedConsumerCount;
    }

    /**
     * The leading branch of the input's set operation, under column projections, that the generator reads from the head
     * of {@link #getSharedOperation()} when the aggregate reads its input that way, see {@link SharedRead#SET_OPERATION_HEAD}.
     */
    /**
     * The copy a consumer that copies rows without row ids itself requests should the GROUP BY stream, see
     * {@link OutputCopy}.
     */
    public OutputCopy getRequestedCopy() {
        return requestedCopy;
    }

    /**
     * The direction the consumer requests the rows in by {@link #getRequestedOrderColumnId()}, null for none.
     */
    public SortDirection getRequestedDirection() {
        return requestedDirection;
    }

    /**
     * The output column the consumer requests the rows ordered by, -1 for none.
     */
    public int getRequestedOrderColumnId() {
        return requestedOrderColumnId;
    }

    /**
     * The LIMIT of the consumer's sort that orders the rows by {@link #getRequestedTopKColumnId()} alone and keeps its
     * first rows, null for none.
     */
    public LimitPlan getRequestedTopK() {
        return requestedTopK;
    }

    /**
     * The output column the sort of {@link #getRequestedTopK()} orders the rows by, -1 for none.
     */
    public int getRequestedTopKColumnId() {
        return requestedTopKColumnId;
    }

    public LogicalPlan getSharedHead() {
        checkPhysicalRead("AggregatePlan.sharedHead");
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
        checkPhysicalRead("AggregatePlan.sharedOperation");
        return sharedOperation;
    }

    /**
     * How the generator reads the input of a decorrelation domain that re-reads {@link #getSharedSource()}, which
     * operator planning records; null for an aggregate without a shared source, and before planning.
     */
    public SharedRead getSharedRead() {
        checkPhysicalRead("AggregatePlan.sharedRead");
        return sharedRead;
    }

    public JoinInput getSharedSource() {
        return sharedSource;
    }

    public IntList getSharedSourceIds() {
        return sharedSourceIds;
    }

    /**
     * The index of the grouping expression a stream GROUP BY finishes its groups by, -1 for a hash GROUP BY. The key
     * never decreases along the input rows, so the GROUP BY emits the groups of each key value once the input moves
     * past it, in the order of the key, and in reverse order when it scans its input backward, see
     * {@link #isStreamBackward()}.
     */
    public int getStreamKeyIndex() {
        checkPhysicalRead("AggregatePlan.streamKeyIndex");
        return streamKeyIndex;
    }

    @Override
    public boolean hasGroupingSets() {
        return groupingSets.size() > 0;
    }

    /**
     * Whether the consumer reads the rows by row id.
     */
    public boolean isRandomAccessRequired() {
        return isRandomAccessRequired;
    }

    /**
     * Whether the consumer reads the rows again from the first: the inner side of a nested loop, or a LATEST BY that
     * reads its input a second time in the same order.
     */
    public boolean isRewound() {
        return isRewound;
    }

    /**
     * True when the aggregate groups its whole input into the one row it returns. Grouping sets return a row per set,
     * even when no set has a key.
     */
    public boolean isSingleGroup() {
        return getGroupingExpressions().size() == 0 && !hasGroupingSets();
    }

    /**
     * Whether the stream GROUP BY reads the page frames of its input backward, emitting its groups in descending key
     * order.
     */
    public boolean isStreamBackward() {
        checkPhysicalRead("AggregatePlan.isStreamBackward");
        return isStreamBackward;
    }

    /**
     * Whether other consumers read the aggregate's rows: decorrelation domains that re-read it through their shared
     * source, see {@link #getSharedConsumerCount()}, or domain reads of the decorrelation domain, see
     * {@link #getReaderCount()}, which read it through shared cursors whenever its GROUP BY serves them.
     */
    public boolean hasSharedConsumers() {
        checkPhysicalRead("AggregatePlan.sharedConsumerCount");
        return sharedConsumerCount > 0 || readerCount > 0;
    }

    @Override
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

    /**
     * Records what the consumer requires of the rows, see {@link #getRequestedOrderColumnId()},
     * {@link #isRandomAccessRequired()}, {@link #isRewound()} and {@link #getRequestedCopy()}.
     */
    public void setConsumerRequirement(
            int requestedOrderColumnId,
            SortDirection requestedDirection,
            boolean isRandomAccessRequired,
            boolean isRewound,
            OutputCopy requestedCopy
    ) {
        this.requestedOrderColumnId = requestedOrderColumnId;
        this.requestedDirection = requestedDirection;
        this.isRandomAccessRequired = isRandomAccessRequired;
        this.isRewound = isRewound;
        this.requestedCopy = requestedCopy;
    }

    public void setRequestedTopK(int requestedTopKColumnId, LimitPlan requestedTopK) {
        this.requestedTopKColumnId = requestedTopKColumnId;
        this.requestedTopK = requestedTopK;
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
     * Records how the GROUP BY groups its rows: by hash for a negative {@code streamKeyIndex}, else as a stream by the
     * grouping expression at that index, scanning backward when {@code isStreamBackward}, see
     * {@link #getStreamKeyIndex()}, with the copy {@code outputCopy} its consumer reads.
     */
    public void setStreaming(int streamKeyIndex, boolean isStreamBackward, OutputCopy outputCopy) {
        assert streamKeyIndex >= 0 || (!isStreamBackward && outputCopy == OutputCopy.NONE);
        this.streamKeyIndex = streamKeyIndex;
        this.isStreamBackward = isStreamBackward;
        this.outputCopy = outputCopy;
    }

    @Override
    public void visitFacts(PlanFactRecorder visitor) {
        super.visitFacts(visitor);
        visitor.facts("groupingSets", groupingSets);
        visitor.columnIds("sharedInputIds", sharedInputIds);
        visitor.columnIds("sharedSourceIds", sharedSourceIds);
        visitor.fact("algorithm", algorithm);
        visitor.reference("hashJoin", hashJoin);
        visitor.fact("isRandomAccessRequired", isRandomAccessRequired);
        visitor.fact("isRewound", isRewound);
        visitor.fact("isStreamBackward", isStreamBackward);
        visitor.fact("outputCopy", outputCopy);
        visitor.fact("readerCount", readerCount);
        visitor.fact("requestedCopy", requestedCopy);
        visitor.fact("requestedDirection", requestedDirection);
        visitor.fact("requestedOrderColumnId", requestedOrderColumnId);
        visitor.reference("requestedTopK", requestedTopK);
        visitor.fact("requestedTopKColumnId", requestedTopKColumnId);
        visitor.fact("sharedConsumerCount", sharedConsumerCount);
        visitor.reference("sharedHead", sharedHead);
        visitor.reference("sharedOperation", sharedOperation);
        visitor.fact("sharedRead", sharedRead);
        visitor.reference("sharedSource", sharedSource);
        visitor.fact("streamKeyIndex", streamKeyIndex);
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
     * How the generator copies the rows of a stream GROUP BY, which computes its groups again when its consumer reads
     * them again and offers no row ids.
     */
    public enum OutputCopy {
        /**
         * The consumer reads the rows once, in order.
         */
        NONE,
        /**
         * The consumer reads the rows by row id or again, which a copy serves; the copy fails the query past the query
         * memory limit.
         */
        RANDOM_ACCESS,
        /**
         * As {@link #RANDOM_ACCESS}, for a hash join that reads every row of its slave, which the copy takes in batches
         * ahead of the reads.
         */
        RANDOM_ACCESS_IN_BATCHES
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
