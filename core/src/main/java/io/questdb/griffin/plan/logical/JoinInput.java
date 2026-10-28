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

import io.questdb.std.LongIntHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;
import io.questdb.std.ObjectPool;

import java.util.Objects;

/**
 * One source occurrence and its matching conditions in a join block.
 */
public final class JoinInput implements Mutable {
    public static final ObjectFactory<JoinInput> FACTORY = JoinInput::new;
    public static final int HINT_ASOF_DENSE = 1;
    public static final int HINT_ASOF_INDEX = 2;
    public static final int HINT_ASOF_LINEAR = 4;
    public static final int HINT_ASOF_MEMOIZED = 8;
    public static final int HINT_ASOF_MEMOIZED_DRIVEBY = 16;
    public static final int HINT_MARKOUT_HORIZON = 32;
    // Column remapping can collapse distinct keys before decorrelation removes them.
    private final LongIntHashMap keyPairCounts = new LongIntHashMap();
    private final ObjectPool<JoinKey> keyPool = new ObjectPool<>(JoinKey.FACTORY, 1, 1);
    private final ObjList<JoinKey> keys = new ObjList<>();
    private final OutputSchema output = new OutputSchema();
    private Algorithm algorithm;
    private CharSequence bindingAlias;
    private BuildSide buildSide;
    private int hints;
    private LogicalPlan input;
    private boolean isDependent;
    private boolean isSubquery;
    private JoinKind joinType;
    private BoundExpression keyCheck;
    private BoundExpression keyFilter;
    private int markoutSequenceColumnId = -1;
    private int markoutTimestampColumnId = -1;
    private MasterSide masterSide;
    private NullExtension nullExtension = NullExtension.BY_KEYS;
    private BoundExpression nullKeyCheck;
    private NullKeyMatch nullKeyMatch;
    private BoundExpression onResidual;
    private int position = -1;
    private BoundExpression postJoinFilter;
    private TimeFrameScan timeFrameScan;
    private long toleranceInterval = Numbers.LONG_NULL;
    private UnnestSpec unnest;

    /**
     * Keys the step on a master and a slave column unless it already is; a CROSS step that gains a key joins INNER.
     */
    public void addKey(int masterId, int slaveId, CharSequence masterName, CharSequence slaveName, int position) {
        final long pair = Numbers.encodeLowHighInts(masterId, slaveId);
        final int index = keyPairCounts.keyIndex(pair);
        if (index < 0) {
            return;
        }
        keyPairCounts.putAt(index, pair, 1);
        keys.add(keyPool.next().of(masterId, slaveId, masterName, slaveName, position));
        if (joinType == JoinKind.CROSS) {
            joinType = JoinKind.INNER;
        }
    }

    @Override
    public void clear() {
        keyPairCounts.clear();
        keys.clear();
        keyPool.clear();
        output.clear();
        algorithm = null;
        bindingAlias = null;
        buildSide = null;
        hints = 0;
        input = null;
        isDependent = false;
        isSubquery = false;
        keyCheck = null;
        keyFilter = null;
        joinType = null;
        markoutSequenceColumnId = -1;
        markoutTimestampColumnId = -1;
        masterSide = null;
        nullExtension = NullExtension.BY_KEYS;
        nullKeyCheck = null;
        nullKeyMatch = null;
        onResidual = null;
        position = -1;
        postJoinFilter = null;
        timeFrameScan = null;
        toleranceInterval = Numbers.LONG_NULL;
        unnest = null;
    }

    /**
     * How the generator joins this step to its master, or null before operator planning decided it.
     */
    public Algorithm getAlgorithm() {
        return algorithm;
    }

    public CharSequence getBindingAlias() {
        return bindingAlias;
    }

    /**
     * The input a light INNER hash join step hashes, which operator planning records once it settles the step: the
     * slave of a step whose master side is {@link MasterSide#FIXED}, and for one the smaller input drives, the input
     * with the smaller row bound or {@link BuildSide#SMALLER}, which leaves the choice to execution. Null for a step of
     * another algorithm or kind, and for one operator planning has not settled.
     */
    public BuildSide getBuildSide() {
        return buildSide;
    }

    public int getHints() {
        return hints;
    }

    public LogicalPlan getInput() {
        return input;
    }

    public JoinKind getJoinType() {
        return joinType;
    }

    /**
     * The predicate over the master row of an ASOF or LT step that a slave row can match it, which operator planning
     * records: the conjunction of {@code =} between the master column of each key the step checks instead of looking
     * it up and the master column of the key it looks up instead, see {@link JoinKey#getLookupKeyIndex}. Null when the
     * step looks up every key, and for any other step.
     */
    public BoundExpression getKeyCheck() {
        return keyCheck;
    }

    /**
     * Slave-only equality derived when two join keys share a master column.
     */
    public BoundExpression getKeyFilter() {
        return keyFilter;
    }

    public ObjList<JoinKey> getKeys() {
        return keys;
    }

    /**
     * Slave column of a markout horizon CROSS join, or -1 when the join is not one.
     */
    public int getMarkoutSequenceColumnId() {
        return markoutSequenceColumnId;
    }

    public int getMarkoutTimestampColumnId() {
        return markoutTimestampColumnId;
    }

    /**
     * Which input drives the step, or null before operator planning decided it.
     */
    public MasterSide getMasterSide() {
        return masterSide;
    }

    /**
     * How the outer step decides which of the rows it preserves it NULL-extends.
     */
    public NullExtension getNullExtension() {
        return nullExtension;
    }

    /**
     * The predicate over the master row that the master columns of the keys all hold the values a NULL record reads for
     * them, see {@link NullExtension#BY_ON_RESIDUAL}, which operator planning records for a step that NULL-extends
     * master rows by its ON residual, see {@link PlanToFactoryRules#isMasterNullKeyChecked}; null for any other step.
     */
    public BoundExpression getNullKeyCheck() {
        return nullKeyCheck;
    }

    /**
     * How the factory finds the rows that pass the ON residual with a row whose keys are all NULL, see
     * {@link #getNullKeyCheck}, which operator planning records for a step that NULL-extends by its ON residual, see
     * {@link PlanToFactoryRules#nullKeyMatch}; null for any other step.
     */
    public NullKeyMatch getNullKeyMatch() {
        return nullKeyMatch;
    }

    public BoundExpression getOnResidual() {
        return onResidual;
    }

    public OutputSchema getOutput() {
        return output;
    }

    public int getPosition() {
        return position;
    }

    public BoundExpression getPostJoinFilter() {
        return postJoinFilter;
    }

    public OutputSchema getSourceOutput() {
        return unnest == null ? input.getOutput() : unnest.getOutput();
    }

    /**
     * How a keyed ASOF join that navigates the time frames of its slave finds the slave row of each master row, which
     * operator planning records; null for any other step.
     */
    public TimeFrameScan getTimeFrameScan() {
        return timeFrameScan;
    }

    /**
     * ASOF/LT TOLERANCE in the units of the higher-precision designated timestamp of the two sides, or
     * {@link Numbers#LONG_NULL} when the join has none.
     */
    public long getToleranceInterval() {
        return toleranceInterval;
    }

    public UnnestSpec getUnnest() {
        return unnest;
    }

    /**
     * True when a key is on the column, master or slave side.
     */
    public boolean hasKeyOn(int columnId) {
        for (int i = 0, n = keys.size(); i < n; i++) {
            if (keys.getQuick(i).isOn(columnId)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The index of the first key on the master column, or -1 when none is.
     */
    public int indexOfMasterKey(int masterColumnId) {
        for (int i = 0, n = keys.size(); i < n; i++) {
            if (keys.getQuick(i).getMasterColumnId() == masterColumnId) {
                return i;
            }
        }
        return -1;
    }

    /**
     * True when operator planning decided the algorithm of the step and the input that drives it.
     */
    public boolean isAlgorithmDecided() {
        return algorithm != null && masterSide != null;
    }

    /**
     * True when the input reads columns of the inputs before it through {@link OuterColumnExpression}s, as a
     * LATERAL body does; decorrelation rewrites the step into an ordinary one before optimisation.
     */
    public boolean isDependent() {
        return isDependent;
    }

    public boolean isSubquery() {
        return isSubquery;
    }

    public JoinInput of(LogicalPlan input, JoinKind joinType, CharSequence bindingAlias, int position) {
        this.input = Objects.requireNonNull(input);
        this.isSubquery = false;
        this.joinType = Objects.requireNonNull(joinType);
        this.bindingAlias = bindingAlias;
        this.position = position;
        this.unnest = null;
        return this;
    }

    public JoinInput ofUnnest(UnnestSpec unnest, CharSequence bindingAlias, int position) {
        this.input = null;
        this.isSubquery = false;
        this.joinType = JoinKind.UNNEST;
        this.bindingAlias = bindingAlias;
        this.position = position;
        this.unnest = Objects.requireNonNull(unnest);
        return this;
    }

    public void removeKey(int index) {
        final JoinKey key = keys.getQuick(index);
        final long pair = Numbers.encodeLowHighInts(key.getMasterColumnId(), key.getSlaveColumnId());
        final int pairIndex = keyPairCounts.keyIndex(pair);
        final int count = keyPairCounts.valueAt(pairIndex);
        if (count == 1) {
            keyPairCounts.removeAt(pairIndex);
        } else {
            keyPairCounts.putAt(pairIndex, pair, count - 1);
        }
        keys.remove(index);
    }

    /**
     * Records the algorithm; the master side of a light INNER hash join stays undecided until operator planning decides
     * it.
     */
    public void setAlgorithm(Algorithm algorithm) {
        this.algorithm = algorithm;
        masterSide = algorithm == Algorithm.LIGHT_HASH && joinType == JoinKind.INNER ? null : MasterSide.FIXED;
        buildSide = null;
    }

    public void setBuildSide(BuildSide buildSide) {
        this.buildSide = buildSide;
    }

    public void setDependent(boolean isDependent) {
        this.isDependent = isDependent;
    }

    public void setHints(int hints) {
        this.hints = hints;
    }

    public void setInput(LogicalPlan input) {
        this.input = Objects.requireNonNull(input);
    }

    public void setJoinType(JoinKind joinType) {
        this.joinType = Objects.requireNonNull(joinType);
    }

    public void setKeyCheck(BoundExpression keyCheck) {
        this.keyCheck = keyCheck;
    }

    public void setKeyFilter(BoundExpression keyFilter) {
        this.keyFilter = keyFilter;
    }

    public void setMarkout(int timestampColumnId, int sequenceColumnId) {
        markoutTimestampColumnId = timestampColumnId;
        markoutSequenceColumnId = sequenceColumnId;
    }

    public void setMasterSide(MasterSide masterSide) {
        this.masterSide = masterSide;
    }

    public void setNullExtension(NullExtension nullExtension) {
        this.nullExtension = Objects.requireNonNull(nullExtension);
    }

    public void setNullKeyCheck(BoundExpression nullKeyCheck) {
        this.nullKeyCheck = nullKeyCheck;
    }

    public void setNullKeyMatch(NullKeyMatch nullKeyMatch) {
        this.nullKeyMatch = nullKeyMatch;
    }

    public void setOnResidual(BoundExpression onResidual) {
        this.onResidual = onResidual;
    }

    public void setPostJoinFilter(BoundExpression postJoinFilter) {
        this.postJoinFilter = postJoinFilter;
    }

    public void setSubquery(boolean isSubquery) {
        this.isSubquery = isSubquery;
    }

    public void setTimeFrameScan(TimeFrameScan timeFrameScan) {
        this.timeFrameScan = timeFrameScan;
    }

    public void setToleranceInterval(long toleranceInterval) {
        this.toleranceInterval = toleranceInterval;
    }

    public void visitReads(PlanExpressionVisitor visitor) {
        if (unnest != null) {
            unnest.visitReads(visitor);
        }
        if (JoinKey.visitReads(keys, visitor)) {
            rebuildKeyPairCounts();
        }
        keyCheck = PlanReads.expression(keyCheck, visitor);
        keyFilter = PlanReads.expression(keyFilter, visitor);
        nullKeyCheck = PlanReads.expression(nullKeyCheck, visitor);
        onResidual = PlanReads.expression(onResidual, visitor);
        postJoinFilter = PlanReads.expression(postJoinFilter, visitor);
        markoutTimestampColumnId = PlanReads.columnId(markoutTimestampColumnId, -1, visitor);
        markoutSequenceColumnId = PlanReads.columnId(markoutSequenceColumnId, -1, visitor);
    }

    private void rebuildKeyPairCounts() {
        keyPairCounts.clear();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final JoinKey key = keys.getQuick(i);
            final long pair = Numbers.encodeLowHighInts(key.getMasterColumnId(), key.getSlaveColumnId());
            final int index = keyPairCounts.keyIndex(pair);
            keyPairCounts.putAt(index, pair, index < 0 ? keyPairCounts.valueAt(index) + 1 : 1);
        }
    }

    /**
     * How the generator joins a step to its master.
     */
    public enum Algorithm {
        /**
         * Pairs every master row with every slave row that passes the join condition.
         */
        NESTED_LOOP,
        /**
         * A nested loop with a fixed leading input that caches the slave rows within its memory budget and
         * streams them when they do not fit.
         */
        MATERIALIZED_NESTED_LOOP,
        /**
         * Hashes copies of the slave rows by key.
         */
        HASH,
        /**
         * Hashes the row ids of a random-access slave by key.
         */
        LIGHT_HASH,
        /**
         * Expands each master row over a long_sequence() slave of horizon offsets, in master timestamp order.
         */
        MARKOUT,
        /**
         * An ASOF or LT join that scans a random-access slave linearly and reads it back by row id.
         */
        TEMPORAL,
        /**
         * An ASOF or LT join that navigates the time frames of its slave.
         */
        TEMPORAL_TIME_FRAME,
        /**
         * An ASOF join that applies the filter of its slave itself, while it reads the time frames under that filter.
         */
        TEMPORAL_STOLEN_FILTER,
        /**
         * An ASOF or LT join that keeps copies of the slave rows.
         */
        FULL_FAT_TEMPORAL,
        /**
         * A SPLICE join.
         */
        SPLICE,
        /**
         * A SPLICE join under full-fat joins, which the generator rejects.
         */
        FULL_FAT_SPLICE,
        /**
         * Expands each master row by the UNNEST expressions.
         */
        UNNEST
    }

    /**
     * How an outer step decides which of the rows it preserves it NULL-extends.
     */
    public enum NullExtension {
        /**
         * A preserved row whose key partners all fail the ON residual: the keys state the ON clause's equalities.
         */
        BY_KEYS,
        /**
         * A preserved row whose keys all hold the values the NULL record of the other input reads for them, NULL, or
         * false or 0 for a type with no NULL sentinel, and whom every row of the other input fails the ON residual; the
         * step drops every other preserved row without a key partner that passes the ON residual. The keys are
         * equalities a later INNER join implies, which hold for every row that join keeps, and the ON residual holds
         * the whole ON clause: the later join drops every other row the ON clause pairs or NULL-extends, so every row
         * the step emits holds its keys.
         */
        BY_ON_RESIDUAL
    }

    /**
     * How the factory of a step NULL-extended by its ON residual, see {@link NullExtension#BY_ON_RESIDUAL}, finds the
     * rows that pass the ON residual with a row whose keys are all NULL: the slave rows that pass it with such a master
     * row, and for a RIGHT or FULL step the rows of the NULL key chain that pass it with some master row.
     */
    public enum NullKeyMatch {
        /**
         * Evaluates the ON residual on each pair of a master and a slave row the factory walks.
         */
        PER_PAIR,
        /**
         * Evaluates the ON residual on slave rows alone: the ON residual reads only the slave input and takes the same
         * value for a row on every evaluation, so no master row changes whether a slave row passes. The factory tests
         * once per execution whether some slave row passes, and tests a row of the NULL key chain when it emits the
         * slave rows no master row matched.
         */
        PER_SLAVE_ROW
    }

    /**
     * How a keyed ASOF join that navigates the time frames of its slave finds the slave row of each master row, see
     * {@link #getTimeFrameScan}. The variants that read a single symbol key map the master's symbol keys onto the
     * slave's symbol table.
     */
    public enum TimeFrameScan {
        /**
         * Scans the time frames back from the master timestamp until a slave row matches the key.
         */
        FAST,
        /**
         * Resumes a forward scan from the previous master row and a backward scan from where both started,
         * remembering the latest slave row of each key either passes, which the {@code asof_dense} hint asks for.
         */
        DENSE,
        /**
         * The dense scan of a single symbol key.
         */
        DENSE_SINGLE_SYMBOL,
        /**
         * Reads the latest slave row of the single symbol key from the index of the slave's key column, which the
         * {@code asof_index} hint asks for.
         */
        INDEXED,
        /**
         * Scans the time frames back for the single symbol key, remembering where it found each key and where it did
         * not, which the {@code asof_memoized} hint asks for.
         */
        MEMOIZED,
        /**
         * The memoized scan that also remembers the other keys it passes on the way, which the
         * {@code asof_memoized_driveby} hint asks for.
         */
        MEMOIZED_DRIVEBY
    }

    /**
     * Which input of a hash join, its master or its slave, the operator hashes; it probes the hash with the other one.
     * An outer join hashes the input it NULL-extends.
     */
    public enum BuildSide {
        MASTER,
        SLAVE,
        /**
         * The input whose cursor reports fewer rows when the light INNER hash join opens, a master only when its
         * factory supports random access, and the slave when either cursor cannot tell.
         */
        SMALLER;

        /**
         * The input to hash so that the hash holds the fewer rows, {@code tie} when both inputs hold as many rows, and
         * {@link #SMALLER} when either row count is unknown, -1.
         */
        public static BuildSide smaller(long masterRows, long slaveRows, BuildSide tie) {
            if (masterRows < 0 || slaveRows < 0) {
                return SMALLER;
            }
            return masterRows < slaveRows ? MASTER : masterRows > slaveRows ? SLAVE : tie;
        }
    }

    /**
     * The input a join step drives the join with.
     */
    public enum MasterSide {
        /**
         * Always the master, so the step emits rows in the master's order.
         */
        FIXED,
        /**
         * The smaller of the two inputs at execution; the step then emits rows in either input's order and declares no
         * designated timestamp.
         */
        SMALLER
    }
}
