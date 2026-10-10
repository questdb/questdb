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

import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;

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
    private final IntList carrierColumnIds = new IntList();
    private final IntList keyPositions = new IntList();
    private final IntList masterKeyColumnIds = new IntList();
    private final ObjList<CharSequence> masterKeyNames = new ObjList<>();
    private final OutputSchema output = new OutputSchema();
    private final IntList slaveKeyColumnIds = new IntList();
    private final ObjList<CharSequence> slaveKeyNames = new ObjList<>();
    private Algorithm algorithm;
    private CharSequence bindingAlias;
    private int hints;
    private LogicalPlan input;
    private boolean isDependent;
    private boolean isSubquery;
    private JoinKind joinType;
    private BoundExpression keyFilter;
    private int markoutSequenceColumnId = -1;
    private int markoutTimestampColumnId = -1;
    private MasterSide masterSide;
    private BoundExpression onResidual;
    private int position = -1;
    private BoundExpression postJoinFilter;
    private long toleranceInterval = Numbers.LONG_NULL;
    private UnnestSpec unnest;

    /**
     * Keys the step on a master and a slave column unless it already is.
     */
    public void addKey(int masterId, int slaveId, CharSequence masterName, CharSequence slaveName, int position) {
        for (int i = 0, n = masterKeyColumnIds.size(); i < n; i++) {
            if (masterKeyColumnIds.getQuick(i) == masterId && slaveKeyColumnIds.getQuick(i) == slaveId) {
                return;
            }
        }
        masterKeyColumnIds.add(masterId);
        slaveKeyColumnIds.add(slaveId);
        masterKeyNames.add(masterName);
        slaveKeyNames.add(slaveName);
        keyPositions.add(position);
    }

    @Override
    public void clear() {
        carrierColumnIds.clear();
        keyPositions.clear();
        masterKeyColumnIds.clear();
        masterKeyNames.clear();
        output.clear();
        slaveKeyColumnIds.clear();
        slaveKeyNames.clear();
        algorithm = null;
        bindingAlias = null;
        hints = 0;
        input = null;
        isDependent = false;
        isSubquery = false;
        keyFilter = null;
        joinType = null;
        markoutSequenceColumnId = -1;
        markoutTimestampColumnId = -1;
        masterSide = null;
        onResidual = null;
        position = -1;
        postJoinFilter = null;
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
     * The columns that stand for outer columns which decorrelation keys the step on or its ON condition reads, master
     * and slave side alike.
     */
    public IntList getCarrierColumnIds() {
        return carrierColumnIds;
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
     * Slave-only equality derived when two join keys share a master column.
     */
    public BoundExpression getKeyFilter() {
        return keyFilter;
    }

    public IntList getKeyPositions() {
        return keyPositions;
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

    public IntList getMasterKeyColumnIds() {
        return masterKeyColumnIds;
    }

    public ObjList<CharSequence> getMasterKeyNames() {
        return masterKeyNames;
    }

    /**
     * Which input drives the step, or null before operator planning decided it.
     */
    public MasterSide getMasterSide() {
        return masterSide;
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

    public IntList getSlaveKeyColumnIds() {
        return slaveKeyColumnIds;
    }

    public ObjList<CharSequence> getSlaveKeyNames() {
        return slaveKeyNames;
    }

    public OutputSchema getSourceOutput() {
        return unnest == null ? input.getOutput() : unnest.getOutput();
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

    /**
     * Records the algorithm; the master side of a light INNER hash join stays undecided until operator planning decides
     * it.
     */
    public void setAlgorithm(Algorithm algorithm) {
        this.algorithm = algorithm;
        masterSide = algorithm == Algorithm.LIGHT_HASH && joinType == JoinKind.INNER ? null : MasterSide.FIXED;
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

    public void setOnResidual(BoundExpression onResidual) {
        this.onResidual = onResidual;
    }

    public void setPostJoinFilter(BoundExpression postJoinFilter) {
        this.postJoinFilter = postJoinFilter;
    }

    public void setSubquery(boolean isSubquery) {
        this.isSubquery = isSubquery;
    }

    public void setToleranceInterval(long toleranceInterval) {
        this.toleranceInterval = toleranceInterval;
    }

    /**
     * Drops a slave key's qualifier once a filter sharing the key moves into the slave subquery.
     */
    public void unqualifySlaveKeyName(int index) {
        final CharSequence name = slaveKeyNames.getQuick(index);
        final int dot = Chars.indexOfLastUnquoted(name, '.');
        if (dot > -1) {
            slaveKeyNames.setQuick(index, name.subSequence(dot + 1, name.length()));
        }
    }

    void visitReads(PlanExpressionVisitor visitor) {
        if (unnest != null) {
            unnest.visitReads(visitor);
        }
        PlanReads.columnIds(masterKeyColumnIds, keyPositions, visitor);
        PlanReads.columnIds(slaveKeyColumnIds, keyPositions, visitor);
        keyFilter = PlanReads.expression(keyFilter, visitor);
        onResidual = PlanReads.expression(onResidual, visitor);
        postJoinFilter = PlanReads.expression(postJoinFilter, visitor);
        markoutTimestampColumnId = PlanReads.columnId(markoutTimestampColumnId, -1, visitor);
        markoutSequenceColumnId = PlanReads.columnId(markoutSequenceColumnId, -1, visitor);
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
