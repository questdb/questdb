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
    private final IntList keyPositions = new IntList();
    private final IntList masterKeyColumnIds = new IntList();
    private final ObjList<CharSequence> masterKeyNames = new ObjList<>();
    private final OutputSchema output = new OutputSchema();
    private final IntList slaveKeyColumnIds = new IntList();
    private final ObjList<CharSequence> slaveKeyNames = new ObjList<>();
    private CharSequence bindingAlias;
    private int hints;
    private LogicalPlan input;
    private boolean isDependent;
    private boolean isSubquery;
    private JoinKind joinType;
    private BoundExpression keyFilter;
    private int markoutSequenceColumnId = -1;
    private int markoutTimestampColumnId = -1;
    private BoundExpression onResidual;
    private int position = -1;
    private BoundExpression postJoinFilter;
    private long toleranceInterval = Numbers.LONG_NULL;
    private UnnestSpec unnest;

    @Override
    public void clear() {
        keyPositions.clear();
        masterKeyColumnIds.clear();
        masterKeyNames.clear();
        output.clear();
        slaveKeyColumnIds.clear();
        slaveKeyNames.clear();
        bindingAlias = null;
        hints = 0;
        input = null;
        isDependent = false;
        isSubquery = false;
        keyFilter = null;
        joinType = null;
        markoutSequenceColumnId = -1;
        markoutTimestampColumnId = -1;
        onResidual = null;
        position = -1;
        postJoinFilter = null;
        toleranceInterval = Numbers.LONG_NULL;
        unnest = null;
    }

    public CharSequence getBindingAlias() {
        return bindingAlias;
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
}
