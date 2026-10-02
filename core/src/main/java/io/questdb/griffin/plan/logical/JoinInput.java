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

import io.questdb.griffin.model.QueryModel;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
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
    private BoundExpression keyFilter;
    private int joinType = -1;
    private int markoutSequenceColumnId = -1;
    private int markoutTimestampColumnId = -1;
    private BoundExpression onResidual;
    private int position = -1;
    private BoundExpression postJoinFilter;
    private int tolerancePosition = -1;
    private CharSequence toleranceToken;
    private UnnestSpec unnest;
    private String unsupportedOnExpression;
    private int unsupportedOnPosition = -1;

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
        joinType = -1;
        markoutSequenceColumnId = -1;
        markoutTimestampColumnId = -1;
        onResidual = null;
        position = -1;
        postJoinFilter = null;
        tolerancePosition = -1;
        unnest = null;
        toleranceToken = null;
        unsupportedOnExpression = null;
        unsupportedOnPosition = -1;
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

    public int getJoinType() {
        return joinType;
    }

    /** Slave-only equality derived when two join keys share a master column. */
    public BoundExpression getKeyFilter() {
        return keyFilter;
    }

    public IntList getKeyPositions() {
        return keyPositions;
    }

    public IntList getMasterKeyColumnIds() {
        return masterKeyColumnIds;
    }

    public ObjList<CharSequence> getMasterKeyNames() {
        return masterKeyNames;
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

    public int getTolerancePosition() {
        return tolerancePosition;
    }

    public CharSequence getToleranceToken() {
        return toleranceToken;
    }

    public UnnestSpec getUnnest() {
        return unnest;
    }

    public String getUnsupportedOnExpression() {
        return unsupportedOnExpression;
    }

    public int getUnsupportedOnPosition() {
        return unsupportedOnPosition;
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

    public JoinInput of(LogicalPlan input, int joinType, CharSequence bindingAlias, int position) {
        this.input = Objects.requireNonNull(input);
        this.isSubquery = false;
        this.joinType = joinType;
        this.bindingAlias = bindingAlias;
        this.position = position;
        this.unnest = null;
        return this;
    }

    public JoinInput ofUnnest(UnnestSpec unnest, CharSequence bindingAlias, int position) {
        this.input = null;
        this.isSubquery = false;
        this.joinType = QueryModel.JOIN_UNNEST;
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

    public void setJoinType(int joinType) {
        this.joinType = joinType;
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

    public void setTolerance(CharSequence token, int position) {
        toleranceToken = token;
        tolerancePosition = position;
    }

    public void setUnsupportedOnExpression(String expression, int position) {
        unsupportedOnExpression = expression;
        unsupportedOnPosition = position;
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
