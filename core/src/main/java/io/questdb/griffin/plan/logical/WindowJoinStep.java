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
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;

import java.util.Objects;

public final class WindowJoinStep implements Mutable {
    public static final ObjectFactory<WindowJoinStep> FACTORY = WindowJoinStep::new;
    private final IntList aggregateColumnIds = new IntList();
    private final ObjList<FunctionExpression> aggregates = new ObjList<>();
    private final OutputSchema masterScope = new OutputSchema();
    private final OutputSchema scope = new OutputSchema();
    private BoundExpression filter;
    private long hi;
    private BoundExpression hiExpression;
    private int hiPosition;
    private int hiSign;
    private char hiTimeUnit;
    private boolean isIncludePrevailing;
    private boolean isTableSource;
    private long lo;
    private BoundExpression loExpression;
    private int loPosition;
    private int loSign;
    private char loTimeUnit;
    private CharSequence masterAlias;
    private int position;
    private LogicalPlan slave;
    private CharSequence slaveAlias;

    @Override
    public void clear() {
        aggregateColumnIds.clear();
        aggregates.clear();
        masterScope.clear();
        scope.clear();
        filter = null;
        hi = 0;
        hiExpression = null;
        hiPosition = 0;
        hiSign = 0;
        hiTimeUnit = 0;
        isIncludePrevailing = false;
        isTableSource = false;
        lo = 0;
        loExpression = null;
        loPosition = 0;
        loSign = 0;
        loTimeUnit = 0;
        masterAlias = null;
        position = 0;
        slave = null;
        slaveAlias = null;
    }

    public IntList getAggregateColumnIds() {
        return aggregateColumnIds;
    }

    public ObjList<FunctionExpression> getAggregates() {
        return aggregates;
    }

    public BoundExpression getFilter() {
        return filter;
    }

    public long getHi() {
        return hi;
    }

    public BoundExpression getHiExpression() {
        return hiExpression;
    }

    public int getHiPosition() {
        return hiPosition;
    }

    public int getHiSign() {
        return hiSign;
    }

    public char getHiTimeUnit() {
        return hiTimeUnit;
    }

    public long getLo() {
        return lo;
    }

    public BoundExpression getLoExpression() {
        return loExpression;
    }

    public int getLoPosition() {
        return loPosition;
    }

    public int getLoSign() {
        return loSign;
    }

    public char getLoTimeUnit() {
        return loTimeUnit;
    }

    public CharSequence getMasterAlias() {
        return masterAlias;
    }

    public OutputSchema getMasterScope() {
        return masterScope;
    }

    public int getPosition() {
        return position;
    }

    public OutputSchema getScope() {
        return scope;
    }

    public LogicalPlan getSlave() {
        return slave;
    }

    public CharSequence getSlaveAlias() {
        return slaveAlias;
    }

    public boolean isDynamic() {
        return loExpression != null || hiExpression != null;
    }

    public boolean isIncludePrevailing() {
        return isIncludePrevailing;
    }

    public boolean isTableSource() {
        return isTableSource;
    }

    public WindowJoinStep of(LogicalPlan slave, CharSequence masterAlias, CharSequence slaveAlias, boolean isIncludePrevailing, int position) {
        this.slave = Objects.requireNonNull(slave);
        this.masterAlias = masterAlias;
        this.slaveAlias = slaveAlias;
        this.isIncludePrevailing = isIncludePrevailing;
        this.position = position;
        return this;
    }

    public void setFilter(BoundExpression filter) {
        this.filter = filter;
    }

    public void setHi(long hi, BoundExpression expression, int sign, char timeUnit, int position) {
        this.hi = hi;
        this.hiExpression = expression;
        this.hiSign = sign;
        this.hiTimeUnit = timeUnit;
        this.hiPosition = position;
    }

    public void setLo(long lo, BoundExpression expression, int sign, char timeUnit, int position) {
        this.lo = lo;
        this.loExpression = expression;
        this.loSign = sign;
        this.loTimeUnit = timeUnit;
        this.loPosition = position;
    }

    public void setTableSource(boolean isTableSource) {
        this.isTableSource = isTableSource;
    }

    void setSlave(LogicalPlan slave) {
        this.slave = Objects.requireNonNull(slave);
    }
}
