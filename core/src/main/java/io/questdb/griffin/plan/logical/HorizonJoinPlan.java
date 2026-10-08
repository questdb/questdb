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

import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;

import java.util.Objects;

/**
 * The HORIZON JOIN record stream: every master row repeated for each horizon offset, with each slave
 * ASOF-matched at the offset timestamp. Output is the master columns, the horizon offset and timestamp,
 * then every slave's columns. Only an aggregate consumes it.
 */
public final class HorizonJoinPlan extends LogicalPlan {
    public static final ObjectFactory<HorizonJoinPlan> FACTORY = HorizonJoinPlan::new;
    private final LongList offsetValues = new LongList();
    private final ObjList<CharSequence> offsets = new ObjList<>();
    private final ObjList<HorizonJoinSlave> slaves = new ObjList<>();
    private CharSequence horizonAlias;
    private CharSequence masterAlias;
    private LogicalPlan master;

    @Override
    public void clear() {
        super.clear();
        offsetValues.clear();
        offsets.clear();
        slaves.clear();
        horizonAlias = null;
        masterAlias = null;
        master = null;
    }

    public CharSequence getHorizonAlias() {
        return horizonAlias;
    }

    public LogicalPlan getMaster() {
        return master;
    }

    public CharSequence getMasterAlias() {
        return masterAlias;
    }

    /**
     * Interval literals: FROM, TO and STEP for RANGE, otherwise the LIST entries in order.
     */
    public ObjList<CharSequence> getOffsets() {
        return offsets;
    }

    /**
     * Horizon offsets in the units of the master's designated timestamp, ascending.
     */
    public LongList getOffsetValues() {
        return offsetValues;
    }

    public ObjList<HorizonJoinSlave> getSlaves() {
        return slaves;
    }

    @Override
    public LogicalPlan inputAt(int index) {
        return index == 0 ? master : slaves.getQuick(index - 1).getInput();
    }

    @Override
    public int inputCount() {
        return slaves.size() + 1;
    }

    public HorizonJoinPlan of(LogicalPlan master, CharSequence masterAlias, CharSequence horizonAlias, int position) {
        this.master = Objects.requireNonNull(master);
        this.masterAlias = masterAlias;
        this.horizonAlias = horizonAlias;
        setPosition(position);
        return this;
    }

    @Override
    public void replaceInput(int index, LogicalPlan input) {
        if (index == 0) {
            master = Objects.requireNonNull(input);
        } else {
            slaves.getQuick(index - 1).setInput(input);
        }
    }
}
