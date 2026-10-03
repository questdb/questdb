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
import io.questdb.std.ObjectFactory;

import java.util.Objects;

public final class HorizonJoinSlave implements Mutable {
    public static final ObjectFactory<HorizonJoinSlave> FACTORY = HorizonJoinSlave::new;
    private final IntList keyPositions = new IntList();
    private final IntList masterKeyColumnIds = new IntList();
    private final IntList slaveKeyColumnIds = new IntList();
    private CharSequence alias;
    private LogicalPlan input;
    private int position;

    @Override
    public void clear() {
        keyPositions.clear();
        masterKeyColumnIds.clear();
        slaveKeyColumnIds.clear();
        alias = null;
        input = null;
        position = 0;
    }

    public CharSequence getAlias() {
        return alias;
    }

    public LogicalPlan getInput() {
        return input;
    }

    public IntList getKeyPositions() {
        return keyPositions;
    }

    public IntList getMasterKeyColumnIds() {
        return masterKeyColumnIds;
    }

    public int getPosition() {
        return position;
    }

    public IntList getSlaveKeyColumnIds() {
        return slaveKeyColumnIds;
    }

    public HorizonJoinSlave of(LogicalPlan input, CharSequence alias, int position) {
        this.input = Objects.requireNonNull(input);
        this.alias = alias;
        this.position = position;
        return this;
    }

    void setInput(LogicalPlan input) {
        this.input = Objects.requireNonNull(input);
    }
}
