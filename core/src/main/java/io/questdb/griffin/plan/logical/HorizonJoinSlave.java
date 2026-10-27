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

import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;
import io.questdb.std.ObjectPool;

import java.util.Objects;

public final class HorizonJoinSlave implements Mutable {
    public static final ObjectFactory<HorizonJoinSlave> FACTORY = HorizonJoinSlave::new;
    private final ObjectPool<JoinKey> keyPool = new ObjectPool<>(JoinKey.FACTORY, 1, 1);
    private final ObjList<JoinKey> keys = new ObjList<>();
    private CharSequence alias;
    private LogicalPlan input;
    private BoundExpression keyCheck;
    private int position;

    public void addKey(int masterId, int slaveId, int position) {
        keys.add(keyPool.next().of(masterId, slaveId, null, null, position));
    }

    @Override
    public void clear() {
        keys.clear();
        keyPool.clear();
        alias = null;
        input = null;
        keyCheck = null;
        position = 0;
    }

    public CharSequence getAlias() {
        return alias;
    }

    public LogicalPlan getInput() {
        return input;
    }

    /**
     * The predicate over the master row that a row of this slave can match it, see {@link JoinInput#getKeyCheck}.
     */
    public BoundExpression getKeyCheck() {
        return keyCheck;
    }

    public ObjList<JoinKey> getKeys() {
        return keys;
    }

    public int getPosition() {
        return position;
    }

    public HorizonJoinSlave of(LogicalPlan input, CharSequence alias, int position) {
        this.input = Objects.requireNonNull(input);
        this.alias = alias;
        this.position = position;
        return this;
    }

    public void setKeyCheck(BoundExpression keyCheck) {
        this.keyCheck = keyCheck;
    }

    void setInput(LogicalPlan input) {
        this.input = Objects.requireNonNull(input);
    }

    void visitReads(PlanExpressionVisitor visitor) {
        JoinKey.visitReads(keys, visitor);
        keyCheck = PlanReads.expression(keyCheck, visitor);
    }
}
