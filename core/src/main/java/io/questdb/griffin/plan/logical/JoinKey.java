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
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;

/**
 * An equi-join key of a join step: a master column equal to a slave column, their names in the join condition the
 * generator prints, null where it prints none, and the SQL position of the key.
 */
public final class JoinKey implements Mutable {
    public static final ObjectFactory<JoinKey> FACTORY = JoinKey::new;
    private int masterColumnId = -1;
    private CharSequence masterName;
    private int position = -1;
    private int slaveColumnId = -1;
    private CharSequence slaveName;

    @Override
    public void clear() {
        masterColumnId = slaveColumnId = -1;
        masterName = slaveName = null;
        position = -1;
    }

    public int getMasterColumnId() {
        return masterColumnId;
    }

    public CharSequence getMasterName() {
        return masterName;
    }

    public int getPosition() {
        return position;
    }

    public int getSlaveColumnId() {
        return slaveColumnId;
    }

    public CharSequence getSlaveName() {
        return slaveName;
    }

    /**
     * True when the column is either side of the key.
     */
    public boolean isOn(int columnId) {
        return masterColumnId == columnId || slaveColumnId == columnId;
    }

    /**
     * Drops the slave name's qualifier once a filter sharing the key moves into the slave subquery.
     */
    public void unqualifySlaveName() {
        final int dot = Chars.indexOfLastUnquoted(slaveName, '.');
        if (dot > -1) {
            slaveName = slaveName.subSequence(dot + 1, slaveName.length());
        }
    }

    static boolean visitReads(ObjList<JoinKey> keys, PlanExpressionVisitor visitor) {
        boolean isChanged = false;
        for (int i = 0, n = keys.size(); i < n; i++) {
            final JoinKey key = keys.getQuick(i);
            final int columnId = PlanReads.columnId(key.masterColumnId, key.position, visitor);
            isChanged |= key.masterColumnId != columnId;
            key.masterColumnId = columnId;
        }
        for (int i = 0, n = keys.size(); i < n; i++) {
            final JoinKey key = keys.getQuick(i);
            final int columnId = PlanReads.columnId(key.slaveColumnId, key.position, visitor);
            isChanged |= key.slaveColumnId != columnId;
            key.slaveColumnId = columnId;
        }
        return isChanged;
    }

    JoinKey of(int masterColumnId, int slaveColumnId, CharSequence masterName, CharSequence slaveName, int position) {
        this.masterColumnId = masterColumnId;
        this.slaveColumnId = slaveColumnId;
        this.masterName = masterName;
        this.slaveName = slaveName;
        this.position = position;
        return this;
    }
}
