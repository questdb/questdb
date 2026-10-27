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
    private int lookupKeyIndex = -1;
    private int masterColumnId = -1;
    private CharSequence masterName;
    private int position = -1;
    private int slaveColumnId = -1;
    private CharSequence slaveName;

    /**
     * The number of keys the join looks up, see {@link #getLookupKeyIndex}.
     */
    public static int lookupKeyCount(ObjList<JoinKey> keys) {
        int count = 0;
        for (int i = 0, n = keys.size(); i < n; i++) {
            if (keys.getQuick(i).lookupKeyIndex == -1) {
                count++;
            }
        }
        return count;
    }

    @Override
    public void clear() {
        lookupKeyIndex = -1;
        masterColumnId = slaveColumnId = -1;
        masterName = slaveName = null;
        position = -1;
    }

    /**
     * The index of the earlier key on the same slave column that the join looks up instead of this key, which operator
     * planning records for an ASOF, LT or HORIZON JOIN, or -1 when the join looks this key up. The join compares the
     * master column of this key with the master column of that key and answers a master row whose two columns differ
     * with no slave row, since no slave row equals both.
     */
    public int getLookupKeyIndex() {
        return lookupKeyIndex;
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

    public void setLookupKeyIndex(int lookupKeyIndex) {
        this.lookupKeyIndex = lookupKeyIndex;
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
        this.lookupKeyIndex = -1;
        this.masterColumnId = masterColumnId;
        this.slaveColumnId = slaveColumnId;
        this.masterName = masterName;
        this.slaveName = slaveName;
        this.position = position;
        return this;
    }
}
