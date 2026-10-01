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
import io.questdb.std.ObjectFactory;

public final class LatestByPlan extends UnaryPlan {
    public static final ObjectFactory<LatestByPlan> FACTORY = LatestByPlan::new;
    private final IntList keyColumnIds = new IntList();
    private boolean isTimestampOrderInherited;
    private int timestampColumnId = -1;

    @Override
    public void clear() {
        super.clear();
        keyColumnIds.clear();
        isTimestampOrderInherited = false;
        timestampColumnId = -1;
    }

    public IntList getKeyColumnIds() {
        return keyColumnIds;
    }

    public int getTimestampColumnId() {
        return timestampColumnId;
    }

    @Override
    public Type getType() {
        return Type.LATEST_BY;
    }

    public boolean isTimestampOrderInherited() {
        return isTimestampOrderInherited;
    }

    public LatestByPlan of(LogicalPlan input, int timestampColumnId, int position) {
        configure(input, position);
        this.timestampColumnId = timestampColumnId;
        return this;
    }

    public void setTimestampOrderInherited(boolean isTimestampOrderInherited) {
        this.isTimestampOrderInherited = isTimestampOrderInherited;
    }
}
