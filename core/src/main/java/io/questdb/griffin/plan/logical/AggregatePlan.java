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

/**
 * A GROUP BY grouping, or a decorrelation domain that may re-read a join input it shares with its master.
 */
public final class AggregatePlan extends GroupingPlan {
    public static final ObjectFactory<AggregatePlan> FACTORY = AggregatePlan::new;
    private final IntList sharedInputIds = new IntList();
    private final IntList sharedSourceIds = new IntList();
    private JoinInput sharedSource;

    @Override
    public void clear() {
        super.clear();
        sharedInputIds.clear();
        sharedSourceIds.clear();
        sharedSource = null;
    }

    /**
     * Input column ids of a relation that re-reads {@link #getSharedSource()}, paired with
     * {@link #getSharedSourceIds()}, so the generator can read the source's factory instead.
     */
    public IntList getSharedInputIds() {
        return sharedInputIds;
    }

    public JoinInput getSharedSource() {
        return sharedSource;
    }

    public IntList getSharedSourceIds() {
        return sharedSourceIds;
    }

    public AggregatePlan of(LogicalPlan input, int position) {
        configure(input, position);
        return this;
    }

    public void setSharedSource(JoinInput sharedSource) {
        this.sharedSource = sharedSource;
    }
}
