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

/**
 * A node that defines no column of its own: its output is its input's columns, attribute for attribute, under
 * the designated timestamp {@link #derivedTimestampIndex()} names. {@link #deriveOutput()} is the one rule that
 * lays the output out; {@link #replaceInput} applies it, so a rewrite that gives the node another input keeps the
 * output in step, and a rewrite that changes the input's output in place calls {@link #deriveOutput()}.
 */
public abstract sealed class ForwardingPlan extends UnaryPlan
        permits DistinctPlan, FillPlan, FilterPlan, LatestByPlan, LimitPlan, SortPlan {

    public final void deriveOutput() {
        final OutputSchema output = getOutput();
        output.copyFrom(getInput().getOutput());
        output.setTimestampIndex(derivedTimestampIndex());
    }

    /**
     * The index, in the input's layout, of the column this node designates: the input's own timestamp, unless the
     * node orders or fills by another column.
     */
    public int derivedTimestampIndex() {
        return getInput().getOutput().getTimestampIndex();
    }

    @Override
    public void replaceInput(int index, LogicalPlan input) {
        super.replaceInput(index, input);
        deriveOutput();
    }
}
