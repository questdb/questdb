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

import io.questdb.cairo.ColumnType;
import io.questdb.std.IntList;
import io.questdb.std.ObjectFactory;

public final class SortPlan extends UnaryPlan {
    public static final ObjectFactory<SortPlan> FACTORY = SortPlan::new;
    private final IntList columnIds = new IntList();
    private final IntList directions = new IntList();
    private boolean hasAliasedKey;
    private boolean isLimited = true;
    private boolean isMarkoutHorizon;
    private boolean isReversal;

    @Override
    public void clear() {
        super.clear();
        columnIds.clear();
        directions.clear();
        hasAliasedKey = false;
        isLimited = true;
        isMarkoutHorizon = false;
        isReversal = false;
    }

    /**
     * Sets the output to the input's columns. The sorted rows are ordered by the first key, so it is
     * the designated timestamp when it is a timestamp; otherwise the output has none.
     */
    public void deriveOutput() {
        final OutputSchema output = getOutput();
        output.copyFrom(getInput().getOutput());
        final int firstIndex = output.getColumnIndexById(columnIds.getQuick(0));
        output.setTimestampIndex(ColumnType.isTimestamp(output.getColumnType(firstIndex)) ? firstIndex : -1);
    }

    public IntList getColumnIds() {
        return columnIds;
    }

    public IntList getDirections() {
        return directions;
    }

    @Override
    public Type getType() {
        return Type.SORT;
    }

    /**
     * True when an ORDER BY key names a computing projection's alias of an input column.
     */
    public boolean hasAliasedKey() {
        return hasAliasedKey;
    }

    /**
     * False when the query level that orders by this sort has no LIMIT.
     */
    public boolean isLimited() {
        return isLimited;
    }

    /**
     * True when a markout horizon join below delivers this order whenever it runs.
     */
    public boolean isMarkoutHorizon() {
        return isMarkoutHorizon;
    }

    /**
     * True when this sort reverses a negative LIMIT; it scans backward even over a filter.
     */
    public boolean isReversal() {
        return isReversal;
    }

    public void markAliasedKey() {
        hasAliasedKey = true;
    }

    public void markMarkoutHorizon() {
        isMarkoutHorizon = true;
    }

    public void markReversal() {
        isReversal = true;
    }

    public void markUnlimited() {
        isLimited = false;
    }

    public SortPlan of(LogicalPlan input, int position) {
        configure(input, position);
        return this;
    }
}
