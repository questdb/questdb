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

package io.questdb.cairo.lv;

import org.jetbrains.annotations.NotNull;

/**
 * Immutable compiler-owned union of the finite RANGE dependencies in one live
 * view. The plan is present when the view's {@code RANGE W PRECEDING ... CURRENT
 * ROW} functions share one partition/order domain and each holds frame-local
 * state; it says nothing about window functions of another kind, which the ROWS
 * and anchor plans describe instead.
 * <p>
 * The union is what a localized out-of-order repair plans against: it takes the
 * widest look-behind of any RANGE function in the view, because the dependency
 * floor has to satisfy every one of them at once. The partition/order signatures
 * and the timestamp type are shared by construction, so the plan carries one
 * copy.
 */
public final class LiveViewCheckpointRangePlan {
    private final int functionCount;
    private final long maxFrameWidth;
    private final String orderSignature;
    private final String partitionSignature;
    private final int timestampType;

    public LiveViewCheckpointRangePlan(
            int functionCount,
            long maxFrameWidth,
            @NotNull CharSequence partitionSignature,
            @NotNull CharSequence orderSignature,
            int timestampType
    ) {
        if (functionCount < 1 || maxFrameWidth < 0) {
            throw new IllegalArgumentException("invalid RANGE dependency plan");
        }
        this.functionCount = functionCount;
        this.maxFrameWidth = maxFrameWidth;
        this.partitionSignature = partitionSignature.toString();
        this.orderSignature = orderSignature.toString();
        this.timestampType = timestampType;
    }

    public int getFunctionCount() {
        return functionCount;
    }

    /**
     * Returns the widest finite preceding width {@code W} across the view's RANGE
     * functions, in the designated timestamp column's native units. A repair floor
     * derived from this width satisfies every function in the view.
     */
    public long getMaxFrameWidth() {
        return maxFrameWidth;
    }

    public String getOrderSignature() {
        return orderSignature;
    }

    public String getPartitionSignature() {
        return partitionSignature;
    }

    public int getTimestampType() {
        return timestampType;
    }
}
