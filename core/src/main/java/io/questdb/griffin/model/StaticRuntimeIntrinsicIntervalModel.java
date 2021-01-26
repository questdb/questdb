/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.model;

import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.LongList;
import io.questdb.std.datetime.microtime.Timestamps;

    public class StaticRuntimeIntrinsicIntervalModel implements RuntimeIntrinsicIntervalModel {
    private final LongList intervals;

    public StaticRuntimeIntrinsicIntervalModel(LongList intervals) {
        this.intervals = intervals;
    }

    @Override
    public LongList calculateIntervals(SqlExecutionContext sqlContext) {
        return intervals;
    }

    @Override
    public boolean isFocused(Timestamps.TimestampFloorMethod floorDd) {
        return isFocused(intervals, floorDd);
    }

    private boolean isFocused(LongList intervals, Timestamps.TimestampFloorMethod floorMethod) {
        long floor = floorMethod.floor(intervals.getQuick(0));
        for (int i = 1, n = intervals.size(); i < n; i++) {
            if (floor != floorMethod.floor(intervals.getQuick(i))) {
                return false;
            }
        }
        return true;
    }
}
