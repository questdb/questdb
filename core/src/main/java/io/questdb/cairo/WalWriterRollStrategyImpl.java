/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cairo;

public class WalWriterRollStrategyImpl implements WalWriterRollStrategy {
    private long maxRowCount = Long.MAX_VALUE;      // number of rows
    private long maxSegmentSize = Long.MAX_VALUE;   // storage size in bytes
    private long rollInterval = Long.MAX_VALUE;     // roll interval in millis

    @Override
    public void setMaxSegmentSize(long maxSegmentSize) {
        if (maxSegmentSize < 1) {
            throw CairoException.critical(CairoException.METADATA_VALIDATION)
                    .put("Max segment size cannot be less than 1 byte, maxSegmentSize=").put(maxSegmentSize);
        }
        this.maxSegmentSize = maxSegmentSize;
    }

    @Override
    public void setMaxRowCount(long maxRowCount) {
        if (maxRowCount < 1) {
            throw CairoException.critical(CairoException.METADATA_VALIDATION)
                    .put("Max number of rows cannot be less than 1, maxRowCount=").put(maxRowCount);
        }
        this.maxRowCount = maxRowCount;
    }

    @Override
    public void setRollInterval(long rollInterval) {
        if (rollInterval < 1) {
            throw CairoException.critical(CairoException.METADATA_VALIDATION)
                    .put("Roll interval cannot be less than 1 millisecond, rollInterval=").put(rollInterval);
        }
        this.rollInterval = rollInterval;
    }

    // segmentSize in bytes, segmentAge in millis
    @Override
    public boolean shouldRoll(long segmentSize, long rowCount, long segmentAge) {
        return segmentSize >= maxSegmentSize
                || rowCount >= maxRowCount
                || segmentAge >= rollInterval;
    }

    @Override
    public boolean isMaxSegmentSizeSet() {
        return maxSegmentSize != Long.MAX_VALUE;
    }

    @Override
    public boolean isMaxRowCountSet() {
        return maxRowCount != Long.MAX_VALUE;
    }

    @Override
    public boolean isRollIntervalSet() {
        return rollInterval != Long.MAX_VALUE;
    }
}
