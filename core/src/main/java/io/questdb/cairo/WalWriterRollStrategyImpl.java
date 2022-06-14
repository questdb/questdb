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
    private long maxRowCount = Long.MAX_VALUE;
    private long maxSegmentSize = Long.MAX_VALUE;

    // create a builder instead of this constructor
    // when time period based strategy is added
    public WalWriterRollStrategyImpl(long maxSegmentSize, long maxRowCount) {
        setMaxSegmentSize(maxSegmentSize);
        setMaxRowCount(maxRowCount);
    }

    @Override
    public void setMaxSegmentSize(long maxSegmentSize) {
        if (maxSegmentSize < 1) {
            throw CairoException.instance(CairoException.METADATA_VALIDATION)
                    .put("Max segment size cannot be less than 1 byte, maxSegmentSize=").put(maxSegmentSize);
        }
        this.maxSegmentSize = maxSegmentSize;
    }

    @Override
    public void setMaxRowCount(long maxRowCount) {
        if (maxRowCount < 1) {
            throw CairoException.instance(CairoException.METADATA_VALIDATION)
                    .put("Max number of rows cannot be less than 1, maxRowCount=").put(maxRowCount);
        }
        this.maxRowCount = maxRowCount;
    }

    @Override
    public boolean shouldRoll(long segmentSize, long rowCount) {
        return segmentSize >= maxSegmentSize || rowCount >= maxRowCount;
    }

    @Override
    public boolean isMaxSegmentSizeSet() {
        return maxSegmentSize != Long.MAX_VALUE;
    }

    @Override
    public boolean isMaxRowCountSet() {
        return maxRowCount != Long.MAX_VALUE;
    }
}
