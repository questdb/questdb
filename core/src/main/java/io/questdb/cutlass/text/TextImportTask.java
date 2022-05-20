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

package io.questdb.cutlass.text;

import io.questdb.griffin.SqlException;
import io.questdb.mp.CountDownLatchSPI;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;

public class TextImportTask {

    public static final byte PHASE_BOUNDARY_CHECK = 1;
    public static final byte PHASE_INDEXING = 2;
    public static final byte PHASE_INDEX_MERGE = 3;

    private CountDownLatchSPI doneLatch;
    private byte phase;
    private FileIndexer.TaskContext context;
    private int index;
    private long lo;
    private long hi;
    private long lineNumber;
    private LongList chunkStats;
    private ObjList<CharSequence> partitionNames;

    public void of(
            CountDownLatchSPI doneLatch,
            byte phase,
            FileIndexer.TaskContext context,
            int index,
            long lo,
            long hi,
            long lineNumber,
            LongList chunkStats
    ) {
        this.doneLatch = doneLatch;
        this.phase = phase;
        this.context = context;
        this.index = index;
        this.lo = lo;
        this.hi = hi;
        this.lineNumber = lineNumber;
        this.chunkStats = chunkStats;
    }

    public void of(
            CountDownLatchSPI doneLatch,
            byte phase,
            FileIndexer.TaskContext context,
            int index,
            long lo,
            long hi,
            ObjList<CharSequence> partitionNames
    ) {
        this.doneLatch = doneLatch;
        this.phase = phase;
        this.context = context;
        this.index = index;
        this.lo = lo;
        this.hi = hi;
        this.partitionNames = partitionNames;
    }

    public boolean run() {
        try {
            if (phase == PHASE_BOUNDARY_CHECK) {
                context.countQuotesStage(index, lo, hi, chunkStats);
            } else if (phase == PHASE_INDEXING) {
                context.buildIndexStage(index, lo, hi, lineNumber);
            } else if (phase == PHASE_INDEX_MERGE) {
                context.mergeIndexStage(index, lo, hi, partitionNames);
            } else {
                throw new RuntimeException("Unexpected phase " + phase);
            }
        } catch (Throwable t) {
            t.printStackTrace();//TODO: how can we react to job failing
            return false;
        } finally {
            doneLatch.countDown();
        }
        
        return true;
    }
}
