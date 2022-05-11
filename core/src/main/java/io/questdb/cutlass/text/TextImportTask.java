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

import io.questdb.cairo.CairoException;
import io.questdb.griffin.SqlException;
import io.questdb.mp.CountDownLatchSPI;
import io.questdb.std.LongList;

public class TextImportTask {

    public static final byte PHASE_BOUNDARY_CHECK = 1;
    public static final byte PHASE_INDEXING = 2;

    private int index;

    private FileSplitter splitter;

    private long chunkLo;
    private long chunkHi;
    private LongList stats;
    private CountDownLatchSPI doneLatch;
    private byte phase;

    public void of(
            int index,
            FileSplitter splitter,
            long chunkLo,
            long chunkHi,
            LongList stats,
            CountDownLatchSPI doneLatch,
            byte phase
    ) {
        this.index = index;
        this.splitter = splitter;
        this.chunkLo = chunkLo;
        this.chunkHi = chunkHi;
        this.stats = stats;
        this.doneLatch = doneLatch;
        this.phase = phase;
    }

    public boolean run() {
        try {
            if (phase == PHASE_BOUNDARY_CHECK) {
                splitter.countQuotes(chunkLo, chunkHi, stats, index);
            } else if (phase == PHASE_INDEXING) {
                splitter.index(chunkLo, chunkHi, index);
            } else {
                throw new RuntimeException("Unexpected phase " + phase);
            }
        } catch (SqlException e) {
            e.printStackTrace();//TODO: how can we react to job failing 
        }

        doneLatch.countDown();
        return true;
    }
}
