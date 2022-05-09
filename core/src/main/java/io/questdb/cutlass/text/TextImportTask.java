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

import io.questdb.mp.CountDownLatchSPI;
import io.questdb.std.LongList;

public class TextImportTask {
    private int index;
    private long chunkLo;
    private long chunkHi;
    private LongList stats;
    private CountDownLatchSPI doneLatch;

    public void of(
            int index,
            long chunkLo,
            long chunkHi,
            LongList stats,
            CountDownLatchSPI doneLatch
    ) {
        this.index = index;
        this.chunkLo = chunkLo;
        this.chunkHi = chunkHi;
        this.stats = stats;
        this.doneLatch = doneLatch;
    }

    public boolean run() {
        // process
        stats.set(index, index);
        stats.set(index + 1, index + 1);
        stats.set(index + 2, index + 2);
        stats.set(index + 3, index + 3);
        doneLatch.countDown();
        return true;
    }
}
