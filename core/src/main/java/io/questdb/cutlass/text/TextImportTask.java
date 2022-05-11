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
    private int index;

    private FileIndexer indexer;

    private long chunkLo;
    private long chunkHi;
    private LongList stats;
    private CountDownLatchSPI doneLatch;

    public void of(
            int index,
            FileIndexer splitter,
            long chunkLo,
            long chunkHi,
            LongList stats,
            CountDownLatchSPI doneLatch
    ) {
        this.index = index;
        this.indexer = splitter;
        this.chunkLo = chunkLo;
        this.chunkHi = chunkHi;
        this.stats = stats;
        this.doneLatch = doneLatch;
    }

    public boolean run() {
        long fd = indexer.getFf().openRO(indexer.getInputFilePath());
        if (fd < 0)
            throw CairoException.instance(indexer.getFf().errno()).put("could not open read-only [file=").put(indexer.getInputFilePath()).put(']');

        try {
            FileIndexer.countQuotes(fd, chunkLo, chunkHi, stats, index, indexer.getBufferLength(), indexer.getFf());
        } catch (SqlException e) {
            e.printStackTrace();//TODO: fix 
        } finally {
            indexer.getFf().close(fd);
        }
        doneLatch.countDown();
        return true;
    }
}
