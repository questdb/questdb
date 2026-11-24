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

package io.questdb.test.cairo.fuzz;

import io.questdb.cairo.O3ParquetCounters;
import io.questdb.log.Log;

/**
 * The parquet O3 paths taken since this object was created, read from
 * {@link O3ParquetCounters}. The counters are process-wide, so a test takes a
 * baseline at its start instead of resetting them.
 */
public class O3ParquetPathCounts {
    private final long dedupIdenticalBase = O3ParquetCounters.getDedupIdenticalCount();
    private final long dropInPlaceBase = O3ParquetCounters.getDropInPlaceCount();
    private final long dropRewriteBase = O3ParquetCounters.getDropRewriteCount();
    private final long inPlaceBase = O3ParquetCounters.getInPlaceCount();
    private final long replaceNoopBase = O3ParquetCounters.getReplaceNoopCount();
    private final long replaceRemovalBase = O3ParquetCounters.getReplaceRemovalCount();
    private final long rewriteBase = O3ParquetCounters.getRewriteCount();

    public long getDedupIdentical() {
        return O3ParquetCounters.getDedupIdenticalCount() - dedupIdenticalBase;
    }

    public long getDropInPlace() {
        return O3ParquetCounters.getDropInPlaceCount() - dropInPlaceBase;
    }

    public long getDropRewrite() {
        return O3ParquetCounters.getDropRewriteCount() - dropRewriteBase;
    }

    public long getInPlace() {
        return O3ParquetCounters.getInPlaceCount() - inPlaceBase;
    }

    public long getReplaceNoop() {
        return O3ParquetCounters.getReplaceNoopCount() - replaceNoopBase;
    }

    public long getReplaceRemoval() {
        return O3ParquetCounters.getReplaceRemovalCount() - replaceRemovalBase;
    }

    public long getRewrite() {
        return O3ParquetCounters.getRewriteCount() - rewriteBase;
    }

    /**
     * Parquet O3 partition tasks that wrote the partition: in place or by rewrite.
     */
    public long getWritten() {
        return getInPlace() + getRewrite();
    }

    /**
     * Logs the counts, so CI history shows when a fuzz test drifts off a path.
     */
    public void log(Log log, CharSequence testName) {
        log.info().$("parquet o3 paths [test=").$safe(testName)
                .$(", inPlace=").$(getInPlace())
                .$(", rewrite=").$(getRewrite())
                .$(", dropInPlace=").$(getDropInPlace())
                .$(", dropRewrite=").$(getDropRewrite())
                .$(", replaceNoop=").$(getReplaceNoop())
                .$(", replaceRemoval=").$(getReplaceRemoval())
                .$(", dedupIdentical=").$(getDedupIdentical())
                .I$();
    }

    @Override
    public String toString() {
        return "inPlace=" + getInPlace()
                + ", rewrite=" + getRewrite()
                + ", dropInPlace=" + getDropInPlace()
                + ", dropRewrite=" + getDropRewrite()
                + ", replaceNoop=" + getReplaceNoop()
                + ", replaceRemoval=" + getReplaceRemoval()
                + ", dedupIdentical=" + getDedupIdentical();
    }
}
