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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.IntHashJoinBuild;
import io.questdb.griffin.engine.join.MapHashJoinBuild;
import io.questdb.griffin.engine.table.HashJoinBuildFrames;

/**
 * Builds from a table's page frames as the fused operator's owner does, without a filter, and
 * freezes over the frames, so that probes read real payload columns at the kept row ids. The
 * caller keeps the frames open until every probe closes, then closes the build and the frames.
 */
final class FrameBuilds {
    private FrameBuilds() {
    }

    static FrozenHashJoinBuild.IntKeyed buildInt(
            CairoConfiguration configuration,
            IntHashJoinBuild build,
            HashJoinBuildFrames frames,
            RecordCursorFactory factory,
            int keyColumn,
            SqlExecutionContext context
    ) throws SqlException {
        frames.of(factory, context);
        build.open(context.getMemoryTracker(), context.getCircuitBreaker());
        try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
             PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)) {
            pool.of(frames.getAddressCache());
            record.of(frames.getSymbolTableSource());
            for (int frameIndex = 0; frameIndex < frames.getFrameCount(); frameIndex++) {
                record.init(pool.navigateTo(frameIndex));
                build.appendFrame(record, keyColumn, frames.getFrameRowCount(frameIndex));
            }
        }
        return build.freeze(frames);
    }

    static FrozenHashJoinBuild.RecordKeyed buildMap(
            CairoConfiguration configuration,
            MapHashJoinBuild build,
            HashJoinBuildFrames frames,
            RecordCursorFactory factory,
            RecordSink keySink,
            SqlExecutionContext context
    ) throws SqlException {
        frames.of(factory, context);
        build.open(context.getMemoryTracker(), context.getCircuitBreaker());
        try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
             PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)) {
            pool.of(frames.getAddressCache());
            record.of(frames.getSymbolTableSource());
            for (int frameIndex = 0; frameIndex < frames.getFrameCount(); frameIndex++) {
                record.init(pool.navigateTo(frameIndex));
                build.appendFrame(record, keySink, frames.getFrameRowCount(frameIndex));
            }
        }
        return build.freeze(frames);
    }
}
