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
import io.questdb.cairo.map.MapProbeView;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.IntHashJoinBuild;
import io.questdb.griffin.engine.join.MapHashJoinBuild;
import io.questdb.griffin.engine.table.HashJoinBuildFrames;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Rows;
import org.jetbrains.annotations.Nullable;

import java.util.function.LongPredicate;

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

    /**
     * Builds from the open frames as the fused operator's parallel build does, running every frame
     * task and then every partition task on this thread: the frames in order, the partitions in
     * reverse order, so that no partition relies on an earlier one having been built. {@code keep}
     * picks the rows a frame keeps, by row id, as the build filters would; null keeps every row. On
     * failure the build is closed, as the operator closes it once the failed round has drained.
     */
    static FrozenHashJoinBuild.IntKeyed buildIntPartitioned(
            CairoConfiguration configuration,
            IntHashJoinBuild build,
            HashJoinBuildFrames frames,
            int keyColumn,
            long rowsPerPartition,
            long keyCountHint,
            @Nullable LongPredicate keep,
            @Nullable MemoryTracker memoryTracker,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        build.open(memoryTracker, circuitBreaker);
        try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
             PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
             DirectLongList rows = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT)) {
            build.beginPartitioning(frames.getFrameCount(), frames.getRowCount(), rowsPerPartition);
            pool.of(frames.getAddressCache());
            record.of(frames.getSymbolTableSource());
            for (int frameIndex = 0; frameIndex < frames.getFrameCount(); frameIndex++) {
                record.init(pool.navigateTo(frameIndex));
                final long rowCount = frames.getFrameRowCount(frameIndex);
                if (keep == null) {
                    build.partitionFrame(frameIndex, record, keyColumn, rowCount);
                } else {
                    rows.clear();
                    for (long row = 0; row < rowCount; row++) {
                        if (keep.test(Rows.toRowID(frameIndex, row))) {
                            rows.add(row);
                        }
                    }
                    build.partitionFrame(frameIndex, record, keyColumn, rows);
                }
            }
            final int partitionCount = build.planPartitions(rowsPerPartition, keyCountHint);
            for (int partition = partitionCount - 1; partition >= 0; partition--) {
                build.buildPartition(partition, circuitBreaker);
            }
            return build.freezePartitioned(frames);
        } catch (Throwable th) {
            build.close();
            throw th;
        }
    }

    /**
     * The staged-key twin of {@link #buildIntPartitioned}: every frame task stages its keys through
     * the given sink and one stager of the build's, which this thread keeps for the whole build and
     * closes at its end, as a worker slot closes its own at the end of an execution.
     */
    static FrozenHashJoinBuild.RecordKeyed buildMapPartitioned(
            CairoConfiguration configuration,
            MapHashJoinBuild build,
            HashJoinBuildFrames frames,
            RecordSink keySink,
            long rowsPerPartition,
            long keyCountHint,
            @Nullable LongPredicate keep,
            @Nullable MemoryTracker memoryTracker,
            SqlExecutionCircuitBreaker circuitBreaker
    ) {
        build.open(memoryTracker, circuitBreaker);
        try (PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration);
             PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
             DirectLongList rows = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT);
             MapProbeView stager = build.newKeyStager()) {
            build.beginPartitioning(frames.getFrameCount(), frames.getRowCount(), rowsPerPartition);
            pool.of(frames.getAddressCache());
            record.of(frames.getSymbolTableSource());
            for (int frameIndex = 0; frameIndex < frames.getFrameCount(); frameIndex++) {
                record.init(pool.navigateTo(frameIndex));
                final long rowCount = frames.getFrameRowCount(frameIndex);
                if (keep == null) {
                    build.partitionFrame(frameIndex, record, keySink, stager, rowCount);
                } else {
                    rows.clear();
                    for (long row = 0; row < rowCount; row++) {
                        if (keep.test(Rows.toRowID(frameIndex, row))) {
                            rows.add(row);
                        }
                    }
                    build.partitionFrame(frameIndex, record, keySink, stager, rows);
                }
            }
            final int partitionCount = build.planPartitions(rowsPerPartition, keyCountHint);
            for (int partition = partitionCount - 1; partition >= 0; partition--) {
                build.buildPartition(partition, circuitBreaker);
            }
            return build.freezePartitioned(frames);
        } catch (Throwable th) {
            build.close();
            throw th;
        }
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
