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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;

/**
 * A parallel hash join as the planner compiled it: the left input, which the join streams, its
 * stages, one per join, and the joined rows' metadata, which lists the left input's columns and
 * then every stage's build columns, as the serial joins' metadata does.
 * <p>
 * The spec also records how the left input as compiled answered the two questions a parent asks a
 * join about its order, {@code followedOrderByAdvice()} and {@code getScanDirection()}: the join
 * answers them as the serial join does, by asking its left input, and the left input it keeps is
 * the frame scan the planner peeled from what the query compiled.
 * <p>
 * The spec owns the inputs, the stages and the metadata.
 */
public final class AsyncHashJoinSpec implements QuietCloseable {
    private final boolean isLeftOrderByAdviceFollowed;
    private final HashJoinInput leftInput;
    private final int leftScanDirection;
    private final ObjList<HashJoinStageSpec> stages = new ObjList<>();
    private RecordMetadata metadata;

    /**
     * Takes ownership of the left input, the stage and the metadata.
     *
     * @param leftInput                   the input the join streams
     * @param stage                       the first join
     * @param metadata                    the joined rows' metadata
     * @param isLeftOrderByAdviceFollowed the compiled left input's {@code followedOrderByAdvice()}
     * @param leftScanDirection           the compiled left input's {@code getScanDirection()}
     */
    public AsyncHashJoinSpec(
            HashJoinInput leftInput,
            HashJoinStageSpec stage,
            RecordMetadata metadata,
            boolean isLeftOrderByAdviceFollowed,
            int leftScanDirection
    ) {
        this.leftInput = leftInput;
        this.metadata = metadata;
        this.isLeftOrderByAdviceFollowed = isLeftOrderByAdviceFollowed;
        this.leftScanDirection = leftScanDirection;
        stages.add(stage);
    }

    @Override
    public void close() {
        Throwable failure = Misc.freeObjListBestEffort(null, stages);
        failure = Misc.freeBestEffort(failure, leftInput);
        failure = Misc.freeIfCloseableBestEffort(failure, metadata);
        metadata = null;
        CairoException.rethrowCleanupFailure(failure);
    }

    public HashJoinInput getLeftInput() {
        return leftInput;
    }

    /**
     * The compiled left input's {@code getScanDirection()}, which the join returns as its own.
     */
    public int getLeftScanDirection() {
        return leftScanDirection;
    }

    public RecordMetadata getMetadata() {
        return metadata;
    }

    public HashJoinStageSpec getStage(int stage) {
        return stages.getQuick(stage);
    }

    public int getStageCount() {
        return stages.size();
    }

    /**
     * The compiled left input's {@code followedOrderByAdvice()}, which the join returns as its own.
     */
    public boolean isLeftOrderByAdviceFollowed() {
        return isLeftOrderByAdviceFollowed;
    }

    /**
     * True when the left input or a stage's build input reads data from outside the database.
     */
    public boolean usesExternalDataSource() {
        if (leftInput.usesExternalDataSource()) {
            return true;
        }
        for (int i = 0, n = stages.size(); i < n; i++) {
            if (stages.getQuick(i).getBuildInput().usesExternalDataSource()) {
                return true;
            }
        }
        return false;
    }
}
