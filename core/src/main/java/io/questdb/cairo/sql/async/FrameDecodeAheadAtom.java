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


package io.questdb.cairo.sql.async;

import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import org.jetbrains.annotations.Nullable;

/**
 * An atom whose frames the workers can decode ahead of a fold on the query's thread. A sequence
 * whose rows are below its base's local reduce threshold, but whose frames are too many to decode
 * on one thread, first runs a round in which each covered frame is decoded into the memory pool of
 * the slot that takes it, then reduces every frame on the query's thread, in frame order, reading a
 * decoded frame from the pool that decoded it. The fold therefore sees the rows in the order of a
 * local reduce, and its results are bit for bit those of a local reduce, also for aggregates whose
 * result depends on the order they see rows in (a DOUBLE sum, for instance).
 */
public interface FrameDecodeAheadAtom {

    /**
     * The memory pool of a slot, as {@link #maybeAcquire} returns it; -1 is the owner's.
     */
    PageFrameMemoryPool getDecodeMemoryPool(int slotId);

    /**
     * Takes a slot as the atom's reducers do; a {@link #release(int)} must follow.
     */
    int maybeAcquire(int workerId, boolean owner, SqlExecutionCircuitBreaker circuitBreaker);

    void release(int slotId);

    /**
     * Makes the owner's reducers read frames through {@code pool} instead of the owner's own pool,
     * until called with null. Only the owner calls it, and only while no worker reduces a frame of
     * this atom.
     */
    void setOwnerMemoryPool(@Nullable PageFrameMemoryPool pool);
}
