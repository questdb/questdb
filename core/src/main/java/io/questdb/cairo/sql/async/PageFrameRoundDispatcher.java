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

import io.questdb.std.LongList;

/**
 * Runs rounds of tasks on the shared query workers, apart from any page frames: a parallel hash
 * join build dispatches its partition, build and copy rounds through one. The tasks of a round
 * run a reducer of their own, which receives the task index where a frame reducer receives the
 * frame index.
 */
public interface PageFrameRoundDispatcher {

    /**
     * Dispatches one task per entry of {@code taskRowCounts}, which holds the rows each task
     * processes, and waits for them, the owner thread work-stealing while it waits. Whether this
     * returns or throws, no task of the round is still queued or running, so the caller may
     * release what the round's tasks used.
     *
     * @param roundReducer  the reducer each task of the round runs
     * @param taskRowCounts the rows of each task, borrowed for the round
     */
    void dispatchRoundAndAwait(UnorderedPageFrameReducer roundReducer, LongList taskRowCounts);
}
