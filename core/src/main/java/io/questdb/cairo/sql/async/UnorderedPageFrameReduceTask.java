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

import io.questdb.std.Mutable;

/**
 * Lightweight ticket for unordered page frame reduction. Unlike {@link PageFrameReduceTask},
 * this task holds no off-heap resources and names no frame: a worker releases the queue slot,
 * then claims the sequence's next unclaimed frame via {@link UnorderedPageFrameSequence#claimFrame(long)}.
 * A ticket left over after the owner claimed every frame claims nothing and is dropped.
 */
public class UnorderedPageFrameReduceTask implements Mutable {
    private UnorderedPageFrameSequence<?> frameSequence;
    private long frameSequenceId = -1;

    @Override
    public void clear() {
        frameSequence = null;
        frameSequenceId = -1;
    }

    public UnorderedPageFrameSequence<?> getFrameSequence() {
        return frameSequence;
    }

    public long getFrameSequenceId() {
        return frameSequenceId;
    }

    public void of(UnorderedPageFrameSequence<?> seq) {
        this.frameSequence = seq;
        this.frameSequenceId = seq.getId();
    }
}
