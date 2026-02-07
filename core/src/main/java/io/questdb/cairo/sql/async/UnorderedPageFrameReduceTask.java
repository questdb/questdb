/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
 * Lightweight task for unordered page frame reduction. Unlike {@link PageFrameReduceTask},
 * this task holds no off-heap resources — workers read the frame index and sequence reference,
 * then release the queue slot immediately. Per-frame resources live on the atom.
 */
public class UnorderedPageFrameReduceTask implements Mutable {
    private int frameIndex = -1;
    private UnorderedPageFrameSequence<?> frameSequence;

    @Override
    public void clear() {
        frameIndex = -1;
        frameSequence = null;
    }

    public int getFrameIndex() {
        return frameIndex;
    }

    public UnorderedPageFrameSequence<?> getFrameSequence() {
        return frameSequence;
    }

    public void of(UnorderedPageFrameSequence<?> seq, int frameIndex) {
        this.frameSequence = seq;
        this.frameIndex = frameIndex;
    }
}
