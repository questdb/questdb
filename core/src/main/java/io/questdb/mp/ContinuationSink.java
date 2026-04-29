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

package io.questdb.mp;

/**
 * Sink onto which a {@link WorkerContinuation} pushes itself when it wants to be
 * remounted on a worker. The pool that hosts the originating SQL evaluation owns
 * the sink (typically a {@link ContinuationQueue} assigned to that pool's
 * workers), so a parked body always resumes on a worker from the pool that
 * launched it.
 *
 * <p>The cont captures its sink at construction; nothing else in the codebase
 * needs to know which pool will drive the resume.
 */
@FunctionalInterface
public interface ContinuationSink {
    /**
     * Schedule {@code cont} for resumption. Must be safe to call concurrently and
     * idempotent against double-puts of the same continuation reference (the
     * caller is expected to gate via {@code TxnWaiter.tryFire} / {@code tryCancel}).
     */
    void put(WorkerContinuation cont);
}
