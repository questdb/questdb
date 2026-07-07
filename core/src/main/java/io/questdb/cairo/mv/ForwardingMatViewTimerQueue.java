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

package io.questdb.cairo.mv;

import io.questdb.mp.Queue;
import org.jetbrains.annotations.NotNull;

/**
 * A stable mat-view timer {@link Queue} that forwards to a swappable inner delegate.
 * <p>
 * Mirrors {@link ForwardingMatViewStateStore}: {@link MatViewStateStoreImpl} and
 * {@link MatViewTimerJob} cache the queue returned by {@code engine.getMatViewTimerQueue()} at
 * construction, so a role switch swaps this wrapper's delegate (a real concurrent queue on a
 * primary, a no-op queue on a read-only replica) rather than replacing the field, keeping every
 * cached reference in lockstep.
 */
public class ForwardingMatViewTimerQueue implements Queue<MatViewTimerTask> {
    private volatile Queue<MatViewTimerTask> delegate;

    public ForwardingMatViewTimerQueue(@NotNull Queue<MatViewTimerTask> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public void enqueue(MatViewTimerTask item) {
        delegate.enqueue(item);
    }

    public @NotNull Queue<MatViewTimerTask> getDelegate() {
        return delegate;
    }

    /**
     * Swaps the inner delegate and returns the previous one (or null when unchanged).
     */
    public Queue<MatViewTimerTask> swapDelegate(@NotNull Queue<MatViewTimerTask> newDelegate) {
        final Queue<MatViewTimerTask> previous = delegate;
        if (previous == newDelegate) {
            return null;
        }
        delegate = newDelegate;
        return previous;
    }

    @Override
    public boolean tryDequeue(MatViewTimerTask result) {
        return delegate.tryDequeue(result);
    }
}
