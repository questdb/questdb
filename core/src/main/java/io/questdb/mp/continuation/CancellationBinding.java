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

package io.questdb.mp.continuation;

import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;

public final class CancellationBinding {
    public static final long NO_GENERATION = -1;
    private AtomicBoolean flag;
    private long generation = NO_GENERATION;
    private volatile long version;

    public CancellationBinding() {
    }

    public CancellationBinding(AtomicBoolean flag) {
        set(flag);
    }

    public boolean cancel() {
        AtomicBoolean flag;
        long generation;
        while (true) {
            final long version = this.version;
            if ((version & 1) != 0) {
                Os.pause();
                continue;
            }
            flag = this.flag;
            generation = this.generation;
            Unsafe.loadFence();
            if (version == this.version) {
                break;
            }
        }
        if (flag instanceof FiberCancellationSignal signal) {
            return signal.cancel(generation);
        }
        if (flag != null) {
            flag.set(true);
            return true;
        }
        return false;
    }

    public synchronized void clear() {
        publish(null, NO_GENERATION);
    }

    public synchronized void clear(AtomicBoolean expected) {
        if (flag == expected) {
            publish(null, NO_GENERATION);
        }
    }

    public synchronized void clear(AtomicBoolean expected, long expectedGeneration) {
        if (flag == expected && generation == expectedGeneration) {
            publish(null, NO_GENERATION);
        }
    }

    public void copyTo(CancellationBinding target) {
        if (target == this) {
            return;
        }
        AtomicBoolean flag;
        long generation;
        while (true) {
            final long version = this.version;
            if ((version & 1) != 0) {
                Os.pause();
                continue;
            }
            flag = this.flag;
            generation = this.generation;
            Unsafe.loadFence();
            if (version == this.version) {
                break;
            }
        }
        target.set(flag, generation);
    }

    public @Nullable AtomicBoolean getFlag() {
        AtomicBoolean flag;
        while (true) {
            final long version = this.version;
            if ((version & 1) != 0) {
                Os.pause();
                continue;
            }
            flag = this.flag;
            Unsafe.loadFence();
            if (version == this.version) {
                break;
            }
        }
        return flag;
    }

    public long getGeneration(AtomicBoolean expected) {
        AtomicBoolean flag;
        long generation;
        while (true) {
            final long version = this.version;
            if ((version & 1) != 0) {
                Os.pause();
                continue;
            }
            flag = this.flag;
            generation = this.generation;
            Unsafe.loadFence();
            if (version == this.version) {
                break;
            }
        }
        return flag == expected ? generation : NO_GENERATION;
    }

    public boolean isCancelled() {
        AtomicBoolean flag;
        long generation;
        while (true) {
            final long version = this.version;
            if ((version & 1) != 0) {
                Os.pause();
                continue;
            }
            flag = this.flag;
            generation = this.generation;
            Unsafe.loadFence();
            if (version == this.version) {
                break;
            }
        }
        return flag instanceof FiberCancellationSignal signal
                ? signal.isCancelled(generation)
                : flag != null && flag.get();
    }

    public boolean isCancelledOrUnbound() {
        AtomicBoolean flag;
        long generation;
        while (true) {
            final long version = this.version;
            if ((version & 1) != 0) {
                Os.pause();
                continue;
            }
            flag = this.flag;
            generation = this.generation;
            Unsafe.loadFence();
            if (version == this.version) {
                break;
            }
        }
        return flag instanceof FiberCancellationSignal signal
                ? signal.isCancelled(generation)
                : flag == null || flag.get();
    }

    public void reset() {
        AtomicBoolean flag;
        long generation;
        while (true) {
            final long version = this.version;
            if ((version & 1) != 0) {
                Os.pause();
                continue;
            }
            flag = this.flag;
            generation = this.generation;
            Unsafe.loadFence();
            if (version == this.version) {
                break;
            }
        }
        if (flag instanceof FiberCancellationSignal signal) {
            signal.reset(generation);
        } else if (flag != null) {
            flag.set(false);
        }
    }

    public synchronized void set(@Nullable AtomicBoolean flag) {
        set(flag, generationOf(flag));
    }

    public synchronized void set(@Nullable AtomicBoolean flag, long generation) {
        if (flag instanceof FiberCancellationSignal) {
            if (generation < 0) {
                throw new IllegalArgumentException("fiber cancellation generation must be non-negative");
            }
        } else if (generation != NO_GENERATION) {
            throw new IllegalArgumentException("ordinary cancellation flag cannot have a generation");
        }
        publish(flag, generation);
    }

    private static long generationOf(@Nullable AtomicBoolean flag) {
        return flag instanceof FiberCancellationSignal signal ? signal.getGeneration() : NO_GENERATION;
    }

    private void publish(@Nullable AtomicBoolean flag, long generation) {
        // A volatile store is release-only: without the fences the plain stores below may become
        // visible before the odd sequence, and a reader would accept a torn (flag, generation) pair.
        version++;
        Unsafe.storeFence();
        this.flag = flag;
        this.generation = generation;
        Unsafe.storeFence();
        version++;
    }

    /**
     * The cancellation face of a query circuit breaker, visible to the fiber runtime without
     * a cairo dependency. Installed per connection fiber via
     * {@link SuspensionScope#enterCancellationSource}; waits built outside a reduce scope
     * resolve the current cancelled flag from it at wait-build time.
     */
    public interface Source {

        void copyCancelledFlagTo(CancellationBinding target);

        void statefulThrowExceptionIfTrippedNoThrottle();
    }
}
