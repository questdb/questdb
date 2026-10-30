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
 * A {@link Job} that a {@link Worker} may park in, instead of sleeping. A worker that has found
 * nothing to do backs off: it spins, then naps for a millisecond at a time, then sleeps for its
 * sleep timeout. An event for one of its jobs meanwhile waits for the worker to wake up. When the
 * worker is about to nap or sleep, it offers the park to its jobs of this kind instead: a job that
 * takes it waits for its own events, an IO dispatcher in its socket poll, for no longer than the
 * worker would have slept. The worker's other jobs see no difference, as it would not have run
 * them before then either, and the job's event is served as soon as it arrives.
 * <p>
 * Only a job whose events can wake it may take the park. A worker parked in a job is not a target
 * for fiber wake-ups ({@code LockSupport.unpark()} does not wake a poll), so a fiber-hosting worker
 * parks in a job only while another worker of its pool is parked and can be woken instead.
 * <p>
 * The protocol: {@link #tryBeginIdlePark()}, and when that returns true, exactly one of
 * {@link #idlePark(long)} or {@link #cancelIdlePark()}.
 */
public interface IdleParkJob extends Job {

    /**
     * Ends a park that {@link #tryBeginIdlePark()} began, without waiting.
     */
    void cancelIdlePark();

    /**
     * Waits for the job's own events for at most {@code maxParkNanos}, rounded down to the job's
     * wait granularity but never to zero, and handles what arrives, as {@link #run} would. Ends
     * the park that {@link #tryBeginIdlePark()} began.
     *
     * @param maxParkNanos the nap or sleep the worker would have taken instead
     * @return true when the job found something to do, as {@link #run} would return
     */
    boolean idlePark(long maxParkNanos);

    /**
     * Whether this worker may park in the job now. When it returns true, the caller holds the job
     * exclusively until it calls {@link #idlePark(long)} or {@link #cancelIdlePark()}.
     */
    boolean tryBeginIdlePark();
}
