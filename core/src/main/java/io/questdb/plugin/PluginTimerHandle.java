/*******************************************************************************
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

package io.questdb.plugin;

/**
 * Handle returned by timer registration methods on {@link PluginContext}.
 * Allows the plugin to cancel a timer before the plugin is unloaded.
 * <p>
 * Timers are automatically cancelled when the owning plugin is unloaded,
 * so explicit cancellation is only needed for early termination.
 */
public interface PluginTimerHandle {

    /**
     * Cancels this timer. The callback will not fire after cancellation.
     * If the callback is currently executing, it will complete but will
     * not be rescheduled.
     * <p>
     * Safe to call multiple times; subsequent calls are no-ops.
     */
    void cancel();

    /**
     * Returns {@code true} if this timer has been cancelled.
     */
    boolean isCancelled();
}
