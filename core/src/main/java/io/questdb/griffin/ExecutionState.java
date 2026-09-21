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

package io.questdb.griffin;

/**
 * Engine-attached per-execution-context state. An engine may attach one instance to every
 * {@link SqlExecutionContextImpl} it serves (see {@code CairoEngine#createExecutionState()}).
 * The runtime invokes {@link #onExecutionStart(SqlExecutionContext)} at execution boundaries:
 * from {@link SqlExecutionContext#initNow()} — the statement-entry point where the now()
 * timestamp snapshot is frozen — and from the statement-root cursor open
 * ({@code QueryProgress}), before any function of the execution initializes. Implementations
 * snapshot whatever engine-level state must stay constant for the duration of one execution.
 * Called on the hot per-statement path: implementations must be allocation-free snapshot
 * reads only.
 */
public interface ExecutionState {
    void onExecutionStart(SqlExecutionContext executionContext);
}
