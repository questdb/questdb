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

package io.questdb.cairo.view;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.Mutable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * It holds per-view states.
 * <p>
 * The actual implementation may be no-op in case of disabled views or read-only replica.
 */
public interface ViewStateStore extends Mutable {

    /**
     * Tells whether a view compile run was running at the first of two reads of
     * {@link #getCompileRunActivity()}, or started between the two reads. A borrow of a table that
     * a run made between the reads shows up in one of the two ways.
     */
    static boolean hasCompileRunOverlapped(long activityBefore, long activityAfter) {
        return isCompileRunRunning(activityBefore) || (activityBefore >>> 32) != (activityAfter >>> 32);
    }

    static boolean isCompileRunRunning(long activity) {
        return (int) activity != 0;
    }

    default void createViewState(ViewDefinition viewDefinition) {
        createViewState(viewDefinition, null);
    }

    void createViewState(ViewDefinition viewDefinition, @Nullable RecordMetadata metadata);

    void enqueueCompile(@NotNull TableToken viewToken);

    /**
     * Queues a compile of each view whose busy retry, see {@link #scheduleCompileBusyRetry}, is due,
     * with the update timestamp of the retry, and forgets the retry. The view compiler calls it
     * before it takes the first task of a drain, so a drain never runs a retry that it scheduled.
     */
    void enqueueDueCompileBusyRetries();

    /**
     * Ends the view compile run that {@link #startCompileRun()} started.
     */
    void finishCompileRun();

    /**
     * Returns the view compile activity in one read: the high 32 bits count the compile runs
     * started so far, the low 32 bits count the runs still running.
     */
    long getCompileRunActivity();

    @Nullable
    ViewState getViewState(TableToken viewToken);

    void removeViewState(TableToken viewToken);

    /**
     * Keeps one pending compile of the view, after its compile met the lock of a table it reads and
     * left its state as it was. The retry is due once the busy retry timeout passes after it was
     * scheduled, see {@link io.questdb.cairo.CairoConfiguration#getViewCompileBusyRetryTimeout()}.
     * When the view already has a pending retry, the retry keeps its due time and takes the newer of
     * the two update timestamps: it compiles after both requests were made. A view with no state,
     * such as a dropped one, gets no retry.
     *
     * @return the due time of the view's pending retry, or {@link io.questdb.std.Numbers#LONG_NULL}
     * when the view has no state
     */
    long scheduleCompileBusyRetry(@NotNull TableToken viewToken, long updateTimestamp);

    /**
     * Starts a view compile run: a drain of the compile queue, or the startup compile of all
     * views. A run borrows the readers and the metadata of the tables its views read, so a
     * DROP, RENAME or TRUNCATE of a non-WAL table that fails to lock the table waits until no
     * run is running when a run overlapped its lock attempt.
     */
    void startCompileRun();

    boolean tryDequeueCompilerTask(ViewCompilerTask task);
}
