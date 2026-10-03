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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.Reopenable;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.std.MemoryTracker;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * Detects master rows whose join keys the slave symbol tables lack.
 * <p>
 * An implementation may hold native memory, for example a symbol key cache. The owning
 * cursor binds the per-query tracker via {@link #setMemoryTracker}, calls {@link #reopen()}
 * before it adopts the master and slave cursors, {@link #of} after, and {@link #close()}
 * when it closes.
 */
public interface SymbolShortCircuit extends QuietCloseable, Reopenable {

    /**
     * Releases the native memory. The instance stays reusable: the next
     * {@link #reopen()} call allocates it again.
     */
    @Override
    default void close() {
    }

    /**
     * When joining on one or more symbol columns, detects when any slave column
     * doesn't have the symbol at all (by inspecting its int-to-symbol mapping). This
     * allows the record cursor to avoid searching for the matching slave row.
     */
    boolean isShortCircuit(Record masterRecord);

    void of(TimeFrameCursor slaveCursor);

    /**
     * Allocates the native memory. The owning cursor calls it before it adopts the
     * master and slave cursors, so that an allocation failure leaves the cursors to
     * the caller to free.
     */
    @Override
    default void reopen() {
    }

    /**
     * Binds the per-query native memory tracker. Call before {@link #reopen()}, so
     * that the native memory gets allocated under the tracker.
     */
    default void setMemoryTracker(@Nullable MemoryTracker tracker) {
    }
}
