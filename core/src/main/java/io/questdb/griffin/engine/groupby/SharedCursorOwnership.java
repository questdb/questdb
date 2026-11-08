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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.sql.RecordCursor;

/**
 * Tracks who holds the owner state of a factory that serves shared cursors, see
 * {@link io.questdb.cairo.sql.RecordCursorFactory#getSharedCursor}: the base cursor that the primary cursor
 * aggregates and the functions bound to it. The first of {@code getCursor()} and {@code getSharedCursor()} in an
 * execution opens the owner state.
 * <p>
 * The primary cursor claims the owner state when it opens, see {@link #claim()}, and releases it when it closes.
 * Until the primary claims it, the shared cursors that are open hold it, and the last of them to close releases
 * it. An execution can end before the primary opens: it fails between the two opens, or a parent never opens the
 * primary. The parents then close the shared cursors, which leaves no owner state that the next execution of the
 * factory would read.
 * <p>
 * The owner thread of the query opens and closes the cursors, so this class does not synchronize.
 */
public final class SharedCursorOwnership {
    private final RecordCursor owner;
    private int holderCount;
    // a shared cursor opened the owner state, and the primary cursor did not claim it
    private boolean isUnclaimed;

    /**
     * @param owner the primary cursor, whose close() releases the owner state
     */
    public SharedCursorOwnership(RecordCursor owner) {
        this.owner = owner;
    }

    /**
     * Called by {@code getCursor()}: returns true when a shared cursor of this execution opened the owner state, which
     * the primary cursor then takes over as is. The primary cursor releases it when it closes.
     */
    public boolean claim() {
        if (isUnclaimed) {
            isUnclaimed = false;
            return true;
        }
        return false;
    }

    /**
     * Forgets the holders when the factory closes, so that a shared cursor that closes later does not release the
     * owner state the factory already freed.
     */
    public void clear() {
        holderCount = 0;
        isUnclaimed = false;
    }

    /**
     * Called by {@code getSharedCursor()} once it opened the owner state.
     */
    public void markOpenedBySharedCursor() {
        isUnclaimed = true;
    }

    /**
     * Returns the holder for a shared cursor, which holds the owner state while the shared cursor is open.
     */
    public Holder newHolder() {
        return new Holder();
    }

    /**
     * Releases the owner state, unless the primary cursor claimed it. {@code getSharedCursor()} calls it when it fails
     * after it opened the owner state.
     */
    public void releaseUnclaimed() {
        if (isUnclaimed) {
            isUnclaimed = false;
            owner.close();
        }
    }

    public final class Holder {
        private boolean isHolding;

        /**
         * Called when the shared cursor opened: it holds the owner state until it closes. Idempotent.
         */
        public void hold() {
            if (!isHolding) {
                isHolding = true;
                holderCount++;
            }
        }

        /**
         * Called when the shared cursor closes. The last holder to close releases the owner state, unless the primary
         * cursor claimed it. Idempotent: the parents of a cursor can close it more than once on error paths.
         */
        public void release() {
            if (isHolding) {
                isHolding = false;
                if (holderCount > 0 && --holderCount == 0) {
                    releaseUnclaimed();
                }
            }
        }
    }
}
