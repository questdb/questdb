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

package io.questdb.cairo.sql;

import io.questdb.cairo.idx.IndexReader;
import io.questdb.std.QuietCloseable;

/**
 * Used internally for index-based (but not only) row access.
 * <p>
 * <b>Lifecycle is the caller's responsibility.</b> A cursor acquired from
 * {@link IndexReader#getCursor} must be closed
 * exactly once by whoever acquired it — the reader does not track outstanding
 * cursors and will not free them on its own.
 * <p>
 * Most implementations (BITMAP-based, wrapper, singleton) have a no-op
 * {@code close()}; POSTING cursors return to their reader's free list.
 */
public interface RowCursor extends QuietCloseable {

    /**
     * Releases cursor resources. For pool-backed cursors (e.g. POSTING) this returns
     * the cursor to the owning reader's free list. For stateless / singleton cursors
     * this is a no-op. Implementations must be idempotent: calling close more than
     * once must be safe and a no-op.
     */
    @Override
    default void close() {
    }

    /**
     * @return true if cursor has more rows, otherwise false.
     */
    boolean hasNext();

    /**
     * Iterates or jumps to given position. Jumping to position has to be performed before
     * attempting to iterate row cursor.
     *
     * @param position row position to jump
     */
    default void jumpTo(long position) {
        while (position-- > 0 && hasNext()) {
            next();
        }
    }

    /**
     * @return numeric index of the next row
     */
    long next();

    default long size() {
        return -1;
    }
}
