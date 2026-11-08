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

package io.questdb.cairo.sql.async;

import io.questdb.cairo.sql.RecordCursor;

/**
 * Sizes the page frame tasks that an ordered parallel cursor keeps in flight for a reader that may
 * stop before the last frame, such as the reader of a LIMIT. {@link #get()} returns the dispatch
 * limit that the cursor passes to {@link PageFrameSequence#next(int, boolean)}.
 * <p>
 * A limit too small starves the workers of a long scan: the owner thread has no queued task to
 * steal, and a worker that finishes ahead of the oldest task idles until the reader collects that
 * task. A limit too large makes the workers start tasks whose frames the reader never reads, and
 * the reader then waits for them when it stops. The limit starts at the configured
 * {@code cairo.sql.parallel.filter.dispatch.limit}, so a reader that finds its rows in the first
 * frames stops as early as before, and grows while the reader keeps asking for frames:
 * <ul>
 * <li>to the number of frames the reader collected, rounded down to a power of two, so that it
 * doubles each time the reader consumes the frames it dispatched and still asks for rows;</li>
 * <li>when the reader announced how many rows it reads, to the number of frames that the rows it
 * still needs take at the yield of the collected frames, where frames without any row count as
 * having found one;</li>
 * <li>when the reader announced nothing, to no limit once the frames of the initial limit held no
 * row at all: such a reader got nothing to stop at, so the scan goes on.</li>
 * </ul>
 * A read that nobody announced as limited keeps the limit unbounded, as does a reader that
 * announces that it reads every row.
 * <p>
 * The owner thread alone calls the methods of this class.
 */
public final class AdaptiveDispatchLimit {
    private static final long NO_ROW_COUNT = -1;
    private final int initialLimit;
    // Rows that the reader announced to read from the start of the read, NO_ROW_COUNT when it did not.
    private long announcedRowCount = NO_ROW_COUNT;
    // Frames collected since the start of the read, the empty ones included.
    private int collectedFrameCount;
    // Rows of the collected frames that the reader reads.
    private long collectedRowCount;
    private boolean isLimitedRead;
    // The LIMIT of the cursor itself, Long.MAX_VALUE when it has none.
    private long limitRowCount = Long.MAX_VALUE;

    public AdaptiveDispatchLimit(int initialLimit) {
        this.initialLimit = initialLimit;
    }

    /**
     * Marks the read as one that the reader may stop before the last row, as a parent that
     * implements a LIMIT announces with {@link RecordCursor#expectLimitedIteration()}.
     */
    public void expectLimitedRead() {
        isLimitedRead = true;
    }

    public int get() {
        if (!isLimitedRead) {
            return Integer.MAX_VALUE;
        }
        if (initialLimit < 1) {
            // A limit below one makes the owner thread reduce every frame of a limited read.
            return initialLimit;
        }
        long expectedRowCount = limitRowCount;
        if (announcedRowCount != NO_ROW_COUNT) {
            if (announcedRowCount == Long.MAX_VALUE && limitRowCount == Long.MAX_VALUE) {
                // The reader reads every row, as LIMIT 5, 9223372036854775807 does.
                return Integer.MAX_VALUE;
            }
            expectedRowCount = Math.min(expectedRowCount, announcedRowCount);
        }
        if (expectedRowCount == Long.MAX_VALUE && collectedRowCount == 0 && collectedFrameCount >= initialLimit) {
            return Integer.MAX_VALUE;
        }
        long limit = Math.max(initialLimit, Integer.highestOneBit(collectedFrameCount));
        if (expectedRowCount != Long.MAX_VALUE && collectedFrameCount > 0) {
            final long neededRowCount = Math.max(expectedRowCount - collectedRowCount, 1);
            final long foundRowCount = Math.max(collectedRowCount, 1);
            // The frames that the needed rows take at the yield of the collected frames, rounded up.
            final long frameCount = neededRowCount > (Long.MAX_VALUE - foundRowCount) / collectedFrameCount
                    ? Long.MAX_VALUE
                    : (neededRowCount * collectedFrameCount + foundRowCount - 1) / foundRowCount;
            limit = Math.max(limit, frameCount);
        }
        return (int) Math.min(limit, Integer.MAX_VALUE);
    }

    /**
     * Opens a read.
     *
     * @param limitRowCount the LIMIT of the cursor itself, Long.MAX_VALUE when it has none
     */
    public void of(long limitRowCount) {
        this.limitRowCount = limitRowCount;
        this.isLimitedRead = limitRowCount != Long.MAX_VALUE;
        toTop();
    }

    /**
     * Accounts for a collected frame.
     *
     * @param rowCount the rows of the frame that the reader reads, zero for an empty frame and for
     *                 a frame of a cancelled sequence
     */
    public void onFrameCollected(long rowCount) {
        collectedFrameCount++;
        collectedRowCount += rowCount;
    }

    /**
     * Takes the bound that the reader passes to {@link RecordCursor#skipRows(RecordCursor.Counter, long)}.
     *
     * @param pendingRowCount  rows of the collected frames that the reader has not read yet
     * @param skipRowCount     rows that the reader skips
     * @param maxRowsAfterSkip rows that the reader reads at most after the skip,
     *                         {@link RecordCursor#UNBOUNDED_ROW_COUNT} when it does not know
     */
    public void onSkip(long pendingRowCount, long skipRowCount, long maxRowsAfterSkip) {
        if (maxRowsAfterSkip == RecordCursor.UNBOUNDED_ROW_COUNT) {
            return;
        }
        final long consumedRowCount = Math.max(collectedRowCount - Math.max(pendingRowCount, 0), 0);
        final long skip = Math.max(skipRowCount, 0);
        final long max = Math.max(maxRowsAfterSkip, 0);
        announcedRowCount = max > Long.MAX_VALUE - consumedRowCount - skip
                ? Long.MAX_VALUE
                : consumedRowCount + skip + max;
    }

    /**
     * Restarts the read from the first frame. The read stays limited, and the reader announces its
     * bound again.
     */
    public void toTop() {
        announcedRowCount = NO_ROW_COUNT;
        collectedFrameCount = 0;
        collectedRowCount = 0;
    }
}
