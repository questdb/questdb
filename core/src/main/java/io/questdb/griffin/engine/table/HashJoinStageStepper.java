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

package io.questdb.griffin.engine.table;

import io.questdb.std.DirectLongList;
import org.jetbrains.annotations.TestOnly;

/**
 * Probes one stage of a parallel hash join for the probe rows of a frame, on the thread that joins
 * the frame, and appends each probe row's joined tuples: the probe row and the stage's entry.
 * <p>
 * A stage's lookup calls classes that each query compiles for itself or picks per execution: its
 * key sink, its probe of the build, serial or partitioned, its SYMBOL key translation and its
 * filters. A shared copy of the lookup would profile every query's classes; see
 * {@link HiddenClassCopies}. {@link #newInstance()} therefore hands every stage a hidden copy of
 * {@link HashJoinStageStepperTemplate} of its own.
 * <p>
 * A stage's entry in a tuple is the locator of the build row that the probe row matches, see
 * {@code HashJoinPayloadSource.Reader.locator()}, {@link #NO_MATCH} for a LEFT join's miss, or a
 * tagged chain head when the key matches several build rows: the head of the key's chain, negated,
 * which a link's minimum of eight keeps below {@link #NO_MATCH}. The consumer walks such a chain
 * itself, so a key's duplicates cost the worker one lookup however many there are; see
 * {@link #toChainHead(long)}. An INNER join's miss drops the probe row.
 * <p>
 * A stage whose pairs run a filter, an ON residual or a post-join filter, is enumerated instead: the
 * stepper walks the key's matches itself, runs the filters over each pair of the probe row and a
 * match, and appends a tuple, with the match's locator, for each pair that passes them. A LEFT
 * join's probe row whose pairs all fail the residual, or that has no match, yields one tuple with
 * {@link #NO_MATCH}, if it passes the post-join filter. Its tuples then come in chain order, the
 * order the serial join returns its pairs in, and the consumer walks no chain. The stepper checks
 * the circuit breaker every so many pairs, see {@link AsyncHashJoinAtom.Slot#isInterrupted()}, and
 * unwinds once the execution is cancelled.
 * <p>
 * The loops over a frame's rows stop at a probe row's boundary once the tuples reach their cap, so
 * that the consumer continues the frame from there; see {@link AsyncHashJoinRecordCursor}.
 */
abstract class HashJoinStageStepper {
    // The key reads an INT column of the key source.
    static final int KEY_INT = 0;
    // The key stages through the stage's key sink.
    static final int KEY_STAGED = 2;
    // The key reads a SYMBOL column of the key source and translates it into the build's keys.
    static final int KEY_SYMBOL = 1;
    // What a loop over a frame's rows returns when the execution was cancelled.
    static final long CANCELLED = -1;
    // The entry of a LEFT join's probe row that the build has no row for.
    static final long NO_MATCH = -1;
    private static final HiddenClassCopies<HashJoinStageStepper> COPIES = new HiddenClassCopies<>(
            HashJoinStageStepper.class,
            HashJoinStageStepperTemplate.class,
            HashJoinStageStepperTemplate::new
    );
    // Set by tests alone; see AsyncHashJoinRecordCursorFactory.setStepperCopiesEnabledForTesting().
    private static volatile boolean isCopiesEnabled = true;
    // True when the stage's pairs run a filter, so that the stepper walks the key's matches itself.
    boolean isEnumerated;
    // True for a LEFT join, whose probe rows survive a miss.
    boolean isOuter;
    // The key source's column that an INT or SYMBOL key reads; -1 for a staged key.
    int keyColumn;
    int keyKind;

    /**
     * Returns a stepper whose class no other stage shares, or the shared template when the JVM
     * cannot define hidden classes.
     */
    static HashJoinStageStepper newInstance() {
        return isCopiesEnabled ? COPIES.newInstance() : new HashJoinStageStepperTemplate();
    }

    @TestOnly
    static void setCopiesEnabled(boolean isEnabled) {
        isCopiesEnabled = isEnabled;
    }

    /**
     * The chain head that a tagged entry carries; see {@link HashJoinStageStepper}.
     */
    static long toChainHead(long entry) {
        assert entry < NO_MATCH;
        return -entry;
    }

    /**
     * Binds the stepper to its stage in an operator: what the lookup reads, whether a miss survives
     * and whether the stepper walks the key's matches itself.
     */
    final void of(int keyKind, int keyColumn, boolean isOuter, boolean isEnumerated) {
        this.keyKind = keyKind;
        this.keyColumn = keyColumn;
        this.isOuter = isOuter;
        this.isEnumerated = isEnumerated;
    }

    /**
     * Probes the stage for the rows of a frame, in the order the probe scan yields them, from the
     * last row for a backward scan, starting at the given position in that order, and stops after
     * the probe row whose tuples bring the slot's tuples to the cap.
     *
     * @param rowCount     the frame's rows
     * @param from         the position, in the scan's order, of the first row to probe
     * @param isDescending true for a backward probe scan
     * @param tupleCap     the longs of the slot's tuples that end the call at a probe row's boundary
     * @return the position of the first row not probed, {@code rowCount} once every row was, or
     * {@link #CANCELLED} when the execution was cancelled
     */
    abstract long probeRows(AsyncHashJoinAtom.Slot slot, long rowCount, long from, boolean isDescending, long tupleCap);

    /**
     * Probes the stage for the rows of a frame that the probe filter kept, as
     * {@link #probeRows(AsyncHashJoinAtom.Slot, long, long, boolean, long)} probes every row: from
     * the last kept row for a backward scan.
     *
     * @param rows the kept rows' indexes in the frame, ascending
     * @return the position of the first kept row not probed, {@code rows.size()} once every kept row
     * was, or {@link #CANCELLED} when the execution was cancelled
     */
    abstract long probeRows(AsyncHashJoinAtom.Slot slot, DirectLongList rows, long from, boolean isDescending, long tupleCap);

    /**
     * Probes this stage for the slot's current probe row and appends the row's tuples to the slot's
     * tuples. An INNER join's miss drops the row.
     *
     * @return false when the execution was cancelled, which unwinds the frame's loop
     */
    abstract boolean step(AsyncHashJoinAtom.Slot slot, int stage);
}
