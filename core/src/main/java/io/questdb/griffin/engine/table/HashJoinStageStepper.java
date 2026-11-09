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

import org.jetbrains.annotations.TestOnly;

/**
 * Probes one stage of a parallel hash join for the probe rows of a frame, on the thread that joins
 * the frame, and appends each probe row's joined tuple: the probe row and the stage's entry.
 * <p>
 * A stage's lookup calls classes that each query compiles for itself or picks per execution: its
 * key sink, its probe of the build, serial or partitioned, and its SYMBOL key translation. A shared
 * copy of the lookup would profile every query's classes; see {@link HiddenClassCopies}.
 * {@link #newInstance()} therefore hands every stage a hidden copy of
 * {@link HashJoinStageStepperTemplate} of its own.
 * <p>
 * A stage's entry in a tuple is the locator of the build row that the probe row matches, see
 * {@code HashJoinPayloadSource.Reader.locator()}, {@link #NO_MATCH} for a LEFT join's miss, or a
 * tagged chain head when the key matches several build rows: the head of the key's chain, negated,
 * which a link's minimum of eight keeps below {@link #NO_MATCH}. The consumer walks such a chain
 * itself, so a key's duplicates cost the worker one lookup however many there are; see
 * {@link #toChainHead(long)}. An INNER join's miss drops the probe row.
 */
abstract class HashJoinStageStepper {
    // The key reads an INT column of the key source.
    static final int KEY_INT = 0;
    // The key stages through the stage's key sink.
    static final int KEY_STAGED = 2;
    // The key reads a SYMBOL column of the key source and translates it into the build's keys.
    static final int KEY_SYMBOL = 1;
    // The entry of a LEFT join's probe row that the build has no row for.
    static final long NO_MATCH = -1;
    private static final HiddenClassCopies<HashJoinStageStepper> COPIES = new HiddenClassCopies<>(
            HashJoinStageStepper.class,
            HashJoinStageStepperTemplate.class,
            HashJoinStageStepperTemplate::new
    );
    // Set by tests alone; see AsyncHashJoinRecordCursorFactory.setStepperCopiesEnabledForTesting().
    private static volatile boolean isCopiesEnabled = true;
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
     * Binds the stepper to its stage in an operator: what the lookup reads and whether a miss
     * survives.
     */
    final void of(int keyKind, int keyColumn, boolean isOuter) {
        this.keyKind = keyKind;
        this.keyColumn = keyColumn;
        this.isOuter = isOuter;
    }

    /**
     * Probes the stage for each row of a frame, in the order the probe scan yields them:
     * from the last row for a backward scan.
     *
     * @param rowCount     the frame's rows
     * @param isDescending true for a backward probe scan
     */
    abstract void probeRows(AsyncHashJoinAtom.Slot slot, long rowCount, boolean isDescending);

    /**
     * Probes this stage for the slot's current probe row and appends the row's tuple to the slot's
     * tuples. An INNER join's miss drops the row.
     */
    abstract void step(AsyncHashJoinAtom.Slot slot, int stage);
}
