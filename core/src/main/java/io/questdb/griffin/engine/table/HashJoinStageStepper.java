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

import io.questdb.cairo.CairoException;
import io.questdb.std.DirectLongList;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Probes one stage of a parallel hash join for the probe rows of a frame, on the thread that joins
 * the frame, and hands each of a probe row's matches on to the next stage's stepper, or, at the last
 * stage, appends the joined tuple: the probe row and every stage's entry. The stages thus run nested,
 * the first one outermost, as the serial joins of a stack nest, so a probe row's tuples come in the
 * serial plan's order, lexicographic in stage order.
 * <p>
 * A stage's lookup calls classes that each query compiles for itself or picks per execution: its
 * key sink, its probe of the build, serial or partitioned, its SYMBOL key translation and its
 * filters. A shared copy of the lookup would profile every query's classes; see
 * {@link HiddenClassCopies}. {@link #newInstance()} therefore hands every stage a hidden copy of
 * {@link HashJoinStageStepperTemplate} of its own. A copy starts cold, so every stage also keeps an
 * instance of the shared template, whose code the JVM has long compiled, and the operator runs those
 * until it has probed enough rows to repay the copies' warm-up. Only then does it define the copies,
 * see {@link #ofCopy(HashJoinStageStepper, HashJoinStageStepper)} and {@link AsyncHashJoinAtom}, so
 * that a factory that never probes that many rows defines no class.
 * <p>
 * A stage's entry in a tuple is the locator of the build row that the probe row matches, see
 * {@code HashJoinPayloadSource.Reader.locator()}, {@link #NO_MATCH} for a LEFT join's miss, or a
 * tagged chain head when the key matches several build rows: the head of the key's chain, negated,
 * which a link's minimum of eight keeps below {@link #NO_MATCH}. The consumer walks such a chain
 * itself, so a key's duplicates cost the worker one lookup however many there are; see
 * {@link #toChainHead(long)}. An INNER join's miss drops the probe row.
 * <p>
 * A stage whose pairs run a filter in the open cursor's execution, an ON residual or a post-join
 * filter, is enumerated instead: the stepper walks the key's matches itself, runs the filters over
 * each pair of the probe row and a match, and hands on each pair that passes them, with the match's
 * locator. A LEFT join's probe row whose pairs all fail the residual, or that has no match, goes on
 * once with {@link #NO_MATCH}, if it passes the post-join filter. Its pairs then come in chain order,
 * the order the serial join returns them in, and the consumer walks no chain. Since a filter may read
 * the columns of every stage before its own, every stage before the last enumerated one is
 * enumerated too, with its payload reader on the current match, and only the stages after it hand
 * their chains to the consumer, which expands them in the same nesting order. So is the first stage
 * of an execution that swaps the first join's inputs whenever later stages follow it: the later
 * stages' keys read the left input's columns, which such an execution holds in the first stage's
 * payload, so the first stage's payload reader must sit on each of its matches in turn while the
 * later stages look them up; see {@link #ofKeyColumn(int)}. Each execution decides which stages
 * walk their matches, see {@link #ofEnumerated(boolean)}, since its builds decide what the pairs run:
 * a LEFT join whose build ran its whole ON residual over the build's rows runs none over its pairs,
 * see {@link HashJoinStageSpec#hasPairFilter(boolean)}. The stepper checks the circuit breaker every
 * so many pairs, see {@link AsyncHashJoinAtom.Slot#isInterrupted()}, and unwinds once the execution
 * is cancelled.
 * <p>
 * The loops over a frame's rows stop at a probe row's boundary once the tuples reach their cap, so
 * that the consumer continues the frame from there; see {@link AsyncHashJoinRecordCursor}.
 * <p>
 * A count of the joined rows, see {@link #countRows(AsyncHashJoinAtom.Slot, long, long, boolean)},
 * walks the stages as a frame's tuples do, but appends no tuple: each tuple that would come out of
 * the last stage adds the rows it stands for to the slot's pair count instead, one, or, for a tuple
 * with chains that the stages after the last enumerated one would hand to the consumer, the product
 * of those chains' lengths. A deferred stage reads its key's chain length off the index that its
 * build took for the count, see {@link AsyncHashJoinAtom#indexChainLengths}, rather than walk the
 * chain, and leaves every link of the build as it is, so that the same execution can join the
 * frames again afterwards. A count fails rather than wrap once its rows pass {@code Long.MAX_VALUE};
 * see {@link #addRowCounts(long, long)}.
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
    // What an enumerated stage makes of a pair: it fails the ON residual, it passes it, or the
    // execution was cancelled while the later stages joined it.
    static final int PAIR_CANCELLED = -1;
    static final int PAIR_FAILED = 0;
    static final int PAIR_MATCHED = 1;
    // The entry of a LEFT join's probe row that the build has no row for.
    static final long NO_MATCH = -1;
    // A product of row counts that a LONG does not hold; see multiplyRowCounts().
    static final long ROW_COUNT_OVERFLOW = Long.MIN_VALUE;
    private static final HiddenClassCopies<HashJoinStageStepper> COPIES = new HiddenClassCopies<>(
            HashJoinStageStepper.class,
            HashJoinStageStepperTemplate.class,
            HashJoinStageStepperTemplate::new
    );
    // Set by tests alone; see AsyncHashJoinRecordCursorFactory.setStepperCopiesEnabledForTesting().
    private static volatile boolean isCopiesEnabled = true;
    // True when the stepper walks the key's matches itself in the open cursor's execution: the
    // stage's pairs, or those of a later stage, run a filter, or the stage is the first one of a
    // swapped execution, whose later stages read their keys off its matches; see ofEnumerated().
    boolean isEnumerated;
    // True for a LEFT join, whose probe rows survive a miss.
    boolean isOuter;
    // The key source's column that an INT or SYMBOL key reads; -1 for a staged key.
    int keyColumn;
    int keyKind;
    // The next stage's stepper, which each of this stage's matches goes on to; null for the last
    // stage, whose matches append their tuples.
    HashJoinStageStepper next;

    /**
     * Adds rows to a count of joined rows.
     *
     * @param count the rows counted so far
     * @param rows  the rows to add, at least 0, or {@link #ROW_COUNT_OVERFLOW}
     * @return the sum
     * @throws CairoException when the rows carry {@link #ROW_COUNT_OVERFLOW} or the sum passes
     *                        {@code Long.MAX_VALUE}: the count fails rather than wrap
     */
    static long addRowCounts(long count, long rows) {
        if (rows < 0 || count > Long.MAX_VALUE - rows) {
            throw CairoException.nonCritical().put("hash join row count overflow");
        }
        return count + rows;
    }

    /**
     * Multiplies two row counts: the rows a tuple stands for by a chain's length, or the matches
     * after a chain's current one by the combinations of the chains after it. A product that a LONG
     * does not hold comes out as {@link #ROW_COUNT_OVERFLOW}, which stays so through later products
     * and fails the count only once {@link #addRowCounts(long, long)} adds it: a later stage may
     * still drop the tuple.
     *
     * @param a a row count, at least 1, or {@link #ROW_COUNT_OVERFLOW}
     * @param b a row count, at least 1, or {@link #ROW_COUNT_OVERFLOW}
     */
    static long multiplyRowCounts(long a, long b) {
        assert (a > 0 || a == ROW_COUNT_OVERFLOW) && (b > 0 || b == ROW_COUNT_OVERFLOW);
        final long product = a * b;
        // The product fits when the high half of the 128-bit product is 0 and its low half is not
        // negative. ROW_COUNT_OVERFLOW, the smallest LONG, times a count of at least 1 is negative,
        // and times itself is 2^126: either way the high half is not 0, so the overflow stays.
        return Math.multiplyHigh(a, b) == 0 && product >= 0 ? product : ROW_COUNT_OVERFLOW;
    }

    /**
     * Returns a stepper whose class no other stage shares, or the shared template when the JVM
     * cannot define hidden classes or a test turned the copies off.
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
     * and which stepper the matches go on to. Whether the stepper walks the key's matches itself is
     * each execution's to decide; see {@link #ofEnumerated(boolean)}.
     *
     * @param next the next stage's stepper, null for the last stage
     */
    final void of(int keyKind, int keyColumn, boolean isOuter, @Nullable HashJoinStageStepper next) {
        assert next == null || next != this;
        this.keyKind = keyKind;
        this.keyColumn = keyColumn;
        this.isOuter = isOuter;
        this.next = next;
    }

    /**
     * Binds the stepper, a stage's hidden copy, as the stage's instance of the shared template is
     * bound: its key, its join type, the key column of the execution's orientation and whether it
     * walks the key's matches in the open cursor's execution, so that a copy that the owner defines
     * in the middle of an execution joins the execution's frames as the template does. The owner
     * calls it before it publishes the copy to the workers.
     *
     * @param template the stage's instance of the shared template
     * @param next     the next stage's copy, null for the last stage
     */
    final void ofCopy(HashJoinStageStepper template, @Nullable HashJoinStageStepper next) {
        assert (next == null) == (template.next == null);
        of(template.keyKind, template.keyColumn, template.isOuter, next);
        isEnumerated = template.isEnumerated;
    }

    /**
     * Decides whether the stepper walks the key's matches itself in the open cursor's execution,
     * which the execution's builds tell: they decide which filters the stages' pairs run. The owner
     * calls it before it dispatches the execution's first task, which publishes it to the workers.
     */
    final void ofEnumerated(boolean isEnumerated) {
        this.isEnumerated = isEnumerated;
    }

    /**
     * Points an INT or SYMBOL key at another column of its key source, for an execution whose key
     * source lays the columns out differently: a later stage of a stack reads the left input's
     * columns from the probe record, or, once an execution swaps the first join's inputs, from the
     * first stage's payload. The owner calls it before it dispatches the execution's first task,
     * which publishes it to the workers.
     *
     * @param keyColumn the key source's column that the key reads, -1 for a staged key
     */
    final void ofKeyColumn(int keyColumn) {
        assert (keyColumn == -1) == (keyKind == KEY_STAGED);
        this.keyColumn = keyColumn;
    }

    /**
     * Probes the stage for the rows of a frame, in the order the probe scan yields them, from the
     * last row for a backward scan, from the given position in that order up to, not including,
     * the given end, and stops early after the probe row whose tuples bring the slot's tuples to the
     * cap.
     *
     * @param rowCount     the frame's rows
     * @param from         the position, in the scan's order, of the first row to probe
     * @param to           the position, in the scan's order, of the row to stop before, at most
     *                     {@code rowCount}
     * @param isDescending true for a backward probe scan
     * @param tupleCap     the longs of the slot's tuples that end the call at a probe row's boundary
     * @return the position of the first row not probed, {@code to} once every row before it was, or
     * {@link #CANCELLED} when the execution was cancelled
     */
    abstract long probeRows(AsyncHashJoinAtom.Slot slot, long rowCount, long from, long to, boolean isDescending, long tupleCap);

    /**
     * Probes the stage for the rows of a frame that the probe filter kept, as
     * {@link #probeRows(AsyncHashJoinAtom.Slot, long, long, long, boolean, long)} probes every row:
     * from the last kept row for a backward scan.
     *
     * @param rows the kept rows' indexes in the frame, ascending
     * @param to   the position, in the scan's order, of the kept row to stop before, at most
     *             {@code rows.size()}
     * @return the position of the first kept row not probed, {@code to} once every kept row before
     * it was, or {@link #CANCELLED} when the execution was cancelled
     */
    abstract long probeRows(AsyncHashJoinAtom.Slot slot, DirectLongList rows, long from, long to, boolean isDescending, long tupleCap);

    /**
     * Counts the joined rows of a frame's rows, from the given position in the probe scan's order to
     * the end of the frame, into the slot's pair count, which the slot holds for a count: see
     * {@link AsyncHashJoinAtom.Slot#ofCount}. Writes no tuple, and has no cap.
     *
     * @param rowCount     the frame's rows
     * @param from         the position, in the scan's order, of the first row to count
     * @param isDescending true for a backward probe scan
     * @return the slot's pair count, or {@link #CANCELLED} when the execution was cancelled
     */
    abstract long countRows(AsyncHashJoinAtom.Slot slot, long rowCount, long from, boolean isDescending);

    /**
     * Counts the joined rows of the frame's rows that the probe filter kept, as
     * {@link #countRows(AsyncHashJoinAtom.Slot, long, long, boolean)} counts those of every row.
     *
     * @param rows the kept rows' indexes in the frame, ascending
     * @return the slot's pair count, or {@link #CANCELLED} when the execution was cancelled
     */
    abstract long countRows(AsyncHashJoinAtom.Slot slot, DirectLongList rows, long from, boolean isDescending);

    /**
     * Probes this stage for the slot's current probe row, hands each match on to the next stage,
     * and appends the tuples that come out of the last one to the slot's tuples. An INNER join's
     * miss drops the row.
     *
     * @return false when the execution was cancelled, which unwinds the frame's loop
     */
    abstract boolean step(AsyncHashJoinAtom.Slot slot, int stage);
}
