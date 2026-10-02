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

package io.questdb.cairo.lv;

import io.questdb.cairo.TimestampDriver;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

/**
 * Whether a whole-view rebuild from the applied base would change output the live view
 * already retains, decided before the rebuild's replacement commits.
 * <p>
 * Seven routes reach {@code LiveViewRefreshJob}'s whole-view rebuild: a restart that finds
 * the timeline absent (a directory reset for its format, a history-epoch replacement, a
 * timeline an earlier failure retired), a restore that fails, a surviving repair marker, a
 * base schema change the view survives, a refresh that failed mid-drain, and a lost base WAL
 * segment. The two running routes - the schema change and the mid-drain failure - try the
 * restart's own recovery in place first and reach the rebuild only when it cannot restore the
 * accumulators from the view's timeline.
 * The rebuild recomputes the view from the rows its pinned base snapshot holds and replaces
 * the view's whole output, from its lower bound up, with the result. Incremental refresh does
 * not treat base history that way. TTL, DROP/DETACH PARTITION and TRUNCATE are non-DATA
 * operations it walks past: the view keeps the rows it derived from the removed history and
 * its accumulators keep counting them. So once any of them has run, the rebuild drops rows the
 * view has published and restarts accumulations the view had carried forward - a restatement,
 * and a silent one.
 *
 * <h2>The checks</h2>
 * They rest on two properties every live view has: its output's designated timestamp is the
 * base's designated timestamp column, passed through unchanged, and every base row its filter
 * admits produces exactly one output row.
 * <ul>
 *     <li><b>History floor</b>, before the rebuild touches anything: the view holds a row
 *     below the earliest row the pinned base snapshot holds, or holds rows while the base
 *     holds none. That row came from a base row that no longer exists, so the recompute cannot
 *     emit it, and the replacement - which starts at the view's lower bound - deletes it. It
 *     reads two row counts and two minimum timestamps off the transaction files, so a view the
 *     base's TTL has overtaken, which is the common case, is refused without a scan.</li>
 *     <li><b>Row shortfall</b>, after the scan and before the commit: the recompute emits fewer
 *     rows at or below the view's durable frontier than the view's table holds. Every durable
 *     row sits at or below that frontier by definition, so the difference is rows the
 *     replacement would delete and not put back. This is the check that sees a partition
 *     dropped or detached from the middle of the view's range, which leaves the base's
 *     earliest row where it was.</li>
 *     <li><b>Lost partition</b>, before the rebuild touches anything, and only where the row
 *     shortfall stands down (see below): the view holds a row in a range of timestamps no
 *     partition of the pinned base snapshot covers. The base partition that held the row's
 *     source is gone, so the recompute cannot emit it. It reads the base's partition list off
 *     its transaction file and asks the view's table about each range between those partitions
 *     that the view's rows span, opening a partition of the view's table only where the range
 *     cuts through it.</li>
 * </ul>
 *
 * <h2>When it compares less, or nothing</h2>
 * The checks read the view's table as the output of the base transactions the view has
 * consumed, while the rebuild runs at a pinned snapshot that may hold more. The snapshot never
 * holds less: a view draining raw base WAL flushes ahead of the base's own apply, so the rebuild
 * waits for the apply to reach the highest transaction whose output the view's table may hold
 * before it pins, deferring the recovery that asked for it rather than blocking where it cannot.
 * A snapshot behind that point would read the output of the commits it lacks as a loss, which
 * could only refuse a rebuild that restates nothing. The view's un-flushed lead does not count:
 * it is not in the table, and the rebuild drops it. What remains is sound only while every
 * transaction the snapshot holds beyond the view's durable coordinate either adds base rows or
 * removes them the way incremental refresh would have frozen. The guard therefore abstains -
 * lets the rebuild run exactly as it did before the guard existed - when:
 * <ul>
 *     <li>there is nothing to protect: the view's table is empty, or the operator turned the
 *     guard off ({@link #ABSTAIN_DISABLED}, {@link #ABSTAIN_NOTHING_RETAINED});</li>
 *     <li>the view was carried over from an older checkpoint format and rebuilds on upgrade,
 *     where a refusal would leave nothing to resume from ({@link #ABSTAIN_FORMAT_UPGRADE});</li>
 *     <li>a backlog transaction can legitimately lower the output at or below the frontier,
 *     because incremental refresh propagates it and the rebuild restates nothing by following
 *     it: a REPLACE_RANGE commit whose delete band reaches the frontier (a materialized-view
 *     base) or a materialized view's TRUNCATE ({@link #ABSTAIN_BACKLOG_MAY_REMOVE});</li>
 *     <li>a backlog transaction cannot be read, which is what a lost or purged base WAL segment
 *     leaves, and the base is a materialized view, which can produce such a commit
 *     ({@link #ABSTAIN_BACKLOG_UNREADABLE}).</li>
 * </ul>
 * For a view with a filter, a third kind of commit lowers the output legitimately: one that
 * reaches the frontier and that the base applied under dedup, whose replacement row may fail
 * the filter the replaced row passed. The snapshot's dedup flag says whether the base applied a
 * commit under dedup unless an {@code ALTER TABLE ... DEDUP ENABLE} or {@code DEDUP DISABLE}
 * follows the commit, and no other schema change can switch dedup on or off. Such a backlog -
 * a commit read, or an unreadable one over a base whose snapshot deduplicates or whose sequencer
 * records a DEDUP change after it - stands down the row shortfall alone
 * ({@link #disarmRowShortfall}, with the same two abstentions). The history floor still
 * compares, and the lost partition check compares in the row shortfall's place: every dedup
 * key set holds the designated timestamp, so a replacement leaves a base row at each timestamp
 * it replaced, in the partition that held the row it replaced. No such commit can move the
 * base's earliest row past a row the view holds, or take away the partition a row the view
 * holds came from. A DEDUP ENABLE or DISABLE after a commit that ran without dedup reads the
 * same on the sequencer as one after a replacement - the dedup state at a commit is not
 * recoverable - so the row shortfall stands down behind it too, and a loss only the row
 * shortfall sees goes unseen: a base partition lost and then re-created by later commits into
 * its range. Any other base has no such commit to hide, so an unreadable backlog does not stop
 * the checks there. Neither does a DEDUP change the sequencer's metadata change log cannot
 * name: the checks compare, and a refusal that follows is one a restart retries.
 *
 * <h2>What it cannot see</h2>
 * The checks detect restatements; they do not prove there are none. Proving the negative would
 * take a record of every row the base ever removed, and QuestDB keeps none: WAL segments are
 * purged once applied, dropped and detached partitions are not archived, and TTL eviction
 * keeps no journal. What passes the checks and still restates:
 * <ul>
 *     <li>a loss that keeps the row count - the base and the view's own table lost the same
 *     prefix, as a TTL on the view matched to the base's leaves them, so the retained rows'
 *     accumulated values change and their count does not;</li>
 *     <li>a loss the backlog masks - base rows inserted at or below the frontier after the
 *     view's durable coordinate add to the recompute what the loss took from it;</li>
 *     <li>a loss only the row shortfall sees, behind a backlog that stood it down: a base
 *     partition lost and then re-created by later commits into its range;</li>
 *     <li>anything in a backlog the guard abstained over.</li>
 * </ul>
 * <p>
 * One instance per refresh job, armed per whole-view rebuild and left holding that rebuild's
 * evidence afterwards, which is what the job's caller reads to explain a refusal.
 */
public final class LiveViewRebuildRestatementGuard implements Mutable {
    /**
     * A backlog transaction can legitimately lower the output at or below the frontier. When it
     * is a dedup replacement, only the row shortfall stands down, and the history floor and the
     * lost partition check still compare.
     */
    public static final int ABSTAIN_BACKLOG_MAY_REMOVE = 3;
    /**
     * A backlog transaction could not be read, and the base can produce one that lowers the
     * output legitimately. When that one can only be a dedup replacement, only the row shortfall
     * stands down, and the history floor and the lost partition check still compare.
     */
    public static final int ABSTAIN_BACKLOG_UNREADABLE = 4;
    /**
     * {@code cairo.live.view.rebuild.restatement.guard.enabled} is off.
     */
    public static final int ABSTAIN_DISABLED = 1;
    /**
     * The rebuild is the one a view carried over from an older checkpoint format owes on
     * upgrade. The view cannot resume from a layout this build does not read, and stopping it
     * would leave the operator only the same recompute through DROP and re-create, so the
     * rebuild follows the base table. The job still compares the history floor and logs what
     * it finds, without refusing.
     */
    public static final int ABSTAIN_FORMAT_UPGRADE = 5;
    /**
     * The guard is armed and compares the history floor and the row shortfall. A guard that
     * compares less reports the abstention that stood the rest down.
     */
    public static final int ABSTAIN_NONE = 0;
    /**
     * The view's table holds no rows, so no rebuild can drop any.
     */
    public static final int ABSTAIN_NOTHING_RETAINED = 2;
    /**
     * No whole-view rebuild has armed the guard since the job was built.
     */
    public static final int ABSTAIN_NOT_EVALUATED = -1;
    /**
     * Returned by the job's backlog walk: every transaction between the view's durable
     * coordinate and the rebuild's snapshot adds base rows, or removes them the way incremental
     * refresh freezes.
     */
    public static final int BACKLOG_ADDITIVE = 0;
    /**
     * Returned by the job's backlog walk: the only transactions in the backlog that can
     * legitimately take an output row at or below the durable frontier away are dedup
     * replacements a filter may reject, which leave a base row at every timestamp they replaced.
     */
    public static final int BACKLOG_MAY_DEDUP_REPLACE = 3;
    /**
     * Returned by the job's backlog walk: a transaction in the backlog can legitimately remove
     * an output row at or below the durable frontier, and may move the base's earliest row.
     */
    public static final int BACKLOG_MAY_REMOVE = 1;
    /**
     * Returned by the job's backlog walk: a transaction in the backlog could not be read, and
     * the base is a materialized view, which can produce a transaction that removes output
     * legitimately and moves the base's earliest row.
     */
    public static final int BACKLOG_UNREADABLE = 2;
    /**
     * Returned by the job's backlog walk: a transaction in the backlog could not be read, and
     * the only transaction the base can produce that removes output legitimately is a dedup
     * replacement a filter may reject.
     */
    public static final int BACKLOG_UNREADABLE_MAY_DEDUP_REPLACE = 4;
    /**
     * The view holds a row below the pinned base snapshot's earliest row, or holds rows while
     * the base holds none.
     */
    public static final int VERDICT_HISTORY_FLOOR = 1;
    /**
     * The view holds a row in a range of timestamps no partition of the pinned base snapshot
     * covers, found where the row shortfall stood down.
     */
    public static final int VERDICT_LOST_PARTITION = 3;
    /**
     * The guard found nothing, either because it abstained or because the evidence it compared
     * agreed.
     */
    public static final int VERDICT_NONE = 0;
    /**
     * The recompute reproduced fewer rows at or below the durable frontier than the view holds.
     */
    public static final int VERDICT_ROW_SHORTFALL = 2;
    private int abstention = ABSTAIN_NOT_EVALUATED;
    private long baseMinTimestamp = Numbers.LONG_NULL;
    private long baseRows;
    private long durableMaxTimestamp = Numbers.LONG_NULL;
    private long durableMinTimestamp = Numbers.LONG_NULL;
    private long durableRows;
    private boolean isHistoryFloorArmed;
    private long lostPartitionHi = Numbers.LONG_NULL;
    private long lostPartitionLo = Numbers.LONG_NULL;
    private long lostPartitionRowTimestamp = Numbers.LONG_NULL;
    private long reproducedRows;
    private int verdict = VERDICT_NONE;

    /**
     * @param abstention one of the {@code ABSTAIN_} constants
     * @return the abstention's name for a log line
     */
    public static String abstentionName(int abstention) {
        return switch (abstention) {
            case ABSTAIN_NONE -> "none";
            case ABSTAIN_DISABLED -> "disabled";
            case ABSTAIN_NOTHING_RETAINED -> "nothing retained";
            case ABSTAIN_BACKLOG_MAY_REMOVE -> "backlog may remove rows";
            case ABSTAIN_BACKLOG_UNREADABLE -> "backlog unreadable";
            case ABSTAIN_FORMAT_UPGRADE -> "format upgrade";
            default -> "not evaluated";
        };
    }

    /**
     * Appends the evidence behind the verdict, in operator terms. Meaningful only after
     * {@link #refuse}.
     *
     * @param sink   where the text goes
     * @param driver the base's timestamp driver, which formats the timestamps
     */
    public void appendEvidence(@NotNull CharSink<?> sink, @NotNull TimestampDriver driver) {
        switch (verdict) {
            case VERDICT_HISTORY_FLOOR -> {
                sink.put("the view holds rows from ");
                driver.append(sink, durableMinTimestamp);
                if (baseRows > 0) {
                    sink.put(" but the base table's earliest row is at ");
                    driver.append(sink, baseMinTimestamp);
                } else {
                    sink.put(" but the base table holds no rows");
                }
            }
            case VERDICT_ROW_SHORTFALL -> {
                sink.put("the rebuild reproduces ").put(reproducedRows)
                        .put(" of the ").put(durableRows).put(" rows the view holds up to ");
                driver.append(sink, durableMaxTimestamp);
            }
            case VERDICT_LOST_PARTITION -> {
                sink.put("the view holds a row at ");
                driver.append(sink, lostPartitionRowTimestamp);
                sink.put(" but the base table holds no partition between ");
                driver.append(sink, lostPartitionLo);
                sink.put(" and ");
                driver.append(sink, lostPartitionHi);
            }
            default -> sink.put("no restatement found");
        }
    }

    /**
     * Arms the guard for one rebuild with the evidence the history floor and the row shortfall
     * compare against.
     *
     * @param durableRows         rows the view's table holds
     * @param durableMinTimestamp the view table's earliest designated timestamp
     * @param durableMaxTimestamp the view table's latest designated timestamp: the frontier
     *                            the row shortfall counts up to
     * @param baseRows            rows the rebuild's pinned base snapshot holds
     * @param baseMinTimestamp    the snapshot's earliest designated timestamp; ignored when it
     *                            holds no rows
     */
    public void arm(
            long durableRows,
            long durableMinTimestamp,
            long durableMaxTimestamp,
            long baseRows,
            long baseMinTimestamp
    ) {
        assert durableRows > 0 : "a view with no rows has nothing for the guard to protect";
        clear();
        this.abstention = ABSTAIN_NONE;
        this.isHistoryFloorArmed = true;
        this.durableRows = durableRows;
        this.durableMinTimestamp = durableMinTimestamp;
        this.durableMaxTimestamp = durableMaxTimestamp;
        this.baseRows = baseRows;
        this.baseMinTimestamp = baseRows > 0 ? baseMinTimestamp : Numbers.LONG_NULL;
    }

    /**
     * Returns the guard to its unevaluated state.
     */
    @Override
    public void clear() {
        abstention = ABSTAIN_NOT_EVALUATED;
        baseMinTimestamp = Numbers.LONG_NULL;
        baseRows = 0;
        durableMaxTimestamp = Numbers.LONG_NULL;
        durableMinTimestamp = Numbers.LONG_NULL;
        durableRows = 0;
        isHistoryFloorArmed = false;
        lostPartitionHi = Numbers.LONG_NULL;
        lostPartitionLo = Numbers.LONG_NULL;
        lostPartitionRowTimestamp = Numbers.LONG_NULL;
        reproducedRows = 0;
        verdict = VERDICT_NONE;
    }

    /**
     * Records that the guard compares nothing for this rebuild, and why.
     *
     * @param abstention one of the {@code ABSTAIN_} constants other than {@link #ABSTAIN_NONE}
     */
    public void disarm(int abstention) {
        assert abstention != ABSTAIN_NONE;
        clear();
        this.abstention = abstention;
    }

    /**
     * Stands the row shortfall down for the rebuild the guard is armed for, and records why. The
     * history floor keeps comparing, and the lost partition check compares in the row shortfall's
     * place ({@link #observeLostPartition}): this is for a backlog whose only legitimate removals
     * are dedup replacements, which leave the base's earliest row and every base partition where
     * they were.
     *
     * @param abstention {@link #ABSTAIN_BACKLOG_MAY_REMOVE} or {@link #ABSTAIN_BACKLOG_UNREADABLE}
     */
    public void disarmRowShortfall(int abstention) {
        assert this.abstention == ABSTAIN_NONE && isHistoryFloorArmed : "the guard must be armed";
        assert abstention == ABSTAIN_BACKLOG_MAY_REMOVE || abstention == ABSTAIN_BACKLOG_UNREADABLE;
        this.abstention = abstention;
    }

    /**
     * @return why the last rebuild compared nothing, or stood the row shortfall down and compared
     * the history floor and the lost partition check in its place, or {@link #ABSTAIN_NONE} when it
     * compared the history floor and the row shortfall
     */
    public int getAbstention() {
        return abstention;
    }

    /**
     * @return rows the pinned base snapshot held when the guard was armed
     */
    public long getBaseRows() {
        return baseRows;
    }

    /**
     * @return rows the view's table held when the guard was armed
     */
    public long getDurableRows() {
        return durableRows;
    }

    /**
     * @return rows the recompute emitted at or below the durable frontier so far
     */
    public long getReproducedRows() {
        return reproducedRows;
    }

    /**
     * @return one of the {@code VERDICT_} constants
     */
    public int getVerdict() {
        return verdict;
    }

    /**
     * The lost partition check, over what {@link #observeLostPartition} recorded. Ask it where
     * the history floor is asked, and after it.
     *
     * @return true when the view holds a row in a range no partition of the pinned base snapshot
     * covers
     */
    public boolean isBasePartitionLost() {
        return isHistoryFloorArmed && lostPartitionRowTimestamp != Numbers.LONG_NULL;
    }

    /**
     * The check that needs no scan. Ask before the rebuild retires, wipes or replays anything,
     * so a refusal leaves the view exactly as the rebuild found it.
     *
     * @return true when the view holds a row below the pinned base snapshot's earliest row, or
     * holds rows while the base holds none
     */
    public boolean isHistoryFloorBreached() {
        // An empty base holds no row at all, so every row the view keeps is below its floor.
        // The comparison is strict: a base row AT the view's earliest timestamp may be the one
        // that produced it.
        return isHistoryFloorArmed && (baseRows == 0 || durableMinTimestamp < baseMinTimestamp);
    }

    /**
     * The check that needs the scan. Ask once the recompute has emitted every row and before
     * the replacement commits; before that the count is partial.
     *
     * @return true when the recompute reproduces fewer rows at or below the durable frontier
     * than the view's table holds
     */
    public boolean isRowShortfall() {
        return abstention == ABSTAIN_NONE && reproducedRows < durableRows;
    }

    /**
     * Counts one row the recompute emitted. Rows must be the ones the replacement would commit,
     * each passed once.
     *
     * @param timestamp the emitted row's designated timestamp
     */
    public void observe(long timestamp) {
        if (abstention == ABSTAIN_NONE && timestamp <= durableMaxTimestamp) {
            reproducedRows++;
        }
    }

    /**
     * Records a row the view's table holds in a range no partition of the pinned base snapshot
     * covers, which {@link #isBasePartitionLost} then reports. The job looks for one only once
     * {@link #disarmRowShortfall} has stood the row shortfall down.
     *
     * @param rowTimestamp the row's designated timestamp
     * @param partitionLo  the start of the base partition the row belongs to, inclusive
     * @param partitionHi  the end of that partition, exclusive
     */
    public void observeLostPartition(long rowTimestamp, long partitionLo, long partitionHi) {
        assert isHistoryFloorArmed && abstention != ABSTAIN_NONE : "the row shortfall must have stood down";
        assert rowTimestamp != Numbers.LONG_NULL && partitionLo <= rowTimestamp && rowTimestamp < partitionHi;
        this.lostPartitionRowTimestamp = rowTimestamp;
        this.lostPartitionLo = partitionLo;
        this.lostPartitionHi = partitionHi;
    }

    /**
     * Records the check that refused the rebuild, which is what {@link #appendEvidence} then
     * explains. Recorded by the refusal rather than by the check, so a rebuild that failed for
     * any other reason part-way through its scan never reads as a shortfall.
     *
     * @param verdict {@link #VERDICT_HISTORY_FLOOR}, {@link #VERDICT_LOST_PARTITION} or
     *                {@link #VERDICT_ROW_SHORTFALL}
     */
    public void refuse(int verdict) {
        assert switch (verdict) {
            case VERDICT_HISTORY_FLOOR -> isHistoryFloorBreached();
            case VERDICT_LOST_PARTITION -> isBasePartitionLost();
            case VERDICT_ROW_SHORTFALL -> isRowShortfall();
            default -> false;
        };
        this.verdict = verdict;
    }
}
