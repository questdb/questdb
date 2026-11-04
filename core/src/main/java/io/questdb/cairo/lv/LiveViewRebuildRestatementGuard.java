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
 * Rejects a whole-view rebuild that would drop retained output, before replacement
 * commits. Incremental refresh preserves output for ordinary base TTL, partition loss
 * and TRUNCATE; recomputing from surviving rows can silently restate that history.
 * <p>
 * The checks rely on unchanged designated timestamps and one output per qualifying
 * base row. Before replay, reject a view prefix below the base's earliest row. Before
 * commit, reject fewer recomputed rows through the durable frontier. When shortfall
 * cannot be compared, check for view rows in timestamp ranges with no base partition.
 * The pinned base must include every transaction represented by durable view output;
 * an unflushed lead is outside this comparison.
 * <p>
 * Abstain for disabled checking, empty output, older-format upgrades, dedup-restore
 * mismatch, or a materialized-view backlog that legitimately removes output. Unreadable
 * backlog weakens checks only when the base can hide such removals. Filtered dedup
 * replacement disables shortfall alone: timestamps and partitions remain protected.
 * Use the snapshot dedup flag and subsequent DEDUP changes; other schema changes do
 * not prove replacement. Earlier dedup state may be unknowable after a later change.
 * <p>
 * These checks detect loss but cannot prove equivalence. Equal-count loss, surplus
 * backlog masking removed rows, recreated partitions with shortfall disabled, and
 * abstained rebuilds can still restate output. One worker arms this guard per rebuild
 * and retains the evidence afterward for the refusal reason.
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
     * The rebuild stands in for a restore from the view's checkpoint timeline whose replay of the
     * raw base WAL did not reproduce the view's durable row count and frontier, and the pinned
     * snapshot deduplicates: a duplicate the base's dedup keys collapsed between the newest root
     * and the frontier fails the restore, since the view drained the one row the base kept and
     * the replay feeds both. The rebuild follows the base table, as every rebuild did before the
     * guard, rather than stop a view whose timeline may be intact.
     */
    public static final int ABSTAIN_DEDUP_RESTORE_MISMATCH = 6;
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
            case ABSTAIN_DEDUP_RESTORE_MISMATCH -> "dedup restore mismatch";
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
