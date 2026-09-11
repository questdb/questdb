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
 * <h2>The two checks</h2>
 * Both rest on two properties every live view has: its output's designated timestamp is the
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
 * </ul>
 *
 * <h2>When it compares nothing</h2>
 * Both checks read the view's table as the output of the base transactions the view has
 * consumed, while the rebuild runs at a pinned snapshot that may hold more. That is sound only
 * while every transaction the snapshot holds beyond the view's durable coordinate either adds
 * base rows or removes them the way incremental refresh would have frozen. The guard therefore
 * abstains - lets the rebuild run exactly as it did before the guard existed - when:
 * <ul>
 *     <li>there is nothing to protect: the view's table is empty, or the operator turned the
 *     guard off ({@link #ABSTAIN_DISABLED}, {@link #ABSTAIN_NOTHING_RETAINED});</li>
 *     <li>the snapshot is behind the view, which a view draining raw base WAL ahead of the
 *     base's own apply can be: its table then holds output of transactions the snapshot does
 *     not ({@link #ABSTAIN_SNAPSHOT_BEHIND});</li>
 *     <li>a backlog transaction can legitimately lower the output at or below the frontier,
 *     because incremental refresh propagates it and the rebuild restates nothing by following
 *     it: a REPLACE_RANGE commit whose delete band reaches the frontier (a materialized-view
 *     base), a materialized view's TRUNCATE, or - for a view with a filter - a commit on a
 *     deduplicating base that reaches the frontier, whose replacement row may fail the filter
 *     the replaced row passed ({@link #ABSTAIN_BACKLOG_MAY_REMOVE});</li>
 *     <li>a backlog transaction cannot be read, which is what a lost base WAL segment leaves,
 *     and the base is one that can produce such a commit ({@link #ABSTAIN_BACKLOG_UNREADABLE}).
 *     A base that is neither a materialized view nor, for a filtering view, deduplicating has
 *     no such commit to hide, so an unreadable backlog does not stop the checks there.</li>
 * </ul>
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
 *     <li>anything in a backlog the guard abstained over.</li>
 * </ul>
 * <p>
 * One instance per refresh job, armed per whole-view rebuild and left holding that rebuild's
 * evidence afterwards, which is what the job's caller reads to explain a refusal.
 */
public final class LiveViewRebuildRestatementGuard implements Mutable {
    /**
     * A backlog transaction can legitimately lower the output at or below the frontier.
     */
    public static final int ABSTAIN_BACKLOG_MAY_REMOVE = 4;
    /**
     * A backlog transaction could not be read, and the base can produce one that lowers the
     * output legitimately.
     */
    public static final int ABSTAIN_BACKLOG_UNREADABLE = 5;
    /**
     * {@code cairo.live.view.rebuild.restatement.guard.enabled} is off.
     */
    public static final int ABSTAIN_DISABLED = 1;
    /**
     * The guard is armed and compares.
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
     * The rebuild's pinned snapshot is behind the view's own coordinate.
     */
    public static final int ABSTAIN_SNAPSHOT_BEHIND = 3;
    /**
     * Returned by the job's backlog walk: every transaction between the view's durable
     * coordinate and the rebuild's snapshot adds base rows, or removes them the way incremental
     * refresh freezes.
     */
    public static final int BACKLOG_ADDITIVE = 0;
    /**
     * Returned by the job's backlog walk: a transaction in the backlog can legitimately remove
     * or filter out an output row at or below the durable frontier.
     */
    public static final int BACKLOG_MAY_REMOVE = 1;
    /**
     * Returned by the job's backlog walk: a transaction in the backlog could not be read, and
     * the base is one that can produce a transaction that removes output legitimately.
     */
    public static final int BACKLOG_UNREADABLE = 2;
    /**
     * The view holds a row below the pinned base snapshot's earliest row, or holds rows while
     * the base holds none.
     */
    public static final int VERDICT_HISTORY_FLOOR = 1;
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
            case ABSTAIN_SNAPSHOT_BEHIND -> "snapshot behind the view";
            case ABSTAIN_BACKLOG_MAY_REMOVE -> "backlog may remove rows";
            case ABSTAIN_BACKLOG_UNREADABLE -> "backlog unreadable";
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
            default -> sink.put("no restatement found");
        }
    }

    /**
     * Arms the guard for one rebuild with the evidence both checks compare against.
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
     * @return why the last rebuild compared nothing, or {@link #ABSTAIN_NONE} when it compared
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
     * The check that needs no scan. Ask before the rebuild retires, wipes or replays anything,
     * so a refusal leaves the view exactly as the rebuild found it.
     *
     * @return true when the view holds a row the pinned base snapshot has no history for
     */
    public boolean isHistoryFloorBreached() {
        // An empty base holds no row at all, so every row the view keeps is below its floor.
        // The comparison is strict: a base row AT the view's earliest timestamp may be the one
        // that produced it.
        return abstention == ABSTAIN_NONE && (baseRows == 0 || durableMinTimestamp < baseMinTimestamp);
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
     * Records the check that refused the rebuild, which is what {@link #appendEvidence} then
     * explains. Recorded by the refusal rather than by the check, so a rebuild that failed for
     * any other reason part-way through its scan never reads as a shortfall.
     *
     * @param verdict {@link #VERDICT_HISTORY_FLOOR} or {@link #VERDICT_ROW_SHORTFALL}
     */
    public void refuse(int verdict) {
        assert verdict == VERDICT_HISTORY_FLOOR ? isHistoryFloorBreached() : verdict == VERDICT_ROW_SHORTFALL && isRowShortfall();
        this.verdict = verdict;
    }
}
