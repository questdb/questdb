/*******************************************************************************
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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.IntHashSet;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

/**
 * Whether one repair's qualifying output holds two rows carrying the same
 * {@code (designated timestamp, projected partition key)} pair.
 * <p>
 * That pair is the identity a <b>sparse</b> publication would have to stand on. A repair
 * publishes with {@code WAL_DEDUP_MODE_REPLACE_RANGE}, which deletes the replaced interval
 * wholesale and so has to carry every row of it - including, for a keyed repair, the rows
 * {@link LiveViewCheckpointKeyedReplay}'s merge copies forward untouched. Publishing only
 * the affected keys' rows instead needs {@code WAL_DEDUP_MODE_UPSERT_NEW} over dedup keys,
 * and dedup collapses rows that share those keys. So the pair has to be proved unique over
 * the corrected output before anything is published on it: a pair admitted twice would be
 * silently collapsed to one row, and the cadence boundaries - whose
 * {@link LiveViewCheckpointTimelineEntry#baseLvRowPosition} counts live-view rows - would
 * then describe a row set nothing wrote.
 * <p>
 * Insert-only, which the repair envelope already requires, is what makes the check over
 * the <i>new</i> output sufficient against the <i>durable</i> rows it replaces as well: no
 * qualifying row can disappear, so a key's corrected group is at least as large as the
 * stored one it supersedes, and a corrected group of one cannot conceal a stored group of
 * two.
 *
 * <h2>It runs dark</h2>
 * Nothing reads the verdict yet. Every repair publishes the whole replaced range with
 * {@code REPLACE_RANGE} exactly as it did before this existed, and what the detector
 * produces is the rate at which a sparse publication would have to fall back to that -
 * which is the measurement the keyed-publication stage is decided on, and the one Stage 0
 * could not take without a running view. Gating the publication on
 * {@link #isUnique()} is that stage's own item.
 *
 * <h2>Why a set rather than an adjacency test</h2>
 * A replay emits rows in ascending designated-timestamp order, so all the rows of one
 * timestamp are contiguous - but their keys arrive in whatever order the base holds them,
 * so two rows sharing a key need not be adjacent. The scratch is therefore a set scoped to
 * one timestamp group, cleared as the group closes, and never larger than the widest group
 * the output holds ({@link #getMaxGroupRows()} reports it).
 * <p>
 * A group of one - which is every group of a view whose keys report distinct timestamps -
 * never touches the set at all: the first row of a group is held in a scalar and only
 * copied in when a second row of the same timestamp arrives. That is what keeps an exact
 * check affordable on the shape a whole-segment replay walks, which is millions of rows.
 *
 * <h2>The key is the reader's symbol integer</h2>
 * A symbol map is a bijection over one reader's lifetime, so equal integers name equal
 * strings and distinct integers name distinct ones. One repair replays against one pinned
 * snapshot, which is exactly the scope a group is compared in - and a repair that parks
 * keeps that snapshot, so the integers a resumed turn compares against are the ones its
 * predecessor recorded. {@code -1} - the set's own empty-slot marker - is not a symbol key:
 * values number from zero and the null symbol is {@link SymbolTable#VALUE_IS_NULL}, which
 * is {@link Numbers#INT_NULL}.
 *
 * <h2>Ownership</h2>
 * One instance per refresh job, armed per repair through {@link #of(int)}. A repair that
 * parks mid-replay hands its state to {@link LiveViewCheckpointRepairSession}, which
 * {@link #copyFrom} puts back on the turn that resumes: a duplicate whose two rows sit on
 * either side of a park is still a duplicate, and a detector re-armed from scratch would
 * not see it.
 */
public final class LiveViewCheckpointOutputUniqueness implements Mutable {
    /**
     * Passed to {@link #of(int)} for output that carries no key this can name, which
     * leaves the detector disarmed and the repair unchecked.
     */
    public static final int NO_KEY_COLUMN = -1;
    // The keys of the timestamp group being walked, empty until that group holds a second
    // row. Never holds the set's own -1 marker, for the reason the class comment gives.
    private final IntHashSet groupKeys = new IntHashSet();
    private long checkedRows;
    private long duplicateRows;
    private int firstDuplicateKey = SymbolTable.VALUE_NOT_FOUND;
    private long firstDuplicateTs = Numbers.LONG_NULL;
    private int groupFirstKey = SymbolTable.VALUE_NOT_FOUND;
    private long groupRows;
    private long groupTs;
    private int keyColumnIndex = NO_KEY_COLUMN;
    private long maxGroupRows;

    @Override
    public void clear() {
        groupKeys.clear();
        checkedRows = 0;
        duplicateRows = 0;
        firstDuplicateKey = SymbolTable.VALUE_NOT_FOUND;
        firstDuplicateTs = Numbers.LONG_NULL;
        groupFirstKey = SymbolTable.VALUE_NOT_FOUND;
        groupRows = 0;
        groupTs = 0;
        keyColumnIndex = NO_KEY_COLUMN;
        maxGroupRows = 0;
    }

    /**
     * Takes over the state of a repair that parked mid-replay, including the timestamp
     * group it stopped inside.
     * <p>
     * Copied rather than referenced: the session holding it outlives the turn, while this
     * instance is the worker's own scratch and the next repair it classifies re-arms it.
     */
    public void copyFrom(@NotNull LiveViewCheckpointOutputUniqueness that) {
        clear();
        keyColumnIndex = that.keyColumnIndex;
        checkedRows = that.checkedRows;
        duplicateRows = that.duplicateRows;
        firstDuplicateKey = that.firstDuplicateKey;
        firstDuplicateTs = that.firstDuplicateTs;
        groupFirstKey = that.groupFirstKey;
        groupRows = that.groupRows;
        groupTs = that.groupTs;
        maxGroupRows = that.maxGroupRows;
        for (int i = 0, n = that.groupKeys.size(); i < n; i++) {
            groupKeys.add(that.groupKeys.get(i));
        }
    }

    /**
     * @return qualifying output rows this repair walked
     */
    public long getCheckedRows() {
        return checkedRows;
    }

    /**
     * @return rows whose {@code (timestamp, key)} pair a row already walked in the same
     * repair had taken. A sparse publication would lose exactly these.
     */
    public long getDuplicateRows() {
        return duplicateRows;
    }

    /**
     * @return the symbol key of the first duplicate, or {@link SymbolTable#VALUE_NOT_FOUND}
     * when the output is unique
     */
    public int getFirstDuplicateKey() {
        return firstDuplicateKey;
    }

    /**
     * @return the designated timestamp of the first duplicate, or {@link Numbers#LONG_NULL}
     * when the output is unique
     */
    public long getFirstDuplicateTs() {
        return firstDuplicateTs;
    }

    /**
     * @return the key column's index in the record the caller reads, or
     * {@link #NO_KEY_COLUMN}
     */
    public int getKeyColumnIndex() {
        return keyColumnIndex;
    }

    /**
     * @return rows in the widest equal-timestamp group this repair emitted, which is what
     * the scratch above is worth and how close the output sits to a duplicate
     */
    public long getMaxGroupRows() {
        return maxGroupRows;
    }

    public boolean isArmed() {
        return keyColumnIndex != NO_KEY_COLUMN;
    }

    /**
     * @return whether every pair this repair emitted was distinct. Meaningless on a
     * disarmed detector, which observes nothing.
     */
    public boolean isUnique() {
        return duplicateRows == 0;
    }

    /**
     * Arms the detector for one repair.
     *
     * @param keyColumnIndex the projected partition key's column index in the record the
     *                       replay emits, or {@link #NO_KEY_COLUMN} to leave the repair
     *                       unchecked
     */
    public void of(int keyColumnIndex) {
        clear();
        this.keyColumnIndex = keyColumnIndex;
    }

    /**
     * Walks one qualifying output row. Rows must arrive in ascending timestamp order,
     * which is what a replay emits and what lets the scratch hold one group at a time.
     *
     * @param ts  the row's designated timestamp
     * @param key the row's projected partition key, as the pinned reader's symbol integer
     * @return false when the pair repeats one this repair has already emitted
     */
    public boolean observe(long ts, int key) {
        if (keyColumnIndex == NO_KEY_COLUMN) {
            return true;
        }
        checkedRows++;
        if (groupRows == 0 || ts != groupTs) {
            // The group before this one is closed, so its scratch goes with it - and only
            // when it actually took any, which a group of one never does.
            if (groupKeys.size() > 0) {
                groupKeys.clear();
            }
            groupTs = ts;
            groupFirstKey = key;
            groupRows = 1;
            if (maxGroupRows == 0) {
                maxGroupRows = 1;
            }
            return true;
        }
        groupRows++;
        if (groupRows > maxGroupRows) {
            maxGroupRows = groupRows;
        }
        if (groupRows == 2) {
            // The group has stopped being a scalar. Its first row goes in before this one,
            // so the pair the two of them may form is compared rather than assumed apart.
            groupKeys.add(groupFirstKey);
        }
        if (groupKeys.add(key)) {
            return true;
        }
        duplicateRows++;
        if (duplicateRows == 1) {
            firstDuplicateTs = ts;
            firstDuplicateKey = key;
        }
        return false;
    }

    /**
     * The projected partition key's column index in the record a replay emits, which is the
     * half of {@code (designated timestamp, projected partition key)} that is not the
     * timestamp.
     * <p>
     * One resolution serves two callers, and they have to agree: CREATE marks this column
     * and the designated timestamp as the view table's dedup keys, and a repair names the
     * same column when it checks whether its output could be published on that pair. A view
     * whose dedup keys and whose checked pair named different columns would prove one
     * identity and publish on another.
     * <p>
     * Deliberately NOT the keyed scan's column, which answers a different question with the
     * same vocabulary. That one asks whether a repair can <b>read</b> one key's rows through
     * a posting index, so it turns on the index; this one asks whether a repair's output can
     * be <b>named</b> by its key, which an index has nothing to do with. A view whose key
     * column carries no index still publishes rows that carry the key, and its duplicate
     * rate is exactly as interesting.
     * <p>
     * {@link #NO_KEY_COLUMN} for anything a symbol integer cannot name: a compound or
     * expression PARTITION BY, a key of another type, or a key the view's SELECT does not
     * carry into its output. A repair of such a view is counted unchecked rather than
     * denied, and its table carries no dedup keys - there would be no identity to publish
     * on.
     */
    public static int outputKeyColumnIndex(@NotNull LiveViewCompiledPlan compiledPlan) {
        final LiveViewCheckpointKeyProjector projector =
                compiledPlan.getWindowFactory().getCheckpointKeyProjector();
        if (projector == null || projector.getPartitionByColumnCount() != 1) {
            return NO_KEY_COLUMN;
        }
        final int scanColumnIndex =
                compiledPlan.traceWindowInputColumnToBaseScan(projector.getPartitionByColumnIndex(0));
        if (scanColumnIndex < 0) {
            return NO_KEY_COLUMN;
        }
        final RecordMetadata outputMetadata = compiledPlan.getOutputMetadata();
        for (int i = 0, n = outputMetadata.getColumnCount(); i < n; i++) {
            // The trace is exact rather than a name match, for the reason
            // traceOutputColumnToBaseScan gives: an alias would defeat the name.
            if (compiledPlan.traceOutputColumnToBaseScan(i) == scanColumnIndex
                    && ColumnType.isSymbol(outputMetadata.getColumnType(i))) {
                return i;
            }
        }
        return NO_KEY_COLUMN;
    }
}
