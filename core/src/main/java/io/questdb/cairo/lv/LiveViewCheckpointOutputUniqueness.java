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
import io.questdb.std.CompactIntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

/**
 * Checks uniqueness of corrected (timestamp, projected key) pairs before sparse
 * upsert. Duplicate output would collapse rows and invalidate checkpoint positions.
 * Stored-row pairing is a separate obligation of {@link LiveViewCheckpointKeyedReplay}:
 * uniqueness alone does not prove an upsert removes every superseded row.
 * <p>
 * Replay is timestamp-ordered, but equal keys within a timestamp need not be adjacent.
 * A per-group set detects duplicates; singleton groups stay scalar, and clearing
 * costs the number of occupied entries rather than the retained table capacity.
 * <p>
 * Keys are SYMBOL integers from the pinned reader, whose mapping remains stable
 * across parked turns. -1 is the set's empty marker; SQL null is INT_NULL.
 * The session saves and restores the detector, including the current group, so a
 * park between duplicate rows cannot hide the collision.
 */
public final class LiveViewCheckpointOutputUniqueness implements Mutable {
    /**
     * Passed to {@link #of(int)} for output that carries no key this can name, which
     * leaves the detector disarmed and the repair unchecked.
     */
    public static final int NO_KEY_COLUMN = -1;
    // The keys of the timestamp group being walked, empty until that group holds a second
    // row. Never holds the set's own -1 marker, for the reason the class comment gives.
    private final GroupKeySet groupKeys = new GroupKeySet();
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
        groupKeys.clearKeys();
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
                groupKeys.clearKeys();
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

    /**
     * The group scratch: a set that empties in work proportional to the keys it holds,
     * not to the table it has grown.
     * <p>
     * The table only ever grows. One wide group - a whole-second snapshot of every account -
     * leaves it sized for that group for as long as the worker lives, and the worker's one
     * detector serves every repair of every view on it. A clear that swept the whole table
     * would charge each later group of two for the widest group the worker ever walked, on
     * every group close, every re-arm and every resumed park. So {@link #clearKeys()}
     * sweeps the whole table only while the keys fill enough of it that the sweep is the
     * cheaper way, and otherwise erases just the clusters those keys sit in.
     * <p>
     * Public and open to extension only so that a test can stand a failing set in for the
     * copy a park makes, whose allocation failure has no reproducible natural producer.
     */
    public static class GroupKeySet extends CompactIntHashSet {
        // Past this many table slots per key, erasing the keys' clusters beats sweeping the
        // table. A sweep writes a slot for a small fraction of what finding and erasing a key
        // costs, so either path stays within a constant factor of the keys themselves.
        private static final int SLOTS_PER_KEY_TO_SWEEP = 32;
        // The keys in insertion order: what a clear erases and what a park copies.
        private final IntList keyList = new IntList();

        public GroupKeySet() {
            super(MIN_INITIAL_CAPACITY, 0.4);
        }

        @Override
        public boolean add(int key) {
            final int index = keyIndex(key);
            if (index < 0) {
                return false;
            }
            // Listed ahead of the table write. A list that fails to grow changes nothing,
            // while the table takes the key before its own growth can fail, so either way
            // the list holds every key the table does - which is what a clear and a park
            // both walk.
            keyList.add(key);
            addAt(index, key);
            return true;
        }

        public int get(int index) {
            return keyList.getQuick(index);
        }

        /**
         * Empties the set.
         * <p>
         * Linear probing leaves every key inside the unbroken run of occupied slots that
         * starts at its home slot, {@code key & mask}. Erasing forward from a home slot to
         * the next empty one therefore takes that key along with the rest of its cluster,
         * all of which is leaving anyway. A key whose home slot an earlier erasure already
         * emptied left with that erasure: the emptied part of a cluster is always a tail of
         * it, and a key never sits before its home slot. So each occupied slot is written
         * once, and nothing is read past the empty slot that ends each of those clusters.
         * <p>
         * That stands on the probe {@link #keyIndex} runs, so the erasure counts the slots
         * it empties rather than trusting it. A count that falls short of the set's size
         * means a key sat somewhere its cluster did not reach, and the whole table gets
         * swept: a key left behind would make the same key in a later group a false
         * duplicate.
         */
        private void clearKeys() {
            final int size = size();
            if (keys.length <= SLOTS_PER_KEY_TO_SWEEP * (long) size) {
                keyList.clear();
                clear();
                return;
            }
            int erased = 0;
            for (int i = 0, n = keyList.size(); i < n; i++) {
                for (int slot = keyList.getQuick(i) & mask; keys[slot] != noEntryKeyValue; slot = (slot + 1) & mask) {
                    keys[slot] = noEntryKeyValue;
                    erased++;
                }
            }
            keyList.clear();
            if (erased == size) {
                free = capacity;
            } else {
                clear();
            }
        }
    }
}
