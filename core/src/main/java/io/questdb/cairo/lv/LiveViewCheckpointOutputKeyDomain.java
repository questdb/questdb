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

import io.questdb.std.Hash;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * {@code Q}, the output key domain of one localized repair: the partition keys the
 * timestamp-global replacement re-emits, in the encoding a checkpoint partition map
 * keys an entry by.
 * <p>
 * A repair whose replay reconstructs every live key needs none of this - the state it
 * freezes is the whole truth about every boundary it crosses. A ROWS dependency does
 * not reconstruct every key: {@code L} only warms up the keys with a row in
 * {@code [R, H)}, so a key outside that set ends the replay holding whatever rows
 * happened to fall inside {@code [L, H)} rather than its real last {@code Nmax}. This
 * set is what lets the publication tell the two apart - it takes the replay's entry
 * for a key inside {@code Q} and leaves every key outside it exactly as the old root
 * wrote it, which is correct because a key with no qualifying row in {@code [R, H)}
 * is a key the change did not touch.
 * <p>
 * The encoding is the one {@link LiveViewSnapshotKeyCodec} writes off a window
 * function's own map record, because that is what the two sides have to be comparable
 * in. {@link LiveViewCheckpointRowsPlan#getCheckpointKeySink()} is what produces it
 * from a base row: a SYMBOL partition column is a resolved STRING on both sides, never
 * a reader-local integer.
 * <p>
 * Keys are held by reference rather than copied, and never mutated once added, so
 * {@link #copyFrom} shares the arrays it was handed. The repair plan derives one of
 * these per repair and the capture takes its own set from it, because the plan is
 * refilled by the next repair while a parked capture still owes its publication.
 * <p>
 * The set is open-addressed over the key arrays themselves, in the shape
 * {@code AbstractCharSequenceHashSet} uses, rather than a {@code HashSet} of wrappers.
 * Membership is asked once per key per function root the boundary writes, so the probe
 * count is the key domain times the roots, and a wrapper allocated per probe would be
 * charged to every publication. Matching is on content: the key a seal probes with is a
 * fresh array encoded off its own map record, never the array the repair plan added.
 */
public final class LiveViewCheckpointOutputKeyDomain implements Mutable {
    private static final double LOAD_FACTOR = 0.4;
    private static final int MIN_INITIAL_CAPACITY = 16;
    private int capacity;
    private int free;
    private byte[][] keys;
    private int mask;

    public LiveViewCheckpointOutputKeyDomain() {
        capacity = MIN_INITIAL_CAPACITY;
        free = capacity;
        final int slotCount = Numbers.ceilPow2((int) (capacity / LOAD_FACTOR));
        keys = new byte[slotCount][];
        mask = slotCount - 1;
    }

    /**
     * Joins one encoded partition key to the domain. The array must not be mutated
     * afterwards: the set holds it rather than a copy. Adding a key the domain already
     * holds leaves it as it is.
     */
    public void add(@NotNull byte[] key) {
        final int index = keyIndex(key);
        if (index < 0) {
            return;
        }
        keys[index] = key;
        if (--free < 1) {
            rehash();
        }
    }

    /**
     * Empties the domain, keeping the slot array it has grown to so a reused capture does
     * not pay for the growth again.
     */
    @Override
    public void clear() {
        Arrays.fill(keys, null);
        free = capacity;
    }

    public boolean contains(@NotNull byte[] key) {
        return keyIndex(key) < 0;
    }

    /**
     * Replaces this domain with {@code other}'s, sharing its key arrays.
     */
    public void copyFrom(@NotNull LiveViewCheckpointOutputKeyDomain other) {
        clear();
        final byte[][] otherKeys = other.keys;
        for (int i = 0, n = otherKeys.length; i < n; i++) {
            final byte[] key = otherKeys[i];
            if (key != null) {
                add(key);
            }
        }
    }

    /**
     * Hands every key this domain holds to {@code visitor}, in slot order.
     * <p>
     * The order is the hash table's rather than any order the caller put the keys in,
     * which is what a key-scoped restore wants: it looks each one up in a persistent tree
     * whose descents the reader memoizes, and slot order is as good as any for that.
     */
    public void forEach(@NotNull Visitor visitor) {
        for (int i = 0, n = keys.length; i < n; i++) {
            final byte[] key = keys[i];
            if (key != null) {
                visitor.visit(key);
            }
        }
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public int size() {
        return capacity - free;
    }

    /**
     * The slot {@code key} belongs in when the domain does not hold it, or
     * {@code -slot - 1} when it does.
     */
    private int keyIndex(byte[] key) {
        final int index = Hash.spread(Arrays.hashCode(key)) & mask;
        final byte[] slot = keys[index];
        if (slot == null) {
            return index;
        }
        if (Arrays.equals(slot, key)) {
            return -index - 1;
        }
        return probe(key, index);
    }

    private int probe(byte[] key, int index) {
        do {
            index = (index + 1) & mask;
            final byte[] slot = keys[index];
            if (slot == null) {
                return index;
            }
            if (Arrays.equals(slot, key)) {
                return -index - 1;
            }
        } while (true);
    }

    private void rehash() {
        final byte[][] oldKeys = keys;
        capacity *= 2;
        free = capacity;
        final int slotCount = Numbers.ceilPow2((int) (capacity / LOAD_FACTOR));
        keys = new byte[slotCount][];
        mask = slotCount - 1;
        for (int i = 0, n = oldKeys.length; i < n; i++) {
            final byte[] key = oldKeys[i];
            if (key != null) {
                keys[keyIndex(key)] = key;
                free--;
            }
        }
    }

    @FunctionalInterface
    public interface Visitor {
        void visit(@NotNull byte[] key);
    }
}
