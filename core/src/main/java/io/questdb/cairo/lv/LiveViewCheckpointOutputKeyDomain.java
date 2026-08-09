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

import io.questdb.std.Mutable;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.HashSet;

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
 * Keys are held wrapped rather than copied, and never mutated once added, so
 * {@link #copyFrom} shares the arrays it was handed. The repair plan derives one of
 * these per repair and the capture takes its own set from it, because the plan is
 * refilled by the next repair while a parked capture still owes its publication.
 */
public final class LiveViewCheckpointOutputKeyDomain implements Mutable {
    private final HashSet<ByteBuffer> keys = new HashSet<>();

    /**
     * Joins one encoded partition key to the domain. The array must not be mutated
     * afterwards: the set holds it rather than a copy.
     */
    public void add(@NotNull byte[] key) {
        keys.add(ByteBuffer.wrap(key));
    }

    @Override
    public void clear() {
        keys.clear();
    }

    public boolean contains(@NotNull byte[] key) {
        return keys.contains(ByteBuffer.wrap(key));
    }

    /**
     * Replaces this domain with {@code other}'s, sharing its key arrays.
     */
    public void copyFrom(@NotNull LiveViewCheckpointOutputKeyDomain other) {
        keys.clear();
        keys.addAll(other.keys);
    }

    public boolean isEmpty() {
        return keys.isEmpty();
    }

    public int size() {
        return keys.size();
    }
}
