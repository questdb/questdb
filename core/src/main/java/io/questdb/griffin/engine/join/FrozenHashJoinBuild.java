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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.std.QuietCloseable;

/**
 * Immutable lookup backing, borrowed from its builder for one execution. Publish
 * this object through the frame-task publication barrier before probing. The owner
 * must drain all probes and finish reading output symbols before closing the build.
 * Handles expire at close. Payload columns and their symbol tables come from the build's
 * input through its {@link HashJoinPayloadSource}, so they stay valid only while that input
 * stays open, and the owner closes the input after the build. A later partitioned
 * implementation can route keys without changing this contract.
 * <p>
 * A build hands out probes through the keyed sub-interface that matches its lookup:
 * {@link IntKeyed} for a single INT key and {@link RecordKeyed} for a key that a
 * {@link RecordSink} stages from the probe record. The iteration half of a probe is
 * the same either way, so consumers that only walk matches hold the base
 * {@link Probe}.
 */
public interface FrozenHashJoinBuild {
    long getKeyCount();

    long getRowCount();

    /** Allocated native bytes, including unused capacity. */
    long getSizeInBytes();

    /**
     * Row heap bytes that one build row takes. Every implementation stores its rows in the same
     * layout, a link plus the build row's id when the build has payload columns, so a build of N
     * rows fills N times this whatever its payload width, and whatever its key table adds on top.
     */
    static long getRowSize(boolean hasPayload) {
        return HashJoinRowHeap.getRowSize(hasPayload);
    }

    /** A build whose probes look up a single INT key. */
    interface IntKeyed extends FrozenHashJoinBuild {
        /**
         * Each acquired execution slot needs its own probe. Probes do not consult a circuit
         * breaker; callers check cancellation at frame boundaries and on a work budget.
         */
        IntProbe newProbe();
    }

    /**
     * Lookups keyed by a single INT. SYMBOL keys arrive translated into the build's key
     * domain, so they take this shape too.
     */
    interface IntProbe extends Probe {
        /** Replaces the current duplicate iterator, including on a miss. */
        void find(int key);

        /**
         * Positions the payload directly for a build with exactly one row per key.
         * The caller must establish rowCount == keyCount for this execution and
         * check cancellation at frame boundaries. Clears the duplicate iterator;
         * read the payload only when this returns true.
         */
        boolean findSingleUnchecked(int key);

        /**
         * Replaces the current duplicate iterator using the lookup metadata cached at reopen.
         * Read payload columns only after advancing a matching row.
         */
        void findUnchecked(int key);
    }

    /**
     * Walks the matches of one lookup and reads their payload. Every build implementation
     * shares this half; the keyed sub-interfaces add the lookup itself.
     * <p>
     * A probe may hold native memory of its own, so its owner closes it at the end of the
     * execution that charged it, before releasing that execution's memory tracker.
     * {@link #reopen()} brings a closed probe back for the next execution.
     */
    interface Probe extends SymbolTableSource, QuietCloseable {
        /**
         * The payload columns of the last row that {@link #next()}, a unique lookup or
         * {@link #recordAt(long)} positioned; null for a build without payload columns.
         */
        Record getRecord();

        boolean hasNext();

        /** Advances the payload record and returns an opaque, execution-local handle. */
        long next();

        /** Positions the payload record without changing the duplicate iterator. */
        void recordAt(long handle);

        /**
         * Explicitly bind this slot-owned view to a refreshed snapshot, after consumer drain.
         * This also takes fresh symbol tables from the build's input, so call it on the owner.
         */
        void reopen();
    }

    /** A build whose probes stage the key from a probe record through a {@link RecordSink}. */
    interface RecordKeyed extends FrozenHashJoinBuild {
        /**
         * Each acquired execution slot needs its own probe, and its own key sink: sinks hold
         * scratch state and must not be shared across workers. Probes do not consult a circuit
         * breaker; callers check cancellation at frame boundaries and on a work budget.
         */
        RecordProbe newProbe(RecordSink probeKeySink);
    }

    /**
     * Lookups keyed by the probe record, staged through the probe's own key sink. The key
     * columns of the probe record must match the build key's column order and types.
     */
    interface RecordProbe extends Probe {
        /** Replaces the current duplicate iterator, including on a miss. */
        void find(Record probeRecord);

        /**
         * Positions the payload directly for a build with exactly one row per key.
         * The caller must establish rowCount == keyCount for this execution and
         * check cancellation at frame boundaries. Clears the duplicate iterator;
         * read the payload only when this returns true.
         */
        boolean findSingleUnchecked(Record probeRecord);

        /**
         * Replaces the current duplicate iterator using the lookup metadata cached at reopen.
         * Read payload columns only after advancing a matching row.
         */
        void findUnchecked(Record probeRecord);
    }
}
